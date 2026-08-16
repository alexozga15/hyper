import unittest
from unittest.mock import patch

import pytest

from server import (
    RANKING_WINDOW_MS,
    RECENT_FILL_ALERT_LIMIT,
    WALLET_RECENT_FILL_CACHE_LIMIT,
    HyperliquidClient,
    TrackedWallet,
    WalletStore,
    WalletTrackerService,
)


class RecentFillTruncationMetadataTests(unittest.TestCase):
    """FIX 1: recentFills truncation must be visible on dataQuality."""

    @pytest.fixture(autouse=True)
    def _inject_tmp_path(self, tmp_path):
        self.tmp_path = tmp_path

    def setUp(self) -> None:
        self.service = WalletTrackerService(WalletStore(self.tmp_path / "wallets.json"), HyperliquidClient())

    def _truncated_snapshot(self, *, fill_count: int, now_ms: int) -> dict:
        wallet = TrackedWallet(address="0x1111111111111111111111111111111111111111", alias="", notes="", created_at="")
        state = {
            "marginSummary": {"accountValue": "1000000", "totalNtlPos": "0", "totalMarginUsed": "0"},
            "withdrawable": "1000000",
            "assetPositions": [],
        }
        # Fills one minute apart, newest first, all well inside the last day -
        # a "busy wallet" whose retained history is far shorter than a 7-day
        # question.
        fills = [
            {
                "coin": "BTC",
                "dir": "Open Long",
                "px": "70000",
                "sz": "1",
                "closedPnl": "0",
                "fee": "0",
                "time": now_ms - i * 60_000,
            }
            for i in range(fill_count)
        ]
        with patch("server.current_time_ms", return_value=now_ms), patch.object(
            self.service.client, "safe_subscribe_all_dexs_clearinghouse_state", return_value=state
        ), patch.object(
            self.service, "fetch_fills_result", return_value={"ok": True, "data": fills, "error": ""}
        ), patch.object(
            self.service, "fetch_recent_fills_result", return_value={"ok": True, "data": [], "error": ""}
        ), patch.object(
            self.service, "fetch_open_orders_result", return_value={"ok": True, "data": [], "error": ""}
        ), patch.object(
            self.service, "fetch_portfolio_result", return_value={"ok": True, "data": {}, "error": ""}
        ), patch.object(self.service, "fetch_wallet_role", return_value="user"):
            return self.service.fetch_wallet_snapshot(wallet)

    def test_truncation_metadata_is_written(self) -> None:
        now_ms = 1_700_000_000_000
        snapshot = self._truncated_snapshot(fill_count=RECENT_FILL_ALERT_LIMIT + 50, now_ms=now_ms)

        quality = snapshot["dataQuality"]
        self.assertTrue(quality["recentFillsTruncated"])
        expected_oldest = now_ms - (RECENT_FILL_ALERT_LIMIT - 1) * 60_000
        self.assertEqual(quality["oldestFillTime"], expected_oldest)
        self.assertEqual(quality["fillCoverageMs"], now_ms - expected_oldest)
        self.assertEqual(len(snapshot["recentFills"]), RECENT_FILL_ALERT_LIMIT)

    def test_untruncated_snapshot_reports_no_truncation(self) -> None:
        now_ms = 1_700_000_000_000
        snapshot = self._truncated_snapshot(fill_count=5, now_ms=now_ms)

        quality = snapshot["dataQuality"]
        self.assertFalse(quality["recentFillsTruncated"])

    def test_merge_with_cache_can_truncate_even_when_fetched_page_did_not(self) -> None:
        """FIX 3: recentFillsTruncated must describe the *merged* list.

        A wallet can fetch a small, untruncated page this cycle and still have
        the merge with WALLET_RECENT_FILL_CACHE_LIMIT worth of retained cache
        history get cut - that must be visible on dataQuality too, not just
        page-level truncation.
        """
        now_ms = 1_700_000_000_000
        wallet = TrackedWallet(
            address="0x2222222222222222222222222222222222222222", alias="", notes="", created_at=""
        )
        state = {
            "marginSummary": {"accountValue": "1000000", "totalNtlPos": "0", "totalMarginUsed": "0"},
            "withdrawable": "1000000",
            "assetPositions": [],
        }
        fetched_count = 10
        self.assertLess(fetched_count, RECENT_FILL_ALERT_LIMIT)
        fetched_fills = [
            {
                "coin": "BTC",
                "dir": "Open Long",
                "px": "70000",
                "sz": "1",
                "closedPnl": "0",
                "fee": "0",
                "time": now_ms - i * 60_000,
            }
            for i in range(fetched_count)
        ]
        cached_count = WALLET_RECENT_FILL_CACHE_LIMIT + 5
        cached_snapshot = {
            "recentFills": [
                {
                    "coin": "BTC",
                    "direction": "Open Long",
                    "price": 70000,
                    "size": 1,
                    "time": now_ms - (fetched_count + i) * 60_000,
                }
                for i in range(cached_count)
            ]
        }
        with patch("server.current_time_ms", return_value=now_ms), patch.object(
            self.service.client, "safe_subscribe_all_dexs_clearinghouse_state", return_value=state
        ), patch.object(
            self.service, "fetch_fills_result", return_value={"ok": True, "data": fetched_fills, "error": ""}
        ), patch.object(
            self.service, "fetch_recent_fills_result", return_value={"ok": True, "data": [], "error": ""}
        ), patch.object(
            self.service, "fetch_open_orders_result", return_value={"ok": True, "data": [], "error": ""}
        ), patch.object(
            self.service, "fetch_portfolio_result", return_value={"ok": True, "data": {}, "error": ""}
        ), patch.object(self.service, "fetch_wallet_role", return_value="user"):
            snapshot = self.service.fetch_wallet_snapshot(wallet, cached_snapshot=cached_snapshot)

        quality = snapshot["dataQuality"]
        self.assertEqual(len(snapshot["recentFills"]), WALLET_RECENT_FILL_CACHE_LIMIT)
        self.assertTrue(quality["recentFillsTruncated"])
        expected_oldest = min(int(fill["time"]) for fill in snapshot["recentFills"])
        self.assertEqual(quality["oldestFillTime"], expected_oldest)
        self.assertEqual(quality["fillCoverageMs"], now_ms - expected_oldest)

    def test_wallet_fill_window_covered_false_when_truncated_and_short(self) -> None:
        now_ms = 1_700_000_000_000
        snapshot = self._truncated_snapshot(fill_count=RECENT_FILL_ALERT_LIMIT + 50, now_ms=now_ms)

        # Retained history spans under two hours; a 7-day question cannot be
        # answered from it.
        self.assertFalse(
            self.service.wallet_fill_window_covered(snapshot, now_ms=now_ms, window_ms=RANKING_WINDOW_MS)
        )
        # A window shorter than the retained coverage is answerable.
        self.assertTrue(
            self.service.wallet_fill_window_covered(snapshot, now_ms=now_ms, window_ms=30 * 60_000)
        )

    def test_wallet_fill_window_covered_true_when_metadata_absent(self) -> None:
        # Snapshots written before this field existed keep the old, permissive
        # behaviour.
        snapshot = {"address": "0x1", "dataQuality": {"fillsOk": True}}
        self.assertTrue(
            self.service.wallet_fill_window_covered(snapshot, now_ms=1_700_000_000_000, window_ms=RANKING_WINDOW_MS)
        )

    def test_is_wallet_dormant_returns_false_when_window_not_covered(self) -> None:
        now_ms = 1_700_000_000_000
        snapshot = {
            "address": "0x1111111111111111111111111111111111111111",
            "recentFills": [
                {"coin": "BTC", "direction": "Open Long", "time": now_ms - 3 * 60 * 60 * 1000}
            ],
            "dataQuality": {
                "fillsOk": True,
                "fillsUsable": True,
                "fillsDegraded": False,
                # Only ~3 hours of history retained - far short of the 7-day
                # dormancy window.
                "recentFillsTruncated": True,
                "oldestFillTime": now_ms - 3 * 60 * 60 * 1000,
            },
        }

        self.assertFalse(
            self.service.is_wallet_dormant(snapshot, now_ms=now_ms, window_ms=RANKING_WINDOW_MS)
        )

    def test_is_wallet_dormant_still_true_when_window_covered(self) -> None:
        now_ms = 1_700_000_000_000
        day_ms = 24 * 60 * 60 * 1000
        snapshot = {
            "address": "0x1111111111111111111111111111111111111111",
            "recentFills": [{"coin": "BTC", "direction": "Open Long", "time": now_ms - 9 * day_ms}],
            "dataQuality": {
                "fillsOk": True,
                "fillsUsable": True,
                "fillsDegraded": False,
                "recentFillsTruncated": True,
                "oldestFillTime": now_ms - 9 * day_ms,
            },
        }

        self.assertTrue(
            self.service.is_wallet_dormant(snapshot, now_ms=now_ms, window_ms=RANKING_WINDOW_MS)
        )


class CorrelationJaccardGuardTests(unittest.TestCase):
    """FIX 2: overlap coefficient merges must also clear a Jaccard floor."""

    @pytest.fixture(autouse=True)
    def _inject_tmp_path(self, tmp_path):
        self.tmp_path = tmp_path

    def setUp(self) -> None:
        self.service = WalletTrackerService(WalletStore(self.tmp_path / "wallets.json"), HyperliquidClient())

    @staticmethod
    def _fills(coins: list[str], *, time_ms: int) -> list[dict]:
        return [
            {"coin": coin, "direction": "Open Long", "time": time_ms}
            for coin in coins
        ]

    def test_small_fingerprint_is_not_swallowed_by_a_large_one(self) -> None:
        time_ms = 1_700_000_000_000
        shared_coins = ["SHARE0", "SHARE1", "SHARE2"]
        right_only_coins = [f"UNIQ{i}" for i in range(497)]

        snapshots = [
            {
                "address": "0x1111111111111111111111111111111111111111",
                "recentFills": self._fills(shared_coins, time_ms=time_ms),
            },
            {
                "address": "0x2222222222222222222222222222222222222222",
                "recentFills": self._fills(shared_coins + right_only_coins, time_ms=time_ms),
            },
        ]

        groups = self.service.build_wallet_correlation_groups(snapshots)

        # Overlap coefficient alone would be 3/3 == 1.0 and merge these; the
        # Jaccard guard (3 / 500 << 0.15) must block it.
        self.assertNotEqual(
            groups["0x1111111111111111111111111111111111111111"],
            groups["0x2222222222222222222222222222222222222222"],
        )

    def test_similar_sized_heavily_overlapping_wallets_still_merge(self) -> None:
        time_ms = 1_700_000_000_000
        shared_coins = [f"SHARE{i}" for i in range(8)]
        left_only = ["LEFT0", "LEFT1"]
        right_only = ["RIGHT0", "RIGHT1"]

        snapshots = [
            {
                "address": "0x1111111111111111111111111111111111111111",
                "recentFills": self._fills(shared_coins + left_only, time_ms=time_ms),
            },
            {
                "address": "0x2222222222222222222222222222222222222222",
                "recentFills": self._fills(shared_coins + right_only, time_ms=time_ms),
            },
        ]

        groups = self.service.build_wallet_correlation_groups(snapshots)

        # overlap = 8/10 = 0.8 >= 0.5, jaccard = 8/12 ~= 0.667 >= 0.15: merges.
        self.assertEqual(
            groups["0x1111111111111111111111111111111111111111"],
            groups["0x2222222222222222222222222222222222222222"],
        )


class ConsensusTotalSizeTests(unittest.TestCase):
    """FIX 3: consensus rows must carry totalSize alongside markPrice."""

    @pytest.fixture(autouse=True)
    def _inject_tmp_path(self, tmp_path):
        self.tmp_path = tmp_path

    def setUp(self) -> None:
        self.service = WalletTrackerService(WalletStore(self.tmp_path / "wallets.json"), HyperliquidClient())

    def test_total_size_present_in_consensus_row(self) -> None:
        now_ms = 1_700_000_000_000
        snapshots = [
            {
                "address": "0x1111111111111111111111111111111111111111",
                "alias": "One",
                "positions": [{"coin": "BTC", "side": "Long", "positionValue": 700_000.0, "size": 10.0}],
                "dataQuality": {"fillsOk": True, "fillsUsable": True, "fillsDegraded": False},
                "recentFills": [
                    {"coin": "BTC", "direction": "Open Long", "time": now_ms - 60 * 60 * 1000}
                ],
            }
        ]

        with patch("server.current_time_ms", return_value=now_ms):
            summary = self.service.build_sentiment_summary(snapshots, min_wallets=1)

        item = summary["consensus"][0]
        self.assertIn("totalSize", item)
        self.assertEqual(item["totalSize"], 10.0)
        self.assertEqual(item["markPrice"], round(700_000.0 / 10.0, 8))


if __name__ == "__main__":
    unittest.main()
