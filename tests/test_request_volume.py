from __future__ import annotations

import unittest
from unittest.mock import patch

from scripts.run_health_monitor import detect_health_issues
from server import (
    WALLET_IDLE_FILL_THRESHOLD_MS,
    WALLET_LIVE_FILL_SKIP_MAX,
    WALLET_WINDOW_FILL_CAP,
    HyperliquidClient,
    RequestRateLimiter,
    TrackedWallet,
    WalletTrackerService,
    cached_window_fill_count,
    iso_to_ms,
)

NOW_MS = 2_000_000_000_000
MINUTE_MS = 60 * 1000
HOUR_MS = 60 * MINUTE_MS
DAY_MS = 24 * HOUR_MS


def wallet(address: str) -> TrackedWallet:
    return TrackedWallet(address=address, alias=address, notes="", created_at="")


def cache_entry(*, last_fill_ms: int, fetched_ms: int) -> dict[str, object]:
    return {
        "recentFills": [{"coin": "BTC", "direction": "Open Long", "time": last_fill_ms}],
        "fillFetchedAtMs": fetched_ms,
    }


def aged_out_entry(*, idle_days: float, fetched_ms: int) -> dict[str, object]:
    """A wallet quiet for longer than the recent-fill cache retains.

    Its fills have aged out of the cache, so the only remaining evidence of
    when it last traded is the cached daysSinceLastFill.
    """
    return {"recentFills": [], "daysSinceLastFill": idle_days, "fillFetchedAtMs": fetched_ms}


class IdleFillBackoffTests(unittest.TestCase):
    """The idle-wallet backoff decides which wallets skip their fill fetch."""

    def setUp(self) -> None:
        self.service = WalletTrackerService(object(), HyperliquidClient(RequestRateLimiter(1000)))

    def skip_for(self, entry: object, address: str = "0xabc") -> set[str]:
        cache = {address: entry} if entry is not None else {}
        return self.service.wallet_idle_fill_skip_addresses(
            [wallet(address)], cache, now_ms=NOW_MS
        )

    def test_idle_wallet_fetched_recently_is_skipped(self) -> None:
        entry = cache_entry(last_fill_ms=NOW_MS - 5 * DAY_MS, fetched_ms=NOW_MS - MINUTE_MS)
        self.assertEqual(self.skip_for(entry), {"0xabc"})

    def test_active_wallet_is_never_skipped(self) -> None:
        entry = cache_entry(last_fill_ms=NOW_MS - HOUR_MS, fetched_ms=NOW_MS - MINUTE_MS)
        self.assertEqual(self.skip_for(entry), set())

    def test_wallet_just_under_the_idle_threshold_is_not_skipped(self) -> None:
        entry = cache_entry(
            last_fill_ms=NOW_MS - WALLET_IDLE_FILL_THRESHOLD_MS + MINUTE_MS,
            fetched_ms=NOW_MS - MINUTE_MS,
        )
        self.assertEqual(self.skip_for(entry), set())

    def test_idle_wallet_due_for_its_slow_poll_is_fetched(self) -> None:
        entry = cache_entry(last_fill_ms=NOW_MS - 5 * DAY_MS, fetched_ms=NOW_MS - 2 * HOUR_MS)
        self.assertEqual(self.skip_for(entry), set())

    def test_long_idle_wallet_is_skipped_after_its_fills_age_out(self) -> None:
        """The quietest wallets are the point of the backoff.

        The recent-fill cache only retains a week, so a wallet silent for
        longer has an empty fill list. Reading idleness from that list alone
        made exactly these wallets look like wallets with no data, and they
        were polled every cycle forever.
        """
        for idle_days in (8.1, 30.1, 61.4):
            entry = aged_out_entry(idle_days=idle_days, fetched_ms=NOW_MS - MINUTE_MS)
            self.assertEqual(self.skip_for(entry), {"0xabc"}, f"idle {idle_days}d")

    def test_known_idle_age_wins_over_the_cached_fill_list(self) -> None:
        entry = {
            "recentFills": [{"coin": "BTC", "direction": "Open Long", "time": NOW_MS - HOUR_MS}],
            "daysSinceLastFill": 9.0,
            "fillFetchedAtMs": NOW_MS - MINUTE_MS,
        }
        self.assertEqual(self.skip_for(entry), {"0xabc"})

    def test_wallet_with_no_evidence_at_all_is_never_skipped(self) -> None:
        """Silence with no data behind it is not evidence of idleness."""
        self.assertEqual(self.skip_for({"recentFills": [], "fillFetchedAtMs": NOW_MS}), set())
        self.assertEqual(self.skip_for({"fillFetchedAtMs": NOW_MS}), set())
        self.assertEqual(self.skip_for(None), set())

    def test_recently_active_wallet_with_aged_out_list_is_not_skipped(self) -> None:
        entry = aged_out_entry(idle_days=0.5, fetched_ms=NOW_MS - MINUTE_MS)
        self.assertEqual(self.skip_for(entry), set())

    def test_wallet_never_fill_fetched_is_not_skipped(self) -> None:
        entry = {"recentFills": [{"coin": "BTC", "direction": "Open Long", "time": NOW_MS - 5 * DAY_MS}]}
        self.assertEqual(self.skip_for(entry), set())

    def test_zero_interval_disables_the_backoff(self) -> None:
        entry = cache_entry(last_fill_ms=NOW_MS - 5 * DAY_MS, fetched_ms=NOW_MS - MINUTE_MS)
        with patch("server.WALLET_IDLE_FILL_INTERVAL_MS", 0):
            self.assertEqual(self.skip_for(entry), set())


class LiveFillSkipTests(unittest.TestCase):
    """The live userFills request is only worth issuing for a capped page."""

    def setUp(self) -> None:
        self.service = WalletTrackerService(object(), HyperliquidClient(RequestRateLimiter(1000)))

    def skip_for(self, entry: object, address: str = "0xabc") -> set[str]:
        cache = {address: entry} if entry is not None else {}
        return self.service.wallet_live_fill_skip_addresses([wallet(address)], cache)

    def test_capped_page_keeps_the_live_request(self) -> None:
        """A page at the cap is the one case that can be missing newest fills."""
        self.assertEqual(self.skip_for({"windowFillCount": WALLET_WINDOW_FILL_CAP}), set())

    def test_page_well_under_the_cap_is_skipped(self) -> None:
        self.assertEqual(self.skip_for({"windowFillCount": 300}), {"0xabc"})
        self.assertEqual(self.skip_for({"windowFillCount": 0}), {"0xabc"})

    def test_page_inside_the_margin_keeps_the_live_request(self) -> None:
        """The margin covers a wallet whose fill count climbs between cycles."""
        self.assertEqual(self.skip_for({"windowFillCount": WALLET_LIVE_FILL_SKIP_MAX}), set())
        self.assertEqual(self.skip_for({"windowFillCount": WALLET_LIVE_FILL_SKIP_MAX - 1}), {"0xabc"})

    def test_unknown_count_is_never_skipped(self) -> None:
        """No recorded page is not the same as a small one."""
        for entry in ({}, {"windowFillCount": None}, {"windowFillCount": "300"}, {"windowFillCount": -1}, None):
            self.assertEqual(self.skip_for(entry), set(), repr(entry))

    def test_zero_max_disables_the_skip(self) -> None:
        with patch("server.WALLET_LIVE_FILL_SKIP_MAX", 0):
            self.assertEqual(self.skip_for({"windowFillCount": 10}), set())

    def test_booleans_are_not_counts(self) -> None:
        self.assertIsNone(cached_window_fill_count({"windowFillCount": True}))


class LiveFillSkipAccountingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.service = WalletTrackerService(object(), HyperliquidClient(RequestRateLimiter(1000)))

    def test_skipped_live_fetch_is_not_a_failure(self) -> None:
        snapshot = {"dataQuality": {"fillsFetchOk": True, "recentFillsSkipped": True}}
        self.assertFalse(self.service.wallet_fill_fetch_failed(snapshot))
        self.assertTrue(self.service.wallet_live_fill_skipped(snapshot))

    def test_full_fetch_skip_is_reported_separately(self) -> None:
        """The idle backoff and the live-request skip are different states."""
        snapshot = {"dataQuality": {"fillsFetchSkipped": True, "recentFillsSkipped": False}}
        self.assertTrue(self.service.wallet_fill_fetch_skipped(snapshot))
        self.assertFalse(self.service.wallet_live_fill_skipped(snapshot))


class WindowFillCountTests(unittest.TestCase):
    """The recorded page length is what the next cycle's skip decision reads."""

    ADDRESS = "0x1111111111111111111111111111111111111111"

    def setUp(self) -> None:
        self.service = WalletTrackerService(object(), HyperliquidClient(RequestRateLimiter(1000)))

    def snapshot(self, *, page: list[dict[str, object]], **kwargs: object) -> tuple[dict, object]:
        tracked = TrackedWallet(address=self.ADDRESS, alias="", notes="", created_at="")
        state = {"marginSummary": {"accountValue": "1"}, "withdrawable": "1", "assetPositions": []}
        with patch.object(
            self.service.client, "safe_subscribe_all_dexs_clearinghouse_state", return_value=state
        ), patch.object(
            self.service, "fetch_fills_result", return_value={"ok": True, "data": page, "error": ""}
        ), patch.object(
            self.service, "fetch_recent_fills_result", return_value={"ok": True, "data": [], "error": ""}
        ) as live, patch.object(
            self.service, "fetch_open_orders_result", return_value={"ok": True, "data": [], "error": ""}
        ), patch.object(
            self.service, "fetch_portfolio_result", return_value={"ok": True, "data": {}, "error": ""}
        ), patch.object(self.service, "fetch_wallet_role", return_value="user"):
            return self.service.fetch_wallet_snapshot(tracked, **kwargs), live

    def test_raw_page_length_is_recorded(self) -> None:
        page = [{"coin": "BTC", "dir": "Open Long", "px": "1", "sz": "1", "time": NOW_MS, "tid": i} for i in range(7)]
        snapshot, _ = self.snapshot(page=page)
        self.assertEqual(snapshot["dataQuality"]["windowFillCount"], 7)

    def test_idle_skipped_cycle_does_not_clobber_the_cached_count(self) -> None:
        """The idle backoff sends no request, so its empty page means nothing.

        Recording 0 for it would mark a wallet that sits right below the cap as
        comfortably under it, and the live request would be dropped on evidence
        that was never gathered.
        """
        snapshot, _ = self.snapshot(
            page=[],
            full_quality_refresh=False,
            skip_fill_fetch=True,
            cached_snapshot={"windowFillCount": 1900},
        )
        self.assertEqual(snapshot["dataQuality"]["windowFillCount"], 1900)

    def test_idle_skipped_cycle_with_no_cached_count_records_nothing(self) -> None:
        snapshot, _ = self.snapshot(page=[], full_quality_refresh=False, skip_fill_fetch=True)
        self.assertNotIn("windowFillCount", snapshot["dataQuality"])

    def test_skipping_the_live_request_issues_no_call_and_is_not_a_failure(self) -> None:
        snapshot, live = self.snapshot(page=[], full_quality_refresh=False, skip_live_fill_fetch=True)
        live.assert_not_called()
        self.assertTrue(snapshot["dataQuality"]["recentFillsSkipped"])
        self.assertTrue(snapshot["dataQuality"]["fillsFetchOk"])
        self.assertFalse(self.service.wallet_fill_fetch_failed(snapshot))

    def test_full_quality_refresh_always_issues_the_live_request(self) -> None:
        _, live = self.snapshot(page=[], full_quality_refresh=True, skip_live_fill_fetch=True)
        live.assert_called_once()


class SkippedFetchAccountingTests(unittest.TestCase):
    """A skipped fetch must not be reported as a failed one."""

    def setUp(self) -> None:
        self.service = WalletTrackerService(object(), HyperliquidClient(RequestRateLimiter(1000)))

    def test_skipped_fetch_is_not_a_failure(self) -> None:
        snapshot = {"dataQuality": {"fillsFetchOk": False, "fillsFetchSkipped": True}}
        self.assertFalse(self.service.wallet_fill_fetch_failed(snapshot))
        self.assertTrue(self.service.wallet_fill_fetch_skipped(snapshot))

    def test_real_failure_still_counts(self) -> None:
        snapshot = {"dataQuality": {"fillsFetchOk": False, "fillsFetchSkipped": False}}
        self.assertTrue(self.service.wallet_fill_fetch_failed(snapshot))
        self.assertFalse(self.service.wallet_fill_fetch_skipped(snapshot))

    def test_successful_fetch_is_neither(self) -> None:
        snapshot = {"dataQuality": {"fillsFetchOk": True}}
        self.assertFalse(self.service.wallet_fill_fetch_failed(snapshot))
        self.assertFalse(self.service.wallet_fill_fetch_skipped(snapshot))


class ThrottleVisibilityTests(unittest.TestCase):
    def test_penalize_is_counted_and_reported(self) -> None:
        limiter = RequestRateLimiter(6)
        self.assertEqual(limiter.throttle_report()["events"], 0)
        limiter.penalize(1.5)
        limiter.penalize(0.5)
        report = limiter.throttle_report()
        self.assertEqual(report["events"], 2)
        self.assertAlmostEqual(report["backoffSeconds"], 2.0)
        self.assertAlmostEqual(report["requestsPerSecond"], 6.0)


class StaleThresholdTests(unittest.TestCase):
    CHECKED_AT = "2033-05-18T03:21:00Z"

    def issues_at(self, age_ms: int) -> list[str]:
        checked_ms = iso_to_ms(self.CHECKED_AT)
        self.assertGreater(checked_ms, 0, "fixture timestamp must parse")
        return detect_health_issues(
            {"state": {"lastCheckedAt": self.CHECKED_AT}},
            {"cacheCoverage": 1.0},
            now_ms=checked_ms + age_ms,
            disk_free_pct=50,
        )

    def test_threshold_tolerates_a_missed_ten_minute_run(self) -> None:
        """A 12-minute-old check is one late run, not an outage."""
        self.assertNotIn("sentiment check stale >25m", self.issues_at(12 * MINUTE_MS))

    def test_threshold_fires_once_two_runs_are_missed(self) -> None:
        self.assertIn("sentiment check stale >25m", self.issues_at(26 * MINUTE_MS))

    def test_threshold_still_fires_when_genuinely_stale(self) -> None:
        issues = detect_health_issues(
            {"state": {"lastCheckedAt": "2020-01-01T00:00:00Z"}},
            {"cacheCoverage": 1.0},
            now_ms=NOW_MS,
            disk_free_pct=50,
        )
        self.assertIn("sentiment check stale >25m", issues)


class WindowPhrasingTests(unittest.TestCase):
    """The window is env-tunable, so no message may spell it out by hand."""

    def test_phrasing_tracks_the_constant(self) -> None:
        from server import WALLET_SIGNAL_ACTIVITY_WINDOW_MS, format_window_minutes

        self.assertEqual(format_window_minutes(WALLET_SIGNAL_ACTIVITY_WINDOW_MS), "2 hours")

    def test_phrasing_handles_the_units(self) -> None:
        from server import format_window_minutes

        self.assertEqual(format_window_minutes(60_000), "1 minute")
        self.assertEqual(format_window_minutes(15 * 60_000), "15 minutes")
        self.assertEqual(format_window_minutes(60 * 60_000), "1 hour")
        self.assertEqual(format_window_minutes(120 * 60_000), "2 hours")


if __name__ == "__main__":
    unittest.main()
