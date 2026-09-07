import unittest
from typing import Any

import pytest

import server
from server import (
    CMM_HEATMAP_HISTORY_RETENTION_MS,
    CMM_HEATMAP_HISTORY_WINDOWS,
    CMM_HEATMAP_SNAPSHOT_INTERVAL_MS,
    HyperliquidClient,
    WalletStore,
    WalletTrackerService,
)


class FakeCmmHeatmapClient:
    def __init__(self, responses: dict[str, list[dict]] | None = None, *, error_on: str | None = None) -> None:
        self.responses = responses or {}
        self.error_on = error_on
        self.calls: list[str] = []

    def positions_heatmap(self, *, opened_within: str):
        self.calls.append(opened_within)
        if self.error_on == opened_within:
            raise server.CoinMarketManApiError("boom")
        return self.responses.get(opened_within, [])


def _row(coin: str, *, total_value: float = 1_000_000.0) -> dict:
    return {
        "coin": coin,
        "totalValue": total_value,
        "totalLongValue": total_value * 0.6,
        "totalShortValue": total_value * 0.4,
        "count": 10,
        "countLong": 6,
        "countShort": 4,
    }


class CmmHeatmapHistoryTests(unittest.TestCase):
    @pytest.fixture(autouse=True)
    def _inject(self, tmp_path, monkeypatch):
        self.tmp_path = tmp_path
        monkeypatch.setattr(server, "CMM_HEATMAP_HISTORY_FILE", tmp_path / "cmm_heatmap_history.json")

    def setUp(self) -> None:
        self.service = WalletTrackerService(WalletStore(self.tmp_path / "wallets.json"), HyperliquidClient())

    # -- compact_cmm_heatmap_rows --------------------------------------------

    def test_compact_rows_preserves_coin_case_and_dex_prefix(self) -> None:
        rows = [_row("xyz:NVDA")]
        compacted = self.service.compact_cmm_heatmap_rows(rows)
        self.assertEqual(compacted[0]["coin"], "xyz:NVDA")

    def test_compact_rows_drops_empty_coin_and_non_positive_value(self) -> None:
        rows = [
            _row(""),
            {**_row("BTC"), "totalValue": 0},
            {**_row("ETH"), "totalValue": -5},
            _row("SOL"),
        ]
        compacted = self.service.compact_cmm_heatmap_rows(rows)
        self.assertEqual([row["coin"] for row in compacted], ["SOL"])

    def test_compact_rows_emit_only_seven_expected_keys(self) -> None:
        compacted = self.service.compact_cmm_heatmap_rows([_row("BTC")])
        self.assertEqual(
            set(compacted[0].keys()),
            {"coin", "totalValue", "totalLongValue", "totalShortValue", "count", "countLong", "countShort"},
        )

    # -- capture_cmm_heatmap_snapshot -----------------------------------------

    def test_snapshot_within_interval_short_circuits_without_api_calls(self) -> None:
        fake = FakeCmmHeatmapClient()
        self.service.cmm_client = fake
        history = {
            "version": 1,
            "snapshots": [{"at": "x", "atMs": 1_000, "windows": {}, "rowCounts": {}, "totals": {}}],
        }
        result = self.service.capture_cmm_heatmap_snapshot(
            now_ms=1_000 + CMM_HEATMAP_SNAPSHOT_INTERVAL_MS - 1,
            history=history,
        )
        self.assertIsNone(result)
        self.assertEqual(fake.calls, [])

    def test_snapshot_past_interval_calls_all_windows_and_appends(self) -> None:
        fake = FakeCmmHeatmapClient(
            responses={window: [_row("BTC"), _row("xyz:NVDA")] for window in CMM_HEATMAP_HISTORY_WINDOWS}
        )
        self.service.cmm_client = fake
        history = {
            "version": 1,
            "snapshots": [{"at": "x", "atMs": 0, "windows": {}, "rowCounts": {}, "totals": {}}],
        }
        now_ms = CMM_HEATMAP_SNAPSHOT_INTERVAL_MS + 1
        result = self.service.capture_cmm_heatmap_snapshot(now_ms=now_ms, history=history)
        self.assertEqual(len(fake.calls), len(CMM_HEATMAP_HISTORY_WINDOWS))
        self.assertIsNotNone(result)
        self.assertEqual(len(history["snapshots"]), 2)
        for window in CMM_HEATMAP_HISTORY_WINDOWS:
            self.assertEqual(len(result["windows"][window]), 2)

    def test_snapshot_aborts_and_preserves_history_if_a_window_errors(self) -> None:
        windows = list(CMM_HEATMAP_HISTORY_WINDOWS)
        assert len(windows) >= 2
        fake = FakeCmmHeatmapClient(
            responses={window: [_row("BTC")] for window in windows},
            error_on=windows[1],
        )
        self.service.cmm_client = fake
        original_snapshots = [{"at": "x", "atMs": 0, "windows": {}, "rowCounts": {}, "totals": {}}]
        history = {"version": 1, "snapshots": list(original_snapshots)}
        now_ms = CMM_HEATMAP_SNAPSHOT_INTERVAL_MS + 1
        result = self.service.capture_cmm_heatmap_snapshot(now_ms=now_ms, history=history)
        self.assertIsNone(result)
        self.assertEqual(history["snapshots"], original_snapshots)
        self.assertIn("lastError", history)

    def test_snapshots_older_than_retention_are_pruned(self) -> None:
        fake = FakeCmmHeatmapClient(responses={window: [_row("BTC")] for window in CMM_HEATMAP_HISTORY_WINDOWS})
        self.service.cmm_client = fake
        now_ms = CMM_HEATMAP_HISTORY_RETENTION_MS + CMM_HEATMAP_SNAPSHOT_INTERVAL_MS * 2
        old_ms = now_ms - CMM_HEATMAP_HISTORY_RETENTION_MS - 1
        recent_ms = now_ms - CMM_HEATMAP_HISTORY_RETENTION_MS + CMM_HEATMAP_SNAPSHOT_INTERVAL_MS
        history = {
            "version": 1,
            "snapshots": [
                {"at": "old", "atMs": old_ms, "windows": {}, "rowCounts": {}, "totals": {}},
                {"at": "recent", "atMs": recent_ms, "windows": {}, "rowCounts": {}, "totals": {}},
            ],
        }
        self.service.capture_cmm_heatmap_snapshot(now_ms=now_ms, history=history)
        remaining_ats = [s["at"] for s in history["snapshots"]]
        self.assertNotIn("old", remaining_ats)
        self.assertIn("recent", remaining_ats)

    def test_absent_history_file_treated_as_empty(self) -> None:
        fake = FakeCmmHeatmapClient(responses={window: [_row("BTC")] for window in CMM_HEATMAP_HISTORY_WINDOWS})
        self.service.cmm_client = fake
        result = self.service.capture_cmm_heatmap_snapshot(now_ms=10_000)
        self.assertIsNotNone(result)
        self.assertEqual(len(fake.calls), len(CMM_HEATMAP_HISTORY_WINDOWS))

    def test_corrupt_history_file_treated_as_empty(self) -> None:
        history_path = self.tmp_path / "cmm_heatmap_history.json"
        history_path.write_text("{not valid json", encoding="utf-8")
        fake = FakeCmmHeatmapClient(responses={window: [_row("BTC")] for window in CMM_HEATMAP_HISTORY_WINDOWS})
        self.service.cmm_client = fake
        result = self.service.capture_cmm_heatmap_snapshot(now_ms=10_000)
        self.assertIsNotNone(result)


if __name__ == "__main__":
    unittest.main()
