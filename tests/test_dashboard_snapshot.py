"""Telegram commands must reuse the sentiment run's dashboard snapshot.

Two systemd timers each used to build a full 33-wallet dashboard independently:
the sentiment check every 5 minutes and the Telegram command handler every
minute. The Hyperliquid rate limiter is a module-level singleton, so it is
per-process with no cross-process coordination, and each process behaved as if
it owned the whole request budget. Measured on production within one minute:
the scheduled sweep running alone reported 0 of 33 wallets with unavailable
fill data, while a Telegram command building its own dashboard during that
sweep reported 24 of 33.
"""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import scripts.process_telegram_commands as commands
from server import (
    DASHBOARD_SNAPSHOT_DROPPED_FILL_FIELDS,
    DASHBOARD_SNAPSHOT_DROPPED_WALLET_FIELDS,
    DASHBOARD_SNAPSHOT_FILL_LIMIT,
    DASHBOARD_SNAPSHOT_MAX_AGE_SECONDS,
    build_dashboard_snapshot,
    current_time_ms,
    now_iso,
)


def iso_minutes_ago(minutes: float) -> str:
    from datetime import datetime, timedelta, timezone

    moment = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    return moment.isoformat().replace("+00:00", "Z")


def dashboard_payload(generated_at: str) -> dict:
    return {
        "generatedAt": generated_at,
        "wallets": [
            {
                "address": "0x" + "a" * 40,
                "alias": "Alpha",
                "accountValue": 1_000_000.0,
                "positions": [{"coin": "BTC", "side": "long", "positionValue": 500_000.0}],
                "recentFills": [
                    {
                        "coin": "BTC",
                        "direction": "Open Long",
                        "price": 60_000.0,
                        "size": 1.0,
                        "time": current_time_ms(),
                        "closedPnl": 12.0,
                        "fee": 3.0,
                    }
                ],
                "performance": {"day": {"pnl": 1.0}, "allTime": {"pnl": 2.0}},
                "openOrders": [{"coin": "BTC", "size": 1.0}],
            }
        ],
    }


class SnapshotTrimmingTests(unittest.TestCase):
    def test_snapshot_drops_only_fields_no_reply_reads(self) -> None:
        snapshot = build_dashboard_snapshot(dashboard_payload(now_iso()))
        wallet = snapshot["wallets"][0]

        # Named explicitly rather than looped over the constant: iterating the
        # same tuple the implementation uses passes vacuously if it is emptied.
        for dropped in ("performance", "openOrders"):
            self.assertNotIn(dropped, wallet)
        for dropped in ("closedPnl", "fee"):
            self.assertNotIn(dropped, wallet["recentFills"][0])
        # And the constants must still describe what is actually dropped.
        self.assertIn("performance", DASHBOARD_SNAPSHOT_DROPPED_WALLET_FIELDS)
        self.assertIn("closedPnl", DASHBOARD_SNAPSHOT_DROPPED_FILL_FIELDS)

        # Everything the position, ranking and elite replies read must survive.
        for kept in ("address", "alias", "accountValue", "positions"):
            self.assertIn(kept, wallet)
        for kept in ("coin", "direction", "price", "size", "time"):
            self.assertIn(kept, wallet["recentFills"][0])

    def test_snapshot_caps_fills_per_wallet_at_the_ui_limit(self) -> None:
        """FIX 4: the UI-serving snapshot must not inflate with the analysis
        cache's (much larger) retained-fill cap. It slices to
        DASHBOARD_SNAPSHOT_FILL_LIMIT per wallet regardless of how many fills
        the source dashboard carries.
        """
        fill_count = DASHBOARD_SNAPSHOT_FILL_LIMIT + 25
        payload = dashboard_payload(now_iso())
        payload["wallets"][0]["recentFills"] = [
            {
                "coin": "BTC",
                "direction": "Open Long",
                "price": 60_000.0,
                "size": 1.0,
                "time": current_time_ms() - i * 60_000,
                "closedPnl": 0.0,
                "fee": 0.0,
            }
            for i in range(fill_count)
        ]

        snapshot = build_dashboard_snapshot(payload)

        fills = snapshot["wallets"][0]["recentFills"]
        self.assertEqual(len(fills), DASHBOARD_SNAPSHOT_FILL_LIMIT)
        # The newest fills (front of the source list) are the ones kept.
        self.assertEqual(fills[0]["time"], payload["wallets"][0]["recentFills"][0]["time"])

    def test_snapshot_preserves_the_build_time_not_the_save_time(self) -> None:
        built_at = iso_minutes_ago(4)
        snapshot = build_dashboard_snapshot(dashboard_payload(built_at))
        # The "Updated:" line in replies reads generatedAt, so a reused snapshot
        # must advertise when the sweep happened, not when it was answered.
        self.assertEqual(snapshot["generatedAt"], built_at)


class SnapshotReuseTests(unittest.TestCase):
    def setUp(self) -> None:
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        self.path = Path(temp_dir.name) / "dashboard_snapshot.json"
        patcher = patch.object(commands, "DASHBOARD_SNAPSHOT_FILE", self.path)
        patcher.start()
        self.addCleanup(patcher.stop)

    def write_snapshot(self, generated_at: str) -> None:
        self.path.write_text(
            json.dumps(build_dashboard_snapshot(dashboard_payload(generated_at))),
            encoding="utf-8",
        )

    def build_service(self):
        class FakeService:
            def __init__(self) -> None:
                self.dashboard_called = 0

            def dashboard(self) -> dict:
                self.dashboard_called += 1
                return dashboard_payload(now_iso())

        return FakeService()

    def test_fresh_snapshot_is_reused_without_a_second_api_sweep(self) -> None:
        self.write_snapshot(iso_minutes_ago(4))
        service = self.build_service()

        resolved = commands.resolve_dashboard(service)

        self.assertEqual(service.dashboard_called, 0)
        self.assertEqual(len(resolved["wallets"]), 1)

    def test_stale_snapshot_triggers_a_rebuild(self) -> None:
        self.write_snapshot(iso_minutes_ago(DASHBOARD_SNAPSHOT_MAX_AGE_SECONDS / 60 + 5))
        service = self.build_service()

        commands.resolve_dashboard(service)

        self.assertEqual(service.dashboard_called, 1)

    def test_missing_snapshot_falls_back_to_building(self) -> None:
        service = self.build_service()

        commands.resolve_dashboard(service)

        self.assertEqual(service.dashboard_called, 1)

    def test_corrupt_snapshot_falls_back_instead_of_raising(self) -> None:
        self.path.write_text("{not json", encoding="utf-8")
        service = self.build_service()

        commands.resolve_dashboard(service)

        self.assertEqual(service.dashboard_called, 1)

    def test_snapshot_without_wallets_is_rejected(self) -> None:
        self.path.write_text(json.dumps({"generatedAt": now_iso()}), encoding="utf-8")
        service = self.build_service()

        commands.resolve_dashboard(service)

        self.assertEqual(service.dashboard_called, 1)

    def test_future_dated_snapshot_is_rejected(self) -> None:
        self.write_snapshot(iso_minutes_ago(-30))
        service = self.build_service()

        commands.resolve_dashboard(service)

        self.assertEqual(service.dashboard_called, 1)

    def test_max_age_is_env_overridable(self) -> None:
        self.write_snapshot(iso_minutes_ago(4))
        service = self.build_service()

        with patch.dict("os.environ", {"DASHBOARD_SNAPSHOT_MAX_AGE_SECONDS": "60"}, clear=False):
            commands.resolve_dashboard(service)

        self.assertEqual(service.dashboard_called, 1)


if __name__ == "__main__":
    unittest.main()
