from __future__ import annotations

import os
import subprocess
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from scripts.run_health_monitor import (
    detect_health_issues,
    detect_signal_drought,
    dirty_checkout_paths,
)
from scripts.run_health_monitor import main as run_health_monitor_main
from scripts.run_wallet_review import evaluate_wallets
from server import (
    SIGNAL_CALIBRATION_MIN_SAMPLE,
    HyperliquidClient,
    RequestRateLimiter,
    WalletTrackerService,
)


HOUR_MS = 60 * 60 * 1000
NOW_MS = 2_000_000_000_000


def ms_to_iso(value: int) -> str:
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z")


class OperationalControlTests(unittest.TestCase):
    def setUp(self) -> None:
        self.service = WalletTrackerService(object(), HyperliquidClient(RequestRateLimiter(1000)))

    def test_stale_quality_is_downweighted_then_excluded(self) -> None:
        now = datetime.now(timezone.utc)
        wallet = {
            "address": "0x1",
            "dataQuality": {"qualityRefreshedAt": (now - timedelta(hours=3)).isoformat()},
        }
        self.assertEqual(self.service.wallet_conviction_weight(wallet), 0.75)
        wallet["dataQuality"]["qualityRefreshedAt"] = (now - timedelta(hours=7)).isoformat()
        self.assertEqual(self.service.wallet_conviction_weight(wallet), 0.0)
        self.assertFalse(self.service.should_count_wallet_for_conviction(wallet))

    def test_calibration_requires_sample_and_caps_adjustment(self) -> None:
        def build_records(count: int) -> dict[str, dict[str, object]]:
            return {
                str(index): {
                    "coin": "BTC",
                    "probabilityScore": 85,
                    "outcomes": {"4h": {"returnPct": -1}},
                }
                for index in range(count)
            }

        summary = {"signals": [{"coin": "BTC", "probabilityScore": 85}], "signalCount": 1}

        thin = self.service.apply_signal_calibration(
            summary, self.service.build_signal_calibration(build_records(8))
        )
        # 8 samples is below SIGNAL_CALIBRATION_MIN_SAMPLE, so nothing moves.
        self.assertEqual(thin["signals"][0]["probabilityScore"], 85.0)
        self.assertFalse(thin["signals"][0]["calibrationApplied"])

        calibration = self.service.build_signal_calibration(
            build_records(SIGNAL_CALIBRATION_MIN_SAMPLE)
        )
        adjusted = self.service.apply_signal_calibration(summary, calibration)
        self.assertEqual(adjusted["signals"][0]["probabilityScore"], 75.0)
        self.assertEqual(adjusted["signals"][0]["rawProbabilityScore"], 85.0)
        self.assertTrue(adjusted["signals"][0]["calibrationApplied"])

    def test_wallet_review_reduces_weak_wallet_weight(self) -> None:
        reviews = evaluate_wallets(
            [
                {
                    "address": "0xweak",
                    "closedTrades30d": 8,
                    "qualityNetPnl30d": -100,
                    "qualityProfitFactor30d": 0.8,
                    "positions": [],
                }
            ]
        )
        self.assertEqual(reviews["0xweak"]["weight"], 0.5)
        self.assertIn("negative_30d_pnl", reviews["0xweak"]["reasons"])

    def test_wallet_review_uses_matching_quality_event_sample(self) -> None:
        reviews = evaluate_wallets(
            [
                {
                    "address": "0xthin",
                    "closedTrades30d": 20,
                    "qualityClosedEvents30d": 3,
                    "qualityNetPnl30d": -100,
                    "qualityProfitFactor30d": 0.8,
                    "positions": [],
                }
            ]
        )
        self.assertNotIn("0xthin", reviews)

    def test_wallet_review_weight_reduces_non_top_conviction(self) -> None:
        wallet = {
            "address": "0xweak",
            "recentWinRateRank": {"convictionWeightScore": 40, "label": "Cold"},
            "reviewWeightMultiplier": 0.5,
        }
        self.assertEqual(self.service.wallet_conviction_weight(wallet, {"0xother"}), 0.25)

    def test_manual_zero_review_weight_excludes_wallet(self) -> None:
        wallet = {
            "address": "0xexcluded",
            "recentWinRateRank": {"convictionWeightScore": 90, "label": "Strong"},
            "reviewWeightMultiplier": 0,
        }
        self.assertEqual(self.service.wallet_conviction_weight(wallet), 0.0)

    def test_health_monitor_detects_stale_and_low_cache(self) -> None:
        issues = detect_health_issues(
            {"state": {"lastCheckedAt": "2020-01-01T00:00:00Z"}},
            {"cacheCoverage": 0.5},
            now_ms=2_000_000_000_000,
            disk_free_pct=50,
        )
        self.assertIn("sentiment check stale >10m", issues)
        self.assertIn("wallet quality cache coverage <80%", issues)

    def test_signal_drought_stays_quiet_before_the_first_check(self) -> None:
        self.assertEqual(detect_signal_drought({}, now_ms=NOW_MS), [])
        self.assertEqual(
            detect_signal_drought({"shadowSignalOutcomes": {}}, now_ms=NOW_MS), []
        )

    def test_live_shadow_flow_only_flags_the_published_drought(self) -> None:
        issues = detect_signal_drought(
            {
                "lastCheckedAt": ms_to_iso(NOW_MS),
                "shadowSignalOutcomes": {"BTC:long:1": {"startedAt": NOW_MS - HOUR_MS}},
            },
            now_ms=NOW_MS,
        )
        self.assertEqual(issues, ["no published or candidate signal in >7d"])

    def test_silent_pipeline_flags_both_horizons(self) -> None:
        issues = detect_signal_drought(
            {
                "lastCheckedAt": ms_to_iso(NOW_MS),
                "shadowSignalOutcomes": {"BTC:long:1": {"startedAt": NOW_MS - 40 * HOUR_MS}},
            },
            now_ms=NOW_MS,
        )
        self.assertEqual(
            issues,
            ["no signal of any tier in >24h", "no published or candidate signal in >7d"],
        )

    def test_recent_candidate_signal_clears_the_drought(self) -> None:
        issues = detect_signal_drought(
            {
                "lastCheckedAt": ms_to_iso(NOW_MS),
                "shadowSignalOutcomes": {"BTC:long:1": {"startedAt": NOW_MS - 40 * HOUR_MS}},
                "candidateSignalOutcomes": {"ETH:short:2": {"startedAt": NOW_MS - HOUR_MS}},
            },
            now_ms=NOW_MS,
        )
        self.assertEqual(issues, [])

    def test_health_check_reports_the_signal_drought(self) -> None:
        issues = detect_health_issues(
            {"state": {"lastCheckedAt": ms_to_iso(NOW_MS)}},
            {"cacheCoverage": 1.0},
            now_ms=NOW_MS,
            disk_free_pct=50,
        )
        self.assertIn("no signal of any tier in >24h", issues)

    def test_dirty_checkout_is_reported_and_clean_one_is_not(self) -> None:
        modified = subprocess.CompletedProcess([], 0, stdout=" M data/alerts.json\n", stderr="")
        with patch("scripts.run_health_monitor.subprocess.run", return_value=modified):
            self.assertEqual(dirty_checkout_paths(Path("/repo")), ["data/alerts.json"])
        clean = subprocess.CompletedProcess([], 0, stdout="", stderr="")
        with patch("scripts.run_health_monitor.subprocess.run", return_value=clean):
            self.assertEqual(dirty_checkout_paths(Path("/repo")), [])

    def test_missing_git_does_not_become_a_health_issue(self) -> None:
        with patch("scripts.run_health_monitor.subprocess.run", side_effect=FileNotFoundError):
            self.assertEqual(dirty_checkout_paths(Path("/repo")), [])
        failed = subprocess.CompletedProcess([], 128, stdout="", stderr="not a git repository")
        with patch("scripts.run_health_monitor.subprocess.run", return_value=failed):
            self.assertEqual(dirty_checkout_paths(Path("/repo")), [])

    def test_detected_issues_do_not_fail_the_monitor_process(self) -> None:
        with (
            patch("scripts.run_health_monitor.load_json_file", return_value={}),
            patch("scripts.run_health_monitor.save_json_file") as save,
            patch("scripts.run_health_monitor.dirty_checkout_paths", return_value=[]),
            patch(
                "scripts.run_health_monitor.shutil.disk_usage",
                return_value=SimpleNamespace(total=100, used=50, free=50),
            ),
            patch.dict(os.environ, {"TELEGRAM_BOT_TOKEN": "", "TELEGRAM_CHAT_ID": ""}, clear=False),
        ):
            exit_code = run_health_monitor_main()
        self.assertEqual(exit_code, 0)
        self.assertFalse(save.call_args.args[1]["healthy"])

    def test_rate_limiter_spaces_requests(self) -> None:
        limiter = RequestRateLimiter(2)
        with patch("server.time.monotonic", side_effect=[10.0, 10.1]), patch("server.time.sleep") as sleep:
            limiter.wait()
            limiter.wait()
        self.assertAlmostEqual(sleep.call_args.args[0], 0.4)


if __name__ == "__main__":
    unittest.main()
