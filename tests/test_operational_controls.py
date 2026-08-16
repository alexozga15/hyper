from __future__ import annotations

import json
import os
import subprocess
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from scripts.run_health_monitor import (
    detect_health_issues,
    detect_signal_drought,
    dirty_checkout_paths,
)
from scripts.run_health_monitor import main as run_health_monitor_main
from scripts.run_wallet_review import evaluate_wallets
from scripts.run_wallet_review import main as run_wallet_review_main
from server import (
    SIGNAL_CALIBRATION_MIN_SAMPLE,
    HyperliquidClient,
    RequestRateLimiter,
    WalletTrackerService,
)


HOUR_MS = 60 * 60 * 1000
DAY_MS = 24 * HOUR_MS
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

    def test_wallet_review_skips_pnl_reasons_for_a_capped_fill_window(self) -> None:
        """A qualityWindowTruncated wallet's 30d PnL/profit-factor may describe
        only a couple of hours (userFillsByTime caps at WALLET_WINDOW_FILL_CAP
        rows with no pagination), so neither PnL-based reason should fire even
        though the raw numbers look identical to the weak wallet above. The
        wallet's coverage span (2h, well under QUALITY_WINDOW_MIN_COVERAGE_MS)
        is what makes wallet_quality_window_trusted call this genuinely
        untrusted rather than just capped.
        """
        stats: dict[str, int] = {}
        reviews = evaluate_wallets(
            [
                {
                    "address": "0xcapped",
                    "closedTrades30d": 8,
                    "qualityNetPnl30d": -100,
                    "qualityProfitFactor30d": 0.8,
                    "qualityWindowTruncated": True,
                    "qualityWindowCoverageMs": 2 * HOUR_MS,
                    "positions": [],
                }
            ],
            stats,
        )
        self.assertNotIn("0xcapped", reviews)
        self.assertEqual(stats["skippedCappedWindow"], 1)

    def test_wallet_review_judges_well_covered_capped_window_normally(self) -> None:
        """A capped page that still spans most of the 30-day window is a fair

        sample (see wallet_quality_window_trusted / QUALITY_WINDOW_MIN_COVERAGE_MS),
        so its PnL-based reasons must fire exactly as an uncapped wallet's would.
        """
        stats: dict[str, int] = {}
        reviews = evaluate_wallets(
            [
                {
                    "address": "0xcappedwellcovered",
                    "closedTrades30d": 8,
                    "qualityNetPnl30d": -100,
                    "qualityProfitFactor30d": 0.8,
                    "qualityWindowTruncated": True,
                    "qualityWindowCoverageMs": 25 * DAY_MS,
                    "positions": [],
                }
            ],
            stats,
        )
        self.assertIn("negative_30d_pnl", reviews["0xcappedwellcovered"]["reasons"])
        self.assertIn("profit_factor_below_1", reviews["0xcappedwellcovered"]["reasons"])
        self.assertEqual(stats["skippedCappedWindow"], 0)

    def test_wallet_review_distrusts_a_capped_window_with_no_coverage_evidence(self) -> None:
        """A snapshot cached between the two deploys carries the truncation flag

        but no coverage field. That is not a reason to trust it: the flag
        already tells us the page was capped, and the missing field only means
        we cannot tell whether the sample was adequate. Treating it as trusted
        would leave the PnL checks running on exactly the wallets this whole
        mechanism exists to protect, for as long as it takes every wallet to
        refresh.
        """
        stats: dict[str, int] = {}
        reviews = evaluate_wallets(
            [
                {
                    "address": "0xnocoverage",
                    "closedTrades30d": 8,
                    "qualityNetPnl30d": -100,
                    "qualityProfitFactor30d": 0.8,
                    "qualityWindowTruncated": True,
                    "positions": [],
                }
            ],
            stats,
        )
        self.assertNotIn("0xnocoverage", reviews)
        self.assertEqual(stats["skippedCappedWindow"], 1)

    def test_wallet_review_trusts_a_wallet_carrying_no_truncation_flag(self) -> None:
        """The permissive default survives where it belongs: a snapshot from

        before any of these fields existed carries no truncation flag at all
        and must keep being judged exactly as it was.
        """
        reviews = evaluate_wallets(
            [
                {
                    "address": "0xnoflag",
                    "closedTrades30d": 8,
                    "qualityNetPnl30d": -100,
                    "qualityProfitFactor30d": 0.8,
                    "positions": [],
                }
            ]
        )
        self.assertIn("negative_30d_pnl", reviews["0xnoflag"]["reasons"])

    def test_wallet_review_still_flags_inactive_when_capped(self) -> None:
        """The capped-window flag only silences the PnL reasons, not inactivity -
        daysSinceLastFill/holdingOnly30d come from a different endpoint and are
        unaffected by the userFillsByTime cap.
        """
        reviews = evaluate_wallets(
            [
                {
                    "address": "0xcappedidle",
                    "closedTrades30d": 8,
                    "qualityNetPnl30d": -100,
                    "qualityProfitFactor30d": 0.8,
                    "qualityWindowTruncated": True,
                    "qualityWindowCoverageMs": 2 * HOUR_MS,
                    "daysSinceLastFill": 45,
                    "positions": [],
                }
            ]
        )
        self.assertEqual(reviews["0xcappedidle"]["reasons"], ["inactive"])

    def test_wallet_review_summary_reports_the_skipped_count(self) -> None:
        """A suppressed PnL check must leave a trace in the payload and the

        Telegram summary, not just silently vanish - otherwise a capped fill
        window looks identical to a wallet that genuinely passed its checks.
        """
        wallets = [
            {
                "address": "0xcapped",
                "closedTrades30d": 8,
                "qualityNetPnl30d": -100,
                "qualityProfitFactor30d": 0.8,
                "qualityWindowTruncated": True,
                "qualityWindowCoverageMs": 2 * HOUR_MS,
                "positions": [],
            }
        ]
        with tempfile.TemporaryDirectory() as tmp_dir:
            review_file = Path(tmp_dir) / "wallet_review.json"
            fake_service = MagicMock()
            fake_service.dashboard.return_value = {"wallets": wallets}
            with patch(
                "scripts.run_wallet_review.WalletTrackerService", return_value=fake_service
            ), patch("scripts.run_wallet_review.WalletStore"), patch(
                "scripts.run_wallet_review.HyperliquidClient"
            ), patch(
                "scripts.run_wallet_review.WALLET_REVIEW_FILE", review_file
            ), patch.dict(
                os.environ, {"TELEGRAM_BOT_TOKEN": "token", "TELEGRAM_CHAT_ID": "chat"}
            ):
                run_wallet_review_main()

            payload = json.loads(review_file.read_text())
            self.assertEqual(payload["skippedCappedWindowCount"], 1)
            self.assertEqual(payload["reviewCount"], 0)

            fake_service.send_telegram_message.assert_called_once()
            _, _, message = fake_service.send_telegram_message.call_args[0]
            self.assertIn("Skipped (capped fill window): 1", message)

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

    def test_untrusted_quality_window_gets_neutral_conviction_weight(self) -> None:
        """A wallet whose 30d window is capped-and-poorly-covered must neither

        gain nor lose weight from its (unreadable) score - it falls back to
        the same neutral 1.0 base an unranked wallet gets.
        """
        wallet = {
            "address": "0xuntrusted",
            "recentWinRateRank": {"convictionWeightScore": 90, "label": "Strong"},
            "qualityWindowTruncated": True,
            "qualityWindowCoverageMs": 2 * HOUR_MS,
        }
        self.assertEqual(self.service.wallet_conviction_weight(wallet), 1.0)

    def test_well_covered_capped_window_conviction_weight_uses_score(self) -> None:
        """A capped page that still covers most of the 30 days is trusted, so

        its score-derived weight is used exactly as an uncapped wallet's would be.
        """
        wallet = {
            "address": "0xwellcovered",
            "recentWinRateRank": {"convictionWeightScore": 100, "label": "Strong"},
            "qualityWindowTruncated": True,
            "qualityWindowCoverageMs": 25 * DAY_MS,
        }
        self.assertEqual(self.service.wallet_conviction_weight(wallet), 1.5)

    def test_untrusted_quality_window_is_never_quarantined(self) -> None:
        """Mirrors is_wallet_dormant: a wallet whose fill data cannot be read

        must not be judged at all, rather than risk a false quarantine off
        noise from an unreadable window.
        """
        wallet = {
            "address": "0xuntrusted",
            "recentWinRateRank": {
                "sampleSize": 10,
                "pnlReturnPct": -50,
                "winRate": 10,
            },
            "qualityWindowTruncated": True,
            "qualityWindowCoverageMs": 2 * HOUR_MS,
        }
        self.assertFalse(self.service.is_wallet_quarantined(wallet))

    def test_well_covered_capped_window_can_still_be_quarantined(self) -> None:
        wallet = {
            "address": "0xwellcovered",
            "recentWinRateRank": {
                "sampleSize": 10,
                "pnlReturnPct": -50,
                "winRate": 10,
            },
            "qualityWindowTruncated": True,
            "qualityWindowCoverageMs": 25 * DAY_MS,
        }
        self.assertTrue(self.service.is_wallet_quarantined(wallet))

    def test_health_monitor_detects_stale_and_low_cache(self) -> None:
        issues = detect_health_issues(
            {"state": {"lastCheckedAt": "2020-01-01T00:00:00Z"}},
            {"cacheCoverage": 0.5},
            now_ms=2_000_000_000_000,
            disk_free_pct=50,
        )
        self.assertIn("sentiment check stale >25m", issues)
        self.assertIn("wallet quality cache coverage <80%", issues)

    def test_health_monitor_reports_untrusted_quality_window_wallets(self) -> None:
        """Visibility check: how many tracked wallets currently have an

        untrusted quality window must be reported, not go quiet the way the
        original unconditional-truncation defect did.
        """
        issues = detect_health_issues(
            {"state": {"lastCheckedAt": ms_to_iso(NOW_MS)}},
            {"cacheCoverage": 1.0, "untrustedQualityWindowWallets": 3},
            now_ms=NOW_MS,
            disk_free_pct=50,
        )
        self.assertIn("3 wallets with untrusted quality window", issues)

    def test_health_monitor_stays_quiet_with_no_untrusted_wallets(self) -> None:
        issues = detect_health_issues(
            {"state": {"lastCheckedAt": ms_to_iso(NOW_MS)}},
            {"cacheCoverage": 1.0, "untrustedQualityWindowWallets": 0},
            now_ms=NOW_MS,
            disk_free_pct=50,
        )
        self.assertFalse(any("untrusted quality window" in issue for issue in issues))

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
        with patch("ratelimit.time.monotonic", side_effect=[10.0, 10.1]), patch("ratelimit.time.sleep") as sleep:
            limiter.wait()
            limiter.wait()
        self.assertAlmostEqual(sleep.call_args.args[0], 0.4)


if __name__ == "__main__":
    unittest.main()
