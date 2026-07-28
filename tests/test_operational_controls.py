from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from scripts.run_health_monitor import detect_health_issues
from scripts.run_wallet_review import evaluate_wallets
from server import HyperliquidClient, RequestRateLimiter, WalletTrackerService


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
        records = {
            str(index): {
                "coin": "BTC",
                "probabilityScore": 85,
                "outcomes": {"4h": {"returnPct": -1}},
            }
            for index in range(8)
        }
        calibration = self.service.build_signal_calibration(records)
        summary = {"signals": [{"coin": "BTC", "probabilityScore": 85}], "signalCount": 1}
        adjusted = self.service.apply_signal_calibration(summary, calibration)
        self.assertEqual(adjusted["signals"][0]["probabilityScore"], 75.0)
        self.assertEqual(adjusted["signals"][0]["rawProbabilityScore"], 85.0)

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

    def test_rate_limiter_spaces_requests(self) -> None:
        limiter = RequestRateLimiter(2)
        with patch("server.time.monotonic", side_effect=[10.0, 10.1]), patch("server.time.sleep") as sleep:
            limiter.wait()
            limiter.wait()
        self.assertAlmostEqual(sleep.call_args.args[0], 0.4)


if __name__ == "__main__":
    unittest.main()
