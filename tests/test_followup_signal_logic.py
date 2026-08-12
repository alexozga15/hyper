import importlib.util
import unittest
from pathlib import Path

from server import HyperliquidClient, RequestRateLimiter, WalletTrackerService


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("backtest_wallet_signals", ROOT / "scripts" / "backtest_wallet_signals.py")
assert SPEC and SPEC.loader
backtest = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(backtest)


class FollowupSignalLogicTests(unittest.TestCase):
    def test_degraded_only_bucket_stays_visible_to_calibration(self) -> None:
        service = WalletTrackerService(object(), HyperliquidClient(RequestRateLimiter(1000)))
        records = {
            "late": {
                "coin": "BTC",
                "probabilityScore": 85,
                "outcomes": {"4h": {"netReturnPct": 2.0, "degraded": True}},
            }
        }

        calibration = service.build_signal_calibration(records)

        stats = calibration["groups"]["crypto"]["80"]
        self.assertEqual(stats["sample"], 0)
        self.assertEqual(stats["degradedSample"], 1)

    def test_backtest_warnings_expose_uncertain_selection(self) -> None:
        thin = {"observations": 7, "ciExcludesZero": False, "netReturnCi95": {"lowerPct": -0.2, "upperPct": 0.4}}
        selected = {"name": "thin", "summary": {"validation": {"horizons": {"4h": thin}}, "test": {"horizons": {"4h": thin}}}}

        warnings = backtest.build_report_warnings(selected, list(backtest.DEFAULT_CONFIGS))

        self.assertTrue(any("comparisons" in warning for warning in warnings))
        self.assertTrue(any("includes zero" in warning for warning in warnings))
        self.assertEqual(sum("below the minimum" in warning for warning in warnings), 2)


if __name__ == "__main__":
    unittest.main()
