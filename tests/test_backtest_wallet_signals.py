import importlib.util
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "backtest_wallet_signals.py"
SPEC = importlib.util.spec_from_file_location("backtest_wallet_signals", SCRIPT)
assert SPEC and SPEC.loader
backtest = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(backtest)


class WalletSignalBacktestTests(unittest.TestCase):
    def test_open_consensus_requires_distinct_wallets_in_window(self) -> None:
        fills = [
            {"address": "0xa", "coin": "BTC", "side": "long", "price": 100, "size": 1, "time": 1_000},
            {"address": "0xa", "coin": "BTC", "side": "long", "price": 110, "size": 1, "time": 2_000},
            {"address": "0xb", "coin": "BTC", "side": "long", "price": 90, "size": 2, "time": 3_000},
            {"address": "0xc", "coin": "BTC", "side": "long", "price": 100, "size": 1, "time": 4_000},
        ]

        events = backtest.build_consensus_events(fills, min_wallets=3, window_minutes=10)

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["walletCount"], 3)
        self.assertEqual(events[0]["fillCount"], 4)
        self.assertEqual(events[0]["entryPrice"], 98.0)

    def test_short_return_and_cost_are_direction_adjusted(self) -> None:
        event = {"coin": "BTC", "side": "short", "time": 0, "entryPrice": 100}
        candles = [
            {"t": 0, "h": 105, "l": 95, "c": 100},
            {"t": 3_600_000, "h": 101, "l": 89, "c": 90},
        ]

        result = backtest.evaluate_event(event, candles, cost_bps_per_side=10)

        outcome = result["outcomes"]["1h"]
        self.assertEqual(outcome["grossReturnPct"], 10.0)
        self.assertEqual(outcome["netReturnPct"], 9.8)
        self.assertEqual(outcome["mfePct"], 11.0)
        self.assertEqual(outcome["maePct"], -5.0)

    def test_walk_forward_split_is_chronological(self) -> None:
        events = [{"time": time} for time in (5, 1, 4, 2, 3)]

        split = backtest.split_walk_forward(events)

        self.assertEqual([item["time"] for item in split["train"]], [1, 2, 3])
        self.assertEqual([item["time"] for item in split["validation"]], [4])
        self.assertEqual([item["time"] for item in split["test"]], [5])


if __name__ == "__main__":
    unittest.main()
