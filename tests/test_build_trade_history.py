import importlib.util
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "build_trade_history.py"
SPEC = importlib.util.spec_from_file_location("build_trade_history", SCRIPT)
assert SPEC and SPEC.loader
trades = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(trades)


MINUTE_MS = 60 * 1000


def fill(*, start: float, size: float, buy: bool, price: float, at: int, pnl: float = 0.0, fee: float = 0.0, tid: int = 0, coin: str = "BTC") -> dict:
    return {
        "coin": coin, "startPosition": str(start), "sz": str(abs(size)),
        "side": "B" if buy else "A", "px": str(price), "time": at,
        "closedPnl": str(pnl), "fee": str(fee), "tid": tid,
    }


class OrderFillsTests(unittest.TestCase):
    """Same-millisecond fills are not ordered by tid, so tid order breaks the chain."""

    def test_same_timestamp_fills_are_chained_by_position(self) -> None:
        # True order: 0 -> +3 -> +5 -> +1. tid order is deliberately wrong.
        a = fill(start=0, size=3, buy=True, price=100, at=1000, tid=30)
        b = fill(start=3, size=2, buy=True, price=101, at=1000, tid=10)
        c = fill(start=5, size=4, buy=False, price=102, at=1000, tid=20)
        ordered, unresolved = trades.order_fills([b, c, a])
        self.assertEqual(unresolved, 0)
        self.assertEqual([item["tid"] for item in ordered], [30, 10, 20])

    def test_a_broken_chain_is_counted_not_hidden(self) -> None:
        a = fill(start=0, size=3, buy=True, price=100, at=1000, tid=1)
        orphan = fill(start=99, size=1, buy=True, price=100, at=1000, tid=2)
        _ordered, unresolved = trades.order_fills([a, orphan])
        self.assertEqual(unresolved, 1)

    def test_ordering_across_timestamps_is_chronological(self) -> None:
        a = fill(start=0, size=1, buy=True, price=100, at=1000, tid=9)
        b = fill(start=1, size=1, buy=False, price=110, at=2000, tid=1)
        ordered, _ = trades.order_fills([b, a])
        self.assertEqual([item["time"] for item in ordered], [1000, 2000])


class ReconstructTests(unittest.TestCase):
    def test_a_round_trip_prices_entry_and_exit_by_volume(self) -> None:
        rows = [
            fill(start=0, size=1, buy=True, price=100, at=0),
            fill(start=1, size=3, buy=True, price=200, at=MINUTE_MS),
            fill(start=4, size=4, buy=False, price=300, at=5 * MINUTE_MS, pnl=700.0, fee=10.0),
        ]
        result, checks = trades.reconstruct(rows)
        self.assertEqual(checks["positionMismatches"], 0)
        self.assertEqual(len(result), 1)
        trade = result[0]
        self.assertEqual(trade["side"], "long")
        # (100*1 + 200*3) / 4
        self.assertAlmostEqual(trade["entryPrice"], 175.0)
        self.assertAlmostEqual(trade["exitPrice"], 300.0)
        self.assertAlmostEqual(trade["netPnl"], 690.0)
        self.assertEqual(trade["durationMs"], 5 * MINUTE_MS)

    def test_result_comes_from_the_venue_not_from_recomputed_prices(self) -> None:
        # closedPnl already nets funding and partial closes; a price-derived
        # figure would quietly disagree with the wallet's own accounting.
        rows = [
            fill(start=0, size=1, buy=True, price=100, at=0),
            fill(start=1, size=1, buy=False, price=100, at=MINUTE_MS, pnl=-42.0, fee=1.0),
        ]
        result, _ = trades.reconstruct(rows)
        self.assertAlmostEqual(result[0]["netPnl"], -43.0)

    def test_a_flip_closes_one_trade_and_opens_the_other(self) -> None:
        rows = [
            fill(start=0, size=2, buy=True, price=100, at=0),
            fill(start=2, size=5, buy=False, price=110, at=MINUTE_MS, pnl=20.0),
            fill(start=-3, size=3, buy=True, price=105, at=2 * MINUTE_MS, pnl=15.0),
        ]
        result, checks = trades.reconstruct(rows)
        self.assertEqual(checks["positionMismatches"], 0)
        self.assertEqual([item["side"] for item in result], ["short", "long"])
        self.assertEqual(len(result), 2)

    def test_a_position_open_at_the_end_is_reported_not_listed(self) -> None:
        rows = [fill(start=0, size=2, buy=True, price=100, at=0)]
        result, checks = trades.reconstruct(rows)
        self.assertEqual(result, [])
        self.assertEqual(checks["openAtEnd"], 1)

    def test_history_starting_mid_position_is_skipped_rather_than_mispriced(self) -> None:
        # Without the opening fills there is no entry price; attributing the
        # exit to an invented entry would be fiction.
        rows = [fill(start=10, size=10, buy=False, price=100, at=0, pnl=5.0)]
        result, _ = trades.reconstruct(rows)
        self.assertEqual(result, [])

    def test_coins_are_kept_apart(self) -> None:
        rows = [
            fill(start=0, size=1, buy=True, price=100, at=0, coin="BTC"),
            fill(start=0, size=1, buy=True, price=50, at=0, coin="ETH"),
            fill(start=1, size=1, buy=False, price=110, at=MINUTE_MS, coin="BTC", pnl=10.0),
            fill(start=1, size=1, buy=False, price=40, at=MINUTE_MS, coin="ETH", pnl=-10.0),
        ]
        result, checks = trades.reconstruct(rows)
        self.assertEqual(checks["positionMismatches"], 0)
        self.assertEqual(sorted(item["coin"] for item in result), ["BTC", "ETH"])


class MessageTests(unittest.TestCase):
    def test_empty_window_says_so(self) -> None:
        message = trades.build_message("0x" + "a" * 40, [], {}, days=7)
        self.assertIn("No round trip completed", message)

    def test_a_broken_chain_is_surfaced_to_the_reader(self) -> None:
        rows = [
            fill(start=0, size=1, buy=True, price=100, at=0),
            fill(start=1, size=1, buy=False, price=110, at=MINUTE_MS, pnl=10.0),
        ]
        result, _ = trades.reconstruct(rows)
        message = trades.build_message("0x" + "a" * 40, result, {"unchainable": 3}, days=7)
        self.assertIn("could not be chained", message)

    def test_summary_line_reports_count_win_rate_and_net(self) -> None:
        rows = [
            fill(start=0, size=1, buy=True, price=100, at=0),
            fill(start=1, size=1, buy=False, price=110, at=MINUTE_MS, pnl=1000.0),
        ]
        result, checks = trades.reconstruct(rows)
        message = trades.build_message("0x" + "a" * 40, result, checks, days=7)
        self.assertIn("1 closed · win 100% · net $1.00K", message)
        self.assertIn("BTC LONG", message)


if __name__ == "__main__":
    unittest.main()
