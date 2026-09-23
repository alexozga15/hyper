import unittest

from paper_execution import (
    modeled_funding_cashflow, paper_trade_result, quote_book,
    stressed_trade_result,
)


class PaperExecutionTests(unittest.TestCase):
    def book(self):
        return {
            "coin": "BTC", "time": 2_000,
            "levels": [
                [{"px": "99", "sz": "20"}, {"px": "98", "sz": "20"}],
                [{"px": "101", "sz": "5"}, {"px": "102", "sz": "20"}],
            ],
        }

    def test_long_entry_walks_asks_and_exit_walks_bids(self):
        entry = quote_book(
            self.book(), action="buy", received_at_ms=2_100,
            detected_at_ms=2_000, notional_usd=1_010, size_decimals=2,
        )
        self.assertTrue(entry["ok"])
        self.assertLessEqual(entry["baseSize"], 10.0)
        self.assertLessEqual(entry["fillValueUsd"] + entry["feeUsd"], 1_010)
        self.assertGreater(entry["bookVwap"], 101.0)
        self.assertGreater(entry["fillPrice"], entry["bookVwap"])
        exit_quote = quote_book(
            self.book(), action="sell", received_at_ms=2_200,
            detected_at_ms=2_150, base_size=entry["baseSize"], size_decimals=2,
        )
        self.assertTrue(exit_quote["ok"])
        self.assertLess(exit_quote["fillPrice"], 99.0)
        result = paper_trade_result(
            entry, exit_quote, side="long", funding_cashflow_usd=-2.0,
            initial_notional_usd=1_010,
        )
        self.assertTrue(result["complete"])
        self.assertAlmostEqual(
            result["netUsd"], result["grossUsd"] - result["feesUsd"] - 2.0
        )

    def test_short_entry_and_cover_use_opposite_book_sides(self):
        entry = quote_book(
            self.book(), action="sell", received_at_ms=2_100,
            detected_at_ms=2_000, notional_usd=990, size_decimals=2,
        )
        exit_quote = quote_book(
            self.book(), action="buy", received_at_ms=2_200,
            detected_at_ms=2_150, base_size=entry["baseSize"], size_decimals=2,
        )
        result = paper_trade_result(
            entry, exit_quote, side="short", funding_cashflow_usd=3.0,
            initial_notional_usd=990,
        )
        self.assertTrue(result["complete"])
        self.assertLess(result["grossUsd"], 0)
        self.assertEqual(result["fundingUsd"], 3.0)

    def test_double_cost_stress_is_worse_on_the_same_books(self):
        entry = quote_book(
            self.book(), action="buy", received_at_ms=2_100,
            detected_at_ms=2_000, notional_usd=1_000, size_decimals=2,
        )
        exit_quote = quote_book(
            self.book(), action="sell", received_at_ms=2_200,
            detected_at_ms=2_150, base_size=entry["baseSize"], size_decimals=2,
        )
        normal = paper_trade_result(
            entry, exit_quote, side="long", funding_cashflow_usd=0.0,
            initial_notional_usd=1_000,
        )
        stressed = stressed_trade_result(
            entry, exit_quote, side="long", funding_cashflow_usd=0.0,
            initial_notional_usd=1_000,
        )
        self.assertTrue(stressed["complete"])
        self.assertLess(stressed["netUsd"], normal["netUsd"])

    def test_stale_or_thin_book_is_skipped_not_filled_at_mark(self):
        stale = quote_book(
            self.book(), action="buy", received_at_ms=20_000,
            detected_at_ms=19_000, notional_usd=1_000, size_decimals=2,
        )
        self.assertEqual(stale["reason"], "stale_book")
        thin = quote_book(
            self.book(), action="buy", received_at_ms=2_100,
            detected_at_ms=2_000, notional_usd=1_000_000, size_decimals=2,
        )
        self.assertEqual(thin["reason"], "insufficient_depth")

    def test_unknown_funding_prevents_a_claimed_net_result(self):
        entry = quote_book(
            self.book(), action="buy", received_at_ms=2_100,
            detected_at_ms=2_000, notional_usd=1_000, size_decimals=2,
        )
        exit_quote = quote_book(
            self.book(), action="sell", received_at_ms=2_200,
            detected_at_ms=2_150, base_size=entry["baseSize"], size_decimals=2,
        )
        result = paper_trade_result(
            entry, exit_quote, side="long", funding_cashflow_usd=None,
            initial_notional_usd=1_000,
        )
        self.assertFalse(result["complete"])
        self.assertEqual(result["reason"], "funding_unverified")
        self.assertNotIn("netReturnPct", result)

    def test_order_size_is_rounded_down_to_market_lot(self):
        entry = quote_book(
            self.book(), action="buy", received_at_ms=2_100,
            detected_at_ms=2_000, notional_usd=1_000, size_decimals=2,
        )
        self.assertLess(entry["baseSize"], 9.90)
        self.assertEqual(round(entry["baseSize"] * 100), entry["baseSize"] * 100)
        self.assertLessEqual(entry["fillValueUsd"] + entry["feeUsd"], 1_000)
        invalid_exit = quote_book(
            self.book(), action="sell", received_at_ms=2_200,
            detected_at_ms=2_150, base_size=9.901, size_decimals=2,
        )
        self.assertEqual(invalid_exit["reason"], "invalid_lot_size")

    def test_hourly_funding_uses_adverse_oracle_cushion(self):
        hour = 3_600_000
        rates = [
            {"time": hour + 75, "fundingRate": "0.001"},
            {"time": 2 * hour + 75, "fundingRate": "0.001"},
        ]
        samples = [
            {"observedAtMs": hour + 1000, "oraclePrice": 100},
            {"observedAtMs": 2 * hour + 1000, "oraclePrice": 100},
        ]
        common = dict(
            base_size=10, entry_at_ms=hour // 2,
            exit_at_ms=2 * hour + hour // 2, history_complete=True,
        )
        long = modeled_funding_cashflow(rates, samples, side="long", **common)
        short = modeled_funding_cashflow(rates, samples, side="short", **common)
        self.assertTrue(long["complete"])
        self.assertEqual(long["eventCount"], 2)
        self.assertAlmostEqual(long["cashflowUsd"], -2.1)
        self.assertAlmostEqual(short["cashflowUsd"], 1.9)

    def test_missing_rate_or_oracle_keeps_net_unverified(self):
        hour = 3_600_000
        common = dict(
            side="long", base_size=10,
            entry_at_ms=hour // 2,
            exit_at_ms=2 * hour + hour // 2,
            history_complete=True,
        )
        rates = [{"time": hour + 75, "fundingRate": "0.001"}]
        samples = [{"observedAtMs": hour, "oraclePrice": 100}]
        self.assertEqual(
            modeled_funding_cashflow(rates, samples, **common)["reason"],
            "funding_event_missing_or_duplicate",
        )
        self.assertEqual(
            modeled_funding_cashflow(
                rates + [{"time": 2 * hour + 75, "fundingRate": "0.001"}],
                samples, **common,
            )["reason"],
            "oracle_sample_missing",
        )

    def test_exit_exactly_at_funding_hour_is_ambiguous(self):
        hour = 3_600_000
        result = modeled_funding_cashflow(
            [], [], side="long", base_size=10,
            entry_at_ms=hour // 2, exit_at_ms=hour,
            history_complete=True,
        )
        self.assertFalse(result["complete"])
        self.assertEqual(result["reason"], "funding_boundary_ambiguous")


if __name__ == "__main__":
    unittest.main()
