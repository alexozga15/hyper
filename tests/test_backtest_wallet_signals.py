import importlib.util
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "backtest_wallet_signals.py"
SPEC = importlib.util.spec_from_file_location("backtest_wallet_signals", SCRIPT)
assert SPEC and SPEC.loader
backtest = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(backtest)


MINUTE_MS = 60 * 1000


def fill(address: str, minute: float, *, price: float = 100.0, size: float = 1.0, coin: str = "BTC", side: str = "long") -> dict:
    return {
        "address": address,
        "coin": coin,
        "side": side,
        "price": price,
        "size": size,
        "time": int(minute * MINUTE_MS),
    }


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
        self.assertEqual(events[0]["time"], 4_000)
        self.assertEqual(events[0]["windowStart"], 1_000)
        self.assertEqual(events[0]["wallets"], ["0xa", "0xb", "0xc"])
        self.assertEqual(events[0]["size"], 5.0)

    def test_consensus_spanning_a_clock_boundary_is_detected(self) -> None:
        """09:59 plus 10:01 is consensus; fixed hourly-aligned buckets used to miss it."""
        fills = [
            fill("0xa", 599),  # 09:59
            fill("0xb", 600),  # 10:00
            fill("0xc", 601),  # 10:01
        ]

        events = backtest.build_consensus_events(fills, min_wallets=3, window_minutes=10)

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["walletCount"], 3)
        self.assertEqual(events[0]["time"], 601 * MINUTE_MS)
        self.assertEqual(events[0]["windowStart"], 599 * MINUTE_MS)

    def test_wallets_further_apart_than_the_window_are_not_consensus(self) -> None:
        fills = [fill("0xa", 601), fill("0xb", 610), fill("0xc", 629)]

        self.assertEqual(backtest.build_consensus_events(fills, min_wallets=3, window_minutes=10), [])

    def test_window_edge_is_inclusive(self) -> None:
        exactly_ten_minutes = [fill("0xa", 0), fill("0xb", 5), fill("0xc", 10)]
        one_second_too_wide = [fill("0xa", 0), fill("0xb", 5), fill("0xc", 10 + 1 / 60)]

        self.assertEqual(len(backtest.build_consensus_events(exactly_ten_minutes, min_wallets=3, window_minutes=10)), 1)
        self.assertEqual(backtest.build_consensus_events(one_second_too_wide, min_wallets=3, window_minutes=10), [])

    def test_refractory_period_suppresses_duplicate_events_from_one_cluster(self) -> None:
        """A fourth and fifth wallet joining the same cluster must not re-fire the signal."""
        fills = [fill("0xa", 0), fill("0xb", 1), fill("0xc", 2), fill("0xd", 3), fill("0xe", 4)]

        events = backtest.build_consensus_events(fills, min_wallets=3, window_minutes=10)

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["time"], 2 * MINUTE_MS)
        self.assertEqual(events[0]["walletCount"], 3)

    def test_pair_fires_again_once_the_refractory_period_has_elapsed(self) -> None:
        first_cluster = [fill("0xa", 0), fill("0xb", 1), fill("0xc", 2)]
        # 12 minutes after the first event is still muted; 13 minutes after is eligible.
        muted = first_cluster + [fill("0xa", 12), fill("0xb", 12), fill("0xc", 12)]
        eligible = first_cluster + [fill("0xa", 13), fill("0xb", 13), fill("0xc", 13)]

        self.assertEqual(len(backtest.build_consensus_events(muted, min_wallets=3, window_minutes=10)), 1)
        later = backtest.build_consensus_events(eligible, min_wallets=3, window_minutes=10)
        self.assertEqual([event["time"] for event in later], [2 * MINUTE_MS, 13 * MINUTE_MS])
        # The refractory period is a full window, so no fill is counted into two events.
        self.assertEqual(later[1]["fillCount"], 3)

    def test_refractory_period_is_scoped_per_coin_and_side(self) -> None:
        fills = [
            fill("0xa", 0), fill("0xb", 1), fill("0xc", 2),
            fill("0xa", 0, coin="ETH"), fill("0xb", 1, coin="ETH"), fill("0xc", 2, coin="ETH"),
            fill("0xa", 1, side="short"), fill("0xb", 1, side="short"), fill("0xc", 2, side="short"),
        ]

        events = backtest.build_consensus_events(fills, min_wallets=3, window_minutes=10)

        self.assertEqual(
            sorted((event["coin"], event["side"]) for event in events),
            [("BTC", "long"), ("BTC", "short"), ("ETH", "long")],
        )

    def test_entry_price_is_fill_size_weighted_over_the_rolling_window(self) -> None:
        fills = [
            fill("0xa", 599, price=100, size=1),
            fill("0xb", 600, price=200, size=3),
            fill("0xc", 601, price=50, size=2),
        ]

        event = backtest.build_consensus_events(fills, min_wallets=3, window_minutes=10)[0]

        self.assertEqual(event["entryPrice"], round((100 * 1 + 200 * 3 + 50 * 2) / 6, 10))
        self.assertEqual(event["size"], 6.0)

    def test_events_are_returned_in_chronological_order(self) -> None:
        fills = [
            fill("0xa", 100, coin="ETH"), fill("0xb", 101, coin="ETH"), fill("0xc", 102, coin="ETH"),
            fill("0xa", 0), fill("0xb", 1), fill("0xc", 2),
        ]

        events = backtest.build_consensus_events(fills, min_wallets=3, window_minutes=10)

        self.assertEqual([event["time"] for event in events], sorted(event["time"] for event in events))
        self.assertEqual([event["coin"] for event in events], ["BTC", "ETH"])

    def test_repeat_fills_from_one_wallet_never_reach_consensus(self) -> None:
        fills = [fill("0xa", minute) for minute in range(6)]

        self.assertEqual(backtest.build_consensus_events(fills, min_wallets=3, window_minutes=10), [])

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

    def test_events_without_candles_do_not_consume_walk_forward_slots(self) -> None:
        events = [
            {"time": 1, "outcomes": {"1h": {}}},
            {"time": 2, "outcomes": {}},
        ]

        self.assertEqual([item["time"] for item in backtest.evaluable_events(events)], [1])


def outcome_event(net: float, time: int = 0, horizon: str = "1h") -> dict:
    return {"time": time, "outcomes": {horizon: {"netReturnPct": net, "mfePct": 0.0, "maePct": 0.0}}}


class SignificanceTests(unittest.TestCase):
    def test_bootstrap_ci_of_a_constant_sample_collapses_to_that_constant(self) -> None:
        interval = backtest.bootstrap_mean_ci([2.5] * 20)

        self.assertEqual(interval["lowerPct"], 2.5)
        self.assertEqual(interval["upperPct"], 2.5)
        self.assertEqual(interval["confidence"], 0.95)
        self.assertTrue(backtest.ci_excludes_zero(interval))

    def test_bootstrap_ci_is_reproducible_and_brackets_the_sample_mean(self) -> None:
        values = [1.0, -0.5, 2.0, 0.25, -1.5, 3.0, 0.75, -0.25, 1.25, 0.5]

        first = backtest.bootstrap_mean_ci(values)
        second = backtest.bootstrap_mean_ci(values)

        self.assertEqual(first, second)
        self.assertEqual(first["seed"], backtest.BOOTSTRAP_SEED)
        self.assertEqual(first["resamples"], backtest.BOOTSTRAP_RESAMPLES)
        mean = sum(values) / len(values)
        self.assertLessEqual(first["lowerPct"], mean)
        self.assertGreaterEqual(first["upperPct"], mean)
        self.assertLess(first["lowerPct"], first["upperPct"])

    def test_bootstrap_ci_widens_with_a_noisier_sample(self) -> None:
        tight = backtest.bootstrap_mean_ci([1.0, 1.1, 0.9, 1.0, 1.05, 0.95, 1.0, 1.02])
        noisy = backtest.bootstrap_mean_ci([1.0, 20.0, -18.0, 1.0, 15.0, -12.0, 1.0, 4.0])

        self.assertLess(tight["upperPct"] - tight["lowerPct"], noisy["upperPct"] - noisy["lowerPct"])

    def test_bootstrap_ci_needs_at_least_two_observations(self) -> None:
        self.assertIsNone(backtest.bootstrap_mean_ci([]))
        self.assertIsNone(backtest.bootstrap_mean_ci([1.0]))
        self.assertFalse(backtest.ci_excludes_zero(None))

    def test_ci_excludes_zero_only_when_zero_is_outside_the_interval(self) -> None:
        self.assertTrue(backtest.ci_excludes_zero({"lowerPct": 0.1, "upperPct": 0.4}))
        self.assertTrue(backtest.ci_excludes_zero({"lowerPct": -0.4, "upperPct": -0.1}))
        self.assertFalse(backtest.ci_excludes_zero({"lowerPct": -0.1, "upperPct": 0.4}))
        self.assertFalse(backtest.ci_excludes_zero({"lowerPct": 0.0, "upperPct": 0.4}))

    def test_summary_reports_sample_size_confidence_interval_and_flags(self) -> None:
        events = [outcome_event(1.0) for _ in range(10)]

        horizon = backtest.summarize_events(events, period="validation")["horizons"]["1h"]

        self.assertEqual(horizon["observations"], 10)
        self.assertEqual(horizon["netReturnCi95"]["lowerPct"], 1.0)
        self.assertEqual(horizon["netReturnCi95"]["upperPct"], 1.0)
        self.assertTrue(horizon["ciExcludesZero"])
        self.assertTrue(horizon["belowMinObservations"])
        self.assertEqual(horizon["minObservationsForSelection"], backtest.MIN_OBSERVATIONS_FOR_SELECTION)
        self.assertEqual(horizon["stdevNetReturnPct"], 0.0)

    def test_summary_flags_a_mean_that_is_indistinguishable_from_zero(self) -> None:
        events = [outcome_event(value) for value in (5.0, -5.0) * 20]

        horizon = backtest.summarize_events(events, period="validation")["horizons"]["1h"]

        self.assertEqual(horizon["observations"], 40)
        self.assertFalse(horizon["belowMinObservations"])
        self.assertFalse(horizon["ciExcludesZero"])
        self.assertLess(horizon["netReturnCi95"]["lowerPct"], 0.0)
        self.assertGreater(horizon["netReturnCi95"]["upperPct"], 0.0)

    def test_selection_gate_is_thirty_observations(self) -> None:
        self.assertGreaterEqual(backtest.MIN_OBSERVATIONS_FOR_SELECTION, 30)


class ReportWarningTests(unittest.TestCase):
    @staticmethod
    def _config(name: str, validation: dict, test: dict) -> dict:
        return {"name": name, "summary": {"validation": {"horizons": {"4h": validation}}, "test": {"horizons": {"4h": test}}}}

    def test_multiple_comparison_warning_is_always_present(self) -> None:
        warnings = backtest.build_report_warnings(None, list(backtest.DEFAULT_CONFIGS))

        self.assertTrue(any("comparisons" in text for text in warnings))
        self.assertTrue(any("nothing was selected" in text for text in warnings))

    def test_warns_when_the_selected_config_interval_includes_zero(self) -> None:
        strong = {"observations": 50, "ciExcludesZero": True, "netReturnCi95": {"lowerPct": 0.2, "upperPct": 1.0}}
        weak = {"observations": 50, "ciExcludesZero": False, "netReturnCi95": {"lowerPct": -0.9, "upperPct": 1.4}}

        warnings = backtest.build_report_warnings(self._config("weak", weak, strong), [{}, {}, {}])

        self.assertTrue(any("includes zero" in text for text in warnings))
        self.assertFalse(any("below the minimum" in text for text in warnings))

    def test_warns_when_observations_are_below_the_minimum(self) -> None:
        thin = {"observations": 7, "ciExcludesZero": True, "netReturnCi95": {"lowerPct": 0.2, "upperPct": 1.0}}

        warnings = backtest.build_report_warnings(self._config("thin", thin, thin), [{}, {}, {}])

        below = [text for text in warnings if "below the minimum" in text]
        self.assertEqual(len(below), 2)
        self.assertTrue(any("validation observations" in text for text in below))
        self.assertTrue(any("test observations" in text for text in below))

    def test_no_noise_warnings_for_a_strong_selection(self) -> None:
        strong = {"observations": 120, "ciExcludesZero": True, "netReturnCi95": {"lowerPct": 0.4, "upperPct": 1.6}}

        warnings = backtest.build_report_warnings(self._config("strong", strong, strong), [{}, {}, {}])

        self.assertEqual(len(warnings), 1)
        self.assertIn("comparisons", warnings[0])


if __name__ == "__main__":
    unittest.main()


class ParseConfigsTests(unittest.TestCase):
    def test_parses_wallet_and_window_pairs(self) -> None:
        self.assertEqual(
            backtest.parse_configs("3w/5m, 4w/120m"),
            (
                {"name": "3w_5m", "minWallets": 3, "windowMinutes": 5},
                {"name": "4w_120m", "minWallets": 4, "windowMinutes": 120},
            ),
        )

    def test_empty_spec_keeps_the_built_in_defaults(self) -> None:
        self.assertEqual(backtest.parse_configs("  "), backtest.DEFAULT_CONFIGS)

    def test_rejects_malformed_and_degenerate_specs(self) -> None:
        for spec in ("bogus", "3/5", "1w/5m", "3w/0m"):
            with self.subTest(spec=spec):
                with self.assertRaises(ValueError):
                    backtest.parse_configs(spec)


class RawMarketLabelTests(unittest.TestCase):
    """HIP-3 markets are only identifiable before the venue prefix is stripped.

    Measured over 99 days of tracked-wallet history: 1162 of 2670 large fills
    were HIP-3 markets, and none could be scored, because candleSnapshot
    answers HTTP 500 to a bare "SP500" and SILVER alone resolves to six
    different venues.
    """

    def test_normalize_fill_keeps_the_qualified_name(self) -> None:
        raw = {"dir": "Open Long", "px": "100", "sz": "2", "time": 1, "coin": "xyz:NVDA"}
        normalized = backtest.normalize_fill("0xabc", raw)
        assert normalized is not None
        self.assertEqual(normalized["coin"], "NVDA")
        self.assertEqual(normalized["marketCoin"], "xyz:NVDA")

    def test_crypto_is_unchanged(self) -> None:
        raw = {"dir": "Open Long", "px": "100", "sz": "2", "time": 1, "coin": "BTC"}
        normalized = backtest.normalize_fill("0xabc", raw)
        assert normalized is not None
        self.assertEqual(normalized["coin"], normalized["marketCoin"], "BTC")

    def test_the_same_ticker_on_two_venues_is_two_markets(self) -> None:
        # Pooling them produced one event whose entry price was a VWAP across
        # markets that cannot be traded against one another.
        fills = []
        for index, market in enumerate(("xyz:GOLD", "xyz:GOLD", "xyz:GOLD", "flx:GOLD", "flx:GOLD", "flx:GOLD")):
            item = fill(f"0x{index}", index * 0.1, price=100.0 + index, coin="GOLD")
            item["marketCoin"] = market
            fills.append(item)
        events = backtest.build_consensus_events(fills, min_wallets=3, window_minutes=10)
        self.assertEqual(sorted({event["marketCoin"] for event in events}), ["flx:GOLD", "xyz:GOLD"])
        self.assertEqual({event["coin"] for event in events}, {"GOLD"})

    def test_events_fall_back_to_the_bare_coin(self) -> None:
        # Fills cached before marketCoin existed carry no qualified name.
        fills = [fill(f"0x{index}", index * 0.1) for index in range(3)]
        events = backtest.build_consensus_events(fills, min_wallets=3, window_minutes=10)
        self.assertEqual(events[0]["marketCoin"], "BTC")
