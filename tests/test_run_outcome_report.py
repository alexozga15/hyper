import importlib.util
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "run_outcome_report.py"
SPEC = importlib.util.spec_from_file_location("run_outcome_report", SCRIPT)
assert SPEC and SPEC.loader
report = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(report)


NOW_MS = 1_787_500_000_000
HOUR_MS = 60 * 60 * 1000


def record(coin: str, side: str, *, age_hours: float, returns: dict[str, float | None]) -> dict:
    return {
        "coin": coin,
        "side": side,
        "startedAt": NOW_MS - int(age_hours * HOUR_MS),
        "outcomes": {
            horizon: ({} if value is None else {"netReturnPct": value})
            for horizon, value in returns.items()
        },
    }


class HorizonReturnTests(unittest.TestCase):
    def test_missing_or_unmeasured_horizons_return_none(self) -> None:
        self.assertIsNone(report.horizon_return({}, "1h"))
        self.assertIsNone(report.horizon_return({"outcomes": "junk"}, "1h"))
        self.assertIsNone(report.horizon_return({"outcomes": {"1h": {}}}, "1h"))

    def test_falls_back_to_gross_when_net_is_absent(self) -> None:
        # Older records predate the net figure; dropping them would quietly
        # shrink the sample the report is built from.
        self.assertEqual(report.horizon_return({"outcomes": {"1h": {"grossReturnPct": 1.5}}}, "1h"), 1.5)
        self.assertEqual(
            report.horizon_return({"outcomes": {"1h": {"grossReturnPct": 1.5, "netReturnPct": 1.3}}}, "1h"),
            1.3,
        )

    def test_zero_return_is_a_measurement_not_a_gap(self) -> None:
        self.assertEqual(report.horizon_return({"outcomes": {"1h": {"netReturnPct": 0.0}}}, "1h"), 0.0)


class WindowTests(unittest.TestCase):
    def test_only_records_inside_the_window_are_counted(self) -> None:
        source = {
            "fresh": record("BTC", "long", age_hours=1, returns={"1h": 1.0}),
            "stale": record("ETH", "long", age_hours=24 * 9, returns={"1h": 1.0}),
        }
        kept = report.records_in_window(source, now_ms=NOW_MS, window_days=7)
        self.assertEqual([item["coin"] for item in kept], ["BTC"])

    def test_non_dict_sources_and_entries_are_ignored(self) -> None:
        self.assertEqual(report.records_in_window(None, now_ms=NOW_MS, window_days=7), [])
        self.assertEqual(report.records_in_window({"a": "junk"}, now_ms=NOW_MS, window_days=7), [])


class SummaryTests(unittest.TestCase):
    def test_summary_reports_mean_median_and_hit_rate(self) -> None:
        records = [
            record("BTC", "long", age_hours=1, returns={"1h": 2.0}),
            record("BTC", "long", age_hours=2, returns={"1h": -1.0}),
            record("BTC", "long", age_hours=3, returns={"1h": -3.0}),
        ]
        summary = report.summarize(records, "1h")
        assert summary is not None
        self.assertEqual(summary["observations"], 3)
        self.assertEqual(summary["meanPct"], round((2.0 - 1.0 - 3.0) / 3, 3))
        self.assertEqual(summary["medianPct"], -1.0)
        self.assertEqual(summary["hitRatePct"], round(100 / 3, 1))

    def test_summary_is_none_when_nothing_was_measured(self) -> None:
        self.assertIsNone(report.summarize([record("BTC", "long", age_hours=1, returns={"1h": None})], "1h"))

    def test_sides_are_reported_separately(self) -> None:
        records = [
            record("BTC", "long", age_hours=1, returns={"1h": 2.0}),
            record("BTC", "short", age_hours=1, returns={"1h": -2.0}),
        ]
        sides = report.summarize_by_side(records, "1h")
        self.assertEqual(sides["long"]["meanPct"], 2.0)
        self.assertEqual(sides["short"]["meanPct"], -2.0)

    def test_coins_below_the_minimum_are_left_out(self) -> None:
        records = [record("BTC", "long", age_hours=1, returns={"1h": 1.0}) for _ in range(3)]
        records += [record("ETH", "long", age_hours=1, returns={"1h": 1.0})]
        rows = report.summarize_by_coin(records, "1h", minimum=3)
        self.assertEqual([row["coin"] for row in rows], ["BTC"])


class MessageTests(unittest.TestCase):
    def test_published_signal_is_shown_with_its_realised_return(self) -> None:
        state = {
            "shadowSignalOutcomes": {
                "a": record("BTC", "short", age_hours=2, returns={"1h": -3.419, "4h": -3.126})
            },
            "signalOutcomes": {
                "s": record("BTC", "short", age_hours=2, returns={"1h": -3.419, "4h": -3.126})
            },
        }
        payload = report.build_payload(state, now_ms=NOW_MS)
        message = report.build_message(payload)

        self.assertEqual(payload["publishedCount"], 1)
        self.assertIn("Published signals", message)
        self.assertIn("BTC SHORT", message)
        self.assertIn("1h -3.42%", message)

    def test_message_states_the_gap_instead_of_hiding_it(self) -> None:
        state = {"shadowSignalOutcomes": {"a": record("BTC", "long", age_hours=2, returns={"1h": 1.0})}}
        message = report.build_message(report.build_payload(state, now_ms=NOW_MS))
        self.assertIn("24h: no measured outcomes yet", message)

    def test_both_ends_are_listed_only_when_the_slices_cannot_overlap(self) -> None:
        def coin_records(coin: str, value: float) -> list[dict]:
            return [record(coin, "long", age_hours=1, returns={"1h": value}) for _ in range(10)]

        few = {f"k{index}": item for index, item in enumerate(coin_records("BTC", 1.0) + coin_records("ETH", -1.0))}
        message = report.build_message(report.build_payload({"shadowSignalOutcomes": few}, now_ms=NOW_MS))
        self.assertEqual(message.count("- BTC:"), 1)
        self.assertNotIn("...", message)

        many_records: list[dict] = []
        for index in range(7):
            many_records.extend(coin_records(f"C{index}", float(index)))
        many = {f"k{index}": item for index, item in enumerate(many_records)}
        message = report.build_message(report.build_payload({"shadowSignalOutcomes": many}, now_ms=NOW_MS))
        self.assertIn("...", message)
        self.assertEqual(message.count("- C6:"), 1)


if __name__ == "__main__":
    unittest.main()
