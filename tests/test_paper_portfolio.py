import unittest

from paper_portfolio import (
    observed_drawdown, paired_portfolio_comparison, portfolio_snapshot,
    scenario_portfolio_snapshot,
)


class PaperPortfolioTests(unittest.TestCase):
    def test_closed_winner_does_not_hide_unpriced_open_loser(self):
        records = {
            "winner": {
                "experimentArm": "ranked_consensus", "status": "closed",
                "executionResult": {"complete": True, "netUsd": 100},
            },
            "loser": {
                "experimentArm": "ranked_consensus", "status": "open",
                "executionStatus": "entry_quoted",
            },
        }
        unknown = portfolio_snapshot(
            records, arm="ranked_consensus", observed_at_ms=1000,
            open_trade_values_usd={},
        )
        self.assertFalse(unknown["complete"])
        self.assertIsNone(unknown["equityUsd"])
        priced = portfolio_snapshot(
            records, arm="ranked_consensus", observed_at_ms=2000,
            open_trade_values_usd={"loser": -500},
        )
        self.assertTrue(priced["complete"])
        self.assertEqual(priced["equityUsd"], 9600)

    def test_confirmed_drawdown_survives_later_history_gap(self):
        rows = [
            {"observedAtMs": 1000, "equityUsd": 10_000, "complete": True},
            {"observedAtMs": 2000, "equityUsd": 4_000, "complete": True},
            {"observedAtMs": 3000, "equityUsd": None, "complete": False},
            {"observedAtMs": 4000, "equityUsd": 9_000, "complete": True},
        ]
        result = observed_drawdown(rows)
        self.assertEqual(result["observedMaxDrawdownPct"], 60)
        self.assertFalse(result["complete"])

    def test_paired_comparison_uses_daily_portfolio_equity_not_winning_trades(self):
        day_ms = 86_400_000
        start = 1_700_000_000_000
        ranked = [
            {"observedAtMs": start + day * day_ms,
             "equityUsd": 10_000 + day * 100, "complete": True}
            for day in range(21)
        ]
        control = [
            {"observedAtMs": start + day * day_ms,
             "equityUsd": 10_000, "complete": True}
            for day in range(21)
        ]
        result = paired_portfolio_comparison(ranked, control)
        self.assertTrue(result["complete"])
        self.assertEqual(result["rankedMinusControlNetUsd"], 2_000)
        self.assertEqual(result["interval95PctOn10k"], [20.0, 20.0])
        control[5]["complete"] = False
        self.assertEqual(
            paired_portfolio_comparison(ranked, control)["reason"],
            "incomplete_portfolio_equity",
        )

    def test_paired_comparison_rejects_unmatched_snapshot_times(self):
        day_ms = 86_400_000
        start = 1_700_000_000_000
        ranked = [
            {"observedAtMs": start + day * day_ms,
             "equityUsd": 10_000 + day * 100, "complete": True}
            for day in range(3)
        ]
        control = [dict(row) for row in ranked]
        control[-1]["observedAtMs"] += 60_000
        self.assertEqual(
            paired_portfolio_comparison(ranked, control)["reason"],
            "unmatched_snapshot_times",
        )

    def test_stress_portfolio_keeps_open_loss_and_delayed_unpriced_gap(self):
        hour = 3_600_000
        records = {"one": {
            "experimentArm": "ranked_consensus", "status": "open",
            "executionStatus": "entry_quoted", "startedAt": hour,
        }}
        stressed = scenario_portfolio_snapshot(
            records, arm="ranked_consensus", scenario="double_cost",
            observed_at_ms=hour + 1_000,
            open_trade_values_usd={"one": -600},
        )
        self.assertEqual(stressed["equityUsd"], 9_400)
        before_delay = scenario_portfolio_snapshot(
            records, arm="ranked_consensus", scenario="delayed_entry",
            observed_at_ms=hour + 1_000, open_trade_values_usd={},
        )
        self.assertEqual(before_delay["equityUsd"], 10_000)
        self.assertEqual(before_delay["notEnteredCount"], 1)
        after_delay = scenario_portfolio_snapshot(
            records, arm="ranked_consensus", scenario="delayed_entry",
            observed_at_ms=2 * hour, open_trade_values_usd={},
        )
        self.assertFalse(after_delay["complete"])
        self.assertIsNone(after_delay["equityUsd"])


if __name__ == "__main__":
    unittest.main()
