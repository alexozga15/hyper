import unittest

import scripts.run_wallet_review as review


class ReviewLineTests(unittest.TestCase):
    ADDRESS = "0x63d417a577b50c96f4f09148d4e4d70950db0522"

    def test_the_line_prints_the_whole_address(self) -> None:
        # These lines are read to be pasted into Hyperdash and block
        # explorers, so an abbreviation is not a shorter form of the answer -
        # it is not an answer.
        line = review.format_review_line(
            self.ADDRESS, {"reasons": ["negative_30d_pnl"], "qualityWinRatePct": 63.9}
        )
        self.assertIn(self.ADDRESS, line)
        self.assertNotIn("...", line)

    def test_the_line_states_what_the_signal_weights_the_wallet_on(self) -> None:
        line = review.format_review_line(
            self.ADDRESS,
            {"reasons": ["negative_30d_pnl", "profit_factor_below_1"], "qualityWinRatePct": 63.9},
        )
        self.assertIn("quality 64%", line)
        self.assertIn("negative_30d_pnl, profit_factor_below_1", line)

    def test_an_unscorable_wallet_omits_the_quality_figure(self) -> None:
        line = review.format_review_line(
            self.ADDRESS, {"reasons": ["inactive"], "qualityWinRatePct": None}
        )
        self.assertNotIn("quality", line)
        self.assertIn("(inactive)", line)


if __name__ == "__main__":
    unittest.main()


class PenaltyScopeTests(unittest.TestCase):
    A = "0x63d417a577b50c96f4f09148d4e4d70950db0522"

    def test_a_structural_reason_still_halves_the_weight(self) -> None:
        # The score reads closed round trips, so it cannot see a wallet that
        # stopped trading or churns inventory. These are the review's job.
        for reason in ("inactive", "market_maker_fill_rate", "manual_exclusion"):
            self.assertIn(reason, review.PENALISING_REVIEW_REASONS)

    def test_performance_reasons_no_longer_penalise(self) -> None:
        # The score already reads this history, and reads it better.
        self.assertNotIn("negative_30d_pnl", review.PENALISING_REVIEW_REASONS)
        self.assertNotIn("profit_factor_below_1", review.PENALISING_REVIEW_REASONS)

    def test_a_wallet_flagged_only_on_performance_keeps_full_weight(self) -> None:
        reviews = {
            self.A: {
                "weight": 1.0,
                "reasons": [],
                "notes": ["negative_30d_pnl", "profit_factor_below_1"],
                "qualityWinRatePct": 63.9,
            }
        }
        self.assertEqual(review.penalised_reviews(reviews), {})
        self.assertEqual(list(review.noted_reviews(reviews)), [self.A])

    def test_a_noted_wallet_is_still_reported_with_its_quality(self) -> None:
        line = review.format_noted_line(
            self.A, {"notes": ["negative_30d_pnl"], "qualityWinRatePct": 63.9}
        )
        self.assertIn(self.A, line)
        self.assertIn("quality 64%", line)
        self.assertIn("negative_30d_pnl", line)

    def test_a_penalised_wallet_is_counted_and_a_noted_one_is_not(self) -> None:
        reviews = {
            self.A: {"weight": 0.5, "reasons": ["inactive"], "notes": [], "qualityWinRatePct": 40.0},
            "0x" + "2" * 40: {
                "weight": 1.0,
                "reasons": [],
                "notes": ["negative_30d_pnl"],
                "qualityWinRatePct": 66.5,
            },
        }
        self.assertEqual(len(review.penalised_reviews(reviews)), 1)
        self.assertEqual(len(review.noted_reviews(reviews)), 1)
