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
