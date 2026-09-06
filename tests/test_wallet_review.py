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


class FillsPerClosedPositionTests(unittest.TestCase):
    def test_returns_the_ratio_for_a_normal_wallet(self) -> None:
        wallet = {"qualityWindowFillCount": 620, "qualityClosedEvents30d": 31}
        self.assertAlmostEqual(review.fills_per_closed_position(wallet), 20.0)

    def test_none_when_closed_events_is_zero(self) -> None:
        wallet = {"qualityWindowFillCount": 620, "qualityClosedEvents30d": 0}
        self.assertIsNone(review.fills_per_closed_position(wallet))

    def test_none_when_closed_events_is_missing(self) -> None:
        wallet = {"qualityWindowFillCount": 620}
        self.assertIsNone(review.fills_per_closed_position(wallet))

    def test_none_when_closed_events_is_non_numeric(self) -> None:
        wallet = {"qualityWindowFillCount": 620, "qualityClosedEvents30d": "n/a"}
        self.assertIsNone(review.fills_per_closed_position(wallet))

    def test_none_when_fill_count_is_missing_or_non_numeric(self) -> None:
        self.assertIsNone(
            review.fills_per_closed_position({"qualityClosedEvents30d": 31})
        )
        self.assertIsNone(
            review.fills_per_closed_position(
                {"qualityWindowFillCount": "n/a", "qualityClosedEvents30d": 31}
            )
        )


class FillRateWindowCappedTests(unittest.TestCase):
    def test_true_when_truncated_flag_is_set(self) -> None:
        wallet = {"dataQuality": {"recentFillsTruncated": True}}
        self.assertTrue(review.fill_rate_window_capped(wallet))

    def test_false_when_truncated_flag_is_absent_or_false(self) -> None:
        self.assertFalse(review.fill_rate_window_capped({"dataQuality": {}}))
        self.assertFalse(
            review.fill_rate_window_capped({"dataQuality": {"recentFillsTruncated": False}})
        )

    def test_false_when_data_quality_is_missing_or_not_a_dict(self) -> None:
        self.assertFalse(review.fill_rate_window_capped({}))
        self.assertFalse(review.fill_rate_window_capped({"dataQuality": "n/a"}))


class EvaluateWalletsFillVisibilityTests(unittest.TestCase):
    A = "0x63d417a577b50c96f4f09148d4e4d70950db0522"
    B = "0x" + "2" * 40

    def test_capped_window_without_market_maker_reason_becomes_a_note(self) -> None:
        wallet = {
            "address": self.A,
            "dataQuality": {"recentFillsTruncated": True},
            "holdingOnly30d": True,  # forces a reasons entry so it appears in reviews
        }
        reviews = review.evaluate_wallets([wallet])
        entry = reviews[self.A]
        self.assertIn("fill_rate_window_capped", entry["notes"])
        self.assertNotIn("fill_rate_window_capped", entry["reasons"])
        self.assertNotIn("market_maker_fill_rate", entry["reasons"])

    def test_market_maker_reason_suppresses_the_capped_note(self) -> None:
        wallet = {
            "address": self.B,
            "dataQuality": {
                "recentFillsTruncated": True,
                "recentFillCount": 3000,
                "fillCoverageMs": 60000,  # 3000/min, well above the 10/min threshold
            },
        }
        reviews = review.evaluate_wallets([wallet])
        entry = reviews[self.B]
        self.assertIn("market_maker_fill_rate", entry["reasons"])
        self.assertNotIn("fill_rate_window_capped", entry["notes"])
        self.assertNotIn("fill_rate_window_capped", entry["reasons"])


class HighestFillToCloseWalletsTests(unittest.TestCase):
    def test_sorted_descending_capped_at_five_with_full_addresses(self) -> None:
        wallets = [
            {
                "address": f"0x{'%040d' % i}",
                "qualityWindowFillCount": (i + 1) * 100,
                "qualityClosedEvents30d": 10,
            }
            for i in range(7)
        ]
        top = review.highest_fill_to_close_wallets(wallets)
        self.assertEqual(len(top), 5)
        values = [entry["fillsPerClosedPosition"] for entry in top]
        self.assertEqual(values, sorted(values, reverse=True))
        self.assertEqual(top[0]["address"], f"0x{'%040d' % 6}")
        for entry in top:
            self.assertEqual(len(entry["address"]), 42)
            self.assertNotIn("...", entry["address"])
