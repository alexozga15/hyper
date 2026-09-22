import unittest

from paper_analysis import matched_opportunity_analysis


class MatchedOpportunityAnalysisTests(unittest.TestCase):
    def test_rank_rejection_counts_zero_only_against_completed_control(self):
        baseline = 1_700_000_000_000
        fingerprint = {
            "walletAddresses": ["0xa", "0xb", "0xc"],
            "freshAddLatestTime": baseline + 1_000,
        }
        evaluations = [
            {"experimentArm": "ranked_consensus", "signalKey": "ETH:long",
             "coin": "ETH", "evaluatedAtMs": baseline + 2_000,
             "inputs": {"consensusFingerprint": fingerprint,
                        "entryDecision": "policy_rejected"}},
            {"experimentArm": "consensus_unranked", "signalKey": "ETH:long",
             "coin": "ETH", "evaluatedAtMs": baseline + 2_000,
             "inputs": {"consensusFingerprint": fingerprint,
                        "entryDecision": "selected_for_quote"}},
        ]
        control = {
            "experimentArm": "consensus_unranked", "signalKey": "ETH:long",
            "coin": "ETH", "consensusFingerprint": fingerprint,
            "status": "closed", "executionStatus": "closed_modeled_net",
            "executionResult": {"complete": True, "netUsd": 25.0},
        }
        result = matched_opportunity_analysis(
            [control], evaluations, baseline_at_ms=baseline,
        )
        self.assertTrue(result["complete"])
        self.assertEqual(result["completedOpportunities"], 1)
        self.assertEqual(result["rankRejectedControlEntered"], 1)
        self.assertEqual(result["rankedMinusControlNetUsdOnCompleted"], -25.0)

    def test_selected_but_open_trade_is_unknown_not_zero(self):
        baseline = 1_700_000_000_000
        fingerprint = {
            "walletAddresses": ["0xa", "0xb", "0xc"],
            "freshAddLatestTime": baseline + 1_000,
        }
        evaluations = [
            {"experimentArm": arm, "signalKey": "BTC:long", "coin": "BTC",
             "evaluatedAtMs": baseline + 2_000,
             "inputs": {"consensusFingerprint": fingerprint,
                        "entryDecision": "selected_for_quote"}}
            for arm in ("ranked_consensus", "consensus_unranked")
        ]
        records = [{
            "experimentArm": arm, "signalKey": "BTC:long", "coin": "BTC",
            "consensusFingerprint": fingerprint, "status": "open",
            "executionStatus": "entry_quoted",
        } for arm in ("ranked_consensus", "consensus_unranked")]
        result = matched_opportunity_analysis(
            records, evaluations, baseline_at_ms=baseline,
        )
        self.assertFalse(result["complete"])
        self.assertEqual(result["completedOpportunities"], 0)
        self.assertEqual(result["unresolved"]["open_or_unverified_trade"], 1)


if __name__ == "__main__":
    unittest.main()
