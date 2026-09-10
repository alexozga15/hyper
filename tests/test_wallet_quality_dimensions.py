from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import server  # noqa: E402
from server import ALERTS_FILE, HyperliquidClient, WalletStore, WalletTrackerService  # noqa: E402


ADDRESS = "0x1111111111111111111111111111111111111111"


def healthy_wallet(address: str = ADDRESS) -> dict:
    return {
        "address": address,
        "accountValue": 1_000_000.0,
        "totalNotional": 2_000_000.0,
        "unrealizedPnl": 25_000.0,
        "realizedPnl30d": 50_000.0,
        "closedTrades90d": 50,
        "qualityClosedEvents30d": 12,
        "qualityNetPnl30d": 30_000.0,
        "qualityProfitFactor30d": 1.8,
        "qualityTopWinConcentrationPct": 35.0,
        "qualityWindowTruncated": False,
        "positions": [],
    }


def validated_copyability() -> dict:
    return {
        "method": server.COPYABILITY_METHOD,
        "independentCompletedEpisodes": server.COPYABILITY_MIN_INDEPENDENT_EPISODES,
        "costAdjustedNetReturnPct": 4.2,
        "lowerConfidenceBoundPct": 0.3,
        "observationComplete": True,
    }


class WalletQualityDimensionTests(unittest.TestCase):
    def test_good_historical_statistics_are_not_trade_admission(self) -> None:
        dimensions = server.wallet_quality_dimensions(healthy_wallet())
        self.assertEqual(dimensions["performance"]["status"], "pass")
        self.assertEqual(dimensions["evidence"]["status"], "pass")
        self.assertEqual(dimensions["risk"]["status"], "pass")
        self.assertEqual(dimensions["copyability"]["status"], "pending")
        self.assertEqual(dimensions["copyability"]["score"], 50.0)
        self.assertGreaterEqual(dimensions["rank"], 0.0)
        self.assertLessEqual(dimensions["rank"], 100.0)
        self.assertIs(dimensions["tradeEligible"], False)

    def test_copyability_needs_sample_and_positive_lower_bound(self) -> None:
        thin = validated_copyability()
        thin["independentCompletedEpisodes"] -= 1
        self.assertEqual(server.copyability_assessment(thin)["status"], "pending")

        failed = validated_copyability()
        failed["lowerConfidenceBoundPct"] = -0.1
        self.assertEqual(server.copyability_assessment(failed)["status"], "fail")

        passed = server.wallet_quality_dimensions(healthy_wallet(), validated_copyability())
        self.assertEqual(passed["copyability"]["status"], "pass")
        expected = sum(
            passed[name]["score"] * passed["rankWeights"][name]
            for name in ("performance", "evidence", "risk", "copyability")
        )
        self.assertAlmostEqual(passed["rank"], expected, places=1)
        self.assertIs(passed["tradeEligible"], True)

    def test_open_loss_is_a_separate_risk_veto(self) -> None:
        wallet = healthy_wallet()
        wallet["accountValue"] = 200_000.0
        wallet["positions"] = [
            {"coin": "BTC", "side": "Long", "unrealizedPnl": -250_000.0},
            {"coin": "ETH", "side": "Long", "unrealizedPnl": 300_000.0},
        ]
        dimensions = server.wallet_quality_dimensions(wallet, validated_copyability())
        self.assertEqual(dimensions["risk"]["status"], "fail")
        self.assertEqual(dimensions["risk"]["openLossToEquityPct"], 125.0)
        self.assertIs(dimensions["tradeEligible"], False)

    def test_group_persists_members_and_only_admits_validated_wallets(self) -> None:
        service = WalletTrackerService(WalletStore(Path(ALERTS_FILE)), HyperliquidClient())
        wallets = []
        registry = {}
        for digit in ("1", "2", "3"):
            address = "0x" + digit * 40
            wallet = healthy_wallet(address)
            wallet["positions"] = [
                {"coin": "BTC", "side": "Long", "positionValue": 1_000_000.0, "size": 10.0, "entryPx": 100.0}
            ]
            wallet["recentFills"] = []
            wallets.append(wallet)
            registry[address] = validated_copyability()

        with patch("server.load_wallet_copyability", return_value=registry):
            group = service.build_position_groups({"wallets": wallets})[0]

        self.assertEqual(group["walletAddresses"], sorted(registry))
        self.assertEqual(group["eligibleWalletAddresses"], sorted(registry))
        self.assertEqual(group["tradeAdmission"]["status"], "eligible")
        self.assertIn("positionRank", group)
        self.assertEqual(set(group["positionRankComponents"]), {"performance", "evidence", "risk", "copyability"})
        self.assertTrue(server.group_trade_admission(group))


if __name__ == "__main__":
    unittest.main()
