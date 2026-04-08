import unittest
from pathlib import Path

from server import (
    ALERTS_FILE,
    HyperliquidClient,
    WalletStore,
    WalletTrackerService,
    classify_profitability,
    classify_wallet_size,
    normalize_address,
    parse_import_lines,
    side_from_size,
)


class SegmentTests(unittest.TestCase):
    def test_wallet_size_bands(self) -> None:
        self.assertEqual(classify_wallet_size(5_000_000), "Apex")
        self.assertEqual(classify_wallet_size(200_000), "Large")
        self.assertEqual(classify_wallet_size(5_000), "Small")

    def test_profitability_bands(self) -> None:
        self.assertEqual(classify_profitability(2_000_000), "Money Printer")
        self.assertEqual(classify_profitability(50_000), "Profitable")
        self.assertEqual(classify_profitability(-250_000), "Very Unprofitable")

    def test_side_from_size(self) -> None:
        self.assertEqual(side_from_size(10), "Long")
        self.assertEqual(side_from_size(-0.5), "Short")
        self.assertEqual(side_from_size(0), "Flat")

    def test_normalize_address(self) -> None:
        self.assertEqual(
            normalize_address("alias 0xa5232e97b4ded3d2EF25Be059c3489e61Be475Aa notes"),
            "0xa5232e97b4ded3d2EF25Be059c3489e61Be475Aa",
        )
        self.assertEqual(normalize_address("bad input"), "")

    def test_parse_import_lines(self) -> None:
        entries, invalid = parse_import_lines(
            "\n".join(
                [
                    "0xa5232e97b4ded3d2EF25Be059c3489e61Be475Aa",
                    "Alpha,0xa5232e97b4ded3d2EF25Be059c3489e61Be475Aa,desk note",
                    "0xa5232e97b4ded3d2EF25Be059c3489e61Be475Aa,Beta,carry",
                    "bad",
                ]
            )
        )
        self.assertEqual(len(entries), 3)
        self.assertEqual(entries[1]["alias"], "Alpha")
        self.assertEqual(entries[2]["alias"], "Beta")
        self.assertEqual(entries[2]["notes"], "carry")
        self.assertEqual(invalid, ["bad"])


class AlertSummaryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.service = WalletTrackerService(WalletStore(Path(ALERTS_FILE)), HyperliquidClient())

    def test_build_sentiment_summary_respects_threshold_and_hip3(self) -> None:
        snapshots = [
            {
                "address": "0x1111111111111111111111111111111111111111",
                "alias": "One",
                "positions": [
                    {"coin": "BTC", "side": "Long", "positionValue": 1000},
                    {"coin": "@123", "side": "Short", "positionValue": 200},
                ],
            },
            {
                "address": "0x2222222222222222222222222222222222222222",
                "alias": "Two",
                "positions": [{"coin": "BTC", "side": "Long", "positionValue": 1500}],
            },
            {
                "address": "0x3333333333333333333333333333333333333333",
                "alias": "Three",
                "positions": [{"coin": "BTC", "side": "Long", "positionValue": 2000}],
            },
        ]

        summary = self.service.build_sentiment_summary(snapshots, min_wallets=3)
        self.assertEqual(summary["overallBias"], "bullish")
        self.assertEqual(len(summary["consensus"]), 1)
        self.assertEqual(summary["consensus"][0]["coin"], "BTC")
        self.assertEqual(summary["consensus"][0]["walletCount"], 3)
        self.assertEqual(len(summary["hip3Positions"]), 1)
        self.assertEqual(summary["hip3Positions"][0]["coin"], "@123")

    def test_summarize_changes_detects_consensus_and_hip3_deltas(self) -> None:
        previous = {
            "overallBias": "mixed",
            "consensus": [{"coin": "BTC", "side": "long", "walletCount": 3, "totalValue": 100.0}],
            "hip3Positions": [{"coin": "@1", "side": "short", "address": "0xabc"}],
        }
        current = {
            "overallBias": "bearish",
            "consensus": [{"coin": "ETH", "side": "short", "walletCount": 4, "totalValue": 200.0}],
            "hip3Positions": [{"coin": "@2", "side": "long", "address": "0xdef"}],
        }

        changes = self.service.summarize_changes(previous, current, track_hip3=True)
        self.assertTrue(changes["biasChanged"])
        self.assertEqual(changes["addedConsensus"][0]["coin"], "ETH")
        self.assertEqual(changes["removedConsensus"][0]["coin"], "BTC")
        self.assertEqual(changes["hip3Added"][0]["coin"], "@2")
        self.assertEqual(changes["hip3Removed"][0]["coin"], "@1")


if __name__ == "__main__":
    unittest.main()
