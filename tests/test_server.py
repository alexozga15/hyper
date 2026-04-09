import unittest
from unittest.mock import patch
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
                "positions": [
                    {"coin": "BTC", "side": "Long", "positionValue": 1500},
                    {"coin": "@123", "side": "Short", "positionValue": 300},
                ],
            },
            {
                "address": "0x3333333333333333333333333333333333333333",
                "alias": "Three",
                "positions": [
                    {"coin": "BTC", "side": "Long", "positionValue": 2000},
                    {"coin": "@123", "side": "Short", "positionValue": 400},
                ],
            },
        ]

        summary = self.service.build_sentiment_summary(snapshots, min_wallets=3)
        self.assertEqual(summary["overallBias"], "bullish")
        self.assertEqual(len(summary["consensus"]), 2)
        self.assertEqual(summary["consensus"][0]["coin"], "BTC")
        self.assertEqual(summary["consensus"][0]["walletCount"], 3)
        self.assertEqual(len(summary["hip3Consensus"]), 1)
        self.assertEqual(summary["hip3Consensus"][0]["coin"], "@123")
        self.assertEqual(summary["hip3Consensus"][0]["walletCount"], 3)

    def test_summarize_changes_detects_consensus_and_hip3_deltas(self) -> None:
        previous = {
            "overallBias": "mixed",
            "consensus": [{"coin": "BTC", "side": "long", "walletCount": 3, "totalValue": 100.0}],
            "hip3Consensus": [{"coin": "@1", "side": "short", "walletCount": 3, "totalValue": 50.0}],
        }
        current = {
            "overallBias": "bearish",
            "consensus": [{"coin": "ETH", "side": "short", "walletCount": 4, "totalValue": 200.0}],
            "hip3Consensus": [{"coin": "@2", "side": "long", "walletCount": 4, "totalValue": 75.0}],
        }

        changes = self.service.summarize_changes(previous, current, track_hip3=True)
        self.assertTrue(changes["biasChanged"])
        self.assertEqual(changes["addedConsensus"][0]["coin"], "ETH")
        self.assertEqual(changes["removedConsensus"][0]["coin"], "BTC")
        self.assertEqual(changes["hip3Added"][0]["coin"], "@2")
        self.assertEqual(changes["hip3Removed"][0]["coin"], "@1")

    def test_resolve_alert_config_prefers_env_over_stored_values(self) -> None:
        stored = {
            "enabled": False,
            "botToken": "stored-token",
            "chatId": "stored-chat",
            "minConsensusWallets": 2,
            "trackHip3": False,
        }
        with patch.dict(
            "os.environ",
            {
                "ALERTS_ENABLED": "true",
                "TELEGRAM_BOT_TOKEN": "env-token",
                "TELEGRAM_CHAT_ID": "env-chat",
                "MIN_CONSENSUS_WALLETS": "3",
                "TRACK_HIP3": "true",
            },
            clear=False,
        ):
            config = self.service.resolve_alert_config(stored)

        self.assertTrue(config["enabled"])
        self.assertEqual(config["botToken"], "env-token")
        self.assertEqual(config["chatId"], "env-chat")
        self.assertEqual(config["minConsensusWallets"], 3)
        self.assertTrue(config["trackHip3"])

    def test_build_summary_message_includes_consensus_and_hip3_sections(self) -> None:
        summary = {
            "generatedAt": "2026-04-09T06:00:00Z",
            "overallBias": "bearish",
            "walletCount": 16,
            "consensus": [{"coin": "BTC", "side": "long", "walletCount": 3, "totalValue": 12345.0}],
            "hip3Consensus": [{"coin": "@PUMP-1", "side": "short", "walletCount": 3, "totalValue": 456.0}],
        }

        message = self.service.build_summary_message(summary, min_wallets=3)
        self.assertIn("Current wallet sentiment", message)
        self.assertIn("BTC long (3 wallets, $12,345)", message)
        self.assertIn("@PUMP-1 short (3 wallets, $456)", message)

    def test_build_positions_message_lists_all_open_positions(self) -> None:
        dashboard = {
            "generatedAt": "2026-04-09T06:00:00Z",
            "wallets": [
                {
                    "alias": "main-1",
                    "address": "0x1111111111111111111111111111111111111111",
                    "positions": [
                        {"coin": "BTC", "side": "Long", "positionValue": 125000.0},
                        {"coin": "ETH", "side": "Short", "positionValue": 99000.0},
                    ],
                },
                {
                    "alias": "main-2",
                    "address": "0x2222222222222222222222222222222222222222",
                    "positions": [{"coin": "BTC", "side": "Long", "positionValue": 225000.0}],
                },
            ],
        }

        message = self.service.build_positions_message(dashboard)
        self.assertIn("Open positions now", message)
        self.assertIn("By position (>= $100,000):", message)
        self.assertIn("BTC long (2 wallets, 2 positions, $350,000)", message)
        self.assertNotIn("ETH short", message)
        self.assertIn("Position groups: 1", message)

    def test_build_positions_message_includes_hip3_positions_below_main_threshold(self) -> None:
        dashboard = {
            "generatedAt": "2026-04-09T06:00:00Z",
            "wallets": [
                {
                    "alias": "main-1",
                    "address": "0x1111111111111111111111111111111111111111",
                    "positions": [
                        {"coin": "@MOON-1", "side": "Long", "positionValue": 1200.0},
                        {"coin": "BTC", "side": "Long", "positionValue": 150000.0},
                    ],
                },
                {
                    "alias": "main-2",
                    "address": "0x2222222222222222222222222222222222222222",
                    "positions": [{"coin": "@MOON-1", "side": "Long", "positionValue": 800.0}],
                },
            ],
        }

        message = self.service.build_positions_message(dashboard)
        self.assertIn("HIP-3 positions:", message)
        self.assertIn("@MOON-1 long (2 wallets, 2 positions, $2,000)", message)
        self.assertIn("BTC long (1 wallets, 1 positions, $150,000)", message)
        self.assertIn("Position groups: 2", message)


if __name__ == "__main__":
    unittest.main()
