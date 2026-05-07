import unittest
from unittest.mock import patch
from pathlib import Path

from server import (
    ALERTS_FILE,
    ELITE_WALLET_OVERRIDES,
    HyperliquidClient,
    WalletStore,
    WalletTrackerService,
    build_wallet_quality_rank,
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

    def test_wallet_quality_rank_combines_7d_hit_rate_and_pnl(self) -> None:
        self.assertEqual(build_wallet_quality_rank(100, 1, 10_000, 100_000)["label"], "Unranked")
        strong = build_wallet_quality_rank(70, 20, 20_000, 100_000)
        self.assertEqual(strong["label"], "Strong")
        self.assertEqual(strong["metric"], "multi_period_quality")
        self.assertEqual(strong["score"], 71.5)
        losing = build_wallet_quality_rank(80, 20, -50_000, 100_000)
        self.assertEqual(losing["label"], "Cold")
        self.assertEqual(losing["pnlReturnPct"], -50.0)

    def test_wallet_quality_rank_requires_drawdown_control_for_elite(self) -> None:
        elite = build_wallet_quality_rank(
            90,
            20,
            30_000,
            100_000,
            closed_trade_count_30d=30,
            pnl_30d=40_000,
            gross_profit_30d=60_000,
            gross_loss_30d=10_000,
            max_drawdown_pct=5,
            margin_usage_pct=25,
            unrealized_pnl=5_000,
        )
        self.assertEqual(elite["label"], "Elite")
        self.assertTrue(elite["eliteEligible"])
        high_drawdown = build_wallet_quality_rank(
            90,
            20,
            30_000,
            100_000,
            closed_trade_count_30d=30,
            pnl_30d=40_000,
            gross_profit_30d=60_000,
            gross_loss_30d=10_000,
            max_drawdown_pct=40,
            margin_usage_pct=25,
            unrealized_pnl=5_000,
        )
        self.assertNotEqual(high_drawdown["label"], "Elite")
        self.assertFalse(high_drawdown["eliteEligible"])

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

    def test_elite_override_wallet_is_configured(self) -> None:
        self.assertIn("0xc9e839a529d1a3a46e2b48d20c461d4afecb72e4", ELITE_WALLET_OVERRIDES)

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

    def test_summarize_changes_ignores_minor_consensus_size_drift(self) -> None:
        previous = {
            "overallBias": "mixed",
            "consensus": [{"coin": "BTC", "side": "long", "walletCount": 7, "totalValue": 70_000_000.0}],
            "hip3Consensus": [],
        }
        current = {
            "overallBias": "mixed",
            "consensus": [{"coin": "BTC", "side": "long", "walletCount": 8, "totalValue": 75_000_000.0}],
            "hip3Consensus": [],
        }

        changes = self.service.summarize_changes(previous, current, track_hip3=False)

        self.assertEqual(changes["changedConsensus"], [])

    def test_summarize_changes_detects_major_consensus_size_change(self) -> None:
        previous = {
            "overallBias": "mixed",
            "consensus": [{"coin": "BTC", "side": "long", "walletCount": 3, "totalValue": 30_000_000.0}],
            "hip3Consensus": [],
        }
        current = {
            "overallBias": "mixed",
            "consensus": [{"coin": "BTC", "side": "long", "walletCount": 5, "totalValue": 50_000_000.0}],
            "hip3Consensus": [],
        }

        changes = self.service.summarize_changes(previous, current, track_hip3=False)

        self.assertEqual(len(changes["changedConsensus"]), 1)
        self.assertEqual(changes["changedConsensus"][0]["fromWalletCount"], 3)
        self.assertEqual(changes["changedConsensus"][0]["toWalletCount"], 5)

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
            "consensus": [{"coin": "BTC", "side": "long", "walletCount": 3, "totalValue": 12345.0, "convictionScore": 84.0}],
            "hip3Consensus": [{"coin": "@PUMP-1", "side": "short", "walletCount": 3, "totalValue": 456.0}],
        }

        message = self.service.build_summary_message(summary, min_wallets=3)
        self.assertIn("Current wallet sentiment", message)
        self.assertIn("BTC long (3 wallets, $12,345, conviction 84/100)", message)
        self.assertNotIn("HIP-3 consensus:", message)
        self.assertNotIn("@PUMP-1 short (3 wallets, $456)", message)

        hip3_message = self.service.build_summary_message(summary, min_wallets=3, include_consensus=False, include_hip3=True)
        self.assertIn("HIP-3 consensus:", hip3_message)
        self.assertIn("@PUMP-1 short (3 wallets, $456)", hip3_message)

    def test_build_sentiment_summary_assigns_conviction_scores(self) -> None:
        snapshots = [
            {
                "address": "0x1111111111111111111111111111111111111111",
                "alias": "One",
                "positions": [
                    {"coin": "BTC", "side": "Long", "positionValue": 500000},
                    {"coin": "ETH", "side": "Long", "positionValue": 200000},
                ],
            },
            {
                "address": "0x2222222222222222222222222222222222222222",
                "alias": "Two",
                "positions": [
                    {"coin": "BTC", "side": "Long", "positionValue": 400000},
                    {"coin": "ETH", "side": "Long", "positionValue": 100000},
                ],
            },
            {
                "address": "0x3333333333333333333333333333333333333333",
                "alias": "Three",
                "positions": [
                    {"coin": "BTC", "side": "Long", "positionValue": 300000},
                ],
            },
        ]

        summary = self.service.build_sentiment_summary(snapshots, min_wallets=2)
        self.assertEqual(summary["consensus"][0]["coin"], "BTC")
        self.assertEqual(summary["consensus"][0]["convictionScore"], 100.0)
        self.assertGreater(summary["consensus"][0]["convictionScore"], summary["consensus"][1]["convictionScore"])

    def test_build_sentiment_summary_emits_high_conviction_signals(self) -> None:
        snapshots = [
            {
                "address": "0x1111111111111111111111111111111111111111",
                "alias": "One",
                "positions": [{"coin": "BTC", "side": "Long", "positionValue": 500000}],
            },
            {
                "address": "0x2222222222222222222222222222222222222222",
                "alias": "Two",
                "positions": [{"coin": "BTC", "side": "Long", "positionValue": 400000}],
            },
            {
                "address": "0x3333333333333333333333333333333333333333",
                "alias": "Three",
                "positions": [{"coin": "BTC", "side": "Long", "positionValue": 300000}],
            },
            {
                "address": "0x4444444444444444444444444444444444444444",
                "alias": "Four",
                "positions": [{"coin": "ETH", "side": "Short", "positionValue": 100000}],
            },
            {
                "address": "0x5555555555555555555555555555555555555555",
                "alias": "Five",
                "positions": [{"coin": "ETH", "side": "Short", "positionValue": 100000}],
            },
        ]

        summary = self.service.build_sentiment_summary(snapshots, min_wallets=2)

        self.assertEqual(summary["signalCount"], 1)
        self.assertEqual(summary["signals"][0]["coin"], "BTC")
        self.assertEqual(summary["signals"][0]["action"], "buy")
        self.assertEqual(summary["signals"][0]["strength"], "extreme")
        self.assertEqual(summary["signals"][0]["convictionScore"], 100.0)

    def test_summarize_changes_detects_signal_changes(self) -> None:
        previous = {
            "overallBias": "mixed",
            "consensus": [],
            "hip3Consensus": [],
            "signals": [
                {
                    "coin": "BTC",
                    "side": "long",
                    "action": "buy",
                    "walletCount": 3,
                    "totalValue": 1_000_000.0,
                    "convictionScore": 82.0,
                }
            ],
        }
        current = {
            "overallBias": "mixed",
            "consensus": [],
            "hip3Consensus": [],
            "signals": [
                {
                    "coin": "BTC",
                    "side": "long",
                    "action": "buy",
                    "walletCount": 5,
                    "totalValue": 2_000_000.0,
                    "convictionScore": 95.0,
                },
                {
                    "coin": "ETH",
                    "side": "short",
                    "action": "sell",
                    "walletCount": 4,
                    "totalValue": 1_500_000.0,
                    "convictionScore": 91.0,
                },
            ],
        }

        changes = self.service.summarize_changes(previous, current, track_hip3=False)

        self.assertEqual(changes["addedSignals"][0]["coin"], "ETH")
        self.assertEqual(changes["changedSignals"][0]["coin"], "BTC")
        self.assertEqual(changes["changedSignals"][0]["fromConvictionScore"], 82.0)
        self.assertEqual(changes["changedSignals"][0]["toConvictionScore"], 95.0)

    def test_build_signals_message_formats_signal_actions(self) -> None:
        summary = {
            "generatedAt": "2026-05-07T00:00:00Z",
            "signals": [
                {
                    "coin": "BTC",
                    "side": "long",
                    "action": "buy",
                    "walletCount": 3,
                    "totalValue": 1_250_000.0,
                    "convictionScore": 94.0,
                }
            ],
        }

        message = self.service.build_signals_message(summary)

        self.assertIn("High-conviction signals", message)
        self.assertIn("1. BUY BTC long (3 wallets, $1.2M, conviction 94/100)", message)

    def test_build_holding_only_wallets_returns_30d_holders_by_notional(self) -> None:
        wallets = [
            {
                "address": "0x1111111111111111111111111111111111111111",
                "alias": "Holder",
                "accountValue": 500_000.0,
                "totalNotional": 1_500_000.0,
                "unrealizedPnl": 25_000.0,
                "holdingOnly30d": True,
                "openOrderCount": 0,
                "fills30d": 0,
                "daysSinceLastFill": 45.0,
                "positions": [{"coin": "BTC", "side": "Long", "positionValue": 1_500_000.0}],
            },
            {
                "address": "0x2222222222222222222222222222222222222222",
                "alias": "Bigger Holder",
                "accountValue": 700_000.0,
                "totalNotional": 2_000_000.0,
                "unrealizedPnl": -10_000.0,
                "holdingOnly30d": True,
                "openOrderCount": 0,
                "fills30d": 0,
                "daysSinceLastFill": None,
                "positions": [{"coin": "ETH", "side": "Short", "positionValue": 2_000_000.0}],
            },
            {
                "address": "0x3333333333333333333333333333333333333333",
                "alias": "Trader",
                "accountValue": 900_000.0,
                "totalNotional": 3_000_000.0,
                "holdingOnly30d": False,
                "positions": [{"coin": "SOL", "side": "Long", "positionValue": 3_000_000.0}],
            },
        ]

        holders = self.service.build_holding_only_wallets(wallets)

        self.assertEqual([wallet["alias"] for wallet in holders], ["Bigger Holder", "Holder"])
        self.assertEqual(holders[0]["fills30d"], 0)
        self.assertEqual(holders[0]["topPosition"]["coin"], "ETH")

    def test_build_sentiment_summary_groups_oil_aliases(self) -> None:
        snapshots = [
            {
                "address": "0x1111111111111111111111111111111111111111",
                "alias": "One",
                "positions": [{"coin": "flx:OIL", "side": "Long", "positionValue": 120000}],
            },
            {
                "address": "0x2222222222222222222222222222222222222222",
                "alias": "Two",
                "positions": [{"coin": "xyz:BRENTOIL", "side": "Long", "positionValue": 240000}],
            },
            {
                "address": "0x3333333333333333333333333333333333333333",
                "alias": "Three",
                "positions": [{"coin": "cash:WTI", "side": "Long", "positionValue": 360000}],
            },
        ]

        summary = self.service.build_sentiment_summary(snapshots, min_wallets=3)
        self.assertEqual(summary["consensus"][0]["coin"], "OIL")
        self.assertEqual(summary["consensus"][0]["walletCount"], 3)
        self.assertEqual(summary["consensus"][0]["totalValue"], 720000)

    def test_build_sentiment_summary_groups_gold_and_silver_aliases(self) -> None:
        snapshots = [
            {
                "address": "0x1111111111111111111111111111111111111111",
                "alias": "One",
                "positions": [{"coin": "xyz:GOLD", "side": "Long", "positionValue": 120000}],
            },
            {
                "address": "0x2222222222222222222222222222222222222222",
                "alias": "Two",
                "positions": [{"coin": "cash:GOLD", "side": "Long", "positionValue": 240000}],
            },
            {
                "address": "0x3333333333333333333333333333333333333333",
                "alias": "Three",
                "positions": [{"coin": "xyz:SILVER", "side": "Short", "positionValue": 360000}],
            },
            {
                "address": "0x4444444444444444444444444444444444444444",
                "alias": "Four",
                "positions": [{"coin": "xyz:SILVER", "side": "Short", "positionValue": 480000}],
            },
            {
                "address": "0x5555555555555555555555555555555555555555",
                "alias": "Five",
                "positions": [{"coin": "cash:SILVER", "side": "Short", "positionValue": 600000}],
            },
        ]

        summary = self.service.build_sentiment_summary(snapshots, min_wallets=2)
        self.assertTrue(any(item["coin"] == "GOLD" for item in summary["consensus"]))
        self.assertTrue(any(item["coin"] == "SILVER" for item in summary["consensus"]))

    def test_build_sentiment_summary_strips_stock_prefixes(self) -> None:
        snapshots = [
            {
                "address": "0x1111111111111111111111111111111111111111",
                "alias": "One",
                "positions": [{"coin": "xyz:NVDA", "side": "Long", "positionValue": 120000}],
            },
            {
                "address": "0x2222222222222222222222222222222222222222",
                "alias": "Two",
                "positions": [{"coin": "xyz:NVDA", "side": "Long", "positionValue": 240000}],
            },
            {
                "address": "0x3333333333333333333333333333333333333333",
                "alias": "Three",
                "positions": [{"coin": "xyz:NVDA", "side": "Long", "positionValue": 360000}],
            },
        ]

        summary = self.service.build_sentiment_summary(snapshots, min_wallets=3)
        self.assertEqual(summary["consensus"][0]["coin"], "NVDA")
        self.assertEqual(summary["consensus"][0]["walletCount"], 3)
        self.assertEqual(summary["consensus"][0]["totalValue"], 720000)

    def test_build_wallet_rankings_message_orders_by_7d_quality_score(self) -> None:
        dashboard = {
            "generatedAt": "2026-05-05T08:00:00Z",
            "wallets": [
                {
                    "alias": "Lucky Small Sample",
                    "address": "0x1111111111111111111111111111111111111111",
                    "hitRate": 100.0,
                    "recentClosedTrades": 3,
                    "recentRealizedPnl": 5000.0,
                    "recentWinRateRank": build_wallet_quality_rank(100.0, 3, 5000.0, 100_000.0),
                },
                {
                    "alias": "Consistent Winner",
                    "address": "0x2222222222222222222222222222222222222222",
                    "hitRate": 70.0,
                    "recentClosedTrades": 20,
                    "recentRealizedPnl": 25000.0,
                    "recentWinRateRank": build_wallet_quality_rank(70.0, 20, 25_000.0, 100_000.0),
                },
                {
                    "alias": "High WR Losing",
                    "address": "0x3333333333333333333333333333333333333333",
                    "hitRate": 80.0,
                    "recentClosedTrades": 20,
                    "recentRealizedPnl": -50_000.0,
                    "recentWinRateRank": build_wallet_quality_rank(80.0, 20, -50_000.0, 100_000.0),
                },
            ],
        }

        message = self.service.build_wallet_rankings_message(dashboard)

        self.assertIn("Wallet ranks by multi-period quality", message)
        self.assertIn("1. Consistent Winner: Strong", message)
        self.assertIn("2. High WR Losing: Cold", message)
        self.assertNotIn("Lucky Small Sample", message)

    def test_build_elite_wallet_positions_message_lists_only_elite_wallet_positions(self) -> None:
        dashboard = {
            "generatedAt": "2026-05-07T08:00:00Z",
            "wallets": [
                {
                    "alias": "Elite Trader",
                    "address": "0x1111111111111111111111111111111111111111",
                    "accountValue": 100_000.0,
                    "totalNotional": 1_250_000.0,
                    "recentWinRateRank": build_wallet_quality_rank(
                        90.0,
                        20,
                        30_000.0,
                        100_000.0,
                        closed_trade_count_30d=30,
                        pnl_30d=40_000.0,
                        gross_profit_30d=60_000.0,
                        gross_loss_30d=10_000.0,
                        max_drawdown_pct=5.0,
                        margin_usage_pct=25.0,
                        unrealized_pnl=5_000.0,
                    ),
                    "positions": [
                        {
                            "coin": "BTC",
                            "side": "Long",
                            "positionValue": 1_000_000.0,
                            "size": 10.0,
                            "entryPx": 100_000.0,
                            "unrealizedPnl": 12_345.0,
                        },
                        {"coin": "ETH", "side": "Short", "positionValue": 250_000.0},
                    ],
                },
                {
                    "alias": "Strong Trader",
                    "address": "0x2222222222222222222222222222222222222222",
                    "accountValue": 100_000.0,
                    "totalNotional": 2_000_000.0,
                    "recentWinRateRank": build_wallet_quality_rank(70.0, 20, 25_000.0, 100_000.0),
                    "positions": [{"coin": "SOL", "side": "Long", "positionValue": 2_000_000.0}],
                },
            ],
        }

        message = self.service.build_elite_wallet_positions_message(dashboard)

        self.assertIn("Elite wallet positions", message)
        self.assertIn("Elite Trader (88.8/100", message)
        self.assertIn("30D closes, PF 6.0, DD 5.0%", message)
        self.assertIn("- BTC long $1,000K, size 10, entry $100,000, uPnL $12,345", message)
        self.assertIn("- ETH short $250K", message)
        self.assertNotIn("Strong Trader", message)

    def test_build_positions_message_lists_all_open_positions(self) -> None:
        dashboard = {
            "generatedAt": "2026-04-09T06:00:00Z",
            "wallets": [
                {
                    "alias": "main-1",
                    "address": "0x1111111111111111111111111111111111111111",
                    "positions": [
                        {"coin": "BTC", "side": "Long", "positionValue": 325000.0, "size": 4.0, "entryPx": 80000.0},
                        {"coin": "ETH", "side": "Short", "positionValue": 99000.0},
                    ],
                },
                {
                    "alias": "main-2",
                    "address": "0x2222222222222222222222222222222222222222",
                    "positions": [{"coin": "BTC", "side": "Long", "positionValue": 225000.0, "size": 3.0, "entryPx": 76000.0}],
                },
            ],
        }

        message = self.service.build_positions_message(dashboard)
        self.assertIn("Open positions now", message)
        self.assertIn("By position (>= $500,000, 2+ wallets):", message)
        self.assertIn("BTC long (2 wallets, 2 positions, $550K, size 7, entry $78,286)", message)
        self.assertNotIn("ETH short", message)
        self.assertIn("Position groups: 1", message)

    def test_build_positions_message_filters_hip3_positions_below_threshold(self) -> None:
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
        self.assertIn("- No open positions", message)
        self.assertNotIn("@MOON-1 long", message)
        self.assertNotIn("BTC long (1 wallets, 1 positions, $150,000)", message)
        self.assertIn("Position groups: 0", message)

    def test_build_positions_message_groups_oil_aliases_under_oil(self) -> None:
        dashboard = {
            "generatedAt": "2026-04-09T06:00:00Z",
            "wallets": [
                {
                    "alias": "main-1",
                    "address": "0x1111111111111111111111111111111111111111",
                    "positions": [{"coin": "flx:OIL", "side": "Long", "positionValue": 404840.15}],
                },
                {
                    "alias": "main-2",
                    "address": "0x2222222222222222222222222222222222222222",
                    "positions": [{"coin": "xyz:BRENTOIL", "side": "Long", "positionValue": 1590960.44}],
                },
                {
                    "alias": "main-3",
                    "address": "0x3333333333333333333333333333333333333333",
                    "positions": [{"coin": "cash:WTI", "side": "Short", "positionValue": 573226.89}],
                },
            ],
        }

        message = self.service.build_positions_message(dashboard)
        self.assertIn("OIL long (2 wallets, 2 positions, $1,996K)", message)
        self.assertNotIn("OIL short", message)

    def test_build_positions_message_filters_commodities_below_threshold(self) -> None:
        dashboard = {
            "generatedAt": "2026-04-09T06:00:00Z",
            "wallets": [
                {
                    "alias": "main-1",
                    "address": "0x1111111111111111111111111111111111111111",
                    "positions": [
                        {"coin": "xyz:GOLD", "side": "Long", "positionValue": 96322.0},
                        {"coin": "xyz:SILVER", "side": "Short", "positionValue": 76938.0},
                    ],
                },
                {
                    "alias": "main-2",
                    "address": "0x2222222222222222222222222222222222222222",
                    "positions": [{"coin": "cash:GOLD", "side": "Long", "positionValue": 17206.81}],
                },
            ],
        }

        message = self.service.build_positions_message(dashboard)
        self.assertIn("- No open positions", message)
        self.assertNotIn("GOLD long", message)
        self.assertNotIn("SILVER short", message)
        self.assertNotIn("xyz:GOLD", message)
        self.assertNotIn("xyz:SILVER", message)

    def test_build_positions_message_filters_stocks_below_threshold(self) -> None:
        dashboard = {
            "generatedAt": "2026-04-09T06:00:00Z",
            "wallets": [
                {
                    "alias": "main-1",
                    "address": "0x1111111111111111111111111111111111111111",
                    "positions": [
                        {"coin": "xyz:NVDA", "side": "Long", "positionValue": 75250.0},
                        {"coin": "vntl:SPACEX", "side": "Short", "positionValue": 2500.0},
                    ],
                },
                {
                    "alias": "main-2",
                    "address": "0x2222222222222222222222222222222222222222",
                    "positions": [{"coin": "xyz:NVDA", "side": "Long", "positionValue": 25000.0}],
                },
            ],
        }

        message = self.service.build_positions_message(dashboard)
        self.assertIn("- No open positions", message)
        self.assertNotIn("NVDA long", message)
        self.assertNotIn("SPACEX short", message)
        self.assertNotIn("xyz:NVDA", message)

    def test_build_positions_message_shows_empty_sections_when_category_has_no_positions(self) -> None:
        dashboard = {
            "generatedAt": "2026-04-09T06:00:00Z",
            "wallets": [
                {
                    "alias": "main-1",
                    "address": "0x1111111111111111111111111111111111111111",
                    "positions": [{"coin": "BTC", "side": "Long", "positionValue": 550000.0}],
                },
                {
                    "alias": "main-2",
                    "address": "0x2222222222222222222222222222222222222222",
                    "positions": [{"coin": "BTC", "side": "Long", "positionValue": 550000.0}],
                },
            ],
        }

        message = self.service.build_positions_message(dashboard)
        self.assertIn("Commodities:\n- None", message)
        self.assertIn("Stocks / indices:\n- None", message)
        self.assertNotIn("HIP-3 positions:", message)

    def test_build_positions_message_supports_raw_commodity_and_index_symbols(self) -> None:
        dashboard = {
            "generatedAt": "2026-04-09T06:00:00Z",
            "wallets": [
                {
                    "alias": "main-1",
                    "address": "0x1111111111111111111111111111111111111111",
                    "positions": [
                        {"coin": "CL", "side": "Long", "positionValue": 323450.0},
                        {"coin": "SP500", "side": "Long", "positionValue": 290950.0},
                        {"coin": "XYZ100", "side": "Long", "positionValue": 914001.24},
                        {"coin": "SILVER", "side": "Short", "positionValue": 576938.0},
                    ],
                },
                {
                    "alias": "main-2",
                    "address": "0x2222222222222222222222222222222222222222",
                    "positions": [
                        {"coin": "CL", "side": "Long", "positionValue": 300000.0},
                        {"coin": "SP500", "side": "Long", "positionValue": 300000.0},
                    ],
                },
            ],
        }

        message = self.service.build_positions_message(dashboard)
        self.assertIn("Commodities:", message)
        self.assertIn("OIL long (2 wallets, 2 positions, $623K)", message)
        self.assertNotIn("SILVER short", message)
        self.assertIn("Stocks / indices:", message)
        self.assertNotIn("XYZ100 long", message)
        self.assertIn("SP500 long (2 wallets, 2 positions, $591K)", message)

    def test_check_alerts_ignores_hip3_only_changes(self) -> None:
        previous_summary = {
            "overallBias": "mixed",
            "consensus": [],
            "hip3Consensus": [{"coin": "@OLD", "side": "long", "walletCount": 3, "totalValue": 100.0}],
        }
        current_summary = {
            "overallBias": "mixed",
            "consensus": [],
            "hip3Consensus": [{"coin": "@NEW", "side": "long", "walletCount": 3, "totalValue": 200.0}],
        }

        with patch("server.load_json_file", return_value={"config": {"enabled": True, "botToken": "token", "chatId": "chat"}, "state": {"summary": previous_summary}}), patch(
            "server.save_json_file"
        ), patch.object(self.service, "dashboard", return_value={"wallets": []}), patch.object(
            self.service, "build_sentiment_summary", return_value=current_summary
        ), patch.object(self.service, "send_telegram_message") as send_telegram_message:
            result = self.service.check_alerts(send_notification=True)

        self.assertFalse(result["shouldNotify"])
        self.assertFalse(result["sent"])
        self.assertEqual(result["changes"]["hip3Added"], [])
        self.assertEqual(result["changes"]["hip3Removed"], [])
        send_telegram_message.assert_not_called()

    def test_check_alerts_notifies_on_new_large_positions(self) -> None:
        previous_summary = {
            "overallBias": "mixed",
            "consensus": [],
            "hip3Consensus": [],
        }
        current_summary = {
            "overallBias": "mixed",
            "consensus": [],
            "hip3Consensus": [],
        }
        dashboard = {
            "wallets": [
                {
                    "address": "0x1111111111111111111111111111111111111111",
                    "alias": "Trader One",
                    "positions": [
                        {"coin": "BTC", "side": "Long", "positionValue": 750000.0, "size": 10.0, "entryPx": 75000.0},
                    ],
                }
            ]
        }

        with patch("server.load_json_file", return_value={"config": {"enabled": True, "botToken": "token", "chatId": "chat"}, "state": {"summary": previous_summary, "largePositions": {}}}), patch(
            "server.save_json_file"
        ), patch.object(self.service, "dashboard", return_value=dashboard), patch.object(
            self.service, "build_sentiment_summary", return_value=current_summary
        ), patch.object(self.service, "send_telegram_message") as send_telegram_message:
            result = self.service.check_alerts(send_notification=True)

        self.assertTrue(result["shouldNotify"])
        self.assertTrue(result["sent"])
        self.assertEqual(len(result["changes"]["newLargePositions"]), 1)
        self.assertEqual(result["changes"]["newLargePositions"][0]["coin"], "BTC")
        sent_message = send_telegram_message.call_args.args[2]
        self.assertIn("Open >$500K", sent_message)
        self.assertIn("Trader One: BTC long $750K sz 10 @$75,000", sent_message)

    def test_check_alerts_notifies_on_closed_large_positions(self) -> None:
        previous_summary = {
            "overallBias": "mixed",
            "consensus": [],
            "hip3Consensus": [],
        }
        current_summary = {
            "overallBias": "mixed",
            "consensus": [],
            "hip3Consensus": [],
        }
        previous_positions = {
            "0x1111111111111111111111111111111111111111:ETH:short": {
                "address": "0x1111111111111111111111111111111111111111",
                "alias": "Trader One",
                "coin": "ETH",
                "side": "short",
                "totalValue": 900000.0,
                "totalSize": 300.0,
            }
        }
        dashboard = {"wallets": [{"address": "0x1111111111111111111111111111111111111111", "alias": "Trader One", "positions": []}]}

        with patch("server.load_json_file", return_value={"config": {"enabled": True, "botToken": "token", "chatId": "chat"}, "state": {"summary": previous_summary, "largePositions": previous_positions}}), patch(
            "server.save_json_file"
        ), patch.object(self.service, "dashboard", return_value=dashboard), patch.object(
            self.service, "build_sentiment_summary", return_value=current_summary
        ), patch.object(self.service, "send_telegram_message") as send_telegram_message:
            result = self.service.check_alerts(send_notification=True)

        self.assertTrue(result["shouldNotify"])
        self.assertTrue(result["sent"])
        self.assertEqual(len(result["changes"]["closedLargePositions"]), 1)
        sent_message = send_telegram_message.call_args.args[2]
        self.assertIn("Closed >$500K", sent_message)
        self.assertIn("Trader One: ETH short $900K sz 300", sent_message)

    def test_send_hourly_update_syncs_alert_baseline(self) -> None:
        summary = {
            "overallBias": "mixed",
            "consensus": [{"coin": "BTC", "side": "long", "walletCount": 8, "totalValue": 75_000_000.0}],
            "hip3Consensus": [],
        }
        dashboard = {
            "wallets": [
                {
                    "address": "0x69906b0ed626ca01a4b7c001e5711e5714ccf207",
                    "alias": "Trader One",
                    "positions": [
                        {"coin": "BTC", "side": "Long", "positionValue": 807800.0},
                    ],
                }
            ]
        }

        with patch(
            "server.load_json_file",
            return_value={
                "config": {"enabled": True},
                "state": {
                    "summary": {
                        "overallBias": "mixed",
                        "consensus": [{"coin": "BTC", "side": "long", "walletCount": 7, "totalValue": 70_000_000.0}],
                        "hip3Consensus": [],
                    }
                },
            },
        ), patch("server.save_json_file") as save_json_file, patch.object(
            self.service, "dashboard", return_value=dashboard
        ), patch.object(
            self.service, "build_sentiment_summary", return_value=summary
        ), patch.object(
            self.service, "send_telegram_message"
        ) as send_telegram_message:
            result = self.service.send_hourly_update(3, "token", "chat")

        self.assertTrue(result["sent"])
        self.assertTrue(result["positionAlertSent"])
        self.assertEqual(send_telegram_message.call_count, 2)
        hourly_message = send_telegram_message.call_args_list[0].args[2]
        self.assertNotIn("Wallet ranks by 7D hit rate + PnL", hourly_message)
        alert_message = send_telegram_message.call_args_list[1].args[2]
        self.assertIn("Open >$500K", alert_message)
        self.assertIn("Trader One: BTC long $808K", alert_message)
        saved_state = save_json_file.call_args.args[1]["state"]
        self.assertEqual(saved_state["summary"]["consensus"][0]["walletCount"], 8)
        self.assertIn("0x69906b0ed626ca01a4b7c001e5711e5714ccf207:BTC:long", saved_state["largePositions"])
        self.assertIn("lastHourlySyncedAt", saved_state)

    def test_send_hourly_update_does_not_resync_failed_large_position_alert(self) -> None:
        summary = {
            "overallBias": "mixed",
            "consensus": [],
            "hip3Consensus": [],
        }
        dashboard = {
            "wallets": [
                {
                    "address": "0x1111111111111111111111111111111111111111",
                    "alias": "Trader One",
                    "positions": [
                        {"coin": "ETH", "side": "Short", "positionValue": 900000.0, "size": 300.0},
                    ],
                }
            ]
        }
        previous_positions = {
            "0x2222222222222222222222222222222222222222:BTC:long": {
                "address": "0x2222222222222222222222222222222222222222",
                "alias": "Old Trader",
                "coin": "BTC",
                "side": "long",
                "totalValue": 700000.0,
                "totalSize": 7.0,
            }
        }

        with patch(
            "server.load_json_file",
            return_value={"config": {"enabled": True}, "state": {"summary": summary, "largePositions": previous_positions}},
        ), patch("server.save_json_file") as save_json_file, patch.object(
            self.service, "dashboard", return_value=dashboard
        ), patch.object(
            self.service, "build_sentiment_summary", return_value=summary
        ), patch.object(
            self.service,
            "send_telegram_message",
            side_effect=[None, ValueError("telegram down")],
        ):
            result = self.service.send_hourly_update(3, "token", "chat")

        self.assertTrue(result["sent"])
        self.assertFalse(result["positionAlertSent"])
        self.assertIn("telegram down", result["positionAlertError"])
        saved_state = save_json_file.call_args.args[1]["state"]
        self.assertEqual(saved_state["largePositions"], previous_positions)

    def test_check_alerts_notifies_on_large_position_increases(self) -> None:
        previous_summary = {
            "overallBias": "mixed",
            "consensus": [],
            "hip3Consensus": [],
        }
        current_summary = {
            "overallBias": "mixed",
            "consensus": [],
            "hip3Consensus": [],
        }
        previous_positions = {
            "0x1111111111111111111111111111111111111111:BTC:long": {
                "address": "0x1111111111111111111111111111111111111111",
                "alias": "Trader One",
                "coin": "BTC",
                "side": "long",
                "totalValue": 600000.0,
                "totalSize": 10.0,
                "entryPx": 75000.0,
            }
        }
        dashboard = {
            "wallets": [
                {
                    "address": "0x1111111111111111111111111111111111111111",
                    "alias": "Trader One",
                    "positions": [
                        {"coin": "BTC", "side": "Long", "positionValue": 1200000.0, "size": 20.0, "entryPx": 78000.0},
                    ],
                }
            ]
        }

        with patch("server.load_json_file", return_value={"config": {"enabled": True, "botToken": "token", "chatId": "chat"}, "state": {"summary": previous_summary, "largePositions": previous_positions}}), patch(
            "server.save_json_file"
        ), patch.object(self.service, "dashboard", return_value=dashboard), patch.object(
            self.service, "build_sentiment_summary", return_value=current_summary
        ), patch.object(self.service, "send_telegram_message") as send_telegram_message:
            result = self.service.check_alerts(send_notification=True)

        self.assertTrue(result["shouldNotify"])
        self.assertTrue(result["sent"])
        self.assertEqual(len(result["changes"]["increasedLargePositions"]), 1)
        sent_message = send_telegram_message.call_args.args[2]
        self.assertIn("Added >$500K", sent_message)
        self.assertIn("Trader One: BTC long $600K->$1.2M (+$600K +10 add @$60,000)", sent_message)
        self.assertNotIn("@$78,000", sent_message)

    def test_large_position_increases_notify_on_big_size_add_even_if_pct_small(self) -> None:
        previous = {
            "wallet:BTC:long": {
                "address": "wallet",
                "alias": "wallet",
                "coin": "BTC",
                "side": "long",
                "totalValue": 8000000.0,
                "totalSize": 100.0,
            }
        }
        current = {
            "wallet:BTC:long": {
                "address": "wallet",
                "alias": "wallet",
                "coin": "BTC",
                "side": "long",
                "totalValue": 9900000.0,
                "totalSize": 110.0,
            }
        }

        added, increased, closed = self.service.summarize_large_position_changes(previous, current)

        self.assertEqual(added, [])
        self.assertEqual(len(increased), 1)
        self.assertEqual(increased[0]["sizeIncrease"], 10.0)
        self.assertEqual(increased[0]["addPrice"], 90000.0)
        self.assertEqual(closed, [])

    def test_large_position_increases_ignore_small_drift(self) -> None:
        previous = {
            "wallet:BTC:long": {
                "address": "wallet",
                "alias": "wallet",
                "coin": "BTC",
                "side": "long",
                "totalValue": 1000000.0,
                "totalSize": 10.0,
            }
        }
        current = {
            "wallet:BTC:long": {
                "address": "wallet",
                "alias": "wallet",
                "coin": "BTC",
                "side": "long",
                "totalValue": 1600000.0,
                "totalSize": 10.0,
            }
        }

        added, increased, closed = self.service.summarize_large_position_changes(previous, current)

        self.assertEqual(added, [])
        self.assertEqual(increased, [])
        self.assertEqual(closed, [])


class HyperliquidClientTests(unittest.TestCase):
    def test_merge_all_dexs_clearinghouse_state_combines_positions_and_balances(self) -> None:
        client = HyperliquidClient()
        merged = client.merge_all_dexs_clearinghouse_state(
            "0x1111111111111111111111111111111111111111",
            {
                "user": "0x1111111111111111111111111111111111111111",
                "clearinghouseStates": [
                    [
                        "",
                        {
                            "marginSummary": {
                                "accountValue": "2139672.7633750001",
                                "totalNtlPos": "0.0",
                                "totalRawUsd": "2139672.7633750001",
                                "totalMarginUsed": "0.0",
                            },
                            "crossMarginSummary": {
                                "accountValue": "2139672.7633750001",
                                "totalNtlPos": "0.0",
                                "totalRawUsd": "2139672.7633750001",
                                "totalMarginUsed": "0.0",
                            },
                            "crossMaintenanceMarginUsed": "0.0",
                            "withdrawable": "2005860.2633750001",
                            "assetPositions": [],
                            "time": 1775742877177,
                        },
                    ],
                    [
                        "xyz",
                        {
                            "marginSummary": {
                                "accountValue": "4184888.718471",
                                "totalNtlPos": "19380689.7974",
                                "totalRawUsd": "4184888.718471",
                                "totalMarginUsed": "2539707.789871",
                            },
                            "crossMarginSummary": {
                                "accountValue": "4184888.718471",
                                "totalNtlPos": "19380689.7974",
                                "totalRawUsd": "4184888.718471",
                                "totalMarginUsed": "2539707.789871",
                            },
                            "crossMaintenanceMarginUsed": "0.0",
                            "withdrawable": "1645180.9286",
                            "assetPositions": [
                                {
                                    "type": "oneWay",
                                    "position": {
                                        "coin": "xyz:XYZ100",
                                        "szi": "600.2214",
                                        "positionValue": "14910099.7974",
                                    },
                                },
                                {
                                    "type": "oneWay",
                                    "position": {
                                        "coin": "xyz:CL",
                                        "szi": "25000.0",
                                        "positionValue": "2448275.0",
                                    },
                                },
                            ],
                            "time": 1775742878000,
                        },
                    ],
                ],
            },
        )

        self.assertEqual(merged["user"], "0x1111111111111111111111111111111111111111")
        self.assertEqual(len(merged["assetPositions"]), 2)
        self.assertEqual(merged["assetPositions"][0]["dex"], "xyz")
        self.assertEqual(merged["assetPositions"][0]["position"]["coin"], "xyz:XYZ100")
        self.assertAlmostEqual(float(merged["marginSummary"]["accountValue"]), 6324561.481846001)
        self.assertAlmostEqual(float(merged["marginSummary"]["totalNtlPos"]), 19380689.7974)
        self.assertAlmostEqual(float(merged["withdrawable"]), 3651041.1919750003)
        self.assertEqual(merged["time"], 1775742878000)


if __name__ == "__main__":
    unittest.main()
