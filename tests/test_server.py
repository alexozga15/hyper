import unittest
from unittest.mock import patch
from pathlib import Path
from typing import Any

from coinmarketman import CoinMarketManApiError
from server import (
    ALERTS_FILE,
    ELITE_WALLET_OVERRIDES,
    HyperliquidClient,
    WalletStore,
    WalletTrackerService,
    build_wallet_quality_rank,
    classify_profitability,
    classify_wallet_size,
    now_iso,
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

    def test_wallet_quality_rank_blends_30d_base_with_capped_7d_weight(self) -> None:
        cold_week = build_wallet_quality_rank(
            0,
            20,
            -100_000,
            100_000,
            hit_rate_30d=70,
            closed_trade_count_30d=30,
            pnl_30d=30_000,
            gross_profit_30d=50_000,
            gross_loss_30d=10_000,
            max_drawdown_pct=5,
        )
        hot_but_thin_week = build_wallet_quality_rank(
            100,
            2,
            100_000,
            100_000,
            hit_rate_30d=70,
            closed_trade_count_30d=30,
            pnl_30d=30_000,
            gross_profit_30d=50_000,
            gross_loss_30d=10_000,
            max_drawdown_pct=5,
        )

        self.assertLessEqual(
            abs(cold_week["convictionWeightScore"] - cold_week["quality30dScore"]),
            20.0,
        )
        self.assertEqual(hot_but_thin_week["convictionWeight7dShare"], 0.12)
        self.assertLess(
            abs(hot_but_thin_week["convictionWeightScore"] - hot_but_thin_week["quality30dScore"]),
            5.0,
        )

        hot_active_week = build_wallet_quality_rank(
            100,
            5,
            100_000,
            100_000,
            hit_rate_30d=70,
            closed_trade_count_30d=30,
            pnl_30d=30_000,
            gross_profit_30d=50_000,
            gross_loss_30d=10_000,
            max_drawdown_pct=5,
        )
        self.assertEqual(hot_active_week["convictionWeight7dShare"], 0.3)
        self.assertGreater(hot_active_week["convictionWeightScore"], hot_active_week["quality30dScore"])
        self.assertLessEqual(
            hot_active_week["convictionWeightScore"] - hot_active_week["quality30dScore"],
            20.0,
        )

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
        self.assertEqual(summary["overallBias"], "mixed")
        self.assertEqual(summary["longWalletCount"], 3)
        self.assertEqual(summary["shortWalletCount"], 3)
        self.assertEqual(len(summary["consensus"]), 2)
        self.assertEqual(summary["consensus"][0]["coin"], "BTC")
        self.assertEqual(summary["consensus"][0]["walletCount"], 3)
        self.assertEqual(len(summary["hip3Consensus"]), 1)
        self.assertEqual(summary["hip3Consensus"][0]["coin"], "@123")
        self.assertEqual(summary["hip3Consensus"][0]["walletCount"], 3)

    def test_loracle_hype_positions_are_excluded_from_sentiment_counts(self) -> None:
        snapshots = [
            {
                "address": "0x8def9f50456c6c4e37fa5d3d57f108ed23992dae",
                "alias": "Loracle",
                "positions": [
                    {"coin": "HYPE", "side": "Short", "positionValue": 10_000_000},
                    {"coin": "BTC", "side": "Long", "positionValue": 1_000_000},
                ],
            },
            {
                "address": "0x2222222222222222222222222222222222222222",
                "alias": "Two",
                "positions": [
                    {"coin": "HYPE", "side": "Short", "positionValue": 2_000_000},
                    {"coin": "BTC", "side": "Long", "positionValue": 1_000_000},
                ],
            },
            {
                "address": "0x3333333333333333333333333333333333333333",
                "alias": "Three",
                "positions": [
                    {"coin": "HYPE", "side": "Short", "positionValue": 2_000_000},
                    {"coin": "BTC", "side": "Long", "positionValue": 1_000_000},
                ],
            },
        ]

        summary = self.service.build_sentiment_summary(snapshots, min_wallets=3)

        self.assertEqual([item["coin"] for item in summary["consensus"]], ["BTC"])
        self.assertEqual(summary["consensus"][0]["walletCount"], 3)
        self.assertEqual(summary["longWalletCount"], 3)
        self.assertEqual(summary["shortWalletCount"], 2)

    def test_large_losing_positions_are_excluded_from_sentiment_counts(self) -> None:
        snapshots = [
            {
                "address": "0x1111111111111111111111111111111111111111",
                "positions": [
                    {"coin": "BTC", "side": "Long", "positionValue": 2_000_000.0, "unrealizedPnl": -1_200_000.0}
                ],
            },
            {
                "address": "0x2222222222222222222222222222222222222222",
                "positions": [
                    {"coin": "BTC", "side": "Long", "positionValue": 1_500_000.0, "unrealizedPnl": -900_000.0}
                ],
            },
            {
                "address": "0x3333333333333333333333333333333333333333",
                "positions": [
                    {"coin": "BTC", "side": "Long", "positionValue": 1_000_000.0, "unrealizedPnl": 100_000.0}
                ],
            },
        ]

        summary = self.service.build_sentiment_summary(snapshots, min_wallets=2)

        self.assertEqual(summary["longWalletCount"], 2)
        self.assertEqual(summary["consensus"][0]["walletCount"], 2)
        self.assertEqual(summary["consensus"][0]["totalValue"], 2_500_000.0)

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
            "consensus": [
                {"coin": "BTC", "side": "long", "walletCount": 3, "totalValue": 12345.0, "convictionScore": 84.0},
                {"coin": "OIL", "side": "short", "walletCount": 3, "totalValue": 789.0, "convictionScore": 72.0},
                {"coin": "EWY", "side": "long", "walletCount": 3, "totalValue": 456.0, "convictionScore": 68.0},
            ],
            "hip3Consensus": [{"coin": "@PUMP-1", "side": "short", "walletCount": 3, "totalValue": 456.0}],
        }

        message = self.service.build_summary_message(summary, min_wallets=3)
        self.assertIn("Current wallet sentiment", message)
        self.assertIn("BTC long (3 wallets, conviction 84/100)", message)
        self.assertIn("Commodities consensus:", message)
        self.assertIn("OIL short (3 wallets, conviction 72/100)", message)
        self.assertIn("Stocks / indices consensus:", message)
        self.assertIn("EWY long (3 wallets, conviction 68/100)", message)
        self.assertNotIn("$12,345", message)
        self.assertNotIn("HIP-3 consensus:", message)
        self.assertNotIn("@PUMP-1 short (3 wallets, $456)", message)

        hip3_message = self.service.build_summary_message(summary, min_wallets=3, include_consensus=False, include_hip3=True)
        self.assertIn("HIP-3 consensus:", hip3_message)
        self.assertIn("@PUMP-1 short (3 wallets)", hip3_message)
        self.assertNotIn("$456", hip3_message)

    def test_build_sentiment_summary_assigns_conviction_scores(self) -> None:
        snapshots = [
            {
                "address": "0x1111111111111111111111111111111111111111",
                "alias": "One",
                "positions": [
                    {"coin": "BTC", "side": "Long", "positionValue": 500000},
                    {"coin": "ETH", "side": "Long", "positionValue": 20_000_000},
                ],
            },
            {
                "address": "0x2222222222222222222222222222222222222222",
                "alias": "Two",
                "positions": [
                    {"coin": "BTC", "side": "Long", "positionValue": 400000},
                    {"coin": "ETH", "side": "Long", "positionValue": 10_000_000},
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

    def test_build_sentiment_summary_uses_net_wallet_conviction(self) -> None:
        now_ms = 1_700_000_000_000
        snapshots = [
            {
                "address": "0x1111111111111111111111111111111111111111",
                "positions": [
                    {"coin": "BTC", "side": "Long", "positionValue": 1_000_000.0},
                    {"coin": "BNB", "side": "Short", "positionValue": 1_000_000.0},
                ],
                "recentFills": [
                    {
                        "coin": "BNB",
                        "direction": "Increase Short",
                        "price": 700.0,
                        "size": 715.0,
                        "time": now_ms - 60_000,
                    }
                ],
            },
            {
                "address": "0x2222222222222222222222222222222222222222",
                "positions": [
                    {"coin": "BTC", "side": "Long", "positionValue": 1_000_000.0},
                    {"coin": "BNB", "side": "Short", "positionValue": 1_000_000.0},
                ],
            },
            {
                "address": "0x3333333333333333333333333333333333333333",
                "positions": [
                    {"coin": "BTC", "side": "Long", "positionValue": 1_000_000.0},
                    {"coin": "BNB", "side": "Short", "positionValue": 1_000_000.0},
                ],
            },
            {
                "address": "0x4444444444444444444444444444444444444444",
                "positions": [
                    {"coin": "BTC", "side": "Short", "positionValue": 1_000_000.0},
                    {"coin": "BNB", "side": "Short", "positionValue": 1_000_000.0},
                ],
            },
            {
                "address": "0x5555555555555555555555555555555555555555",
                "positions": [
                    {"coin": "BTC", "side": "Short", "positionValue": 1_000_000.0},
                ],
            },
            {
                "address": "0x6666666666666666666666666666666666666666",
                "positions": [
                    {"coin": "BTC", "side": "Short", "positionValue": 1_000_000.0},
                ],
            },
        ]

        with patch("server.current_time_ms", return_value=now_ms):
            summary = self.service.build_sentiment_summary(snapshots, min_wallets=3)
        consensus_by_key = {f'{item["coin"]}:{item["side"]}': item for item in summary["consensus"]}

        self.assertEqual(consensus_by_key["BTC:long"]["netWalletCount"], 0)
        self.assertEqual(consensus_by_key["BTC:short"]["netWalletCount"], 0)
        self.assertEqual(consensus_by_key["BTC:long"]["convictionScore"], 0.0)
        self.assertEqual(consensus_by_key["BTC:short"]["convictionScore"], 0.0)
        self.assertEqual(consensus_by_key["BNB:short"]["netWalletCount"], 4)
        self.assertEqual(consensus_by_key["BNB:short"]["convictionScore"], 100.0)
        self.assertEqual(summary["signals"][0]["coin"], "BNB")
        self.assertNotIn("BTC", {item["coin"] for item in summary["signals"]})

    def test_net_wallet_conviction_counts_below_threshold_opposition(self) -> None:
        snapshots = [
            {
                "address": "0x1111111111111111111111111111111111111111",
                "positions": [{"coin": "SOL", "side": "Short", "positionValue": 1_000_000.0}],
            },
            {
                "address": "0x2222222222222222222222222222222222222222",
                "positions": [{"coin": "SOL", "side": "Short", "positionValue": 1_000_000.0}],
            },
            {
                "address": "0x3333333333333333333333333333333333333333",
                "positions": [{"coin": "SOL", "side": "Short", "positionValue": 1_000_000.0}],
            },
            {
                "address": "0x4444444444444444444444444444444444444444",
                "positions": [{"coin": "SOL", "side": "Long", "positionValue": 1_000_000.0}],
            },
        ]

        summary = self.service.build_sentiment_summary(snapshots, min_wallets=3)

        self.assertEqual(len(summary["consensus"]), 1)
        self.assertEqual(summary["consensus"][0]["coin"], "SOL")
        self.assertEqual(summary["consensus"][0]["side"], "short")
        self.assertEqual(summary["consensus"][0]["oppositeWalletCount"], 1)
        self.assertEqual(summary["consensus"][0]["netWalletCount"], 2)

    def test_quality_weighted_conviction_can_break_raw_wallet_ties(self) -> None:
        snapshots = [
            {
                "address": "0x1111111111111111111111111111111111111111",
                "recentWinRateRank": {"score": 100.0, "label": "Elite"},
                "positions": [{"coin": "BTC", "side": "Long", "positionValue": 1_000_000.0}],
            },
            {
                "address": "0x2222222222222222222222222222222222222222",
                "recentWinRateRank": {"score": 100.0, "label": "Elite"},
                "positions": [{"coin": "BTC", "side": "Long", "positionValue": 1_000_000.0}],
            },
            {
                "address": "0x3333333333333333333333333333333333333333",
                "recentWinRateRank": {"score": 100.0, "label": "Elite"},
                "positions": [{"coin": "BTC", "side": "Long", "positionValue": 1_000_000.0}],
            },
            {
                "address": "0x4444444444444444444444444444444444444444",
                "recentWinRateRank": {"score": 45.0, "label": "Cold"},
                "positions": [{"coin": "BTC", "side": "Short", "positionValue": 1_000_000.0}],
            },
            {
                "address": "0x5555555555555555555555555555555555555555",
                "recentWinRateRank": {"score": 45.0, "label": "Cold"},
                "positions": [{"coin": "BTC", "side": "Short", "positionValue": 1_000_000.0}],
            },
            {
                "address": "0x6666666666666666666666666666666666666666",
                "recentWinRateRank": {"score": 45.0, "label": "Cold"},
                "positions": [{"coin": "BTC", "side": "Short", "positionValue": 1_000_000.0}],
            },
        ]

        summary = self.service.build_sentiment_summary(snapshots, min_wallets=3)
        consensus_by_key = {f'{item["coin"]}:{item["side"]}': item for item in summary["consensus"]}

        self.assertEqual(consensus_by_key["BTC:long"]["netWalletCount"], 0)
        self.assertGreater(consensus_by_key["BTC:long"]["netWeightedWalletCount"], 0)
        self.assertEqual(consensus_by_key["BTC:long"]["convictionScore"], 100.0)
        self.assertEqual(consensus_by_key["BTC:short"]["convictionScore"], 0.0)
        self.assertEqual(summary["signals"], [])

    def test_top_ten_wallets_get_extra_conviction_weight(self) -> None:
        snapshots = []
        for index in range(1, 4):
            snapshots.append(
                {
                    "address": f"0x{index:040x}",
                    "recentWinRateRank": {"score": 90.0, "label": "Strong"},
                    "realizedPnl30d": 100_000.0,
                    "positions": [{"coin": "BTC", "side": "Long", "positionValue": 1_000_000.0}],
                }
            )
        for index in range(4, 11):
            snapshots.append(
                {
                    "address": f"0x{index:040x}",
                    "recentWinRateRank": {"score": 80.0, "label": "Strong"},
                    "realizedPnl30d": 50_000.0,
                    "positions": [],
                }
            )
        for index in range(11, 14):
            snapshots.append(
                {
                    "address": f"0x{index:040x}",
                    "recentWinRateRank": {"score": 40.0, "label": "Cold"},
                    "realizedPnl30d": -10_000.0,
                    "positions": [{"coin": "BTC", "side": "Short", "positionValue": 1_000_000.0}],
                }
            )

        summary = self.service.build_sentiment_summary(snapshots, min_wallets=3)
        consensus_by_key = {f'{item["coin"]}:{item["side"]}': item for item in summary["consensus"]}

        self.assertEqual(consensus_by_key["BTC:long"]["netWalletCount"], 0)
        self.assertGreater(consensus_by_key["BTC:long"]["netWeightedWalletCount"], 0)
        self.assertEqual(consensus_by_key["BTC:long"]["convictionScore"], 100.0)
        self.assertEqual(consensus_by_key["BTC:short"]["convictionScore"], 0.0)

    def test_monthly_top_conviction_cohort_reuses_same_month(self) -> None:
        wallets = [
            {
                "address": f"0x{index:040x}",
                "recentWinRateRank": {"score": 10.0, "label": "Cold"},
                "realizedPnl30d": 0.0,
                "positions": [],
            }
            for index in range(1, 11)
        ]
        wallets.append(
            {
                "address": "0x0000000000000000000000000000000000000011",
                "recentWinRateRank": {"score": 100.0, "label": "Elite"},
                "realizedPnl30d": 1_000_000.0,
                "positions": [],
            }
        )
        stored_addresses = [f"0x{index:040x}" for index in range(1, 11)]
        state = {"topConvictionWallets": {"month": "2026-06", "addresses": stored_addresses}}

        selected, cohort = self.service.resolve_monthly_top_conviction_cohort(
            wallets,
            state,
            month_key="2026-06",
            limit=10,
        )

        self.assertEqual(selected, set(stored_addresses))
        self.assertEqual(cohort["addresses"], stored_addresses)
        self.assertNotIn("0x0000000000000000000000000000000000000011", selected)

    def test_monthly_top_conviction_cohort_refreshes_new_month(self) -> None:
        wallets = [
            {
                "address": f"0x{index:040x}",
                "recentWinRateRank": {"score": 10.0, "label": "Cold"},
                "realizedPnl30d": 0.0,
                "positions": [],
            }
            for index in range(1, 5)
        ]
        wallets.append(
            {
                "address": "0x0000000000000000000000000000000000000011",
                "recentWinRateRank": {"score": 100.0, "label": "Elite"},
                "realizedPnl30d": 1_000_000.0,
                "positions": [],
            }
        )
        state = {
            "topConvictionWallets": {
                "month": "2026-05",
                "addresses": [f"0x{index:040x}" for index in range(1, 4)],
            }
        }

        selected, cohort = self.service.resolve_monthly_top_conviction_cohort(
            wallets,
            state,
            month_key="2026-06",
            limit=3,
        )

        self.assertIn("0x0000000000000000000000000000000000000011", selected)
        self.assertEqual(cohort["month"], "2026-06")
        self.assertEqual(len(cohort["addresses"]), 3)

    def test_monthly_top_conviction_cohort_demotes_toxic_wallet(self) -> None:
        toxic_address = "0x0000000000000000000000000000000000000099"
        wallets = [
            {
                "address": toxic_address,
                "recentWinRateRank": {"score": 100.0, "label": "Elite"},
                "realizedPnl30d": -600_000.0,
                "positions": [],
            },
            {
                "address": "0x0000000000000000000000000000000000000001",
                "recentWinRateRank": {"score": 80.0, "label": "Strong"},
                "realizedPnl30d": 100_000.0,
                "positions": [],
            },
            {
                "address": "0x0000000000000000000000000000000000000002",
                "recentWinRateRank": {"score": 70.0, "label": "Strong"},
                "realizedPnl30d": 50_000.0,
                "positions": [],
            },
            {
                "address": "0x0000000000000000000000000000000000000003",
                "recentWinRateRank": {"score": 60.0, "label": "Strong"},
                "realizedPnl30d": 25_000.0,
                "positions": [],
            },
        ]
        state = {
            "topConvictionWallets": {
                "month": "2026-06",
                "addresses": [
                    toxic_address,
                    "0x0000000000000000000000000000000000000001",
                    "0x0000000000000000000000000000000000000002",
                ],
            }
        }

        selected, cohort = self.service.resolve_monthly_top_conviction_cohort(
            wallets,
            state,
            month_key="2026-06",
            limit=3,
        )

        self.assertNotIn(toxic_address, selected)
        self.assertIn(toxic_address, cohort["demoted"])
        self.assertEqual(len(selected), 3)

    def test_stale_positions_need_recent_large_add_for_conviction(self) -> None:
        now_ms = 1_700_000_000_000
        snapshots = [
            {
                "address": "0x1111111111111111111111111111111111111111",
                "positions": [{"coin": "BTC", "side": "Long", "positionValue": 2_000_000.0}],
                "holdingOnly30d": True,
                "recentFills": [],
            },
            {
                "address": "0x2222222222222222222222222222222222222222",
                "positions": [{"coin": "ETH", "side": "Long", "positionValue": 2_000_000.0}],
                "holdingOnly30d": True,
                "recentFills": [
                    {
                        "coin": "ETH",
                        "direction": "Increase Long",
                        "price": 4_000.0,
                        "size": 90.0,
                        "time": now_ms - 24 * 60 * 60 * 1000,
                    }
                ],
            },
            {
                "address": "0x3333333333333333333333333333333333333333",
                "positions": [{"coin": "SOL", "side": "Long", "positionValue": 2_000_000.0}],
                "holdingOnly30d": True,
                "recentFills": [
                    {
                        "coin": "SOL",
                        "direction": "Increase Long",
                        "price": 200.0,
                        "size": 3_000.0,
                        "time": now_ms - 10 * 24 * 60 * 60 * 1000,
                    }
                ],
            },
            {
                "address": "0x4444444444444444444444444444444444444444",
                "positions": [{"coin": "BNB", "side": "Long", "positionValue": 2_000_000.0}],
                "holdingOnly30d": True,
                "recentFills": [
                    {
                        "coin": "BNB",
                        "direction": "Increase Long",
                        "price": 700.0,
                        "size": 1_000.0,
                        "time": now_ms - 2 * 24 * 60 * 60 * 1000,
                    }
                ],
            },
            {
                "address": "0x5555555555555555555555555555555555555555",
                "positions": [{"coin": "HYPE", "side": "Long", "positionValue": 2_000_000.0}],
                "holdingOnly30d": False,
                "recentFills": [
                    {
                        "coin": "HYPE",
                        "direction": "Open Long",
                        "price": 50.0,
                        "size": 1_000.0,
                        "time": now_ms - 20 * 24 * 60 * 60 * 1000,
                    }
                ],
            },
            {
                "address": "0x6666666666666666666666666666666666666666",
                "positions": [
                    {"coin": "PAXG", "side": "Long", "positionValue": 2_000_000.0, "unrealizedPnl": 1_200_000.0}
                ],
                "holdingOnly30d": True,
                "recentFills": [],
            },
            {
                "address": "0x7777777777777777777777777777777777777777",
                "positions": [{"coin": "TON", "side": "Long", "positionValue": 1_000_000.0}],
                "holdingOnly30d": True,
                "recentFills": [
                    {
                        "coin": "TON",
                        "direction": "Increase Long",
                        "price": 2.5,
                        "size": 100_000.0,
                        "time": now_ms - 2 * 24 * 60 * 60 * 1000,
                    }
                ],
            },
        ]

        with patch("server.current_time_ms", return_value=now_ms):
            summary = self.service.build_sentiment_summary(snapshots, min_wallets=1)

        consensus_keys = {f'{item["coin"]}:{item["side"]}' for item in summary["consensus"]}
        self.assertNotIn("BTC:long", consensus_keys)
        self.assertNotIn("ETH:long", consensus_keys)
        self.assertNotIn("SOL:long", consensus_keys)
        self.assertIn("BNB:long", consensus_keys)
        self.assertIn("HYPE:long", consensus_keys)
        self.assertNotIn("PAXG:long", consensus_keys)
        self.assertIn("TON:long", consensus_keys)

    def test_stale_positions_remain_visible_in_position_groups(self) -> None:
        dashboard = {
            "generatedAt": "2026-04-09T06:00:00Z",
            "wallets": [
                {
                    "address": "0x1111111111111111111111111111111111111111",
                    "positions": [{"coin": "BTC", "side": "Long", "positionValue": 800_000.0}],
                    "recentFills": [],
                },
                {
                    "address": "0x2222222222222222222222222222222222222222",
                    "positions": [{"coin": "BTC", "side": "Long", "positionValue": 800_000.0}],
                    "recentFills": [],
                },
                {
                    "address": "0x3333333333333333333333333333333333333333",
                    "positions": [{"coin": "BTC", "side": "Long", "positionValue": 800_000.0}],
                    "recentFills": [],
                },
            ],
        }

        message = self.service.build_positions_message(dashboard)

        self.assertIn("BTC long (3 wallets, 3 positions, $2,400K", message)

    def test_build_sentiment_summary_emits_high_conviction_signals(self) -> None:
        now_ms = 1_700_000_000_000
        snapshots = [
            {
                "address": "0x1111111111111111111111111111111111111111",
                "alias": "One",
                "positions": [{"coin": "BTC", "side": "Long", "positionValue": 500000}],
                "recentFills": [
                    {
                        "coin": "BTC",
                        "direction": "Increase Long",
                        "price": 50_000.0,
                        "size": 10.0,
                        "time": now_ms - 60_000,
                    }
                ],
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
                "address": "0x6666666666666666666666666666666666666666",
                "alias": "Six",
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

        with patch("server.current_time_ms", return_value=now_ms):
            summary = self.service.build_sentiment_summary(snapshots, min_wallets=2)

        self.assertEqual(summary["signalCount"], 1)
        self.assertEqual(summary["signals"][0]["coin"], "BTC")
        self.assertEqual(summary["signals"][0]["action"], "buy")
        self.assertEqual(summary["signals"][0]["strength"], "extreme")
        self.assertEqual(summary["signals"][0]["convictionScore"], 100.0)
        self.assertGreaterEqual(summary["signals"][0]["probabilityScore"], 70.0)

    def test_build_sentiment_summary_requires_recent_activity_for_signals(self) -> None:
        snapshots = [
            {
                "address": "0x1111111111111111111111111111111111111111",
                "positions": [{"coin": "BTC", "side": "Long", "positionValue": 1_000_000.0}],
            },
            {
                "address": "0x2222222222222222222222222222222222222222",
                "positions": [{"coin": "BTC", "side": "Long", "positionValue": 1_000_000.0}],
            },
            {
                "address": "0x3333333333333333333333333333333333333333",
                "positions": [{"coin": "BTC", "side": "Long", "positionValue": 1_000_000.0}],
            },
            {
                "address": "0x4444444444444444444444444444444444444444",
                "positions": [{"coin": "BTC", "side": "Long", "positionValue": 1_000_000.0}],
            },
        ]

        summary = self.service.build_sentiment_summary(snapshots, min_wallets=4)

        self.assertEqual(summary["consensus"][0]["coin"], "BTC")
        self.assertEqual(summary["signals"], [])
        probability = self.service.signal_probability_score(summary["consensus"][0])
        self.assertIn("no_recent_activity", self.service.signal_rejection_reasons(summary["consensus"][0], probability))

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
        self.assertEqual(changes["changedSignals"][0]["fromProbabilityScore"], 82.0)
        self.assertEqual(changes["changedSignals"][0]["toProbabilityScore"], 95.0)

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

        self.assertIn("Actionable wallet signals", message)
        self.assertIn("1. BUY BTC long (3 wallets, p94/100)", message)
        self.assertNotIn("$1.2M", message)

    def test_build_cmm_signal_summary_scores_cohort_bias(self) -> None:
        class FakeCmmClient:
            token = "token"

            def position_metrics(self, coin: str, segment_id: int, **kwargs: Any) -> dict[str, Any]:
                return {
                    "metrics": [
                        {
                            "createdAt": "2026-06-16T00:00:00Z",
                            "coin": coin,
                            "segmentId": segment_id,
                            "positionCount": 100,
                            "positionCountLong": 15,
                            "totalPositionValue": 10_000_000,
                            "totalPositionValueLong": 1_000_000,
                            "totalUnrealizedPnl": 250_000,
                        }
                    ]
                }

        self.service.cmm_client = FakeCmmClient()

        summary = self.service.build_cmm_signal_summary(coins=["BTC"], segment_ids=[8, 7, 9])

        self.assertTrue(summary["enabled"])
        self.assertEqual(summary["signalCount"], 1)
        self.assertEqual(summary["signals"][0]["coin"], "BTC")
        self.assertEqual(summary["signals"][0]["side"], "short")
        self.assertGreaterEqual(summary["signals"][0]["probabilityScore"], 70)

    def test_build_cmm_signal_summary_uses_heatmap_and_trend_metrics(self) -> None:
        class FakeCmmClient:
            token = "token"

            def positions_heatmap(self, *, opened_within: str) -> list[dict[str, Any]]:
                self.opened_within = opened_within
                return [
                    {
                        "coin": "BTC",
                        "segments": [
                            {
                                "segmentId": 8,
                                "count": 100,
                                "countLong": 10,
                                "totalValue": 10_000_000,
                                "totalLongValue": 1_000_000,
                                "totalShortValue": 9_000_000,
                                "bias": 0.1,
                            },
                            {
                                "segmentId": 7,
                                "positionCount": 80,
                                "positionCountLong": 12,
                                "totalPositionValue": 8_000_000,
                                "totalPositionValueLong": 1_200_000,
                                "totalPositionValueShort": 6_800_000,
                                "bias": 0.15,
                            },
                            {
                                "segmentId": 9,
                                "totalCount": 90,
                                "longCount": 20,
                                "positionValue": 6_000_000,
                                "longValue": 1_400_000,
                                "shortValue": 4_600_000,
                                "bias": 0.23,
                            },
                        ],
                    }
                ]

            def position_metrics(self, coin: str, segment_id: int, **kwargs: Any) -> dict[str, Any]:
                return {
                    "metrics": [
                        {
                            "createdAt": "2026-06-16T00:00:00Z",
                            "positionCount": 100,
                            "positionCountLong": 25,
                            "totalPositionValue": 10_000_000,
                            "totalPositionValueLong": 2_000_000,
                        },
                        {
                            "createdAt": "2026-06-16T01:00:00Z",
                            "positionCount": 100,
                            "positionCountLong": 10,
                            "totalPositionValue": 10_000_000,
                            "totalPositionValueLong": 1_000_000,
                        },
                    ]
                }

        fake_client = FakeCmmClient()
        self.service.cmm_client = fake_client

        with patch.dict("os.environ", {"CMM_TREND_ENRICHMENT": "true"}, clear=False):
            summary = self.service.build_cmm_signal_summary(coins=["BTC"], segment_ids=[8, 7, 9])

        self.assertEqual(fake_client.opened_within, "7d")
        self.assertTrue(summary["enabled"])
        self.assertEqual(summary["signalCount"], 1)
        self.assertEqual(summary["signals"][0]["coin"], "BTC")
        self.assertEqual(summary["signals"][0]["side"], "short")
        self.assertGreaterEqual(summary["signals"][0]["probabilityScore"], 70)
        self.assertGreater(summary["signals"][0]["trendScore"], 0)

    def test_cmm_trend_enrichment_limits_to_top_three(self) -> None:
        class FakeCmmClient:
            token = "token"

            def __init__(self) -> None:
                self.metric_calls: list[tuple[str, int]] = []

            def positions_heatmap(self, *, opened_within: str) -> list[dict[str, Any]]:
                return [
                    {
                        "coin": coin,
                        "segments": [
                            {
                                "segmentId": 8,
                                "count": 100,
                                "countLong": 10,
                                "totalValue": 10_000_000,
                                "totalLongValue": 1_000_000,
                                "totalShortValue": 9_000_000,
                                "bias": 0.1,
                            },
                            {
                                "segmentId": 7,
                                "count": 80,
                                "countLong": 8,
                                "totalValue": 8_000_000,
                                "totalLongValue": 800_000,
                                "totalShortValue": 7_200_000,
                                "bias": 0.1,
                            },
                        ],
                    }
                    for coin in ("BTC", "ETH", "SOL", "HYPE")
                ]

            def position_metrics(self, coin: str, segment_id: int, **kwargs: Any) -> dict[str, Any]:
                self.metric_calls.append((coin, segment_id))
                return {
                    "metrics": [
                        {
                            "createdAt": "2026-06-16T00:00:00Z",
                            "positionCount": 100,
                            "positionCountLong": 20,
                            "totalPositionValue": 10_000_000,
                            "totalPositionValueLong": 2_000_000,
                        },
                        {
                            "createdAt": "2026-06-16T01:00:00Z",
                            "positionCount": 100,
                            "positionCountLong": 10,
                            "totalPositionValue": 10_000_000,
                            "totalPositionValueLong": 1_000_000,
                        },
                    ]
                }

        fake_client = FakeCmmClient()
        self.service.cmm_client = fake_client

        with patch.dict(
            "os.environ",
            {"CMM_TREND_ENRICHMENT": "true", "CMM_SIGNAL_MAX_TREND_COINS": "3"},
            clear=False,
        ):
            summary = self.service.build_cmm_signal_summary()

        self.assertEqual(summary["signalCount"], 4)
        self.assertEqual(len(fake_client.metric_calls), 6)
        self.assertEqual(len({coin for coin, _segment in fake_client.metric_calls}), 3)

    def test_build_cmm_signal_summary_scans_all_heatmap_assets_by_default(self) -> None:
        class FakeCmmClient:
            token = "token"

            def positions_heatmap(self, *, opened_within: str) -> list[dict[str, Any]]:
                return [
                    {
                        "coin": "AAVE",
                        "segments": [
                            {
                                "segmentId": 8,
                                "count": 100,
                                "countLong": 90,
                                "totalValue": 10_000_000,
                                "totalLongValue": 9_000_000,
                                "totalShortValue": 1_000_000,
                                "bias": 0.9,
                            },
                            {
                                "segmentId": 7,
                                "count": 80,
                                "countLong": 70,
                                "totalValue": 8_000_000,
                                "totalLongValue": 7_000_000,
                                "totalShortValue": 1_000_000,
                                "bias": 0.875,
                            },
                        ],
                    }
                ]

            def position_metrics(self, coin: str, segment_id: int, **kwargs: Any) -> dict[str, Any]:
                return {"metrics": []}

        self.service.cmm_client = FakeCmmClient()

        summary = self.service.build_cmm_signal_summary()

        self.assertEqual(summary["coins"], [])
        self.assertEqual(summary["signals"][0]["coin"], "AAVE")
        self.assertEqual(summary["signals"][0]["side"], "long")

    def test_build_cmm_signal_summary_accepts_data_wrapped_heatmap(self) -> None:
        class FakeCmmClient:
            token = "token"

            def positions_heatmap(self, *, opened_within: str) -> dict[str, Any]:
                return {
                    "data": [
                        {
                            "coin": "AAVE",
                            "segments": [
                                {
                                    "segmentId": 8,
                                    "count": 100,
                                    "countLong": 90,
                                    "totalValue": 10_000_000,
                                    "totalSize": 100_000,
                                    "entryPrice": 100,
                                    "totalLongValue": 9_000_000,
                                    "totalShortValue": 1_000_000,
                                    "bias": 0.9,
                                },
                                {
                                    "segmentId": 7,
                                    "count": 80,
                                    "countLong": 70,
                                    "totalValue": 8_000_000,
                                    "totalSize": 80_000,
                                    "totalLongValue": 7_000_000,
                                    "totalShortValue": 1_000_000,
                                    "bias": 0.875,
                                },
                            ],
                        }
                    ]
                }

            def position_metrics(self, coin: str, segment_id: int, **kwargs: Any) -> dict[str, Any]:
                return {"metrics": []}

        self.service.cmm_client = FakeCmmClient()

        summary = self.service.build_cmm_signal_summary()

        self.assertEqual(summary["diagnostics"]["heatmapRows"], 1)
        self.assertEqual(summary["signals"][0]["coin"], "AAVE")
        self.assertEqual(summary["signals"][0]["side"], "long")
        self.assertEqual(summary["signals"][0]["price"], 100)

    def test_build_cmm_signal_summary_does_not_call_trends_by_default(self) -> None:
        class FakeCmmClient:
            token = "token"

            def positions_heatmap(self, *, opened_within: str) -> dict[str, Any]:
                return {
                    "data": [
                        {
                            "coin": "AAVE",
                            "segments": [
                                {
                                    "segmentId": 8,
                                    "count": 100,
                                    "countLong": 90,
                                    "totalValue": 10_000_000,
                                    "totalLongValue": 9_000_000,
                                    "totalShortValue": 1_000_000,
                                    "bias": 0.9,
                                },
                                {
                                    "segmentId": 7,
                                    "count": 80,
                                    "countLong": 70,
                                    "totalValue": 8_000_000,
                                    "totalLongValue": 7_000_000,
                                    "totalShortValue": 1_000_000,
                                    "bias": 0.875,
                                },
                            ],
                        }
                    ]
                }

            def position_metrics(self, coin: str, segment_id: int, **kwargs: Any) -> dict[str, Any]:
                raise AssertionError("Trend calls should be disabled by default")

        self.service.cmm_client = FakeCmmClient()

        summary = self.service.build_cmm_signal_summary()

        self.assertEqual(summary["signals"][0]["coin"], "AAVE")
        self.assertEqual(summary["signals"][0]["trendScore"], 0)

    def test_build_cmm_signal_summary_does_not_fallback_after_rate_limit(self) -> None:
        class FakeCmmClient:
            token = "token"

            def positions_heatmap(self, *, opened_within: str) -> dict[str, Any]:
                raise CoinMarketManApiError("CMM API returned HTTP 429: daily limit")

            def position_metrics(self, coin: str, segment_id: int, **kwargs: Any) -> dict[str, Any]:
                raise AssertionError("Fallback should not run after CMM rate limit")

        self.service.cmm_client = FakeCmmClient()

        summary = self.service.build_cmm_signal_summary()

        self.assertTrue(summary["rateLimited"])
        self.assertEqual(summary["signals"], [])
        self.assertIn("429", summary["error"])

    def test_build_cmm_signal_summary_filters_low_value_candidates(self) -> None:
        class FakeCmmClient:
            token = "token"

            def positions_heatmap(self, *, opened_within: str) -> dict[str, Any]:
                return {
                    "data": [
                        {
                            "coin": "HYNA:ZEC",
                            "segments": [
                                {
                                    "segmentId": 8,
                                    "count": 10,
                                    "countLong": 0,
                                    "totalValue": 40_000,
                                    "totalLongValue": 0,
                                    "totalShortValue": 40_000,
                                    "bias": 0,
                                },
                                {
                                    "segmentId": 7,
                                    "count": 8,
                                    "countLong": 0,
                                    "totalValue": 30_000,
                                    "totalLongValue": 0,
                                    "totalShortValue": 30_000,
                                    "bias": 0,
                                },
                            ],
                        },
                        {
                            "coin": "HYNA:XMR",
                            "segments": [
                                {
                                    "segmentId": 8,
                                    "count": 100,
                                    "countLong": 90,
                                    "totalValue": 700_000,
                                    "totalLongValue": 650_000,
                                    "totalShortValue": 50_000,
                                    "bias": 0.9,
                                },
                                {
                                    "segmentId": 7,
                                    "count": 80,
                                    "countLong": 70,
                                    "totalValue": 600_000,
                                    "totalLongValue": 550_000,
                                    "totalShortValue": 50_000,
                                    "bias": 0.875,
                                },
                            ],
                        },
                    ]
                }

            def position_metrics(self, coin: str, segment_id: int, **kwargs: Any) -> dict[str, Any]:
                return {"metrics": []}

        self.service.cmm_client = FakeCmmClient()

        summary = self.service.build_cmm_signal_summary()

        self.assertEqual(summary["diagnostics"]["lowValueCandidates"], 1)
        self.assertEqual([item["coin"] for item in summary["signals"]], ["XMR"])

    def test_build_cached_cmm_signal_summary_reuses_fresh_cache(self) -> None:
        cached = {
            "enabled": True,
            "signals": [{"coin": "LINK", "side": "short"}],
            "generatedAt": now_iso(),
        }

        with patch.object(self.service, "build_cmm_signal_summary") as live_summary:
            summary = self.service.build_cached_cmm_signal_summary({"cmmSignals": cached})

        live_summary.assert_not_called()
        self.assertTrue(summary["cacheHit"])
        self.assertEqual(summary["signals"], cached["signals"])

    def test_build_cached_cmm_signal_summary_refreshes_expired_cache(self) -> None:
        cached = {
            "enabled": True,
            "signals": [{"coin": "LINK", "side": "short"}],
            "generatedAt": "2026-06-20T00:00:00Z",
        }
        live = {
            "enabled": True,
            "signals": [{"coin": "LTC", "side": "short"}],
            "generatedAt": now_iso(),
        }

        with patch.object(self.service, "build_cmm_signal_summary", return_value=live) as live_summary:
            summary = self.service.build_cached_cmm_signal_summary({"cmmSignals": cached})

        live_summary.assert_called_once()
        self.assertEqual(summary["signals"], live["signals"])

    def test_cmm_signal_tier_requires_actionable_value(self) -> None:
        self.assertEqual(self.service.cmm_signal_tier(79, 866_000), "watch")
        self.assertEqual(self.service.cmm_signal_tier(79, 1_100_000), "actionable")
        self.assertEqual(self.service.cmm_signal_tier(86, 1_100_000), "alert")

    def test_build_cmm_signals_message_limits_groups_and_marks_tracked(self) -> None:
        signals = [
            {
                "coin": f"COIN{i}",
                "side": "short",
                "action": "sell",
                "signalTier": "watch",
                "probabilityScore": 80 - i,
                "cohortCount": 3,
                "valueBias": 0.8,
                "trendScore": 0,
                "contrarianScore": 0,
                "totalValue": 2_000_000,
                "price": 123.45,
                "priceSource": "api",
                "components": [{"segment": "Money Printer"}],
            }
            for i in range(12)
        ]
        signals[0]["coin"] = "LINK"
        wallet_summary = {
            "consensus": [
                {
                    "coin": "LINK",
                    "side": "short",
                    "walletCount": 4,
                    "netWeightedWalletCount": 2.5,
                }
            ],
            "signals": [],
        }

        message = self.service.build_cmm_signals_message(
            {
                "enabled": True,
                "timeframe": "7d",
                "signals": signals,
                "generatedAt": "2026-06-20T00:00:00Z",
            },
            wallet_summary=wallet_summary,
        )

        self.assertIn("Crypto:", message)
        self.assertIn("tracked 4w qnet 2.5", message)
        self.assertIn("entry $123.45", message)
        self.assertIn("10. WATCH", message)
        self.assertNotIn("11. WATCH", message)

    def test_cmm_confirmation_filters_unconfirmed_wallet_alerts(self) -> None:
        summary = {
            "signals": [
                {
                    "coin": "BTC",
                    "side": "long",
                    "probabilityScore": 95.0,
                    "walletCount": 6,
                    "netWeightedWalletCount": 4.0,
                }
            ],
            "signalCount": 1,
        }
        cmm_summary = {"enabled": True, "signals": []}

        filtered = self.service.apply_cmm_confirmation_to_summary(
            summary,
            cmm_summary,
            require_confirmation=True,
        )

        self.assertEqual(filtered["signals"], [])

    def test_cmm_confirmation_keeps_strong_agreement(self) -> None:
        summary = {
            "signals": [
                {
                    "coin": "BTC",
                    "side": "short",
                    "probabilityScore": 95.0,
                    "walletCount": 6,
                    "netWeightedWalletCount": 4.0,
                }
            ],
            "signalCount": 1,
        }
        cmm_summary = {
            "enabled": True,
            "signals": [
                {
                    "coin": "BTC",
                    "side": "short",
                    "probabilityScore": 95.0,
                    "trendScore": 100.0,
                    "contrarianScore": 100.0,
                    "cohortCount": 3,
                }
            ],
        }

        filtered = self.service.apply_cmm_confirmation_to_summary(
            summary,
            cmm_summary,
            require_confirmation=True,
        )

        self.assertEqual(filtered["signals"][0]["cmmConfirmation"], "confirmed")
        self.assertGreaterEqual(filtered["signals"][0]["probabilityScore"], 80)

    def test_build_signals_message_includes_cmm_section(self) -> None:
        summary = {"generatedAt": "2026-06-16T00:00:00Z", "signals": []}
        cmm_summary = {
            "enabled": True,
            "timeframe": "7d",
            "signals": [
                {
                    "coin": "ETH",
                    "side": "short",
                    "action": "sell",
                    "probabilityScore": 81.0,
                    "cohortCount": 3,
                    "valueBias": -0.71,
                    "totalValue": 12_000_000,
                    "components": [{"segment": "Money Printer"}],
                }
            ],
            "generatedAt": "2026-06-16T00:00:00Z",
        }

        message = self.service.build_signals_message(summary, cmm_summary=cmm_summary)

        self.assertIn("CMM cohort signals", message)
        self.assertIn("SELL ETH short", message)

    def test_check_alerts_notifies_on_new_cmm_signal(self) -> None:
        current_summary = {
            "overallBias": "mixed",
            "consensus": [],
            "hip3Consensus": [],
            "signals": [],
        }
        cmm_summary = {
            "enabled": True,
            "signals": [
                {
                    "coin": "SOL",
                    "side": "short",
                    "action": "sell",
                    "probabilityScore": 86.0,
                    "cohortCount": 2,
                    "valueBias": -0.8,
                    "totalValue": 7_500_000,
                    "components": [{"segment": "Money Printer"}, {"segment": "Leviathan"}],
                }
            ],
            "generatedAt": "2026-06-16T00:00:00Z",
        }

        with patch(
            "server.load_json_file",
            return_value={
                "config": {"enabled": True, "botToken": "token", "chatId": "chat"},
                "state": {"summary": current_summary, "largePositions": {}, "cmmSignals": {"signals": []}},
            },
        ), patch("server.save_json_file"), patch.object(
            self.service, "dashboard", return_value={"wallets": []}
        ), patch.object(
            self.service, "build_sentiment_summary", return_value=current_summary
        ), patch.object(
            self.service, "build_cmm_signal_summary", return_value=cmm_summary
        ), patch.object(
            self.service, "send_telegram_message"
        ) as send_telegram_message:
            result = self.service.check_alerts(send_notification=True)

        self.assertTrue(result["shouldNotify"])
        self.assertTrue(result["sent"])
        self.assertEqual(result["changes"]["addedCmmSignals"][0]["coin"], "SOL")
        sent_message = send_telegram_message.call_args.args[2]
        self.assertIn("CMM cohort alerts", sent_message)
        self.assertIn("SELL SOL short", sent_message)

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
                        {"coin": "BTC", "side": "Long", "positionValue": 650000.0, "size": 4.0, "entryPx": 80000.0},
                        {"coin": "ETH", "side": "Short", "positionValue": 99000.0},
                    ],
                },
                {
                    "alias": "main-2",
                    "address": "0x2222222222222222222222222222222222222222",
                    "positions": [{"coin": "BTC", "side": "Long", "positionValue": 450000.0, "size": 3.0, "entryPx": 76000.0}],
                },
                {
                    "alias": "main-3",
                    "address": "0x3333333333333333333333333333333333333333",
                    "positions": [{"coin": "BTC", "side": "Long", "positionValue": 250000.0, "size": 2.0, "entryPx": 77000.0}],
                },
            ],
        }

        message = self.service.build_positions_message(dashboard)
        self.assertIn("Open positions now", message)
        self.assertIn("By wallet count (3+ wallets, $1.0M+):", message)
        self.assertIn(
            "BTC long (3 wallets, 3 positions, $1,350K, size-w entry $78,000, dist +92.3%, extended)",
            message,
        )
        self.assertNotIn("ETH short", message)
        self.assertIn("Position groups: 1", message)

    def test_build_positions_message_labels_simple_entry_average_when_size_missing(self) -> None:
        dashboard = {
            "generatedAt": "2026-04-09T06:00:00Z",
            "wallets": [
                {
                    "alias": "main-1",
                    "address": "0x1111111111111111111111111111111111111111",
                    "positions": [{"coin": "BTC", "side": "Long", "positionValue": 500000.0, "entryPx": 80000.0}],
                },
                {
                    "alias": "main-2",
                    "address": "0x2222222222222222222222222222222222222222",
                    "positions": [{"coin": "BTC", "side": "Long", "positionValue": 400000.0, "entryPx": 76000.0}],
                },
                {
                    "alias": "main-3",
                    "address": "0x3333333333333333333333333333333333333333",
                    "positions": [{"coin": "BTC", "side": "Long", "positionValue": 300000.0, "entryPx": 77000.0}],
                },
            ],
        }

        message = self.service.build_positions_message(dashboard)

        self.assertIn("BTC long (3 wallets, 3 positions, $1,200K, avg entry $77,667)", message)
        self.assertNotIn("size-w entry", message)

    def test_build_positions_message_filters_groups_below_value_threshold(self) -> None:
        dashboard = {
            "generatedAt": "2026-04-09T06:00:00Z",
            "wallets": [
                {
                    "alias": "main-1",
                    "address": "0x1111111111111111111111111111111111111111",
                    "positions": [{"coin": "CHIP", "side": "Short", "positionValue": 10000.0}],
                },
                {
                    "alias": "main-2",
                    "address": "0x2222222222222222222222222222222222222222",
                    "positions": [{"coin": "CHIP", "side": "Short", "positionValue": 8000.0}],
                },
                {
                    "alias": "main-3",
                    "address": "0x3333333333333333333333333333333333333333",
                    "positions": [{"coin": "CHIP", "side": "Short", "positionValue": 5000.0}],
                },
            ],
        }

        message = self.service.build_positions_message(dashboard)
        self.assertIn("- No open positions", message)
        self.assertNotIn("CHIP short", message)
        self.assertIn("Position groups: 0", message)

    def test_build_positions_message_excludes_loracle_hype_positions(self) -> None:
        dashboard = {
            "generatedAt": "2026-04-09T06:00:00Z",
            "wallets": [
                {
                    "alias": "Loracle",
                    "address": "0x8def9f50456c6c4e37fa5d3d57f108ed23992dae",
                    "positions": [{"coin": "HYPE", "side": "Short", "positionValue": 10_000_000.0}],
                },
                {
                    "alias": "main-2",
                    "address": "0x2222222222222222222222222222222222222222",
                    "positions": [{"coin": "HYPE", "side": "Short", "positionValue": 2_000_000.0}],
                },
                {
                    "alias": "main-3",
                    "address": "0x3333333333333333333333333333333333333333",
                    "positions": [{"coin": "HYPE", "side": "Short", "positionValue": 2_000_000.0}],
                },
            ],
        }

        message = self.service.build_positions_message(dashboard)

        self.assertIn("- No open positions", message)
        self.assertNotIn("HYPE short", message)
        self.assertIn("Position groups: 0", message)

    def test_build_positions_message_excludes_large_losing_positions(self) -> None:
        dashboard = {
            "generatedAt": "2026-04-09T06:00:00Z",
            "wallets": [
                {
                    "alias": "losing",
                    "address": "0x1111111111111111111111111111111111111111",
                    "positions": [
                        {"coin": "BTC", "side": "Long", "positionValue": 2_000_000.0, "unrealizedPnl": -1_200_000.0}
                    ],
                },
                {
                    "alias": "ok-2",
                    "address": "0x2222222222222222222222222222222222222222",
                    "positions": [
                        {"coin": "BTC", "side": "Long", "positionValue": 1_000_000.0, "unrealizedPnl": -900_000.0}
                    ],
                },
                {
                    "alias": "ok-3",
                    "address": "0x3333333333333333333333333333333333333333",
                    "positions": [
                        {"coin": "BTC", "side": "Long", "positionValue": 1_000_000.0, "unrealizedPnl": 0.0}
                    ],
                },
            ],
        }

        message = self.service.build_positions_message(dashboard)

        self.assertIn("- No open positions", message)
        self.assertNotIn("BTC long", message)
        self.assertIn("Open positions: 0", message)

    def test_build_position_wallets_message_lists_matching_wallets(self) -> None:
        dashboard = {
            "generatedAt": "2026-04-09T06:00:00Z",
            "wallets": [
                {
                    "alias": "Big BTC",
                    "address": "0x1111111111111111111111111111111111111111",
                    "positions": [
                        {
                            "coin": "BTC",
                            "side": "Long",
                            "positionValue": 800000.0,
                            "size": 10.0,
                            "entryPx": 78000.0,
                            "unrealizedPnl": 12345.0,
                        }
                    ],
                },
                {
                    "alias": "Small BTC",
                    "address": "0x2222222222222222222222222222222222222222",
                    "positions": [
                        {"coin": "BTC", "side": "Long", "positionValue": 400000.0, "size": 5.0, "entryPx": 76000.0}
                    ],
                },
                {
                    "alias": "Short BTC",
                    "address": "0x3333333333333333333333333333333333333333",
                    "positions": [
                        {"coin": "BTC", "side": "Short", "positionValue": 900000.0, "size": 12.0, "entryPx": 79000.0}
                    ],
                },
            ],
        }

        message = self.service.build_position_wallets_message(dashboard, "btc", "long")

        self.assertIn("BTC long wallets", message)
        self.assertIn("Wallets: 2 | Positions: 2 | Total: $1,200K, size-w entry $77,333", message)
        self.assertIn(
            "1. 0x1111111111111111111111111111111111111111: $800K, size 10, entry $78,000, uPnL $12,345",
            message,
        )
        self.assertIn("2. 0x2222222222222222222222222222222222222222: $400K, size 5, entry $76,000", message)
        self.assertNotIn("Short BTC", message)

    def test_build_position_wallets_message_excludes_loracle_hype(self) -> None:
        dashboard = {
            "generatedAt": "2026-04-09T06:00:00Z",
            "wallets": [
                {
                    "alias": "Loracle",
                    "address": "0x8def9f50456c6c4e37fa5d3d57f108ed23992dae",
                    "positions": [
                        {"coin": "HYPE", "side": "Short", "positionValue": 10_000_000.0, "size": 200_000.0}
                    ],
                },
                {
                    "alias": "Other",
                    "address": "0x2222222222222222222222222222222222222222",
                    "positions": [
                        {"coin": "HYPE", "side": "Short", "positionValue": 1_000_000.0, "size": 20_000.0}
                    ],
                },
            ],
        }

        message = self.service.build_position_wallets_message(dashboard, "hype", "short")

        self.assertIn("Wallets: 1 | Positions: 1 | Total: $1,000K", message)
        self.assertIn("0x2222222222222222222222222222222222222222: $1,000K", message)
        self.assertNotIn("Loracle", message)

    def test_build_position_wallets_message_matches_mixed_case_tickers(self) -> None:
        dashboard = {
            "generatedAt": "2026-04-09T06:00:00Z",
            "wallets": [
                {
                    "alias": "Mixed",
                    "address": "0x1111111111111111111111111111111111111111",
                    "positions": [
                        {"coin": "kPEPE", "side": "Short", "positionValue": 600000.0, "size": 1000000.0}
                    ],
                }
            ],
        }

        message = self.service.build_position_wallets_message(dashboard, "kpepe", "short")

        self.assertIn("KPEPE short wallets", message)
        self.assertIn("0x1111111111111111111111111111111111111111: $600K", message)

    def test_build_position_wallets_message_excludes_large_losing_positions(self) -> None:
        dashboard = {
            "generatedAt": "2026-04-09T06:00:00Z",
            "wallets": [
                {
                    "address": "0x1111111111111111111111111111111111111111",
                    "positions": [
                        {
                            "coin": "BTC",
                            "side": "Long",
                            "positionValue": 2_000_000.0,
                            "size": 20.0,
                            "entryPx": 100_000.0,
                            "unrealizedPnl": -1_200_000.0,
                        }
                    ],
                },
                {
                    "address": "0x2222222222222222222222222222222222222222",
                    "positions": [
                        {
                            "coin": "BTC",
                            "side": "Long",
                            "positionValue": 800_000.0,
                            "size": 10.0,
                            "entryPx": 80_000.0,
                            "unrealizedPnl": -999_999.0,
                        }
                    ],
                },
            ],
        }

        message = self.service.build_position_wallets_message(dashboard, "btc", "long")

        self.assertIn("Wallets: 1 | Positions: 1 | Total: $800K", message)
        self.assertNotIn("0x1111111111111111111111111111111111111111", message)
        self.assertIn("0x2222222222222222222222222222222222222222", message)

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
        self.assertNotIn("BTC long (1 wallets, 1 positions", message)
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
                {
                    "alias": "main-4",
                    "address": "0x4444444444444444444444444444444444444444",
                    "positions": [{"coin": "CL", "side": "Long", "positionValue": 604840.15}],
                },
            ],
        }

        message = self.service.build_positions_message(dashboard)
        self.assertIn("OIL long (3 wallets, 3 positions, $2,601K)", message)
        self.assertNotIn("OIL short", message)

    def test_build_positions_message_groups_commodities_by_wallet_count(self) -> None:
        dashboard = {
            "generatedAt": "2026-04-09T06:00:00Z",
            "wallets": [
                {
                    "alias": "main-1",
                    "address": "0x1111111111111111111111111111111111111111",
                    "positions": [
                        {"coin": "xyz:GOLD", "side": "Long", "positionValue": 600000.0},
                        {"coin": "xyz:SILVER", "side": "Short", "positionValue": 76938.0},
                    ],
                },
                {
                    "alias": "main-2",
                    "address": "0x2222222222222222222222222222222222222222",
                    "positions": [{"coin": "cash:GOLD", "side": "Long", "positionValue": 300000.0}],
                },
                {
                    "alias": "main-3",
                    "address": "0x3333333333333333333333333333333333333333",
                    "positions": [{"coin": "GOLD", "side": "Long", "positionValue": 200000.0}],
                },
            ],
        }

        message = self.service.build_positions_message(dashboard)
        self.assertIn("Commodities:", message)
        self.assertIn("GOLD long (3 wallets, 3 positions, $1,100K)", message)
        self.assertNotIn("SILVER short", message)
        self.assertNotIn("xyz:GOLD", message)
        self.assertNotIn("xyz:SILVER", message)

    def test_build_positions_message_groups_stocks_by_wallet_count(self) -> None:
        dashboard = {
            "generatedAt": "2026-04-09T06:00:00Z",
            "wallets": [
                {
                    "alias": "main-1",
                    "address": "0x1111111111111111111111111111111111111111",
                    "positions": [
                        {"coin": "xyz:NVDA", "side": "Long", "positionValue": 600000.0},
                        {"coin": "EWY", "side": "Long", "positionValue": 500000.0},
                        {"coin": "vntl:SPACEX", "side": "Short", "positionValue": 2500.0},
                    ],
                },
                {
                    "alias": "main-2",
                    "address": "0x2222222222222222222222222222222222222222",
                    "positions": [
                        {"coin": "xyz:NVDA", "side": "Long", "positionValue": 300000.0},
                        {"coin": "EWY", "side": "Long", "positionValue": 300000.0},
                    ],
                },
                {
                    "alias": "main-3",
                    "address": "0x3333333333333333333333333333333333333333",
                    "positions": [
                        {"coin": "xyz:NVDA", "side": "Long", "positionValue": 200000.0},
                        {"coin": "EWY", "side": "Long", "positionValue": 250000.0},
                    ],
                },
            ],
        }

        message = self.service.build_positions_message(dashboard)
        self.assertIn("Stocks / indices:", message)
        self.assertIn("EWY long (3 wallets, 3 positions, $1,050K)", message)
        self.assertIn("NVDA long (3 wallets, 3 positions, $1,100K)", message)
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
                {
                    "alias": "main-3",
                    "address": "0x3333333333333333333333333333333333333333",
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
                        {"coin": "CL", "side": "Long", "positionValue": 450000.0},
                        {"coin": "SP500", "side": "Long", "positionValue": 450000.0},
                        {"coin": "XYZ100", "side": "Long", "positionValue": 914001.24},
                        {"coin": "SILVER", "side": "Short", "positionValue": 576938.0},
                    ],
                },
                {
                    "alias": "main-2",
                    "address": "0x2222222222222222222222222222222222222222",
                    "positions": [
                        {"coin": "CL", "side": "Long", "positionValue": 350000.0},
                        {"coin": "SP500", "side": "Long", "positionValue": 350000.0},
                    ],
                },
                {
                    "alias": "main-3",
                    "address": "0x3333333333333333333333333333333333333333",
                    "positions": [
                        {"coin": "OIL", "side": "Long", "positionValue": 250000.0},
                        {"coin": "SP500", "side": "Long", "positionValue": 250000.0},
                    ],
                },
            ],
        }

        message = self.service.build_positions_message(dashboard)
        self.assertIn("Commodities:", message)
        self.assertIn("OIL long (3 wallets, 3 positions, $1,050K)", message)
        self.assertNotIn("SILVER short", message)
        self.assertIn("Stocks / indices:", message)
        self.assertNotIn("XYZ100 long", message)
        self.assertIn("SP500 long (3 wallets, 3 positions, $1,050K)", message)

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

    def test_check_alerts_does_not_notify_on_weak_consensus_churn(self) -> None:
        previous_summary = {
            "overallBias": "mixed",
            "consensus": [],
            "hip3Consensus": [],
            "signals": [],
        }
        current_summary = {
            "overallBias": "bullish",
            "consensus": [{"coin": "BTC", "side": "long", "walletCount": 3, "totalValue": 900_000.0}],
            "hip3Consensus": [],
            "signals": [],
        }

        with patch(
            "server.load_json_file",
            return_value={
                "config": {"enabled": True, "botToken": "token", "chatId": "chat"},
                "state": {"summary": previous_summary, "largePositions": {}},
            },
        ), patch("server.save_json_file"), patch.object(
            self.service, "dashboard", return_value={"wallets": []}
        ), patch.object(
            self.service, "build_sentiment_summary", return_value=current_summary
        ), patch.object(
            self.service, "send_telegram_message"
        ) as send_telegram_message:
            result = self.service.check_alerts(send_notification=True)

        self.assertFalse(result["shouldNotify"])
        self.assertFalse(result["sent"])
        self.assertEqual(len(result["changes"]["addedConsensus"]), 1)
        send_telegram_message.assert_not_called()

    def test_check_alerts_notifies_on_actionable_signal(self) -> None:
        previous_summary = {
            "overallBias": "mixed",
            "consensus": [],
            "hip3Consensus": [],
            "signals": [],
        }
        current_summary = {
            "overallBias": "bullish",
            "consensus": [],
            "hip3Consensus": [],
            "signals": [
                {
                    "coin": "BTC",
                    "side": "long",
                    "action": "buy",
                    "walletCount": 5,
                    "netWalletCount": 4,
                    "netWeightedWalletCount": 3.5,
                    "probabilityScore": 82.0,
                    "totalValue": 2_500_000.0,
                }
            ],
        }

        with patch(
            "server.load_json_file",
            return_value={
                "config": {"enabled": True, "botToken": "token", "chatId": "chat"},
                "state": {"summary": previous_summary, "largePositions": {}},
            },
        ), patch("server.save_json_file"), patch.object(
            self.service, "dashboard", return_value={"wallets": []}
        ), patch.object(
            self.service, "build_sentiment_summary", return_value=current_summary
        ), patch.object(
            self.service, "send_telegram_message"
        ) as send_telegram_message:
            result = self.service.check_alerts(send_notification=True)

        self.assertTrue(result["shouldNotify"])
        self.assertTrue(result["sent"])
        sent_message = send_telegram_message.call_args.args[2]
        self.assertIn("Actionable signals", sent_message)
        self.assertIn("BUY BTC long: p82", sent_message)

    def test_check_alerts_notifies_on_new_large_positions(self) -> None:
        now_ms = 1_700_000_000_000
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
                        {"coin": "BTC", "side": "Long", "positionValue": 1_200_000.0, "size": 12.0, "entryPx": 100000.0},
                    ],
                    "recentFills": [
                        {"coin": "BTC", "direction": "Open Long", "price": 100000.0, "size": 12.0, "time": now_ms - 60_000}
                    ],
                }
            ]
        }

        with patch("server.load_json_file", return_value={"config": {"enabled": True, "botToken": "token", "chatId": "chat"}, "state": {"summary": previous_summary, "largePositions": {}}}), patch(
            "server.save_json_file"
        ), patch("server.current_time_ms", return_value=now_ms), patch.object(self.service, "dashboard", return_value=dashboard), patch.object(
            self.service, "build_sentiment_summary", return_value=current_summary
        ), patch.object(self.service, "send_telegram_message") as send_telegram_message:
            result = self.service.check_alerts(send_notification=True)

        self.assertTrue(result["shouldNotify"])
        self.assertTrue(result["sent"])
        self.assertEqual(len(result["changes"]["newLargePositions"]), 1)
        self.assertEqual(result["changes"]["newLargePositions"][0]["coin"], "BTC")
        sent_message = send_telegram_message.call_args.args[2]
        self.assertIn("Open >$500K", sent_message)
        self.assertIn("Trader One: BTC long $1.2M sz 12 open @$100,000", sent_message)

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
                "totalValue": 1_200_000.0,
                "totalSize": 400.0,
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
        self.assertIn("Trader One: ETH short $1.2M sz 400 last ~$3,000", sent_message)

    def test_check_alerts_preview_does_not_sync_alert_baseline(self) -> None:
        now_ms = 1_700_000_000_000
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
                    "positions": [{"coin": "BTC", "side": "Long", "positionValue": 1_200_000.0}],
                    "recentFills": [
                        {"coin": "BTC", "direction": "Open Long", "price": 100000.0, "size": 12.0, "time": now_ms - 60_000}
                    ],
                }
            ]
        }

        with patch(
            "server.load_json_file",
            return_value={
                "config": {"enabled": True, "botToken": "token", "chatId": "chat"},
                "state": {"summary": previous_summary, "largePositions": {}},
            },
        ), patch("server.save_json_file") as save_json_file, patch("server.current_time_ms", return_value=now_ms), patch.object(
            self.service, "dashboard", return_value=dashboard
        ), patch.object(
            self.service, "build_sentiment_summary", return_value=current_summary
        ), patch.object(
            self.service, "send_telegram_message"
        ) as send_telegram_message:
            result = self.service.check_alerts(send_notification=False)

        self.assertTrue(result["shouldNotify"])
        self.assertFalse(result["sent"])
        save_json_file.assert_not_called()
        send_telegram_message.assert_not_called()

    def test_check_alerts_suppresses_recent_duplicate_position_alert(self) -> None:
        now_ms = 1_700_000_000_000
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
        address = "0x1111111111111111111111111111111111111111"
        dashboard = {
            "wallets": [
                {
                    "address": address,
                    "alias": "Trader One",
                    "positions": [
                        {"coin": "BTC", "side": "Long", "positionValue": 1_200_000.0, "size": 12.0, "entryPx": 100000.0},
                    ],
                    "recentFills": [
                        {"coin": "BTC", "direction": "Open Long", "price": 100000.0, "size": 12.0, "time": now_ms - 60_000}
                    ],
                }
            ]
        }
        duplicate_key = self.service.large_position_event_key(
            "open",
            {"address": address, "coin": "BTC", "side": "long", "totalValue": 1_200_000.0, "totalSize": 12.0},
        )

        with patch(
            "server.load_json_file",
            return_value={
                "config": {"enabled": True, "botToken": "token", "chatId": "chat"},
                "state": {
                    "summary": previous_summary,
                    "largePositions": {},
                    "alertDedupe": {duplicate_key: 9_999_999_000_000},
                },
            },
        ), patch("server.save_json_file") as save_json_file, patch("server.current_time_ms", return_value=now_ms), patch.object(
            self.service, "dashboard", return_value=dashboard
        ), patch.object(
            self.service, "build_sentiment_summary", return_value=current_summary
        ), patch.object(
            self.service, "send_telegram_message"
        ) as send_telegram_message:
            result = self.service.check_alerts(send_notification=True)

        self.assertFalse(result["shouldNotify"])
        self.assertFalse(result["sent"])
        self.assertEqual(result["suppressedAlertCount"], 1)
        send_telegram_message.assert_not_called()
        saved_state = save_json_file.call_args.args[1]["state"]
        self.assertEqual(saved_state["summary"], current_summary)
        self.assertIn(f"{address}:BTC:long", saved_state["largePositions"])

    def test_check_alerts_records_dedupe_after_successful_alert(self) -> None:
        now_ms = 1_700_000_000_000
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
                        {"coin": "BTC", "side": "Long", "positionValue": 1_200_000.0, "size": 12.0, "entryPx": 100000.0},
                    ],
                    "recentFills": [
                        {"coin": "BTC", "direction": "Open Long", "price": 100000.0, "size": 12.0, "time": now_ms - 60_000}
                    ],
                }
            ]
        }

        with patch(
            "server.load_json_file",
            return_value={
                "config": {"enabled": True, "botToken": "token", "chatId": "chat"},
                "state": {"summary": previous_summary, "largePositions": {}, "alertDedupe": {}},
            },
        ), patch("server.save_json_file") as save_json_file, patch("server.current_time_ms", return_value=now_ms), patch.object(
            self.service, "dashboard", return_value=dashboard
        ), patch.object(
            self.service, "build_sentiment_summary", return_value=current_summary
        ), patch.object(
            self.service, "send_telegram_message"
        ):
            result = self.service.check_alerts(send_notification=True)

        self.assertTrue(result["sent"])
        saved_dedupe = save_json_file.call_args.args[1]["state"]["alertDedupe"]
        self.assertEqual(len(saved_dedupe), 1)
        self.assertTrue(next(iter(saved_dedupe)).startswith("position:open:"))

    def test_check_alerts_notifies_on_clustered_large_opens(self) -> None:
        now_ms = 1_700_000_000_000
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
                        {"coin": "BTC", "side": "Long", "positionValue": 1_200_000.0, "size": 12.0, "entryPx": 100000.0},
                    ],
                    "recentFills": [
                        {"coin": "BTC", "direction": "Open Long", "price": 100000.0, "size": 12.0, "time": now_ms - 60_000}
                    ],
                },
                {
                    "address": "0x2222222222222222222222222222222222222222",
                    "alias": "Trader Two",
                    "positions": [
                        {"coin": "BTC", "side": "Long", "positionValue": 1_100_000.0, "size": 10.0, "entryPx": 110000.0},
                    ],
                    "recentFills": [
                        {"coin": "BTC", "direction": "Open Long", "price": 110000.0, "size": 10.0, "time": now_ms - 300_000}
                    ],
                },
                {
                    "address": "0x3333333333333333333333333333333333333333",
                    "alias": "Trader Three",
                    "positions": [
                        {"coin": "BTC", "side": "Long", "positionValue": 1_300_000.0, "size": 13.0, "entryPx": 100000.0},
                    ],
                    "recentFills": [
                        {"coin": "BTC", "direction": "Open Long", "price": 100000.0, "size": 13.0, "time": now_ms - 240_000}
                    ],
                },
            ]
        }
        previous_positions = self.service.build_large_position_snapshot(dashboard)

        with patch(
            "server.load_json_file",
            return_value={
                "config": {"enabled": True, "botToken": "token", "chatId": "chat"},
                "state": {"summary": previous_summary, "largePositions": previous_positions, "alertDedupe": {}},
            },
        ), patch("server.save_json_file") as save_json_file, patch("server.current_time_ms", return_value=now_ms), patch.object(
            self.service, "dashboard", return_value=dashboard
        ), patch.object(
            self.service, "build_sentiment_summary", return_value=current_summary
        ), patch.object(
            self.service, "send_telegram_message"
        ) as send_telegram_message:
            result = self.service.check_alerts(send_notification=True)

        self.assertTrue(result["shouldNotify"])
        self.assertTrue(result["sent"])
        self.assertEqual(len(result["changes"]["clusteredOpenPositions"]), 1)
        self.assertEqual(result["changes"]["clusteredOpenPositions"][0]["coin"], "BTC")
        self.assertEqual(result["changes"]["clusteredOpenPositions"][0]["walletCount"], 3)
        sent_message = send_telegram_message.call_args.args[2]
        self.assertIn("3+ opens >$500K in 5m", sent_message)
        self.assertIn("- BTC long: 3 wallets, $3.6M", sent_message)
        self.assertIn("Trader One: $1.2M", sent_message)
        saved_dedupe = save_json_file.call_args.args[1]["state"]["alertDedupe"]
        self.assertTrue(next(iter(saved_dedupe)).startswith("position:cluster-open:BTC:long:"))

    def test_clustered_large_open_alert_requires_three_wallets_inside_window(self) -> None:
        now_ms = 1_700_000_000_000
        dashboard = {
            "wallets": [
                {
                    "address": "0x1111111111111111111111111111111111111111",
                    "positions": [
                        {"coin": "ETH", "side": "Short", "positionValue": 1_100_000.0, "size": 440.0, "entryPx": 2500.0}
                    ],
                    "recentFills": [
                        {"coin": "ETH", "direction": "Open Short", "price": 2500.0, "size": 440.0, "time": now_ms - 60_000}
                    ],
                },
                {
                    "address": "0x2222222222222222222222222222222222222222",
                    "positions": [
                        {"coin": "ETH", "side": "Short", "positionValue": 1_200_000.0, "size": 480.0, "entryPx": 2500.0}
                    ],
                    "recentFills": [
                        {"coin": "ETH", "direction": "Open Short", "price": 2500.0, "size": 480.0, "time": now_ms - 120_000}
                    ],
                },
                {
                    "address": "0x3333333333333333333333333333333333333333",
                    "positions": [
                        {"coin": "ETH", "side": "Short", "positionValue": 1_300_000.0, "size": 520.0, "entryPx": 2500.0}
                    ],
                    "recentFills": [
                        {"coin": "ETH", "direction": "Open Short", "price": 2500.0, "size": 520.0, "time": now_ms - 660_000}
                    ],
                },
            ]
        }
        current_positions = self.service.build_large_position_snapshot(dashboard)

        alerts = self.service.build_clustered_open_position_alerts(
            dashboard,
            current_positions,
            now_ms=now_ms,
        )

        self.assertEqual(alerts, [])

    def test_check_alerts_does_not_sync_failed_telegram_alert(self) -> None:
        previous_summary = {
            "overallBias": "mixed",
            "consensus": [{"coin": "BTC", "side": "long", "walletCount": 3, "totalValue": 30_000_000.0}],
            "hip3Consensus": [],
        }
        current_summary = {
            "overallBias": "mixed",
            "consensus": [{"coin": "ETH", "side": "short", "walletCount": 3, "totalValue": 40_000_000.0}],
            "hip3Consensus": [],
        }
        previous_positions = {
            "0x2222222222222222222222222222222222222222:BTC:long": {
                "address": "0x2222222222222222222222222222222222222222",
                "alias": "Old Trader",
                "coin": "BTC",
                "side": "long",
                "totalValue": 1_200_000.0,
                "totalSize": 7.0,
            }
        }
        dashboard = {
            "wallets": [
                {
                    "address": "0x1111111111111111111111111111111111111111",
                    "alias": "Trader One",
                    "positions": [{"coin": "ETH", "side": "Short", "positionValue": 1_300_000.0}],
                }
            ]
        }

        with patch(
            "server.load_json_file",
            return_value={
                "config": {"enabled": True, "botToken": "token", "chatId": "chat"},
                "state": {"summary": previous_summary, "largePositions": previous_positions},
            },
        ), patch("server.save_json_file") as save_json_file, patch.object(
            self.service, "dashboard", return_value=dashboard
        ), patch.object(
            self.service, "build_sentiment_summary", return_value=current_summary
        ), patch.object(
            self.service,
            "send_telegram_message",
            side_effect=ValueError("telegram down"),
        ):
            result = self.service.check_alerts(send_notification=True)

        self.assertTrue(result["shouldNotify"])
        self.assertFalse(result["sent"])
        self.assertIn("telegram down", result["error"])
        saved_state = save_json_file.call_args.args[1]["state"]
        self.assertEqual(saved_state["summary"], previous_summary)
        self.assertEqual(saved_state["largePositions"], previous_positions)

    def test_send_hourly_update_syncs_alert_baseline(self) -> None:
        now_ms = 1_700_000_000_000
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
                        {"coin": "BTC", "side": "Long", "positionValue": 1_207_800.0},
                    ],
                    "recentFills": [
                        {"coin": "BTC", "direction": "Open Long", "price": 100000.0, "size": 12.078, "time": now_ms - 60_000}
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
        ), patch("server.save_json_file") as save_json_file, patch("server.current_time_ms", return_value=now_ms), patch.object(
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
        self.assertNotIn("High-conviction signals", hourly_message)
        alert_message = send_telegram_message.call_args_list[1].args[2]
        self.assertIn("Open >$500K", alert_message)
        self.assertIn("Trader One: BTC long $1.2M", alert_message)
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
                        {"coin": "ETH", "side": "Short", "positionValue": 1_300_000.0, "size": 300.0},
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
                "totalValue": 1_200_000.0,
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
                "totalValue": 1_200_000.0,
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
                        {"coin": "BTC", "side": "Long", "positionValue": 2_400_000.0, "size": 20.0, "entryPx": 78000.0},
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
        self.assertIn("Trader One: BTC long $1.2M->$2.4M (+$1.2M +10 add ~$120,000)", sent_message)
        self.assertNotIn("@$78,000", sent_message)

    def test_large_position_snapshot_filters_after_aggregation(self) -> None:
        dashboard = {
            "wallets": [
                {
                    "address": "0x1111111111111111111111111111111111111111",
                    "alias": "Trader One",
                    "positions": [
                        {"coin": "BTC", "side": "Long", "positionValue": 600000.0, "size": 6.0, "entryPx": 75000.0},
                        {"coin": "BTC", "side": "Long", "positionValue": 500000.0, "size": 5.0, "entryPx": 76000.0},
                    ],
                }
            ]
        }

        snapshot = self.service.build_large_position_snapshot(dashboard)

        self.assertIn("0x1111111111111111111111111111111111111111:BTC:long", snapshot)
        self.assertEqual(snapshot["0x1111111111111111111111111111111111111111:BTC:long"]["totalValue"], 1_100_000.0)

    def test_large_position_snapshot_excludes_large_losing_positions(self) -> None:
        dashboard = {
            "wallets": [
                {
                    "address": "0x1111111111111111111111111111111111111111",
                    "alias": "Trader One",
                    "positions": [
                        {
                            "coin": "BTC",
                            "side": "Long",
                            "positionValue": 2_000_000.0,
                            "size": 20.0,
                            "entryPx": 100_000.0,
                            "unrealizedPnl": -1_200_000.0,
                        }
                    ],
                },
                {
                    "address": "0x2222222222222222222222222222222222222222",
                    "alias": "Trader Two",
                    "positions": [
                        {
                            "coin": "BTC",
                            "side": "Long",
                            "positionValue": 1_100_000.0,
                            "size": 10.0,
                            "entryPx": 80_000.0,
                            "unrealizedPnl": -1_000_000.0,
                        }
                    ],
                },
            ]
        }

        snapshot = self.service.build_large_position_snapshot(dashboard)

        self.assertNotIn("0x1111111111111111111111111111111111111111:BTC:long", snapshot)
        self.assertIn("0x2222222222222222222222222222222222222222:BTC:long", snapshot)

    def test_large_position_alerts_exclude_loracle_hype_positions(self) -> None:
        previous = {
            "0x8def9f50456c6c4e37fa5d3d57f108ed23992dae:HYPE:short": {
                "address": "0x8def9f50456c6c4e37fa5d3d57f108ed23992dae",
                "alias": "Loracle",
                "coin": "HYPE",
                "side": "short",
                "totalValue": 10_000_000.0,
                "totalSize": 250_000.0,
            }
        }
        dashboard = {
            "wallets": [
                {
                    "address": "0x8def9f50456c6c4e37fa5d3d57f108ed23992dae",
                    "alias": "Loracle",
                    "positions": [
                        {"coin": "HYPE", "side": "Short", "positionValue": 12_000_000.0, "size": 300_000.0},
                    ],
                }
            ]
        }

        current = self.service.build_large_position_snapshot(dashboard)
        changes = self.service.build_large_position_alert_changes(previous, current)

        self.assertEqual(current, {})
        self.assertEqual(changes["newLargePositions"], [])
        self.assertEqual(changes["increasedLargePositions"], [])
        self.assertEqual(changes["closedLargePositions"], [])

    def test_new_large_position_alert_requires_open_fill_inside_five_minutes(self) -> None:
        now_ms = 1_700_000_000_000
        current = {
            "wallet:BTC:long": {
                "address": "wallet",
                "alias": "wallet",
                "coin": "BTC",
                "side": "long",
                "totalValue": 1_200_000.0,
                "totalSize": 12.0,
            }
        }
        stale_fill = {"wallet:BTC:long:add": {"price": 100000.0, "size": 12.0, "latestTime": now_ms - 6 * 60 * 1000}}
        fresh_fill = {"wallet:BTC:long:add": {"price": 100000.0, "size": 12.0, "latestTime": now_ms - 5 * 60 * 1000}}

        stale_changes = self.service.build_large_position_alert_changes({}, current, stale_fill, now_ms=now_ms)
        fresh_changes = self.service.build_large_position_alert_changes({}, current, fresh_fill, now_ms=now_ms)

        self.assertEqual(stale_changes["newLargePositions"], [])
        self.assertEqual(len(fresh_changes["newLargePositions"]), 1)
        self.assertEqual(fresh_changes["newLargePositions"][0]["entryPriceSource"], "fill")

    def test_large_position_changes_use_recent_fill_add_price(self) -> None:
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
                "totalSize": 115.0,
            }
        }

        added, increased, closed = self.service.summarize_large_position_changes(
            previous,
            current,
            {"wallet:BTC:long:add": {"price": 72909.0, "size": 15.0}},
        )

        self.assertEqual(added, [])
        self.assertEqual(len(increased), 1)
        self.assertEqual(increased[0]["addPrice"], 72909.0)
        self.assertEqual(increased[0]["addValue"], 1_093_635.0)
        self.assertEqual(increased[0]["addPriceSource"], "fill")
        self.assertEqual(closed, [])

    def test_large_position_changes_use_recent_fill_close_price(self) -> None:
        previous = {
            "wallet:ETH:short": {
                "address": "wallet",
                "alias": "wallet",
                "coin": "ETH",
                "side": "short",
                "totalValue": 900000.0,
                "totalSize": 300.0,
            }
        }

        added, increased, closed = self.service.summarize_large_position_changes(
            previous,
            {},
            {"wallet:ETH:short:close": {"price": 2345.0, "size": 300.0}},
        )

        self.assertEqual(added, [])
        self.assertEqual(increased, [])
        self.assertEqual(len(closed), 1)
        self.assertEqual(closed[0]["closePrice"], 2345.0)
        self.assertEqual(closed[0]["closePriceSource"], "fill")

    def test_recent_fill_price_map_filters_before_last_check(self) -> None:
        dashboard = {
            "wallets": [
                {
                    "address": "wallet",
                    "recentFills": [
                        {"coin": "BTC", "direction": "Open Long", "price": 70000.0, "size": 1.0, "time": 10},
                        {"coin": "BTC", "direction": "Open Long", "price": 73000.0, "size": 2.0, "time": 20},
                        {"coin": "ETH", "direction": "Close Short", "price": 2400.0, "size": 3.0, "time": 20},
                    ],
                }
            ]
        }

        fill_prices = self.service.build_recent_fill_price_map(dashboard, since_ms=10)

        self.assertEqual(fill_prices["wallet:BTC:long:add"]["price"], 73000.0)
        self.assertEqual(fill_prices["wallet:ETH:short:close"]["price"], 2400.0)

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
                "totalSize": 115.0,
            }
        }

        added, increased, closed = self.service.summarize_large_position_changes(previous, current)

        self.assertEqual(added, [])
        self.assertEqual(len(increased), 1)
        self.assertEqual(increased[0]["sizeIncrease"], 15.0)
        self.assertAlmostEqual(increased[0]["addPrice"], 86086.95652174)
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
