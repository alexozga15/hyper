import json
import os
import random
import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path
from typing import Any

from coinmarketman import CoinMarketManApiError
from server import (
    ALERTS_FILE,
    DORMANT_WALLET_MAX_IDLE_MS,
    FRESH_ACTIVITY_DIAGNOSTIC_WINDOWS_MS,
    WALLET_SIGNAL_ACTIVITY_WINDOW_MS,
    ELITE_WALLET_OVERRIDES,
    HyperliquidClient,
    POSITION_INCREASE_ALERT_MIN_DELTA,
    RANKING_WINDOW_MS,
    RECENT_FILL_ALERT_LIMIT,
    SHADOW_SIGNAL_OUTCOME_MAX_RECORDS,
    SHADOW_SIGNAL_OUTCOME_RETENTION_MS,
    SIGNAL_ROUND_TRIP_COST_PCT,
    TrackedWallet,
    WALLETS_FILE,
    WALLET_LIVE_FILL_LOOKBACK_MS,
    WALLET_RECENT_FILL_CONSUMER_WINDOW_MS,
    WalletStore,
    WalletTrackerService,
    build_wallet_quality_rank,
    current_time_ms,
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
        self.assertEqual(strong["score"], 70.0)
        losing = build_wallet_quality_rank(80, 20, -50_000, 100_000)
        self.assertEqual(losing["label"], "Cold")
        self.assertEqual(losing["pnlReturnPct"], -50.0)

    def test_wallet_quality_rank_carries_window_trusted_flag(self) -> None:
        """windowTrusted travels with the score it describes, defaulting True

        so every existing caller (which never passes window_trusted) is
        unaffected, without changing how the score itself is computed.
        """
        default = build_wallet_quality_rank(70, 20, 20_000, 100_000)
        self.assertTrue(default["windowTrusted"])
        untrusted = build_wallet_quality_rank(70, 20, 20_000, 100_000, window_trusted=False)
        self.assertFalse(untrusted["windowTrusted"])
        self.assertEqual(untrusted["score"], default["score"])

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

    def test_wallet_quality_rank_does_not_penalize_margin_usage(self) -> None:
        kwargs = {
            "hit_rate_30d": 90,
            "closed_trade_count_30d": 30,
            "pnl_30d": 40_000,
            "gross_profit_30d": 60_000,
            "gross_loss_30d": 10_000,
            "max_drawdown_pct": 5,
            "unrealized_pnl": 5_000,
        }
        low_margin = build_wallet_quality_rank(90, 20, 30_000, 100_000, margin_usage_pct=10, **kwargs)
        high_margin = build_wallet_quality_rank(90, 20, 30_000, 100_000, margin_usage_pct=95, **kwargs)

        self.assertEqual(low_margin["score"], high_margin["score"])
        self.assertEqual(low_margin["eliteEligible"], high_margin["eliteEligible"])

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
        self.assertIn("BTC LONG: 3 wallets", message)
        self.assertIn("Commodities", message)
        self.assertIn("OIL SHORT: 3 wallets", message)
        self.assertIn("Stocks and indices", message)
        self.assertIn("EWY LONG: 3 wallets", message)
        # The rendered score was the next line's number rescaled by the
        # cycle's strongest row, so it is gone from the display entirely.
        self.assertNotIn("Confidence", message)
        self.assertIn("Where wallets are crowded", message)
        self.assertNotIn("Strongest wallet agreement", message)
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

    def test_monthly_quality_gate_requires_repeatable_positive_results(self) -> None:
        eligible = {
            "address": "0x1111111111111111111111111111111111111111",
            "qualityClosedEvents30d": 8,
            "qualityNetPnl30d": 20_000.0,
            "qualityProfitFactor30d": 1.6,
            "qualityTopWinConcentrationPct": 45.0,
            "qualityHoldout6dEvents": 2,
            "qualityHoldout6dNetPnl": 1_000.0,
            "realizedPnl30d": 20_000.0,
            "unrealizedPnl": 0.0,
        }

        self.assertTrue(self.service.is_monthly_quality_eligible(eligible))
        for field, bad_value in (
            ("qualityClosedEvents30d", 4),
            ("qualityNetPnl30d", -1.0),
            ("qualityProfitFactor30d", 1.2),
            ("qualityTopWinConcentrationPct", 60.0),
            ("qualityHoldout6dNetPnl", -1.0),
        ):
            with self.subTest(field=field):
                candidate = {**eligible, field: bad_value}
                self.assertFalse(self.service.is_monthly_quality_eligible(candidate))

    def test_monthly_quality_gate_denies_promotion_on_untrusted_window(self) -> None:
        """Unlike wallet_conviction_weight/is_wallet_quarantined, which refuse

        to judge (neutral/False) on an untrusted window, this gate is a
        positive selection into the top-conviction cohort: unknown data must
        not earn promotion, so an otherwise-eligible wallet with a capped and
        poorly-covered window is denied, not passed through.
        """
        eligible = {
            "address": "0x1111111111111111111111111111111111111111",
            "qualityClosedEvents30d": 8,
            "qualityNetPnl30d": 20_000.0,
            "qualityProfitFactor30d": 1.6,
            "qualityTopWinConcentrationPct": 45.0,
            "qualityHoldout6dEvents": 2,
            "qualityHoldout6dNetPnl": 1_000.0,
            "realizedPnl30d": 20_000.0,
            "unrealizedPnl": 0.0,
        }
        untrusted = {
            **eligible,
            "qualityWindowTruncated": True,
            "qualityWindowCoverageMs": 2 * 60 * 60 * 1000,
        }
        self.assertFalse(self.service.is_monthly_quality_eligible(untrusted))

        well_covered = {
            **eligible,
            "qualityWindowTruncated": True,
            "qualityWindowCoverageMs": 25 * 24 * 60 * 60 * 1000,
        }
        self.assertTrue(self.service.is_monthly_quality_eligible(well_covered))

    def test_monthly_cohort_demotes_wallet_that_fails_quality_gate(self) -> None:
        address = "0x1111111111111111111111111111111111111111"
        wallet = {
            "address": address,
            "recentWinRateRank": {"score": 100.0, "label": "Elite"},
            "qualityClosedEvents30d": 10,
            "qualityNetPnl30d": 10_000.0,
            "qualityProfitFactor30d": 2.0,
            "qualityTopWinConcentrationPct": 70.0,
            "qualityHoldout6dEvents": 1,
            "qualityHoldout6dNetPnl": 1_000.0,
            "realizedPnl30d": 10_000.0,
            "unrealizedPnl": 0.0,
        }

        selected, cohort = self.service.resolve_monthly_top_conviction_cohort(
            [wallet],
            {"topConvictionWallets": {"month": "2026-07", "addresses": [address]}},
            month_key="2026-07",
        )

        self.assertEqual(selected, set())
        self.assertEqual(cohort["demoted"], [address])

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
        # The HYPE wallet is not holding-only, but its last fill is 20 days
        # old, so the dormancy filter drops it even though the stale-position
        # rule would have let it through.
        self.assertNotIn("HYPE:long", consensus_keys)
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

        self.assertIn("BTC LONG: 3 wallets, 3 pos | $2.4M open", message)

    def test_build_sentiment_summary_emits_high_conviction_signals(self) -> None:
        now_ms = 1_700_000_000_000
        snapshots = [
            {
                "address": "0x1111111111111111111111111111111111111111",
                "alias": "One",
                "positions": [{"coin": "BTC", "side": "Long", "positionValue": 500000, "size": 10}],
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
                "positions": [{"coin": "BTC", "side": "Long", "positionValue": 500000, "size": 10}],
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
                "address": "0x3333333333333333333333333333333333333333",
                "alias": "Three",
                "positions": [{"coin": "BTC", "side": "Long", "positionValue": 500000, "size": 10}],
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
                "address": "0x6666666666666666666666666666666666666666",
                "alias": "Six",
                "positions": [{"coin": "BTC", "side": "Long", "positionValue": 300000, "size": 6}],
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
        self.assertEqual(summary["signals"][0]["freshAddVwap"], 50000.0)
        self.assertEqual(summary["signals"][0]["markPrice"], 50000.0)

    def test_signal_rejection_blocks_extended_price_from_fresh_vwap(self) -> None:
        reasons = self.service.signal_rejection_reasons(
            {"freshAddVwap": 100.0, "markPrice": 102.0, "entryDistancePct": 2.0, "maxEntryDistancePct": 1.5},
            90.0,
        )

        self.assertIn("extended_from_fresh_vwap", reasons)

    def test_signal_rejection_requires_three_net_fresh_wallets_and_blocks_opposition(self) -> None:
        base = {
            "independentWalletCount": 4,
            "netIndependentWalletCount": 3,
            "netIndependentWeightedWalletCount": 2.0,
            "verifiedFreshIndependentWalletCount": 3,
            "netFreshIndependentWalletCount": 3,
            "oppositeVerifiedFreshIndependentWalletCount": 0,
            "independentTopWalletCount": 2,
            "freshAddVwap": 100.0,
            "markPrice": 100.0,
            "entryDistancePct": 0.0,
            "maxEntryDistancePct": 1.5,
        }

        self.assertEqual(self.service.signal_rejection_reasons(base, 80.0), [])
        self.assertIn(
            "weak_fresh_net",
            self.service.signal_rejection_reasons({**base, "netFreshIndependentWalletCount": 2}, 80.0),
        )
        self.assertIn(
            "opposite_fresh_flow",
            self.service.signal_rejection_reasons(
                {**base, "oppositeVerifiedFreshIndependentWalletCount": 2}, 80.0
            ),
        )

    def test_fresh_signal_flow_requires_500k_per_wallet_inside_the_window(self) -> None:
        now_ms = 1_700_000_000_000
        # Derived from the window rather than hardcoded: this wallet exists to
        # sit just outside it, and pinning it to a literal age quietly turns it
        # into an inside-the-window wallet whenever the window widens.
        stale_minutes = WALLET_SIGNAL_ACTIVITY_WINDOW_MS // 60_000 + 1
        snapshots = []
        for index, (value, age_minutes) in enumerate(
            ((500_000.0, 1), (500_000.0, 5), (499_999.0, 2), (500_000.0, stale_minutes)),
            start=1,
        ):
            size = value / 50_000.0
            snapshots.append(
                {
                    "address": f"0x{index:040x}",
                    "positions": [{"coin": "BTC", "side": "Long", "positionValue": value, "size": size}],
                    "recentFills": [
                        {
                            "coin": "BTC",
                            "direction": "Increase Long",
                            "price": 50_000.0,
                            "size": size,
                            "time": now_ms - age_minutes * 60_000,
                        }
                    ],
                }
            )

        with patch("server.current_time_ms", return_value=now_ms):
            summary = self.service.build_sentiment_summary(snapshots, min_wallets=4)

        self.assertEqual(summary["consensus"][0]["verifiedFreshIndependentWalletCount"], 2)
        self.assertEqual(summary["signals"], [])

    def test_candidate_uses_aggregate_500k_flow_and_one_top_wallet_is_watch(self) -> None:
        now_ms = 1_700_000_000_000
        addresses = [f"0x{index:040x}" for index in range(1, 4)]
        snapshots = []
        for index, address in enumerate(addresses):
            value = 200_000.0 + index * 10_000.0
            size = value / 50_000.0
            snapshots.append(
                {
                    "address": address,
                    "positions": [
                        {"coin": "BTC", "side": "Long", "positionValue": value, "size": size}
                    ],
                    "recentFills": [
                        {
                            "coin": "BTC",
                            "direction": "Increase Long",
                            "price": 50_000.0,
                            "size": size,
                            "time": now_ms - (index + 1) * 60_000,
                        }
                    ],
                }
            )

        with patch("server.current_time_ms", return_value=now_ms):
            summary = self.service.build_sentiment_summary(
                snapshots,
                min_wallets=4,
                top_wallet_addresses={addresses[0]},
            )

        self.assertEqual(summary["consensus"], [])
        self.assertEqual(summary["candidateSignalCount"], 1)
        candidate = summary["candidateSignals"][0]
        self.assertEqual(candidate["candidateTier"], "watch")
        self.assertEqual(candidate["independentWalletCount"], 3)
        self.assertEqual(candidate["independentTopWalletCount"], 1)
        self.assertEqual(candidate["freshNotional"], 630_000.0)
        self.assertEqual(candidate["freshAddVwap"], 50_000.0)

    def test_candidate_two_top_wallets_or_cmm_promotes_actionable(self) -> None:
        base = {
            "coin": "BTC",
            "side": "long",
            "candidateTier": "watch",
            "candidateReason": "one_top_wallet",
            "independentTopWalletCount": 1,
            "freshNotional": 600_000.0,
        }
        cmm_promoted = self.service.apply_cmm_confirmation_to_summary(
            {"signals": [], "candidateSignals": [base]},
            {
                "enabled": True,
                "generatedAt": "2026-08-02T00:00:00Z",
                "signals": [
                    {"coin": "BTC", "side": "long", "probabilityScore": 70.0, "cohortCount": 3}
                ],
            },
        )["candidateSignals"][0]
        top_promoted = self.service.apply_cmm_confirmation_to_summary(
            {
                "signals": [],
                "candidateSignals": [
                    {
                        **base,
                        "candidateTier": "actionable",
                        "candidateReason": "two_top_wallets",
                        "independentTopWalletCount": 2,
                    }
                ],
            },
            {"enabled": True, "signals": []},
        )["candidateSignals"][0]

        self.assertEqual(cmm_promoted["candidateTier"], "actionable")
        self.assertEqual(cmm_promoted["candidateReason"], "cmm_confirmed")
        self.assertEqual(cmm_promoted["cmmSnapshotGeneratedAt"], "2026-08-02T00:00:00Z")
        self.assertEqual(top_promoted["candidateTier"], "actionable")
        self.assertEqual(top_promoted["candidateReason"], "two_top_wallets")

    def test_candidate_below_threshold_cmm_score_is_recorded_not_zeroed(self) -> None:
        base = {"coin": "HYPE", "side": "long", "candidateTier": "watch", "candidateReason": "one_top_wallet"}

        result = self.service.apply_cmm_confirmation_to_summary(
            {"signals": [], "candidateSignals": [base]},
            {
                "enabled": True,
                "generatedAt": "2026-08-02T00:00:00Z",
                "signals": [],
                "scoredProbabilities": {"cmm:HYPE:long": 27.7},
            },
        )["candidateSignals"][0]

        self.assertEqual(result["cmmScoreStatus"], "scored")
        self.assertEqual(result["cmmScoredProbability"], 27.7)
        # The legacy zero-fabricating field is untouched: HYPE never entered
        # cmm_by_key (it scored below min_probability), so this stays 0.0.
        self.assertEqual(result["cmmProbabilityScore"], 0.0)

    def test_candidate_absent_from_present_scored_map_gets_no_opinion(self) -> None:
        base = {"coin": "DOGE", "side": "long", "candidateTier": "watch", "candidateReason": "one_top_wallet"}

        result = self.service.apply_cmm_confirmation_to_summary(
            {"signals": [], "candidateSignals": [base]},
            {
                "enabled": True,
                "generatedAt": "2026-08-02T00:00:00Z",
                "signals": [],
                "scoredProbabilities": {"cmm:HYPE:long": 27.7},
            },
        )["candidateSignals"][0]

        self.assertEqual(result["cmmScoreStatus"], "no_opinion")
        self.assertIsNone(result["cmmScoredProbability"])

    def test_candidate_with_no_cmm_snapshot_is_not_consulted(self) -> None:
        base = {"coin": "HYPE", "side": "long", "candidateTier": "watch", "candidateReason": "one_top_wallet"}

        result = self.service.apply_cmm_confirmation_to_summary(
            {"signals": [], "candidateSignals": [base]},
            {"enabled": False, "error": "Missing COINMARKETMAN_API_TOKEN", "signals": []},
        )["candidateSignals"][0]

        self.assertEqual(result["cmmScoreStatus"], "not_consulted")
        self.assertIsNone(result["cmmScoredProbability"])

    def test_candidate_from_pre_deploy_cache_without_scored_map_is_unknown(self) -> None:
        # A cmm_summary written by the previous version of the code never had a
        # "scoredProbabilities" key. Every cache entry looks like this for up to
        # CMM_SIGNAL_CACHE_TTL_MINUTES (60 minutes) after this change deploys, so
        # this state must be distinguishable from a genuine "no opinion" scan.
        base = {"coin": "HYPE", "side": "long", "candidateTier": "watch", "candidateReason": "one_top_wallet"}
        legacy_cmm_summary = {
            "enabled": True,
            "generatedAt": "2026-08-02T00:00:00Z",
            "signals": [],
        }
        self.assertNotIn("scoredProbabilities", legacy_cmm_summary)

        result = self.service.apply_cmm_confirmation_to_summary(
            {"signals": [], "candidateSignals": [base]},
            legacy_cmm_summary,
        )["candidateSignals"][0]

        self.assertEqual(result["cmmScoreStatus"], "unknown")
        self.assertIsNone(result["cmmScoredProbability"])

    def test_candidate_opposite_fresh_flow_is_blocked(self) -> None:
        now_ms = 1_700_000_000_000
        snapshots = []
        for index in range(1, 4):
            snapshots.append(
                {
                    "address": f"0x{index:040x}",
                    "positions": [
                        {"coin": "ETH", "side": "Long", "positionValue": 200_000.0, "size": 100.0}
                    ],
                    "recentFills": [
                        {
                            "coin": "ETH",
                            "direction": "Increase Long",
                            "price": 2_000.0,
                            "size": 100.0,
                            "time": now_ms - 60_000,
                        }
                    ],
                }
            )
        snapshots.append(
            {
                "address": "0x9999999999999999999999999999999999999999",
                "positions": [
                    {"coin": "ETH", "side": "Short", "positionValue": 100_000.0, "size": 50.0}
                ],
                "recentFills": [
                    {
                        "coin": "ETH",
                        "direction": "Increase Short",
                        "price": 2_000.0,
                        "size": 50.0,
                        "time": now_ms - 60_000,
                    }
                ],
            }
        )

        with patch("server.current_time_ms", return_value=now_ms):
            summary = self.service.build_sentiment_summary(
                snapshots,
                min_wallets=4,
                top_wallet_addresses={snapshots[0]["address"], snapshots[1]["address"]},
            )

        candidate = summary["candidateSignals"][0]
        self.assertEqual(candidate["candidateTier"], "blocked")
        self.assertEqual(candidate["candidateReason"], "opposite_fresh_flow")
        self.assertEqual(candidate["oppositeFreshIndependentWalletCount"], 1)

    def test_candidate_outcomes_snapshot_evidence_and_measure_4h_net_return(self) -> None:
        started_at = 1_700_000_000_000
        candidate = {
            "coin": "BTC",
            "marketCoin": "BTC",
            "side": "long",
            "status": "NEW",
            "firstSeenAt": started_at,
            "markPrice": 100.0,
            "freshAddVwap": 99.0,
            "freshNotional": 600_000.0,
            "freshWalletAddresses": ["0x1", "0x2", "0x3"],
            "topWalletAddresses": ["0x1"],
            "independentWalletCount": 3,
            "independentTopWalletCount": 1,
            "candidateTier": "watch",
            "topConvictionMonth": "2026-08",
            "cmmConfirmation": "unconfirmed",
            "cmmProbabilityScore": 0.0,
            "cmmSnapshotGeneratedAt": "2026-08-02T00:00:00Z",
        }
        records = self.service.update_candidate_signal_outcomes(
            {},
            {
                "candidateSignals": [candidate],
                "positionMarks": [{"coin": "BTC", "side": "long", "markPrice": 100.0}],
            },
            now_ms=started_at,
        )
        measured = self.service.update_candidate_signal_outcomes(
            records,
            {
                "candidateSignals": [],
                "positionMarks": [{"coin": "BTC", "side": "long", "markPrice": 110.0}],
            },
            now_ms=started_at + 4 * 60 * 60 * 1000,
        )

        record = next(iter(measured.values()))
        self.assertEqual(record["topConvictionMonth"], "2026-08")
        self.assertEqual(record["cmmSnapshotGeneratedAt"], "2026-08-02T00:00:00Z")
        self.assertEqual(record["outcomes"]["4h"]["grossReturnPct"], 10.0)
        self.assertEqual(record["outcomes"]["4h"]["netReturnPct"], 9.8)
        self.assertNotIn("12h", record["outcomes"])

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
        self.assertIn("insufficient_verified_activity", self.service.signal_rejection_reasons(summary["consensus"][0], probability))

    def test_recent_position_add_metrics_by_window_matches_single_window_calls(self) -> None:
        # Diagnostic-only helper (see FRESH_ACTIVITY_DIAGNOSTIC_WINDOWS_MS):
        # a single pass over recentFills must return exactly what calling
        # recent_position_add_metrics once per window would, for a wallet
        # whose fills straddle all three window boundaries.
        now_ms = 1_700_000_000_000
        wallet = {
            "recentFills": [
                # 30 min ago - inside 2h, 8h, 24h.
                {"coin": "BTC", "direction": "Increase Long", "price": 50_000.0, "size": 1.0, "time": now_ms - 30 * 60 * 1000},
                # 5h ago - outside 2h, inside 8h and 24h.
                {"coin": "BTC", "direction": "Increase Long", "price": 50_000.0, "size": 2.0, "time": now_ms - 5 * 60 * 60 * 1000},
                # 20h ago - outside 2h and 8h, inside 24h.
                {"coin": "BTC", "direction": "Increase Long", "price": 50_000.0, "size": 3.0, "time": now_ms - 20 * 60 * 60 * 1000},
                # 30h ago - outside all three windows.
                {"coin": "BTC", "direction": "Increase Long", "price": 50_000.0, "size": 4.0, "time": now_ms - 30 * 60 * 60 * 1000},
            ],
        }
        position = {"coin": "BTC", "side": "Long"}

        combined = self.service.recent_position_add_metrics_by_window(
            wallet, position, now_ms=now_ms, windows_ms=FRESH_ACTIVITY_DIAGNOSTIC_WINDOWS_MS
        )

        self.assertEqual(set(combined.keys()), set(FRESH_ACTIVITY_DIAGNOSTIC_WINDOWS_MS.keys()))
        for label, window_ms in FRESH_ACTIVITY_DIAGNOSTIC_WINDOWS_MS.items():
            expected = self.service.recent_position_add_metrics(
                wallet, position, now_ms=now_ms, window_ms=window_ms
            )
            self.assertEqual(combined[label], expected, f"mismatch for window {label}")

    def test_consensus_fresh_add_values_by_window_group_by_correlation(self) -> None:
        # Two wallets sharing an identical recent-fill fingerprint (3 shared
        # "Increase Long" events in the same 5-minute buckets) correlate into
        # one group, so their fresh-add value must be summed into a single
        # diagnostic entry, not reported as two.
        now_ms = 1_700_000_000_000
        shared_times = [now_ms - 10 * 60 * 1000, now_ms - 70 * 60 * 1000, now_ms - 130 * 60 * 1000]

        def correlated_wallet(address: str) -> dict[str, Any]:
            return {
                "address": address,
                "positions": [{"coin": "BTC", "side": "Long", "positionValue": 300.0, "size": 3.0}],
                "recentFills": [
                    {"coin": "BTC", "direction": "Increase Long", "price": 100.0, "size": 1.0, "time": t}
                    for t in shared_times
                ],
            }

        snapshots = [
            correlated_wallet(f"0x{1:040x}"),
            correlated_wallet(f"0x{2:040x}"),
            {
                "address": f"0x{3:040x}",
                "positions": [{"coin": "BTC", "side": "Long", "positionValue": 1_000_000.0, "size": 10.0}],
                "recentFills": [
                    {
                        "coin": "BTC",
                        "direction": "Increase Long",
                        "price": 100_000.0,
                        "size": 10.0,
                        "time": now_ms - 5 * 60 * 1000,
                    }
                ],
            },
        ]

        with patch("server.current_time_ms", return_value=now_ms):
            summary = self.service.build_sentiment_summary(snapshots, min_wallets=3)

        item = summary["consensus"][0]
        self.assertEqual(item["independentWalletCount"], 2)
        self.assertEqual(
            item["freshAddValuesByWindow"],
            {"2h": [1_000_000, 400], "8h": [1_000_000, 600], "24h": [1_000_000, 600]},
        )

    def test_diagnostic_windows_capture_adds_below_fresh_wallet_flow_min_value(self) -> None:
        # $1,000 adds are far below FRESH_WALLET_FLOW_MIN_VALUE ($500k) and so
        # must never count toward verifiedFreshIndependentWalletCount, but the
        # entire point of the diagnostic windows is to surface exactly this
        # sub-threshold distribution.
        now_ms = 1_700_000_000_000
        snapshots = [
            {
                "address": f"0x{index:040x}",
                "positions": [{"coin": "BTC", "side": "Long", "positionValue": 1_000.0, "size": 0.02}],
                "recentFills": [
                    {
                        "coin": "BTC",
                        "direction": "Increase Long",
                        "price": 50_000.0,
                        "size": 0.02,
                        "time": now_ms - 10 * 60 * 1000,
                    }
                ],
            }
            for index in (1, 2, 3)
        ]

        with patch("server.current_time_ms", return_value=now_ms):
            summary = self.service.build_sentiment_summary(snapshots, min_wallets=3)

        item = summary["consensus"][0]
        self.assertEqual(item["verifiedFreshIndependentWalletCount"], 0)
        self.assertEqual(
            item["freshAddValuesByWindow"],
            {"2h": [1000, 1000, 1000], "8h": [1000, 1000, 1000], "24h": [1000, 1000, 1000]},
        )

    def test_signal_rejection_reasons_unchanged_by_diagnostic_windows(self) -> None:
        # Regression pin: the diagnostic freshAddValuesByWindow field must not
        # move signal_rejection_reasons output. Any future gating drift on
        # this exact fixture should fail this assertion loudly.
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
        item = summary["consensus"][0]

        self.assertEqual(
            item["freshAddValuesByWindow"],
            {"2h": [], "8h": [], "24h": []},
        )
        probability = self.service.signal_probability_score(item)
        self.assertEqual(
            self.service.signal_rejection_reasons(item, probability),
            ["insufficient_verified_activity", "weak_fresh_net", "missing_fresh_vwap"],
        )

    def test_unreliable_fill_data_does_not_count_as_verified_activity(self) -> None:
        item = {
            "walletCount": 4,
            "independentWalletCount": 4,
            "netWalletCount": 4,
            "netIndependentWalletCount": 4,
            "netWeightedWalletCount": 4.0,
            "netIndependentWeightedWalletCount": 4.0,
            "independentWeightedWalletCount": 4.0,
            "recentAddWalletCount": 0,
            "freshActivityWalletCount": 0,
            "verifiedFreshIndependentWalletCount": 0,
            "independentTopWalletCount": 4,
            "fillQualityUnknownWalletCount": 4,
        }

        self.assertIn("insufficient_verified_activity", self.service.signal_rejection_reasons(item, 80.0))

    def test_reliable_inactive_holders_are_excluded_from_conviction(self) -> None:
        snapshots = [
            {
                "address": f"0x{index:040x}",
                "holdingOnly30d": True,
                "fills30d": 0,
                "closedTrades30d": 0,
                "positions": [{"coin": "BTC", "side": "Long", "positionValue": 1_000_000.0}],
                "recentFills": [],
                "dataQuality": {"fillsOk": True, "fillsDegraded": False},
            }
            for index in range(1, 5)
        ]

        summary = self.service.build_sentiment_summary(snapshots, min_wallets=1)

        self.assertEqual(summary["consensus"], [])

    def test_unreliable_fill_data_keeps_positions_in_conviction(self) -> None:
        snapshots = [
            {
                "address": f"0x{index:040x}",
                "holdingOnly30d": True,
                "fills30d": 0,
                "closedTrades30d": 0,
                "positions": [{"coin": "BTC", "side": "Long", "positionValue": 1_000_000.0}],
                "recentFills": [],
                "dataQuality": {"fillsOk": False, "fillsDegraded": False},
            }
            for index in range(1, 5)
        ]

        summary = self.service.build_sentiment_summary(snapshots, min_wallets=4)

        self.assertEqual(summary["consensus"][0]["coin"], "BTC")
        self.assertEqual(summary["consensus"][0]["fillQualityUnknownWalletCount"], 4)

    def test_wallet_correlation_groups_reduce_independent_votes(self) -> None:
        now_ms = 1_700_000_000_000
        shared_fills = [
            {"coin": "BTC", "direction": "Increase Long", "time": now_ms - offset}
            for offset in (60_000, 360_000, 660_000)
        ]
        snapshots = [
            {"address": "0x1111111111111111111111111111111111111111", "recentFills": shared_fills},
            {"address": "0x2222222222222222222222222222222222222222", "recentFills": shared_fills},
            {"address": "0x3333333333333333333333333333333333333333", "recentFills": []},
        ]

        groups = self.service.build_wallet_correlation_groups(snapshots)

        self.assertEqual(
            groups["0x1111111111111111111111111111111111111111"],
            groups["0x2222222222222222222222222222222222222222"],
        )

    def test_correlation_groups_merge_wallets_sharing_a_burst_on_one_coin(self) -> None:
        now_ms = 1_700_000_000_000
        shared_fills = [
            {"coin": "BTC", "direction": "Increase Long", "time": now_ms - index * 300_000}
            for index in range(8)
        ]
        left_only = [
            {"coin": "ETH", "direction": "Increase Long", "time": now_ms - 10_000_000 - index * 300_000}
            for index in range(4)
        ]
        right_only = [
            {"coin": "SOL", "direction": "Increase Short", "time": now_ms - 20_000_000 - index * 300_000}
            for index in range(20)
        ]
        snapshots = [
            {"address": "0x1111111111111111111111111111111111111111", "recentFills": [*shared_fills, *left_only]},
            {"address": "0x2222222222222222222222222222222222222222", "recentFills": [*shared_fills, *right_only]},
        ]

        groups = self.service.build_wallet_correlation_groups(snapshots)

        self.assertEqual(
            groups["0x1111111111111111111111111111111111111111"],
            groups["0x2222222222222222222222222222222222222222"],
        )

    def test_correlation_groups_ignore_incidental_shared_events(self) -> None:
        now_ms = 1_700_000_000_000
        shared_fills = [
            {"coin": "BTC", "direction": "Increase Long", "time": now_ms - index * 300_000}
            for index in range(3)
        ]
        left_only = [
            {"coin": "ETH", "direction": "Increase Long", "time": now_ms - 10_000_000 - index * 300_000}
            for index in range(197)
        ]
        right_only = [
            {"coin": "SOL", "direction": "Increase Short", "time": now_ms - 10_000_000 - index * 300_000}
            for index in range(197)
        ]
        snapshots = [
            {"address": "0x1111111111111111111111111111111111111111", "recentFills": [*shared_fills, *left_only]},
            {"address": "0x2222222222222222222222222222222222222222", "recentFills": [*shared_fills, *right_only]},
        ]

        groups = self.service.build_wallet_correlation_groups(snapshots)

        self.assertNotEqual(
            groups["0x1111111111111111111111111111111111111111"],
            groups["0x2222222222222222222222222222222222222222"],
        )

    def test_current_cycle_fill_success_is_not_marked_fill_quality_unknown(self) -> None:
        wallet = TrackedWallet(address="0x1111111111111111111111111111111111111111", alias="", notes="", created_at="")
        cached = {
            "realizedPnl30d": 999.0,
            "recentWinRateRank": {"label": "Strong", "score": 70.0},
            "recentFills": [],
            "qualityRefreshedAt": "2026-07-21T10:00:00Z",
            "refreshAttemptedAtMs": 1,
        }
        state = {
            "marginSummary": {"accountValue": "1000000", "totalNtlPos": "0", "totalMarginUsed": "0"},
            "withdrawable": "1000000",
            "assetPositions": [],
        }

        with patch.object(
            self.service.client, "safe_subscribe_all_dexs_clearinghouse_state", return_value=state
        ), patch.object(
            self.service, "fetch_fills_result", return_value={"ok": True, "data": [], "error": ""}
        ), patch.object(
            self.service, "fetch_recent_fills_result", return_value={"ok": True, "data": [], "error": ""}
        ), patch.object(
            self.service, "fetch_open_orders_result", return_value={"ok": True, "data": [], "error": ""}
        ), patch.object(
            # The cached entry was written by an older cycle that failed here.
            self.service,
            "fetch_portfolio_result",
            return_value={"ok": False, "data": {}, "error": "HTTP 429"},
        ), patch.object(self.service, "fetch_wallet_role", return_value="user"):
            snapshot = self.service.fetch_wallet_snapshot(wallet, cached_snapshot=cached)

        quality = snapshot["dataQuality"]
        self.assertTrue(quality["fillsOk"])
        self.assertTrue(self.service.wallet_fill_data_reliable(snapshot))
        self.assertEqual(quality["fillsCheckedAt"], snapshot["fetchedAt"])
        self.assertTrue(quality["cachedQuality"]["used"])
        self.assertIn("realizedPnl30d", quality["cachedQuality"]["fields"])
        self.assertEqual(quality["cachedQuality"]["refreshedAt"], "2026-07-21T10:00:00Z")

    def test_skipped_quality_endpoints_are_not_reported_as_successful_fetches(self) -> None:
        wallet = TrackedWallet(address="0x1111111111111111111111111111111111111111", alias="", notes="", created_at="")
        cached = {"realizedPnl30d": 1.0, "recentFills": [], "qualityRefreshedAt": "2026-07-21T10:00:00Z"}
        state = {
            "marginSummary": {"accountValue": "1000000", "totalNtlPos": "0", "totalMarginUsed": "0"},
            "withdrawable": "1000000",
            "assetPositions": [],
        }

        with patch.object(
            self.service.client, "safe_subscribe_all_dexs_clearinghouse_state", return_value=state
        ), patch.object(
            self.service, "fetch_fills_result", return_value={"ok": True, "data": [], "error": ""}
        ), patch.object(
            self.service, "fetch_recent_fills_result", return_value={"ok": True, "data": [], "error": ""}
        ):
            snapshot = self.service.fetch_wallet_snapshot(
                wallet,
                full_quality_refresh=False,
                cached_snapshot=cached,
            )

        quality = snapshot["dataQuality"]
        self.assertTrue(quality["fillsOk"])
        self.assertIsNone(quality["portfolioOk"])
        self.assertIsNone(quality["ordersOk"])
        self.assertFalse(quality["portfolioFetched"])
        self.assertFalse(quality["ordersFetched"])

    def test_failed_quality_refresh_does_not_monopolise_the_refresh_rotation(self) -> None:
        wallets = [
            TrackedWallet(address=f"0x{index:040x}", alias="", notes="", created_at="")
            for index in range(1, 6)
        ]
        cached = {
            # Refresh keeps failing for this wallet, so it never gets a
            # successful "refreshedAtMs" - only the attempt marker.
            wallets[0].address: {"refreshAttemptedAtMs": 900},
            wallets[1].address: {"refreshedAtMs": 100},
            wallets[2].address: {"refreshedAtMs": 300},
            wallets[3].address: {"refreshedAtMs": 400},
            wallets[4].address: {"refreshedAtMs": 500},
        }

        selected = self.service.wallet_quality_refresh_addresses(wallets, cached)

        self.assertNotIn(wallets[0].address, selected)
        self.assertEqual(selected, {wallets[1].address, wallets[2].address, wallets[3].address})

    def test_dashboard_records_failed_refresh_attempts_in_quality_cache(self) -> None:
        snapshots = [
            {
                "address": f"0x{index:040x}",
                "accountValue": 1_000_000.0,
                "totalNotional": 1_000_000.0,
                "unrealizedPnl": 0.0,
                "realizedPnl": 0.0,
                "recentFills": [],
                "positions": [{"coin": "BTC", "side": "Long", "positionValue": 1_000_000.0}],
                "exposure": {"long": 1_000_000.0, "short": 0.0, "net": 1_000_000.0},
                "cohorts": {"walletSize": "Whale", "profitability": "Profitable"},
                "dataQuality": {
                    "fillsOk": False,
                    "fillsDegraded": False,
                    "qualityRefreshAttempted": True,
                    "qualityRefreshSucceeded": False,
                },
            }
            for index in range(1, 3)
        ]
        wallets = [
            TrackedWallet(address=f"0x{index:040x}", alias="", notes="", created_at="")
            for index in range(1, len(snapshots) + 1)
        ]

        with tempfile.TemporaryDirectory() as tmp:
            cache_path = Path(tmp) / "wallet_quality_cache.json"
            with patch.object(self.service, "wallet_quality_cache_path", cache_path), patch(
                "server.RUNTIME_HEALTH_FILE", Path(tmp) / "runtime_health.json"
            ), patch.object(self.service.store, "list_wallets", return_value=wallets), patch.object(
                self.service.client, "list_markets", return_value=[]
            ), patch.object(self.service, "fetch_wallet_snapshot", side_effect=snapshots):
                self.service.dashboard()

            stored = json.loads(cache_path.read_text(encoding="utf-8"))

        entries = stored["wallets"]
        self.assertEqual(len(entries), 2)
        for entry in entries.values():
            self.assertGreater(int(entry["refreshAttemptedAtMs"]), 0)
            self.assertNotIn("refreshedAtMs", entry)

    def test_summary_reports_fill_quality_and_dormant_wallet_counts(self) -> None:
        now_ms = current_time_ms()
        position = {"coin": "BTC", "side": "Long", "positionValue": 1_000_000.0}
        snapshots = [
            {
                "address": "0x1111111111111111111111111111111111111111",
                "positions": [position],
                "recentFills": [{"coin": "BTC", "direction": "Increase Long", "time": now_ms - 3_600_000}],
                "dataQuality": {"fillsOk": True, "fillsDegraded": False},
            },
            {
                "address": "0x2222222222222222222222222222222222222222",
                "positions": [position],
                "recentFills": [{"coin": "BTC", "direction": "Increase Long", "time": now_ms - 30 * 86_400_000}],
                "dataQuality": {"fillsOk": True, "fillsDegraded": False},
            },
            {
                "address": "0x3333333333333333333333333333333333333333",
                "positions": [position],
                "recentFills": [],
                "dataQuality": {"fillsOk": False, "fillsDegraded": False},
            },
        ]

        summary = self.service.build_sentiment_summary(snapshots, min_wallets=1)

        self.assertEqual(summary["fillQualityUnknownWalletCount"], 1)
        self.assertEqual(
            summary["fillQualityUnknownWalletAddresses"],
            ["0x3333333333333333333333333333333333333333"],
        )
        self.assertEqual(summary["dormantWalletCount"], 2)
        # Only the wallet with usable fills proving no recent trading is
        # dropped; the one whose fills could not be read still counts.
        self.assertEqual(summary["excludedDormantWalletCount"], 1)
        self.assertEqual(summary["consensus"][0]["walletCount"], 2)

        message = self.service.build_summary_message(summary, min_wallets=1)
        self.assertIn("No usable fill data: 1 wallets", message)
        self.assertIn("Fill fetch failed this cycle: 1 wallets", message)
        self.assertIn("No fills in the last 7 days: 2 wallets", message)
        self.assertIn("Dropped from consensus as dormant: 1 wallets", message)

    def test_summary_message_omits_zero_data_quality_counts(self) -> None:
        summary = {
            "overallBias": "mixed",
            "walletCount": 3,
            "fillQualityUnknownWalletCount": 0,
            "fillFetchFailedWalletCount": 0,
            "dormantWalletCount": 0,
        }

        message = self.service.build_summary_message(summary, min_wallets=1)

        self.assertNotIn("No usable fill data", message)
        self.assertNotIn("Fill fetch failed this cycle", message)
        self.assertNotIn("No fills in the last 7 days", message)
        self.assertNotIn("Dropped from consensus as dormant", message)

    @staticmethod
    def dormancy_snapshot(
        address: str,
        *,
        now_ms: int,
        last_fill_age_ms: int | None,
        value: float = 1_000_000.0,
        data_quality: dict[str, Any] | None = None,
        with_fills_key: bool = True,
    ) -> dict[str, Any]:
        """One wallet, one BTC long, one fill of a chosen age.

        ``last_fill_age_ms=None`` means an empty fill list, and
        ``with_fills_key=False`` drops the fill record entirely.
        """
        snapshot: dict[str, Any] = {
            "address": address,
            "alias": address[-4:],
            "positions": [{"coin": "BTC", "side": "Long", "positionValue": value, "size": 10.0}],
            "dataQuality": data_quality
            if data_quality is not None
            else {"fillsOk": True, "fillsUsable": True, "fillsDegraded": False},
        }
        if with_fills_key:
            snapshot["recentFills"] = (
                []
                if last_fill_age_ms is None
                else [
                    {
                        "coin": "BTC",
                        "direction": "Increase Long",
                        "price": 100_000.0,
                        "size": 10.0,
                        "time": now_ms - last_fill_age_ms,
                    }
                ]
            )
        return snapshot

    def test_dormant_wallet_is_dropped_from_consensus_counts(self) -> None:
        now_ms = 1_700_000_000_000
        day_ms = 24 * 60 * 60 * 1000
        dormant_address = "0x3333333333333333333333333333333333333333"
        snapshots = [
            self.dormancy_snapshot(
                "0x1111111111111111111111111111111111111111", now_ms=now_ms, last_fill_age_ms=day_ms
            ),
            self.dormancy_snapshot(
                "0x2222222222222222222222222222222222222222", now_ms=now_ms, last_fill_age_ms=2 * day_ms
            ),
            self.dormancy_snapshot(
                dormant_address, now_ms=now_ms, last_fill_age_ms=9 * day_ms, value=5_000_000.0
            ),
        ]
        top_wallets = {"0x1111111111111111111111111111111111111111", dormant_address}

        with patch("server.current_time_ms", return_value=now_ms):
            summary = self.service.build_sentiment_summary(
                snapshots, min_wallets=1, top_wallet_addresses=top_wallets
            )
            fresh_snapshots = [
                self.dormancy_snapshot(
                    "0x1111111111111111111111111111111111111111", now_ms=now_ms, last_fill_age_ms=day_ms
                ),
                self.dormancy_snapshot(
                    "0x2222222222222222222222222222222222222222", now_ms=now_ms, last_fill_age_ms=2 * day_ms
                ),
                self.dormancy_snapshot(
                    dormant_address, now_ms=now_ms, last_fill_age_ms=3 * day_ms, value=5_000_000.0
                ),
            ]
            fresh_summary = self.service.build_sentiment_summary(
                fresh_snapshots, min_wallets=1, top_wallet_addresses=top_wallets
            )

        item = summary["consensus"][0]
        fresh_item = fresh_summary["consensus"][0]
        self.assertEqual(item["coin"], "BTC")
        self.assertEqual(item["walletCount"], 2)
        self.assertEqual(item["independentWalletCount"], 2)
        self.assertEqual(item["topWalletCount"], 1)
        self.assertEqual(item["independentTopWalletCount"], 1)
        self.assertEqual(item["excludedDormantWalletCount"], 1)
        self.assertEqual(item["excludedDormantWalletValue"], 5_000_000.0)
        self.assertNotIn(
            dormant_address, [wallet["address"] for wallet in item["wallets"]]
        )
        # The same book with that wallet still trading keeps every count.
        self.assertEqual(fresh_item["walletCount"], 3)
        self.assertEqual(fresh_item["independentWalletCount"], 3)
        self.assertEqual(fresh_item["topWalletCount"], 2)
        self.assertEqual(fresh_item["excludedDormantWalletCount"], 0)
        self.assertLess(item["weightedWalletCount"], fresh_item["weightedWalletCount"])
        self.assertLess(
            item["independentWeightedWalletCount"], fresh_item["independentWeightedWalletCount"]
        )
        # Their capital is still in the book.
        self.assertEqual(item["totalValue"], fresh_item["totalValue"])
        self.assertEqual(summary["longExposure"], fresh_summary["longExposure"])
        self.assertEqual(summary["longWalletCount"], 3)
        self.assertEqual(summary["walletCount"], 3)
        self.assertEqual(summary["excludedDormantWalletCount"], 1)
        self.assertEqual(summary["excludedDormantWalletAddresses"], [dormant_address])

    def test_unassessable_fill_data_never_makes_a_wallet_dormant(self) -> None:
        now_ms = 1_700_000_000_000
        day_ms = 24 * 60 * 60 * 1000
        snapshots = [
            self.dormancy_snapshot(
                "0x1111111111111111111111111111111111111111", now_ms=now_ms, last_fill_age_ms=day_ms
            ),
            # Fetch failed and nothing usable was cached: no evidence either way.
            self.dormancy_snapshot(
                "0x2222222222222222222222222222222222222222",
                now_ms=now_ms,
                last_fill_age_ms=None,
                data_quality={"fillsOk": False, "fillsFetchOk": False, "fillsUsable": False},
            ),
            # Degraded fills are unusable too.
            self.dormancy_snapshot(
                "0x3333333333333333333333333333333333333333",
                now_ms=now_ms,
                last_fill_age_ms=20 * day_ms,
                data_quality={"fillsOk": True, "fillsUsable": True, "fillsDegraded": True},
            ),
            # No fill record at all is missing data, not proven silence.
            self.dormancy_snapshot(
                "0x4444444444444444444444444444444444444444",
                now_ms=now_ms,
                last_fill_age_ms=None,
                with_fills_key=False,
            ),
        ]

        with patch("server.current_time_ms", return_value=now_ms):
            summary = self.service.build_sentiment_summary(snapshots, min_wallets=1)

        item = summary["consensus"][0]
        self.assertEqual(item["walletCount"], 4)
        self.assertEqual(item["excludedDormantWalletCount"], 0)
        self.assertEqual(summary["excludedDormantWalletCount"], 0)
        for snapshot in snapshots[1:]:
            self.assertFalse(self.service.is_wallet_dormant(snapshot, now_ms=now_ms))

    def test_coin_drops_out_when_dormant_wallets_break_the_threshold(self) -> None:
        now_ms = 1_700_000_000_000
        day_ms = 24 * 60 * 60 * 1000
        snapshots = [
            self.dormancy_snapshot(
                f"0x{index}{'0' * 39}", now_ms=now_ms, last_fill_age_ms=index * day_ms
            )
            for index in range(1, 5)
        ]
        snapshots.append(
            self.dormancy_snapshot(
                "0x5555555555555555555555555555555555555555", now_ms=now_ms, last_fill_age_ms=8 * day_ms
            )
        )

        with patch("server.current_time_ms", return_value=now_ms):
            summary = self.service.build_sentiment_summary(snapshots, min_wallets=5)
            kept = self.service.build_sentiment_summary(snapshots, min_wallets=4)

        self.assertEqual(summary["consensus"], [])
        self.assertEqual(summary["signals"], [])
        self.assertEqual(summary["excludedDormantWalletCount"], 1)
        self.assertEqual([item["coin"] for item in kept["consensus"]], ["BTC"])
        self.assertEqual(kept["consensus"][0]["walletCount"], 4)
        # Nothing downstream may assume a non-empty consensus.
        message = self.service.build_summary_message(summary, min_wallets=5)
        self.assertIn("- None", message)

    def test_dormancy_window_is_env_overridable(self) -> None:
        now_ms = 1_700_000_000_000
        day_ms = 24 * 60 * 60 * 1000
        self.assertEqual(DORMANT_WALLET_MAX_IDLE_MS, 7 * 24 * 60 * 60 * 1000)
        snapshots = [
            self.dormancy_snapshot(
                "0x1111111111111111111111111111111111111111", now_ms=now_ms, last_fill_age_ms=day_ms
            ),
            self.dormancy_snapshot(
                "0x2222222222222222222222222222222222222222", now_ms=now_ms, last_fill_age_ms=3 * day_ms
            ),
        ]

        with patch("server.current_time_ms", return_value=now_ms):
            with patch.dict(os.environ, {"DORMANT_WALLET_MAX_IDLE_MS": str(2 * day_ms)}):
                tightened = self.service.build_sentiment_summary(snapshots, min_wallets=1)
            with patch.dict(os.environ, {"DORMANT_WALLET_MAX_IDLE_MS": "0"}):
                stale = self.service.build_sentiment_summary(
                    [
                        self.dormancy_snapshot(
                            "0x1111111111111111111111111111111111111111",
                            now_ms=now_ms,
                            last_fill_age_ms=90 * day_ms,
                        )
                    ],
                    min_wallets=1,
                )
            default = self.service.build_sentiment_summary(snapshots, min_wallets=1)

        self.assertEqual(tightened["consensus"][0]["walletCount"], 1)
        self.assertEqual(tightened["consensus"][0]["excludedDormantWalletCount"], 1)
        self.assertEqual(tightened["dormantWalletMaxIdleMs"], 2 * day_ms)
        self.assertEqual(default["consensus"][0]["walletCount"], 2)
        self.assertEqual(default["consensus"][0]["excludedDormantWalletCount"], 0)
        # Zero switches the exclusion off, however long the wallet has idled.
        self.assertEqual(stale["consensus"][0]["walletCount"], 1)
        self.assertEqual(stale["excludedDormantWalletCount"], 0)

    def test_position_lifecycle_preserves_verified_recent_add(self) -> None:
        now_ms = 1_700_000_000_000
        wallet = {
            "address": "0x1111111111111111111111111111111111111111",
            "dataQuality": {"fillsOk": True, "fillsDegraded": False},
            "recentFills": [],
        }
        position = {"coin": "BTC", "side": "Long", "positionValue": 1_000_000.0}
        lifecycle = {
            self.service.position_lifecycle_key(wallet["address"], "BTC", "long"): {"lastAddAt": now_ms - 60_000}
        }

        with patch("server.current_time_ms", return_value=now_ms):
            self.assertTrue(self.service.has_verified_recent_activity(wallet, position, lifecycle, now_ms=now_ms))

    def test_asset_quality_adjusts_wallet_weight(self) -> None:
        wallet = {
            "address": "0x1111111111111111111111111111111111111111",
            "recentWinRateRank": {"score": 65.0, "label": "Strong"},
            "assetQuality": {"BTC": {"closedTrades": 6, "winRate": 90.0}},
        }

        btc_weight = self.service.wallet_conviction_weight(wallet, set(), coin="BTC")
        eth_weight = self.service.wallet_conviction_weight(wallet, set(), coin="ETH")

        self.assertGreater(btc_weight, eth_weight)

    def test_backtest_tiers_and_global_caps_bound_wallet_weight(self) -> None:
        rank = {"score": 100.0, "label": "Elite"}
        elite = {
            "address": "0x8bae3527e5a33fa0cf184f37bc112d071463ab6d",
            "recentWinRateRank": rank,
        }
        review = {
            "address": "0x350e33a777d510616fbdb483d1de3b50d1edfcfb",
            "recentWinRateRank": rank,
        }
        standard = {
            "address": "0xa5fd942d4badbab4fe84a9e10f565dd40d5f15ff",
            "recentWinRateRank": rank,
        }
        ordinary = {
            "address": "0x1111111111111111111111111111111111111111",
            "recentWinRateRank": rank,
        }

        self.assertEqual(self.service.wallet_conviction_weight(elite, set()), 1.5)
        self.assertEqual(self.service.wallet_conviction_weight(standard, set()), 1.0)
        self.assertEqual(self.service.wallet_conviction_weight(review, set()), 0.5)
        self.assertEqual(self.service.wallet_conviction_weight(ordinary, {ordinary["address"]}), 1.5)

    def test_tracked_wallet_list_excludes_backtest_removals(self) -> None:
        removed = {
            "0xb3e475368ed0fa0ad23c04de0423d48a0758806f",
            "0x3d89bcea338f35edfaeb313b1c713978c6dceb14",
            "0x69906b0ed626ca01a4b7c001e5711e5714ccf207",
            "0x939f95036d2e7b6d7419ec072bf9d967352204d2",
            "0x99b1098d9d50aa076f78bd26ab22e6abd3710729",
            "0x091144e651b334341eabdbbbfed644ad0100023e",
            "0xdbcc96bcada067864902aad14e029fe7c422f147",
        }
        addresses = {wallet.address.lower() for wallet in WalletStore(Path(WALLETS_FILE)).list_wallets()}
        additions = {
            "0x1ce8ed87b7b4cb60f0cc3664bf1fe216163ff55a",
            "0x215b369a532dc84654c244449cb119986ceaf603",
            "0x1e771e1b95c86491299d6e2a5c3b3842d03b552e",
            "0xd487e26c62ed8c28ce3cc70b5791e501c2934982",
            "0xfc98b6ec7f59ea13354bae6171a9120692fb8777",
        }

        self.assertEqual(len(addresses), 33)
        self.assertTrue(additions.issubset(addresses))
        self.assertTrue(removed.isdisjoint(addresses))

    def test_dashboard_marks_globally_empty_fills_as_degraded(self) -> None:
        snapshots = [
            {
                "address": f"0x{index:040x}",
                "accountValue": 1_000_000.0,
                "totalNotional": 1_000_000.0,
                "unrealizedPnl": 0.0,
                "realizedPnl": 0.0,
                "holdingOnly30d": True,
                "recentFills": [],
                "positions": [{"coin": "BTC", "side": "Long", "positionValue": 1_000_000.0}],
                "exposure": {"long": 1_000_000.0, "short": 0.0, "net": 1_000_000.0},
                "cohorts": {"walletSize": "Whale", "profitability": "Profitable"},
                "dataQuality": {"fillsOk": True, "fillsDegraded": False},
            }
            for index in range(1, 6)
        ]

        wallets = [
            TrackedWallet(address=f"0x{index:040x}", alias="", notes="", created_at="")
            for index in range(1, len(snapshots) + 1)
        ]
        with patch.object(self.service.store, "list_wallets", return_value=wallets), patch.object(
            self.service.client, "list_markets", return_value=[]
        ):
            with patch.object(self.service, "fetch_wallet_snapshot", side_effect=snapshots):
                dashboard = self.service.dashboard()

        self.assertTrue(dashboard["totals"]["dataQuality"]["fillsGloballyDegraded"])
        self.assertEqual(dashboard["totals"]["holdingOnly30dWallets"], 0)
        self.assertTrue(all(wallet["dataQuality"]["fillsDegraded"] for wallet in dashboard["wallets"]))

    def test_quality_refresh_rotation_prioritizes_missing_then_oldest_wallets(self) -> None:
        wallets = [
            TrackedWallet(address=f"0x{index:040x}", alias="", notes="", created_at="")
            for index in range(1, 6)
        ]
        cached = {
            wallets[0].address: {"refreshedAtMs": 400},
            wallets[1].address: {"refreshedAtMs": 100},
            wallets[2].address: {"refreshedAtMs": 300},
            wallets[3].address: {"refreshedAtMs": 200},
        }

        selected = self.service.wallet_quality_refresh_addresses(wallets, cached)

        self.assertEqual(selected, {wallets[1].address, wallets[3].address, wallets[4].address})

    def test_failed_full_quality_refresh_preserves_last_good_metrics(self) -> None:
        wallet = TrackedWallet(address="0x1111111111111111111111111111111111111111", alias="", notes="", created_at="")
        cached = {
            "realizedPnl30d": 123_456.0,
            "recentWinRateRank": {"label": "Elite", "score": 81.0},
            "performance": {"month": {"pnl": 123_456.0}},
            "recentFills": [],
            "qualityRefreshedAt": "2026-07-21T10:00:00Z",
        }
        state = {
            "marginSummary": {"accountValue": "1000000", "totalNtlPos": "0", "totalMarginUsed": "0"},
            "withdrawable": "1000000",
            "assetPositions": [],
        }

        with patch.object(
            self.service.client, "safe_subscribe_all_dexs_clearinghouse_state", return_value=state
        ), patch.object(
            self.service, "fetch_fills_result", return_value={"ok": False, "data": [], "error": "HTTP 429"}
        ), patch.object(
            self.service, "fetch_recent_fills_result", return_value={"ok": False, "data": [], "error": "HTTP 429"}
        ), patch.object(
            self.service, "fetch_open_orders_result", return_value={"ok": True, "data": [], "error": ""}
        ), patch.object(
            self.service, "fetch_portfolio_result", return_value={"ok": False, "data": {}, "error": "HTTP 429"}
        ), patch.object(self.service, "fetch_wallet_role", return_value="user"):
            snapshot = self.service.fetch_wallet_snapshot(wallet, cached_snapshot=cached)

        self.assertEqual(snapshot["realizedPnl30d"], 123_456.0)
        self.assertEqual(snapshot["recentWinRateRank"]["label"], "Elite")
        self.assertTrue(snapshot["dataQuality"]["qualityCacheHit"])
        self.assertFalse(snapshot["dataQuality"]["qualityRefreshSucceeded"])

    def test_incremental_snapshot_skips_expensive_quality_endpoints(self) -> None:
        wallet = TrackedWallet(address="0x1111111111111111111111111111111111111111", alias="", notes="", created_at="")
        cached = {
            "realizedPnl30d": 50_000.0,
            "recentWinRateRank": {"label": "Balanced", "score": 60.0},
            "performance": {},
            "recentFills": [],
            "qualityRefreshedAt": "2026-07-21T10:00:00Z",
        }
        state = {
            "marginSummary": {"accountValue": "1000000", "totalNtlPos": "0", "totalMarginUsed": "0"},
            "withdrawable": "1000000",
            "assetPositions": [],
        }

        with patch.object(
            self.service.client, "safe_subscribe_all_dexs_clearinghouse_state", return_value=state
        ), patch.object(
            self.service,
            "fetch_fills_result",
            return_value={
                "ok": True,
                "data": [
                    {
                        "coin": "BTC",
                        "dir": "Open Long",
                        "px": "70000",
                        "sz": "1",
                        "closedPnl": "0",
                        "fee": "1",
                        "time": 1_700_000_000_000,
                    }
                ],
                "error": "",
            },
        ), patch.object(
            self.service, "fetch_recent_fills_result", return_value={"ok": True, "data": [], "error": ""}
        ), patch.object(self.service, "fetch_open_orders_result") as orders, patch.object(
            self.service, "fetch_portfolio_result"
        ) as portfolio, patch.object(self.service, "fetch_wallet_role") as role:
            snapshot = self.service.fetch_wallet_snapshot(
                wallet,
                full_quality_refresh=False,
                cached_snapshot=cached,
            )

        orders.assert_not_called()
        portfolio.assert_not_called()
        role.assert_not_called()
        self.assertEqual(snapshot["realizedPnl30d"], 50_000.0)
        self.assertTrue(snapshot["dataQuality"]["fillsOk"])
        self.assertTrue(snapshot["dataQuality"]["qualityCacheHit"])
        self.assertFalse(snapshot["holdingOnly30d"])

    def test_recent_fills_keep_newest_fills_regardless_of_api_ordering(self) -> None:
        wallet = TrackedWallet(address="0x1111111111111111111111111111111111111111", alias="", notes="", created_at="")
        state = {
            "marginSummary": {"accountValue": "1000000", "totalNtlPos": "0", "totalMarginUsed": "0"},
            "withdrawable": "1000000",
            "assetPositions": [],
        }
        now_ms = current_time_ms()
        total_fills = RECENT_FILL_ALERT_LIMIT * 2 + 37
        fills = [
            {
                "coin": "BTC",
                "dir": "Open Long",
                "px": "70000",
                "sz": "1",
                "closedPnl": "100",
                "fee": "1",
                "time": now_ms - (index + 1) * 60_000,
            }
            for index in range(total_fills)
        ]
        shuffled = list(fills)
        random.Random(7).shuffle(shuffled)
        expected_times = sorted((int(fill["time"]) for fill in fills), reverse=True)[:RECENT_FILL_ALERT_LIMIT]

        with patch.object(
            self.service.client, "safe_subscribe_all_dexs_clearinghouse_state", return_value=state
        ), patch.object(
            self.service,
            "fetch_fills_result",
            return_value={"ok": True, "data": shuffled, "error": ""},
        ), patch.object(
            self.service, "fetch_recent_fills_result", return_value={"ok": True, "data": [], "error": ""}
        ), patch.object(self.service, "fetch_open_orders_result", return_value={"ok": True, "data": [], "error": ""}), patch.object(
            self.service, "fetch_portfolio_result", return_value={"ok": True, "data": {}, "error": ""}
        ), patch.object(self.service, "fetch_wallet_role", return_value="user"):
            snapshot = self.service.fetch_wallet_snapshot(wallet)

        recent_fills = snapshot["recentFills"]
        self.assertEqual(len(recent_fills), RECENT_FILL_ALERT_LIMIT)
        self.assertEqual([int(fill["time"]) for fill in recent_fills], expected_times)
        self.assertEqual(
            set(recent_fills[0]),
            {"coin", "direction", "price", "size", "closedPnl", "fee", "time"},
        )
        # Aggregates must still see every fill, not just the retained newest slice.
        self.assertEqual(snapshot["fills30d"], total_fills)
        self.assertEqual(snapshot["closedTrades30d"], total_fills)
        self.assertAlmostEqual(snapshot["realizedPnl30d"], 100.0 * total_fills)

    def _snapshot_with_fill_pages(
        self,
        windowed: dict[str, Any],
        recent: dict[str, Any],
    ) -> dict[str, Any]:
        wallet = TrackedWallet(address="0x1111111111111111111111111111111111111111", alias="", notes="", created_at="")
        state = {
            "marginSummary": {"accountValue": "1000000", "totalNtlPos": "0", "totalMarginUsed": "0"},
            "withdrawable": "1000000",
            "assetPositions": [],
        }
        with patch.object(
            self.service.client, "safe_subscribe_all_dexs_clearinghouse_state", return_value=state
        ), patch.object(
            self.service, "fetch_fills_result", return_value=windowed
        ), patch.object(
            self.service, "fetch_recent_fills_result", return_value=recent
        ), patch.object(
            self.service, "fetch_open_orders_result", return_value={"ok": True, "data": [], "error": ""}
        ), patch.object(
            self.service, "fetch_portfolio_result", return_value={"ok": True, "data": {}, "error": ""}
        ), patch.object(self.service, "fetch_wallet_role", return_value="user"):
            return self.service.fetch_wallet_snapshot(wallet)

    @staticmethod
    def _raw_fill(time_ms: int, **overrides: Any) -> dict[str, Any]:
        fill = {
            "coin": "BTC",
            "dir": "Open Long",
            "px": "70000",
            "sz": "1",
            "closedPnl": "0",
            "fee": "1",
            "time": int(time_ms),
        }
        fill.update(overrides)
        return fill

    def test_recent_fills_include_live_page_when_time_window_page_is_truncated(self) -> None:
        # userFillsByTime caps at 2000 rows ascending from startTime, so a busy
        # wallet's page is exhausted days before "now" and its current activity
        # is invisible. The userFills page is the only source of fresh fills.
        now_ms = current_time_ms()
        truncated_ascending = [
            self._raw_fill(now_ms - (6 * 24 * 60 * 60 * 1000) + index * 60_000, tid=index)
            for index in range(2000)
        ]
        live_page = [
            self._raw_fill(now_ms - 18 * 60_000, tid=900_001, coin="ETH"),
            self._raw_fill(now_ms - 45 * 60_000, tid=900_002, coin="SOL"),
        ]

        snapshot = self._snapshot_with_fill_pages(
            {"ok": True, "data": truncated_ascending, "error": ""},
            {"ok": True, "data": list(reversed(live_page)), "error": ""},
        )

        recent_fills = snapshot["recentFills"]
        times = [int(fill["time"]) for fill in recent_fills]
        self.assertEqual(times, sorted(times, reverse=True))
        self.assertEqual([fill["coin"] for fill in recent_fills[:2]], ["ETH", "SOL"])
        self.assertLess(now_ms - times[0], 60 * 60 * 1000)
        self.assertEqual(snapshot["daysSinceLastFill"], 0.0)

    def test_recent_fills_deduplicate_overlap_between_fill_endpoints(self) -> None:
        now_ms = current_time_ms()
        shared_with_tid = self._raw_fill(now_ms - 30 * 60_000, tid=4242)
        shared_without_tid = self._raw_fill(now_ms - 40 * 60_000, coin="SOL", oid=77)
        live_only = self._raw_fill(now_ms - 5 * 60_000, coin="ETH", tid=4243)

        snapshot = self._snapshot_with_fill_pages(
            {"ok": True, "data": [shared_without_tid, shared_with_tid], "error": ""},
            {
                "ok": True,
                "data": [live_only, dict(shared_with_tid), dict(shared_without_tid)],
                "error": "",
            },
        )

        recent_fills = snapshot["recentFills"]
        self.assertEqual([fill["coin"] for fill in recent_fills], ["ETH", "BTC", "SOL"])
        self.assertEqual(
            [int(fill["time"]) for fill in recent_fills],
            [int(live_only["time"]), int(shared_with_tid["time"]), int(shared_without_tid["time"])],
        )

    def test_recent_fills_degrade_to_time_window_page_when_live_call_fails(self) -> None:
        now_ms = current_time_ms()
        windowed = [self._raw_fill(now_ms - 2 * 60 * 60 * 1000, tid=1)]

        snapshot = self._snapshot_with_fill_pages(
            {"ok": True, "data": windowed, "error": ""},
            {"ok": False, "data": [], "error": "HTTP 429"},
        )

        self.assertEqual(len(snapshot["recentFills"]), 1)
        self.assertTrue(snapshot["dataQuality"]["fillsOk"])
        self.assertFalse(snapshot["dataQuality"]["recentFillsOk"])
        self.assertEqual(snapshot["dataQuality"]["recentFillsError"], "HTTP 429")
        self.assertTrue(snapshot["dataQuality"]["qualityRefreshSucceeded"])

    def test_live_fill_page_does_not_change_thirty_day_aggregates(self) -> None:
        now_ms = current_time_ms()
        windowed = [self._raw_fill(now_ms - 60 * 60 * 1000, closedPnl="100", tid=1)]
        live_only = [
            self._raw_fill(now_ms - 60_000, closedPnl="500", tid=2),
            self._raw_fill(now_ms - 40 * 24 * 60 * 60 * 1000, closedPnl="900", tid=3),
        ]

        baseline = self._snapshot_with_fill_pages(
            {"ok": True, "data": windowed, "error": ""},
            {"ok": True, "data": [], "error": ""},
        )
        merged = self._snapshot_with_fill_pages(
            {"ok": True, "data": windowed, "error": ""},
            {"ok": True, "data": live_only, "error": ""},
        )

        for field in ("fills30d", "closedTrades30d", "realizedPnl30d", "grossProfit30d", "recentWins"):
            self.assertEqual(merged[field], baseline[field], field)
        self.assertEqual(merged["assetQuality"], baseline["assetQuality"])
        self.assertEqual(merged["qualityNetPnl30d"], baseline["qualityNetPnl30d"])
        # Only the freshness-facing view grows.
        self.assertEqual(len(baseline["recentFills"]), 1)
        self.assertEqual(len(merged["recentFills"]), 2)

    def test_live_fill_lookback_covers_widest_recent_fill_consumer_window(self) -> None:
        self.assertEqual(WALLET_RECENT_FILL_CONSUMER_WINDOW_MS, RANKING_WINDOW_MS)
        self.assertGreaterEqual(WALLET_LIVE_FILL_LOOKBACK_MS, RANKING_WINDOW_MS)

        wallet = TrackedWallet(address="0x1111111111111111111111111111111111111111", alias="", notes="", created_at="")
        state = {
            "marginSummary": {"accountValue": "1000000", "totalNtlPos": "0", "totalMarginUsed": "0"},
            "withdrawable": "1000000",
            "assetPositions": [],
        }
        captured: dict[str, int] = {}

        def fake_fills(address: str, start_time: int) -> dict[str, Any]:
            captured["startTime"] = int(start_time)
            return {"ok": True, "data": [], "error": ""}

        before_ms = current_time_ms()
        with patch.object(
            self.service.client, "safe_subscribe_all_dexs_clearinghouse_state", return_value=state
        ), patch.object(self.service, "fetch_fills_result", side_effect=fake_fills), patch.object(
            self.service, "fetch_recent_fills_result", return_value={"ok": True, "data": [], "error": ""}
        ):
            self.service.fetch_wallet_snapshot(
                wallet,
                full_quality_refresh=False,
                cached_snapshot={"recentFills": []},
            )

        # fetch_wallet_snapshot reads its own clock, so the requested startTime is
        # anchored a few milliseconds *after* before_ms. Comparing the two clock
        # reads exactly is a race; allow a few seconds of slack in both directions
        # while still proving the request covers at least RANKING_WINDOW_MS.
        tolerance_ms = 5_000
        start_time_ms = captured["startTime"]
        self.assertLessEqual(start_time_ms, before_ms - RANKING_WINDOW_MS + tolerance_ms)
        self.assertGreaterEqual(start_time_ms, before_ms - WALLET_LIVE_FILL_LOOKBACK_MS - tolerance_ms)

    def test_recent_add_threshold_uses_aggregate_of_fills(self) -> None:
        now_ms = 1_700_000_000_000
        position = {"coin": "BTC", "side": "Long", "positionValue": 100_000_000.0}
        small_fills = [
            {
                "coin": "BTC",
                "direction": "Open Long",
                "price": 100_000.0,
                "size": 1.0,
                "time": now_ms - (index + 1) * 60_000,
            }
            for index in range(12)
        ]
        wallet = {"recentFills": small_fills}

        # Twelve $100K fills: no single fill clears the $1M floor, but the
        # aggregate ($1.2M) does, so this is a real recent add.
        for fill in small_fills:
            self.assertLess(fill["price"] * fill["size"], POSITION_INCREASE_ALERT_MIN_DELTA)
        self.assertTrue(
            self.service.has_recent_position_fill(
                wallet, position, now_ms=now_ms, event="add", window_ms=RANKING_WINDOW_MS
            )
        )

        # Same fills, but only the ones inside the tight window are aggregated.
        self.assertFalse(
            self.service.has_recent_position_fill(
                wallet, position, now_ms=now_ms, event="add", window_ms=5 * 60 * 1000
            )
        )

        # Aggregate under the absolute floor still qualifies at >= 20% of the position.
        small_position = {"coin": "BTC", "side": "Long", "positionValue": 500_000.0}
        self.assertTrue(
            self.service.has_recent_position_fill(
                {"recentFills": small_fills[:1]},
                small_position,
                now_ms=now_ms,
                event="add",
                window_ms=RANKING_WINDOW_MS,
            )
        )

        # Under both the absolute floor and the relative share: no add.
        tiny = {
            "recentFills": [
                {
                    "coin": "BTC",
                    "direction": "Open Long",
                    "price": 100_000.0,
                    "size": 0.01,
                    "time": now_ms - 60_000,
                }
            ]
        }
        self.assertFalse(
            self.service.has_recent_position_fill(
                tiny, position, now_ms=now_ms, event="add", window_ms=RANKING_WINDOW_MS
            )
        )

    def test_recent_close_fill_keeps_any_match_semantics(self) -> None:
        now_ms = 1_700_000_000_000
        position = {"coin": "BTC", "side": "Long", "positionValue": 100_000_000.0}
        wallet = {
            "recentFills": [
                {
                    "coin": "BTC",
                    "direction": "Close Long",
                    "price": 100_000.0,
                    "size": 0.001,
                    "time": now_ms - 60_000,
                }
            ]
        }
        self.assertTrue(
            self.service.has_recent_position_fill(
                wallet, position, now_ms=now_ms, event="close", window_ms=RANKING_WINDOW_MS
            )
        )
        self.assertFalse(
            self.service.has_recent_position_fill(
                wallet, position, now_ms=now_ms, event="add", window_ms=RANKING_WINDOW_MS
            )
        )

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
                    "convictionScore": 98.0,
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
        self.assertEqual(changes["changedSignals"][0]["toProbabilityScore"], 98.0)

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
        self.assertIn("1. BUY BTC (LONG) - 94/100 confidence", message)
        self.assertIn("Support: 3 wallets", message)
        self.assertNotIn("$1.2M", message)

    def test_build_signals_message_formats_moni_social_context(self) -> None:
        summary = {
            "generatedAt": "2026-07-30T00:00:00Z",
            "signals": [
                {
                    "coin": "SOL",
                    "side": "short",
                    "action": "sell",
                    "walletCount": 4,
                    "probabilityScore": 82,
                    "moniSocialTrend": "rising",
                    "moniSocialPaceRatio": 2.1,
                }
            ],
        }
        message = self.service.build_signals_message(summary)
        self.assertIn("Social activity: rising (2.10x normal pace)", message)

    def test_build_signals_message_shows_watch_candidate_on_explicit_command(self) -> None:
        summary = {
            "generatedAt": "2026-08-02T00:00:00Z",
            "signals": [],
            "candidateSignals": [
                {
                    "coin": "ETH",
                    "side": "long",
                    "action": "buy",
                    "candidateTier": "watch",
                    "independentWalletCount": 3,
                    "independentTopWalletCount": 1,
                    "freshNotional": 650_000.0,
                    "freshAddVwap": 1_850.0,
                }
            ],
        }

        message = self.service.build_signals_message(summary)

        self.assertIn("Fresh candidates from the last 2 hours", message)
        self.assertIn("WATCH BUY ETH (LONG)", message)
        self.assertIn("3 wallets added $650K", message)

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
                            {
                                "segmentId": 9,
                                "count": 70,
                                "countLong": 7,
                                "totalValue": 7_000_000,
                                "totalLongValue": 700_000,
                                "totalShortValue": 6_300_000,
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
        self.assertTrue(all(segment in {7, 8} for _, segment in fake_client.metric_calls))

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

    def test_cmm_signal_derives_aggregate_entry_from_unrealized_pnl(self) -> None:
        components = [
            {
                "side": "long",
                "valueBias": 0.8,
                "countBias": 0.8,
                "weight": 1.0,
                "positionCount": 10,
                "totalValue": 1_200,
                "totalSize": 10,
                "unrealizedPnl": 200,
                "hasUnrealizedPnl": True,
            },
            {
                "side": "long",
                "valueBias": 0.7,
                "countBias": 0.7,
                "weight": 1.0,
                "positionCount": 8,
                "totalValue": 2_400,
                "totalSize": 20,
                "unrealizedPnl": 400,
                "hasUnrealizedPnl": True,
            },
        ]

        signal = self.service.score_cmm_components("BTC", components)

        self.assertIsNotNone(signal)
        self.assertEqual(signal["price"], 100.0)
        self.assertEqual(signal["priceSource"], "cohort-implied-entry")
        self.assertEqual(signal["entryCoveragePct"], 100.0)

    def test_cmm_position_entry_enrichment_uses_unique_open_positions(self) -> None:
        class FakeCmmClient:
            token = "token"

            def positions(self, **kwargs: Any) -> dict[str, Any]:
                self.kwargs = kwargs
                return {
                    "positions": [
                        {"id": "one", "coin": "BTC", "side": "long", "size": 2, "entryPrice": 90, "closeTime": None},
                        {"id": "two", "coin": "BTC", "side": "long", "size": 1, "entryPrice": 120, "closeTime": None},
                        {"id": "two", "coin": "BTC", "side": "long", "size": 1, "entryPrice": 120, "closeTime": None},
                        {"id": "closed", "coin": "BTC", "side": "long", "size": 10, "entryPrice": 1, "closeTime": "2026-06-20T10:00:00Z"},
                    ]
                }

        client = FakeCmmClient()
        self.service.cmm_client = client
        summary = {"enabled": True, "signals": [{"coin": "BTC", "side": "long", "price": 0.0}]}

        enriched = self.service.enrich_cmm_signals_with_position_entries(summary)
        signal = enriched["signals"][0]

        self.assertEqual(client.kwargs["coin"], "BTC")
        self.assertRegex(client.kwargs["start"], r"^\d{4}-\d{2}-\d{2}T.*Z$")
        self.assertRegex(client.kwargs["end"], r"^\d{4}-\d{2}-\d{2}T.*Z$")
        self.assertTrue(client.kwargs["open_only"])
        self.assertEqual(signal["price"], 100.0)
        self.assertEqual(signal["priceSource"], "position-vwap-entry")
        self.assertEqual(signal["entryPositionCount"], 2)

    def test_cmm_message_surfaces_position_entry_enrichment_errors(self) -> None:
        message = self.service.build_cmm_signals_message(
            {
                "enabled": True,
                "signals": [],
                "entryEnrichmentError": "BTC entries: CMM API returned HTTP 404",
            }
        )

        self.assertIn("Entry enrichment unavailable: BTC entries", message)

    def test_build_cmm_signal_summary_can_disable_trends_with_env(self) -> None:
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

        with patch.dict("os.environ", {"CMM_TREND_ENRICHMENT": "false"}, clear=False):
            summary = self.service.build_cmm_signal_summary()

        self.assertEqual(summary["signals"][0]["coin"], "AAVE")
        self.assertIsNone(summary["signals"][0]["trendScore"])
        self.assertFalse(summary["signals"][0]["trendAvailable"])

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

    def test_build_cached_cmm_signal_summary_honors_rate_limit_backoff(self) -> None:
        cached = {
            "enabled": True,
            "signals": [{"coin": "LINK", "side": "short"}],
            "generatedAt": "2026-06-20T00:00:00Z",
            "rateLimitedUntil": "2099-01-01T00:00:00Z",
        }

        with patch.object(self.service, "build_cmm_signal_summary") as live_summary:
            summary = self.service.build_cached_cmm_signal_summary({"cmmSignals": cached})

        live_summary.assert_not_called()
        self.assertTrue(summary["cacheHit"])
        self.assertTrue(summary["stale"])

    def test_build_cached_cmm_signal_summary_sets_rate_limit_backoff(self) -> None:
        cached = {
            "enabled": True,
            "signals": [{"coin": "LINK", "side": "short"}],
            "generatedAt": "2026-06-20T00:00:00Z",
        }
        limited = {"enabled": True, "signals": [], "rateLimited": True, "error": "HTTP 429", "generatedAt": now_iso()}

        with patch.object(self.service, "build_cmm_signal_summary", return_value=limited):
            summary = self.service.build_cached_cmm_signal_summary({"cmmSignals": cached})

        self.assertTrue(summary["stale"])
        self.assertTrue(summary["rateLimited"])
        self.assertGreater(summary["rateLimitedUntil"], now_iso())

    def test_cmm_signal_tier_requires_actionable_value(self) -> None:
        self.assertEqual(self.service.cmm_signal_tier(79, 866_000), "watch")
        self.assertEqual(self.service.cmm_signal_tier(79, 1_100_000), "actionable")
        self.assertEqual(self.service.cmm_signal_tier(86, 1_100_000), "alert")

    def _cmm_signal(self, coin: str, total_value: float, probability: float = 90.0) -> dict:
        return {
            "coin": coin,
            "side": "short",
            "action": "sell",
            "signalTier": "watch",
            "probabilityScore": probability,
            "cohortCount": 3,
            "valueBias": 0.8,
            "trendScore": 0,
            "trendAvailable": False,
            "contrarianScore": 0,
            "totalValue": total_value,
            "components": [{"segment": "Money Printer"}],
        }

    def test_build_cmm_signals_message_hides_cohorts_below_actionable_exposure(self) -> None:
        """Sub-$1M cohorts are capped at WATCH by cmm_signal_tier whatever they

        score, so a 93/100 cohort at $560K led the list wearing a WATCH label
        with nothing on screen to explain it. Those rows are not shown at all.
        """
        message = self.service.build_cmm_signals_message(
            {
                "enabled": True,
                "signals": [
                    self._cmm_signal("SMALL", 560_000, probability=93.0),
                    self._cmm_signal("BIG", 2_000_000, probability=75.0),
                ],
                "generatedAt": "2026-06-20T00:00:00Z",
            }
        )

        self.assertNotIn("SMALL", message)
        self.assertIn("BIG", message)

    def test_build_cmm_signals_message_says_so_when_every_cohort_is_too_small(self) -> None:
        message = self.service.build_cmm_signals_message(
            {
                "enabled": True,
                "signals": [self._cmm_signal("SMALL", 560_000, probability=93.0)],
                "generatedAt": "2026-06-20T00:00:00Z",
            }
        )

        self.assertNotIn("SMALL", message)
        self.assertIn("No cohort at $1.0M exposure or above", message)

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
                "trendAvailable": False,
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

        self.assertIn("Crypto", message)
        self.assertIn("Tracked wallets: 4 | Quality-adjusted support: 2.5", message)
        self.assertIn("Exposure: $2.0M", message)
        self.assertIn("Aggregate Entry: $123.45", message)
        # Trend history is absent here, and the probability already
        # renormalises for that, so the row must not carry a "not available"
        # placeholder - it is ten repetitions of nothing in the real message.
        self.assertNotIn("Trend confirmation", message)
        self.assertIn("Contrarian support: 0/100", message)
        self.assertIn("Tiers: WATCH 60+ | ACTIONABLE 70+ | ALERT 80+", message)
        self.assertIn("Listed from $1.0M cohort exposure", message)
        self.assertNotIn("Confidence levels:", message)
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

    def test_cmm_confirmation_keeps_native_wallet_signal_without_cmm(self) -> None:
        summary = {
            "signals": [
                {
                    "coin": "BTC",
                    "side": "long",
                    "probabilityScore": 90.0,
                    "walletCount": 4,
                    "independentWalletCount": 4,
                    "netIndependentWalletCount": 3,
                    "verifiedFreshIndependentWalletCount": 3,
                    "netFreshIndependentWalletCount": 3,
                    "oppositeVerifiedFreshIndependentWalletCount": 0,
                    "independentTopWalletCount": 2,
                }
            ],
            "signalCount": 1,
        }

        filtered = self.service.apply_cmm_confirmation_to_summary(
            summary,
            {"enabled": True, "signals": []},
            require_confirmation=True,
        )

        self.assertEqual(filtered["signals"][0]["cmmConfirmation"], "unconfirmed")
        self.assertEqual(filtered["signals"][0]["probabilityScore"], 90.0)

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

    def test_cmm_opposite_signal_at_70_is_hard_veto(self) -> None:
        wallet_signal = {
            "coin": "BTC",
            "side": "long",
            "probabilityScore": 95.0,
            "walletCount": 5,
            "independentWalletCount": 5,
            "netIndependentWalletCount": 4,
            "verifiedFreshIndependentWalletCount": 3,
            "netFreshIndependentWalletCount": 3,
            "oppositeVerifiedFreshIndependentWalletCount": 0,
            "independentTopWalletCount": 2,
        }
        filtered = self.service.apply_cmm_confirmation_to_summary(
            {"signals": [wallet_signal], "signalCount": 1},
            {
                "enabled": True,
                "signals": [{"coin": "BTC", "side": "short", "probabilityScore": 70.0}],
            },
            require_confirmation=True,
        )

        self.assertEqual(filtered["signals"], [])
        self.assertEqual(filtered["vetoedSignals"][0]["invalidationReason"], "cmm_conflict")

    def test_signal_lifecycle_confirms_new_wallet_and_expires_after_two_hours(self) -> None:
        now_ms = 1_700_000_000_000
        prior = {
            "signals": [
                {
                    "coin": "BTC",
                    "side": "long",
                    "probabilityScore": 80.0,
                    "walletCount": 4,
                    "freshWalletAddresses": ["0x1", "0x2", "0x3"],
                    "firstSeenAt": now_ms - 30 * 60_000,
                    "lastFreshAt": now_ms - 20 * 60_000,
                }
            ]
        }
        current_signal = {
            **prior["signals"][0],
            "freshWalletAddresses": ["0x1", "0x2", "0x3", "0x4"],
            "freshAddLatestTime": now_ms - 60_000,
        }

        confirmed = self.service.apply_signal_lifecycle(
            {"signals": [current_signal], "consensus": [current_signal]},
            prior,
            now_ms=now_ms,
        )
        self.assertEqual(confirmed["signals"][0]["status"], "CONFIRMED")

        expired = self.service.apply_signal_lifecycle(
            {
                "signals": [],
                "consensus": [
                    {
                        "coin": "BTC",
                        "side": "long",
                        "netIndependentWalletCount": 4,
                        "oppositeVerifiedFreshIndependentWalletCount": 0,
                    }
                ],
            },
            prior,
            now_ms=now_ms + 2 * 60 * 60 * 1000,
        )
        self.assertEqual(expired["signals"], [])
        self.assertEqual(expired["invalidatedSignals"][0]["invalidationReason"], "expired")

    def test_signal_realert_ignores_small_drift_but_accepts_new_fresh_wallet(self) -> None:
        prior_signal = {
            "coin": "BTC",
            "side": "long",
            "walletCount": 4,
            "probabilityScore": 80.0,
            "freshAddVwap": 100.0,
            "freshWalletAddresses": ["0x1", "0x2", "0x3"],
        }
        small_drift = {
            **prior_signal,
            "walletCount": 5,
            "probabilityScore": 90.0,
            "freshAddVwap": 100.5,
        }
        confirmed = {**small_drift, "freshWalletAddresses": ["0x1", "0x2", "0x3", "0x4"]}

        quiet = self.service.summarize_signal_changes(
            {"signals": [prior_signal]}, {"signals": [small_drift]}, track_hip3=False
        )
        noisy = self.service.summarize_signal_changes(
            {"signals": [prior_signal]}, {"signals": [confirmed]}, track_hip3=False
        )

        self.assertEqual(quiet["changedSignals"], [])
        self.assertEqual(noisy["changedSignals"][0]["addedFreshWallets"], ["0x4"])

    def test_signal_outcomes_record_direction_adjusted_horizons(self) -> None:
        started_at = 1_700_000_000_000
        new_summary = {
            "signals": [
                {
                    "coin": "BTC",
                    "side": "short",
                    "status": "NEW",
                    "firstSeenAt": started_at,
                    "markPrice": 100.0,
                    "freshAddVwap": 98.0,
                    "probabilityScore": 82.0,
                    "verifiedFreshIndependentWalletCount": 3,
                }
            ],
            "consensus": [{"coin": "BTC", "side": "short", "markPrice": 100.0}],
        }
        records = self.service.update_signal_outcomes({}, new_summary, now_ms=started_at)
        measured = self.service.update_signal_outcomes(
            records,
            {
                "signals": [],
                "consensus": [{"coin": "BTC", "side": "short", "markPrice": 95.0}],
            },
            now_ms=started_at + 60 * 60 * 1000,
        )

        record = next(iter(measured.values()))
        self.assertEqual(record["outcomes"]["15m"]["returnPct"], 5.0)
        self.assertEqual(record["outcomes"]["1h"]["returnPct"], 5.0)
        self.assertNotIn("4h", record["outcomes"])

    def test_signal_outcome_entry_is_mark_price_and_keeps_wallet_vwap(self) -> None:
        started_at = 1_700_000_000_000
        summary = {
            "signals": [
                {
                    "coin": "BTC",
                    "side": "long",
                    "status": "NEW",
                    "firstSeenAt": started_at,
                    # A follower enters here, not at the wallets' add VWAP.
                    "markPrice": 104.0,
                    "freshAddVwap": 100.0,
                    "entryDistancePct": 4.0,
                    "probabilityScore": 82.0,
                }
            ],
            "consensus": [{"coin": "BTC", "side": "long", "markPrice": 104.0}],
            "positionMarks": [
                {"coin": "BTC", "marketCoin": "BTC", "side": "long", "markPrice": 104.0}
            ],
        }
        records = self.service.update_signal_outcomes({}, summary, now_ms=started_at)
        record = next(iter(records.values()))

        self.assertEqual(record["entryPrice"], 104.0)
        self.assertEqual(record["walletVwap"], 100.0)
        self.assertEqual(record["marketCoin"], "BTC")
        self.assertFalse(record["shadow"])

    def test_signal_outcome_records_gross_and_net_returns(self) -> None:
        started_at = 1_700_000_000_000
        summary = {
            "signals": [
                {
                    "coin": "BTC",
                    "side": "long",
                    "status": "NEW",
                    "firstSeenAt": started_at,
                    "markPrice": 100.0,
                    "freshAddVwap": 100.0,
                    "probabilityScore": 82.0,
                }
            ],
            "consensus": [{"coin": "BTC", "side": "long", "markPrice": 100.0}],
        }
        records = self.service.update_signal_outcomes({}, summary, now_ms=started_at)
        measured = self.service.update_signal_outcomes(
            records,
            {"signals": [], "consensus": [{"coin": "BTC", "side": "long", "markPrice": 100.1}]},
            now_ms=started_at + 15 * 60 * 1000,
        )

        outcome = next(iter(measured.values()))["outcomes"]["15m"]
        self.assertEqual(outcome["grossReturnPct"], 0.1)
        self.assertEqual(
            outcome["netReturnPct"], round(0.1 - SIGNAL_ROUND_TRIP_COST_PCT, 3)
        )
        # A +0.1% move does not survive the round trip.
        self.assertLess(outcome["netReturnPct"], 0)
        self.assertEqual(outcome["priceSource"], "mark")

    def test_signal_outcome_falls_back_to_candle_when_coin_leaves_consensus(self) -> None:
        started_at = 1_700_000_000_000
        summary = {
            "signals": [
                {
                    "coin": "BTC",
                    "side": "long",
                    "status": "NEW",
                    "firstSeenAt": started_at,
                    "markPrice": 100.0,
                    "freshAddVwap": 100.0,
                    "probabilityScore": 82.0,
                }
            ],
            "consensus": [{"coin": "BTC", "side": "long", "markPrice": 100.0}],
            "positionMarks": [
                {"coin": "BTC", "marketCoin": "BTC", "side": "long", "markPrice": 100.0}
            ],
        }
        records = self.service.update_signal_outcomes({}, summary, now_ms=started_at)

        # The wallets closed out, so the coin is gone from consensus entirely.
        with patch.object(
            self.service.client,
            "safe_post_result",
            return_value={"ok": True, "data": [{"c": "90.0"}]},
        ) as post:
            measured = self.service.update_signal_outcomes(
                records,
                {"signals": [], "consensus": [], "positionMarks": []},
                now_ms=started_at + 15 * 60 * 1000,
            )

        outcome = next(iter(measured.values()))["outcomes"]["15m"]
        self.assertEqual(outcome["markPrice"], 90.0)
        self.assertEqual(outcome["grossReturnPct"], -10.0)
        self.assertEqual(outcome["priceSource"], "candle")
        self.assertEqual(post.call_count, 1)

    def test_signal_outcome_skips_network_when_no_horizon_is_due(self) -> None:
        started_at = 1_700_000_000_000
        summary = {
            "signals": [
                {
                    "coin": "BTC",
                    "side": "long",
                    "status": "NEW",
                    "firstSeenAt": started_at,
                    "markPrice": 100.0,
                    "probabilityScore": 82.0,
                }
            ],
            "consensus": [{"coin": "BTC", "side": "long", "markPrice": 100.0}],
        }
        records = self.service.update_signal_outcomes({}, summary, now_ms=started_at)

        with patch.object(self.service.client, "safe_post_result") as post:
            self.service.update_signal_outcomes(
                records,
                {"signals": [], "consensus": []},
                now_ms=started_at + 60 * 1000,
            )

        post.assert_not_called()

    def test_late_measurement_is_flagged_degraded_and_left_out_of_calibration(self) -> None:
        started_at = 1_700_000_000_000
        summary = {
            "signals": [
                {
                    "coin": "BTC",
                    "side": "long",
                    "status": "NEW",
                    "firstSeenAt": started_at,
                    "markPrice": 100.0,
                    "probabilityScore": 92.0,
                }
            ],
            "consensus": [{"coin": "BTC", "side": "long", "markPrice": 100.0}],
        }
        records = self.service.update_signal_outcomes({}, summary, now_ms=started_at)
        # Runner was down: the "1h" outcome is only measured six hours later.
        measured = self.service.update_signal_outcomes(
            records,
            {"signals": [], "consensus": [{"coin": "BTC", "side": "long", "markPrice": 110.0}]},
            now_ms=started_at + 6 * 60 * 60 * 1000,
        )

        outcome = next(iter(measured.values()))["outcomes"]["1h"]
        self.assertTrue(outcome["degraded"])
        self.assertEqual(outcome["measuredAfterMs"], 6 * 60 * 60 * 1000)
        self.assertEqual(outcome["horizonMs"], 60 * 60 * 1000)
        # On-time measurement is not flagged.
        self.assertFalse(measured[next(iter(measured))]["outcomes"]["4h"]["degraded"])

        calibration = self.service.build_signal_calibration(measured, horizon="1h")
        stats = calibration["groups"]["crypto"]["90"]
        self.assertEqual(stats["sample"], 0)
        self.assertEqual(stats["degradedSample"], 1)
        # The record itself is kept on disk, just excluded from the maths.
        self.assertIn("1h", next(iter(measured.values()))["outcomes"])

    def test_shadow_outcomes_cover_unpublished_sub_threshold_scores(self) -> None:
        started_at = 1_700_000_000_000
        consensus_item = {
            "coin": "ETH",
            "side": "long",
            "markPrice": 100.0,
            "freshAddVwap": 99.0,
            "convictionScore": 50.0,
            "verifiedFreshIndependentWalletCount": 1,
        }
        summary = {
            "signals": [],
            "consensus": [consensus_item],
            "positionMarks": [
                {"coin": "ETH", "marketCoin": "ETH", "side": "long", "markPrice": 100.0}
            ],
        }
        records = self.service.update_shadow_signal_outcomes({}, summary, now_ms=started_at)
        record = next(iter(records.values()))
        self.assertTrue(record["shadow"])
        self.assertFalse(record["published"])
        self.assertLess(record["rawProbabilityScore"], 70.0)

        measured = self.service.update_shadow_signal_outcomes(
            records,
            {
                "signals": [],
                "consensus": [{**consensus_item, "markPrice": 110.0}],
                "positionMarks": [],
            },
            now_ms=started_at + 4 * 60 * 60 * 1000,
        )
        calibration = self.service.build_signal_calibration({}, shadow_records=measured)
        buckets = calibration["groups"]["crypto"]
        # The published pipeline can only ever produce 70/80/90 buckets.
        self.assertTrue(all(int(bucket) < 70 for bucket in buckets))
        stats = next(iter(buckets.values()))
        self.assertEqual(stats["sample"], 1)
        self.assertEqual(stats["shadowSample"], 1)

    def test_shadow_record_copies_fresh_add_values_by_window_and_defaults_absent(self) -> None:
        started_at = 1_700_000_000_000
        with_windows = {
            "coin": "ETH",
            "side": "long",
            "markPrice": 100.0,
            "freshAddVwap": 99.0,
            "convictionScore": 50.0,
            "verifiedFreshIndependentWalletCount": 1,
            "freshAddValuesByWindow": {"2h": [1000], "8h": [1000, 500], "24h": [2000]},
        }
        without_windows = {
            "coin": "BTC",
            "side": "long",
            "markPrice": 100.0,
            "freshAddVwap": 99.0,
            "convictionScore": 50.0,
            "verifiedFreshIndependentWalletCount": 1,
        }
        summary = {
            "signals": [],
            "consensus": [with_windows, without_windows],
            "positionMarks": [
                {"coin": "ETH", "marketCoin": "ETH", "side": "long", "markPrice": 100.0},
                {"coin": "BTC", "marketCoin": "BTC", "side": "long", "markPrice": 100.0},
            ],
        }

        records = self.service.update_shadow_signal_outcomes({}, summary, now_ms=started_at)

        by_coin = {record["coin"]: record for record in records.values()}
        self.assertEqual(
            by_coin["ETH"]["freshAddValuesByWindow"],
            {"2h": [1000], "8h": [1000, 500], "24h": [2000]},
        )
        # Consensus items built before this field existed (or any item that
        # simply has none) must not require its presence.
        self.assertEqual(by_coin["BTC"]["freshAddValuesByWindow"], {})

    def test_shadow_outcomes_skip_published_signals_and_rearm_slowly(self) -> None:
        started_at = 1_700_000_000_000
        consensus = [
            {"coin": "ETH", "side": "long", "markPrice": 100.0, "convictionScore": 50.0},
            {"coin": "BTC", "side": "long", "markPrice": 100.0, "convictionScore": 50.0},
        ]
        summary = {
            "signals": [{"coin": "BTC", "side": "long"}],
            "consensus": consensus,
            "positionMarks": [],
        }
        records = self.service.update_shadow_signal_outcomes({}, summary, now_ms=started_at)
        self.assertEqual([record["coin"] for record in records.values()], ["ETH"])

        # A second cycle inside the re-arm window must not duplicate the record.
        again = self.service.update_shadow_signal_outcomes(
            records, summary, now_ms=started_at + 15 * 60 * 1000
        )
        self.assertEqual(len(again), 1)

    def test_shadow_outcomes_stay_within_retention_budget(self) -> None:
        now_ms = 1_700_000_000_000
        previous = {
            f"shadow:COIN{index}:long:{now_ms - index}": {
                "coin": f"COIN{index}",
                "side": "long",
                "signalKey": f"COIN{index}:long",
                "startedAt": now_ms - index,
                "entryPrice": 100.0,
                "probabilityScore": 40.0,
                "rawProbabilityScore": 40.0,
                "shadow": True,
                "outcomes": {},
            }
            for index in range(SHADOW_SIGNAL_OUTCOME_MAX_RECORDS + 250)
        }
        records = self.service.update_shadow_signal_outcomes(
            previous, {"signals": [], "consensus": []}, now_ms=now_ms
        )
        self.assertEqual(len(records), SHADOW_SIGNAL_OUTCOME_MAX_RECORDS)
        # The newest records survive the trim.
        self.assertIn(f"shadow:COIN0:long:{now_ms}", records)

    def test_shadow_outcomes_drop_records_older_than_the_retention_window(self) -> None:
        now_ms = 1_700_000_000_000
        fresh_at = now_ms - SHADOW_SIGNAL_OUTCOME_RETENTION_MS + 60_000
        stale_at = now_ms - SHADOW_SIGNAL_OUTCOME_RETENTION_MS - 60_000

        def record(coin: str, started_at: int) -> dict:
            return {
                "coin": coin,
                "side": "long",
                "signalKey": f"{coin}:long",
                "startedAt": started_at,
                "entryPrice": 100.0,
                "probabilityScore": 40.0,
                "rawProbabilityScore": 40.0,
                "shadow": True,
                "outcomes": {},
            }

        previous = {
            f"shadow:FRESH:long:{fresh_at}": record("FRESH", fresh_at),
            f"shadow:STALE:long:{stale_at}": record("STALE", stale_at),
        }
        with patch.object(self.service, "candidate_outcome_market_price", return_value=0.0):
            records = self.service.update_shadow_signal_outcomes(
                previous, {"signals": [], "consensus": []}, now_ms=now_ms
            )
        self.assertIn(f"shadow:FRESH:long:{fresh_at}", records)
        self.assertNotIn(f"shadow:STALE:long:{stale_at}", records)

    def test_calibration_prior_is_neutral_base_rate_not_the_bucket(self) -> None:
        records = {
            f"a{index}": {
                "coin": "BTC",
                "probabilityScore": 92.0,
                "outcomes": {"4h": {"netReturnPct": -1.0, "degraded": False}},
            }
            for index in range(10)
        }
        records.update(
            {
                f"b{index}": {
                    "coin": "BTC",
                    "probabilityScore": 45.0,
                    "outcomes": {"4h": {"netReturnPct": -1.0, "degraded": False}},
                }
                for index in range(10)
            }
        )
        calibration = self.service.build_signal_calibration(records)
        stats = calibration["groups"]["crypto"]["90"]

        self.assertEqual(calibration["baseRates"]["crypto"], 0.0)
        self.assertEqual(stats["baseRateProbability"], 0.0)
        # The old prior (bucket + 5, weight 12) would have produced ~51.8 here.
        self.assertEqual(stats["calibratedProbability"], 0.0)
        self.assertEqual(stats["confidenceLow"], 0.0)
        self.assertLess(stats["confidenceHigh"], 40.0)

    def test_calibration_reads_legacy_gross_only_records(self) -> None:
        # Records already on disk only carry the pre-cost "returnPct".
        records = {
            str(index): {
                "coin": "BTC",
                "probabilityScore": 82.0,
                "outcomes": {"4h": {"returnPct": 0.1, "measuredAt": 1}},
            }
            for index in range(4)
        }
        calibration = self.service.build_signal_calibration(records)
        stats = calibration["groups"]["crypto"]["80"]

        self.assertEqual(stats["sample"], 4)
        # +0.1% gross is a loss once the round trip is charged.
        self.assertEqual(stats["wins"], 0)

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

        self.assertIn("CMM market signals", message)
        self.assertIn("SELL ETH (SHORT)", message)

    def test_check_alerts_does_not_notify_on_cmm_only_signal(self) -> None:
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

        self.assertFalse(result["shouldNotify"])
        self.assertFalse(result["sent"])
        self.assertEqual(result["changes"]["addedCmmSignals"], [])
        send_telegram_message.assert_not_called()

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

        self.assertIn("Elite wallet pos", message)
        self.assertIn("Elite Trader (87.6/100", message)
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
        self.assertIn("Open pos now", message)
        self.assertIn("Crypto (3+ wallets, $1.0M+ combined)", message)
        self.assertIn("BTC LONG: 3 wallets, 3 pos | $1.4M open | weighted entry: $78,000", message)
        self.assertNotIn("ETH short", message)
        self.assertIn("Summary: 1 groups, 3 pos", message)

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

        self.assertIn("BTC LONG: 3 wallets, 3 pos | $1.2M open | average entry: $77,667", message)
        self.assertNotIn("weighted entry", message)

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
        self.assertIn("- No open pos", message)
        self.assertNotIn("CHIP short", message)
        self.assertIn("Summary: 0 groups, 0 pos", message)

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

        self.assertIn("- No open pos", message)
        self.assertNotIn("HYPE short", message)
        self.assertIn("Summary: 0 groups, 0 pos", message)

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

        self.assertIn("- No open pos", message)
        self.assertNotIn("BTC long", message)
        self.assertIn("Summary: 0 groups, 0 pos", message)

    def test_build_positions_message_includes_recent_add_vwap(self) -> None:
        now_ms = 1_700_000_000_000
        dashboard = {
            "wallets": [
                {
                    "address": "0x1111111111111111111111111111111111111111",
                    "positions": [{"coin": "BTC", "side": "Long", "positionValue": 1_000_000, "size": 10, "entryPx": 70_000}],
                    "recentFills": [{"coin": "BTC", "direction": "Open Long", "price": 80_000, "size": 1, "time": now_ms - 60_000}],
                },
                {
                    "address": "0x2222222222222222222222222222222222222222",
                    "positions": [{"coin": "BTC", "side": "Long", "positionValue": 1_000_000, "size": 10, "entryPx": 70_000}],
                    "recentFills": [{"coin": "BTC", "direction": "Open Long", "price": 100_000, "size": 1, "time": now_ms - 120_000}],
                },
                {
                    "address": "0x3333333333333333333333333333333333333333",
                    "positions": [{"coin": "BTC", "side": "Long", "positionValue": 1_000_000, "size": 10, "entryPx": 70_000}],
                    "recentFills": [],
                },
            ]
        }

        with patch("server.current_time_ms", return_value=now_ms):
            message = self.service.build_positions_message(dashboard)

        self.assertIn("weighted entry: $70,000", message)
        self.assertIn("7d add VWAP: $90,000 (2 wallets)", message)

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
        self.assertIn("Wallets: 2 | Pos: 2 | Total: $1,200K, weighted entry $77,333", message)
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

        self.assertIn("Wallets: 1 | Pos: 1 | Total: $1,000K", message)
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

        self.assertIn("Wallets: 1 | Pos: 1 | Total: $800K", message)
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
        self.assertIn("- No open pos", message)
        self.assertNotIn("@MOON-1 long", message)
        self.assertNotIn("BTC long (1 wallets, 1 positions", message)
        self.assertIn("Summary: 0 groups, 0 pos", message)

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
        self.assertIn("OIL LONG: 3 wallets, 3 pos | $2.6M open", message)
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
        self.assertIn("Commodities", message)
        self.assertIn("GOLD LONG: 3 wallets, 3 pos | $1.1M open", message)
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
        self.assertIn("Stocks and indices", message)
        self.assertIn("EWY LONG: 3 wallets, 3 pos | $1.1M open", message)
        self.assertIn("NVDA LONG: 3 wallets, 3 pos | $1.1M open", message)
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
        self.assertIn("Commodities\n- None", message)
        self.assertIn("Stocks and indices\n- None", message)
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
        self.assertIn("Commodities", message)
        self.assertIn("OIL LONG: 3 wallets, 3 pos | $1.1M open", message)
        self.assertNotIn("SILVER short", message)
        self.assertIn("Stocks and indices", message)
        self.assertNotIn("XYZ100 long", message)
        self.assertIn("SP500 LONG: 3 wallets, 3 pos | $1.1M open", message)

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
        self.assertIn("High-confidence signals", sent_message)
        self.assertIn("1. BUY BTC (LONG) - NEW | conf 82/100", sent_message)

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
        self.assertIn("New large pos ($1.0M+)", sent_message)
        self.assertIn("Trader One BTC LONG $1.2M @ $100,000", sent_message)

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
        self.assertIn("Closed large pos ($1.0M+)", sent_message)
        self.assertIn("Trader One ETH SHORT $1.2M ~$3,000", sent_message)

    def test_check_alerts_ignores_closed_positions_for_untracked_wallets(self) -> None:
        previous_summary = {
            "overallBias": "mixed",
            "consensus": [],
            "hip3Consensus": [],
        }
        previous_positions = {
            "0x09bc1cf4d9f0b59e1425a8fde4d4b1f7d3c9410d:BTC:short": {
                "address": "0x09bc1cf4d9f0b59e1425a8fde4d4b1f7d3c9410d",
                "alias": "Removed Trader",
                "coin": "BTC",
                "side": "short",
                "totalValue": 16_400_000.0,
                "totalSize": 256.0,
            }
        }
        dashboard = {"wallets": [{"address": "0x1111111111111111111111111111111111111111", "alias": "Trader One", "positions": []}]}

        with patch(
            "server.load_json_file",
            return_value={
                "config": {"enabled": True, "botToken": "token", "chatId": "chat"},
                "state": {"summary": previous_summary, "largePositions": previous_positions},
            },
        ), patch("server.save_json_file"), patch.object(self.service, "dashboard", return_value=dashboard), patch.object(
            self.service, "build_sentiment_summary", return_value=previous_summary
        ), patch.object(self.service, "send_telegram_message") as send_telegram_message:
            result = self.service.check_alerts(send_notification=True)

        self.assertFalse(result["shouldNotify"])
        self.assertFalse(result["sent"])
        self.assertEqual(result["changes"]["closedLargePositions"], [])
        self.assertFalse(send_telegram_message.called)

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

    def test_check_alerts_acknowledges_quiet_hour_changes_without_sending(self) -> None:
        now_ms = 1_700_000_000_000
        previous_summary = {"overallBias": "mixed", "consensus": [], "hip3Consensus": []}
        current_summary = {"overallBias": "mixed", "consensus": [], "hip3Consensus": []}
        address = "0x1111111111111111111111111111111111111111"
        dashboard = {
            "wallets": [
                {
                    "address": address,
                    "positions": [{"coin": "BTC", "side": "Long", "positionValue": 1_200_000.0}],
                    "recentFills": [
                        {
                            "coin": "BTC",
                            "direction": "Open Long",
                            "price": 100_000.0,
                            "size": 12.0,
                            "time": now_ms - 60_000,
                        }
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
        ), patch("server.save_json_file") as save_json_file, patch(
            "server.current_time_ms", return_value=now_ms
        ), patch.object(self.service, "dashboard", return_value=dashboard), patch.object(
            self.service, "build_sentiment_summary", return_value=current_summary
        ), patch.object(self.service, "send_telegram_message") as send_telegram_message:
            result = self.service.check_alerts(
                send_notification=False,
                acknowledge_suppressed=True,
            )

        self.assertTrue(result["shouldNotify"])
        self.assertTrue(result["suppressed"])
        self.assertFalse(result["sent"])
        send_telegram_message.assert_not_called()
        saved_state = save_json_file.call_args.args[1]["state"]
        self.assertIn(f"{address}:BTC:long", saved_state["largePositions"])
        self.assertTrue(saved_state["alertDedupe"])

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
        self.assertIn("Coordinated openings (5 min)", sent_message)
        self.assertIn("- BTC LONG: 3 wallets, $3.6M", sent_message)
        self.assertIn("@ $102,857", sent_message)
        self.assertIn("Trader One $1.2M", sent_message)
        saved_dedupe = save_json_file.call_args.args[1]["state"]["alertDedupe"]
        self.assertTrue(next(iter(saved_dedupe)).startswith("position:cluster-open:BTC:long:"))

    def test_build_telegram_message_omits_new_large_position_already_in_cluster(self) -> None:
        summary = {"overallBias": "mixed", "walletCount": 10}
        changes = {
            "addedSignals": [],
            "changedSignals": [],
            "removedSignals": [],
            "addedCandidateSignals": [],
            "addedCmmSignals": [],
            "changedCmmSignals": [],
            "clusteredOpenPositions": [
                {
                    "coin": "BTC",
                    "side": "long",
                    "walletCount": 3,
                    "totalValue": 3_600_000.0,
                    "entryPx": 100_000.0,
                    "wallets": [
                        {"address": "0x1111111111111111111111111111111111111111", "alias": "Trader One", "totalValue": 1_200_000.0},
                        {"address": "0x2222222222222222222222222222222222222222", "alias": "Trader Two", "totalValue": 1_200_000.0},
                        {"address": "0x3333333333333333333333333333333333333333", "alias": "Trader Three", "totalValue": 1_200_000.0},
                    ],
                }
            ],
            "newLargePositions": [
                {
                    "address": "0x1111111111111111111111111111111111111111",
                    "alias": "Trader One",
                    "coin": "BTC",
                    "side": "long",
                    "totalValue": 1_200_000.0,
                    "entryPx": 100_000.0,
                    "entryPriceSource": "fill",
                },
                {
                    "address": "0x4444444444444444444444444444444444444444",
                    "alias": "Trader Four",
                    "coin": "ETH",
                    "side": "short",
                    "totalValue": 1_500_000.0,
                    "entryPx": 3_000.0,
                    "entryPriceSource": "fill",
                },
            ],
            "closedLargePositions": [],
            "increasedLargePositions": [],
        }

        message = self.service.build_telegram_message(changes, summary, min_wallets=4)

        self.assertNotIn("Trader One BTC LONG", message)
        self.assertIn("Trader Four ETH SHORT $1.5M @ $3,000", message)

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
            "0x69906b0ed626ca01a4b7c001e5711e5714ccf207:BTC:long": {
                "address": "0x69906b0ed626ca01a4b7c001e5711e5714ccf207",
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
                    "address": "0x69906b0ed626ca01a4b7c001e5711e5714ccf207",
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
        self.assertIn("4-hour wallet update", hourly_message)
        self.assertNotIn("Hourly wallet update", hourly_message)
        self.assertNotIn("Wallet ranks by 7D hit rate + PnL", hourly_message)
        self.assertNotIn("High-conviction signals", hourly_message)
        alert_message = send_telegram_message.call_args_list[1].args[2]
        self.assertIn("New large pos ($1.0M+)", alert_message)
        self.assertIn("Trader One BTC LONG $1.2M", alert_message)
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
                    "address": "0x69906b0ed626ca01a4b7c001e5711e5714ccf207",
                    "alias": "Trader One",
                    "positions": [
                        {"coin": "ETH", "side": "Short", "positionValue": 1_300_000.0, "size": 300.0},
                    ],
                }
            ]
        }
        previous_positions = {
            "0x69906b0ed626ca01a4b7c001e5711e5714ccf207:BTC:long": {
                "address": "0x69906b0ed626ca01a4b7c001e5711e5714ccf207",
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
        self.assertIn("Large pos additions ($1.0M+)", sent_message)
        self.assertIn("Trader One +$1.2M BTC LONG ~ $120,000 ($1.2M -> $2.4M)", sent_message)
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

    def test_large_position_snapshot_uses_1m_alert_threshold(self) -> None:
        dashboard = {
            "wallets": [
                {
                    "address": "0x1111111111111111111111111111111111111111",
                    "positions": [
                        {"coin": "BTC", "side": "Long", "positionValue": 999_999.0},
                        {"coin": "ETH", "side": "Short", "positionValue": 1_000_000.0},
                    ],
                }
            ]
        }

        snapshot = self.service.build_large_position_snapshot(dashboard)

        self.assertNotIn("0x1111111111111111111111111111111111111111:BTC:long", snapshot)
        self.assertIn("0x1111111111111111111111111111111111111111:ETH:short", snapshot)

    def test_threshold_migration_does_not_report_sub_1m_position_as_closed(self) -> None:
        previous = {
            "wallet:BTC:long": {
                "address": "wallet",
                "coin": "BTC",
                "side": "long",
                "totalValue": 900_000.0,
            }
        }

        changes = self.service.build_large_position_alert_changes(previous, {})

        self.assertEqual(changes["closedLargePositions"], [])

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


class TransientFillFailureTests(unittest.TestCase):
    """A flaky fills request must not make a wallet ineligible.

    ``fillsOk`` describes one request (userFillsByTime). ``recentFills`` comes
    from a different request unioned with a cache that retains a week of fills.
    The two are not interchangeable.
    """

    def setUp(self) -> None:
        self.service = WalletTrackerService(WalletStore(Path(ALERTS_FILE)), HyperliquidClient())
        self.wallet = TrackedWallet(
            address="0x2fcb6898d5000000000000000000000000000000", alias="", notes="", created_at=""
        )
        self.now_ms = current_time_ms()
        self.state = {
            "marginSummary": {"accountValue": "1000000", "totalNtlPos": "500000", "totalMarginUsed": "0"},
            "withdrawable": "500000",
            "assetPositions": [
                {
                    "position": {
                        "coin": "BTC",
                        "szi": "10",
                        "positionValue": "500000",
                        "entryPx": "50000",
                        "unrealizedPnl": "0",
                        "returnOnEquity": "0",
                    }
                }
            ],
        }
        self.cached_fill = {
            "coin": "BTC",
            "direction": "Open Long",
            "price": 50000.0,
            "size": 10.0,
            "closedPnl": 0.0,
            "fee": 0.0,
            "time": self.now_ms - 5 * 60 * 1000,
        }

    def snapshot_with_failed_fills(self, cached: dict[str, Any] | None) -> dict[str, Any]:
        failure = {"ok": False, "data": [], "error": "HTTP 429: Too Many Requests"}
        with patch.object(
            self.service.client, "safe_subscribe_all_dexs_clearinghouse_state", return_value=self.state
        ), patch.object(
            self.service, "fetch_fills_result", return_value=failure
        ), patch.object(
            self.service, "fetch_recent_fills_result", return_value=failure
        ), patch.object(
            self.service, "fetch_open_orders_result", return_value={"ok": True, "data": [], "error": ""}
        ), patch.object(
            self.service, "fetch_portfolio_result", return_value={"ok": True, "data": {}, "error": ""}
        ), patch.object(self.service, "fetch_wallet_role", return_value="user"):
            return self.service.fetch_wallet_snapshot(self.wallet, cached_snapshot=cached)

    def test_cached_fills_keep_a_wallet_eligible_when_the_fills_request_fails(self) -> None:
        snapshot = self.snapshot_with_failed_fills({"recentFills": [self.cached_fill]})
        quality = snapshot["dataQuality"]

        # The per-cycle flags stay honest about the failure...
        self.assertFalse(quality["fillsOk"])
        self.assertFalse(quality["fillsFetchOk"])
        self.assertTrue(self.service.wallet_fill_fetch_failed(snapshot))
        self.assertIn("429", quality["fillsError"])
        # ...but the wallet is still usable off the merged recent-fill cache.
        self.assertTrue(quality["fillsUsable"])
        self.assertTrue(quality["fillsServedFromCache"])
        self.assertEqual(quality["recentFillCount"], 1)
        self.assertEqual(quality["liveRecentFillCount"], 0)
        self.assertEqual(snapshot["recentFills"], [self.cached_fill])
        self.assertTrue(self.service.wallet_fill_data_reliable(snapshot))

    def test_a_failed_fills_request_no_longer_blocks_freshness_verification(self) -> None:
        position = {"coin": "BTC", "side": "Long", "positionValue": 500_000.0}
        with_cache = self.snapshot_with_failed_fills({"recentFills": [self.cached_fill]})
        without_cache = self.snapshot_with_failed_fills({"recentFills": []})

        self.assertTrue(
            self.service.has_verified_recent_activity(
                with_cache, position, {}, now_ms=self.now_ms, window_ms=15 * 60 * 1000
            )
        )
        # Same failed request, but nothing usable to fall back on.
        self.assertFalse(
            self.service.has_verified_recent_activity(
                without_cache, position, {}, now_ms=self.now_ms, window_ms=15 * 60 * 1000
            )
        )

    def test_a_wallet_with_no_recent_fill_data_at_all_stays_fill_quality_unknown(self) -> None:
        snapshot = self.snapshot_with_failed_fills({"recentFills": []})
        quality = snapshot["dataQuality"]

        self.assertFalse(quality["fillsUsable"])
        self.assertFalse(quality["fillsServedFromCache"])
        self.assertEqual(quality["recentFillCount"], 0)
        self.assertFalse(self.service.wallet_fill_data_reliable(snapshot))

    def test_cached_fills_older_than_the_retention_window_are_not_treated_as_usable(self) -> None:
        stale_fill = {**self.cached_fill, "time": self.now_ms - 30 * 24 * 60 * 60 * 1000}
        snapshot = self.snapshot_with_failed_fills({"recentFills": [stale_fill]})

        self.assertEqual(snapshot["recentFills"], [])
        self.assertFalse(snapshot["dataQuality"]["fillsUsable"])
        self.assertFalse(self.service.wallet_fill_data_reliable(snapshot))

    def test_summary_counts_fetch_failures_separately_from_unusable_fill_data(self) -> None:
        position = {"coin": "BTC", "side": "Long", "positionValue": 1000.0}
        snapshots = [
            {
                "address": "0x1111111111111111111111111111111111111111",
                "positions": [position],
                "recentFills": [self.cached_fill],
                "dataQuality": {"fillsOk": True, "fillsFetchOk": True, "fillsUsable": True},
            },
            {
                # Fills request failed, cache carried the wallet.
                "address": "0x2222222222222222222222222222222222222222",
                "positions": [position],
                "recentFills": [self.cached_fill],
                "dataQuality": {"fillsOk": False, "fillsFetchOk": False, "fillsUsable": True},
            },
            {
                # Fills request failed and there is nothing cached either.
                "address": "0x3333333333333333333333333333333333333333",
                "positions": [position],
                "recentFills": [],
                "dataQuality": {"fillsOk": False, "fillsFetchOk": False, "fillsUsable": False},
            },
        ]

        summary = self.service.build_sentiment_summary(snapshots, min_wallets=1)

        self.assertEqual(summary["fillFetchFailedWalletCount"], 2)
        self.assertEqual(
            summary["fillFetchFailedWalletAddresses"],
            [
                "0x2222222222222222222222222222222222222222",
                "0x3333333333333333333333333333333333333333",
            ],
        )
        self.assertEqual(summary["fillQualityUnknownWalletCount"], 1)
        self.assertEqual(
            summary["fillQualityUnknownWalletAddresses"],
            ["0x3333333333333333333333333333333333333333"],
        )

    def test_dashboard_totals_split_fetch_failures_from_unusable_wallets(self) -> None:
        def snapshot(index: int, fills_ok: bool, usable: bool) -> dict[str, Any]:
            return {
                "address": f"0x{index:040x}",
                "accountValue": 1_000_000.0,
                "totalNotional": 1_000_000.0,
                "unrealizedPnl": 0.0,
                "realizedPnl": 0.0,
                "recentFills": [self.cached_fill] if usable else [],
                "positions": [{"coin": "BTC", "side": "Long", "positionValue": 1_000_000.0}],
                "exposure": {"long": 1_000_000.0, "short": 0.0, "net": 1_000_000.0},
                "cohorts": {"walletSize": "Whale", "profitability": "Profitable"},
                "dataQuality": {
                    "fillsOk": fills_ok,
                    "fillsFetchOk": fills_ok,
                    "fillsUsable": usable,
                    "fillsDegraded": False,
                },
            }

        snapshots = [
            snapshot(1, True, True),
            snapshot(2, True, True),
            snapshot(3, False, True),
            snapshot(4, False, True),
            snapshot(5, False, False),
        ]
        wallets = [
            TrackedWallet(address=f"0x{index:040x}", alias="", notes="", created_at="")
            for index in range(1, len(snapshots) + 1)
        ]

        with patch.object(self.service.store, "list_wallets", return_value=wallets), patch.object(
            self.service.client, "list_markets", return_value=[]
        ):
            with patch.object(self.service, "fetch_wallet_snapshot", side_effect=snapshots):
                dashboard = self.service.dashboard()

        quality = dashboard["totals"]["dataQuality"]
        self.assertEqual(quality["fillsFetchFailedWallets"], 3)
        self.assertEqual(quality["fillsServedFromCacheWallets"], 2)
        self.assertEqual(quality["fillsUnusableWallets"], 1)


class ShadowSignalSamplingTests(unittest.TestCase):
    """The shadow control group must grow without stacking duplicate samples."""

    # Deliberately hard-coded rather than imported: a test that reads the same
    # constants as the implementation passes even if they are zeroed out.
    MIN_GAP_MS = 2 * 60 * 60 * 1000
    RESTART_MS = 24 * 60 * 60 * 1000

    def setUp(self) -> None:
        self.service = WalletTrackerService(WalletStore(Path(ALERTS_FILE)), HyperliquidClient())
        self.started_at = 1_700_000_000_000

    def consensus_item(self, **overrides: Any) -> dict[str, Any]:
        item = {
            "coin": "ETH",
            "side": "long",
            "markPrice": 100.0,
            "freshAddVwap": 99.0,
            "convictionScore": 50.0,
            "walletCount": 2,
            "totalValue": 1_000_000.0,
            "freshAddLatestTime": self.started_at - 60_000,
            "verifiedFreshIndependentWalletCount": 1,
            "wallets": [
                {"address": "0xaaa", "value": 500_000.0},
                {"address": "0xbbb", "value": 500_000.0},
            ],
        }
        item.update(overrides)
        return item

    def summary(self, item: dict[str, Any]) -> dict[str, Any]:
        return {"signals": [], "consensus": [item], "positionMarks": []}

    def sample(self, previous: dict[str, Any], item: dict[str, Any], at_ms: int) -> dict[str, Any]:
        return self.service.update_shadow_signal_outcomes(previous, self.summary(item), now_ms=at_ms)

    def newest(self, records: dict[str, Any]) -> dict[str, Any]:
        return max(records.values(), key=lambda record: int(record["startedAt"]))

    def test_first_observation_is_recorded_with_its_fingerprint(self) -> None:
        records = self.sample({}, self.consensus_item(), self.started_at)

        self.assertEqual(len(records), 1)
        record = self.newest(records)
        self.assertEqual(record["sampleReason"], "initial")
        self.assertTrue(record["independentSample"])
        self.assertEqual(record["previousSampleAt"], 0)
        self.assertEqual(record["sampleIndex"], 1)
        self.assertEqual(record["consensusFingerprint"]["walletAddresses"], ["0xaaa", "0xbbb"])
        self.assertEqual(record["consensusFingerprint"]["totalValue"], 1_000_000.0)

    def test_an_unchanged_position_is_not_resampled(self) -> None:
        item = self.consensus_item()
        records = self.sample({}, item, self.started_at)

        for offset in (self.MIN_GAP_MS, self.MIN_GAP_MS * 2, self.RESTART_MS - 60_000):
            records = self.sample(records, item, self.started_at + offset)
            self.assertEqual(len(records), 1, f"resampled an unchanged position at +{offset}ms")

    def test_a_changed_wallet_set_creates_a_new_sample(self) -> None:
        records = self.sample({}, self.consensus_item(), self.started_at)
        joined = self.consensus_item(
            walletCount=3,
            wallets=[
                {"address": "0xaaa", "value": 500_000.0},
                {"address": "0xbbb", "value": 500_000.0},
                {"address": "0xccc", "value": 500_000.0},
            ],
        )

        records = self.sample(records, joined, self.started_at + self.MIN_GAP_MS)

        self.assertEqual(len(records), 2)
        record = self.newest(records)
        self.assertEqual(record["sampleReason"], "walletSetChanged")
        self.assertTrue(record["independentSample"])
        self.assertEqual(record["previousSampleAt"], self.started_at)
        self.assertEqual(record["msSincePreviousSample"], self.MIN_GAP_MS)
        self.assertEqual(record["sampleIndex"], 2)
        self.assertEqual(
            record["consensusFingerprint"]["walletAddresses"], ["0xaaa", "0xbbb", "0xccc"]
        )

    def test_a_fresh_add_creates_a_new_sample(self) -> None:
        records = self.sample({}, self.consensus_item(), self.started_at)
        added = self.consensus_item(freshAddLatestTime=self.started_at + self.MIN_GAP_MS - 1_000)

        records = self.sample(records, added, self.started_at + self.MIN_GAP_MS)

        self.assertEqual(len(records), 2)
        self.assertEqual(self.newest(records)["sampleReason"], "freshAdd")

    def test_a_large_size_change_creates_a_new_sample_but_drift_does_not(self) -> None:
        records = self.sample({}, self.consensus_item(), self.started_at)

        # 10% is ordinary mark drift on an untouched position.
        drifted = self.consensus_item(totalValue=1_100_000.0)
        records = self.sample(records, drifted, self.started_at + self.MIN_GAP_MS)
        self.assertEqual(len(records), 1)

        # Doubling the position is a different setup.
        doubled = self.consensus_item(totalValue=2_000_000.0)
        records = self.sample(records, doubled, self.started_at + self.MIN_GAP_MS * 2)
        self.assertEqual(len(records), 2)
        self.assertEqual(self.newest(records)["sampleReason"], "sizeChanged")

    def test_material_changes_inside_the_minimum_gap_are_suppressed(self) -> None:
        records = self.sample({}, self.consensus_item(), self.started_at)
        churned = self.consensus_item(wallets=[{"address": "0xzzz", "value": 1_000_000.0}])

        for offset in (60_000, 30 * 60_000, self.MIN_GAP_MS - 1_000):
            records = self.sample(records, churned, self.started_at + offset)
            self.assertEqual(len(records), 1, f"sampled twice inside the min gap at +{offset}ms")

        records = self.sample(records, churned, self.started_at + self.MIN_GAP_MS)
        self.assertEqual(len(records), 2)

    def test_a_static_consensus_still_yields_one_flagged_periodic_sample_per_day(self) -> None:
        item = self.consensus_item()
        records = self.sample({}, item, self.started_at)

        records = self.sample(records, item, self.started_at + self.RESTART_MS)

        self.assertEqual(len(records), 2)
        record = self.newest(records)
        self.assertEqual(record["sampleReason"], "periodic")
        self.assertFalse(record["independentSample"])

    def test_records_written_before_fingerprints_existed_are_resampled_once(self) -> None:
        legacy = {
            "shadow:ETH:long:1": {
                "coin": "ETH",
                "side": "long",
                "signalKey": "ETH:long",
                "startedAt": self.started_at,
                "entryPrice": 100.0,
                "rawProbabilityScore": 50.0,
                "shadow": True,
                "published": False,
                "outcomes": {},
            }
        }

        records = self.sample(legacy, self.consensus_item(), self.started_at + self.MIN_GAP_MS)

        self.assertEqual(len(records), 2)
        record = self.newest(records)
        self.assertEqual(record["sampleReason"], "unknownPriorFingerprint")
        self.assertFalse(record["independentSample"])

    def test_published_signals_are_still_excluded_from_the_control_group(self) -> None:
        summary = {
            "signals": [{"coin": "ETH", "side": "long"}],
            "consensus": [self.consensus_item()],
            "positionMarks": [],
        }

        records = self.service.update_shadow_signal_outcomes({}, summary, now_ms=self.started_at)

        self.assertEqual(records, {})


if __name__ == "__main__":
    unittest.main()
