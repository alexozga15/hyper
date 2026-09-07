"""The shadow stream samples a wider population than the one that publishes.

After the tracked set went from 32 wallets to 25, only 6 coin/sides still
reached the publication threshold of 4 and shadow sampling collapsed from ~50
records a day to ~5. The stream exists to measure, not to publish, so it does
not have to share that gate - but it must still exclude anything that actually
published, and it must not touch state on the extra pass.
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import server  # noqa: E402
from server import (  # noqa: E402
    ALERTS_FILE,
    HyperliquidClient,
    WalletStore,
    WalletTrackerService,
)

NOW_MS = 1_700_000_000_000


def consensus_item(coin: str, side: str = "long", wallets: int = 3) -> dict:
    return {
        "coin": coin,
        "side": side,
        "marketCoin": coin,
        "walletCount": wallets,
        "independentWalletCount": wallets,
        "netIndependentWalletCount": wallets,
        "markPrice": 100.0,
        "totalValue": 5_000_000.0,
        "totalSize": 50_000.0,
        "wallets": [],
    }


class ShadowSamplingThresholdTests(unittest.TestCase):
    def setUp(self) -> None:
        self.service = WalletTrackerService(WalletStore(Path(ALERTS_FILE)), HyperliquidClient())

    def test_no_widening_when_the_threshold_is_not_lower(self) -> None:
        # Equal or higher means the publication summary is already the widest
        # population, so the extra build must be skipped entirely.
        for shadow_threshold in (4, 5):
            with self.subTest(shadow_threshold=shadow_threshold):
                with patch.object(server, "SHADOW_SIGNAL_MIN_CONSENSUS_WALLETS", shadow_threshold):
                    with patch.object(self.service, "build_monthly_sentiment_summary") as builder:
                        result = self.service.shadow_sampling_consensus({}, {}, 4)
                self.assertIsNone(result)
                builder.assert_not_called()

    def test_widening_builds_at_the_shadow_threshold_without_persisting(self) -> None:
        wider = [consensus_item("BTC"), consensus_item("ETH"), consensus_item("SOL")]
        with patch.object(server, "SHADOW_SIGNAL_MIN_CONSENSUS_WALLETS", 3):
            with patch.object(
                self.service,
                "build_monthly_sentiment_summary",
                return_value=({"consensus": wider}, {}),
            ) as builder:
                result = self.service.shadow_sampling_consensus({}, {}, 4)

        self.assertEqual(result, wider)
        builder.assert_called_once()
        args, kwargs = builder.call_args
        self.assertEqual(args[1], 3, "must build at the shadow threshold, not the publication one")
        self.assertIs(kwargs.get("persist"), False, "the extra pass must not write state")

    def test_a_summary_without_a_consensus_list_yields_none(self) -> None:
        with patch.object(server, "SHADOW_SIGNAL_MIN_CONSENSUS_WALLETS", 3):
            with patch.object(
                self.service, "build_monthly_sentiment_summary", return_value=({}, {})
            ):
                self.assertIsNone(self.service.shadow_sampling_consensus({}, {}, 4))

    def test_the_recorder_samples_the_wider_list(self) -> None:
        narrow = {"consensus": [consensus_item("BTC")], "signals": []}
        wider = [consensus_item("BTC"), consensus_item("ETH"), consensus_item("SOL")]

        only_narrow = self.service.update_shadow_signal_outcomes({}, narrow, now_ms=NOW_MS)
        with_wider = self.service.update_shadow_signal_outcomes(
            {}, narrow, now_ms=NOW_MS, consensus=wider
        )

        self.assertGreater(
            len(with_wider),
            len(only_narrow),
            "passing a wider consensus must record more setups",
        )
        coins = {record.get("coin") for record in with_wider.values()}
        self.assertEqual(coins, {"BTC", "ETH", "SOL"})

    def test_a_published_setup_is_still_excluded_from_the_wider_list(self) -> None:
        # published_keys comes from the publication summary, so widening the
        # sampled population must not start shadowing something that alerted.
        published = consensus_item("BTC")
        narrow = {
            "consensus": [],
            "signals": [{"coin": "BTC", "side": "long", "action": "buy"}],
        }
        records = self.service.update_shadow_signal_outcomes(
            {}, narrow, now_ms=NOW_MS, consensus=[published, consensus_item("ETH")]
        )
        coins = {record.get("coin") for record in records.values()}
        self.assertNotIn("BTC", coins)
        self.assertIn("ETH", coins)


if __name__ == "__main__":
    unittest.main()
