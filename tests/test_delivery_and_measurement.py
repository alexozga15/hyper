"""Delivery, measurement, and the calibration tier.

Three properties that have to hold together:

- an alert that failed to reach Telegram is not marked as announced, or it is
  lost until the group moves beyond the release band;
- every alert that *was* delivered lands in outcome tracking, priced at the mark
  the message quoted, so the stream answers what a reader acting on it got;
- calibration built from a shadow stream that samples below the publication
  threshold does not quietly apply a wider population's base rate to signals
  that would publish.
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import server  # noqa: E402
from server import (  # noqa: E402
    ACTIONABLE_ENTRY_OUTCOME_RETENTION_MS,
    ALERTS_FILE,
    CALIBRATION_TIER_MIN_WALLETS,
    SIGNAL_CALIBRATION_MIN_SAMPLE,
    HyperliquidClient,
    WalletStore,
    WalletTrackerService,
)

NOW_MS = 1_700_000_000_000
HOUR_MS = 60 * 60 * 1000


def alert(coin: str = "HYPE", *, mark: float = 88.0, started: int = NOW_MS) -> dict:
    return {
        "coin": coin,
        "side": "long",
        "walletCount": 5,
        "totalValue": 21_000_000.0,
        "referencePrice": 87.0,
        "markPrice": mark,
        "distancePct": 1.15,
        "qualityWinRatePct": 70.6,
        "qualityBestWinRatePct": 82.0,
        "enteredAt": "2026-09-08T00:00:00Z",
        "enteredAtMs": started,
    }


class DeliveredAlertMeasurementTests(unittest.TestCase):
    def setUp(self) -> None:
        self.service = WalletTrackerService(WalletStore(Path(ALERTS_FILE)), HyperliquidClient())

    def test_a_delivered_alert_is_recorded_at_the_mark_the_message_quoted(self) -> None:
        records = self.service.update_actionable_entry_outcomes(
            {}, [alert()], marks_by_coin={"HYPE": 88.0}, now_ms=NOW_MS
        )
        self.assertEqual(len(records), 1)
        record = next(iter(records.values()))
        self.assertEqual(record["entryPrice"], 88.0, "entry is the message's mark, not the wallets'")
        self.assertEqual(record["referencePrice"], 87.0)
        self.assertEqual(record["source"], "actionableEntry")
        self.assertIs(record["delivered"], True)
        self.assertEqual(list(records), ["entry:HYPE:long:%d" % NOW_MS])

    def test_nothing_is_recorded_when_nothing_was_delivered(self) -> None:
        records = self.service.update_actionable_entry_outcomes(
            {}, [], marks_by_coin={"HYPE": 88.0}, now_ms=NOW_MS
        )
        self.assertEqual(records, {})

    def test_an_existing_record_keeps_being_measured_with_no_new_alerts(self) -> None:
        first = self.service.update_actionable_entry_outcomes(
            {}, [alert()], marks_by_coin={"HYPE": 88.0}, now_ms=NOW_MS
        )
        later = self.service.update_actionable_entry_outcomes(
            first, [], marks_by_coin={"HYPE": 96.8}, now_ms=NOW_MS + 2 * HOUR_MS
        )
        self.assertEqual(len(later), 1, "the record survives a cycle with no delivery")
        outcomes = next(iter(later.values()))["outcomes"]
        self.assertTrue(outcomes, "a due horizon must have been measured")

    def test_a_record_is_never_rewritten(self) -> None:
        first = self.service.update_actionable_entry_outcomes(
            {}, [alert()], marks_by_coin={"HYPE": 88.0}, now_ms=NOW_MS
        )
        again = self.service.update_actionable_entry_outcomes(
            first, [alert(mark=99.0)], marks_by_coin={"HYPE": 99.0}, now_ms=NOW_MS
        )
        self.assertEqual(next(iter(again.values()))["entryPrice"], 88.0)

    def test_an_unpriceable_alert_is_skipped(self) -> None:
        broken = alert()
        broken["markPrice"] = 0.0
        records = self.service.update_actionable_entry_outcomes(
            {}, [broken], marks_by_coin={}, now_ms=NOW_MS
        )
        self.assertEqual(records, {})

    def test_records_past_retention_are_dropped(self) -> None:
        old = self.service.update_actionable_entry_outcomes(
            {}, [alert()], marks_by_coin={"HYPE": 88.0}, now_ms=NOW_MS
        )
        kept = self.service.update_actionable_entry_outcomes(
            old, [], marks_by_coin={"HYPE": 88.0},
            now_ms=NOW_MS + ACTIONABLE_ENTRY_OUTCOME_RETENTION_MS + HOUR_MS,
        )
        self.assertEqual(kept, {})


class CalibrationTierTests(unittest.TestCase):
    def setUp(self) -> None:
        self.service = WalletTrackerService(WalletStore(Path(ALERTS_FILE)), HyperliquidClient())

    def shadow(self, count: int, *, wallets: int | None, win: bool) -> dict:
        records = {}
        for index in range(count):
            record = {
                "coin": "BTC",
                "side": "long",
                "startedAt": NOW_MS - index * HOUR_MS,
                "rawProbabilityScore": 85.0,
                "independentSample": True,
                "outcomes": {"4h": {"netReturnPct": 1.0 if win else -1.0}},
            }
            if wallets is not None:
                record["walletCount"] = wallets
            records[f"{wallets}-{win}-{index}"] = record
        return records

    def test_a_record_without_a_wallet_count_has_no_tier(self) -> None:
        self.assertEqual(self.service.signal_calibration_tier({"coin": "BTC"}), "")
        self.assertEqual(self.service.signal_calibration_tier(None), "")

    def test_the_tier_splits_at_the_publication_threshold(self) -> None:
        below = CALIBRATION_TIER_MIN_WALLETS - 1
        self.assertEqual(self.service.signal_calibration_tier({"walletCount": below}), "wide")
        self.assertEqual(
            self.service.signal_calibration_tier({"walletCount": CALIBRATION_TIER_MIN_WALLETS}), "pub"
        )

    def test_legacy_records_still_populate_the_untiered_bucket(self) -> None:
        legacy = self.shadow(30, wallets=None, win=True)
        calibration = self.service.build_signal_calibration({}, shadow_records=legacy)
        crypto = calibration["groups"].get("crypto", {})
        self.assertEqual(int(crypto.get("80", {}).get("sample", 0)), 30)
        self.assertNotIn("crypto:pub", calibration["groups"])
        self.assertNotIn("crypto:wide", calibration["groups"])

    def test_a_tiered_bucket_is_used_once_it_has_the_sample(self) -> None:
        # The wide tier is all losses, the publishing tier all wins. A signal
        # that would publish must be calibrated on its own tier, not on the
        # pooled rate the wider population drags down.
        pool = {
            **self.shadow(SIGNAL_CALIBRATION_MIN_SAMPLE + 5, wallets=CALIBRATION_TIER_MIN_WALLETS, win=True),
            **self.shadow(SIGNAL_CALIBRATION_MIN_SAMPLE + 5, wallets=CALIBRATION_TIER_MIN_WALLETS - 1, win=False),
        }
        calibration = self.service.build_signal_calibration({}, shadow_records=pool)
        self.assertIn("crypto:pub", calibration["groups"])
        self.assertIn("crypto:wide", calibration["groups"])

        summary = {
            "signals": [
                {
                    "coin": "BTC",
                    "side": "long",
                    "rawProbabilityScore": 85.0,
                    "probabilityScore": 85.0,
                    "walletCount": CALIBRATION_TIER_MIN_WALLETS,
                }
            ]
        }
        adjusted = self.service.apply_signal_calibration(summary, calibration)
        signal = adjusted["signals"][0]
        pooled = calibration["groups"]["crypto"]["80"]["calibratedProbability"]
        tiered = calibration["groups"]["crypto:pub"]["80"]["calibratedProbability"]
        self.assertNotAlmostEqual(pooled, tiered, places=1, msg="fixture must separate the tiers")
        self.assertAlmostEqual(signal["probabilityScore"], max(85.0 - 10.0, min(85.0 + 10.0, tiered)), places=1)

    def test_a_thin_tier_falls_back_to_the_untiered_bucket(self) -> None:
        # hip3 bands hold 2-13 observations on the live pools and never reach
        # the minimum once split, so the fallback is what keeps them usable.
        pool = {
            **self.shadow(30, wallets=None, win=True),
            **self.shadow(3, wallets=CALIBRATION_TIER_MIN_WALLETS, win=False),
        }
        calibration = self.service.build_signal_calibration({}, shadow_records=pool)
        self.assertLess(
            int(calibration["groups"]["crypto:pub"]["80"]["sample"]), SIGNAL_CALIBRATION_MIN_SAMPLE
        )
        summary = {
            "signals": [
                {
                    "coin": "BTC",
                    "side": "long",
                    "rawProbabilityScore": 85.0,
                    "probabilityScore": 85.0,
                    "walletCount": CALIBRATION_TIER_MIN_WALLETS,
                }
            ]
        }
        adjusted = self.service.apply_signal_calibration(summary, calibration)
        untiered = calibration["groups"]["crypto"]["80"]["calibratedProbability"]
        self.assertAlmostEqual(
            adjusted["signals"][0]["probabilityScore"], max(85.0 - 10.0, min(85.0 + 10.0, untiered)), places=1
        )


if __name__ == "__main__":
    unittest.main()
