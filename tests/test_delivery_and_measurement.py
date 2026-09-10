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

from unittest.mock import patch  # noqa: E402

import server  # noqa: E402
from server import (  # noqa: E402
    ACTIONABLE_ENTRY_ALERTS_PER_MESSAGE,
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
        "positionRank": 72.5,
        "positionRankComponents": {"performance": 70.0, "evidence": 75.0, "risk": 80.0, "copyability": 50.0},
        "walletAddresses": ["0xa", "0xb", "0xc"],
        "eligibleWalletAddresses": ["0xa", "0xb", "0xc"],
        "qualityDimensions": {"copyability": {"pass": 3}},
        "tradeAdmission": {"status": "eligible", "eligibleWalletCount": 3, "requiredWalletCount": 3},
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

    def test_silent_copyability_observation_is_identified_as_research(self) -> None:
        records = self.service.update_actionable_entry_outcomes(
            {}, [alert()], marks_by_coin={"HYPE": 88.0}, now_ms=NOW_MS,
            source="copyabilityResearch", delivered_flag=False,
        )
        record = next(iter(records.values()))
        self.assertEqual(record["source"], "copyabilityResearch")
        self.assertIs(record["delivered"], False)

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


def qualifying_group(coin: str, *, quality: float = 75.0) -> dict:
    reference = 100.0
    mark = reference * 1.01
    return {
        "coin": coin,
        "side": "long",
        "walletCount": 4,
        "qualityScoredWallets": 4,
        "qualityWinRatePct": quality,
        "qualityBestWinRatePct": quality,
        "positionRank": quality,
        "positionRankComponents": {"performance": quality, "evidence": quality, "risk": quality, "copyability": quality},
        "totalSize": 10.0,
        "totalValue": mark * 10.0,
        "recentAddPx": reference,
        "entryPx": reference,
        "positionCount": 4,
        "walletAddresses": ["0xa", "0xb", "0xc", "0xd"],
        "eligibleWalletAddresses": ["0xa", "0xb", "0xc"],
        "tradeAdmission": {"status": "eligible", "eligibleWalletCount": 3, "requiredWalletCount": 3},
    }


class AnnouncedAndRecordedAgreeTests(unittest.TestCase):
    """What is latched and recorded must equal what the message actually said."""

    def setUp(self) -> None:
        self.service = WalletTrackerService(WalletStore(Path(ALERTS_FILE)), HyperliquidClient())

    def build(self, groups: list[dict], previous: dict | None = None):
        def fake_groups(_dashboard, **kwargs):
            if kwargs.get("stock_like_only") or kwargs.get("commodity_like_only"):
                return []
            return groups

        with patch.object(self.service, "build_position_groups", side_effect=fake_groups):
            return self.service.build_actionable_entry_alerts({}, previous, now_ms=NOW_MS)

    def test_more_qualifying_groups_than_fit_are_capped_together(self) -> None:
        # Before this, the message truncated at the render limit while the latch
        # and the outcome stream took the whole list, so a group past the cap was
        # marked announced and recorded as delivered without ever appearing.
        over = ACTIONABLE_ENTRY_ALERTS_PER_MESSAGE + 1
        groups = [qualifying_group(f"C{index}", quality=90.0 - index) for index in range(over)]
        alerts, state = self.build(groups)

        self.assertEqual(len(alerts), ACTIONABLE_ENTRY_ALERTS_PER_MESSAGE)
        self.assertEqual(len(state), ACTIONABLE_ENTRY_ALERTS_PER_MESSAGE)

        changes = {key: [] for key in (
            "addedConsensus", "removedConsensus", "changedConsensus", "hip3Added", "hip3Removed",
            "clusteredOpenPositions", "newLargePositions", "increasedLargePositions",
            "closedLargePositions", "addedSignals", "removedSignals", "changedSignals",
            "addedCandidateSignals", "addedCmmSignals", "changedCmmSignals",
        )}
        changes["biasChanged"] = False
        changes["actionableEntries"] = alerts
        message = self.service.build_telegram_message(changes, {"consensus": []}, min_wallets=3)

        announced = {alert["coin"] for alert in alerts}
        self.assertEqual(
            announced,
            {coin for coin in (f"C{index}" for index in range(over)) if f"{coin} LONG" in message},
            "every latched alert must appear in the message, and nothing else",
        )
        self.assertNotIn("C8", announced, "the lowest-quality group waits for a later cycle")

    def test_the_capped_group_is_announced_on_a_later_cycle(self) -> None:
        over = ACTIONABLE_ENTRY_ALERTS_PER_MESSAGE + 1
        groups = [qualifying_group(f"C{index}", quality=90.0 - index) for index in range(over)]
        _alerts, state = self.build(groups)
        # The others are latched now, so only the one held back can fire.
        alerts, _next_state = self.build(groups, state)
        self.assertEqual([alert["coin"] for alert in alerts], ["C8"])


class FailedSendIsRetriedWithoutDuplicatingTests(unittest.TestCase):
    """The whole chain: a Telegram failure, a retry, and no duplicate after."""

    def setUp(self) -> None:
        self.service = WalletTrackerService(WalletStore(Path(ALERTS_FILE)), HyperliquidClient())
        self.group = qualifying_group("HYPE")

    def run_cycle(self, state: dict, *, fail: bool):
        saved: list[tuple] = []

        def fake_groups(_dashboard, **kwargs):
            if kwargs.get("stock_like_only") or kwargs.get("commodity_like_only"):
                return []
            return [self.group]

        def send(*_args, **_kwargs):
            if fail:
                raise ValueError("telegram is down")

        with patch("server.load_json_file", return_value={
            "config": {"enabled": True, "botToken": "token", "chatId": "chat"},
            "state": state,
        }), patch("server.save_json_file", side_effect=lambda path, payload: saved.append((path, payload))), \
            patch.object(self.service, "dashboard", return_value={"wallets": []}), \
            patch.object(self.service, "build_sentiment_summary", return_value={
                "overallBias": "mixed", "consensus": [], "hip3Consensus": [], "signals": [],
            }), \
            patch.object(self.service, "build_position_groups", side_effect=fake_groups), \
            patch.object(self.service, "send_telegram_message", side_effect=send) as sender:
            result = self.service.check_alerts(send_notification=True)

        written = {}
        for path, payload in saved:
            if isinstance(payload, dict) and "state" in payload:
                written = payload["state"]
        return result, written, sender

    def test_failure_then_retry_delivers_once(self) -> None:
        first, state_after_failure, sender = self.run_cycle({}, fail=True)
        self.assertTrue(first["shouldNotify"])
        self.assertFalse(first["sent"])
        sender.assert_called_once()
        self.assertEqual(
            state_after_failure.get("actionableGroups", {}), {},
            "a failed send must not mark the group as announced",
        )
        self.assertEqual(
            state_after_failure.get("actionableEntryOutcomes", {}), {},
            "an alert nobody saw must not be measured",
        )
        self.assertEqual(
            len(state_after_failure.get("copyabilityEntryOutcomes", {})), 1,
            "the silent research stream must not depend on Telegram delivery",
        )

        second, state_after_success, sender = self.run_cycle(state_after_failure, fail=False)
        self.assertTrue(second["sent"], "the retry delivers the alert that was lost")
        self.assertEqual(len(state_after_success.get("actionableGroups", {})), 1)
        self.assertEqual(len(state_after_success.get("actionableEntryOutcomes", {})), 1)

        third, state_after_third, _sender = self.run_cycle(state_after_success, fail=False)
        self.assertEqual(third["changes"]["actionableEntries"], [], "no duplicate on the next cycle")
        self.assertEqual(
            len(state_after_third.get("actionableEntryOutcomes", {})), 1,
            "and no second record for the same alert",
        )


if __name__ == "__main__":
    unittest.main()
