import tempfile
import unittest
from pathlib import Path
from unittest import mock
from unittest.mock import patch

import pytest

import server
from server import (
    SIGNAL_OUTCOME_HORIZONS_MS,
    HyperliquidClient,
    WalletStore,
    WalletTrackerService,
)


class ShadowCalibrationTests(unittest.TestCase):
    """Regression coverage for four shadow-sampling/calibration fixes:

    1. Pseudo-replicated ("periodic"/no-fingerprint) shadow records must not
       inflate the sample/wins used for calibration, but must still be
       visible via "dependentSample".
    2. sizeChanged must react to a real change in position size, not to a
       mark-price move on an otherwise static position.
    3. Eviction of excess shadow records must not skip measuring outcomes
       that became due in the same cycle.
    4. A genuine walletCount of 0 must not be replaced by len(addresses).
    """

    @pytest.fixture(autouse=True)
    def _inject_tmp_path(self, tmp_path):
        self.tmp_path = tmp_path

    def setUp(self) -> None:
        self.service = WalletTrackerService(WalletStore(self.tmp_path / "wallets.json"), HyperliquidClient())

    # -- FIX 1: pseudo-replication -----------------------------------------

    def test_dependent_periodic_records_excluded_from_sample_but_counted(self) -> None:
        records: dict[str, dict] = {}
        for index in range(5):
            records[f"dep{index}"] = {
                "coin": "BTC",
                "probabilityScore": 82.0,
                "rawProbabilityScore": 82.0,
                "sampleReason": "periodic",
                "independentSample": False,
                "outcomes": {"4h": {"netReturnPct": 1.0, "degraded": False}},
            }
        for index in range(3):
            records[f"ind{index}"] = {
                "coin": "BTC",
                "probabilityScore": 82.0,
                "rawProbabilityScore": 82.0,
                "sampleReason": "walletSetChanged",
                "independentSample": True,
                "outcomes": {"4h": {"netReturnPct": -1.0, "degraded": False}},
            }

        calibration = self.service.build_signal_calibration(records)
        stats = calibration["groups"]["crypto"]["80"]

        # Only the 3 independent samples drive the maths...
        self.assertEqual(stats["sample"], 3)
        self.assertEqual(stats["wins"], 0)
        # ...but the 5 pseudo-replicated ones are still visible.
        self.assertEqual(stats["dependentSample"], 5)
        self.assertTrue(calibration["requireIndependentSamples"])

    def test_legacy_records_without_independent_sample_key_still_counted(self) -> None:
        # Records written before this field existed must remain independent.
        records = {
            str(index): {
                "coin": "BTC",
                "probabilityScore": 82.0,
                "rawProbabilityScore": 82.0,
                "outcomes": {"4h": {"netReturnPct": 1.0, "degraded": False}},
            }
            for index in range(4)
        }

        calibration = self.service.build_signal_calibration(records)
        stats = calibration["groups"]["crypto"]["80"]

        self.assertEqual(stats["sample"], 4)
        self.assertEqual(stats["dependentSample"], 0)

    # -- FIX 2: sizeChanged should key off size, not price -------------------

    def test_size_changed_ignores_pure_price_moves_but_reacts_to_size(self) -> None:
        now_ms = 1_700_000_000_000
        previous_fingerprint = {
            "walletAddresses": ["0xabc"],
            "walletCount": 1,
            "totalValue": 1000.0,
            "totalSize": 10.0,
            "freshAddLatestTime": 0,
        }
        # Gap large enough to pass the min-gap floor, small enough to stay
        # under the 24h "periodic" restart floor.
        previous_record = {
            "startedAt": now_ms - 3 * 60 * 60 * 1000,
            "consensusFingerprint": previous_fingerprint,
        }

        price_only_move = {**previous_fingerprint, "totalValue": 1400.0}
        reason = self.service.shadow_sample_reason(price_only_move, previous_record, now_ms=now_ms)
        self.assertEqual(reason, "")

        real_size_change = {**previous_fingerprint, "totalSize": 14.0, "totalValue": 1000.0}
        reason = self.service.shadow_sample_reason(real_size_change, previous_record, now_ms=now_ms)
        self.assertEqual(reason, "sizeChanged")

    def test_shadow_consensus_fingerprint_includes_total_size(self) -> None:
        fingerprint = self.service.shadow_consensus_fingerprint(
            {"coin": "ETH", "side": "long", "totalValue": 500.0, "totalSize": 5.0, "wallets": []}
        )
        self.assertEqual(fingerprint["totalSize"], 5.0)
        self.assertEqual(fingerprint["totalValue"], 500.0)

    # -- FIX 3: measure before evicting, and keep in-flight records ---------

    def test_due_in_flight_record_survives_eviction_over_newer_measured_ones(self) -> None:
        """A record that becomes due (and gets measured) this cycle must not
        be evicted in favour of newer records that are already fully
        measured and have nothing left to learn from."""
        now_ms = 1_700_000_000_000

        def record(coin: str, started_at: int, *, outcomes: dict) -> dict:
            return {
                "coin": coin,
                "side": "long",
                "signalKey": f"{coin}:long",
                "startedAt": started_at,
                "entryPrice": 100.0,
                "probabilityScore": 40.0,
                "rawProbabilityScore": 40.0,
                "shadow": True,
                "outcomes": dict(outcomes),
            }

        # Started 20h ago: due this cycle for every horizon up to 12h, but
        # not yet for 24h, so it remains in-flight even after measurement.
        due_started_at = now_ms - 20 * 60 * 60 * 1000
        due_key = f"shadow:DUE:long:{due_started_at}"
        previous = {due_key: record("DUE", due_started_at, outcomes={})}

        # Newer records that are already fully measured for every horizon -
        # nothing about them is due, so they never touch measure_signal_outcome_records.
        already_measured = {
            label: {
                "markPrice": 100.0,
                "grossReturnPct": 0.0,
                "netReturnPct": 0.0,
                "returnPct": 0.0,
                "measuredAt": now_ms,
                "measuredAfterMs": horizon_ms,
                "horizonMs": horizon_ms,
                "priceSource": "mark",
                "degraded": False,
            }
            for label, horizon_ms in SIGNAL_OUTCOME_HORIZONS_MS.items()
        }
        newer_keys = []
        for index in range(3):
            started_at = now_ms - index
            key = f"shadow:NEW{index}:long:{started_at}"
            newer_keys.append(key)
            previous[key] = record(f"NEW{index}", started_at, outcomes=already_measured)

        with patch.object(server, "SHADOW_SIGNAL_OUTCOME_MAX_RECORDS", 2), patch.object(
            self.service, "candidate_outcome_market_price", return_value=100.0
        ):
            records = self.service.update_shadow_signal_outcomes(
                previous, {"signals": [], "consensus": []}, now_ms=now_ms
            )

        # Cap is still enforced on the returned dict.
        self.assertEqual(len(records), 2)
        # The in-flight record's freshly-written outcome must have survived
        # eviction, even though it is the oldest record by startedAt and a
        # pure startedAt sort would have discarded it in favour of newer,
        # fully-measured records.
        self.assertIn(due_key, records)
        self.assertIn("12h", records[due_key]["outcomes"])
        self.assertNotIn("24h", records[due_key]["outcomes"])
        # Only the newest fully-measured record survives alongside it.
        self.assertIn(newer_keys[0], records)
        self.assertNotIn(newer_keys[1], records)
        self.assertNotIn(newer_keys[2], records)

    # -- FIX 4: a genuine walletCount of 0 must stay 0 -----------------------

    def test_wallet_count_zero_is_not_replaced_by_address_count(self) -> None:
        fingerprint = self.service.shadow_consensus_fingerprint(
            {
                "coin": "BTC",
                "side": "long",
                "walletCount": 0,
                "wallets": [{"address": "0xAAA"}, {"address": "0xBBB"}],
            }
        )
        self.assertEqual(fingerprint["walletCount"], 0)
        self.assertEqual(len(fingerprint["walletAddresses"]), 2)

    def test_wallet_count_falls_back_to_addresses_when_absent(self) -> None:
        fingerprint = self.service.shadow_consensus_fingerprint(
            {"coin": "BTC", "side": "long", "wallets": [{"address": "0xAAA"}, {"address": "0xBBB"}]}
        )
        self.assertEqual(fingerprint["walletCount"], 2)


if __name__ == "__main__":
    unittest.main()


class ShadowEvictionKeepsMeasurementsTest(unittest.TestCase):
    """A measurement written this cycle must survive the same cycle's trim.

    Regression for the original "measure before evict" reorder, which was a
    no-op: eviction sorted on ``startedAt``, which measurement never changes,
    so the oldest record could be measured and discarded in one call.
    """

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.service = WalletTrackerService(
            WalletStore(Path(self.tmp.name) / "wallets.json"), HyperliquidClient()
        )

    @staticmethod
    def _record(coin: str, started_at: int, outcomes: dict) -> dict:
        return {
            "coin": coin,
            "marketCoin": coin,
            "side": "long",
            "signalKey": f"{coin}:long",
            "startedAt": started_at,
            "entryPrice": 100.0,
            "probabilityScore": 60.0,
            "rawProbabilityScore": 60.0,
            "shadow": True,
            "published": False,
            "outcomes": outcomes,
        }

    def test_freshly_measured_oldest_record_is_not_trimmed_away(self) -> None:
        now_ms = 10_000_000_000
        longest = max(server.SIGNAL_OUTCOME_HORIZONS_MS.values())
        previous = {
            # Oldest by startedAt, unmeasured, and every horizon is now due.
            "shadow:BTC:long:1": self._record("BTC", now_ms - longest - 1_000, {}),
            # Newer, but nothing is due for them yet, so they stay empty.
            "shadow:ETH:long:2": self._record("ETH", now_ms - 1_000, {}),
            "shadow:SOL:long:3": self._record("SOL", now_ms - 500, {}),
        }
        summary = {
            "consensus": [
                {
                    "coin": "BTC",
                    "side": "long",
                    "markPrice": 110.0,
                    "wallets": [],
                    "totalValue": 1.0,
                    "totalSize": 1.0,
                }
            ],
            "signals": [],
        }
        with mock.patch.object(server, "SHADOW_SIGNAL_OUTCOME_MAX_RECORDS", 2):
            result = self.service.update_shadow_signal_outcomes(
                previous, summary, now_ms=now_ms
            )

        self.assertLessEqual(len(result), 2, "the cap must still be enforced")
        kept = result.get("shadow:BTC:long:1")
        self.assertIsNotNone(
            kept, "the record measured this cycle was evicted, losing its measurement"
        )
        self.assertTrue(
            kept.get("outcomes"),
            "the measured outcome must be persisted, not discarded by the trim",
        )
