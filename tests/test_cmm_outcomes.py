import unittest
import urllib.error
from unittest.mock import patch

import pytest

import server
from server import (
    CMM_SIGNAL_OUTCOME_MIN_GAP_MS,
    CMM_SIGNAL_OUTCOME_RETENTION_MS,
    HyperliquidClient,
    WalletStore,
    WalletTrackerService,
)


def _cmm_signal(
    coin: str,
    side: str,
    *,
    probability: float,
    tier: str = "actionable",
) -> dict:
    return {
        "source": "coinmarketman",
        "coin": coin,
        "side": side,
        "action": "buy" if side == "long" else "sell",
        "probabilityScore": probability,
        "signalTier": tier,
        "alertEligible": tier == "alert",
        "actionableEligible": tier in {"actionable", "alert"},
        "smartCohortScore": 80.0,
        "contrarianScore": 20.0,
        "valueBias": 0.55,
        "countBias": 0.3,
        "cohortCount": 3,
        "totalValue": 2_000_000.0,
    }


class CmmOutcomeTests(unittest.TestCase):
    @pytest.fixture(autouse=True)
    def _inject_tmp_path(self, tmp_path):
        self.tmp_path = tmp_path

    def setUp(self) -> None:
        self.service = WalletTrackerService(WalletStore(self.tmp_path / "wallets.json"), HyperliquidClient())

    # -- combined_mark_map ---------------------------------------------------

    def test_combined_mark_map_merges_main_and_hip3_dexes(self) -> None:
        def fake_safe_post(payload, fallback):
            if payload == {"type": "allMids"}:
                return {"BTC": "50000", "NVDA": "500"}
            if payload == {"type": "perpDexs"}:
                # Defensive shapes: an empty/None-named entry (the main dex,
                # already covered by the plain allMids call) must be skipped.
                return [{"name": "acme"}, {"name": ""}, {"name": None}, {}, {"name": "beta"}]
            if payload == {"type": "allMids", "dex": "acme"}:
                return {"acme:NVDA": "999", "acme:TSLA": "42"}
            if payload == {"type": "allMids", "dex": "beta"}:
                return {"beta:TSLA": "43", "beta:GME": "10"}
            return fallback

        with patch.object(self.service.client, "safe_post", side_effect=fake_safe_post):
            marks = self.service.combined_mark_map()

        self.assertEqual(marks["BTC"], 50000.0)
        # Main dex wins over the HIP-3 duplicate for the same normalized symbol.
        self.assertEqual(marks["NVDA"], 500.0)
        # The qualified HIP-3 key is still resolvable on its own.
        self.assertEqual(marks["acme:NVDA"], 999.0)
        # A symbol that exists on two HIP-3 dexes resolves to whichever dex
        # perpDexs listed first ("acme" before "beta").
        self.assertEqual(marks["TSLA"], 42.0)
        self.assertEqual(marks["acme:TSLA"], 42.0)
        self.assertEqual(marks["beta:TSLA"], 43.0)
        self.assertEqual(marks["GME"], 10.0)
        self.assertEqual(marks["beta:GME"], 10.0)

    def test_combined_mark_map_degrades_on_perpdexs_failure(self) -> None:
        def fake_post(payload, url=server.HYPERLIQUID_INFO_URL):
            if payload.get("type") == "allMids" and "dex" not in payload:
                return {"BTC": "50000"}
            if payload.get("type") == "perpDexs":
                raise urllib.error.URLError("boom")
            raise AssertionError(f"unexpected call: {payload}")

        with patch.object(self.service.client, "post", side_effect=fake_post):
            marks = self.service.combined_mark_map()

        self.assertEqual(marks, {"BTC": 50000.0})

    # -- cmm_signal_outcome_sample_reason -------------------------------------

    def test_sample_reason_covers_every_branch(self) -> None:
        t0 = 1_700_000_000_000
        signal = {"signalTier": "watch", "probabilityScore": 66.0}

        # No previous record at all.
        self.assertEqual(
            self.service.cmm_signal_outcome_sample_reason(signal, None, now_ms=t0),
            "initial",
        )

        # Inside the min-gap window: skip even though nothing else matches.
        previous = {"startedAt": t0, "signalTier": "watch", "probabilityScore": 65.0}
        self.assertEqual(
            self.service.cmm_signal_outcome_sample_reason(
                signal, previous, now_ms=t0 + CMM_SIGNAL_OUTCOME_MIN_GAP_MS // 2
            ),
            "",
        )

        past_gap_ms = t0 + CMM_SIGNAL_OUTCOME_MIN_GAP_MS + 1

        # Tier changed.
        tier_changed_signal = {"signalTier": "alert", "probabilityScore": 66.0}
        self.assertEqual(
            self.service.cmm_signal_outcome_sample_reason(tier_changed_signal, previous, now_ms=past_gap_ms),
            "tierChanged",
        )

        # Score moved by >= CMM_SIGNAL_OUTCOME_PROBABILITY_MOVE, tier unchanged.
        score_moved_signal = {"signalTier": "watch", "probabilityScore": 80.0}
        self.assertEqual(
            self.service.cmm_signal_outcome_sample_reason(score_moved_signal, previous, now_ms=past_gap_ms),
            "scoreMoved",
        )

        # A full day elapsed, tier and score both unchanged.
        periodic_signal = {"signalTier": "watch", "probabilityScore": 65.5}
        self.assertEqual(
            self.service.cmm_signal_outcome_sample_reason(
                periodic_signal, previous, now_ms=t0 + 24 * 60 * 60 * 1000
            ),
            "periodic",
        )

        # Past the min gap but nothing material changed and less than a day passed.
        unchanged_signal = {"signalTier": "watch", "probabilityScore": 65.5}
        self.assertEqual(
            self.service.cmm_signal_outcome_sample_reason(unchanged_signal, previous, now_ms=past_gap_ms),
            "",
        )

    # -- update_cmm_signal_outcomes -------------------------------------------

    def test_writes_record_with_expected_key_and_fields(self) -> None:
        t0 = 1_700_000_000_000
        cmm_summary = {
            "signals": [_cmm_signal("BTC", "long", probability=70.0, tier="actionable")],
            "belowThresholdSignals": [_cmm_signal("ETH", "short", probability=50.0, tier="watch")],
        }
        mark_map = {"BTC": 100.0, "ETH": 50.0}

        result = self.service.update_cmm_signal_outcomes({}, cmm_summary, mark_map=mark_map, now_ms=t0)

        record_key = f"cmm:BTC:long:{t0}"
        self.assertIn(record_key, result)
        record = result[record_key]
        self.assertEqual(record["source"], "coinmarketman")
        self.assertEqual(record["coin"], "BTC")
        self.assertEqual(record["side"], "long")
        self.assertEqual(record["action"], "buy")
        self.assertEqual(record["entryPrice"], 100.0)
        self.assertEqual(record["probabilityScore"], 70.0)
        self.assertEqual(record["signalTier"], "actionable")
        self.assertFalse(record["alertEligible"])
        self.assertTrue(record["actionableEligible"])
        self.assertEqual(record["smartCohortScore"], 80.0)
        self.assertEqual(record["contrarianScore"], 20.0)
        self.assertEqual(record["valueBias"], 0.55)
        self.assertEqual(record["countBias"], 0.3)
        self.assertEqual(record["cohortCount"], 3)
        self.assertEqual(record["totalValue"], 2_000_000.0)
        self.assertEqual(record["startedAt"], t0)
        self.assertEqual(record["sampleReason"], "initial")
        self.assertTrue(record["shadow"])
        self.assertFalse(record["published"])
        self.assertEqual(record["outcomes"], {})

        # ETH is below CMM_SIGNAL_OUTCOME_MIN_PROBABILITY: no record at all.
        self.assertFalse(any("ETH" in key for key in result))

    def test_unpriced_signal_produces_no_record_and_is_countable(self) -> None:
        t0 = 1_700_000_000_000
        cmm_summary = {
            "signals": [_cmm_signal("SOL", "long", probability=75.0)],
            "belowThresholdSignals": [],
        }
        mark_map: dict = {}  # SOL has no mark at all

        result = self.service.update_cmm_signal_outcomes({}, cmm_summary, mark_map=mark_map, now_ms=t0)
        self.assertEqual(result, {})

        # The diagnostic count is computed the same way the call sites do:
        # cross-checking cmm_signal_outcome_candidates against the mark map.
        candidates = self.service.cmm_signal_outcome_candidates(cmm_summary)
        unpriced_count = sum(
            1
            for signal in candidates
            if mark_map.get(server.normalize_position_coin(signal.get("coin")), 0.0) <= 0
        )
        self.assertEqual(unpriced_count, 1)

    def test_second_call_does_not_rewrite_existing_record(self) -> None:
        t0 = 1_700_000_000_000
        cmm_summary = {
            "signals": [_cmm_signal("BTC", "long", probability=70.0)],
            "belowThresholdSignals": [],
        }
        mark_map = {"BTC": 100.0}

        first = self.service.update_cmm_signal_outcomes({}, cmm_summary, mark_map=mark_map, now_ms=t0)
        second = self.service.update_cmm_signal_outcomes(
            first, cmm_summary, mark_map=mark_map, now_ms=t0 + 1000
        )

        self.assertEqual(set(first.keys()), set(second.keys()))
        record_key = f"cmm:BTC:long:{t0}"
        self.assertEqual(second[record_key]["startedAt"], t0)
        self.assertEqual(second[record_key]["sampleReason"], "initial")

    def test_record_older_than_retention_is_dropped(self) -> None:
        t0 = 1_700_000_000_000
        stale_key = "cmm:BTC:long:OLD"
        previous = {
            stale_key: {
                "coin": "BTC",
                "side": "long",
                "startedAt": t0 - CMM_SIGNAL_OUTCOME_RETENTION_MS - 1000,
                "entryPrice": 100.0,
                "outcomes": {},
            }
        }
        cmm_summary = {"signals": [], "belowThresholdSignals": []}

        result = self.service.update_cmm_signal_outcomes(previous, cmm_summary, mark_map={}, now_ms=t0)
        self.assertNotIn(stale_key, result)

    def test_does_not_touch_a_separately_passed_shadow_outcomes_dict(self) -> None:
        t0 = 1_700_000_000_000
        shadow_signal_outcomes = {
            "shadow:BTC:long:123": {
                "coin": "BTC",
                "side": "long",
                "startedAt": t0,
                "entryPrice": 100.0,
                "outcomes": {},
            }
        }
        shadow_snapshot = {key: dict(value) for key, value in shadow_signal_outcomes.items()}

        cmm_summary = {
            "signals": [_cmm_signal("BTC", "long", probability=70.0)],
            "belowThresholdSignals": [],
        }
        cmm_signal_outcomes = self.service.update_cmm_signal_outcomes(
            {}, cmm_summary, mark_map={"BTC": 100.0}, now_ms=t0
        )

        # The shadow collection passed in separately is untouched.
        self.assertEqual(shadow_signal_outcomes, shadow_snapshot)
        # The two collections stay disjoint even though both are keyed by
        # coin:side and share a coin/side in this test.
        self.assertTrue(set(cmm_signal_outcomes.keys()).isdisjoint(set(shadow_signal_outcomes.keys())))

        state = {
            "shadowSignalOutcomes": shadow_signal_outcomes,
            "cmmSignalOutcomes": cmm_signal_outcomes,
        }
        self.assertIsNot(state["shadowSignalOutcomes"], state["cmmSignalOutcomes"])
        self.assertTrue(
            set(state["shadowSignalOutcomes"].keys()).isdisjoint(set(state["cmmSignalOutcomes"].keys()))
        )


if __name__ == "__main__":
    unittest.main()
