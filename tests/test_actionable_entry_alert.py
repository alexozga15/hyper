"""The alert that fires when a group first comes within reach.

The board's actionable rows are the one surface here with a measured edge, but
they were only ever seen on the four-hourly digest, so a group could sit inside
the band for hours before the reader heard about it. These tests pin the
transition and the latch that stops a group hovering at the edge from producing
a stream of alerts.
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from server import (  # noqa: E402
    ACTIONABLE_ENTRY_RELEASE_DISTANCE_PCT,
    ALERT_ACTIONABLE_MAX_DISTANCE_PCT,
    ALERTS_FILE,
    CONVICTION_WIN_RATE_BASELINE,
    HyperliquidClient,
    WalletStore,
    WalletTrackerService,
)

NOW_MS = 1_700_000_000_000
ABOVE_BASELINE = CONVICTION_WIN_RATE_BASELINE * 100 + 5.0
BELOW_BASELINE = CONVICTION_WIN_RATE_BASELINE * 100 - 5.0


def group(
    distance_pct: float,
    *,
    quality: float | None = ABOVE_BASELINE,
    scored: int = 4,
    admitted: bool = True,
) -> dict:
    """A position group whose mark sits `distance_pct` from its reference."""
    reference = 100.0
    mark = reference * (1.0 + distance_pct / 100.0)
    size = 10.0
    return {
        "coin": "HYPE",
        "side": "long",
        "walletCount": 4,
        "qualityScoredWallets": scored,
        "qualityWinRatePct": quality,
        "qualityBestWinRatePct": quality,
        "positionRank": 72.5,
        "positionRankComponents": {"performance": 70.0, "evidence": 75.0, "risk": 80.0, "copyability": 50.0},
        "totalSize": size,
        "totalValue": mark * size,
        "recentAddPx": reference,
        "entryPx": reference,
        "positionCount": 4,
        "walletAddresses": ["0xa", "0xb", "0xc", "0xd"],
        "eligibleWalletAddresses": ["0xa", "0xb", "0xc"] if admitted else [],
        "tradeAdmission": {
            "status": "eligible" if admitted else "pending",
            "eligibleWalletCount": 3 if admitted else 0,
            "requiredWalletCount": 3,
        },
    }


class ActionableEntryAlertTests(unittest.TestCase):
    def setUp(self) -> None:
        self.service = WalletTrackerService(WalletStore(Path(ALERTS_FILE)), HyperliquidClient())

    def run_cycle(self, groups: list[dict], previous: dict | None) -> tuple[list[dict], dict]:
        # Only the crypto call returns rows; the commodity and stock calls
        # return nothing, so a group is not counted three times.
        responses = [groups, [], []]

        def fake_groups(_dashboard, **_kwargs):
            return responses.pop(0) if responses else []

        with patch.object(self.service, "build_position_groups", side_effect=fake_groups):
            return self.service.build_actionable_entry_alerts({}, previous, now_ms=NOW_MS)

    def test_entering_the_band_fires_once_and_latches(self) -> None:
        alerts, state = self.run_cycle([group(1.0)], None)
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0]["coin"], "HYPE")
        self.assertAlmostEqual(alerts[0]["distancePct"], 1.0, places=2)
        self.assertIn("HYPE:long", state)

    def test_staying_inside_the_band_does_not_alert_again(self) -> None:
        _, state = self.run_cycle([group(1.0)], None)
        alerts, next_state = self.run_cycle([group(2.0)], state)
        self.assertEqual(alerts, [])
        self.assertIn("HYPE:long", next_state)

    def test_leaving_the_band_but_not_the_release_gap_stays_latched(self) -> None:
        # The gap between the two thresholds is the whole point: without it a
        # group oscillating around 3.00% would alert on every crossing.
        _, state = self.run_cycle([group(1.0)], None)
        midway = (ALERT_ACTIONABLE_MAX_DISTANCE_PCT + ACTIONABLE_ENTRY_RELEASE_DISTANCE_PCT) / 2
        alerts, next_state = self.run_cycle([group(midway)], state)
        self.assertEqual(alerts, [])
        self.assertIn("HYPE:long", next_state, "must not re-arm inside the release gap")

        back_inside, after = self.run_cycle([group(1.0)], next_state)
        self.assertEqual(back_inside, [], "still latched, so no second alert")
        self.assertIn("HYPE:long", after)

    def test_moving_beyond_the_release_gap_re_arms(self) -> None:
        _, state = self.run_cycle([group(1.0)], None)
        far = ACTIONABLE_ENTRY_RELEASE_DISTANCE_PCT + 1.0
        alerts, released = self.run_cycle([group(far)], state)
        self.assertEqual(alerts, [])
        self.assertNotIn("HYPE:long", released, "beyond the release gap the group re-arms")

        alerts, _ = self.run_cycle([group(1.0)], released)
        self.assertEqual(len(alerts), 1, "a genuine re-entry alerts again")

    def test_historical_wr_below_baseline_does_not_override_copyability(self) -> None:
        alerts, state = self.run_cycle([group(1.0, quality=BELOW_BASELINE)], None)
        self.assertEqual(len(alerts), 1)
        self.assertIn("HYPE:long", state)

    def test_missing_historical_wr_does_not_override_copyability(self) -> None:
        alerts, state = self.run_cycle([group(1.0, scored=1)], None)
        self.assertEqual(len(alerts), 1)
        self.assertIn("HYPE:long", state)

        missing = group(1.0)
        missing["qualityWinRatePct"] = None
        alerts, state = self.run_cycle([missing], None)
        self.assertEqual(len(alerts), 1)
        self.assertIn("HYPE:long", state)

    def test_wr_alone_cannot_authorize_an_alert(self) -> None:
        alerts, state = self.run_cycle([group(1.0, admitted=False)], None)
        self.assertEqual(alerts, [])
        self.assertEqual(state, {})

    def test_pending_group_still_enters_the_silent_research_stream(self) -> None:
        responses = [[group(1.0, admitted=False)], [], []]

        def fake_groups(_dashboard, **_kwargs):
            return responses.pop(0) if responses else []

        with patch.object(self.service, "build_position_groups", side_effect=fake_groups):
            observations, state = self.service.build_actionable_entry_alerts(
                {}, None, now_ms=NOW_MS, require_admission=False, limit=None
            )
        self.assertEqual(len(observations), 1)
        self.assertIn("HYPE:long", state)

    def test_a_group_without_a_usable_reference_keeps_its_latch(self) -> None:
        # Re-arming on missing data would let the next readable cycle fire a
        # duplicate alert for a group that never actually left.
        _, state = self.run_cycle([group(1.0)], None)
        broken = group(1.0)
        broken["recentAddPx"] = 0.0
        broken["entryPx"] = 0.0
        alerts, next_state = self.run_cycle([broken], state)
        self.assertEqual(alerts, [])
        self.assertIn("HYPE:long", next_state)

    def test_the_boundary_itself_counts_as_inside(self) -> None:
        alerts, _ = self.run_cycle([group(ALERT_ACTIONABLE_MAX_DISTANCE_PCT)], None)
        self.assertEqual(len(alerts), 1, "exactly at the threshold is within reach")

    def test_the_message_renders_the_block(self) -> None:
        alerts, _ = self.run_cycle([group(-1.5)], None)
        changes = {
            "biasChanged": False,
            "addedConsensus": [],
            "removedConsensus": [],
            "changedConsensus": [],
            "hip3Added": [],
            "hip3Removed": [],
            "actionableEntries": alerts,
            "clusteredOpenPositions": [],
            "newLargePositions": [],
            "increasedLargePositions": [],
            "closedLargePositions": [],
            "addedSignals": [],
            "removedSignals": [],
            "changedSignals": [],
            "addedCandidateSignals": [],
            "addedCmmSignals": [],
            "changedCmmSignals": [],
        }
        message = self.service.build_telegram_message(changes, {"consensus": []}, min_wallets=3)
        self.assertIn("Now within", message)
        self.assertIn("HYPE LONG", message)
        self.assertIn("Rank 72/100", message)


if __name__ == "__main__":
    unittest.main()
