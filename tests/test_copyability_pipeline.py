from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import server
from copyability_registry import build_wallet_copyability_registry, independent_wallet_episodes
from tests.test_wallet_quality_dimensions import ADDRESS, healthy_wallet


def departure_trade(index: int, *, address: str = ADDRESS, net_pct: float = 2.0) -> dict:
    entry = 1_800_000_000_000 + index * 20_000
    return {
        "id": f"trade-{index}",
        "coin": "ETH",
        "side": "long",
        "entryAt": entry,
        "departures": [{"address": address, "detectedAt": entry + 10_000, "netPct": net_pct}],
    }


class CopyabilityPipelineTests(unittest.TestCase):
    def test_wallet_exit_becomes_exact_registry_schema_and_passes_gate(self) -> None:
        trades = [departure_trade(index, net_pct=2.0 + (index % 3) * 0.1) for index in range(30)]
        state = {"version": "v3", "trades": trades}
        registry = build_wallet_copyability_registry(
            state, observation_complete=True, generated_at_ms=1_800_001_000_000
        )
        record = registry["wallets"][ADDRESS]
        self.assertEqual(
            set(record),
            {
                "method",
                "independentCompletedEpisodes",
                "costAdjustedNetReturnPct",
                "lowerConfidenceBoundPct",
                "observationComplete",
            },
        )
        self.assertEqual(record["independentCompletedEpisodes"], 30)
        self.assertGreater(record["lowerConfidenceBoundPct"], 0)

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "wallet_copyability.json"
            path.write_text(json.dumps(registry))
            loaded = server.load_wallet_copyability(path)

        dimensions = server.wallet_quality_dimensions(healthy_wallet(), loaded[ADDRESS])
        self.assertEqual(dimensions["copyability"]["status"], "pass")
        self.assertTrue(dimensions["tradeEligible"])

    def test_overlapping_wallet_positions_count_as_one_episode(self) -> None:
        first = departure_trade(0)
        overlapping = departure_trade(0)
        overlapping["id"] = "overlap"
        overlapping["entryAt"] += 1_000
        overlapping["departures"][0]["detectedAt"] += 5_000
        selected = independent_wallet_episodes({"trades": [first, overlapping]})
        self.assertEqual(len(selected[ADDRESS]), 1)

    def test_incomplete_experiment_remains_pending_even_with_sample(self) -> None:
        registry = build_wallet_copyability_registry(
            {"version": "v3", "trades": [departure_trade(index) for index in range(30)]},
            observation_complete=False,
        )
        assessment = server.copyability_assessment(registry["wallets"][ADDRESS])
        self.assertEqual(assessment["status"], "pending")


if __name__ == "__main__":
    unittest.main()
