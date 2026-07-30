from __future__ import annotations

import unittest
from unittest.mock import patch

from moni import MoniApiError, MoniClient
from server import HyperliquidClient, WalletTrackerService


class MoniClientTests(unittest.TestCase):
    def test_requires_api_key(self) -> None:
        client = MoniClient(api_key="")
        with self.assertRaises(MoniApiError):
            client.api_key_status()


class MoniSignalContextTests(unittest.TestCase):
    def setUp(self) -> None:
        self.service = WalletTrackerService(object(), HyperliquidClient())

    def test_social_trend_compares_h24_with_d7_daily_pace(self) -> None:
        self.assertEqual(self.service.moni_social_trend(30, 70)[0], "rising")
        self.assertEqual(self.service.moni_social_trend(8, 70)[0], "steady")
        self.assertEqual(self.service.moni_social_trend(2, 70)[0], "fading")

    def test_cached_summary_fetches_only_top_mapped_signal(self) -> None:
        class FakeMoniClient:
            enabled = True

            def __init__(self) -> None:
                self.handles: list[tuple[str, str]] = []

            def api_key_status(self) -> dict:
                return {"monthPointsLimit": 300, "monthPointsUsage": 10}

            def smart_mentions_history(self, handle: str, timeframe: str) -> dict:
                self.handles.append((handle, timeframe))
                return {"timeframeChange": 21 if timeframe == "H24" else 70}

        fake = FakeMoniClient()
        self.service.moni_client = fake
        signals = [
            {"coin": "SOL", "probabilityScore": 85},
            {"coin": "ETH", "probabilityScore": 80},
        ]

        summary = self.service.build_cached_moni_social_summary({}, signals)

        self.assertEqual(fake.handles, [("solana", "H24"), ("solana", "D7")])
        self.assertEqual(summary["projects"]["SOL"]["trend"], "rising")
        self.assertEqual(summary["monthPointsUsage"], 18)

    def test_global_next_fetch_prevents_command_or_signal_quota_burn(self) -> None:
        class FakeMoniClient:
            enabled = True

            def api_key_status(self) -> dict:
                raise AssertionError("cache should prevent API requests")

        self.service.moni_client = FakeMoniClient()
        cached = {
            "moniSocial": {
                "enabled": True,
                "nextFetchAt": "2099-01-01T00:00:00Z",
                "projects": {},
            }
        }
        summary = self.service.build_cached_moni_social_summary(
            cached,
            [{"coin": "SOL", "probabilityScore": 90}],
        )
        self.assertTrue(summary["cacheHit"])

    def test_empty_signal_set_seeds_free_quota_status(self) -> None:
        class FakeMoniClient:
            enabled = True

            def api_key_status(self) -> dict:
                return {
                    "isActive": True,
                    "monthPointsLimit": 300,
                    "monthPointsUsage": 10,
                    "expiresAt": 1_900_000_000,
                }

        self.service.moni_client = FakeMoniClient()
        summary = self.service.build_cached_moni_social_summary({}, [])
        self.assertEqual(summary["monthPointsUsage"], 10)
        self.assertEqual(summary["monthPointsLimit"], 300)
        self.assertEqual(summary["projects"], {})

    def test_social_context_does_not_change_probability(self) -> None:
        summary = {
            "signals": [{"coin": "SOL", "side": "short", "probabilityScore": 82}],
            "signalCount": 1,
        }
        social = {
            "enabled": True,
            "projects": {
                "SOL": {
                    "trend": "rising",
                    "h24SmartMentions": 30,
                    "d7SmartMentions": 70,
                    "paceRatio": 3,
                    "handle": "solana",
                    "generatedAt": "2099-01-01T00:00:00Z",
                }
            },
        }
        with patch("server.current_time_ms", return_value=1_800_000_000_000):
            enriched = self.service.apply_moni_social_context(summary, social)
        self.assertEqual(enriched["signals"][0]["probabilityScore"], 82)
        self.assertEqual(enriched["signals"][0]["moniSocialTrend"], "rising")

    def test_moni_message_reports_quota_without_fetching(self) -> None:
        message = self.service.build_moni_social_message(
            {
                "enabled": True,
                "checkedAt": "2026-07-30T00:00:00Z",
                "monthPointsLimit": 300,
                "monthPointsUsage": 18,
                "projects": {
                    "SOL": {
                        "trend": "rising",
                        "h24SmartMentions": 21,
                        "d7SmartMentions": 70,
                        "paceRatio": 2.1,
                    }
                },
            }
        )
        self.assertIn("SOL: rising", message)
        self.assertIn("Quota: 18/300 points", message)


if __name__ == "__main__":
    unittest.main()
