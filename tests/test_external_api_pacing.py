from __future__ import annotations

import unittest
import urllib.error
from unittest.mock import patch

from coinmarketman import (
    COINMARKETMAN_RATE_LIMIT_PAUSE_SECONDS,
    CoinMarketManApiError,
    CoinMarketManClient,
)
from moni import MoniApiError, MoniClient
from ratelimit import RequestRateLimiter
from scripts.run_health_monitor import detect_external_api_backoff, detect_health_issues

NOW_MS = 2_000_000_000_000
HOUR_MS = 60 * 60 * 1000


def iso(ms: int) -> str:
    from datetime import datetime, timezone

    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z")


class RecordingLimiter(RequestRateLimiter):
    """Counts pacing calls without spending real time on them."""

    def __init__(self) -> None:
        super().__init__(1000)
        self.waits = 0
        self.penalties: list[float] = []

    def wait(self) -> None:
        self.waits += 1

    def penalize(self, seconds: float) -> None:
        self.penalties.append(seconds)


class ExternalClientPacingTests(unittest.TestCase):
    """Both third-party clients used to fire requests with nothing between them."""

    def test_cmm_paces_every_request(self) -> None:
        limiter = RecordingLimiter()
        client = CoinMarketManClient(token="t", base_url="https://example.test", rate_limiter=limiter)
        with patch("coinmarketman.urllib.request.urlopen", side_effect=urllib.error.URLError("boom")):
            for _ in range(3):
                with self.assertRaises(CoinMarketManApiError):
                    client.request("segments")
        self.assertEqual(limiter.waits, 3)

    def test_cmm_pauses_before_retrying_on_the_backup_token(self) -> None:
        """The retry after a 429 is the likeliest request to be limited next."""
        limiter = RecordingLimiter()
        client = CoinMarketManClient(token="primary", base_url="https://example.test", rate_limiter=limiter)
        client.backup_token = "backup"

        def fake_request(url: str, token: str) -> dict:
            if token == "primary":
                raise CoinMarketManApiError("CMM API returned HTTP 429: daily limit")
            return {"ok": True}

        with patch.object(client, "_request_with_token", side_effect=fake_request):
            self.assertEqual(client.request("positions/heatmap"), {"ok": True})
        self.assertEqual(limiter.penalties, [COINMARKETMAN_RATE_LIMIT_PAUSE_SECONDS])

    def test_cmm_does_not_pause_for_errors_that_do_not_fail_over(self) -> None:
        limiter = RecordingLimiter()
        client = CoinMarketManClient(token="primary", base_url="https://example.test", rate_limiter=limiter)
        client.backup_token = "backup"
        with patch.object(
            client, "_request_with_token", side_effect=CoinMarketManApiError("HTTP 401: unauthorized")
        ), self.assertRaises(CoinMarketManApiError):
            client.request("segments")
        self.assertEqual(limiter.penalties, [])

    def test_moni_paces_every_request(self) -> None:
        limiter = RecordingLimiter()
        client = MoniClient(api_key="k", rate_limiter=limiter)
        with patch("moni.urllib.request.urlopen", side_effect=urllib.error.URLError("boom")):
            for _ in range(2):
                with self.assertRaises(MoniApiError):
                    client.api_key_status()
        self.assertEqual(limiter.waits, 2)

    def test_clients_share_one_limiter_by_default(self) -> None:
        """The provider limits the account, not the client object."""
        self.assertIs(
            CoinMarketManClient(token="a").rate_limiter,
            CoinMarketManClient(token="b").rate_limiter,
        )
        self.assertIs(MoniClient(api_key="a").rate_limiter, MoniClient(api_key="b").rate_limiter)


class ExternalBackoffVisibilityTests(unittest.TestCase):
    """A six-hour backoff must not be invisible.

    Both services keep answering from cache while backed off, so every other
    health signal stays green while published signals are being confirmed
    against hours-old external data.
    """

    def test_active_cmm_rate_limit_is_reported(self) -> None:
        issues = detect_external_api_backoff(
            {"cmmSignals": {"rateLimitedUntil": iso(NOW_MS + 5 * HOUR_MS)}}, now_ms=NOW_MS
        )
        self.assertEqual(issues, ["CMM rate limited for another 5.0h"])

    def test_expired_cmm_rate_limit_is_not_reported(self) -> None:
        issues = detect_external_api_backoff(
            {"cmmSignals": {"rateLimitedUntil": iso(NOW_MS - HOUR_MS)}}, now_ms=NOW_MS
        )
        self.assertEqual(issues, [])

    def test_moni_error_backoff_is_reported(self) -> None:
        issues = detect_external_api_backoff(
            {"moniSocial": {"error": "HTTP 429", "nextFetchAt": iso(NOW_MS + 2 * HOUR_MS)}},
            now_ms=NOW_MS,
        )
        self.assertEqual(issues, ["Moni backed off for another 2.0h"])

    def test_moni_ordinary_cache_ttl_is_not_a_backoff(self) -> None:
        """nextFetchAt is set on success too; only an error makes it a penalty."""
        issues = detect_external_api_backoff(
            {"moniSocial": {"nextFetchAt": iso(NOW_MS + 20 * HOUR_MS)}}, now_ms=NOW_MS
        )
        self.assertEqual(issues, [])

    def test_missing_or_malformed_state_is_quiet(self) -> None:
        for state in ({}, {"cmmSignals": None}, {"cmmSignals": {}}, {"moniSocial": {"error": ""}}):
            self.assertEqual(detect_external_api_backoff(state, now_ms=NOW_MS), [], repr(state))

    def test_health_check_surfaces_the_backoff(self) -> None:
        issues = detect_health_issues(
            {
                "state": {
                    "lastCheckedAt": iso(NOW_MS),
                    "shadowSignalOutcomes": {"BTC:long:1": {"startedAt": NOW_MS}},
                    "signalOutcomes": {"BTC:long:1": {"startedAt": NOW_MS}},
                    "cmmSignals": {"rateLimitedUntil": iso(NOW_MS + 6 * HOUR_MS)},
                }
            },
            {"cacheCoverage": 1.0},
            now_ms=NOW_MS,
            disk_free_pct=50,
        )
        self.assertEqual(issues, ["CMM rate limited for another 6.0h"])


if __name__ == "__main__":
    unittest.main()
