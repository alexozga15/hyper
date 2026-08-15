from __future__ import annotations

import unittest
from unittest.mock import patch

from scripts.run_health_monitor import detect_health_issues
from server import (
    WALLET_IDLE_FILL_THRESHOLD_MS,
    HyperliquidClient,
    RequestRateLimiter,
    TrackedWallet,
    WalletTrackerService,
    iso_to_ms,
)

NOW_MS = 2_000_000_000_000
MINUTE_MS = 60 * 1000
HOUR_MS = 60 * MINUTE_MS
DAY_MS = 24 * HOUR_MS


def wallet(address: str) -> TrackedWallet:
    return TrackedWallet(address=address, alias=address, notes="", created_at="")


def cache_entry(*, last_fill_ms: int, fetched_ms: int) -> dict[str, object]:
    return {
        "recentFills": [{"coin": "BTC", "direction": "Open Long", "time": last_fill_ms}],
        "fillFetchedAtMs": fetched_ms,
    }


def aged_out_entry(*, idle_days: float, fetched_ms: int) -> dict[str, object]:
    """A wallet quiet for longer than the recent-fill cache retains.

    Its fills have aged out of the cache, so the only remaining evidence of
    when it last traded is the cached daysSinceLastFill.
    """
    return {"recentFills": [], "daysSinceLastFill": idle_days, "fillFetchedAtMs": fetched_ms}


class IdleFillBackoffTests(unittest.TestCase):
    """The idle-wallet backoff decides which wallets skip their fill fetch."""

    def setUp(self) -> None:
        self.service = WalletTrackerService(object(), HyperliquidClient(RequestRateLimiter(1000)))

    def skip_for(self, entry: object, address: str = "0xabc") -> set[str]:
        cache = {address: entry} if entry is not None else {}
        return self.service.wallet_idle_fill_skip_addresses(
            [wallet(address)], cache, now_ms=NOW_MS
        )

    def test_idle_wallet_fetched_recently_is_skipped(self) -> None:
        entry = cache_entry(last_fill_ms=NOW_MS - 5 * DAY_MS, fetched_ms=NOW_MS - MINUTE_MS)
        self.assertEqual(self.skip_for(entry), {"0xabc"})

    def test_active_wallet_is_never_skipped(self) -> None:
        entry = cache_entry(last_fill_ms=NOW_MS - HOUR_MS, fetched_ms=NOW_MS - MINUTE_MS)
        self.assertEqual(self.skip_for(entry), set())

    def test_wallet_just_under_the_idle_threshold_is_not_skipped(self) -> None:
        entry = cache_entry(
            last_fill_ms=NOW_MS - WALLET_IDLE_FILL_THRESHOLD_MS + MINUTE_MS,
            fetched_ms=NOW_MS - MINUTE_MS,
        )
        self.assertEqual(self.skip_for(entry), set())

    def test_idle_wallet_due_for_its_slow_poll_is_fetched(self) -> None:
        entry = cache_entry(last_fill_ms=NOW_MS - 5 * DAY_MS, fetched_ms=NOW_MS - 2 * HOUR_MS)
        self.assertEqual(self.skip_for(entry), set())

    def test_long_idle_wallet_is_skipped_after_its_fills_age_out(self) -> None:
        """The quietest wallets are the point of the backoff.

        The recent-fill cache only retains a week, so a wallet silent for
        longer has an empty fill list. Reading idleness from that list alone
        made exactly these wallets look like wallets with no data, and they
        were polled every cycle forever.
        """
        for idle_days in (8.1, 30.1, 61.4):
            entry = aged_out_entry(idle_days=idle_days, fetched_ms=NOW_MS - MINUTE_MS)
            self.assertEqual(self.skip_for(entry), {"0xabc"}, f"idle {idle_days}d")

    def test_known_idle_age_wins_over_the_cached_fill_list(self) -> None:
        entry = {
            "recentFills": [{"coin": "BTC", "direction": "Open Long", "time": NOW_MS - HOUR_MS}],
            "daysSinceLastFill": 9.0,
            "fillFetchedAtMs": NOW_MS - MINUTE_MS,
        }
        self.assertEqual(self.skip_for(entry), {"0xabc"})

    def test_wallet_with_no_evidence_at_all_is_never_skipped(self) -> None:
        """Silence with no data behind it is not evidence of idleness."""
        self.assertEqual(self.skip_for({"recentFills": [], "fillFetchedAtMs": NOW_MS}), set())
        self.assertEqual(self.skip_for({"fillFetchedAtMs": NOW_MS}), set())
        self.assertEqual(self.skip_for(None), set())

    def test_recently_active_wallet_with_aged_out_list_is_not_skipped(self) -> None:
        entry = aged_out_entry(idle_days=0.5, fetched_ms=NOW_MS - MINUTE_MS)
        self.assertEqual(self.skip_for(entry), set())

    def test_wallet_never_fill_fetched_is_not_skipped(self) -> None:
        entry = {"recentFills": [{"coin": "BTC", "direction": "Open Long", "time": NOW_MS - 5 * DAY_MS}]}
        self.assertEqual(self.skip_for(entry), set())

    def test_zero_interval_disables_the_backoff(self) -> None:
        entry = cache_entry(last_fill_ms=NOW_MS - 5 * DAY_MS, fetched_ms=NOW_MS - MINUTE_MS)
        with patch("server.WALLET_IDLE_FILL_INTERVAL_MS", 0):
            self.assertEqual(self.skip_for(entry), set())


class SkippedFetchAccountingTests(unittest.TestCase):
    """A skipped fetch must not be reported as a failed one."""

    def setUp(self) -> None:
        self.service = WalletTrackerService(object(), HyperliquidClient(RequestRateLimiter(1000)))

    def test_skipped_fetch_is_not_a_failure(self) -> None:
        snapshot = {"dataQuality": {"fillsFetchOk": False, "fillsFetchSkipped": True}}
        self.assertFalse(self.service.wallet_fill_fetch_failed(snapshot))
        self.assertTrue(self.service.wallet_fill_fetch_skipped(snapshot))

    def test_real_failure_still_counts(self) -> None:
        snapshot = {"dataQuality": {"fillsFetchOk": False, "fillsFetchSkipped": False}}
        self.assertTrue(self.service.wallet_fill_fetch_failed(snapshot))
        self.assertFalse(self.service.wallet_fill_fetch_skipped(snapshot))

    def test_successful_fetch_is_neither(self) -> None:
        snapshot = {"dataQuality": {"fillsFetchOk": True}}
        self.assertFalse(self.service.wallet_fill_fetch_failed(snapshot))
        self.assertFalse(self.service.wallet_fill_fetch_skipped(snapshot))


class ThrottleVisibilityTests(unittest.TestCase):
    def test_penalize_is_counted_and_reported(self) -> None:
        limiter = RequestRateLimiter(6)
        self.assertEqual(limiter.throttle_report()["events"], 0)
        limiter.penalize(1.5)
        limiter.penalize(0.5)
        report = limiter.throttle_report()
        self.assertEqual(report["events"], 2)
        self.assertAlmostEqual(report["backoffSeconds"], 2.0)
        self.assertAlmostEqual(report["requestsPerSecond"], 6.0)


class StaleThresholdTests(unittest.TestCase):
    CHECKED_AT = "2033-05-18T03:21:00Z"

    def issues_at(self, age_ms: int) -> list[str]:
        checked_ms = iso_to_ms(self.CHECKED_AT)
        self.assertGreater(checked_ms, 0, "fixture timestamp must parse")
        return detect_health_issues(
            {"state": {"lastCheckedAt": self.CHECKED_AT}},
            {"cacheCoverage": 1.0},
            now_ms=checked_ms + age_ms,
            disk_free_pct=50,
        )

    def test_threshold_tolerates_a_missed_ten_minute_run(self) -> None:
        """A 12-minute-old check is one late run, not an outage."""
        self.assertNotIn("sentiment check stale >25m", self.issues_at(12 * MINUTE_MS))

    def test_threshold_fires_once_two_runs_are_missed(self) -> None:
        self.assertIn("sentiment check stale >25m", self.issues_at(26 * MINUTE_MS))

    def test_threshold_still_fires_when_genuinely_stale(self) -> None:
        issues = detect_health_issues(
            {"state": {"lastCheckedAt": "2020-01-01T00:00:00Z"}},
            {"cacheCoverage": 1.0},
            now_ms=NOW_MS,
            disk_free_pct=50,
        )
        self.assertIn("sentiment check stale >25m", issues)


if __name__ == "__main__":
    unittest.main()
