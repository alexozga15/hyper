from __future__ import annotations

import threading
import time
from typing import Any


class RequestRateLimiter:
    """Paces outbound requests to one provider and absorbs its penalties.

    Lives in its own module rather than in server.py so the CoinMarketMan and
    Moni clients can pace themselves too: server.py imports those clients, so
    anything they import from server.py would be a cycle.
    """

    def __init__(self, requests_per_second: float = 6.0) -> None:
        self.interval = 1.0 / max(0.5, requests_per_second)
        self.next_request_at = 0.0
        self.lock = threading.Lock()
        # Every 429 lands here. Nothing else in the process counts them, which
        # is why the journal showed no trace of rate limiting while a third of
        # the wallets were being throttled.
        self.throttle_events = 0
        self.throttle_seconds = 0.0

    def wait(self) -> None:
        with self.lock:
            now = time.monotonic()
            delay = max(0.0, self.next_request_at - now)
            self.next_request_at = max(now, self.next_request_at) + self.interval
        if delay > 0:
            time.sleep(delay)

    def penalize(self, seconds: float) -> None:
        with self.lock:
            self.throttle_events += 1
            self.throttle_seconds += max(0.0, seconds)
            self.next_request_at = max(self.next_request_at, time.monotonic() + max(0.0, seconds))

    def throttle_report(self) -> dict[str, Any]:
        with self.lock:
            return {
                "events": self.throttle_events,
                "backoffSeconds": round(self.throttle_seconds, 2),
                "requestsPerSecond": round(1.0 / self.interval, 2),
            }
