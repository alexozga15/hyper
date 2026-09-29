from __future__ import annotations

import threading
import time
import math
import sqlite3
from contextlib import closing
from pathlib import Path
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

    def wait(self, *, max_wait_seconds: float | None = None) -> bool:
        with self.lock:
            now = time.monotonic()
            delay = max(0.0, self.next_request_at - now)
            if max_wait_seconds is not None and delay > max_wait_seconds:
                return False
            self.next_request_at = max(now, self.next_request_at) + self.interval
        if delay > 0:
            time.sleep(delay)
        return True

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


LOW_WEIGHT_INFO = {
    "l2Book", "allMids", "clearinghouseState", "orderStatus",
    "spotClearinghouseState", "exchangeStatus",
}
ROW_WEIGHT_INFO = {
    "recentTrades", "historicalOrders", "userFills", "userFillsByTime",
    "fundingHistory", "userFunding", "nonUserFundingUpdates", "twapHistory",
    "userTwapSliceFills", "userTwapSliceFillsByTime", "delegatorHistory",
    "delegatorRewards", "validatorStats",
}


def hyperliquid_request_weight(payload: dict[str, Any], response: Any = None) -> int:
    """Reserve a full page before dispatch; charge actual returned rows afterward."""
    kind = str(payload.get("type") or "")
    if kind in LOW_WEIGHT_INFO:
        return 2
    if kind == "userRole":
        return 60
    if kind in ROW_WEIGHT_INFO:
        count = len(response) if isinstance(response, list) else 2000
        return 20 + math.ceil(count / 20)
    if kind == "candleSnapshot":
        count = 5000
        req = payload.get("req") or {}
        interval = str(req.get("interval") or "")
        units = {"m": 60_000, "h": 3_600_000, "d": 86_400_000, "w": 604_800_000, "M": 28 * 86_400_000}
        try:
            duration = int(interval[:-1]) * units[interval[-1]]
            span = int(req["endTime"]) - int(req["startTime"])
            if duration > 0 and span >= 0:
                count = min(5000, math.ceil(span / duration) + 2)
        except (KeyError, ValueError, TypeError, IndexError):
            pass
        if isinstance(response, list):
            count = len(response)
        return 20 + math.ceil(count / 60)
    return 20


class HyperliquidRateLimiter(RequestRateLimiter):
    """One rolling weighted budget and cooldown shared by every local process.

    Reservations are written before HTTP dispatch. A killed process leaves its
    reservation charged for sixty seconds, so crashes cannot free spent quota.
    Historical traffic has a smaller budget, leaving headroom for live data.
    """

    def __init__(self, path: Path, *, weight_per_minute: int = 900,
                 historical_weight_per_minute: int = 600,
                 requests_per_second: float = 6.0) -> None:
        super().__init__(requests_per_second)
        self.path = Path(path)
        self.budget = max(240, min(int(weight_per_minute), 1100))
        self.history_budget = max(120, min(int(historical_weight_per_minute), self.budget - 120))
        self.reserved_weight = 0
        self.budget_wait_seconds = 0.0

    def _connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        db = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        db.execute("PRAGMA busy_timeout=10000")
        db.execute("CREATE TABLE IF NOT EXISTS requests (id INTEGER PRIMARY KEY, at REAL NOT NULL, weight INTEGER NOT NULL, priority TEXT NOT NULL)")
        db.execute("CREATE TABLE IF NOT EXISTS control (id INTEGER PRIMARY KEY, next_at REAL NOT NULL DEFAULT 0, cooldown_until REAL NOT NULL DEFAULT 0, last_429 REAL NOT NULL DEFAULT 0, streak INTEGER NOT NULL DEFAULT 0)")
        db.execute("INSERT OR IGNORE INTO control(id) VALUES(1)")
        return db

    def acquire(self, payload: dict[str, Any], *, priority: str = "live",
                max_wait_seconds: float | None = None) -> int | None:
        weight = hyperliquid_request_weight(payload)
        started = time.monotonic()
        while True:
            now = time.time()
            with closing(self._connect()) as db:
                db.execute("BEGIN IMMEDIATE")
                db.execute("DELETE FROM requests WHERE at <= ?", (now - 60,))
                next_at, cooldown, _, _ = db.execute("SELECT next_at,cooldown_until,last_429,streak FROM control WHERE id=1").fetchone()
                if priority == "history" and cooldown > now:
                    db.commit()
                    return None  # Leave history to a later background run.
                total, historical = db.execute("SELECT COALESCE(SUM(weight),0),COALESCE(SUM(CASE WHEN priority='history' THEN weight ELSE 0 END),0) FROM requests").fetchone()
                delay = max(0.0, next_at - now, cooldown - now)
                if total + weight > self.budget or (priority == "history" and historical + weight > self.history_budget):
                    oldest = db.execute("SELECT MIN(at) FROM requests").fetchone()[0]
                    delay = max(delay, (oldest + 60 - now) if oldest is not None else 0.1)
                if delay <= 0:
                    token = db.execute("INSERT INTO requests(at,weight,priority) VALUES(?,?,?)", (now, weight, priority)).lastrowid
                    db.execute("UPDATE control SET next_at=? WHERE id=1", (now + self.interval,))
                    db.commit()
                    self.reserved_weight += weight
                    self.budget_wait_seconds += time.monotonic() - started
                    return token
                db.commit()
            remaining = None if max_wait_seconds is None else max_wait_seconds - (time.monotonic() - started)
            if remaining is not None and delay > remaining:
                return None
            # Recheck frequently: a live request, refunded reservation, or new
            # 429 from another process may change the shared state while waiting.
            time.sleep(min(delay, 0.25))

    def settle(self, token: int, payload: dict[str, Any], response: Any) -> None:
        with closing(self._connect()) as db:
            db.execute("UPDATE requests SET weight=? WHERE id=?", (hyperliquid_request_weight(payload, response), token))

    def penalize(self, seconds: float) -> None:
        now = time.time()
        with closing(self._connect()) as db:
            db.execute("BEGIN IMMEDIATE")
            previous, last, streak = db.execute("SELECT cooldown_until,last_429,streak FROM control WHERE id=1").fetchone()
            streak = min(streak + 1, 5) if now - last < 120 else 1
            pause = max(float(seconds), min(60.0, 5.0 * 2 ** (streak - 1)))
            db.execute("UPDATE control SET cooldown_until=?,last_429=?,streak=? WHERE id=1", (max(previous, now + pause), now, streak))
            db.commit()
        with self.lock:
            self.throttle_events += 1
            self.throttle_seconds += pause

    def throttle_report(self) -> dict[str, Any]:
        return {
            **super().throttle_report(),
            "weightPerMinute": self.budget,
            "historicalWeightPerMinute": self.history_budget,
            "reservedWeight": self.reserved_weight,
            "budgetWaitSeconds": round(self.budget_wait_seconds, 2),
            "sharedAcrossProcesses": True,
        }
