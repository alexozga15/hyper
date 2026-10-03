from __future__ import annotations

import json
import sqlite3
import time
from contextlib import closing
from pathlib import Path
from typing import Any, Callable


class ResumableHistoryCache:
    """Checkpoint every page; incomplete walks never claim complete history.

    A single background worker owns the walk (its service has a process lock).
    SQLite commits rows and cursor together, including across worker restarts.
    Completed histories next fetch only the tail, overlapping five minutes to
    include same-timestamp fills and remove duplicates by their source identity.
    """

    def __init__(self, path: Path, *, pages_per_run: int = 4) -> None:
        self.path = Path(path)
        self.pages_per_run = max(1, pages_per_run)

    def _connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        db = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        db.execute("PRAGMA busy_timeout=30000")
        db.execute("CREATE TABLE IF NOT EXISTS streams (address TEXT, kind TEXT, covered_from INTEGER, cursor INTEGER, complete_until INTEGER DEFAULT 0, pending INTEGER DEFAULT 1, PRIMARY KEY(address,kind))")
        db.execute("CREATE TABLE IF NOT EXISTS rows (address TEXT,kind TEXT,identity TEXT,at INTEGER,payload TEXT, PRIMARY KEY(address,kind,identity))")
        db.execute("CREATE TABLE IF NOT EXISTS coverage_limits (address TEXT,kind TEXT,earliest INTEGER, PRIMARY KEY(address,kind))")
        return db

    def walk(self, address: str, kind: str, start: int, *,
             fetch: Callable[[int], dict[str, Any]], page_size: int,
             timestamp: Callable[[dict[str, Any]], int],
             identity: Callable[[dict[str, Any]], Any]) -> dict[str, Any]:
        address = address.lower()
        checked_at = int(time.time() * 1000)
        with closing(self._connect()) as db:
            db.execute("BEGIN IMMEDIATE")
            previous = db.execute("SELECT covered_from,cursor,complete_until,pending FROM streams WHERE address=? AND kind=?", (address, kind)).fetchone()
            if previous is None or start < previous[0]:
                # Requesting an earlier interval requires a new backfill; a
                # later complete window cannot prove coverage of earlier days.
                db.execute("DELETE FROM rows WHERE address=? AND kind=?", (address, kind))
                db.execute("DELETE FROM coverage_limits WHERE address=? AND kind=?", (address, kind))
                cursor = start
                db.execute("INSERT OR REPLACE INTO streams VALUES(?,?,?,?,0,1)", (address, kind, start, cursor))
            else:
                cursor = max(start, previous[1]) if previous[3] else max(start, previous[2] - 300_000)
                db.execute("UPDATE streams SET covered_from=?,cursor=?,pending=1 WHERE address=? AND kind=?", (start, cursor, address, kind))
            db.execute("DELETE FROM rows WHERE address=? AND kind=? AND at<?", (address, kind, start))
            db.commit()
        complete = False
        error = ""
        pages = 0
        for _ in range(self.pages_per_run):
            result = fetch(cursor)
            if not result.get("ok") or not isinstance(result.get("data"), list):
                error = result.get("error") or "history page unavailable"
                break
            page = result["data"]
            pages += 1
            newest = max((timestamp(row) for row in page if isinstance(row, dict)), default=cursor)
            complete = len(page) < page_size
            with closing(self._connect()) as db:
                db.execute("BEGIN IMMEDIATE")
                for row in page:
                    if isinstance(row, dict):
                        db.execute("INSERT OR IGNORE INTO rows VALUES(?,?,?,?,?)", (address, kind, json.dumps(identity(row), sort_keys=True), timestamp(row), json.dumps(row, sort_keys=True)))
                if complete:
                    db.execute("UPDATE streams SET cursor=?,complete_until=?,pending=0 WHERE address=? AND kind=?", (max(cursor, newest), checked_at, address, kind))
                else:
                    db.execute("UPDATE streams SET cursor=? WHERE address=? AND kind=?", (max(cursor, newest), address, kind))
                db.commit()
            if complete:
                break
            if newest <= cursor:
                error = "history page saturated at one timestamp; coverage unverified"
                break
            cursor = newest  # Inclusive overlap preserves boundary records.
        with closing(self._connect()) as db:
            # Re-identify legacy rows too: an upgraded identity must not double
            # count the overlap between old checkpoints and fresh API pages.
            unique = {}
            for stored in db.execute("SELECT payload FROM rows WHERE address=? AND kind=? AND at>=? ORDER BY at,identity", (address, kind, start)):
                row = json.loads(stored[0])
                unique[json.dumps(identity(row), sort_keys=True)] = row
            data = sorted(unique.values(), key=timestamp)
            limit = db.execute("SELECT earliest FROM coverage_limits WHERE address=? AND kind=?", (address, kind)).fetchone()
            # Ending the walk proves the right edge only. The provider retains
            # at most 10,000 ordinary fills; a capped first backfill cannot
            # prove that its left edge reaches the requested cutoff.
            if kind == "fills" and limit is None and (complete or len(data) >= 10_000):
                floor = min((timestamp(row) for row in data), default=start) if len(data) >= 10_000 else start
                db.execute("INSERT OR REPLACE INTO coverage_limits VALUES(?,?,?)", (address, kind, floor))
            elif kind == "fills" and previous and previous[2]:
                # A long outage can also evict an unseen tail from the API.
                # A previously verified left edge does not bridge that gap.
                tail_from = max(start, previous[2] - 300_000)
                tail_times = [timestamp(row) for row in data if timestamp(row) >= tail_from]
                if len(tail_times) >= 10_000 and min(tail_times) > tail_from:
                    floor = max(min(tail_times), limit[0] if limit else start)
                    db.execute("INSERT OR REPLACE INTO coverage_limits VALUES(?,?,?)", (address, kind, floor))
            limit = db.execute("SELECT earliest FROM coverage_limits WHERE address=? AND kind=?", (address, kind)).fetchone()
        return {"ok": not bool(error), "data": data, "error": error,
                "truncated": not complete, "pages": pages, "resumable": True,
                "retentionLimited": bool(limit and start < limit[0])}
