"""Preserve observed perp equity/PnL points; never interpolate missing days."""
from __future__ import annotations

import math
import sqlite3
import time
from contextlib import closing
from pathlib import Path
from typing import Any


def paired_points(block: dict[str, Any]) -> dict[int, tuple[float, float]]:
    def series(key: str) -> dict[int, float]:
        result = {}
        for row in block.get(key, []) or []:
            try:
                timestamp, value = int(row[0]), float(row[1])
                if timestamp >= 0 and math.isfinite(value):
                    result[timestamp] = value
            except (TypeError, ValueError, IndexError, OverflowError):
                continue
        return result
    account, pnl = series("accountValueHistory"), series("pnlHistory")
    return {t: (account[t], pnl[t]) for t in account.keys() & pnl.keys()}


def observed_perp_history(portfolio: dict[str, Any]) -> dict[str, Any]:
    """Align window-relative PnL to perpAllTime using shared observations.

    Reject inconsistent windows instead of joining different books or PnL
    origins. Account values at shared timestamps must agree as well.
    """
    points = paired_points(portfolio.get("perpAllTime") or {})
    rejected = []
    for name in ("perpMonth", "perpWeek", "perpDay"):
        incoming = paired_points(portfolio.get(name) or {})
        shared = sorted(points.keys() & incoming.keys())
        if not shared:
            continue
        offsets = [points[t][1] - incoming[t][1] for t in shared]
        offset = offsets[-1]
        if any(not math.isclose(delta, offset, rel_tol=1e-9, abs_tol=1e-5) for delta in offsets) or any(
            not math.isclose(points[t][0], incoming[t][0], rel_tol=1e-9, abs_tol=1e-5)
            for t in shared
        ):
            rejected.append(name)
            continue
        points.update({t: (account, pnl + offset) for t, (account, pnl) in incoming.items()})
    return {"accountValueHistory": [[t, points[t][0]] for t in sorted(points)],
            "pnlHistory": [[t, points[t][1]] for t in sorted(points)],
            "rejectedWindows": rejected}


class EquityHistoryCache:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)

    def merge(self, address: str, block: dict[str, Any], *, cutoff_ms: int) -> dict[str, Any]:
        points = paired_points(block)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(sqlite3.connect(self.path, timeout=30)) as db:
            db.execute("CREATE TABLE IF NOT EXISTS equity_points (address TEXT, at INTEGER, account REAL, pnl REAL, PRIMARY KEY(address,at))")
            db.execute("CREATE TABLE IF NOT EXISTS equity_observations (address TEXT, at INTEGER, account REAL, pnl REAL, observed_at INTEGER, PRIMARY KEY(address,at,account,pnl))")
            db.execute("CREATE TABLE IF NOT EXISTS equity_conflicts (address TEXT, at INTEGER, account REAL, pnl REAL, observed_at INTEGER, PRIMARY KEY(address,at,account,pnl))")
            with db:
                observed_at = int(time.time() * 1000)
                # Migrate canonical observations before any new input. Neither
                # conflicts nor a moving calculation window may erase evidence.
                db.execute("INSERT OR IGNORE INTO equity_observations SELECT address,at,account,pnl,? FROM equity_points WHERE address=?", (observed_at, address))
                db.executemany("INSERT OR IGNORE INTO equity_observations VALUES(?,?,?,?,?)",
                               [(address, t, a, p, observed_at) for t, (a, p) in points.items()])
                existing = {t: (a, p) for t, a, p in db.execute(
                    "SELECT at,account,pnl FROM equity_points WHERE address=?", (address,))}
                conflicts = [t for t, values in points.items() if t in existing and any(
                    not math.isclose(old, new, rel_tol=1e-9, abs_tol=1e-5)
                    for old, new in zip(existing[t], values))]
                shared = points.keys() & existing.keys()
                # Once a series exists, absence of a shared anchor is not
                # permission to splice an unrelated PnL origin into it.
                unanchored = bool(existing and points and not shared)
                quarantined = bool(conflicts or unanchored)
                if quarantined:
                    db.executemany("INSERT OR IGNORE INTO equity_conflicts VALUES(?,?,?,?,?)",
                                   [(address, t, a, p, observed_at) for t, (a, p) in points.items()])
                else:
                    # Preserve the first accepted value at a timestamp. Tiny
                    # tolerated revisions do not rewrite the observed past.
                    db.executemany("INSERT OR IGNORE INTO equity_points VALUES(?,?,?,?)",
                                   [(address, t, a, p) for t, (a, p) in points.items()])
            rows = db.execute("SELECT at,account,pnl FROM equity_points WHERE address=? AND at>=? ORDER BY at",
                              (address, cutoff_ms - 2 * 86_400_000)).fetchall()
        return {**block, "accountValueHistory": [[t, a] for t, a, p in rows],
                "pnlHistory": [[t, p] for t, a, p in rows],
                "historyConflict": quarantined,
                "historyInputMissing": not bool(points),
                "conflictingTimestamps": conflicts,
                "historyUnanchored": unanchored,
                "incomingObservationsQuarantined": len(points) if quarantined else 0}
