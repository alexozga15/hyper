"""Preserve observed perp equity/PnL points; never interpolate missing days."""
from __future__ import annotations

import math
import sqlite3
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
            with db:
                # A changed cumulative PnL origin must not create artificial
                # returns at the join with an older archive.
                for t, (_account, pnl) in points.items():
                    old = db.execute("SELECT pnl FROM equity_points WHERE address=? AND at=?", (address, t)).fetchone()
                    if old and not math.isclose(old[0], pnl, rel_tol=1e-9, abs_tol=1e-5):
                        db.execute("DELETE FROM equity_points WHERE address=?", (address,))
                        break
                db.executemany("INSERT OR REPLACE INTO equity_points VALUES(?,?,?,?)",
                               [(address, t, a, p) for t, (a, p) in points.items()])
                db.execute("DELETE FROM equity_points WHERE address=? AND at<?", (address, cutoff_ms - 2 * 86_400_000))
            rows = db.execute("SELECT at,account,pnl FROM equity_points WHERE address=? ORDER BY at", (address,)).fetchall()
        return {**block, "accountValueHistory": [[t, a] for t, a, p in rows],
                "pnlHistory": [[t, p] for t, a, p in rows]}
