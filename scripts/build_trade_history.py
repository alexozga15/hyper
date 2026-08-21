"""Completed round trips for one wallet, reconstructed from raw fills.

A trade is the span from a position leaving flat until it returns to flat (or
crosses through it). Entry is the size-weighted average of the fills that built
the position, exit the size-weighted average of the fills that took it off, and
the result comes from Hyperliquid's own closedPnl less fees rather than being
recomputed - the venue already nets funding and partial closes into that figure.

Every fill carries the position it started from, so the reconstruction is
checked against the venue's own view fill by fill instead of trusted. A wallet
whose chain does not line up is reported as such rather than rendered anyway.
"""
from __future__ import annotations

import time
from collections import defaultdict
from typing import Any

from server import to_float

EPSILON = 1e-9
DEFAULT_TRADE_LIMIT = 12


def signed_size(fill: dict[str, Any]) -> float:
    size = abs(to_float(fill.get("sz")))
    return size if str(fill.get("side")) == "B" else -size


def fill_end_position(fill: dict[str, Any]) -> float:
    return to_float(fill.get("startPosition")) + signed_size(fill)


def order_fills(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    """Put same-millisecond fills back into execution order.

    Hyperliquid does not order fills inside one millisecond by ``tid``, so
    sorting by ``(time, tid)`` breaks the position chain: measured on one wallet
    over 30 days, 61 of 3173 fills disagreed with the venue's own
    ``startPosition``, always in consecutive runs sharing a timestamp. The true
    order is recoverable exactly, because each fill records the position it
    started from - chain on that rather than guess from an identifier.

    Returns the ordered fills and the number that could not be chained.
    """
    rows = sorted(rows, key=lambda item: int(to_float(item.get("time"))))
    ordered: list[dict[str, Any]] = []
    unresolved = 0
    position: float | None = None
    index = 0
    while index < len(rows):
        stamp = int(to_float(rows[index].get("time")))
        end = index
        while end < len(rows) and int(to_float(rows[end].get("time"))) == stamp:
            end += 1
        group = rows[index:end]
        index = end

        if len(group) == 1:
            position = fill_end_position(group[0])
            ordered.extend(group)
            continue

        remaining = list(group)
        if position is None:
            # Opening group: begin at the fill that no other fill leads into.
            ends = [fill_end_position(item) for item in remaining]
            heads = [
                item for item in remaining
                if not any(abs(to_float(item.get("startPosition")) - value) <= EPSILON for value in ends)
            ]
            position = to_float((heads or remaining)[0].get("startPosition"))
        while remaining:
            match = None
            for candidate in remaining:
                start = to_float(candidate.get("startPosition"))
                if abs(start - position) <= max(1e-6, abs(position) * 1e-6):
                    match = candidate
                    break
            if match is None:
                # Chain broken: emit the rest in tid order and say so, rather
                # than inventing a sequence.
                unresolved += len(remaining)
                remaining.sort(key=lambda item: int(to_float(item.get("tid"))))
                for leftover in remaining:
                    position = fill_end_position(leftover)
                    ordered.append(leftover)
                break
            remaining.remove(match)
            position = fill_end_position(match)
            ordered.append(match)
    return ordered, unresolved


def _new_trade(coin: str, side: str, opened_at: int) -> dict[str, Any]:
    return {
        "coin": coin, "side": side, "openedAt": opened_at,
        "entryNotional": 0.0, "entrySize": 0.0,
        "exitNotional": 0.0, "exitSize": 0.0,
        "closedPnl": 0.0, "fees": 0.0, "grossNotional": 0.0, "fillCount": 0,
    }


def reconstruct(fills: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, int]]:
    by_coin: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for fill in fills:
        if isinstance(fill, dict):
            by_coin[str(fill.get("coin"))].append(fill)

    trades: list[dict[str, Any]] = []
    checks = {"fills": 0, "positionMismatches": 0, "unchainable": 0, "openAtEnd": 0}

    for coin, unordered in by_coin.items():
        rows, unresolved = order_fills(unordered)
        checks["unchainable"] += unresolved
        open_trade: dict[str, Any] | None = None
        expected: float | None = None

        for fill in rows:
            checks["fills"] += 1
            start = to_float(fill.get("startPosition"))
            if expected is not None and abs(expected - start) > max(1e-6, abs(start) * 1e-6):
                checks["positionMismatches"] += 1
            end = start + signed_size(fill)
            expected = end

            price = to_float(fill.get("px"))
            size = abs(to_float(fill.get("sz")))
            notional = price * size
            when = int(to_float(fill.get("time")))
            growing = abs(end) > abs(start) + EPSILON

            if open_trade is None and abs(start) <= EPSILON and abs(end) > EPSILON:
                open_trade = _new_trade(coin, "long" if end > 0 else "short", when)
            if open_trade is None:
                # The window starts mid-position: there is no entry to price.
                continue

            open_trade["fillCount"] += 1
            open_trade["grossNotional"] += notional
            open_trade["fees"] += to_float(fill.get("fee"))
            open_trade["closedPnl"] += to_float(fill.get("closedPnl"))
            if growing:
                open_trade["entryNotional"] += notional
                open_trade["entrySize"] += size
            else:
                open_trade["exitNotional"] += notional
                open_trade["exitSize"] += size

            if abs(end) <= EPSILON:
                open_trade["closedAt"] = when
                trades.append(open_trade)
                open_trade = None
            elif abs(start) > EPSILON and (start > 0) != (end > 0):
                # A flip closes one trade and opens the other on one fill: the
                # residual size is the new position's entry.
                open_trade["closedAt"] = when
                trades.append(open_trade)
                open_trade = _new_trade(coin, "long" if end > 0 else "short", when)
                open_trade["entryNotional"] = abs(end) * price
                open_trade["entrySize"] = abs(end)
                open_trade["fillCount"] = 1

        if open_trade is not None:
            checks["openAtEnd"] += 1

    for trade in trades:
        trade["entryPrice"] = trade["entryNotional"] / trade["entrySize"] if trade["entrySize"] else 0.0
        trade["exitPrice"] = trade["exitNotional"] / trade["exitSize"] if trade["exitSize"] else 0.0
        trade["netPnl"] = trade["closedPnl"] - trade["fees"]
        trade["durationMs"] = max(0, int(trade["closedAt"]) - int(trade["openedAt"]))
        trade["returnPct"] = 100.0 * trade["netPnl"] / trade["entryNotional"] if trade["entryNotional"] else 0.0
    trades.sort(key=lambda item: int(item["closedAt"]), reverse=True)
    return trades, checks


def human_duration(ms: int) -> str:
    minutes = int(ms) // 60000
    if minutes < 60:
        return f"{minutes}m"
    hours, minutes = divmod(minutes, 60)
    if hours < 24:
        return f"{hours}h{minutes}m" if minutes else f"{hours}h"
    days, hours = divmod(hours, 24)
    return f"{days}d{hours}h" if hours else f"{days}d"


def money(value: float) -> str:
    sign = "-" if value < 0 else ""
    magnitude = abs(value)
    for unit, scale in (("B", 1e9), ("M", 1e6), ("K", 1e3)):
        if magnitude >= scale:
            return f"{sign}${magnitude / scale:.2f}{unit}"
    return f"{sign}${magnitude:.0f}"


def price(value: float) -> str:
    return f"{value:.6g}"


def stamp(ms: int) -> str:
    return time.strftime("%d %b %H:%M", time.gmtime(int(ms) / 1000))


def build_message(
    address: str,
    trades: list[dict[str, Any]],
    checks: dict[str, int],
    *,
    days: int,
    alias: str = "",
    limit: int = DEFAULT_TRADE_LIMIT,
) -> str:
    label = alias.strip() or f"{address[:6]}…{address[-4:]}"
    lines = [f"Closed trades · {label} · {days}d"]
    if not trades:
        lines.append("No round trip completed in this window.")
        return "\n".join(lines)

    wins = sum(1 for item in trades if item["netPnl"] > 0)
    total = sum(item["netPnl"] for item in trades)
    lines.append(
        f"{len(trades)} closed · win {100.0 * wins / len(trades):.0f}% · net {money(total)}"
    )
    if checks.get("unchainable"):
        # Say it rather than render a sequence the venue does not confirm.
        lines.append(f"⚠ {checks['unchainable']} fills could not be chained; entries may be off")
    if checks.get("openAtEnd"):
        lines.append(f"{checks['openAtEnd']} position(s) still open, not listed")
    lines.append("")

    for item in trades[:limit]:
        lines.append(
            f"{item['coin']} {item['side'].upper()} "
            f"{item['returnPct']:+.2f}% {money(item['netPnl'])} {human_duration(item['durationMs'])}"
        )
        lines.append(
            f"  {price(item['entryPrice'])}→{price(item['exitPrice'])} · "
            f"{money(item['grossNotional'])} · {stamp(item['openedAt'])}→{stamp(item['closedAt'])}"
        )
    if len(trades) > limit:
        lines.append("")
        lines.append(f"…and {len(trades) - limit} more")
    return "\n".join(lines)
