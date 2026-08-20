from __future__ import annotations

import json
import os
import statistics
import sys
from pathlib import Path
from datetime import datetime, timezone
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server import (
    ALERTS_FILE,
    HyperliquidClient,
    WALLETS_FILE,
    WalletStore,
    WalletTrackerService,
    current_time_ms,
    load_json_file,
    now_iso,
    to_float,
)

# The bot records a realised outcome for every observation it makes and has
# never shown any of them back. Every other message asserts what the wallets
# are doing; this one is the only place the system says what came of it, which
# is what makes the rest checkable rather than merely confident. One message a
# week, because that is the rate at which the answer changes.
REPORT_WINDOW_DAYS = int(os.environ.get("OUTCOME_REPORT_WINDOW_DAYS", "7"))
# Horizons worth reporting: 1h is the shortest one that is not dominated by
# spread, 24h is where a swing position is actually judged.
REPORT_HORIZONS = ("1h", "4h", "24h")
# A coin needs this many observations before its average says anything.
MIN_COIN_OBSERVATIONS = int(os.environ.get("OUTCOME_REPORT_MIN_COIN_OBSERVATIONS", "10"))


def format_started_at(started_at_ms: int) -> str:
    """server.format_update_time takes an ISO string; these records store ms."""
    if started_at_ms <= 0:
        return "unknown"
    return datetime.fromtimestamp(started_at_ms / 1000, timezone.utc).strftime("%d %b %H:%M UTC")


def horizon_return(record: dict[str, Any], horizon: str) -> float | None:
    """Net return at one horizon, or None when it was never measured."""
    outcomes = record.get("outcomes")
    if not isinstance(outcomes, dict):
        return None
    entry = outcomes.get(horizon)
    if not isinstance(entry, dict):
        return None
    if entry.get("netReturnPct") is None and entry.get("grossReturnPct") is None:
        return None
    if entry.get("netReturnPct") is not None:
        return to_float(entry.get("netReturnPct"))
    return to_float(entry.get("grossReturnPct"))


def records_in_window(source: Any, *, now_ms: int, window_days: int) -> list[dict[str, Any]]:
    cutoff = now_ms - window_days * 24 * 60 * 60 * 1000
    if not isinstance(source, dict):
        return []
    return [
        record
        for record in source.values()
        if isinstance(record, dict) and int(to_float(record.get("startedAt"))) >= cutoff
    ]


def summarize(records: list[dict[str, Any]], horizon: str) -> dict[str, Any] | None:
    values = [value for record in records if (value := horizon_return(record, horizon)) is not None]
    if not values:
        return None
    return {
        "observations": len(values),
        "meanPct": round(statistics.mean(values), 3),
        "medianPct": round(statistics.median(values), 3),
        "hitRatePct": round(100.0 * sum(1 for value in values if value > 0) / len(values), 1),
    }


def summarize_by_side(records: list[dict[str, Any]], horizon: str) -> dict[str, dict[str, Any]]:
    sides: dict[str, dict[str, Any]] = {}
    for side in ("long", "short"):
        subset = [record for record in records if str(record.get("side") or "").lower() == side]
        summary = summarize(subset, horizon)
        if summary:
            sides[side] = summary
    return sides


def summarize_by_coin(records: list[dict[str, Any]], horizon: str, *, minimum: int) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        grouped.setdefault(str(record.get("coin") or "?"), []).append(record)
    rows = []
    for coin, subset in grouped.items():
        summary = summarize(subset, horizon)
        if summary and summary["observations"] >= minimum:
            rows.append({"coin": coin, **summary})
    rows.sort(key=lambda item: item["meanPct"], reverse=True)
    return rows


def build_payload(state: dict[str, Any], *, now_ms: int, window_days: int = REPORT_WINDOW_DAYS) -> dict[str, Any]:
    shadow = records_in_window(state.get("shadowSignalOutcomes"), now_ms=now_ms, window_days=window_days)
    candidates = records_in_window(state.get("candidateSignalOutcomes"), now_ms=now_ms, window_days=window_days)
    published = records_in_window(state.get("signalOutcomes"), now_ms=now_ms, window_days=window_days)
    headline = REPORT_HORIZONS[0]
    return {
        "version": 1,
        "generatedAt": now_iso(),
        "windowDays": window_days,
        "shadow": {horizon: summarize(shadow, horizon) for horizon in REPORT_HORIZONS},
        "shadowBySide": {horizon: summarize_by_side(shadow, horizon) for horizon in REPORT_HORIZONS},
        "candidates": {horizon: summarize(candidates, horizon) for horizon in REPORT_HORIZONS},
        "published": [
            {
                "coin": record.get("coin"),
                "side": record.get("side"),
                "startedAt": int(to_float(record.get("startedAt"))),
                "returns": {
                    horizon: horizon_return(record, horizon)
                    for horizon in REPORT_HORIZONS
                    if horizon_return(record, horizon) is not None
                },
            }
            for record in sorted(published, key=lambda item: int(to_float(item.get("startedAt"))))
        ],
        "coins": summarize_by_coin(shadow, headline, minimum=MIN_COIN_OBSERVATIONS),
        "shadowCount": len(shadow),
        "candidateCount": len(candidates),
        "publishedCount": len(published),
    }


def format_summary_line(label: str, summary: dict[str, Any] | None) -> str:
    if not summary:
        return f"{label}: no measured outcomes yet"
    return (
        f"{label}: n={summary['observations']} | mean {summary['meanPct']:+.2f}% | "
        f"median {summary['medianPct']:+.2f}% | hit {summary['hitRatePct']:.0f}%"
    )


def build_message(payload: dict[str, Any]) -> str:
    lines = [
        f"Outcome report, last {payload['windowDays']} days",
        (
            f"Observed: {payload['shadowCount']} | candidates: {payload['candidateCount']} | "
            f"published: {payload['publishedCount']}"
        ),
        "",
        "What the observations did (net of costs)",
    ]
    for horizon in REPORT_HORIZONS:
        lines.append(f"- {format_summary_line(horizon, payload['shadow'].get(horizon))}")

    sides = payload["shadowBySide"].get(REPORT_HORIZONS[0]) or {}
    if sides:
        lines.append("")
        lines.append(f"By side at {REPORT_HORIZONS[0]}")
        for side, summary in sides.items():
            lines.append(f"- {format_summary_line(side, summary)}")

    if payload["published"]:
        lines.append("")
        lines.append("Published signals")
        for item in payload["published"]:
            returns = " | ".join(f"{horizon} {value:+.2f}%" for horizon, value in item["returns"].items())
            lines.append(
                f"- {item['coin']} {str(item['side'] or '').upper()} "
                f"{format_started_at(item['startedAt'])}: {returns or 'not measured yet'}"
            )

    coins = payload["coins"]
    if coins:
        lines.append("")
        lines.append(f"Coins with {MIN_COIN_OBSERVATIONS}+ observations at {REPORT_HORIZONS[0]}")
        def coin_line(row: dict[str, Any]) -> str:
            return (
                f"- {row['coin']}: n={row['observations']} "
                f"mean {row['meanPct']:+.2f}% hit {row['hitRatePct']:.0f}%"
            )

        # Both ends only once there are enough coins for the two slices not to
        # overlap; otherwise the same row would be printed twice.
        if len(coins) > 6:
            lines.extend(coin_line(row) for row in coins[:3])
            lines.append("...")
            lines.extend(coin_line(row) for row in coins[-3:])
        else:
            lines.extend(coin_line(row) for row in coins)
    return "\n".join(lines)


def main() -> int:
    raw = load_json_file(ALERTS_FILE, {})
    state = raw.get("state", {}) if isinstance(raw, dict) else {}
    payload = build_payload(state, now_ms=current_time_ms())
    message = build_message(payload)

    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    # Nothing measured means nothing to say; a weekly "n=0" is the kind of
    # line this report exists to argue against.
    has_content = any(payload["shadow"].get(horizon) for horizon in REPORT_HORIZONS) or payload["published"]
    if bot_token and chat_id and has_content:
        service = WalletTrackerService(WalletStore(WALLETS_FILE), HyperliquidClient())
        service.send_telegram_message(bot_token, chat_id, message)
    print(json.dumps({**payload, "message": message}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
