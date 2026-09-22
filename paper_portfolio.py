"""Conservative portfolio accounting for prospective paper arms."""

from __future__ import annotations

import math
import random
from datetime import datetime, timezone
from typing import Any


def portfolio_snapshot(
    records: dict[str, dict[str, Any]],
    *,
    arm: str,
    observed_at_ms: int,
    open_trade_values_usd: dict[str, float],
    initial_capital_usd: float = 10_000.0,
) -> dict[str, Any]:
    """Include closed and open trades; unknown funding leaves equity unknown."""
    if not math.isfinite(initial_capital_usd) or initial_capital_usd <= 0:
        return {"complete": False, "reason": "invalid_initial_capital"}
    realized = unrealized = 0.0
    open_count = closed_count = skipped_count = 0
    incomplete: list[str] = []
    for key, record in records.items():
        if not isinstance(record, dict) or record.get("experimentArm") != arm:
            continue
        status = str(record.get("status") or "")
        if status == "skipped":
            skipped_count += 1
            continue
        if status == "closed":
            closed_count += 1
            result = record.get("executionResult")
            net = result.get("netUsd") if isinstance(result, dict) and result.get("complete") else None
            if not isinstance(net, (int, float)) or not math.isfinite(net):
                incomplete.append(str(key))
            else:
                realized += net
            continue
        if record.get("executionStatus") in {"entry_quoted", "exit_pending"}:
            open_count += 1
            value = open_trade_values_usd.get(str(key))
            if not isinstance(value, (int, float)) or not math.isfinite(value):
                incomplete.append(str(key))
            else:
                unrealized += value
            continue
        incomplete.append(str(key))
    complete = not incomplete
    return {
        "arm": arm,
        "observedAtMs": observed_at_ms,
        "complete": complete,
        "equityUsd": (initial_capital_usd + realized + unrealized) if complete else None,
        "realizedNetUsd": realized,
        "openMarkToMarketNetUsd": unrealized if complete else None,
        "openCount": open_count,
        "closedCount": closed_count,
        "skippedCount": skipped_count,
        "incompleteRecordIds": incomplete,
    }


def scenario_portfolio_snapshot(
    records: dict[str, dict[str, Any]],
    *,
    arm: str,
    scenario: str,
    observed_at_ms: int,
    open_trade_values_usd: dict[str, float],
    initial_capital_usd: float = 10_000.0,
) -> dict[str, Any]:
    """Stress scenario equity; unpriced exposure stays unknown, never zero."""
    if scenario not in {"double_cost", "delayed_entry"}:
        return {"complete": False, "reason": "unknown_scenario"}
    if not math.isfinite(initial_capital_usd) or initial_capital_usd <= 0:
        return {"complete": False, "reason": "invalid_initial_capital"}
    result_field = "doubleCostResult" if scenario == "double_cost" else "delayedEntryResult"
    realized = unrealized = 0.0
    open_count = closed_count = skipped_count = not_entered_count = 0
    incomplete: list[str] = []
    for key, record in records.items():
        if not isinstance(record, dict) or record.get("experimentArm") != arm:
            continue
        status = str(record.get("status") or "")
        if status == "skipped":
            skipped_count += 1
            continue
        delayed_status = str(record.get("delayedEntryStatus") or "")
        if scenario == "delayed_entry" and delayed_status == "closed_before_delay":
            not_entered_count += 1
            continue
        if status == "closed":
            closed_count += 1
            result = record.get(result_field)
            net = result.get("netUsd") if isinstance(result, dict) and result.get("complete") else None
            if not isinstance(net, (int, float)) or not math.isfinite(net):
                incomplete.append(str(key))
            else:
                realized += net
            continue
        if record.get("executionStatus") in {"entry_quoted", "exit_pending"}:
            if scenario == "delayed_entry" and not delayed_status:
                started = record.get("startedAt")
                if isinstance(started, (int, float)) and observed_at_ms < started + 60 * 60 * 1000:
                    not_entered_count += 1
                else:
                    incomplete.append(str(key))
                continue
            open_count += 1
            value = open_trade_values_usd.get(str(key))
            if not isinstance(value, (int, float)) or not math.isfinite(value):
                incomplete.append(str(key))
            else:
                unrealized += value
            continue
        incomplete.append(str(key))
    complete = not incomplete
    return {
        "arm": f"{arm}:{scenario}",
        "scenario": scenario,
        "observedAtMs": observed_at_ms,
        "complete": complete,
        "equityUsd": (initial_capital_usd + realized + unrealized) if complete else None,
        "realizedNetUsd": realized,
        "openMarkToMarketNetUsd": unrealized if complete else None,
        "openCount": open_count,
        "closedCount": closed_count,
        "skippedCount": skipped_count,
        "notEnteredCount": not_entered_count,
        "incompleteRecordIds": incomplete,
    }


def observed_drawdown(
    snapshots: list[dict[str, Any]], *, max_gap_ms: int = 20 * 60 * 1000
) -> dict[str, Any]:
    """A later missing day never erases a previously observed risk breach."""
    ordered = sorted(
        (row for row in snapshots if isinstance(row, dict)),
        key=lambda row: int(row.get("observedAtMs") or 0),
    )
    peak = 0.0
    worst_pct = 0.0
    prior_at: int | None = None
    incomplete = False
    valid = 0
    for row in ordered:
        at = int(row.get("observedAtMs") or 0)
        equity = row.get("equityUsd")
        if not row.get("complete") or not isinstance(equity, (int, float)) or not math.isfinite(equity):
            incomplete = True
            continue
        if equity <= 0:
            worst_pct = 100.0
            peak = max(peak, equity)
        else:
            peak = max(peak, equity)
            worst_pct = max(worst_pct, (peak - equity) / peak * 100 if peak > 0 else 0)
        if prior_at is not None and at - prior_at > max_gap_ms:
            incomplete = True
        prior_at = at
        valid += 1
    return {
        "observedMaxDrawdownPct": worst_pct if valid else None,
        "complete": valid >= 2 and not incomplete,
        "validSnapshotCount": valid,
    }


def paired_portfolio_comparison(
    ranked: list[dict[str, Any]],
    control: list[dict[str, Any]],
    *,
    bootstrap_samples: int = 2_000,
    block_days: int = 3,
) -> dict[str, Any]:
    """Diagnostic paired after-cost equity difference, including open PnL.

    The interval resamples adjacent decision days, not independent trades.
    It is not an asset-clustered confidence interval or permission to trade.
    """
    by_arm = []
    for snapshots in (ranked, control):
        days: dict[str, tuple[int, float | None]] = {}
        for row in snapshots:
            if not isinstance(row, dict):
                continue
            at = row.get("observedAtMs")
            if not isinstance(at, int) or at <= 0:
                continue
            day = datetime.fromtimestamp(at / 1000, tz=timezone.utc).date().isoformat()
            equity = row.get("equityUsd")
            value = (
                float(equity)
                if row.get("complete") and isinstance(equity, (int, float))
                and math.isfinite(equity) else None
            )
            if day not in days or at > days[day][0]:
                days[day] = (at, value)
        by_arm.append(days)
    first, second = by_arm
    common = sorted(set(first) & set(second))
    if len(common) < 2:
        return {"complete": False, "reason": "insufficient_paired_days", "pairedDays": len(common)}
    if len(common) != len(first) or len(common) != len(second):
        return {"complete": False, "reason": "unmatched_portfolio_days", "pairedDays": len(common)}
    if any(first[day][1] is None or second[day][1] is None for day in common):
        return {"complete": False, "reason": "incomplete_portfolio_equity", "pairedDays": len(common)}
    if any(first[day][0] != second[day][0] for day in common):
        return {"complete": False, "reason": "unmatched_snapshot_times", "pairedDays": len(common)}
    calendar_days = [datetime.fromisoformat(day).date() for day in common]
    if any((later - earlier).days != 1 for earlier, later in zip(calendar_days, calendar_days[1:])):
        return {"complete": False, "reason": "daily_coverage_gap", "pairedDays": len(common)}
    daily_differences = [
        (first[later][1] - first[earlier][1])
        - (second[later][1] - second[earlier][1])
        for earlier, later in zip(common, common[1:])
    ]
    total = sum(daily_differences)
    result: dict[str, Any] = {
        "complete": True,
        "pairedDays": len(common),
        "pairedDailyChanges": len(daily_differences),
        "rankedMinusControlNetUsd": round(total, 6),
        "rankedMinusControlReturnPctOn10k": round(total / 10_000 * 100, 6),
        "basis": "paired_daily_mark_to_close_equity_including_open_positions",
        "uncertaintyBasis": "three_day_moving_block_bootstrap_not_asset_clustered",
        "interval95PctOn10k": None,
    }
    if len(daily_differences) < 20:
        result["uncertaintyReason"] = "fewer_than_20_paired_daily_changes"
        return result
    block = min(block_days, len(daily_differences))
    if block < 1 or bootstrap_samples < 100:
        result["uncertaintyReason"] = "invalid_bootstrap_parameters"
        return result
    rng = random.Random(20260922)
    possible_starts = len(daily_differences) - block + 1
    totals = []
    for _ in range(bootstrap_samples):
        sample = []
        while len(sample) < len(daily_differences):
            start = rng.randrange(possible_starts)
            sample.extend(daily_differences[start:start + block])
        totals.append(sum(sample[:len(daily_differences)]) / 10_000 * 100)
    totals.sort()
    result["interval95PctOn10k"] = [
        round(totals[int(0.025 * (bootstrap_samples - 1))], 6),
        round(totals[int(0.975 * (bootstrap_samples - 1))], 6),
    ]
    return result
