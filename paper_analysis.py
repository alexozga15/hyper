"""Read-only matched-opportunity diagnostics for the frozen paper study."""

from __future__ import annotations

import json
import math
import random
from collections import Counter
from datetime import datetime, timezone
from typing import Any


_NO_ENTRY_DECISIONS = {
    "policy_rejected",
    "pre_baseline_fresh_event",
    "existing_open_setup",
    "resample_cooldown",
    "unchanged_setup",
    "one_open_position_per_coin",
    "portfolio_position_cap",
    "missing_mark_price",
}


def _opportunity_key(row: dict[str, Any], *, evaluation: bool) -> str | None:
    fingerprint = (
        (row.get("inputs") or {}).get("consensusFingerprint")
        if evaluation and isinstance(row.get("inputs"), dict)
        else row.get("consensusFingerprint")
    )
    if not isinstance(fingerprint, dict):
        return None
    fresh_at = fingerprint.get("freshAddLatestTime")
    wallets = fingerprint.get("walletAddresses")
    if not isinstance(fresh_at, int) or fresh_at <= 0 or not isinstance(wallets, list):
        return None
    addresses = sorted({str(value).lower() for value in wallets if str(value).strip()})
    if not addresses:
        return None
    signal_key = str(row.get("signalKey") or "")
    if not signal_key:
        return None
    return json.dumps(
        [signal_key, fresh_at, addresses], separators=(",", ":"),
    )


def _completed_trade_net(row: dict[str, Any]) -> tuple[float | None, str | None]:
    if row.get("status") == "skipped":
        entry = row.get("entryQuote")
        if isinstance(entry, dict) and entry.get("ok"):
            return None, "skipped_after_entry_quote"
        return 0.0, None
    if row.get("status") != "closed" or row.get("executionStatus") != "closed_modeled_net":
        return None, "open_or_unverified_trade"
    result = row.get("executionResult")
    net = result.get("netUsd") if isinstance(result, dict) and result.get("complete") else None
    if not isinstance(net, (int, float)) or isinstance(net, bool) or not math.isfinite(net):
        return None, "invalid_net_result"
    return float(net), None


def matched_opportunity_analysis(
    records: list[dict[str, Any]],
    evaluations: list[dict[str, Any]],
    *,
    baseline_at_ms: int | None,
    bootstrap_samples: int = 2_000,
) -> dict[str, Any]:
    """Compare the same fresh event under ranked and unranked rules.

    Unselected decisions count as zero only when their explicit reason is
    recorded. Selected-but-missing, open, and unverified results stay unknown.
    This is a trade-opportunity sensitivity check, not portfolio performance.
    """
    if not isinstance(baseline_at_ms, int) or baseline_at_ms <= 0:
        return {"complete": False, "reason": "missing_prospective_baseline"}
    opportunities: dict[str, dict[str, Any]] = {}
    invalid_evaluations = 0
    for row in evaluations:
        if not isinstance(row, dict) or row.get("experimentArm") not in {
            "ranked_consensus", "consensus_unranked"
        }:
            continue
        key = _opportunity_key(row, evaluation=True)
        if key is None:
            invalid_evaluations += 1
            continue
        _signal_key, fresh_at, _wallets = json.loads(key)
        if fresh_at <= baseline_at_ms:
            continue
        inputs = row.get("inputs") if isinstance(row.get("inputs"), dict) else {}
        decision = inputs.get("entryDecision")
        if decision == "experiment_paused":
            continue
        at = row.get("evaluatedAtMs")
        if not isinstance(at, int) or at <= 0:
            invalid_evaluations += 1
            continue
        group = opportunities.setdefault(key, {
            "asset": str(row.get("coin") or "unknown"),
            "firstAtMs": at,
            "evaluations": {},
            "records": {},
        })
        group["firstAtMs"] = min(group["firstAtMs"], at)
        group["evaluations"].setdefault(row["experimentArm"], set()).add(decision)
    invalid_records = 0
    for row in records:
        if not isinstance(row, dict) or row.get("experimentArm") not in {
            "ranked_consensus", "consensus_unranked"
        }:
            continue
        key = _opportunity_key(row, evaluation=False)
        if key is None:
            invalid_records += 1
            continue
        if key in opportunities:
            opportunities[key]["records"].setdefault(row["experimentArm"], []).append(row)
        else:
            invalid_records += 1
    complete_rows: list[dict[str, Any]] = []
    unresolved = Counter()
    rank_rejected_control_entered = 0
    for group in opportunities.values():
        values = {}
        for arm in ("ranked_consensus", "consensus_unranked"):
            decisions = group["evaluations"].get(arm)
            if not decisions:
                unresolved["missing_arm_evaluation"] += 1
                break
            arm_records = group["records"].get(arm, [])
            if arm_records:
                amounts = [_completed_trade_net(row) for row in arm_records]
                failures = [reason for _net, reason in amounts if reason]
                if failures:
                    unresolved[failures[0]] += 1
                    break
                values[arm] = sum(net for net, _reason in amounts if net is not None)
            elif "selected_for_quote" in decisions:
                unresolved["selected_record_missing"] += 1
                break
            elif any(decision not in _NO_ENTRY_DECISIONS for decision in decisions):
                unresolved["unknown_no_entry_decision"] += 1
                break
            else:
                values[arm] = 0.0
        if len(values) != 2:
            continue
        if (
            group["evaluations"]["ranked_consensus"] == {"policy_rejected"}
            and group["records"].get("consensus_unranked")
        ):
            rank_rejected_control_entered += 1
        at = group["firstAtMs"]
        complete_rows.append({
            "day": datetime.fromtimestamp(at / 1000, tz=timezone.utc).date().isoformat(),
            "asset": group["asset"],
            "rankedNetUsd": values["ranked_consensus"],
            "controlNetUsd": values["consensus_unranked"],
            "differenceUsd": values["ranked_consensus"] - values["consensus_unranked"],
        })
    total_difference = sum(row["differenceUsd"] for row in complete_rows)
    result: dict[str, Any] = {
        "complete": not unresolved and not invalid_evaluations and not invalid_records,
        "basis": "matched_fresh_opportunities_closed_trade_net_not_portfolio_equity",
        "opportunities": len(opportunities),
        "completedOpportunities": len(complete_rows),
        "unresolved": dict(unresolved),
        "invalidEvaluations": invalid_evaluations,
        "unmatchedOrInvalidRecords": invalid_records,
        "rankRejectedControlEntered": rank_rejected_control_entered,
        "dayAssetClusters": len({(row["day"], row["asset"]) for row in complete_rows}),
        "rankedMinusControlNetUsdOnCompleted": round(total_difference, 6),
        "twoWayClusterInterval95Usd": None,
        "uncertaintyBasis": "day_and_asset_pigeonhole_bootstrap_diagnostic",
    }
    days = sorted({row["day"] for row in complete_rows})
    assets = sorted({row["asset"] for row in complete_rows})
    if not result["complete"]:
        result["uncertaintyReason"] = "incomplete_matched_outcomes"
        return result
    if len(complete_rows) < 30 or len(days) < 20 or len(assets) < 3:
        result["uncertaintyReason"] = "insufficient_opportunities_days_or_asset_clusters"
        return result
    if bootstrap_samples < 100:
        result["uncertaintyReason"] = "invalid_bootstrap_sample_count"
        return result
    rng = random.Random(20260922)
    totals = []
    for _ in range(bootstrap_samples):
        day_counts = Counter(rng.choices(days, k=len(days)))
        asset_counts = Counter(rng.choices(assets, k=len(assets)))
        totals.append(sum(
            row["differenceUsd"] * day_counts[row["day"]] * asset_counts[row["asset"]]
            for row in complete_rows
        ))
    totals.sort()
    result["twoWayClusterInterval95Usd"] = [
        round(totals[int(0.025 * (bootstrap_samples - 1))], 6),
        round(totals[int(0.975 * (bootstrap_samples - 1))], 6),
    ]
    return result
