"""Build the production copyability registry from prospective wallet exits."""
from __future__ import annotations

import math
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any


COPYABILITY_METHOD = "enter_after_detection_exit_with_wallet"
ONE_SIDED_95_Z = 1.6448536269514722


def _number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def independent_wallet_episodes(state: Any) -> dict[str, list[dict[str, Any]]]:
    """Select a maximum set of non-overlapping completed episodes per wallet.

    Simultaneous signals for several coins are not independent observations of
    the same wallet. Sorting intervals by their exit and greedily retaining the
    next interval that starts after the preceding exit is the standard maximum
    cardinality interval selection and makes the sample claim reproducible.
    """
    candidates: dict[str, list[dict[str, Any]]] = defaultdict(list)
    trades = state.get("trades", []) if isinstance(state, dict) else []
    for trade in trades if isinstance(trades, list) else []:
        if not isinstance(trade, dict):
            continue
        entry_at = int(_number(trade.get("entryAt")) or 0)
        if entry_at <= 0:
            continue
        for departure in trade.get("departures", []):
            if not isinstance(departure, dict):
                continue
            address = str(departure.get("address") or "").lower()
            exit_at = int(_number(departure.get("detectedAt")) or 0)
            net_pct = _number(departure.get("netPct"))
            if not address or exit_at < entry_at or net_pct is None:
                continue
            candidates[address].append(
                {
                    "entryAt": entry_at,
                    "exitAt": exit_at,
                    "netPct": net_pct,
                    "coin": str(trade.get("coin") or ""),
                    "side": str(trade.get("side") or ""),
                    "tradeId": str(trade.get("id") or ""),
                }
            )

    selected: dict[str, list[dict[str, Any]]] = {}
    for address, rows in candidates.items():
        last_exit = -1
        kept = []
        for row in sorted(rows, key=lambda item: (item["exitAt"], item["entryAt"], item["tradeId"])):
            if row["entryAt"] < last_exit:
                continue
            kept.append(row)
            last_exit = row["exitAt"]
        selected[address] = kept
    return selected


def lower_confidence_bound(values: list[float]) -> float | None:
    """One-sided 95% lower bound for a mean; used only after n reaches 30."""
    n = len(values)
    if n < 2:
        return None
    mean = sum(values) / n
    variance = sum((value - mean) ** 2 for value in values) / (n - 1)
    return mean - ONE_SIDED_95_Z * math.sqrt(variance / n)


def build_wallet_copyability_registry(
    state: Any,
    *,
    observation_complete: bool,
    generated_at_ms: int | None = None,
) -> dict[str, Any]:
    episodes = independent_wallet_episodes(state)
    wallets: dict[str, dict[str, Any]] = {}
    for address, rows in sorted(episodes.items()):
        values = [row["netPct"] for row in rows]
        mean = sum(values) / len(values)
        bound = lower_confidence_bound(values)
        wallets[address] = {
            "method": COPYABILITY_METHOD,
            "independentCompletedEpisodes": len(values),
            "costAdjustedNetReturnPct": round(mean, 6),
            "lowerConfidenceBoundPct": None if bound is None else round(bound, 6),
            "observationComplete": bool(observation_complete),
        }
    generated_at = generated_at_ms
    if generated_at is None:
        generated_at = int(datetime.now(timezone.utc).timestamp() * 1000)
    return {
        "version": 2,
        "method": COPYABILITY_METHOD,
        "generatedAt": datetime.fromtimestamp(generated_at / 1000, timezone.utc).isoformat(),
        "observationComplete": bool(observation_complete),
        "sourceExperiment": str(state.get("version") or "") if isinstance(state, dict) else "",
        "wallets": wallets,
    }
