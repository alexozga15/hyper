#!/usr/bin/env python3
"""Walk-forward evaluator for fresh tracked-wallet opening consensus.

The live system keeps only the latest position snapshot, but Hyperliquid exposes
recent fills.  This script reconstructs *new opening* consensus from those
timestamped fills and evaluates the subsequent Hyperliquid candle path.  It is
therefore intentionally narrower than the live signal engine: historical CMM
confirmation, correlation groups, and point-in-time top-10 membership cannot
be reconstructed honestly from current state alone.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server import HyperliquidClient, WalletStore, WalletTrackerService, WALLETS_FILE, normalize_position_coin, now_iso, to_float


HORIZONS_HOURS = (1, 4, 12, 24)
DEFAULT_CONFIGS = (
    {"name": "fast_3w_10m", "minWallets": 3, "windowMinutes": 10},
    {"name": "core_4w_30m", "minWallets": 4, "windowMinutes": 30},
    {"name": "strict_5w_30m", "minWallets": 5, "windowMinutes": 30},
)


def classify_open_side(direction: Any) -> str | None:
    text = str(direction or "").strip().lower()
    if "open" not in text and "increase" not in text:
        return None
    if "long" in text:
        return "long"
    if "short" in text:
        return "short"
    return None


def normalize_fill(address: str, fill: dict[str, Any]) -> dict[str, Any] | None:
    side = classify_open_side(fill.get("dir"))
    price = to_float(fill.get("px"))
    size = abs(to_float(fill.get("sz")))
    time_ms = int(to_float(fill.get("time")))
    if not side or price <= 0 or size <= 0 or time_ms <= 0:
        return None
    return {
        "address": address.lower(),
        "coin": normalize_position_coin(fill.get("coin")),
        "side": side,
        "price": price,
        "size": size,
        "time": time_ms,
    }


def build_consensus_events(
    fills: list[dict[str, Any]], *, min_wallets: int, window_minutes: int
) -> list[dict[str, Any]]:
    """Emit one signal when a fixed, non-overlapping window first reaches consensus."""
    window_ms = window_minutes * 60 * 1000
    buckets: dict[tuple[str, str, int], list[dict[str, Any]]] = defaultdict(list)
    for fill in fills:
        bucket_start = (int(fill["time"]) // window_ms) * window_ms
        buckets[(str(fill["coin"]), str(fill["side"]), bucket_start)].append(fill)

    events = []
    for (coin, side, bucket_start), bucket_fills in sorted(buckets.items(), key=lambda item: item[0][2]):
        wallet_fills: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for fill in bucket_fills:
            wallet_fills[str(fill["address"])].append(fill)
        if len(wallet_fills) < min_wallets:
            continue
        weighted_size = sum(fill["price"] * fill["size"] for fill in bucket_fills)
        total_size = sum(fill["size"] for fill in bucket_fills)
        event_time = max(int(fill["time"]) for fill in bucket_fills)
        events.append(
            {
                "coin": coin,
                "side": side,
                "time": event_time,
                "windowStart": bucket_start,
                "windowMinutes": window_minutes,
                "walletCount": len(wallet_fills),
                "fillCount": len(bucket_fills),
                "entryPrice": round(weighted_size / total_size, 10) if total_size else 0.0,
                "size": round(total_size, 10),
                "wallets": sorted(wallet_fills),
            }
        )
    return events


def candle_close_at_or_after(candles: list[dict[str, Any]], time_ms: int) -> float:
    for candle in candles:
        if int(to_float(candle.get("t"))) >= time_ms:
            return to_float(candle.get("c"))
    return 0.0


def evaluate_event(
    event: dict[str, Any], candles: list[dict[str, Any]], *, cost_bps_per_side: float
) -> dict[str, Any]:
    entry = to_float(event.get("entryPrice"))
    if entry <= 0:
        return {**event, "outcomes": {}}
    side_multiplier = 1.0 if event.get("side") == "long" else -1.0
    cost_pct = (cost_bps_per_side * 2.0) / 10_000.0 * 100.0
    outcomes: dict[str, dict[str, float]] = {}
    for hours in HORIZONS_HOURS:
        start = int(event["time"])
        end = start + hours * 60 * 60 * 1000
        path = [
            candle
            for candle in candles
            if start <= int(to_float(candle.get("t"))) <= end
        ]
        close = candle_close_at_or_after(candles, end)
        if not path or close <= 0:
            continue
        gross_return = side_multiplier * ((close / entry) - 1.0) * 100.0
        favorable = max(to_float(item.get("h")) for item in path) if side_multiplier > 0 else min(to_float(item.get("l")) for item in path)
        adverse = min(to_float(item.get("l")) for item in path) if side_multiplier > 0 else max(to_float(item.get("h")) for item in path)
        mfe = side_multiplier * ((favorable / entry) - 1.0) * 100.0
        mae = side_multiplier * ((adverse / entry) - 1.0) * 100.0
        outcomes[f"{hours}h"] = {
            "grossReturnPct": round(gross_return, 4),
            "netReturnPct": round(gross_return - cost_pct, 4),
            "mfePct": round(max(mfe, 0.0), 4),
            "maePct": round(min(mae, 0.0), 4),
        }
    return {**event, "outcomes": outcomes}


def summarize_events(events: list[dict[str, Any]], *, period: str) -> dict[str, Any]:
    output: dict[str, Any] = {"period": period, "signals": len(events), "horizons": {}}
    for horizon in HORIZONS_HOURS:
        rows = [item["outcomes"][f"{horizon}h"] for item in events if f"{horizon}h" in item.get("outcomes", {})]
        if not rows:
            continue
        returns = [to_float(row["netReturnPct"]) for row in rows]
        wins = [value for value in returns if value > 0]
        losses = [value for value in returns if value <= 0]
        output["horizons"][f"{horizon}h"] = {
            "observations": len(rows),
            "winRatePct": round(len(wins) / len(rows) * 100, 2),
            "avgNetReturnPct": round(sum(returns) / len(rows), 4),
            "medianNetReturnPct": round(sorted(returns)[len(rows) // 2], 4),
            "profitFactor": round(sum(wins) / abs(sum(losses)), 3) if losses and sum(losses) else None,
            "avgMfePct": round(sum(to_float(row["mfePct"]) for row in rows) / len(rows), 4),
            "avgMaePct": round(sum(to_float(row["maePct"]) for row in rows) / len(rows), 4),
        }
    return output


def split_walk_forward(events: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    ordered = sorted(events, key=lambda item: int(item["time"]))
    train_end = math.floor(len(ordered) * 0.60)
    validation_end = math.floor(len(ordered) * 0.80)
    return {
        "train": ordered[:train_end],
        "validation": ordered[train_end:validation_end],
        "test": ordered[validation_end:],
    }


def evaluable_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep missing candle history from consuming a walk-forward sample slot."""
    return [item for item in events if item.get("outcomes")]


def fetch_candles(client: HyperliquidClient, coin: str, start_ms: int, end_ms: int) -> list[dict[str, Any]]:
    result = client.safe_post_result(
        {"type": "candleSnapshot", "req": {"coin": coin, "interval": "1h", "startTime": start_ms, "endTime": end_ms}},
        [],
    )
    data = result.get("data") if result.get("ok") else []
    return data if isinstance(data, list) else []


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=30, help="Fill lookback; Hyperliquid retention permitting.")
    parser.add_argument("--cost-bps-per-side", type=float, default=10.0, help="Round-trip cost assumes this value twice.")
    parser.add_argument("--output", type=Path, default=ROOT / "data" / "wallet_signal_backtest.json")
    parser.add_argument("--max-wallets", type=int, default=0, help="Optional cap for a fast smoke test.")
    args = parser.parse_args()
    if args.days < 2 or args.cost_bps_per_side < 0:
        parser.error("--days must be >= 2 and cost cannot be negative")

    service = WalletTrackerService(WalletStore(WALLETS_FILE), HyperliquidClient())
    wallets = service.store.list_wallets()
    if args.max_wallets:
        wallets = wallets[: args.max_wallets]
    end_ms = int(__import__("time").time() * 1000)
    start_ms = end_ms - args.days * 24 * 60 * 60 * 1000
    fills: list[dict[str, Any]] = []
    errors: list[str] = []
    for wallet in wallets:
        result = service.fetch_fills_result(wallet.address, start_ms)
        if not result["ok"]:
            errors.append(f"{wallet.address}: {result['error']}")
            continue
        fills.extend(
            normalized
            for raw in result["data"]
            if isinstance(raw, dict)
            if (normalized := normalize_fill(wallet.address, raw)) is not None
        )

    all_events: dict[str, list[dict[str, Any]]] = {}
    coins: set[str] = set()
    for config in DEFAULT_CONFIGS:
        events = build_consensus_events(fills, min_wallets=config["minWallets"], window_minutes=config["windowMinutes"])
        all_events[config["name"]] = events
        coins.update(str(item["coin"]) for item in events)

    candle_end_ms = end_ms - min(HORIZONS_HOURS) * 60 * 60 * 1000
    candles_by_coin: dict[str, list[dict[str, Any]]] = {}
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(fetch_candles, service.client, coin, start_ms, end_ms): coin for coin in coins}
        for future in as_completed(futures):
            coin = futures[future]
            try:
                candles_by_coin[coin] = future.result()
            except Exception as exc:  # diagnostics belong in the report, not a false clean result
                candles_by_coin[coin] = []
                errors.append(f"{coin} candles: {exc}")

    configurations = []
    for config in DEFAULT_CONFIGS:
        evaluated = [
            evaluate_event(event, candles_by_coin.get(str(event["coin"]), []), cost_bps_per_side=args.cost_bps_per_side)
            for event in all_events[config["name"]]
            if int(event["time"]) <= candle_end_ms
        ]
        evaluated = evaluable_events(evaluated)
        splits = split_walk_forward(evaluated)
        configurations.append(
            {
                **config,
                "summary": {name: summarize_events(rows, period=name) for name, rows in splits.items()},
                "events": evaluated,
            }
        )

    validation_candidates = [
        item for item in configurations
        if item["summary"]["validation"]["horizons"].get("4h", {}).get("observations", 0) >= 5
    ]
    selected = max(
        validation_candidates,
        key=lambda item: item["summary"]["validation"]["horizons"]["4h"]["avgNetReturnPct"],
        default=None,
    )
    report = {
        "generatedAt": now_iso(),
        "methodology": {
            "source": "Hyperliquid userFillsByTime plus 1h candleSnapshot",
            "signal": "distinct wallets opening the same coin and side within a fixed window",
            "entry": "fill-size-weighted consensus price at the last fill in the window",
            "outcomes": "direction-adjusted close return, MFE and MAE after estimated round-trip costs",
            "costBpsPerSide": args.cost_bps_per_side,
            "walkForward": "chronological 60% train / 20% validation / 20% test; select only on validation",
            "limitations": [
                "Historical position snapshots, correlation groups, CMM confirmation, and point-in-time top-10 membership are unavailable.",
                "This evaluates opening consensus only; it is not a claim that the complete live signal engine is backtested.",
            ],
        },
        "coverage": {"wallets": len(wallets), "openFills": len(fills), "coinsWithEvents": len(coins), "days": args.days},
        "configs": configurations,
        "selectedOnValidation": {
            "name": selected["name"],
            "test4h": selected["summary"]["test"]["horizons"].get("4h", {}),
        } if selected else None,
        "errors": errors,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps({"output": str(args.output), "coverage": report["coverage"], "selectedOnValidation": report["selectedOnValidation"], "errors": errors}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
