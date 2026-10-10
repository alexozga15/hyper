#!/usr/bin/env python3
"""Archive public Hyperdash history separately from verified ranking caches.

No credentials, trading operations, production writes, or automatic imports.
Pagination completion describes the API response, not underlying chain coverage.
"""
import argparse
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import time
import urllib.error
import urllib.request

GRAPHQL_URL = "https://api.hyperdash.com/graphql"
INFO_URL = "https://h.hyperdash.com/info"
TRADER_QUERY = """query GetTrader($address: String!) {
  getTrader(address: $address) {
    address pnl perpsEquity winrate180d sharpe180d sortino180d drawdown180d
    totalTrades totalWinningTrades totalLosingTrades
    portfolioGraph { timestamp value }
  }
}"""
TRADES_QUERY = """query GetTraderCompletedTrades($address: String!, $page: Int, $pageSize: Int) {
  getTraderCompletedTrades(address: $address, page: $page, pageSize: $pageSize) {
    tradeCount assetTradeCounts { coin tradeCount }
    trades {
      startTime endTime coin direction durationMins sz avgEntryPx avgExitPx
      grossPnl fundingPnl totalFees netPnl notional
    }
  }
}"""
STATS_QUERY = """query GetTraderDetailedStatsSummary($address: String!) {
  getTraderDetailedStatsSummary(address: $address) {
    tradeCount startDate endDate completedTradePnl { pnl longPnl shortPnl fees net }
  }
}"""


def timestamp_ms(value):
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return int(value)
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)


def identity(row):
    return hashlib.sha256(json.dumps(row, sort_keys=True).encode()).hexdigest()


def request(url, payload):
    req = urllib.request.Request(url, data=json.dumps(payload).encode(), headers={
        "Content-Type": "application/json", "User-Agent": "hyper-wallet-history-audit/1.0"})
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=40) as response:
                data = json.load(response)
            if isinstance(data, dict) and data.get("errors"):
                raise ValueError("GraphQL errors: " + json.dumps(data["errors"]))
            return data
        except urllib.error.HTTPError as exc:
            if exc.code not in (429, 500, 502, 503, 504) or attempt == 2:
                raise
            time.sleep(3 * (attempt + 1))
        except (TimeoutError, urllib.error.URLError):
            if attempt == 2:
                raise
            time.sleep(3 * (attempt + 1))


def gql(query, variables, key):
    data = request(GRAPHQL_URL, {"query": query, "variables": variables})
    value = data.get("data", {}).get(key)
    if value is None:
        raise ValueError(f"Missing GraphQL result: {key}")
    return value


def completed_trades(address, cutoff_ms, max_pages):
    pages, rows, seen, total = [], [], set(), None
    last_end = None
    status = "page_limit"
    for page in range(1, max_pages + 1):
        block = gql(TRADES_QUERY, {"address": address, "page": page, "pageSize": 200},
                    "getTraderCompletedTrades")
        batch = block.get("trades")
        if not isinstance(batch, list):
            raise ValueError("Missing trades list")
        pages.append(block)
        count = block.get("tradeCount")
        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            status = "invalid_count"
            break
        if total is not None and count != total:
            status = "count_changed_during_fetch"
            break
        total = count
        times = [timestamp_ms(r["endTime"]) for r in batch]
        if times != sorted(times, reverse=True) or (times and last_end is not None and times[0] > last_end):
            status = "unexpected_sort_order"
            break
        new = [r for r in batch if identity(r) not in seen]
        if batch and not new:
            status = "repeated_page"
            break
        for row in new:
            seen.add(identity(row))
            rows.append(row)
        if len(rows) == total:
            status = "advertised_count_received"
            break
        if len(rows) > total or not batch:
            status = "count_mismatch"
            break
        if times and times[-1] < cutoff_ms:
            status = "passed_180d_boundary"
            break
        last_end = times[-1] if times else last_end
        time.sleep(0.25)
    return {"pages": pages, "trades": rows, "paginationStatus": status,
            "advertisedCount": total, "chainCoverageVerified": False}


def raw_fills(address, cutoff_ms, end_ms, max_pages, previous=None):
    pages = list((previous or {}).get("pages", []))
    rows = list((previous or {}).get("fills", []))
    seen = {identity(r) for r in rows}
    cursor = max((timestamp_ms(r["time"]) for r in rows), default=cutoff_ms)
    status = "page_limit"
    for _ in range(max_pages):
        batch = request(INFO_URL, {"type": "userFillsByTime", "user": address,
            "startTime": cursor, "endTime": end_ms, "aggregateByTime": False,
            "reversed": False})
        if not isinstance(batch, list):
            raise ValueError("Missing raw fills list")
        pages.append({"startTime": cursor, "count": len(batch)})
        times = [timestamp_ms(r["time"]) for r in batch]
        if any(t < cursor or t > end_ms for t in times):
            status = "out_of_requested_range"
            break
        new = [r for r in batch if identity(r) not in seen]
        if batch and not new and len(batch) >= 2000:
            status = "repeated_page_or_timestamp_saturation"
            break
        for row in new:
            seen.add(identity(row))
            rows.append(row)
        if not batch:
            status = "empty_final_page"
            break
        # Inclusive overlap protects fills sharing the timestamp at a boundary.
        if max(times) <= cursor:
            if len(batch) >= 2000:
                status = "timestamp_saturation"
                break
            cursor += 1
        else:
            cursor = max(times)
        time.sleep(0.25)
    return {"pages": pages, "fills": rows, "paginationStatus": status,
            "chainCoverageVerified": False}


def closed_summary(block, cutoff_ms, end_ms):
    rows = [r for r in block["trades"] if cutoff_ms <= timestamp_ms(r["endTime"]) <= end_ms]
    profit = loss = Decimal(0)
    residuals = []
    for r in rows:
        net, gross, funding, fees = [Decimal(str(r[k])) for k in
                                   ("netPnl", "grossPnl", "fundingPnl", "totalFees")]
        if not all(x.is_finite() for x in (net, gross, funding, fees)):
            raise ValueError("Nonfinite trade financial data")
        profit += max(net, Decimal(0))
        loss += max(-net, Decimal(0))
        if abs(net - (gross + funding - fees)) > Decimal("0.02"):
            residuals.append({"coin": r["coin"], "endTime": r["endTime"],
                              "residualUsd": str(net - (gross + funding - fees))})
    return {"closedTrades180d": len(rows), "winningTrades180d": sum(Decimal(str(r["netPnl"])) > 0 for r in rows),
            "losingTrades180d": sum(Decimal(str(r["netPnl"])) < 0 for r in rows),
            "closedGrossProfitUsd": float(profit), "closedGrossLossUsd": float(loss),
            "closedNetPnlUsd": float(profit - loss),
            "closedOnlyProfitFactor": float(profit / loss) if loss else None,
            "netPnlEquationExceptions": residuals,
            "rankingEligible": False,
            "limitations": ["Provider coverage and episode boundaries are not chain-verified",
                            "Closed-only PF excludes current open losses and open costs",
                            "Trades crossing the 180-day boundary include out-of-window costs/PnL"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wallets", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--max-pages", type=int, default=30)
    parser.add_argument("--reuse", action="store_true", help="Reuse successful exports; resume capped fill histories")
    args = parser.parse_args()
    if args.max_pages < 1:
        parser.error("--max-pages must be positive")
    inputs = json.loads(args.wallets.read_text())
    addresses = [w["address"].lower() for w in inputs["wallets"]]
    if len(set(addresses)) != len(addresses) or any(
            len(a) != 42 or not a.startswith("0x") or any(c not in "0123456789abcdef" for c in a[2:])
            for a in addresses):
        parser.error("Expected unique Ethereum wallet addresses")
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / "wallets.json").write_text(json.dumps(inputs, indent=2))
    now_ms = int(time.time() * 1000)
    cutoff_ms = now_ms - 180 * 86400000
    if args.reuse:
        windows = set()
        for address in addresses:
            path = args.output / f"{address}.json"
            if path.exists():
                previous = json.loads(path.read_text())
                windows.add((previous["startMs"], previous["endMs"]))
        if len(windows) > 1:
            parser.error("Cannot reuse exports with different observation windows")
        if windows:
            cutoff_ms, now_ms = windows.pop()
    manifest = {"provider": "Hyperdash", "graphqlUrl": GRAPHQL_URL, "infoUrl": INFO_URL,
                "asOf": datetime.fromtimestamp(now_ms / 1000, timezone.utc).isoformat(),
                "startMs": cutoff_ms, "endMs": now_ms, "wallets": []}
    for address in addresses:
        path = args.output / f"{address}.json"
        data = {"address": address, "startMs": cutoff_ms, "endMs": now_ms,
                "retrievedAt": datetime.now(timezone.utc).isoformat(), "errors": {}}
        previous_raw = None
        if args.reuse and path.exists():
            previous = json.loads(path.read_text())
            for key in ("trader", "completed", "stats", "funding", "twapFills", "currentState"):
                if key in previous:
                    data[key] = previous[key]
            data["reusedFrom"] = previous.get("retrievedAt")
            previous_raw = previous.get("rawFills")
            if previous_raw and previous_raw.get("paginationStatus") == "empty_final_page":
                data["rawFills"] = previous_raw
        operations = {
            "trader": lambda: gql(TRADER_QUERY, {"address": address}, "getTrader"),
            "completed": lambda: completed_trades(address, cutoff_ms, args.max_pages),
            "stats": lambda: gql(STATS_QUERY, {"address": address}, "getTraderDetailedStatsSummary"),
            "rawFills": lambda: raw_fills(address, cutoff_ms, now_ms, args.max_pages, previous_raw),
            "funding": lambda: request(INFO_URL, {"type": "userFunding", "user": address}),
            "twapFills": lambda: request(INFO_URL, {"type": "userTwapSliceFills", "user": address}),
            "currentState": lambda: request(INFO_URL, {"type": "portfolioState", "user": address, "dex": "ALL_DEXES"}),
        }
        for key, fn in operations.items():
            if key in data:
                continue
            try:
                data[key] = fn()
            except Exception as exc:
                data["errors"][key] = f"{type(exc).__name__}: {exc}"
            time.sleep(0.25)
        summary = {"address": address, "errors": data["errors"]}
        if "completed" in data:
            try:
                summary.update(closed_summary(data["completed"], cutoff_ms, now_ms))
            except Exception as exc:
                summary["validationError"] = str(exc)
            summary["completedPagination"] = data["completed"]["paginationStatus"]
            summary["completedReceived"] = len(data["completed"]["trades"])
            summary["completedAdvertised"] = data["completed"]["advertisedCount"]
        if "rawFills" in data:
            summary["rawFillsReceived"] = len(data["rawFills"]["fills"])
            summary["rawPagination"] = data["rawFills"]["paginationStatus"]
        graph = data.get("trader", {}).get("portfolioGraph", [])
        summary["portfolioGraphPoints"] = len(graph)
        summary["portfolioGraph180dPoints"] = sum(cutoff_ms <= r["timestamp"] <= now_ms for r in graph)
        data["summary"] = summary
        path.write_text(json.dumps(data, indent=2, allow_nan=False))
        summary["sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
        manifest["wallets"].append(summary)
        (args.output / "manifest.json").write_text(json.dumps(manifest, indent=2, allow_nan=False))
        print(address, summary.get("closedTrades180d"), summary.get("rawFillsReceived"),
              summary.get("completedPagination"), summary.get("rawPagination"), data["errors"], flush=True)


if __name__ == "__main__":
    main()
