import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Iterable


COINMARKETMAN_API_BASE_URL = "https://ht-api.coinmarketman.com/api/external"
COINMARKETMAN_TOKEN_ENV = "COINMARKETMAN_API_TOKEN"


class CoinMarketManApiError(RuntimeError):
    pass


class CoinMarketManClient:
    def __init__(self, token: str | None = None, base_url: str | None = None, timeout: int = 30) -> None:
        self.token = token or os.environ.get(COINMARKETMAN_TOKEN_ENV, "")
        self.base_url = (base_url or os.environ.get("COINMARKETMAN_API_BASE_URL") or COINMARKETMAN_API_BASE_URL).rstrip("/")
        self.timeout = timeout

    def request(self, path: str, params: dict[str, Any] | None = None) -> Any:
        if not self.token:
            raise CoinMarketManApiError(f"Missing {COINMARKETMAN_TOKEN_ENV}")

        url = f"{self.base_url}/{path.lstrip('/')}"
        query = self._encode_params(params or {})
        if query:
            url = f"{url}?{query}"

        req = urllib.request.Request(
            url,
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {self.token}",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                body = resp.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise CoinMarketManApiError(f"CMM API returned HTTP {exc.code}: {detail[:500]}") from exc
        except urllib.error.URLError as exc:
            raise CoinMarketManApiError(f"CMM API request failed: {exc.reason}") from exc

        try:
            return json.loads(body)
        except json.JSONDecodeError as exc:
            raise CoinMarketManApiError("CMM API returned non-JSON response") from exc

    def segments(self) -> Any:
        return self.request("segments")

    def wallets(
        self,
        *,
        offset: int = 0,
        limit: int = 50,
        order_by: str = "perpPnl",
        order: str = "desc",
        segment_ids: Iterable[int] | None = None,
        has_open_positions: bool | None = None,
        address: str | None = None,
    ) -> Any:
        params: dict[str, Any] = {
            "offset": offset,
            "limit": limit,
            "orderBy": order_by,
            "order": order,
        }
        if segment_ids:
            params["segmentIds"] = [str(item) for item in segment_ids]
        if has_open_positions is not None:
            params["hasOpenPositions"] = "true" if has_open_positions else "false"
        if address:
            params["address"] = address
        return self.request("wallets", params)

    def all_pnl_leaderboard(
        self,
        *,
        offset: int = 0,
        limit: int = 25,
        order_by: str = "pnlMonth",
        rank_by: str | None = None,
        order: str = "desc",
    ) -> Any:
        rank_field = rank_by or order_by
        return self.request(
            "leaderboards/all-pnl",
            {
                "offset": offset,
                "limit": limit,
                "rankBy": rank_field,
                "orderBy": order_by,
                "order": order,
            },
        )

    def cohort_summary(self, segment_id: int, *, position_age: str = "7d") -> Any:
        return self.request(f"segments/{segment_id}/summary", {"positionAge": position_age})

    def positions_heatmap(self, *, opened_within: str = "7d") -> Any:
        return self.request("positions/heatmap", {"openedWithin": opened_within})

    def position_metrics(
        self,
        coin: str,
        segment_id: int,
        *,
        start: str | None = None,
        end: str | None = None,
        limit: int = 500,
        position_recency_timeframe: str = "7d",
        next_cursor: str | None = None,
    ) -> Any:
        params: dict[str, Any] = {
            "limit": limit,
            "positionRecencyTimeframe": position_recency_timeframe,
        }
        if start:
            params["start"] = start
        if end:
            params["end"] = end
        if next_cursor:
            params["nextCursor"] = next_cursor
        return self.request(f"position-metrics/coin/{coin.upper()}/segment/{segment_id}", params)

    def closed_trades_summary(self, address: str, *, interval: str = "30d") -> Any:
        return self.request("closed-trades/summary", {"address": address, "interval": interval})

    @staticmethod
    def _encode_params(params: dict[str, Any]) -> str:
        pairs: list[tuple[str, str]] = []
        for key, value in params.items():
            if value is None:
                continue
            if isinstance(value, (list, tuple, set)):
                pairs.extend((key, str(item)) for item in value)
            else:
                pairs.append((key, str(value)))
        return urllib.parse.urlencode(pairs)


def wallet_age_days(value: str | None, now: datetime | None = None) -> float | None:
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        created = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if created.tzinfo is None:
        created = created.replace(tzinfo=timezone.utc)
    current = now or datetime.now(timezone.utc)
    return max(0.0, (current - created.astimezone(timezone.utc)).total_seconds() / 86400)


def extract_items(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        items = payload.get("items") or payload.get("data")
        if isinstance(items, list):
            return [item for item in items if isinstance(item, dict)]
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    return []


def select_top_wallets(
    payload: Any,
    *,
    limit: int = 10,
    min_age_days: float = 30,
    min_total_equity: float = 100_000,
    min_perp_pnl: float = 0,
    require_open_positions: bool = False,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    for item in extract_items(payload):
        profile = item.get("profile") or item
        age = wallet_age_days(item.get("age") or profile.get("earliestActivityAt"), now)
        total_equity = float(profile.get("totalEquity") or item.get("totalValue") or 0)
        perp_pnl = float(profile.get("perpPnl") or item.get("perpPnl") or 0)
        open_value = float(profile.get("openValue") or item.get("openValue") or 0)
        pnl_month = float(item.get("pnlMonth") or 0)
        pnl_all_time = float(item.get("pnlAllTime") or perp_pnl or 0)

        if age is None or age < min_age_days:
            continue
        if total_equity < min_total_equity:
            continue
        if perp_pnl < min_perp_pnl:
            continue
        if require_open_positions and open_value <= 0:
            continue

        enriched = {
            **item,
            "ageDays": age,
            "totalEquity": total_equity,
            "perpPnl": perp_pnl,
            "openValue": open_value,
            "selectionScore": pnl_month + (0.15 * pnl_all_time) + (0.05 * total_equity),
        }
        selected.append(enriched)

    selected.sort(key=lambda item: item["selectionScore"], reverse=True)
    return selected[:limit]


def compact_wallet_rows(wallets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for wallet in wallets:
        profile = wallet.get("profile") or wallet
        rows.append(
            {
                "address": wallet.get("address") or profile.get("address"),
                "rank": wallet.get("rank"),
                "ageDays": round(float(wallet.get("ageDays") or 0), 1),
                "pnlMonth": round(float(wallet.get("pnlMonth") or 0), 2),
                "pnlAllTime": round(float(wallet.get("pnlAllTime") or 0), 2),
                "totalEquity": round(float(wallet.get("totalEquity") or 0), 2),
                "perpPnl": round(float(wallet.get("perpPnl") or 0), 2),
                "openValue": round(float(wallet.get("openValue") or 0), 2),
                "topHolding": wallet.get("topHolding"),
                "segments": profile.get("segments"),
            }
        )
    return rows


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="CoinMarketMan HyperTracker API helper")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("segments")

    wallets = subparsers.add_parser("wallets")
    wallets.add_argument("--limit", type=int, default=50)
    wallets.add_argument("--offset", type=int, default=0)
    wallets.add_argument("--order-by", default="perpPnl")
    wallets.add_argument("--order", default="desc")
    wallets.add_argument("--segment-id", action="append", type=int, default=[])
    wallets.add_argument("--has-open-positions", action="store_true")
    wallets.add_argument("--address")

    top = subparsers.add_parser("top-wallets")
    top.add_argument("--fetch-limit", type=int, default=100, choices=[25, 50, 100])
    top.add_argument("--limit", type=int, default=10)
    top.add_argument("--order-by", default="pnlMonth", choices=["pnlDay", "pnlWeek", "pnlMonth", "pnlAllTime"])
    top.add_argument("--rank-by", choices=["pnlDay", "pnlWeek", "pnlMonth", "pnlAllTime"])
    top.add_argument("--min-age-days", type=float, default=30)
    top.add_argument("--min-total-equity", type=float, default=100_000)
    top.add_argument("--min-perp-pnl", type=float, default=0)
    top.add_argument("--require-open-positions", action="store_true")
    top.add_argument("--raw", action="store_true")

    summary = subparsers.add_parser("cohort-summary")
    summary.add_argument("segment_id", type=int)
    summary.add_argument("--position-age", default="7d", choices=["all", "24h", "7d", "30d"])

    heatmap = subparsers.add_parser("positions-heatmap")
    heatmap.add_argument("--opened-within", default="7d", choices=["all", "24h", "7d", "30d"])

    metrics = subparsers.add_parser("position-metrics")
    metrics.add_argument("coin")
    metrics.add_argument("segment_id", type=int)
    metrics.add_argument("--start")
    metrics.add_argument("--end")
    metrics.add_argument("--limit", type=int, default=500)
    metrics.add_argument("--position-recency-timeframe", default="7d", choices=["all", "24h", "7d", "30d"])
    metrics.add_argument("--next-cursor")

    closed = subparsers.add_parser("closed-trades-summary")
    closed.add_argument("address")
    closed.add_argument("--interval", default="30d", choices=["all", "365d", "180d", "90d", "30d", "last50"])

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    client = CoinMarketManClient()

    if args.command == "segments":
        payload = client.segments()
    elif args.command == "wallets":
        payload = client.wallets(
            offset=args.offset,
            limit=args.limit,
            order_by=args.order_by,
            order=args.order,
            segment_ids=args.segment_id,
            has_open_positions=args.has_open_positions or None,
            address=args.address,
        )
    elif args.command == "top-wallets":
        payload = client.all_pnl_leaderboard(
            limit=args.fetch_limit,
            order_by=args.order_by,
            rank_by=args.rank_by,
        )
        if not args.raw:
            payload = compact_wallet_rows(
                select_top_wallets(
                    payload,
                    limit=args.limit,
                    min_age_days=args.min_age_days,
                    min_total_equity=args.min_total_equity,
                    min_perp_pnl=args.min_perp_pnl,
                    require_open_positions=args.require_open_positions,
                )
            )
    elif args.command == "cohort-summary":
        payload = client.cohort_summary(args.segment_id, position_age=args.position_age)
    elif args.command == "positions-heatmap":
        payload = client.positions_heatmap(opened_within=args.opened_within)
    elif args.command == "position-metrics":
        payload = client.position_metrics(
            args.coin,
            args.segment_id,
            start=args.start,
            end=args.end,
            limit=args.limit,
            position_recency_timeframe=args.position_recency_timeframe,
            next_cursor=args.next_cursor,
        )
    elif args.command == "closed-trades-summary":
        payload = client.closed_trades_summary(args.address, interval=args.interval)
    else:
        raise CoinMarketManApiError(f"Unsupported command: {args.command}")

    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except CoinMarketManApiError as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(1)
