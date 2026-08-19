from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server import (
    HyperliquidClient,
    WALLET_REVIEW_FILE,
    WALLETS_FILE,
    WalletStore,
    WalletTrackerService,
    now_iso,
    save_json_file,
    to_float,
    wallet_quality_window_trusted,
)


def position_fingerprint(wallet: dict[str, Any]) -> set[str]:
    return {
        f"{position.get('coin')}:{position.get('side')}"
        for position in wallet.get("positions", [])
        if isinstance(position, dict) and position.get("coin") and position.get("side")
    }


def open_unrealized_pnl(wallet: dict[str, Any]) -> float:
    return sum(
        to_float(position.get("unrealizedPnl"))
        for position in wallet.get("positions", [])
        if isinstance(position, dict)
    )


def evaluate_wallets(
    wallets: list[dict[str, Any]], stats: dict[str, int] | None = None
) -> dict[str, dict[str, Any]]:
    reviews: dict[str, dict[str, Any]] = {}
    fingerprints = {
        str(wallet.get("address") or "").lower(): position_fingerprint(wallet)
        for wallet in wallets
    }
    skipped_capped_window = 0
    suppressed_by_open_profit = 0
    for wallet in wallets:
        address = str(wallet.get("address") or "").lower()
        reasons: list[str] = []
        closed = int(
            to_float(
                wallet.get(
                    "qualityClosedEvents30d",
                    wallet.get("closedTrades30d"),
                )
            )
        )
        pnl = to_float(wallet.get("qualityNetPnl30d", wallet.get("realizedPnl30d")))
        profit_factor = to_float(wallet.get("qualityProfitFactor30d"))
        # qualityNetPnl30d/qualityProfitFactor30d are computed from a single
        # userFillsByTime page that caps at WALLET_WINDOW_FILL_CAP rows with no
        # pagination (server.py fetch_wallet_snapshot). A high-frequency wallet
        # can exhaust that cap within hours of the 30-day window, so a capped
        # page's 30d PnL/profit-factor describe only that opening slice - not
        # 30 days - and must not be trusted to downweight the wallet. A capped
        # page that still spans most of the 30 days is a fair sample though,
        # so wallet_quality_window_trusted (not the raw truncation flag) is
        # what decides whether these two checks are skipped.
        if not wallet_quality_window_trusted(wallet):
            skipped_capped_window += 1
        else:
            # negative_30d_pnl and profit_factor_below_1 both read qualityNetPnl30d/
            # qualityProfitFactor30d, which count ONLY closed events. Measured across
            # the 37 tracked wallets: exactly 4 tripped profit_factor_below_1, and all
            # four were sitting on positive unrealised PnL, totalling +$1,073,021
            # (0x795cfd1b03 pf 0.31 realised -$244k unrealised +$545,564;
            # 0x3fc56e944a pf 0.35 realised -$6k unrealised +$511,362; 0x7d5c17cdda
            # pf 0.82 realised -$8k unrealised +$15,037; 0x31dea2516b pf 0.38
            # realised -$4k unrealised +$1,059). Four out of four is not sampling
            # noise - a wallet that cuts losers fast and lets winners run always
            # looks like a loser on realised-only numbers while its profit sits in
            # open positions. Gating on the combined (realised + open unrealised)
            # figure means a wallet whose open profit more than covers its realised
            # loss is carrying, not losing, and both reasons share this defect
            # since both read the same realised-only pnl.
            combined = pnl + open_unrealized_pnl(wallet)
            would_flag_negative_pnl = closed >= 5 and pnl < 0
            would_flag_profit_factor = closed >= 5 and 0 < profit_factor < 1
            if closed >= 5 and pnl < 0 and combined < 0:
                reasons.append("negative_30d_pnl")
            if closed >= 5 and 0 < profit_factor < 1 and combined < 0:
                reasons.append("profit_factor_below_1")
            if (would_flag_negative_pnl or would_flag_profit_factor) and combined >= 0:
                suppressed_by_open_profit += 1
        if wallet.get("holdingOnly30d") or to_float(wallet.get("daysSinceLastFill")) > 30:
            reasons.append("inactive")
        if wallet.get("reviewWeightMultiplier") == 0:
            reasons.append("manual_exclusion")
        if reasons:
            reviews[address] = {"weight": 0.5, "reasons": sorted(set(reasons))}

    ordered = sorted(
        wallets,
        key=lambda item: to_float(item.get("recentWinRateRank", {}).get("convictionWeightScore")),
        reverse=True,
    )
    for index, better in enumerate(ordered):
        better_address = str(better.get("address") or "").lower()
        better_events = fingerprints.get(better_address, set())
        for weaker in ordered[index + 1 :]:
            weaker_address = str(weaker.get("address") or "").lower()
            weaker_events = fingerprints.get(weaker_address, set())
            shared = len(better_events & weaker_events)
            union = len(better_events | weaker_events)
            if shared >= 3 and union and shared / union >= 0.8:
                review = reviews.setdefault(weaker_address, {"weight": 0.5, "reasons": []})
                review["reasons"] = sorted(set([*review["reasons"], f"correlated_with_{better_address}"]))
    if stats is not None:
        stats["skippedCappedWindow"] = skipped_capped_window
        stats["suppressedByOpenProfit"] = suppressed_by_open_profit
    return reviews


def main() -> int:
    service = WalletTrackerService(WalletStore(WALLETS_FILE), HyperliquidClient())
    dashboard = service.dashboard()
    review_stats: dict[str, int] = {}
    reviews = evaluate_wallets(dashboard.get("wallets", []), review_stats)
    payload = {
        "version": 1,
        "generatedAt": now_iso(),
        "walletCount": len(dashboard.get("wallets", [])),
        "reviewCount": len(reviews),
        "skippedCappedWindowCount": review_stats.get("skippedCappedWindow", 0),
        "suppressedByOpenProfitCount": review_stats.get("suppressedByOpenProfit", 0),
        "wallets": reviews,
    }
    previous = {}
    try:
        previous = json.loads(WALLET_REVIEW_FILE.read_text())
    except (OSError, ValueError):
        pass
    save_json_file(WALLET_REVIEW_FILE, payload)

    changed = previous.get("wallets") != reviews if isinstance(previous, dict) else True
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    if changed and bot_token and chat_id:
        lines = [
            "Weekly wallet health review",
            f"Tracked: {payload['walletCount']} | Reduced weight: {payload['reviewCount']}",
            f"Skipped (capped fill window): {payload['skippedCappedWindowCount']}",
            f"Suppressed (open profit offsets realised loss): {payload['suppressedByOpenProfitCount']}",
        ]
        for address, review in list(reviews.items())[:10]:
            lines.append(f"- {address[:6]}...{address[-4:]}: 0.5 ({', '.join(review['reasons'])})")
        service.send_telegram_message(bot_token, chat_id, "\n".join(lines))
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
