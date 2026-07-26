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
)


def position_fingerprint(wallet: dict[str, Any]) -> set[str]:
    return {
        f"{position.get('coin')}:{position.get('side')}"
        for position in wallet.get("positions", [])
        if isinstance(position, dict) and position.get("coin") and position.get("side")
    }


def evaluate_wallets(wallets: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    reviews: dict[str, dict[str, Any]] = {}
    fingerprints = {
        str(wallet.get("address") or "").lower(): position_fingerprint(wallet)
        for wallet in wallets
    }
    for wallet in wallets:
        address = str(wallet.get("address") or "").lower()
        reasons: list[str] = []
        closed = int(to_float(wallet.get("closedTrades30d")))
        pnl = to_float(wallet.get("qualityNetPnl30d", wallet.get("realizedPnl30d")))
        profit_factor = to_float(wallet.get("qualityProfitFactor30d"))
        if closed >= 5 and pnl < 0:
            reasons.append("negative_30d_pnl")
        if closed >= 5 and 0 < profit_factor < 1:
            reasons.append("profit_factor_below_1")
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
    return reviews


def main() -> int:
    service = WalletTrackerService(WalletStore(WALLETS_FILE), HyperliquidClient())
    dashboard = service.dashboard()
    reviews = evaluate_wallets(dashboard.get("wallets", []))
    payload = {
        "version": 1,
        "generatedAt": now_iso(),
        "walletCount": len(dashboard.get("wallets", [])),
        "reviewCount": len(reviews),
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
        ]
        for address, review in list(reviews.items())[:10]:
            lines.append(f"- {address[:6]}...{address[-4:]}: 0.5 ({', '.join(review['reasons'])})")
        service.send_telegram_message(bot_token, chat_id, "\n".join(lines))
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
