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

# A wallet whose fills are inventory turnover rather than directional
# conviction pollutes the "coordinated openings in the last 5 minutes"
# consensus signal: it opens and closes constantly, so it shows up alongside
# genuinely coordinated wallets and inflates the agreement count. Measured on
# production against each wallet's newest 1500 fills (userFillsByTime, newest
# first, so no ascending-cap distortion - see the note on
# recent_fill_rate_per_min below): 188.05, 88.81 and 58.83 fills/min for three
# wallets, 19.72 and 17.12 for two more, then a clear 3.6x gap down to 4.73,
# 4.36 and 2.29, with everything else under 1/min. Four wallets were already
# pulled from tracking today for exactly this behaviour at 66-111 fills/min.
# The threshold sits at 10 - in the gap, not at 4, which would also catch
# 0xbcd420d133 and 0x8cc94dc843, both kept deliberately for good profit
# factors.
MARKET_MAKER_FILLS_PER_MIN = float(os.environ.get("MARKET_MAKER_FILLS_PER_MIN", "10"))
MARKET_MAKER_MIN_FILL_SAMPLE = int(os.environ.get("MARKET_MAKER_MIN_FILL_SAMPLE", "300"))


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
def recent_fill_rate_per_min(wallet: dict[str, Any]) -> float:
    # recentFillCount/fillCoverageMs (not qualityWindowFillCount/
    # qualityWindowCoverageMs) are the matched pair describing the same
    # retained history: server.py sets fillCoverageMs to (now - oldest
    # retained fill) once the retained history is truncated and to
    # (now - fills_start_ms) otherwise (server.py:1851, server.py:2076), and
    # recentFillCount is the length of that same merged list (server.py:2142).
    # The quality-window pair comes from userFillsByTime, which caps at 2000
    # rows ascending from startTime - a wallet that only recently turned
    # high-frequency still shows ~29 days of coverage there, understating the
    # rate by roughly three orders of magnitude (~68/day instead of ~100/min).
    # That is the exact trap this helper exists to avoid.
    quality = wallet.get("dataQuality")
    if not isinstance(quality, dict):
        return 0.0
    count = to_float(quality.get("recentFillCount"))
    coverage_ms = to_float(quality.get("fillCoverageMs"))
    if count < MARKET_MAKER_MIN_FILL_SAMPLE or coverage_ms <= 0:
        return 0.0
    return count / (coverage_ms / 60000.0)


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
    market_maker_wallets = 0
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
        # A market maker's positions are inventory, not conviction, and its
        # 30d quality window is usually untrusted anyway (capped within hours
        # by its own fill rate) - so this check must run independent of the
        # trusted branch above, not nested inside its else, or it would never
        # fire for the wallets it exists to catch.
        fill_rate = recent_fill_rate_per_min(wallet)
        if fill_rate >= MARKET_MAKER_FILLS_PER_MIN:
            reasons.append("market_maker_fill_rate")
            market_maker_wallets += 1
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
        stats["marketMakerWallets"] = market_maker_wallets
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
        "marketMakerCount": review_stats.get("marketMakerWallets", 0),
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
            f"Market maker fill rate: {payload['marketMakerCount']}",
        ]
        for address, review in list(reviews.items())[:10]:
            lines.append(f"- {address[:6]}...{address[-4:]}: 0.5 ({', '.join(review['reasons'])})")
        service.send_telegram_message(bot_token, chat_id, "\n".join(lines))
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
