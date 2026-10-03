"""Refresh three wallets outside the latency-sensitive sentiment cycle."""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from history_cache import ResumableHistoryCache
from server import (
    DATA_DIR, HyperliquidClient, WalletStore, WalletTrackerService,
    WALLETS_FILE, load_json_file, now_iso, save_json_file,
)


def main() -> int:
    service = WalletTrackerService(WalletStore(WALLETS_FILE), HyperliquidClient(priority="history"))
    service.history_cache = ResumableHistoryCache(
        DATA_DIR / "wallet_history.sqlite3",
        pages_per_run=int(os.environ.get("HYPERLIQUID_HISTORY_PAGES_PER_RUN", "4")),
    )
    wallets = service.store.list_wallets()
    cache = load_json_file(service.wallet_quality_cache_path, {}).get("wallets", {})
    chosen = service.wallet_quality_refresh_addresses(wallets, cache)
    tracked = {w.address.lower() for w in wallets}
    results = []
    for wallet in wallets:
        if wallet.address.lower() not in chosen:
            continue
        snapshot = service.fetch_wallet_snapshot(
            wallet, full_quality_refresh=True,
            cached_snapshot=cache.get(wallet.address.lower()),
        )
        # Save after every wallet, not only after the batch. Incomplete history
        # retains its previous quality score and rotates without starving others.
        service.persist_wallet_snapshots([snapshot], tracked)
        quality = snapshot.get("dataQuality") or {}
        rank = snapshot.get("recentWinRateRank") or {}
        results.append({"address": wallet.address,
                        "fetchSucceeded": bool(quality.get("qualityRefreshSucceeded")),
                        "complete": bool(rank.get("rankable") and rank.get("assessmentStatus") == "Verified"),
                        "rankable": bool(rank.get("rankable")),
                        "assessmentStatus": rank.get("assessmentStatus", "Preliminary"),
                        "missingComponents": [name for name, available in rank.get("scoreComponentsAvailable", {}).items() if not available],
                        "fillRetentionLimited": bool(quality.get("fillRetentionLimited")),
                        "fillsError": quality.get("fillsError"), "fundingError": quality.get("fundingError")})
        if service.client.rate_limiter.throttle_report().get("events"):
            break  # Resume later, after the shared provider cooldown.
    payload = {"checkedAt": now_iso(), "wallets": results,
               "throttle": service.client.rate_limiter.throttle_report()}
    save_json_file(DATA_DIR / "quality_refresh_health.json", payload)
    print(json.dumps(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
