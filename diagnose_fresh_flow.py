#!/usr/bin/env python3
"""Check the data assumptions behind fresh wallet-flow signals.

Run from the repository root with ``python3 diagnose_fresh_flow.py``. It reads
the configured tracked wallets but never writes state or sends Telegram output.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from server import (  # noqa: E402
    HyperliquidClient,
    RANKING_WINDOW_MS,
    RECENT_FILL_ALERT_LIMIT,
    WALLET_LIVE_FILL_LOOKBACK_MS,
    WALLET_RECENT_FILL_CONSUMER_WINDOW_MS,
    WalletStore,
    WalletTrackerService,
    WALLETS_FILE,
    to_float,
)


def main(sample: int = 8, days: int = 30) -> int:
    service = WalletTrackerService(WalletStore(WALLETS_FILE), HyperliquidClient())
    now_ms = int(time.time() * 1000)
    start_ms = now_ms - days * 24 * 60 * 60 * 1000
    print(
        f"Config: retain {RECENT_FILL_ALERT_LIMIT} newest fills; "
        f"live lookback {WALLET_LIVE_FILL_LOOKBACK_MS / 86400000:.1f}d; "
        f"consumer window {WALLET_RECENT_FILL_CONSUMER_WINDOW_MS / 86400000:.1f}d"
    )
    if WALLET_LIVE_FILL_LOOKBACK_MS < WALLET_RECENT_FILL_CONSUMER_WINDOW_MS:
        print("FAIL: live lookback is narrower than a freshness consumer window.")
        return 1

    checked = 0
    newest_kept = 0
    for wallet in service.store.list_wallets()[:sample]:
        result = service.fetch_fills_result(wallet.address, start_ms)
        if not result["ok"]:
            print(f"{wallet.address[:10]}... error: {result['error']}")
            continue
        fills = [fill for fill in result["data"] if isinstance(fill, dict)]
        if not fills:
            print(f"{wallet.address[:10]}... no fills in {days}d")
            continue
        checked += 1
        actual_newest = max(int(to_float(fill.get("time"))) for fill in fills)
        retained = sorted(fills, key=lambda fill: int(to_float(fill.get("time"))), reverse=True)[:RECENT_FILL_ALERT_LIMIT]
        retained_newest = max(int(to_float(fill.get("time"))) for fill in retained)
        is_current = actual_newest == retained_newest
        newest_kept += int(is_current)
        age_minutes = (now_ms - actual_newest) / 60000
        print(f"{wallet.address[:10]}... {len(fills):4} fills | newest age {age_minutes:.0f}m | retained newest: {'OK' if is_current else 'FAIL'}")

    print(f"Summary: newest fill retained for {newest_kept}/{checked} sampled wallets.")
    print(f"7d ranking window: {RANKING_WINDOW_MS / 86400000:.1f}d.")
    return 0 if newest_kept == checked else 1


if __name__ == "__main__":
    raise SystemExit(main())
