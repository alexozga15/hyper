from __future__ import annotations

import json
import os
import shutil
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server import (
    ALERTS_FILE,
    DATA_DIR,
    RUNTIME_HEALTH_FILE,
    HyperliquidClient,
    WalletStore,
    WalletTrackerService,
    WALLETS_FILE,
    current_time_ms,
    iso_to_ms,
    load_json_file,
    now_iso,
    save_json_file,
)

MONITOR_STATE_FILE = DATA_DIR / "health_monitor_state.json"


def detect_health_issues(
    alerts: dict[str, Any],
    runtime: dict[str, Any],
    *,
    now_ms: int,
    disk_free_pct: float,
) -> list[str]:
    issues: list[str] = []
    last_checked = iso_to_ms(alerts.get("state", {}).get("lastCheckedAt"))
    if last_checked <= 0 or now_ms - last_checked > 10 * 60 * 1000:
        issues.append("sentiment check stale >10m")
    if float(runtime.get("cacheCoverage", 0)) < 0.8:
        issues.append("wallet quality cache coverage <80%")
    if runtime.get("fillsGloballyDegraded"):
        issues.append("wallet fills globally degraded")
    if disk_free_pct < 10:
        issues.append("disk free space <10%")
    return issues


def main() -> int:
    now_ms = current_time_ms()
    alerts = load_json_file(ALERTS_FILE, {})
    runtime = load_json_file(RUNTIME_HEALTH_FILE, {})
    disk = shutil.disk_usage(DATA_DIR)
    free_pct = disk.free / max(disk.total, 1) * 100
    issues = detect_health_issues(alerts, runtime, now_ms=now_ms, disk_free_pct=free_pct)
    prior = load_json_file(MONITOR_STATE_FILE, {})
    first_seen = dict(prior.get("firstSeen", {})) if isinstance(prior.get("firstSeen"), dict) else {}

    if int(runtime.get("rateLimitedWallets", 0)) > 0:
        first_seen.setdefault("rate_limit", now_ms)
        if now_ms - int(first_seen["rate_limit"]) > 30 * 60 * 1000:
            issues.append("Hyperliquid HTTP 429 persisted >30m")
    else:
        first_seen.pop("rate_limit", None)

    prior_issues = list(prior.get("issues", [])) if isinstance(prior, dict) else []
    changed = issues != prior_issues
    payload = {
        "checkedAt": now_iso(),
        "healthy": not issues,
        "issues": issues,
        "firstSeen": first_seen,
        "diskFreePct": round(free_pct, 1),
    }
    save_json_file(MONITOR_STATE_FILE, payload)

    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    if changed and bot_token and chat_id:
        service = WalletTrackerService(WalletStore(WALLETS_FILE), HyperliquidClient())
        if issues:
            service.send_telegram_message(bot_token, chat_id, "Wallet monitor alert\n- " + "\n- ".join(issues))
        elif prior_issues:
            service.send_telegram_message(bot_token, chat_id, "Wallet monitor recovered")
    print(json.dumps(payload, indent=2))
    return 1 if issues else 0


if __name__ == "__main__":
    raise SystemExit(main())
