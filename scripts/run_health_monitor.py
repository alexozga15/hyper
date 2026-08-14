from __future__ import annotations

import json
import os
import shutil
import subprocess
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
    to_float,
)

MONITOR_STATE_FILE = DATA_DIR / "health_monitor_state.json"

# A silent pipeline and a pipeline with nothing to say look identical from the
# outside, so the two are checked at different horizons. Shadow records are
# written whenever consensus moves at all, so a full day without one means the
# machinery broke. Published and candidate signals are gated much harder, so a
# drought there is about the gates being too tight, not about breakage.
SIGNAL_PIPELINE_SILENCE_HOURS = float(os.environ.get("SIGNAL_PIPELINE_SILENCE_HOURS", "24"))
PUBLISHED_SIGNAL_DROUGHT_DAYS = float(os.environ.get("PUBLISHED_SIGNAL_DROUGHT_DAYS", "7"))


def latest_signal_start_ms(records: Any) -> int:
    """Newest `startedAt` in a signal-outcome record map, or 0 when there is none."""
    if not isinstance(records, dict):
        return 0
    newest = 0
    for record in records.values():
        if not isinstance(record, dict):
            continue
        started_at = int(to_float(record.get("startedAt")))
        if started_at > newest:
            newest = started_at
    return newest


def detect_health_issues(
    alerts: dict[str, Any],
    runtime: dict[str, Any],
    *,
    now_ms: int,
    disk_free_pct: float,
) -> list[str]:
    issues: list[str] = []
    state = alerts.get("state", {})
    if not isinstance(state, dict):
        state = {}
    last_checked = iso_to_ms(state.get("lastCheckedAt"))
    if last_checked <= 0 or now_ms - last_checked > 10 * 60 * 1000:
        issues.append("sentiment check stale >10m")
    if float(runtime.get("cacheCoverage", 0)) < 0.8:
        issues.append("wallet quality cache coverage <80%")
    if runtime.get("fillsGloballyDegraded"):
        issues.append("wallet fills globally degraded")
    if disk_free_pct < 10:
        issues.append("disk free space <10%")
    issues.extend(detect_signal_drought(state, now_ms=now_ms))
    return issues


def dirty_checkout_paths(root: Path) -> list[str]:
    """Paths that make `git pull --ff-only` refuse to update this checkout.

    Each unit self-updates with `ExecStartPre=-... git pull --ff-only`, whose
    leading `-` swallows the failure, so a dirty tree freezes deploys in
    silence. Returns an empty list when the checkout is clean, when git is
    unavailable, or when this is not a checkout at all.
    """
    try:
        result = subprocess.run(
            ["git", "-C", str(root), "status", "--porcelain", "--untracked-files=no"],
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return []
    if result.returncode != 0:
        return []
    return [line[3:] for line in result.stdout.splitlines() if line[3:].strip()]


def detect_signal_drought(state: dict[str, Any], *, now_ms: int) -> list[str]:
    """Flag a signal pipeline that has gone quiet.

    Only meaningful once the sentiment check has run at least once - a fresh
    install has no records yet and must not alert on that.
    """
    if iso_to_ms(state.get("lastCheckedAt")) <= 0:
        return []
    issues: list[str] = []
    published_at = max(
        latest_signal_start_ms(state.get("signalOutcomes")),
        latest_signal_start_ms(state.get("candidateSignalOutcomes")),
    )
    pipeline_at = max(published_at, latest_signal_start_ms(state.get("shadowSignalOutcomes")))

    silence_ms = int(SIGNAL_PIPELINE_SILENCE_HOURS * 60 * 60 * 1000)
    if pipeline_at <= 0 or now_ms - pipeline_at > silence_ms:
        issues.append(f"no signal of any tier in >{SIGNAL_PIPELINE_SILENCE_HOURS:g}h")

    drought_ms = int(PUBLISHED_SIGNAL_DROUGHT_DAYS * 24 * 60 * 60 * 1000)
    if published_at <= 0 or now_ms - published_at > drought_ms:
        issues.append(f"no published or candidate signal in >{PUBLISHED_SIGNAL_DROUGHT_DAYS:g}d")
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

    dirty = dirty_checkout_paths(ROOT)
    if dirty:
        issues.append("deploy checkout dirty, ff-only pull blocked: " + ", ".join(sorted(dirty)[:5]))

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
    # A detected issue is a finding, not a failure of this script: exiting
    # non-zero made systemd mark the unit failed on every unhealthy run, so a
    # real crash was indistinguishable from a working monitor doing its job.
    # Issues travel by Telegram and health_monitor_state.json; an unhandled
    # exception is what should ever make this exit non-zero.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
