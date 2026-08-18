from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
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

# Must stay comfortably above the sentiment timer's interval, or a single
# late run reads as an outage. The timer runs every 10 minutes, so this
# tolerates two consecutive misses before complaining.
SENTIMENT_STALE_MINUTES = float(os.environ.get("SENTIMENT_STALE_MINUTES", "25"))

# HTTP 429s are background noise on this box, not a signal of anything
# degrading: 118 sentiment cycles overnight produced 0 errors and only 1
# failed fill fetch. The alert on it oscillated between
# ["untrusted_quality_window"] and [..., "http_429"] and re-sent repeatedly
# (23:15, 00:26, 05:00, 06:01, 06:41), so it has been retired in favor of the
# outcome-based `fill_fetch_failing` check below, which tracks what actually
# degrades rather than a proxy for it.

# The fraction of tracked wallets whose fill fetch must be failing before it
# is treated as a real degradation rather than the ordinary background rate
# (measured: 1 failed fill fetch across 118 overnight cycles).
FILL_FETCH_FAIL_ALERT_FRACTION = float(os.environ.get("FILL_FETCH_FAIL_ALERT_FRACTION", "0.25"))
# Consecutive breaching (or clearing) checks required before the condition
# raises (or clears), giving the same symmetric hysteresis the retired
# rate-limit alert needed: a count hovering at the threshold must not
# oscillate the alert on and off every run.
FILL_FETCH_FAIL_CONFIRM_CHECKS = int(os.environ.get("FILL_FETCH_FAIL_CONFIRM_CHECKS", "2"))

# The standing count of wallets with an untrusted quality window on this box
# (2-4) is permanent background noise, not a problem - alerting on any
# nonzero count kept the monitor permanently unhealthy. Fire only once a
# meaningful fraction of tracked wallets are affected; the max(3, ...) floor
# keeps the check meaningful when walletsTracked is missing or small. With 37
# tracked wallets the threshold is 7, so the standing count of 2 goes quiet.
UNTRUSTED_QUALITY_ALERT_FRACTION = float(os.environ.get("UNTRUSTED_QUALITY_ALERT_FRACTION", "0.2"))


@dataclass(frozen=True)
class Issue:
    """A detected health problem.

    `key` is a stable identity for the *condition* (e.g. "cmm_rate_limited")
    used to decide whether anything actually changed since the last check.
    `text` is the human-readable line sent to Telegram and stored for
    display; it may embed numbers (counts, hours remaining) that drift on
    their own without the underlying condition being new.
    """

    key: str
    text: str


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
) -> list[Issue]:
    issues: list[Issue] = []
    state = alerts.get("state", {})
    if not isinstance(state, dict):
        state = {}
    last_checked = iso_to_ms(state.get("lastCheckedAt"))
    if last_checked <= 0 or now_ms - last_checked > SENTIMENT_STALE_MINUTES * 60 * 1000:
        issues.append(Issue("sentiment_stale", f"sentiment check stale >{SENTIMENT_STALE_MINUTES:g}m"))
    if float(runtime.get("cacheCoverage", 0)) < 0.8:
        issues.append(Issue("quality_cache_coverage", "wallet quality cache coverage <80%"))
    if runtime.get("fillsGloballyDegraded"):
        issues.append(Issue("fills_degraded", "wallet fills globally degraded"))
    if disk_free_pct < 10:
        issues.append(Issue("disk_low", "disk free space <10%"))
    untrusted_quality_window_wallets = int(runtime.get("untrustedQualityWindowWallets", 0))
    # Fire on a systemic breakdown, not the standing background count (see
    # UNTRUSTED_QUALITY_ALERT_FRACTION above for the measurement behind this).
    untrusted_quality_threshold = max(
        3, int(UNTRUSTED_QUALITY_ALERT_FRACTION * int(runtime.get("walletsTracked", 0)))
    )
    if untrusted_quality_window_wallets >= untrusted_quality_threshold:
        issues.append(
            Issue(
                "untrusted_quality_window",
                f"{untrusted_quality_window_wallets} wallets with untrusted quality window",
            )
        )
    issues.extend(detect_signal_drought(state, now_ms=now_ms))
    issues.extend(detect_external_api_backoff(state, now_ms=now_ms))
    return issues


def detect_external_api_backoff(state: dict[str, Any], *, now_ms: int) -> list[Issue]:
    """Flag CoinMarketMan or Moni sitting out a self-imposed backoff.

    Both back off for hours after a rate limit or an error, and both keep
    answering from cache while they do, so the pipeline looks healthy from
    every other angle. Only these two fields say that the signals being
    published are being confirmed against hours-old external data.
    """
    issues: list[Issue] = []
    cmm = state.get("cmmSignals")
    if isinstance(cmm, dict):
        until_ms = iso_to_ms(cmm.get("rateLimitedUntil"))
        if until_ms > now_ms:
            issues.append(
                Issue("cmm_rate_limited", f"CMM rate limited for another {format_hours(until_ms - now_ms)}")
            )

    moni = state.get("moniSocial")
    # nextFetchAt is also set on success as an ordinary cache TTL, so only an
    # error alongside it means the wait is a penalty rather than a schedule.
    if isinstance(moni, dict) and str(moni.get("error") or "").strip():
        until_ms = iso_to_ms(moni.get("nextFetchAt"))
        if until_ms > now_ms:
            issues.append(
                Issue("moni_backoff", f"Moni backed off for another {format_hours(until_ms - now_ms)}")
            )
    return issues


def format_hours(duration_ms: int) -> str:
    hours = duration_ms / (60 * 60 * 1000)
    return f"{hours:.1f}h" if hours >= 1 else f"{max(1, round(duration_ms / 60000))}m"


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


def detect_signal_drought(state: dict[str, Any], *, now_ms: int) -> list[Issue]:
    """Flag a signal pipeline that has gone quiet.

    Only meaningful once the sentiment check has run at least once - a fresh
    install has no records yet and must not alert on that.
    """
    if iso_to_ms(state.get("lastCheckedAt")) <= 0:
        return []
    issues: list[Issue] = []
    published_at = max(
        latest_signal_start_ms(state.get("signalOutcomes")),
        latest_signal_start_ms(state.get("candidateSignalOutcomes")),
    )
    pipeline_at = max(published_at, latest_signal_start_ms(state.get("shadowSignalOutcomes")))

    silence_ms = int(SIGNAL_PIPELINE_SILENCE_HOURS * 60 * 60 * 1000)
    if pipeline_at <= 0 or now_ms - pipeline_at > silence_ms:
        issues.append(
            Issue("signal_pipeline_silent", f"no signal of any tier in >{SIGNAL_PIPELINE_SILENCE_HOURS:g}h")
        )

    drought_ms = int(PUBLISHED_SIGNAL_DROUGHT_DAYS * 24 * 60 * 60 * 1000)
    if published_at <= 0 or now_ms - published_at > drought_ms:
        issues.append(
            Issue(
                "signal_drought_published",
                f"no published or candidate signal in >{PUBLISHED_SIGNAL_DROUGHT_DAYS:g}d",
            )
        )
    return issues


def main() -> int:
    now_ms = current_time_ms()
    alerts = load_json_file(ALERTS_FILE, {})
    runtime = load_json_file(RUNTIME_HEALTH_FILE, {})
    disk = shutil.disk_usage(DATA_DIR)
    free_pct = disk.free / max(disk.total, 1) * 100
    issues = detect_health_issues(alerts, runtime, now_ms=now_ms, disk_free_pct=free_pct)
    prior = load_json_file(MONITOR_STATE_FILE, {})

    # State written by the currently deployed version has no such key; that
    # is correctly read as 0 - the first breaching check after deploy starts
    # a fresh streak rather than raising immediately, which is what "unknown
    # prior state" should mean for a counter that has never been anything
    # but zero.
    fill_fetch_fail_streak = int(prior.get("fillFetchFailStreak", 0)) if isinstance(prior, dict) else 0

    # walletsTracked absent or 0 means a fresh install or an unreadable
    # runtime file, not a degradation - never treat that as a breach, and
    # never divide by it.
    wallets_tracked = int(runtime.get("walletsTracked", 0))
    fills_fetch_failed_wallets = int(runtime.get("fillsFetchFailedWallets", 0))
    fill_fetch_breach = (
        wallets_tracked > 0
        and fills_fetch_failed_wallets >= FILL_FETCH_FAIL_ALERT_FRACTION * wallets_tracked
    )
    # The cap is set above the raise threshold, not equal to it: a cap equal
    # to the threshold means a raised alert's streak can only ever be sitting
    # exactly at the threshold, so a single clean check always drops it below
    # and clears the alert in one step - no real hysteresis at all. Capping
    # at 2N-1 gives the streak headroom to saturate above the threshold
    # during a sustained run of breaches, so clearing a saturated alert takes
    # the same N consecutive clean checks it took to raise it. This is the
    # anti-oscillation property the whole counter exists for: an input that
    # alternates breach/clean every check keeps the streak hovering around
    # the threshold (2-3 for the N=2 default) instead of never reaching it or
    # instantly dropping out of it, so the alert stays steadily raised
    # instead of toggling on and off every run.
    if fill_fetch_breach:
        fill_fetch_fail_streak = min(fill_fetch_fail_streak + 1, 2 * FILL_FETCH_FAIL_CONFIRM_CHECKS - 1)
    else:
        fill_fetch_fail_streak = max(fill_fetch_fail_streak - 1, 0)

    if fill_fetch_fail_streak >= FILL_FETCH_FAIL_CONFIRM_CHECKS:
        issues.append(
            Issue(
                "fill_fetch_failing",
                f"{fills_fetch_failed_wallets} of {wallets_tracked} wallets failed fill fetch",
            )
        )

    dirty = dirty_checkout_paths(ROOT)
    if dirty:
        issues.append(
            Issue(
                "dirty_checkout",
                "deploy checkout dirty, ff-only pull blocked: " + ", ".join(sorted(dirty)[:5]),
            )
        )

    # Dedupe on the stable keys, not the rendered text: several issues embed a
    # number that drifts on its own (a wallet count draining, a backoff timer
    # counting down), and re-notifying on every such drift is the bug this
    # guards against. `issueKeys` is new; state written by the prior version
    # of this script has only `issues` (rendered strings), so the very first
    # run after deploying this change reads `issueKeys` as absent, treats it
    # as "unknown prior state", and sends one message. That is expected and
    # self-correcting - every run after that compares keys against keys.
    prior_issues = list(prior.get("issues", [])) if isinstance(prior, dict) else []
    raw_prior_keys = prior.get("issueKeys") if isinstance(prior, dict) else None
    prior_issue_keys = raw_prior_keys if isinstance(raw_prior_keys, list) else None
    issue_keys = [issue.key for issue in issues]
    changed = prior_issue_keys is None or issue_keys != prior_issue_keys
    payload = {
        "checkedAt": now_iso(),
        "healthy": not issues,
        "issues": [issue.text for issue in issues],
        "issueKeys": issue_keys,
        "fillFetchFailStreak": fill_fetch_fail_streak,
        "diskFreePct": round(free_pct, 1),
    }
    save_json_file(MONITOR_STATE_FILE, payload)

    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    if changed and bot_token and chat_id:
        service = WalletTrackerService(WalletStore(WALLETS_FILE), HyperliquidClient())
        if issues:
            texts = [issue.text for issue in issues]
            service.send_telegram_message(bot_token, chat_id, "Wallet monitor alert\n- " + "\n- ".join(texts))
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
