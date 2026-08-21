from __future__ import annotations

import json
import os
import sys
import urllib.error
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import importlib.util

from server import (
    ALERTS_FILE,
    DASHBOARD_SNAPSHOT_FILE,
    DASHBOARD_SNAPSHOT_MAX_AGE_SECONDS,
    HyperliquidClient,
    TELEGRAM_STATE_FILE,
    WalletStore,
    WalletTrackerService,
    WALLETS_FILE,
    current_time_ms,
    env_int,
    iso_to_ms,
    load_json_file,
    save_json_file,
)


# run_outcome_report is a script, not a package module, so it is loaded by
# path the same way the tests load it. Importing it here keeps one
# implementation of the report behind both the weekly job and /outcomes.
_OUTCOME_SPEC = importlib.util.spec_from_file_location(
    "run_outcome_report", ROOT / "scripts" / "run_outcome_report.py"
)
assert _OUTCOME_SPEC and _OUTCOME_SPEC.loader
outcome_report = importlib.util.module_from_spec(_OUTCOME_SPEC)
_OUTCOME_SPEC.loader.exec_module(outcome_report)

_TRADES_SPEC = importlib.util.spec_from_file_location(
    "build_trade_history", ROOT / "scripts" / "build_trade_history.py"
)
assert _TRADES_SPEC and _TRADES_SPEC.loader
trade_history = importlib.util.module_from_spec(_TRADES_SPEC)
_TRADES_SPEC.loader.exec_module(trade_history)

# How far back /trades looks when no window is given, and the ceiling: the
# venue clamps userFillsByTime to roughly 100 days of retention, so asking for
# more silently returns the same window.
TRADES_DEFAULT_DAYS = 7
TRADES_MAX_DAYS = 99

# Commands here trigger a live dashboard fetch before the reply is built.
# /outcomes reads stored outcomes only, so it stays out - listing it would
# make a report about past calls pay for a fresh snapshot it never reads.
LIVE_COMMANDS = {"/update", "/sentiment", "/consensus", "/signals", "/cmm", "/hip3", "/positions", "/ranks", "/elite"}
# Every command name, live or not: a ticker query must never swallow one.
# /trades fetches one wallet's fills directly, so it needs no dashboard.
KNOWN_COMMANDS = LIVE_COMMANDS | {"/moni", "/outcomes", "/trades", "/help"}
SUMMARY_COMMANDS = {"/update", "/sentiment", "/consensus", "/signals", "/hip3"}
CMM_COMMANDS = {"/signals", "/cmm"}
MONI_COMMANDS = {"/signals", "/moni"}


def normalize_command(text: str) -> str:
    message = (text or "").strip().split()
    if not message:
        return ""
    command = message[0].lower()
    if "@" in command:
        command = command.split("@", 1)[0]
    return command


def parse_position_wallet_query(text: str) -> tuple[str, str] | None:
    message = (text or "").strip().split()
    if len(message) < 2:
        return None
    command = message[0].strip()
    if not command.startswith("/"):
        return None
    if "@" in command:
        command = command.split("@", 1)[0]
    ticker = command[1:].strip()
    side = message[1].strip().lower()
    if not ticker or side not in {"long", "short"}:
        return None
    if f"/{ticker.lower()}" in KNOWN_COMMANDS:
        return None
    return ticker.upper(), side


def parse_trades_query(text: str, addresses: list[str]) -> tuple[str, int] | str | None:
    """Resolve "/trades <address or prefix> [days]".

    A full address is unusable on a phone, so a prefix is accepted and matched
    against the tracked list. An ambiguous or unknown prefix returns an
    explanatory string rather than silently picking one of the matches.
    """
    message = (text or "").strip().split()
    if not message or normalize_command(text) != "/trades":
        return None
    if len(message) < 2:
        return "Usage: /trades <address or prefix> [days]"
    needle = message[1].strip().lower()
    matches = [item for item in addresses if item.lower().startswith(needle)]
    if not matches:
        return f"No tracked wallet starts with {message[1].strip()}"
    if len(matches) > 1:
        listed = ", ".join(f"{item[:10]}…" for item in matches[:5])
        return f"{len(matches)} tracked wallets start with that; be more specific: {listed}"
    days = TRADES_DEFAULT_DAYS
    if len(message) > 2:
        try:
            days = int(message[2])
        except ValueError:
            return f"Not a number of days: {message[2]}"
        days = max(1, min(days, TRADES_MAX_DAYS))
    return matches[0], days


def build_help_message() -> str:
    return "\n".join(
        [
            "Hyperwatch Pro commands",
            "/update - live sentiment plus all open positions",
            "/sentiment - full live sentiment update",
            "/consensus - current consensus only",
            "/signals - high-conviction trade signals",
            "/cmm - CoinMarketMan cohort API signals",
            "/moni - cached Moni social context and quota",
            "/hip3 - current HIP-3 consensus only",
            "/positions - all open positions now",
            "/ranks - tracked wallets ranked by 7D hit rate plus 7D PnL",
            "/outcomes - what the last 7 days of calls actually returned",
            "/trades 0x350e 30 - one wallet's closed round trips, entry to exit",
            "/elite - open positions for Elite-ranked wallets",
            "/btc long - wallets currently long BTC",
            "/hype short - wallets currently short HYPE",
            "/help - show commands",
        ]
    )


def parse_update_id(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def normalize_update(update: dict[str, Any]) -> dict[str, Any] | None:
    message = update.get("message")
    if not isinstance(message, dict):
        edited_message = update.get("edited_message")
        if not isinstance(edited_message, dict):
            return None
        message = edited_message

    normalized = {"message": message}
    update_id = parse_update_id(update.get("update_id"))
    if update_id:
        normalized["update_id"] = update_id
    return normalized


def load_dispatch_updates() -> list[dict[str, Any]]:
    if os.environ.get("GITHUB_EVENT_NAME", "").strip() != "repository_dispatch":
        return []

    event_path = os.environ.get("GITHUB_EVENT_PATH", "").strip()
    if not event_path:
        return []

    try:
        event = json.loads(Path(event_path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return []

    if not isinstance(event, dict):
        return []
    client_payload = event.get("client_payload", {})
    if not isinstance(client_payload, dict):
        return []

    direct_update = client_payload.get("update")
    if isinstance(direct_update, dict):
        normalized = normalize_update(direct_update)
        return [normalized] if normalized else []

    text = str(client_payload.get("text", "")).strip()
    chat_id = str(client_payload.get("chat_id", "")).strip()
    if not text or not chat_id:
        return []

    update: dict[str, Any] = {
        "message": {
            "chat": {"id": chat_id},
            "text": text,
        }
    }
    update_id = parse_update_id(client_payload.get("update_id"))
    if update_id:
        update["update_id"] = update_id
    return [update]


def load_updates(service: WalletTrackerService, bot_token: str, last_update_id: int) -> tuple[list[dict[str, Any]], str]:
    dispatch_updates = load_dispatch_updates()
    if dispatch_updates:
        updates = [update for update in dispatch_updates if parse_update_id(update.get("update_id")) > last_update_id]
        return updates, "repository_dispatch"

    polling_backup_enabled = os.environ.get("TELEGRAM_POLLING_BACKUP", "").strip().lower() in {"1", "true", "yes", "on"}
    if not polling_backup_enabled:
        print("Telegram polling backup disabled; waiting for webhook dispatch.")
        return [], "polling_disabled"

    try:
        return service.fetch_telegram_updates(bot_token, offset=last_update_id + 1), "getUpdates"
    except urllib.error.HTTPError as exc:
        if exc.code == 409:
            print("Telegram webhook is active; skipping getUpdates polling.")
            return [], "getUpdates"
        raise


def build_reply(
    service: WalletTrackerService,
    command: str,
    position_query: tuple[str, str] | None,
    summary_cache: dict[str, Any] | None,
    dashboard_cache: dict[str, Any] | None,
    cmm_cache: dict[str, Any] | None,
    min_wallets: int,
    moni_cache: dict[str, Any] | None = None,
    trades_query: tuple[str, int] | str | None = None,
) -> str:
    if command == "/update":
        return "\n\n".join(
            [
                # /update is the routine digest, so it carries sentiment,
                # data health and open positions only. The crowding board is
                # a reference rather than a decision aid and stays available
                # on demand through /consensus.
                service.build_summary_message(
                    summary_cache,
                    min_wallets,
                    include_consensus=False,
                    include_signals=False,
                    include_footer=False,
                    include_data_health=False,
                ),
                service.build_positions_message(dashboard_cache),
            ]
        )
    if command == "/sentiment":
        return service.build_summary_message(summary_cache, min_wallets)
    if command == "/signals":
        if cmm_cache is not None and summary_cache is not None:
            summary_cache = service.apply_cmm_confirmation_to_summary(summary_cache, cmm_cache)
        if moni_cache is not None and summary_cache is not None:
            summary_cache = service.apply_moni_social_context(summary_cache, moni_cache)
        if summary_cache is not None and hasattr(service, "apply_signal_lifecycle") and hasattr(service, "alerts_path"):
            raw = load_json_file(service.alerts_path, {})
            alert_state = raw.get("state", {}) if isinstance(raw, dict) else {}
            previous_summary = alert_state.get("summary", {}) if isinstance(alert_state, dict) else {}
            summary_cache = service.apply_signal_lifecycle(
                summary_cache,
                previous_summary,
                now_ms=current_time_ms(),
            )
        return service.build_signals_message(summary_cache, cmm_summary=cmm_cache)
    if command == "/cmm":
        return service.build_cmm_signals_message(cmm_cache, wallet_summary=summary_cache)
    if command == "/moni":
        return service.build_moni_social_message(moni_cache)
    if command == "/consensus":
        return service.build_summary_message(
            summary_cache,
            min_wallets,
            title="Current consensus",
            include_consensus=True,
            include_hip3=False,
            include_signals=False,
        )
    if command == "/hip3":
        return service.build_summary_message(
            summary_cache,
            min_wallets,
            title="Current HIP-3 consensus",
            include_consensus=False,
            include_hip3=True,
            include_signals=False,
        )
    if command == "/positions":
        return service.build_positions_message(dashboard_cache)
    if command == "/ranks":
        return service.build_wallet_rankings_message(dashboard_cache, limit=20)
    if command == "/elite":
        return service.build_elite_wallet_positions_message(dashboard_cache)
    if command == "/outcomes":
        # The only message that reports what came of the bot's own calls; the
        # weekly job sends the same text on a timer.
        return outcome_report.build_message(
            outcome_report.build_payload(
                (load_json_file(ALERTS_FILE, {}) or {}).get("state", {}),
                now_ms=current_time_ms(),
            )
        )
    if command == "/trades":
        if isinstance(trades_query, str):
            return trades_query
        if not trades_query:
            return "Usage: /trades <address or prefix> [days]"
        address, days = trades_query
        start_ms = current_time_ms() - days * 24 * 60 * 60 * 1000
        result = service.fetch_fills_paginated_result(address, start_ms)
        if not result["ok"] and not result["data"]:
            return f"Could not read fills for {address[:10]}…: {result['error']}"
        reconstructed, checks = trade_history.reconstruct(result["data"])
        message = trade_history.build_message(address, reconstructed, checks, days=days)
        if result.get("truncated"):
            message += f"\n\n⚠ history truncated after {result.get('pages')} pages; older trades missing"
        return message
    if position_query:
        coin, side = position_query
        return service.build_position_wallets_message(dashboard_cache, coin, side)
    return build_help_message()


def dashboard_snapshot_max_age_ms() -> int:
    return max(0, env_int("DASHBOARD_SNAPSHOT_MAX_AGE_SECONDS", DASHBOARD_SNAPSHOT_MAX_AGE_SECONDS)) * 1000


def load_dashboard_snapshot(now_ms: int | None = None) -> dict[str, Any] | None:
    """Return the dashboard the sentiment cycle already built, when fresh enough.

    The Hyperliquid rate limiter is a per-process singleton, so an on-demand
    rebuild that overlaps the 5-minute sentiment sweep makes both processes
    exceed the real budget and silently lose fill data. Returns None when the
    snapshot is missing, unreadable, malformed or stale, and the caller then
    falls back to a full rebuild.
    """
    snapshot = load_json_file(DASHBOARD_SNAPSHOT_FILE, None)
    if not isinstance(snapshot, dict):
        return None
    if not isinstance(snapshot.get("wallets"), list):
        return None
    generated_ms = iso_to_ms(snapshot.get("generatedAt"))
    if generated_ms <= 0:
        return None
    age_ms = (current_time_ms() if now_ms is None else now_ms) - generated_ms
    if age_ms < 0 or age_ms > dashboard_snapshot_max_age_ms():
        return None
    return snapshot


def resolve_dashboard(service: WalletTrackerService) -> dict[str, Any]:
    snapshot = load_dashboard_snapshot()
    if snapshot is not None:
        print(f'Reusing dashboard snapshot built at {snapshot.get("generatedAt", "")}.')
        return snapshot
    return service.dashboard()


def build_summary_cache(
    service: WalletTrackerService,
    dashboard: dict[str, Any],
    min_wallets: int,
) -> dict[str, Any]:
    if hasattr(service, "build_monthly_sentiment_summary") and hasattr(service, "alerts_path"):
        raw = load_json_file(service.alerts_path, {})
        stored_config = raw.get("config", {}) if isinstance(raw, dict) else {}
        state = raw.get("state", {}) if isinstance(raw, dict) else {}
        summary, _cohort = service.build_monthly_sentiment_summary(
            dashboard,
            min_wallets,
            state,
            persist=True,
            stored_config=stored_config,
        )
    else:
        summary = service.build_sentiment_summary(dashboard["wallets"], min_wallets)
    # The summary is computed now but describes the dashboard's data, so the
    # "Updated:" line has to report when that dashboard was built, not when the
    # command happened to be answered.
    generated_at = str(dashboard.get("generatedAt") or "")
    if generated_at and isinstance(summary, dict):
        summary = {**summary, "generatedAt": generated_at}
    return summary


def build_cmm_cache(service: WalletTrackerService) -> dict[str, Any]:
    raw = load_json_file(service.alerts_path, {}) if hasattr(service, "alerts_path") else {}
    state = raw.get("state", {}) if isinstance(raw, dict) else {}
    return service.build_cached_cmm_signal_summary(state)


def build_moni_cache(service: WalletTrackerService) -> dict[str, Any]:
    raw = load_json_file(service.alerts_path, {}) if hasattr(service, "alerts_path") else {}
    state = raw.get("state", {}) if isinstance(raw, dict) else {}
    summary = state.get("moniSocial", {}) if isinstance(state, dict) else {}
    return summary if isinstance(summary, dict) else {}


def main() -> int:
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    allowed_chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    min_wallets = max(1, int(os.environ.get("MIN_CONSENSUS_WALLETS", "4")))

    if not bot_token or not allowed_chat_id:
        print("Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID")
        return 1

    service = WalletTrackerService(WalletStore(WALLETS_FILE), HyperliquidClient())
    state = load_json_file(TELEGRAM_STATE_FILE, {})
    last_update_id = int(state.get("lastUpdateId", 0)) if isinstance(state, dict) else 0
    updates, source = load_updates(service, bot_token, last_update_id)
    latest_seen = last_update_id
    summary_cache = None
    dashboard_cache = None

    for update in updates:
        update_id = parse_update_id(update.get("update_id"))

        message = update.get("message", {})
        if not isinstance(message, dict):
            latest_seen = max(latest_seen, update_id)
            continue

        chat = message.get("chat", {})
        chat_id = str(chat.get("id", "")).strip()
        if chat_id != allowed_chat_id:
            latest_seen = max(latest_seen, update_id)
            continue

        message_text = str(message.get("text", ""))
        command = normalize_command(message_text)
        if not command:
            latest_seen = max(latest_seen, update_id)
            continue

        position_query = parse_position_wallet_query(message_text)
        trades_query = None
        if command == "/trades":
            trades_query = parse_trades_query(
                message_text, [wallet.address for wallet in service.store.list_wallets()]
            )
        if command in LIVE_COMMANDS or position_query:
            if dashboard_cache is None:
                dashboard_cache = resolve_dashboard(service)
            if (command in SUMMARY_COMMANDS or command in CMM_COMMANDS) and summary_cache is None:
                summary_cache = build_summary_cache(service, dashboard_cache, min_wallets)

        cmm_cache = None
        if command in CMM_COMMANDS:
            cmm_cache = build_cmm_cache(service)

        moni_cache = build_moni_cache(service) if command in MONI_COMMANDS else None
        reply = build_reply(
            service,
            command,
            position_query,
            summary_cache,
            dashboard_cache,
            cmm_cache,
            min_wallets,
            moni_cache=moni_cache,
            trades_query=trades_query,
        )

        service.send_telegram_message(bot_token, chat_id, reply)
        latest_seen = max(latest_seen, update_id)
        if latest_seen != last_update_id:
            save_json_file(TELEGRAM_STATE_FILE, {"lastUpdateId": latest_seen})
            last_update_id = latest_seen

    if latest_seen != last_update_id:
        save_json_file(TELEGRAM_STATE_FILE, {"lastUpdateId": latest_seen})

    print(f"Processed {len(updates)} Telegram updates via {source}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
