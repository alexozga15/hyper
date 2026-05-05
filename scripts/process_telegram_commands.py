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

from server import (
    HyperliquidClient,
    TELEGRAM_STATE_FILE,
    WalletStore,
    WalletTrackerService,
    WALLETS_FILE,
    load_json_file,
    save_json_file,
)


LIVE_COMMANDS = {"/update", "/sentiment", "/consensus", "/hip3", "/positions", "/ranks"}


def normalize_command(text: str) -> str:
    message = (text or "").strip().split()
    if not message:
        return ""
    command = message[0].lower()
    if "@" in command:
        command = command.split("@", 1)[0]
    return command


def build_help_message() -> str:
    return "\n".join(
        [
            "Hyperwatch Pro commands",
            "/update - live sentiment plus all open positions",
            "/sentiment - full live sentiment update",
            "/consensus - current consensus only",
            "/hip3 - current HIP-3 consensus only",
            "/positions - all open positions now",
            "/ranks - tracked wallets ranked by 7D hit rate plus 7D PnL",
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
    summary_cache: dict[str, Any] | None,
    dashboard_cache: dict[str, Any] | None,
    min_wallets: int,
) -> str:
    if command == "/update":
        return "\n\n".join(
            [
                service.build_summary_message(summary_cache, min_wallets),
                service.build_wallet_rankings_message(dashboard_cache, limit=10),
                service.build_positions_message(dashboard_cache),
            ]
        )
    if command == "/sentiment":
        return service.build_summary_message(summary_cache, min_wallets)
    if command == "/consensus":
        return service.build_summary_message(
            summary_cache,
            min_wallets,
            title="Current consensus",
            include_consensus=True,
            include_hip3=False,
        )
    if command == "/hip3":
        return service.build_summary_message(
            summary_cache,
            min_wallets,
            title="Current HIP-3 consensus",
            include_consensus=False,
            include_hip3=True,
        )
    if command == "/positions":
        return service.build_positions_message(dashboard_cache)
    if command == "/ranks":
        return service.build_wallet_rankings_message(dashboard_cache, limit=20)
    return build_help_message()


def main() -> int:
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    allowed_chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    min_wallets = max(1, int(os.environ.get("MIN_CONSENSUS_WALLETS", "3")))

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
        latest_seen = max(latest_seen, update_id)

        message = update.get("message", {})
        if not isinstance(message, dict):
            continue

        chat = message.get("chat", {})
        chat_id = str(chat.get("id", "")).strip()
        if chat_id != allowed_chat_id:
            continue

        command = normalize_command(str(message.get("text", "")))
        if not command:
            continue

        if command in LIVE_COMMANDS:
            if dashboard_cache is None:
                dashboard_cache = service.dashboard()
            if summary_cache is None:
                summary_cache = service.build_sentiment_summary(dashboard_cache["wallets"], min_wallets)

        reply = build_reply(service, command, summary_cache, dashboard_cache, min_wallets)

        service.send_telegram_message(bot_token, chat_id, reply)

    if latest_seen != last_update_id:
        save_json_file(TELEGRAM_STATE_FILE, {"lastUpdateId": latest_seen})

    print(f"Processed {len(updates)} Telegram updates via {source}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
