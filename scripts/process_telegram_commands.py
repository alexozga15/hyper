from __future__ import annotations

import os
import sys
from pathlib import Path

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
            "/help - show commands",
        ]
    )


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
    updates = service.fetch_telegram_updates(bot_token, offset=last_update_id + 1)
    latest_seen = last_update_id
    summary_cache = None
    dashboard_cache = None

    for update in updates:
        update_id = int(update.get("update_id", 0))
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

        if command in {"/update", "/sentiment", "/consensus", "/hip3", "/positions"}:
            if dashboard_cache is None:
                dashboard_cache = service.dashboard()
            if summary_cache is None:
                summary_cache = service.build_sentiment_summary(dashboard_cache["wallets"], min_wallets)

        if command == "/update":
            reply = "\n\n".join(
                [
                    service.build_summary_message(summary_cache, min_wallets),
                    service.build_positions_message(dashboard_cache),
                ]
            )
        elif command == "/sentiment":
            reply = service.build_summary_message(summary_cache, min_wallets)
        elif command == "/consensus":
            reply = service.build_summary_message(
                summary_cache,
                min_wallets,
                title="Current consensus",
                include_consensus=True,
                include_hip3=False,
            )
        elif command == "/hip3":
            reply = service.build_summary_message(
                summary_cache,
                min_wallets,
                title="Current HIP-3 consensus",
                include_consensus=False,
                include_hip3=True,
            )
        elif command == "/positions":
            reply = service.build_positions_message(dashboard_cache)
        else:
            reply = build_help_message()

        service.send_telegram_message(bot_token, chat_id, reply)

    if latest_seen != last_update_id:
        save_json_file(TELEGRAM_STATE_FILE, {"lastUpdateId": latest_seen})

    print(f"Processed {len(updates)} Telegram updates.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
