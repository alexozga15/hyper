from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server import HyperliquidClient, WalletStore, WalletTrackerService, WALLETS_FILE


def main() -> int:
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

    if not bot_token or not chat_id:
        print("Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID")
        return 1

    service = WalletTrackerService(WalletStore(WALLETS_FILE), HyperliquidClient())
    service.send_telegram_message(
        bot_token,
        chat_id,
        "Hyperwatch Pro test message\nTelegram delivery is working.",
    )
    print("Test Telegram message sent.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
