from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server import HyperliquidClient, WalletStore, WalletTrackerService, WALLETS_FILE


def env_flag(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def main() -> int:
    service = WalletTrackerService(WalletStore(WALLETS_FILE), HyperliquidClient())
    min_wallets = int(os.environ.get("MIN_CONSENSUS_WALLETS", "3"))
    send_hourly_update = env_flag("SEND_HOURLY_UPDATE", False)
    send_change_alerts = env_flag("SEND_CHANGE_ALERTS", not send_hourly_update)
    service.update_alert_settings(
        {
            "enabled": env_flag("ALERTS_ENABLED", True),
            "minConsensusWallets": min_wallets,
            "trackHip3": env_flag("TRACK_HIP3", True),
        }
    )

    results: dict[str, object] = {}

    if send_hourly_update:
        bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
        chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
        if not bot_token or not chat_id:
            print("Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID")
            return 1
        results["hourlyUpdate"] = service.send_hourly_update(min_wallets, bot_token, chat_id)

    if send_change_alerts:
        results["changeAlert"] = service.check_alerts(send_notification=True)

    if not results:
        results["changeAlert"] = service.check_alerts(send_notification=True)

    print(json.dumps(results, indent=2))
    has_error = any(isinstance(item, dict) and item.get("error") for item in results.values())
    return 1 if has_error else 0


if __name__ == "__main__":
    sys.exit(main())
