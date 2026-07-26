from __future__ import annotations

import json
import os
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server import HyperliquidClient, WalletStore, WalletTrackerService, WALLETS_FILE


def env_flag(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def env_hour(name: str, default: int) -> int:
    try:
        return max(0, min(int(os.environ.get(name, str(default))), 23))
    except (TypeError, ValueError):
        return default


def is_quiet_hours(now: datetime | None = None) -> bool:
    if not env_flag("QUIET_HOURS_ENABLED", True):
        return False
    timezone_name = os.environ.get("QUIET_HOURS_TIMEZONE", "Europe/Warsaw").strip() or "Europe/Warsaw"
    try:
        timezone = ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError:
        timezone = ZoneInfo("UTC")
    local_now = now.astimezone(timezone) if now is not None else datetime.now(timezone)
    start_hour = env_hour("QUIET_HOURS_START", 23)
    end_hour = env_hour("QUIET_HOURS_END", 7)
    if start_hour == end_hour:
        return False
    if start_hour < end_hour:
        return start_hour <= local_now.hour < end_hour
    return local_now.hour >= start_hour or local_now.hour < end_hour


def main() -> int:
    service = WalletTrackerService(WalletStore(WALLETS_FILE), HyperliquidClient())
    min_wallets = int(os.environ.get("MIN_CONSENSUS_WALLETS", "4"))
    send_hourly_update = env_flag("SEND_HOURLY_UPDATE", False)
    send_change_alerts = env_flag("SEND_CHANGE_ALERTS", not send_hourly_update)
    quiet_hours = is_quiet_hours()
    service.update_alert_settings(
        {
            "enabled": env_flag("ALERTS_ENABLED", True),
            "minConsensusWallets": min_wallets,
            "trackHip3": env_flag("TRACK_HIP3", True),
        }
    )

    results: dict[str, object] = {}

    if quiet_hours:
        results["quietHours"] = {
            "active": True,
            "timezone": os.environ.get("QUIET_HOURS_TIMEZONE", "Europe/Warsaw"),
            "startHour": env_hour("QUIET_HOURS_START", 23),
            "endHour": env_hour("QUIET_HOURS_END", 7),
        }
        results["stateRefresh"] = service.check_alerts(
            send_notification=False,
            acknowledge_suppressed=True,
        )
        print(json.dumps(results, indent=2))
        has_error = any(isinstance(item, dict) and item.get("error") for item in results.values())
        return 1 if has_error else 0

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
