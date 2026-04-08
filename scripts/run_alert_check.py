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
    service.update_alert_settings(
        {
            "enabled": env_flag("ALERTS_ENABLED", True),
            "minConsensusWallets": int(os.environ.get("MIN_CONSENSUS_WALLETS", "3")),
            "trackHip3": env_flag("TRACK_HIP3", True),
        }
    )
    result = service.check_alerts(send_notification=True)
    print(json.dumps(result, indent=2))
    return 1 if result.get("error") else 0


if __name__ == "__main__":
    sys.exit(main())
