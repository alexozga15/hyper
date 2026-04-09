import unittest
from unittest.mock import patch

from scripts import run_alert_check


class RunAlertCheckTests(unittest.TestCase):
    @patch("scripts.run_alert_check.HyperliquidClient")
    @patch("scripts.run_alert_check.WalletStore")
    @patch("scripts.run_alert_check.WalletTrackerService")
    def test_main_sends_hourly_update_and_change_alert(self, service_cls, wallet_store_cls, client_cls) -> None:
        service = service_cls.return_value
        service.send_hourly_update.return_value = {"sent": True}
        service.check_alerts.return_value = {"sent": True, "error": ""}

        with patch.dict(
            "os.environ",
            {
                "ALERTS_ENABLED": "true",
                "SEND_HOURLY_UPDATE": "true",
                "SEND_CHANGE_ALERTS": "true",
                "TELEGRAM_BOT_TOKEN": "token",
                "TELEGRAM_CHAT_ID": "chat",
                "MIN_CONSENSUS_WALLETS": "3",
                "TRACK_HIP3": "true",
            },
            clear=False,
        ):
            exit_code = run_alert_check.main()

        self.assertEqual(exit_code, 0)
        service.update_alert_settings.assert_called_once()
        service.send_hourly_update.assert_called_once_with(3, "token", "chat")
        service.check_alerts.assert_called_once_with(send_notification=True)
        wallet_store_cls.assert_called_once()
        client_cls.assert_called_once()


if __name__ == "__main__":
    unittest.main()
