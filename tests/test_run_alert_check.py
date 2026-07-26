import unittest
from datetime import datetime
from unittest.mock import patch
from zoneinfo import ZoneInfo

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
                "QUIET_HOURS_ENABLED": "false",
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

    def test_quiet_hours_wrap_midnight_in_warsaw(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "QUIET_HOURS_ENABLED": "true",
                "QUIET_HOURS_TIMEZONE": "Europe/Warsaw",
                "QUIET_HOURS_START": "23",
                "QUIET_HOURS_END": "7",
            },
            clear=False,
        ):
            self.assertTrue(run_alert_check.is_quiet_hours(datetime(2026, 7, 26, 23, 0, tzinfo=ZoneInfo("Europe/Warsaw"))))
            self.assertTrue(run_alert_check.is_quiet_hours(datetime(2026, 7, 27, 6, 59, tzinfo=ZoneInfo("Europe/Warsaw"))))
            self.assertFalse(run_alert_check.is_quiet_hours(datetime(2026, 7, 27, 7, 0, tzinfo=ZoneInfo("Europe/Warsaw"))))
            self.assertFalse(run_alert_check.is_quiet_hours(datetime(2026, 7, 27, 22, 59, tzinfo=ZoneInfo("Europe/Warsaw"))))

    @patch("scripts.run_alert_check.is_quiet_hours", return_value=True)
    @patch("scripts.run_alert_check.HyperliquidClient")
    @patch("scripts.run_alert_check.WalletStore")
    @patch("scripts.run_alert_check.WalletTrackerService")
    def test_main_acknowledges_state_without_sending_during_quiet_hours(
        self,
        service_cls,
        wallet_store_cls,
        client_cls,
        quiet_hours,
    ) -> None:
        service = service_cls.return_value
        service.check_alerts.return_value = {"sent": False, "suppressed": True, "error": ""}

        with patch.dict(
            "os.environ",
            {
                "SEND_HOURLY_UPDATE": "true",
                "SEND_CHANGE_ALERTS": "true",
                "TELEGRAM_BOT_TOKEN": "token",
                "TELEGRAM_CHAT_ID": "chat",
            },
            clear=False,
        ):
            exit_code = run_alert_check.main()

        self.assertEqual(exit_code, 0)
        service.send_hourly_update.assert_not_called()
        service.check_alerts.assert_called_once_with(
            send_notification=False,
            acknowledge_suppressed=True,
        )


if __name__ == "__main__":
    unittest.main()
