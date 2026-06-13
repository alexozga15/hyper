import json
import tempfile
import unittest
import urllib.error
from pathlib import Path
from unittest.mock import patch

import scripts.process_telegram_commands as commands
from scripts.process_telegram_commands import load_dispatch_updates, load_updates, parse_position_wallet_query


class DispatchUpdateTests(unittest.TestCase):
    def write_event(self, payload: dict) -> str:
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        event_path = Path(temp_dir.name) / "event.json"
        event_path.write_text(json.dumps(payload), encoding="utf-8")
        return str(event_path)

    def test_load_dispatch_updates_uses_full_telegram_update(self) -> None:
        event_path = self.write_event(
            {
                "client_payload": {
                    "update": {
                        "update_id": 42,
                        "message": {
                            "chat": {"id": 301411846},
                            "text": "/update",
                        },
                    }
                }
            }
        )

        with patch.dict(
            "os.environ",
            {
                "GITHUB_EVENT_NAME": "repository_dispatch",
                "GITHUB_EVENT_PATH": event_path,
            },
            clear=False,
        ):
            updates = load_dispatch_updates()

        self.assertEqual(
            updates,
            [
                {
                    "update_id": 42,
                    "message": {
                        "chat": {"id": 301411846},
                        "text": "/update",
                    },
                }
            ],
        )

    def test_load_dispatch_updates_falls_back_to_flat_payload_fields(self) -> None:
        event_path = self.write_event(
            {
                "client_payload": {
                    "update_id": 99,
                    "chat_id": "301411846",
                    "text": "/positions",
                }
            }
        )

        with patch.dict(
            "os.environ",
            {
                "GITHUB_EVENT_NAME": "repository_dispatch",
                "GITHUB_EVENT_PATH": event_path,
            },
            clear=False,
        ):
            updates = load_dispatch_updates()

        self.assertEqual(
            updates,
            [
                {
                    "update_id": 99,
                    "message": {
                        "chat": {"id": "301411846"},
                        "text": "/positions",
                    },
                }
            ],
        )

    def test_load_updates_skips_older_dispatch_updates(self) -> None:
        event_path = self.write_event(
            {
                "client_payload": {
                    "update": {
                        "update_id": 4,
                        "message": {
                            "chat": {"id": 301411846},
                            "text": "/update",
                        },
                    }
                }
            }
        )

        class FakeService:
            def fetch_telegram_updates(self, bot_token: str, offset: int = 0) -> list[dict]:
                raise AssertionError("Polling should not run when a dispatch payload exists")

        with patch.dict(
            "os.environ",
            {
                "GITHUB_EVENT_NAME": "repository_dispatch",
                "GITHUB_EVENT_PATH": event_path,
            },
            clear=False,
        ):
            updates, source = load_updates(FakeService(), "token", last_update_id=10)

        self.assertEqual(updates, [])
        self.assertEqual(source, "repository_dispatch")

    def test_load_updates_handles_webhook_conflict_during_polling(self) -> None:
        class FakeService:
            def fetch_telegram_updates(self, bot_token: str, offset: int = 0) -> list[dict]:
                raise urllib.error.HTTPError(
                    url="https://api.telegram.org/bot123/getUpdates",
                    code=409,
                    msg="Conflict",
                    hdrs=None,
                    fp=None,
                )

        with patch.dict("os.environ", {"TELEGRAM_POLLING_BACKUP": "true"}, clear=True):
            updates, source = load_updates(FakeService(), "token", last_update_id=10)

        self.assertEqual(updates, [])
        self.assertEqual(source, "getUpdates")

    def test_load_updates_skips_polling_unless_backup_enabled(self) -> None:
        class FakeService:
            def fetch_telegram_updates(self, bot_token: str, offset: int = 0) -> list[dict]:
                raise AssertionError("Polling should require an explicit backup flag")

        with patch.dict("os.environ", {}, clear=True):
            updates, source = load_updates(FakeService(), "token", last_update_id=10)

        self.assertEqual(updates, [])
        self.assertEqual(source, "polling_disabled")

    def test_parse_position_wallet_query_from_ticker_direction_command(self) -> None:
        self.assertEqual(parse_position_wallet_query("/btc long"), ("BTC", "long"))
        self.assertEqual(parse_position_wallet_query("/hype@HyperwatchBot short"), ("HYPE", "short"))
        self.assertIsNone(parse_position_wallet_query("/btc sideways"))
        self.assertIsNone(parse_position_wallet_query("/update long"))

    def test_build_reply_routes_ticker_direction_query(self) -> None:
        class FakeService:
            def build_position_wallets_message(self, dashboard: dict, coin: str, side: str) -> str:
                return f"{coin}:{side}:{dashboard['generatedAt']}"

        reply = commands.build_reply(
            FakeService(),
            "/btc",
            ("BTC", "long"),
            None,
            {"generatedAt": "now"},
            3,
        )

        self.assertEqual(reply, "BTC:long:now")

    def test_build_reply_update_uses_compact_signal_view(self) -> None:
        class FakeService:
            def __init__(self) -> None:
                self.summary_kwargs = None
                self.positions_called = False

            def build_summary_message(self, summary: dict, min_wallets: int, **kwargs) -> str:
                self.summary_kwargs = kwargs
                return "signal summary"

            def build_positions_message(self, dashboard: dict) -> str:
                self.positions_called = True
                return "positions"

        service = FakeService()
        reply = commands.build_reply(
            service,
            "/update",
            None,
            {"generatedAt": "now"},
            {"generatedAt": "now"},
            3,
        )

        self.assertEqual(reply, "signal summary")
        self.assertFalse(service.positions_called)
        self.assertEqual(service.summary_kwargs["title"], "Current wallet signals")
        self.assertFalse(service.summary_kwargs["include_consensus"])
        self.assertTrue(service.summary_kwargs["include_signals"])

    def test_main_persists_successful_update_before_later_send_failure(self) -> None:
        class FakeService:
            def __init__(self) -> None:
                self.send_count = 0

            def send_telegram_message(self, bot_token: str, chat_id: str, reply: str) -> None:
                self.send_count += 1
                if self.send_count == 2:
                    raise ValueError("telegram down")

        fake_service = FakeService()
        updates = [
            {"update_id": 1, "message": {"chat": {"id": "chat"}, "text": "/help"}},
            {"update_id": 2, "message": {"chat": {"id": "chat"}, "text": "/help"}},
        ]

        with patch.dict(
            "os.environ",
            {"TELEGRAM_BOT_TOKEN": "token", "TELEGRAM_CHAT_ID": "chat"},
            clear=True,
        ), patch.object(commands, "WalletTrackerService", return_value=fake_service), patch.object(
            commands, "WalletStore"
        ), patch.object(
            commands, "HyperliquidClient"
        ), patch.object(
            commands, "load_json_file", return_value={"lastUpdateId": 0}
        ), patch.object(
            commands, "load_updates", return_value=(updates, "repository_dispatch")
        ), patch.object(
            commands, "save_json_file"
        ) as save_json_file:
            with self.assertRaises(ValueError):
                commands.main()

        save_json_file.assert_called_once_with(commands.TELEGRAM_STATE_FILE, {"lastUpdateId": 1})

    def test_main_processes_ticker_direction_command(self) -> None:
        class FakeService:
            def __init__(self) -> None:
                self.sent_reply = ""
                self.dashboard_called = 0

            def dashboard(self) -> dict:
                self.dashboard_called += 1
                return {"generatedAt": "now", "wallets": []}

            def build_sentiment_summary(self, wallets: list, min_wallets: int) -> dict:
                raise AssertionError("Ticker-direction commands should not build sentiment summaries")

            def build_position_wallets_message(self, dashboard: dict, coin: str, side: str) -> str:
                return f"{coin} {side} reply"

            def send_telegram_message(self, bot_token: str, chat_id: str, reply: str) -> None:
                self.sent_reply = reply

        fake_service = FakeService()
        updates = [{"update_id": 1, "message": {"chat": {"id": "chat"}, "text": "/btc long"}}]

        with patch.dict(
            "os.environ",
            {"TELEGRAM_BOT_TOKEN": "token", "TELEGRAM_CHAT_ID": "chat"},
            clear=True,
        ), patch.object(commands, "WalletTrackerService", return_value=fake_service), patch.object(
            commands, "WalletStore"
        ), patch.object(
            commands, "HyperliquidClient"
        ), patch.object(
            commands, "load_json_file", return_value={"lastUpdateId": 0}
        ), patch.object(
            commands, "load_updates", return_value=(updates, "repository_dispatch")
        ), patch.object(
            commands, "save_json_file"
        ):
            exit_code = commands.main()

        self.assertEqual(exit_code, 0)
        self.assertEqual(fake_service.dashboard_called, 1)
        self.assertEqual(fake_service.sent_reply, "BTC long reply")


if __name__ == "__main__":
    unittest.main()
