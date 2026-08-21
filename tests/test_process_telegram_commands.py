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

    def test_build_cmm_cache_uses_service_cache_helper(self) -> None:
        class FakeService:
            alerts_path = Path("alerts.json")

            def build_cached_cmm_signal_summary(self, state: dict) -> dict:
                self.state = state
                return {"enabled": True, "signals": [{"coin": "LINK", "side": "short"}]}

        cached = {
            "enabled": True,
            "signals": [{"coin": "LINK", "side": "short"}],
            "generatedAt": "2026-06-20T09:00:00Z",
        }
        fake_service = FakeService()

        with patch.object(commands, "load_json_file", return_value={"state": {"cmmSignals": cached}}):
            summary = commands.build_cmm_cache(fake_service)

        self.assertEqual(summary["signals"], cached["signals"])
        self.assertEqual(fake_service.state["cmmSignals"], cached)

    def test_build_reply_update_omits_the_crowding_board(self) -> None:
        """/update is the routine digest and must not carry the consensus block.

        The board ranks nothing - measured over 805 shadow samples, neither the
        wallet count nor the score separated profitable observations from
        unprofitable ones - so it stays available on demand via /consensus
        instead of implying a recommendation in every digest.
        """

        class FakeService:
            def __init__(self) -> None:
                self.kwargs: dict = {}

            def build_summary_message(self, summary, min_wallets, **kwargs) -> str:
                self.kwargs = kwargs
                return "summary"

            def build_positions_message(self, dashboard) -> str:
                return "positions"

        service = FakeService()
        reply = commands.build_reply(service, "/update", None, {}, {}, None, 3)

        self.assertIs(service.kwargs.get("include_consensus"), False)
        self.assertIs(service.kwargs.get("include_signals"), False)
        self.assertEqual(reply, "summary\n\npositions")

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
            None,
            3,
        )

        self.assertEqual(reply, "BTC:long:now")

    def test_build_reply_routes_moni_without_refreshing_api(self) -> None:
        class FakeService:
            def build_moni_social_message(self, summary: dict) -> str:
                return f"moni:{summary['monthPointsUsage']}"

        reply = commands.build_reply(
            FakeService(),
            "/moni",
            None,
            None,
            None,
            None,
            4,
            moni_cache={"monthPointsUsage": 18},
        )
        self.assertEqual(reply, "moni:18")

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


class OutcomeCommandTests(unittest.TestCase):
    def test_outcomes_is_not_a_live_command(self) -> None:
        # It reads stored outcomes only; listing it in LIVE_COMMANDS would
        # make it pay for a dashboard fetch it never reads.
        self.assertNotIn("/outcomes", commands.LIVE_COMMANDS)
        self.assertIn("/outcomes", commands.KNOWN_COMMANDS)

    def test_command_names_are_never_parsed_as_tickers(self) -> None:
        for text in ("/outcomes long", "/moni short", "/help long"):
            with self.subTest(text=text):
                self.assertIsNone(commands.parse_position_wallet_query(text))
        self.assertEqual(commands.parse_position_wallet_query("/btc long"), ("BTC", "long"))

    def test_outcomes_reply_reports_stored_outcomes(self) -> None:
        state = {
            "state": {
                "shadowSignalOutcomes": {
                    "a": {
                        "coin": "BTC",
                        "side": "short",
                        "startedAt": commands.current_time_ms() - 3_600_000,
                        "outcomes": {"1h": {"netReturnPct": -3.419}},
                    }
                }
            }
        }
        with patch.object(commands, "load_json_file", return_value=state):
            reply = commands.build_reply(object(), "/outcomes", None, None, None, None, 4)

        self.assertIn("Outcome report", reply)
        self.assertIn("n=1", reply)


class TradesQueryTests(unittest.TestCase):
    """A full address is unusable on a phone, so /trades matches a prefix."""

    ADDRESSES = [
        "0x350e33a777d510616fbdb483d1de3b50d1edfcfb",
        "0x35aa11112222333344445555666677778888aaaa",
        "0x9e8b1e51c642f4c8b87c6ba11c53d516a218afc4",
    ]

    def test_prefix_resolves_to_one_wallet_with_the_default_window(self) -> None:
        self.assertEqual(
            commands.parse_trades_query("/trades 0x350e", self.ADDRESSES),
            (self.ADDRESSES[0], commands.TRADES_DEFAULT_DAYS),
        )

    def test_an_explicit_window_is_honoured_and_capped_at_retention(self) -> None:
        self.assertEqual(commands.parse_trades_query("/trades 0x350e 30", self.ADDRESSES)[1], 30)
        # The venue clamps userFillsByTime to roughly 100 days, so a larger
        # request would quietly return the same window.
        self.assertEqual(
            commands.parse_trades_query("/trades 0x350e 400", self.ADDRESSES)[1],
            commands.TRADES_MAX_DAYS,
        )
        self.assertEqual(commands.parse_trades_query("/trades 0x350e 0", self.ADDRESSES)[1], 1)

    def test_an_ambiguous_prefix_is_explained_rather_than_guessed(self) -> None:
        reply = commands.parse_trades_query("/trades 0x35", self.ADDRESSES)
        self.assertIsInstance(reply, str)
        self.assertIn("be more specific", reply)

    def test_unknown_prefix_and_missing_argument_are_reported(self) -> None:
        self.assertIn("No tracked wallet", commands.parse_trades_query("/trades 0xdead", self.ADDRESSES))
        self.assertIn("Usage:", commands.parse_trades_query("/trades", self.ADDRESSES))
        self.assertIn("Not a number", commands.parse_trades_query("/trades 0x350e soon", self.ADDRESSES))

    def test_other_commands_are_left_alone(self) -> None:
        self.assertIsNone(commands.parse_trades_query("/positions", self.ADDRESSES))

    def test_trades_is_a_known_command_so_a_ticker_query_cannot_swallow_it(self) -> None:
        self.assertIn("/trades", commands.KNOWN_COMMANDS)
        self.assertNotIn("/trades", commands.LIVE_COMMANDS)
