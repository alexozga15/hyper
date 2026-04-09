import json
import tempfile
import unittest
import urllib.error
from pathlib import Path
from unittest.mock import patch

from scripts.process_telegram_commands import load_dispatch_updates, load_updates


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

        with patch.dict("os.environ", {}, clear=True):
            updates, source = load_updates(FakeService(), "token", last_update_id=10)

        self.assertEqual(updates, [])
        self.assertEqual(source, "getUpdates")


if __name__ == "__main__":
    unittest.main()
