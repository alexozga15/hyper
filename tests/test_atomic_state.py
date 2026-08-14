from __future__ import annotations

import json
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from server import (
    HyperliquidClient,
    RequestRateLimiter,
    WalletTrackerService,
    atomic_write_text,
    load_json_file,
    locked_path,
    save_json_file,
)


class AtomicWriteTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmpdir.cleanup)
        self.dir = Path(self._tmpdir.name)
        self.target = self.dir / "state.json"

    def test_save_json_file_leaves_no_tmp_residue_and_content_is_complete(self) -> None:
        payload = {"config": {"enabled": True}, "state": {"lastCheckedAt": "2026-08-14T00:00:00Z"}}
        save_json_file(self.target, payload)

        self.assertEqual(json.loads(self.target.read_text(encoding="utf-8")), payload)
        self.assertEqual(self.target.read_text(encoding="utf-8"), json.dumps(payload, indent=2) + "\n")

        residue = [entry.name for entry in self.dir.iterdir() if entry.name.endswith(".tmp")]
        self.assertEqual(residue, [])

    def test_failed_write_leaves_preexisting_file_intact_and_cleans_up_tmp(self) -> None:
        original_payload = {"config": {"enabled": False}, "state": {"alertDedupe": {"a": 1}}}
        save_json_file(self.target, original_payload)
        original_bytes = self.target.read_bytes()

        with patch("server.os.replace", side_effect=OSError("simulated crash mid-write")):
            with self.assertRaises(OSError):
                save_json_file(self.target, {"config": {"enabled": True}, "state": {}})

        # The pre-existing file must be byte-for-byte untouched: a reader
        # never observes a truncated/partial write because the new content
        # only ever lands in a same-directory temp file until os.replace
        # succeeds.
        self.assertEqual(self.target.read_bytes(), original_bytes)

        residue = [entry.name for entry in self.dir.iterdir() if entry.name.endswith(".tmp")]
        self.assertEqual(residue, [])

    def test_dumps_failure_before_any_write_leaves_file_untouched(self) -> None:
        original_payload = {"config": {"enabled": False}, "state": {}}
        save_json_file(self.target, original_payload)
        original_bytes = self.target.read_bytes()

        with patch("server.json.dumps", side_effect=ValueError("boom")):
            with self.assertRaises(ValueError):
                save_json_file(self.target, {"config": {"enabled": True}, "state": {}})

        self.assertEqual(self.target.read_bytes(), original_bytes)
        residue = [entry.name for entry in self.dir.iterdir() if entry.name.endswith(".tmp")]
        self.assertEqual(residue, [])

    def test_load_json_file_returns_default_for_corrupt_file(self) -> None:
        self.target.write_text("{not valid json!!", encoding="utf-8")
        result = load_json_file(self.target, {"fallback": True})
        self.assertEqual(result, {"fallback": True})

    def test_load_json_file_returns_default_when_missing(self) -> None:
        missing = self.dir / "does-not-exist.json"
        self.assertEqual(load_json_file(missing, []), [])

    def test_locked_path_degrades_to_no_op_when_fcntl_unavailable(self) -> None:
        with patch("server.fcntl", None):
            entered = False
            with locked_path(self.target, exclusive=True):
                entered = True
                # Should not raise even though the file doesn't exist yet and
                # there is no real fcntl module backing this context manager.
            self.assertTrue(entered)

        # Also verify save/load still function end-to-end with locking
        # disabled (no crash, no lock file created).
        with patch("server.fcntl", None):
            save_json_file(self.target, {"ok": True})
            self.assertEqual(load_json_file(self.target, None), {"ok": True})
        lock_file = self.dir / f"{self.target.name}.lock"
        self.assertFalse(lock_file.exists())

    def test_atomic_write_text_helper_directly(self) -> None:
        atomic_write_text(self.target, "hello\n")
        self.assertEqual(self.target.read_text(encoding="utf-8"), "hello\n")
        residue = [entry.name for entry in self.dir.iterdir() if entry.name.endswith(".tmp")]
        self.assertEqual(residue, [])


class UpdateAlertSettingsConcurrencyTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmpdir.cleanup)
        self.alerts_path = Path(self._tmpdir.name) / "alerts.json"
        self.service = WalletTrackerService(object(), HyperliquidClient(RequestRateLimiter(1000)))
        self.service.alerts_path = self.alerts_path

        stale_payload = {
            "config": {"enabled": False},
            "state": {"alertDedupe": {"stale": True}, "signalOutcomes": {}},
        }
        save_json_file(self.alerts_path, stale_payload)

    def test_competing_writer_state_survives_concurrent_update_alert_settings(self) -> None:
        """A concurrent writer (standing in for check_alerts) holds the
        exclusive lock, writes fresher `state`, then releases it. Once
        update_alert_settings finally acquires the lock it must re-read that
        fresher state from disk rather than clobbering it with the `state`
        it would have captured before waiting for the lock.
        """
        lock_acquired = threading.Event()
        fresh_state = {
            "alertDedupe": {"fresh": True, "sentKeys": ["a", "b"]},
            "signalOutcomes": {"BTC": {"outcomes": 1}},
        }

        def competing_writer() -> None:
            with locked_path(self.alerts_path, exclusive=True):
                lock_acquired.set()
                time.sleep(0.4)
                atomic_write_text(
                    self.alerts_path,
                    json.dumps({"config": {"enabled": False}, "state": fresh_state}, indent=2) + "\n",
                )

        writer_thread = threading.Thread(target=competing_writer)
        writer_thread.start()
        self.assertTrue(lock_acquired.wait(timeout=2), "competing writer never acquired the lock")

        # update_alert_settings is called while the competing writer still
        # holds the lock; it must block, not read stale data.
        result = self.service.update_alert_settings({"enabled": True})
        writer_thread.join(timeout=2)
        self.assertFalse(writer_thread.is_alive())

        on_disk = json.loads(self.alerts_path.read_text(encoding="utf-8"))
        self.assertEqual(on_disk["state"], fresh_state)
        self.assertTrue(on_disk["config"]["enabled"])
        self.assertTrue(result["enabled"])

    def test_update_alert_settings_preserves_state_without_contention(self) -> None:
        result = self.service.update_alert_settings({"minConsensusWallets": 7})
        on_disk = json.loads(self.alerts_path.read_text(encoding="utf-8"))
        self.assertEqual(on_disk["state"], {"alertDedupe": {"stale": True}, "signalOutcomes": {}})
        self.assertEqual(on_disk["config"]["minConsensusWallets"], 7)
        self.assertEqual(result["minConsensusWallets"], 7)


if __name__ == "__main__":
    unittest.main()
