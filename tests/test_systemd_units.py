from __future__ import annotations

import configparser
import unittest
from pathlib import Path

UNIT_DIR = Path(__file__).resolve().parents[1] / "deploy" / "ec2"

# Anchors that are guaranteed to yield an elapse point in the future whenever
# the timer itself is started. OnBootSec is measured from boot and
# OnUnitActiveSec/OnUnitInactiveSec from the service's own history, so all
# three can be in the past at the same moment.
SELF_ANCHORS = ("OnActiveSec", "OnCalendar")


def timer_files() -> list[Path]:
    return sorted(UNIT_DIR.glob("*.timer"))


def timer_section(path: Path) -> dict[str, str]:
    parser = configparser.ConfigParser(strict=False)
    # systemd keys are case-sensitive; configparser lowercases them by default.
    parser.optionxform = str
    parser.read(path, encoding="utf-8")
    return dict(parser["Timer"])


class TimerAnchorTests(unittest.TestCase):
    """A timer must never be able to strand itself with nothing scheduled.

    Restarting a timer whose only anchors are OnBootSec and OnUnitActiveSec
    leaves systemd with both elapse points in the past. It then parks in
    "active (elapsed)" with "Trigger: n/a" and never fires again - without
    failing, without logging, and without any unit entering a failed state. On
    2026-08-15 that silently stopped the Telegram command poller for four
    hours, and the same shape was present in the sentiment and health-monitor
    timers, either of which would have taken the whole pipeline down the same
    way.
    """

    def test_every_timer_is_discovered(self) -> None:
        self.assertGreaterEqual(len(timer_files()), 5, "timer units not found - has the path moved?")

    def test_every_timer_has_a_self_anchored_elapse_point(self) -> None:
        for path in timer_files():
            with self.subTest(unit=path.name):
                keys = timer_section(path)
                self.assertTrue(
                    any(anchor in keys for anchor in SELF_ANCHORS),
                    f"{path.name} can only be scheduled from boot time or from the service's own "
                    f"history; add one of {', '.join(SELF_ANCHORS)} so a timer restart always "
                    "leaves a future elapse point",
                )

    def test_every_timer_names_the_unit_it_triggers(self) -> None:
        for path in timer_files():
            with self.subTest(unit=path.name):
                keys = timer_section(path)
                expected = path.name.replace(".timer", ".service")
                self.assertEqual(keys.get("Unit"), expected)


if __name__ == "__main__":
    unittest.main()
