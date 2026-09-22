"""Read-only diagnostic report for the prospective v4 SQLite journal."""

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from execution_journal import ExecutionJournal


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("journal", type=Path, help="Existing execution_journal.sqlite3")
    args = parser.parse_args()
    if not args.journal.is_file():
        parser.error("journal file does not exist; refusing to create an empty report")
    print(json.dumps(ExecutionJournal(args.journal).paper_report(), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
