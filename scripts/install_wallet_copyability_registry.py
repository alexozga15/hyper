#!/usr/bin/env python3
"""Validate and atomically install a completed copyability registry."""
from __future__ import annotations

import argparse
import json
import os
import pathlib
import shutil
import sys
import tempfile
from datetime import datetime, timezone

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from server import COPYABILITY_METHOD, WALLET_COPYABILITY_FILE, copyability_assessment, normalize_address


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=pathlib.Path)
    parser.add_argument("--target", type=pathlib.Path, default=WALLET_COPYABILITY_FILE)
    parser.add_argument("--allow-incomplete", action="store_true")
    args = parser.parse_args()
    payload = json.loads(args.source.read_text())
    if payload.get("method") != COPYABILITY_METHOD or not isinstance(payload.get("wallets"), dict):
        raise SystemExit("invalid copyability registry schema")
    if not payload.get("observationComplete") and not args.allow_incomplete:
        raise SystemExit("refusing to promote an incomplete observation")
    for address, record in payload["wallets"].items():
        if normalize_address(address).lower() != address.lower() or not isinstance(record, dict):
            raise SystemExit(f"invalid wallet record: {address}")
        assessment = copyability_assessment(record)
        if assessment["method"] != COPYABILITY_METHOD:
            raise SystemExit(f"invalid method for {address}")
        for field in ("independentCompletedEpisodes", "costAdjustedNetReturnPct", "lowerConfidenceBoundPct", "observationComplete"):
            if field not in record:
                raise SystemExit(f"missing {field} for {address}")
    args.target.parent.mkdir(parents=True, exist_ok=True)
    backup = None
    if args.target.exists():
        suffix = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        backup = args.target.with_name(f"{args.target.name}.backup.{suffix}")
        shutil.copy2(args.target, backup)
    with tempfile.NamedTemporaryFile("w", dir=args.target.parent, delete=False) as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=False)
        handle.write("\n")
        temp = pathlib.Path(handle.name)
    os.replace(temp, args.target)
    message = f"installed {len(payload['wallets'])} wallet records at {args.target}"
    if backup is not None:
        message += f"; rollback copy: {backup}"
    print(message)


if __name__ == "__main__":
    main()
