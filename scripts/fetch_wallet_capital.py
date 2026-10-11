#!/usr/bin/env python3
"""Export public official portfolio, transfers and current account snapshots.

Snapshots are separate observations, not an atomic historical account state.
This script never changes production ranks or trading state.
"""
import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from capital_history import walk_ledger
from ratelimit import HyperliquidRateLimiter
from server import HyperliquidClient


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--wallets', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--max-pages', type=int, default=100)
    parser.add_argument('--reuse', action='store_true')
    args = parser.parse_args()
    addresses = [row['address'].lower() for row in json.loads(args.wallets.read_text())['wallets']]
    if len(set(addresses)) != len(addresses) or any(len(a) != 42 or not a.startswith('0x') or
            any(c not in '0123456789abcdef' for c in a[2:]) for a in addresses):
        parser.error('Expected unique Ethereum addresses')
    if args.max_pages < 1:
        parser.error('--max-pages must be positive')
    args.output.mkdir(parents=True, exist_ok=True)
    manifest_path = args.output / 'manifest.json'
    manifest = json.loads(manifest_path.read_text()) if args.reuse and manifest_path.exists() else {
        'provider': 'Hyperliquid', 'url': 'https://api.hyperliquid.xyz/info',
        'endMs': int(time.time() * 1000), 'wallets': [], 'productionImportPerformed': False}
    manifest['startMs'] = manifest['endMs'] - 180 * 86400000
    manifest['wallets'] = []
    manifest_path.write_text(json.dumps(manifest, indent=2))
    limiter = HyperliquidRateLimiter(args.output / 'request-budget.sqlite3',
                                    weight_per_minute=900, historical_weight_per_minute=600,
                                    requests_per_second=0.5)
    client = HyperliquidClient(limiter, priority='history')

    def request(payload):
        # Reserve the worst-case row weight even though the docs' rate-limit
        # list uses a legacy name for the non-funding ledger endpoint.
        weighted = dict(payload, type='userFillsByTime')
        token = limiter.acquire(weighted, priority='history', max_wait_seconds=60)
        if token is None:
            raise TimeoutError('History budget/cooldown; resume on the next run')
        from urllib.request import Request, urlopen
        from urllib.error import HTTPError
        try:
            with urlopen(Request(manifest['url'], data=json.dumps(payload).encode(),
                    headers={'Content-Type': 'application/json'}), timeout=35) as response:
                data = json.load(response)
        except HTTPError as exc:
            if exc.code == 429:
                limiter.penalize(30)
            raise
        limiter.settle(token, weighted, data)
        return data

    for address in addresses:
        path = args.output / (address + '.json')
        previous = json.loads(path.read_text()) if args.reuse and path.exists() else {}
        if previous and (previous.get('startMs'), previous.get('endMs')) != (manifest['startMs'], manifest['endMs']):
            raise ValueError('Cannot reuse a different observation window')
        data = {**previous, 'address': address, 'startMs': manifest['startMs'],
                'endMs': manifest['endMs'], 'errors': dict(previous.get('errors', {}))}
        old_ledger = data.get('ledger')
        if not old_ledger or old_ledger.get('paginationStatus') != 'empty_final_page':
            try:
                # Include pre-window cashflows to support a real initial anchor.
                data['ledger'] = walk_ledger(request, address, 0, manifest['endMs'],
                                            max_pages=args.max_pages, previous=old_ledger)
                if data['ledger'].get('error'):
                    data['errors']['ledger'] = data['ledger']['error']
                else:
                    data['errors'].pop('ledger', None)
            except Exception as exc:
                data['errors']['ledger'] = str(exc)
            path.write_text(json.dumps(data, separators=(',', ':'), allow_nan=False))
        for kind in ['portfolio', 'userAbstraction', 'spotClearinghouseState',
                     'clearinghouseState', 'userVaultEquities']:
            if kind in data and kind not in data['errors']:
                continue
            result = client.safe_post_result({'type': kind, 'user': address}, None,
                    attempts=1, request_timeout_seconds=35, max_rate_limit_wait_seconds=60)
            if result['ok']:
                data[kind] = {'observedAt': datetime.now(timezone.utc).isoformat(), 'data': result['data']}
                data['errors'].pop(kind, None)
            else:
                data['errors'][kind] = result['error']
            path.write_text(json.dumps(data, separators=(',', ':'), allow_nan=False))
        if 'allDexsClearinghouseState' not in data:
            try:
                raw = client._subscribe_channel('allDexsClearinghouseState',
                      {'type': 'allDexsClearinghouseState', 'user': address}, address=address, timeout=12)
                data['allDexsClearinghouseState'] = {'observedAt': datetime.now(timezone.utc).isoformat(), 'data': raw}
                data['errors'].pop('allDexsClearinghouseState', None)
            except Exception as exc:
                data['errors']['allDexsClearinghouseState'] = str(exc)
        path.write_text(json.dumps(data, separators=(',', ':'), allow_nan=False))
        ledger = data.get('ledger') or {}
        summary = {'address': address, 'ledgerRows': len(ledger.get('rows', [])),
                   'ledgerPaginationStatus': ledger.get('paginationStatus'),
                   'ledgerKinds': dict(Counter(r['delta'].get('type') for r in ledger.get('rows', []))),
                   'accountMode': (data.get('userAbstraction') or {}).get('data'),
                   'errors': data['errors'], 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}
        manifest['wallets'].append(summary)
        manifest['throttle'] = limiter.throttle_report()
        manifest_path.write_text(json.dumps(manifest, indent=2, allow_nan=False))
        print(json.dumps(summary), flush=True)


if __name__ == '__main__':
    main()
