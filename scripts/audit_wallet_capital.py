#!/usr/bin/env python3
"""Verify public capital export and describe remaining ranking input gaps."""
import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
import gzip
import hashlib
import json
import math
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from capital_history import audit_capital_changes, row_identity
from equity_history import observed_perp_history, paired_points
from server import daily_equity_metrics_180d


def join_observations(primary, incoming):
    shared = sorted(primary.keys() & incoming.keys())
    if len(shared) < 2:
        return incoming, {'status': 'insufficient_shared_anchors', 'sharedPoints': len(shared)}
    offset = incoming[shared[-1]][1] - primary[shared[-1]][1]
    conflicts = sum(not math.isclose(incoming[t][0], primary[t][0], abs_tol=1e-5, rel_tol=1e-9) or
                    not math.isclose(incoming[t][1] - primary[t][1], offset, abs_tol=1e-5, rel_tol=1e-9)
                    for t in shared)
    if conflicts:
        return incoming, {'status': 'shared_anchor_conflict', 'sharedPoints': len(shared), 'conflicts': conflicts}
    merged = {t: (account, pnl + offset) for t, (account, pnl) in primary.items()}
    merged.update(incoming)
    return merged, {'status': 'matched_shared_anchors', 'sharedPoints': len(shared), 'pnlOriginOffset': offset}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--capital', required=True, type=Path)
    parser.add_argument('--primary', required=True, type=Path)
    args = parser.parse_args()
    manifest = json.loads((args.capital / 'manifest.json').read_text())
    primary = defaultdict(dict)
    with gzip.open(args.primary, 'rt') as inp:
        for line in inp:
            row = json.loads(line)
            if row['type'] == 'equity':
                primary[row['address']][row['at']] = (row['account'], row['pnl'])
    result = {'generatedAt': datetime.now(timezone.utc).isoformat(), 'startMs': manifest['startMs'],
              'endMs': manifest['endMs'], 'wallets': [], 'productionImportPerformed': False, 'newVerifiedRanks': 0}
    for item in manifest['wallets']:
        address = item['address']
        path = args.capital / (address + '.json')
        assert hashlib.sha256(path.read_bytes()).hexdigest() == item['sha256']
        data = json.loads(path.read_text())
        assert (data['startMs'], data['endMs']) == (manifest['startMs'], manifest['endMs'])
        rows = data.get('ledger', {}).get('rows', [])
        assert len({row_identity(row) for row in rows}) == len(rows)
        assert all(0 <= int(row['time']) <= manifest['endMs'] for row in rows)
        portfolio = dict((data.get('portfolio') or {}).get('data') or [])
        curve = observed_perp_history(portfolio)
        incoming = paired_points(curve)
        points, join = join_observations(primary[address], incoming)
        start, end = manifest['startMs'], manifest['endMs']
        window = [row for row in rows if start <= int(row['time']) <= end]
        metrics = daily_equity_metrics_180d([[t,p] for t,(a,p) in sorted(points.items())],
                      [[t,a] for t,(a,p) in sorted(points.items())], start, end)
        current_book = portfolio.get('perpAllTime') or {}
        combined_book = portfolio.get('allTime') or {}
        def last(block, key):
            values = block.get(key) or []
            return values[-1] if values else None
        result['wallets'].append({'address': address, 'ledgerRowsAllTime': len(rows),
            'ledgerRows180d': len(window), 'ledgerKinds180d': dict(Counter(r['delta']['type'] for r in window)),
            'ledgerPaginationStatus': data.get('ledger', {}).get('paginationStatus'),
            'accountModeNow': item['accountMode'], 'historicalAccountModeVerified': False,
            'officialObservedPoints': len(incoming), 'mergedObservedPoints': len(points),
            'primaryJoin': join, 'rejectedPortfolioWindows': curve['rejectedWindows'],
            'equityMetrics': metrics, 'capitalAudit': audit_capital_changes(points, rows, start_ms=start, end_ms=end),
            'currentPerpPortfolioValue': last(current_book, 'accountValueHistory'),
            'currentCombinedPortfolioValue': last(combined_book, 'accountValueHistory'),
            'snapshotErrors': item['errors'], 'rankEligible': False,
            'remainingRequirements': ['continuous_daily_equity', 'cashflow_book_and_order',
                                     'loss_opening_capital', 'verified_execution_coverage']})
    result['totals'] = {'wallets': len(result['wallets']),
        'ledgerRowsAllTime': sum(w['ledgerRowsAllTime'] for w in result['wallets']),
        'ledgerRows180d': sum(w['ledgerRows180d'] for w in result['wallets']),
        'accountModes': dict(Counter(w['accountModeNow'] for w in result['wallets'])),
        'completeEquityCurves': sum(w['equityMetrics']['equityCurveComplete'] for w in result['wallets']),
        'matchedPrimaryJoins': sum(w['primaryJoin']['status'] == 'matched_shared_anchors' for w in result['wallets'])}
    (args.capital / 'capital-audit.json').write_text(json.dumps(result, indent=2, allow_nan=False))
    print(json.dumps(result['totals']))


if __name__ == '__main__':
    main()
