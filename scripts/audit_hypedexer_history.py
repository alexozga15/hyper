#!/usr/bin/env python3
"""Validate independent archives and compare overlapping execution IDs.

Compact fills omit realised PnL/direction. Agreement on IDs, prices and fees
does not manufacture verified PnL or complete episodes. Hourly equity covers
the main perp book only and is never imported into combined account history.
"""
import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import gzip
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from capital_history import row_identity


def timestamp_ms(value):
    if isinstance(value, (int, float)):
        return int(value)
    dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp()) * 1000 + dt.microsecond // 1000


def matches_execution(api, original):
    """A missing financial field is unknown, never silently replaced by zero."""
    if int(api.get('tid') or 0) <= 0 or int(api.get('tid') or 0) != int(original.get('tid') or 0):
        return {'status': 'missing_or_conflicting_execution_id', 'pnlCorroborated': False}
    required = ('coin', 'hash', 'oid', 'side', 'feeToken', 'px', 'sz', 'fee', 'startPosition', 'time')
    if any(api.get(k) is None or original.get(k) is None for k in required):
        return {'status': 'missing_execution_fields', 'pnlCorroborated': False}
    same = timestamp_ms(api['time']) == timestamp_ms(original['time'])
    same = same and all(str(api[k]) == str(original[k]) for k in ('coin', 'hash', 'oid', 'side', 'feeToken'))
    for k in ('px', 'sz', 'fee', 'startPosition'):
        try:
            a, b = Decimal(str(api[k])), Decimal(str(original[k]))
        except InvalidOperation:
            return {'status': 'invalid_numeric_field', 'pnlCorroborated': False}
        if not a.is_finite() or not b.is_finite():
            return {'status': 'invalid_numeric_field', 'pnlCorroborated': False}
        same = same and abs(a - b) <= max(Decimal('1e-8'), max(abs(a), abs(b)) * Decimal('1e-12'))
    return {'status': 'matched_execution_fields' if same else 'execution_field_conflict',
            'pnlCorroborated': False}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--archive', required=True, type=Path)
    p.add_argument('--official', required=True, type=Path)
    p.add_argument('--hyperdash', required=True, type=Path)
    args = p.parse_args()
    manifest = json.loads((args.archive / 'manifest.json').read_text())
    raw = {}
    for r in manifest['requests']:
        if 'file' not in r:
            continue
        path = args.archive / r['file']
        if hashlib.sha256(path.read_bytes()).hexdigest() != r['sha256']:
            raise ValueError('Raw response checksum mismatch')
        raw[r['file']] = json.loads(gzip.decompress(path.read_bytes()))
    result = {'startMs': manifest['startMs'], 'endMs': manifest['endMs'], 'wallets': [],
              'productionImportPerformed': False, 'newVerifiedRanks': 0,
              'sumReportedCreditCosts': sum(int(r['creditCost']) for r in manifest['requests'] if r.get('creditCost')),
              'actualAccountCreditBalanceVerified': False, 'verifiedRawResponses': len(raw)}
    for w in manifest['wallets']:
        address = w['address']
        official = json.loads((args.official / (address + '.json')).read_text())
        hd = json.loads((args.hyperdash / (address + '.json')).read_text())
        low, high = max(hd['startMs'], manifest['startMs']), min(hd['endMs'], manifest['endMs'])
        source = defaultdict(list)
        for r in hd['rawFills']['fills']:
            if low <= r['time'] <= high and int(r.get('tid') or 0) > 0:
                source[(r['coin'], int(r['tid']))].append(r)
        fills = [r for page in w.get('fills', {}).get('pages', []) for r in raw[page['file']]['data']]
        unique = {row_identity(r) for r in fills}
        statuses = Counter()
        invalid_window = 0
        missing_pnl = 0
        unverified_id_samples = []
        for r in fills:
            if r.get('closedPnl') is None:
                missing_pnl += 1
            t = timestamp_ms(r['time'])
            if not manifest['startMs'] <= t <= manifest['endMs']:
                invalid_window += 1
                continue
            if not low <= t <= high:
                statuses['outside_shared_window'] += 1
                continue
            if int(r.get('tid') or 0) <= 0:
                statuses['missing_valid_execution_id'] += 1
                if len(unverified_id_samples) < 10:
                    unverified_id_samples.append(r)
                continue
            candidates = source[(r['coin'], int(r.get('tid') or 0))]
            if not candidates:
                statuses['no_matching_hyperdash_id'] += 1
            elif len(candidates) != 1:
                statuses['ambiguous_hyperdash_id'] += 1
            else:
                statuses[matches_execution(r, candidates[0])['status']] += 1
        if len(fills) != w.get('fills', {}).get('rowCount', 0):
            raise ValueError('Manifest fill count mismatch')
        start, end = manifest['startMs'], manifest['endMs']
        native = {row_identity(r) for r in official['ledger']['rows'] if start <= r['time'] <= end}
        indexed = {row_identity(r) for r in w.get('ledger', {}).get('rows', [])}
        pts = [r for r in w.get('equity', {}).get('data', {}).get('points', [])
               if start <= timestamp_ms(r['time']) <= end]
        days = {datetime.fromtimestamp(timestamp_ms(r['time']) / 1000, timezone.utc).date().isoformat() for r in pts}
        result['wallets'].append({'address': address, 'accountModeNow': w['accountModeNow'],
            'equityScope': 'main_perp_only_no_spot_or_hip3', 'equityPoints180d': len(pts),
            'equityObservedDates180d': len(days), 'firstEquityTime': pts[0]['time'] if pts else None,
            'historicalAccountModeVerified': False, 'calendarDaysAllTime': len(w.get('calendar', {}).get('data', {}).get('days', [])),
            'ledgerRows180d': len(indexed), 'exactLedgerMatches': len(native & indexed),
            'onlyOfficialLedgerRows': len(native - indexed), 'onlyHypedexerLedgerRows': len(indexed - native),
            'downloadedFills': len(fills), 'duplicateDownloadedRows': len(fills) - len(unique),
            'outOfRequestedWindow': invalid_window, 'fillComparison': dict(statuses),
            'unverifiedExecutionIdSamples': unverified_id_samples,
            'missingClosedPnl': missing_pnl, 'fillPaginationStatus': w.get('fills', {}).get('paginationStatus'),
            'advertisedFills180d': w.get('fills', {}).get('advertisedCount'), 'chainCoverageVerified': False,
            'equityCurveComplete180d': False, 'rankInputsComplete': False})
    result['totals'] = {'wallets': len(result['wallets']),
        'equityPoints180d': sum(w['equityPoints180d'] for w in result['wallets']),
        'calendarDaysAllTime': sum(w['calendarDaysAllTime'] for w in result['wallets']),
        'ledgerRows180d': sum(w['ledgerRows180d'] for w in result['wallets']),
        'exactLedgerMatches': sum(w['exactLedgerMatches'] for w in result['wallets']),
        'downloadedFills': sum(w['downloadedFills'] for w in result['wallets']),
        'advertisedFills180d': sum(w['advertisedFills180d'] or 0 for w in result['wallets']),
        'matchedExecutionFields': sum(w['fillComparison'].get('matched_execution_fields', 0) for w in result['wallets']),
        'missingClosedPnl': sum(w['missingClosedPnl'] for w in result['wallets'])}
    (args.archive / 'audit.json').write_text(json.dumps(result, indent=2, allow_nan=False))
    print(json.dumps(result['totals']))


if __name__ == '__main__':
    main()
