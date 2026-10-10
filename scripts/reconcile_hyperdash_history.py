#!/usr/bin/env python3
"""Build a local, source-labelled staging tape; never edit production caches."""
import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
import gzip
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from fill_order import order_position_fills
from hyperdash_reconciliation import (DAY, HOUR, funding_hour_index,
                                     reconcile_fills, reconcile_funding_event)
import server


def emit(out, row):
    out.write(json.dumps(row, separators=(',', ':'), allow_nan=False) + '\n')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--hyperdash', required=True, type=Path)
    parser.add_argument('--primary', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    manifest = json.loads((args.hyperdash / 'manifest.json').read_text())
    start, end = manifest['startMs'], manifest['endMs']
    primary, equity, ranks = defaultdict(lambda: defaultdict(dict)), defaultdict(list), {}
    metadata = None
    with gzip.open(args.primary, 'rt') as inp:
        for line in inp:
            row = json.loads(line)
            address = row.get('address')
            if row['type'] == 'metadata':
                metadata = row
            elif row['type'] == 'history':
                kind, payload = row['kind'], row['payload']
                if kind == 'twap':
                    kind, payload = 'fills', payload['fill']
                if not start <= int(payload['time']) <= end:
                    continue
                key = server.raw_fill_identity(payload) if kind == 'fills' else json.dumps(payload, sort_keys=True)
                primary[address][kind][key] = payload
            elif row['type'] == 'equity':
                equity[address].append(row)
            elif row['type'] == 'quality':
                ranks[address] = row.get('rank') or {}
    args.output.mkdir(parents=True, exist_ok=True)
    results = {'generatedAt': datetime.now(timezone.utc).isoformat(), 'startMs': start, 'endMs': end,
               'primaryExportedAt': metadata['exportedAt'], 'wallets': [],
               'primarySha256': hashlib.sha256(args.primary.read_bytes()).hexdigest(),
               'newVerifiedRanks': 0, 'productionImportPerformed': False}
    for item in manifest['wallets']:
        address = item['address']
        path = args.hyperdash / (address + '.json')
        if hashlib.sha256(path.read_bytes()).hexdigest() != item['sha256']:
            raise ValueError('Archive checksum mismatch: ' + address)
        data = json.loads(path.read_text())
        tape, decisions = reconcile_fills(list(primary[address]['fills'].values()), data['rawFills']['fills'])
        fill_stats = Counter(row['evidence'] for row in tape)
        group_stats = Counter(row['evidence'] for row in decisions)
        index = funding_hour_index(data['funding'])
        funding, funding_decisions, blocked_hours = {}, [], set()
        funding_stats = Counter()
        for event in primary[address]['funding'].values():
            expanded, status = reconcile_funding_event(event, index)
            funding_stats[status] += 1
            coin, hour = event['delta']['coin'], int(event['time']) // HOUR
            if event['delta'].get('nSamples') is not None and int(event['time']) % DAY == 0:
                blocked_hours.update((coin, h) for h in range(hour, hour + 24))
            else:
                blocked_hours.add((coin, hour))
            if expanded:
                for row in expanded:
                    if start <= int(row['time']) <= end:
                        funding[(coin, int(row['time']))] = dict(evidence=status, payload=row)
            elif event['delta'].get('nSamples') is None:
                funding[(coin, int(event['time']))] = dict(evidence='primary_hourly_unmatched', payload=event)
            funding_decisions.append(dict(status=status, primary=event, expandedRows=len(expanded)))
        for event in data['funding']:
            if not start <= int(event['time']) <= end:
                continue
            coin, at = event['delta']['coin'], int(event['time'])
            if (coin, at // HOUR) not in blocked_hours:
                funding[(coin, at)] = dict(evidence='hyperdash_supplemental', payload=event)
        funding_evidence = Counter(row['evidence'] for row in funding.values())
        ordered, ordering = order_position_fills([row['payload'] for row in tape])
        diagnostics = {}
        episodes = server.reconstruct_position_episodes(ordered, start,
                    [row['payload'] for row in funding.values()], include_open=True, diagnostics=diagnostics)
        points = [row for row in equity[address] if start <= row['at'] <= end]
        metrics = server.daily_equity_metrics_180d([[r['at'],r['pnl']] for r in points],
                    [[r['at'],r['account']] for r in points], start, end)
        # The primary equity curve remains separate. External pnlGraph is not
        # paired with a different book's capital history to manufacture returns.
        result = dict(address=address, fillRows=len(tape), fillEvidence=dict(fill_stats),
            fillGroupDecisions=dict(group_stats), fundingRows=len(funding), fundingEvidence=dict(funding_evidence),
            fundingDecisions=dict(funding_stats), orderingDiagnostics=ordering,
            episodeDiagnostics=diagnostics, episodes=len(episodes),
            anchoredContinuousEpisodes=sum(bool(r.get('anchored')) and bool(r.get('historyComplete')) for r in episodes),
            equityPoints=len(points), equityMetrics=metrics,
            currentRankBeforeDeployment=ranks.get(address),
            rankEligible=False, reasons=['source_coverage_not_verified', 'historical_capital_incomplete'])
        staged = args.output / (address + '.jsonl.gz')
        with gzip.open(staged, 'wt') as out:
            emit(out, dict(type='metadata', address=address, startMs=start, endMs=end,
                           rankEligible=False, sourceArchiveSha256=item['sha256']))
            evidence = {server.raw_fill_identity(row['payload']): row['evidence'] for row in tape}
            for row in ordered:
                emit(out, dict(type='fill', evidence=evidence[server.raw_fill_identity(row)], payload=row))
            for row in sorted(funding.values(), key=lambda r: r['payload']['time']):
                emit(out, dict(type='funding', **row))
            for row in decisions:
                if row['evidence'] == 'primary_disputed_group':
                    emit(out, dict(type='fill_dispute', **row))
            for row in funding_decisions:
                if not row['status'].startswith('matched_'):
                    emit(out, dict(type='funding_unresolved', **row))
        result['stagedSha256'] = hashlib.sha256(staged.read_bytes()).hexdigest()
        results['wallets'].append(result)
        print(address, 'fills',len(tape), 'groups',dict(group_stats), 'funding',dict(funding_stats),flush=True)
    results['totals'] = {
        'matchedFillGroups': sum(w['fillGroupDecisions'].get('matched_fill_group',0) for w in results['wallets']),
        'matchedRawFills': sum(w['fillEvidence'].get('matched_fill_group',0) for w in results['wallets']),
        'disputedFillGroups': sum(w['fillGroupDecisions'].get('primary_disputed_group',0) for w in results['wallets']),
        'matchedFundingSummaries': sum(w['fundingDecisions'].get('matched_summary_split',0) for w in results['wallets']),
        'matchedHourlyFunding': sum(w['fundingDecisions'].get('matched_hourly',0) for w in results['wallets']),
        'corroboratedHourlyFunding': sum(w['fundingEvidence'].get('matched_hourly',0)+w['fundingEvidence'].get('matched_summary_split',0) for w in results['wallets']),
        'completeEquityCurves': sum(w['equityMetrics']['equityCurveComplete'] for w in results['wallets']),
    }
    (args.output / 'reconciliation.json').write_text(json.dumps(results,indent=2,allow_nan=False))
    print(json.dumps(results['totals']))


if __name__ == '__main__':
    main()
