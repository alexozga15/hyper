#!/usr/bin/env python3
"""Rank frozen public archives offline, with explicit incomplete-data status."""
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
import server
from available_info_rank import provisional_rank
from equity_history import observed_perp_history, paired_points
from scripts.audit_wallet_capital import join_observations

DAY = 86_400_000


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def interpolate(points, at):
    """A trend estimate only. Never create synthetic daily equity returns."""
    points = sorted(points)
    for t, value in points:
        if t == at:
            return value, [t, t]
    for (t0, p0), (t1, p1) in zip(points, points[1:]):
        if t0 < at < t1:
            return p0 + (p1 - p0) * (at - t0) / (t1 - t0), [t0, t1]
    raise ValueError('Unbracketed trend boundary')


def unallocated_funding_loss(runs, funding, start, end):
    """Retain known negative funding omitted by uncertain episode allocation."""
    by_coin = defaultdict(list)
    for r in runs:
        by_coin[r['coin']].append((r['startMs'], r['endMs']))
    loss = 0.0
    for r in funding:
        delta = r['delta']
        amount = float(delta.get('usdc', 0))
        if amount >= 0 or not start <= r['time'] <= end:
            continue
        first = r['time']
        aggregate = delta.get('nSamples') is not None
        last = first + DAY if aggregate else first
        assigned = any(a <= first and (last <= b if aggregate else first < b)
                       for a, b in by_coin[delta['coin']])
        if not assigned:
            loss -= amount
    return loss


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--reports', type=Path, default=ROOT / 'reports')
    p.add_argument('--output', type=Path, required=True)
    args = p.parse_args()
    capital = args.reports / 'hyperliquid-capital-2026-10-11'
    reconciliation = args.reports / 'hyperdash-reconciliation-2026-10-10'
    staged = reconciliation / 'staged'
    recon = json.loads((staged / 'reconciliation.json').read_text())
    manifest = json.loads((capital / 'manifest.json').read_text())
    primary_path = reconciliation / 'primary-history.jsonl.gz'
    if sha(primary_path) != recon['primarySha256']:
        raise ValueError('Primary archive checksum mismatch')
    tail = defaultdict(lambda: defaultdict(list))
    equity = defaultdict(dict)
    primary_metadata = None
    with gzip.open(primary_path, 'rt') as inp:
        for line in inp:
            r = json.loads(line)
            if r['type'] == 'metadata':
                primary_metadata = r
            elif r['type'] == 'equity':
                equity[r['address']][r['at']] = (r['account'], r['pnl'])
            elif r['type'] == 'history' and r['at'] > recon['endMs']:
                kind, payload = r['kind'], r['payload']
                if kind == 'twap':
                    kind, payload = 'fills', payload['fill']
                tail[r['address']][kind].append(payload)
    addresses = {w['address'] for w in manifest['wallets']}
    streams = [s for s in primary_metadata['streams'] if s['address'] in addresses]
    if any(not any(s['address'] == a and s['kind'] == kind for s in streams)
           for a in addresses for kind in ('fills', 'funding', 'twap')):
        raise ValueError('Missing primary stream')
    end = min(s['completeUntil'] for s in streams)
    start = end - 180 * DAY
    result = dict(generatedAt=datetime.now(timezone.utc).isoformat(), startMs=start, endMs=end,
                  currentRiskSnapshotDate='2026-10-11', wallets=[], productionImportPerformed=False,
                  methodology='PF/trend 50/50 on available observations; original coverage 40%; no Elite',
                  sourceHashes={'primary': sha(primary_path), 'reconciliation': sha(staged / 'reconciliation.json'),
                                'capitalManifest': sha(capital / 'manifest.json')})
    recon_by_address = {w['address']: w for w in recon['wallets']}
    for item in manifest['wallets']:
        address = item['address']
        source_path = staged / (address + '.jsonl.gz')
        official_path = capital / (address + '.json')
        if sha(source_path) != recon_by_address[address]['stagedSha256'] or sha(official_path) != item['sha256']:
            raise ValueError('Wallet archive checksum mismatch')
        fills, funding = [], []
        with gzip.open(source_path, 'rt') as inp:
            for line in inp:
                r = json.loads(line)
                if r['type'] == 'fill':
                    fills.append(r['payload'])
                elif r['type'] == 'funding':
                    funding.append(r['payload'])
        fills.extend(tail[address]['fills'])
        fills = list({server.raw_fill_identity(r): r for r in fills if r['time'] <= end}.values())
        funding.extend(tail[address]['funding'])
        funding = list({json.dumps(r, sort_keys=True): r for r in funding if r['time'] <= end}.values())
        diagnostics = {}
        runs = server.reconstruct_position_episodes(fills, recon['startMs'], funding,
                                                   include_open=True, diagnostics=diagnostics)
        closed = [r for r in runs if not r.get('open') and start <= r['endMs'] <= end]
        good = [r for r in closed if r.get('anchored') and r.get('historyComplete')
                and not r.get('fundingAllocationAmbiguous')]
        # Incomplete winners receive no PF credit. Every observed negative
        # episode remains a cost, so filtering cannot erase a known loser.
        profit = sum(max(0, r['pnl']) for r in good)
        loss = sum(max(0, -r['pnl']) for r in closed)
        open_costs = sum(max(0, -r['pnl']) for r in runs if r.get('open'))
        unassigned_funding_loss = unallocated_funding_loss(runs, funding, start, end)
        official = json.loads(official_path.read_text())
        portfolio = dict(official['portfolio']['data'])
        curve = observed_perp_history(portfolio)
        points, join = join_observations(equity[address], paired_points(curve))
        if join['status'] != 'matched_shared_anchors':
            raise ValueError('Unverified PnL origin join')
        pnl = sorted((t, values[1]) for t, values in points.items())
        boundaries = [end, end - 7 * DAY, end - 30 * DAY, start]
        estimates = [interpolate(pnl, t) for t in boundaries]
        values = [v for v, bracket in estimates]
        segments = dict(pnl7d=values[0] - values[1], pnl8to30d=values[1] - values[2],
                        pnl31to180d=values[2] - values[3], olderPnl=values[3])
        trend = sum(weight for weight, k in [(4, 'pnl7d'), (8, 'pnl8to30d'), (8, 'pnl31to180d')] if segments[k] > 0)
        states = official['allDexsClearinghouseState']['data']['clearinghouseStates']
        positions = [r['position'] for _, state in states for r in state.get('assetPositions', [])]
        open_losses = [max(0, -float(r['unrealizedPnl'])) for r in positions]
        open_loss = sum(open_losses)
        # Unified/portfolio margin balances must not be summed per DEX.
        # Total portfolio equity is only a broad denominator: the resulting
        # ratio is an indicative lower risk estimate, not collateral proof.
        combined = portfolio['allTime']['accountValueHistory'][-1]
        capital_value = float(combined[1])
        risk_pct = open_loss / capital_value * 100 if capital_value > 0 else None
        largest_open_pct = max(open_losses, default=0) / capital_value * 100 if capital_value > 0 else None
        denom = loss + open_costs + open_loss + unassigned_funding_loss
        pf = profit / denom if denom > 0 else None  # No infinite PF from an unobserved loss tail.
        measured = sorted((t, a, p) for t, (a, p) in points.items() if start <= t <= end)
        metrics = server.daily_equity_metrics_180d([[t,p] for t,a,p in measured],
                                                  [[t,a] for t,a,p in measured], start, end)
        loss_metrics = server.episode_loss_metrics(good, [[t,a] for t,a,p in measured],
                                                   [[t,p] for t,a,p in measured])
        n_losses = sum(r['pnl'] < 0 for r in good)
        rank = provisional_rank(profit_factor=pf, trend_points=trend, episodes=len(good), losses=n_losses,
                                observed_drawdown_pct=metrics['observedMaxDrawdownPct'],
                                largest_loser_pct=loss_metrics['largestLoserPct'],
                                current_open_loss_pct=risk_pct, largest_open_loss_pct=largest_open_pct)
        result['wallets'].append(dict(address=address, **rank, adjustedObservedPF=pf,
            trendPoints=trend, trendSegmentsUsd=segments, trendBoundaryBracketsMs=dict(zip(map(str,boundaries),[b for v,b in estimates])),
            pnlIncludesOpen=True, pnlTrendEstimated=True, historicalModeVerified=False,
            closedEpisodesObserved=len(closed), usableEpisodes=len(good), usableLosses=n_losses,
            excludedPositiveEpisodeCount=sum(r['pnl'] > 0 for r in closed if r not in good),
            closedProfitCreditedUsd=profit, closedLossObservedUsd=loss, openCostsObservedUsd=open_costs,
            unallocatedFundingLossUsd=unassigned_funding_loss,
            currentOpenLossUsd=open_loss, currentOpenLossPctOfTotalPortfolio=risk_pct,
            largestOpenLossPctOfTotalPortfolio=largest_open_pct, riskCapitalUsd=capital_value,
            riskCapitalAtMs=combined[0], riskStateObservedAt=official['allDexsClearinghouseState']['observedAt'],
            observedDrawdownPct=metrics['observedMaxDrawdownPct'], largestLoser=loss_metrics,
            episodeDiagnostics=diagnostics, pfHistoryVerified=False,
            sourceHashes={'staged': sha(source_path), 'capital': sha(official_path)}))
        print(address, rank['label'], rank['availableInfoScore'], 'PF',round(pf,3) if pf is not None else None, flush=True)
    order = {'Strong': 0, 'Balanced': 1, 'Weak': 2, 'Cold': 3, 'Shadow': 4, 'InsufficientData': 5}
    result['wallets'].sort(key=lambda w:(order[w['label']], -(w['availableInfoScore'] or 0), w['address']))
    for n, w in enumerate(result['wallets'], 1):
        w['position'] = n
    result['totals'] = dict(Counter(w['label'] for w in result['wallets']))
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / 'rankings.json').write_text(json.dumps(result, indent=2, allow_nan=False) + '\n')
    seed = dict(version=1, endMs=end, expiresAtMs=end + 72 * 60 * 60 * 1000,
                generatedAt=result['generatedAt'], sourceHashes=result['sourceHashes'],
                wallets={w['address']: w for w in result['wallets']})
    (args.output / 'runtime-seed.json').write_text(json.dumps(seed, indent=2, allow_nan=False) + '\n')
    print(json.dumps(result['totals']))


if __name__ == '__main__':
    main()
