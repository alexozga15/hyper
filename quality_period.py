"""Explicit period identity; legacy 180D evidence is never relabelled 90D."""
import re

QUALITY_PERIOD_DAYS = 90
WINDOW_INDEPENDENT_CACHE_FIELDS = frozenset({
    'role', 'performance', 'openOrderCount', 'openOrders', 'realizedPnl',
    'recentRealizedPnl', 'realizedPnl30d', 'recentWins', 'recentLosses',
    'recentClosedTrades', 'closedTrades30d', 'grossProfit30d', 'grossLoss30d',
    'winRate90d', 'closedTrades90d', 'qualityClosedEvents30d', 'qualityNetPnl30d',
    'qualityProfitFactor30d', 'fundingPnl30d', 'fundingEvents30d',
    'qualityTopWinConcentrationPct', 'qualityHoldout6dEvents', 'qualityHoldout6dNetPnl',
    'qualityWindowTruncated', 'qualityWindowFillCount', 'qualityWindowCoverageMs',
    'fills30d', 'daysSinceLastFill', 'holdingOnly30d', 'assetQuality', 'hitRate',
})


def rank_period_days(rank):
    rank = rank if isinstance(rank, dict) else {}
    if rank.get('underlying180dRank'):
        return rank_period_days(rank['underlying180dRank'])
    value = rank.get('periodDays')
    if value is not None:
        return int(value)
    match = re.search(r'_(\d+)d(?:_|$)', str(rank.get('metric', '')))
    return int(match.group(1)) if match else 180


def period_key(legacy_key, days):
    return legacy_key.replace('180d', f'{days}d')


def period_fields(values, days):
    """Used only for newly calculated values, never for converting old evidence."""
    result = {period_key(k, days): v for k, v in values.items()}
    if 'trendChecks' in result:
        result['trendChecks'] = {period_key(k, days): v for k, v in result['trendChecks'].items()}
    return result


def period_metric(days):
    return f'calmar_sortino_adjusted_pf_trend_{days}d_v{2 if days == 180 else 3}'


def cache_period_matches(cached, days=QUALITY_PERIOD_DAYS):
    if not isinstance(cached, dict):
        return False
    return int(cached.get('qualityPeriodDays') or rank_period_days(cached.get('recentWinRateRank'))) == days


def hold_previous_window_risk(rank, previous, days=QUALITY_PERIOD_DAYS):
    """A period migration cannot erase a confirmed veto using incomplete data."""
    previous = previous if isinstance(previous, dict) else {}
    reasons = previous.get('previousWindowRiskReasons') or []
    if rank_period_days(previous) != days and previous.get('label') == 'Shadow':
        reasons = previous.get('shadowReasons') or ['previous_window_shadow']
    if reasons and not (rank.get('equityCurveVerified') and rank.get('largestLoserComplete')
                        and rank.get('pfHistoryComplete') and rank.get('currentRiskKnown')):
        return {**rank, 'label': 'Shadow', 'eliteEligible': False,
                'previousWindowRiskReasons': list(reasons),
                'shadowReasons': list(dict.fromkeys([*(rank.get('shadowReasons') or []),
                                                     'previous_window_risk_pending_reassessment']))}
    return rank
