"""Explicitly provisional scores; never substitute for verified 180-day ranks."""
import math

METRIC = 'available_pf_trend_v1'
MAX_HISTORY_AGE_MS = 72 * 60 * 60 * 1000


def provisional_rank(*, profit_factor, trend_points, episodes, losses,
                     observed_drawdown_pct=None, largest_loser_pct=None,
                     current_open_loss_pct=None, largest_open_loss_pct=None):
    def number(value):
        if value is None:
            return None
        value = float(value)
        if not math.isfinite(value):
            raise ValueError('Non-finite ranking input')
        return value

    pf = number(profit_factor)
    trend = number(trend_points)
    if pf is not None and pf < 0:
        raise ValueError('Negative profit factor')
    if trend is not None and not 0 <= trend <= 20:
        raise ValueError('Trend points must be within 0..20')
    components = []
    if pf is not None:
        components.append((20, min(100, max(0, (pf - 1) / 1.5 * 100))))
    if trend is not None:
        components.append((20, trend * 5))
    coverage = sum(weight for weight, _ in components)
    raw_score = sum(weight * score for weight, score in components) / coverage if coverage else None
    score = None if raw_score is None else round(raw_score, 1)
    gates = []
    for key, value, threshold, inclusive in (
        ('observed_drawdown', observed_drawdown_pct, 50, False),
        ('largest_realized_loser', largest_loser_pct, 20, True),
        ('largest_open_loser', largest_open_loss_pct, 20, True),
        ('aggregate_open_loss', current_open_loss_pct, 20, True),
    ):
        value = number(value)
        if value is not None and value < 0:
            raise ValueError('Negative risk percentage')
        if value is not None and (value >= threshold if inclusive else value > threshold):
            gates.append(key)
    if gates:
        label = 'Shadow'
    elif raw_score is None:
        label = 'InsufficientData'
    else:
        label = 'Strong' if raw_score >= 70 else 'Balanced' if raw_score >= 55 else 'Weak' if raw_score >= 40 else 'Cold'
        # A single component alone does not support a positive quality label.
        if coverage < 40 and label in ('Strong', 'Balanced'):
            label = 'Weak'
    return dict(label=label, availableInfoScore=score, originalWeightCoveragePct=coverage,
                effectiveWeights={'profitFactor': 20 / coverage if pf is not None else 0,
                                  'trend': 20 / coverage if trend is not None else 0},
                confidence='Low', assessmentStatus='Provisional', eliteEligible=False,
                verifiedScore=None, tradeEligible=False, shadowReasons=gates,
                sampleGatePassed=episodes >= 40 and losses >= 10,
                sampleWarning=episodes < 40 or losses < 10,
                metric=METRIC)


def underlying_rank(rank):
    if isinstance(rank, dict) and rank.get('metric') == METRIC:
        return rank.get('underlying180dRank') or {}
    return rank


def active_seed(payload, address, now_ms):
    """Do not let a reload or regenerated timestamp renew historical evidence."""
    try:
        end = int(payload['endMs'])
        expires = int(payload['expiresAtMs'])
        rows = payload['wallets']
        if payload.get('version') != 1 or not isinstance(rows, dict):
            return None
        if not 0 < end <= now_ms < expires <= end + MAX_HISTORY_AGE_MS:
            return None
        row = rows.get(address.lower())
        if not isinstance(row, dict) or row.get('metric') != METRIC:
            return None
        row = dict(row)
        fields = ('closedProfitCreditedUsd', 'closedLossObservedUsd', 'openCostsObservedUsd',
                  'unallocatedFundingLossUsd', 'usableEpisodes', 'usableLosses', 'trendPoints')
        for field in fields:
            value = float(row[field])
            if not math.isfinite(value) or value < 0:
                return None
            row[field] = value
        if any(not row[k].is_integer() for k in ('usableEpisodes', 'usableLosses')):
            return None
        row['usableEpisodes'], row['usableLosses'] = int(row['usableEpisodes']), int(row['usableLosses'])
        row['largestLoser'] = dict(row.get('largestLoser') or {})
        for container, key in ((row, 'observedDrawdownPct'), (row['largestLoser'], 'largestLoserPct')):
            risk_value = container.get(key)
            if risk_value is not None and (not math.isfinite(float(risk_value)) or float(risk_value) < 0):
                return None
            if risk_value is not None:
                container[key] = float(risk_value)
        source_score = float(row['availableInfoScore'])
        if not math.isfinite(source_score) or not 0 <= source_score <= 100:
            return None
        row['availableInfoScore'] = source_score
        segments = row.get('trendSegmentsUsd')
        if segments is not None:
            row['trendSegmentsUsd'] = {key: float(segments[key])
                for key in ('pnl7d', 'pnl8to30d', 'pnl31to180d', 'olderPnl')}
            if not all(math.isfinite(v) for v in row['trendSegmentsUsd'].values()):
                return None
        if float(row['trendPoints']) > 20 or row['usableLosses'] > row['usableEpisodes']:
            return None
        return {**row, 'historyThroughMs': end, 'expiresAtMs': expires}
    except (KeyError, TypeError, ValueError, OverflowError, AttributeError):
        return None


def apply_live_rank(base, seed, *, now_ms, open_loss, largest_open_loss,
                    total_portfolio_equity, risk_ok):
    base = underlying_rank(base) or {}
    if (not seed or now_ms >= seed['expiresAtMs'] or
            (base.get('rankable') and base.get('assessmentStatus') == 'Verified')):
        return base
    risk_ok = bool(risk_ok and math.isfinite(total_portfolio_equity) and total_portfolio_equity > 0
                   and math.isfinite(open_loss) and open_loss >= 0
                   and math.isfinite(largest_open_loss) and 0 <= largest_open_loss
                   <= open_loss + max(1e-9, abs(open_loss) * 1e-12))
    pct = open_loss / total_portfolio_equity * 100 if risk_ok else None
    largest_pct = largest_open_loss / total_portfolio_equity * 100 if risk_ok else None
    denom = (seed['closedLossObservedUsd'] + seed['openCostsObservedUsd']
             + seed['unallocatedFundingLossUsd'] + open_loss) if risk_ok else 0
    pf = seed['closedProfitCreditedUsd'] / denom if denom > 0 else None
    observed_dd = seed.get('observedDrawdownPct')
    live_dd = base.get('observedDrawdownPct')
    if live_dd is not None:
        observed_dd = max(observed_dd or 0, live_dd)
    measured_loser = (seed.get('largestLoser') or {}).get('largestLoserPct')
    if base.get('lossCapitalMethod') == 'fresh_bracket_v1' and base.get('largestLoserPct') is not None:
        measured_loser = max(measured_loser or 0, base['largestLoserPct'])
    rank = provisional_rank(profit_factor=pf, trend_points=seed['trendPoints'],
                            episodes=seed['usableEpisodes'], losses=seed['usableLosses'],
                            observed_drawdown_pct=observed_dd, largest_loser_pct=measured_loser,
                            current_open_loss_pct=pct, largest_open_loss_pct=largest_pct)
    if not risk_ok:
        rank.update(label='Shadow' if rank['shadowReasons'] or seed.get('label') == 'Shadow' else 'Unranked',
                    availableInfoScore=None)
    score = rank['availableInfoScore']
    # Only reduce influence. Unverified observations do not earn a boost.
    weight = 0 if score is None or rank['label'] == 'Shadow' else round(
        0.6 + 0.4 * min(1, max(0.5, score / 70)), 3)
    segments = seed.get('trendSegmentsUsd') or {}
    trend_fields = {}
    if segments:
        pnl7, pnl8to30, pnl31to180 = (segments[k] for k in ('pnl7d', 'pnl8to30d', 'pnl31to180d'))
        trend_fields = dict(pnl=pnl7, pnl30d=pnl7+pnl8to30, pnl180d=pnl7+pnl8to30+pnl31to180,
                            pnlAllTime=pnl7+pnl8to30+pnl31to180+segments['olderPnl'],
                            positiveTrend=pnl7 > 0 and pnl8to30 > 0 and pnl31to180 > 0,
                            allTimeControl=segments['olderPnl'] >= 0,
                            trendChecks={'pnl7dPositive': pnl7 > 0, 'pnl30dAbove7d': pnl8to30 > 0,
                                         'pnl180dAbove30d': pnl31to180 > 0})
    return {**base, **rank, **trend_fields, 'underlying180dRank': base, 'score': score,
            'convictionWeightScore': score, 'provisionalConvictionWeight': weight,
            'rankable': False, 'currentRiskKnown': risk_ok,
            'historyThroughMs': seed['historyThroughMs'], 'expiresAtMs': seed['expiresAtMs'],
            'adjustedObservedPF': pf, 'trendScore': seed['trendPoints'],
            'currentOpenLossPctOfTotalPortfolio': pct,
            'riskCapitalMethod': 'fresh_total_portfolio_indicator',
            'riskRefreshedAtMs': now_ms if risk_ok else None,
            'observedDrawdownPct': observed_dd, 'largestLoserPct': measured_loser,
            'largestLoserComplete': False, 'pfHistoryComplete': False,
            'scoreComponentsAvailable': {'calmar': False, 'sortino': False,
                                         'adjustedProfitFactor': pf is not None, 'trend': True},
            'trendSource': 'official_perp_archive_interpolated_fixed_cutoff',
            'episodesObserved': seed['usableEpisodes'], 'lossesObserved': seed['usableLosses'],
            'historicalTrendEstimated': True,
            'sourceScore': seed['availableInfoScore']}
