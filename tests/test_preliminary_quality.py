from copy import deepcopy

import pytest
import server
from quality_evidence import quality_assessment, format_quality_assessment


def wallet(*, positions=None, profit=300, costs=0, complete=False):
    rank = server.build_risk_trend_quality_rank(
        pnl_7d=10, pnl_30d=30, pnl_180d=100, pnl_all_time=200,
        sortino_180d=1 if complete else None, calmar_180d=3 if complete else None,
        adjusted_profit_factor_180d=3 if complete else None,
        episode_count_180d=40, loss_count_180d=10,
        daily_return_count_180d=90 if complete else 30, downside_day_count_180d=10,
        max_drawdown_pct=10 if complete else None, window_trusted=complete,
        equity_curve_complete=complete, equity_curve_verified=complete,
        largest_loser_pct=1, largest_loser_complete=complete, period_days=90)
    rank.update(closedGrossProfit90d=profit, closedGrossLoss90d=100,
                openCostsLoss90d=costs, pfHistoryComplete=complete,
                currentRiskKnown=True, lossCapitalMethod='fresh_bracket_v1')
    return dict(qualityPeriodDays=90, recentWinRateRank=rank, accountValue=1000,
                positions=positions or [], dataQuality={'stateOk': True})


def assess(w):
    return quality_assessment(w, server.wallet_quality_dimensions(w))


def test_incomplete_history_has_explicit_provisional_number_without_changing_admission():
    w = wallet()
    original = deepcopy(w)
    result = assess(w)
    q = result['quality']
    assert q['score'] == 100 and q['scoreType'] == 'Provisional'
    assert q['status'] == 'Provisional' and q['verifiedScore'] is None
    assert q['preliminary']['originalWeightCoveragePct'] == 40
    assert q['preliminary']['effectiveWeights'] == {'trend': .5, 'adjustedProfitFactor': .5}
    assert q['preliminary']['excludedComponents'] == ['calmar', 'sortino']
    assert not q['preliminary']['profitFactorVerified']
    assert result['data']['status'] == 'Preliminary' and not result['tradeEligible']
    assert not server.wallet_rank_allows_promotion(w)
    assert 'Provisional / Low' in format_quality_assessment(result)
    assert w == original and w['recentWinRateRank']['score'] is None


def test_open_loss_forty_percent_keeps_shadow_and_cannot_become_elite():
    w = wallet(positions=[{'unrealizedPnl': -400}])
    w['recentWinRateRank'] = server.refresh_current_rank_risk(
        w['recentWinRateRank'], open_loss=400, account_value=1000, state_ok=True)
    result = assess(w)
    assert result['quality']['score'] == 50
    assert result['quality']['status'] == 'Shadow'
    assert result['quality']['verifiedScore'] is None
    assert 'current_open_loss' in result['risk']['reasons']
    assert not result['tradeEligible'] and not server.wallet_rank_allows_promotion(w)


def test_open_winners_do_not_offset_open_losers_and_costs_are_included():
    w = wallet(positions=[{'unrealizedPnl': 1000}, {'unrealizedPnl': -100}], costs=100)
    result = assess(w)
    assert result['quality']['preliminary']['observedAdjustedProfitFactor'] == 1
    assert result['quality']['score'] == 50


@pytest.mark.parametrize('bad_profit', [None, float('nan'), -10])
def test_unknown_or_invalid_pf_is_excluded_instead_of_becoming_a_zero(bad_profit):
    result = assess(wallet(profit=bad_profit))
    p = result['quality']['preliminary']
    assert p['originalWeightCoveragePct'] == 20 and p['score'] == 100
    assert 'adjustedProfitFactor' in p['excludedComponents']
    assert p['confidence'] == 'Low'


def test_no_observations_cannot_produce_an_invented_score():
    w = wallet()
    w['recentWinRateRank'].update(dailyReturns90d=0, episodes90d=0)
    assert assess(w)['quality']['score'] is None


def test_verified_full_rank_keeps_its_original_weights_and_status():
    w = wallet(complete=True)
    result = assess(w)
    assert result['quality']['score'] == w['recentWinRateRank']['score'] == 87.5
    assert result['quality']['verifiedScore'] == 87.5
    assert result['quality']['scoreType'] == 'Verified'
    assert result['quality']['preliminary'] is None


def test_telegram_ranking_orders_by_displayed_preliminary_score():
    low, high = wallet(profit=100), wallet(profit=300)
    for w, alias, address in ((low, 'Low', '0x1111'), (high, 'High', '0x2222')):
        w.update(alias=alias, address=address, qualityAssessment=assess(w))
    service = server.WalletTrackerService.__new__(server.WalletTrackerService)
    text = service.build_wallet_rankings_message({'wallets': [low, high]})
    assert text.index('1. High') < text.index('2. Low')
    assert '100.0/100' in text and '50.0/100' in text
