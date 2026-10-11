import pytest

from available_info_rank import provisional_rank
from scripts.rank_available_history import interpolate, unallocated_funding_loss, DAY


def rank(**changes):
    return provisional_rank(**dict(profit_factor=2.5, trend_points=20, episodes=100,
                                   losses=20, **changes))


def test_large_open_loss_vetoes_perfect_observed_performance():
    r = rank(current_open_loss_pct=20)
    assert r['label'] == 'Shadow'
    assert r['availableInfoScore'] == 100
    assert not r['eliteEligible'] and not r['tradeEligible']


def test_sparse_history_cannot_become_elite():
    r = provisional_rank(profit_factor=4, trend_points=20, episodes=5, losses=1)
    assert r['label'] == 'Strong'
    assert r['sampleWarning'] and r['confidence'] == 'Low'
    assert r['verifiedScore'] is None and not r['eliteEligible']


def test_missing_components_are_not_fabricated():
    r = provisional_rank(profit_factor=None, trend_points=None, episodes=0, losses=0)
    assert r['availableInfoScore'] is None and r['originalWeightCoveragePct'] == 0
    r = provisional_rank(profit_factor=None, trend_points=20, episodes=0, losses=0)
    assert r['label'] == 'Weak' and r['originalWeightCoveragePct'] == 20


def test_observed_drawdown_gate_uses_strict_50_boundary():
    assert rank(observed_drawdown_pct=50)['label'] == 'Strong'
    assert rank(observed_drawdown_pct=50.01)['label'] == 'Shadow'


@pytest.mark.parametrize('value', [float('nan'), float('inf'), -1])
def test_invalid_metrics_are_rejected(value):
    with pytest.raises(ValueError):
        rank(current_open_loss_pct=value)


def test_boundary_interpolation_cannot_extrapolate():
    assert interpolate([(0, 10), (10, 30)], 5) == (20, [0, 10])
    with pytest.raises(ValueError):
        interpolate([(0, 10), (10, 30)], 11)


def test_unallocated_daily_funding_loss_cannot_disappear_or_be_double_charged():
    funding = [{'time': DAY, 'delta': {'coin': 'BTC', 'usdc': '-25', 'nSamples': 24}}]
    runs = [{'coin': 'BTC', 'startMs': DAY, 'endMs': DAY + 1000}]
    assert unallocated_funding_loss(runs, funding, 0, 3 * DAY) == 25
    runs = [{'coin': 'BTC', 'startMs': DAY, 'endMs': 2 * DAY}]
    assert unallocated_funding_loss(runs, funding, 0, 3 * DAY) == 0
