import server
from fill_order import order_position_fills


def fill(at, tid, start, size, side, pnl=0):
    return dict(time=at, tid=tid, coin='ETH', startPosition=str(start), sz=str(size),
                side=side, dir='Close Long' if side == 'A' else 'Open Long',
                closedPnl=str(pnl), fee='1', px='100')


def test_hashed_ids_cannot_split_one_close_into_two_episodes():
    rows = [fill(1, 1, 0, 2, 'B'), fill(2, 90, 2, 1, 'A', -5), fill(2, 3, 1, 1, 'A', -7)]
    ordered, diagnostics = order_position_fills(rows)
    assert [r['tid'] for r in ordered] == [1, 90, 3]
    assert diagnostics == {}
    episodes = server.reconstruct_position_episodes(rows, 0)
    assert len(episodes) == 1
    assert episodes[0]['historyComplete'] is True
    assert episodes[0]['pnl'] == -15


def test_missing_execution_cannot_be_repaired_by_sorting():
    rows = [fill(1, 1, 0, 2, 'B'), fill(2, 90, 2, '.5', 'A'), fill(2, 3, 1, 1, 'A')]
    _, diagnostics = order_position_fills(rows)
    assert diagnostics['ambiguousFillOrder'] == 1
    diagnostics = {}
    server.reconstruct_position_episodes(rows, 0, include_open=True, diagnostics=diagnostics)
    assert diagnostics['ambiguousFillOrder'] == 1


def test_cycle_without_previous_position_is_ambiguous():
    rows = [fill(1, 1, 0, 1, 'B'), fill(1, 2, 1, 1, 'A')]
    assert order_position_fills(rows)[1]['ambiguousFillOrder'] == 1
    assert server.reconstruct_position_episodes(rows, 0)[0]['historyComplete'] is False
    assert '_positionOrderUnverified' not in rows[0]


def test_cycle_can_use_a_preceding_position_observation():
    rows = [fill(0, 50, 1, 1, 'A'), fill(1, 90, 0, 1, 'B'), fill(1, 1, 1, 1, 'A')]
    ordered, diagnostics = order_position_fills(rows)
    assert [r['tid'] for r in ordered] == [50, 90, 1]
    assert diagnostics == {}


def test_decimal_noise_is_not_silently_rounded_into_continuity():
    rows = [fill(1, 1, 0, '1.000000001', 'B'), fill(1, 2, 1, 1, 'A')]
    ordered, _ = order_position_fills(rows)
    assert ordered[0]['tid'] == 2  # Actual unique route closes a pre-existing position first.
    assert server.reconstruct_position_episodes(rows, 0)[0]['anchored'] is False


def test_daily_funding_summary_cannot_prove_an_earlier_episode_pf():
    start = 86_400_000
    episode = dict(coin='ETH', startMs=start+3600000, endMs=start+22*3600000, pnl=10)
    summary = dict(time=start, delta=dict(type='funding', coin='ETH', usdc='24', nSamples=24))
    result = server.apply_funding_to_position_episodes([episode], [summary])[0]
    assert result['fundingAllocationAmbiguous'] is True
    assert result['fundingUsd'] == 0


def test_summary_for_other_coin_or_day_does_not_veto_pf():
    episode = dict(coin='ETH', startMs=1, endMs=100, pnl=10)
    rows = [dict(time=86_400_000, delta=dict(type='funding', coin='BTC', usdc='24', nSamples=24)),
            dict(time=3*86_400_000, delta=dict(type='funding', coin='ETH', usdc='24', nSamples=24))]
    assert server.apply_funding_to_position_episodes([episode], rows)[0]['fundingAllocationAmbiguous'] is False


def test_whole_daily_bucket_inside_episode_can_be_allocated_exactly():
    day = 86_400_000
    episode = dict(coin='ETH', startMs=day-1, endMs=2*day, pnl=10)
    summary = dict(time=day, delta=dict(type='funding', coin='ETH', usdc='24', nSamples=24))
    result = server.apply_funding_to_position_episodes([episode], [summary])[0]
    assert result['fundingUsd'] == 24
    assert result['pnl'] == 34
    assert result['fundingAllocationAmbiguous'] is False


def test_one_sample_daily_summary_is_still_not_a_precise_hourly_event():
    day = 86_400_000
    episode = dict(coin='ETH', startMs=day, endMs=day+3600000, pnl=10)
    summary = dict(time=day, delta=dict(type='funding', coin='ETH', usdc='5', nSamples=1))
    result = server.apply_funding_to_position_episodes([episode], [summary])[0]
    assert result['fundingUsd'] == 0
    assert result['fundingAllocationAmbiguous'] is True
