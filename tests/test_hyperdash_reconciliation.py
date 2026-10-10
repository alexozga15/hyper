from copy import deepcopy
from decimal import InvalidOperation
import pytest

from hyperdash_reconciliation import (compare_fill_groups, funding_hour_index,
                                     reconcile_fills, reconcile_funding_event)


def fill(**changes):
    return dict(coin='xyz:ETH', time=1, oid=1, hash='block', side='B', dir='Open Long',
                feeToken='USDC', tid=1, sz='2', fee='1', closedPnl='0', px='100', **changes)


def test_aggregated_execution_matches_sum_not_one_tid():
    p = fill()
    a, b = deepcopy(p), deepcopy(p)
    a.update(sz='1', fee='.5')
    b.update(sz='1', fee='.5', tid=2)
    b['hash'] = 'another-transaction-in-same-block'
    assert compare_fill_groups([p], [a, b])[0]
    assert not compare_fill_groups([p], [a])[0]
    tape, _ = reconcile_fills([p], [a, b])
    assert len(tape) == 2
    assert all(r['evidence'] == 'matched_fill_group' for r in tape)


def test_dispute_preserves_primary_group_without_adding_constituents():
    p, e = fill(), fill()
    e['sz'] = '1'
    tape, decisions = reconcile_fills([p], [e])
    assert len(tape) == 1 and tape[0]['payload']['sz'] == '2'
    assert decisions[0]['evidence'] == 'primary_disputed_group'


def test_builder_fee_is_already_inside_total_fee():
    p, e = fill(), fill()
    p['builderFee'] = '.8'
    assert compare_fill_groups([p], [e])[0]


def test_same_order_id_does_not_merge_different_venues():
    p, e = fill(), fill()
    e['coin'] = 'ETH'
    assert not compare_fill_groups([p], [e])[0]
    tape, _ = reconcile_fills([p], [e])
    assert {r['evidence'] for r in tape} == {'primary_only', 'hyperdash_supplemental'}


def test_equal_sums_without_shared_identifier_do_not_prove_a_match():
    p, e = fill(), fill()
    e['tid'] = 999
    assert not compare_fill_groups([p], [e])[0]


def test_nonfinite_data_is_rejected():
    p = fill()
    p['fee'] = 'NaN'
    with pytest.raises((ValueError, InvalidOperation)):
        compare_fill_groups([p], [fill()])


def event(at, amount, samples=None):
    return dict(time=at, delta=dict(type='funding', coin='ETH', usdc=str(amount),
                                   szi='1', fundingRate='.001', nSamples=samples))


def test_daily_summary_expands_nonconsecutive_samples_inside_its_utc_day():
    day = 86_400_000
    rows = [event(day+2*3600000, 1), event(day+20*3600000, 2)]
    expanded, status = reconcile_funding_event(event(day, 3, 2), funding_hour_index(rows))
    assert expanded == rows and status == 'matched_summary_split'


def test_missing_sample_cannot_be_filled_from_the_previous_day():
    day = 86_400_000
    rows = [event(day-3600000, 1), event(day+20*3600000, 2)]
    expanded, status = reconcile_funding_event(event(day, 3, 2), funding_hour_index(rows))
    assert not expanded and status == 'missing_samples'


def test_summary_with_equal_count_but_wrong_cashflow_stays_unmatched():
    day = 86_400_000
    rows = [event(day+2*3600000, 1), event(day+20*3600000, 2)]
    assert reconcile_funding_event(event(day, 4, 2), funding_hour_index(rows))[1] == 'cashflow_mismatch'


def test_duplicate_hour_cannot_prove_a_funding_split():
    rows = [event(3600000, 1), event(3600001, 1)]
    assert reconcile_funding_event(event(0, 2, 2), funding_hour_index(rows))[1] == 'ambiguous_hour'
