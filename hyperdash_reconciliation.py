"""Reconcile provider representations without granting chain-coverage trust."""
from collections import defaultdict
from decimal import Decimal

DAY = 86_400_000
HOUR = 3_600_000


def number(value):
    result = Decimal(str(value))
    if not result.is_finite():
        raise ValueError('Nonfinite financial data')
    return result


def fill_group_key(row):
    # Keep venues, orders, sides, currencies and block timestamps separate. A tid may
    # identify the first constituent of an aggregated official execution.
    # Maker aggregation can span several transaction hashes in one block.
    return (row['coin'], int(row['time']), str(row.get('oid')),
            row.get('side'), row.get('dir'), row.get('feeToken'))


def compare_fill_groups(primary, external):
    """Total fee already includes builderFee; never add it a second time."""
    if not primary or not external:
        return False, {'missingGroup': True}
    if len({fill_group_key(row) for row in primary + external}) != 1:
        return False, {'differentGroup': True}
    primary_ids = {str(row['tid']) for row in primary if row.get('tid') is not None}
    external_ids = {str(row['tid']) for row in external if row.get('tid') is not None}
    if not primary_ids or not primary_ids.intersection(external_ids):
        return False, {'missingIdentityAnchor': True}
    differences = {}
    for field in ('sz', 'fee', 'closedPnl'):
        p = sum((number(row[field]) for row in primary), Decimal(0))
        e = sum((number(row[field]) for row in external), Decimal(0))
        if p != e:
            differences[field] = str(e - p)
    p = sum((number(row['px']) * number(row['sz']) for row in primary), Decimal(0))
    e = sum((number(row['px']) * number(row['sz']) for row in external), Decimal(0))
    # Official aggregated px is rounded; this tolerance is only for its VWAP.
    if abs(e - p) > max(Decimal('0.000001'), abs(p) * Decimal('0.0000000001')):
        differences['notional'] = str(e - p)
    return not differences, differences


def funding_hour_index(rows):
    index = defaultdict(list)
    for row in rows:
        if row.get('delta', {}).get('type') != 'funding':
            continue
        if row['delta'].get('nSamples') is not None:
            continue  # An external summary cannot prove its own hourly split.
        index[(row['delta']['coin'], int(row['time']) // HOUR)].append(row)
    return index


def reconcile_funding_event(primary, index):
    """Official nSamples summaries label a UTC day's start, including n=1.

    Require observed hourly cashflows, the declared sample count and their
    sum. Never interpolate events or select a period to make the sum fit.
    """
    delta = primary['delta']
    sample_count = delta.get('nSamples')
    hour = int(primary['time']) // HOUR
    if sample_count is not None:
        if isinstance(sample_count, bool) or int(sample_count) != sample_count or not 1 <= sample_count <= 24:
            return [], 'invalid_sample_count'
        if int(primary['time']) % DAY:
            return [], 'unknown_summary_bounds'
        hours = range(hour, hour + 24)
        expected = int(sample_count)
    else:
        hours = [hour]
        expected = 1
    buckets = [index.get((delta['coin'], h), []) for h in hours]
    if any(len(bucket) > 1 for bucket in buckets):
        return [], 'ambiguous_hour'
    rows = [row for bucket in buckets for row in bucket]
    if len(rows) != expected:
        return [], 'missing_samples'
    cash = sum((number(row['delta']['usdc']) for row in rows), Decimal(0))
    if abs(cash - number(delta['usdc'])) > Decimal('0.000001') * expected:
        return [], 'cashflow_mismatch'
    if sample_count is None:
        if number(rows[0]['delta']['szi']) != number(delta['szi']) or number(rows[0]['delta']['fundingRate']) != number(delta['fundingRate']):
            return [], 'hourly_state_mismatch'
    return rows, 'matched_summary_split' if sample_count is not None else 'matched_hourly'


def reconcile_fills(primary, external):
    """Return a staging tape: disputed groups use the entire primary group.

    Supplemental rows retain their source and do not establish completeness.
    Substituting one constituent by tid into an aggregate would double count.
    """
    groups_p, groups_e = defaultdict(list), defaultdict(list)
    for row in primary:
        groups_p[fill_group_key(row)].append(row)
    for row in external:
        groups_e[fill_group_key(row)].append(row)
    tape, decisions = [], []
    for key in sorted(groups_p.keys() | groups_e.keys(), key=repr):
        p, e = groups_p.get(key, []), groups_e.get(key, [])
        if p and e:
            match, differences = compare_fill_groups(p, e)
            rows, evidence = (e, 'matched_fill_group') if match else (p, 'primary_disputed_group')
        else:
            differences = {}
            rows, evidence = (p, 'primary_only') if p else (e, 'hyperdash_supplemental')
        tape.extend({'evidence': evidence, 'payload': row} for row in rows)
        decisions.append({'group': key, 'evidence': evidence, 'primaryRows': len(p),
                          'hyperdashRows': len(e), 'differences': differences})
    return tape, decisions
