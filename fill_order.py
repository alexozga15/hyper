"""Order simultaneous executions only when their position transitions prove it."""
from collections import defaultdict
from decimal import Decimal, InvalidOperation


def transition(row):
    try:
        start, size = Decimal(str(row['startPosition'])), Decimal(str(row['sz']))
        if not start.is_finite() or not size.is_finite() or size <= 0 or row.get('side') not in ('B', 'A'):
            return None
        return start, start + (size if row['side'] == 'B' else -size)
    except (KeyError, InvalidOperation, ValueError, TypeError):
        return None


def order_position_fills(fills):
    """A trade id or provider txIndex is not an execution-order guarantee.

    Keep ambiguous buckets diagnostic, with an explicit verification veto.
    Do not round position values or bridge missing executions.
    """
    groups = defaultdict(list)
    for row in fills:
        groups[(int(row.get('time') or 0), str(row.get('coin') or 'Unknown'))].append(row)
    ordered, diagnostics, previous = [], {}, {}
    for (_at, coin), bucket in sorted(groups.items()):
        bucket = sorted(bucket, key=lambda r: (len(str(r.get('tid') or '')), str(r.get('tid') or '')))
        if len(bucket) == 1 or all(r.get('dir') in ('Buy', 'Sell') for r in bucket):
            ordered.extend(bucket)
            step = transition(bucket[-1])
            previous[coin] = step[1] if step else None
            continue
        steps = [transition(row) for row in bucket]
        if any(step is None for step in steps):
            ordered.extend(bucket)
            previous[coin] = None
            continue  # The episode builder reports invalid payloads separately.
        outgoing = defaultdict(list)
        ends = set()
        for i, (start, end) in enumerate(steps):
            outgoing[start].append(i)
            ends.add(end)
        roots = set(outgoing) - ends
        anchor = previous.get(coin)
        if len(roots) == 1:
            node = next(iter(roots))
        elif not roots and anchor is not None and anchor in outgoing:
            node = anchor
        else:
            node = None
        route, seen = [], set()
        while node is not None:
            candidates = [i for i in outgoing.get(node, []) if i not in seen]
            if len(candidates) != 1:
                break
            i = candidates[0]
            seen.add(i)
            route.append(bucket[i])
            node = steps[i][1]
        if len(route) == len(bucket):
            ordered.extend(route)
            previous[coin] = node
        else:
            ordered.extend(dict(row, _positionOrderUnverified=True) for row in bucket)
            previous[coin] = None
            diagnostics['ambiguousFillOrder'] = diagnostics.get('ambiguousFillOrder', 0) + 1
    return ordered, diagnostics
