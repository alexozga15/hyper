"""Public cashflow acquisition helpers; pagination is not chain coverage."""
import hashlib
import json
from decimal import Decimal, InvalidOperation


def row_identity(row):
    return hashlib.sha256(json.dumps(row, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()


def audit_capital_changes(points, ledger, *, start_ms, end_ms):
    """Locate changes in equity minus cumulative PnL without inventing flows.

    An event inside an interval is a possible explanation, not proof of its
    amount, book or ordering. The current account mode is not historical mode.
    """
    pairs = sorted((t, values) for t, values in points.items() if start_ms <= t <= end_ms)
    events = sorted(int(row['time']) for row in ledger)
    from bisect import bisect_right
    changes, unsupported, examples = 0, 0, []
    for (first, (a0, p0)), (last, (a1, p1)) in zip(pairs, pairs[1:]):
        try:
            numbers = [Decimal(str(value)) for value in (a0, p0, a1, p1)]
        except (InvalidOperation, ValueError):
            continue
        if not all(value.is_finite() for value in numbers):
            continue
        a0, p0, a1, p1 = numbers
        change = (a1 - a0) - (p1 - p0)
        tolerance = max(Decimal('0.00001'), max(abs(a0), abs(a1)) * Decimal('0.00000001'))
        if abs(change) <= tolerance:
            continue
        changes += 1
        count = bisect_right(events, last) - bisect_right(events, first)
        if not count:
            unsupported += 1
        if len(examples) < 20:
            examples.append({'startMs': first, 'endMs': last, 'capitalChangeUsd': str(change),
                             'ledgerEventsInsideInterval': count, 'cashflowVerified': False})
    return {'observedCapitalChangeIntervals': changes,
            'capitalChangeIntervalsWithoutLedgerEvent': unsupported,
            'examples': examples, 'reconstructedDailyEquity': False}


def walk_ledger(request, address, start_ms, end_ms, *, max_pages=100, previous=None):
    """Walk inclusive timestamps to an empty response, retaining uncertain tails.

    The documented range limit is 500 elements or distinct blocks. A short
    page is not an end marker. A saturated single timestamp cannot be skipped.
    """
    rows = list((previous or {}).get('rows', []))
    pages = list((previous or {}).get('pages', []))
    seen = {row_identity(row) for row in rows}
    cursor = max((int(row['time']) for row in rows), default=start_ms)
    status = 'page_limit'
    error = None
    for _ in range(max_pages):
        try:
            batch = request({'type': 'userNonFundingLedgerUpdates', 'user': address,
                             'startTime': cursor, 'endTime': end_ms})
        except Exception as exc:
            status, error = 'request_failed', str(exc)
            break
        if not isinstance(batch, list):
            status = 'invalid_response'
            break
        pages.append({'startTime': cursor, 'count': len(batch)})
        if not batch:
            status = 'empty_final_page'
            break
        try:
            times = [int(row['time']) for row in batch]
            if any(t < cursor or t > end_ms for t in times):
                status = 'out_of_requested_range'
                break
            keys = [row_identity(row) for row in batch]
        except (ValueError, TypeError, KeyError):
            status = 'invalid_row'
            break
        for row, key in zip(batch, keys):
            if key not in seen:
                rows.append(row)
                seen.add(key)
        latest = max(times)
        if latest == cursor:
            if len(batch) >= 500:
                status = 'timestamp_saturation'
                break
            cursor += 1
        else:
            cursor = latest
    return {'rows': sorted(rows, key=lambda row: int(row['time'])), 'pages': pages,
            'paginationStatus': status, 'chainCoverageVerified': False, 'error': error}
