from capital_history import walk_ledger, audit_capital_changes


def test_short_page_is_not_end_and_inclusive_boundary_preserves_rows():
    calls = []
    batches = [[{'time': 1, 'hash': 'a'}],
               [{'time': 1, 'hash': 'a'}, {'time': 1, 'hash': 'b'}, {'time': 2, 'hash': 'c'}],
               [{'time': 2, 'hash': 'c'}], []]
    def request(payload):
        calls.append(payload['startTime'])
        return batches.pop(0)
    result = walk_ledger(request, 'wallet', 0, 10)
    assert calls == [0, 1, 2, 3]
    assert [r['hash'] for r in result['rows']] == ['a', 'b', 'c']
    assert result['paginationStatus'] == 'empty_final_page'
    assert result['chainCoverageVerified'] is False


def test_saturated_timestamp_is_not_skipped():
    result = walk_ledger(lambda _: [{'time': 1, 'hash': str(i)} for i in range(500)],
                         'wallet', 1, 10)
    assert result['paginationStatus'] == 'timestamp_saturation'
    assert len(result['rows']) == 500


def test_out_of_range_response_is_quarantined():
    result = walk_ledger(lambda _: [{'time': 11}], 'wallet', 0, 10)
    assert result['rows'] == []
    assert result['paginationStatus'] == 'out_of_requested_range'


def test_failed_page_preserves_progress_and_is_not_complete():
    def request(payload):
        if payload['startTime'] == 0:
            return [{'time': 2, 'hash': 'a'}]
        raise TimeoutError('Provider timeout')
    result = walk_ledger(request, 'wallet', 0, 10)
    assert result['rows'] == [{'time': 2, 'hash': 'a'}]
    assert result['paginationStatus'] == 'request_failed'
    assert result['error'] == 'Provider timeout'
    resumed = walk_ledger(lambda _: [], 'wallet', 0, 10, previous=result)
    assert resumed['paginationStatus'] == 'empty_final_page'
    assert resumed['rows'] == result['rows']


def test_ledger_presence_does_not_manufacture_a_verified_return():
    points = {0: (1000, 0), 10: (2100, 100), 20: (2600, 100)}
    result = audit_capital_changes(points, [{'time': 5}], start_ms=0, end_ms=20)
    assert result['observedCapitalChangeIntervals'] == 2
    assert result['capitalChangeIntervalsWithoutLedgerEvent'] == 1
    assert result['examples'][0]['capitalChangeUsd'] == '1000'
    assert result['examples'][0]['cashflowVerified'] is False
    assert result['reconstructedDailyEquity'] is False


def test_incompatible_pnl_origins_are_not_joined():
    from scripts.audit_wallet_capital import join_observations
    primary = {1: (100, 1), 2: (200, 2)}
    incoming = {1: (100, 10), 2: (200, 12)}
    merged, status = join_observations(primary, incoming)
    assert merged == incoming
    assert status['status'] == 'shared_anchor_conflict'


def test_verified_shared_offsets_align_the_pnl_origin():
    from scripts.audit_wallet_capital import join_observations
    primary = {0: (50, 0), 1: (100, 1), 2: (200, 2)}
    incoming = {1: (100, 11), 2: (200, 12)}
    merged, status = join_observations(primary, incoming)
    assert merged[0] == (50, 10)
    assert status['status'] == 'matched_shared_anchors'
