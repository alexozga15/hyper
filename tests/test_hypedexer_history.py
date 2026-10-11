import pytest

from scripts.fetch_hypedexer_history import PublicArchiveClient, walk_backward


def test_backward_pages_preserve_inclusive_boundary_events():
    batches = [[{'time': 9, 'hash': 'a'}, {'time': 5, 'hash': 'b'}],
               [{'time': 5, 'hash': 'b'}, {'time': 5, 'hash': 'c'}],
               [{'time': 2, 'hash': 'd'}],
               [{'time': 2, 'hash': 'd'}], []]
    calls = []
    def request(payload):
        calls.append(payload['endTime'])
        return batches.pop(0)
    rows, pages, status = walk_backward(request, {'startTime': 0, 'endTime': 10}, limit=3)
    assert calls == [10, 5, 4, 2, 1]
    assert {r['hash'] for r in rows} == {'a', 'b', 'c', 'd'}
    assert status == 'empty_final_page'


def test_backward_saturated_boundary_is_not_silently_skipped():
    rows, pages, status = walk_backward(lambda _: [{'time': 9, 'hash': 'a'}, {'time': 9, 'hash': 'b'}],
                                        {'startTime': 0, 'endTime': 9}, limit=2)
    assert len(rows) == 2
    assert status == 'timestamp_saturation'


def test_backward_out_of_window_batch_is_quarantined():
    rows, pages, status = walk_backward(lambda _: [{'time': 11}], {'startTime': 0, 'endTime': 10})
    assert rows == []
    assert status == 'out_of_requested_range'


def test_credit_budget_stops_before_network_call(tmp_path, monkeypatch):
    client = PublicArchiveClient('dummy-test-key', tmp_path, max_credits=20)
    client.spent = 19
    def unexpected_request(*args, **kwargs):
        pytest.fail('Exhausted credit budget must not make a request')
    monkeypatch.setattr('scripts.fetch_hypedexer_history.urlopen', unexpected_request)
    with pytest.raises(RuntimeError, match='credit budget exhausted'):
        client.request('/info', reserve=2)
    assert client.requests == []


def test_missing_credit_headers_are_explicitly_estimated_and_do_not_log_secret(tmp_path, monkeypatch):
    import json
    class Response:
        status = 200
        headers = {}
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass
        def read(self):
            return b'{"success":true,"data":[]}'
    monkeypatch.setattr('scripts.fetch_hypedexer_history.urlopen', lambda *a, **k: Response())
    secret = 'dummy-secret-not-for-logs'
    client = PublicArchiveClient(secret, tmp_path, max_credits=50, allow_credit_estimates=True)
    client.request('/fills/user/test', reserve=5, estimated_cost=5)
    assert client.spent == 5
    assert client.balance is None
    assert client.estimated_requests == 1
    assert client.requests[0]['creditAccounting'] == 'console_price_estimate_not_actual_spend'
    assert secret not in json.dumps(client.requests)


def test_missing_credit_headers_stop_by_default_and_preserve_response(tmp_path, monkeypatch):
    class Response:
        status = 200
        headers = {}
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass
        def read(self):
            return b'[]'
    monkeypatch.setattr('scripts.fetch_hypedexer_history.urlopen', lambda *a, **k: Response())
    client = PublicArchiveClient('dummy-key', tmp_path, max_credits=50)
    with pytest.raises(RuntimeError, match='Missing credit headers'):
        client.request('/info', reserve=20)
    assert len(list(tmp_path.glob('*.json.gz'))) == 1


def test_matching_execution_does_not_invent_missing_realized_pnl():
    from scripts.audit_hypedexer_history import matches_execution
    row = {'coin': 'BTC', 'hash': 'hash', 'oid': 12, 'tid': 123, 'side': 'B', 'feeToken': 'USDC',
           'px': '60000', 'sz': '0.1', 'fee': '1.2', 'startPosition': '-0.1',
           'time': '2026-04-14 07:30:00.123000'}
    original = {**row, 'time': 1776151800123, 'closedPnl': '-50', 'dir': 'Close Short'}
    assert matches_execution(row, original) == {'status': 'matched_execution_fields', 'pnlCorroborated': False}
    assert matches_execution({**row, 'fee': None}, original)['status'] == 'missing_execution_fields'
    assert matches_execution({**row, 'px': '61000'}, original)['status'] == 'execution_field_conflict'
    assert matches_execution({**row, 'tid': 0}, {**original, 'tid': 0})['status'] == 'missing_or_conflicting_execution_id'
