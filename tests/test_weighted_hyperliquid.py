from __future__ import annotations

import json
import multiprocessing
import urllib.error
from io import BytesIO
from pathlib import Path
from unittest.mock import patch

from ratelimit import HyperliquidRateLimiter, hyperliquid_request_weight
from history_cache import ResumableHistoryCache
from server import HyperliquidClient, RequestRateLimiter, TrackedWallet, WalletTrackerService


def _reserve_in_process(path: str, queue) -> None:
    limiter = HyperliquidRateLimiter(Path(path))
    limiter.interval = 0
    queue.put(limiter.acquire({"type": "userFills"}, max_wait_seconds=0) is not None)


def test_endpoint_weights_and_row_surcharges():
    assert hyperliquid_request_weight({"type": "l2Book"}) == 2
    assert hyperliquid_request_weight({"type": "userRole"}) == 60
    assert hyperliquid_request_weight({"type": "userFills"}) == 120
    assert hyperliquid_request_weight({"type": "userFills"}, [None] * 21) == 22


def test_processes_share_one_budget(tmp_path):
    path = tmp_path / "limit.sqlite3"
    limiter = HyperliquidRateLimiter(path)
    limiter.interval = 0
    for _ in range(7):
        assert limiter.acquire({"type": "userFills"}, max_wait_seconds=0) is not None
    context = multiprocessing.get_context("spawn")
    queue = context.Queue()
    process = context.Process(target=_reserve_in_process, args=(str(path), queue))
    process.start()
    process.join(10)
    assert process.exitcode == 0
    assert queue.get(timeout=1) is False
    assert limiter.acquire({"type": "l2Book"}, max_wait_seconds=0) is not None


def test_history_cannot_spend_live_reserve(tmp_path):
    limiter = HyperliquidRateLimiter(tmp_path / "limit.sqlite3", weight_per_minute=360, historical_weight_per_minute=240)
    limiter.interval = 0
    for _ in range(2):
        assert limiter.acquire({"type": "userFills"}, priority="history", max_wait_seconds=0)
    assert limiter.acquire({"type": "userFills"}, priority="history", max_wait_seconds=0) is None
    assert limiter.acquire({"type": "userFills"}, priority="live", max_wait_seconds=0)


def test_response_refund_and_cooldown_shared_with_new_instance(tmp_path):
    path = tmp_path / "limit.sqlite3"
    limiter = HyperliquidRateLimiter(path, weight_per_minute=240, historical_weight_per_minute=120)
    limiter.interval = 0
    token = limiter.acquire({"type": "userFills"}, max_wait_seconds=0)
    limiter.settle(token, {"type": "userFills"}, [])
    second = HyperliquidRateLimiter(path, weight_per_minute=240, historical_weight_per_minute=120)
    second.interval = 0
    assert second.acquire({"type": "userFills"}, max_wait_seconds=0)
    limiter.penalize(0.25)
    assert second.acquire({"type": "l2Book"}, max_wait_seconds=0) is None
    assert second.acquire({"type": "l2Book"}, priority="history") is None


def test_single_attempt_429_honors_retry_after_without_repeating_history(tmp_path):
    limiter = HyperliquidRateLimiter(tmp_path / "limit.sqlite3")
    client = HyperliquidClient(limiter, priority="history")
    error = urllib.error.HTTPError("https://example.test", 429, "Too Many Requests", {"Retry-After": "30"}, None)
    with patch.object(client, "post", side_effect=error) as post:
        assert client.safe_post({"type": "userRole"}, {}) == {}
    assert post.call_count == 1
    assert limiter.throttle_report()["backoffSeconds"] == 30
    assert limiter.acquire({"type": "userFills"}, priority="history") is None


def test_client_settles_actual_response_weight(tmp_path):
    limiter = HyperliquidRateLimiter(tmp_path / "limit.sqlite3", weight_per_minute=240)
    limiter.interval = 0
    client = HyperliquidClient(limiter)
    with patch("server.urllib.request.urlopen", side_effect=lambda *a, **k: BytesIO(b"[]")):
        assert client.post({"type": "userFills"}) == []
        assert client.post({"type": "userFills"}) == []
        assert client.post({"type": "userFills"}) == []


def walk(cache, fetch, start=100):
    return cache.walk("0xabc", "fills", start, fetch=fetch, page_size=2,
                      timestamp=lambda row: row["time"], identity=lambda row: row["id"])


def test_history_restart_resumes_failed_page_and_deduplicates_boundary(tmp_path):
    path = tmp_path / "history.sqlite3"
    seen = []

    def fetch(cursor):
        seen.append(cursor)
        if cursor == 100:
            return {"ok": True, "data": [{"id": 1, "time": 100}, {"id": 2, "time": 200}]}
        return {"ok": False, "data": [], "error": "HTTP 429"}

    result = walk(ResumableHistoryCache(path), fetch)
    assert not result["ok"] and result["truncated"]
    assert seen == [100, 200]

    def resumed(cursor):
        assert cursor == 200
        return {"ok": True, "data": [{"id": 2, "time": 200}]}

    result = walk(ResumableHistoryCache(path), resumed)
    assert result["ok"] and not result["truncated"]
    assert [row["id"] for row in result["data"]] == [1, 2]


def test_page_budget_keeps_incomplete_status_and_continues_next_run(tmp_path):
    path = tmp_path / "history.sqlite3"
    result = walk(ResumableHistoryCache(path, pages_per_run=1), lambda cursor: {
        "ok": True, "data": [{"id": 1, "time": 100}, {"id": 2, "time": 200}],
    })
    assert result["ok"] and result["truncated"]
    seen = []
    result = walk(ResumableHistoryCache(path), lambda cursor: seen.append(cursor) or {"ok": True, "data": []})
    assert seen == [200] and not result["truncated"]


def test_completed_history_fetches_only_tail_and_rebackfills_earlier_request(tmp_path):
    path = tmp_path / "history.sqlite3"
    with patch("history_cache.time.time", return_value=1000):
        assert not walk(ResumableHistoryCache(path), lambda cursor: {"ok": True, "data": []})["truncated"]
    seen = []
    walk(ResumableHistoryCache(path), lambda cursor: seen.append(cursor) or {"ok": True, "data": []})
    assert seen == [700_000]
    seen.clear()
    walk(ResumableHistoryCache(path), lambda cursor: seen.append(cursor) or {"ok": True, "data": []}, start=50)
    assert seen == [50]


def test_saturated_timestamp_cannot_claim_complete_history(tmp_path):
    result = walk(ResumableHistoryCache(tmp_path / "history.sqlite3"), lambda cursor: {
        "ok": True, "data": [{"id": 1, "time": 100}, {"id": 2, "time": 100}],
    })
    assert not result["ok"] and result["truncated"]


def test_429_does_not_trigger_outer_page_retries():
    service = WalletTrackerService(object(), HyperliquidClient(RequestRateLimiter(1000)))
    with patch.object(service, "fetch_fills_result", return_value={"ok": False, "data": [], "error": "HTTP 429"}) as fetch:
        result = service.fetch_fills_paginated_result("0xabc", 100)
    assert not result["ok"] and fetch.call_count == 1


def test_incomplete_background_history_preserves_last_good_score():
    service = WalletTrackerService(object(), HyperliquidClient(RequestRateLimiter(1000)))
    good = {"ok": True, "data": [], "error": "", "truncated": False}
    partial = {**good, "truncated": True}
    with patch.object(service.client, "safe_subscribe_all_dexs_clearinghouse_state", return_value={"marginSummary": {}, "assetPositions": []}), \
         patch.object(service, "fetch_fills_paginated_result", return_value=partial), \
         patch.object(service, "fetch_twap_slice_fills_paginated_result", return_value=good), \
         patch.object(service, "fetch_user_funding_paginated_result", return_value=good), \
         patch.object(service, "fetch_recent_fills_result", return_value=good), \
         patch.object(service, "fetch_open_orders_result", return_value=good), \
         patch.object(service, "fetch_portfolio_result", return_value={**good, "data": {}}), \
         patch.object(service, "fetch_wallet_role", return_value="user"):
        result = service.fetch_wallet_snapshot(TrackedWallet("0xabc", "", "", ""), cached_snapshot={"winRate90d": 80})
    assert not result["dataQuality"]["qualityRefreshSucceeded"]
    assert result["winRate90d"] == 80


def test_older_live_snapshot_cannot_overwrite_new_background_quality(tmp_path):
    service = WalletTrackerService(object(), HyperliquidClient(RequestRateLimiter(1000)))
    service.wallet_quality_cache_path = tmp_path / "quality.json"
    recent = {"coin": "BTC", "direction": "Open Long", "price": 100, "size": 1, "time": 2_000_000_000_000}
    fresh = {"address": "0xabc", "fetchedAt": "2033-05-18T03:33:20Z", "winRate90d": 80,
             "recentFills": [recent], "dataQuality": {"qualityRefreshSucceeded": True, "qualityRefreshAttempted": True}}
    stale = {"address": "0xabc", "fetchedAt": "2033-05-18T03:30:00Z", "winRate90d": 20,
             "recentFills": [], "dataQuality": {"qualityRefreshSucceeded": True}}
    with patch("server.current_time_ms", return_value=2_000_000_000_000):
        service.persist_wallet_snapshots([fresh], {"0xabc"})
        service.persist_wallet_snapshots([stale], {"0xabc"})
    saved = json.loads(service.wallet_quality_cache_path.read_text())["wallets"]["0xabc"]
    assert saved["winRate90d"] == 80
    assert saved["recentFills"] == [recent]
