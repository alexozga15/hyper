import sqlite3
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from equity_history import EquityHistoryCache
from history_cache import ResumableHistoryCache
from server import WalletTrackerService, daily_equity_metrics_180d, equity_curve_verification
from server import shorter_equity_metrics, wallet_quality_dimensions
from server import TrackedWallet
from quality_evidence import quality_assessment, format_quality_assessment
from available_info_rank import evidence_aware_rank

DAY = 86_400_000


def block(account, pnl):
    return {"accountValueHistory": account, "pnlHistory": pnl}


def test_conflict_keeps_old_curve_and_both_observation_versions(tmp_path):
    path = tmp_path / "equity.sqlite3"
    archive = EquityHistoryCache(path)
    archive.merge("a", block([[0, 100], [DAY, 40]], [[0, 0], [DAY, -60]]), cutoff_ms=0)
    result = archive.merge("a", block([[DAY, 40], [2 * DAY, 45]], [[DAY, 0], [2 * DAY, 5]]), cutoff_ms=0)
    assert result["historyConflict"]
    assert result["pnlHistory"] == [[0, 0], [DAY, -60]]
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT count(*) FROM equity_observations").fetchone()[0] == 4
        assert db.execute("SELECT count(*) FROM equity_conflicts").fetchone()[0] == 2
    metrics = daily_equity_metrics_180d(result["pnlHistory"], result["accountValueHistory"], 0, 2 * DAY)
    assert metrics["observedMaxDrawdownPct"] == 60
    verification = equity_curve_verification(metrics, portfolio_ok=True, funding_complete=True,
                                           history_conflict=result["historyConflict"])
    assert "equity_history_conflict" in verification["reasons"]


def test_conflicting_account_value_is_not_silently_overwritten(tmp_path):
    archive = EquityHistoryCache(tmp_path / "equity.sqlite3")
    archive.merge("a", block([[0, 100], [DAY, 90]], [[0, 0], [DAY, -10]]), cutoff_ms=0)
    result = archive.merge("a", block([[DAY, 900]], [[DAY, -10]]), cutoff_ms=0)
    assert result["historyConflict"]
    assert result["accountValueHistory"][-1] == [DAY, 90]


def test_unanchored_extension_is_archived_not_spliced(tmp_path):
    archive = EquityHistoryCache(tmp_path / "equity.sqlite3")
    archive.merge("a", block([[0, 100]], [[0, 0]]), cutoff_ms=0)
    result = archive.merge("a", block([[DAY, 100]], [[DAY, 9000]]), cutoff_ms=0)
    assert result["historyUnanchored"] and result["historyConflict"]
    assert result["pnlHistory"] == [[0, 0]]


def test_empty_update_does_not_erase_observed_drawdown(tmp_path):
    archive = EquityHistoryCache(tmp_path / "equity.sqlite3")
    archive.merge("a", block([[0, 100], [DAY, 40]], [[0, 0], [DAY, -60]]), cutoff_ms=0)
    result = archive.merge("a", block([], []), cutoff_ms=0)
    assert result["historyInputMissing"]
    assert result["pnlHistory"] == [[0, 0], [DAY, -60]]


def test_window_filter_does_not_delete_raw_or_canonical_archive(tmp_path):
    path = tmp_path / "equity.sqlite3"
    archive = EquityHistoryCache(path)
    values = block([[0, 100], [DAY, 90], [2 * DAY, 95]], [[0, 0], [DAY, -10], [2 * DAY, -5]])
    archive.merge("a", values, cutoff_ms=0)
    result = archive.merge("a", values, cutoff_ms=4 * DAY)
    assert result["pnlHistory"] == [[2 * DAY, -5]]
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT count(*) FROM equity_points").fetchone()[0] == 3
        assert db.execute("SELECT count(*) FROM equity_observations").fetchone()[0] == 3


def test_equity_proof_and_pf_proof_are_independent():
    metrics = dict(equityCurveComplete=True, dailyReturnCount=180, missingDayCount=0,
                   invalidDayCount=0, cashflowAmbiguousIntervals=0)
    result = equity_curve_verification(metrics, portfolio_ok=True, funding_complete=True,
                                      episode_history_complete=False, cashflow_complete=True)
    assert result["verified"] and not result["profitFactorVerified"]
    result = equity_curve_verification(metrics, portfolio_ok=True, funding_complete=True,
                                      episode_history_complete=True, cashflow_complete=False)
    assert not result["verified"] and result["profitFactorVerified"]
    assert result["reasons"] == ["cashflow_history_incomplete"]


def test_equal_opposite_transfers_cannot_make_a_false_exact_return():
    result = daily_equity_metrics_180d([[0, 0], [DAY, -10]], [[0, 100], [DAY, 90]],
                                     0, DAY, ledger_times=[DAY // 3, DAY // 2])
    assert result["cashflowAmbiguousIntervals"] == 1
    assert result["ledgerGuardedIntervals"] == 1
    assert result["dailyReturnCount"] == 0
    assert result["maxDrawdownPct"] is None and result["maxDrawdownUpperBoundPct"] is None


def test_unknown_flow_order_keeps_existing_risk_breach():
    result = daily_equity_metrics_180d([[0, 0], [DAY, -60], [2 * DAY, -55]],
                                     [[0, 100], [DAY, 40], [2 * DAY, 145]],
                                     0, 2 * DAY, ledger_times=[DAY + DAY // 2])
    assert result["observedMaxDrawdownPct"] == 60
    assert result["cashflowAmbiguousIntervals"] == 1


def test_full_capital_loss_is_not_erased_by_later_deposit():
    result = daily_equity_metrics_180d([[0, 0], [DAY, -100], [2 * DAY, -100]],
                                     [[0, 100], [DAY, 0], [2 * DAY, 900]],
                                     0, 2 * DAY, ledger_times=[2 * DAY])
    assert result["observedMaxDrawdownPct"] == 100


def test_moving_history_window_and_failed_earlier_backfill_preserve_rows(tmp_path):
    path = tmp_path / "history.sqlite3"
    cache = ResumableHistoryCache(path)
    def walk(start, fetch):
        return cache.walk("a", "fills", start, fetch=fetch, page_size=500,
                          timestamp=lambda r: r["time"], identity=lambda r: r["id"])
    walk(100, lambda _: {"ok": True, "data": [{"id": 1, "time": 100}, {"id": 2, "time": 200}]})
    walk(150, lambda _: {"ok": True, "data": []})
    result = walk(0, lambda _: {"ok": False, "data": [], "error": "unavailable"})
    assert not result["ok"]
    assert [r["id"] for r in result["data"]] == [1, 2]
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT count(*) FROM rows").fetchone()[0] == 2


def test_ledger_short_page_does_not_claim_end_and_restart_keeps_events(tmp_path):
    calls = []
    def post(payload, default):
        cursor = payload["startTime"]
        calls.append(cursor)
        rows = ([{"time": 1000, "hash": "a"}, {"time": 2000, "hash": "b"}]
                if cursor == 1000 else [{"time": 2000, "hash": "b"}] if cursor == 2000 else [])
        return {"ok": True, "data": rows}
    service = WalletTrackerService(object(), SimpleNamespace(safe_post_result=post))
    service.history_cache = ResumableHistoryCache(tmp_path / "history.sqlite3")
    with patch("server.current_time_ms", return_value=3000):
        result = service.fetch_user_ledger_paginated_result("a", 1000)
    assert result["ok"] and not result["truncated"]
    assert calls == [1000, 2000, 2001]
    assert [r["hash"] for r in result["data"]] == ["a", "b"]
    with sqlite3.connect(tmp_path / "history.sqlite3") as db:
        assert db.execute("SELECT complete_until FROM streams WHERE kind='ledger'").fetchone()[0] == 3000


def test_saturated_ledger_timestamp_cannot_be_skipped(tmp_path):
    service = WalletTrackerService(object(), SimpleNamespace(safe_post_result=lambda payload, default: {
        "ok": True, "data": [{"time": 1000, "hash": str(i)} for i in range(500)]}))
    service.history_cache = ResumableHistoryCache(tmp_path / "history.sqlite3")
    with patch("server.current_time_ms", return_value=3000):
        result = service.fetch_user_ledger_paginated_result("a", 1000)
    assert not result["ok"] and result["truncated"]
    assert len(result["data"]) == 500


@pytest.mark.parametrize("rows", [[{"time": "bad"}], [{"time": True}], [{"time": 4000}]])
def test_invalid_ledger_rows_fail_closed(rows):
    service = WalletTrackerService(object(), SimpleNamespace(safe_post_result=lambda *args: {"ok": True, "data": rows}))
    result = service.fetch_user_ledger_result("a", 1000, 3000)
    assert not result["ok"] and result["data"] == []


def test_unknown_pf_is_not_zero_profitability_or_a_failed_performance_test():
    wallet = {"qualityNetPnl30d": 100, "qualityProfitFactor30d": None}
    dimensions = wallet_quality_dimensions(wallet)
    assert dimensions["performance"]["status"] == "unknown"
    assert dimensions["performance"]["profitFactor30d"] is None
    assert not dimensions["tradeEligible"]


def test_evidence_v2_keeps_incomplete_zero_pf_as_unknown_not_cold():
    args = dict(profit_factor=0, trend_points=20, episodes=1, losses=1)
    rank = evidence_aware_rank(**args)
    assert rank["label"] == "InsufficientData"
    assert rank["availableInfoScore"] is None and rank["diagnosticScore"] == 50
    assert not rank["tradeEligible"] and not rank["eliteEligible"]
    shadow = evidence_aware_rank(**args, current_open_loss_pct=40)
    assert shadow["label"] == "Shadow" and shadow["availableInfoScore"] is None


def test_verified_zero_pf_is_real_performance_not_missing_data():
    rank = evidence_aware_rank(profit_factor=0, trend_points=0, episodes=60, losses=40,
                               profit_factor_verified=True, window_coverage_verified=True)
    assert rank["label"] == "Cold" and rank["availableInfoScore"] == 0
    assert rank["qualityVerified"] and rank["dataReasons"] == []


def test_partial_pf_trend_score_is_a_diagnostic_not_verified_quality():
    wallet = {"recentWinRateRank": {
        "metric": "available_pf_trend_v1", "label": "Strong", "score": 100,
        "pfHistoryComplete": False, "sampleGatePassed": True,
        "scoreComponentsAvailable": {"trend": True, "adjustedProfitFactor": True},
    }}
    result = quality_assessment(wallet, wallet_quality_dimensions(wallet))
    assert result["quality"]["status"] == "InsufficientData"
    assert result["quality"]["score"] is None
    assert result["quality"]["diagnosticScore"] == 100
    assert result["data"]["profitFactorVerified"] is False
    assert "adjustedProfitFactor" in result["data"]["missingComponents"]
    assert not result["tradeEligible"]
    assert "n/a" in format_quality_assessment(result)
    assert "0.0/100" not in format_quality_assessment(result)


def test_known_shadow_is_preserved_even_without_current_equity():
    wallet = {"accountValue": 0, "recentWinRateRank": {
        "metric": "calmar_sortino_adjusted_pf_trend_180d_v2",
        "label": "Shadow", "score": None, "shadowReasons": ["observed_drawdown"],
    }}
    result = quality_assessment(wallet, wallet_quality_dimensions(wallet))
    assert result["risk"]["status"] == "blocked"
    assert result["quality"]["status"] == "Shadow"
    assert result["quality"]["score"] is None and not result["tradeEligible"]


def test_empty_account_is_unknown_risk_not_zero_risk():
    result = quality_assessment({"accountValue": 0}, wallet_quality_dimensions({"accountValue": 0}))
    assert result["risk"]["status"] == "unknown"


def test_short_window_is_measured_separately_and_cannot_become_a_180d_rank():
    equity = 100.0
    pnl = 0.0
    accounts, profits = [[0, equity]], [[0, pnl]]
    for day in range(1, 31):
        change = equity * (-0.01 if day % 3 == 0 else 0.02)
        equity += change
        pnl += change
        accounts.append([day * DAY, equity])
        profits.append([day * DAY, pnl])
    values = shorter_equity_metrics(block(accounts, profits), 30 * DAY,
                                    funding_complete=True, ledger_times=[],
                                    ledger_complete=True, portfolio_ok=True)
    assert values["30d"]["verification"]["verified"]
    assert values["30d"]["sortino"] > 0
    assert values["30d"]["periodDays"] == 30
    assert values["30d"]["qualityRank"] is None
    assert not values["90d"]["verification"]["verified"]
    assert values["90d"]["sortino"] is None
    full = daily_equity_metrics_180d(profits, accounts, -150 * DAY, 30 * DAY)
    assert full["sortino"] is None and not full["equityCurveComplete"]
    partial = shorter_equity_metrics(
        block(accounts + [[30 * DAY + DAY // 2, equity + 100]],
              profits + [[30 * DAY + DAY // 2, pnl + 100]]),
        30 * DAY + DAY // 2, funding_complete=True, ledger_times=[],
        ledger_complete=True, portfolio_ok=True,
    )
    assert partial["30d"]["sortino"] == values["30d"]["sortino"]
    assert partial["30d"]["asOfMs"] == 30 * DAY
    assert not partial["30d"]["includesPartialDay"]


def test_short_window_without_cashflow_coverage_is_preliminary():
    values = shorter_equity_metrics(block([], []), 30 * DAY, funding_complete=True,
                                    ledger_times=[], ledger_complete=None, portfolio_ok=True)
    assert not values["30d"]["verification"]["verified"]
    assert "cashflow_history_unknown" in values["30d"]["verification"]["reasons"]


def test_live_archive_deduplicates_but_does_not_claim_historical_coverage(tmp_path):
    path = tmp_path / "history.sqlite3"
    cache = ResumableHistoryCache(path)
    rows = [{"id": 1, "time": 100}, {"id": 2, "time": 200}]
    args = dict(timestamp=lambda row: row["time"], identity=lambda row: row["id"])
    assert cache.record_observations("A", "fills", rows, **args) == 2
    assert cache.record_observations("a", "fills", rows, **args) == 0
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT count(*) FROM streams").fetchone()[0] == 0
        assert db.execute("SELECT count(*) FROM coverage_limits").fetchone()[0] == 0
        assert db.execute("SELECT count(*) FROM rows").fetchone()[0] == 2


def test_background_snapshot_collects_ledger_and_preserves_risk_on_conflict(tmp_path):
    current = {"curve": block([[0, 100], [DAY, 40], [2 * DAY, 940]],
                               [[0, 0], [DAY, -60], [2 * DAY, -60]])}
    ledger = [{"time": 2 * DAY, "hash": "deposit", "delta": {"type": "deposit", "usdc": "900"}}]
    calls = []
    def post(payload, default):
        calls.append(payload["type"])
        if payload["type"] == "portfolio":
            return {"ok": True, "data": [["perpAllTime", current["curve"]]]}
        if payload["type"] == "userNonFundingLedgerUpdates":
            return {"ok": True, "data": [row for row in ledger if row["time"] >= payload["startTime"]]}
        assert payload["type"] in {"userFills", "userFillsByTime", "userFunding", "userTwapSliceFillsByTime", "openOrders"}
        return {"ok": True, "data": []}
    state = {"_fetchOk": True, "marginSummary": {"accountValue": "940"},
             "withdrawable": "940", "assetPositions": []}
    client = SimpleNamespace(safe_post_result=post, safe_post=lambda *args: "user",
                             safe_subscribe_all_dexs_clearinghouse_state=lambda *args: state)
    service = WalletTrackerService(object(), client)
    service.wallet_quality_cache_path = tmp_path / "quality.json"
    service.history_cache = ResumableHistoryCache(tmp_path / "history.sqlite3")
    wallet = TrackedWallet("0x" + "a" * 40, "", "", "")
    with patch("server.current_time_ms", return_value=3 * DAY):
        first = service.fetch_wallet_snapshot(wallet, full_quality_refresh=True)
        assert first["cashflowHistoryComplete"] is True
        assert first["dataQuality"]["ledgerFetched"]
        assert first["recentWinRateRank"]["label"] == "Shadow"
        current["curve"] = block([[DAY, 40], [2 * DAY, 940]], [[DAY, 0], [2 * DAY, 0]])
        second = service.fetch_wallet_snapshot(wallet, full_quality_refresh=True)
        bogus_loss = {"largestLoserPct": 80, "largestLoserMeasuredEpisodes": 1,
                      "largestLoserMissingCapitalEpisodes": 0}
        with patch("server.episode_loss_metrics", return_value=bogus_loss):
            untrusted = service.fetch_wallet_snapshot(wallet, full_quality_refresh=True)
            previous = {**first, "recentWinRateRank": {
                **first["recentWinRateRank"], "largestLoserPct": 30,
                "largestLoserEvidenceAtMs": DAY, "lossCapitalMethod": "fresh_bracket_v1"}}
            retained = service.fetch_wallet_snapshot(wallet, full_quality_refresh=True,
                                                       cached_snapshot=previous)
    assert "userNonFundingLedgerUpdates" in calls
    assert second["dataQuality"]["equityHistoryConflict"]
    assert second["recentWinRateRank"]["observedDrawdownPct"] == 60
    assert second["recentWinRateRank"]["label"] == "Shadow"
    assert not second["recentWinRateRank"]["equityCurveVerified"]
    assert untrusted["recentWinRateRank"]["largestLoserPct"] is None
    assert "largest_loser" not in untrusted["recentWinRateRank"]["shadowReasons"]
    assert retained["recentWinRateRank"]["largestLoserPct"] == 30
    assert retained["recentWinRateRank"]["largestLoserEvidenceAtMs"] == DAY
    assert not retained["recentWinRateRank"]["largestLoserComplete"]
    with sqlite3.connect(tmp_path / "history.sqlite3") as db:
        assert db.execute("SELECT count(*) FROM rows WHERE kind='ledger'").fetchone()[0] == 1


def test_failed_raw_archival_is_visible_in_health_monitor():
    from scripts.run_health_monitor import detect_health_issues
    from server import now_iso, current_time_ms
    issues = detect_health_issues({"state": {"lastCheckedAt": now_iso()}},
                                  {"cacheCoverage": 1.0, "rawArchiveFailedWallets": 1},
                                  now_ms=current_time_ms(), disk_free_pct=20)
    assert "raw_archive_failed" in {issue.key for issue in issues}
