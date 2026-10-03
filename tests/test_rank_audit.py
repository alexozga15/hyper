from pathlib import Path
from unittest.mock import patch

import server
from equity_history import EquityHistoryCache, observed_perp_history
from history_cache import ResumableHistoryCache

DAY = 86_400_000


def healthy_rank(**changes):
    values = dict(pnl_7d=10, pnl_30d=30, pnl_180d=100, pnl_all_time=150,
                  sortino_180d=2, calmar_180d=3, adjusted_profit_factor_180d=2.5,
                  episode_count_180d=100, loss_count_180d=20,
                  daily_return_count_180d=180, downside_day_count_180d=20,
                  max_drawdown_pct=10, window_trusted=True, equity_curve_complete=True,
                  equity_curve_verified=True, largest_loser_pct=1, largest_loser_complete=True)
    return server.build_risk_trend_quality_rank(**{**values, **changes})


def test_deposit_cannot_create_a_false_largest_loser_veto():
    result = server.episode_loss_metrics([{"startMs": DAY, "pnl": -2500}],
                                        [[0, 100], [2 * DAY, 97600]])
    assert result["largestLoserPct"] is None
    assert result["largestLoserMissingCapitalEpisodes"] == 1


def test_fresh_bracket_requires_matching_pnl_and_no_cashflow():
    episode = [{"startMs": 60_000, "pnl": -10, "anchored": True}]
    account = [[0, 100], [120_000, 1000]]
    assert server.episode_loss_metrics(episode, account, [[0, 0], [120_000, 0]])["largestLoserPct"] is None
    account = [[0, 100], [120_000, 90]]
    result = server.episode_loss_metrics(episode, account, [[0, 0], [120_000, -10]])
    assert result["largestLoserPct"] == 10


def test_unanchored_or_incomplete_losers_cannot_trigger_a_measured_veto():
    for marker in ({"anchored": False}, {"historyComplete": False}):
        result = server.episode_loss_metrics([{"startMs": 0, "pnl": -90, **marker}], [[0, 100]])
        assert result["largestLoserPct"] is None


def test_old_history_and_drawdown_boundary_rules():
    assert healthy_rank(pnl_all_time=-1000)["label"] != "Elite"
    assert healthy_rank(pnl_all_time=100)["label"] == "Elite"  # No older loss.
    assert healthy_rank(max_drawdown_pct=30)["label"] == "Elite"
    assert healthy_rank(max_drawdown_pct=30.01)["label"] != "Elite"
    assert healthy_rank(max_drawdown_pct=50)["label"] != "Shadow"
    assert healthy_rank(max_drawdown_pct=50.01)["label"] == "Shadow"


def test_worst_possible_drawdown_is_uncertainty_not_confirmed_shadow():
    rank = healthy_rank(calmar_180d=None, sortino_180d=None, equity_curve_complete=False,
                        max_drawdown_pct=None, drawdown_upper_bound_pct=60)
    assert rank["label"] == "Unranked"
    assert rank["assessmentStatus"] == "Preliminary"


def test_cached_rank_reacts_to_current_open_loss_and_can_recover():
    rank = {**healthy_rank(), "pfHistoryComplete": True,
            "closedGrossProfit180d": 250, "closedGrossLoss180d": 100,
            "lossCapitalMethod": "fresh_bracket_v1"}
    failed = server.refresh_current_rank_risk(rank, open_loss=300, account_value=1000, state_ok=True)
    assert failed["label"] == "Shadow"
    assert failed["currentOpenLossPct"] == 30
    assert failed["adjustedProfitFactor180d"] == 0.625
    recovered = server.refresh_current_rank_risk(failed, open_loss=0, account_value=1000, state_ok=True)
    assert recovered["label"] == "Elite"
    assert recovered["adjustedProfitFactor180d"] == 2.5


def test_new_fills_make_cached_profit_factor_preliminary():
    rank = {**healthy_rank(), "pfHistoryComplete": True, "historyThroughMs": 100,
            "closedGrossProfit180d": 250, "closedGrossLoss180d": 100,
            "lossCapitalMethod": "fresh_bracket_v1"}
    updated = server.refresh_current_rank_risk(rank, open_loss=0, account_value=1000,
                                              state_ok=True, latest_fill_ms=101)
    assert updated["label"] == "Unranked"
    assert updated["adjustedProfitFactor180d"] is None
    assert updated["historyCurrent"] is False


def test_shadow_and_missing_metrics_cannot_be_bypassed_by_copyability():
    from test_wallet_quality_dimensions import healthy_wallet, validated_copyability
    for rank in (healthy_rank(current_open_loss_pct=30), healthy_rank(calmar_180d=None)):
        wallet = {**healthy_wallet(), "recentWinRateRank": rank}
        result = server.wallet_quality_dimensions(wallet, validated_copyability())
        assert not result["tradeEligible"]
        assert result["evidence"]["status"] != "pass"


def test_malformed_fill_marks_history_incomplete_without_crashing():
    for malformed in (fill(1, "NaN", 1, "B"), fill(1, 0, "NaN", "B"),
                      fill(1, 0, 1, "?"), fill(1, 0, 1, "B", fee="NaN")):
        diagnostic = {}
        assert server.reconstruct_position_episodes([malformed], 0, diagnostics=diagnostic) == []
        assert diagnostic["invalidFills"] == 1


def test_cache_migration_discards_unchecked_capital_but_keeps_live_loss():
    rank = {**healthy_rank(largest_loser_pct=2500), "pfHistoryComplete": True}
    updated = server.refresh_current_rank_risk(rank, open_loss=0, account_value=1000, state_ok=True)
    assert updated["largestLoserPct"] is None
    assert updated["label"] == "Unranked"
    updated = server.refresh_current_rank_risk(rank, open_loss=300, account_value=1000, state_ok=True)
    assert updated["label"] == "Shadow"
    assert updated["shadowReasons"] == ["current_open_loss"]


def test_unknown_or_shadow_wallets_do_not_enter_top_cohort():
    service = server.WalletTrackerService(object(), server.HyperliquidClient())
    wallets = [{"address": "bad", "recentWinRateRank": healthy_rank(current_open_loss_pct=40)},
               {"address": "unknown", "recentWinRateRank": healthy_rank(calmar_180d=None)},
               {"address": "good", "recentWinRateRank": healthy_rank()}]
    assert service.top_conviction_wallet_addresses(wallets) == {"good"}
    assert not service.is_monthly_quality_eligible(wallets[0])
    assert not service.is_monthly_quality_eligible(wallets[1])


def block(account, pnl):
    return {"accountValueHistory": account, "pnlHistory": pnl}


def test_different_window_pnl_origins_are_aligned_without_inventing_points():
    portfolio = {"perpAllTime": block([[0, 100], [3, 130]], [[0, 1000], [3, 1030]]),
                 "perpMonth": block([[1, 110], [2, 120], [3, 130]], [[1, 0], [2, 10], [3, 20]])}
    merged = observed_perp_history(portfolio)
    assert merged["pnlHistory"] == [[0, 1000], [1, 1010], [2, 1020], [3, 1030]]
    assert len(merged["accountValueHistory"]) == 4
    portfolio["perpMonth"]["accountValueHistory"][-1][1] = 999
    assert observed_perp_history(portfolio)["rejectedWindows"] == ["perpMonth"]


def test_equity_archive_survives_restart_and_resets_changed_pnl_origin(tmp_path):
    path = tmp_path / "equity.sqlite3"
    EquityHistoryCache(path).merge("a", block([[0, 100], [DAY, 90]], [[0, 0], [DAY, -10]]), cutoff_ms=0)
    merged = EquityHistoryCache(path).merge("a", block([[DAY, 90], [2 * DAY, 95]], [[DAY, -10], [2 * DAY, -5]]), cutoff_ms=0)
    assert len(merged["pnlHistory"]) == 3
    merged = EquityHistoryCache(path).merge("a", block([[DAY, 90], [2 * DAY, 95]], [[DAY, 0], [2 * DAY, 5]]), cutoff_ms=0)
    assert merged["pnlHistory"] == [[DAY, 0], [2 * DAY, 5]]


def fill(t, start, size, side, pnl=0, fee=0, coin="BTC", tid=None):
    return {"time": t, "coin": coin, "startPosition": str(start), "sz": str(size),
            "side": side, "closedPnl": str(pnl), "fee": str(fee), "px": "100", "tid": tid}


def test_flip_allocates_fee_and_keeps_open_costs():
    fills = [fill(0, 0, 1, "B"), fill(1, 1, 3, "A", pnl=10, fee=3), fill(2, -2, 2, "B", pnl=20)]
    episodes = server.reconstruct_position_episodes(fills, 0)
    assert [e["pnl"] for e in episodes] == [9, 18]
    episodes = server.reconstruct_position_episodes(fills[:2], 0, include_open=True)
    assert episodes[1]["pnl"] == -2
    assert episodes[1]["open"]


def test_position_history_gaps_and_venues_are_kept_separate():
    fills = [fill(0, 0, 1, "B"), fill(1, 2, 2, "A", pnl=10)]
    assert server.reconstruct_position_episodes(fills, 0)[0]["historyComplete"] is False
    fills = [fill(0, 0, 1, "B", coin="xyz:BTC"), fill(1, 0, 2, "B"),
             fill(2, 1, 1, "A", coin="xyz:BTC", pnl=-10), fill(3, 2, 2, "A", pnl=20)]
    episodes = server.reconstruct_position_episodes(fills, 0)
    assert [(e["coin"], e["pnl"]) for e in episodes] == [("xyz:BTC", -10), ("BTC", 20)]
    assert server.raw_fill_identity(fill(0, 0, 1, "B", coin="xyz:BTC", tid=1)) != server.raw_fill_identity(fill(0, 0, 1, "B", tid=1))


def test_interleaved_twap_and_regular_fills_produce_actual_round_trips(tmp_path):
    service = server.WalletTrackerService(object(), server.HyperliquidClient())
    service.wallet_quality_cache_path = tmp_path / "quality.json"
    now = 10 * DAY
    regular = [fill(now - 3000, 1, 1, "A", pnl=10, fee=1),
               fill(now - 1000, 1, 1, "A", pnl=-10, fee=1)]
    slices = [{"twapId": 1, "fill": fill(now - 4000, 0, 1, "B", fee=1)},
              {"twapId": 1, "fill": fill(now - 2000, 0, 1, "B", fee=1)}]
    good = {"ok": True, "data": [], "truncated": False, "error": ""}
    with patch("server.current_time_ms", return_value=now), \
         patch.object(service.client, "safe_subscribe_all_dexs_clearinghouse_state", return_value={
             "marginSummary": {"accountValue": "1000"}, "assetPositions": [], "_fetchOk": True}), \
         patch.object(service, "fetch_fills_paginated_result", return_value={**good, "data": regular}), \
         patch.object(service, "fetch_twap_slice_fills_paginated_result", return_value={**good, "data": slices}), \
         patch.object(service, "fetch_user_funding_paginated_result", return_value=good), \
         patch.object(service, "fetch_recent_fills_result", return_value=good), \
         patch.object(service, "fetch_open_orders_result", return_value=good), \
         patch.object(service, "fetch_portfolio_result", return_value={**good, "data": {}}), \
         patch.object(service, "fetch_wallet_role", return_value="user"):
        snapshot = service.fetch_wallet_snapshot(server.TrackedWallet("a", "", "", ""))
    assert snapshot["episodes180d"] == 2
    assert snapshot["qualityNetPnl30d"] == -4
    assert snapshot["recentWinRateRank"]["closedGrossProfit180d"] == 8
    assert snapshot["recentWinRateRank"]["closedGrossLoss180d"] == 12


def test_history_identity_upgrade_does_not_duplicate_existing_rows(tmp_path):
    cache = ResumableHistoryCache(tmp_path / "history.sqlite3")
    row = {"time": 100, "id": 1, "coin": "BTC"}
    kwargs = dict(address="a", kind="fills", start=100, page_size=2000,
                  timestamp=lambda r: r["time"], fetch=lambda cursor: {"ok": True, "data": [row]})
    with patch("history_cache.time.time", return_value=1):
        cache.walk(**kwargs, identity=lambda r: ("old", r["id"]))
        upgraded = cache.walk(**kwargs, identity=lambda r: ("new", r["coin"], r["id"]))
    assert upgraded["data"] == [row]


def test_fill_retention_limit_is_persistent_and_expires_with_window(tmp_path):
    cache = ResumableHistoryCache(tmp_path / "history.sqlite3", pages_per_run=10)
    rows = [{"time": 1000 + i, "id": i} for i in range(10_000)]
    def fetch(cursor):
        return {"ok": True, "data": [r for r in rows if r["time"] >= cursor][:2000]}
    def walk(start):
        return cache.walk("a", "fills", start, fetch=fetch, page_size=2000,
                          timestamp=lambda r: r["time"], identity=lambda r: r["id"])
    with patch("history_cache.time.time", return_value=11):
        result = walk(0)
        assert not result["truncated"] and result["retentionLimited"]
        assert walk(0)["retentionLimited"]
        assert not walk(1001)["retentionLimited"]
