from unittest.mock import patch

import server
from available_info_rank import MAX_HISTORY_AGE_MS, METRIC, active_seed, apply_live_rank
from test_rank_audit import healthy_rank
from test_wallet_quality_dimensions import healthy_wallet, validated_copyability

ADDRESS = '0x' + '1' * 40
END = 10 * 86_400_000
NOW = END + 1000


def payload(**changes):
    row = dict(metric=METRIC, closedProfitCreditedUsd=250, closedLossObservedUsd=100,
               openCostsObservedUsd=0, unallocatedFundingLossUsd=0, usableEpisodes=100,
               usableLosses=20, trendPoints=20, observedDrawdownPct=10,
               largestLoser={'largestLoserPct': None}, availableInfoScore=100, label='Strong')
    return {**dict(version=1, endMs=END, expiresAtMs=END+MAX_HISTORY_AGE_MS,
                  wallets={ADDRESS: row}), **changes}


def live(**changes):
    args = dict(now_ms=NOW, open_loss=0, largest_open_loss=0,
                total_portfolio_equity=1000, risk_ok=True)
    args.update(changes)
    return apply_live_rank(healthy_rank(calmar_180d=None), active_seed(payload(), ADDRESS, NOW), **args)


def test_provisional_affects_consensus_but_cannot_admit_trade_or_promote():
    rank = live()
    service = server.WalletTrackerService(object(), server.HyperliquidClient())
    wallet = {**healthy_wallet(), 'recentWinRateRank': rank}
    with patch('server.current_time_ms', return_value=NOW):
        assert service.wallet_conviction_weight(wallet) == 1
        assert service.should_count_wallet_for_conviction(wallet)
    assert not server.wallet_rank_allows_promotion(wallet)
    assert not server.wallet_quality_dimensions(wallet, validated_copyability())['tradeEligible']
    assert rank['score'] == 100 and not rank['rankable']


def test_new_open_loss_changes_pf_label_weight_and_participation():
    rank = live(open_loss=300, largest_open_loss=250)
    assert rank['label'] == 'Shadow' and rank['adjustedObservedPF'] == 0.625
    wallet = {**healthy_wallet(), 'recentWinRateRank': rank}
    service = server.WalletTrackerService(object(), server.HyperliquidClient())
    with patch('server.current_time_ms', return_value=NOW):
        assert service.wallet_conviction_weight(wallet) == 0
        assert not service.should_count_wallet_for_conviction(wallet)
    recovered = live(open_loss=0)
    assert recovered['label'] == 'Strong'


def test_missing_current_risk_cannot_use_previous_positive_score():
    rank = live(risk_ok=False)
    assert rank['score'] is None and rank['provisionalConvictionWeight'] == 0
    service = server.WalletTrackerService(object(), server.HyperliquidClient())
    wallet = {**healthy_wallet(), 'recentWinRateRank': rank}
    with patch('server.current_time_ms', return_value=NOW):
        assert not service.should_count_wallet_for_conviction(wallet)
        assert service.wallet_conviction_weight(wallet) == 0


def test_expiry_is_tied_to_evidence_not_reload_time():
    assert active_seed(payload(), ADDRESS, END + MAX_HISTORY_AGE_MS) is None
    assert active_seed(payload(expiresAtMs=END+MAX_HISTORY_AGE_MS+1), ADDRESS, NOW) is None
    base = healthy_rank(calmar_180d=None)
    prior = apply_live_rank(base, active_seed(payload(), ADDRESS, NOW), now_ms=NOW,
                           open_loss=0, largest_open_loss=0, total_portfolio_equity=1000, risk_ok=True)
    assert apply_live_rank(prior, None, now_ms=NOW, open_loss=0, largest_open_loss=0,
                           total_portfolio_equity=1000, risk_ok=True) == base
    assert server.refreshed_quality_rank(prior, 100, 100)['label'] == 'Unranked'


def test_verified_full_rank_supersedes_provisional_and_cannot_be_downgraded():
    base = healthy_rank()
    assert apply_live_rank(base, active_seed(payload(), ADDRESS, NOW), now_ms=NOW,
                          open_loss=0, largest_open_loss=0, total_portfolio_equity=1000, risk_ok=True) == base


def test_malformed_seed_never_applies():
    p = payload()
    p['wallets'][ADDRESS]['closedProfitCreditedUsd'] = float('nan')
    assert active_seed(p, ADDRESS, NOW) is None
    assert active_seed({'wallets': []}, ADDRESS, NOW) is None


def test_dashboard_snapshot_fetch_uses_fresh_capital_and_overlay(tmp_path):
    service = server.WalletTrackerService(object(), server.HyperliquidClient())
    service.wallet_quality_cache_path = tmp_path / 'quality.json'
    good = {'ok': True, 'data': {'allTime': {'accountValueHistory': [[NOW, 1000]]}}}
    with patch('server.current_time_ms', return_value=NOW), \
         patch('server.load_json_file', return_value=payload()), \
         patch.object(service.client, 'safe_subscribe_all_dexs_clearinghouse_state', return_value={
             'marginSummary': {'accountValue': '10'}, 'assetPositions': [], '_fetchOk': True}), \
         patch.object(service, 'fetch_portfolio_result', return_value=good) as portfolio:
        snapshot = service.fetch_wallet_snapshot(server.TrackedWallet(ADDRESS, '', '', ''),
            full_quality_refresh=False, skip_fill_fetch=True,
            cached_snapshot={'recentWinRateRank': healthy_rank(calmar_180d=None)})
    portfolio.assert_called_once()
    assert snapshot['recentWinRateRank']['metric'] == METRIC
    assert snapshot['recentWinRateRank']['score'] == 100
    assert snapshot['recentWinRateRank']['riskCapitalMethod'] == 'fresh_total_portfolio_indicator'


def test_missing_position_pnl_cannot_be_interpreted_as_no_open_loss(tmp_path):
    service = server.WalletTrackerService(object(), server.HyperliquidClient())
    service.wallet_quality_cache_path = tmp_path / 'quality.json'
    good = {'ok': True, 'data': {'allTime': {'accountValueHistory': [[NOW, 1000]]}}}
    with patch('server.current_time_ms', return_value=NOW), \
         patch('server.load_json_file', return_value=payload()), \
         patch.object(service.client, 'safe_subscribe_all_dexs_clearinghouse_state', return_value={
             'marginSummary': {'accountValue': '10'}, 'assetPositions': [{'position': {'coin': 'BTC'}}],
             '_fetchOk': True}), \
         patch.object(service, 'fetch_portfolio_result', return_value=good):
        snapshot = service.fetch_wallet_snapshot(server.TrackedWallet(ADDRESS, '', '', ''),
            full_quality_refresh=False, skip_fill_fetch=True,
            cached_snapshot={'recentWinRateRank': healthy_rank(calmar_180d=None)})
    assert not snapshot['recentWinRateRank']['currentRiskKnown']
    assert snapshot['recentWinRateRank']['score'] is None
