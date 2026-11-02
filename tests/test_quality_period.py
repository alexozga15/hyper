from types import SimpleNamespace
from unittest.mock import patch

import server
from available_info_rank import apply_live_rank
from history_cache import ResumableHistoryCache
from quality_evidence import quality_assessment, format_quality_assessment
from quality_period import cache_period_matches, hold_previous_window_risk

DAY=86_400_000


def healthy(days=90):
    return server.build_risk_trend_quality_rank(
        pnl_7d=10,pnl_30d=30,pnl_180d=100,pnl_all_time=200,
        sortino_180d=2,calmar_180d=3,adjusted_profit_factor_180d=2.5,
        episode_count_180d=100,loss_count_180d=20,daily_return_count_180d=days,
        downside_day_count_180d=20,max_drawdown_pct=10,window_trusted=True,
        equity_curve_complete=True,equity_curve_verified=True,
        largest_loser_pct=1,largest_loser_complete=True,period_days=days)


def test_new_period_has_distinct_fields_method_and_trend():
    rank=healthy()
    assert rank['periodDays']==90 and rank['metric'].endswith('90d_v3')
    assert rank['pnl90d']==100 and rank['sortino90d']==2
    assert rank['trendChecks']['pnl90dAbove30d']
    assert not any('180d' in k for k in rank)
    old=healthy(180)
    assert old['periodDays']==180 and old['metric'].endswith('180d_v2')


def test_legacy_cache_and_seed_cannot_be_relabelled_as_90d():
    old=healthy(180)
    assert not cache_period_matches({'recentWinRateRank':old})
    rank=healthy()
    assert apply_live_rank(rank,{'periodDays':180},now_ms=1,open_loss=0,
                          largest_open_loss=0,total_portfolio_equity=100,risk_ok=True)==rank
    assessment=quality_assessment({'qualityPeriodDays':90,'recentWinRateRank':old},
                                 {'risk':{'status':'pass'}})
    assert assessment['quality']['score'] is None
    assert 'quality_period_mismatch' in assessment['data']['reasons']


def test_unknown_new_window_cannot_erase_previous_shadow():
    old={**healthy(180),'label':'Shadow','shadowReasons':['observed_drawdown']}
    candidate={**healthy(),'equityCurveVerified':False}
    held=hold_previous_window_risk(candidate,old)
    assert held['label']=='Shadow' and not held['eliteEligible']
    assert 'previous_window_risk_pending_reassessment' in held['shadowReasons']
    assert hold_previous_window_risk(candidate,held)['label']=='Shadow'
    fresh={**healthy(),'pfHistoryComplete':True,'currentRiskKnown':True}
    assert hold_previous_window_risk(fresh,old)['label']=='Elite'


def test_live_risk_refresh_uses_90d_components_without_mixing_old_fields():
    rank={**healthy(),'closedGrossProfit90d':250,'closedGrossLoss90d':100,
          'openCostsLoss90d':0,'pfHistoryComplete':True,'lossCapitalMethod':'fresh_bracket_v1',
          'historyThroughMs':100,'qualityWindowEndMs':90}
    updated=server.refresh_current_rank_risk(rank,open_loss=300,account_value=1000,
                                            state_ok=True,latest_fill_ms=110)
    assert updated['adjustedProfitFactor90d']==0.625 and updated['periodDays']==90
    assert updated['label']=='Shadow' and updated['historyCurrent']
    a=quality_assessment({'qualityPeriodDays':90,'recentWinRateRank':updated},
                         {'risk':{'status':'blocked'}})
    assert a['quality']['periodDays']==90 and 'Quality 90D:' in format_quality_assessment(a)


def test_runtime_uses_ninety_closed_days_and_excludes_today_from_quality(tmp_path):
    now=200*DAY+DAY//2
    points=[[d*DAY,1000+d] for d in range(20,201)]
    curve={'accountValueHistory':points,'pnlHistory':[[d*DAY,d] for d in range(20,201)]}
    fills=[]
    for day in (100,171,200):
        for i,(start,side,pnl) in enumerate(((0,'B',0),(1,'A',-2))):
            fills.append({'time':day*DAY+1000+i,'coin':f'X{day}', 'startPosition':str(start),
                          'sz':'1','side':side,'closedPnl':str(pnl),'fee':'0','px':'100','tid':day*10+i})
    calls=[]
    def post(payload,default):
        calls.append(payload)
        if payload['type']=='portfolio':return {'ok':True,'data':[['perpAllTime',curve]]}
        if payload['type']=='userFillsByTime':
            return {'ok':True,'data':[f for f in fills if f['time']>=payload['startTime']]}
        if payload['type']=='userFills':return {'ok':True,'data':fills}
        return {'ok':True,'data':[]}
    client=SimpleNamespace(safe_post_result=post,safe_post=lambda *args:'user',
        safe_subscribe_all_dexs_clearinghouse_state=lambda *args:{'_fetchOk':True,
            'marginSummary':{'accountValue':'1200'},'assetPositions':[]})
    service=server.WalletTrackerService(object(),client)
    service.wallet_quality_cache_path=tmp_path/'quality.json'
    service.history_cache=ResumableHistoryCache(tmp_path/'history.sqlite3')
    with patch('server.current_time_ms',return_value=now):
        snapshot=service.fetch_wallet_snapshot(server.TrackedWallet('a','','',''))
    assert server.SORTINO_WINDOW_DAYS==90 and server.RISK_SCORE_MIN_DAILY_RETURNS==90
    assert snapshot['qualityWindowStartMs']==110*DAY and snapshot['qualityWindowEndMs']==200*DAY
    assert snapshot['qualityPeriodDays']==90 and 'episodes180d' not in snapshot
    assert snapshot['episodes90d']==1 and snapshot['qualityClosedEvents30d']==2
    assert snapshot['recentWinRateRank']['closedGrossLoss90d']==2
    assert snapshot['dailyReturns90d']==90
    assert {p['startTime'] for p in calls if p['type']=='userFillsByTime'} >= {110*DAY}
    assert server.execution_rule_manifest()['riskGates']['periodDays']==90
