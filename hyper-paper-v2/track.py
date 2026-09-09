"""Prospective wallet-cohort exits; never writes production or submits orders."""
import datetime as dt
import fcntl
import json
import pathlib
import subprocess
import time

ROOT = pathlib.Path(__file__).resolve().parent
DAY = 86400000
MAX_AGE = 1800000
THRESHOLD = 3
EXCLUDED = ('0x8def9f50456c6c4e37fa5d3d57f108ed23992dae', 'HYPE')
REMOTE = r'''
import json,pathlib,hashlib,urllib.request,subprocess,time,concurrent.futures
r=pathlib.Path('/home/ubuntu/hyper-alerts')
a=json.loads(pathlib.Path('/home/ubuntu/hyper-state/alerts.json').read_text())
d=json.loads(pathlib.Path('/home/ubuntu/hyper-state/dashboard_snapshot.json').read_text())
hashes={str(n):hashlib.sha256((r/n).read_bytes()).hexdigest() for n in ['server.py','data/tracked_wallets.json']}
hashes['config']=hashlib.sha256(json.dumps(a.get('config',{}),sort_keys=True).encode()).hexdigest()
def api(payload):
    req=urllib.request.Request('https://api.hyperliquid.xyz/info',data=json.dumps(payload).encode(),headers={'Content-Type':'application/json'})
    with urllib.request.urlopen(req,timeout=20) as resp: return json.load(resp)
wallets=[{k:w.get(k) for k in ['address','fetchedAt','positions']} for w in d['wallets']]
targets=sorted(set(TARGETS)|{w['address'].lower() for w in wallets})
def positions(addr):
    try:
        v=api({'type':'clearinghouseState','user':addr})
        if not isinstance(v.get('assetPositions'),list): raise ValueError('missing assetPositions')
        return addr,{'ok':True,'observedAt':int(time.time()*1000),'positions':{p['position']['coin']:float(p['position']['szi']) for p in v['assetPositions']}}
    except Exception as e: return addr,{'ok':False,'error':type(e).__name__}
with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool: live=dict(pool.map(positions,targets))
meta,ctx=api({'type':'metaAndAssetCtxs'})
marks={u['name']:float(c['markPx']) for u,c in zip(meta['universe'],ctx) if not u.get('isDelisted')}
state=a.get('state',{})
records=state.get('copyabilityEntryOutcomes')
if not isinstance(records,dict): records=state.get('actionableEntryOutcomes',{})
print(json.dumps({'records':records,'hashes':hashes,'commit':subprocess.check_output(['git','-C',str(r),'rev-parse','HEAD'],text=True).strip(),'marks':marks,'observedAt':int(time.time()*1000),'wallets':wallets,'live':live}))
'''

def stamp(value):
    try: return int(dt.datetime.fromisoformat(value.replace('Z','+00:00')).timestamp()*1000)
    except (ValueError,TypeError,AttributeError): return 0

def live_size(snap, addr, coin):
    w=snap['live'].get(addr,{})
    if not w.get('ok') or not 0<=snap['observedAt']-w.get('observedAt',0)<=120000: return None
    return w['positions'].get(coin,0)

def cohort(snap, coin, side, count, recorded=None):
    if isinstance(recorded,list) and recorded:
        result=sorted(set(str(a).lower() for a in recorded if isinstance(a,str)))
        if len(result)!=count or len(result)<THRESHOLD: return None
        sign=1 if side=='long' else -1
        if any(live_size(snap,a,coin) is None or live_size(snap,a,coin)*sign<=0 for a in result): return None
        return result
    result=[]
    for w in snap['wallets']:
        if not 0<=snap['observedAt']-stamp(w.get('fetchedAt'))<=MAX_AGE or not isinstance(w.get('positions'),list):
            return None
        addr=w['address'].lower()
        if (addr,coin)==EXCLUDED: continue
        for p in w['positions']:
            if p.get('coin')==coin and p.get('side','').lower()==side and float(p.get('unrealizedPnl') or 0)>=-1000000 and abs(float(p.get('size') or 0))>0:
                result.append(addr)
                break
    result=sorted(set(result))
    if len(result)!=count or len(result)<THRESHOLD: return None
    sign=1 if side=='long' else -1
    if any(live_size(snap,a,coin) is None or live_size(snap,a,coin)*sign<=0 for a in result): return None
    return result

def outcome(row,snap):
    sign=1 if row['side']=='long' else -1
    gross=sign*(snap['marks'][row['coin']]/row['entryPrice']-1)*100
    benchmark=sign*(snap['marks']['BTC']/row['btcEntry']-1)*100
    return dict(exitAt=snap['observedAt'],exitPrice=snap['marks'][row['coin']],btcExit=snap['marks']['BTC'],grossPct=gross,netPct=gross-.20,benchmarkNetPct=benchmark-.20,excessPct=gross-benchmark,pnlUsd=(gross-.20)*10)

def advance(s,snap):
    now=snap['observedAt']; marks=snap['marks']
    if snap['hashes']!=s['hashes']: s['paused']=True
    for row in s['trades']:
        priced=marks.get(row['coin'],0)>0 and marks.get('BTC',0)>0
        if row.get('control24h') is None and now>=row['dueAt'] and priced:
            row['control24h']={**outcome(row,snap),'exitDelayMs':now-row['dueAt'],'degraded':now-row['dueAt']>MAX_AGE}
        if row['status']!='open': continue
        if now-row['lastWalletCheckAt']>MAX_AGE: row['observationGap']=True
        sign=1 if row['side']=='long' else -1
        unknown=[]
        for addr in row['activeWallets'][:]:
            size=live_size(snap,addr,row['coin'])
            if size is None: unknown.append(addr)
            elif sign*size<=0:
                row['activeWallets'].remove(addr)
                member_result=outcome(row,snap)
                row['departures'].append({
                    'address':addr,
                    'detectedAt':now,
                    'reason':'flat' if size==0 else 'flipped',
                    'exitPrice':member_result['exitPrice'],
                    'btcExit':member_result['btcExit'],
                    'grossPct':member_result['grossPct'],
                    'netPct':member_result['netPct'],
                    'benchmarkNetPct':member_result['benchmarkNetPct'],
                    'excessPct':member_result['excessPct'],
                })
        row['unknownWallets']=unknown
        if not unknown: row['lastWalletCheckAt']=now
        # Unknown members remain active: missing data cannot cause an exit.
        if len(row['activeWallets'])<row['exitThreshold']:
            row.setdefault('exitDetectedAt',now)
        if row.get('exitDetectedAt') and priced:
            row.update(outcome(row,snap),status='closed',exitReason='original_cohort_below_threshold',exitDelayMs=now-row['exitDetectedAt'],degraded=row['observationGap'] or now-row['exitDetectedAt']>MAX_AGE)
    for key,r in sorted(snap['records'].items(),key=lambda kv:(kv[1].get('startedAt',0),kv[0])):
        if key in s['seen']: continue
        s['seen'].append(key)
        start=int(r.get('startedAt') or 0); coin=r.get('coin'); side=r.get('side'); members=None
        reason=None
        if start<s['startAt']: reason='before_start'
        elif s['paused']: reason='source_changed'
        elif now>=s['endAt']: reason='enrollment_finished'
        elif not (r.get('delivered') or r.get('source')=='copyabilityResearch') or side not in ('long','short'): reason='invalid_delivery_or_side'
        elif not 0<=now-start<=MAX_AGE: reason='late_or_future_signal'
        elif coin not in s['universe'] or marks.get(coin,0)<=0 or marks.get('BTC',0)<=0: reason='unsupported_or_missing_price'
        elif any(x['coin']==coin and x['status']=='open' for x in s['trades']): reason='coin_already_open'
        elif sum(x['status']=='open' for x in s['trades'])>=10: reason='capacity'
        else:
            members=cohort(snap,coin,side,int(r.get('walletCount') or 0),r.get('walletAddresses'))
            if members is None: reason='cohort_unverifiable'
        if reason: s['skipped'].append({'id':key,'observedAt':now,'reason':reason})
        else:
            s['trades'].append({'id':key,'coin':coin,'side':side,'signal':r,'entryAt':now,'entryPrice':marks[coin],'btcEntry':marks['BTC'],'dueAt':now+DAY,'entryDelayMs':now-start,'notionalUsd':1000,'status':'open','initialWallets':members,'activeWallets':members[:],'exitThreshold':THRESHOLD,'departures':[],'unknownWallets':[],'lastWalletCheckAt':now,'observationGap':False,'control24h':None})
    s['lastObservedAt']=now
    return s

def save(path,value):
    temp=path.with_suffix('.tmp'); temp.write_text(json.dumps(value,indent=2,ensure_ascii=False)+'\n'); temp.replace(path)

def main():
    with (ROOT/'collector.lock').open('w') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        path=ROOT/'state.json'; s=json.loads(path.read_text()) if path.exists() else None
        targets=sorted({a for row in (s or {}).get('trades',[]) if row['status']=='open' for a in row['initialWallets']})
        remote='TARGETS='+repr(targets)+'\n'+REMOTE
        raw=subprocess.check_output(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=10','-i','/Users/alexozga/Downloads/openclaw.pem','ubuntu@13.63.166.252','python3 -'],input=remote,text=True,timeout=240)
        snap=json.loads(raw); now=snap['observedAt']
        if abs(int(time.time()*1000)-now)>120000: raise ValueError('Remote clock or snapshot stale')
        if s is None:
            s={'version':'v2','startAt':now,'endAt':now+25*DAY,'hashes':snap['hashes'],'baselineCommit':snap['commit'],'universe':sorted(snap['marks']),'seen':list(snap['records']),'baselineExcluded':len(snap['records']),'trades':[],'skipped':[],'paused':False}
            save(ROOT/'baseline.json',snap)
        (ROOT/'observations').mkdir(exist_ok=True); save(ROOT/'observations'/f'{now}.json',snap)
        s=advance(s,snap); save(path,s)
        closed=[x for x in s['trades'] if x['status']=='closed']; controls=[x['control24h'] for x in s['trades'] if x.get('control24h')]
        wallet_results={}
        for trade in s['trades']:
            for departure in trade.get('departures',[]):
                if departure.get('netPct') is None: continue
                wallet_results.setdefault(departure['address'],[]).append(departure)
        wallet_copyability={address:{'method':'enter_after_detection_exit_with_wallet','completedEpisodes':len(results),'meanCostAdjustedNetReturnPct':sum(x['netPct'] for x in results)/len(results),'meanExcessPct':sum(x['excessPct'] for x in results)/len(results)} for address,results in sorted(wallet_results.items())}
        report={'version':'v2','startUTC':dt.datetime.fromtimestamp(s['startAt']/1000,dt.timezone.utc).isoformat(),'enrollmentEndUTC':dt.datetime.fromtimestamp(s['endAt']/1000,dt.timezone.utc).isoformat(),'paused':s['paused'],'open':len(s['trades'])-len(closed),'closed':len(closed),'degraded':sum(x['degraded'] for x in closed),'skipped':len(s['skipped']),'meanNetPct':sum(x['netPct'] for x in closed)/len(closed) if closed else None,'meanExcessPct':sum(x['excessPct'] for x in closed)/len(closed) if closed else None,'paperPnlUsd':sum(x['pnlUsd'] for x in closed),'control24hCount':len(controls),'control24hMeanNetPct':sum(x['netPct'] for x in controls)/len(controls) if controls else None,'walletCopyability':wallet_copyability,'walletFetchErrors':sum(not x.get('ok') for x in snap['live'].values()),'lastObservedAt':now,'complete':now>=s['endAt'] and len(closed)==len(s['trades']) and len(controls)==len(s['trades'])}
        save(ROOT/'report.json',report); print(json.dumps(report,ensure_ascii=False))

if __name__=='__main__': main()
