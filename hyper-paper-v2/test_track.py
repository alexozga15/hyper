import copy
import datetime as dt
import unittest
from track import advance, DAY

class WalletExitTests(unittest.TestCase):
    def setUp(self):
        self.now=1800000000000
        self.s={'hashes':{},'paused':False,'trades':[],'seen':[],'startAt':self.now-1000,'endAt':self.now+25*DAY,'universe':['ETH','BTC'],'skipped':[]}
        self.p={'hashes':{},'observedAt':self.now,'marks':{'ETH':100,'BTC':100},'records':{'a':{'coin':'ETH','side':'long','startedAt':self.now-1,'delivered':True,'walletCount':3}},'wallets':[{'address':a,'fetchedAt':dt.datetime.fromtimestamp(self.now/1000,dt.timezone.utc).isoformat(),'positions':[{'coin':'ETH','side':'long','size':10}]} for a in ['a','b','c']],'live':{a:{'ok':True,'observedAt':self.now,'positions':{'ETH':10}} for a in ['a','b','c']}}
        advance(self.s,self.p)
        self.assertEqual(len(self.s['trades']),1)

    def tick(self,delta=900000):
        self.p['observedAt']+=delta
        for w in self.p['live'].values(): w['observedAt']=self.p['observedAt']

    def test_partial_unknown_and_new_wallet_do_not_exit(self):
        self.tick();self.p['live']['a']['positions']['ETH']=.001
        self.p['live']['b']={'ok':False}
        self.p['live']['new']={'ok':True,'observedAt':self.p['observedAt'],'positions':{'ETH':50}}
        advance(self.s,self.p)
        self.assertEqual(self.s['trades'][0]['status'],'open')
        self.assertEqual(self.s['trades'][0]['activeWallets'],['a','b','c'])

    def test_flat_exit_and_later_24h_control(self):
        self.tick();self.p['live']['a']['positions']={};self.p['marks']['ETH']=110
        advance(self.s,self.p);r=self.s['trades'][0]
        self.assertEqual(r['status'],'closed');self.assertAlmostEqual(r['netPct'],9.8)
        self.assertAlmostEqual(r['departures'][0]['netPct'],9.8)
        self.assertEqual(r['departures'][0]['exitPrice'],110)
        self.assertIsNone(r['control24h'])
        self.tick(DAY-900000);self.p['marks']['ETH']=90;advance(self.s,self.p)
        self.assertAlmostEqual(r['control24h']['netPct'],-10.2)

    def test_24h_does_not_close_main(self):
        self.tick(DAY);advance(self.s,self.p)
        self.assertEqual(self.s['trades'][0]['status'],'open')
        self.assertIsNotNone(self.s['trades'][0]['control24h'])

    def test_flip_exits_even_when_source_changes(self):
        self.tick();self.p['hashes']={'changed':True};self.p['live']['a']['positions']['ETH']=-2
        advance(self.s,self.p)
        self.assertTrue(self.s['paused']);self.assertEqual(self.s['trades'][0]['status'],'closed')

    def test_reopened_member_does_not_rejoin(self):
        r=self.s['trades'][0];r['activeWallets'].append('d');r['initialWallets'].append('d')
        self.p['live']['d']={'ok':True,'observedAt':self.now,'positions':{'ETH':10}}
        self.tick();self.p['live']['a']['positions']={};advance(self.s,self.p)
        self.assertEqual(r['status'],'open')
        self.tick();self.p['live']['a']['positions']['ETH']=10;self.p['live']['b']['positions']={};advance(self.s,self.p)
        self.assertEqual(r['status'],'closed');self.assertNotIn('a',r['activeWallets'])

    def test_missing_cohort_and_duplicate(self):
        advance(self.s,self.p);self.assertEqual(len(self.s['trades']),1)
        self.s['trades']=[];self.s['seen']=[];self.p['records']['a']['walletCount']=4
        advance(self.s,self.p)
        self.assertEqual(self.s['skipped'][-1]['reason'],'cohort_unverifiable')

    def test_recorded_cohort_is_used_instead_of_reconstruction(self):
        state=copy.deepcopy(self.s);state['trades']=[];state['seen']=[];state['skipped']=[]
        payload=copy.deepcopy(self.p)
        payload['records']['a']['walletAddresses']=['a','b','c']
        payload['wallets']=[]
        advance(state,payload)
        self.assertEqual(state['trades'][0]['initialWallets'],['a','b','c'])

if __name__=='__main__': unittest.main()
