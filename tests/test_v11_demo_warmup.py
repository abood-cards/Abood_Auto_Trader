import unittest
from types import SimpleNamespace
from unittest.mock import patch
from chess_market import Scanner, Series, Bar
from cloud_settings import defaults
from payout_guard import PayoutBook
from worker import TradingWorker


class DemoWarmupTests(unittest.IsolatedAsyncioTestCase):
    async def test_old_asset_ticks_do_not_cause_rotation_during_warmup(self):
        now=[1000.0]
        s=Scanner(lambda:now[0],lambda:now[0]); s.catalog={'EURUSD':True,'GBPUSD':True}
        b=PayoutBook('PRACTICE'); seen=[]
        w=TradingWorker(SimpleNamespace(doc=defaults()))
        async def subscribe(asset):
            seen.append(asset)
            if len(seen)==2: w.stop_event.set()
        async def advance(_):
            now[0]+=1
            t=now[0]
            s.series.setdefault('EURUSD',Series()).bar=Bar(int(t//60)*60,t,t,1,1)
            # Real parallel data only arrives 30 seconds after the new request.
            other=t if t>=1030 else 1000
            s.series.setdefault('GBPUSD',Series()).bar=Bar(int(other//60)*60,other,other,1,1)
            if t==1034:
                self.assertEqual(seen,['EURUSD'])
                w.stop_event.set()
        w.po=SimpleNamespace(scanner=s,payout_book=b,subscribe=subscribe,connected=True,wrong_mode=False)
        with patch('worker.time.monotonic',lambda:now[0]),patch('worker.asyncio.sleep',advance):
            b.feed(['updateAssets',[{'asset':a,'payout':91} for a in s.catalog]])
            await w.probe()
        self.assertEqual(seen,['EURUSD'])

    def test_strict_demo_requires_prior_focus_even_when_last_five_match(self):
        t=[6000.0]; s=Scanner(lambda:t[0],lambda:t[0]); s.enabled=True; s.strict_focus=True
        a='OMRCNY_otc'
        def tick(at,price):
            t[0]=at;s.feed(['updateStream',[[a,at,price]]])
        s.eligible=lambda _:t[0]>=6300
        for i,c in enumerate((1,1,-1,1,-1)):
            for sec in (.2,8,16,24,32,40,48,56,59.3):
                tick(6000+i*60+sec,100+(c if sec>=56 else 0))
        tick(6300.2,100)
        self.assertEqual(s.closed_checks['NO_PRIOR_FOCUS'],1)
        self.assertFalse(s.entries)
