import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from chess_market import Scanner
from cloud_settings import defaults
from payout_guard import PayoutBook
from worker import TradingWorker


class PayoutFirstTests(unittest.TestCase):
    def test_ineligible_tick_does_not_build_bars(self):
        s=Scanner(lambda:6000,lambda:6000)
        s.track_price=lambda asset:asset=='USDCOP_otc'
        s.feed(['updateStream',[['USDVND_otc',6000,50],['USDCOP_otc',6000,100]]])
        self.assertNotIn('USDVND_otc',s.series)
        self.assertEqual(s.accepted,1)
        self.assertEqual(s.payout_filtered['USDVND_otc'],1)

    def test_lock_with_30s_left_holds_through_color_reversal(self):
        clock=[6000.0]
        s=Scanner(lambda:clock[0],lambda:clock[0]); s.enabled=True
        payout={'USDCOP_otc':True,'USDVND_otc':False}
        s.eligible=payout.__getitem__
        s.track_price=lambda a: payout[a] or bool(s.locked and s.locked[0]==a)
        def tick(t,p):
            clock[0]=t
            s.feed(['updateStream',[[a,t,p] for a in payout]])
        for i,c in enumerate((1,1,-1,1)):
            for sec in (.2,8,16,24,32,40,48,56,59.3):
                tick(6000+i*60+sec,100+c if sec>=56 else 100)
        for sec in (.2,8,16,24,30): tick(6240+sec,99 if sec==30 else 100)
        self.assertEqual(s.locked,('USDCOP_otc',6300))
        for sec in (38,46,54): tick(6240+sec,101)
        self.assertEqual(s.locked,('USDCOP_otc',6300))
        tick(6299.3,99)
        self.assertTrue(s.last_second)
        self.assertFalse(s.entries)
        tick(6300.2,100)
        self.assertEqual([(e.asset,e.direction) for e in s.entries],[('USDCOP_otc','BUY')])

    def test_refresh_rate_and_focus_guard(self):
        w=TradingWorker(SimpleNamespace(doc=defaults(),leader=True))
        s=Scanner(); b=PayoutBook('PRACTICE'); w.po=SimpleNamespace(scanner=s,payout_book=b)
        b.feed(['updateAssets',[{'asset':'USDCOP_otc','payout':91}]])
        self.assertFalse(w.payout_due(s,time.monotonic()))
        b.quotes['USDCOP_otc'].at-=361
        self.assertTrue(w.payout_due(s,time.monotonic()))
        s.locked=('USDCOP_otc',6300)
        self.assertFalse(w.payout_due(s,time.monotonic()))
        s.locked=None
        w.last_refresh=time.monotonic()
        self.assertFalse(w.payout_due(s,time.monotonic()))


class ProbeTests(unittest.IsolatedAsyncioTestCase):
    async def test_subscribes_to_fresh_eligible_asset_only(self):
        w=TradingWorker(SimpleNamespace(doc=defaults(),leader=True))
        s=Scanner(); b=PayoutBook('PRACTICE')
        s.catalog=dict.fromkeys(['AUDUSD','BTCUSD','EURUSD','USDJPY'],True)
        b.feed(['updateAssets',[{'asset':'AUDUSD','payout':50},{'asset':'BTCUSD','payout':91},{'asset':'USDJPY','payout':92}]])
        b.quotes['BTCUSD'].at-=361
        called=[]
        async def subscribe(asset):
            called.append(asset);w.stop_event.set()
        w.po=SimpleNamespace(connected=True,wrong_mode=False,scanner=s,payout_book=b,subscribe=subscribe)
        await w.probe()
        self.assertEqual(called,['USDJPY'])
