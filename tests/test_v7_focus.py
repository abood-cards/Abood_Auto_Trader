import unittest
from types import SimpleNamespace
from unittest.mock import patch

import scan_scope
from chess_market import Scanner
from cloud_settings import defaults, patched
from control_bot import scan_panel
from payout_guard import PayoutBook
from worker import TradingWorker


class FocusTests(unittest.TestCase):
    def setup_market(self):
        clock=[6000.0]
        s=Scanner(lambda:clock[0],lambda:clock[0])
        s.enabled=True
        s.eligible=lambda a: True
        selected=[]
        s.on_select=selected.append
        def feed(t, prices):
            clock[0]=t
            s.feed(['updateStream',[[a,t,p] for a,p in prices.items()]])
        for i,c in enumerate((1,1,-1,1)):
            for sec in (.2,8,16,24,32,40,48,56,59.3):
                price=100 if sec<55 else 100+c
                feed(6000+i*60+sec,{'BTCUSD':price,'ETHUSD':price})
        for sec in (.2,8,16,24,32,40,48,51):
            price=100 if sec<50 else 99
            feed(6240+sec,{'BTCUSD':price,'ETHUSD':price})
        return s,feed,selected

    def test_focus_waits_despite_reversal_then_rejects_at_close(self):
        s,feed,selected=self.setup_market()
        self.assertEqual(s.locked,('BTCUSD',6300))
        feed(6299,{'BTCUSD':101,'ETHUSD':99})
        self.assertEqual(s.locked,('BTCUSD',6300))
        feed(6300.2,{'ETHUSD':100,'BTCUSD':100})
        self.assertFalse(s.entries)
        self.assertIsNone(s.locked)
        self.assertEqual(selected,['BTCUSD'])

    def test_success_only_for_locked_asset(self):
        s,feed,_=self.setup_market()
        feed(6299,{'BTCUSD':99,'ETHUSD':99})
        feed(6300.2,{'ETHUSD':100,'BTCUSD':100})
        self.assertEqual([(e.asset,e.direction) for e in s.entries],[('BTCUSD','BUY')])

    def test_selection_and_timeout_release_focus(self):
        s,_,_=self.setup_market()
        s.expire_focus(6303)
        self.assertIsNone(s.locked)
        s,_,_=self.setup_market()
        s.allowed=lambda a:a=='ETHUSD'
        s.expire_focus()
        self.assertIsNone(s.locked)

    def test_scope_validation_category_and_menu(self):
        d=defaults()
        scope={'mode':'GROUPS','groups':['crypto','stocks'],'symbols':[]}
        d=patched(d,{'scan':scope})
        self.assertTrue(scan_scope.allowed(d['scan'],'BTCUSD',{}))
        self.assertTrue(scan_scope.allowed(d['scan'],'ACME',{'ACME':'stocks'}))
        self.assertFalse(scan_scope.allowed(d['scan'],'EURUSD_otc',{}))
        _,kb=scan_panel(d)
        self.assertEqual(sum(b.text.startswith('✅') for r in kb.inline_keyboard for b in r),2)
        self.assertEqual(scan_scope.parse_symbols('BTCUSD، btcusd #AAPL_otc'),['BTCUSD','#AAPL_otc'])
        with self.assertRaises(ValueError):
            patched(d,{'scan':{'mode':'GROUPS','groups':[],'symbols':[]}})


class ResumeTests(unittest.IsolatedAsyncioTestCase):
    async def test_pause_no_subscription_then_next_wrap_and_scope(self):
        doc=defaults();doc['scan']={'mode':'GROUPS','groups':['crypto'],'symbols':[]}
        w=TradingWorker(SimpleNamespace(doc=doc))
        s=Scanner();s.catalog=dict.fromkeys(('ADAUSD','BTCUSD','ETHUSD','EURUSD'),True)
        s.locked=s.last_candidate=('BTCUSD',6300)
        w.cursor='BTCUSD'
        seen=[]; starts=[]; t=[1000]
        async def subscribe(asset):
            seen.append(asset)
            if len(seen)==3:
                w.stop_event.set()
        async def sleep(seconds):
            t[0]+=11
            starts.append(list(seen))
            if len(starts)==2:
                s.locked=None
        w.po=SimpleNamespace(connected=True,wrong_mode=False,scanner=s,subscribe=subscribe,payout_book=PayoutBook('PRACTICE'))
        with patch('worker.time.monotonic',lambda:t[0]),patch('worker.asyncio.sleep',sleep):
            await w.probe()
        self.assertEqual(starts[:2],[[],[]])
        self.assertEqual(seen,['ETHUSD','ADAUSD','BTCUSD'])
