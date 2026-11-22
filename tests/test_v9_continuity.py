import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from chess_market import Scanner
from cloud_settings import defaults, patched
from payout_guard import PayoutBook
from worker import TradingWorker
import test_chess


class ContinuityTests(unittest.IsolatedAsyncioTestCase):
    async def test_ignored_delayed_ticks_then_closed_match_reaches_worker(self):
        clock=[6000.0]
        scanner=Scanner(lambda:clock[0],lambda:clock[0]); scanner.enabled=True
        asset='AUDCAD_otc'
        selected=[]
        scanner.on_select=selected.append
        # Simulate a gate which only becomes eligible at the fifth close.
        scanner.eligible=lambda a:clock[0]>=6300
        def tick(wall,when,price):
            clock[0]=wall
            scanner.feed(['updateStream',[[asset,when,price]]])
        for i,color in enumerate((1,1,-1,1,-1)):
            start=6000+i*60
            for sec in (.2,8,16,24,32,40,48,56,59.3):
                if sec==40:
                    tick(start+39,start+37,999)
                tick(start+sec,start+sec,100+color if sec>=56 else 100)
        self.assertIsNone(scanner.locked)
        self.assertFalse(scanner.entries)
        tick(6300.2,6300.2,100)
        self.assertEqual(scanner.series[asset].ready_depth(),5)
        self.assertEqual(scanner.closed_checks['MATCH'],1)
        self.assertEqual(selected,[asset])
        w,_=test_chess.ExecutionTests().mock_worker([('win',.9)])
        w.po.scanner=scanner
        with patch('worker.time.time',return_value=6300.2):
            await w.chain(scanner.entries.popleft(),w.repo.doc)
        w.po.send_order.assert_awaited_once()
        order=w.po.send_order.await_args.args[0]
        self.assertEqual((order['asset'],order['action'],order['amount']),(asset,'call',1))

    async def test_active_lock_suppresses_other_asset_at_same_close(self):
        from test_v7_focus import FocusTests
        s,feed,_=FocusTests().setup_market()
        feed(6299,{'BTCUSD':101,'ETHUSD':99})
        feed(6300.2,{'BTCUSD':100,'ETHUSD':100})
        self.assertIsNone(s.locked)
        self.assertFalse(s.entries)
        self.assertEqual(s.last_candidate,('BTCUSD',6300))

    async def test_stale_quote_preserves_only_previously_qualified_feed(self):
        w=TradingWorker(SimpleNamespace(doc=defaults()))
        book=PayoutBook('PRACTICE'); w.po=SimpleNamespace(payout_book=book)
        book.feed(['updateAssets',[{'asset':'AUDCAD_otc','payout':92},{'asset':'EURUSD','payout':91}]])
        self.assertTrue(w.keep_warming('AUDCAD_otc'))
        for q in book.quotes.values(): q.at=time.monotonic()-361
        self.assertTrue(w.keep_warming('AUDCAD_otc'))
        self.assertFalse(w.keep_warming('EURUSD'))
        self.assertEqual(w.payout('AUDCAD_otc')[0],'STALE')
        book.feed(['updatePayout',{'asset':'AUDCAD_otc','payout':89}])
        self.assertFalse(w.keep_warming('AUDCAD_otc'))

    async def test_accepts_extended_steps_rejects_out_of_range(self):
        for count in (0,4,8):
            self.assertEqual(patched(defaults(),{'trading.martingale_steps':count})['trading']['martingale_steps'],count)
        for count in (-1,9,4.0,True):
            with self.assertRaises(ValueError): patched(defaults(),{'trading.martingale_steps':count})
