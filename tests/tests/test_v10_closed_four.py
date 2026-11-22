import time
import unittest
from types import SimpleNamespace

from chess_market import Scanner
from cloud_settings import defaults
from payout_guard import PayoutBook
from worker import TradingWorker


class ClosedFourTests(unittest.TestCase):
    def test_lock_on_opening_neutral_and_only_enter_on_confirmed_close(self):
        for seed in (-1,1):
            for valid in (False,True):
                with self.subTest(seed=seed,valid=valid):
                    now=[6000.0]
                    s=Scanner(lambda:now[0],lambda:now[0]); s.enabled=True
                    s.eligible=lambda _:True
                    def tick(at,price):
                        now[0]=at
                        s.feed(['updateStream',[[a,at,price] for a in ('USD1','USD2')]])
                    for i,color in enumerate((seed,seed,-seed,seed)):
                        for sec in (.2,8,16,24,32,40,48,56,59.3):
                            tick(6000+i*60+sec,100+color if sec>=56 else 100)
                    tick(6240.2,100)
                    self.assertEqual(s.locked,('USD1',6300))
                    self.assertFalse(s.entries)
                    for sec in (8,16,24,32,40,48,56,59.3):
                        tick(6240+sec,100+seed if sec<50 or not valid else 100-seed)
                        self.assertEqual(s.locked,('USD1',6300))
                    self.assertFalse(s.entries)
                    tick(6300.2,100)
                    self.assertIsNone(s.locked)
                    self.assertEqual([(e.asset,e.direction) for e in s.entries],
                                     [('USD1','SELL' if seed==-1 else 'BUY')] if valid else [])

    def test_payout_order_and_time_for_five_candles(self):
        d=defaults();d['scan']={'mode':'GROUPS','groups':['stocks','crypto','forex'],'symbols':[]}
        book=PayoutBook('PRACTICE');s=Scanner();w=TradingWorker(SimpleNamespace(doc=d,leader=True))
        w.po=SimpleNamespace(scanner=s,payout_book=book)
        rates={'AUDUSD':89,'EURUSD':91,'GBPUSD':92,'NZDUSD':92,'BTCUSD':90,'#AAPL_otc':91,'#TSLA_otc':88}
        book.feed(['updateAssets',[{'asset':asset,'payout':rate} for asset,rate in rates.items()]])
        for q in book.quotes.values(): q.at=time.monotonic()-305
        self.assertEqual(w.scan_order(dict.fromkeys(rates,True)),['GBPUSD','NZDUSD','EURUSD','BTCUSD','#AAPL_otc'])
        self.assertFalse(w.payout_due(s,time.monotonic()))
        book.feed(['updatePayout',{'asset':'GBPUSD','payout':89}])
        self.assertEqual(w.payout('GBPUSD')[0],'OUT_OF_RANGE')
