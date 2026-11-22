import unittest
from types import SimpleNamespace
from unittest.mock import patch

from chess_market import Scanner, Series, Bar
from cloud_settings import defaults
from payout_guard import PayoutBook
from worker import TradingWorker


class ChartHistoryTests(unittest.IsolatedAsyncioTestCase):
    def test_history_only_ignores_live_backfill_and_locks_then_buys(self):
        a='EURGBP_otc'; now=[6260.0]
        s=Scanner(lambda:now[0],lambda:now[0]); s.enabled=s.strict_focus=s.history_only=True
        s.eligible=lambda _:True
        live=s.series.setdefault(a,Series())
        live.closed.extend(Bar(t,t,t+59,100,c) for t,c in [(6000,101),(6060,101),(6120,99),(6180,101)])
        s.request_history(a)
        def tick(t,p):
            now[0]=t; s.feed(['updateStream',[[a,t,p]]])
        tick(6260,101)
        self.assertIsNone(s.locked)
        for t,c in [(6000,101),(6060,101),(6120,99),(6180,101),(6240,101)]:
            s.accept_history({'asset':a,'period':60,'candles':[[t,100,c,102,98]]})
        tick(6261,101)
        self.assertEqual(s.locked,(a,6300))
        self.assertFalse(s.entries)
        for t in (6275,6290,6299): tick(t,99)
        tick(6300.2,100)
        self.assertEqual([(e.asset,e.direction) for e in s.entries],[(a,'BUY')])

    async def test_next_asset_after_history_timeout_despite_live_ticks(self):
        a,b='AUDCHF_otc','EURGBP_otc'; now=[6000.0]
        s=Scanner(lambda:now[0],lambda:now[0]); s.history_only=True
        s.catalog={a:True,b:True}
        w=TradingWorker(SimpleNamespace(doc=defaults()))
        book=PayoutBook('PRACTICE'); seen=[]
        async def subscribe(asset):
            seen.append((asset,now[0])); s.request_history(asset)
            if asset==b: w.stop_event.set()
        async def sleep(_):
            now[0]+=.5
            s.series.setdefault(a,Series()).bar=Bar(6000,6000,now[0],100,100)
            if now[0]>6020: raise AssertionError('rotation stalled')
        w.po=SimpleNamespace(scanner=s,payout_book=book,subscribe=subscribe,connected=True,wrong_mode=False)
        with patch('worker.time.monotonic',lambda:now[0]),patch('worker.asyncio.sleep',sleep):
            book.feed(['updateAssets',[{'asset':asset,'payout':92} for asset in (a,b)]])
            await w.probe()
        self.assertEqual(seen,[(a,6000),(b,6012)])
