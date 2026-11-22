import json
import unittest
from unittest.mock import AsyncMock, patch

from chess_market import Scanner
from chess_transport import ChessPocket, Decoder
from cloud_settings import defaults
import test_chess


class HistoryTests(unittest.IsolatedAsyncioTestCase):
    async def test_demo_subscription_requests_m1(self):
        p=ChessPocket(defaults()['demo_account'],'PRACTICE')
        p.connected=True; p.scanner.strict_focus=True; p.select_asset=AsyncMock()
        await p.subscribe('KESUSD_otc')
        p.select_asset.assert_awaited_once_with('KESUSD_otc',60)
        self.assertEqual(p.scanner.history_asset,'KESUSD_otc')

    async def test_snapshot_locks_without_waiting_four_minutes_then_sends_only_matching_close(self):
        for seed,final,expected in [(-1,1,'put'),(-1,-1,None),(1,-1,'call')]:
            a='KESUSD_otc'; wall=[6263.0]
            s=Scanner(lambda:wall[0],lambda:wall[0]); s.strict_focus=True; s.enabled=True
            s.eligible=lambda _:True
            rows=[[6000+i*60,100,100+c,102,98] for i,c in enumerate((seed,seed,-seed,seed))]
            rows.append([6240,100,100+seed,102,98])
            d=Decoder()
            events=d.feed(json.dumps({'asset':a,'period':60,'candles':rows}).encode())
            self.assertEqual(events[0][0],'m1History')
            s.request_history(a)
            s.accept_history(events[0][1])
            def tick(t,price):
                wall[0]=t; s.feed(['updateStream',[[a,t,price]]])
            tick(6263,100+seed)
            self.assertEqual(s.locked,(a,6300)); self.assertEqual(s.series[a].ready_depth(),4)
            self.assertFalse(s.entries)
            tick(6279,100+seed); tick(6295.4,100+final); tick(6300.2,100)
            self.assertEqual(bool(s.entries),expected is not None)
            if expected:
                worker,_=test_chess.ExecutionTests().mock_worker([('win',.9)])
                worker.po.scanner=s
                worker.po.payout_book.feed(['updateAssets',[{'asset':a,'payout':92}]])
                with patch('worker.time.time',return_value=6300.2):
                    await worker.chain(s.entries.popleft(),worker.repo.doc)
                worker.po.send_order.assert_awaited_once()
                self.assertEqual(worker.po.send_order.await_args.args[0]['action'],expected)

    async def test_wrong_symbol_or_period_and_missing_minute_dont_seed(self):
        s=Scanner(lambda:6263,lambda:6263); s.strict_focus=True; s.enabled=True
        s.eligible=lambda _:True; s.request_history('KESUSD_otc')
        candles=[[t,100,99,101,98] for t in (5940,6000,6060,6180)]
        for asset,period in [('OTHER',60),('KESUSD_otc',5)]:
            s.accept_history(dict(asset=asset,period=period,candles=candles))
            self.assertIsNone(s.history_rows)
        s.accept_history(dict(asset='KESUSD_otc',period=60,candles=candles))
        s.feed(['updateStream',[['KESUSD_otc',6263,100]]])
        self.assertEqual(s.series['KESUSD_otc'].ready_depth(),0)
        self.assertIsNone(s.locked)
