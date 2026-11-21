import asyncio
import json
import time
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from chess_transport import ChessPocket, Decoder
from cloud_settings import defaults
from payout_guard import PayoutBook, scan_payout
import test_chess
from worker import TradingWorker

ASSETS = ('AEDCNY_otc', '#AAPL_otc', 'BTCUSD', 'EURUSD')


class MarketFeedTests(unittest.TestCase):
    def test_transport_reads_mixed_catalog_and_concurrent_ticks(self):
        p = ChessPocket(defaults()['demo_account'],'PRACTICE')
        rows = [[1,a,'label',0,0,90+i%3,0,0,0,0,0,0,0,0,True] for i,a in enumerate(ASSETS)]
        decoder = Decoder()
        for frame in (json.dumps(rows).encode(), json.dumps([[a,time.time(),100] for a in ASSETS]).encode()):
            for event in decoder.feed(frame):
                p.process(event)
        self.assertEqual(set(p.scanner.catalog),set(ASSETS))
        self.assertEqual(set(p.scanner.active_assets()),set(ASSETS))
        self.assertEqual([scan_payout(p.payout_book,a,60)[0] for a in ASSETS], ['KNOWN']*4)

    def test_payout_rejects_stale_unavailable_or_other_account(self):
        b = PayoutBook('PRACTICE')
        b.feed(['updateAssets',[{'symbol':'BTCUSD','payout':92,'isDemo':0},
                                {'symbol':'EURUSD','payout':92,'durations':[30]},
                                {'symbol':'#AAPL_otc','payout':93},
                                {'symbol':'AEDCNY_otc','payout':90},
                                {'symbol':'GBPUSD','payout':91,'tradable':False}]])
        self.assertEqual([scan_payout(b,a,60)[0] for a in ASSETS], ['KNOWN','OUT_OF_RANGE','ASSET_MISSING','DURATION'])
        self.assertEqual(scan_payout(b,'GBPUSD',60)[0],'CLOSED')
        b.quotes['AEDCNY_otc'].at-=61
        self.assertEqual(scan_payout(b,'AEDCNY_otc',60)[0],'STALE')


class WorkerScanTests(unittest.IsolatedAsyncioTestCase):
    async def test_rechecks_payout_after_reserve_before_wire(self):
        w,e = test_chess.ExecutionTests().mock_worker([('win',.9)])
        async def reserve(order):
            w.po.payout_book.feed(['updatePayout',{'symbol':order['asset'],'payout':89}])
        async def send(order, can_send):
            return can_send()
        w.repo.reserve.side_effect=reserve
        w.po.send_order.side_effect=send
        await w.chain(e,w.repo.doc)
        self.assertFalse(w.repo.resolve.await_args.kwargs['dispatched'])

    async def test_unknown_payout_blocks_without_reserving(self):
        w,e = test_chess.ExecutionTests().mock_worker([])
        w.po.payout_book.clear()
        await w.chain(e,w.repo.doc)
        w.repo.reserve.assert_not_awaited()
        w.po.send_order.assert_not_awaited()

    async def test_no_fixed_priority_and_rotation_tests_all_asset_classes(self):
        from chess_market import Scanner
        repo=SimpleNamespace(doc=defaults())
        repo.doc['trading']['asset']='EURUSD_otc'
        w=TradingWorker(repo)
        scanner=Scanner()
        scanner.catalog=dict.fromkeys(ASSETS,True)
        seen=[]
        async def subscribe(asset):
            seen.append(asset)
            if len(seen)==len(ASSETS):
                w.stop_event.set()
        t=[1000]
        async def advance(seconds):
            t[0]+=11
        w.po=SimpleNamespace(connected=True,wrong_mode=False,scanner=scanner,subscribe=subscribe,payout_book=PayoutBook('PRACTICE'))
        with patch('worker.time.monotonic',lambda: t[0]), patch('worker.asyncio.sleep',advance):
            await w.probe()
        self.assertEqual(seen,sorted(ASSETS))
        self.assertEqual(w.probed,set(ASSETS))
