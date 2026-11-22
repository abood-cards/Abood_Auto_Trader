import asyncio
import json
import time
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from chess_market import Scanner, Entry
from chess_transport import ChessPocket, OrderResult, Decoder
from cloud_settings import defaults, validate_document, patched
from worker import TradingWorker
from payout_guard import PayoutBook


class MarketTests(unittest.TestCase):
    def setup_bars(self, colors, gap=False):
        clock=[6000.0]
        s=Scanner(lambda:clock[0], lambda:clock[0]); s.enabled=True
        a='AUDCAD_otc'
        s.feed(['updateAssets',[{'asset':a,'enabled':True},{'asset':'#AAPL_otc','enabled':True},{'asset':'EURUSD','enabled':True}]])
        def tick(ts,p):
            clock[0]=ts
            s.feed(['updateStream',[[a,ts,p]]])
        for i,c in enumerate(colors):
            t=6000+i*60
            for sec in [0.2,8,16,24,32,40,48,56,59.3]:
                if gap and i==2 and 8<=sec<=40:
                    continue
                tick(t+sec,100 if sec<55 else 100+c)
        return s, tick, 6000+60*len(colors)

    def test_sell_five(self):
        s,t,end=self.setup_bars([-1,-1,1,-1,1]); self.assertFalse(s.entries)
        t(end+.2,100); self.assertEqual(s.entries.popleft().direction,'SELL')
        self.assertEqual(len(s.catalog),3)

    def test_buy_close_requires_repeated_green(self):
        for pattern in ([1,1,-1,1,-1], [1,1,1,-1,1,-1]):
            with self.subTest(pattern=pattern):
                s,t,end=self.setup_bars(pattern)
                self.assertFalse(s.entries)
                self.assertEqual(s.forming()[::2],(4,'AUDCAD_otc'))
                t(end+.2,100); t(end+.2,999); t(end+.5,101)
                self.assertEqual(len(s.entries),1)
                e=s.entries.popleft()
                self.assertEqual((e.asset,e.direction,e.boundary,e.early),('AUDCAD_otc','BUY',end,False))

    def test_open_red_reverses_before_close(self):
        s,t,end=self.setup_bars([1,1,-1,1,-1]); t(end-.2,101); t(end+.2,100)
        self.assertFalse(s.entries)

    def test_break_doji_gaps(self):
        for colors,gap in [([1,1,-1,-1,1],False),([-1,-1,1,0,1],False),([-1,-1,1,-1,1],True),([1,-1,1,-1],False)]:
            s,t,end=self.setup_bars(colors,gap=gap); t(end+.2,100)
            self.assertFalse(s.entries)

    def test_missing_minute_or_late_boundary(self):
        for seconds in (4,60.2):
            s,t,end=self.setup_bars([1,1,-1,1,-1]); t(end+seconds,100)
            self.assertFalse(s.entries)

    def test_stale_future_duplicates_and_ms(self):
        now=1_700_000_000
        s=Scanner(lambda:now)
        s.feed(['updateStream',[['EURUSD_otc',now-8,9],['EURUSD_otc',now+9,9],['EURUSD_otc',now*1000,1],['EURUSD_otc',now*1000,8]]])
        self.assertEqual(s.series['EURUSD_otc'].bar.close,1)
        self.assertEqual(s.active_count(),1)

    def test_partial_pattern(self):
        s,t,end=self.setup_bars([-1,-1,1,-1]); t(end+.2,100); t(end+45,100)
        self.assertEqual(s.forming()[2],'AUDCAD_otc'); t(end+50,101)
        self.assertEqual(s.forming()[::2],(4,'AUDCAD_otc')); self.assertFalse(s.entries)

    def test_binary_and_unnamed(self):
        d=Decoder(); raw=b'[["EURUSD_otc",1700000000,1.1]]'
        self.assertEqual(d.feed(raw)[0][0],'updateStream')
        self.assertEqual(d.feed('451-["updateStream",{"_placeholder":true,"num":0}]'),[])
        self.assertEqual(d.feed(raw)[0][1][0][0],'EURUSD_otc')

    def test_settings_scope(self):
        self.assertFalse(validate_document(defaults())['chess']['early'])
        self.assertTrue(patched(defaults(),{'chess.early':True})['chess']['early'])
        self.assertEqual(patched(defaults(),{'account_mode':'REAL'})['account_mode'],'REAL')


class ExecutionTests(unittest.IsolatedAsyncioTestCase):
    async def test_matching_result_and_balance(self):
        order=dict(requestId=99,asset='USDINR_otc',amount=2,mode='PRACTICE',sent_at=100,sent_mono=200,balance_before=100)
        r=OrderResult(order)
        r.feed('successopenorder',{'requestId':99,'id':'mine'},100)
        r.feed('closeorder',{'id':'old','profit':-2},161)
        r.feed('closeorder',{'id':'mine','profit':-2},159)
        self.assertIsNone(r.candidate)
        r.feed('closeorder',{'requestId':98,'id':'mine','profit':-2},161)
        self.assertIsNone(r.candidate)
        r.feed('closeorder',{'id':'mine','profit':-2},161)
        r.confirm_balance(98,259); self.assertFalse(r.future.done())
        r.confirm_balance(100,261); self.assertFalse(r.future.done())
        r.confirm_balance(98,261); self.assertEqual(await r.future,('loss',-2))

    async def test_same_socket_with_demo_gate(self):
        p=ChessPocket(defaults()['demo_account'],'PRACTICE'); p.connected=True
        p.ws=SimpleNamespace(send=AsyncMock())
        order=dict(requestId=1,asset='USDINR_otc',amount=1,mode='PRACTICE',action='put')
        self.assertFalse(await p.send_order(order,lambda:True))
        p.process(['successauth',{'isDemo':1,'balance':100}]); await p.subscribe('USDINR_otc')
        self.assertTrue(await p.send_order(order,lambda:True))
        frames=[json.loads(call.args[0][2:]) for call in p.ws.send.await_args_list]
        self.assertEqual([x[0] for x in frames],['changeSymbol','openOrder'])
        self.assertEqual(frames[-1][1]['time'],60)
        p.process(['successauth',{'isDemo':0,'balance':999}])
        self.assertFalse(await p.send_order(order,lambda:True))

    def mock_worker(self, outcomes, enhance=True):
        d=defaults(); d['recovery'].update(mode='ENABLED' if enhance else 'TRADITIONAL',increment=1)
        d['trading'].update(martingale_steps=4,trade_amount=1)
        r=SimpleNamespace(doc=d,leader=True,reserve=AsyncMock(),resolve=AsyncMock())
        w=TradingWorker(r); now=time.time()
        s=Scanner(); s.feed(['updateStream',[['USDINR_otc',now,100]]]); queued=iter(outcomes)
        async def settle():
            item=next(queued)
            if item is None: raise TimeoutError
            return item
        book=PayoutBook(d['account_mode'])
        book.feed(['updateAssets',[{'symbol':asset,'payout':91,'tradable':True} for asset in ('USDINR_otc','AUDCAD_otc')]])
        w.po=SimpleNamespace(scanner=s,balance=1000,send_order=AsyncMock(return_value=True),settlement=settle,payout_book=book)
        return w,Entry('USDINR_otc','SELL',int(now),now+3,False)

    async def test_audcad_pattern_dispatches_one_buy(self):
        scanner,tick,end = MarketTests().setup_bars([1,1,-1,1,-1])
        self.assertFalse(scanner.entries)
        tick(end+.2,100)
        w,_ = self.mock_worker([('win',.9)])
        w.po.scanner = scanner
        with patch('worker.time.time',return_value=end+.2):
            await w.chain(scanner.entries.popleft(),w.repo.doc)
        w.po.send_order.assert_awaited_once()
        order = w.po.send_order.await_args.args[0]
        self.assertEqual((order['asset'],order['action'],order['amount']),('AUDCAD_otc','call',1))
        self.assertFalse(scanner.entries)

    async def test_regular_ladder_then_one_enhancement(self):
        w,e=self.mock_worker([('loss',-1),('loss',-2),('loss',-4),('loss',-8),('loss',-2)])
        await w.chain(e,w.repo.doc)
        orders=[x.args[0] for x in w.po.send_order.await_args_list]
        self.assertEqual([x['amount'] for x in orders],[1,2,4,8,2])
        self.assertEqual({x['action'] for x in orders},{'put'})
        self.assertEqual(w.repo.resolve.await_args_list[3].kwargs['seq_lost_amount'],15)

    async def test_stops_on_win_or_draw(self):
        for outcome in [('win',.9),('draw',0)]:
            w,e=self.mock_worker([outcome]); await w.chain(e,w.repo.doc)
            w.po.send_order.assert_awaited_once()

    async def test_timeout_not_replayed(self):
        w,e=self.mock_worker([None]);
        with self.assertRaises(TimeoutError): await w.chain(e,w.repo.doc)
        w.po.send_order.assert_awaited_once(); w.repo.resolve.assert_not_awaited()

    async def test_stop_blocks_order(self):
        w,e=self.mock_worker([('win',.9)]); w.request_stop(); await w.chain(e,w.repo.doc)
        w.po.send_order.assert_not_awaited()
