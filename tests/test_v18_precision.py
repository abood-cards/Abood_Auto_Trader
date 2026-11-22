import unittest
from unittest.mock import patch
from chess_market import Scanner
import test_chess

ASSET = 'EURUSD_otc'


class PrecisionTests(unittest.IsolatedAsyncioTestCase):
    def setup_feed(self, buy=False, fifth=None):
        now = [6260.0]
        s = Scanner(lambda: now[0], lambda: now[0]); s.enabled = s.strict_focus = s.history_only = True
        s.precision_lead = .3
        s.eligible = lambda _: True
        offsets = (1, 1, -1, 1) if buy else (-1, -1, 1, -1)
        rows = [[6000+60*i,100,100+v,102,98] for i,v in enumerate((*offsets,0))]
        def tick(t,p):
            now[0]=t; s.feed(['updateStream',[[ASSET,t,p]]])
        s.request_history(ASSET)
        s.accept_history(dict(asset=ASSET,period=60,candles=rows))
        tick(6260,100)
        self.assertEqual(s.locked, (ASSET,6300))
        for t in (6279,6298,6299.60):
            tick(t, fifth if fifth is not None else (99 if buy else 101))
        return s, now, tick

    async def test_pre_close_both_directions_and_only_once(self):
        for buy in (False, True):
            s, now, tick = self.setup_feed(buy)
            s.precision_check()
            self.assertFalse(s.entries)
            now[0] = 6299.71
            s.precision_check(); s.precision_check()
            self.assertEqual(len(s.entries), 1)
            entry=s.entries[0]
            self.assertEqual(entry.direction, 'BUY' if buy else 'SELL')
            self.assertTrue(entry.early)
            self.assertEqual(entry.expires_at, 6300)

    async def test_unchanged_candle_expired_and_stale_tick_skip(self):
        for price, check_time, expected in ((99,6299.71,'حمراء'), (100,6299.71,'PRE_CLOSE_UNVERIFIED'), (101,6300,'ENTRY_WINDOW_MISSED')):
            s, now, tick = self.setup_feed(fifth=price)
            now[0] = check_time; s.precision_check()
            self.assertIsNone(s.locked)
            self.assertIn(expected,s.last_closed)
            self.assertFalse(s.entries)
        s, now, tick = self.setup_feed()
        s.clocks[ASSET].last_server = 6298.0
        now[0]=6299.71; s.precision_check()
        self.assertFalse(s.entries)

    async def test_worker_sends_one_demo_order(self):
        s,now,tick = self.setup_feed()
        now[0]=6299.71; s.precision_check()
        e=s.entries.popleft()
        w,_=test_chess.ExecutionTests().mock_worker([('win',.9)])
        w.po.mode='PRACTICE'; w.po.scanner=s
        w.po.payout_book.feed(['updateAssets',[{'asset':ASSET,'payout':91}]])
        with patch('worker.time.time',return_value=now[0]):
            await w.chain(e,w.repo.doc)
            await w.chain(e,w.repo.doc)
        w.po.send_order.assert_awaited_once()
        self.assertEqual(w.po.send_order.await_args.args[0]['action'],'put')

    async def test_worker_rechecks_flip_before_order(self):
        s,now,tick=self.setup_feed()
        now[0]=6299.71; s.precision_check()
        e=s.entries.popleft()
        tick(6299.72,99)
        w,_=test_chess.ExecutionTests().mock_worker([('win',.9)])
        w.po.mode='PRACTICE'; w.po.scanner=s
        w.po.payout_book.feed(['updateAssets',[{'asset':ASSET,'payout':91}]])
        with patch('worker.time.time',return_value=now[0]):
            await w.chain(e,w.repo.doc)
        w.po.send_order.assert_not_awaited()

    async def test_expiry_during_reservation_clears_unsent_order(self):
        s,now,tick=self.setup_feed()
        now[0]=6299.71; s.precision_check()
        e=s.entries.popleft()
        w,_=test_chess.ExecutionTests().mock_worker([('win',.9)])
        w.po.mode='PRACTICE'; w.po.scanner=s
        w.po.payout_book.feed(['updateAssets',[{'asset':ASSET,'payout':91}]])
        async def reserve(order):
            now[0]=6300.01
        async def guarded_send(order, valid):
            self.assertFalse(valid())
            return False
        w.repo.reserve.side_effect=reserve
        w.po.send_order.side_effect=guarded_send
        with patch('worker.time.time',side_effect=lambda:now[0]):
            await w.chain(e,w.repo.doc)
        self.assertFalse(w.in_flight)
        self.assertFalse(w.repo.resolve.await_args.kwargs['dispatched'])
