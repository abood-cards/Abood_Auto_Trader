import unittest
from unittest.mock import patch
from chess_market import Entry, Scanner
import test_chess


class VerifiedCloseTests(unittest.IsolatedAsyncioTestCase):
    def setup_feed(self):
        now=[6260.0]; a='EURUSD_otc'
        s=Scanner(lambda:now[0], lambda:now[0]); s.enabled=s.strict_focus=s.history_only=s.confirm_history=True
        s.eligible=lambda _:True
        initial=[[t,100,c,102,98] for t,c in [(6000,99),(6060,99),(6120,101),(6180,99),(6240,100)]]
        def history(rows, confirm=False):
            s.accept_history(dict(asset=a,period=60,candles=rows),confirmation=confirm)
        def tick(t,p):
            now[0]=t; s.feed(['updateStream',[[a,t,p]]])
        s.request_history(a); history(initial)
        tick(6260,100)
        self.assertEqual(s.locked,(a,6300))
        for t in (6275,6290,6299,6300.2): tick(t,101)
        self.assertFalse(s.entries)
        return s,history,initial

    async def test_live_green_but_official_close_red_no_order(self):
        s,history,rows=self.setup_feed()
        history(rows[:4]+[[6240,100,99,102,98],[6300,99,99,99,99]],True)
        self.assertIn('CLOSE_COLOR_MISMATCH',s.last_closed)
        self.assertFalse(s.entries)

    async def test_changed_history_and_missing_next_minute_are_rejected(self):
        for changed in (True,False):
            s,history,rows=self.setup_feed()
            rows[4][2]=101
            if changed:
                rows[0][2]=101; rows.append([6300,101,101,101,101])
            history(rows,True)
            self.assertIn('OHLC_CHANGED' if changed else 'UNVERIFIED_CLOSE',s.last_closed)
            self.assertFalse(s.entries)

    async def test_verified_sell_reaches_worker_and_proof_is_one_time(self):
        s,history,rows=self.setup_feed()
        rows[4][2]=101
        history(rows+[[6300,101,101,101,101]],True)
        e=s.entries.popleft()
        w,_=test_chess.ExecutionTests().mock_worker([('win',.9)])
        w.po.scanner=s; w.po.payout_book.feed(['updateAssets',[{'asset':e.asset,'payout':92}]])
        with patch('worker.time.time',return_value=6300.2):
            await w.chain(Entry(e.asset,e.direction,e.boundary,e.expires_at,False),w.repo.doc)
            w.po.send_order.assert_not_awaited()
            await w.chain(e,w.repo.doc)
            await w.chain(e,w.repo.doc)
        w.po.send_order.assert_awaited_once()
        self.assertEqual(w.po.send_order.await_args.args[0]['action'],'put')
