import unittest
from unittest.mock import patch
from chess_market import Scanner
import test_chess


class AEDCoverageTests(unittest.IsolatedAsyncioTestCase):
    async def test_demo_focus_and_sell_with_sparse_ticks(self):
        for last, color, result in [(55.5, 1, 'MATCH'), (55.5, -1, 'CLOSE_COLOR_MISMATCH'), (53, 1, 'MISSING_END_TICK')]:
            with self.subTest(last=last,color=color):
                wall=[6000.0]; a='AEDCNY_otc'
                s=Scanner(lambda:wall[0],lambda:wall[0]); s.strict_focus=True; s.enabled=True
                s.eligible=lambda _:True
                def tick(t,p):
                    wall[0]=t; s.feed(['updateStream',[[a,t,p]]])
                for i,c in enumerate((-1,-1,1,-1,color)):
                    start=6000+i*60
                    for sec in (4.2,17,30,43, last if i==4 else 55.5):
                        tick(start+sec,100 if sec==4.2 else 100+c)
                    if i==4:
                        self.assertEqual(s.locked,(a,6300))
                        self.assertFalse(s.entries)
                tick(6300.2,100)
                self.assertEqual(s.closed_checks[result],1)
                self.assertEqual(bool(s.entries),result=='MATCH')
                if s.entries:
                    w,_=test_chess.ExecutionTests().mock_worker([('win',.9)])
                    w.po.scanner=s
                    w.po.payout_book.feed(['updateAssets',[{'asset':a,'payout':92}]])
                    with patch('worker.time.time',return_value=6300.2):
                        await w.chain(s.entries.popleft(),w.repo.doc)
                    w.po.send_order.assert_awaited_once()
                    self.assertEqual(w.po.send_order.await_args.args[0]['action'],'put')

    async def test_real_tolerance_unchanged(self):
        from chess_market import Bar
        self.assertFalse(Bar(6000,6004,6056,100,101,13).good)
        self.assertTrue(Bar(6000,6004,6056,100,101,13,demo_tolerance=True).good)
