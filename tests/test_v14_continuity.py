import unittest
from chess_market import Scanner, Series, Bar


class HistoryContinuityTests(unittest.TestCase):
    def test_three_historical_plus_one_completed_live_lock(self):
        asset='AUDNZD_otc'; clock=[6260.0]
        s=Scanner(lambda:clock[0],lambda:clock[0]); s.enabled=s.strict_focus=True
        s.eligible=lambda a: True
        series=s.series.setdefault(asset,Series())
        series.closed.append(Bar(6180,6180,6239,100,101))
        s.request_history(asset)
        s.accept_history({'asset':asset,'period':60,'candles':[[t,100,c,102,98] for t,c in [(6000,101),(6060,101),(6120,99),(6240,100)]]})
        def tick(t,p):
            clock[0]=t; s.feed(['updateStream',[[asset,t,p]]])
        tick(6260,101)
        self.assertEqual(series.ready_depth(),4)
        self.assertEqual(s.locked,(asset,6300))
        self.assertFalse(s.entries)
        for t in (6275,6290,6299): tick(t,99)
        tick(6300.2,100)
        self.assertEqual([(e.asset,e.direction) for e in s.entries],[(asset,'BUY')])

    def test_no_fill_of_missing_minute(self):
        asset='AUDNZD_otc'; s=Scanner(lambda:6260,lambda:6260); s.strict_focus=True
        s.request_history(asset)
        s.accept_history({'asset':asset,'period':60,'candles':[[t,100,99,102,98] for t in [5940,6000,6060,6180]]})
        s.feed(['updateStream',[[asset,6260,100]]])
        self.assertEqual(s.series[asset].ready_depth(),0)
        self.assertIn('3/4',s.history_state)
