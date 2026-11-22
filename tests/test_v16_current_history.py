import json
import unittest
from unittest.mock import AsyncMock, patch
from chess_transport import ChessPocket
from cloud_settings import defaults


class CurrentHistoryTests(unittest.IsolatedAsyncioTestCase):
    async def test_indexed_m1_response_seeds_four_closed_candles(self):
        p=ChessPocket(defaults()['demo_account'],'PRACTICE')
        s=p.scanner; s.strict_focus=s.history_only=s.enabled=True
        s.eligible=lambda _:True
        now=[6260.0]; s.clock=s.mono=lambda:now[0]
        p.connected=True; p.ws=AsyncMock()
        a='GBPUSD_otc'
        with patch('chess_transport.time.time',return_value=now[0]):
            await p.subscribe(a)
        name,req=json.loads(p.ws.send.await_args.args[0][2:])
        self.assertEqual((name,req['asset'],req['period'],req['time']),('loadHistoryPeriod',a,60,6260))
        rows=[dict(time=t,open=100,close=c,high=102,low=98) for t,c in [(6000,101),(6060,101),(6120,99),(6180,101),(6240,101)]]
        reply=dict(asset=a,index=req['index'],period=60,data=rows)
        for wrong in [dict(reply,index=req['index']+1),dict(reply,asset='EURUSD_otc'),dict(reply,period=5)]:
            p.process(['loadHistoryPeriod',wrong]); self.assertIsNone(s.history_rows)
        for event in p.decoder.feed(json.dumps(reply).encode()): p.process(event)
        p.process(['updateStream',[[a,6260,101]]])
        self.assertEqual(s.series[a].ready_depth(),4)
        self.assertEqual(s.locked,(a,6300))
        self.assertFalse(s.entries)
        for t in (6275,6290,6299,6300.2):
            now[0]=t; p.process(['updateStream',[[a,t,99]]])
        self.assertEqual([e.direction for e in s.entries],['BUY'])

    async def test_old_rows_are_not_relabelled(self):
        p=ChessPocket(defaults()['demo_account'],'PRACTICE')
        s=p.scanner; s.strict_focus=s.history_only=s.enabled=True
        s.clock=s.mono=lambda:6260
        s.request_history('GBPUSD_otc')
        s.accept_history(dict(asset='GBPUSD_otc',period=60,candles=[[t,100,101,102,98] for t in range(5700,5941,60)]))
        s.feed(['updateStream',[['GBPUSD_otc',6260,100]]])
        self.assertIn('0/4',s.history_state)
        self.assertFalse(s.entries)
