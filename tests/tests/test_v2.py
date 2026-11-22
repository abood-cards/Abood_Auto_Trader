import asyncio
import json
import time
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from chess_transport import ChessPocket, OrderResult, Decoder
from cloud_settings import defaults
import test_chess


class V2(unittest.IsolatedAsyncioTestCase):
    async def test_catalog_without_flag_can_subscribe_but_not_trade(self):
        p=ChessPocket(defaults()['demo_account'],'PRACTICE')
        p.connected=True
        p.ws=SimpleNamespace(send=AsyncMock())
        p.process(['successauth',{}]); p.process(['updateAssets',[{'asset':'USDINR_otc'}]])
        await p.subscribe('USDINR_otc')
        self.assertIsInstance(p.decoder,Decoder)
        self.assertFalse(p.account_verified)
        self.assertFalse(await p.send_order({'mode':'PRACTICE'},lambda:True))
        self.assertEqual(json.loads(p.ws.send.call_args.args[0][2:])[0],'changeSymbol')

    async def test_real_nested_balance_and_order_mode(self):
        p=ChessPocket(defaults()['real_account'],'REAL');p.connected=True
        p.ws=SimpleNamespace(send=AsyncMock())
        p.process(['successauth',{'data':{'isDemo':0,'balance':75}}])
        p.process(['updateBalance',{'isDemo':1,'balance':900}])
        self.assertEqual(p.verified_balance,75)
        o=dict(requestId=99,asset='USDINR_otc',amount=10,mode='REAL',action='call')
        self.assertFalse(await p.send_order(dict(o,mode='PRACTICE'),lambda:True))
        self.assertTrue(await p.send_order(o,lambda:True))
        self.assertEqual(json.loads(p.ws.send.call_args.args[0][2:])[1]['isDemo'],0)

    async def test_matched_close_fast_path_and_ack_price(self):
        o=dict(requestId=99,asset='USDINR_otc',amount=10,mode='PRACTICE',sent_at=100)
        r=OrderResult(o)
        r.feed('successopenorder',{'requestId':10,'openPrice':2},101)
        self.assertIsNone(r.entry_price)
        r.feed('successopenorder',{'requestId':99,'id':'ok','openPrice':1.1},101)
        self.assertEqual(r.entry_price,1.1)
        r.feed('closeorder',{'id':'ok','result':'loss','profit':-10},159)
        self.assertFalse(r.future.done())
        r.feed('closeorder',{'id':'ok','result':'loss','profit':-10},160)
        self.assertEqual(await r.future,('loss',-10))

    async def test_early_only_demo_and_drains_followup_after_first_reversal(self):
        for mode in ('PRACTICE','REAL'):
            w,e=test_chess.ExecutionTests().mock_worker([],enhance=False)
            w.repo.doc['account_mode']=mode
            w.repo.doc['chess']['early_double']=True
            w.repo.reserve_followup=AsyncMock(); w.repo.discard_followup=AsyncMock()
            first=asyncio.Event(); calls=[]
            async def send(order, guard):
                self.assertTrue(guard()); calls.append(order)
                w.po.result=SimpleNamespace(deal='ack',entry_price=99,future=asyncio.get_running_loop().create_future())
                if len(calls)==2: first.set()
                return True
            async def settle(_tracked):
                if mode=='PRACTICE': await first.wait()
                return ('win',.9)
            async def clock_wait(tasks,timeout):
                if mode=='REAL' or len(calls)>1:
                    await asyncio.sleep(0)
                    return tasks,set()
                return set(),tasks
            w.po.send_order=send; w.po.settlement=settle
            with patch('worker.asyncio.wait',new=clock_wait):
                await w.chain(e,w.repo.doc)
            self.assertEqual([o['amount'] for o in calls],[1,2] if mode=='PRACTICE' else [1])
            self.assertEqual(w.repo.resolve.await_count,len(calls))

