import asyncio
import json
import time
import unittest
from unittest.mock import AsyncMock, patch

from chess_market import Scanner
from chess_transport import ChessPocket
from cloud_settings import defaults


def pocket(mode):
    demo = mode == 'PRACTICE'
    creds = defaults()['demo_account' if demo else 'real_account']
    creds.update(ssid='42'+json.dumps(['auth', {'isDemo': int(demo), 'uid': 123, 'session': 'mock-session'}]))
    return ChessPocket(creds, mode)


class LocalTickTests(unittest.TestCase):
    def test_millisecond_hour_skew_builds_live_m1(self):
        wall = [1_740_000_000.0]
        scanner = Scanner(lambda: wall[0], lambda: wall[0] - 1_740_000_000)
        asset = 'EURUSD_otc'
        for i in range(321):
            wall[0] = 1_740_000_000 + i/5
            scanner.feed(['updateStream', [[asset, round((wall[0]+3600)*1000), 1+i/10000]]])
        self.assertEqual(scanner.last_clock.offset, 3600)
        self.assertEqual(scanner.rejected['CALIBRATING_LIVE_CLOCK'], 7)
        self.assertEqual(len(scanner.series[asset].closed), 1)
        self.assertEqual(scanner.active_count(), 1)
        self.assertFalse(scanner.entries)  # read-only
        scanner.clear()
        self.assertEqual(scanner.accepted, 0)
        self.assertIsNone(scanner.last_clock)

    def test_rejects_unsafely_shifted_stream(self):
        now = 1_740_000_000
        scanner = Scanner(lambda: now, lambda: 1)
        scanner.feed(['updateStream', [['EURUSD_otc', now-15, 1]]])
        self.assertEqual(scanner.accepted, 0)
        self.assertEqual(scanner.rejected['CLOCK_OFFSET_NOT_SAFE_WHOLE_HOUR'], 1)

    def test_real_ignores_untagged_balance(self):
        p = pocket('REAL'); p._build_auth_frame()
        p.process(['success_auth', {}])
        p.process(['successUpdateBalance', {'data': [{'balance': 88}]}])
        self.assertIsNone(p.verified_balance)
        p.process(['successUpdateBalance', {'data': {'isDemo': 0, 'account': {'balance': 12}}}])
        self.assertEqual(p.verified_balance, 12)

    def test_demo_rejects_mixed_and_conflicting_records(self):
        p = pocket('PRACTICE'); p._build_auth_frame()
        p.process(['successUpdateBalance', {'balance': 16}])
        self.assertIsNone(p.verified_balance)
        p.process(['successauth', {}])
        for rows in ([{'balance': 99, 'isDemo': 0}, {'balance': 55}], [{'balance': 1}, {'balance': 2}]):
            p.process(['successUpdateBalance', {'data': rows}]); self.assertIsNone(p.verified_balance)
        p.process(['successUpdateBalance', {'data': {'balance': 0}}]); self.assertEqual(p.verified_balance, 0)
        p.process(['successauth', {'isDemo': 0}]); self.assertTrue(p.wrong_mode); self.assertIsNone(p.verified_balance)


class MockSocket:
    def __init__(self):
        self.incoming = asyncio.Queue()
        self.send = AsyncMock()
        self.close = AsyncMock()
    def __aiter__(self):
        return self
    async def __anext__(self):
        return await self.recv()
    async def recv(self):
        return await self.incoming.get()


class IntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_same_socket_reads_binary_tick_and_nested_demo_balance(self):
        ws = MockSocket(); p = pocket('PRACTICE')
        for frame in ('0{}', '40', '42["success_auth",{}]'):
            ws.incoming.put_nowait(frame)
        try:
            with patch('cloud_transport.websockets.connect', new=AsyncMock(return_value=ws)):
                await p.connect()
            await p.subscribe('EURUSD_otc')
            for frame in ('42["successUpdateBalance",{"data":{"balance":100}}]',
                          '451-["updateStream",{"_placeholder":true,"num":0}]',
                          json.dumps([['EURUSD_otc', time.time(), 1.05]]).encode()):
                ws.incoming.put_nowait(frame)
            await asyncio.sleep(0)
            self.assertEqual(p.verified_balance, 100)
            self.assertEqual(p.scanner.accepted, 1)
            self.assertEqual(p.scanner.active_count(), 1)
            event, payload = json.loads(ws.send.await_args.args[0][2:])
            self.assertEqual((event, payload), ('changeSymbol', {'asset':'EURUSD_otc', 'period': 5}))
            self.assertEqual(ws.send.await_count, 3)  # namespace, auth, subscription; no order
        finally:
            await p.close()
