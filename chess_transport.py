"""Single broker socket: discovery, tick ingestion, and correlated orders."""
import asyncio
import json
import time
import zlib
from cloud_transport import CloudPocket
from wire_decoder import EventDecoder, payload_json
from payout_guard import flag
from chess_market import Scanner, extract_ticks, num


class Decoder(EventDecoder):
    def feed(self, raw):
        pending = self.pending
        events = super().feed(raw)
        if events or pending or (isinstance(raw,str) and not raw.lstrip().startswith('[[')):
            return events
        try:
            data = payload_json(raw)
            if isinstance(data, list) and any(extract_ticks(data)):
                return [['updateStream', data]]
        except (ValueError, TypeError, UnicodeError, RecursionError, zlib.error):
            pass
        return []


def records(data):
    stack = [data]
    for _ in range(4096):
        if not stack:
            break
        node = stack.pop()
        if isinstance(node,dict):
            yield node
            stack.extend(v for k,v in node.items() if k in ('data','deals','orders','result','deal','order') and isinstance(v,(dict,list)))
        elif isinstance(node,list):
            stack.extend(node[:4096])


class OrderResult:
    CLOSE = {'updatecloseddeals','closeddeals','successcloseorder','closeorder'}
    OPEN = {'successopenorder','openorder','updateopeneddeals'}

    def __init__(self, order):
        self.order = order
        self.deal = None
        self.candidate = None
        self.entry_price = None
        self.future = asyncio.get_running_loop().create_future()

    def feed(self, name, payload, now):
        if self.future.done() or name not in self.OPEN | self.CLOSE:
            return
        expected = self.order
        for row in records(payload):
            request = row.get('requestId', row.get('request_id'))
            deal = row.get('id', row.get('dealId', row.get('orderId')))
            request_matches = request is not None and str(request) == str(expected['requestId'])
            if request is not None and not request_matches:
                continue
            if row.get('asset',expected['asset']) != expected['asset'] or ('amount' in row and num(row['amount']) != expected['amount']):
                continue
            if 'isDemo' in row and flag(row['isDemo']) != (expected['mode']=='PRACTICE'):
                continue
            if row.get('error') or row.get('success') is False:
                continue
            if name in self.OPEN:
                if request_matches:
                    if deal is not None:
                        self.deal = str(deal)
                    price = num(row.get('openPrice',row.get('open_price',row.get('entryPrice'))))
                    if price is not None and price > 0:
                        self.entry_price = price
                continue
            if not (request_matches or (deal is not None and self.deal == str(deal))):
                continue
            if now < expected['sent_at']+60:
                continue
            profit = num(row.get('profit'))
            label = str(row.get('result','')).lower()
            amount = expected['amount']
            if profit is None or not -amount <= profit <= amount:
                continue
            inferred = 'win' if profit>0 else 'loss' if profit<0 else 'draw' if label=='draw' else None
            if inferred is None or (label in ('win','loss','draw') and label != inferred):
                continue
            self.candidate = (inferred,profit)
            # Fast path: explicit matching result and signed net P/L. Without
            # an explicit result, legacy profit ambiguity requires balance.
            if label == inferred:
                self.future.set_result(self.candidate)
            return

    def confirm_balance(self, balance, balance_at):
        if self.future.done() or not self.candidate or balance is None or balance_at < self.order['sent_mono']+60:
            return
        if abs(balance-self.order['balance_before']-self.candidate[1]) <= .00001:
            self.future.set_result(self.candidate)


class ChessPocket(CloudPocket):
    decoder_class = Decoder
    def __init__(self, credentials, mode):
        super().__init__(credentials,mode)
        self.scanner = Scanner()
        self.result = None
        self.trackers = []
        self.account_verified = False
        self.wrong_mode = False
        self.auth_seen = False
        self.subscription_requests = 0
        self.verified_balance = None
        self.verified_balance_at = 0.0

    async def connect(self):
        self.scanner.clear()
        self.account_verified = self.wrong_mode = self.auth_seen = False
        self.result = None
        self.trackers.clear()
        self.subscription_requests = 0
        self.verified_balance = None
        self.verified_balance_at = 0.0
        await super().connect()

    def process(self, event):
        if not isinstance(event,list) or len(event)<2 or not isinstance(event[0],str):
            return
        name,payload = event[:2]
        expected = self.mode == 'PRACTICE'
        kind = name.lower()
        if kind == 'successauth':
            self.auth_seen = True
        account_event = kind in ('successauth','profile','updateprofile','updatebalance')
        if account_event:
            for row in records(payload):
                if 'isDemo' not in row:
                    continue
                seen = flag(row['isDemo'])
                if seen is expected:
                    self.account_verified = not self.wrong_mode
                    # A nested balance explicitly tagged with the selected mode.
                    value = num(row.get('balance'))
                    if value is not None and value >= 0 and self.account_verified:
                        self.verified_balance = self.balance = value
                        self.verified_balance_at = time.monotonic()
                elif seen is not None and kind == 'successauth' and row is payload:
                    self.wrong_mode = True
                    self.account_verified = False
        if isinstance(payload,dict) and 'isDemo' in payload and flag(payload['isDemo']) is not expected:
            return
        if name in ('successauth','profile','updateProfile','updateBalance'):
            super().process(event)
            if isinstance(payload,dict) and self.account_verified and not self.wrong_mode:
                value = num(payload.get('balance'))
                if value is not None and value >= 0:
                    self.verified_balance = value
                    self.verified_balance_at = time.monotonic()
            self.balance = self.verified_balance if self.account_verified else self.balance
        self.scanner.feed(event)
        if not self.wrong_mode:
            for tracked in self.trackers:
                tracked.feed(kind,payload,time.time())
                if not tracked.order.get('overlap'):
                    tracked.confirm_balance(self.verified_balance,self.verified_balance_at)

    async def subscribe(self, asset):
        if self.connected and not self.wrong_mode:
            # Match the local collector subscription; M1 bars are built offline from ticks.
            await self.select_asset(asset,5)
            self.subscription_requests += 1

    async def send_order(self, order, valid):
        async with self._lock:
            if not self.connected or self.wrong_mode or not self.account_verified or order['mode'] != self.mode or not valid():
                return False
            if self.verified_balance is None or self.verified_balance < order['amount']:
                return False
            order.update(balance_before=self.verified_balance, sent_mono=time.monotonic(), sent_at=time.time())
            self.result = OrderResult(order)
            self.trackers = [t for t in self.trackers if not t.future.done()][-1:]
            if order.get('overlap'):
                for t in self.trackers:
                    t.order['overlap'] = True
            self.trackers.append(self.result)
            fields = dict(asset=order['asset'], amount=order['amount'], action=order['action'],
                          isDemo=int(self.mode=='PRACTICE'), optionType=100, time=60, requestId=order['requestId'])
            async with asyncio.timeout(3):
                await self.ws.send('42'+json.dumps(['openOrder',fields],separators=(',',':')))
            return True

    async def settlement(self, tracked=None):
        future = (tracked or self.result).future
        deadline = time.monotonic()+75
        while not future.done():
            if not self.connected or self.wrong_mode or time.monotonic() >= deadline:
                raise TimeoutError('unconfirmed_order')
            await asyncio.wait({future},timeout=.25)
        return future.result()
