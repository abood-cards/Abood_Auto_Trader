"""Single broker socket: discovery, tick ingestion, and correlated orders."""
import asyncio
import json
import time
import zlib
from cloud_transport import CloudPocket
from wire_decoder import EventDecoder, payload_json
from payout_guard import flag
from chess_market import Scanner, extract_ticks, num

AUTH_EVENTS = {'successauth','success_auth'}
BALANCE_EVENTS = AUTH_EVENTS | {'profile','updateprofile','updatebalance','successupdatebalance','balance','balances'}
SCOPED = {'demobalance': True,'practicebalance': True,'realbalance': False}
UNKNOWN = object()


def balance_rows(payload, inherited=None, depth=0):
    if depth > 8:
        return
    if isinstance(payload,dict):
        mode = inherited
        if 'isDemo' in payload:
            mode = flag(payload['isDemo'])
            if mode is None:
                mode = UNKNOWN
        for key,value in payload.items():
            if isinstance(value,(dict,list)):
                yield from balance_rows(value,mode,depth+1)
                continue
            key = key.replace('_','').lower()
            if key == 'balance':
                yield mode, num(value)
            elif key in SCOPED and (mode is None or mode is SCOPED[key]):
                yield SCOPED[key],num(value)
    elif isinstance(payload,list):
        for value in payload:
            yield from balance_rows(value,inherited,depth+1)


class Decoder(EventDecoder):
    def feed(self, raw):
        pending = self.pending
        events = super().feed(raw)
        if events or pending or (isinstance(raw,str) and not raw.lstrip().startswith(('[','{'))):
            return events
        try:
            data = payload_json(raw)
            if isinstance(data,dict):
                if 'candles' in data and ('asset' in data or 'symbol' in data):
                    return [['m1History',data]]
                name = data.get('event')
                if isinstance(name,str) and name.lower() in (BALANCE_EVENTS | {'updatestream','updatehistorynew','updatehistory','stream_update','m1history'}):
                    return [[name,data if 'asset' in data or 'symbol' in data else data.get('data',data.get('payload',{}))]]
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
            stack.extend(v for k,v in node.items() if k in ('data','deals','orders','result','deal','order','user','account','accounts','profile','balances') and isinstance(v,(dict,list)))
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
        self.sent_mode = None
        self.balance_source = "—"
        self.balance_events = 0
        self.subscription_requests = 0
        self.verified_balance = None
        self.verified_balance_at = 0.0

    async def connect(self):
        self.scanner.clear()
        self.account_verified = self.wrong_mode = self.auth_seen = False
        self.result = None
        self.trackers.clear()
        self.sent_mode = None
        self.balance_source = "—"
        self.balance_events = 0
        self.subscription_requests = 0
        self.verified_balance = None
        self.verified_balance_at = 0.0
        await super().connect()

    def _build_auth_frame(self):
        frame = super()._build_auth_frame()
        self.sent_mode = flag(json.loads(frame[2:])[1].get('isDemo'))
        return frame

    def process(self, event):
        if not isinstance(event,list) or len(event)<2 or not isinstance(event[0],str):
            return
        name,payload = event[:2]
        expected = self.mode == 'PRACTICE'
        kind = name.lower()
        if kind in AUTH_EVENTS:
            if isinstance(payload,dict) and (payload.get('success') is False or payload.get('error')):
                self.wrong_mode = True
            if isinstance(payload,dict) and 'isDemo' in payload and flag(payload['isDemo']) is not expected:
                self.wrong_mode = True
            self.auth_seen = not self.wrong_mode
        if self.wrong_mode:
            self.account_verified = False
            self.balance = self.verified_balance = None
            return
        if kind in BALANCE_EVENTS:
            self.balance_events += 1
            rows = [(mode,value) for mode,value in balance_rows(payload) if value is not None and value >= 0]
            tagged = {value for mode,value in rows if mode is expected}
            # Local bot fallback: untagged balance on the same explicitly
            # authenticated demo socket. Never apply this fallback to REAL.
            untagged = {value for mode,value in rows if mode is None}
            scoped = bool(tagged)
            allowed = tagged if self.auth_seen else set()
            if not allowed and expected and self.auth_seen and self.sent_mode is True and all(mode is None for mode,_ in rows):
                allowed = untagged
            if len(allowed) == 1:
                self.verified_balance = self.balance = next(iter(allowed))
                self.account_verified = True
                self.balance_source = 'محدد النوع' if scoped else 'جلسة ديمو مؤكدة'
                self.verified_balance_at = time.monotonic()
        if kind not in BALANCE_EVENTS and isinstance(payload,dict) and 'isDemo' in payload and flag(payload['isDemo']) is not expected:
            return
        self.payout_book.feed(event)
        if self.mode == 'PRACTICE' and isinstance(payload,dict):
            history = payload.get('data') if isinstance(payload.get('data'),dict) else payload
            if 'candles' in history or kind in ('updatehistorynew','updatehistory','stream_update','m1history'):
                if history is not payload:
                    history = {**payload,**history}
                if 'isDemo' not in history or flag(history['isDemo']) is True:
                    self.scanner.accept_history(history)
        self.scanner.feed(event)
        if not self.wrong_mode:
            for tracked in self.trackers:
                tracked.feed(kind,payload,time.time())
                if not tracked.order.get('overlap'):
                    tracked.confirm_balance(self.verified_balance,self.verified_balance_at)

    async def subscribe(self, asset):
        if self.connected and not self.wrong_mode:
            if self.mode == 'PRACTICE' and self.scanner.strict_focus:
                self.scanner.request_history(asset)
                await self.select_asset(asset,60)
            else:
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
