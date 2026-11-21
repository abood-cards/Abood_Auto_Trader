"""Independent scanner and single-order coordinator. No Telegram signal client."""
import asyncio
from contextlib import suppress
from copy import deepcopy
import secrets
import time
from uuid import uuid4
from chess_transport import ChessPocket
from render_accounts import resolve_account, AccountConfigError
from risk_limits import risk_state
from diagnostics import SafeIssue


def account_key(mode):
    return 'real_account' if mode == 'REAL' else 'demo_account'


class TradingWorker:
    def __init__(self, repo, factory=ChessPocket):
        self.repo, self.factory = repo, factory
        self.po = None
        self.task = None
        self.stop_event = asyncio.Event()
        self.in_flight = False
        self.stage = 'متوقف'
        self.notify = None
        self.lock = asyncio.Lock()
        self.focus = None
        self.preview = '—'

    @property
    def pending(self):
        return self.repo.doc['runtime'].get('pending')

    def request_stop(self):
        self.stop_event.set()
        if self.task and not self.task.done() and not self.in_flight:
            self.task.cancel()

    async def close(self):
        self.request_stop()
        if self.task:
            try:
                await asyncio.wait_for(asyncio.shield(self.task), 8)
            except TimeoutError:
                self.task.cancel()
                with suppress(asyncio.CancelledError):
                    await self.task
            except asyncio.CancelledError:
                pass

    async def start(self):
        async with self.lock:
            if self.task and not self.task.done():
                return 'الجلسة تعمل أو تنهي صفقة قائمة.'
            if self.pending:
                return 'يوجد أمر غير محسوم. راجع سجل المنصة قبل أرشفته من الحالة.'
            if risk_state(self.repo.doc)['reason']:
                return 'بلغ الحساب حداً مالياً؛ الدخول متوقف.'
            if not self.repo.leader:
                return 'كاتب قاعدة البيانات غير متاح.'
            doc = self.repo.doc
            try:
                mode = doc['account_mode']
                _, source = resolve_account(doc,mode)
            except AccountConfigError as exc:
                return str(exc)
            self.stop_event.clear()
            self.stage = 'اتصال'
            self.task = asyncio.create_task(self.run())
            return 'بدأ طلب الاتصال والمسح على M1. مصدر الاتصال: '+source

    def valid(self, settings, deadline):
        d = self.repo.doc
        return (self.repo.leader and not self.stop_event.is_set() and time.time() < deadline
                and d['account_mode']==settings['account_mode']
                and d[account_key(d['account_mode'])]==settings[account_key(d['account_mode'])]
                and d['trading']==settings['trading'] and d['recovery']==settings['recovery']
                and d['chess']==settings['chess']
                and not risk_state(d)['reason'])

    async def probe(self):
        """Probe catalog symbols over the same socket. Actual ticks prove coverage."""
        scanner = self.po.scanner
        checked = {}
        candidate = None
        probe_at = 0.0
        while self.po.connected and not self.stop_event.is_set():
            now = time.monotonic()
            if not self.focus and not self.po.wrong_mode and now >= probe_at:
                latest = scanner.series.get(candidate)
                # If changeSymbol replaces the stream, retain a live symbol long
                # enough for five complete candles before rotating again.
                fresh = scanner.active_count()
                _, _, forming = scanner.forming()
                if (forming is not None and forming == candidate) or (latest and latest.bar and time.time()-latest.bar.last <= 5 and fresh < 2 and now-checked.get(candidate,0) < 370 and scanner.discard != candidate):
                    probe_at = now+3
                else:
                    candidates = [a for a, enabled in scanner.catalog.items() if enabled and a != scanner.discard]
                    candidate = min(candidates, key=lambda a:checked.get(a,0), default=None)
                    if candidate:
                        checked[candidate] = now
                        async with asyncio.timeout(4):
                            await self.po.subscribe(candidate)
                    probe_at = now+12
            await asyncio.sleep(1)

    async def chain(self, entry, settings):
        self.focus = entry.asset
        scanner = self.po.scanner
        t = settings['trading']
        # Setting 4 means four regular orders total, matching the previous bot.
        levels = max(1,t['martingale_steps'])
        rec = settings['recovery']
        regular_losses = 0
        next_deadline = entry.expires_at
        total = levels + int(rec['mode']=='ENABLED')
        preopened = None
        draining = False
        def make_order(step):
            amount = round(t['trade_amount'] + float(rec['increment']) if step == levels else t['trade_amount'] * 2**step,2)
            return dict(id=uuid4().hex, requestId=secrets.randbelow(1_999_999_999)+1,
                        asset=entry.asset, mode=settings['account_mode'], amount=amount,
                        action='call' if entry.direction=='BUY' else 'put', sent_at=time.time())
        def fresh(seconds):
            latest = scanner.series.get(entry.asset)
            if latest and latest.bar and -0.5 <= time.time()-latest.bar.last <= seconds:
                return latest.bar.close
            return None
        def valid(deadline):
            return self.valid(settings,deadline) and scanner.catalog.get(entry.asset,True) and fresh(3) is not None
        def adverse(price):
            mark = fresh(2)
            if price is None or mark is None:
                return False
            return mark < price if entry.direction=='BUY' else mark > price
        for step in range(total):
            if preopened is None:
                if not valid(next_deadline):
                    break
                order = make_order(step)
                if self.po.balance is None or order['amount'] > self.po.balance:
                    self.stage = 'توقف: الرصيد غير كافٍ'
                    self.stop_event.set()
                    break
                self.in_flight = True
                await self.repo.reserve(order)
                order['sent_at'] = time.time()
                if not await self.po.send_order(order,lambda:valid(next_deadline)):
                    await self.repo.resolve(order,None,None,None,0,dispatched=False)
                    self.in_flight = False
                    break
                tracked = getattr(self.po,'result',None)
            else:
                order, tracked = preopened
                preopened = None
            self.stage = f'صفقة {entry.direction} | {order["amount"]:g}$'
            self.preview = '—'
            expiry = order['sent_at']+60
            settle = asyncio.create_task(self.po.settlement(tracked) if tracked else self.po.settlement())
            try:
                done, _ = await asyncio.wait({settle}, timeout=max(0,expiry-2-time.time()))
                if not done:
                    price = getattr(tracked,'entry_price',None)
                    mark = fresh(2)
                    if price is not None and mark is not None:
                        self.preview = f'دخول {price:.10g} | آخر {mark:.10g} | ' + ('خاسرة لحظياً' if adverse(price) else 'غير خاسرة لحظياً')
                    else:
                        self.preview = 'سعر دخول مؤكد أو تكة حديثة غير متاحين'
                    if (not draining and step+1 < total and settings['account_mode']=='PRACTICE'
                        and settings['chess']['early_double'] and tracked and tracked.deal is not None
                        and valid(expiry) and adverse(price)):
                        followup = make_order(step+1)
                        # Keep enough balance for both stakes even if debit of the first is delayed.
                        if self.po.balance is not None and self.po.balance >= order['amount']+followup['amount']:
                            followup['overlap'] = True
                            await self.repo.reserve_followup(order,followup)
                            if await self.po.send_order(followup,lambda:valid(expiry) and not tracked.future.done() and adverse(price)):
                                preopened = (followup,self.po.result)
                            else:
                                await self.repo.discard_followup(order,followup)
                result, net = await settle
            finally:
                if not settle.done():
                    settle.cancel()
                    with suppress(asyncio.CancelledError):
                        await settle
            confirmed_at = time.time()
            if result == 'loss' and step < levels:
                regular_losses += -net
            exhausted = int(not draining and result=='loss' and step==levels-1)
            await self.repo.resolve(order,result,0,net,0,seq_lost=exhausted,
                                    seq_lost_amount=regular_losses if exhausted else 0)
            self.in_flight = preopened is not None
            limit = risk_state(self.repo.doc)['reason']
            if limit:
                self.stop_event.set()
                self.stage = 'توقف: بلوغ حد مالي'
            if result != 'loss' or limit or self.stop_event.is_set() or draining:
                if preopened:
                    draining = True  # settle already-sent exposure, but do not extend the ladder
                    continue
                break
            next_deadline = min(confirmed_at+3,order['sent_at']+70)

    async def run(self):
        delay = 1
        previous_asset = None
        try:
            while not self.stop_event.is_set():
                if risk_state(self.repo.doc)['reason']:
                    self.stage='توقف: بلوغ حد مالي'
                    break
                doc = deepcopy(self.repo.doc)
                mode = doc['account_mode']
                try:
                    credentials, source = resolve_account(doc,mode)
                except AccountConfigError as exc:
                    self.stage = 'توقف: ' + str(exc)
                    break
                self.po = self.factory(credentials,mode)
                print(f'[CHESS] settings_source={source} mode={mode}',flush=True)
                probe = None
                try:
                    await self.po.connect()
                    scanner = self.po.scanner
                    probe = asyncio.create_task(self.probe())
                    delay = 1
                    while self.po.connected and not self.stop_event.is_set():
                        if self.po.wrong_mode:
                            self.stage = 'توقف: تأكيد المصادقة يخص حساباً مختلفاً'
                            self.stop_event.set()
                            break
                        if risk_state(self.repo.doc)['reason']:
                            self.stage = 'توقف: بلوغ حد مالي'
                            self.stop_event.set()
                            break
                        scanner.enabled = bool(self.po.account_verified and self.po.verified_balance is not None)
                        scanner.early = self.repo.doc['chess']['early']
                        scanner.discard = previous_asset if len([a for a,on in scanner.catalog.items() if on]) > 1 else None
                        self.stage = 'مسح M1' if scanner.enabled else 'مسح للقراءة فقط — انتظار تأكيد نوع الحساب والرصيد'
                        if probe.done():
                            probe.result()
                        if scanner.entries:
                            entry = scanner.entries.popleft()
                            if entry.expires_at > time.time() and entry.asset != scanner.discard:
                                scanner.enabled = False
                                scanner.entries.clear()
                                await self.chain(entry,deepcopy(self.repo.doc))
                                previous_asset = entry.asset
                                self.focus = None
                        await asyncio.sleep(.05 if scanner.early else .1)
                except Exception as exc:
                    print('[CHESS]',type(exc).__name__,flush=True)
                    if self.pending:
                        self.stage = 'توقف: أمر غير محسوم — لم يُعَد إرساله'
                        self.stop_event.set()
                        if self.notify:
                            with suppress(Exception):
                                await asyncio.wait_for(self.notify(self.stage),5)
                        break
                    if isinstance(exc, SafeIssue) and getattr(exc, 'code', None) == 'PO_AUTH_REJECTED':
                        self.stage = 'توقف: المنصة رفضت المصادقة. راجع جلسة الحساب المختار في Render.'
                        self.stop_event.set()
                        break
                    self.stage = 'إعادة الاتصال'
                finally:
                    if probe:
                        probe.cancel()
                        with suppress(asyncio.CancelledError, Exception):
                            await probe
                    await self.po.close()
                    self.in_flight = False
                    self.focus = None
                if not self.stop_event.is_set():
                    await asyncio.sleep(delay)
                    delay = min(20,delay*2)
        finally:
            if not self.stage.startswith('توقف:'):
                self.stage = 'متوقف'

    def status(self):
        po = self.po
        s = po.scanner if po else None
        coverage = (f'الأصول المكتشفة: {sum(s.catalog.values())} | بث حديث: {s.active_count()} | شموع M1 مكتملة: {s.coverage()}' if s else 'لم يبدأ المسح')
        focus = self.focus or '—'
        count, _, forming = s.forming() if s else (0,0,None)
        formation = f'{forming} ({count}/4)' if forming else '—'
        account = ('مطابق' if po.account_verified else 'لم يرد تأكيد مطابق') if po else '—'
        balance = ('وصل' if po.verified_balance is not None else 'لم يصل') if po else '—'
        subscriptions = po.subscription_requests if po else 0
        try:
            _, source = resolve_account(self.repo.doc,self.repo.doc['account_mode'])
        except AccountConfigError as exc:
            source = str(exc)
        if po and po.connected:
            market = 'تكات غير مستلمة' if not s.ticks_received else 'تكات وصلت؛ راجع عداد البث الحديث'
            diag = f'\nالمصادقة: {int(po.auth_seen)} | إطارات الرصيد: {po.balance_events} | {market}'
        else:
            diag = ''
        return (f'{self.stage} | {self.repo.doc["account_mode"]}\n{coverage}\nمصدر الاتصال: {source}\nطلبات الاشتراك: {subscriptions}{diag}\nتأكيد الحساب: {account} | الرصيد: {balance}\nنموذج قيد التكوّن: {formation}\nالأصل قيد التنفيذ: {focus}\nقراءة آخر ثانيتين: {self.preview}'
                + ('\n⚠️ أمر غير محسوم: تحقّق من سجل المنصة.' if self.pending else ''))
