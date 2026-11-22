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
from payout_guard import scan_payout, SCAN_PAYOUT_MIN, SCAN_PAYOUT_MAX

PAYOUT_REFRESH_INTERVAL = 360
SCAN_QUOTE_MIN_AGE_LIMIT = 360
VERIFY_PARALLEL_AFTER = 20
COLLECT_WINDOW = 310
import scan_scope


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
        self.scan_target = None
        self.scan_state = '—'
        self.cursor = None
        self.probed = set()
        self.last_signal = '—'
        self.refresh_attempts = 0
        self.last_refresh = float('-inf')
        self.qualified_before = set()

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
            self.cursor = None
            self.refresh_attempts = 0
            self.qualified_before.clear()
            self.last_refresh = float('-inf')
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
                and d['payout']==settings['payout']
                and d.get('scan')==settings.get('scan')
                and not risk_state(d)['reason'])

    def payout(self, asset):
        book = getattr(self.po,'payout_book',None)
        return scan_payout(book,asset,max(SCAN_QUOTE_MIN_AGE_LIMIT,self.repo.doc['payout']['max_age_seconds']))

    def allowed_asset(self, asset):
        return scan_scope.allowed(self.repo.doc.get('scan',scan_scope.defaults()), asset, self.po.scanner.groups)

    def payout_due(self, scanner, now):
        if (self.focus or scanner.locked or scanner.entries or self.pending or not self.repo.leader
                or now-self.last_refresh < PAYOUT_REFRESH_INTERVAL):
            return False
        book = self.po.payout_book
        scoped = [asset for asset in set(book.quotes) | set(scanner.catalog)
                  if self.allowed_asset(asset) and scanner.catalog.get(asset,True)]
        if not scoped:
            return not book.quotes
        codes = {self.payout(asset)[0] for asset in scoped}
        return 'KNOWN' not in codes and bool(codes & {'STALE','NO_TABLE','ASSET_MISSING'})

    def keep_warming(self, asset):
        code,rate=self.payout(asset)
        if code=='KNOWN':
            self.qualified_before.add(asset)
            return True
        # A previously qualified live feed may keep collecting ticks. A stale
        # quote NEVER authorizes a new subscription, candidate or order.
        if code=='STALE' and rate is not None and SCAN_PAYOUT_MIN<=rate<=SCAN_PAYOUT_MAX:
            return asset in self.qualified_before
        self.qualified_before.discard(asset)
        return False

    def focus_selected(self, asset):
        self.cursor = asset

    def scan_key(self, asset):
        group = self.po.scanner.groups.get(asset) or scan_scope.category(asset)
        groups = tuple(scan_scope.LABELS)
        _,rate = self.payout(asset)
        return (groups.index(group) if group in groups else len(groups),
                -(rate if rate is not None else -1),asset.casefold(),asset)

    def scan_order(self, sources):
        # Filtering before scheduling is equivalent to skipping the rest of a
        # descending payout group at its first <90 row. Missing/stale never enter.
        return sorted((a for a,on in sources.items() if on and self.allowed_asset(a)
                       and self.payout(a)[0]=='KNOWN'),key=self.scan_key)

    async def probe(self):
        """Equal rotation when only one feed is available; concurrent streams are all scanned."""
        scanner = self.po.scanner
        requested_wall = None
        requested_mono = None
        self.probed.clear()
        self.scan_target = None
        target = None
        probe_at = time.monotonic()
        handled_focus = None
        finished_target = None
        last_scope = None
        while self.po.connected and not self.stop_event.is_set():
            now = time.monotonic()
            scope = self.repo.doc.get('scan', scan_scope.defaults())
            if scope != last_scope:
                last_scope = scope
                probe_at = now
            if scanner.last_candidate != handled_focus and not scanner.locked:
                handled_focus = scanner.last_candidate
                finished_target = target
                probe_at = now
            if not self.focus and not scanner.locked and not scanner.entries and not self.po.wrong_mode and now >= probe_at:
                sources = {**{a:q.tradable is not False for a,q in self.po.payout_book.quotes.items()},
                           **scanner.catalog}
                candidates = self.scan_order(sources)
                if self.cursor in candidates:
                    pos = candidates.index(self.cursor)+1
                    candidates = candidates[pos:]+candidates[:pos]
                elif self.cursor:
                    cursor_key = self.scan_key(self.cursor)
                    candidates = [a for a in candidates if self.scan_key(a)>cursor_key]+[a for a in candidates if self.scan_key(a)<=cursor_key]
                live = set(scanner.active_assets())
                depth = scanner.series[target].ready_depth() if target in live else 0
                age = now-requested_mono if requested_mono is not None else 0
                # A tick left over from the last subscription is not simultaneous coverage.
                parallel = (requested_wall is not None and age >= VERIFY_PARALLEL_AFTER and
                    any(a != target and scanner.series[a].bar.last >= requested_wall+VERIFY_PARALLEL_AFTER
                        for a in live if a in candidates))
                hold = (target in live and target in candidates and target != finished_target
                        and depth < 4 and not parallel and age < (15 if scanner.history_asset else COLLECT_WINDOW))
                if hold:
                    self.scan_state = f'{target}: تجميع {depth}/4 — لا تبديل أثناء التهيئة'
                    probe_at = now+.5
                else:
                    choices = [a for a in candidates if a not in live]
                    asset = next(iter(choices),None)
                    if asset is not None:
                        finished_target = None
                        self.probed.add(asset)
                        target = self.scan_target = self.cursor = asset
                        requested_wall,requested_mono = scanner.clock(),now
                        self.scan_state = f'{asset}: طلب بث'
                        async with asyncio.timeout(4):
                            await self.po.subscribe(asset)
                    else:
                        self.scan_state = 'متابعة البث المتاح' if live else 'انتظار أصل بعائد مؤهل'
                    probe_at = now+10
            await asyncio.sleep(.25)

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
            return (self.valid(settings,deadline) and scanner.catalog.get(entry.asset,True)
                    and self.allowed_asset(entry.asset) and fresh(3) is not None and self.payout(entry.asset)[0] == 'KNOWN')
        def adverse(price):
            mark = fresh(2)
            if price is None or mark is None:
                return False
            return mark < price if entry.direction=='BUY' else mark > price
        for step in range(total):
            if preopened is None:
                if not valid(next_deadline):
                    self.last_signal = f'{entry.asset} {entry.direction} | لم يُرسل: انتهت نافذة الدخول أو تغيّر شرط التنفيذ'
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
                    self.last_signal = f'{entry.asset} {entry.direction} | رفض قبل الإرسال: الحساب أو الرصيد أو صلاحية الإشارة'
                    break
                self.last_signal = f'{entry.asset} {entry.direction} | أُرسل طلب الأمر'
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
        handoff = None
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
                    if handoff and (mode,credentials)==handoff[:2]:
                        scanner.series, scanner.clocks = handoff[2:4]
                    handoff = None
                    connected_at = time.monotonic()
                    scanner.allowed = self.allowed_asset
                    scanner.eligible = lambda asset: self.repo.leader and not self.stop_event.is_set() and not self.focus and self.payout(asset)[0]=='KNOWN'
                    scanner.track_price = lambda asset: (asset == self.focus
                        or (scanner.locked is not None and asset == scanner.locked[0])
                        or self.keep_warming(asset))
                    scanner.on_select = self.focus_selected
                    scanner.strict_focus = mode == 'PRACTICE'
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
                        scanner.expire_focus()
                        self.stage = 'مسح M1' if scanner.enabled else 'مسح للقراءة فقط — انتظار تأكيد نوع الحساب والرصيد'
                        if scanner.locked:
                            self.stage = f'تركيز: {scanner.locked[0]} — ' + ('آخر ثانية؛ انتظار إغلاق مؤكد' if scanner.last_second else 'انتظار إغلاق الشمعة')
                        elif (time.monotonic()-connected_at >= 30 and self.payout_due(scanner,time.monotonic())):
                            self.last_refresh = time.monotonic()
                            self.refresh_attempts += 1
                            self.stage = 'تحديث جدول العوائد — إعادة اتصال متسلسلة'
                            handoff = (mode,credentials,scanner.series,scanner.clocks)
                            break
                        if probe.done():
                            probe.result()
                        if scanner.entries:
                            entry = scanner.entries.popleft()
                            code, rate = self.payout(entry.asset)
                            shown = f'{rate:g}%' if rate is not None else '—'
                            if entry.expires_at <= time.time():
                                code = 'ENTRY_EXPIRED'
                            self.last_signal = f'{entry.asset} {entry.direction} | {shown} | {code}'
                            if code == 'KNOWN' and self.allowed_asset(entry.asset):
                                scanner.enabled = False
                                scanner.entries.clear()
                                await self.chain(entry,deepcopy(self.repo.doc))
                                self.cursor = entry.asset
                                self.focus = None
                        await asyncio.sleep(.05)
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
        live_depths = [s.series[a].ready_depth() for a in s.active_assets() if self.payout(a)[0]=='KNOWN'] if s else []
        ready = sum(d>=4 for d in live_depths)
        coverage = (f'الأصول المكتشفة: {sum(s.catalog.values())} | بث حديث: {s.active_count()} | أصول لديها شمعة M1 مكتملة: {s.coverage()}' if s else 'لم يبدأ المسح')
        focus = self.focus or '—'
        live = s.active_assets() if s else []
        receiving = ', '.join(live[:4]) + (f' (+{len(live)-4})' if len(live) > 4 else '') if live else '—'
        forming = s.locked[0] if s and s.locked else None
        scope = scan_scope.describe(self.repo.doc.get('scan',scan_scope.defaults()))
        watched = forming or (self.scan_target if self.scan_target in live else live[0] if live else None)
        series = s.series.get(watched) if watched else None
        colors = {1:'🟢', -1:'🔴', 0:'⚪'}
        candles = ' '.join(colors[b.color] if b.good else '▫️' for b in list(series.closed)[-4:]) if series else ''
        if series and series.bar:
            candles = f'{candles or "—"} | الحالية: {colors[series.bar.color]}'
        formation = f'{forming} — باقي {max(0,s.locked[1]-time.time()):.0f} ث' if forming else '—'
        quotes = po.payout_book.quotes if po else {}
        scoped_payouts = [self.payout(asset)[0] for asset in quotes if self.allowed_asset(asset)]
        eligible = scoped_payouts.count('KNOWN')
        stale = scoped_payouts.count('STALE')
        if po and po.connected and not eligible and not self.focus and not forming:
            payout_status = ' — انتظار عائد حديث ومؤهل'
        else:
            payout_status = '' 
        code, rate = self.payout(watched) if watched else ('—',None)
        rate_text = f'{rate:g}% ({code})' if rate is not None else code
        account = ('مطابق' if po.account_verified else 'لم يرد تأكيد مطابق') if po else '—'
        balance = ('وصل' if po.verified_balance is not None else 'لم يصل') if po else '—'
        subscriptions = po.subscription_requests if po else 0
        try:
            _, source = resolve_account(self.repo.doc,self.repo.doc['account_mode'])
        except AccountConfigError as exc:
            source = str(exc)
        if po and po.connected:
            clock = s.last_clock.status() if s.last_clock else {}
            reason = clock.get('clock_status','WAITING_FOR_LIVE_TICKS')
            offset = clock.get('clock_offset_seconds')
            correction = '—' if offset is None else f'{offset/3600:+g}h'
            diag = (f'\nالمصادقة: {int(po.auth_seen)} | أحداث الحساب: {po.balance_events} | مصدر الرصيد: {po.balance_source}'
                    f'\nالتكات المستلمة: {s.ticks_received} | المقبولة: {s.accepted}'
                    f'\nساعة البث: {reason} | فرق: {correction}')
        else:
            diag = ''
        return (f'{self.stage} | {self.repo.doc["account_mode"]}\n{coverage}\nالنطاق: {scope} | موضع القائمة: {self.cursor or "—"}\nهدف الاشتراك الحالي: {self.scan_target or "—"} | بث مستلم: {receiving}\nآخر 4 لـ {watched or "—"}: {candles or "—"}\nمصدر الاتصال: {source}\nطلبات الاشتراك: {subscriptions} | أصول طُلبت: {len(self.probed)}\nعائد الأصل المعروض: {rate_text} | مطابق 90–92%: {eligible}/{len(scoped_payouts)} | قديم: {stale}{payout_status} | صلاحية حتى {max(SCAN_QUOTE_MIN_AGE_LIMIT,self.repo.doc["payout"]["max_age_seconds"]):g} ث\nمحاولات تحديث العائد: {self.refresh_attempts}\nجاهزة (4 شموع متتالية): {ready}/{len(live_depths)} | أعلى تغطية: {max(live_depths,default=0)}/5\nآخر فحص إغلاق: {s.last_closed if s else chr(8212)}\nآخر نموذج مغلق: {self.last_signal}\nتاريخ M1: {s.history_state if s else chr(8212)}\nجدولة المسح: {('مثبّت حتى الإغلاق' if forming else self.scan_state)}{diag}\nتأكيد الحساب: {account} | الرصيد: {balance}\nنموذج قيد التكوّن: {formation}\nآخر تركيز: {s.last_wait if s else chr(8212)}\nالأصل قيد التنفيذ: {focus}\nقراءة آخر ثانيتين: {self.preview}'
                + ('\n⚠️ أمر غير محسوم: تحقّق من سجل المنصة.' if self.pending else ''))
