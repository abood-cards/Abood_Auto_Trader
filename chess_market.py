"""M1 bars from timestamped market ticks, with no synthetic gap filling."""
from collections import deque
from dataclasses import dataclass
import math
import time
from tick_clock import TickClock
from collections import Counter
from scan_scope import category
from payout_guard import symbol as asset_symbol


def num(value):
    if isinstance(value, bool):
        return None
    try:
        v = float(value)
        return v if math.isfinite(v) else None
    except (ValueError, TypeError, OverflowError):
        return None


def extract_ticks(data, depth=0):
    if depth > 12:
        return
    if isinstance(data, list):
        if len(data) >= 3 and asset_symbol(data[0]) and num(data[1]) is not None and num(data[2]) is not None:
            yield data[0], data[1], data[2]
        else:
            for entry in data:
                yield from extract_ticks(entry, depth + 1)
    elif isinstance(data, dict):
        symbol = asset_symbol(data.get('asset', data.get('symbol')))
        if symbol:
            ts, price = data.get('timestamp', data.get('time')), data.get('price', data.get('value'))
            if num(ts) is not None and num(price) is not None:
                yield symbol, ts, price
        for key in ('data', 'ticks'):
            if key in data:
                yield from extract_ticks(data[key], depth + 1)


def extract_catalog(value, depth=0):
    if depth > 10:
        return
    if isinstance(value, list):
        if len(value) >= 6 and asset_symbol(value[1]):
            yield value[1], value[14] if len(value) > 14 else None, value[3] if isinstance(value[3],str) else None
        else:
            for row in value:
                yield from extract_catalog(row, depth+1)
    elif isinstance(value, dict):
        symbol = asset_symbol(value.get('asset', value.get('symbol')))
        if symbol:
            yield symbol, next((value[k] for k in ('tradable','isActive','enabled') if k in value), None), next((value[k] for k in ('category','assetType','type','group') if isinstance(value.get(k),str)),None)
        for key, row in value.items():
            if key in ('assets','data','result'):
                yield from extract_catalog(row, depth+1)
            elif asset_symbol(key) and isinstance(row,dict) and any(k in row for k in ('tradable','isActive','enabled','payout','profit','percent')):
                yield key, next((row[k] for k in ('tradable','isActive','enabled') if k in row), None), next((row[k] for k in ('category','assetType','type','group') if isinstance(row.get(k),str)),None)


@dataclass
class Bar:
    minute: int
    first: float
    last: float
    opened: float
    close: float
    gap: float = 0
    demo_tolerance: bool = False
    history_open: bool = False

    @property
    def color(self):
        return (self.close > self.opened) - (self.close < self.opened)

    @property
    def good(self):
        return self.coverage_issue is None

    @property
    def coverage_issue(self):
        edge, gap = (5, 20) if self.demo_tolerance else (3, 10)
        if not self.history_open and self.first - self.minute > edge:
            return 'LATE_FIRST_TICK'
        if self.minute + 60 - self.last > edge:
            return 'MISSING_END_TICK'
        if (not self.history_open and self.last <= self.first) or self.gap > gap:
            return 'TICK_GAP'
        return None


@dataclass(frozen=True)
class Entry:
    asset: str
    direction: str
    boundary: int
    expires_at: float
    early: bool


class Series:
    def __init__(self):
        self.closed = deque(maxlen=8)
        self.bar = None

    def ready_depth(self):
        next_minute = self.bar.minute if self.bar else None
        count = 0
        for candle in reversed(self.closed):
            if not candle.good or next_minute != candle.minute + 60:
                break
            count += 1
            next_minute = candle.minute
        return min(count,5)

    @staticmethod
    def expected(bars):
        """Four closed bars: tail of a same-colour run followed by alternation."""
        bars = list(bars)[-4:]
        if len(bars) != 4 or not all(b.good for b in bars):
            return 0
        if any(b.minute-a.minute != 60 for a,b in zip(bars,bars[1:])):
            return 0
        a,b,c,d = (bar.color for bar in bars)
        return -a if a and (b,c,d) == (a,-a,a) else 0

    @classmethod
    def direction(cls, bars):
        bars = list(bars)[-5:]
        if len(bars) != 5:
            return None
        last = bars[-1]
        expected = cls.expected(bars[:4])
        if expected and last.good and last.minute == bars[-2].minute + 60 and last.color == expected:
            return 'BUY' if expected == -1 else 'SELL'
        return None

    def tick(self, ts, price, demo_tolerance=False):
        minute = int(ts // 60) * 60
        if self.bar and ts <= self.bar.last:
            return False
        closed = bool(self.bar and minute != self.bar.minute)
        if closed:
            self.closed.append(self.bar)
            if minute - self.bar.minute != 60:
                self.closed.clear()
        if not self.bar or closed:
            self.bar = Bar(minute,ts,ts,price,price,demo_tolerance=demo_tolerance)
        else:
            self.bar.gap = max(self.bar.gap, ts-self.bar.last)
            self.bar.last = ts
            self.bar.close = price
        return closed


class Scanner:
    def __init__(self, clock=time.time, mono=time.monotonic):
        self.clock = clock
        self.mono = mono
        self.clocks = {}
        self.rejected = Counter()
        self.accepted = 0
        self.last_clock = None
        self.series = {}
        self.catalog = {}
        self.groups = {}
        self.allowed = lambda asset: True
        self.eligible = None  # production supplies the account payout gate
        self.strict_focus = False
        self.history_only = False
        self.history_checked = None
        self.history_minute = None
        self.history_asset = None
        self.history_state = '—'
        self.history_rows = None
        self.track_price = lambda asset: True
        self.payout_filtered = Counter()
        self.last_second = False
        self.closed_checks = Counter()
        self.last_closed = '—'
        self.on_select = lambda asset: None
        self.locked = None
        self.last_wait = '—'
        self.last_candidate = None
        self.verified = set()
        self.entries = deque(maxlen=128)
        self.enabled = False
        self.discard = None
        self.ticks_received = 0

    def clear(self):
        self.history_checked = self.history_minute = None
        self.history_asset = self.history_rows = None
        self.history_state = '—'
        self.series.clear(); self.catalog.clear(); self.entries.clear(); self.verified.clear(); self.ticks_received = 0
        self.groups.clear(); self.locked = None; self.last_candidate = None; self.last_wait = '—'
        self.payout_filtered.clear(); self.last_second = False
        self.closed_checks.clear(); self.last_closed = '—'
        self.clocks.clear(); self.rejected.clear(); self.accepted = 0; self.last_clock = None

    def request_history(self, asset):
        self.history_checked = self.history_minute = None
        self.history_asset = asset
        self.history_rows = None
        self.history_state = f'{asset}: انتظار رد M1 الحالي'

    def accept_history(self, payload):
        if not self.strict_focus or not isinstance(payload,dict):
            return
        asset = payload.get('asset',payload.get('symbol'))
        period = num(payload.get('period',payload.get('timeframe')))
        if not asset or asset != self.history_asset or period != 60:
            return
        candles = payload.get('candles',payload.get('data'))
        if not isinstance(candles,list) or not 1 <= len(candles) <= 5000:
            return
        rows = {}
        for item in candles:
            if isinstance(item,dict):
                values = [item.get('time',item.get('timestamp')),item.get('open'),item.get('close'),item.get('high'),item.get('low')]
            elif isinstance(item,list) and len(item) >= 5:
                values = item[:5]
            else:
                continue
            ts,o,c,h,l = map(num,values)
            if None in (ts,o,c,h,l) or not 0 < l <= min(o,c) <= max(o,c) <= h:
                continue
            if ts > 1e11:
                ts /= 1000
            # Accept explicit close-stamped candles only with an explicit convention.
            if payload.get('timestampType') == 'close':
                ts -= 60
            if ts <= 0 or ts % 60:
                continue
            if ts in rows and rows[ts] != (o,c):
                self.history_state = f'{asset}: تاريخ متضارب'
                return
            rows[ts] = (o,c)
        if not rows:
            self.history_state = f'{asset}: رد M1 بلا صفوف OHLC صالحة'
            return
        merged = dict(self.history_rows or {})
        merged.update(rows)
        self.history_rows = dict(sorted(merged.items())[-5000:])
        self.history_state = f'{asset}: وصل التاريخ؛ انتظار تكة حديثة لمطابقة الدقيقة'

    def seed_history(self, asset, series, ts, offset):
        if asset != self.history_asset or self.history_rows is None:
            return
        rows = self.history_rows
        if not self.history_only:
            self.history_rows = None
        minute = int(ts//60)*60
        times = tuple(range(minute-240,minute,60))
        if self.locked or (series.bar and series.bar.minute > minute):
            return
        existing = {b.minute:b for b in series.closed if b.minute < minute}
        # At the boundary the previous live bar has not been rolled yet.
        if series.bar and series.bar.minute == minute-60 and series.bar.good:
            existing[series.bar.minute] = series.bar
        live = set() if self.history_only else {m for m in times if m in existing and existing[m].good}
        shifts = tuple(dict.fromkeys((offset,0)))
        candidates = [(shift, {m for m in times if m+shift in rows}) for shift in shifts]
        # Both clocks are known, not guessed. Overlapping ambiguous snapshots
        # are not used unless exactly one yields continuity and contributes data.
        viable = [(shift, present) for shift,present in candidates
                  if present and len(present | live)==4]
        if len(viable)!=1:
            best=max((len(present | live) for _,present in candidates),default=0)
            why='توقيت تاريخ ملتبس' if viable else 'دقائق مغلقة غير متصلة'
            latest=min((int(minute-(max(rows)-shift)) for shift in shifts),key=abs)
            self.history_state=f'{asset}: {why} ({best}/4 | صفوف {len(rows)} | فرق أحدث صف {latest:+d}ث)'
            return
        shift,present=viable[0]
        self.history_rows = None
        self.history_minute = minute
        if not series.bar or series.bar.minute != minute:
            series.bar = None
        for m in present-live:
            o,c=rows[m+shift]
            existing[m]=Bar(m,m,m+59,o,c,demo_tolerance=True)
        series.closed = deque(sorted(existing.values(),key=lambda b:b.minute)[-8:],maxlen=8)
        if minute+shift in rows:
            # Use the historical OPEN only. The close of the running candle
            # must come from live ticks, never from this snapshot.
            opening = rows[minute+shift][0]
            if series.bar is None:
                series.bar = Bar(minute,ts,ts-0.001,opening,opening,demo_tolerance=True,history_open=True)
            elif not series.bar.good:
                series.bar = Bar(minute,ts,ts-0.001,opening,opening,demo_tolerance=True,history_open=True)
        self.history_state = f'{asset}: تاريخ M1 جاهز (4/4؛ حي {len(live)})'

    def feed(self, event):
        if not isinstance(event,list) or len(event)<2:
            return
        name,data = event[:2]
        if name in ('updateAssets','assets','listAssets','updateAsset'):
            for asset, enabled, hint in extract_catalog(data):
                if len(self.catalog) < 4096 or asset in self.catalog:
                    self.catalog[asset] = str(enabled).lower() not in ('0','false','0.0')
                    if hint or asset not in self.groups:
                        guessed = category(asset,hint)
                        if guessed != 'other' or asset not in self.groups:
                            self.groups[asset] = guessed
        if not isinstance(name,str) or name.lower() != 'updatestream':
            return
        now, mono = self.clock(), self.mono()
        held_at_start = self.locked[0] if self.locked else None
        for asset, raw_ts, raw_price in extract_ticks(data):
            self.ticks_received += 1
            ts, price = num(raw_ts), num(raw_price)
            if ts is None or price is None or price <= 0:
                continue
            if ts > 1e11:
                ts /= 1000
            if not self.allowed(asset):
                continue
            if not self.catalog.get(asset, True):
                self.rejected['ASSET_DISABLED'] += 1
                continue
            self.expire_focus(now)
            if not self.track_price(asset):
                self.payout_filtered[asset] += 1
                continue
            if asset not in self.clocks and len(self.clocks) >= 4096:
                continue
            if asset not in self.clocks:
                self.clocks[asset] = TickClock()
            clock = self.last_clock = self.clocks[asset]
            ts = clock.normalize(ts,now,mono)
            if ts is None:
                self.rejected[clock.reason] += 1
                if clock.reason == 'LOCAL_CLOCK_CHANGED_RECALIBRATING':
                    self.series.pop(asset,None)
                    self.entries = deque((e for e in self.entries if e.asset != asset),maxlen=128)
                continue
            self.accepted += 1
            self.verified.add(asset)
            series = self.series.setdefault(asset, Series())
            self.seed_history(asset,series,ts,clock.offset)
            if series.bar and ts <= series.bar.last:
                continue
            rolled = series.tick(ts, price,self.strict_focus)
            bar = series.bar
            if not self.enabled or self.discard == asset or (held_at_start is not None and asset != held_at_start):
                continue
            if self.eligible is not None:
                if self.locked:
                    held,boundary = self.locked
                    if asset == held and not self.last_second and 0 < boundary-now <= 1:
                        self.last_second = True
                    if asset == held and rolled and bar.minute >= boundary:
                        direction = series.direction(series.closed) if bar.minute == boundary else None
                        closed = series.closed[-1] if series.closed else None
                        gap_issue = closed.coverage_issue if closed and closed.minute == boundary-60 else 'MISSING_CANDLE'
                        code = 'MATCH' if direction else (gap_issue or 'CLOSE_COLOR_MISMATCH')
                        if direction and not self.eligible(asset):
                            code = 'PAYOUT_OR_EXECUTION_GATE'
                        self.last_closed = f'{asset}: {series.ready_depth()}/5 | {code}'
                        self.closed_checks[code] += 1
                        if code == 'MATCH' and now < boundary+3:
                            self.entries.append(Entry(asset,direction,boundary,boundary+3,False))
                            self.last_wait = f'{asset}: تأكّد الإغلاق'
                        else:
                            self.last_wait = f'{asset}: لا دخول | {code}'
                        self.locked = None
                    continue
                if self.entries or not self.eligible(asset):
                    continue
                if rolled and bar.minute <= now < bar.minute+3 and self.last_candidate != (asset,bar.minute):
                    depth = series.ready_depth()
                    direction = series.direction(series.closed) if depth >= 5 else None
                    code = ('NO_PRIOR_FOCUS' if self.strict_focus else 'MATCH') if direction else ('PATTERN' if depth >= 5 else 'WARMUP')
                    self.closed_checks[code] += 1
                    issue = series.closed[-1].coverage_issue if series.closed else None
                    self.last_closed = f'{asset}: {depth}/5 | {code}' + (f' / {issue}' if issue else '')
                    if direction and not self.strict_focus:
                        self.last_candidate = (asset,bar.minute)
                        self.on_select(asset)
                        self.entries.append(Entry(asset,direction,bar.minute,bar.minute+3,False))
                        self.last_wait = f'{asset}: تطابق عند الإغلاق'
                        continue
                if self.history_only and (asset != self.history_asset or self.history_minute != bar.minute):
                    continue
                if self.last_candidate == (asset,bar.minute+60):
                    continue
                remaining = bar.minute+60-now
                expected = series.expected(series.closed) if series.closed and series.closed[-1].minute+60 == bar.minute else 0
                if self.history_only and 0 < remaining <= 60:
                    self.history_checked = (asset,bar.minute)
                    if not expected:
                        self.history_state = f'{asset}: 4/4 — الألوان لا تطابق النموذج'
                if 0 < remaining <= 60 and expected:
                    self.locked = self.last_candidate = (asset,bar.minute+60)
                    self.last_second = 0 < remaining <= 1
                    self.last_wait = f'{asset}: انتظار الإغلاق'
                    self.on_select(asset)
                continue
            if rolled and now <= bar.minute+3:
                direction = series.direction(series.closed)
                if direction:
                    self.entries.append(Entry(asset,direction,bar.minute,bar.minute+3,False))

    def expire_focus(self, now=None):
        if self.locked:
            asset,boundary = self.locked
            if not self.enabled or not self.allowed(asset) or not self.catalog.get(asset,True) or (self.clock() if now is None else now) >= boundary+3:
                self.last_wait = f'{asset}: أُلغي الانتظار (تغيير النطاق أو غياب إغلاق صالح)'
                self.locked = None

    def forming(self, assets=None):
        """Track a candidate without assuming the open candle's final color."""
        now = self.clock()
        candidates = []
        for asset, series in self.series.items():
            bar = series.bar
            if (assets is not None and asset not in assets) or asset == self.discard or not self.catalog.get(asset, True) or not bar or now-bar.last > 10:
                continue
            closed = series.closed
            remaining = bar.minute + 60 - now
            expected = series.expected(closed)
            if (0 < remaining <= 60 and closed and closed[-1].minute+60 == bar.minute
                    and expected):
                candidates.append((4,bar.last,asset))
        return max(candidates, default=(0,0,None))

    def active_assets(self):
        now = self.clock()
        return sorted(a for a,s in self.series.items() if self.allowed(a) and s.bar is not None
                      and now-s.bar.last <= 10 and self.catalog.get(a,True))

    def active_count(self):
        return len(self.active_assets())

    def coverage(self):
        now = self.clock()
        return sum(bool(s.closed) and s.closed[-1].good and now-s.closed[-1].minute < 123 and self.catalog.get(a,True) for a,s in self.series.items())
