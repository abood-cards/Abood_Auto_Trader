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

    @property
    def color(self):
        return (self.close > self.opened) - (self.close < self.opened)

    @property
    def good(self):
        return self.first - self.minute <= 3 and self.minute + 60 - self.last <= 3 and self.gap <= 10


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

    def tick(self, ts, price):
        minute = int(ts // 60) * 60
        if self.bar and ts <= self.bar.last:
            return False
        closed = bool(self.bar and minute != self.bar.minute)
        if closed:
            self.closed.append(self.bar)
            if minute - self.bar.minute != 60:
                self.closed.clear()
        if not self.bar or closed:
            self.bar = Bar(minute,ts,ts,price,price)
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
        self.track_price = lambda asset: True
        self.payout_filtered = Counter()
        self.last_second = False
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
        self.series.clear(); self.catalog.clear(); self.entries.clear(); self.verified.clear(); self.ticks_received = 0
        self.groups.clear(); self.locked = None; self.last_candidate = None; self.last_wait = '—'
        self.payout_filtered.clear(); self.last_second = False
        self.clocks.clear(); self.rejected.clear(); self.accepted = 0; self.last_clock = None

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
                if clock.reason != 'DUPLICATE_OR_REVERSED_TICK':
                    self.series.pop(asset,None)
                    self.entries = deque((e for e in self.entries if e.asset != asset),maxlen=128)
                continue
            self.accepted += 1
            self.verified.add(asset)
            series = self.series.setdefault(asset, Series())
            if series.bar and ts <= series.bar.last:
                continue
            rolled = series.tick(ts, price)
            bar = series.bar
            if not self.enabled or self.discard == asset:
                continue
            if self.eligible is not None:
                if self.locked:
                    held,boundary = self.locked
                    if asset == held and not self.last_second and 0 < boundary-now <= 1:
                        self.last_second = True
                    if asset == held and rolled and bar.minute >= boundary:
                        direction = series.direction(series.closed) if bar.minute == boundary else None
                        if direction and now < boundary+3 and self.eligible(asset):
                            self.entries.append(Entry(asset,direction,boundary,boundary+3,False))
                            self.last_wait = f'{asset}: تأكّد الإغلاق'
                        else:
                            self.last_wait = f'{asset}: ' + ('أغلق بالنمط لكن العائد غير مؤهل' if direction else 'إغلاق معاكس أو شمعة غير مكتملة')
                        self.locked = None
                    continue
                if self.entries or self.last_candidate == (asset,bar.minute+60) or not self.eligible(asset):
                    continue
                remaining = bar.minute+60-now
                expected = series.expected(series.closed) if series.closed and series.closed[-1].minute+60 == bar.minute else 0
                if (0 < remaining <= 60 and expected and bar.color == expected
                        and bar.first-bar.minute <= 3 and bar.gap <= 10):
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
                    and expected and bar.color == expected):
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
