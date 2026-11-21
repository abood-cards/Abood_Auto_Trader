"""M1 bars from timestamped market ticks, with no synthetic gap filling."""
from collections import deque
from dataclasses import dataclass
import math
import re
import time


def otc(value):
    if not isinstance(value, str):
        return None
    return value if re.fullmatch(r'[A-Za-z0-9#._-]{1,40}_otc', value, re.I) else None


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
        if len(data) >= 3 and otc(data[0]):
            yield data[0], data[1], data[2]
        else:
            for entry in data:
                yield from extract_ticks(entry, depth + 1)
    elif isinstance(data, dict):
        symbol = otc(data.get('asset', data.get('symbol')))
        if symbol:
            yield symbol, data.get('timestamp', data.get('time')), data.get('price', data.get('value'))
        for key in ('data', 'ticks'):
            if key in data:
                yield from extract_ticks(data[key], depth + 1)


def extract_catalog(value, depth=0):
    if depth > 10:
        return
    if isinstance(value, list):
        if len(value) >= 6 and otc(value[1]):
            yield value[1], value[14] if len(value) > 14 else None
        else:
            for row in value:
                yield from extract_catalog(row, depth+1)
    elif isinstance(value, dict):
        symbol = otc(value.get('asset', value.get('symbol')))
        if symbol:
            yield symbol, next((value[k] for k in ('tradable','isActive','enabled') if k in value), None)
        for key, row in value.items():
            if otc(key):
                yield key, next((row[k] for k in ('tradable','isActive','enabled') if k in row), None) if isinstance(row,dict) else None
            elif key in ('assets','data','result'):
                yield from extract_catalog(row, depth+1)


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
        self.early_boundary = None

    @staticmethod
    def direction(bars):
        # Last two same-colored candles are the end of the initial run, not
        # the start of a long run. A doji, bad data or missing minute breaks it.
        if len(bars) < 5:
            return None
        bars = list(bars)[-5:]
        if not all(b.good for b in bars) or any(b.minute-a.minute != 60 for a,b in zip(bars,bars[1:])):
            return None
        a,b,c,d,e = [x.color for x in bars]
        if a and (a,b,c,d,e) == (a,a,-a,a,-a):
            return 'BUY' if a == 1 else 'SELL'
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
    def __init__(self, clock=time.time):
        self.clock = clock
        self.series = {}
        self.catalog = {}
        self.verified = set()
        self.entries = deque(maxlen=128)
        self.enabled = False
        self.early = False
        self.discard = None

    def clear(self):
        self.series.clear(); self.catalog.clear(); self.entries.clear(); self.verified.clear()

    def feed(self, event):
        if not isinstance(event,list) or len(event)<2:
            return
        name,data = event[:2]
        if name in ('updateAssets','assets','listAssets','updateAsset'):
            for asset, enabled in extract_catalog(data):
                if len(self.catalog) < 4096 or asset in self.catalog:
                    self.catalog[asset] = str(enabled).lower() not in ('0','false','0.0')
        if name != 'updateStream':
            return
        now = self.clock()
        for asset, raw_ts, raw_price in extract_ticks(data):
            ts, price = num(raw_ts), num(raw_price)
            if ts is None or price is None or price <= 0:
                continue
            if ts > 1e11:
                ts /= 1000
            if ts < now-3 or ts > now+0.5 or not self.catalog.get(asset, True):
                continue
            if asset not in self.series and len(self.series) >= 4096:
                continue
            self.verified.add(asset)
            series = self.series.setdefault(asset, Series())
            if series.bar and ts <= series.bar.last:
                continue
            rolled = series.tick(ts, price)
            bar = series.bar
            if not self.enabled or self.discard == asset:
                continue
            if rolled:
                direction = series.direction(series.closed)
                if direction and series.early_boundary != bar.minute and now <= bar.minute+3:
                    self.entries.append(Entry(asset,direction,bar.minute,bar.minute+3,False))
            elif self.early:
                end = bar.minute+60
                if end-1 <= ts < end and end-1 <= now < end and series.early_boundary != end:
                    direction = series.direction([*series.closed,bar])
                    if direction == 'SELL':
                        series.early_boundary = end
                        self.entries.append(Entry(asset,direction,end,end,True))

    def forming(self):
        """Best current candidate after at least one alternating closed bar."""
        now = self.clock()
        candidates = []
        for asset, series in self.series.items():
            if asset == self.discard or not self.catalog.get(asset, True) or not series.bar or now-series.bar.last > 10:
                continue
            closed = list(series.closed)
            for size in (4, 3):
                tail = closed[-size:]
                if len(tail) != size or not all(b.good for b in tail):
                    continue
                if tail[-1].minute+60 != series.bar.minute or any(b.minute-a.minute != 60 for a,b in zip(tail,tail[1:])):
                    continue
                color = tail[0].color
                expected = [color,color,-color,color][:size]
                if color and [b.color for b in tail] == expected:
                    candidates.append((size-1,series.bar.last,asset))
                    break
        return max(candidates, default=(0,0,None))

    def active_count(self):
        now = self.clock()
        return sum(s.bar is not None and now-s.bar.last <= 10 and self.catalog.get(a,True) for a,s in self.series.items())

    def coverage(self):
        now = self.clock()
        return sum(bool(s.closed) and s.closed[-1].good and now-s.closed[-1].minute < 123 and self.catalog.get(a,True) for a,s in self.series.items())
