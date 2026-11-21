"""Bounded whole-hour clock-skew calibration for LIVE updateStream only.
This measures relative clock alignment, not exchange-to-client latency. A feed
with constant delay cannot be distinguished from clock skew from ticks alone.
Never fit an arbitrary per-packet offset: delayed packets retain their delay.
"""
from collections import deque
import math
import statistics
import time


class TickClock:
    def __init__(self):
        self.reset()

    def reset(self):
        self.samples=deque(maxlen=20)
        self.offset=None
        self.reason='WAITING_FOR_LIVE_TICKS'
        self.last_raw_age=None
        self.last_adjusted_age=None
        self.anchor=None
        self.last_server=None

    def normalize(self, server, wall=None, mono=None):
        wall=time.time() if wall is None else wall
        mono=time.monotonic() if mono is None else mono
        if not all(isinstance(x,(float,int)) and not isinstance(x,bool) and math.isfinite(x)
                   for x in (server,wall,mono)):
            self.reason='INVALID_TIMESTAMP'; return None
        self.last_raw_age=wall-server
        if self.anchor:
            oldwall,oldmono=self.anchor
            if abs((wall-oldwall)-(mono-oldmono))>1:
                self.reset(); self.reason='LOCAL_CLOCK_CHANGED_RECALIBRATING'
                self.anchor=(wall,mono); return None
        self.anchor=(wall,mono)
        if self.last_server is not None and server<=self.last_server:
            self.reason='DUPLICATE_OR_REVERSED_TICK'; return None
        if self.offset is None:
            # In-sync clocks need no warmup or correction.
            if abs(wall-server)<=2:
                self.offset=0.0
            else:
                candidate=round((server-wall)/3600)*3600
                if not 1<=abs(candidate)/3600<=14 or abs(server-wall-candidate)>1:
                    self.reason='CLOCK_OFFSET_NOT_SAFE_WHOLE_HOUR'; return None
                if self.samples and candidate!=self.samples[-1][3]: self.samples.clear()
                if self.samples and server <= self.samples[-1][0]:
                    self.reason='DUPLICATE_OR_REVERSED_TICK'; return None
                self.samples.append((server,wall,mono,candidate))
                if len(self.samples)<8 or mono-self.samples[0][2]<1:
                    self.reason='CALIBRATING_LIVE_CLOCK'; return None
                start=self.samples[0]
                dt=mono-start[2]; ds=server-start[0]
                deviations=[s-w-c for s,w,m,c in self.samples]
                if not .7<=ds/dt<=1.3 or max(deviations)-min(deviations)>.6:
                    self.reason='CLOCK_CADENCE_UNSTABLE'; return None
                self.offset=float(candidate)
        adjusted=server-self.offset
        age=wall-adjusted
        self.last_adjusted_age=age
        if not -0.5<=age<=1:
            self.reason='STALE_OR_FUTURE_AFTER_CORRECTION'; return None
        # <=0.5s future jitter is mapped to receipt time, never a multi-second shift.
        self.last_server=server
        self.reason='READY'
        return min(adjusted,wall)

    def status(self):
        return dict(clock_status=self.reason,clock_offset_seconds=self.offset,
                    clock_calibration_samples=len(self.samples),
                    raw_tick_age_seconds=round(self.last_raw_age,3) if self.last_raw_age is not None else None,
                    adjusted_tick_age_seconds=round(self.last_adjusted_age,3) if self.last_adjusted_age is not None else None)
