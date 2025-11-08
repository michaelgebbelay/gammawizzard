#!/usr/bin/env python3
from __future__ import annotations
import datetime as dt, pytz

PT = pytz.timezone("America/Los_Angeles")

def now_pt() -> dt.datetime:
    return dt.datetime.now(PT)

def is_rth(ts: dt.datetime) -> bool:
    # 06:30 <= t < 13:00 PT
    h, m = ts.hour, ts.minute
    return (h > 6 or (h == 6 and m >= 30)) and h < 13

def is_first_tick(ts: dt.datetime, hm="06:41") -> bool:
    hh, mm = map(int, hm.split(":"))
    return ts.hour == hh and ts.minute == mm

def is_10m_gate(ts: dt.datetime) -> bool:
    # :41, :51, :01, :11, :21, :31
    return (ts.minute % 10) == 1

def is_ignored(ts: dt.datetime, last_minutes=30) -> bool:
    close = ts.replace(hour=13, minute=0, second=0, microsecond=0)
    return ts >= (close - dt.timedelta(minutes=last_minutes))
