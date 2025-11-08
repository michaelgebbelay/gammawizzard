#!/usr/bin/env python3
from __future__ import annotations
import pandas as pd

def compute_acceleration_10m(closes_10m: pd.Series, L: int = 26, ema_span: int = 9) -> pd.Series:
    """
    Acceleration on 10m bars (same math as your ThinkScript):
      avgVelocity_t = (1/L) * sum_{i=1..L} (p_t - p_{t-i})/i
      smv = EMA(avgVelocity, span=ema_span)
      Accel_t = 100 * (1/L) * sum_{j=1..L} (smv_t - smv_{t-j})/j
    """
    p = closes_10m.copy()
    av = pd.Series(0.0, index=p.index)
    for i in range(1, L + 1):
        av += (p - p.shift(i)) / i
    av /= L
    smv = av.ewm(span=ema_span, adjust=False).mean()
    aa = pd.Series(0.0, index=p.index)
    for j in range(1, L + 1):
        aa += (smv - smv.shift(j)) / j
    accel = 100.0 * (aa / L)
    return accel

def edge_on_last_bar(accel: pd.Series, thr: float = 3.0) -> int:
    """
    +1 if prior bar is the FIRST close > +thr (two bars ago <= +thr)
    -1 if prior bar is the FIRST close < -thr (two bars ago >= -thr)
     0 otherwise
    """
    if len(accel) < 3 or accel.iloc[-2] != accel.iloc[-2] or accel.iloc[-3] != accel.iloc[-3]:
        return 0
    a1 = float(accel.iloc[-2])
    a2 = float(accel.iloc[-3])
    if a1 >  thr and a2 <=  thr: return +1
    if a1 < -thr and a2 >= -thr: return -1
    return 0

def last_edge_anytime(accel: pd.Series, thr: float = 3.0) -> int:
    """Scan backward to find the most recent edge (+1/-1), used at 06:41 PT."""
    vals = accel.dropna().values
    n = len(vals)
    for i in range(n - 2, 1, -1):
        if vals[i] >  thr and vals[i-1] <=  thr: return +1
        if vals[i] < -thr and vals[i-1] >= -thr: return -1
    return 0
