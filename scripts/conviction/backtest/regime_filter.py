#!/usr/bin/env python3
"""
Regime filter for Path S strategy. Three gates:

  spy   — SPY > 200d SMA  (trend filter)
  vix   — VIX state machine with hysteresis (vol-regime filter)
  both  — AND of the two

Each gate has a per-day RISK_ON / RISK_OFF state. Regime transitions are
implemented as state machines so single-day spikes don't whipsaw entries.

Usage from replay.py:
    from regime_filter import build_regime_lookup
    regime = build_regime_lookup("vix")  # dict {date: "RISK_ON" | "RISK_OFF"}
    state_today = regime.get(today, "RISK_ON")  # default ON if no data
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd


HERE = Path(__file__).resolve().parent
DATA_DIR = HERE / "data"
STOCKS_PATH = DATA_DIR / "aggs_daily_adjusted.parquet"
VIX_PATH = DATA_DIR / "vix_daily.parquet"


# ---------------------------------------------------------------------------
# VIX ingest (yfinance fallback — Massive S3 us_indices/ requires higher tier)
# ---------------------------------------------------------------------------

def fetch_vix(*, start: str = "2018-01-01", end: str | None = None,
              force: bool = False) -> pd.DataFrame:
    """Pull ^VIX daily history from yfinance, save to parquet, return DataFrame.

    Always re-fetches if `force=True` or the cached parquet doesn't reach `end`.
    """
    if not force and VIX_PATH.exists():
        df = pd.read_parquet(VIX_PATH)
        if end is None or pd.to_datetime(df["date"].max()) >= pd.to_datetime(end) - pd.Timedelta(days=10):
            return df

    import yfinance as yf
    print(f"[regime] fetching ^VIX from yfinance...", file=sys.stderr)
    raw = yf.Ticker("^VIX").history(start=start, end=end or "2026-12-31")
    if raw.empty:
        raise SystemExit("[regime] yfinance returned no VIX data")
    raw = raw.reset_index()
    raw["date"] = pd.to_datetime(raw["Date"]).dt.tz_localize(None).dt.normalize()
    df = raw[["date", "Close"]].rename(columns={"Close": "close"}).reset_index(drop=True)
    df = df.dropna(subset=["close"]).sort_values("date").reset_index(drop=True)

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(VIX_PATH, index=False)
    print(f"[regime] wrote {VIX_PATH} ({len(df):,} rows, "
          f"{df['date'].min().date()} → {df['date'].max().date()})",
          file=sys.stderr)
    return df


# ---------------------------------------------------------------------------
# State machines
# ---------------------------------------------------------------------------

def compute_spy_regime(
    spy_df: pd.DataFrame,
    *,
    sma_window: int = 200,
    resume_confirm_days: int = 3,
) -> dict[pd.Timestamp, str]:
    """SPY > 200d SMA trend filter with re-entry confirmation.

    Pause: SPY_close < SMA200
    Resume: SPY_close > SMA200 for `resume_confirm_days` consecutive days
    """
    df = spy_df[["date", "close"]].sort_values("date").reset_index(drop=True).copy()
    df["sma"] = df["close"].rolling(sma_window, min_periods=sma_window).mean()

    state = "RISK_ON"
    above_streak = 0
    out: dict[pd.Timestamp, str] = {}
    for _, row in df.iterrows():
        if pd.isna(row["sma"]):
            out[row["date"]] = "RISK_ON"  # warmup → default ON
            continue
        is_above = row["close"] > row["sma"]
        if state == "RISK_ON":
            if not is_above:
                state = "RISK_OFF"
                above_streak = 0
        else:  # RISK_OFF
            if is_above:
                above_streak += 1
                if above_streak >= resume_confirm_days:
                    state = "RISK_ON"
                    above_streak = 0
            else:
                above_streak = 0
        out[row["date"]] = state
    return out


def compute_vix_regime(
    vix_df: pd.DataFrame,
    *,
    pause_level: float = 25.0,
    resume_level: float = 20.0,
    resume_confirm_days: int = 3,
) -> dict[pd.Timestamp, str]:
    """VIX state machine with hysteresis.

    Pause: VIX > pause_level AND 5d_EMA > 20d_EMA
    Resume: VIX < resume_level AND 5d_EMA < 20d_EMA  for `resume_confirm_days` days
    """
    df = vix_df[["date", "close"]].sort_values("date").reset_index(drop=True).copy()
    df["ema5"] = df["close"].ewm(span=5, adjust=False).mean()
    df["ema20"] = df["close"].ewm(span=20, adjust=False).mean()

    state = "RISK_ON"
    resume_streak = 0
    out: dict[pd.Timestamp, str] = {}
    for _, row in df.iterrows():
        v = row["close"]
        e5 = row["ema5"]
        e20 = row["ema20"]
        pause_cond = v > pause_level and e5 > e20
        resume_cond = v < resume_level and e5 < e20
        if state == "RISK_ON":
            if pause_cond:
                state = "RISK_OFF"
                resume_streak = 0
        else:  # RISK_OFF
            if resume_cond:
                resume_streak += 1
                if resume_streak >= resume_confirm_days:
                    state = "RISK_ON"
                    resume_streak = 0
            else:
                resume_streak = 0
        out[row["date"]] = state
    return out


def compute_combined_regime(
    spy_df: pd.DataFrame,
    vix_df: pd.DataFrame,
) -> dict[pd.Timestamp, str]:
    """RISK_ON only if BOTH SPY and VIX gates are RISK_ON for that date."""
    spy_states = compute_spy_regime(spy_df)
    vix_states = compute_vix_regime(vix_df)
    all_dates = sorted(set(spy_states.keys()) | set(vix_states.keys()))
    out: dict[pd.Timestamp, str] = {}
    for d in all_dates:
        s_spy = spy_states.get(d, "RISK_ON")
        s_vix = vix_states.get(d, "RISK_ON")
        out[d] = "RISK_ON" if (s_spy == "RISK_ON" and s_vix == "RISK_ON") else "RISK_OFF"
    return out


# ---------------------------------------------------------------------------
# Public loader
# ---------------------------------------------------------------------------

def build_regime_lookup(gate: str) -> dict[pd.Timestamp, str]:
    """Returns {date: RISK_ON | RISK_OFF}. `gate` ∈ {spy, vix, both, none}.

    For gate='none' returns an empty dict — callers should treat missing keys
    as RISK_ON.
    """
    if gate == "none":
        return {}
    if not STOCKS_PATH.exists():
        raise SystemExit(f"missing {STOCKS_PATH}; can't build regime")

    if gate in ("spy", "both"):
        stocks = pd.read_parquet(STOCKS_PATH, columns=["ticker", "date", "close"])
        stocks["date"] = pd.to_datetime(stocks["date"]).dt.normalize()
        spy = stocks[stocks["ticker"] == "SPY"][["date", "close"]].copy()
        if spy.empty:
            raise SystemExit("[regime] no SPY rows in stocks parquet")
    else:
        spy = None

    if gate in ("vix", "both"):
        vix = fetch_vix()
        vix["date"] = pd.to_datetime(vix["date"]).dt.normalize()
    else:
        vix = None

    if gate == "spy":
        out = compute_spy_regime(spy)
    elif gate == "vix":
        out = compute_vix_regime(vix)
    elif gate == "both":
        out = compute_combined_regime(spy, vix)
    else:
        raise SystemExit(f"unknown gate: {gate}")

    n_off = sum(1 for s in out.values() if s == "RISK_OFF")
    print(f"[regime] gate={gate}: {len(out):,} days, {n_off:,} RISK_OFF "
          f"({n_off/max(len(out), 1):.1%})", file=sys.stderr)
    return out


# ---------------------------------------------------------------------------
# CLI for inspection
# ---------------------------------------------------------------------------

def _summarize(states: dict[pd.Timestamp, str], label: str) -> None:
    """Print regime stats + per-year breakdown."""
    if not states:
        print(f"{label}: empty (gate=none)")
        return
    df = pd.DataFrame([{"date": d, "state": s} for d, s in states.items()])
    df["date"] = pd.to_datetime(df["date"])
    df["year"] = df["date"].dt.year
    print(f"\n=== {label} ===")
    print(f"window: {df['date'].min().date()} → {df['date'].max().date()}")
    n_off = (df["state"] == "RISK_OFF").sum()
    print(f"  total: {len(df):,} days, RISK_OFF: {n_off:,} ({n_off/len(df):.1%})")
    print(f"  per-year RISK_OFF days:")
    for yr, sub in df.groupby("year"):
        off = (sub["state"] == "RISK_OFF").sum()
        print(f"    {yr}: {off:>4,}/{len(sub):>4,}  ({off/len(sub):.1%})")

    # Identify regime transitions
    df["prev_state"] = df["state"].shift(1)
    transitions = df[(df["state"] != df["prev_state"]) & df["prev_state"].notna()]
    print(f"  transitions: {len(transitions)}")
    if len(transitions) > 0:
        print(f"  first 10 transitions:")
        for _, row in transitions.head(10).iterrows():
            print(f"    {row['date'].date()}: {row['prev_state']} → {row['state']}")


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--gate", default="all", choices=["spy", "vix", "both", "all"])
    args = ap.parse_args()
    if args.gate == "all":
        for g in ("spy", "vix", "both"):
            _summarize(build_regime_lookup(g), f"gate={g}")
    else:
        _summarize(build_regime_lookup(args.gate), f"gate={args.gate}")
