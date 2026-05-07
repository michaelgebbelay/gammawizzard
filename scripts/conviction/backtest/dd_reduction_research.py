#!/usr/bin/env python3
"""
DD-reduction research sweep — separate from the locked C1-HYST.

Goal: figure out which exit-side / overlay tactics actually move the
max-drawdown needle without giving up too much return. The C1-HYST entry
gate stays fixed (load-bearing per the attribution test); only the
EXIT/SIZING side is varied.

This is RESEARCH ONLY. None of these variants are production-eligible
without going through the locked spec's replacement bar.

Asset axis: TQQQ (3x QQQ), UPRO (3x SPX), 50/50 blend.
Exit tactics axis:
  B0   baseline (C1-HYST exactly as live)
  EX1  stricter exit (no hold zone — exit when score < 3)
  EX2  trailing stop -10% from in-position peak
  EX3  trailing stop -15% from in-position peak
  EX4  half-position de-risk: 100% at score==3, 50% at score==2, 0% at score<=1
  EX5  hard stop -10% from entry on each risk-on episode
  EX6  hard stop -15% from entry
  EX7  vol-targeted sizing: scale exposure by 25%/(realized 21d vol annualized),
       capped at 100%
Asset alternates (apply on top of B0):
  AS-UPRO   same signal, hold UPRO (3x SPX) instead of TQQQ
  AS-BLEND  50/50 TQQQ+UPRO when risk-on
Volatility / VIX overlays (apply on top of B0):
  OV1  cut to 50% risk-on when VIX > 25
  OV2  cut to 0% when VIX > 30 (hard veto)
  OV3  cut to 50% if QQQ has a -3% single-day move (next session)

Every variant emits a daily weight vector across {TQQQ, UPRO, BIL}
that sums to 1.0. The simulator is a clean weighted-portfolio walker
with slippage on weight changes.

Output: ranked metrics table + markdown summary.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

WIN_START = pd.Timestamp("2011-01-03")
DATA_START = "2010-01-01"
DATA_END_DEFAULT = pd.Timestamp.today().strftime("%Y-%m-%d")
SLIPPAGE_BPS_DEFAULT = 10

ASSETS = ["TQQQ", "UPRO", "BIL"]
ALL_TICKERS = ["QQQ", "TQQQ", "UPRO", "SPY", "BIL", "^VIX"]


# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------

def load_data(end_date: str) -> dict:
    import yfinance as yf
    print(f"[data] yfinance {DATA_START}..{end_date}", file=sys.stderr)
    out = {}
    for t in ALL_TICKERS:
        s = yf.Ticker(t).history(start=DATA_START, end=end_date, auto_adjust=True)
        if s.empty:
            raise SystemExit(f"no data for {t}")
        s.index = pd.to_datetime(s.index).tz_localize(None).normalize()
        s = s[s.index < pd.Timestamp(end_date)]
        out[t] = s[["Open", "High", "Low", "Close"]].rename(
            columns={"Open": "open", "High": "high", "Low": "low", "Close": "close"})
        print(f"[data] {t}: {len(out[t])} rows {out[t].index[0].date()}->{out[t].index[-1].date()}",
              file=sys.stderr)
    return out


# ---------------------------------------------------------------------------
# C1-HYST signal (unchanged, locked)
# ---------------------------------------------------------------------------

def compute_signal(qqq_close: pd.Series) -> pd.DataFrame:
    df = pd.DataFrame({"close": qqq_close})
    df["sma50"]  = df["close"].rolling(50, min_periods=50).mean()
    df["sma150"] = df["close"].rolling(150, min_periods=150).mean()
    df["sma200"] = df["close"].rolling(200, min_periods=200).mean()
    df["ret63"]  = df["close"].pct_change(63)
    df["A"] = (df["close"] > df["sma150"]).astype(int)
    df["B"] = (df["sma50"] > df["sma200"]).astype(int)
    df["C"] = (df["ret63"] > 0).astype(int)
    df["score"] = df["A"] + df["B"] + df["C"]
    # state machine with hold zone
    states = []
    s = None
    for sc in df["score"].values:
        if pd.isna(sc):
            states.append(None); continue
        sc = int(sc)
        if s is None:
            s = "RISKON" if sc == 3 else "BIL"
        else:
            if sc == 3:
                s = "RISKON"
            elif sc <= 1:
                s = "BIL"
        states.append(s)
    df["state"] = states
    return df


# ---------------------------------------------------------------------------
# Variant generators — each returns a DataFrame of weights {TQQQ, UPRO, BIL}
# ---------------------------------------------------------------------------

def _empty_weights(idx: pd.DatetimeIndex) -> pd.DataFrame:
    return pd.DataFrame(0.0, index=idx, columns=ASSETS)


def variant_B0(sig: pd.DataFrame, dates: pd.DatetimeIndex,
               vix: pd.Series, qqq_close: pd.Series) -> pd.DataFrame:
    """Baseline: TQQQ when state==RISKON else BIL. Lagged by 1 day (signal at
    close[t-1] determines today's holding)."""
    w = _empty_weights(dates)
    state_lag = sig["state"].shift(1)
    w.loc[state_lag == "RISKON", "TQQQ"] = 1.0
    w.loc[state_lag != "RISKON", "BIL"] = 1.0
    w.iloc[0] = w.iloc[0]   # noop
    return w


def variant_EX1_strict(sig: pd.DataFrame, dates, vix, qqq_close) -> pd.DataFrame:
    """No hold zone — TQQQ only when score==3, else BIL. (Drop the hysteresis.)"""
    w = _empty_weights(dates)
    score_lag = sig["score"].shift(1)
    w.loc[score_lag == 3, "TQQQ"] = 1.0
    w.loc[score_lag != 3, "BIL"] = 1.0
    return w


def _trailing_stop(sig: pd.DataFrame, dates, qqq_close, stop_pct):
    """Variants EX2/EX3: while in TQQQ, exit if QQQ falls stop_pct from
    in-position rolling peak. Re-entry follows normal C1-HYST logic."""
    w = _empty_weights(dates)
    state_lag = sig["state"].shift(1).reindex(dates)
    qqq_in = qqq_close.reindex(dates).ffill()
    holding_tqqq = False
    in_position_peak = None
    overrode = False  # flag: we manually exited even though state still RISKON
    for d in dates:
        st = state_lag.loc[d]
        if st == "RISKON":
            if not holding_tqqq and not overrode:
                holding_tqqq = True
                in_position_peak = qqq_in.loc[d]
            elif holding_tqqq:
                in_position_peak = max(in_position_peak, qqq_in.loc[d])
                if (qqq_in.loc[d] / in_position_peak - 1) <= stop_pct:
                    holding_tqqq = False
                    overrode = True
        else:  # BIL state
            holding_tqqq = False
            in_position_peak = None
            overrode = False
        w.loc[d, "TQQQ" if holding_tqqq else "BIL"] = 1.0
    return w


def variant_EX2_ts10(sig, dates, vix, qqq_close):
    return _trailing_stop(sig, dates, qqq_close, -0.10)


def variant_EX3_ts15(sig, dates, vix, qqq_close):
    return _trailing_stop(sig, dates, qqq_close, -0.15)


def variant_EX4_half(sig, dates, vix, qqq_close):
    """100% TQQQ at score==3, 50% TQQQ + 50% BIL at score==2, 0% at score<=1.
    Uses raw score not state — this is a 3-stage mapping."""
    w = _empty_weights(dates)
    score_lag = sig["score"].shift(1).reindex(dates)
    full = score_lag == 3
    half = score_lag == 2
    cash = score_lag <= 1
    w.loc[full, "TQQQ"] = 1.0
    w.loc[half, "TQQQ"] = 0.5
    w.loc[half, "BIL"]  = 0.5
    w.loc[cash, "BIL"]  = 1.0
    return w


def _hard_stop(sig, dates, qqq_close, stop_pct):
    """EX5/EX6: while in TQQQ, exit if QQQ down stop_pct from THIS episode's
    entry price. Re-entry follows normal C1-HYST."""
    w = _empty_weights(dates)
    state_lag = sig["state"].shift(1).reindex(dates)
    qqq_in = qqq_close.reindex(dates).ffill()
    holding_tqqq = False
    entry_price = None
    overrode = False
    for d in dates:
        st = state_lag.loc[d]
        if st == "RISKON":
            if not holding_tqqq and not overrode:
                holding_tqqq = True
                entry_price = qqq_in.loc[d]
            elif holding_tqqq:
                if (qqq_in.loc[d] / entry_price - 1) <= stop_pct:
                    holding_tqqq = False
                    overrode = True
        else:
            holding_tqqq = False
            entry_price = None
            overrode = False
        w.loc[d, "TQQQ" if holding_tqqq else "BIL"] = 1.0
    return w


def variant_EX5_hardstop10(sig, dates, vix, qqq_close):
    return _hard_stop(sig, dates, qqq_close, -0.10)


def variant_EX6_hardstop15(sig, dates, vix, qqq_close):
    return _hard_stop(sig, dates, qqq_close, -0.15)


def variant_EX7_voltarget(sig, dates, vix, qqq_close):
    """Vol-targeted sizing: target 25% annualized vol, scale TQQQ exposure
    inversely to QQQ realized vol. Cap at 100% TQQQ. Used when state==RISKON,
    else 100% BIL."""
    w = _empty_weights(dates)
    state_lag = sig["state"].shift(1).reindex(dates)
    qqq_ret = qqq_close.pct_change()
    vol21 = qqq_ret.rolling(21).std() * np.sqrt(252)
    vol21 = vol21.reindex(dates).ffill()
    target_vol = 0.25
    # When risk-on, weight = min(1, target_vol / 3 / vol21)
    # divide by 3 because TQQQ's effective vol is ~3x QQQ vol
    raw_w = (target_vol / 3.0) / vol21.replace(0, np.nan)
    raw_w = raw_w.clip(0, 1).fillna(0)
    on = state_lag == "RISKON"
    w.loc[on, "TQQQ"] = raw_w.loc[on]
    w.loc[on, "BIL"]  = 1.0 - raw_w.loc[on]
    w.loc[~on, "BIL"] = 1.0
    return w


def variant_AS_UPRO(sig, dates, vix, qqq_close):
    """C1-HYST signal, but hold UPRO when risk-on (not TQQQ)."""
    w = _empty_weights(dates)
    state_lag = sig["state"].shift(1).reindex(dates)
    w.loc[state_lag == "RISKON", "UPRO"] = 1.0
    w.loc[state_lag != "RISKON", "BIL"]  = 1.0
    return w


def variant_AS_BLEND(sig, dates, vix, qqq_close):
    """C1-HYST signal, 50/50 TQQQ+UPRO when risk-on."""
    w = _empty_weights(dates)
    state_lag = sig["state"].shift(1).reindex(dates)
    on = state_lag == "RISKON"
    w.loc[on, "TQQQ"] = 0.5
    w.loc[on, "UPRO"] = 0.5
    w.loc[~on, "BIL"] = 1.0
    return w


def variant_OV1_vix25(sig, dates, vix, qqq_close):
    """Cut to 50% TQQQ when VIX > 25, on top of B0."""
    w = variant_B0(sig, dates, vix, qqq_close)
    vix_lag = vix.shift(1).reindex(dates).ffill()
    cut = (vix_lag > 25.0) & (w["TQQQ"] > 0)
    w.loc[cut, "TQQQ"] = 0.5
    w.loc[cut, "BIL"]  = 0.5
    return w


def variant_OV2_vix30(sig, dates, vix, qqq_close):
    """Hard veto: 0% TQQQ when VIX > 30 (force BIL), on top of B0."""
    w = variant_B0(sig, dates, vix, qqq_close)
    vix_lag = vix.shift(1).reindex(dates).ffill()
    veto = (vix_lag > 30.0) & (w["TQQQ"] > 0)
    w.loc[veto, "TQQQ"] = 0.0
    w.loc[veto, "BIL"]  = 1.0
    return w


def variant_OV3_dailygap3(sig, dates, vix, qqq_close):
    """Cut to 50% TQQQ next session if QQQ had a -3% single-day close, on top of B0."""
    w = variant_B0(sig, dates, vix, qqq_close)
    qqq_dret = qqq_close.pct_change().reindex(dates).ffill()
    flag = (qqq_dret.shift(1) <= -0.03)
    cut = flag & (w["TQQQ"] > 0)
    w.loc[cut, "TQQQ"] = 0.5
    w.loc[cut, "BIL"]  = 0.5
    return w


VARIANTS = [
    ("B0",            "baseline (C1-HYST live)",                 variant_B0),
    ("EX1_strict",    "drop hold zone (exit when score < 3)",    variant_EX1_strict),
    ("EX2_ts10",      "trailing stop -10% from in-position peak",variant_EX2_ts10),
    ("EX3_ts15",      "trailing stop -15% from in-position peak",variant_EX3_ts15),
    ("EX4_half",      "half-position at score==2",                variant_EX4_half),
    ("EX5_hardstop10","hard stop -10% from episode entry",        variant_EX5_hardstop10),
    ("EX6_hardstop15","hard stop -15% from episode entry",        variant_EX6_hardstop15),
    ("EX7_voltarget", "vol-targeted sizing (25% annual)",         variant_EX7_voltarget),
    ("AS_UPRO",       "swap to UPRO (3x SPX) when risk-on",       variant_AS_UPRO),
    ("AS_BLEND",      "50/50 TQQQ+UPRO when risk-on",              variant_AS_BLEND),
    ("OV1_vix25",     "cut to 50% if VIX > 25",                    variant_OV1_vix25),
    ("OV2_vix30",     "veto to 0% if VIX > 30",                    variant_OV2_vix30),
    ("OV3_gap3",      "cut to 50% next session after -3% QQQ day", variant_OV3_dailygap3),
]


# ---------------------------------------------------------------------------
# Weighted-portfolio simulator
# ---------------------------------------------------------------------------

def simulate_weights(weights: pd.DataFrame, returns: pd.DataFrame,
                     slippage_bps: int) -> pd.Series:
    aligned_w = weights.fillna(0.0)
    aligned_r = returns.reindex(aligned_w.index).fillna(0.0)
    daily_strat = (aligned_w * aligned_r).sum(axis=1)
    weight_change = aligned_w.diff().abs().sum(axis=1).fillna(0.0)
    cost = weight_change * (slippage_bps / 10000.0)
    return (1.0 + daily_strat - cost).cumprod()


def metrics(eq: pd.Series, daily_returns: pd.Series, name: str, label: str) -> dict:
    eq = eq.dropna()
    if len(eq) < 2:
        return {"variant": name, "label": label}
    end_mult = float(eq.iloc[-1])
    yrs = (eq.index[-1] - eq.index[0]).days / 365.25
    cagr = end_mult ** (1.0 / yrs) - 1.0 if yrs > 0 else float("nan")
    rolling_max = eq.cummax()
    dd = (eq / rolling_max - 1.0)
    max_dd = float(dd.min())
    # days underwater = pct of days where dd <= -10%
    days_under_10 = float((dd <= -0.10).mean()) * 100
    days_under_25 = float((dd <= -0.25).mean()) * 100
    dr = daily_returns.dropna()
    sharpe = float(dr.mean() / dr.std() * np.sqrt(252)) if dr.std() > 0 else None
    downside = dr[dr < 0]
    sortino = float(dr.mean() / downside.std() * np.sqrt(252)) if len(downside) > 1 and downside.std() > 0 else None
    calmar = float(cagr / abs(max_dd)) if max_dd < 0 else None
    return {
        "variant": name, "label": label,
        "total_pct": round((end_mult - 1) * 100, 2),
        "cagr_pct": round(cagr * 100, 2),
        "max_dd_pct": round(max_dd * 100, 2),
        "calmar": round(calmar, 2) if calmar else None,
        "sharpe": round(sharpe, 2) if sharpe else None,
        "sortino": round(sortino, 2) if sortino else None,
        "pct_days_dd_le_10": round(days_under_10, 1),
        "pct_days_dd_le_25": round(days_under_25, 1),
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--out-dir", default="out")
    p.add_argument("--end", default=DATA_END_DEFAULT)
    p.add_argument("--slippage-bps", type=int, default=SLIPPAGE_BPS_DEFAULT)
    args = p.parse_args()
    out = Path(args.out_dir); out.mkdir(parents=True, exist_ok=True)

    bars = load_data(args.end)

    # Build daily returns for each asset (close-to-close)
    returns = pd.DataFrame(index=bars["TQQQ"].index)
    for asset in ASSETS:
        returns[asset] = bars[asset]["close"].pct_change()

    # Signal on QQQ
    sig = compute_signal(bars["QQQ"]["close"])

    # Restrict to L window starting 2011-01-03 with all indicators valid
    sig_clean = sig.dropna(subset=["sma200", "sma150", "sma50", "ret63"])
    start = max(WIN_START, sig_clean.index[0])
    common_idx = returns.index.intersection(sig.index)
    common_idx = common_idx[common_idx >= start]
    common_idx = common_idx[common_idx <= sig.index[-1]]

    sig_aligned = sig.reindex(common_idx)
    returns_aligned = returns.reindex(common_idx)
    qqq_close = bars["QQQ"]["close"].reindex(common_idx).ffill()
    vix = bars["^VIX"]["close"].reindex(common_idx).ffill()

    print(f"[run] window: {common_idx[0].date()} .. {common_idx[-1].date()}  "
          f"({len(common_idx)} sessions)", file=sys.stderr)

    rows = []
    eq_curves = {}
    for name, label, fn in VARIANTS:
        weights = fn(sig_aligned, common_idx, vix, qqq_close)
        weights = weights.reindex(common_idx).fillna(0.0)
        # ensure rows sum to 1 (pure-cash days = all BIL)
        s = weights.sum(axis=1)
        if (s == 0).any():
            weights.loc[s == 0, "BIL"] = 1.0
        eq = simulate_weights(weights, returns_aligned, args.slippage_bps)
        dr = eq.pct_change()
        m = metrics(eq, dr, name, label)
        rows.append(m)
        eq_curves[name] = eq
        print(f"[done] {name:<16} total={m['total_pct']:>+9.2f}%  "
              f"DD={m['max_dd_pct']:>+7.2f}%  Calmar={m['calmar']}  "
              f"underwater_-10%={m['pct_days_dd_le_10']:>5.1f}%",
              file=sys.stderr)

    df = pd.DataFrame(rows)
    df.to_csv(out / "metrics.csv", index=False)

    # equity curves CSV (long format for plotting)
    ec_rows = []
    for name, eq in eq_curves.items():
        for d, v in eq.items():
            ec_rows.append({"variant": name, "date": d, "equity": v})
    pd.DataFrame(ec_rows).to_csv(out / "equity_curves.csv", index=False)

    # Markdown summary, ranked by Calmar then by max_dd
    df_sorted = df.sort_values(by=["calmar", "max_dd_pct"], ascending=[False, False])
    lines = []
    lines.append("=" * 100)
    lines.append("DD-reduction research sweep (research-only, NOT for production)")
    lines.append("=" * 100)
    lines.append(f"Window: {common_idx[0].date()} .. {common_idx[-1].date()}  "
                 f"({len(common_idx)} sessions)  slippage={args.slippage_bps}bp")
    lines.append("Signal: C1-HYST (locked) on QQQ, unchanged across variants.")
    lines.append("Asset/exit/overlay variations applied after signal.")
    lines.append("")
    lines.append(f"{'variant':<16} {'label':<46} "
                 f"{'total':>10} {'CAGR':>8} {'maxDD':>8} {'Calmar':>7} "
                 f"{'Sharpe':>7} {'%days≤−10':>10} {'%days≤−25':>10}")
    lines.append("-" * 130)
    for _, r in df_sorted.iterrows():
        lines.append(f"{r['variant']:<16} {r['label']:<46} "
                     f"{r['total_pct']:>+9.2f}% {r['cagr_pct']:>+7.2f}% "
                     f"{r['max_dd_pct']:>+7.2f}% {(r['calmar'] or 0):>7.2f} "
                     f"{(r['sharpe'] or 0):>7.2f} "
                     f"{r['pct_days_dd_le_10']:>9.1f}% {r['pct_days_dd_le_25']:>9.1f}%")
    lines.append("")

    # delta vs B0
    base = df[df["variant"] == "B0"].iloc[0]
    lines.append("Delta vs baseline (B0):")
    lines.append(f"{'variant':<16} {'Δ_total_pp':>12} {'Δ_DD_pp':>10} {'Δ_Calmar':>10} {'Δ_underwater':>15}")
    for _, r in df_sorted.iterrows():
        if r["variant"] == "B0":
            continue
        lines.append(f"{r['variant']:<16} "
                     f"{r['total_pct']-base['total_pct']:>+11.2f}pp "
                     f"{r['max_dd_pct']-base['max_dd_pct']:>+9.2f}pp "
                     f"{(r['calmar'] or 0)-(base['calmar'] or 0):>+9.2f} "
                     f"{r['pct_days_dd_le_10']-base['pct_days_dd_le_10']:>+13.2f}pp")
    lines.append("")

    # Verdict heuristic
    lines.append("Heuristic verdict:")
    lines.append("  * Improves Calmar AND reduces max DD: candidate worth deeper study")
    lines.append("  * Improves Calmar but reduces total return >40%: gives up too much")
    lines.append("  * Worsens both DD and total: rejected")

    (out / "summary.txt").write_text("\n".join(lines))
    print("\n".join(lines))


if __name__ == "__main__":
    main()
