#!/usr/bin/env python3
"""
DD-reduction robustness check — narrow parameter sweep + sub-window
decomposition.

Pre-test hypothesis: EX3 (trailing stop -15%) and EX7 (vol-target 25%)
looked like winners on 2011-2026. The question this script answers:
do they keep helping across multiple regimes, or did they get lucky on
one crisis shape (e.g., 2022)?

Variants tested (locked C1-HYST signal, exit-side overlays only):
  Baseline:
    B0       C1-HYST live spec
  Trailing stop band:
    TS_8,  TS_10, TS_12, TS_15, TS_20      (% drop from in-position QQQ peak)
  Vol target band:
    VT_20, VT_25, VT_30, VT_35             (target annualized vol, scale TQQQ)
  Combinations (best + neighbors):
    TS_15 + VT_25
    TS_12 + VT_25
    TS_10 + VT_30

Sub-windows for regime check:
  W1  2011-01-03 .. 2017-12-31    (post-GFC bull, low vol)
  W2  2018-01-01 .. 2021-12-31    (vol regime + COVID + recovery)
  W3  2022-01-01 .. 2026-05-06    (rate-hike bear + AI rally)
  WL  full window 2011-01-03 .. 2026-05-06

A variant is "ROBUST" if it improves Calmar vs B0 in ≥2 of {W1, W2, W3}
AND in WL. Otherwise "fragile" or "regime-dependent".

Output:
  metrics_by_window.csv  one row per (variant, window)
  robustness.csv         per-variant robustness verdict
  summary.txt            ranked tables + robustness flag
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

WIN_START = pd.Timestamp("2011-01-03")
DATA_START = "2010-01-01"
DATA_END = pd.Timestamp.today().strftime("%Y-%m-%d")
SLIPPAGE_BPS = 10
ASSETS = ["TQQQ", "BIL"]

WINDOWS = {
    "W1": ("2011-01-03", "2017-12-31"),
    "W2": ("2018-01-01", "2021-12-31"),
    "W3": ("2022-01-01", DATA_END),
    "WL": ("2011-01-03", DATA_END),
}


# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------

def load_data():
    import yfinance as yf
    out = {}
    for t in ["QQQ", "TQQQ", "BIL"]:
        s = yf.Ticker(t).history(start=DATA_START, end=DATA_END, auto_adjust=True)
        s.index = pd.to_datetime(s.index).tz_localize(None).normalize()
        s = s[s.index < pd.Timestamp(DATA_END)]
        out[t] = s[["Open", "Close"]].rename(columns={"Open": "open", "Close": "close"})
    return out


def compute_signal(qqq_close: pd.Series) -> pd.DataFrame:
    df = pd.DataFrame({"close": qqq_close})
    df["sma50"]  = df["close"].rolling(50, min_periods=50).mean()
    df["sma150"] = df["close"].rolling(150, min_periods=150).mean()
    df["sma200"] = df["close"].rolling(200, min_periods=200).mean()
    df["ret63"]  = df["close"].pct_change(63)
    df["score"] = ((df["close"] > df["sma150"]).astype(int)
                 + (df["sma50"] > df["sma200"]).astype(int)
                 + (df["ret63"] > 0).astype(int))
    states, s = [], None
    for sc in df["score"].values:
        if pd.isna(sc):
            states.append(None); continue
        sc = int(sc)
        if s is None:
            s = "RISKON" if sc == 3 else "BIL"
        else:
            if sc == 3: s = "RISKON"
            elif sc <= 1: s = "BIL"
        states.append(s)
    df["state"] = states
    return df


# ---------------------------------------------------------------------------
# Parameterised variant builder
# ---------------------------------------------------------------------------

def make_weights(sig: pd.DataFrame, dates: pd.DatetimeIndex, qqq_close: pd.Series,
                 stop_pct: float | None, vol_target: float | None) -> pd.DataFrame:
    """Build daily weights {TQQQ, BIL} for a parameterised variant.

    stop_pct:    None or negative number (e.g. -0.15). Trailing stop on
                 QQQ from in-position peak.
    vol_target:  None or float (e.g. 0.25). Scales TQQQ exposure to target
                 this annualized vol via 21d realized QQQ vol; capped at 1.0.
                 Daily computed; weight = min(1, vt / (3 × vol21)).
    """
    w = pd.DataFrame(0.0, index=dates, columns=ASSETS)
    state_lag = sig["state"].shift(1).reindex(dates)
    qqq_in = qqq_close.reindex(dates).ffill()

    # vol-target multiplier (applied to TQQQ weight when in RISKON)
    if vol_target is not None:
        qqq_ret = qqq_close.pct_change()
        vol21 = qqq_ret.rolling(21).std() * np.sqrt(252)
        vol21 = vol21.reindex(dates).ffill()
        vt_mult = (vol_target / 3.0) / vol21.replace(0, np.nan)
        vt_mult = vt_mult.clip(0, 1).fillna(0)
    else:
        vt_mult = pd.Series(1.0, index=dates)

    holding_tqqq = False
    peak = None
    overrode = False  # trailing stop fired, blocks re-entry until next BIL state

    for d in dates:
        st = state_lag.loc[d]
        if st == "RISKON":
            if not holding_tqqq and not overrode:
                holding_tqqq = True
                peak = qqq_in.loc[d]
            elif holding_tqqq:
                peak = max(peak, qqq_in.loc[d])
                if stop_pct is not None and (qqq_in.loc[d] / peak - 1) <= stop_pct:
                    holding_tqqq = False
                    overrode = True
        else:  # BIL state — reset everything
            holding_tqqq = False
            peak = None
            overrode = False

        if holding_tqqq:
            tqqq_w = float(vt_mult.loc[d])
            w.loc[d, "TQQQ"] = tqqq_w
            w.loc[d, "BIL"]  = 1.0 - tqqq_w
        else:
            w.loc[d, "BIL"] = 1.0
    return w


VARIANTS = [
    ("B0",        None, None),
    ("TS_8",      -0.08, None),
    ("TS_10",     -0.10, None),
    ("TS_12",     -0.12, None),
    ("TS_15",     -0.15, None),
    ("TS_20",     -0.20, None),
    ("VT_20",      None, 0.20),
    ("VT_25",      None, 0.25),
    ("VT_30",      None, 0.30),
    ("VT_35",      None, 0.35),
    ("TS_15+VT_25", -0.15, 0.25),
    ("TS_12+VT_25", -0.12, 0.25),
    ("TS_10+VT_30", -0.10, 0.30),
]


# ---------------------------------------------------------------------------
# Simulator (weight-based)
# ---------------------------------------------------------------------------

def simulate_weights(weights: pd.DataFrame, returns: pd.DataFrame,
                     slippage_bps: int) -> pd.Series:
    aligned_w = weights.fillna(0.0)
    aligned_r = returns.reindex(aligned_w.index).fillna(0.0)
    daily_strat = (aligned_w * aligned_r).sum(axis=1)
    weight_change = aligned_w.diff().abs().sum(axis=1).fillna(0.0)
    cost = weight_change * (slippage_bps / 10000.0)
    return (1.0 + daily_strat - cost).cumprod()


def metrics_for(eq: pd.Series, name: str, window: str) -> dict:
    eq = eq.dropna()
    if len(eq) < 5:
        return {"variant": name, "window": window}
    end_mult = float(eq.iloc[-1] / eq.iloc[0])
    yrs = (eq.index[-1] - eq.index[0]).days / 365.25
    cagr = end_mult ** (1.0 / yrs) - 1.0 if yrs > 0 else float("nan")
    rolling_max = eq.cummax()
    dd = (eq / rolling_max - 1.0)
    max_dd = float(dd.min())
    days_under_10 = float((dd <= -0.10).mean()) * 100
    days_under_25 = float((dd <= -0.25).mean()) * 100
    dr = eq.pct_change().dropna()
    sharpe = float(dr.mean() / dr.std() * np.sqrt(252)) if dr.std() > 0 else None
    downside = dr[dr < 0]
    sortino = float(dr.mean() / downside.std() * np.sqrt(252)) if len(downside) > 1 and downside.std() > 0 else None
    calmar = float(cagr / abs(max_dd)) if max_dd < 0 else None
    return {
        "variant": name, "window": window,
        "total_pct": round((end_mult - 1) * 100, 2),
        "cagr_pct":  round(cagr * 100, 2),
        "max_dd_pct": round(max_dd * 100, 2),
        "calmar": round(calmar, 3) if calmar else None,
        "sharpe": round(sharpe, 3) if sharpe else None,
        "sortino": round(sortino, 3) if sortino else None,
        "pct_days_le_10": round(days_under_10, 1),
        "pct_days_le_25": round(days_under_25, 1),
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--out-dir", default="out")
    args = p.parse_args()
    out = Path(args.out_dir); out.mkdir(parents=True, exist_ok=True)

    print("[load] yfinance", file=sys.stderr)
    bars = load_data()
    sig = compute_signal(bars["QQQ"]["close"])

    sig_clean = sig.dropna(subset=["sma200", "sma150", "sma50", "ret63"])
    common_start = max(WIN_START, sig_clean.index[0])
    common_end = bars["TQQQ"].index[-1]
    full_idx = bars["TQQQ"].index
    full_idx = full_idx[(full_idx >= common_start) & (full_idx <= common_end)]

    returns = pd.DataFrame(index=full_idx)
    for asset in ASSETS:
        returns[asset] = bars[asset]["close"].pct_change().reindex(full_idx)

    qqq_close = bars["QQQ"]["close"]

    # Compute weight curves once for each variant on the full index
    weight_curves = {}
    for name, stop_pct, vt in VARIANTS:
        w = make_weights(sig, full_idx, qqq_close, stop_pct, vt)
        # ensure rows sum to 1
        s = w.sum(axis=1)
        w.loc[s == 0, "BIL"] = 1.0
        weight_curves[name] = w
        print(f"[built] {name}", file=sys.stderr)

    # Evaluate each variant on each window separately (re-base equity to 1 at window start)
    rows = []
    for name in [v[0] for v in VARIANTS]:
        w = weight_curves[name]
        for w_label, (s, e) in WINDOWS.items():
            mask = (full_idx >= pd.Timestamp(s)) & (full_idx <= pd.Timestamp(e))
            sub_idx = full_idx[mask]
            if len(sub_idx) < 30:
                continue
            sub_w = w.reindex(sub_idx)
            sub_r = returns.reindex(sub_idx)
            eq = simulate_weights(sub_w, sub_r, SLIPPAGE_BPS)
            m = metrics_for(eq, name, w_label)
            rows.append(m)

    df = pd.DataFrame(rows)
    df.to_csv(out / "metrics_by_window.csv", index=False)

    # Robustness verdict per variant
    base_calmar = {w: df[(df["variant"]=="B0") & (df["window"]==w)]["calmar"].iloc[0]
                   for w in WINDOWS}
    base_dd     = {w: df[(df["variant"]=="B0") & (df["window"]==w)]["max_dd_pct"].iloc[0]
                   for w in WINDOWS}
    base_total  = {w: df[(df["variant"]=="B0") & (df["window"]==w)]["total_pct"].iloc[0]
                   for w in WINDOWS}

    rob_rows = []
    for name in [v[0] for v in VARIANTS]:
        if name == "B0": continue
        wins = {}
        for w in ("W1", "W2", "W3"):
            row = df[(df["variant"]==name) & (df["window"]==w)]
            if row.empty:
                wins[w] = None; continue
            v_calmar = row["calmar"].iloc[0]
            wins[w] = (v_calmar is not None and base_calmar[w] is not None
                       and v_calmar > base_calmar[w])
        wl_row = df[(df["variant"]==name) & (df["window"]=="WL")]
        wl_wins = (wl_row["calmar"].iloc[0] > base_calmar["WL"]) if not wl_row.empty else False
        n_wins = sum(1 for v in wins.values() if v)
        verdict = "ROBUST" if (wl_wins and n_wins >= 2) else \
                  "regime-dependent" if (wl_wins and n_wins == 1) else \
                  "fragile" if wl_wins else \
                  "rejected"
        rob_rows.append({
            "variant": name,
            "wl_calmar_beats_B0": wl_wins,
            "W1_calmar_beats_B0": wins["W1"],
            "W2_calmar_beats_B0": wins["W2"],
            "W3_calmar_beats_B0": wins["W3"],
            "n_subwindow_wins": n_wins,
            "verdict": verdict,
        })
    rob_df = pd.DataFrame(rob_rows)
    rob_df.to_csv(out / "robustness.csv", index=False)

    # ----- Markdown summary -----
    lines = []
    lines.append("=" * 100)
    lines.append("DD-reduction robustness check (research-only)")
    lines.append("=" * 100)
    lines.append(f"Window slices:")
    for w_label, (s, e) in WINDOWS.items():
        n = len(full_idx[(full_idx >= pd.Timestamp(s)) & (full_idx <= pd.Timestamp(e))])
        lines.append(f"  {w_label}: {s} .. {e}  ({n} sessions)")
    lines.append(f"Slippage = {SLIPPAGE_BPS}bp on every weight change.")
    lines.append("")

    # Tables per window
    for w_label in ("W1", "W2", "W3", "WL"):
        sub = df[df["window"] == w_label].copy()
        sub = sub.sort_values("calmar", ascending=False, na_position="last")
        lines.append(f"--- Window {w_label} ---")
        lines.append(f"  {'variant':<14} {'total':>10} {'CAGR':>8} {'maxDD':>8} {'Calmar':>7} "
                     f"{'Sharpe':>7} {'%≤−10':>7} {'%≤−25':>7}")
        for _, r in sub.iterrows():
            lines.append(f"  {r['variant']:<14} {r['total_pct']:>+9.2f}% "
                         f"{r['cagr_pct']:>+7.2f}% {r['max_dd_pct']:>+7.2f}% "
                         f"{(r['calmar'] or 0):>7.2f} {(r['sharpe'] or 0):>7.2f} "
                         f"{r['pct_days_le_10']:>6.1f}% {r['pct_days_le_25']:>6.1f}%")
        lines.append("")

    # Delta vs B0 in each window
    lines.append("--- Delta vs B0 (Δ_total_pp / Δ_DD_pp / Δ_Calmar) per window ---")
    lines.append(f"  {'variant':<14} "
                 f"{'W1 (Δtotal/ΔDD/ΔCal)':>30} "
                 f"{'W2 (Δtotal/ΔDD/ΔCal)':>30} "
                 f"{'W3 (Δtotal/ΔDD/ΔCal)':>30} "
                 f"{'WL (Δtotal/ΔDD/ΔCal)':>30}")
    for name in [v[0] for v in VARIANTS]:
        if name == "B0": continue
        cells = []
        for w in ("W1", "W2", "W3", "WL"):
            row = df[(df["variant"]==name) & (df["window"]==w)]
            if row.empty:
                cells.append("       —"); continue
            r = row.iloc[0]
            dt = r["total_pct"] - base_total[w]
            dd = r["max_dd_pct"] - base_dd[w]
            dc = (r["calmar"] or 0) - (base_calmar[w] or 0)
            cells.append(f"{dt:+8.1f}/{dd:+5.1f}/{dc:+5.2f}")
        lines.append(f"  {name:<14} " + " ".join(f"{c:>30}" for c in cells))
    lines.append("")

    # Robustness verdict
    lines.append("--- Robustness verdict ---")
    lines.append(f"  {'variant':<14} {'WL':>4} {'W1':>4} {'W2':>4} {'W3':>4} "
                 f"{'subwins':>8}  {'verdict':<22}")
    rob_df_sorted = rob_df.sort_values(by=["verdict", "n_subwindow_wins"],
                                        ascending=[True, False])
    for _, r in rob_df_sorted.iterrows():
        wl = "YES" if r["wl_calmar_beats_B0"] else "no"
        w1 = "YES" if r["W1_calmar_beats_B0"] else ("no" if r["W1_calmar_beats_B0"] is False else "?")
        w2 = "YES" if r["W2_calmar_beats_B0"] else ("no" if r["W2_calmar_beats_B0"] is False else "?")
        w3 = "YES" if r["W3_calmar_beats_B0"] else ("no" if r["W3_calmar_beats_B0"] is False else "?")
        lines.append(f"  {r['variant']:<14} {wl:>4} {w1:>4} {w2:>4} {w3:>4} "
                     f"{r['n_subwindow_wins']:>8}  {r['verdict']:<22}")
    lines.append("")
    lines.append("Verdict scale:")
    lines.append("  ROBUST            = beats baseline Calmar in WL AND ≥2 of 3 sub-windows")
    lines.append("  regime-dependent  = beats in WL AND exactly 1 sub-window")
    lines.append("  fragile           = beats in WL AND 0 sub-windows  (full-window mirage)")
    lines.append("  rejected          = does NOT beat baseline Calmar in WL")

    (out / "summary.txt").write_text("\n".join(lines))
    print("\n".join(lines))


if __name__ == "__main__":
    main()
