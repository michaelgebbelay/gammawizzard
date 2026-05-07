#!/usr/bin/env python3
"""
DD-reduction clean-timing rerun — supersedes dd_robustness_check.

Bug fix: prior version mixed timing conventions. Baseline B0 lagged the
state signal inside the variant function (`shift(1)`), but trailing-stop
and vol-target variants used today's close to set today's weight, which
the simulator then applied to today's return. That's lookahead.

Corrected convention here:
  * Variant emits `weight[t]` = "what I want to hold STARTING TOMORROW,
    decided at close[t]". This is the natural way to write each rule —
    no shift inside the variant.
  * Simulator does `effective_weights = weights.shift(1)` before applying
    to returns. So `effective[t]` = `weight[t-1]` × `return[t]`.
  * Every rule now uses only information that existed before the return
    being earned. Period.

Variant set (focus narrowed per the timing-rerun plan):
  Baseline:
    B0
  Fine trailing-stop band:
    TS_5, TS_6, TS_7, TS_8, TS_9, TS_10, TS_12
  Vol-target band:
    VT_20, VT_25, VT_30, VT_35

Slippage sensitivity: 10 / 25 / 50 bp.
Sub-windows: W1 (2011-2017), W2 (2018-2021), W3 (2022-), WL (full).

Diagnostic metrics added:
  - turnover (sum of |Δw| / 2)
  - exit count (transitions to TQQQ weight = 0)
  - avg TQQQ exposure
  - worst single-day strategy loss while invested
  - avg exposure during 2018-Q4, 2020-Covid, 2022-bear sub-periods
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
SLIPPAGE_LEVELS = [10, 25, 50]
ASSETS = ["TQQQ", "BIL"]

WINDOWS = {
    "W1": ("2011-01-03", "2017-12-31"),
    "W2": ("2018-01-01", "2021-12-31"),
    "W3": ("2022-01-01", DATA_END),
    "WL": ("2011-01-03", DATA_END),
}

NAMED_DDS = {
    "2018Q4": ("2018-10-01", "2018-12-31"),
    "2020covid": ("2020-02-15", "2020-04-15"),
    "2022bear": ("2022-01-01", "2022-12-31"),
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
    """C1-HYST: state[t] is the signal computed at close[t]."""
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
# Variant builder — emits weight[t] decided at close[t]
# ---------------------------------------------------------------------------

def make_weights(sig: pd.DataFrame, dates: pd.DatetimeIndex,
                 qqq_close: pd.Series,
                 stop_pct: float | None,
                 vol_target: float | None) -> pd.DataFrame:
    """
    Returns weights[t] = position to hold STARTING tomorrow, decided at
    close[t]. Simulator does shift(1) before applying.

    All inputs (state, qqq_close, vol21) read as-of close[t] — no internal
    shifting. Centralizing the lag in the simulator keeps each rule
    natural to read.
    """
    w = pd.DataFrame(0.0, index=dates, columns=ASSETS)
    state = sig["state"].reindex(dates)              # state at close[t]
    qqq = qqq_close.reindex(dates).ffill()           # qqq close at t

    # Vol target: vol21[t] uses returns through close[t]
    if vol_target is not None:
        qqq_ret = qqq_close.pct_change()
        vol21 = qqq_ret.rolling(21, min_periods=21).std() * np.sqrt(252)
        vol21 = vol21.reindex(dates).ffill()
        vt_mult = (vol_target / 3.0) / vol21.replace(0, np.nan)
        vt_mult = vt_mult.clip(0, 1).fillna(0)
    else:
        vt_mult = pd.Series(1.0, index=dates)

    holding = False
    peak = None
    overrode = False  # trailing-stop fired this risk-on episode

    for d in dates:
        st = state.loc[d]
        if st == "RISKON":
            if not holding and not overrode:
                holding = True
                peak = qqq.loc[d]
            elif holding:
                peak = max(peak, qqq.loc[d])
                if stop_pct is not None and (qqq.loc[d] / peak - 1) <= stop_pct:
                    holding = False
                    overrode = True
        else:  # BIL — reset
            holding = False
            peak = None
            overrode = False

        if holding:
            tqqq_w = float(vt_mult.loc[d])
            w.loc[d, "TQQQ"] = tqqq_w
            w.loc[d, "BIL"]  = 1.0 - tqqq_w
        else:
            w.loc[d, "BIL"] = 1.0
    return w


VARIANTS = [
    ("B0",     None, None),
    ("TS_5",   -0.05, None),
    ("TS_6",   -0.06, None),
    ("TS_7",   -0.07, None),
    ("TS_8",   -0.08, None),
    ("TS_9",   -0.09, None),
    ("TS_10",  -0.10, None),
    ("TS_12",  -0.12, None),
    ("VT_20",   None, 0.20),
    ("VT_25",   None, 0.25),
    ("VT_30",   None, 0.30),
    ("VT_35",   None, 0.35),
]


# ---------------------------------------------------------------------------
# Simulator — single source of timing truth
# ---------------------------------------------------------------------------

def simulate_strict(weights: pd.DataFrame, returns: pd.DataFrame,
                    slippage_bps: int) -> tuple[pd.Series, pd.Series, pd.DataFrame]:
    """
    Strict next-day-close execution:
      effective_weight[t] = weight[t-1]   (decision at close[t-1])
      strategy_return[t]  = effective_weight[t] · return[t]   (close[t-1] → close[t])
      cost on flip days   = sum(|effective[t] - effective[t-1]|) · bps

    Returns: (equity curve, daily strategy returns, effective weights).
    """
    effective = weights.shift(1).fillna(0.0)
    # First day: no prior position; treat as flat (cash)
    aligned_r = returns.reindex(effective.index).fillna(0.0)
    daily = (effective * aligned_r).sum(axis=1)
    turnover = effective.diff().abs().sum(axis=1).fillna(0.0)
    cost = turnover * (slippage_bps / 10000.0)
    daily_net = daily - cost
    eq = (1.0 + daily_net).cumprod()
    return eq, daily_net, effective


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

def metrics(eq: pd.Series, daily: pd.Series, eff_w: pd.DataFrame,
            qqq_close: pd.Series, name: str, window: str) -> dict:
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
    sharpe = float(daily.mean() / daily.std() * np.sqrt(252)) if daily.std() > 0 else None
    downside = daily[daily < 0]
    sortino = float(daily.mean() / downside.std() * np.sqrt(252)) if len(downside) > 1 and downside.std() > 0 else None
    calmar = float(cagr / abs(max_dd)) if max_dd < 0 else None

    # Diagnostics
    tqqq_w = eff_w["TQQQ"]
    avg_exposure = float(tqqq_w.mean())
    # turnover = sum |delta_w| over period / 2 (entries+exits double-count)
    turnover = float(eff_w.diff().abs().sum(axis=1).sum() / 2)
    # exits: TQQQ weight drops to 0 from positive
    exits = int(((tqqq_w == 0) & (tqqq_w.shift(1) > 0)).sum())
    # entries: TQQQ weight goes positive from 0
    entries = int(((tqqq_w > 0) & (tqqq_w.shift(1).fillna(0) == 0)).sum())
    invested = tqqq_w > 0
    invested_daily = daily[invested]
    worst_invested = float(invested_daily.min()) * 100 if len(invested_daily) else 0.0

    # Exposure during named drawdowns
    named_exp = {}
    for nm, (s, e) in NAMED_DDS.items():
        sub = tqqq_w[(tqqq_w.index >= s) & (tqqq_w.index <= e)]
        named_exp[f"avg_exp_{nm}"] = round(float(sub.mean()), 3) if len(sub) else None

    return {
        "variant": name, "window": window,
        "total_pct":  round((end_mult - 1) * 100, 2),
        "cagr_pct":   round(cagr * 100, 2),
        "max_dd_pct": round(max_dd * 100, 2),
        "calmar":     round(calmar, 3) if calmar else None,
        "sharpe":     round(sharpe, 3) if sharpe else None,
        "sortino":    round(sortino, 3) if sortino else None,
        "pct_days_le_10": round(days_under_10, 1),
        "pct_days_le_25": round(days_under_25, 1),
        "avg_exposure":   round(avg_exposure, 3),
        "turnover":       round(turnover, 1),
        "exits":          exits,
        "entries":        entries,
        "worst_invested_day_pct": round(worst_invested, 2),
        **named_exp,
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
    full_idx = bars["TQQQ"].index
    full_idx = full_idx[(full_idx >= common_start) & (full_idx <= bars["TQQQ"].index[-1])]

    returns = pd.DataFrame(index=full_idx)
    for asset in ASSETS:
        returns[asset] = bars[asset]["close"].pct_change().reindex(full_idx)
    qqq_close = bars["QQQ"]["close"]

    # Build weights once per variant on full index
    weight_curves = {}
    for name, stop_pct, vt in VARIANTS:
        w = make_weights(sig, full_idx, qqq_close, stop_pct, vt)
        s = w.sum(axis=1)
        w.loc[s == 0, "BIL"] = 1.0
        weight_curves[name] = w
        print(f"[built] {name}", file=sys.stderr)

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
            for slip in SLIPPAGE_LEVELS:
                eq, daily, eff = simulate_strict(sub_w, sub_r, slip)
                m = metrics(eq, daily, eff, qqq_close, name, w_label)
                m["slippage_bps"] = slip
                rows.append(m)

    df = pd.DataFrame(rows)
    df.to_csv(out / "metrics_strict.csv", index=False)

    # ----- robustness verdict (10bp baseline) -----
    sub10 = df[df["slippage_bps"] == 10]
    base_calmar = {w: sub10[(sub10["variant"]=="B0") & (sub10["window"]==w)]["calmar"].iloc[0]
                   for w in WINDOWS}
    base_dd     = {w: sub10[(sub10["variant"]=="B0") & (sub10["window"]==w)]["max_dd_pct"].iloc[0]
                   for w in WINDOWS}
    base_total  = {w: sub10[(sub10["variant"]=="B0") & (sub10["window"]==w)]["total_pct"].iloc[0]
                   for w in WINDOWS}

    rob_rows = []
    for name in [v[0] for v in VARIANTS]:
        if name == "B0": continue
        wins = {}
        for w in ("W1", "W2", "W3"):
            row = sub10[(sub10["variant"]==name) & (sub10["window"]==w)]
            if row.empty:
                wins[w] = None; continue
            v_calmar = row["calmar"].iloc[0]
            wins[w] = (v_calmar is not None and base_calmar[w] is not None
                       and v_calmar > base_calmar[w])
        wl_row = sub10[(sub10["variant"]==name) & (sub10["window"]=="WL")]
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
    rob_df.to_csv(out / "robustness_strict.csv", index=False)

    # ----- summary -----
    lines = []
    lines.append("=" * 100)
    lines.append("DD-reduction CLEAN-TIMING rerun (strict shift(1) execution)")
    lines.append("=" * 100)
    lines.append("Convention: variant emits weight[t] = position decided at close[t].")
    lines.append("Simulator applies effective[t]=weight[t-1] to return[t].")
    lines.append("No same-day lookahead.")
    lines.append("")
    lines.append("Sub-windows:")
    for w_label, (s, e) in WINDOWS.items():
        n = len(full_idx[(full_idx >= pd.Timestamp(s)) & (full_idx <= pd.Timestamp(e))])
        lines.append(f"  {w_label}: {s} .. {e}  ({n} sessions)")
    lines.append("Slippage levels: " + ", ".join(f"{s}bp" for s in SLIPPAGE_LEVELS))
    lines.append("")

    # Per-window tables at 10bp
    for w_label in ("W1", "W2", "W3", "WL"):
        sub = sub10[sub10["window"] == w_label].copy()
        sub = sub.sort_values("calmar", ascending=False, na_position="last")
        lines.append(f"--- Window {w_label}  (10bp slip) ---")
        lines.append(f"  {'variant':<8} {'total':>10} {'CAGR':>8} {'maxDD':>8} {'Calmar':>7} "
                     f"{'Sharpe':>7} {'avg_exp':>8} {'exits':>6} {'worst_inv':>10}")
        for _, r in sub.iterrows():
            lines.append(f"  {r['variant']:<8} {r['total_pct']:>+9.2f}% "
                         f"{r['cagr_pct']:>+7.2f}% {r['max_dd_pct']:>+7.2f}% "
                         f"{(r['calmar'] or 0):>7.2f} {(r['sharpe'] or 0):>7.2f} "
                         f"{r['avg_exposure']:>7.2f}  {r['exits']:>5d} "
                         f"{r['worst_invested_day_pct']:>+9.2f}%")
        lines.append("")

    # Slippage sensitivity at WL
    lines.append("--- Slippage sensitivity at WL: total return at 10/25/50bp ---")
    lines.append(f"  {'variant':<8} {'10bp':>11} {'25bp':>11} {'50bp':>11} "
                 f"{'WL maxDD':>10} {'WL Calmar':>10}")
    for name in [v[0] for v in VARIANTS]:
        cells = []
        max_dd_v = None
        cal_v = None
        for slip in SLIPPAGE_LEVELS:
            r = df[(df["variant"]==name) & (df["window"]=="WL") & (df["slippage_bps"]==slip)]
            if r.empty:
                cells.append("    —"); continue
            cells.append(f"{r['total_pct'].iloc[0]:>+9.2f}%")
            if slip == 10:
                max_dd_v = r["max_dd_pct"].iloc[0]
                cal_v = r["calmar"].iloc[0]
        lines.append(f"  {name:<8} " + " ".join(f"{c:>11}" for c in cells) +
                     f" {max_dd_v:>+9.2f}% {(cal_v or 0):>10.2f}")
    lines.append("")

    # Diagnostic: exposure during crashes
    lines.append("--- Avg TQQQ exposure during named drawdowns (WL, 10bp) ---")
    lines.append(f"  {'variant':<8} {'2018Q4':>10} {'2020covid':>11} {'2022bear':>10}")
    for name in [v[0] for v in VARIANTS]:
        r = sub10[(sub10["variant"]==name) & (sub10["window"]=="WL")]
        if r.empty: continue
        lines.append(f"  {name:<8} {r['avg_exp_2018Q4'].iloc[0]:>9.2f} "
                     f"{r['avg_exp_2020covid'].iloc[0]:>10.2f} "
                     f"{r['avg_exp_2022bear'].iloc[0]:>9.2f}")
    lines.append("")

    # Per-window deltas vs B0 at 10bp
    lines.append("--- Δ vs B0 (Δtotal_pp / ΔDD_pp / ΔCalmar) per window @ 10bp ---")
    lines.append(f"  {'variant':<8} "
                 f"{'W1 (Δtot/ΔDD/ΔCal)':>26} "
                 f"{'W2 (Δtot/ΔDD/ΔCal)':>26} "
                 f"{'W3 (Δtot/ΔDD/ΔCal)':>26} "
                 f"{'WL (Δtot/ΔDD/ΔCal)':>26}")
    for name in [v[0] for v in VARIANTS]:
        if name == "B0": continue
        cells = []
        for w in ("W1", "W2", "W3", "WL"):
            row = sub10[(sub10["variant"]==name) & (sub10["window"]==w)]
            if row.empty:
                cells.append("       —"); continue
            r = row.iloc[0]
            dt = r["total_pct"] - base_total[w]
            dd = r["max_dd_pct"] - base_dd[w]
            dc = (r["calmar"] or 0) - (base_calmar[w] or 0)
            cells.append(f"{dt:+7.1f}/{dd:+5.1f}/{dc:+5.2f}")
        lines.append(f"  {name:<8} " + " ".join(f"{c:>26}" for c in cells))
    lines.append("")

    # Robustness verdict
    lines.append("--- Robustness verdict (10bp slippage) ---")
    lines.append(f"  {'variant':<8} {'WL':>4} {'W1':>4} {'W2':>4} {'W3':>4} "
                 f"{'subwins':>8}  {'verdict':<22}")
    for _, r in rob_df.sort_values(by=["verdict","n_subwindow_wins"],
                                    ascending=[True,False]).iterrows():
        wl = "YES" if r["wl_calmar_beats_B0"] else "no"
        w1 = "YES" if r["W1_calmar_beats_B0"] else ("no" if r["W1_calmar_beats_B0"] is False else "?")
        w2 = "YES" if r["W2_calmar_beats_B0"] else ("no" if r["W2_calmar_beats_B0"] is False else "?")
        w3 = "YES" if r["W3_calmar_beats_B0"] else ("no" if r["W3_calmar_beats_B0"] is False else "?")
        lines.append(f"  {r['variant']:<8} {wl:>4} {w1:>4} {w2:>4} {w3:>4} "
                     f"{r['n_subwindow_wins']:>8}  {r['verdict']:<22}")
    lines.append("")
    lines.append("Verdict scale:")
    lines.append("  ROBUST            beats B0 Calmar in WL AND ≥2 of 3 sub-windows")
    lines.append("  regime-dependent  beats in WL AND exactly 1 sub-window")
    lines.append("  fragile           beats in WL AND 0 sub-windows")
    lines.append("  rejected          does NOT beat in WL")

    (out / "summary.txt").write_text("\n".join(lines))
    print("\n".join(lines))


if __name__ == "__main__":
    main()
