#!/usr/bin/env python3
"""
C1-HYST leverage sweep: compare QLD (2x QQQ) vs TQQQ (3x QQQ) as the
risk-on sleeve, holding the locked C1-HYST signal constant.

Question: does giving up 1x of leverage win on risk-adjusted return
because of reduced daily-reset volatility decay, even though it
caps absolute upside?

Mechanics: daily-reset leveraged ETFs decay in proportion to
(leverage * vol)^2. So:
  TQQQ decay coefficient ∝ 9
  QLD  decay coefficient ∝ 4
QLD pays ~44% of TQQQ's decay penalty during chop. The strategy is
already in BIL ~30% of the time (avoiding the worst chop), but during
in-regime chop the decay still bites.

This is an OPERATIONALIZATION test, not a signal-design test.
The C1-HYST signal is locked. Only the risk-on sleeve varies.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from non_ma_regime_backtest import simulate, START_VALUE   # noqa: E402

WINDOWS = {
    "L": ("2011-01-03", "2026-04-29"),
    "A": ("2021-01-04", "2026-04-29"),
    "B": ("2022-01-03", "2026-04-29"),
    "C": ("2022-02-11", "2026-04-29"),
}
SLIPPAGE_BPS = [3, 10, 25, 50]
DATA_START = "2010-01-01"
DATA_END = "2026-12-31"

# Risk-on sleeves to compare. QLD inception 2006; TQQQ inception 2010-02-11.
LEVERAGE_VARIANTS = ["TQQQ", "QLD"]
SUPPORT_TICKERS = ["SPY", "QQQ", "BIL"]


def load_bars(use_cache: bool = True):
    cache = HERE / "data" / "yf_leverage_sweep.parquet"
    if use_cache and cache.exists():
        df = pd.read_parquet(cache)
        print(f"[data] cache hit: {len(df)} rows", file=sys.stderr)
    else:
        import yfinance as yf
        rows = []
        for t in SUPPORT_TICKERS + LEVERAGE_VARIANTS:
            raw = yf.Ticker(t).history(start=DATA_START, end=DATA_END, auto_adjust=True)
            if raw.empty:
                raise SystemExit(f"yfinance no data for {t}")
            r = raw.reset_index()
            r["date"] = pd.to_datetime(r["Date"]).dt.tz_localize(None).dt.normalize()
            r = r[["date","Open","High","Low","Close"]].rename(
                columns={"Open":"open","High":"high","Low":"low","Close":"close"})
            r["ticker"] = t
            rows.append(r)
        df = pd.concat(rows, ignore_index=True).dropna()
        cache.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(cache, index=False)
        print(f"[data] wrote cache: {cache}", file=sys.stderr)
    out = {}
    for t in SUPPORT_TICKERS + LEVERAGE_VARIANTS:
        sub = df[df["ticker"] == t].sort_values("date").reset_index(drop=True)
        out[t] = sub
        print(f"[data] {t}: {len(sub)} rows {sub['date'].iloc[0].date()} -> {sub['date'].iloc[-1].date()}",
              file=sys.stderr)
    return out


def compute_indicators(qqq: pd.DataFrame) -> pd.DataFrame:
    df = qqq[["date","open","high","low","close"]].copy()
    df["sma50"]  = df["close"].rolling(50, min_periods=50).mean()
    df["sma150"] = df["close"].rolling(150, min_periods=150).mean()
    df["sma200"] = df["close"].rolling(200, min_periods=200).mean()
    df["ret63"]  = df["close"].pct_change(63)
    return df


def state_c1_hyst(ind: pd.DataFrame) -> list:
    """C1-HYST locked signal; emit canonical 'TQQQ'/'BIL' labels.
    The actual sleeve is selected by which bars are mapped to the 'TQQQ' key
    in bars_for_run."""
    out, s = [], None
    for _, r in ind.iterrows():
        if any(pd.isna(r[c]) for c in ("sma50","sma150","sma200","ret63")):
            out.append(None); continue
        score = (int(r["close"] > r["sma150"])
               + int(r["sma50"]  > r["sma200"])
               + int(r["ret63"]  > 0))
        if s is None:
            s = "TQQQ" if score == 3 else "BIL"
        else:
            if score == 3: s = "TQQQ"
            elif score <= 1: s = "BIL"
        out.append(s)
    return out


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--out-dir", default="out")
    p.add_argument("--no-cache", action="store_true")
    args = p.parse_args()
    out_dir = Path(args.out_dir); out_dir.mkdir(parents=True, exist_ok=True)

    bars = load_bars(use_cache=not args.no_cache)
    ind = compute_indicators(bars["QQQ"])
    first_valid = ind.dropna(subset=["sma200","sma150","sma50","ret63"])["date"].iloc[0]
    print(f"[run] first valid signal day: {first_valid.date()}", file=sys.stderr)

    rows = []
    full_results = {}
    states = state_c1_hyst(ind)
    for variant in LEVERAGE_VARIANTS:
        full_results[variant] = {}
        # simulate() uses bars["TQQQ"]/bars["BIL"] hardcoded — alias risk-on
        # under the "TQQQ" key so the same state list works for both sleeves
        bars_for_run = dict(bars)
        bars_for_run["TQQQ"] = bars[variant]
        for w_label, (s, e) in WINDOWS.items():
            full_results[variant][w_label] = {}
            start = max(pd.Timestamp(s), first_valid)
            end = pd.Timestamp(e)
            for slip in SLIPPAGE_BPS:
                m = simulate(states, ind, bars_for_run, start, end, slip)
                # simulate() reports tqqq_bh as the alias asset's BH — relabel for clarity
                m_clean = dict(m)
                m_clean["risk_on_bh_total_return"] = m["tqqq_bh_total_return"]
                m_clean["risk_on_bh_final_value"]  = m["tqqq_bh_final_value"]
                full_results[variant][w_label][slip] = m_clean
                print(f"[done] {variant} {w_label} {slip}bp  total={m['total_return']*100:+.2f}%  "
                      f"DD={m['max_drawdown']*100:.2f}%  Sharpe={m['sharpe']}  flips={m['flips']}",
                      file=sys.stderr)
                rows.append({
                    "risk_on": variant, "window": w_label, "slippage_bps": slip,
                    "start_date": str(start.date()), "end_date": str(end.date()),
                    "total_return_pct": round(m["total_return"]*100, 2),
                    "final_value": m["final_value"],
                    "cagr_pct": round(m["cagr"]*100, 2),
                    "max_dd_pct": round(m["max_drawdown"]*100, 2),
                    "sharpe": m["sharpe"],
                    "sortino": m["sortino"],
                    "calmar": m["calmar"],
                    "flips": m["flips"],
                    "pct_in_risk_on": round(m["pct_days_in_TQQQ"]*100, 1),
                    "worst_day_loss_pct": round((m["worst_day_loss"] or 0)*100, 2),
                    "spy_bh_pct":  round(m["spy_bh_total_return"]*100, 2),
                    "qqq_bh_pct":  round(m["qqq_bh_total_return"]*100, 2),
                    "risk_on_bh_pct": round(m["tqqq_bh_total_return"]*100, 2),
                    "pass_2x_spy": m["pass_2x_spy_return"],
                })

    pd.DataFrame(rows).to_csv(out_dir / "leverage_sweep_metrics.csv", index=False)

    # ----- comparison summary -----
    lines = []
    lines.append("="*100)
    lines.append("C1-HYST leverage sweep: TQQQ (3x) vs QLD (2x)")
    lines.append("="*100)
    lines.append("Same locked C1-HYST signal on QQQ. Only the risk-on sleeve differs.")
    lines.append("")

    for w in WINDOWS.keys():
        lines.append(f"--- Window {w} ---")
        lines.append(f"  {'sleeve':<6} {'slip':>4}  {'total':>10} {'CAGR':>8} {'DD':>8} {'Sharpe':>7} "
                     f"{'Sortino':>8} {'Calmar':>7} {'flips':>6} {'%on':>5}  {'pass2x':>6}  {'BH ret':>10}")
        for v in LEVERAGE_VARIANTS:
            for slip in SLIPPAGE_BPS:
                m = full_results[v][w][slip]
                sh = "n/a" if m["sharpe"] is None else f"{m['sharpe']:.2f}"
                so = "n/a" if m["sortino"] is None else f"{m['sortino']:.2f}"
                cl = "n/a" if m["calmar"] is None else f"{m['calmar']:.2f}"
                lines.append(f"  {v:<6} {slip:>3}bp  {m['total_return']*100:>+9.2f}% "
                             f"{m['cagr']*100:>+7.2f}% {m['max_drawdown']*100:>+7.2f}% "
                             f"{sh:>7} {so:>8} {cl:>7} {m['flips']:>6d} "
                             f"{m['pct_days_in_TQQQ']*100:>4.0f}%  {str(m['pass_2x_spy_return']):>6}  "
                             f"{m['risk_on_bh_total_return']*100:>+9.2f}%")
        lines.append("")

    # head-to-head deltas at 10bp
    lines.append("="*100)
    lines.append("Head-to-head TQQQ vs QLD at 10bp slippage")
    lines.append("="*100)
    lines.append(f"  {'win':<4} {'TQQQ tot':>11} {'QLD tot':>11} {'Δabs':>9} "
                 f"{'TQQQ DD':>9} {'QLD DD':>9} {'Δ DD':>8} "
                 f"{'TQQQ Calmar':>13} {'QLD Calmar':>12} {'Δ Calmar':>10}")
    for w in WINDOWS.keys():
        t = full_results["TQQQ"][w][10]; q = full_results["QLD"][w][10]
        delta_abs = (t["total_return"] - q["total_return"]) * 100
        delta_dd  = (q["max_drawdown"] - t["max_drawdown"]) * 100   # +ve = QLD shallower
        delta_calmar = (q["calmar"] or 0) - (t["calmar"] or 0)
        lines.append(f"  {w:<4} {t['total_return']*100:>+10.2f}% {q['total_return']*100:>+10.2f}% "
                     f"{delta_abs:>+8.2f}pp "
                     f"{t['max_drawdown']*100:>+8.2f}% {q['max_drawdown']*100:>+8.2f}% "
                     f"{delta_dd:>+7.2f}pp "
                     f"{(t['calmar'] or 0):>13.2f} {(q['calmar'] or 0):>12.2f} {delta_calmar:>+10.2f}")
    lines.append("")
    lines.append("Reading: Δabs +ve = TQQQ ahead on absolute return (expected).")
    lines.append("         Δ DD +ve = QLD has shallower drawdown (expected).")
    lines.append("         Δ Calmar +ve = QLD better risk-adjusted (the test).")

    (out_dir / "summary.txt").write_text("\n".join(lines))
    print("\n".join(lines))


if __name__ == "__main__":
    main()
