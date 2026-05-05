#!/usr/bin/env python3
"""
Independent-data revalidation of C1-HYST against Stooq daily bars.

Goal: confirm the +6,610% / -48.1% DD result on yfinance is not a
data-source-specific artifact. Pulls SPY/QQQ/TQQQ/BIL from stooq.com,
runs the EXACT C1-HYST signal + simulator from the locked spec, and
diffs the metrics against the yfinance reference numbers.

Tolerance: report the gap; don't pass/fail on a single number.
A ±2pp cumulative miss across the L window would be acceptable. A
miss in the same direction across all four windows means data-source
divergence is real and worth investigating.

Stooq daily CSV API:
  https://stooq.com/q/d/l/?s=spy.us&d1=20100101&d2=20260505&i=d
  Returns: Date,Open,High,Low,Close,Volume
  Adjusted: Stooq applies split + dividend adjustments to historical
  closes for US equities/ETFs, comparable to yfinance auto_adjust=True.

Trigger: gh workflow run c1_hyst_stooq_revalidation.yml
"""
from __future__ import annotations

import argparse
import io
import json
import sys
import time
from pathlib import Path
from urllib.request import urlopen, Request

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
SLIPPAGE_BPS = [10, 50]
TICKERS = ["SPY", "QQQ", "TQQQ", "BIL"]
DATA_START = "2010-01-01"
DATA_END = "2026-05-05"

# from GHA run 25358962345 (entry_exit_attribution.yml), 10bp slippage
YF_REFERENCE = {
    ("L", 10): {"total_return": 66.0978, "max_drawdown": -0.4814, "flips": 49},
    ("A", 10): {"total_return": 4.3397,  "max_drawdown": -0.3687, "flips": 15},
    ("B", 10): {"total_return": 1.9284,  "max_drawdown": -0.3687, "flips": 15},
    ("C", 10): {"total_return": 2.6525,  "max_drawdown": -0.3687, "flips": 14},
    ("L", 50): {"total_return": 54.1227, "max_drawdown": -0.4814, "flips": 49},
    ("A", 50): {"total_return": 4.0278,  "max_drawdown": -0.3687, "flips": 15},
    ("B", 50): {"total_return": 1.7573,  "max_drawdown": -0.3687, "flips": 15},
    ("C", 50): {"total_return": 2.4530,  "max_drawdown": -0.3687, "flips": 14},
}


def fetch_stooq(ticker: str, start: str, end: str, retries: int = 3) -> pd.DataFrame:
    s_yyyymmdd = start.replace("-", "")
    e_yyyymmdd = end.replace("-", "")
    sym = f"{ticker.lower()}.us"
    url = f"https://stooq.com/q/d/l/?s={sym}&d1={s_yyyymmdd}&d2={e_yyyymmdd}&i=d"
    last_err = None
    for attempt in range(retries):
        try:
            req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urlopen(req, timeout=30) as resp:
                body = resp.read().decode("utf-8")
            if not body or body.startswith("No data"):
                raise SystemExit(f"stooq returned empty body for {ticker}")
            df = pd.read_csv(io.StringIO(body))
            if df.empty:
                raise SystemExit(f"stooq returned empty CSV for {ticker}")
            df = df.rename(columns={c: c.lower() for c in df.columns})
            df["date"] = pd.to_datetime(df["date"]).dt.normalize()
            df = df[["date", "open", "high", "low", "close"]].sort_values("date").reset_index(drop=True)
            df["ticker"] = ticker
            print(f"[stooq] {ticker}: {len(df)} rows {df['date'].iloc[0].date()} -> {df['date'].iloc[-1].date()}",
                  file=sys.stderr)
            return df
        except Exception as e:
            last_err = e
            print(f"[stooq] {ticker} attempt {attempt+1}/{retries} failed: {e}", file=sys.stderr)
            time.sleep(2 + attempt)
    raise SystemExit(f"stooq fetch failed for {ticker}: {last_err}")


def load_bars_stooq() -> dict[str, pd.DataFrame]:
    out = {}
    for t in TICKERS:
        out[t] = fetch_stooq(t, DATA_START, DATA_END)
    return out


def compute_c1_hyst_indicators(qqq: pd.DataFrame) -> pd.DataFrame:
    df = qqq[["date", "open", "high", "low", "close"]].copy()
    df["sma50"]  = df["close"].rolling(50, min_periods=50).mean()
    df["sma150"] = df["close"].rolling(150, min_periods=150).mean()
    df["sma200"] = df["close"].rolling(200, min_periods=200).mean()
    df["ret63"]  = df["close"].pct_change(63)
    return df


def state_c1_hyst(ind: pd.DataFrame) -> list:
    """Locked spec: score = (close>sma150) + (sma50>sma200) + (ret63>0).
    in BIL: enter TQQQ iff score==3.
    in TQQQ: hold iff score>=2; exit iff score<=1."""
    out, s = [], None
    for _, r in ind.iterrows():
        if any(pd.isna(r[c]) for c in ("sma200", "sma150", "sma50", "ret63")):
            out.append(None); continue
        A = int(r["close"] > r["sma150"])
        B = int(r["sma50"] > r["sma200"])
        C = int(r["ret63"] > 0)
        score = A + B + C
        if s is None:
            s = "TQQQ" if score == 3 else "BIL"
        else:
            if score == 3:
                s = "TQQQ"
            elif score <= 1:
                s = "BIL"
            # score == 2 -> hold
        out.append(s)
    return out


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--out-dir", default="out")
    args = p.parse_args()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print("[load] fetching all four tickers from Stooq...", file=sys.stderr)
    bars = load_bars_stooq()
    ind = compute_c1_hyst_indicators(bars["QQQ"])
    states = state_c1_hyst(ind)
    first_valid = ind.dropna(subset=["sma200", "sma150", "sma50", "ret63"])["date"].iloc[0]
    print(f"[run] first valid signal day: {first_valid.date()}", file=sys.stderr)

    rows = []
    for w_label, (s, e) in WINDOWS.items():
        start = max(pd.Timestamp(s), first_valid)
        end = pd.Timestamp(e)
        for slip in SLIPPAGE_BPS:
            m = simulate(states, ind, bars, start, end, slip)
            yf_ref = YF_REFERENCE[(w_label, slip)]
            tr_diff_pp = (m["total_return"] - yf_ref["total_return"]) * 100  # in %-points of return
            dd_diff_pp = (m["max_drawdown"] - yf_ref["max_drawdown"]) * 100
            flip_diff = m["flips"] - yf_ref["flips"]
            rows.append({
                "window": w_label, "slippage_bps": slip,
                "stooq_total_return_pct":  round(m["total_return"]*100, 2),
                "yfinance_total_return_pct": round(yf_ref["total_return"]*100, 2),
                "diff_pp_total":  round(tr_diff_pp, 2),
                "stooq_max_dd_pct":  round(m["max_drawdown"]*100, 2),
                "yfinance_max_dd_pct": round(yf_ref["max_drawdown"]*100, 2),
                "diff_pp_dd":     round(dd_diff_pp, 2),
                "stooq_flips":    m["flips"],
                "yf_flips":       yf_ref["flips"],
                "flip_diff":      flip_diff,
                "stooq_sharpe":   m["sharpe"],
                "stooq_pass_2x":  m["pass_2x_spy_return"],
                "stooq_spy_bh_ret_pct": round(m["spy_bh_total_return"]*100, 2),
                "stooq_qqq_bh_ret_pct": round(m["qqq_bh_total_return"]*100, 2),
                "stooq_tqqq_bh_ret_pct": round(m["tqqq_bh_total_return"]*100, 2),
            })
            tdf = pd.DataFrame(m["trades"])
            if not tdf.empty:
                tdf.to_csv(out_dir / f"trades_C1HYST_stooq_{w_label}_{slip}bp.csv", index=False)

    cmp = pd.DataFrame(rows)
    cmp.to_csv(out_dir / "stooq_vs_yfinance.csv", index=False)

    # human readable
    lines = []
    lines.append("=" * 100)
    lines.append("C1-HYST Stooq vs yfinance reproducibility check")
    lines.append("=" * 100)
    lines.append(f"Stooq tickers: {', '.join(t.lower() + '.us' for t in TICKERS)}")
    lines.append(f"Reference: GHA run 25358962345 (entry_exit_attribution.yml), 10bp + 50bp")
    lines.append("")
    lines.append(f"{'window':>6} {'slip':>6} {'stooq_tot':>12} {'yf_tot':>12} {'diff_pp':>10} "
                 f"{'stooq_DD':>9} {'yf_DD':>9} {'flip_stooq':>11} {'flip_yf':>9} {'pass_2x':>8}")
    lines.append("-" * 100)
    for r in rows:
        lines.append(f"{r['window']:>6} {r['slippage_bps']:>5}bp "
                     f"{r['stooq_total_return_pct']:>+11.2f}% "
                     f"{r['yfinance_total_return_pct']:>+11.2f}% "
                     f"{r['diff_pp_total']:>+9.2f}pp "
                     f"{r['stooq_max_dd_pct']:>+8.2f}% "
                     f"{r['yfinance_max_dd_pct']:>+8.2f}% "
                     f"{r['stooq_flips']:>11d} {r['yf_flips']:>9d} "
                     f"{str(r['stooq_pass_2x']):>8}")
    lines.append("")
    lines.append("Buy-and-hold sanity (Stooq):")
    L = next(r for r in rows if r["window"] == "L" and r["slippage_bps"] == 10)
    lines.append(f"  L window: SPY {L['stooq_spy_bh_ret_pct']:+.2f}%  "
                 f"QQQ {L['stooq_qqq_bh_ret_pct']:+.2f}%  "
                 f"TQQQ {L['stooq_tqqq_bh_ret_pct']:+.2f}%")
    lines.append("  yfinance ref: SPY +634.13%  QQQ +1265.39%  TQQQ +16031.27%")
    lines.append("")

    # verdict: max absolute total-return diff and direction agreement
    L_diff_10 = next(r["diff_pp_total"] for r in rows
                     if r["window"] == "L" and r["slippage_bps"] == 10)
    diffs_pp = [r["diff_pp_total"] for r in rows]
    lines.append("--- Reproducibility verdict ---")
    lines.append(f"  max |diff| in total return across all (window,slip): "
                 f"{max(abs(x) for x in diffs_pp):.2f}pp")
    lines.append(f"  L-window 10bp diff: {L_diff_10:+.2f}pp "
                 f"(yfinance reference = +6609.78%)")
    same_dir = all((d >= 0) == (diffs_pp[0] >= 0) for d in diffs_pp)
    lines.append(f"  all diffs same sign?  {same_dir}  (if true and large, suggests "
                 "systematic data-source divergence, not noise)")
    lines.append("")
    lines.append("If |L diff| < 100pp on a +6610% baseline, treat as confirmed; "
                 "if larger, inspect BIL adjustment convention or split-handling.")

    (out_dir / "summary.txt").write_text("\n".join(lines))
    print((out_dir / "summary.txt").read_text())


if __name__ == "__main__":
    main()
