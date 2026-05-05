#!/usr/bin/env python3
"""
SPY 200d trend filter -> UPRO, with symmetric 3% buffer.

State machine:
  risk_on  (UPRO): flip to BIL when SPY.close <= SMA200 * (1 - 0.03)
  risk_off (BIL):  flip to UPRO when SPY.close >= SMA200 * (1 + 0.03)
  initial state: risk_on if SPY > SMA200 else risk_off

Signal computed at close, trade executed at next open.
Costs: 3 bp per flip (1 bp commission + 2 bp slippage, combined).
UPRO 0.91% ER and BIL yield are NOT applied separately -- both are
already baked into the actual ETF NAV / price series we use here.

Output: equity curve CSV + summary text/json under --out-dir.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
DATA_DIR = HERE / "data"
RESULTS_DIR = HERE / "results"

SMA_WINDOW = 200
BUFFER = 0.03                 # symmetric 3% band around the SMA
COST_PER_FLIP = 0.0003        # 1 bp + 2 bp = 3 bp, applied once per flip event


def load_etf_bars(tickers: list[str]) -> dict[str, pd.DataFrame]:
    path = DATA_DIR / "aggs_daily_adjusted.parquet"
    if not path.exists():
        raise SystemExit(f"missing parquet: {path}")
    df = pd.read_parquet(path, columns=["ticker", "date", "open", "high", "low", "close"])
    out: dict[str, pd.DataFrame] = {}
    for t in tickers:
        sub = df[df["ticker"] == t].copy()
        if sub.empty:
            raise SystemExit(f"no bars for {t}")
        sub["date"] = pd.to_datetime(sub["date"]).dt.normalize()
        out[t] = sub.sort_values("date").reset_index(drop=True)
        print(f"[load] {t}: {len(out[t])} rows  {out[t]['date'].iloc[0].date()} -> {out[t]['date'].iloc[-1].date()}",
              file=sys.stderr)
    return out


def compute_state(spy: pd.DataFrame) -> pd.DataFrame:
    df = spy[["date", "close"]].rename(columns={"close": "spy_close"}).copy()
    df["sma200"] = df["spy_close"].rolling(SMA_WINDOW, min_periods=SMA_WINDOW).mean()
    df["upper_band"] = df["sma200"] * (1.0 + BUFFER)
    df["lower_band"] = df["sma200"] * (1.0 - BUFFER)

    state = []
    s = None
    for _, row in df.iterrows():
        if pd.isna(row["sma200"]):
            state.append(None)
            continue
        if s is None:
            s = "risk_on" if row["spy_close"] > row["sma200"] else "risk_off"
        else:
            if s == "risk_on" and row["spy_close"] <= row["lower_band"]:
                s = "risk_off"
            elif s == "risk_off" and row["spy_close"] >= row["upper_band"]:
                s = "risk_on"
        state.append(s)
    df["state"] = state
    return df


def run(start: pd.Timestamp, end: pd.Timestamp, bars: dict[str, pd.DataFrame],
        states: pd.DataFrame) -> tuple[pd.DataFrame, dict]:

    sig = states[(states["date"] >= start) & (states["date"] <= end)].copy().reset_index(drop=True)

    # target sleeve held during day t is determined by state at close[t-1]
    sig["target"] = sig["state"].shift(1).map({"risk_on": "UPRO", "risk_off": "BIL"})
    sig.loc[0, "target"] = "BIL" if sig["state"].iloc[0] == "risk_off" else "UPRO"

    bar_lookup = {t: bars[t].set_index("date")[["open", "close"]].to_dict(orient="index")
                  for t in ("UPRO", "BIL")}

    equity = 1.0
    held = sig["target"].iloc[0]
    flips = 0
    eq_curve = []
    held_history = []

    prev_date = None
    for i, row in sig.iterrows():
        d = row["date"]
        target = row["target"]

        if prev_date is None:
            o = bar_lookup[held].get(d, {}).get("open")
            c = bar_lookup[held].get(d, {}).get("close")
            if o and c and o > 0:
                equity *= c / o
            equity *= (1.0 - COST_PER_FLIP)  # initial entry counts as one flip event
            flips += 1
            eq_curve.append(equity)
            held_history.append(held)
            prev_date = d
            continue

        if target != held:
            old_open = bar_lookup[held].get(d, {}).get("open")
            old_prev_close = bar_lookup[held].get(prev_date, {}).get("close")
            if old_open and old_prev_close and old_prev_close > 0:
                equity *= old_open / old_prev_close
            equity *= (1.0 - COST_PER_FLIP)
            new_open = bar_lookup[target].get(d, {}).get("open")
            new_close = bar_lookup[target].get(d, {}).get("close")
            if new_open and new_close and new_open > 0:
                equity *= new_close / new_open
            held = target
            flips += 1
        else:
            cprev = bar_lookup[held].get(prev_date, {}).get("close")
            ccur = bar_lookup[held].get(d, {}).get("close")
            if cprev and ccur and cprev > 0:
                equity *= ccur / cprev

        eq_curve.append(equity)
        held_history.append(held)
        prev_date = d

    sig["equity"] = eq_curve
    sig["held"] = held_history
    sig["daily_ret"] = pd.Series(eq_curve).pct_change().fillna(0.0).values

    # benchmarks for context
    spy_bh = bars["SPY"].set_index("date")["close"].reindex(sig["date"]).ffill()
    upro_bh = bars["UPRO"].set_index("date")["close"].reindex(sig["date"]).ffill()
    sig["spy_bh"] = spy_bh.values / spy_bh.iloc[0]
    sig["upro_bh"] = upro_bh.values / upro_bh.iloc[0]

    eq = np.asarray(eq_curve)
    dr = sig["daily_ret"].values
    years = (pd.Timestamp(sig["date"].iloc[-1]) - pd.Timestamp(sig["date"].iloc[0])).days / 365.25
    rolling_max = np.maximum.accumulate(eq)
    drawdown = eq / rolling_max - 1.0
    daily_std = float(np.nanstd(dr, ddof=1))
    metrics = {
        "start_date": str(pd.Timestamp(sig["date"].iloc[0]).date()),
        "end_date": str(pd.Timestamp(sig["date"].iloc[-1]).date()),
        "n_days": int(len(eq)),
        "years": round(years, 3),
        "total_return": round(float(eq[-1] - 1.0), 4),
        "cagr": round(float(eq[-1] ** (1.0 / years) - 1.0), 4) if years > 0 else None,
        "sharpe": round(float(np.nanmean(dr) / daily_std * np.sqrt(252)), 3) if daily_std > 0 else None,
        "max_drawdown": round(float(drawdown.min()), 4),
        "flips": int(flips),
        "pct_days_in_upro": round(float((np.array(held_history) == "UPRO").mean()), 4),
        "pct_days_in_bil": round(float((np.array(held_history) == "BIL").mean()), 4),
        "spy_bh_total_return": round(float(sig["spy_bh"].iloc[-1] - 1.0), 4),
        "upro_bh_total_return": round(float(sig["upro_bh"].iloc[-1] - 1.0), 4),
    }
    return sig, metrics


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--start-date", default="2022-01-03")
    p.add_argument("--end-date", default="2026-05-01")
    p.add_argument("--out-dir", default=str(RESULTS_DIR))
    args = p.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    bars = load_etf_bars(["SPY", "UPRO", "BIL"])
    states = compute_state(bars["SPY"])
    first_valid = states.dropna(subset=["sma200"])["date"].iloc[0]
    requested = pd.Timestamp(args.start_date)
    start = max(requested, first_valid)
    end = pd.Timestamp(args.end_date)
    print(f"[run] requested start={requested.date()}  actual start={start.date()}  end={end.date()}",
          file=sys.stderr)

    sig, m = run(start, end, bars, states)

    curve_path = out_dir / "spy_trend_upro_curve.csv"
    sig.to_csv(curve_path, index=False)
    (out_dir / "summary.json").write_text(json.dumps({"metrics": m, "config": {
        "sma_window": SMA_WINDOW,
        "buffer_pct": BUFFER * 100,
        "cost_per_flip_bps": COST_PER_FLIP * 10000,
    }}, indent=2))

    lines = [
        "=" * 70,
        "SPY 200d trend filter -> UPRO (3% symmetric buffer)",
        "=" * 70,
        f"Window:        {m['start_date']} -> {m['end_date']}  ({m['years']}y, {m['n_days']} days)",
        f"Cost/flip:     {COST_PER_FLIP*10000:.0f} bp",
        f"Buffer:        +/- {BUFFER*100:.0f}% around SMA200",
        "",
        f"Total return:  {m['total_return']*100:>8.2f}%",
        f"CAGR:          {m['cagr']*100:>8.2f}%",
        f"Max drawdown:  {m['max_drawdown']*100:>8.2f}%",
        f"Sharpe:        {m['sharpe']:>8.3f}",
        f"Flips:         {m['flips']}",
        f"% days UPRO:   {m['pct_days_in_upro']*100:>5.1f}%",
        f"% days BIL:    {m['pct_days_in_bil']*100:>5.1f}%",
        "",
        f"SPY  buy/hold: {m['spy_bh_total_return']*100:>8.2f}%",
        f"UPRO buy/hold: {m['upro_bh_total_return']*100:>8.2f}%",
        f"vs 2x SPY:     {(m['total_return'] / (2 * m['spy_bh_total_return'])):.2f}x of target",
    ]
    txt = "\n".join(lines)
    (out_dir / "summary.txt").write_text(txt)
    print(txt)


if __name__ == "__main__":
    main()
