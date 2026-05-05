#!/usr/bin/env python3
"""
Chandelier Exit on QQQ -> TQQQ / BIL.

Signal proxy: QQQ. Tradable sleeves: TQQQ (3x), BIL (cash).

Indicators (computed on QQQ daily bars):
    HH22       = max(close, 22-day rolling)
    TR_t       = max(high_t - low_t,
                     abs(high_t - close_{t-1}),
                     abs(low_t  - close_{t-1}))
    ATR22      = simple mean of last 22 TRs
    Stop_t     = HH22_t - 3 * ATR22_t

Rules (evaluated at close, executed at next open):
    if QQQ.close > Stop -> 100% TQQQ
    if QQQ.close < Stop -> 100% BIL
    (close == Stop is treated as no-flip; carry prior holding.)

Cost: 3 bp per flip event (1 bp commission + 2 bp slippage).
ETF expense ratios are baked into the actual ETF NAV / price, not re-applied.

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

ATR_WINDOW = 22
HH_WINDOW = 22
ATR_MULT = 3.0
COST_PER_FLIP = 0.0003


def load_etf_bars(tickers: list[str]) -> dict[str, pd.DataFrame]:
    path = DATA_DIR / "aggs_daily_adjusted.parquet"
    if not path.exists():
        raise SystemExit(f"missing parquet: {path}")
    df = pd.read_parquet(path, columns=["ticker", "date", "open", "high", "low", "close"])
    out = {}
    for t in tickers:
        sub = df[df["ticker"] == t].copy()
        if sub.empty:
            raise SystemExit(f"no bars for {t}")
        sub["date"] = pd.to_datetime(sub["date"]).dt.normalize()
        out[t] = sub.sort_values("date").reset_index(drop=True)
        print(f"[load] {t}: {len(out[t])} rows  {out[t]['date'].iloc[0].date()} -> {out[t]['date'].iloc[-1].date()}",
              file=sys.stderr)
    return out


def compute_state(qqq: pd.DataFrame) -> pd.DataFrame:
    df = qqq[["date", "open", "high", "low", "close"]].copy()
    df["prev_close"] = df["close"].shift(1)
    df["tr"] = np.maximum.reduce([
        df["high"] - df["low"],
        (df["high"] - df["prev_close"]).abs(),
        (df["low"] - df["prev_close"]).abs(),
    ])
    df["atr22"] = df["tr"].rolling(ATR_WINDOW, min_periods=ATR_WINDOW).mean()
    df["hh22"] = df["close"].rolling(HH_WINDOW, min_periods=HH_WINDOW).max()
    df["stop"] = df["hh22"] - ATR_MULT * df["atr22"]

    state = []
    s = None
    for _, row in df.iterrows():
        if pd.isna(row["stop"]):
            state.append(None); continue
        if s is None:
            s = "TQQQ" if row["close"] > row["stop"] else "BIL"
        else:
            if row["close"] > row["stop"]:
                s = "TQQQ"
            elif row["close"] < row["stop"]:
                s = "BIL"
            # close == stop: keep prior state
        state.append(s)
    df["state"] = state
    return df[["date", "close", "atr22", "hh22", "stop", "state"]].rename(columns={"close": "qqq_close"})


def run(start: pd.Timestamp, end: pd.Timestamp, bars: dict[str, pd.DataFrame],
        states: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    sig = states[(states["date"] >= start) & (states["date"] <= end)].copy().reset_index(drop=True)
    sig["target"] = sig["state"].shift(1)
    sig.loc[0, "target"] = sig["state"].iloc[0]

    bar_lookup = {t: bars[t].set_index("date")[["open", "close"]].to_dict(orient="index")
                  for t in ("TQQQ", "BIL")}

    equity, held, flips = 1.0, sig["target"].iloc[0], 0
    eq_curve, held_history = [], []
    prev_date = None

    for i, row in sig.iterrows():
        d = row["date"]
        target = row["target"]
        if prev_date is None:
            o = bar_lookup[held].get(d, {}).get("open")
            c = bar_lookup[held].get(d, {}).get("close")
            if o and c and o > 0:
                equity *= c / o
            equity *= (1.0 - COST_PER_FLIP)
            flips += 1
            eq_curve.append(equity); held_history.append(held); prev_date = d
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
        eq_curve.append(equity); held_history.append(held); prev_date = d

    sig["equity"] = eq_curve
    sig["held"] = held_history
    sig["daily_ret"] = pd.Series(eq_curve).pct_change().fillna(0.0).values

    spy_bh = bars["SPY"].set_index("date")["close"].reindex(sig["date"]).ffill()
    qqq_bh = bars["QQQ"].set_index("date")["close"].reindex(sig["date"]).ffill()
    tqqq_bh = bars["TQQQ"].set_index("date")["close"].reindex(sig["date"]).ffill()
    sig["spy_bh"] = spy_bh.values / spy_bh.iloc[0]
    sig["qqq_bh"] = qqq_bh.values / qqq_bh.iloc[0]
    sig["tqqq_bh"] = tqqq_bh.values / tqqq_bh.iloc[0]

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
        "pct_days_in_TQQQ": round(float((np.array(held_history) == "TQQQ").mean()), 4),
        "pct_days_in_BIL": round(float((np.array(held_history) == "BIL").mean()), 4),
        "spy_bh_total_return": round(float(sig["spy_bh"].iloc[-1] - 1.0), 4),
        "qqq_bh_total_return": round(float(sig["qqq_bh"].iloc[-1] - 1.0), 4),
        "tqqq_bh_total_return": round(float(sig["tqqq_bh"].iloc[-1] - 1.0), 4),
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

    bars = load_etf_bars(["SPY", "QQQ", "TQQQ", "BIL"])
    states = compute_state(bars["QQQ"])
    first_valid = states.dropna(subset=["stop"])["date"].iloc[0]
    requested = pd.Timestamp(args.start_date)
    start = max(requested, first_valid)
    end = pd.Timestamp(args.end_date)
    print(f"[run] requested start={requested.date()}  actual start={start.date()}  end={end.date()}",
          file=sys.stderr)

    sig, m = run(start, end, bars, states)
    sig.to_csv(out_dir / "chandelier_exit_tqqq_curve.csv", index=False)
    (out_dir / "summary.json").write_text(json.dumps({"metrics": m, "config": {
        "atr_window": ATR_WINDOW, "hh_window": HH_WINDOW, "atr_mult": ATR_MULT,
        "cost_per_flip_bps": COST_PER_FLIP * 10000,
    }}, indent=2))

    lines = [
        "=" * 70,
        "Chandelier Exit on QQQ -> TQQQ / BIL",
        "=" * 70,
        f"Window:        {m['start_date']} -> {m['end_date']}  ({m['years']}y, {m['n_days']} days)",
        f"Stop:          HH(close,{HH_WINDOW}) - {ATR_MULT}*ATR({ATR_WINDOW})",
        f"Cost/flip:     {COST_PER_FLIP*10000:.0f} bp",
        "",
        f"Total return:  {m['total_return']*100:>8.2f}%",
        f"CAGR:          {m['cagr']*100:>8.2f}%",
        f"Max drawdown:  {m['max_drawdown']*100:>8.2f}%",
        f"Sharpe:        {m['sharpe']:>8.3f}",
        f"Flips:         {m['flips']}",
        f"% days TQQQ:   {m['pct_days_in_TQQQ']*100:>5.1f}%",
        f"% days  BIL:   {m['pct_days_in_BIL']*100:>5.1f}%",
        "",
        f"SPY  buy/hold: {m['spy_bh_total_return']*100:>8.2f}%",
        f"QQQ  buy/hold: {m['qqq_bh_total_return']*100:>8.2f}%",
        f"TQQQ buy/hold: {m['tqqq_bh_total_return']*100:>8.2f}%",
        f"vs 2x SPY:     {(m['total_return'] / (2 * m['spy_bh_total_return'])):.2f}x of target",
        f"vs 2x QQQ:     {(m['total_return'] / (2 * m['qqq_bh_total_return'])):.2f}x of target",
    ]
    txt = "\n".join(lines)
    (out_dir / "summary.txt").write_text(txt)
    print(txt)


if __name__ == "__main__":
    main()
