#!/usr/bin/env python3
"""
QQQ hard-trend -> TQQQ/BIL.

Asymmetric three-condition state machine on QQQ as the signal proxy.
Tradable sleeves: TQQQ (3x) and BIL (cash). No 1x middle ground.

Entry to TQQQ (all three must hold):
    QQQ.close > QQQ.SMA200
    QQQ.SMA50 > QQQ.SMA200
    QQQ 63-day return > 0

Exit to BIL (any one triggers):
    QQQ.close < QQQ.SMA150
    QQQ.SMA50 < QQQ.SMA200
    QQQ 63-day return < 0

Initial state at first valid signal day: TQQQ if entry condition holds, else BIL.
Signal computed at close, trade executed at next open.
Cost: 3 bp per flip (1 bp commission + 2 bp slippage, combined).
TQQQ ER and BIL yield are baked into ETF NAV / price, not re-applied.

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

SMA_FAST = 50
SMA_MED = 150
SMA_SLOW = 200
RET_WINDOW = 63
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
    df = qqq[["date", "close"]].rename(columns={"close": "qqq_close"}).copy()
    df["sma50"] = df["qqq_close"].rolling(SMA_FAST, min_periods=SMA_FAST).mean()
    df["sma150"] = df["qqq_close"].rolling(SMA_MED, min_periods=SMA_MED).mean()
    df["sma200"] = df["qqq_close"].rolling(SMA_SLOW, min_periods=SMA_SLOW).mean()
    df["ret63"] = df["qqq_close"].pct_change(RET_WINDOW)

    df["enter"] = (
        (df["qqq_close"] > df["sma200"])
        & (df["sma50"] > df["sma200"])
        & (df["ret63"] > 0)
    )
    df["exit"] = (
        (df["qqq_close"] < df["sma150"])
        | (df["sma50"] < df["sma200"])
        | (df["ret63"] < 0)
    )
    df["valid"] = df["sma200"].notna() & df["sma150"].notna() & df["sma50"].notna() & df["ret63"].notna()

    state, s = [], None
    for _, row in df.iterrows():
        if not row["valid"]:
            state.append(None)
            continue
        if s is None:
            s = "TQQQ" if row["enter"] else "BIL"
        else:
            if s == "TQQQ" and bool(row["exit"]):
                s = "BIL"
            elif s == "BIL" and bool(row["enter"]):
                s = "TQQQ"
        state.append(s)
    df["state"] = state
    return df


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
            equity *= (1.0 - COST_PER_FLIP)  # initial entry counts as one flip
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

    qqq_bh = bars["QQQ"].set_index("date")["close"].reindex(sig["date"]).ffill()
    spy_bh = bars["SPY"].set_index("date")["close"].reindex(sig["date"]).ffill()
    tqqq_bh = bars["TQQQ"].set_index("date")["close"].reindex(sig["date"]).ffill()
    sig["qqq_bh"] = qqq_bh.values / qqq_bh.iloc[0]
    sig["spy_bh"] = spy_bh.values / spy_bh.iloc[0]
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
        "pct_days_in_tqqq": round(float((np.array(held_history) == "TQQQ").mean()), 4),
        "pct_days_in_bil": round(float((np.array(held_history) == "BIL").mean()), 4),
        "qqq_bh_total_return": round(float(sig["qqq_bh"].iloc[-1] - 1.0), 4),
        "spy_bh_total_return": round(float(sig["spy_bh"].iloc[-1] - 1.0), 4),
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
    first_valid = states.dropna(subset=["sma200", "sma150", "sma50", "ret63"])["date"].iloc[0]
    requested = pd.Timestamp(args.start_date)
    start = max(requested, first_valid)
    end = pd.Timestamp(args.end_date)
    print(f"[run] requested start={requested.date()}  actual start={start.date()}  end={end.date()}",
          file=sys.stderr)

    sig, m = run(start, end, bars, states)
    sig.to_csv(out_dir / "qqq_hard_trend_tqqq_curve.csv", index=False)
    (out_dir / "summary.json").write_text(json.dumps({"metrics": m, "config": {
        "sma_fast": SMA_FAST, "sma_med": SMA_MED, "sma_slow": SMA_SLOW,
        "ret_window": RET_WINDOW, "cost_per_flip_bps": COST_PER_FLIP * 10000,
    }}, indent=2))

    lines = [
        "=" * 70,
        "QQQ hard-trend -> TQQQ/BIL (binary, no 1x middle)",
        "=" * 70,
        f"Window:        {m['start_date']} -> {m['end_date']}  ({m['years']}y, {m['n_days']} days)",
        f"Cost/flip:     {COST_PER_FLIP*10000:.0f} bp",
        f"Entry:  QQQ>SMA200 AND SMA50>SMA200 AND ret_63>0",
        f"Exit:   QQQ<SMA150 OR SMA50<SMA200 OR ret_63<0",
        "",
        f"Total return:  {m['total_return']*100:>8.2f}%",
        f"CAGR:          {m['cagr']*100:>8.2f}%",
        f"Max drawdown:  {m['max_drawdown']*100:>8.2f}%",
        f"Sharpe:        {m['sharpe']:>8.3f}",
        f"Flips:         {m['flips']}",
        f"% days TQQQ:   {m['pct_days_in_tqqq']*100:>5.1f}%",
        f"% days BIL:    {m['pct_days_in_bil']*100:>5.1f}%",
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
