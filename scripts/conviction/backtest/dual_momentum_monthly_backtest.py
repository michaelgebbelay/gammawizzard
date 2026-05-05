#!/usr/bin/env python3
"""
6-Month Leveraged Dual Momentum, monthly rotation.

Signal generators: SPY, QQQ, BIL (6-month / 126 trading-day total return).
Execution sleeves (2x variant): SSO / QLD / BIL
Execution sleeves (3x variant): UPRO / TQQQ / BIL

Rules (evaluated at close of last trading day of each calendar month;
trade executed at next open):
    1. If SPY.ret_126 < BIL.ret_126 AND QQQ.ret_126 < BIL.ret_126 -> 100% BIL
    2. Else if QQQ.ret_126 > SPY.ret_126 -> 100% leveraged-Nasdaq sleeve
    3. Else -> 100% leveraged-S&P sleeve

Cost: 3 bp per flip event (1 bp commission + 2 bp slippage).
ETF expense ratios are baked into the actual NAV / price series, not added again.

Output: one summary + per-variant equity curve under --out-dir.
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

LOOKBACK = 126                 # ~6 months of trading days
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


def month_end_dates(all_dates: pd.Series) -> list[pd.Timestamp]:
    """Return the last trading day of each calendar month present in `all_dates`."""
    s = pd.Series(all_dates.values, index=pd.DatetimeIndex(all_dates))
    grouped = s.groupby([s.index.year, s.index.month]).max()
    return [pd.Timestamp(d) for d in grouped.values]


def compute_targets(spy: pd.DataFrame, qqq: pd.DataFrame, bil: pd.DataFrame,
                    sleeve_2x: dict, decision_dates: list[pd.Timestamp]) -> pd.DataFrame:
    """For each decision date (month-end close), pick the sleeve to hold next month."""
    spy_c = spy.set_index("date")["close"]
    qqq_c = qqq.set_index("date")["close"]
    bil_c = bil.set_index("date")["close"]
    rows = []
    for d in decision_dates:
        if d not in spy_c.index:
            continue
        idx = spy_c.index.get_loc(d)
        if idx < LOOKBACK:
            continue
        d_prev = spy_c.index[idx - LOOKBACK]
        spy_r = spy_c.loc[d] / spy_c.loc[d_prev] - 1.0
        qqq_r = qqq_c.loc[d] / qqq_c.loc[d_prev] - 1.0
        bil_r = bil_c.loc[d] / bil_c.loc[d_prev] - 1.0
        if spy_r < bil_r and qqq_r < bil_r:
            sleeve = "BIL"
        elif qqq_r > spy_r:
            sleeve = sleeve_2x["nasdaq"]
        else:
            sleeve = sleeve_2x["sp"]
        rows.append({
            "decision_date": d,
            "spy_ret_126": spy_r,
            "qqq_ret_126": qqq_r,
            "bil_ret_126": bil_r,
            "target": sleeve,
        })
    return pd.DataFrame(rows)


def run_variant(label: str, sleeves: dict, bars: dict[str, pd.DataFrame],
                start: pd.Timestamp, end: pd.Timestamp,
                decision_dates: list[pd.Timestamp]) -> tuple[pd.DataFrame, dict]:
    decisions = compute_targets(bars["SPY"], bars["QQQ"], bars["BIL"], sleeves, decision_dates)
    if decisions.empty:
        raise SystemExit(f"no valid decision dates for {label}")

    # filter to backtest window
    decisions = decisions[(decisions["decision_date"] >= start - pd.Timedelta(days=40))
                          & (decisions["decision_date"] <= end)].reset_index(drop=True)

    # build daily equity curve
    spy_dates = bars["SPY"]["date"].tolist()
    sleeve_set = {sleeves["nasdaq"], sleeves["sp"], "BIL"}
    bar_lookup = {t: bars[t].set_index("date")[["open", "close"]].to_dict(orient="index")
                  for t in sleeve_set}

    # for each trading day, the held sleeve = decision made on the last decision_date <= prev_trading_day
    dec_idx = decisions.set_index("decision_date")["target"]
    daily_target = []
    daily_dates = [d for d in spy_dates if start <= d <= end]
    for d in daily_dates:
        # signal was computed at close of latest decision_date < d (so decision_date strictly before d)
        prior = dec_idx.index[dec_idx.index < d]
        if len(prior) == 0:
            daily_target.append("BIL")
        else:
            daily_target.append(dec_idx.loc[prior[-1]])

    equity, held, flips = 1.0, daily_target[0], 0
    eq_curve, held_history = [], []
    prev_date = None

    for i, d in enumerate(daily_dates):
        target = daily_target[i]
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

    out = pd.DataFrame({"date": daily_dates, "target": daily_target, "held": held_history, "equity": eq_curve})
    out["daily_ret"] = out["equity"].pct_change().fillna(0.0)

    spy_bh = bars["SPY"].set_index("date")["close"].reindex(out["date"]).ffill()
    qqq_bh = bars["QQQ"].set_index("date")["close"].reindex(out["date"]).ffill()
    out["spy_bh"] = spy_bh.values / spy_bh.iloc[0]
    out["qqq_bh"] = qqq_bh.values / qqq_bh.iloc[0]

    eq = np.asarray(eq_curve)
    dr = out["daily_ret"].values
    years = (pd.Timestamp(out["date"].iloc[-1]) - pd.Timestamp(out["date"].iloc[0])).days / 365.25
    rolling_max = np.maximum.accumulate(eq)
    drawdown = eq / rolling_max - 1.0
    daily_std = float(np.nanstd(dr, ddof=1))
    nq, sp = sleeves["nasdaq"], sleeves["sp"]
    metrics = {
        "label": label,
        "start_date": str(pd.Timestamp(out["date"].iloc[0]).date()),
        "end_date": str(pd.Timestamp(out["date"].iloc[-1]).date()),
        "n_days": int(len(eq)),
        "years": round(years, 3),
        "total_return": round(float(eq[-1] - 1.0), 4),
        "cagr": round(float(eq[-1] ** (1.0 / years) - 1.0), 4) if years > 0 else None,
        "sharpe": round(float(np.nanmean(dr) / daily_std * np.sqrt(252)), 3) if daily_std > 0 else None,
        "max_drawdown": round(float(drawdown.min()), 4),
        "flips": int(flips),
        f"pct_days_in_{nq}": round(float((np.array(held_history) == nq).mean()), 4),
        f"pct_days_in_{sp}": round(float((np.array(held_history) == sp).mean()), 4),
        "pct_days_in_BIL": round(float((np.array(held_history) == "BIL").mean()), 4),
        "spy_bh_total_return": round(float(out["spy_bh"].iloc[-1] - 1.0), 4),
        "qqq_bh_total_return": round(float(out["qqq_bh"].iloc[-1] - 1.0), 4),
    }
    return out, metrics, decisions


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--start-date", default="2022-01-03")
    p.add_argument("--end-date", default="2026-05-01")
    p.add_argument("--out-dir", default=str(RESULTS_DIR))
    args = p.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    bars = load_etf_bars(["SPY", "QQQ", "BIL", "SSO", "QLD", "UPRO", "TQQQ"])

    decision_dates = month_end_dates(bars["SPY"]["date"])
    requested = pd.Timestamp(args.start_date)
    end = pd.Timestamp(args.end_date)
    print(f"[run] {len(decision_dates)} candidate month-end decision dates", file=sys.stderr)

    sleeves_2x = {"nasdaq": "QLD", "sp": "SSO"}
    sleeves_3x = {"nasdaq": "TQQQ", "sp": "UPRO"}

    # actual start = first day where 126-day return is computable
    spy_c = bars["SPY"].set_index("date")["close"]
    first_valid = spy_c.index[LOOKBACK]
    start = max(requested, first_valid)
    print(f"[run] requested start={requested.date()}  actual start={start.date()}  end={end.date()}",
          file=sys.stderr)

    out2x, m2x, dec2x = run_variant("2x_SSO_QLD", sleeves_2x, bars, start, end, decision_dates)
    out3x, m3x, dec3x = run_variant("3x_UPRO_TQQQ", sleeves_3x, bars, start, end, decision_dates)

    out2x.to_csv(out_dir / "dual_momentum_2x_curve.csv", index=False)
    out3x.to_csv(out_dir / "dual_momentum_3x_curve.csv", index=False)
    dec2x.to_csv(out_dir / "decisions_2x.csv", index=False)
    dec3x.to_csv(out_dir / "decisions_3x.csv", index=False)

    summary = {"variants": {"2x_SSO_QLD": m2x, "3x_UPRO_TQQQ": m3x},
               "config": {"lookback_days": LOOKBACK, "cost_per_flip_bps": COST_PER_FLIP * 10000}}
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2))

    def fmt(label, m, sleeves):
        nq, sp = sleeves["nasdaq"], sleeves["sp"]
        return [
            f"--- {label} ---",
            f"  Total return:  {m['total_return']*100:>8.2f}%",
            f"  CAGR:          {m['cagr']*100:>8.2f}%",
            f"  Max drawdown:  {m['max_drawdown']*100:>8.2f}%",
            f"  Sharpe:        {m['sharpe']:>8.3f}",
            f"  Flips:         {m['flips']}",
            f"  % days {nq:>4}: {m[f'pct_days_in_{nq}']*100:>5.1f}%",
            f"  % days {sp:>4}: {m[f'pct_days_in_{sp}']*100:>5.1f}%",
            f"  % days  BIL: {m['pct_days_in_BIL']*100:>5.1f}%",
            f"  vs 2x SPY (=2*{m['spy_bh_total_return']*100:.1f}%): "
            f"{m['total_return'] / (2 * m['spy_bh_total_return']):.2f}x of target",
            "",
        ]

    lines = [
        "=" * 70,
        "6-Month Leveraged Dual Momentum (monthly rotation)",
        "=" * 70,
        f"Window:   {m2x['start_date']} -> {m2x['end_date']} ({m2x['years']}y, {m2x['n_days']} days)",
        f"Lookback: {LOOKBACK} trading days (~6 months)",
        f"Cost:     {COST_PER_FLIP*10000:.0f} bp/flip",
        f"SPY B&H:  {m2x['spy_bh_total_return']*100:.2f}%   QQQ B&H: {m2x['qqq_bh_total_return']*100:.2f}%",
        "",
    ]
    lines += fmt("2x variant (SSO / QLD / BIL) -- per submission", m2x, sleeves_2x)
    lines += fmt("3x variant (UPRO / TQQQ / BIL) -- aggressive shadow", m3x, sleeves_3x)
    txt = "\n".join(lines)
    (out_dir / "summary.txt").write_text(txt)
    print(txt)


if __name__ == "__main__":
    main()
