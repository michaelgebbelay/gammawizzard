#!/usr/bin/env python3
"""
Non-MA regime strategies for TQQQ/BIL rotation.

Pause on MA-tuning variants. The question this script answers:
  Can a regime model that does NOT use moving averages reproduce or beat
  C1-HYST's edge? If yes, the thesis ("leveraged Nasdaq only during
  favorable regimes") is more durable. If only SMA150/SMA200 specs win,
  C1-HYST is plausibly a tuned wrapper around one favorable cycle.

Strategies (signal asset = QQQ, sleeves = TQQQ/BIL):
  R1   ret-only momentum
       enter:  ret63>0 AND ret126>0
       exit:   ret63<0 AND ret21<0
  D1   short drawdown regime
       enter:  dd_from_63d_high > -5%   AND ret63>0
       exit:   dd_from_63d_high <= -10%
  D2   long drawdown regime
       enter:  dd_from_126d_high > -6%  AND ret63>0
       exit:   dd_from_126d_high <= -12%
  V1   volatility expansion filter
       enter:  ret63>0 AND vol21 < vol63 * 1.25
       exit:   vol21 > vol63 * 1.50 OR ret63<0
  S1   ret + vol composite score (HYST-shaped, no MAs)
       A=ret63>0, B=ret126>0, C=vol21<vol63*1.25
       score = A+B+C
       enter score==3; hold score>=2; exit score<=1
  VX1  VIX level regime
       enter:  ret63>0 AND VIX<25
       exit:   VIX>30 OR ret63<0

Benchmark: C1-HYST (the locked challenger), included for direct comparison.

Windows:
  L: 2011-01-03 -> 2026-04-29  (15-year, includes 2011/2015/2018/2020/2022 corrections)
  A: 2021-01-04 -> 2026-04-29
  B: 2022-01-03 -> 2026-04-29
  C: 2022-02-11 -> 2026-04-29

Slippage cases (per flip event): 3, 10, 25, 50 bps.
Trade execution: signal at close[t], execute at open[t+1].
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

TICKERS = ["SPY", "QQQ", "TQQQ", "BIL"]
DATA_START = "2010-01-01"   # earlier so indicators warm up before window L
DATA_END = "2026-12-31"

WINDOWS = {
    "L": ("2011-01-03", "2026-04-29"),
    "A": ("2021-01-04", "2026-04-29"),
    "B": ("2022-01-03", "2026-04-29"),
    "C": ("2022-02-11", "2026-04-29"),
}
SLIPPAGE_BPS = [3, 10, 25, 50]
VARIANTS = ["R1", "D1", "D2", "V1", "S1", "VX1", "C1-HYST"]
START_VALUE = 100_000.0


# ---------------------------------------------------------------------------
# data
# ---------------------------------------------------------------------------

def load_bars(use_cache: bool = True) -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
    cache_eq = DATA_DIR / "yf_adjusted_ohlc_2010.parquet"
    cache_vix = DATA_DIR / "yf_vix_2010.parquet"
    if use_cache and cache_eq.exists() and cache_vix.exists():
        df = pd.read_parquet(cache_eq)
        vix = pd.read_parquet(cache_vix)
        print(f"[data] cache hit: equities {len(df)} rows, vix {len(vix)} rows", file=sys.stderr)
    else:
        import yfinance as yf
        print(f"[data] downloading {TICKERS} + ^VIX from yfinance ...", file=sys.stderr)
        rows = []
        for t in TICKERS:
            raw = yf.Ticker(t).history(start=DATA_START, end=DATA_END, auto_adjust=True)
            if raw.empty:
                raise SystemExit(f"yfinance returned no data for {t}")
            r = raw.reset_index()
            r["date"] = pd.to_datetime(r["Date"]).dt.tz_localize(None).dt.normalize()
            r = r[["date", "Open", "High", "Low", "Close"]].rename(
                columns={"Open": "open", "High": "high", "Low": "low", "Close": "close"})
            r["ticker"] = t
            rows.append(r)
        df = pd.concat(rows, ignore_index=True).dropna()
        cache_eq.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(cache_eq, index=False)

        vraw = yf.Ticker("^VIX").history(start=DATA_START, end=DATA_END, auto_adjust=False)
        if vraw.empty:
            raise SystemExit("yfinance returned no data for ^VIX")
        vix = vraw.reset_index()[["Date", "Close"]].rename(
            columns={"Date": "date", "Close": "vix"})
        vix["date"] = pd.to_datetime(vix["date"]).dt.tz_localize(None).dt.normalize()
        vix.to_parquet(cache_vix, index=False)
        print(f"[data] wrote caches: {cache_eq.name}, {cache_vix.name}", file=sys.stderr)

    out = {}
    for t in TICKERS:
        sub = df[df["ticker"] == t].copy().sort_values("date").reset_index(drop=True)
        out[t] = sub
        print(f"[data] {t}: {len(sub)} rows  {sub['date'].iloc[0].date()} -> {sub['date'].iloc[-1].date()}",
              file=sys.stderr)
    print(f"[data] VIX: {len(vix)} rows  {vix['date'].iloc[0].date()} -> {vix['date'].iloc[-1].date()}",
          file=sys.stderr)
    return out, vix


# ---------------------------------------------------------------------------
# indicators
# ---------------------------------------------------------------------------

def compute_indicators(qqq: pd.DataFrame, vix: pd.DataFrame) -> pd.DataFrame:
    df = qqq[["date", "open", "high", "low", "close"]].copy()

    # SMAs (only used by C1-HYST benchmark)
    df["sma50"] = df["close"].rolling(50, min_periods=50).mean()
    df["sma150"] = df["close"].rolling(150, min_periods=150).mean()
    df["sma200"] = df["close"].rolling(200, min_periods=200).mean()

    # multi-horizon trailing returns
    df["ret21"] = df["close"].pct_change(21)
    df["ret63"] = df["close"].pct_change(63)
    df["ret126"] = df["close"].pct_change(126)

    # drawdowns from rolling highs
    df["hh63"] = df["close"].rolling(63, min_periods=63).max()
    df["dd63"] = df["close"] / df["hh63"] - 1.0
    df["hh126"] = df["close"].rolling(126, min_periods=126).max()
    df["dd126"] = df["close"] / df["hh126"] - 1.0

    # realized vol (annualized) from close-to-close log returns
    logret = np.log(df["close"] / df["close"].shift(1))
    df["vol21"] = logret.rolling(21, min_periods=21).std() * np.sqrt(252)
    df["vol63"] = logret.rolling(63, min_periods=63).std() * np.sqrt(252)

    # VIX join (forward-fill across non-overlapping holidays)
    vmap = vix.set_index("date")["vix"]
    df["vix"] = df["date"].map(vmap).ffill()

    return df


# ---------------------------------------------------------------------------
# state machines
# ---------------------------------------------------------------------------

def _ready(row: pd.Series, *cols) -> bool:
    return not any(pd.isna(row[c]) for c in cols)


def state_r1(ind: pd.DataFrame) -> list:
    out, s = [], None
    for _, r in ind.iterrows():
        if not _ready(r, "ret21", "ret63", "ret126"):
            out.append(None); continue
        enter = (r["ret63"] > 0) and (r["ret126"] > 0)
        exit_cond = (r["ret63"] < 0) and (r["ret21"] < 0)
        if s is None:
            s = "TQQQ" if enter else "BIL"
        else:
            if s == "TQQQ" and exit_cond:
                s = "BIL"
            elif s == "BIL" and enter:
                s = "TQQQ"
        out.append(s)
    return out


def _state_dd(ind: pd.DataFrame, dd_col: str, enter_threshold: float,
              exit_threshold: float) -> list:
    """enter when dd > enter_threshold AND ret63>0; exit when dd <= exit_threshold."""
    out, s = [], None
    for _, r in ind.iterrows():
        if not _ready(r, dd_col, "ret63"):
            out.append(None); continue
        enter = (r[dd_col] > enter_threshold) and (r["ret63"] > 0)
        exit_cond = r[dd_col] <= exit_threshold
        if s is None:
            s = "TQQQ" if enter else "BIL"
        else:
            if s == "TQQQ" and exit_cond:
                s = "BIL"
            elif s == "BIL" and enter:
                s = "TQQQ"
        out.append(s)
    return out


def state_d1(ind):  return _state_dd(ind, "dd63",  -0.05, -0.10)
def state_d2(ind):  return _state_dd(ind, "dd126", -0.06, -0.12)


def state_v1(ind: pd.DataFrame) -> list:
    out, s = [], None
    for _, r in ind.iterrows():
        if not _ready(r, "ret63", "vol21", "vol63"):
            out.append(None); continue
        enter = (r["ret63"] > 0) and (r["vol21"] < r["vol63"] * 1.25)
        exit_cond = (r["vol21"] > r["vol63"] * 1.50) or (r["ret63"] < 0)
        if s is None:
            s = "TQQQ" if enter else "BIL"
        else:
            if s == "TQQQ" and exit_cond:
                s = "BIL"
            elif s == "BIL" and enter:
                s = "TQQQ"
        out.append(s)
    return out


def state_s1(ind: pd.DataFrame) -> list:
    out, s = [], None
    for _, r in ind.iterrows():
        if not _ready(r, "ret63", "ret126", "vol21", "vol63"):
            out.append(None); continue
        A = int(r["ret63"] > 0)
        B = int(r["ret126"] > 0)
        C = int(r["vol21"] < r["vol63"] * 1.25)
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


def state_vx1(ind: pd.DataFrame) -> list:
    out, s = [], None
    for _, r in ind.iterrows():
        if not _ready(r, "ret63", "vix"):
            out.append(None); continue
        enter = (r["ret63"] > 0) and (r["vix"] < 25.0)
        exit_cond = (r["vix"] > 30.0) or (r["ret63"] < 0)
        if s is None:
            s = "TQQQ" if enter else "BIL"
        else:
            if s == "TQQQ" and exit_cond:
                s = "BIL"
            elif s == "BIL" and enter:
                s = "TQQQ"
        out.append(s)
    return out


def state_c1_hyst(ind: pd.DataFrame) -> list:
    out, s = [], None
    for _, r in ind.iterrows():
        if not _ready(r, "sma200", "sma150", "sma50", "ret63"):
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
        out.append(s)
    return out


def compute_states(ind: pd.DataFrame) -> dict[str, list]:
    return {
        "R1": state_r1(ind),
        "D1": state_d1(ind),
        "D2": state_d2(ind),
        "V1": state_v1(ind),
        "S1": state_s1(ind),
        "VX1": state_vx1(ind),
        "C1-HYST": state_c1_hyst(ind),
    }


# ---------------------------------------------------------------------------
# simulation (signal at close[t] -> execute at open[t+1])
# ---------------------------------------------------------------------------

def simulate(states_full: list, ind: pd.DataFrame, bars: dict[str, pd.DataFrame],
             start: pd.Timestamp, end: pd.Timestamp, slippage_bps: int) -> dict:
    cost = slippage_bps / 10000.0
    mask = (ind["date"] >= start) & (ind["date"] <= end)
    sub_ind = ind[mask].reset_index(drop=True)
    sub_states = [states_full[i] for i in ind.index[mask]]

    # target on day t = state at close[t-1]
    targets = [sub_states[0] if sub_states[0] is not None else "BIL"]
    for i in range(1, len(sub_states)):
        prev = sub_states[i - 1]
        targets.append(prev if prev is not None else "BIL")

    bar_lookup = {t: bars[t].set_index("date")[["open", "close"]].to_dict(orient="index")
                  for t in ("TQQQ", "BIL")}

    equity = 1.0
    held = targets[0]
    flips = 0
    eq_curve, held_history, daily_returns = [], [], []
    trade_open = {"date": None, "sleeve": None, "entry_equity": None}
    trades = []
    prev_date = None

    for i in range(len(sub_ind)):
        d = sub_ind["date"].iloc[i]
        target = targets[i] or "BIL"
        if prev_date is None:
            o = bar_lookup[held].get(d, {}).get("open")
            c = bar_lookup[held].get(d, {}).get("close")
            if o and c and o > 0:
                equity *= c / o
            equity *= (1.0 - cost)
            flips += 1
            trade_open = {"date": d, "sleeve": held, "entry_equity": equity}
            eq_curve.append(equity); held_history.append(held)
            daily_returns.append(equity - 1.0)
            prev_date = d
            continue
        prev_eq = equity
        if target != held:
            old_open = bar_lookup[held].get(d, {}).get("open")
            old_prev_close = bar_lookup[held].get(prev_date, {}).get("close")
            if old_open and old_prev_close and old_prev_close > 0:
                equity *= old_open / old_prev_close
            equity *= (1.0 - cost)
            new_open = bar_lookup[target].get(d, {}).get("open")
            new_close = bar_lookup[target].get(d, {}).get("close")
            if new_open and new_close and new_open > 0:
                equity *= new_close / new_open
            trades.append({
                "entry_date": str(trade_open["date"].date()) if trade_open["date"] is not None else None,
                "exit_date": str(d.date()),
                "sleeve": trade_open["sleeve"],
                "entry_eq": trade_open["entry_equity"],
                "hold_days": int((d - trade_open["date"]).days) if trade_open["date"] is not None else None,
            })
            trade_open = {"date": d, "sleeve": target, "entry_equity": equity}
            held = target
            flips += 1
        else:
            cprev = bar_lookup[held].get(prev_date, {}).get("close")
            ccur = bar_lookup[held].get(d, {}).get("close")
            if cprev and ccur and cprev > 0:
                equity *= ccur / cprev
        eq_curve.append(equity); held_history.append(held)
        daily_returns.append(equity / prev_eq - 1.0)
        prev_date = d

    if trade_open["entry_equity"] is not None:
        trades.append({
            "entry_date": str(trade_open["date"].date()) if trade_open["date"] is not None else None,
            "exit_date": str(sub_ind["date"].iloc[-1].date()),
            "sleeve": trade_open["sleeve"],
            "entry_eq": trade_open["entry_equity"],
            "hold_days": int((sub_ind["date"].iloc[-1] - trade_open["date"]).days)
            if trade_open["date"] is not None else None,
        })
    eq_by_date = dict(zip(sub_ind["date"], eq_curve))
    enriched_trades = []
    for t_ in trades:
        exit_eq = eq_by_date.get(pd.to_datetime(t_["exit_date"]))
        t_["pnl_pct"] = (exit_eq / t_["entry_eq"] - 1.0) if exit_eq and t_["entry_eq"] else None
        enriched_trades.append(t_)

    spy_bh = bars["SPY"].set_index("date")["close"].reindex(sub_ind["date"]).ffill()
    qqq_bh = bars["QQQ"].set_index("date")["close"].reindex(sub_ind["date"]).ffill()
    tqqq_bh = bars["TQQQ"].set_index("date")["close"].reindex(sub_ind["date"]).ffill()
    spy_ret = float(spy_bh.iloc[-1] / spy_bh.iloc[0] - 1.0)
    qqq_ret = float(qqq_bh.iloc[-1] / qqq_bh.iloc[0] - 1.0)
    tqqq_ret = float(tqqq_bh.iloc[-1] / tqqq_bh.iloc[0] - 1.0)

    eq = np.asarray(eq_curve)
    dr = np.asarray(daily_returns)
    days = len(eq)
    years = (sub_ind["date"].iloc[-1] - sub_ind["date"].iloc[0]).days / 365.25
    cagr = float(eq[-1] ** (1 / years) - 1.0) if years > 0 else float("nan")
    rolling_max = np.maximum.accumulate(eq)
    drawdown = eq / rolling_max - 1.0
    max_dd = float(drawdown.min())
    daily_std = float(np.nanstd(dr, ddof=1))
    daily_mean = float(np.nanmean(dr))
    sharpe = float(daily_mean / daily_std * np.sqrt(252)) if daily_std > 0 else None
    downside = dr[dr < 0]
    downside_std = float(np.nanstd(downside, ddof=1)) if len(downside) > 1 else None
    sortino = float(daily_mean / downside_std * np.sqrt(252)) if downside_std and downside_std > 0 else None
    calmar = float(cagr / abs(max_dd)) if max_dd < 0 else None

    held_arr = np.asarray(held_history)
    pct_tqqq = float((held_arr == "TQQQ").mean())
    pct_bil = float((held_arr == "BIL").mean())
    runs, cur, cur_len = [], held_arr[0], 1
    for x in held_arr[1:]:
        if x == cur:
            cur_len += 1
        else:
            runs.append(cur_len); cur_len = 1; cur = x
    runs.append(cur_len)
    avg_hold = float(np.mean(runs)) if runs else None

    worst_trade = float(min((t_["pnl_pct"] for t_ in enriched_trades
                             if t_["pnl_pct"] is not None), default=None) or 0.0)
    worst_day = float(np.nanmin(dr)) if len(dr) else None

    df_ret = pd.DataFrame({"date": sub_ind["date"], "eq": eq_curve})
    df_ret["year"] = df_ret["date"].dt.year
    df_ret["ym"] = df_ret["date"].dt.to_period("M").astype(str)
    annual = df_ret.groupby("year")["eq"].agg(["first", "last"])
    annual["ret"] = annual["last"] / annual["first"] - 1.0
    monthly = df_ret.groupby("ym")["eq"].agg(["first", "last"])
    monthly["ret"] = monthly["last"] / monthly["first"] - 1.0

    total_return = float(eq[-1] - 1.0)
    final_value = START_VALUE * eq[-1]
    target_2x_ret = 2.0 * spy_ret
    pass_2x_ret = bool(total_return >= target_2x_ret)
    target_2x_value = START_VALUE * (1.0 + 2.0 * spy_ret)
    pass_2x_value = bool(final_value >= target_2x_value)

    return {
        "total_return": round(total_return, 4),
        "final_value": round(final_value, 2),
        "cagr": round(cagr, 4),
        "max_drawdown": round(max_dd, 4),
        "sharpe": round(sharpe, 3) if sharpe is not None else None,
        "sortino": round(sortino, 3) if sortino is not None else None,
        "calmar": round(calmar, 3) if calmar is not None else None,
        "flips": int(flips),
        "n_trades_closed": len([t_ for t_ in enriched_trades if t_["exit_date"] is not None]),
        "avg_hold_days_calendar": round(avg_hold, 1) if avg_hold is not None else None,
        "pct_days_in_TQQQ": round(pct_tqqq, 4),
        "pct_days_in_BIL": round(pct_bil, 4),
        "worst_trade": round(worst_trade, 4),
        "worst_day_loss": round(worst_day, 4) if worst_day is not None else None,
        "spy_bh_total_return": round(spy_ret, 4),
        "qqq_bh_total_return": round(qqq_ret, 4),
        "tqqq_bh_total_return": round(tqqq_ret, 4),
        "spy_bh_final_value": round(START_VALUE * (1 + spy_ret), 2),
        "tqqq_bh_final_value": round(START_VALUE * (1 + tqqq_ret), 2),
        "return_multiple_vs_spy": round(total_return / spy_ret, 3) if spy_ret != 0 else None,
        "delta_vs_tqqq_bh_pct": round((total_return - tqqq_ret) * 100, 2),
        "pass_2x_spy_return": pass_2x_ret,
        "pass_2x_spy_final_value": pass_2x_value,
        "target_2x_spy_return": round(target_2x_ret, 4),
        "target_2x_spy_final_value": round(target_2x_value, 2),
        "n_days": int(days),
        "years": round(years, 3),
        "annual_returns": {int(y): round(float(v), 4) for y, v in annual["ret"].items()},
        "monthly_returns": {str(ym): round(float(v), 4) for ym, v in monthly["ret"].items()},
        "trades": enriched_trades,
    }


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--out-dir", default=str(RESULTS_DIR))
    p.add_argument("--no-cache", action="store_true")
    args = p.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    bars, vix = load_bars(use_cache=not args.no_cache)
    ind = compute_indicators(bars["QQQ"], vix)
    states_by_variant = compute_states(ind)

    needed_cols = ["sma200", "sma150", "sma50", "ret21", "ret63", "ret126",
                   "dd63", "dd126", "vol21", "vol63", "vix"]
    first_valid = ind.dropna(subset=needed_cols)["date"].iloc[0]
    print(f"[run] first day all indicators valid: {first_valid.date()}", file=sys.stderr)

    metrics_rows, annual_rows, monthly_rows = [], [], []
    full_results = {}

    for variant in VARIANTS:
        full_results[variant] = {}
        for w_label, (s, e) in WINDOWS.items():
            full_results[variant][w_label] = {}
            start = max(pd.Timestamp(s), first_valid)
            end = pd.Timestamp(e)
            for slip in SLIPPAGE_BPS:
                m = simulate(states_by_variant[variant], ind, bars, start, end, slip)
                key = f"{variant} | window {w_label} ({start.date()}->{end.date()}) | slip {slip}bp"
                print(f"[done] {key}: total={m['total_return']*100:+.2f}%  DD={m['max_drawdown']*100:.2f}%  "
                      f"flips={m['flips']}  pass_2x={m['pass_2x_spy_return']}", file=sys.stderr)
                full_results[variant][w_label][slip] = m
                row = {"variant": variant, "window": w_label, "slippage_bps": slip,
                       "start_date": str(start.date()), "end_date": str(end.date()),
                       **{k: v for k, v in m.items()
                          if k not in ("annual_returns", "monthly_returns", "trades")}}
                metrics_rows.append(row)
                for y, v in m["annual_returns"].items():
                    annual_rows.append({"variant": variant, "window": w_label,
                                        "slippage_bps": slip, "year": y, "return": v})
                for ym, v in m["monthly_returns"].items():
                    monthly_rows.append({"variant": variant, "window": w_label,
                                         "slippage_bps": slip, "month": ym, "return": v})
                tdf = pd.DataFrame(m["trades"])
                if not tdf.empty:
                    tdf.to_csv(out_dir / f"trades_{variant}_{w_label}_{slip}bp.csv", index=False)

    pd.DataFrame(metrics_rows).to_csv(out_dir / "metrics.csv", index=False)
    pd.DataFrame(annual_rows).to_csv(out_dir / "annual_returns.csv", index=False)
    pd.DataFrame(monthly_rows).to_csv(out_dir / "monthly_returns.csv", index=False)
    (out_dir / "summary.json").write_text(json.dumps({
        "config": {
            "tickers": TICKERS,
            "data_source": "yfinance auto_adjust=True (split + dividend adjusted)",
            "windows": WINDOWS,
            "slippage_bps_cases": SLIPPAGE_BPS,
            "starting_value": START_VALUE,
            "variants": VARIANTS,
        },
        "results": full_results,
    }, indent=2, default=str))

    write_summary(out_dir, full_results)
    print(f"[done] wrote artifacts to {out_dir}", file=sys.stderr)


def write_summary(out_dir: Path, results: dict) -> None:
    lines = []
    lines.append("=" * 100)
    lines.append("Non-MA Regime Strategies for TQQQ/BIL")
    lines.append("=" * 100)
    lines.append("Variants:  R1 (ret only) | D1, D2 (drawdown) | V1 (vol) | "
                 "S1 (ret+vol score) | VX1 (VIX) | C1-HYST (benchmark)")
    lines.append("Windows:   L=2011-01-03..2026-04-29  A=2021-01-04..  B=2022-01-03..  C=2022-02-11..")
    lines.append("Slippage:  3 / 10 / 25 / 50 bp per flip")
    lines.append("")

    # benchmarks per window
    lines.append("Benchmarks (buy & hold):")
    lines.append(f"  {'window':<6} {'SPY':>10} {'QQQ':>10} {'TQQQ':>10} {'2x SPY':>12}")
    for w in WINDOWS.keys():
        m = results["R1"][w][3]
        lines.append(f"  {w:<6} {m['spy_bh_total_return']*100:>9.2f}% {m['qqq_bh_total_return']*100:>9.2f}% "
                     f"{m['tqqq_bh_total_return']*100:>9.2f}% {m['target_2x_spy_return']*100:>11.2f}%")
    lines.append("")

    # main table per window: variant rows × slippage cols (total return)
    for w in WINDOWS.keys():
        lines.append(f"--- Window {w}: total return by variant × slippage ---")
        lines.append(f"  {'variant':<10}  {'3bp':>9} {'10bp':>9} {'25bp':>9} {'50bp':>9}  "
                     f"{'maxDD@3':>9} {'Sharpe@3':>9} {'flips@3':>8}  {'pass2x@10':>10}")
        for v in VARIANTS:
            cells = []
            for s in SLIPPAGE_BPS:
                m = results[v][w][s]
                cells.append(f"{m['total_return']*100:>+8.2f}%")
            m3 = results[v][w][3]
            m10 = results[v][w][10]
            sh = "n/a" if m3["sharpe"] is None else f"{m3['sharpe']:>9.2f}"
            lines.append(f"  {v:<10} " + " ".join(cells) +
                         f"   {m3['max_drawdown']*100:>8.2f}% {sh:>9} "
                         f"{m3['flips']:>8d}  {str(m10['pass_2x_spy_return']):>10}")
        lines.append("")

    # head-to-head vs C1-HYST in window L (the longer one)
    lines.append("--- Window L (2011-2026): edge vs C1-HYST at 10bp slippage ---")
    base = results["C1-HYST"]["L"][10]
    lines.append(f"  C1-HYST baseline: total {base['total_return']*100:+.2f}%  "
                 f"DD {base['max_drawdown']*100:.2f}%  Sharpe {base['sharpe']:.2f}  "
                 f"flips {base['flips']}")
    for v in VARIANTS:
        if v == "C1-HYST":
            continue
        m = results[v]["L"][10]
        delta = (m["total_return"] - base["total_return"]) * 100
        lines.append(f"  {v:<10}  total {m['total_return']*100:+.2f}%  "
                     f"DD {m['max_drawdown']*100:.2f}%  Sharpe {(m['sharpe'] or 0):.2f}  "
                     f"flips {m['flips']}  Δ_vs_HYST {delta:+.2f}pp")
    lines.append("")

    # passes 2x SPY at 10bp count
    lines.append("--- # passes (total return >= 2x SPY) at 10bp, by variant ---")
    for v in VARIANTS:
        passes = sum(1 for w in WINDOWS if results[v][w][10]["pass_2x_spy_return"])
        lines.append(f"  {v:<10}  {passes}/{len(WINDOWS)}")
    lines.append("")

    (out_dir / "summary.txt").write_text("\n".join(lines))


if __name__ == "__main__":
    main()
