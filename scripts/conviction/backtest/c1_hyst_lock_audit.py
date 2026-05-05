#!/usr/bin/env python3
"""
C1-HYST lock + audit run.

Strategies (signal: QQQ; sleeves: TQQQ, BIL):

  C1-HYST                  A = close > SMA150
                           B = SMA50 > SMA200
                           C = ret63 > 0
                           score = A + B + C
                           Enter (BIL->TQQQ) only when score == 3
                           Hold TQQQ when score in {2, 3}
                           Exit (TQQQ->BIL) when score <= 1

  C1-NORETEXIT             Enter (BIL->TQQQ): close > SMA200 AND SMA50 > SMA200 AND ret63 > 0
                           Exit  (TQQQ->BIL): close < SMA150 OR SMA50 < SMA200

  C1-HYST-STRICT-ENTRY     Entry: same strict gate as baseline C1
                              close > SMA200 AND SMA50 > SMA200 AND ret63 > 0
                           Exit / hold: HYST score logic
                              hold TQQQ if score in {2, 3}, exit if score <= 1
                           Audit purpose: isolates whether HYST's edge over
                           NORETEXIT comes from the *hold zone* (better exit
                           logic) or from the *looser entry* (close > SMA150
                           vs close > SMA200).

Windows:
  L: 2011-01-03 -> 2026-04-29   (long history)
  A: 2021-01-04 -> 2026-04-29
  B: 2022-01-03 -> 2026-04-29
  C: 2022-02-11 -> 2026-04-29

Slippage cases (per flip event, sell+buy combined): 3, 10, 25, 50 bps.

Data: yfinance auto_adjust=True (split + dividend adjusted) for SPY, QQQ,
TQQQ, BIL from 2010-01-01.

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
DATA_START = "2010-01-01"
DATA_END = "2026-12-31"

WINDOWS = {
    "L": ("2011-01-03", "2026-04-29"),
    "A": ("2021-01-04", "2026-04-29"),
    "B": ("2022-01-03", "2026-04-29"),
    "C": ("2022-02-11", "2026-04-29"),
}
SLIPPAGE_BPS = [3, 10, 25, 50]
VARIANTS = ["C1-HYST", "C1-NORETEXIT", "C1-HYST-STRICT-ENTRY"]
START_VALUE = 100_000.0


# ---------------------------------------------------------------------------
# data
# ---------------------------------------------------------------------------

def load_bars(use_cache: bool = True) -> dict[str, pd.DataFrame]:
    cache = DATA_DIR / "yf_adjusted_ohlc_long.parquet"
    if use_cache and cache.exists():
        df = pd.read_parquet(cache)
        print(f"[data] cache: {len(df)} rows  {df['date'].min().date()} -> {df['date'].max().date()}",
              file=sys.stderr)
    else:
        import yfinance as yf
        print(f"[data] fetching {TICKERS} from yfinance auto_adjust=True ...", file=sys.stderr)
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
        cache.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(cache, index=False)
        print(f"[data] wrote cache: {cache} ({len(df)} rows)", file=sys.stderr)

    out = {}
    for t in TICKERS:
        sub = df[df["ticker"] == t].copy().sort_values("date").reset_index(drop=True)
        out[t] = sub
        print(f"[data] {t}: {len(sub)} rows  {sub['date'].iloc[0].date()} -> {sub['date'].iloc[-1].date()}",
              file=sys.stderr)
    return out


# ---------------------------------------------------------------------------
# indicators
# ---------------------------------------------------------------------------

def compute_indicators(qqq: pd.DataFrame) -> pd.DataFrame:
    df = qqq[["date", "open", "high", "low", "close"]].copy()
    df["sma50"] = df["close"].rolling(50, min_periods=50).mean()
    df["sma150"] = df["close"].rolling(150, min_periods=150).mean()
    df["sma200"] = df["close"].rolling(200, min_periods=200).mean()
    df["ret63"] = df["close"].pct_change(63)
    return df


def _ready(row: pd.Series, *cols) -> bool:
    return not any(pd.isna(row[c]) for c in cols)


def state_hyst(ind: pd.DataFrame) -> list:
    """Pure HYST: entry score==3, hold score>=2, exit score<=1.
    Uses A=close>SMA150 for the close-vs-MA condition."""
    out = []
    s = None
    for _, r in ind.iterrows():
        if not _ready(r, "sma200", "sma150", "sma50", "ret63"):
            out.append(None); continue
        A = r["close"] > r["sma150"]
        B = r["sma50"] > r["sma200"]
        C = r["ret63"] > 0
        score = int(A) + int(B) + int(C)
        if s is None:
            s = "TQQQ" if score == 3 else "BIL"
        else:
            if s == "BIL" and score == 3:
                s = "TQQQ"
            elif s == "TQQQ" and score <= 1:
                s = "BIL"
            # score == 2 -> hold
        out.append(s)
    return out


def state_noretexit(ind: pd.DataFrame) -> list:
    """C1 entry, exit drops ret63<0."""
    out = []
    s = None
    for _, r in ind.iterrows():
        if not _ready(r, "sma200", "sma150", "sma50", "ret63"):
            out.append(None); continue
        enter = (r["close"] > r["sma200"]) and (r["sma50"] > r["sma200"]) and (r["ret63"] > 0)
        exit_cond = (r["close"] < r["sma150"]) or (r["sma50"] < r["sma200"])
        if s is None:
            s = "TQQQ" if enter else "BIL"
        else:
            if s == "TQQQ" and exit_cond:
                s = "BIL"
            elif s == "BIL" and enter:
                s = "TQQQ"
        out.append(s)
    return out


def state_hyst_strict_entry(ind: pd.DataFrame) -> list:
    """Strict baseline-C1 entry; HYST hold/exit logic for already-in TQQQ."""
    out = []
    s = None
    for _, r in ind.iterrows():
        if not _ready(r, "sma200", "sma150", "sma50", "ret63"):
            out.append(None); continue
        # strict entry: close > SMA200 AND SMA50 > SMA200 AND ret63 > 0
        strict_enter = (r["close"] > r["sma200"]) and (r["sma50"] > r["sma200"]) and (r["ret63"] > 0)
        # HYST score for hold/exit (uses SMA150 close gate)
        A = r["close"] > r["sma150"]
        B = r["sma50"] > r["sma200"]
        C = r["ret63"] > 0
        score = int(A) + int(B) + int(C)
        if s is None:
            s = "TQQQ" if strict_enter else "BIL"
        else:
            if s == "BIL" and strict_enter:
                s = "TQQQ"
            elif s == "TQQQ" and score <= 1:
                s = "BIL"
            # in TQQQ with score in {2,3}: hold
        out.append(s)
    return out


def compute_states(ind: pd.DataFrame) -> dict[str, list]:
    return {
        "C1-HYST": state_hyst(ind),
        "C1-NORETEXIT": state_noretexit(ind),
        "C1-HYST-STRICT-ENTRY": state_hyst_strict_entry(ind),
    }


# ---------------------------------------------------------------------------
# simulation
# ---------------------------------------------------------------------------

def simulate(states_full: list, ind: pd.DataFrame, bars: dict[str, pd.DataFrame],
             start: pd.Timestamp, end: pd.Timestamp, slippage_bps: int) -> dict:
    cost = slippage_bps / 10000.0
    mask = (ind["date"] >= start) & (ind["date"] <= end)
    sub_ind = ind[mask].reset_index(drop=True)
    sub_states = [states_full[i] for i in ind.index[mask]]

    targets = [sub_states[0] if sub_states[0] is not None else "BIL"]
    for i in range(1, len(sub_states)):
        prev = sub_states[i - 1]
        targets.append(prev if prev is not None else "BIL")

    bar_lookup = {t: bars[t].set_index("date")[["open", "close"]].to_dict(orient="index")
                  for t in ("TQQQ", "BIL")}

    equity = 1.0
    held = targets[0]
    flips = 0
    eq_curve = []
    held_history = []
    daily_returns = []
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
            daily_returns.append(equity / 1.0 - 1.0)
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

    # close final trade
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
        exit_d = pd.to_datetime(t_["exit_date"])
        exit_eq = eq_by_date.get(exit_d)
        if exit_eq and t_["entry_eq"]:
            t_["pnl_pct"] = exit_eq / t_["entry_eq"] - 1.0
        else:
            t_["pnl_pct"] = None
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
    runs = []
    cur = held_arr[0]; cur_len = 1
    for x in held_arr[1:]:
        if x == cur:
            cur_len += 1
        else:
            runs.append(cur_len); cur_len = 1; cur = x
    runs.append(cur_len)
    avg_hold = float(np.mean(runs)) if runs else None

    worst_trade = float(min((t_["pnl_pct"] for t_ in enriched_trades if t_["pnl_pct"] is not None), default=0.0))
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
    spy_final = START_VALUE * (1.0 + spy_ret)
    target_2x_value = 2.0 * spy_final
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
        "n_trades_closed": len(enriched_trades),
        "avg_hold_days_calendar": round(avg_hold, 1) if avg_hold is not None else None,
        "pct_days_in_TQQQ": round(pct_tqqq, 4),
        "pct_days_in_BIL": round(pct_bil, 4),
        "worst_trade": round(worst_trade, 4),
        "worst_day_loss": round(worst_day, 4) if worst_day is not None else None,
        "spy_bh_total_return": round(spy_ret, 4),
        "qqq_bh_total_return": round(qqq_ret, 4),
        "tqqq_bh_total_return": round(tqqq_ret, 4),
        "spy_bh_final_value": round(spy_final, 2),
        "qqq_bh_final_value": round(START_VALUE * (1 + qqq_ret), 2),
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

    bars = load_bars(use_cache=not args.no_cache)
    ind = compute_indicators(bars["QQQ"])
    states_by_variant = compute_states(ind)

    first_valid = ind.dropna(subset=["sma200", "sma150", "sma50", "ret63"])["date"].iloc[0]
    print(f"[run] first day all indicators valid: {first_valid.date()}", file=sys.stderr)

    metrics_rows = []
    annual_rows = []
    monthly_rows = []
    full_results = {}

    for variant in VARIANTS:
        full_results[variant] = {}
        for w_label, (s, e) in WINDOWS.items():
            full_results[variant][w_label] = {}
            sd_req = pd.Timestamp(s)
            start = max(sd_req, first_valid)
            # also clip to first day TQQQ bars exist (won't help signal but matters for sim)
            tqqq_first = bars["TQQQ"]["date"].iloc[0]
            start = max(start, tqqq_first)
            end = pd.Timestamp(e)
            if start > end:
                print(f"[skip] {variant} {w_label}: start {start.date()} > end {end.date()}", file=sys.stderr)
                continue
            for slip in SLIPPAGE_BPS:
                m = simulate(states_by_variant[variant], ind, bars, start, end, slip)
                full_results[variant][w_label][slip] = m
                print(f"[done] {variant:<22} {w_label} slip={slip:>2}bp  "
                      f"tot={m['total_return']*100:>+8.2f}%  DD={m['max_drawdown']*100:>+6.2f}%  "
                      f"flips={m['flips']:>3}  pass2xR={'P' if m['pass_2x_spy_return'] else 'f'}  "
                      f"pass2xV={'P' if m['pass_2x_spy_final_value'] else 'f'}",
                      file=sys.stderr)
                row = {"variant": variant, "window": w_label, "slippage_bps": slip,
                       "start_date": str(start.date()), "end_date": str(end.date()),
                       **{k: v for k, v in m.items()
                          if k not in ("annual_returns", "monthly_returns", "trades")}}
                metrics_rows.append(row)
                for y, v in m["annual_returns"].items():
                    annual_rows.append({"variant": variant, "window": w_label, "slippage_bps": slip,
                                        "year": y, "return": v})
                for ym, v in m["monthly_returns"].items():
                    monthly_rows.append({"variant": variant, "window": w_label, "slippage_bps": slip,
                                         "month": ym, "return": v})
                tdf = pd.DataFrame(m["trades"])
                if not tdf.empty:
                    tdf.to_csv(out_dir / f"trades_{variant}_{w_label}_{slip}bp.csv", index=False)

    pd.DataFrame(metrics_rows).to_csv(out_dir / "metrics.csv", index=False)
    pd.DataFrame(annual_rows).to_csv(out_dir / "annual_returns.csv", index=False)
    pd.DataFrame(monthly_rows).to_csv(out_dir / "monthly_returns.csv", index=False)
    (out_dir / "summary.json").write_text(json.dumps({
        "config": {
            "tickers": TICKERS, "data_start": DATA_START,
            "data_source": "yfinance auto_adjust=True (split + dividend adjusted)",
            "windows": WINDOWS, "slippage_bps_cases": SLIPPAGE_BPS,
            "cost_convention": "per flip event (sell+buy combined)",
            "starting_value": START_VALUE,
        },
        "results": full_results,
    }, indent=2, default=str))

    # human-readable output
    lines = []
    lines.append("=" * 110)
    lines.append("C1-HYST Lock + Audit Run")
    lines.append("=" * 110)
    lines.append(f"Data:    yfinance auto_adjust=True for {','.join(TICKERS)} from {DATA_START}")
    lines.append(f"Cost:    slippage applied per flip event (sell+buy combined)")
    lines.append(f"Start:   ${START_VALUE:,.0f}")
    lines.append(f"Windows:")
    for w, (s, e) in WINDOWS.items():
        lines.append(f"           {w}: {s} -> {e}")
    lines.append(f"Variants: C1-HYST, C1-NORETEXIT, C1-HYST-STRICT-ENTRY")
    lines.append("")

    lines.append("Benchmarks (B&H, total-return adjusted):")
    lines.append(f"  {'window':<8} {'SPY':>10} {'QQQ':>10} {'TQQQ':>10} "
                 f"{'2xSPY ret':>12} {'2xSPY final$':>16}")
    for w in WINDOWS:
        if w not in full_results["C1-HYST"]:
            continue
        m = full_results["C1-HYST"][w][3]
        lines.append(f"  {w:<8} {m['spy_bh_total_return']*100:>9.2f}% "
                     f"{m['qqq_bh_total_return']*100:>9.2f}% "
                     f"{m['tqqq_bh_total_return']*100:>9.2f}% "
                     f"{m['target_2x_spy_return']*100:>11.2f}% "
                     f"${m['target_2x_spy_final_value']:>14,.0f}")
    lines.append("")

    # detailed table per window
    for w in WINDOWS:
        if w not in full_results["C1-HYST"]:
            continue
        spy_ret = full_results["C1-HYST"][w][3]["spy_bh_total_return"]
        tqqq_ret = full_results["C1-HYST"][w][3]["tqqq_bh_total_return"]
        target_2x_val = full_results["C1-HYST"][w][3]["target_2x_spy_final_value"]
        s_start = full_results["C1-HYST"][w][3]
        # actual start may differ from requested; reach into row
        sd = next((r["start_date"] for r in metrics_rows if r["variant"] == "C1-HYST" and r["window"] == w), "")
        ed = next((r["end_date"] for r in metrics_rows if r["variant"] == "C1-HYST" and r["window"] == w), "")
        lines.append("-" * 110)
        lines.append(f"WINDOW {w}  ({sd} -> {ed})  "
                     f"SPY +{spy_ret*100:.2f}%  TQQQ +{tqqq_ret*100:.2f}%  2x-SPY-final ${target_2x_val:,.0f}")
        lines.append("-" * 110)
        hdr = (f"{'variant':<22} {'slip':>5} {'tot_ret':>9} {'final$':>11} {'CAGR':>8} {'maxDD':>8} "
               f"{'Sharpe':>7} {'Sortino':>8} {'Calmar':>7} {'flips':>6} {'avgHold':>8} "
               f"{'%TQQQ':>7} {'wTrade':>8} {'wDay':>7} {'2xR':>5} {'2xV':>5} {'vsTQQQ':>9}")
        lines.append(hdr)
        for v in VARIANTS:
            for slip in SLIPPAGE_BPS:
                m = full_results[v][w][slip]
                lines.append(
                    f"{v:<22} {slip:>3}bp {m['total_return']*100:>+8.2f}% "
                    f"${m['final_value']:>9,.0f} {m['cagr']*100:>+7.2f}% {m['max_drawdown']*100:>+7.2f}% "
                    f"{(m['sharpe'] or 0):>7.3f} {(m['sortino'] or 0):>8.3f} {(m['calmar'] or 0):>7.3f} "
                    f"{m['flips']:>6} {m['avg_hold_days_calendar']:>8.1f} "
                    f"{m['pct_days_in_TQQQ']*100:>6.1f}% "
                    f"{m['worst_trade']*100:>+7.2f}% {m['worst_day_loss']*100:>+6.2f}% "
                    f"{('P' if m['pass_2x_spy_return'] else 'f'):>5} "
                    f"{('P' if m['pass_2x_spy_final_value'] else 'f'):>5} "
                    f"{m['delta_vs_tqqq_bh_pct']:>+8.2f}%"
                )
        lines.append("")

    # AUDIT VERDICT section: what does HYST-STRICT-ENTRY tell us?
    lines.append("=" * 110)
    lines.append("AUDIT VERDICT: source of HYST's edge")
    lines.append("=" * 110)
    lines.append("Compare HYST-STRICT-ENTRY (strict entry, HYST hold zone) against:")
    lines.append("  - HYST   (loose entry close>SMA150, HYST hold zone)")
    lines.append("  - NORETEXIT (strict entry, no ret63 exit, no hold zone)")
    lines.append("")
    lines.append("If STRICT-ENTRY ~ HYST: hold zone (score>=2 stays) drives the edge")
    lines.append("If STRICT-ENTRY ~ NORETEXIT: looser entry (close>SMA150) drives the edge")
    lines.append("")
    for w in WINDOWS:
        if w not in full_results["C1-HYST"]:
            continue
        h = full_results["C1-HYST"][w][3]
        n = full_results["C1-NORETEXIT"][w][3]
        s = full_results["C1-HYST-STRICT-ENTRY"][w][3]
        lines.append(f"Window {w} @ 3bp:")
        lines.append(f"  HYST            {h['total_return']*100:>+8.2f}%   ${h['final_value']:>10,.0f}   "
                     f"flips={h['flips']:>3}  %TQQQ={h['pct_days_in_TQQQ']*100:>4.1f}%")
        lines.append(f"  NORETEXIT       {n['total_return']*100:>+8.2f}%   ${n['final_value']:>10,.0f}   "
                     f"flips={n['flips']:>3}  %TQQQ={n['pct_days_in_TQQQ']*100:>4.1f}%")
        lines.append(f"  STRICT-ENTRY    {s['total_return']*100:>+8.2f}%   ${s['final_value']:>10,.0f}   "
                     f"flips={s['flips']:>3}  %TQQQ={s['pct_days_in_TQQQ']*100:>4.1f}%")
        # delta diagnostics
        delta_strict_vs_noret = s["total_return"] - n["total_return"]
        delta_hyst_vs_strict = h["total_return"] - s["total_return"]
        gap_total = h["total_return"] - n["total_return"]
        if abs(gap_total) < 1e-9:
            share_hold = 0.0
            share_entry = 0.0
        else:
            share_hold = delta_strict_vs_noret / gap_total
            share_entry = delta_hyst_vs_strict / gap_total
        lines.append(f"     gap HYST - NORETEXIT      = {gap_total*100:>+8.2f}%")
        lines.append(f"     STRICT-ENTRY - NORETEXIT  = {delta_strict_vs_noret*100:>+8.2f}%   "
                     f"(share of gap explained by hold zone:    {share_hold*100:>+6.1f}%)")
        lines.append(f"     HYST - STRICT-ENTRY       = {delta_hyst_vs_strict*100:>+8.2f}%   "
                     f"(share of gap explained by looser entry: {share_entry*100:>+6.1f}%)")
        lines.append("")

    txt = "\n".join(lines)
    (out_dir / "summary.txt").write_text(txt)
    print(txt)


if __name__ == "__main__":
    main()
