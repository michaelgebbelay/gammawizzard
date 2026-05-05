#!/usr/bin/env python3
"""
VIX term-structure regime + 200d trend, leveraged.

Two variants run side-by-side:
  primary : SPY trend filter, allocate SPY / UPRO / BIL
  hedge   : QQQ trend filter, allocate QQQ / TQQQ / BIL

Rules (decidable at daily close, executed next open):
  trend_signal[t] = SPY.close[t] > SPY.SMA200[t]  (with hysteresis on re-entry: 1.5% buffer)
  term_signal[t]  = VIX.close[t] / VIX3M.close[t] < 1.0

Allocation map:
  both ON  -> 3x ETF (UPRO / TQQQ)
  one ON   -> 1x ETF (SPY / QQQ)
  both OFF -> BIL

Costs: 1 bp commission + 2 bp slippage per fill = 3 bp deducted on each leg
of a flip (so a flip pays 6 bp round trip). ETF expense ratios are NOT
re-applied -- they are baked into the actual ETF NAV (UPRO / TQQQ / BIL prices
already include the management fee accrual). Same for BIL: holding BIL gives
the realized 1-3m T-bill yield through price action.

Output: two equity curve CSVs + a summary text file under results/.
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

COST_PER_LEG = 0.0003          # 1 bp commission + 2 bp slippage
SMA_WINDOW = 200
TERM_THRESHOLD = 1.0
REENTRY_BUFFER = 0.015         # SPY must clear SMA by 1.5% to re-arm trend
SLEEVE_BIL = "BIL"


# ---------------------------------------------------------------------------
# data
# ---------------------------------------------------------------------------

def load_etf_bars(tickers: list[str]) -> dict[str, pd.DataFrame]:
    """Pull split-adjusted daily bars for the requested tickers."""
    path = DATA_DIR / "aggs_daily_adjusted.parquet"
    if not path.exists():
        raise SystemExit(f"missing parquet: {path}")
    df = pd.read_parquet(path, columns=["ticker", "date", "open", "high", "low", "close"])
    out: dict[str, pd.DataFrame] = {}
    for t in tickers:
        sub = df[df["ticker"] == t].copy()
        if sub.empty:
            raise SystemExit(f"no bars for {t} in {path}")
        sub["date"] = pd.to_datetime(sub["date"]).dt.normalize()
        sub = sub.sort_values("date").reset_index(drop=True)
        out[t] = sub
        print(f"[load] {t}: {len(sub)} rows  {sub['date'].iloc[0].date()} -> {sub['date'].iloc[-1].date()}",
              file=sys.stderr)
    return out


def load_vix_series(name: str, yahoo_ticker: str, cache: Path) -> pd.DataFrame:
    """Load VIX index series. Use cached parquet if present, otherwise yfinance."""
    if cache.exists():
        df = pd.read_parquet(cache)
        print(f"[vix] {name}: cached {len(df)} rows  {df['date'].iloc[0].date()} -> {df['date'].iloc[-1].date()}",
              file=sys.stderr)
        return df
    import yfinance as yf
    print(f"[vix] {name}: fetching {yahoo_ticker} from yfinance...", file=sys.stderr)
    raw = yf.Ticker(yahoo_ticker).history(start="2018-01-01", end="2026-12-31")
    if raw.empty:
        raise SystemExit(f"yfinance returned no data for {yahoo_ticker}")
    raw = raw.reset_index()
    raw["date"] = pd.to_datetime(raw["Date"]).dt.tz_localize(None).dt.normalize()
    df = (raw[["date", "Close"]]
          .rename(columns={"Close": "close"})
          .dropna()
          .sort_values("date")
          .reset_index(drop=True))
    cache.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(cache, index=False)
    print(f"[vix] {name}: wrote {cache} ({len(df)} rows)", file=sys.stderr)
    return df


# ---------------------------------------------------------------------------
# backtest
# ---------------------------------------------------------------------------

def compute_signals(spy: pd.DataFrame, vix: pd.DataFrame, vix3m: pd.DataFrame) -> pd.DataFrame:
    """Build per-day signal frame. Trend signal has 1.5% re-entry hysteresis."""
    spy = spy[["date", "close"]].rename(columns={"close": "spy_close"}).copy()
    spy["sma200"] = spy["spy_close"].rolling(SMA_WINDOW, min_periods=SMA_WINDOW).mean()

    vv = (vix.rename(columns={"close": "vix"})
              .merge(vix3m.rename(columns={"close": "vix3m"}), on="date", how="outer")
              .sort_values("date")
              .reset_index(drop=True))
    vv["vix"] = vv["vix"].ffill()
    vv["vix3m"] = vv["vix3m"].ffill()
    vv["vix_ratio"] = vv["vix"] / vv["vix3m"]

    df = spy.merge(vv[["date", "vix", "vix3m", "vix_ratio"]], on="date", how="left")
    df["vix"] = df["vix"].ffill()
    df["vix3m"] = df["vix3m"].ffill()
    df["vix_ratio"] = df["vix_ratio"].ffill()

    # term signal: pure threshold, no hysteresis
    df["term_on"] = df["vix_ratio"] < TERM_THRESHOLD

    # trend signal: hysteresis on re-entry only (1.5% buffer)
    trend_state = []
    state = None
    for _, row in df.iterrows():
        if pd.isna(row["sma200"]):
            trend_state.append(np.nan)
            continue
        if state is None:
            state = bool(row["spy_close"] > row["sma200"])
        else:
            if state:  # currently ON: flip OFF on close < SMA
                if row["spy_close"] < row["sma200"]:
                    state = False
            else:      # currently OFF: flip ON only if close > SMA * 1.015
                if row["spy_close"] > row["sma200"] * (1.0 + REENTRY_BUFFER):
                    state = True
        trend_state.append(state)
    df["trend_on"] = trend_state
    return df


def target_sleeve(trend_on: bool, term_on: bool, etf_1x: str, etf_3x: str) -> str:
    if pd.isna(trend_on) or pd.isna(term_on):
        return SLEEVE_BIL
    if bool(trend_on) and bool(term_on):
        return etf_3x
    if bool(trend_on) or bool(term_on):
        return etf_1x
    return SLEEVE_BIL


def run_variant(
    label: str,
    etf_1x: str,
    etf_3x: str,
    bars: dict[str, pd.DataFrame],
    signals: pd.DataFrame,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
) -> tuple[pd.DataFrame, dict]:
    """Run one variant. signals is computed from SPY/VIX (shared across variants)."""
    sig = signals[(signals["date"] >= start_date) & (signals["date"] <= end_date)].copy().reset_index(drop=True)

    # if hedge variant: re-derive trend signal off QQQ instead of SPY
    if etf_1x == "QQQ":
        qqq = bars["QQQ"][["date", "close"]].rename(columns={"close": "qqq_close"}).copy()
        qqq["qqq_sma200"] = qqq["qqq_close"].rolling(SMA_WINDOW, min_periods=SMA_WINDOW).mean()
        sig = sig.drop(columns=["trend_on"], errors="ignore").merge(qqq, on="date", how="left")
        trend_state = []
        state = None
        for _, row in sig.iterrows():
            if pd.isna(row["qqq_sma200"]):
                trend_state.append(np.nan)
                continue
            if state is None:
                state = bool(row["qqq_close"] > row["qqq_sma200"])
            else:
                if state:
                    if row["qqq_close"] < row["qqq_sma200"]:
                        state = False
                else:
                    if row["qqq_close"] > row["qqq_sma200"] * (1.0 + REENTRY_BUFFER):
                        state = True
            trend_state.append(state)
        sig["trend_on"] = trend_state

    sleeves = [SLEEVE_BIL, etf_1x, etf_3x]
    bar_lookup: dict[str, dict[pd.Timestamp, dict]] = {}
    for s in sleeves:
        b = bars[s].set_index("date")[["open", "close"]]
        bar_lookup[s] = b.to_dict(orient="index")

    # build target alloc for each day, executed at open of that day from prev-close signal
    sig["target"] = [
        target_sleeve(t_on, term_on, etf_1x, etf_3x)
        for t_on, term_on in zip(sig["trend_on"].shift(1), sig["term_on"].shift(1))
    ]
    # day 0 has no prev close signal -> default to BIL
    sig.loc[0, "target"] = SLEEVE_BIL

    equity = 1.0
    held = SLEEVE_BIL
    flips = 0
    eq_curve = []
    daily_returns = []
    held_history = []

    prev_date = None
    for i, row in sig.iterrows():
        d = row["date"]
        target = row["target"]

        if prev_date is None:
            # initialize at first day; assume we entered into target sleeve at open
            held = target
            held_history.append(held)
            # day-0 return: open-to-close of held sleeve
            o = bar_lookup[held].get(d, {}).get("open")
            c = bar_lookup[held].get(d, {}).get("close")
            if o is not None and c is not None and o > 0:
                day_ret = c / o - 1.0
            else:
                day_ret = 0.0
            # entry cost (one buy leg)
            equity *= (1.0 - COST_PER_LEG)
            equity *= (1.0 + day_ret)
            eq_curve.append(equity)
            daily_returns.append(day_ret - COST_PER_LEG)
            prev_date = d
            continue

        if target != held:
            # flip at open today: sell `held` at open[d], buy `target` at open[d]
            # 1) realize overnight gap on `held`: held.open[d] / held.close[prev_date]
            old_open = bar_lookup[held].get(d, {}).get("open")
            old_prev_close = bar_lookup[held].get(prev_date, {}).get("close")
            if old_open is not None and old_prev_close is not None and old_prev_close > 0:
                equity *= old_open / old_prev_close
            # 2) two trade legs (sell + buy)
            equity *= (1.0 - COST_PER_LEG) ** 2
            # 3) intraday on new sleeve: target.close[d] / target.open[d]
            new_open = bar_lookup[target].get(d, {}).get("open")
            new_close = bar_lookup[target].get(d, {}).get("close")
            if new_open is not None and new_close is not None and new_open > 0:
                equity *= new_close / new_open
            held = target
            flips += 1
        else:
            # no flip: close-to-close of held sleeve
            cprev = bar_lookup[held].get(prev_date, {}).get("close")
            ccur = bar_lookup[held].get(d, {}).get("close")
            if cprev is not None and ccur is not None and cprev > 0:
                equity *= ccur / cprev

        eq_curve.append(equity)
        daily_returns.append(eq_curve[-1] / eq_curve[-2] - 1.0 if len(eq_curve) >= 2 else 0.0)
        held_history.append(held)
        prev_date = d

    sig["equity"] = eq_curve
    sig["daily_ret"] = daily_returns
    sig["held"] = held_history

    # buy-hold benchmarks for context
    spy_bh = bars["SPY"].set_index("date")["close"].reindex(sig["date"]).ffill()
    qqq_bh = bars["QQQ"].set_index("date")["close"].reindex(sig["date"]).ffill()
    sig["spy_bh"] = spy_bh.values / spy_bh.iloc[0]
    sig["qqq_bh"] = qqq_bh.values / qqq_bh.iloc[0]

    metrics = compute_metrics(sig["date"].values, sig["equity"].values, sig["daily_ret"].values, flips)
    metrics["spy_bh_total_return"] = float(sig["spy_bh"].iloc[-1] - 1.0)
    metrics["qqq_bh_total_return"] = float(sig["qqq_bh"].iloc[-1] - 1.0)
    metrics["start_date"] = str(pd.Timestamp(sig["date"].iloc[0]).date())
    metrics["end_date"] = str(pd.Timestamp(sig["date"].iloc[-1]).date())
    metrics["label"] = label
    metrics["pct_days_in_3x"] = float((np.array(held_history) == etf_3x).mean())
    metrics["pct_days_in_1x"] = float((np.array(held_history) == etf_1x).mean())
    metrics["pct_days_in_bil"] = float((np.array(held_history) == SLEEVE_BIL).mean())
    return sig, metrics


def compute_metrics(dates, equity, daily_ret, flips: int) -> dict:
    eq = np.asarray(equity, dtype=float)
    dr = np.asarray(daily_ret, dtype=float)
    n_days = len(eq)
    if n_days < 2:
        return {}
    total_return = float(eq[-1] - 1.0)
    years = (pd.Timestamp(dates[-1]) - pd.Timestamp(dates[0])).days / 365.25
    cagr = float(eq[-1] ** (1.0 / years) - 1.0) if years > 0 else float("nan")
    daily_mean = float(np.nanmean(dr))
    daily_std = float(np.nanstd(dr, ddof=1))
    sharpe = float(daily_mean / daily_std * np.sqrt(252)) if daily_std > 0 else float("nan")
    rolling_max = np.maximum.accumulate(eq)
    drawdown = eq / rolling_max - 1.0
    max_dd = float(drawdown.min())
    return {
        "n_days": int(n_days),
        "years": round(years, 3),
        "total_return": round(total_return, 4),
        "cagr": round(cagr, 4),
        "sharpe": round(sharpe, 3),
        "max_drawdown": round(max_dd, 4),
        "regime_flips": int(flips),
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--start-date", default="2022-01-03")
    p.add_argument("--end-date", default="2026-05-01")
    p.add_argument("--out-dir", default=str(RESULTS_DIR))
    args = p.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    bars = load_etf_bars(["SPY", "QQQ", "UPRO", "TQQQ", "BIL"])
    vix = load_vix_series("VIX", "^VIX", DATA_DIR / "vix_daily.parquet")
    vix3m = load_vix_series("VIX3M", "^VIX3M", DATA_DIR / "vix3m_daily.parquet")

    signals = compute_signals(bars["SPY"], vix, vix3m)
    first_signal_day = signals.dropna(subset=["sma200"])["date"].iloc[0]
    requested_start = pd.Timestamp(args.start_date)
    start = max(requested_start, first_signal_day)
    end = pd.Timestamp(args.end_date)
    print(f"[run] requested start={requested_start.date()} actual start={start.date()} end={end.date()}",
          file=sys.stderr)

    primary_curve, primary_metrics = run_variant("primary_SPY_UPRO", "SPY", "UPRO", bars, signals, start, end)
    hedge_curve, hedge_metrics = run_variant("hedge_QQQ_TQQQ", "QQQ", "TQQQ", bars, signals, start, end)

    primary_path = out_dir / "primary_SPY_UPRO_curve.csv"
    hedge_path = out_dir / "hedge_QQQ_TQQQ_curve.csv"
    primary_curve.to_csv(primary_path, index=False)
    hedge_curve.to_csv(hedge_path, index=False)
    print(f"[run] wrote {primary_path}", file=sys.stderr)
    print(f"[run] wrote {hedge_path}", file=sys.stderr)

    summary = {
        "primary_SPY_UPRO": primary_metrics,
        "hedge_QQQ_TQQQ": hedge_metrics,
        "config": {
            "sma_window": SMA_WINDOW,
            "term_threshold": TERM_THRESHOLD,
            "reentry_buffer_pct": REENTRY_BUFFER * 100,
            "cost_per_leg_bps": COST_PER_LEG * 10000,
        },
    }
    json_path = out_dir / "summary.json"
    json_path.write_text(json.dumps(summary, indent=2))

    # human-readable summary
    lines = []
    lines.append("=" * 70)
    lines.append("VIX term-structure regime + 200d trend, leveraged")
    lines.append("=" * 70)
    lines.append(f"Window:    {primary_metrics['start_date']} -> {primary_metrics['end_date']}")
    lines.append(f"Years:     {primary_metrics['years']}")
    lines.append(f"Costs:     {COST_PER_LEG*10000:.0f} bp/leg ({COST_PER_LEG*20000:.0f} bp per flip round-trip)")
    lines.append(f"Trend:     SPY (or QQQ) > 200d SMA, 1.5% re-entry buffer")
    lines.append(f"Term:      VIX/VIX3M < {TERM_THRESHOLD}")
    lines.append("")
    for label, m in [("PRIMARY (SPY/UPRO)", primary_metrics), ("HEDGE (QQQ/TQQQ)", hedge_metrics)]:
        lines.append(f"--- {label} ---")
        lines.append(f"  Total return:   {m['total_return']*100:>8.2f}%")
        lines.append(f"  CAGR:           {m['cagr']*100:>8.2f}%")
        lines.append(f"  Max drawdown:   {m['max_drawdown']*100:>8.2f}%")
        lines.append(f"  Sharpe (daily): {m['sharpe']:>8.3f}")
        lines.append(f"  Regime flips:   {m['regime_flips']}")
        lines.append(f"  % days in 3x:   {m['pct_days_in_3x']*100:>5.1f}%")
        lines.append(f"  % days in 1x:   {m['pct_days_in_1x']*100:>5.1f}%")
        lines.append(f"  % days in BIL:  {m['pct_days_in_bil']*100:>5.1f}%")
        lines.append(f"  SPY buy/hold:   {m['spy_bh_total_return']*100:>8.2f}%   "
                     f"QQQ buy/hold: {m['qqq_bh_total_return']*100:>8.2f}%")
        lines.append("")
    txt = "\n".join(lines)
    (out_dir / "summary.txt").write_text(txt)
    print(txt)


if __name__ == "__main__":
    main()
