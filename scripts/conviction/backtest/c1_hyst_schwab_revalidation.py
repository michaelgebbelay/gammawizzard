#!/usr/bin/env python3
"""
Independent-data revalidation of C1-HYST against Schwab price history.

Uses the production Schwab broker as the alternate data source. This is
the most meaningful independent check available because Schwab is also
the live-trading broker — if the signal reproduces on Schwab data, the
deployment path uses one source end-to-end.

Caveats / data adjustment notes:
  - Schwab `priceHistory` returns split-adjusted but NOT dividend-adjusted
    daily closes. yfinance (auto_adjust=True) applies both.
  - For QQQ + TQQQ this is a small effect (TQQQ pays ~0, QQQ ~0.5%/yr).
  - For SPY benchmark: ~1.4%/yr drag — SPY total return on Schwab will
    be ~20% lower than yfinance over the L window. That's expected.
  - For BIL: ALL of BIL's return is its dividend — Schwab close is
    essentially flat. We synthesize BIL total return from FRED 1-3m
    T-bill rate (DTB3) compounded daily. This isn't perfect but it's
    independent of yfinance and reflects the real cash yield.

The revalidation question is NOT "do the numbers match exactly" — it's
"does the strategy still clear 2x SPY in all four windows when run on a
genuinely independent data feed". Direction and ordering matter more
than absolute reproduction.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
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
SLIPPAGE_BPS = [10, 50]
TICKERS_EQ = ["SPY", "QQQ", "TQQQ"]
DATA_START = "2010-01-01"
DATA_END = "2026-05-05"

# from GHA run 25358962345 (entry_exit_attribution.yml)
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


# ----- Schwab fetch ------------------------------------------------------- #

def add_scripts_to_path() -> None:
    cur = HERE
    while True:
        if cur.name == "scripts":
            if str(cur) not in sys.path:
                sys.path.append(str(cur))
            return
        if cur.parent == cur:
            return
        cur = cur.parent


def fetch_schwab_daily(client, ticker: str, start: str, end: str) -> pd.DataFrame:
    sd = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
    ed = datetime.fromisoformat(end).replace(tzinfo=timezone.utc)
    r = client.get_price_history_every_day(
        ticker,
        start_datetime=sd,
        end_datetime=ed,
        need_extended_hours_data=False,
    )
    r.raise_for_status()
    candles = r.json().get("candles") or []
    if not candles:
        raise SystemExit(f"schwab returned no candles for {ticker}")
    df = pd.DataFrame(candles)
    df["date"] = (pd.to_datetime(df["datetime"], unit="ms", utc=True)
                  .dt.tz_convert("America/New_York")
                  .dt.normalize()
                  .dt.tz_localize(None))
    df = df[["date", "open", "high", "low", "close", "volume"]].copy()
    df["ticker"] = ticker
    df = df.sort_values("date").reset_index(drop=True)
    print(f"[schwab] {ticker}: {len(df)} rows {df['date'].iloc[0].date()} -> {df['date'].iloc[-1].date()}",
          file=sys.stderr)
    return df


def synthesize_bil_total_return(eq_dates: pd.DatetimeIndex) -> pd.DataFrame:
    """Approximate BIL total-return series using FRED DTB3 (3-month T-bill).
    No FRED key needed — DTB3 fred CSV is publicly accessible."""
    import io
    from urllib.request import urlopen, Request

    url = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DTB3"
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=30) as resp:
        body = resp.read().decode("utf-8")
    df = pd.read_csv(io.StringIO(body))
    df.columns = [c.strip().lower() for c in df.columns]
    rate_col = next(c for c in df.columns if "dtb3" in c.lower() or c.lower() == "value" or c.lower() == "dtb3")
    if rate_col == "value" and "dtb3" in df.columns:
        rate_col = "dtb3"
    elif rate_col == "value":
        pass
    df["date"] = pd.to_datetime(df["observation_date"]).dt.normalize() \
        if "observation_date" in df.columns \
        else pd.to_datetime(df["date"]).dt.normalize()
    df = df.rename(columns={rate_col: "rate_pct"})
    df["rate_pct"] = pd.to_numeric(df["rate_pct"], errors="coerce")
    df = df[["date", "rate_pct"]].dropna().sort_values("date").reset_index(drop=True)

    # interpolate onto equity-trading-day calendar
    s = df.set_index("date")["rate_pct"].reindex(eq_dates).ffill().bfill()
    # convert annualized % to daily compounded factor
    daily_factor = (1.0 + s / 100.0) ** (1.0 / 252.0)
    bil_close = daily_factor.cumprod()
    out = pd.DataFrame({
        "date": eq_dates,
        "open": bil_close.values,    # use close as open (BIL has near-zero intraday range)
        "high": bil_close.values,
        "low":  bil_close.values,
        "close": bil_close.values,
    })
    out["ticker"] = "BIL"
    print(f"[bil-synth] {len(out)} rows {eq_dates[0].date()} -> {eq_dates[-1].date()}  "
          f"final = {bil_close.iloc[-1]:.4f}  (start = 1.0000)",
          file=sys.stderr)
    return out


# ----- C1-HYST signal ----------------------------------------------------- #

def compute_c1_hyst_indicators(qqq: pd.DataFrame) -> pd.DataFrame:
    df = qqq[["date", "open", "high", "low", "close"]].copy()
    df["sma50"]  = df["close"].rolling(50, min_periods=50).mean()
    df["sma150"] = df["close"].rolling(150, min_periods=150).mean()
    df["sma200"] = df["close"].rolling(200, min_periods=200).mean()
    df["ret63"]  = df["close"].pct_change(63)
    return df


def state_c1_hyst(ind: pd.DataFrame) -> list:
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
        out.append(s)
    return out


# ----- main --------------------------------------------------------------- #

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--out-dir", default="out")
    args = p.parse_args()
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)

    add_scripts_to_path()
    from schwab_token_keeper import schwab_client   # noqa: E402
    client = schwab_client()

    bars = {}
    for t in TICKERS_EQ:
        bars[t] = fetch_schwab_daily(client, t, DATA_START, DATA_END)
        time.sleep(0.5)

    # synthetic BIL on the QQQ trading calendar
    bars["BIL"] = synthesize_bil_total_return(
        pd.DatetimeIndex(bars["QQQ"]["date"]))

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
            tr_diff_pp = (m["total_return"] - yf_ref["total_return"]) * 100
            dd_diff_pp = (m["max_drawdown"] - yf_ref["max_drawdown"]) * 100
            flip_diff = m["flips"] - yf_ref["flips"]
            rows.append({
                "window": w_label, "slippage_bps": slip,
                "schwab_total_return_pct":  round(m["total_return"]*100, 2),
                "yfinance_total_return_pct": round(yf_ref["total_return"]*100, 2),
                "diff_pp_total":  round(tr_diff_pp, 2),
                "schwab_max_dd_pct":  round(m["max_drawdown"]*100, 2),
                "yfinance_max_dd_pct": round(yf_ref["max_drawdown"]*100, 2),
                "diff_pp_dd":     round(dd_diff_pp, 2),
                "schwab_flips":   m["flips"],
                "yf_flips":       yf_ref["flips"],
                "flip_diff":      flip_diff,
                "schwab_sharpe":  m["sharpe"],
                "schwab_pass_2x": m["pass_2x_spy_return"],
                "schwab_spy_bh_ret_pct": round(m["spy_bh_total_return"]*100, 2),
                "schwab_qqq_bh_ret_pct": round(m["qqq_bh_total_return"]*100, 2),
                "schwab_tqqq_bh_ret_pct": round(m["tqqq_bh_total_return"]*100, 2),
            })
            tdf = pd.DataFrame(m["trades"])
            if not tdf.empty:
                tdf.to_csv(out / f"trades_C1HYST_schwab_{w_label}_{slip}bp.csv", index=False)

    cmp = pd.DataFrame(rows)
    cmp.to_csv(out / "schwab_vs_yfinance.csv", index=False)

    lines = []
    lines.append("=" * 100)
    lines.append("C1-HYST Schwab vs yfinance reproducibility check")
    lines.append("=" * 100)
    lines.append("Schwab: split-adjusted, NOT dividend-adjusted; BIL synthesized from FRED DTB3")
    lines.append("yfinance ref: GHA run 25358962345 (entry_exit_attribution.yml), auto_adjust=True")
    lines.append("Expected SPY benchmark drag from missing dividends: ~1.4%/yr × 15y ≈ -22%")
    lines.append("")
    lines.append(f"{'window':>6} {'slip':>6} {'schwab_tot':>12} {'yf_tot':>12} {'diff_pp':>10} "
                 f"{'sw_DD':>9} {'yf_DD':>9} {'sw_flips':>9} {'yf_flips':>9} {'pass_2x':>8}")
    lines.append("-" * 100)
    for r in rows:
        lines.append(f"{r['window']:>6} {r['slippage_bps']:>5}bp "
                     f"{r['schwab_total_return_pct']:>+11.2f}% "
                     f"{r['yfinance_total_return_pct']:>+11.2f}% "
                     f"{r['diff_pp_total']:>+9.2f}pp "
                     f"{r['schwab_max_dd_pct']:>+8.2f}% "
                     f"{r['yfinance_max_dd_pct']:>+8.2f}% "
                     f"{r['schwab_flips']:>+9d} {r['yf_flips']:>+9d} "
                     f"{str(r['schwab_pass_2x']):>8}")
    lines.append("")
    L = next(r for r in rows if r["window"] == "L" and r["slippage_bps"] == 10)
    lines.append("Buy-and-hold sanity (Schwab):")
    lines.append(f"  L window: SPY {L['schwab_spy_bh_ret_pct']:+.2f}%  "
                 f"QQQ {L['schwab_qqq_bh_ret_pct']:+.2f}%  "
                 f"TQQQ {L['schwab_tqqq_bh_ret_pct']:+.2f}%")
    lines.append("  yfinance ref: SPY +634.13%  QQQ +1265.39%  TQQQ +16031.27%")
    lines.append("")

    L_passes = all(r["schwab_pass_2x"] for r in rows)
    flip_match_L = next(r for r in rows if r["window"] == "L" and r["slippage_bps"] == 10)
    lines.append("--- Reproducibility verdict ---")
    lines.append(f"  Pass 2x SPY in all 4 windows on Schwab data: {L_passes}")
    lines.append(f"  Flip count L (Schwab vs yfinance): "
                 f"{flip_match_L['schwab_flips']} vs {flip_match_L['yf_flips']} "
                 f"(diff = {flip_match_L['flip_diff']:+d})")
    lines.append(f"  Max DD L (Schwab vs yfinance): "
                 f"{flip_match_L['schwab_max_dd_pct']:.2f}% vs {flip_match_L['yfinance_max_dd_pct']:.2f}%")
    lines.append("")
    lines.append("Interpretation:")
    lines.append("  - If Schwab passes 2x SPY everywhere AND flip count is within a few of yfinance,")
    lines.append("    the strategy reproduces on independent data despite the dividend-adjustment gap.")
    lines.append("  - If flip count differs materially, the SIGNAL is being computed on different prices,")
    lines.append("    not just the cumulative return — that's a real concern worth investigating.")

    (out / "summary.txt").write_text("\n".join(lines))
    print((out / "summary.txt").read_text())


if __name__ == "__main__":
    main()
