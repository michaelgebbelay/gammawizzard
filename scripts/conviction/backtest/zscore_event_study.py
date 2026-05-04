#!/usr/bin/env python3
"""
Z-score bucket event study for the Path S skew signal.

Diagnostic-only. Does not change replay.py production behavior. Does not
introduce new entry filters. Goal: characterize forward returns and z-score
paths after first-cross events into different skew_z buckets, with an
explicit explanation of the sign convention.

Sign convention (mirrors replay.load_skew_lookup):
    skew_5otm = call_iv_5otm - put_iv_5otm
    skew_z    = (skew_5otm - rolling60d_mean.shift(1)) / rolling60d_std.shift(1)

So:
    z > 0  → call IV is unusually elevated vs trailing 60d (bullish flip)
    z < 0  → put IV is unusually elevated vs trailing 60d (fear / bearish skew)
The Path S "bullish" production candidate fires on z >= 3.0.

Outputs (under results/zscore_event_study/):
    events.csv                          — every first-cross event, all buckets
    summary_by_z_bucket.csv
    summary_by_z_bucket_collapsed.csv
    summary_by_z_bucket_spy_gate.csv
    summary_by_z_bucket_rank.csv
    high_z_exhaustion_summary.csv
    negative_z_summary.csv
    positive_z3_events.csv
    positive_z3_top_by_high_90d.csv
    positive_z3_worst_by_return_90d.csv
    positive_z3_high_z_events.csv       — z_start >= 6
    negative_z_events.csv               — z_start <= -1
    in_position_z_reversal_study.csv    — only if baseline trade logs found
    report.md
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
DATA_DIR = HERE / "data"
RESULTS_DIR = HERE / "results"
OUT_DIR = RESULTS_DIR / "zscore_event_study"

# Forward windows (trading days)
FWD_WINDOWS = [5, 10, 20, 45, 90]
PATH_HORIZON = 90

# Default Path S z entry threshold (production candidate)
Z_PROD = 3.0

# Baseline trade logs to use for Study D when present
BASELINE_TRADE_LOGS = [
    ("z3_single_4y", RESULTS_DIR / "2026-05-03_replay_1460d_massive" / "trade_log.csv"),
    ("z3_top2_4y", RESULTS_DIR / "2026-05-03_replay_1460d_massive_n2" / "trade_log.csv"),
]

# Bucket definitions
DEFAULT_BUCKETS = [
    ("<= -7",        lambda z: z <= -7),
    ("-7 to <-6",    lambda z: -7 < z <= -6),
    ("-6 to <-5",    lambda z: -6 < z <= -5),
    ("-5 to <-4",    lambda z: -5 < z <= -4),
    ("-4 to <-3",    lambda z: -4 < z <= -3),
    ("-3 to <-2",    lambda z: -3 < z <= -2),
    ("-2 to <-1",    lambda z: -2 < z <= -1),
    ("-1 to <0",     lambda z: -1 < z < 0),
    ("0 to <1",      lambda z: 0 <= z < 1),
    ("1 to <2",      lambda z: 1 <= z < 2),
    ("2 to <3",      lambda z: 2 <= z < 3),
    ("3 to <4",      lambda z: 3 <= z < 4),
    ("4 to <5",      lambda z: 4 <= z < 5),
    ("5 to <6",      lambda z: 5 <= z < 6),
    ("6 to <7",      lambda z: 6 <= z < 7),
    (">= 7",         lambda z: z >= 7),
]
COLLAPSED_BUCKETS = [
    ("<= -5",        lambda z: z <= -5),
    ("-5 to <-3",    lambda z: -5 < z <= -3),
    ("-3 to <-2",    lambda z: -3 < z <= -2),
    ("-2 to <-1",    lambda z: -2 < z <= -1),
    ("-1 to <0",     lambda z: -1 < z < 0),
    ("0 to <1",      lambda z: 0 <= z < 1),
    ("1 to <2",      lambda z: 1 <= z < 2),
    ("2 to <3",      lambda z: 2 <= z < 3),
    ("3 to <5",      lambda z: 3 <= z < 5),
    ("5 to <7",      lambda z: 5 <= z < 7),
    (">= 7",         lambda z: z >= 7),
]


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------

def load_skew_z(skew_path: Path) -> pd.DataFrame:
    """Mirror replay.load_skew_lookup math; return DataFrame[underlying, date, skew_z]."""
    df = pd.read_parquet(skew_path, columns=["underlying", "date", "skew_5otm"])
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    df = df.sort_values(["underlying", "date"]).reset_index(drop=True)
    grp = df.groupby("underlying")["skew_5otm"]
    rmean = grp.transform(lambda s: s.shift(1).rolling(60, min_periods=20).mean())
    rstd = grp.transform(lambda s: s.shift(1).rolling(60, min_periods=20).std())
    df["skew_z"] = (df["skew_5otm"] - rmean) / rstd
    df = df.dropna(subset=["skew_z"]).reset_index(drop=True)
    return df[["underlying", "date", "skew_z"]]


def load_bars(stocks_path: Path) -> dict:
    """Returns {ticker: DataFrame[date, open, high, low, close]} sorted ascending."""
    cols = ["ticker", "date", "open", "high", "low", "close"]
    df = pd.read_parquet(stocks_path, columns=cols)
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)
    return {t: g.reset_index(drop=True) for t, g in df.groupby("ticker")}


def load_spy_gate(stocks_path: Path) -> dict:
    """Compute SPY-200d state machine, returns {date: True/False} where True = RISK_ON."""
    sys.path.insert(0, str(HERE))
    from regime_filter import compute_spy_regime  # noqa: E402

    df = pd.read_parquet(stocks_path, columns=["ticker", "date", "close"])
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    spy = df[df["ticker"] == "SPY"][["date", "close"]].copy()
    if spy.empty:
        print("[zscore-study] WARNING: no SPY rows — gate will be all True", file=sys.stderr)
        return {}
    states = compute_spy_regime(spy)
    return {d: (s == "RISK_ON") for d, s in states.items()}


# ---------------------------------------------------------------------------
# Bucket utilities
# ---------------------------------------------------------------------------

def assign_bucket(z: float, buckets: list[tuple[str, callable]]) -> str:
    for name, fn in buckets:
        if fn(z):
            return name
    return "out_of_range"


# ---------------------------------------------------------------------------
# Event detection
# ---------------------------------------------------------------------------

def detect_first_cross_events(
    skew_df: pd.DataFrame,
    *,
    cooldown_days: int = 20,
) -> pd.DataFrame:
    """For each (ticker, day), assign bucket; emit an event when ticker enters
    a bucket it was NOT in on the prior trading day in that ticker's series.
    cooldown_days: skip new events for this many trading days after the last
    event for the same ticker+bucket (avoids continuous-episode duplicates).
    """
    skew_df = skew_df.sort_values(["underlying", "date"]).copy()
    skew_df["bucket"] = skew_df["skew_z"].apply(lambda z: assign_bucket(z, DEFAULT_BUCKETS))
    skew_df["bucket_collapsed"] = skew_df["skew_z"].apply(lambda z: assign_bucket(z, COLLAPSED_BUCKETS))
    skew_df["prev_bucket"] = skew_df.groupby("underlying")["bucket"].shift(1)
    crossed = skew_df[skew_df["bucket"] != skew_df["prev_bucket"]].copy()
    crossed = crossed.dropna(subset=["bucket"])

    if cooldown_days <= 0:
        return crossed[["underlying", "date", "skew_z", "bucket", "bucket_collapsed"]].rename(
            columns={"underlying": "ticker", "date": "event_date", "skew_z": "z_start"}
        ).reset_index(drop=True)

    # Apply cooldown per (ticker, bucket)
    out_rows = []
    last_event_idx: dict[tuple[str, str], int] = {}
    # Build a per-ticker date-to-row-index map for trading-day distance
    skew_df["row_idx"] = skew_df.groupby("underlying").cumcount()
    idx_by_dt = {(r.underlying, r.date): r.row_idx for r in skew_df.itertuples(index=False)}
    for r in crossed.itertuples(index=False):
        key = (r.underlying, r.bucket)
        cur_idx = idx_by_dt[(r.underlying, r.date)]
        prev_idx = last_event_idx.get(key)
        if prev_idx is not None and (cur_idx - prev_idx) < cooldown_days:
            continue
        last_event_idx[key] = cur_idx
        out_rows.append({
            "ticker": r.underlying,
            "event_date": r.date,
            "z_start": r.skew_z,
            "z_bucket": r.bucket,
            "z_bucket_collapsed": r.bucket_collapsed,
        })
    return pd.DataFrame(out_rows)


# ---------------------------------------------------------------------------
# Forward-window helpers
# ---------------------------------------------------------------------------

def _slice_forward(bars: pd.DataFrame, event_date: pd.Timestamp, days: int) -> pd.DataFrame:
    """Return up to `days` trading rows AT/AFTER event_date (inclusive)."""
    return bars[bars["date"] >= event_date].head(days + 1).reset_index(drop=True)


def compute_event_features(
    event: dict,
    bars_by_ticker: dict,
    skew_by_ticker_dict: dict,
    *,
    spy_gate: dict,
    rank_by_date: dict,
) -> dict | None:
    """Compute forward-return + z-path columns for one event."""
    tkr = event["ticker"]
    ed = event["event_date"]
    bars = bars_by_ticker.get(tkr)
    if bars is None or bars.empty:
        return None

    fwd = _slice_forward(bars, ed, PATH_HORIZON)
    if fwd.empty:
        return None
    # entry_close = close at event_date
    if pd.Timestamp(fwd.iloc[0]["date"]) != ed:
        # event_date must align to a trading bar
        return None
    entry_close = float(fwd.iloc[0]["close"])
    if entry_close <= 0 or not np.isfinite(entry_close):
        return None

    # Forward window slice (next bars only, excludes the event-date bar itself)
    after = fwd.iloc[1:].reset_index(drop=True)
    n = len(after)

    out: dict = dict(event)
    out["spy_gate_on"] = bool(spy_gate.get(ed, True))
    out["candidate_rank_that_day"] = rank_by_date.get((ed, tkr))
    out["event_close_price"] = round(entry_close, 4)

    # close-to-close forward returns
    for w in FWD_WINDOWS:
        if w <= n:
            c = float(after.iloc[w - 1]["close"])
            out[f"return_{w}d"] = c / entry_close - 1.0
        else:
            out[f"return_{w}d"] = np.nan

    # 90d MFE / MAE on closes (drawdown computed against running peak)
    if n > 0:
        closes = after["close"].astype(float).to_numpy()
        highs = after["high"].astype(float).to_numpy() if "high" in after.columns else closes
        lows = after["low"].astype(float).to_numpy() if "low" in after.columns else closes
        max_high = float(np.max(highs))
        min_low = float(np.min(lows))
        out["max_high_return_90d"] = max_high / entry_close - 1.0
        # Drawdown: minimum (close - running_peak)/running_peak
        peak = np.maximum.accumulate(np.concatenate([[entry_close], closes]))
        peak_after = peak[1:]
        dd = closes / peak_after - 1.0
        out["max_drawdown_90d"] = float(np.min(dd))
        # Days to MFE / MDD (1-indexed trading days)
        out["days_to_max_high"] = int(np.argmax(highs) + 1)
        out["days_to_max_drawdown"] = int(np.argmin(dd) + 1)
        out["high_price_90d"] = round(max_high, 4)
        out["low_price_90d"] = round(min_low, 4)
    else:
        for k in ("max_high_return_90d", "max_drawdown_90d", "days_to_max_high",
                  "days_to_max_drawdown", "high_price_90d", "low_price_90d"):
            out[k] = np.nan

    # z-score path over next 90 trading days
    z_series = skew_by_ticker_dict.get(tkr)
    if z_series is not None:
        # subset z_series to the dates strictly after ed, up to PATH_HORIZON entries
        sub = z_series[z_series["date"] > ed].head(PATH_HORIZON).reset_index(drop=True)
        if not sub.empty:
            zarr = sub["skew_z"].astype(float).to_numpy()
            dates = sub["date"].tolist()
            out["z_max_90d"] = float(np.max(zarr))
            out["z_min_90d"] = float(np.min(zarr))
            out["z_mean_90d"] = float(np.mean(zarr))
            out["z_median_90d"] = float(np.median(zarr))
            out["z_last_90d"] = float(zarr[-1])

            # First-cross days for the path
            def first_below(thr: float) -> int | None:
                m = np.where(zarr < thr)[0]
                return int(m[0] + 1) if m.size else None

            def first_above(thr: float) -> int | None:
                m = np.where(zarr > thr)[0]
                return int(m[0] + 1) if m.size else None

            def first_at_or_above(thr: float) -> int | None:
                m = np.where(zarr >= thr)[0]
                return int(m[0] + 1) if m.size else None

            def first_at_or_below(thr: float) -> int | None:
                m = np.where(zarr <= thr)[0]
                return int(m[0] + 1) if m.size else None

            out["days_until_z_below_3"] = first_below(3.0)
            out["days_until_z_below_1_5"] = first_below(1.5)
            out["days_until_z_below_0"] = first_below(0.0)
            out["days_until_z_above_0"] = first_above(0.0)
            out["days_until_z_above_minus1"] = first_above(-1.0)
            out["days_until_z_above_minus1_5"] = first_above(-1.5)

            # Counts and first dates for high-z and deep-negative-z
            out["count_z_ge_3_next_90d"] = int(np.sum(zarr >= 3.0))
            out["count_z_ge_6_next_90d"] = int(np.sum(zarr >= 6.0))
            out["count_z_ge_7_next_90d"] = int(np.sum(zarr >= 7.0))
            out["count_z_le_minus3_next_90d"] = int(np.sum(zarr <= -3.0))
            out["count_z_le_minus6_next_90d"] = int(np.sum(zarr <= -6.0))

            def first_date_at_or_above(thr: float):
                m = np.where(zarr >= thr)[0]
                return dates[int(m[0])].date().isoformat() if m.size else None

            def first_date_at_or_below(thr: float):
                m = np.where(zarr <= thr)[0]
                return dates[int(m[0])].date().isoformat() if m.size else None

            out["first_z_ge_6_date"] = first_date_at_or_above(6.0)
            out["first_z_ge_7_date"] = first_date_at_or_above(7.0)
            out["first_z_le_minus3_date"] = first_date_at_or_below(-3.0)
            out["first_z_le_minus6_date"] = first_date_at_or_below(-6.0)
        else:
            for k in ("z_max_90d", "z_min_90d", "z_mean_90d", "z_median_90d", "z_last_90d",
                      "days_until_z_below_3", "days_until_z_below_1_5", "days_until_z_below_0",
                      "days_until_z_above_0", "days_until_z_above_minus1",
                      "days_until_z_above_minus1_5",
                      "count_z_ge_3_next_90d", "count_z_ge_6_next_90d", "count_z_ge_7_next_90d",
                      "count_z_le_minus3_next_90d", "count_z_le_minus6_next_90d",
                      "first_z_ge_6_date", "first_z_ge_7_date",
                      "first_z_le_minus3_date", "first_z_le_minus6_date"):
                out[k] = None if k.endswith("_date") else np.nan

    return out


# ---------------------------------------------------------------------------
# Candidate-rank lookup
# ---------------------------------------------------------------------------

def build_candidate_rank(skew_df: pd.DataFrame, z_min: float = Z_PROD) -> dict:
    """For each date, rank all tickers with skew_z >= z_min by descending z.
    Returns {(date, ticker): rank_int_starting_at_1}.
    """
    qual = skew_df[skew_df["skew_z"] >= z_min].copy()
    if qual.empty:
        return {}
    qual = qual.sort_values(["date", "skew_z"], ascending=[True, False])
    qual["rank"] = qual.groupby("date").cumcount() + 1
    return {(r.date, r.underlying): int(r.rank) for r in qual.itertuples(index=False)}


def rank_to_bucket(r: int | float | None) -> str:
    if r is None or (isinstance(r, float) and np.isnan(r)):
        return "unknown"
    r = int(r)
    if r == 1:
        return "rank_1"
    if r == 2:
        return "rank_2"
    if 3 <= r <= 5:
        return "rank_3_to_5"
    return "rank_6_plus"


# ---------------------------------------------------------------------------
# Summary tables
# ---------------------------------------------------------------------------

def _agg_metrics(sub: pd.DataFrame) -> dict:
    def avg(col): return float(sub[col].mean()) if col in sub and sub[col].notna().any() else np.nan
    def med(col): return float(sub[col].median()) if col in sub and sub[col].notna().any() else np.nan
    def winrate(col): return float((sub[col] > 0).mean()) if col in sub and sub[col].notna().any() else np.nan
    return {
        "n": len(sub),
        "avg_return_5d": avg("return_5d"),
        "median_return_5d": med("return_5d"),
        "win_rate_5d": winrate("return_5d"),
        "avg_return_20d": avg("return_20d"),
        "median_return_20d": med("return_20d"),
        "win_rate_20d": winrate("return_20d"),
        "avg_return_90d": avg("return_90d"),
        "median_return_90d": med("return_90d"),
        "win_rate_90d": winrate("return_90d"),
        "avg_max_high_90d": avg("max_high_return_90d"),
        "median_max_high_90d": med("max_high_return_90d"),
        "avg_max_drawdown_90d": avg("max_drawdown_90d"),
        "median_max_drawdown_90d": med("max_drawdown_90d"),
        "avg_days_to_max_high": avg("days_to_max_high"),
    }


def summary_by_bucket(events: pd.DataFrame, bucket_col: str, bucket_order: list[str]) -> pd.DataFrame:
    rows = []
    for b in bucket_order:
        sub = events[events[bucket_col] == b]
        if sub.empty:
            rows.append({"z_bucket": b, "n": 0})
            continue
        m = _agg_metrics(sub)
        m["z_bucket"] = b
        rows.append(m)
    df = pd.DataFrame(rows)
    cols = ["z_bucket"] + [c for c in df.columns if c != "z_bucket"]
    return df[cols]


def summary_by_bucket_split(events: pd.DataFrame, bucket_col: str, bucket_order: list[str],
                             split_col: str, split_values: list) -> pd.DataFrame:
    rows = []
    for b in bucket_order:
        for sv in split_values:
            sub = events[(events[bucket_col] == b) & (events[split_col] == sv)]
            m = _agg_metrics(sub)
            m["z_bucket"] = b
            m[split_col] = sv
            rows.append(m)
    df = pd.DataFrame(rows)
    cols = ["z_bucket", split_col] + [c for c in df.columns if c not in ("z_bucket", split_col)]
    return df[cols]


def high_z_exhaustion_summary(events: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for thr in (3.0, 4.0, 5.0, 6.0, 7.0):
        sub = events[events["z_start"] >= thr]
        if sub.empty:
            rows.append({"z_start_threshold": f">= {thr}", "n": 0})
            continue
        # Gave back >=50% of MFE: max_high_return - return_90d >= 0.5 * max_high_return
        mfe = sub["max_high_return_90d"]
        ret90 = sub["return_90d"]
        gaveback = ((mfe - ret90) >= 0.5 * mfe).fillna(False)
        rows.append({
            "z_start_threshold": f">= {thr}",
            "n": len(sub),
            "avg_return_90d": float(ret90.mean()) if ret90.notna().any() else np.nan,
            "avg_max_high_90d": float(mfe.mean()) if mfe.notna().any() else np.nan,
            "avg_drawdown_90d": float(sub["max_drawdown_90d"].mean()) if sub["max_drawdown_90d"].notna().any() else np.nan,
            "avg_days_to_high": float(sub["days_to_max_high"].mean()) if sub["days_to_max_high"].notna().any() else np.nan,
            "pct_90d_return_lt_5pct": float((ret90 < 0.05).mean()),
            "pct_max_high_ge_20pct": float((mfe >= 0.20).mean()),
            "pct_gave_back_50pct_of_mfe": float(gaveback.mean()),
        })
    return pd.DataFrame(rows)


def negative_z_summary(events: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for thr in (-1.0, -2.0, -3.0, -5.0, -7.0):
        sub = events[events["z_start"] <= thr]
        if sub.empty:
            rows.append({"z_start_threshold": f"<= {thr}", "n": 0})
            continue
        ret90 = sub["return_90d"]
        mfe = sub["max_high_return_90d"]
        dd = sub["max_drawdown_90d"]
        rows.append({
            "z_start_threshold": f"<= {thr}",
            "n": len(sub),
            "avg_return_90d": float(ret90.mean()) if ret90.notna().any() else np.nan,
            "avg_max_high_90d": float(mfe.mean()) if mfe.notna().any() else np.nan,
            "avg_drawdown_90d": float(dd.mean()) if dd.notna().any() else np.nan,
            "win_rate_90d": float((ret90 > 0).mean()) if ret90.notna().any() else np.nan,
            "pct_positive_90d": float((ret90 > 0).mean()) if ret90.notna().any() else np.nan,
            "pct_max_high_ge_10pct": float((mfe >= 0.10).mean()),
            "pct_drawdown_le_minus10pct": float((dd <= -0.10).mean()),
        })
    return pd.DataFrame(rows)


def z_path_summary_str(row) -> str:
    parts = [f"start={row['z_start']:+.1f}"]
    if pd.notna(row.get("z_max_90d")) and pd.notna(row.get("days_to_max_high")):
        # days_to_max_high is price-MFE day, not z-max day; report z_max separately
        parts.append(f"max={row['z_max_90d']:+.1f}")
    if pd.notna(row.get("z_min_90d")):
        parts.append(f"min={row['z_min_90d']:+.1f}")
    if pd.notna(row.get("z_last_90d")):
        parts.append(f"last={row['z_last_90d']:+.1f}")
    if pd.notna(row.get("count_z_ge_6_next_90d")):
        parts.append(f"ge6_count={int(row['count_z_ge_6_next_90d'])}")
    if pd.notna(row.get("count_z_le_minus3_next_90d")):
        parts.append(f"le-3_count={int(row['count_z_le_minus3_next_90d'])}")
    return " | ".join(parts)


# ---------------------------------------------------------------------------
# Study D — in-position z reversal
# ---------------------------------------------------------------------------

def in_position_reversal(
    trade_log: pd.DataFrame,
    variant_label: str,
    bars_by_ticker: dict,
    skew_by_ticker_dict: dict,
) -> list[dict]:
    """For each baseline trade, walk the holding window and record first-cross
    z events plus return-at and return-from-event-to-exit metrics."""
    out: list[dict] = []
    trade_log = trade_log.copy()
    trade_log["entry_date"] = pd.to_datetime(trade_log["entry_date"]).dt.normalize()
    trade_log["exit_date"] = pd.to_datetime(trade_log["exit_date"]).dt.normalize()

    for t in trade_log.itertuples(index=False):
        tkr = t.ticker
        bars = bars_by_ticker.get(tkr)
        if bars is None or bars.empty:
            continue
        held = bars[(bars["date"] >= t.entry_date) & (bars["date"] <= t.exit_date)].reset_index(drop=True)
        if len(held) < 2:
            continue
        entry_price = float(held.iloc[0]["open"])
        exit_price = float(held.iloc[-1]["close"])
        if entry_price <= 0:
            continue
        final_ret = exit_price / entry_price - 1.0

        # In-trade closes / returns
        closes = held["close"].astype(float).to_numpy()
        rets = closes / entry_price - 1.0
        max_ret = float(np.max(rets))
        min_ret = float(np.min(rets))

        # z path during hold
        z_series = skew_by_ticker_dict.get(tkr)
        z_in_hold = pd.DataFrame()
        if z_series is not None:
            z_in_hold = z_series[(z_series["date"] >= t.entry_date)
                                  & (z_series["date"] <= t.exit_date)].reset_index(drop=True)

        entry_z = np.nan
        if not z_in_hold.empty:
            r0 = z_in_hold[z_in_hold["date"] == t.entry_date]
            if not r0.empty:
                entry_z = float(r0.iloc[0]["skew_z"])

        row = {
            "strategy_variant": variant_label,
            "ticker": tkr,
            "entry_date": t.entry_date.date().isoformat(),
            "exit_date": t.exit_date.date().isoformat(),
            "entry_z": entry_z,
            "final_trade_return": final_ret,
            "max_return_during_trade": max_ret,
            "min_return_during_trade": min_ret,
            "exit_reason": getattr(t, "exit_reason", ""),
        }

        def find_first_geq(thr: float, prefix: str):
            if z_in_hold.empty:
                row[f"{prefix}_date"] = None
                row[f"{prefix}_value"] = np.nan
                row[f"return_at_{prefix}"] = np.nan
                row[f"return_after_{prefix}_to_exit"] = np.nan
                return
            hits = z_in_hold[z_in_hold["skew_z"] >= thr]
            if hits.empty:
                row[f"{prefix}_date"] = None
                row[f"{prefix}_value"] = np.nan
                row[f"return_at_{prefix}"] = np.nan
                row[f"return_after_{prefix}_to_exit"] = np.nan
                return
            first = hits.iloc[0]
            d = first["date"]
            row[f"{prefix}_date"] = d.date().isoformat()
            row[f"{prefix}_value"] = float(first["skew_z"])
            # close at d (or next available)
            on_or_after = held[held["date"] >= d]
            if on_or_after.empty:
                row[f"return_at_{prefix}"] = np.nan
                row[f"return_after_{prefix}_to_exit"] = np.nan
            else:
                price_at = float(on_or_after.iloc[0]["close"])
                row[f"return_at_{prefix}"] = price_at / entry_price - 1.0
                row[f"return_after_{prefix}_to_exit"] = exit_price / price_at - 1.0

        def find_first_leq(thr: float, prefix: str):
            if z_in_hold.empty:
                row[f"{prefix}_date"] = None
                row[f"{prefix}_value"] = np.nan
                row[f"return_at_{prefix}"] = np.nan
                row[f"return_after_{prefix}_to_exit"] = np.nan
                return
            hits = z_in_hold[z_in_hold["skew_z"] <= thr]
            if hits.empty:
                row[f"{prefix}_date"] = None
                row[f"{prefix}_value"] = np.nan
                row[f"return_at_{prefix}"] = np.nan
                row[f"return_after_{prefix}_to_exit"] = np.nan
                return
            first = hits.iloc[0]
            d = first["date"]
            row[f"{prefix}_date"] = d.date().isoformat()
            row[f"{prefix}_value"] = float(first["skew_z"])
            on_or_after = held[held["date"] >= d]
            if on_or_after.empty:
                row[f"return_at_{prefix}"] = np.nan
                row[f"return_after_{prefix}_to_exit"] = np.nan
            else:
                price_at = float(on_or_after.iloc[0]["close"])
                row[f"return_at_{prefix}"] = price_at / entry_price - 1.0
                row[f"return_after_{prefix}_to_exit"] = exit_price / price_at - 1.0

        find_first_geq(6.0, "first_z_ge_6")
        find_first_geq(7.0, "first_z_ge_7")
        find_first_leq(-2.0, "first_z_le_minus2")
        find_first_leq(-3.0, "first_z_le_minus3")
        find_first_leq(-5.0, "first_z_le_minus5")

        out.append(row)
    return out


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

def write_report(out_dir: Path,
                 events: pd.DataFrame,
                 sum_default: pd.DataFrame,
                 sum_collapsed: pd.DataFrame,
                 sum_gate: pd.DataFrame,
                 sum_rank: pd.DataFrame,
                 high_z: pd.DataFrame,
                 neg_z: pd.DataFrame,
                 study_d_present: bool,
                 study_d_path: Path | None,
                 cooldown_days: int,
                 z_prod_thr: float,
                 cli_args: argparse.Namespace) -> None:
    pos_z3 = events[events["z_start"] >= 3.0]
    pos_z6 = events[events["z_start"] >= 6.0]
    pos_z7 = events[events["z_start"] >= 7.0]
    neg_z1 = events[events["z_start"] <= -1.0]
    neg_z3 = events[events["z_start"] <= -3.0]

    def md_table(df: pd.DataFrame, *, fmt_pct_cols=(), fmt_int_cols=("n",), fmt_dec_cols=()) -> str:
        if df.empty:
            return "_(no rows)_"
        d = df.copy()
        for c in d.columns:
            if c in fmt_pct_cols:
                d[c] = d[c].map(lambda v: f"{v:+.1%}" if pd.notna(v) else "—")
            elif c in fmt_dec_cols:
                d[c] = d[c].map(lambda v: f"{v:+.2f}" if pd.notna(v) else "—")
            elif c in fmt_int_cols:
                d[c] = d[c].map(lambda v: f"{int(v):,}" if pd.notna(v) else "—")
        return d.to_markdown(index=False)

    pct_cols_default = (
        "avg_return_5d", "median_return_5d", "win_rate_5d",
        "avg_return_20d", "median_return_20d", "win_rate_20d",
        "avg_return_90d", "median_return_90d", "win_rate_90d",
        "avg_max_high_90d", "median_max_high_90d",
        "avg_max_drawdown_90d", "median_max_drawdown_90d",
    )
    dec_cols_default = ("avg_days_to_max_high",)

    pct_cols_high = (
        "avg_return_90d", "avg_max_high_90d", "avg_drawdown_90d",
        "pct_90d_return_lt_5pct", "pct_max_high_ge_20pct", "pct_gave_back_50pct_of_mfe",
    )
    dec_cols_high = ("avg_days_to_high",)

    pct_cols_neg = (
        "avg_return_90d", "avg_max_high_90d", "avg_drawdown_90d",
        "win_rate_90d", "pct_positive_90d",
        "pct_max_high_ge_10pct", "pct_drawdown_le_minus10pct",
    )

    lines = []
    lines.append("# Z-Score Bucket Event Study\n")
    lines.append(f"_Generated by `zscore_event_study.py`._\n")
    lines.append(f"_Cooldown: {cooldown_days} trading days. "
                 f"Production candidate threshold: z >= {z_prod_thr}._\n")

    lines.append("## 1. Sign convention\n")
    lines.append(
        "`skew_5otm = call_iv_5otm - put_iv_5otm`. The replay engine "
        "(`replay.load_skew_lookup`) computes `skew_z` as the z-score of "
        "today's `skew_5otm` against the prior 60 trading days "
        "(`shift(1).rolling(60)` — no look-ahead). \n\n"
        "- **z > 0** → call IV unusually high vs trailing 60d → bullish "
        "call-skew pressure. The Path S \"bullish\" production config fires on "
        "`z >= z_min` (positive direction).\n"
        "- **z < 0** → put IV unusually high vs trailing 60d → fear / put bid.\n\n"
        "There is no inversion or transformation — positive z is the bullish "
        "direction by construction. Negative z under the existing "
        "implementation is left untraded by Path S bullish; this study tests "
        "what it would have meant if observed.\n"
    )

    lines.append(f"## 2. Universe & event counts\n")
    lines.append(f"- Total events (first-cross, cooldown={cooldown_days}d): **{len(events):,}**")
    lines.append(f"- Tickers: **{events['ticker'].nunique():,}**")
    if not events.empty:
        lines.append(f"- Date range: {events['event_date'].min().date()} → "
                     f"{events['event_date'].max().date()}")
    lines.append("")

    lines.append("## 3. Forward returns by z bucket (default)\n")
    lines.append(md_table(sum_default, fmt_pct_cols=pct_cols_default,
                           fmt_dec_cols=dec_cols_default))
    lines.append("\n## 4. Forward returns by z bucket (collapsed)\n")
    lines.append(md_table(sum_collapsed, fmt_pct_cols=pct_cols_default,
                           fmt_dec_cols=dec_cols_default))

    lines.append("\n## 5. Bucket × SPY-gate split\n")
    lines.append(md_table(sum_gate, fmt_pct_cols=pct_cols_default,
                           fmt_dec_cols=dec_cols_default))

    lines.append("\n## 6. Bucket × candidate-rank split (rank computed at z>=3.0)\n")
    lines.append(md_table(sum_rank, fmt_pct_cols=pct_cols_default,
                           fmt_dec_cols=dec_cols_default))

    lines.append("\n## 7. High-z exhaustion summary\n")
    lines.append(md_table(high_z, fmt_pct_cols=pct_cols_high,
                           fmt_dec_cols=dec_cols_high))

    lines.append("\n## 8. Negative-z summary\n")
    lines.append(md_table(neg_z, fmt_pct_cols=pct_cols_neg))

    # Auto-generated commentary
    def _safe_mean(s):
        return float(s.mean()) if s.notna().any() else np.nan

    pos_z3_mean_90 = _safe_mean(pos_z3["return_90d"]) if not pos_z3.empty else np.nan
    pos_z3_mean_mfe = _safe_mean(pos_z3["max_high_return_90d"]) if not pos_z3.empty else np.nan
    pos_z3_winrate = float((pos_z3["return_90d"] > 0).mean()) if not pos_z3.empty else np.nan
    pos_z6_mean_90 = _safe_mean(pos_z6["return_90d"]) if not pos_z6.empty else np.nan
    pos_z7_mean_90 = _safe_mean(pos_z7["return_90d"]) if not pos_z7.empty else np.nan
    neg_z1_mean_90 = _safe_mean(neg_z1["return_90d"]) if not neg_z1.empty else np.nan
    neg_z3_mean_90 = _safe_mean(neg_z3["return_90d"]) if not neg_z3.empty else np.nan

    lines.append("\n## 9. Findings (data-driven, brief)\n")
    lines.append(f"- **z>=3 events:** n={len(pos_z3):,}, mean 90d return = "
                 f"{pos_z3_mean_90:+.2%}, mean 90d MFE = {pos_z3_mean_mfe:+.2%}, "
                 f"win rate (90d>0) = {pos_z3_winrate:.1%} (where defined).")
    lines.append(f"- **z>=6 events:** n={len(pos_z6):,}, mean 90d return = "
                 f"{pos_z6_mean_90:+.2%}.")
    lines.append(f"- **z>=7 events:** n={len(pos_z7):,}, mean 90d return = "
                 f"{pos_z7_mean_90:+.2%}.")
    lines.append(f"- **z<=-1 events:** n={len(neg_z1):,}, mean 90d return = "
                 f"{neg_z1_mean_90:+.2%}.")
    lines.append(f"- **z<=-3 events:** n={len(neg_z3):,}, mean 90d return = "
                 f"{neg_z3_mean_90:+.2%}.")
    lines.append("")
    lines.append("Interpretation guide (read against the tables above, not in isolation):")
    lines.append(
        "- If avg-90d-return monotonically rises with z bucket through z=3, "
        "z>=3 is supported as an entry threshold. If it plateaus or declines "
        "past z=5–6, that's an exhaustion signature and tighter trail or "
        "earlier exit is worth a backtest pass."
    )
    lines.append(
        "- If z<=-3 events show negative or noisy 90d returns with elevated "
        "drawdowns, negative z is best used as an avoid-long filter rather "
        "than a contrarian entry. If z<=-3 mean-90d is materially positive, "
        "there's a contrarian-bounce hypothesis to investigate further "
        "(separate study)."
    )
    lines.append(
        "- The bucket × rank split shows whether picking only the rank-1/2 "
        "names (production rule) is doing the heavy lifting vs sub-ranks "
        "in the same z bucket."
    )
    lines.append("")

    lines.append("## 10. In-position z reversal study\n")
    if study_d_present and study_d_path is not None:
        lines.append(f"See `{study_d_path.name}`. Use it to check whether "
                     f"intra-trade z>=6/7 spikes precede tops (price gives back "
                     f"after the spike) and whether intra-trade z<=-2/-3 dips "
                     f"precede stop-outs.")
    else:
        lines.append("_Skipped — baseline trade logs not present at "
                     f"{[str(p) for _, p in BASELINE_TRADE_LOGS]}._ "
                     "Run again locally with the trade logs in place, or sync "
                     "them to the runner's `results/` directory.")

    lines.append("\n## 11. Conclusions (preliminary; read tables for evidence)\n")
    lines.append(
        "- **z>=3 as entry threshold:** _verdict from the z-bucket table._ "
        "Compare avg_return_90d at the 3-to-4 bucket vs 1-to-2 and 0-to-1 "
        "buckets. A positive monotone gradient through z=3 supports keeping "
        "the threshold. A flat or noisy gradient suggests the 3.0 cutoff is "
        "convenient, not edge-bearing.\n"
        "- **z>=6 / z>=7:** _verdict from the high-z exhaustion table._ "
        "If `pct_gave_back_50pct_of_mfe` is high and `avg_return_90d` is "
        "lower than at z>=3 while MFE stays large, that's exhaustion — "
        "consider testing a tighter trail above z>=6, NOT a tighter entry.\n"
        "- **Negative z:** _verdict from the negative-z table._ If win rate "
        "and avg-90d-return are weak, treat negative z as avoid-long input. "
        "If there's an asymmetric MFE/drawdown profile, it's a candidate for "
        "an exit-warning rule applied to existing holdings, NOT a new entry.\n"
        "- **What to test next (only if data warrants):** the appropriate "
        "follow-up backtests are (a) a tighter trail kicking in once any held "
        "position prints z>=6 or z>=7 in-trade, and (b) an exit-warning rule "
        "if a held position's z drops <=-2 or <=-3 within a window of entry. "
        "Both are intra-trade rules — they do NOT change the entry filter and "
        "preserve the existing entry edge while testing exit hygiene.\n"
    )

    lines.append("\n## 12. Files\n")
    lines.append("- `events.csv` — every first-cross event with all return + z-path columns")
    lines.append("- `summary_by_z_bucket.csv`, `summary_by_z_bucket_collapsed.csv`")
    lines.append("- `summary_by_z_bucket_spy_gate.csv`, `summary_by_z_bucket_rank.csv`")
    lines.append("- `high_z_exhaustion_summary.csv`, `negative_z_summary.csv`")
    lines.append("- `positive_z3_events.csv`, `positive_z3_top_by_high_90d.csv`,")
    lines.append("  `positive_z3_worst_by_return_90d.csv`, `positive_z3_high_z_events.csv`")
    lines.append("- `negative_z_events.csv`")
    if study_d_present:
        lines.append("- `in_position_z_reversal_study.csv`")
    lines.append(f"\n_CLI args: cooldown-days={cli_args.cooldown_days}, "
                 f"z-prod={cli_args.z_prod_threshold}_\n")

    (out_dir / "report.md").write_text("\n".join(lines))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--cooldown-days", type=int, default=20,
                    help="Trading days between same-bucket events for the same ticker")
    ap.add_argument("--z-prod-threshold", type=float, default=Z_PROD,
                    help="Production candidate z threshold (used for rank lookup)")
    ap.add_argument("--out-dir", type=Path, default=OUT_DIR)
    ap.add_argument("--skew-path", type=Path, default=DATA_DIR / "skew_daily.parquet")
    ap.add_argument("--stocks-path", type=Path, default=DATA_DIR / "aggs_daily_adjusted.parquet")
    args = ap.parse_args()

    out_dir = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"[zscore-study] loading skew z from {args.skew_path}", file=sys.stderr)
    skew_df = load_skew_z(args.skew_path)
    print(f"[zscore-study] {len(skew_df):,} (ticker,date) z-values, "
          f"{skew_df['underlying'].nunique():,} tickers, "
          f"{skew_df['date'].min().date()} → {skew_df['date'].max().date()}",
          file=sys.stderr)

    print(f"[zscore-study] loading bars from {args.stocks_path}", file=sys.stderr)
    bars_by_ticker = load_bars(args.stocks_path)

    print(f"[zscore-study] computing SPY 200d gate", file=sys.stderr)
    spy_gate = load_spy_gate(args.stocks_path)

    print(f"[zscore-study] building rank lookup at z >= {args.z_prod_threshold}", file=sys.stderr)
    rank_by_date = build_candidate_rank(skew_df, z_min=args.z_prod_threshold)

    print(f"[zscore-study] detecting first-cross events "
          f"(cooldown={args.cooldown_days})", file=sys.stderr)
    events_raw = detect_first_cross_events(skew_df, cooldown_days=args.cooldown_days)
    print(f"[zscore-study] raw events: {len(events_raw):,}", file=sys.stderr)

    # Pre-index skew by ticker for fast forward path lookups
    skew_by_ticker_dict = {t: g.sort_values("date").reset_index(drop=True)
                            for t, g in skew_df.groupby("underlying")}

    print(f"[zscore-study] computing forward features...", file=sys.stderr)
    feature_rows = []
    for i, ev in enumerate(events_raw.to_dict("records")):
        if i and i % 10000 == 0:
            print(f"   ... {i:,}/{len(events_raw):,}", file=sys.stderr)
        feat = compute_event_features(ev, bars_by_ticker, skew_by_ticker_dict,
                                       spy_gate=spy_gate, rank_by_date=rank_by_date)
        if feat is not None:
            feature_rows.append(feat)
    events = pd.DataFrame(feature_rows)
    print(f"[zscore-study] features computed: {len(events):,} events with bars",
          file=sys.stderr)

    if events.empty:
        print("[zscore-study] no events with valid bars — abort", file=sys.stderr)
        return 1

    # Write events.csv
    events_sorted = events.sort_values(["event_date", "z_start"], ascending=[True, False])
    events_sorted.to_csv(out_dir / "events.csv", index=False)

    # Bucket order
    default_order = [b[0] for b in DEFAULT_BUCKETS]
    collapsed_order = [b[0] for b in COLLAPSED_BUCKETS]

    # Summaries
    sum_default = summary_by_bucket(events, "z_bucket", default_order)
    sum_default.to_csv(out_dir / "summary_by_z_bucket.csv", index=False)

    sum_collapsed = summary_by_bucket(events, "z_bucket_collapsed", collapsed_order)
    sum_collapsed.to_csv(out_dir / "summary_by_z_bucket_collapsed.csv", index=False)

    sum_gate = summary_by_bucket_split(events, "z_bucket", default_order,
                                        "spy_gate_on", [True, False])
    sum_gate.to_csv(out_dir / "summary_by_z_bucket_spy_gate.csv", index=False)

    events["rank_bucket"] = events["candidate_rank_that_day"].apply(rank_to_bucket)
    rank_order = ["rank_1", "rank_2", "rank_3_to_5", "rank_6_plus", "unknown"]
    sum_rank = summary_by_bucket_split(events, "z_bucket", default_order,
                                        "rank_bucket", rank_order)
    sum_rank.to_csv(out_dir / "summary_by_z_bucket_rank.csv", index=False)

    high_z = high_z_exhaustion_summary(events)
    high_z.to_csv(out_dir / "high_z_exhaustion_summary.csv", index=False)

    neg_z = negative_z_summary(events)
    neg_z.to_csv(out_dir / "negative_z_summary.csv", index=False)

    # Study B — z>=3 detail
    pos_z3 = events[events["z_start"] >= 3.0].copy()
    if not pos_z3.empty:
        pos_z3["z_path_summary"] = pos_z3.apply(z_path_summary_str, axis=1)
        cols_keep_b = [
            "event_date", "ticker", "z_start", "z_bucket", "spy_gate_on",
            "candidate_rank_that_day", "event_close_price",
            "return_5d", "return_10d", "return_20d", "return_45d", "return_90d",
            "max_high_return_90d", "max_drawdown_90d", "days_to_max_high",
            "z_path_summary",
        ]
        pos_z3_b = pos_z3[cols_keep_b].sort_values(
            ["event_date", "z_start"], ascending=[True, False]
        )
        pos_z3_b.to_csv(out_dir / "positive_z3_events.csv", index=False)

        pos_z3_top = pos_z3.sort_values("max_high_return_90d", ascending=False).head(200)
        pos_z3_top[cols_keep_b].to_csv(out_dir / "positive_z3_top_by_high_90d.csv", index=False)

        pos_z3_worst = pos_z3.sort_values("return_90d", ascending=True).head(200)
        pos_z3_worst[cols_keep_b].to_csv(out_dir / "positive_z3_worst_by_return_90d.csv", index=False)

        pos_z6 = pos_z3[pos_z3["z_start"] >= 6.0]
        if not pos_z6.empty:
            pos_z6[cols_keep_b].sort_values(
                ["event_date", "z_start"], ascending=[True, False]
            ).to_csv(out_dir / "positive_z3_high_z_events.csv", index=False)

    # Study C — negative z events
    neg_z_events = events[events["z_start"] <= -1.0].copy()
    if not neg_z_events.empty:
        neg_z_events["z_path_summary"] = neg_z_events.apply(z_path_summary_str, axis=1)
        cols_keep_c = [
            "event_date", "ticker", "z_start", "z_bucket", "spy_gate_on",
            "event_close_price",
            "return_5d", "return_10d", "return_20d", "return_45d", "return_90d",
            "max_high_return_90d", "max_drawdown_90d", "days_to_max_high",
            "z_max_90d", "z_min_90d", "z_last_90d",
            "count_z_ge_3_next_90d", "count_z_le_minus3_next_90d",
            "z_path_summary",
        ]
        neg_z_events[cols_keep_c].sort_values(
            ["event_date", "z_start"], ascending=[True, True]
        ).to_csv(out_dir / "negative_z_events.csv", index=False)

    # Study D — in-position z reversal
    study_d_path = out_dir / "in_position_z_reversal_study.csv"
    study_d_present = False
    d_rows: list[dict] = []
    for label, p in BASELINE_TRADE_LOGS:
        if not p.exists():
            print(f"[zscore-study] Study D: trade log missing {p} — skipping",
                  file=sys.stderr)
            continue
        print(f"[zscore-study] Study D: processing {label} from {p}", file=sys.stderr)
        tl = pd.read_csv(p)
        d_rows.extend(in_position_reversal(tl, label, bars_by_ticker, skew_by_ticker_dict))
    if d_rows:
        pd.DataFrame(d_rows).to_csv(study_d_path, index=False)
        study_d_present = True

    write_report(out_dir,
                 events=events,
                 sum_default=sum_default,
                 sum_collapsed=sum_collapsed,
                 sum_gate=sum_gate,
                 sum_rank=sum_rank,
                 high_z=high_z,
                 neg_z=neg_z,
                 study_d_present=study_d_present,
                 study_d_path=study_d_path if study_d_present else None,
                 cooldown_days=args.cooldown_days,
                 z_prod_thr=args.z_prod_threshold,
                 cli_args=args)

    print(f"[zscore-study] DONE — wrote {out_dir}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
