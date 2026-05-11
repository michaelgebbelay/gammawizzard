#!/usr/bin/env python3
"""Shared point-in-time Path S universe helpers.

These helpers are intentionally lightweight so both the backtest variant
runner and the live Path S refresh path can use the same PIT universe logic.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd


HERE = Path(__file__).resolve().parent
DATA_DIR = HERE / "data"
PIT_CACHE_DIR = DATA_DIR / "pit_cache"
PIT_MIN_TRAILING_SESSIONS = 20


def pit_cache_path(*, target_n: int, window_sessions: int) -> Path:
    return PIT_CACHE_DIR / f"pit_option_liq_{target_n}_{window_sessions}d.parquet"


def build_or_load_pit_universe_table(
    *,
    core_tickers: list[str],
    eligible_df: pd.DataFrame,
    session_dates: pd.DatetimeIndex,
    skew_df: pd.DataFrame,
    target_n: int,
    window_sessions: int,
    rebuild: bool = False,
) -> tuple[pd.DataFrame, dict]:
    cache_path = pit_cache_path(target_n=target_n, window_sessions=window_sessions)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    required_cols = {
        "date",
        "ticker",
        "rank",
        "score",
        "valid_skew_coverage",
        "option_liquidity_metric",
        "stock_liquidity_metric",
    }
    if cache_path.exists() and not rebuild:
        cached = pd.read_parquet(cache_path)
        if required_cols.issubset(cached.columns):
            cached["date"] = pd.to_datetime(cached["date"]).dt.normalize()
            cached["ticker"] = cached["ticker"].astype(str).str.upper()
            return cached, {
                "cache_path": str(cache_path),
                "cache_hit": True,
                "rows": int(len(cached)),
            }

    core = [str(t).upper() for t in core_tickers]
    core_set = set(core)
    skew_core = (
        skew_df[skew_df["underlying"].isin(core_set)][["date", "underlying"]]
        .drop_duplicates()
        .assign(valid_skew=1)
    )
    skew_presence = (
        skew_core.pivot(index="date", columns="underlying", values="valid_skew")
        .reindex(index=session_dates, columns=core)
        .fillna(0.0)
    )
    skew_coverage = skew_presence.shift(1, fill_value=0.0).rolling(
        window_sessions,
        min_periods=1,
    ).sum()

    core_bars = eligible_df[eligible_df["ticker"].isin(core_set)][
        ["date", "ticker", "dollar_vol"]
    ].copy()
    dollar_vol = (
        core_bars.pivot(index="date", columns="ticker", values="dollar_vol")
        .reindex(index=session_dates, columns=core)
    )
    stock_liq = dollar_vol.shift(1).rolling(
        window_sessions,
        min_periods=min(PIT_MIN_TRAILING_SESSIONS, window_sessions),
    ).median()

    rows: list[pd.DataFrame] = []
    for dt in session_dates:
        coverage = skew_coverage.loc[dt]
        liq = stock_liq.loc[dt].fillna(0.0)
        day = pd.DataFrame(
            {
                "ticker": core,
                "valid_skew_coverage": coverage.to_numpy(dtype=float),
                "option_liquidity_metric": coverage.to_numpy(dtype=float),
                "stock_liquidity_metric": liq.to_numpy(dtype=float),
            }
        )
        day = day.sort_values(
            ["valid_skew_coverage", "stock_liquidity_metric", "ticker"],
            ascending=[False, False, True],
        ).head(target_n).reset_index(drop=True)
        day["rank"] = np.arange(1, len(day) + 1, dtype=int)
        stock_rank = day["stock_liquidity_metric"].rank(
            pct=True, method="average"
        ).fillna(0.0)
        day["score"] = (
            day["valid_skew_coverage"].astype(float)
            + stock_rank.astype(float) / 10_000.0
        )
        day["date"] = dt
        rows.append(
            day[
                [
                    "date",
                    "ticker",
                    "rank",
                    "score",
                    "valid_skew_coverage",
                    "option_liquidity_metric",
                    "stock_liquidity_metric",
                ]
            ]
        )

    out = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame(
        columns=[
            "date",
            "ticker",
            "rank",
            "score",
            "valid_skew_coverage",
            "option_liquidity_metric",
            "stock_liquidity_metric",
        ]
    )
    out.to_parquet(cache_path, index=False)
    return out, {
        "cache_path": str(cache_path),
        "cache_hit": False,
        "rows": int(len(out)),
    }
