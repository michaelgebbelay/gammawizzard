"""Price/volume features used by the v0 explosive scanner.

Every feature is computed on data available at end-of-day `t`. Forward-looking
labels live in `labels.py` and start at `t+1`. The two must never share
helpers — keeping them separate prevents accidental lookahead.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def _per_symbol(df: pd.DataFrame, fn) -> pd.Series:
    return df.groupby("symbol", sort=False, group_keys=False).apply(fn)


def build_features(
    universe: pd.DataFrame,
    *,
    realized_vol_window: int = 20,
    atr_window: int = 14,
    max_return_windows: tuple[int, ...] = (20, 60),
    max_gap_window: int = 20,
    skew_window: int = 60,
    volume_ratio_window: int = 20,
    volume_z_window: int = 60,
    high_lookback_windows: tuple[int, ...] = (20, 60),
    sma_windows: tuple[int, ...] = (10, 20),
    prior_high_windows: tuple[int, ...] = (5, 20),
) -> pd.DataFrame:
    """Compute features for each (symbol, date).

    Input must be the output of `build_universe()`. Rows where
    `is_tradeable=False` are kept (so feature values exist for them) but they
    will be excluded from cross-sectional scoring downstream.
    """
    if "symbol" not in universe.columns:
        raise ValueError("universe must have 'symbol' column")

    df = universe.sort_values(["symbol", "date"]).reset_index(drop=True).copy()
    g = df.groupby("symbol", sort=False)

    prev_close = g["close"].shift(1)
    df["ret_1d"] = df["close"] / prev_close - 1.0
    df["gap_pct"] = df["open"] / prev_close - 1.0
    df["intraday_range_pct"] = (df["high"] - df["low"]) / df["close"]

    df["_true_range"] = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    df["atr_pct_14d"] = (
        df.groupby("symbol", sort=False)["_true_range"].transform(
            lambda s: s.rolling(atr_window, min_periods=atr_window).mean()
        )
        / df["close"]
    )
    df = df.drop(columns=["_true_range"])

    df["realized_vol_20d"] = g["ret_1d"].transform(
        lambda s: s.rolling(realized_vol_window, min_periods=realized_vol_window).std()
    )

    for w in max_return_windows:
        df[f"max_ret_{w}d"] = g["ret_1d"].transform(
            lambda s, _w=w: s.rolling(_w, min_periods=_w).max()
        )

    df[f"max_gap_{max_gap_window}d"] = g["gap_pct"].transform(
        lambda s: s.rolling(max_gap_window, min_periods=max_gap_window).max()
    )

    df[f"return_skew_{skew_window}d"] = g["ret_1d"].transform(
        lambda s: s.rolling(skew_window, min_periods=skew_window).skew()
    )

    df["avg_volume_20d"] = g["volume"].transform(
        lambda s: s.shift(1).rolling(volume_ratio_window, min_periods=volume_ratio_window).mean()
    )
    df["avg_dollar_volume_20d_feat"] = g["dollar_volume"].transform(
        lambda s: s.shift(1).rolling(volume_ratio_window, min_periods=volume_ratio_window).mean()
    )
    df["volume_ratio_20d"] = df["volume"] / df["avg_volume_20d"]
    df["dollar_volume_ratio_20d"] = df["dollar_volume"] / df["avg_dollar_volume_20d_feat"]

    df["_log_volume"] = np.log(df["volume"].where(df["volume"] > 0))
    log_vol_mean = df.groupby("symbol", sort=False)["_log_volume"].transform(
        lambda s: s.shift(1).rolling(volume_z_window, min_periods=volume_z_window).mean()
    )
    log_vol_std = df.groupby("symbol", sort=False)["_log_volume"].transform(
        lambda s: s.shift(1).rolling(volume_z_window, min_periods=volume_z_window).std()
    )
    df["log_volume_z_60d"] = (df["_log_volume"] - log_vol_mean) / log_vol_std.replace(0, np.nan)
    df = df.drop(columns=["_log_volume"])

    for w in high_lookback_windows:
        roll_high = g["high"].transform(lambda s, _w=w: s.rolling(_w, min_periods=_w).max())
        df[f"close_vs_{w}d_high"] = df["close"] / roll_high

    for w in sma_windows:
        sma = g["close"].transform(lambda s, _w=w: s.rolling(_w, min_periods=_w).mean())
        df[f"sma_{w}d"] = sma
        df[f"above_sma_{w}d"] = (df["close"] > sma).astype("Int8")

    for w in prior_high_windows:
        df[f"prior_{w}d_high"] = g["high"].transform(
            lambda s, _w=w: s.shift(1).rolling(_w, min_periods=_w).max()
        )

    return df


FEATURE_COLUMNS = (
    "ret_1d",
    "gap_pct",
    "intraday_range_pct",
    "atr_pct_14d",
    "realized_vol_20d",
    "max_ret_20d",
    "max_ret_60d",
    "max_gap_20d",
    "return_skew_60d",
    "volume_ratio_20d",
    "dollar_volume_ratio_20d",
    "log_volume_z_60d",
    "close_vs_20d_high",
    "close_vs_60d_high",
    "above_sma_10d",
    "above_sma_20d",
    "prior_5d_high",
    "prior_20d_high",
)
