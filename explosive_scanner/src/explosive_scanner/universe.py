"""Tradeable-universe construction.

Inputs:  daily_bars conforming to data_contracts/daily_bars.md
Outputs: universe_daily — one row per (symbol, date) with `is_tradeable` flag
         and `exclusion_reason` string for diagnostics.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

REQUIRED_COLS = ("symbol", "date", "open", "high", "low", "close", "volume")


def _validate(bars: pd.DataFrame) -> None:
    missing = [c for c in REQUIRED_COLS if c not in bars.columns]
    if missing:
        raise ValueError(f"daily_bars missing required columns: {missing}")
    if bars.duplicated(subset=["symbol", "date"]).any():
        raise ValueError("daily_bars has duplicate (symbol, date) rows")


def build_universe(
    bars: pd.DataFrame,
    *,
    min_price: float = 2.0,
    min_avg_dollar_volume_20d: float = 10_000_000.0,
    avg_dollar_volume_window: int = 20,
) -> pd.DataFrame:
    """Filter daily_bars into universe_daily.

    A row is tradeable on date `t` iff:
      - close >= min_price
      - 20-day trailing average dollar volume (computed on the *prior*
        `avg_dollar_volume_window` days, not including `t`) is >= the
        threshold
      - OHLC are all positive and form a valid bar (low <= open, close <= high)
      - volume is non-null and >= 0

    The 20-day average uses prior days only to avoid forward-leaking today's
    dollar volume into today's universe membership. This matters because
    today's volume is what makes the stock a candidate.
    """
    _validate(bars)

    df = bars.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    df = df.sort_values(["symbol", "date"]).reset_index(drop=True)

    if "dollar_volume" not in df.columns:
        df["dollar_volume"] = df["close"] * df["volume"]

    # Trailing avg using prior days only — shift before rolling.
    df["avg_dollar_vol_20d"] = (
        df.groupby("symbol", sort=False)["dollar_volume"]
        .transform(lambda s: s.shift(1).rolling(avg_dollar_volume_window, min_periods=avg_dollar_volume_window).mean())
    )

    valid_bar = (
        df[["open", "high", "low", "close"]].gt(0).all(axis=1)
        & (df["low"] <= df[["open", "close"]].min(axis=1))
        & (df["high"] >= df[["open", "close"]].max(axis=1))
        & df["volume"].notna()
        & (df["volume"] >= 0)
    )

    reasons = pd.Series("", index=df.index, dtype=object)
    reasons = reasons.mask(~valid_bar, "invalid_bar")
    reasons = reasons.mask(reasons.eq("") & df["close"].lt(min_price), "below_min_price")
    reasons = reasons.mask(reasons.eq("") & df["avg_dollar_vol_20d"].isna(), "insufficient_history")
    reasons = reasons.mask(
        reasons.eq("") & df["avg_dollar_vol_20d"].lt(min_avg_dollar_volume_20d),
        "below_min_dollar_volume",
    )

    df["is_tradeable"] = reasons.eq("")
    df["exclusion_reason"] = reasons.where(~df["is_tradeable"], np.nan)

    return df[
        [
            "symbol",
            "date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "dollar_volume",
            "avg_dollar_vol_20d",
            "is_tradeable",
            "exclusion_reason",
        ]
    ]
