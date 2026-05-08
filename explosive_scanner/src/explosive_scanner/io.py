"""Data loaders.

The scanner is decoupled from any specific vendor: the public API consumes a
`daily_bars` DataFrame matching `data_contracts/daily_bars.md`. This module
provides a concrete loader for the local Massive parquet cache so the v0
event study can run without needing a separate ingest.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[3]
MASSIVE_PARQUET = REPO_ROOT / "scripts" / "conviction" / "backtest" / "data" / "aggs_daily_adjusted.parquet"
TICKER_METADATA = REPO_ROOT / "scripts" / "conviction" / "backtest" / "data" / "ticker_metadata.parquet"


def load_massive_bars(
    *,
    parquet_path: Optional[Path] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    tickers: Optional[Iterable[str]] = None,
    require_common_stock: bool = True,
    primary_exchanges: tuple[str, ...] = ("XNYS", "XNAS", "ARCX", "BATS"),
) -> pd.DataFrame:
    """Load split-adjusted Massive bars and map to the daily_bars contract.

    Returns columns: symbol, date, open, high, low, close, volume, dollar_volume.
    """
    path = Path(parquet_path) if parquet_path else MASSIVE_PARQUET
    if not path.exists():
        raise FileNotFoundError(
            f"Massive adjusted parquet not found at {path}. "
            "Run scripts/conviction/backtest/run_massive_ingest.sh --full "
            "and --fetch-splits to populate."
        )

    bars = pd.read_parquet(
        path,
        columns=["ticker", "date", "open", "high", "low", "close", "volume"],
    )
    bars = bars.rename(columns={"ticker": "symbol"})
    bars["date"] = pd.to_datetime(bars["date"]).dt.normalize()

    if start_date is not None:
        bars = bars[bars["date"] >= pd.Timestamp(start_date)]
    if end_date is not None:
        bars = bars[bars["date"] <= pd.Timestamp(end_date)]

    if require_common_stock or tickers is not None:
        allowed: Optional[set[str]] = None
        if tickers is not None:
            allowed = {t.upper() for t in tickers}
        elif require_common_stock and TICKER_METADATA.exists():
            meta = pd.read_parquet(TICKER_METADATA)
            mask = pd.Series(True, index=meta.index)
            if "type" in meta.columns:
                mask &= meta["type"].astype(str).str.upper().eq("CS")
            if "primary_exchange" in meta.columns:
                mask &= meta["primary_exchange"].isin(primary_exchanges)
            allowed = set(meta.loc[mask, "ticker"].astype(str).str.upper())
        if allowed is not None:
            bars = bars[bars["symbol"].astype(str).str.upper().isin(allowed)]

    bars["dollar_volume"] = bars["close"] * bars["volume"]
    bars = bars.sort_values(["symbol", "date"]).reset_index(drop=True)
    return bars[["symbol", "date", "open", "high", "low", "close", "volume", "dollar_volume"]]
