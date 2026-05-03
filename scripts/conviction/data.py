"""
Schwab-backed daily-bar fetch with on-disk CSV cache.

Cache layout: scripts/conviction/cache/<TICKER>.csv with columns
[date, open, high, low, close, volume]. Re-running pulls only missing trailing
days, so a refreshed scan over a 100-name universe is cheap.
"""
from __future__ import annotations

import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable

import pandas as pd


def _add_scripts_root() -> None:
    cur = os.path.abspath(os.path.dirname(__file__))
    while True:
        if os.path.basename(cur) == "scripts":
            if cur not in sys.path:
                sys.path.append(cur)
            return
        parent = os.path.dirname(cur)
        if parent == cur:
            return
        cur = parent


_add_scripts_root()
from schwab_token_keeper import schwab_client  # noqa: E402


CACHE_DIR = Path(__file__).resolve().parent / "cache"
CACHE_DIR.mkdir(exist_ok=True)


@dataclass
class FetchResult:
    ticker: str
    bars: pd.DataFrame  # date-indexed daily bars
    error: str | None = None


def _cache_path(ticker: str) -> Path:
    return CACHE_DIR / f"{ticker.upper()}.csv"


def _load_cache(ticker: str) -> pd.DataFrame:
    p = _cache_path(ticker)
    if not p.exists():
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])
    df = pd.read_csv(p, parse_dates=["date"])
    return df.sort_values("date").reset_index(drop=True)


def _save_cache(ticker: str, df: pd.DataFrame) -> None:
    df = df.sort_values("date").drop_duplicates("date", keep="last").reset_index(drop=True)
    df.to_csv(_cache_path(ticker), index=False)


def _bars_from_schwab(client, ticker: str, start: datetime, end: datetime) -> pd.DataFrame:
    r = client.get_price_history_every_day(
        ticker,
        start_datetime=start,
        end_datetime=end,
        need_extended_hours_data=False,
    )
    r.raise_for_status()
    candles = r.json().get("candles") or []
    if not candles:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])
    df = pd.DataFrame(candles)
    df["date"] = pd.to_datetime(df["datetime"], unit="ms", utc=True).dt.tz_convert("America/New_York").dt.normalize().dt.tz_localize(None)
    return df[["date", "open", "high", "low", "close", "volume"]]


def fetch_daily_bars(
    tickers: Iterable[str],
    *,
    lookback_days: int = 400,
    sleep_between: float = 0.05,
    refresh: bool = False,
) -> dict[str, FetchResult]:
    """Fetch (or top up) daily bars for each ticker.

    `lookback_days` ≈ ~13 trading months — enough for 12-month momentum + 200d SMA.
    `refresh=True` ignores the cache and re-pulls everything.
    """
    client = schwab_client()
    # All datetimes here are tz-naive for clean comparison with the cached
    # `date` column (which pandas parses as tz-naive). schwab-py accepts
    # naive datetimes and treats them as UTC.
    end = datetime.now()
    start_full = end - timedelta(days=lookback_days + 30)  # buffer for weekends/holidays

    results: dict[str, FetchResult] = {}
    for tkr in tickers:
        tkr = tkr.upper()
        try:
            cached = pd.DataFrame() if refresh else _load_cache(tkr)
            if cached.empty:
                fetched = _bars_from_schwab(client, tkr, start_full, end)
                merged = fetched
            else:
                last_date = cached["date"].max().to_pydatetime()
                # always re-pull last ~5 days to overwrite any provisional bar
                top_start = max(start_full, last_date - timedelta(days=5))
                fetched = _bars_from_schwab(client, tkr, top_start, end)
                merged = pd.concat([cached, fetched], ignore_index=True)

            if merged.empty:
                results[tkr] = FetchResult(tkr, merged, error="no candles returned")
                continue

            _save_cache(tkr, merged)
            cutoff = end - timedelta(days=lookback_days)
            window = merged[merged["date"] >= pd.Timestamp(cutoff)].reset_index(drop=True)
            results[tkr] = FetchResult(tkr, window)
        except Exception as e:  # noqa: BLE001
            results[tkr] = FetchResult(tkr, pd.DataFrame(), error=f"{type(e).__name__}: {e}")

        if sleep_between:
            time.sleep(sleep_between)

    return results


def fetch_spx_returns(*, refresh: bool = False) -> dict[str, float | None]:
    """Pull SPX daily bars and return 3m / 6m / 12m-skip-month total returns.

    Used to compute relative-strength factors. Cached on disk via the same
    bar cache as individual tickers.
    """
    res = fetch_daily_bars(["$SPX"], refresh=refresh)
    fr = res.get("$SPX")
    if fr is None or fr.bars is None or fr.bars.empty:
        return {"spx_3m": None, "spx_6m": None, "spx_12m_x1": None}
    close = fr.bars.sort_values("date")["close"].astype(float).reset_index(drop=True)
    last = float(close.iloc[-1])

    def _at(n: int) -> float | None:
        if len(close) <= n:
            return None
        return float(close.iloc[-1 - n])

    def _ret(numer: float | None, denom: float | None) -> float | None:
        if numer is None or denom is None or denom == 0:
            return None
        return numer / denom - 1.0

    return {
        "spx_3m": _ret(last, _at(63)),
        "spx_6m": _ret(last, _at(126)),
        "spx_12m_x1": _ret(_at(21), _at(252)) if len(close) > 252 else None,
    }
