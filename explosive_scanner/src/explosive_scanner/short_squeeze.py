"""Short-squeeze ignition research helpers.

This module builds a point-in-time daily signal panel using:
  - Massive split-adjusted daily bars
  - FINRA semi-monthly short-interest files
  - Massive Benzinga news (optional, for catalyst-confirmed variants)

It also provides a simple fixed-hold backtest and Path S sleeve-comparison
helpers so the five requested variants can be evaluated in a repeatable way.
"""

from __future__ import annotations

import html
import json
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

import numpy as np
import pandas as pd
import requests

from explosive_scanner.features import build_features
from explosive_scanner.io import load_massive_bars
from explosive_scanner.trade_simulator import (
    attach_entry_paths,
    build_ledger,
    exit_fixed_horizon,
)
from explosive_scanner.universe import build_universe


SCANNER_ROOT = Path(__file__).resolve().parents[2]
REPO_ROOT = Path(__file__).resolve().parents[3]
CACHE_DIR = SCANNER_ROOT / "experiments" / "_cache"
FINRA_CACHE_DIR = CACHE_DIR / "finra_short_interest"
NEWS_CACHE_PATH = CACHE_DIR / "benzinga_news_short_squeeze.json"
FINRA_URL_TEMPLATE = "https://cdn.finra.org/equity/otcmarket/biweekly/shrt{stamp}.csv"
DEFAULT_PATH_S_DAILY_EQUITY = (
    REPO_ROOT
    / "scripts"
    / "conviction"
    / "backtest"
    / "results"
    / "2026-05-05_replay_1460d_massive_n1_baseline_repro"
    / "daily_equity.csv"
)

CORE_IGNITION = "core_ignition"
NUCLEAR_IGNITION = "nuclear_ignition"
RISING_SHORT_INTEREST_IGNITION = "rising_short_interest_ignition"
CATALYST_CONFIRMED_IGNITION = "catalyst_confirmed_ignition"
SECOND_DAY_CONFIRMATION = "second_day_confirmation"

CATALYST_REGEX = re.compile(
    r"("
    r"earnings|guidance|raises?\s+outlook|raised\s+outlook|contract|partnership|"
    r"strategic\s+review|activist|buyback|acquisition|fda\s+approval|"
    r"settlement|debt\s+refinancing"
    r")",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class VariantSpec:
    name: str
    min_days_to_cover: float
    min_days_to_cover_pctile: float | None
    min_ret_1d: float
    min_rel_vol_20: float
    min_close_location: float
    min_avg_dollar_volume: float | None = None
    min_price: float | None = None
    min_short_interest_change_pct: float | None = None
    require_catalyst: bool = False
    requires_day2_confirmation: bool = False


VARIANT_SPECS: dict[str, VariantSpec] = {
    CORE_IGNITION: VariantSpec(
        name=CORE_IGNITION,
        min_days_to_cover=3.0,
        min_days_to_cover_pctile=0.80,
        min_ret_1d=0.12,
        min_rel_vol_20=5.0,
        min_close_location=0.75,
        min_avg_dollar_volume=5_000_000.0,
        min_price=2.0,
    ),
    NUCLEAR_IGNITION: VariantSpec(
        name=NUCLEAR_IGNITION,
        min_days_to_cover=5.0,
        min_days_to_cover_pctile=0.90,
        min_ret_1d=0.15,
        min_rel_vol_20=7.0,
        min_close_location=0.85,
        min_avg_dollar_volume=10_000_000.0,
        min_price=3.0,
    ),
    RISING_SHORT_INTEREST_IGNITION: VariantSpec(
        name=RISING_SHORT_INTEREST_IGNITION,
        min_days_to_cover=3.0,
        min_days_to_cover_pctile=None,
        min_ret_1d=0.10,
        min_rel_vol_20=4.0,
        min_close_location=0.75,
        min_short_interest_change_pct=0.10,
    ),
    CATALYST_CONFIRMED_IGNITION: VariantSpec(
        name=CATALYST_CONFIRMED_IGNITION,
        min_days_to_cover=3.0,
        min_days_to_cover_pctile=0.80,
        min_ret_1d=0.12,
        min_rel_vol_20=5.0,
        min_close_location=0.75,
        min_avg_dollar_volume=5_000_000.0,
        min_price=2.0,
        require_catalyst=True,
    ),
    SECOND_DAY_CONFIRMATION: VariantSpec(
        name=SECOND_DAY_CONFIRMATION,
        min_days_to_cover=3.0,
        min_days_to_cover_pctile=None,
        min_ret_1d=0.12,
        min_rel_vol_20=5.0,
        min_close_location=0.75,
        requires_day2_confirmation=True,
    ),
}


def prepare_price_volume_panel(
    *,
    analysis_start: str,
    analysis_end: str,
    lookback_calendar_days: int = 120,
    min_price: float = 2.0,
    min_avg_dollar_volume_20d: float = 5_000_000.0,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DatetimeIndex]:
    """Build the price/volume panel for squeeze signals.

    The returned panel includes a pre-start lookback window so percentile,
    liquidity, and day-2 confirmation logic can use only prior information.
    """
    start_ts = pd.Timestamp(analysis_start).normalize()
    end_ts = pd.Timestamp(analysis_end).normalize()
    preload_start = (start_ts - pd.Timedelta(days=lookback_calendar_days)).date().isoformat()
    load_end = (end_ts + pd.Timedelta(days=10)).date().isoformat()

    bars = load_massive_bars(
        start_date=preload_start,
        end_date=load_end,
        require_common_stock=True,
    )
    universe = build_universe(
        bars,
        min_price=min_price,
        min_avg_dollar_volume_20d=min_avg_dollar_volume_20d,
    )
    feats = build_features(universe)
    panel = feats[feats["is_tradeable"]].copy()
    spread = panel["high"] - panel["low"]
    panel["close_location"] = np.where(
        spread > 0,
        (panel["close"] - panel["low"]) / spread,
        np.nan,
    )
    panel["relative_volume_20d"] = panel["volume_ratio_20d"]
    panel["avg_20d_dollar_volume"] = panel["avg_dollar_vol_20d"]
    panel = panel.sort_values(["symbol", "date"]).reset_index(drop=True)
    trading_dates = pd.DatetimeIndex(np.sort(bars["date"].unique()))
    return panel, bars, trading_dates


def settlement_dates_from_calendar(
    trading_dates: Iterable[pd.Timestamp],
    *,
    start_date: str | pd.Timestamp | None = None,
    end_date: str | pd.Timestamp | None = None,
) -> list[pd.Timestamp]:
    """Infer FINRA settlement dates from the exchange trading calendar.

    We treat the mid-month report as the last trading day on or before the 15th
    and the month-end report as the last trading day of the month.
    """
    all_dates = pd.DatetimeIndex(pd.to_datetime(list(trading_dates))).sort_values().normalize().unique()
    if len(all_dates) == 0:
        return []
    settlements: list[pd.Timestamp] = []
    periods = pd.period_range(all_dates.min(), all_dates.max(), freq="M")
    for period in periods:
        in_month = all_dates[(all_dates.year == period.year) & (all_dates.month == period.month)]
        if len(in_month) == 0:
            continue
        mid = in_month[in_month.day <= 15]
        if len(mid) > 0:
            settlements.append(pd.Timestamp(mid.max()).normalize())
        settlements.append(pd.Timestamp(in_month.max()).normalize())
    out = pd.DatetimeIndex(settlements).unique().sort_values()
    if start_date is not None:
        out = out[out >= pd.Timestamp(start_date).normalize()]
    if end_date is not None:
        out = out[out <= pd.Timestamp(end_date).normalize()]
    return sorted(out.to_list())


def _finra_cache_key(settlement_dates: list[pd.Timestamp]) -> str:
    if not settlement_dates:
        return "empty"
    first = pd.Timestamp(settlement_dates[0]).strftime("%Y%m%d")
    last = pd.Timestamp(settlement_dates[-1]).strftime("%Y%m%d")
    return f"{first}_{last}_{len(settlement_dates)}"


def load_finra_short_interest(
    trading_dates: Iterable[pd.Timestamp],
    *,
    start_date: str | pd.Timestamp | None = None,
    end_date: str | pd.Timestamp | None = None,
    refresh: bool = False,
) -> pd.DataFrame:
    """Load historical FINRA short-interest files for the trading window."""
    settlements = settlement_dates_from_calendar(
        trading_dates,
        start_date=start_date,
        end_date=end_date,
    )
    cache_key = _finra_cache_key(settlements)
    cache_path = CACHE_DIR / f"finra_short_interest_{cache_key}.parquet"
    if cache_path.exists() and not refresh:
        return pd.read_parquet(cache_path)

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    FINRA_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    session = requests.Session()
    frames: list[pd.DataFrame] = []

    for settlement in settlements:
        stamp = pd.Timestamp(settlement).strftime("%Y%m%d")
        raw_path = FINRA_CACHE_DIR / f"shrt{stamp}.csv"
        if refresh or not raw_path.exists():
            url = FINRA_URL_TEMPLATE.format(stamp=stamp)
            resp = session.get(url, timeout=60)
            resp.raise_for_status()
            raw_path.write_bytes(resp.content)
            time.sleep(0.02)

        df = pd.read_csv(raw_path, sep="|")
        cols = [
            "symbolCode",
            "settlementDate",
            "currentShortPositionQuantity",
            "previousShortPositionQuantity",
            "averageDailyVolumeQuantity",
            "daysToCoverQuantity",
            "changePercent",
            "marketClassCode",
            "issuerServicesGroupExchangeCode",
        ]
        missing = [c for c in cols if c not in df.columns]
        if missing:
            raise ValueError(f"FINRA short-interest file {raw_path.name} missing columns: {missing}")
        sub = df[cols].rename(
            columns={
                "symbolCode": "symbol",
                "settlementDate": "settlement_date",
                "currentShortPositionQuantity": "current_short_interest",
                "previousShortPositionQuantity": "previous_short_interest",
                "averageDailyVolumeQuantity": "average_daily_volume",
                "daysToCoverQuantity": "days_to_cover",
                "changePercent": "short_interest_change_percent",
                "marketClassCode": "market_class_code",
                "issuerServicesGroupExchangeCode": "exchange_code",
            }
        )
        sub["symbol"] = sub["symbol"].astype(str).str.upper()
        sub["settlement_date"] = pd.to_datetime(sub["settlement_date"]).dt.normalize()
        for col in (
            "current_short_interest",
            "previous_short_interest",
            "average_daily_volume",
            "days_to_cover",
            "short_interest_change_percent",
        ):
            sub[col] = pd.to_numeric(sub[col], errors="coerce")
        sub["short_interest_change_pct"] = sub["short_interest_change_percent"] / 100.0
        sub.loc[sub["average_daily_volume"].le(0), "days_to_cover"] = np.nan
        frames.append(sub)

    if not frames:
        return pd.DataFrame(
            columns=[
                "symbol",
                "settlement_date",
                "current_short_interest",
                "previous_short_interest",
                "average_daily_volume",
                "days_to_cover",
                "short_interest_change_pct",
                "market_class_code",
                "exchange_code",
            ]
        )

    out = (
        pd.concat(frames, ignore_index=True)
        .drop_duplicates(subset=["symbol", "settlement_date"], keep="last")
        .sort_values(["symbol", "settlement_date"])
        .reset_index(drop=True)
    )
    out.to_parquet(cache_path, index=False)
    return out


def _percent_rank(s: pd.Series) -> pd.Series:
    valid = s.notna()
    out = pd.Series(np.nan, index=s.index, dtype=float)
    n = int(valid.sum())
    if n == 0:
        return out
    if n == 1:
        out.loc[valid] = 0.0
        return out
    ranks = s[valid].rank(method="average")
    out.loc[valid] = (ranks - 1.0) / (n - 1.0)
    return out


def attach_short_interest(
    panel: pd.DataFrame,
    short_interest: pd.DataFrame,
    trading_dates: Iterable[pd.Timestamp],
    *,
    availability_lag_trading_days: int = 7,
) -> pd.DataFrame:
    """Join the latest available short-interest record to each panel row."""
    trade_dates = pd.DatetimeIndex(pd.to_datetime(list(trading_dates))).sort_values().normalize().unique()
    calendar = pd.DataFrame({"date": trade_dates, "trading_day_number": np.arange(len(trade_dates), dtype=int)})
    si = short_interest.copy()
    if si.empty:
        merged = panel.copy()
        for col in (
            "settlement_date",
            "si_available_date",
            "current_short_interest",
            "previous_short_interest",
            "average_daily_volume",
            "days_to_cover",
            "short_interest_change_pct",
        ):
            merged[col] = np.nan
        return merged

    si = si.merge(
        calendar,
        left_on="settlement_date",
        right_on="date",
        how="left",
    ).drop(columns=["date"])
    si["available_trading_day_number"] = si["trading_day_number"] + availability_lag_trading_days
    available_calendar = calendar.rename(
        columns={
            "date": "si_available_date",
            "trading_day_number": "available_trading_day_number",
        }
    )
    si = si.merge(available_calendar, on="available_trading_day_number", how="left")
    si = si.sort_values(["si_available_date", "symbol"]).reset_index(drop=True)

    left = panel.sort_values(["date", "symbol"]).reset_index(drop=True)
    merged = pd.merge_asof(
        left,
        si,
        by="symbol",
        left_on="date",
        right_on="si_available_date",
        direction="backward",
        allow_exact_matches=True,
    )
    merged["days_to_cover_pctile"] = merged.groupby("date", sort=False)["days_to_cover"].transform(_percent_rank)
    merged["rel_vol_pctile"] = merged.groupby("date", sort=False)["relative_volume_20d"].transform(_percent_rank)
    merged["ret_1d_pctile"] = merged.groupby("date", sort=False)["ret_1d"].transform(_percent_rank)
    merged["squeeze_score"] = (
        0.35 * merged["days_to_cover_pctile"]
        + 0.25 * merged["rel_vol_pctile"]
        + 0.20 * merged["ret_1d_pctile"]
        + 0.20 * merged["close_location"]
    )
    return merged


def _base_variant_mask(panel: pd.DataFrame, spec: VariantSpec) -> pd.Series:
    mask = (
        panel["days_to_cover"].ge(spec.min_days_to_cover)
        & panel["ret_1d"].ge(spec.min_ret_1d)
        & panel["relative_volume_20d"].ge(spec.min_rel_vol_20)
        & panel["close_location"].ge(spec.min_close_location)
    )
    if spec.min_days_to_cover_pctile is not None:
        mask &= panel["days_to_cover_pctile"].ge(spec.min_days_to_cover_pctile)
    if spec.min_avg_dollar_volume is not None:
        mask &= panel["avg_20d_dollar_volume"].ge(spec.min_avg_dollar_volume)
    if spec.min_price is not None:
        mask &= panel["close"].ge(spec.min_price)
    if spec.min_short_interest_change_pct is not None:
        mask &= panel["short_interest_change_pct"].ge(spec.min_short_interest_change_pct)
    return mask & panel["days_to_cover"].notna()


def build_variant_signal_frames(
    panel: pd.DataFrame,
    *,
    analysis_start: str,
    analysis_end: str,
) -> dict[str, pd.DataFrame]:
    """Create candidate frames for the non-catalyst variants."""
    start_ts = pd.Timestamp(analysis_start).normalize()
    end_ts = pd.Timestamp(analysis_end).normalize()
    window_mask = panel["date"].between(start_ts, end_ts)

    signals: dict[str, pd.DataFrame] = {}
    for name in (CORE_IGNITION, NUCLEAR_IGNITION, RISING_SHORT_INTEREST_IGNITION):
        spec = VARIANT_SPECS[name]
        sub = panel[_base_variant_mask(panel, spec) & window_mask].copy()
        sub["variant"] = name
        signals[name] = sub.reset_index(drop=True)

    day2_spec = VARIANT_SPECS[SECOND_DAY_CONFIRMATION]
    day1_mask = _base_variant_mask(panel, day2_spec)
    day2 = panel.copy()
    g = day2.groupby("symbol", sort=False)
    day2["day1_signal"] = day1_mask.to_numpy(dtype=bool)
    day2["prev_day1_signal"] = g["day1_signal"].shift(1, fill_value=False).astype(bool)
    day2["signal_date_day1"] = g["date"].shift(1)
    day2["close_day1"] = g["close"].shift(1)
    day2["volume_day1"] = g["volume"].shift(1)
    confirm_mask = (
        window_mask
        & day2["prev_day1_signal"]
        & day2["close"].gt(day2["close_day1"])
        & day2["volume"].ge(0.75 * day2["volume_day1"])
        & day2["close_location"].ge(0.60)
    )
    second_day = day2[confirm_mask].copy()
    second_day["variant"] = SECOND_DAY_CONFIRMATION
    second_day["confirmation_date"] = second_day["date"]
    signals[SECOND_DAY_CONFIRMATION] = second_day.reset_index(drop=True)
    return signals


def catalyst_match(article: dict) -> bool:
    parts = [
        article.get("title") or "",
        article.get("body") or "",
        " ".join(article.get("channels") or []),
        " ".join(article.get("tags") or []),
    ]
    text = html.unescape(" ".join(parts))
    text = re.sub(r"<[^>]+>", " ", text)
    return bool(CATALYST_REGEX.search(text))


class BenzingaNewsCache:
    """Small JSON cache for ticker/date-window Benzinga pulls."""

    def __init__(
        self,
        *,
        api_key: str | None = None,
        base_url: str | None = None,
        cache_path: Path | None = None,
    ):
        self.api_key = api_key or os.environ.get("MASSIVE_API_KEY")
        self.base_url = (base_url or os.environ.get("MASSIVE_API_BASE", "https://api.massive.com")).rstrip("/")
        self.cache_path = cache_path or NEWS_CACHE_PATH
        self._cache: dict[str, list[dict]] = {}
        self._dirty = False
        if self.cache_path.exists():
            self._cache = json.loads(self.cache_path.read_text())

    @property
    def enabled(self) -> bool:
        return bool(self.api_key)

    def save(self) -> None:
        if not self._dirty:
            return
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        self.cache_path.write_text(json.dumps(self._cache, indent=2))
        self._dirty = False

    def _cache_key(self, ticker: str, start_date: pd.Timestamp, end_date: pd.Timestamp) -> str:
        return f"{ticker.upper()}|{start_date:%Y-%m-%d}|{end_date:%Y-%m-%d}"

    def fetch_articles(
        self,
        ticker: str,
        *,
        start_date: pd.Timestamp,
        end_date: pd.Timestamp,
        refresh: bool = False,
    ) -> list[dict]:
        if not self.enabled:
            return []
        start_ts = pd.Timestamp(start_date).normalize()
        end_ts = pd.Timestamp(end_date).normalize()
        key = self._cache_key(ticker, start_ts, end_ts)
        if key in self._cache and not refresh:
            return self._cache[key]

        session = requests.Session()
        session.params = {"apiKey": self.api_key}
        params = {
            "tickers": ticker.upper(),
            "published.gte": start_ts.date().isoformat(),
            "published.lte": end_ts.date().isoformat(),
            "sort": "published.asc",
            "limit": 1000,
        }
        url = f"{self.base_url}/benzinga/v2/news"
        rows: list[dict] = []

        while url:
            payload = None
            for attempt in range(4):
                try:
                    resp = session.get(
                        url,
                        params=params if url.endswith("/news") else None,
                        timeout=120,
                    )
                    resp.raise_for_status()
                    payload = resp.json()
                    break
                except requests.RequestException as exc:
                    if attempt == 3:
                        print(f"[news] warning: {ticker} {start_ts:%Y-%m-%d}->{end_ts:%Y-%m-%d} failed: {exc}", flush=True)
                    else:
                        time.sleep(1.5 * (attempt + 1))
            if payload is None:
                break
            for item in payload.get("results", []) or []:
                rows.append(
                    {
                        "benzinga_id": item.get("benzinga_id"),
                        "published": item.get("published"),
                        "title": item.get("title"),
                        "body": item.get("body"),
                        "channels": item.get("channels") or [],
                        "tags": item.get("tags") or [],
                        "tickers": item.get("tickers") or [],
                    }
                )
            url = payload.get("next_url")
            params = None
            time.sleep(0.02)

        self._cache[key] = rows
        self._dirty = True
        return rows


def apply_catalyst_filter(
    core_signals: pd.DataFrame,
    trading_dates: Iterable[pd.Timestamp],
    news_cache: BenzingaNewsCache,
    *,
    prior_trading_days: int = 2,
) -> pd.DataFrame:
    """Require at least one catalyst-matching article in the recent window.

    Assumption: "prior two trading days" is interpreted as the signal day plus
    the two immediately preceding trading sessions.
    """
    if core_signals.empty or not news_cache.enabled:
        return core_signals.iloc[0:0].copy()

    dates = pd.DatetimeIndex(pd.to_datetime(list(trading_dates))).sort_values().normalize().unique()
    date_to_idx = {pd.Timestamp(d): i for i, d in enumerate(dates)}
    rows = []
    signal_dates = core_signals[["symbol", "date"]].drop_duplicates().sort_values(["symbol", "date"])
    grouped = list(signal_dates.groupby("symbol", sort=True))
    for n, (ticker, group) in enumerate(grouped, start=1):
        group_dates = pd.DatetimeIndex(pd.to_datetime(group["date"])).normalize().sort_values()
        query_start = pd.Timestamp(dates[max(date_to_idx[group_dates.min()] - prior_trading_days, 0)]).normalize()
        query_end = pd.Timestamp(group_dates.max()).normalize()
        articles = news_cache.fetch_articles(
            ticker,
            start_date=query_start,
            end_date=query_end,
        )
        matched_articles = []
        for article in articles:
            published = article.get("published")
            if not published or not catalyst_match(article):
                continue
            published_ts = pd.Timestamp(published)
            if published_ts.tzinfo is not None:
                published_ts = published_ts.tz_convert(None)
            matched_articles.append(
                {
                    "published_date": published_ts.normalize(),
                    "title": article.get("title"),
                }
            )
        for signal_date in group_dates:
            start_idx = max(date_to_idx[pd.Timestamp(signal_date)] - prior_trading_days, 0)
            window_start = pd.Timestamp(dates[start_idx]).normalize()
            hits = [
                article for article in matched_articles
                if window_start <= article["published_date"] <= pd.Timestamp(signal_date).normalize()
            ]
            rows.append(
                {
                    "symbol": ticker,
                    "date": pd.Timestamp(signal_date).normalize(),
                    "catalyst_article_count": len(hits),
                    "catalyst_match": bool(hits),
                    "catalyst_first_title": hits[0]["title"] if hits else None,
                }
            )
        if n % 25 == 0 or n == len(grouped):
            news_cache.save()
            print(f"[news] catalyst scan {n}/{len(grouped)} tickers", flush=True)
    flags = pd.DataFrame(rows)
    out = core_signals.merge(flags, on=["symbol", "date"], how="left")
    out["catalyst_article_count"] = out["catalyst_article_count"].fillna(0).astype(int)
    out["catalyst_match"] = out["catalyst_match"].fillna(False)
    return out[out["catalyst_match"]].reset_index(drop=True)


def build_variant_candidates(
    panel: pd.DataFrame,
    trading_dates: Iterable[pd.Timestamp],
    *,
    analysis_start: str,
    analysis_end: str,
    news_cache: BenzingaNewsCache | None = None,
) -> dict[str, pd.DataFrame]:
    """Build all five requested variant candidate sets."""
    signals = build_variant_signal_frames(
        panel,
        analysis_start=analysis_start,
        analysis_end=analysis_end,
    )

    if news_cache is not None and news_cache.enabled:
        catalyst = apply_catalyst_filter(
            signals[CORE_IGNITION],
            trading_dates,
            news_cache,
        )
    else:
        catalyst = signals[CORE_IGNITION].iloc[0:0].copy()
        catalyst["catalyst_article_count"] = pd.Series(dtype=int)
        catalyst["catalyst_match"] = pd.Series(dtype=bool)
        catalyst["catalyst_first_title"] = pd.Series(dtype=object)
    catalyst["variant"] = CATALYST_CONFIRMED_IGNITION
    signals[CATALYST_CONFIRMED_IGNITION] = catalyst.reset_index(drop=True)
    return signals


def apply_round_trip_cost_model(
    ledger: pd.DataFrame,
    *,
    per_side_large_liquid: float = 0.00125,
    per_side_small_mid: float = 0.00375,
    large_liquid_dollar_volume: float = 100_000_000.0,
) -> pd.DataFrame:
    """Baseline slippage model from the user brief."""
    out = ledger.copy()
    slip = np.where(
        out["entry_dollar_volume"].ge(large_liquid_dollar_volume),
        per_side_large_liquid,
        per_side_small_mid,
    )
    out["slippage_per_side"] = slip
    out["net_return"] = out["gross_return"] - 2.0 * out["slippage_per_side"]
    out["round_trip_cost"] = 2.0 * out["slippage_per_side"]
    return out


def apply_fixed_round_trip_cost(ledger: pd.DataFrame, *, round_trip_cost: float) -> pd.DataFrame:
    out = ledger.copy()
    out["slippage_per_side"] = round_trip_cost / 2.0
    out["net_return"] = out["gross_return"] - round_trip_cost
    out["round_trip_cost"] = round_trip_cost
    return out


def build_fixed_hold_ledger(
    candidates: pd.DataFrame,
    bars: pd.DataFrame,
    *,
    hold_days: int,
    score_col: str = "squeeze_score",
    max_exit_date: pd.Timestamp | None = None,
) -> pd.DataFrame:
    """Entry next open, exit at the fixed-horizon close."""
    if candidates.empty:
        return pd.DataFrame()
    forward_days = max(int(hold_days), 1)
    paths = attach_entry_paths(candidates, bars, forward_days=forward_days)
    exit_result = exit_fixed_horizon(paths, days=forward_days)
    extra_cols = [
        "variant",
        "days_to_cover",
        "days_to_cover_pctile",
        "relative_volume_20d",
        "ret_1d",
        "close_location",
        "avg_20d_dollar_volume",
        "current_short_interest",
        "short_interest_change_pct",
        "signal_date_day1",
        "confirmation_date",
        "catalyst_article_count",
        "catalyst_match",
        "catalyst_first_title",
    ]
    ledger = build_ledger(
        paths,
        exit_result,
        slippage_per_side=0.0,
        score_col=score_col,
        extra_cols=extra_cols,
    )
    if "signal_date_day1" in ledger.columns:
        ledger["confirmation_date"] = ledger["signal_date"]
        ledger["signal_date"] = ledger["signal_date_day1"].fillna(ledger["signal_date"])
    if max_exit_date is not None and not ledger.empty:
        ledger = ledger[pd.to_datetime(ledger["exit_date"]).dt.normalize() <= pd.Timestamp(max_exit_date).normalize()]
    return ledger.reset_index(drop=True)


def simulate_portfolio_on_trading_calendar(
    ledger: pd.DataFrame,
    trading_dates: Iterable[pd.Timestamp],
    *,
    max_positions: int,
    score_col: str = "squeeze_score",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Sequential portfolio simulator on the actual exchange calendar."""
    if ledger.empty:
        empty_port = pd.DataFrame(columns=["date", "n_pos", "realized", "equity", "exposure"])
        empty_port.attrs["accepted_count"] = 0
        empty_port.attrs["rejected_count"] = 0
        empty_port.attrs["weight"] = 0.0
        return empty_port, ledger.copy()

    led = ledger.copy()
    led["entry_date"] = pd.to_datetime(led["entry_date"]).dt.normalize()
    led["exit_date"] = pd.to_datetime(led["exit_date"]).dt.normalize()
    led = led.sort_values(["entry_date", score_col], ascending=[True, False]).reset_index(drop=True)

    trade_calendar = pd.DatetimeIndex(pd.to_datetime(list(trading_dates))).sort_values().normalize().unique()
    trade_calendar = trade_calendar[
        (trade_calendar >= led["entry_date"].min()) & (trade_calendar <= led["exit_date"].max())
    ]
    if len(trade_calendar) == 0:
        return pd.DataFrame(), pd.DataFrame()

    weight = 1.0 / max_positions
    cur = 0
    n = len(led)
    book: list[dict] = []
    accepted_idx: list[int] = []
    equity = 1.0
    rejected = 0
    rows: list[dict] = []
    eq_before: dict[int, float] = {}
    eq_after: dict[int, float] = {}

    for d in trade_calendar:
        still_active = [b for b in book if b["exit_date"] > d]
        active_symbols = {b["symbol"] for b in still_active}
        available_slots = max_positions - len(still_active)

        new_entries: list[dict] = []
        while cur < n and led["entry_date"].iat[cur] == d:
            i = cur
            sym = led["symbol"].iat[i]
            if sym in active_symbols or sym in {b["symbol"] for b in new_entries}:
                rejected += 1
            elif available_slots <= 0:
                rejected += 1
            else:
                new_entries.append(
                    {
                        "entry_date": d,
                        "exit_date": led["exit_date"].iat[i],
                        "symbol": sym,
                        "net_return": led["net_return"].iat[i],
                        "idx": i,
                    }
                )
                accepted_idx.append(i)
                eq_before[i] = equity
                available_slots -= 1
            cur += 1

        closing_today = [b for b in book if b["exit_date"] == d]
        book = still_active + new_entries + closing_today

        realized = 0.0
        exiting: list[dict] = []
        keepers: list[dict] = []
        for position in book:
            if position["exit_date"] == d:
                realized += position["net_return"] * weight
                exiting.append(position)
            elif position["exit_date"] > d:
                keepers.append(position)
        equity *= 1.0 + realized
        for position in exiting:
            eq_after[position["idx"]] = equity
        book = keepers
        rows.append(
            {
                "date": d,
                "n_pos": len(book),
                "realized": realized,
                "equity": equity,
                "exposure": len(book) * weight,
            }
        )

    port = pd.DataFrame(rows)
    accepted = led.iloc[accepted_idx].copy().reset_index(drop=True)
    accepted["portfolio_weight"] = weight
    accepted["portfolio_pnl_impact"] = accepted["net_return"] * weight
    accepted["equity_before_trade"] = [eq_before[i] for i in accepted_idx]
    accepted["equity_after_trade"] = [eq_after.get(i, np.nan) for i in accepted_idx]
    port.attrs["accepted_count"] = len(accepted)
    port.attrs["rejected_count"] = rejected
    port.attrs["weight"] = weight
    return port, accepted


def curve_metrics(returns: pd.Series) -> dict[str, float]:
    if returns.empty:
        return {
            "total_return": 0.0,
            "cagr": 0.0,
            "sharpe": np.nan,
            "max_drawdown": 0.0,
            "calmar": np.nan,
            "n_days": 0,
        }
    rets = returns.astype(float).fillna(0.0)
    equity = (1.0 + rets).cumprod()
    span_days = max((rets.index[-1] - rets.index[0]).days, 1)
    years = span_days / 365.25
    cagr = equity.iloc[-1] ** (1.0 / years) - 1.0
    vol = rets.std()
    sharpe = (rets.mean() / vol * np.sqrt(252.0)) if vol > 0 else np.nan
    peak = equity.cummax()
    drawdown = equity / peak - 1.0
    max_dd = float(drawdown.min()) if not drawdown.empty else 0.0
    calmar = cagr / abs(max_dd) if max_dd < 0 else np.nan
    return {
        "total_return": float(equity.iloc[-1] - 1.0),
        "cagr": float(cagr),
        "sharpe": float(sharpe) if sharpe == sharpe else np.nan,
        "max_drawdown": max_dd,
        "calmar": float(calmar) if calmar == calmar else np.nan,
        "n_days": int(len(rets)),
    }


def accepted_trade_metrics(accepted: pd.DataFrame) -> dict[str, float]:
    if accepted.empty:
        return {
            "n_trades": 0,
            "avg_return": np.nan,
            "median_return": np.nan,
            "win_rate": np.nan,
            "avg_winner": np.nan,
            "avg_loser": np.nan,
            "payoff_ratio": np.nan,
        }
    r = accepted["net_return"].astype(float)
    wins = r > 0
    avg_winner = r[wins].mean() if wins.any() else np.nan
    avg_loser = r[~wins].mean() if (~wins).any() else np.nan
    payoff_ratio = avg_winner / abs(avg_loser) if pd.notna(avg_winner) and pd.notna(avg_loser) and avg_loser < 0 else np.nan
    return {
        "n_trades": int(len(accepted)),
        "avg_return": float(r.mean()),
        "median_return": float(r.median()),
        "win_rate": float(wins.mean()),
        "avg_winner": float(avg_winner) if pd.notna(avg_winner) else np.nan,
        "avg_loser": float(avg_loser) if pd.notna(avg_loser) else np.nan,
        "payoff_ratio": float(payoff_ratio) if pd.notna(payoff_ratio) else np.nan,
    }


def remove_top_winners_recompute(
    accepted: pd.DataFrame,
    trading_dates: Iterable[pd.Timestamp],
    *,
    max_positions: int,
    score_col: str = "squeeze_score",
    top_n: int = 5,
) -> dict[str, float]:
    if accepted.empty:
        return {"return_ex_top_winners": np.nan, "top_winner_contribution": np.nan}
    top_idx = accepted["net_return"].nlargest(top_n).index
    top = accepted.loc[top_idx]
    total_sum = accepted["net_return"].sum()
    top_contribution = top["net_return"].sum() / total_sum if total_sum > 0 else np.nan
    trimmed = accepted.drop(index=top_idx, errors="ignore")
    port, _ = simulate_portfolio_on_trading_calendar(
        trimmed,
        trading_dates,
        max_positions=max_positions,
        score_col=score_col,
    )
    if port.empty:
        return {"return_ex_top_winners": np.nan, "top_winner_contribution": float(top_contribution) if pd.notna(top_contribution) else np.nan}
    metrics = curve_metrics(port.set_index("date")["realized"])
    return {
        "return_ex_top_winners": metrics["total_return"],
        "top_winner_contribution": float(top_contribution) if pd.notna(top_contribution) else np.nan,
    }


def load_path_s_daily_equity(path: str | Path = DEFAULT_PATH_S_DAILY_EQUITY) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    out = df[["date", "daily_return", "portfolio_value"]].rename(
        columns={"daily_return": "path_s_return", "portfolio_value": "path_s_equity"}
    )
    return out.sort_values("date").reset_index(drop=True)


def monthly_correlation(path_s_returns: pd.Series, sleeve_returns: pd.Series) -> float:
    monthly = pd.concat(
        [
            ((1.0 + path_s_returns).resample("ME").prod() - 1.0).rename("path_s"),
            ((1.0 + sleeve_returns).resample("ME").prod() - 1.0).rename("sleeve"),
        ],
        axis=1,
    ).dropna()
    if len(monthly) < 2:
        return np.nan
    return float(monthly["path_s"].corr(monthly["sleeve"]))


def blend_with_path_s(
    path_s: pd.DataFrame,
    sleeve_port: pd.DataFrame,
    *,
    sleeve_weight: float,
) -> dict[str, float]:
    merged = path_s.merge(
        sleeve_port[["date", "realized"]],
        on="date",
        how="left",
    )
    merged["realized"] = merged["realized"].fillna(0.0)
    merged["blend_return"] = (1.0 - sleeve_weight) * merged["path_s_return"] + sleeve_weight * merged["realized"]
    return curve_metrics(merged.set_index("date")["blend_return"])


def evaluate_variant(
    name: str,
    candidates: pd.DataFrame,
    bars: pd.DataFrame,
    trading_dates: Iterable[pd.Timestamp],
    path_s: pd.DataFrame,
    *,
    hold_days: int,
    max_positions: int,
    max_exit_date: pd.Timestamp | None = None,
) -> dict[str, object]:
    raw_ledger = build_fixed_hold_ledger(
        candidates,
        bars,
        hold_days=hold_days,
        max_exit_date=max_exit_date,
    )
    baseline_ledger = apply_round_trip_cost_model(raw_ledger)
    stress_150 = apply_fixed_round_trip_cost(raw_ledger, round_trip_cost=0.015)
    stress_200 = apply_fixed_round_trip_cost(raw_ledger, round_trip_cost=0.020)

    port, accepted = simulate_portfolio_on_trading_calendar(
        baseline_ledger,
        trading_dates,
        max_positions=max_positions,
    )
    stress_150_port, _ = simulate_portfolio_on_trading_calendar(
        stress_150,
        trading_dates,
        max_positions=max_positions,
    )
    stress_200_port, _ = simulate_portfolio_on_trading_calendar(
        stress_200,
        trading_dates,
        max_positions=max_positions,
    )

    baseline_curve = curve_metrics(port.set_index("date")["realized"]) if not port.empty else curve_metrics(pd.Series(dtype=float))
    stress_150_curve = curve_metrics(stress_150_port.set_index("date")["realized"]) if not stress_150_port.empty else curve_metrics(pd.Series(dtype=float))
    stress_200_curve = curve_metrics(stress_200_port.set_index("date")["realized"]) if not stress_200_port.empty else curve_metrics(pd.Series(dtype=float))
    trade_stats = accepted_trade_metrics(accepted)
    top5 = remove_top_winners_recompute(
        accepted,
        trading_dates,
        max_positions=max_positions,
    )

    path_rets = path_s.set_index("date")["path_s_return"]
    sleeve_rets = port.set_index("date")["realized"] if not port.empty else pd.Series(dtype=float)
    monthly_corr = monthly_correlation(path_rets, sleeve_rets) if not sleeve_rets.empty else np.nan
    sleeve_20 = blend_with_path_s(path_s, port, sleeve_weight=0.20) if not port.empty else curve_metrics(pd.Series(dtype=float))
    sleeve_30 = blend_with_path_s(path_s, port, sleeve_weight=0.30) if not port.empty else curve_metrics(pd.Series(dtype=float))
    sleeve_50 = blend_with_path_s(path_s, port, sleeve_weight=0.50) if not port.empty else curve_metrics(pd.Series(dtype=float))

    return {
        "variant": name,
        "candidates": candidates,
        "raw_ledger": raw_ledger,
        "baseline_ledger": baseline_ledger,
        "stress_150_ledger": stress_150,
        "stress_200_ledger": stress_200,
        "portfolio": port,
        "accepted": accepted,
        "baseline_metrics": baseline_curve,
        "stress_150_metrics": stress_150_curve,
        "stress_200_metrics": stress_200_curve,
        "trade_metrics": trade_stats,
        "top5_metrics": top5,
        "monthly_corr_to_path_s": monthly_corr,
        "sleeve_20_metrics": sleeve_20,
        "sleeve_30_metrics": sleeve_30,
        "sleeve_50_metrics": sleeve_50,
    }


def variant_summary_row(
    result: dict[str, object],
    path_s_metrics: dict[str, float],
) -> dict[str, object]:
    baseline = result["baseline_metrics"]
    stress_150 = result["stress_150_metrics"]
    stress_200 = result["stress_200_metrics"]
    trades = result["trade_metrics"]
    top5 = result["top5_metrics"]
    sleeve20 = result["sleeve_20_metrics"]
    sleeve30 = result["sleeve_30_metrics"]
    sleeve50 = result["sleeve_50_metrics"]

    improves_20_30 = (
        (pd.notna(sleeve20["sharpe"]) and pd.notna(path_s_metrics["sharpe"]) and sleeve20["sharpe"] > path_s_metrics["sharpe"])
        or (pd.notna(sleeve20["calmar"]) and pd.notna(path_s_metrics["calmar"]) and sleeve20["calmar"] > path_s_metrics["calmar"])
        or (pd.notna(sleeve30["sharpe"]) and pd.notna(path_s_metrics["sharpe"]) and sleeve30["sharpe"] > path_s_metrics["sharpe"])
        or (pd.notna(sleeve30["calmar"]) and pd.notna(path_s_metrics["calmar"]) and sleeve30["calmar"] > path_s_metrics["calmar"])
    )

    return {
        "variant": result["variant"],
        "n_signals": int(len(result["baseline_ledger"])),
        "n_trades": trades["n_trades"],
        "avg_trade_return": trades["avg_return"],
        "median_trade_return": trades["median_return"],
        "win_rate": trades["win_rate"],
        "avg_winner": trades["avg_winner"],
        "avg_loser": trades["avg_loser"],
        "payoff_ratio": trades["payoff_ratio"],
        "top5_trade_contribution": top5["top_winner_contribution"],
        "return_ex_top5_winners": top5["return_ex_top_winners"],
        "monthly_corr_to_path_s": result["monthly_corr_to_path_s"],
        "standalone_total_return": baseline["total_return"],
        "standalone_cagr": baseline["cagr"],
        "standalone_sharpe": baseline["sharpe"],
        "standalone_calmar": baseline["calmar"],
        "standalone_max_drawdown": baseline["max_drawdown"],
        "stress_150_total_return": stress_150["total_return"],
        "stress_150_sharpe": stress_150["sharpe"],
        "stress_150_max_drawdown": stress_150["max_drawdown"],
        "stress_200_total_return": stress_200["total_return"],
        "stress_200_sharpe": stress_200["sharpe"],
        "stress_200_max_drawdown": stress_200["max_drawdown"],
        "path_s_total_return": path_s_metrics["total_return"],
        "path_s_sharpe": path_s_metrics["sharpe"],
        "path_s_calmar": path_s_metrics["calmar"],
        "sleeve_20_total_return": sleeve20["total_return"],
        "sleeve_20_sharpe": sleeve20["sharpe"],
        "sleeve_20_calmar": sleeve20["calmar"],
        "sleeve_20_max_drawdown": sleeve20["max_drawdown"],
        "sleeve_30_total_return": sleeve30["total_return"],
        "sleeve_30_sharpe": sleeve30["sharpe"],
        "sleeve_30_calmar": sleeve30["calmar"],
        "sleeve_30_max_drawdown": sleeve30["max_drawdown"],
        "sleeve_50_total_return": sleeve50["total_return"],
        "sleeve_50_sharpe": sleeve50["sharpe"],
        "sleeve_50_calmar": sleeve50["calmar"],
        "sleeve_50_max_drawdown": sleeve50["max_drawdown"],
        "pass_avg_trade_positive": bool(pd.notna(trades["avg_return"]) and trades["avg_return"] > 0),
        "pass_median_trade_nonnegative": bool(pd.notna(trades["median_return"]) and trades["median_return"] >= 0),
        "pass_payoff_ratio_ge_1_5": bool(pd.notna(trades["payoff_ratio"]) and trades["payoff_ratio"] >= 1.5),
        "pass_remove_top5_still_positive": bool(pd.notna(top5["return_ex_top_winners"]) and top5["return_ex_top_winners"] > 0),
        "pass_sleeve_20_or_30_improves_path_s": bool(improves_20_30),
    }
