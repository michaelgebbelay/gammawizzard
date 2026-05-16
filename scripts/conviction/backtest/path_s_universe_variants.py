#!/usr/bin/env python3
"""Run Path S universe variants without changing replay.py.

This study freezes the canonical Path S signal/exit stack and varies only the
universe presented to the replay engine.

Supported variants:
  - CORE_2000
  - SP500_OR_LARGECAP500_PROXY
  - NDX100
  - R1000_PROXY
  - R2000_PROXY
  - OPTION_LIQ_1000
  - OPTION_LIQ_750
  - OPTION_LIQ_500
  - PIT_OPTION_LIQ_1000_60d
  - PIT_OPTION_LIQ_1000_126d
  - PIT_OPTION_LIQ_750_126d
  - DYNAMIC_SECTOR_TOP4
  - DYNAMIC_SECTOR_TOP6

The script can be run locally for a smoke test or invoked by GitHub Actions
one variant/position combination at a time.
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
import time
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
RESULTS_DIR = HERE / "results"
DATA_DIR = HERE / "data"
PIT_CACHE_DIR = DATA_DIR / "pit_cache"
CONVICTION_DIR = HERE.parent
sys.path.insert(0, str(CONVICTION_DIR))

import replay  # noqa: E402
from massive_ingest import load_parquet  # noqa: E402
from massive_reference import allowed_ticker_set, load_metadata  # noqa: E402
from regime_filter import build_regime_lookup  # noqa: E402


DEFAULT_VARIANTS = [
    "CORE_2000",
    "SP500_OR_LARGECAP500_PROXY",
    "NDX100",
    "R1000_PROXY",
    "R2000_PROXY",
    "OPTION_LIQ_1000",
    "OPTION_LIQ_750",
    "OPTION_LIQ_500",
    "PIT_OPTION_LIQ_1000_60d",
    "PIT_OPTION_LIQ_1000_126d",
    "PIT_OPTION_LIQ_750_126d",
    "DYNAMIC_SECTOR_TOP4",
    "DYNAMIC_SECTOR_TOP6",
]
DEFAULT_END_DATE = "2026-04-29"
DEFAULT_DAYS = 1825
DEFAULT_CAPITAL = 100_000.0
DEFAULT_SLIPPAGE_BPS = 15.0
MIN_THEME_MEMBERS = 4
PIT_MIN_TRAILING_SESSIONS = 20


@dataclass(frozen=True)
class VariantSpec:
    name: str
    universe_type: str
    benchmark_symbol: str
    description: str
    base_universe_key: str
    selection_note: str
    sector_top_n: int | None = None
    use_canonical_core_loading: bool = False
    pit_window_sessions: int | None = None
    pit_target_n: int | None = None
    diagnostic_only: bool = False


@dataclass
class UniverseContext:
    metadata_df: pd.DataFrame
    core_ranking_df: pd.DataFrame
    variant_universes: dict[str, list[str]]
    industry_by_ticker: dict[str, str]
    ret20_lookup: pd.Series
    spy_ret20: dict[pd.Timestamp, float]
    spy_ret60: dict[pd.Timestamp, float]
    session_dates: pd.DatetimeIndex
    skew_df: pd.DataFrame
    skew_window_df: pd.DataFrame
    pit_table_by_variant: dict[str, pd.DataFrame]
    pit_members_by_variant: dict[str, dict[pd.Timestamp, list[str]]]
    phase_timings: dict[str, float]


VARIANT_SPECS: dict[str, VariantSpec] = {
    "CORE_2000": VariantSpec(
        name="CORE_2000",
        universe_type="top_2000_liquid_optionable",
        benchmark_symbol="SPY",
        description="Canonical Path S baseline on the top-2000 liquid/optionable universe.",
        base_universe_key="CORE_2000",
        selection_note="Exact Path S core loading path via replay.py broad-universe top-2000 selection.",
        use_canonical_core_loading=True,
    ),
    "SP500_OR_LARGECAP500_PROXY": VariantSpec(
        name="SP500_OR_LARGECAP500_PROXY",
        universe_type="largecap500_proxy",
        benchmark_symbol="SPY",
        description="Large-cap proxy universe.",
        base_universe_key="SP500_OR_LARGECAP500_PROXY",
        selection_note="Implemented as the top 500 eligible names by full-window median dollar volume; not point-in-time S&P 500 membership.",
    ),
    "NDX100": VariantSpec(
        name="NDX100",
        universe_type="nasdaq100_style_proxy",
        benchmark_symbol="QQQ",
        description="Nasdaq-100 / QQQ-style universe.",
        base_universe_key="NDX100",
        selection_note="Implemented as the top 100 XNAS-listed eligible names by full-window median dollar volume; proxy, not point-in-time NDX membership.",
    ),
    "R1000_PROXY": VariantSpec(
        name="R1000_PROXY",
        universe_type="russell1000_style_proxy",
        benchmark_symbol="IWB",
        description="Large/mid-cap Russell 1000-style proxy universe.",
        base_universe_key="R1000_PROXY",
        selection_note="Implemented as the top 1000 eligible names by full-window median dollar volume; proxy, not point-in-time Russell 1000 membership.",
    ),
    "R2000_PROXY": VariantSpec(
        name="R2000_PROXY",
        universe_type="russell2000_style_proxy",
        benchmark_symbol="IWM",
        description="Small-cap Russell 2000-style proxy universe.",
        base_universe_key="R2000_PROXY",
        selection_note="Implemented as the 1001-3000 dollar-volume ranks from the eligible universe; proxy, not point-in-time Russell 2000 membership.",
    ),
    "OPTION_LIQ_1000": VariantSpec(
        name="OPTION_LIQ_1000",
        universe_type="option_liquidity_proxy",
        benchmark_symbol="SPY",
        description="Top 1000 by Path-S-relevant option coverage.",
        base_universe_key="OPTION_LIQ_1000",
        selection_note="Implemented as the top 1000 names by valid 5%-OTM skew-day coverage in the run window, with median dollar volume as a tiebreaker.",
    ),
    "OPTION_LIQ_750": VariantSpec(
        name="OPTION_LIQ_750",
        universe_type="option_liquidity_proxy",
        benchmark_symbol="SPY",
        description="Top 750 by Path-S-relevant option coverage.",
        base_universe_key="OPTION_LIQ_750",
        selection_note="Implemented as the top 750 names by valid 5%-OTM skew-day coverage in the run window, with median dollar volume as a tiebreaker.",
    ),
    "OPTION_LIQ_500": VariantSpec(
        name="OPTION_LIQ_500",
        universe_type="option_liquidity_proxy",
        benchmark_symbol="SPY",
        description="Top 500 by Path-S-relevant option coverage.",
        base_universe_key="OPTION_LIQ_500",
        selection_note="Implemented as the top 500 names by valid 5%-OTM skew-day coverage in the run window, with median dollar volume as a tiebreaker.",
    ),
    "PIT_OPTION_LIQ_1000_60d": VariantSpec(
        name="PIT_OPTION_LIQ_1000_60d",
        universe_type="pit_option_liquidity_proxy",
        benchmark_symbol="SPY",
        description="Point-in-time top 1000 by trailing 60-session valid skew coverage.",
        base_universe_key="CORE_2000",
        selection_note="Ranks the core 2000 pond using only trailing pre-date valid 5%-OTM skew coverage over 60 sessions, with trailing median stock dollar volume as a tiebreaker.",
        pit_window_sessions=60,
        pit_target_n=1000,
    ),
    "PIT_OPTION_LIQ_1000_126d": VariantSpec(
        name="PIT_OPTION_LIQ_1000_126d",
        universe_type="pit_option_liquidity_proxy",
        benchmark_symbol="SPY",
        description="Point-in-time top 1000 by trailing 126-session valid skew coverage.",
        base_universe_key="CORE_2000",
        selection_note="Ranks the core 2000 pond using only trailing pre-date valid 5%-OTM skew coverage over 126 sessions, with trailing median stock dollar volume as a tiebreaker.",
        pit_window_sessions=126,
        pit_target_n=1000,
    ),
    "PIT_OPTION_LIQ_750_126d": VariantSpec(
        name="PIT_OPTION_LIQ_750_126d",
        universe_type="pit_option_liquidity_proxy",
        benchmark_symbol="SPY",
        description="Point-in-time top 750 by trailing 126-session valid skew coverage.",
        base_universe_key="CORE_2000",
        selection_note="Diagnostic-only narrower PIT option-liquidity universe using trailing 126-session valid skew coverage and trailing median stock dollar volume.",
        pit_window_sessions=126,
        pit_target_n=750,
        diagnostic_only=True,
    ),
    "DYNAMIC_SECTOR_TOP4": VariantSpec(
        name="DYNAMIC_SECTOR_TOP4",
        universe_type="dynamic_sector_heat",
        benchmark_symbol="SPY",
        description="Daily top-4 hot-industry Path S filter.",
        base_universe_key="CORE_2000",
        selection_note="Daily industry heat is computed from stock-only inputs on the core 2000 universe, then Path S runs only inside the top 4 industries.",
        sector_top_n=4,
    ),
    "DYNAMIC_SECTOR_TOP6": VariantSpec(
        name="DYNAMIC_SECTOR_TOP6",
        universe_type="dynamic_sector_heat",
        benchmark_symbol="SPY",
        description="Daily top-6 hot-industry Path S filter.",
        base_universe_key="CORE_2000",
        selection_note="Daily industry heat is computed from stock-only inputs on the core 2000 universe, then Path S runs only inside the top 6 industries.",
        sector_top_n=6,
    ),
}


def _slug(text: str) -> str:
    return "".join(ch.lower() if ch.isalnum() else "_" for ch in text).strip("_")


def _sic_to_theme_name(sic_description: str | None) -> str | None:
    if not isinstance(sic_description, str):
        return None
    val = sic_description.strip()
    if not val:
        return None
    return val.upper().replace(" ", "_").replace("&", "AND").replace("/", "_")[:64]


def _as_pct_rank(series: pd.Series) -> pd.Series:
    if series.empty:
        return series
    return series.rank(pct=True, method="average")


def _calmar(cagr: float | None, max_drawdown: float | None) -> float | None:
    if cagr is None or max_drawdown is None or max_drawdown >= 0:
        return None
    return float(cagr / abs(max_drawdown))


def _safe_mean(values: list[float]) -> float | None:
    if not values:
        return None
    return float(np.mean(values))


def _safe_get(series: pd.Series, key) -> float | None:
    try:
        val = series.get(key)
    except Exception:
        return None
    if val is None or pd.isna(val):
        return None
    return float(val)


def _load_skew_df() -> pd.DataFrame:
    skew_path = DATA_DIR / "skew_daily.parquet"
    if not skew_path.exists():
        return pd.DataFrame(columns=["underlying", "date"])
    df = pd.read_parquet(skew_path, columns=["underlying", "date"])
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    df["underlying"] = df["underlying"].astype(str).str.upper()
    return df


def _pit_cache_path(*, target_n: int, window_sessions: int) -> Path:
    return PIT_CACHE_DIR / f"pit_option_liq_{target_n}_{window_sessions}d.parquet"


def _build_or_load_pit_universe_table(
    *,
    core_tickers: list[str],
    eligible_df: pd.DataFrame,
    session_dates: pd.DatetimeIndex,
    skew_df: pd.DataFrame,
    target_n: int,
    window_sessions: int,
    rebuild: bool = False,
) -> tuple[pd.DataFrame, dict]:
    cache_path = _pit_cache_path(target_n=target_n, window_sessions=window_sessions)
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


def prepare_universe_context(
    *,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    requested_variants: list[str],
    rebuild_pit_cache: bool = False,
) -> UniverseContext:
    phase_timings: dict[str, float] = {}

    t0 = time.perf_counter()
    metadata_df = load_metadata()
    metadata_df["ticker"] = metadata_df["ticker"].astype(str).str.upper()
    phase_timings["load_metadata_seconds"] = round(time.perf_counter() - t0, 3)

    t0 = time.perf_counter()
    allowed = allowed_ticker_set(
        require_type="CS",
        exclude_pharma_biotech=True,
        require_optionable=True,
    )
    bars_df = load_parquet()
    bars_df["ticker"] = bars_df["ticker"].astype(str).str.upper()
    bars_df["date"] = pd.to_datetime(bars_df["date"]).dt.normalize()
    eligible_df = bars_df[bars_df["ticker"].isin(allowed)].copy()
    eligible_df["dollar_vol"] = (
        eligible_df["close"].astype(float) * eligible_df["volume"].astype(float)
    )
    phase_timings["load_bars_seconds"] = round(time.perf_counter() - t0, 3)

    t0 = time.perf_counter()
    ranking_df = (
        eligible_df.groupby("ticker", as_index=False)
        .agg(
            median_dollar_vol=("dollar_vol", "median"),
            n_bars=("date", "nunique"),
        )
        .merge(
            metadata_df[
                [
                    "ticker",
                    "primary_exchange",
                    "sic_description",
                    "market_cap",
                ]
            ],
            on="ticker",
            how="left",
        )
    )
    ranking_df = ranking_df[ranking_df["n_bars"] >= 252].copy()
    ranking_df = ranking_df.sort_values(
        ["median_dollar_vol", "ticker"], ascending=[False, True]
    ).reset_index(drop=True)
    phase_timings["build_ranking_seconds"] = round(time.perf_counter() - t0, 3)

    t0 = time.perf_counter()
    skew_df = _load_skew_df()
    skew_window_df = skew_df[
        (skew_df["date"] >= start_date) & (skew_df["date"] <= end_date)
    ].copy()
    skew_coverage = (
        skew_window_df.groupby("underlying").size().rename("valid_skew_days").reset_index()
        .rename(columns={"underlying": "ticker"})
    )
    ranking_df = ranking_df.merge(skew_coverage, on="ticker", how="left")
    ranking_df["valid_skew_days"] = ranking_df["valid_skew_days"].fillna(0).astype(int)
    phase_timings["load_skew_seconds"] = round(time.perf_counter() - t0, 3)

    core2000 = ranking_df["ticker"].head(2000).tolist()
    sp500_proxy = ranking_df["ticker"].head(500).tolist()
    r1000_proxy = ranking_df["ticker"].head(1000).tolist()
    r2000_proxy = ranking_df["ticker"].iloc[1000:3000].tolist()
    ndx100 = (
        ranking_df[ranking_df["primary_exchange"] == "XNAS"]["ticker"].head(100).tolist()
    )
    option_liq = ranking_df.sort_values(
        ["valid_skew_days", "median_dollar_vol", "ticker"],
        ascending=[False, False, True],
    ).reset_index(drop=True)

    variant_universes = {
        "CORE_2000": core2000,
        "SP500_OR_LARGECAP500_PROXY": sp500_proxy,
        "NDX100": ndx100,
        "R1000_PROXY": r1000_proxy,
        "R2000_PROXY": r2000_proxy,
        "OPTION_LIQ_1000": option_liq["ticker"].head(1000).tolist(),
        "OPTION_LIQ_750": option_liq["ticker"].head(750).tolist(),
        "OPTION_LIQ_500": option_liq["ticker"].head(500).tolist(),
    }

    session_dates = pd.DatetimeIndex(
        sorted(
            bars_df.loc[bars_df["ticker"] == "SPY", "date"]
            .dropna()
            .dt.normalize()
            .unique()
        )
    )
    pit_table_by_variant: dict[str, pd.DataFrame] = {}
    pit_members_by_variant: dict[str, dict[pd.Timestamp, list[str]]] = {}
    requested_specs = [VARIANT_SPECS[name] for name in requested_variants]
    pit_specs = [spec for spec in requested_specs if spec.pit_window_sessions is not None]
    if pit_specs:
        pit_elapsed = 0.0
        for spec in pit_specs:
            t0 = time.perf_counter()
            full_table, cache_meta = _build_or_load_pit_universe_table(
                core_tickers=core2000,
                eligible_df=eligible_df,
                session_dates=session_dates,
                skew_df=skew_df,
                target_n=int(spec.pit_target_n or 0),
                window_sessions=int(spec.pit_window_sessions or 0),
                rebuild=rebuild_pit_cache,
            )
            pit_elapsed += time.perf_counter() - t0
            table = full_table[
                (full_table["date"] >= start_date) & (full_table["date"] <= end_date)
            ].copy()
            pit_table_by_variant[spec.name] = table
            members = {
                pd.Timestamp(dt).normalize(): grp.sort_values("rank")["ticker"].tolist()
                for dt, grp in table.groupby("date")
            }
            pit_members_by_variant[spec.name] = members
            variant_universes[spec.name] = sorted(table["ticker"].dropna().astype(str).str.upper().unique().tolist())
            phase_timings[f"{_slug(spec.name)}_cache_hit"] = 1.0 if cache_meta["cache_hit"] else 0.0
            phase_timings[f"{_slug(spec.name)}_table_rows"] = float(cache_meta["rows"])
        phase_timings["build_pit_tables_seconds"] = round(pit_elapsed, 3)

    industry_by_ticker = {
        row.ticker: _sic_to_theme_name(row.sic_description)
        for row in metadata_df.itertuples()
        if _sic_to_theme_name(row.sic_description) is not None
    }

    # Build the stock-only heat inputs from the core universe plus SPY. SPY is
    # intentionally outside the eligible common-stock universe filter, so we
    # must source it from the full bars panel rather than eligible_df.
    core_df = bars_df[bars_df["ticker"].isin(set(core2000) | {"SPY"})].copy()
    core_df = core_df.sort_values(["ticker", "date"]).reset_index(drop=True)
    core_df["ret20"] = core_df.groupby("ticker")["close"].pct_change(20)
    spy_df = core_df[core_df["ticker"] == "SPY"][["date", "ret20"]].copy()
    spy_df["ret60"] = core_df[core_df["ticker"] == "SPY"]["close"].pct_change(60).to_numpy()
    ret20_lookup = core_df.set_index(["date", "ticker"])["ret20"]
    spy_ret20 = {
        pd.Timestamp(row.date).normalize(): float(row.ret20)
        for row in spy_df.itertuples()
        if pd.notna(row.ret20)
    }
    spy_ret60 = {
        pd.Timestamp(row.date).normalize(): float(row.ret60)
        for row in spy_df.itertuples()
        if pd.notna(row.ret60)
    }

    return UniverseContext(
        metadata_df=metadata_df,
        core_ranking_df=ranking_df,
        variant_universes=variant_universes,
        industry_by_ticker=industry_by_ticker,
        ret20_lookup=ret20_lookup,
        spy_ret20=spy_ret20,
        spy_ret60=spy_ret60,
        session_dates=session_dates,
        skew_df=skew_df,
        skew_window_df=skew_window_df,
        pit_table_by_variant=pit_table_by_variant,
        pit_members_by_variant=pit_members_by_variant,
        phase_timings=phase_timings,
    )


def _variant_selector(
    spec: VariantSpec,
    ctx: UniverseContext,
    day_state: replay.DayState,
    *,
    held_tickers: set[str] | None = None,
) -> tuple[set[str] | None, dict]:
    if spec.name == "CORE_2000":
        allowed_count = len(day_state.factors_by_ticker)
        return None, {
            "allowed_universe_size": int(allowed_count),
            "selected_industries": "",
            "sector_heat_debug": "",
        }

    if spec.pit_window_sessions is not None:
        date_key = pd.Timestamp(day_state.date).normalize()
        allowed = set(ctx.pit_members_by_variant.get(spec.name, {}).get(date_key, []))
        if held_tickers:
            allowed.update(str(t).upper() for t in held_tickers if t)
        return allowed, {
            "allowed_universe_size": int(len(allowed)),
            "selected_industries": "",
            "sector_heat_debug": "",
        }

    if spec.sector_top_n is None:
        base = set(ctx.variant_universes[spec.base_universe_key])
        if held_tickers:
            base.update(str(t).upper() for t in held_tickers if t)
        return base, {
            "allowed_universe_size": int(len(base)),
            "selected_industries": "",
            "sector_heat_debug": "",
        }

    date_key = pd.Timestamp(day_state.date).normalize()
    spy20 = ctx.spy_ret20.get(date_key)
    spy60 = ctx.spy_ret60.get(date_key)
    rows: list[dict] = []
    for tkr, f in day_state.factors_by_ticker.items():
        industry = ctx.industry_by_ticker.get(tkr)
        if not industry:
            continue
        rel20 = None
        ret20 = _safe_get(ctx.ret20_lookup, (date_key, tkr))
        if ret20 is not None and spy20 is not None:
            rel20 = ret20 - spy20
        rel60 = None
        if f.recent_60d_ret is not None and spy60 is not None:
            rel60 = float(f.recent_60d_ret) - float(spy60)
        rows.append(
            {
                "ticker": tkr,
                "industry": industry,
                "rel20": rel20,
                "rel60": rel60,
                "above50": (
                    1.0
                    if f.above_50d_sma is True
                    else 0.0
                    if f.above_50d_sma is False
                    else np.nan
                ),
                "vol60": float(f.vol_60d) if f.vol_60d is not None else np.nan,
            }
        )
    if not rows:
        return set(), {
            "allowed_universe_size": 0,
            "selected_industries": "",
            "sector_heat_debug": "",
        }
    df = pd.DataFrame(rows)
    heat = (
        df.groupby("industry", as_index=False)
        .agg(
            members=("ticker", "nunique"),
            median_rel60=("rel60", "median"),
            median_rel20=("rel20", "median"),
            pct_above50=("above50", "mean"),
            median_vol60=("vol60", "median"),
        )
    )
    heat = heat[heat["members"] >= MIN_THEME_MEMBERS].copy()
    if heat.empty:
        return set(), {
            "allowed_universe_size": 0,
            "selected_industries": "",
            "sector_heat_debug": "",
        }
    for col in ("median_rel60", "median_rel20", "pct_above50", "median_vol60"):
        heat[col] = pd.to_numeric(heat[col], errors="coerce")
    heat = heat.dropna(
        subset=["median_rel60", "median_rel20", "pct_above50", "median_vol60"]
    ).copy()
    if heat.empty:
        return set(), {
            "allowed_universe_size": 0,
            "selected_industries": "",
            "sector_heat_debug": "",
        }
    heat["rel60_rank"] = _as_pct_rank(heat["median_rel60"])
    heat["rel20_rank"] = _as_pct_rank(heat["median_rel20"])
    heat["above50_rank"] = _as_pct_rank(heat["pct_above50"])
    heat["vol60_rank"] = _as_pct_rank(heat["median_vol60"])
    heat["heat_score"] = (
        0.40 * heat["rel60_rank"]
        + 0.25 * heat["rel20_rank"]
        + 0.20 * heat["above50_rank"]
        + 0.15 * heat["vol60_rank"]
    )
    chosen = heat.sort_values(
        ["heat_score", "median_rel60", "industry"],
        ascending=[False, False, True],
    ).head(spec.sector_top_n)
    industries = chosen["industry"].tolist()
    allowed = set(df[df["industry"].isin(industries)]["ticker"].tolist())
    if held_tickers:
        allowed.update(str(t).upper() for t in held_tickers if t)
    debug = "; ".join(
        f"{row.industry}:{row.heat_score:.3f}"
        for row in chosen.itertuples()
    )
    return allowed, {
        "allowed_universe_size": int(len(allowed)),
        "selected_industries": ",".join(industries),
        "sector_heat_debug": debug,
    }


def _path_s_candidates(
    day_state: replay.DayState,
    *,
    allowed_tickers: set[str] | None,
    exclude: str | set[str] | None = None,
) -> list[tuple[str, float]]:
    cfg = replay.PATH_S_CONFIG
    direction = cfg["direction"]
    z_min = cfg["abs_skew_z_min"]
    speculative_only = cfg["speculative_only"]

    excl: set[str] = set()
    if isinstance(exclude, str):
        excl = {exclude}
    elif isinstance(exclude, (set, frozenset, list, tuple)):
        excl = {str(item) for item in exclude}

    if not day_state.skew_z_by_ticker:
        return []

    qualifies_lookup = getattr(day_state, "skew_qualifies_by_ticker", {})
    df = day_state.flyer_ranking
    candidates: list[tuple[str, float]] = []
    for tkr, z in day_state.skew_z_by_ticker.items():
        if allowed_tickers is not None and tkr not in allowed_tickers:
            continue
        if tkr in excl or z is None:
            continue
        if direction == "bullish" and z < z_min:
            continue
        if direction == "bearish" and z > -z_min:
            continue
        if qualifies_lookup and not qualifies_lookup.get(tkr, True):
            continue

        f = day_state.factors_by_ticker.get(tkr)
        if f is None:
            continue
        if f.above_50d_sma is None or not f.above_50d_sma:
            continue
        if f.recent_60d_ret is None or f.recent_60d_ret < 0.0:
            continue
        min_above_200 = cfg.get("min_pct_above_200d")
        if min_above_200 is not None:
            if f.above_200d is None or f.above_200d < min_above_200:
                continue
        min_ret_60 = cfg.get("min_ret_60d")
        if min_ret_60 is not None and f.recent_60d_ret < min_ret_60:
            continue

        if speculative_only:
            if f.last_close is None or not (
                cfg["spec_price_min"] <= f.last_close <= cfg["spec_price_max"]
            ):
                continue
            if f.vol_60d is None or f.vol_60d < cfg["spec_min_vol_60d"]:
                continue
            if (
                f.dollar_vol_20d is None
                or f.dollar_vol_20d < cfg["spec_min_dollar_vol"]
            ):
                continue
        else:
            if df is not None and "eligible" in df.columns and tkr in df.index:
                if not bool(df.loc[tkr, "eligible"]):
                    continue
        candidates.append((tkr, abs(float(z))))
    candidates.sort(key=lambda item: (-item[1], item[0]))
    return candidates


@contextmanager
def _patched_attr(obj, name: str, value):
    original = getattr(obj, name)
    setattr(obj, name, value)
    try:
        yield
    finally:
        setattr(obj, name, original)


def _prepare_path_s_config() -> dict:
    replay.PATH_S_CONFIG["direction"] = "bullish"
    replay.PATH_S_CONFIG["abs_skew_z_min"] = 3.0
    replay.PATH_S_CONFIG["speculative_only"] = False
    replay.PATH_S_CONFIG["min_pct_above_200d"] = None
    replay.PATH_S_CONFIG["min_ret_60d"] = None
    return replay.load_skew_lookup(
        z_window=60,
        persistence_days=1,
        abs_skew_z_min=3.0,
        direction="bullish",
    )


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2, default=str))


def _package_run(
    *,
    export_dir: Path,
    result_dir: Path,
    variant_meta: dict,
    diagnostics_df: pd.DataFrame,
) -> None:
    export_dir.mkdir(parents=True, exist_ok=True)
    replay_dir = export_dir / "replay_run"
    if replay_dir.exists():
        shutil.rmtree(replay_dir)
    shutil.copytree(result_dir, replay_dir)
    diagnostics_df.to_csv(export_dir / "daily_signal_diagnostics.csv", index=False)
    _write_json(export_dir / "variant_meta.json", variant_meta)


def _build_variant_row(
    *,
    spec: VariantSpec,
    summary: dict,
    diagnostics_df: pd.DataFrame,
    runtime_seconds: float,
    implemented_universe_size: int,
    base_universe_size: int,
    contracts_processed_est: int,
) -> dict:
    perf = summary["performance"]
    act = summary["activity"]
    calmar = _calmar(perf.get("cagr"), perf.get("max_drawdown"))
    valid_signal_days = int(diagnostics_df["valid_signal"].fillna(False).sum()) if not diagnostics_df.empty else 0
    return {
        "variant": spec.name,
        "universe_type": spec.universe_type,
        "benchmark_symbol": spec.benchmark_symbol,
        "positions": int(summary["config"]["positions"]),
        "implemented_universe_size": int(implemented_universe_size),
        "base_universe_size": int(base_universe_size),
        "TR": perf.get("total_return"),
        "CAGR": perf.get("cagr"),
        "Sharpe": perf.get("sharpe"),
        "MDD": perf.get("max_drawdown"),
        "Calmar": round(calmar, 5) if calmar is not None else None,
        "trades": act.get("n_trades"),
        "pct_days_in_position": act.get("pct_time_invested"),
        "valid_signal_days": valid_signal_days,
        "runtime_seconds": round(runtime_seconds, 2),
        "contracts_processed_est": int(contracts_processed_est),
        "diagnostic_only": bool(spec.diagnostic_only),
        "selection_note": spec.selection_note,
        "run_name": summary["run_name"],
    }


def run_variant(
    *,
    spec: VariantSpec,
    ctx: UniverseContext,
    skew_lookup: dict,
    regime_lookup: dict[pd.Timestamp, str],
    end_date: pd.Timestamp,
    days: int,
    positions: int,
    run_suffix: str,
    export_root: Path,
) -> dict:
    base_universe = ctx.variant_universes.get(spec.base_universe_key, [])
    implemented_universe = (
        ctx.variant_universes.get(spec.name, base_universe)
        if spec.name != "CORE_2000"
        else base_universe
    )
    daily_diag: dict[pd.Timestamp, dict] = {}
    cached_candidates: dict[pd.Timestamp, list[tuple[str, float]]] = {}
    variant_phase_timings: dict[str, float] = {}

    t0 = time.perf_counter()
    if spec.use_canonical_core_loading:
        variant_skew_lookup = skew_lookup
    else:
        keep = set(implemented_universe)
        variant_skew_lookup = {k: v for k, v in skew_lookup.items() if k in keep}
    variant_phase_timings["filter_skew_lookup_seconds"] = round(
        time.perf_counter() - t0, 3
    )

    def diagnose_day(
        day_state: replay.DayState,
        *,
        held_tickers: set[str] | None = None,
    ) -> None:
        date_key = pd.Timestamp(day_state.date).normalize()
        if date_key in daily_diag:
            return
        allowed, extra = _variant_selector(
            spec,
            ctx,
            day_state,
            held_tickers=held_tickers,
        )
        candidates = _path_s_candidates(day_state, allowed_tickers=allowed, exclude=None)
        cached_candidates[date_key] = candidates
        daily_diag[date_key] = {
            "date": date_key.strftime("%Y-%m-%d"),
            "allowed_universe_size": int(extra.get("allowed_universe_size", len(base_universe))),
            "candidate_count": int(len(candidates)),
            "valid_signal": bool(candidates),
            "top_candidate": candidates[0][0] if candidates else "",
            "selected_industries": str(extra.get("selected_industries", "")),
            "sector_heat_debug": str(extra.get("sector_heat_debug", "")),
        }

    orig_reconstruct_day = replay.reconstruct_day

    def wrapped_reconstruct_day(*args, **kwargs):
        state = orig_reconstruct_day(*args, **kwargs)
        held = kwargs.get("held_ticker")
        held_set = {str(held).upper()} if held else set()
        diagnose_day(state, held_tickers=held_set)
        return state

    def variant_path_s_picker(day_state: replay.DayState, *, exclude=None):
        date_key = pd.Timestamp(day_state.date).normalize()
        excl: set[str]
        if isinstance(exclude, str):
            excl = {exclude}
        elif isinstance(exclude, (set, frozenset, list, tuple)):
            excl = {str(item) for item in exclude}
        else:
            excl = set()
        if date_key not in daily_diag:
            diagnose_day(day_state, held_tickers=excl)
        candidates = list(cached_candidates.get(date_key, []))
        if exclude is not None:
            candidates = [item for item in candidates if item[0] not in excl]
        if not candidates:
            return None, None, "none"
        label = (
            "PATH_S_SKEW_FLIP"
            if replay.PATH_S_CONFIG.get("direction", "bullish") == "bullish"
            else "PATH_S_BEARISH_FLIP"
        )
        return candidates[0][0], label, "path_s"

    def build_variant_universe() -> list[str]:
        return list(implemented_universe)

    run_started = time.perf_counter()
    with ExitStack() as stack:
        stack.enter_context(_patched_attr(replay, "reconstruct_day", wrapped_reconstruct_day))
        stack.enter_context(_patched_attr(replay, "_path_s_skew_flip", variant_path_s_picker))
        if not spec.use_canonical_core_loading:
            stack.enter_context(_patched_attr(replay, "build_universe", build_variant_universe))

        if positions == 1:
            result = replay.run_replay(
                end_date=end_date,
                lookback_days=days,
                source="massive",
                parquet_path=None,
                slippage_bps=DEFAULT_SLIPPAGE_BPS,
                initial_capital=DEFAULT_CAPITAL,
                refresh=False,
                progress_every=50,
                exit_rule="trailing_pct",
                trailing_pct=20.0,
                dynamic_themes=False,
                universe_top_n=2000,
                ignore_themes=spec.use_canonical_core_loading,
                strategy="pathS",
                skew_lookup=variant_skew_lookup,
                max_hold_days=90,
                exit_mode="baseline",
                post90_trail_pct=10.0,
                exclude_tickers=None,
                launch_date=None,
                signal_decay_z=None,
                signal_decay_days=2,
                skew_direction="bullish",
                skew_z_window=60,
                skew_z_rolling_min=20,
                regime_lookup=regime_lookup,
                regime_gate="spy",
                run_suffix=run_suffix,
                displacement_enabled=False,
                displacement_min_hold=20,
                displacement_max_return=0.0,
                displacement_z_min=3.0,
            )
        else:
            result = replay.run_replay_multi(
                end_date=end_date,
                lookback_days=days,
                source="massive",
                parquet_path=None,
                slippage_bps=DEFAULT_SLIPPAGE_BPS,
                initial_capital=DEFAULT_CAPITAL,
                refresh=False,
                progress_every=50,
                exit_rule="trailing_pct",
                trailing_pct=20.0,
                dynamic_themes=False,
                universe_top_n=2000,
                ignore_themes=spec.use_canonical_core_loading,
                n_positions=positions,
                strategy="pathS",
                skew_lookup=variant_skew_lookup,
                max_hold_days=90,
                launch_date=None,
                signal_decay_z=None,
                signal_decay_days=2,
                skew_direction="bullish",
                skew_z_window=60,
                skew_z_rolling_min=20,
                regime_lookup=regime_lookup,
                regime_gate="spy",
                run_suffix=run_suffix,
                displacement_enabled=False,
                displacement_min_hold=20,
                displacement_max_return=0.0,
                displacement_z_min=3.0,
                displacement_max_swaps_per_day=1,
            )
    runtime_seconds = time.perf_counter() - run_started
    variant_phase_timings["replay_seconds"] = round(runtime_seconds, 3)

    result_dir = Path(result["out_dir"])
    summary = result["summary"]
    diagnostics_df = pd.DataFrame(sorted(daily_diag.values(), key=lambda row: row["date"]))
    contracts_processed_est = int(
        2 * diagnostics_df["allowed_universe_size"].fillna(0).astype(int).sum()
    ) if not diagnostics_df.empty else 0
    variant_row = _build_variant_row(
        spec=spec,
        summary=summary,
        diagnostics_df=diagnostics_df,
        runtime_seconds=runtime_seconds,
        implemented_universe_size=len(implemented_universe) if implemented_universe else len(base_universe),
        base_universe_size=len(base_universe),
        contracts_processed_est=contracts_processed_est,
    )

    variant_meta = {
        "variant": spec.name,
        "description": spec.description,
        "selection_note": spec.selection_note,
        "universe_type": spec.universe_type,
        "benchmark_symbol": spec.benchmark_symbol,
        "positions": positions,
        "days": days,
        "end_date": end_date.strftime("%Y-%m-%d"),
        "base_universe_size": len(base_universe),
        "implemented_universe_size": len(implemented_universe) if implemented_universe else len(base_universe),
        "use_canonical_core_loading": spec.use_canonical_core_loading,
        "diagnostic_only": bool(spec.diagnostic_only),
        "runtime_seconds": round(runtime_seconds, 2),
        "contracts_processed_est": contracts_processed_est,
        "phase_timings": {**ctx.phase_timings, **variant_phase_timings},
        "summary_row": variant_row,
        "run_name": summary["run_name"],
    }

    export_dir = export_root / f"{_slug(spec.name)}_p{positions}"
    _package_run(
        export_dir=export_dir,
        result_dir=result_dir,
        variant_meta=variant_meta,
        diagnostics_df=diagnostics_df,
    )
    pd.DataFrame([variant_row]).to_csv(export_dir / "variant_summary.csv", index=False)
    return {
        "variant_meta": variant_meta,
        "diagnostics_df": diagnostics_df,
        "export_dir": export_dir,
        "result_dir": result_dir,
        "summary": summary,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--variant",
        default="CORE_2000",
        help="Comma-separated variant list",
    )
    parser.add_argument(
        "--positions",
        type=int,
        choices=[1, 2],
        default=1,
        help="1 for single-position, 2 for top-2",
    )
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS)
    parser.add_argument("--end-date", default=DEFAULT_END_DATE)
    parser.add_argument("--study-name", default=None)
    parser.add_argument(
        "--rebuild-pit-cache",
        action="store_true",
        help="Force PIT universe-table regeneration instead of reusing cached parquet files.",
    )
    parser.add_argument(
        "--export-dir",
        type=Path,
        default=None,
        help="Optional package export root. Defaults to a study directory under results/.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    requested = [item.strip() for item in args.variant.split(",") if item.strip()]
    invalid = [name for name in requested if name not in VARIANT_SPECS]
    if invalid:
        raise SystemExit(
            "Unknown variants: " + ", ".join(invalid) + ". Valid: " + ", ".join(DEFAULT_VARIANTS)
        )
    end_date = pd.Timestamp(args.end_date).normalize()
    start_date = end_date - pd.Timedelta(days=args.days)

    study_suffix = args.study_name or f"path_s_universe_variants_{args.positions}p_{end_date.date()}"
    study_dir = (
        args.export_dir
        if args.export_dir is not None
        else RESULTS_DIR / f"{datetime.now():%Y-%m-%d}_{study_suffix}"
    )
    study_dir.mkdir(parents=True, exist_ok=True)

    print(f"[variants] preparing context for {start_date.date()} -> {end_date.date()}...", file=sys.stderr)
    ctx = prepare_universe_context(
        start_date=start_date,
        end_date=end_date,
        requested_variants=requested,
        rebuild_pit_cache=args.rebuild_pit_cache,
    )
    print(
        f"[variants] eligible universe rows={len(ctx.core_ranking_df):,}  core2000={len(ctx.variant_universes['CORE_2000']):,}",
        file=sys.stderr,
    )
    if ctx.phase_timings:
        timing_bits = ", ".join(f"{k}={v}" for k, v in sorted(ctx.phase_timings.items()))
        print(f"[variants] context timings: {timing_bits}", file=sys.stderr)
    skew_lookup = _prepare_path_s_config()
    regime_lookup = build_regime_lookup("spy")

    run_rows: list[dict] = []
    export_dirs: list[Path] = []
    for name in requested:
        spec = VARIANT_SPECS[name]
        run_suffix = f"{_slug(study_suffix)}_{_slug(name)}_p{args.positions}"
        print(f"[variants] running {name} p{args.positions}...", file=sys.stderr)
        run = run_variant(
            spec=spec,
            ctx=ctx,
            skew_lookup=skew_lookup,
            regime_lookup=regime_lookup,
            end_date=end_date,
            days=args.days,
            positions=args.positions,
            run_suffix=run_suffix,
            export_root=study_dir,
        )
        run_rows.append(run["variant_meta"]["summary_row"])
        export_dirs.append(run["export_dir"])

    summary_df = pd.DataFrame(run_rows)
    summary_df.to_csv(study_dir / f"variant_summary_p{args.positions}.csv", index=False)
    manifest = {
        "study_name": study_suffix,
        "positions": args.positions,
        "days": args.days,
        "end_date": end_date.strftime("%Y-%m-%d"),
        "variants": requested,
        "export_dirs": [str(path) for path in export_dirs],
    }
    _write_json(study_dir / f"manifest_p{args.positions}.json", manifest)

    print("=" * 100)
    print("PATH S UNIVERSE VARIANTS")
    print("=" * 100)
    print(f"Study dir: {study_dir}")
    print(summary_df.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
