"""
Dynamic-theme construction from data, no hindsight.

Replaces the static `themes.yaml` for backtests. At each as-of date, the
caller passes the eligible universe (already factor-scored) and we group
tickers by their SIC industry classification. Each SIC with >= N members
becomes a "theme" for that day; the existing theme-rotation logic runs
unchanged on top.

Why SIC and not GICS sectors:
  - Massive's REST `/v3/reference/tickers/{ticker}` returns sic_code +
    sic_description directly. We already have it in the metadata parquet.
  - SIC industries are narrower than GICS sectors (e.g. "Semiconductors"
    vs broader "Technology"), which produces more themes — better for the
    "12 semis aren't 12 ideas" deduplication problem.
  - GICS codes aren't in Polygon-style data without an extra vendor.

Why this matters for the backtest:
  - Static themes.yaml was curated in 2025 with knowledge of which names
    had been compounding. Names like SNDK weren't even spun out 5 years
    ago. The historical tests inherit forward-looking bias from the file.
  - Building themes at each as-of date from SIC codes uses only data that
    existed at trade time. The themes that emerge at each date are the
    natural industry clusters of names currently passing the eligibility
    gate — no hand-curation, no hindsight.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from massive_reference import load_metadata  # noqa: E402


def _sic_to_theme_name(sic_description: str) -> str:
    """Normalize the SIC description into a stable, sluggable theme name."""
    if not isinstance(sic_description, str):
        return "UNKNOWN"
    return sic_description.strip().upper().replace(" ", "_").replace("&", "AND").replace("/", "_")[:64]


def build_dynamic_themes(
    eligible_tickers: list[str] | set[str],
    metadata_df: pd.DataFrame | None = None,
    *,
    min_tickers_per_theme: int = 4,
) -> dict[str, list[str]]:
    """Cluster eligible tickers by SIC industry. Returns {theme_name: [tickers]}.

    Themes with fewer than `min_tickers_per_theme` members are dropped — a
    one-name "theme" doesn't carry rotation information.
    """
    if metadata_df is None:
        metadata_df = load_metadata()
    eligible = {t.upper() for t in eligible_tickers}
    if not eligible:
        return {}

    # Filter to eligible rows + ensure SIC fields exist
    df = metadata_df.copy()
    df["ticker"] = df["ticker"].astype(str).str.upper()
    df = df[df["ticker"].isin(eligible)]
    df = df[df["sic_description"].notna()]
    if df.empty:
        return {}

    df["theme"] = df["sic_description"].apply(_sic_to_theme_name)
    grouped: dict[str, list[str]] = {}
    for theme, group in df.groupby("theme"):
        members = sorted(group["ticker"].unique().tolist())
        if len(members) >= min_tickers_per_theme:
            grouped[theme] = members
    return grouped


def build_static_universe_top_n(
    bars_by_ticker: dict,
    metadata_df: pd.DataFrame | None = None,
    *,
    top_n: int = 500,
    min_bars: int = 252,
) -> list[str]:
    """Pick the top N tickers from the eligible universe by median dollar
    volume across the entire bars window. Used to keep the backtest universe
    a manageable size — full universe (~3K) is too slow without vectorized
    factor computation.

    Eligibility comes from the metadata filter (CS, non-pharma/biotech,
    optionable, major exchange).
    """
    from massive_reference import allowed_ticker_set
    if metadata_df is None:
        metadata_df = load_metadata()
    allowed = allowed_ticker_set(
        require_type="CS",
        exclude_pharma_biotech=True,
        require_optionable=True,
    )
    rankings = []
    for tkr, bars in bars_by_ticker.items():
        if tkr not in allowed:
            continue
        if bars is None or bars.empty or len(bars) < min_bars:
            continue
        dv = (bars["close"].astype(float) * bars["volume"].astype(float)).median()
        if dv > 0:
            rankings.append((tkr, float(dv)))
    rankings.sort(key=lambda x: x[1], reverse=True)
    return [t for t, _ in rankings[:top_n]]
