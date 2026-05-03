#!/usr/bin/env python3
"""Compute the top-N tradable universe (matching --universe-top-n in replay.py)
and prefetch yfinance earnings dates for that set.

Usage:
    .venv/bin/python scripts/conviction/backtest/prefetch_earnings_top_n.py --top-n 2000
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
sys.path.insert(0, str(HERE))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--top-n", type=int, default=2000)
    ap.add_argument("--workers", type=int, default=8)
    args = ap.parse_args()

    from massive_ingest import load_parquet, to_bars_by_ticker
    from massive_reference import allowed_ticker_set, load_metadata
    from dynamic_themes import build_static_universe_top_n
    from earnings_calendar import EarningsLookup, DEFAULT_CACHE

    df = load_parquet()
    metadata = load_metadata()
    allowed = allowed_ticker_set(
        require_type="CS",
        exclude_pharma_biotech=True,
        require_optionable=True,
    )
    df = df[df["ticker"].isin(allowed)]
    bars = to_bars_by_ticker(df)
    universe = build_static_universe_top_n(bars, metadata, top_n=args.top_n)
    print(f"[prefetch] universe size: {len(universe)}", file=sys.stderr)

    lookup = EarningsLookup(DEFAULT_CACHE)
    n = lookup.prefetch(universe, max_workers=args.workers)
    print(f"[prefetch] fetched {n} new of {len(universe)}; "
          f"total cache: {len(lookup._cache)}")


if __name__ == "__main__":
    main()
