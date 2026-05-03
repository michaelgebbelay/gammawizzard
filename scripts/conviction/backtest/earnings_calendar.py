#!/usr/bin/env python3
"""Earnings-date lookup for backtest blackout filtering.

Fetches per-ticker historical earnings press-release dates from yfinance and
caches them to a JSON file. yfinance returns the actual press-release date
(e.g. CART 2025-02-25), unlike the Massive `/vX/reference/financials`
filing_date which is the SEC filing (typically 0-14 days after the press
release and not the right anchor for a -7d/+1d blackout).

Cache layout (scripts/conviction/backtest/data/earnings_dates.json):
    { "<TICKER>": ["YYYY-MM-DD", ...], ... }

Empty-list entries cache "yfinance returned no data" — we don't refetch on
every run.
"""
from __future__ import annotations

import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd

DEFAULT_CACHE = Path(__file__).resolve().parent / "data" / "earnings_dates.json"


def _fetch_one(ticker: str, limit: int = 60) -> list[str]:
    """Return sorted unique YYYY-MM-DD strings; empty list on failure."""
    try:
        import yfinance as yf  # local import — keep replay.py free of the dep
        t = yf.Ticker(ticker)
        df = t.get_earnings_dates(limit=limit)
        if df is None or df.empty:
            return []
        return sorted({pd.Timestamp(d).date().isoformat() for d in df.index})
    except Exception:
        return []


class EarningsLookup:
    """Cached {ticker: [date, ...]} with a calendar-blackout helper."""

    def __init__(self, cache_path: Path | None = None):
        self.cache_path = cache_path or DEFAULT_CACHE
        self._cache: dict[str, list[str]] = {}
        self._parsed: dict[str, list[pd.Timestamp]] = {}
        self._dirty = False
        self._load()

    def _load(self) -> None:
        if self.cache_path.exists():
            self._cache = json.loads(self.cache_path.read_text())

    def save(self) -> None:
        if not self._dirty:
            return
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        self.cache_path.write_text(json.dumps(self._cache, indent=0, sort_keys=True))
        self._dirty = False

    def _dates_for(self, ticker: str) -> list[pd.Timestamp]:
        if ticker in self._parsed:
            return self._parsed[ticker]
        raw = self._cache.get(ticker)
        if raw is None:
            raw = _fetch_one(ticker)
            self._cache[ticker] = raw
            self._dirty = True
        parsed = [pd.Timestamp(d).normalize() for d in raw]
        self._parsed[ticker] = parsed
        return parsed

    def is_blackout(
        self,
        ticker: str,
        today: pd.Timestamp,
        before_days: int,
        after_days: int,
    ) -> bool:
        """True iff `today` falls in [earnings - before_days, earnings + after_days]
        for any known earnings date of `ticker`. Both bounds inclusive.

        Empty earnings history → never blackout (don't penalize unknown names).
        """
        if before_days <= 0 and after_days <= 0:
            return False
        dates = self._dates_for(ticker)
        if not dates:
            return False
        today_d = pd.Timestamp(today).normalize()
        for ed in dates:
            delta = (ed - today_d).days  # positive ⇒ earnings is in the future
            if before_days > 0 and 0 < delta <= before_days:
                return True
            if after_days > 0 and 0 <= -delta <= after_days:
                return True
        return False

    def prefetch(self, tickers: list[str], max_workers: int = 8,
                 save_every: int = 50) -> int:
        """Populate cache for `tickers` not yet seen. Returns # newly fetched.
        Saves to disk every `save_every` completed fetches so a kill mid-run
        doesn't lose progress."""
        todo = [t for t in dict.fromkeys(tickers) if t not in self._cache]
        if not todo:
            return 0
        print(f"[earnings] fetching {len(todo)} tickers via yfinance "
              f"({max_workers} workers)...", file=sys.stderr)
        done = 0
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = {ex.submit(_fetch_one, t): t for t in todo}
            for fut in as_completed(futures):
                t = futures[fut]
                try:
                    self._cache[t] = fut.result()
                except Exception:
                    self._cache[t] = []
                self._dirty = True
                done += 1
                if done % 25 == 0:
                    print(f"[earnings] {done}/{len(todo)}", file=sys.stderr, flush=True)
                if done % save_every == 0:
                    self.save()
        self.save()
        return len(todo)


def main():
    """CLI: prefetch earnings for tickers listed in stdin (one per line) or argv."""
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("tickers", nargs="*", help="tickers to fetch (else read stdin)")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--cache", default=str(DEFAULT_CACHE))
    args = ap.parse_args()

    syms = args.tickers
    if not syms:
        syms = [ln.strip().upper() for ln in sys.stdin if ln.strip()]
    syms = [s.upper() for s in syms]

    lookup = EarningsLookup(Path(args.cache))
    n = lookup.prefetch(syms, max_workers=args.workers)
    print(f"[earnings] cached {n} new of {len(syms)}")
    print(f"[earnings] total cache size: {len(lookup._cache)}")


if __name__ == "__main__":
    main()
