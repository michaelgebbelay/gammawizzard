"""JSON file cache for chain snapshots.

Cache layout:
  sim/cache/YYYY-MM-DD/open.json
  sim/cache/YYYY-MM-DD/mid.json
  sim/cache/YYYY-MM-DD/close.json

Each file stores the raw chain JSON (Schwab or TastyTrade format) + metadata.
"""

from __future__ import annotations

import json
import os
from datetime import date, datetime
from pathlib import Path
from typing import List, Optional, Tuple, Union

from sim.config import CACHE_DIR


def _to_date(d: Union[str, date]) -> date:
    """Convert string or date to date object."""
    if isinstance(d, str):
        return date.fromisoformat(d)
    return d


def _to_date_str(d: Union[str, date]) -> str:
    """Convert date or string to ISO date string."""
    if isinstance(d, date):
        return d.isoformat()
    return d


def cache_path(trading_date: Union[str, date], phase: str) -> Path:
    """Get the cache file path for a given date and phase."""
    return CACHE_DIR / _to_date_str(trading_date) / f"{phase}.json"


def save_to_cache(trading_date: Union[str, date], phase: str,
                  raw_chain: dict, vix: float) -> Path:
    """Save raw chain response to cache.

    Wraps the raw JSON with metadata (VIX, fetch timestamp).

    Returns:
        Path to the saved cache file.
    """
    path = cache_path(trading_date, phase)
    path.parent.mkdir(parents=True, exist_ok=True)

    wrapper = {
        "trading_date": _to_date_str(trading_date),
        "phase": phase,
        "vix": vix,
        "fetched_at": datetime.utcnow().isoformat(),
        "chain": raw_chain,
    }

    with open(path, "w") as f:
        json.dump(wrapper, f)

    return path


def load_from_cache(trading_date: Union[str, date],
                    phase: str) -> Optional[dict]:
    """Load a cached chain as raw dict.

    Returns:
        Raw chain dict (caller chooses parser: parse_schwab_chain or
        parse_tt_chain based on _source field), or None if not cached.
    """
    path = cache_path(trading_date, phase)
    if not path.exists():
        return None

    with open(path) as f:
        wrapper = json.load(f)

    raw_chain = wrapper.get("chain", {})
    vix = wrapper.get("vix", 0.0)

    # Inject VIX into raw chain for parser consumption
    raw_chain["_vix"] = vix
    return raw_chain


def has_complete_day(trading_date: Union[str, date],
                     require_close5: bool = True) -> bool:
    """Check if all required phases are cached for a given date.

    Args:
        trading_date: Date to check.
        require_close5: If True, close5 must also be present (v13 dual-window).
    """
    phases = ["open", "mid", "close"]
    if require_close5:
        phases.append("close5")
    return all(
        cache_path(trading_date, phase).exists()
        for phase in phases
    )


def list_cached_dates() -> List[date]:
    """List all dates with at least one cached snapshot, sorted ascending."""
    if not CACHE_DIR.exists():
        return []
    dates = []
    for entry in sorted(CACHE_DIR.iterdir()):
        if entry.is_dir():
            try:
                d = date.fromisoformat(entry.name)
                dates.append(d)
            except ValueError:
                continue
    return dates


def find_contiguous_windows(min_length: int = 80,
                            require_close5: bool = True) -> List[List[str]]:
    """Find all contiguous windows of complete trading days.

    A window is contiguous if each day follows the previous trading day
    (weekdays only, no gaps). Each day must have all required phases cached.

    Args:
        min_length: Minimum number of contiguous days.
        require_close5: If True, close5 must be cached for each day.

    Returns:
        List of contiguous date windows (as ISO strings), sorted by
        length (longest first).
    """
    complete_dates = [d for d in list_cached_dates()
                      if has_complete_day(d, require_close5=require_close5)]
    if not complete_dates:
        return []

    windows: List[List[str]] = []
    current_window: List[str] = [complete_dates[0].isoformat()]

    for i in range(1, len(complete_dates)):
        prev = complete_dates[i - 1]
        curr = complete_dates[i]

        expected = _next_trading_day(prev)
        if curr == expected:
            current_window.append(curr.isoformat())
        else:
            if len(current_window) >= min_length:
                windows.append(current_window)
            current_window = [curr.isoformat()]

    if len(current_window) >= min_length:
        windows.append(current_window)

    return sorted(windows, key=len, reverse=True)


def _next_trading_day(d: date) -> date:
    """Return the next weekday after d (skips weekends)."""
    from datetime import timedelta
    nxt = d + timedelta(days=1)
    while nxt.weekday() >= 5:  # 5=Saturday, 6=Sunday
        nxt += timedelta(days=1)
    return nxt
