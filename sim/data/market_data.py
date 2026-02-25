"""Schwab API integration for fetching SPX options chains and quotes."""

from __future__ import annotations

import os
import sys
from datetime import date, timedelta
from typing import Optional

# Add repo root so we can import the existing schwab_token_keeper
_repo_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)

from scripts.schwab_token_keeper import schwab_client

from sim.data.chain_snapshot import ChainSnapshot, parse_schwab_chain


def fetch_spx_chain(phase: str, target_date: Optional[date] = None,
                    strike_count: int = 40) -> ChainSnapshot:
    """Fetch SPX options chain from Schwab API.

    Fetches the full chain for 1DTE (tomorrow's expiration) with greeks.
    Filters to PM-settled SPXW contracts only.

    Args:
        phase: "open", "mid", or "close"
        target_date: The trading date (defaults to today). Chain is fetched
                     for expirations from target_date to target_date + 2 days
                     to capture 1DTE.
        strike_count: Number of strikes above and below ATM to fetch.

    Returns:
        ChainSnapshot with PM-settled contracts and greeks.
    """
    if target_date is None:
        target_date = date.today()

    c = schwab_client()

    # Fetch chain — include today + next 2 days to capture both 0DTE and 1DTE
    from_date = target_date
    to_date = target_date + timedelta(days=3)  # buffer for weekends

    resp = c.get_option_chain(
        "$SPX",
        contract_type=c.Options.ContractType.ALL,
        strike_count=strike_count,
        include_underlying_quote=True,
        from_date=from_date,
        to_date=to_date,
        option_type=c.Options.Type.ALL,
    )
    resp.raise_for_status()
    raw = resp.json()

    # Fetch VIX quote
    vix = _fetch_vix(c)

    return parse_schwab_chain(raw, phase=phase, vix=vix)


def _fetch_vix(c) -> float:
    """Fetch current VIX level."""
    try:
        resp = c.get_quote("$VIX")
        resp.raise_for_status()
        data = resp.json()
        # Navigate Schwab quote response structure
        for key, val in data.items():
            if isinstance(val, dict):
                q = val.get("quote", val)
                last = q.get("lastPrice") or q.get("last") or q.get("mark")
                if last is not None:
                    return float(last)
    except Exception:
        pass
    return 0.0


def fetch_underlying_price(c=None) -> float:
    """Fetch current SPX price."""
    if c is None:
        c = schwab_client()
    try:
        resp = c.get_quote("$SPX")
        resp.raise_for_status()
        data = resp.json()
        for key, val in data.items():
            if isinstance(val, dict):
                q = val.get("quote", val)
                last = q.get("lastPrice") or q.get("last") or q.get("mark")
                if last is not None:
                    return float(last)
    except Exception:
        pass
    return 0.0
