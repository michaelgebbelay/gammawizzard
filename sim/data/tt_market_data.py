"""TastyTrade chain fetcher — fetch SPX 1DTE chain with greeks via TT API.

Two-step process:
1. GET /option-chains/SPX/nested → strike/symbol mapping for all expirations
2. GET /market-data/by-type?option=<symbol> → greeks/pricing per symbol (batched)
"""

from __future__ import annotations

import json
import logging
import os
import random
import sys
import time
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import requests

# Add TT/Script to path for tt_token_keeper
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_TT_SCRIPT = os.path.join(_REPO_ROOT, "TT", "Script")
if _TT_SCRIPT not in sys.path:
    sys.path.insert(0, _TT_SCRIPT)

from tt_token_keeper import get_access_token, refresh_token, load_token

logger = logging.getLogger(__name__)

ET = ZoneInfo("America/New_York")


# --- TT API helpers ---

def _base_url() -> str:
    return os.environ.get("TT_BASE_URL", "https://api.tastyworks.com").rstrip("/")


def _auth_header() -> dict:
    tok = get_access_token()
    return {"Authorization": f"Bearer {tok}"}


def _tt_get(path: str, params: Optional[dict] = None,
            tries: int = 4) -> dict:
    """GET with auto-refresh on 401 and 429 retry."""
    url = f"{_base_url()}/{path.lstrip('/')}"
    last_err = None

    for attempt in range(tries):
        try:
            headers = _auth_header()
            r = requests.get(url, headers=headers, params=params or {}, timeout=20)

            if r.status_code == 401:
                token = load_token()
                refresh_token(token)
                headers = _auth_header()
                r = requests.get(url, headers=headers, params=params or {}, timeout=20)

            if r.status_code == 429:
                wait = min(6.0, 0.5 * (2 ** attempt)) + random.uniform(0.0, 0.25)
                ra = r.headers.get("Retry-After")
                if ra:
                    try:
                        wait = max(1.0, float(ra))
                    except ValueError:
                        pass
                logger.warning("TT 429 on %s, waiting %.1fs", path, wait)
                time.sleep(wait)
                continue

            r.raise_for_status()
            return r.json()

        except requests.RequestException as e:
            last_err = e
            logger.warning("TT request error on %s attempt %d: %s", path, attempt + 1, e)
            time.sleep(min(2.0, 0.35 * (2 ** attempt)))

    logger.error("TT request failed after %d attempts: %s", tries, last_err)
    return {}


# --- Chain structure fetching ---

def _fetch_nested_chain() -> dict:
    """Fetch the full nested option chain for SPX."""
    # Try SPX first (returns both SPX monthlies and SPXW weeklies)
    for underlying in ["SPX", "SPXW"]:
        j = _tt_get(f"/option-chains/{underlying}/nested")
        items = j.get("data", {}).get("items", [])
        if items:
            logger.info("Fetched nested chain from %s: %d item groups", underlying, len(items))
            return j
    return {}


def _find_1dte_expiration(chain_json: dict) -> Optional[Tuple[str, list]]:
    """Find the 1DTE PM-settled expiration and its strikes.

    Returns (expiration_date_str, strikes_list) or None.
    """
    today = datetime.now(ET).date()
    tomorrow = today + timedelta(days=1)
    # For Friday, 1DTE = Monday
    if today.weekday() == 4:  # Friday
        tomorrow = today + timedelta(days=3)

    target_str = tomorrow.isoformat()

    items = chain_json.get("data", {}).get("items", [])
    for item in items:
        root_symbol = item.get("root-symbol", "")
        exps = item.get("expirations", [])
        for exp in exps:
            exp_date = exp.get("expiration-date", "")
            exp_type = exp.get("expiration-type", "")

            if exp_date != target_str:
                continue

            strikes = exp.get("strikes", [])
            if not strikes:
                continue

            # Prefer SPXW (PM-settled weekly/daily)
            # Skip AM-settled monthlies (root="SPX", type="Regular")
            is_weekly = "W" in root_symbol.upper() or exp_type in ("Weekly", "Daily", "End of Week")
            is_monthly_am = (
                root_symbol.strip().upper() == "SPX"
                and exp_type == "Regular"
            )

            if is_monthly_am:
                logger.info("Skipping AM-settled monthly: %s (type=%s)", exp_date, exp_type)
                continue

            logger.info("Found 1DTE expiration: %s (root=%s, type=%s, strikes=%d)",
                        exp_date, root_symbol, exp_type, len(strikes))
            return exp_date, strikes

    logger.warning("No 1DTE expiration found for %s", target_str)
    return None


# --- Quote batching ---

def _extract_quote_list(j: dict) -> list:
    """Extract option quote list from TT market-data response."""
    if not isinstance(j, dict):
        return []
    for key in ("byType", "by-type", "by_type"):
        bt = j.get(key)
        if isinstance(bt, dict):
            opt = bt.get("option") or bt.get("options")
            if isinstance(opt, list):
                return opt
    if "data" in j and isinstance(j["data"], dict):
        return _extract_quote_list(j["data"])
    if "items" in j and isinstance(j["items"], list):
        return j["items"]
    return []


def _fetch_quotes_batch(symbols: List[str],
                        batch_size: int = 10) -> Dict[str, dict]:
    """Fetch quotes for multiple option symbols via REST.

    TT's /market-data/by-type accepts one symbol at a time.
    We fetch sequentially with brief pauses to avoid 429s.
    """
    quotes = {}
    total = len(symbols)
    fetched = 0

    for i in range(0, total, batch_size):
        batch = symbols[i:i + batch_size]
        for sym in batch:
            j = _tt_get("/market-data/by-type", params={"option": sym})
            items = _extract_quote_list(j)
            if items:
                q = items[0]
                quotes[sym] = q
                fetched += 1
            else:
                logger.debug("No quote data for %s", sym)

        # Brief pause between batches to avoid 429s
        if i + batch_size < total:
            time.sleep(0.5)

        # Progress logging
        if (i + batch_size) % 50 == 0 or i + batch_size >= total:
            logger.info("Quote fetch progress: %d/%d symbols (%d with data)",
                        min(i + batch_size, total), total, fetched)

    logger.info("Fetched quotes for %d/%d symbols", len(quotes), len(symbols))
    return quotes


# --- Safe value extraction ---

def _sfloat(val, default: float = 0.0) -> float:
    if val is None:
        return default
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def _sint(val, default: int = 0) -> int:
    if val is None:
        return default
    try:
        return int(val)
    except (TypeError, ValueError):
        return default


# --- Public API ---

def fetch_tt_spx_chain(phase: str = "open",
                       strike_window: int = 40) -> Optional[dict]:
    """Fetch the SPX 1DTE chain from TastyTrade with greeks.

    Only fetches quotes for strikes within ±strike_window of ATM to
    avoid excessive API calls (TT requires 1 call per symbol).

    Args:
        phase: "open", "mid", or "close".
        strike_window: Number of strikes above and below ATM to fetch.

    Returns a normalized dict compatible with parse_tt_chain(),
    or None on failure.
    """
    # Step 1: Get chain structure
    chain_json = _fetch_nested_chain()
    if not chain_json:
        logger.error("Failed to fetch nested chain")
        return None

    result = _find_1dte_expiration(chain_json)
    if result is None:
        return None

    exp_date_str, strikes_list = result

    # Step 1.5: Get SPX price first to determine ATM for filtering
    spx_price = _fetch_underlying_price()

    # Step 2: Collect option symbols, filtered to ±strike_window of ATM
    all_strike_prices = sorted(set(
        _sfloat(st.get("strike-price")) for st in strikes_list
        if _sfloat(st.get("strike-price")) > 0
    ))

    # Find ATM strike
    atm = min(all_strike_prices, key=lambda s: abs(s - spx_price)) if all_strike_prices else spx_price

    # Filter to window around ATM
    nearby_strikes = set(
        s for s in all_strike_prices
        if abs(s - atm) <= strike_window * 5  # 5-point increments, so ±200 points
    )
    logger.info("ATM=%.0f, filtering to %d/%d strikes within ±%d of ATM",
                atm, len(nearby_strikes), len(all_strike_prices), strike_window * 5)

    call_symbols = []
    put_symbols = []
    strike_map = {}  # symbol → (strike, put_call)

    for st in strikes_list:
        strike_price = _sfloat(st.get("strike-price"))
        if strike_price <= 0 or strike_price not in nearby_strikes:
            continue

        call_sym = st.get("call", "")
        put_sym = st.get("put", "")

        if call_sym:
            call_symbols.append(call_sym)
            strike_map[call_sym] = (strike_price, "C")
        if put_sym:
            put_symbols.append(put_sym)
            strike_map[put_sym] = (strike_price, "P")

    all_symbols = call_symbols + put_symbols
    logger.info("Fetching quotes for %d calls + %d puts = %d symbols",
                len(call_symbols), len(put_symbols), len(all_symbols))

    # Step 3: Fetch quotes/greeks (may be empty after hours)
    quotes = _fetch_quotes_batch(all_symbols)

    # Step 4: Fetch VIX and underlying (already fetched above, refresh)
    vix = _fetch_vix()
    if spx_price <= 0:
        spx_price = _fetch_underlying_price()

    # Step 5: Build normalized output
    # If quotes available, use them. Otherwise build skeleton contracts
    # from chain structure (for after-hours caching).
    contracts = {}

    if quotes:
        # Normal path: we have quote data with greeks
        for sym, quote in quotes.items():
            if sym not in strike_map:
                continue

            strike, put_call = strike_map[sym]
            bid = _sfloat(quote.get("bid") or quote.get("bid-price") or quote.get("bidPrice"))
            ask = _sfloat(quote.get("ask") or quote.get("ask-price") or quote.get("askPrice"))
            mark = _sfloat(quote.get("mark", (bid + ask) / 2.0 if bid and ask else 0.0))

            contracts[sym] = {
                "symbol": sym,
                "strike": strike,
                "put_call": put_call,
                "bid": bid,
                "ask": ask,
                "mark": mark,
                "last": _sfloat(quote.get("last") or quote.get("lastPrice") or quote.get("last-price")),
                "delta": _sfloat(quote.get("delta")),
                "gamma": _sfloat(quote.get("gamma")),
                "theta": _sfloat(quote.get("theta")),
                "vega": _sfloat(quote.get("vega")),
                "rho": _sfloat(quote.get("rho")),
                "implied_vol": _sfloat(quote.get("volatility") or quote.get("implied-volatility") or quote.get("impliedVolatility")),
                "volume": _sint(quote.get("volume") or quote.get("totalVolume")),
                "open_interest": _sint(quote.get("openInterest") or quote.get("open-interest")),
                "in_the_money": bool(quote.get("inTheMoney") or quote.get("in-the-money", False)),
                "days_to_exp": 1,
            }
    else:
        # After-hours fallback: build skeleton from chain structure
        # This allows caching the chain shape; a re-fetch during market
        # hours will overwrite with real quotes.
        logger.warning("No quote data available (market likely closed). "
                       "Building skeleton chain from structure.")
        for sym, (strike, put_call) in strike_map.items():
            contracts[sym] = {
                "symbol": sym,
                "strike": strike,
                "put_call": put_call,
                "bid": 0.0, "ask": 0.0, "mark": 0.0, "last": 0.0,
                "delta": 0.0, "gamma": 0.0, "theta": 0.0,
                "vega": 0.0, "rho": 0.0, "implied_vol": 0.0,
                "volume": 0, "open_interest": 0,
                "in_the_money": strike < spx_price if put_call == "C" else strike > spx_price,
                "days_to_exp": 1,
            }

    if not contracts:
        logger.error("No contracts found at all")
        return None

    has_greeks = any(c.get("delta", 0) != 0 for c in contracts.values())
    logger.info("Built chain: %d contracts (greeks=%s), SPX=%.2f, VIX=%.1f, exp=%s",
                len(contracts), has_greeks, spx_price, vix, exp_date_str)

    # Step 6: Fetch SPX OHLC for context
    spx_ohlc = _fetch_underlying_ohlc()

    return {
        "_source": "tastytrade",
        "_phase": phase,
        "_vix": vix,
        "_timestamp": datetime.now(ET).isoformat(),
        "underlying_price": spx_price,
        "expiration": exp_date_str,
        "contracts": contracts,
        "spx_open": spx_ohlc.get("open", 0.0),
        "spx_high": spx_ohlc.get("high", 0.0),
        "spx_low": spx_ohlc.get("low", 0.0),
        "spx_prev_close": spx_ohlc.get("prev_close", 0.0),
    }


def _fetch_vix() -> float:
    """Fetch current VIX level."""
    try:
        # Plain "VIX" works on TT (not "$VIX.X")
        for sym in ["VIX", "$VIX.X", "$VIX"]:
            j = _tt_get("/market-data/by-type", params={"equity": sym})
            items = _extract_quote_list(j)
            if items:
                q = items[0]
                return _sfloat(q.get("last") or q.get("lastPrice") or q.get("mark") or q.get("bid"))
    except Exception as e:
        logger.warning("Failed to fetch VIX: %s", e)
    return 0.0


def _fetch_underlying_price() -> float:
    """Fetch current SPX price (last)."""
    try:
        for sym in ["SPX", "$SPX.X", "$SPX"]:
            j = _tt_get("/market-data/by-type", params={"equity": sym})
            items = _extract_quote_list(j)
            if items:
                q = items[0]
                return _sfloat(q.get("last") or q.get("lastPrice") or q.get("mark"))
    except Exception as e:
        logger.warning("Failed to fetch SPX price: %s", e)
    return 0.0


def _fetch_underlying_ohlc() -> dict:
    """Fetch SPX open/high/low/prev_close from TT equity quote."""
    try:
        for sym in ["SPX", "$SPX.X"]:
            j = _tt_get("/market-data/by-type", params={"equity": sym})
            items = _extract_quote_list(j)
            if items:
                q = items[0]
                return {
                    "open": _sfloat(q.get("open")),
                    "high": _sfloat(q.get("day-high-price") or q.get("dayHighPrice")),
                    "low": _sfloat(q.get("day-low-price") or q.get("dayLowPrice")),
                    "prev_close": _sfloat(q.get("prev-close") or q.get("prevClose")),
                }
    except Exception as e:
        logger.warning("Failed to fetch SPX OHLC: %s", e)
    return {}
