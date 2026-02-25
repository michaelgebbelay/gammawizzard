"""GammaWizard API client — fetch VIX1D + realized vol for the sim.

Fetches from rapi/GetUltraPureConstantStable (the ConstantStable signal
endpoint) which includes VixOne, RV, RV5, RV10, RV20 alongside trade signals.

Anti-lookahead gating (v13):
  - OPEN window  → only prior-day GW data (RV values lagged by 1 day)
  - CLOSE+5      → same-day GW data allowed (already published by 4:05 PM)

Env vars:
    GW_BASE      — API base URL (default https://gandalf.gammawizard.com)
    GW_TOKEN     — bearer token (optional, falls back to email/password)
    GW_EMAIL     — GW account email
    GW_PASSWORD  — GW account password

Usage:
    from sim.data.gw_client import fetch_gw_data, gate_gw_for_window
    raw = fetch_gw_data("2025-01-15")
    gated = gate_gw_for_window(raw, window="open", session_date="2025-01-15")
"""

from __future__ import annotations

import json
import logging
import os
from datetime import date
from pathlib import Path
from typing import Optional

import requests

from sim.config import CACHE_DIR

logger = logging.getLogger(__name__)

GW_BASE = "https://gandalf.gammawizard.com"
GW_ENDPOINT = "rapi/GetUltraPureConstantStable"


def _sanitize_token(t: str) -> str:
    t = (t or "").strip().strip('"').strip("'")
    return t.split(None, 1)[1] if t.lower().startswith("bearer ") else t


def _auth(base: str, email: str, password: str) -> str:
    r = requests.post(
        f"{base}/goauth/authenticateFireUser",
        data={"email": email, "password": password},
        timeout=30,
    )
    r.raise_for_status()
    tok = r.json().get("token") or ""
    if not tok:
        raise RuntimeError("GW auth returned no token")
    return tok


def _gw_get(endpoint: str) -> dict | list:
    """Authenticated GET against the GW API."""
    base = os.environ.get("GW_BASE", GW_BASE).rstrip("/")
    ep = endpoint.lstrip("/")
    url = f"{base}/{ep}"

    def hit(tok: str | None):
        headers = {"Accept": "application/json"}
        if tok:
            headers["Authorization"] = f"Bearer {_sanitize_token(tok)}"
        return requests.get(url, headers=headers, timeout=30)

    tok = _sanitize_token(os.environ.get("GW_TOKEN", "") or "")
    r = hit(tok if tok else None)

    if r.status_code in (401, 403):
        email = os.environ.get("GW_EMAIL", "")
        pwd = os.environ.get("GW_PASSWORD", "")
        if not (email and pwd):
            r.raise_for_status()
        fresh = _auth(base, email, pwd)
        r = hit(fresh)

    r.raise_for_status()
    return r.json()


def _extract_predictions(j) -> list[dict]:
    """Extract the Predictions array from GW response."""
    if isinstance(j, dict):
        preds = j.get("Predictions") or j.get("Trade")
        if isinstance(preds, list):
            return preds
        if isinstance(preds, dict):
            return [preds]
    return []


def _safe_float(val) -> Optional[float]:
    if val is None or val == "":
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _parse_row(row: dict) -> dict:
    """Parse a single GW prediction row into our normalized dict."""
    return {
        "date": str(row.get("Date") or "").strip()[:10],
        "expiry": str(row.get("TDate") or "").strip()[:10],
        "spx": _safe_float(row.get("SPX")),
        "forward": _safe_float(row.get("Forward")),
        "vix": _safe_float(row.get("VIX")),
        "vix_1d": _safe_float(row.get("VixOne")),
        "rv": _safe_float(row.get("RV")),
        "rv5": _safe_float(row.get("RV5")),
        "rv10": _safe_float(row.get("RV10")),
        "rv20": _safe_float(row.get("RV20")),
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch_gw_data(trading_date: Optional[str] = None) -> Optional[dict]:
    """Fetch VIX1D + RV data from GammaWizard API.

    Args:
        trading_date: ISO date string (YYYY-MM-DD) to match. If None, uses today.
                      Falls back to the latest available row if today's isn't found.

    Returns:
        Dict with keys: date, expiry, spx, forward, vix, vix_1d, rv, rv5, rv10, rv20.
        None if the API call fails or no data is available.
    """
    target = trading_date or date.today().isoformat()

    try:
        raw = _gw_get(GW_ENDPOINT)
        preds = _extract_predictions(raw)
        if not preds:
            logger.warning("GW API returned no predictions")
            return None

        rows = [_parse_row(p) for p in preds if p.get("Date")]

        # Try exact date match
        for row in rows:
            if row["date"] == target:
                logger.info("GW data for %s: VIX1D=%s, RV=%s",
                            target, row["vix_1d"], row["rv"])
                return row

        # Fall back to most recent row (sorted by date desc)
        rows.sort(key=lambda r: r["date"], reverse=True)
        if rows:
            latest = rows[0]
            logger.info("GW data: no exact match for %s, using latest %s "
                        "(VIX1D=%s, RV=%s)",
                        target, latest["date"], latest["vix_1d"], latest["rv"])
            return latest

        logger.warning("GW API returned predictions but none with valid dates")
        return None

    except Exception as e:
        logger.error("GW API fetch failed: %s: %s", type(e).__name__, e)
        return None


# Backwards-compatible alias
fetch_gw_market_data = fetch_gw_data


def gate_gw_for_window(gw_data: Optional[dict],
                       window: str,
                       session_date: str) -> Optional[dict]:
    """Anti-lookahead gating for GW data per v13.

    Rules:
      - OPEN window (9:31 AM):
          * VIX1D: allowed (published by CBOE before open)
          * RV, RV5, RV10, RV20: only if gw_data["date"] < session_date
            (prior-day values only — today's RV isn't known yet at open)
      - CLOSE+5 window (4:05 PM):
          * All fields allowed (day is complete, RV is finalized)

    Args:
        gw_data: Raw GW data dict, or None.
        window: "open" or "close5".
        session_date: ISO date of the current session.

    Returns:
        Gated copy of gw_data with forbidden fields set to None, or None.
    """
    if gw_data is None:
        return None

    if window == "close5":
        # CLOSE+5: all data allowed
        return dict(gw_data)

    # OPEN window: gate RV fields if they're from today (not yet finalized)
    gated = dict(gw_data)
    gw_date = gw_data.get("date", "")

    if gw_date >= session_date:
        # Same-day or future data — RV values are lookahead
        gated["rv"] = None
        gated["rv5"] = None
        gated["rv10"] = None
        gated["rv20"] = None
        logger.info("GW anti-lookahead: gated RV fields for OPEN window "
                    "(gw_date=%s >= session_date=%s)", gw_date, session_date)

    # VIX1D is always allowed (published pre-market by CBOE)
    return gated


# ---------------------------------------------------------------------------
# Cache persistence
# ---------------------------------------------------------------------------

def save_gw_data(trading_date: str, phase: str, data: dict) -> Path:
    """Persist GW data alongside chain snapshot.

    Path: sim/cache/YYYY-MM-DD/{phase}_gw.json
    """
    path = CACHE_DIR / trading_date / f"{phase}_gw.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f)
    logger.info("Saved GW data to %s", path)
    return path


def load_gw_data(trading_date: str, phase: str) -> Optional[dict]:
    """Load persisted GW data from cache."""
    path = CACHE_DIR / trading_date / f"{phase}_gw.json"
    if not path.exists():
        return None
    with open(path) as f:
        return json.load(f)
