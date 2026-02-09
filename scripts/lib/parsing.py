"""Shared parsing helpers for data scripts."""

import re
from datetime import date, datetime, timezone
from typing import Optional
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")


def safe_float(x, default=None) -> Optional[float]:
    """Convert *x* to float, returning *default* on failure."""
    try:
        if x is None or str(x).strip() == "":
            return default
        return float(x)
    except Exception:
        return default


def iso_fix(s: str) -> str:
    """Normalise ISO-8601 offset: ``Z`` → ``+00:00``, ``+HHMM`` → ``+HH:MM``."""
    x = str(s).strip()
    if x.endswith("Z"):
        return x[:-1] + "+00:00"
    m = re.search(r"[+-]\d{4}$", x)
    if m:
        return x[:-5] + x[-5:-2] + ":" + x[-2:]
    return x


def fmt_ts_et(s: str) -> str:
    """Parse an ISO timestamp and return it formatted in Eastern Time."""
    try:
        dt = datetime.fromisoformat(iso_fix(s))
    except Exception:
        return s
    dt = dt.astimezone(ET)
    z = dt.strftime("%Y-%m-%dT%H:%M:%S%z")
    return f"{z[:-2]}:{z[-2:]}"


def parse_sheet_datetime(x) -> Optional[datetime]:
    """Parse a value from a Google Sheet into a tz-aware datetime."""
    if isinstance(x, datetime):
        return x if x.tzinfo else x.replace(tzinfo=timezone.utc)
    if isinstance(x, date) and not isinstance(x, datetime):
        return datetime(x.year, x.month, x.day, tzinfo=timezone.utc)
    if x is None:
        return None
    s = str(x).strip()
    if not s:
        return None
    try:
        s = iso_fix(s)
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def parse_sheet_date(x) -> Optional[date]:
    """Parse a value from a Google Sheet into a date (in ET)."""
    if isinstance(x, date) and not isinstance(x, datetime):
        return x
    dt = parse_sheet_datetime(x)
    if isinstance(dt, datetime):
        return dt.astimezone(ET).date()
    if x is None:
        return None
    s = str(x).strip()
    if not s:
        return None
    try:
        return date.fromisoformat(s[:10])
    except Exception:
        return None


def contract_multiplier(symbol: str, underlying: str) -> int:
    """Return 100 for OCC-coded options / index options, else 1."""
    s = (symbol or "").upper()
    u = (underlying or "").upper()
    if re.search(r"\d{6}[CP]\d{8}$", s):
        return 100
    if u in {"SPX", "SPXW", "NDX", "RUT", "VIX", "XSP"}:
        return 100
    return 1
