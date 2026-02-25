"""Time utilities — trading-day calendar and DTE conversions.

Convention:
    T (time to expiration) is ALWAYS expressed in trading-day units.
    Annualization uses TRADING_DAYS_PER_YEAR = 252.

    OptionContract.days_to_exp is SOURCE DATA (calendar days from Schwab,
    hardcoded from TT) — never use it for annualization directly.
    Always derive trading DTE from actual dates via trading_days_between().
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Set

TRADING_DAYS_PER_YEAR = 252

# US equity market holidays — dates with no regular trading session.
# Expand this set as needed; half-day dates are NOT holidays.
HOLIDAYS: Set[date] = set()


def is_trading_day(d: date) -> bool:
    """True if d is a weekday and not a known holiday."""
    return d.weekday() < 5 and d not in HOLIDAYS


def trading_days_between(start: date, end: date) -> int:
    """Count trading days from start (exclusive) to end (inclusive).

    Matches the options convention: DTE counts business days from today
    (after close) to expiration (at close).

    Examples (assuming no holidays):
        Mon → Tue: 1 trading day
        Thu → Fri: 1 trading day
        Fri → Mon: 1 trading day (weekend skipped)
        Mon → Wed: 2 trading days

    For 0DTE (start == end): returns 0.
    """
    if end <= start:
        return 0

    count = 0
    current = start + timedelta(days=1)
    while current <= end:
        if is_trading_day(current):
            count += 1
        current += timedelta(days=1)
    return count


def trading_dte(snapshot_date: date, expiration: date) -> int:
    """Compute trading DTE for an option contract.

    Args:
        snapshot_date: The date of the chain snapshot (today).
        expiration: The option expiration date.

    Returns:
        Number of trading days to expiration (0 for 0DTE).
    """
    return trading_days_between(snapshot_date, expiration)


def t_years(snapshot_date: date, expiration: date) -> float:
    """Compute T in years (trading-day basis) for annualization.

    Returns trading_dte / TRADING_DAYS_PER_YEAR.
    For 0DTE, returns a small positive value (1/TRADING_DAYS_PER_YEAR)
    to avoid division by zero while still giving a meaningful annualized number.
    """
    td = trading_dte(snapshot_date, expiration)
    if td == 0:
        # 0DTE: use 1 trading day as floor to avoid div-by-zero.
        # The straddle price already embeds the remaining-day vol.
        return 1.0 / TRADING_DAYS_PER_YEAR
    return td / TRADING_DAYS_PER_YEAR
