"""Market session calendar utilities for live round gating."""

from __future__ import annotations

from datetime import date, time

# Minimal 2026 US market holiday set. Extend yearly as needed.
US_MARKET_HOLIDAYS_2026 = {
    date(2026, 1, 1),   # New Year's Day
    date(2026, 1, 19),  # Martin Luther King Jr. Day
    date(2026, 2, 16),  # Washington's Birthday
    date(2026, 4, 3),   # Good Friday
    date(2026, 5, 25),  # Memorial Day
    date(2026, 6, 19),  # Juneteenth
    date(2026, 7, 3),   # Independence Day (observed)
    date(2026, 9, 7),   # Labor Day
    date(2026, 11, 26), # Thanksgiving Day
    date(2026, 12, 25), # Christmas Day
}

# Common equity early closes for 2026.
US_MARKET_EARLY_CLOSES_2026 = {
    date(2026, 11, 27),  # Day after Thanksgiving
    date(2026, 12, 24),  # Christmas Eve
}

REGULAR_CLOSE_ET = time(16, 0)
EARLY_CLOSE_ET = time(13, 0)


def is_trading_day(d: date) -> bool:
    if d.weekday() >= 5:
        return False
    if d in US_MARKET_HOLIDAYS_2026:
        return False
    return True


def session_close_time_et(d: date) -> time:
    if d in US_MARKET_EARLY_CLOSES_2026:
        return EARLY_CLOSE_ET
    return REGULAR_CLOSE_ET

