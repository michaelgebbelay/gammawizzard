"""Post-hoc regime tagging — VIX bucket + market character for analysis."""

from __future__ import annotations


def vix_bucket(vix: float) -> str:
    """Classify VIX level into a regime bucket."""
    if vix < 15:
        return "low"
    elif vix < 20:
        return "normal"
    elif vix < 25:
        return "elevated"
    elif vix < 35:
        return "high"
    else:
        return "extreme"


def market_character(intraday_range: float, expected_move: float) -> str:
    """Classify session character based on realized vs. expected move.

    Args:
        intraday_range: Absolute SPX move (open to close) in points.
        expected_move: ATM straddle price (1σ expected move).

    Returns:
        Character label: "quiet", "normal", "trending", or "volatile".
    """
    if expected_move <= 0:
        return "unknown"

    ratio = intraday_range / expected_move

    if ratio < 0.3:
        return "quiet"
    elif ratio < 0.7:
        return "normal"
    elif ratio < 1.2:
        return "trending"
    else:
        return "volatile"


def tag_session(vix: float, intraday_range: float,
                expected_move: float) -> dict:
    """Generate full regime tag for a session."""
    return {
        "vix_bucket": vix_bucket(vix),
        "market_character": market_character(intraday_range, expected_move),
        "intraday_range": round(intraday_range, 2),
    }
