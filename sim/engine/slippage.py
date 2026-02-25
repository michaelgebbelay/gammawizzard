"""Dynamic slippage model — VIX-scaled, move-scaled, stochastic.

Slippage is applied ONCE per spread (not per leg) to the composite
spread NBBO. This prevents multi-leg structures from being penalized
disproportionately.
"""

from __future__ import annotations

import random
from typing import Optional

from sim.config import (
    SLIPPAGE_BASE,
    SLIPPAGE_MOVE_BANDS,
    SLIPPAGE_NOISE_HI,
    SLIPPAGE_NOISE_LO,
    SLIPPAGE_VIX_BANDS,
    SLIPPAGE_WIDENED_PROB,
)


def _band_multiplier(value: float, bands: list) -> float:
    """Look up multiplier from a list of (threshold, multiplier) bands."""
    for threshold, mult in bands:
        if value < threshold:
            return mult
    return bands[-1][1]


def compute_slippage(vix: float, intraday_move_pts: float,
                     rng: Optional[random.Random] = None) -> float:
    """Compute slippage in dollars per spread for a single fill.

    Args:
        vix: Current VIX level.
        intraday_move_pts: Absolute SPX move in points since the open.
        rng: Random number generator (for reproducibility).

    Returns:
        Slippage amount in dollars per spread. Always non-negative.
        Applied as a penalty: credit fills receive less, debit fills pay more.
    """
    if rng is None:
        rng = random.Random()

    vix_mult = _band_multiplier(vix, SLIPPAGE_VIX_BANDS)
    move_mult = _band_multiplier(abs(intraday_move_pts), SLIPPAGE_MOVE_BANDS)
    noise = rng.uniform(SLIPPAGE_NOISE_LO, SLIPPAGE_NOISE_HI)

    slippage = SLIPPAGE_BASE * vix_mult * move_mult * noise

    # 5% chance of widened spread event (doubled slippage)
    if rng.random() < SLIPPAGE_WIDENED_PROB:
        slippage *= 2.0

    return round(slippage, 2)
