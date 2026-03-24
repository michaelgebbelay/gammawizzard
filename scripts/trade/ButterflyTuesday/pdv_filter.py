"""PDV-based vol filter for the butterfly strategy.

Uses the Guyon-Lekeufack Path-Dependent Volatility model to assess whether
the put tail is overpriced by the market. When the model's predicted
confidence interval is narrower than the market's 20-delta put anchor,
vol is likely overpriced → good conditions for BUY butterfly.

Skip BUY butterfly when the model says put tail is UNDERpriced (expecting
larger downside moves than the market prices).

Backtest results (2DTE put credit spreads):
  - OOS Sharpe 1.06 (87% win, 18/24 months positive)
  - FWD Sharpe 1.95 (89% win, 11/14 months positive)
  - Half the drawdown of always-trade baseline
"""

from __future__ import annotations

import math
import os
from typing import Optional

import numpy as np

DT = 1.0 / 252.0
PDV_LOOKBACK = 1000

# Paper VIX params (Table 3, Guyon-Lekeufack 2023)
A1, D1, A2, D2 = 1.06, 0.020, 1.60, 0.052
B0, B1, B2 = 0.057, -0.095, 0.82

# Short-term R1 window for directional asymmetry
SHORT_R1_WINDOW = 20
ASYM_K = 30.0
Z_VALUE = 0.60

# Skip BUY butterfly when model expects MORE downside than market prices.
# put_div < 0 means model's put tail extends beyond market's 20-delta anchor.
SKIP_THRESHOLD = 0.0


def _tspl_weights(lookback: int, alpha: float, delta: float) -> np.ndarray:
    lags = np.arange(lookback, 0, -1, dtype=float) * DT
    raw = (lags + delta) ** (-alpha)
    Z = raw.sum() * DT
    return raw / Z


def _compute_pdv_sigma(closes: list[float]) -> Optional[float]:
    """Compute PDV predicted daily sigma from closing prices.

    Needs at least PDV_LOOKBACK + 1 closes. Returns None if insufficient data.
    """
    if len(closes) < PDV_LOOKBACK + 2:
        return None

    arr = np.array(closes, dtype=float)
    returns = np.diff(arr) / arr[:-1]

    if len(returns) < PDV_LOOKBACK:
        return None

    w1 = _tspl_weights(PDV_LOOKBACK, A1, D1)
    w2 = _tspl_weights(PDV_LOOKBACK, A2, D2)

    window = returns[-PDV_LOOKBACK:]
    R1 = np.dot(w1, window)
    R2 = np.dot(w2, window ** 2)
    Sigma = math.sqrt(max(R2, 1e-16))

    pdv_iv_ann = B0 + B1 * R1 + B2 * Sigma  # annualized
    sigma_daily = pdv_iv_ann / math.sqrt(252.0)
    return max(sigma_daily, 1e-6)


def _compute_short_r1(closes: list[float], window: int = SHORT_R1_WINDOW) -> float:
    """Exponential-weighted short-term return for directional signal."""
    if len(closes) < window + 2:
        return 0.0

    arr = np.array(closes, dtype=float)
    returns = np.diff(arr) / arr[:-1]

    if len(returns) < window:
        return 0.0

    weights = np.exp(np.linspace(-2, 0, window))
    weights /= weights.sum()
    return float(np.dot(weights, returns[-window:]))


def _fetch_spx_closes() -> list[float]:
    """Download SPX daily closes from Yahoo Finance."""
    try:
        import yfinance as yf
        df = yf.download("^GSPC", start="2000-01-01", progress=False)
        return df[("Close", "^GSPC")].values.flatten().tolist()
    except Exception as e:
        print(f"PDV_FILTER WARN: yfinance download failed: {e}")
        return []


def pdv_filter_decision(
    spot: float,
    put_20d_strike: Optional[float],
    dte: int = 4,
    spx_closes: Optional[list[float]] = None,
) -> tuple[bool, dict]:
    """Decide whether to skip BUY butterfly based on PDV model.

    Args:
        spot: Current SPX price.
        put_20d_strike: 20-delta put strike from the chain (None if unavailable).
        dte: Days to expiration for the butterfly.
        spx_closes: Pre-fetched SPX daily closes (oldest first). If None,
                     downloads from Yahoo Finance.

    Returns:
        (should_skip, debug_info) where should_skip=True means SKIP the BUY.
    """
    if os.environ.get("BF_PDV_FILTER_DISABLE", "0") == "1":
        return False, {"pdv_filter": "disabled"}

    # Get SPX closes
    if spx_closes is None:
        spx_closes = _fetch_spx_closes()

    if not spx_closes or len(spx_closes) < PDV_LOOKBACK + 10:
        return False, {"pdv_filter": "insufficient_data", "n_closes": len(spx_closes)}

    sigma_daily = _compute_pdv_sigma(spx_closes)
    if sigma_daily is None:
        return False, {"pdv_filter": "sigma_computation_failed"}

    r1_short = _compute_short_r1(spx_closes)

    # Asymmetric interval (same logic as backtest)
    r1_effect = r1_short * ASYM_K
    put_mult = max(0.50, min(1.50, 1.0 - r1_effect))
    sigma_nday = sigma_daily * math.sqrt(dte)
    lower = spot * math.exp(-put_mult * Z_VALUE * sigma_nday)

    debug = {
        "pdv_filter": "computed",
        "pdv_sigma_daily": round(sigma_daily, 6),
        "pdv_sigma_ann": round(sigma_daily * math.sqrt(252) * 100, 1),
        "r1_short": round(r1_short, 6),
        "put_mult": round(put_mult, 3),
        "pdv_lower": round(lower, 1),
        "put_20d_strike": put_20d_strike,
    }

    if put_20d_strike is None:
        debug["pdv_filter"] = "no_put_anchor"
        return False, debug

    # Compare PDV put bound vs market 20-delta put
    pdv_put_dist = (spot - lower) / spot
    mkt_put_dist = (spot - put_20d_strike) / spot
    put_div = mkt_put_dist - pdv_put_dist  # + = market wider, - = model wider

    debug["pdv_put_dist_pct"] = round(pdv_put_dist * 100, 3)
    debug["mkt_put_dist_pct"] = round(mkt_put_dist * 100, 3)
    debug["put_div"] = round(put_div, 5)

    # Skip BUY butterfly when model expects MORE downside than market prices
    # (put_div < threshold → model's put tail extends beyond market's → vol underpriced)
    should_skip = put_div < SKIP_THRESHOLD
    debug["pdv_skip"] = should_skip
    debug["pdv_reason"] = (
        f"put_div={put_div:.4f}<{SKIP_THRESHOLD}" if should_skip
        else f"put_div={put_div:.4f}>={SKIP_THRESHOLD}"
    )

    return should_skip, debug
