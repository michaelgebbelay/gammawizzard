"""P&L calculation engine for expired/closed SPX option positions.

Computes realized P&L from settlement data for vertical spreads and
butterflies. SPX is cash-settled, European-style — no assignment risk.

Settlement lookup chain:
  1. strategy_signal_rows.forward (Leo CSV data)
  2. Schwab API quote for SPX close price

Usage:
    from reporting.pnl import compute_all_pnl
    stats = compute_all_pnl(con)
"""

from __future__ import annotations

import json
from datetime import date

from reporting.db import execute, get_connection, init_schema, query_df, query_one


# ---------------------------------------------------------------------------
# Settlement lookup
# ---------------------------------------------------------------------------

def get_settlement(con, expiry_date: date | str) -> float | None:
    """Look up SPX settlement for a given expiry date.

    Chain: strategy_signal_rows.forward → None (caller can add fallbacks).
    """
    if isinstance(expiry_date, date):
        expiry_date = expiry_date.isoformat()

    # Leo signal rows have Forward = SPX settlement price for the expiry
    row = query_one(
        """SELECT forward FROM strategy_signal_rows
           WHERE expiry_date = ? AND forward IS NOT NULL
           ORDER BY trade_date DESC LIMIT 1""",
        [expiry_date],
        con=con,
    )
    if row and row[0]:
        return float(row[0])

    # Fallback: check if trade_date matches (some rows key on trade_date)
    row = query_one(
        """SELECT forward FROM strategy_signal_rows
           WHERE trade_date = ? AND forward IS NOT NULL
           ORDER BY trade_date DESC LIMIT 1""",
        [expiry_date],
        con=con,
    )
    if row and row[0]:
        return float(row[0])

    return None


# ---------------------------------------------------------------------------
# Spread P&L computation
# ---------------------------------------------------------------------------

def _compute_vertical_pnl(
    entry_price: float,
    settlement: float,
    legs: list[dict],
    signal: str,
    qty: int,
) -> tuple[float, float]:
    """Compute P&L for a 2-leg vertical spread.

    Returns (realized_pnl_dollars, exit_price_per_share).

    Leg structure determines spread type:
      PUT legs: higher strike = short put (credit) or long put (debit)
      CALL legs: lower strike = long call (debit) or short call (credit)

    signal=SHORT → credit spread (sold premium)
    signal=LONG  → debit spread (paid premium)
    """
    strikes = sorted(leg["strike"] for leg in legs)
    option_type = legs[0].get("option_type", "PUT")

    if len(strikes) != 2:
        return 0.0, 0.0

    low_strike, high_strike = strikes

    if option_type == "PUT":
        # Put spread intrinsic at settlement
        # High put intrinsic - Low put intrinsic
        high_put = max(0.0, high_strike - settlement)
        low_put = max(0.0, low_strike - settlement)

        if signal == "SHORT":
            # Credit put spread: sold high put, bought low put
            # We collect premium, lose if settlement < high_strike
            intrinsic = high_put - low_put  # what we owe
            exit_price = intrinsic
            pnl_per_share = entry_price - intrinsic
        else:
            # Debit put spread: bought high put, sold low put
            # We pay premium, profit if settlement < high_strike
            intrinsic = high_put - low_put  # what we receive
            exit_price = intrinsic
            pnl_per_share = intrinsic - entry_price

    else:  # CALL
        # Call spread intrinsic at settlement
        low_call = max(0.0, settlement - low_strike)
        high_call = max(0.0, settlement - high_strike)

        if signal == "LONG":
            # Debit call spread: bought low call, sold high call
            intrinsic = low_call - high_call
            exit_price = intrinsic
            pnl_per_share = intrinsic - entry_price
        else:
            # Credit call spread: sold low call, bought high call
            intrinsic = low_call - high_call  # what we owe
            exit_price = intrinsic
            pnl_per_share = entry_price - intrinsic

    realized_pnl = pnl_per_share * qty * 100  # SPX multiplier
    return realized_pnl, exit_price


def _compute_butterfly_pnl(
    entry_price: float,
    settlement: float,
    legs: list[dict],
    signal: str,
    qty: int,
) -> tuple[float, float]:
    """Compute P&L for a 3-leg butterfly spread.

    Standard call butterfly: long 1x low, short 2x middle, long 1x high.
    signal=SHORT means we sold the butterfly (collected premium).
    """
    strikes = sorted(set(leg["strike"] for leg in legs))
    if len(strikes) != 3:
        return 0.0, 0.0

    low, mid, high = strikes
    option_type = legs[0].get("option_type", "CALL")

    if option_type == "CALL":
        intrinsic = (
            max(0.0, settlement - low)
            - 2 * max(0.0, settlement - mid)
            + max(0.0, settlement - high)
        )
    else:
        intrinsic = (
            max(0.0, low - settlement)
            - 2 * max(0.0, mid - settlement)
            + max(0.0, high - settlement)
        )

    intrinsic = max(0.0, intrinsic)  # butterfly can't go negative

    if signal == "SHORT":
        # Sold butterfly: collected premium, pay intrinsic
        pnl_per_share = entry_price - intrinsic
    else:
        # Bought butterfly: paid premium, receive intrinsic
        pnl_per_share = intrinsic - entry_price

    exit_price = intrinsic
    realized_pnl = pnl_per_share * qty * 100
    return realized_pnl, exit_price


# ---------------------------------------------------------------------------
# Batch P&L computation
# ---------------------------------------------------------------------------

def compute_all_pnl(con=None, as_of_date: date | None = None) -> dict:
    """Compute realized P&L for all terminal positions missing it.

    Processes EXPIRED, CLOSED, and ASSIGNED positions where realized_pnl IS NULL.
    Returns stats: {processed, computed, skipped_no_settlement, skipped_no_legs}.
    """
    if con is None:
        con = get_connection()
        init_schema(con)

    stats = {"processed": 0, "computed": 0,
             "skipped_no_settlement": 0, "skipped_no_legs": 0}

    # Find terminal positions without P&L
    df = query_df(
        """SELECT p.position_id, p.strategy, p.entry_price, p.qty, p.signal,
                  p.expiry_date, p.lifecycle_state
           FROM positions p
           WHERE p.lifecycle_state IN ('EXPIRED', 'CLOSED', 'ASSIGNED')
             AND p.realized_pnl IS NULL""",
        con=con,
    )

    for _, pos in df.iterrows():
        stats["processed"] += 1

        expiry = pos["expiry_date"]
        if expiry is None:
            stats["skipped_no_legs"] += 1
            continue

        # Get settlement
        settlement = get_settlement(con, expiry)
        if settlement is None:
            stats["skipped_no_settlement"] += 1
            continue

        # Get legs
        legs_df = query_df(
            "SELECT option_type, strike, action, qty FROM position_legs WHERE position_id = ?",
            [pos["position_id"]],
            con=con,
        )
        if legs_df.empty:
            stats["skipped_no_legs"] += 1
            continue

        legs = legs_df.to_dict("records")
        entry_price = float(pos["entry_price"] or 0)
        qty = int(pos["qty"] or 0)
        signal = pos["signal"] or "LONG"

        # Determine spread type from leg count
        unique_strikes = len(set(leg["strike"] for leg in legs))

        if unique_strikes == 3:
            realized_pnl, exit_price = _compute_butterfly_pnl(
                entry_price, settlement, legs, signal, qty,
            )
        elif unique_strikes == 2:
            realized_pnl, exit_price = _compute_vertical_pnl(
                entry_price, settlement, legs, signal, qty,
            )
        else:
            stats["skipped_no_legs"] += 1
            continue

        # Write P&L back
        execute(
            """UPDATE positions
               SET realized_pnl = ?, exit_price = ?, updated_at = current_timestamp
               WHERE position_id = ?""",
            [realized_pnl, exit_price, pos["position_id"]],
            con=con,
        )
        stats["computed"] += 1

    return stats
