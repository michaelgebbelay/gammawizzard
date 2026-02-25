"""Expiration payoff calculator — intrinsic value at settlement.

SPX is cash-settled, European-style. At expiration, each option's value
is purely intrinsic: max(0, S-K) for calls, max(0, K-S) for puts.
"""

from __future__ import annotations

from sim.config import SPX_MULTIPLIER
from sim.engine.order import Action, Leg, Order, Side, StructureType


def intrinsic_value(strike: float, put_call: str, settlement_price: float) -> float:
    """Intrinsic value of a single option at expiration.

    Args:
        strike: Option strike price.
        put_call: "C" for call, "P" for put.
        settlement_price: SPX closing price at expiration.

    Returns:
        Intrinsic value per share (not multiplied by 100).
    """
    if put_call == "C":
        return max(0.0, settlement_price - strike)
    else:  # put
        return max(0.0, strike - settlement_price)


def spread_settlement_value(legs: list[Leg], settlement_price: float) -> float:
    """Net value of a spread at expiration, from the perspective of the opener.

    Positive = the spread has value to the holder (good for debit buyers,
    bad for credit sellers).

    For a credit seller, P&L = entry_credit - settlement_value.
    For a debit buyer, P&L = settlement_value - entry_debit.
    """
    net_value = 0.0
    for leg in legs:
        iv = intrinsic_value(leg.strike, leg.put_call, settlement_price)
        if leg.action == Action.SELL:
            # We sold this leg — at settlement we pay the intrinsic value
            net_value -= iv * leg.quantity
        else:
            # We bought this leg — at settlement we receive the intrinsic value
            net_value += iv * leg.quantity
    return net_value


def settlement_pnl(entry_price: float, side: Side, legs: list[Leg],
                   settlement_price: float, quantity: int) -> float:
    """Calculate realized P&L at cash settlement.

    Args:
        entry_price: Credit received or debit paid (always positive).
        side: CREDIT or DEBIT.
        legs: The spread legs.
        settlement_price: SPX closing price at expiration.
        quantity: Number of spreads.

    Returns:
        Realized P&L in dollars. Positive = profitable.
    """
    settle_value = spread_settlement_value(legs, settlement_price)

    if side == Side.CREDIT:
        # Credit: we received entry_price, now we owe settle_value
        # P&L = (credit_received + settle_value) * multiplier * qty
        # settle_value is negative when ITM (we owe), positive when OTM from our legs
        pnl_per_spread = entry_price + settle_value
    else:
        # Debit: we paid entry_price, now settle_value is what we get back
        # settle_value is positive when profitable
        pnl_per_spread = settle_value - entry_price

    return pnl_per_spread * SPX_MULTIPLIER * quantity


def max_profit(entry_price: float, side: Side, width: float,
               quantity: int) -> float:
    """Maximum possible profit for a structure.

    Args:
        entry_price: Credit received or debit paid.
        side: CREDIT or DEBIT.
        width: Spread width in points (e.g., 5.0 for 5-wide).
        quantity: Number of spreads.

    Returns:
        Max profit in dollars.
    """
    if side == Side.CREDIT:
        return entry_price * SPX_MULTIPLIER * quantity
    else:
        return (width - entry_price) * SPX_MULTIPLIER * quantity


def max_loss_amount(entry_price: float, side: Side, width: float,
                    quantity: int) -> float:
    """Maximum possible loss for a structure (positive number).

    For ICs: width is one side's width. At expiration only one side
    can be ITM, so max loss = width - total_credit.
    """
    if side == Side.CREDIT:
        return (width - entry_price) * SPX_MULTIPLIER * quantity
    else:
        return entry_price * SPX_MULTIPLIER * quantity
