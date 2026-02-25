"""Margin and max-loss calculator.

Single source of truth for buying power requirements and risk calculations.
All structures are 5-wide ($5 between strikes = $500 max width per spread).
"""

from sim.config import SPX_MULTIPLIER
from sim.engine.order import Order, StructureType, Side


def max_loss(order: Order, fill_price: float) -> float:
    """Calculate maximum possible loss for a filled order.

    Args:
        order: The order with structure type and legs.
        fill_price: The actual fill price (credit received or debit paid).

    Returns:
        Max loss in dollars (positive number).
    """
    width = order.width
    qty = order.quantity

    if order.side == Side.CREDIT:
        if order.structure in (StructureType.IRON_CONDOR, StructureType.IRON_FLY):
            # IC/fly: max loss = worse side's (width - credit).
            # At expiration, only one side can be ITM.
            # For a balanced IC, max loss = width - total_credit.
            # For asymmetric: max(put_side_risk, call_side_risk).
            # With 5-wide on both sides: max_loss = width - fill_price
            return (width - fill_price) * SPX_MULTIPLIER * qty
        else:
            # Credit vertical: max loss = (width - credit)
            return (width - fill_price) * SPX_MULTIPLIER * qty
    else:
        # Debit structure (butterfly): max loss = debit paid
        return fill_price * SPX_MULTIPLIER * qty


def buying_power_required(order: Order, fill_price: float) -> float:
    """Buying power held as collateral = max loss."""
    return max_loss(order, fill_price)


def is_cash_secured_required(account_balance: float) -> bool:
    """Below $25K, cash-secured only (no portfolio margin benefit).

    In practice this means the same max-loss formula but the agent
    gets no leverage benefit. The impact is already captured by
    buying_power_required() since we always use max-loss.
    """
    return account_balance < 25_000.0
