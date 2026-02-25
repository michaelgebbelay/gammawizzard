"""Risk limit validation — enforced before every order fill."""

from __future__ import annotations

from typing import List, Optional, Tuple

from sim.config import (
    MAX_ACCOUNT_RISK_PCT,
    MAX_CONCURRENT_SPREADS,
    MAX_RISK_PER_TRADE_PCT,
    MIN_BP_RESERVE_PCT,
)
from sim.engine.margin import max_loss
from sim.engine.order import Order
from sim.engine.position import SpreadPosition


def validate_order(order: Order, fill_price: float,
                   account_balance: float, buying_power_used: float,
                   open_positions: List[SpreadPosition]) -> Tuple[bool, str]:
    """Validate an order against all risk limits.

    Args:
        order: The order to validate.
        fill_price: Expected fill price (credit or debit).
        account_balance: Current account balance.
        buying_power_used: Buying power currently held by open positions.
        open_positions: All open positions for this agent.

    Returns:
        (is_valid, rejection_reason) — True with empty string if valid.
    """
    # 1. Concurrent spread limit
    open_count = sum(1 for p in open_positions if p.is_open)
    if open_count >= MAX_CONCURRENT_SPREADS:
        return (False, f"Max concurrent spreads ({MAX_CONCURRENT_SPREADS}) reached")

    # 2. Per-trade risk limit
    trade_risk = max_loss(order, fill_price)
    max_per_trade = account_balance * MAX_RISK_PER_TRADE_PCT
    if trade_risk > max_per_trade:
        return (False, f"Trade risk ${trade_risk:.0f} exceeds {MAX_RISK_PER_TRADE_PCT:.0%} limit ${max_per_trade:.0f}")

    # 3. Total account risk limit
    current_risk = sum(
        max_loss(
            _position_to_pseudo_order(p),
            p.entry_price
        )
        for p in open_positions if p.is_open
    )
    new_total_risk = current_risk + trade_risk
    max_account_risk = account_balance * MAX_ACCOUNT_RISK_PCT
    if new_total_risk > max_account_risk:
        return (False, f"Total risk ${new_total_risk:.0f} would exceed {MAX_ACCOUNT_RISK_PCT:.0%} limit ${max_account_risk:.0f}")

    # 4. Buying power reserve
    total_buying_power = account_balance
    bp_after_trade = total_buying_power - buying_power_used - trade_risk
    min_reserve = total_buying_power * MIN_BP_RESERVE_PCT
    if bp_after_trade < min_reserve:
        return (False, f"Insufficient buying power reserve. After trade: ${bp_after_trade:.0f}, minimum: ${min_reserve:.0f}")

    return (True, "")


def _position_to_pseudo_order(pos: SpreadPosition) -> Order:
    """Create a minimal Order object from a position for max_loss calculation."""
    return Order(
        agent_id=pos.agent_id,
        structure=pos.structure,
        legs=pos.legs,
        quantity=pos.quantity,
    )
