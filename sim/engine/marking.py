"""Mark-to-market calculation — single source of truth for position valuation.

Mark price = mid of spread NBBO. Used for unrealized P&L and observation.
NOT used for execution (fills use executable price + slippage) or
settlement (uses intrinsic value at SPX close).
"""

from __future__ import annotations

from typing import Optional, Tuple

from sim.data.chain_snapshot import ChainSnapshot
from sim.engine.order import Action, Leg, Order, Side, StructureType


def spread_nbbo(legs: list[Leg], chain: ChainSnapshot,
                expiration=None) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """Compute spread NBBO from individual leg bids/asks.

    Returns:
        (bid, ask, mid) — bid is what you could sell/close for,
        ask is what you could buy/open for. Mid is the mark.
        Returns (None, None, None) if any leg is missing data.
    """
    # Collect each leg's bid/ask
    total_sell_bid = 0.0   # sum of bid for legs we're selling
    total_buy_ask = 0.0    # sum of ask for legs we're buying
    total_sell_ask = 0.0   # sum of ask for legs we're selling
    total_buy_bid = 0.0    # sum of bid for legs we're buying

    for leg in legs:
        contract = chain.get_contract(leg.strike, leg.put_call, expiration)
        if contract is None or contract.bid is None or contract.ask is None:
            return (None, None, None)

        leg_qty = leg.quantity  # typically 1, but 2 for butterfly center

        if leg.action == Action.SELL:
            total_sell_bid += contract.bid * leg_qty
            total_sell_ask += contract.ask * leg_qty
        else:  # BUY
            total_buy_ask += contract.ask * leg_qty
            total_buy_bid += contract.bid * leg_qty

    # Spread NBBO:
    # bid = what we could close/sell the spread for = sell_bid - buy_ask
    # ask = what we could open/buy the spread for = sell_ask - buy_bid
    bid = total_sell_bid - total_buy_ask
    ask = total_sell_ask - total_buy_bid
    mid = (bid + ask) / 2.0

    return (round(bid, 2), round(ask, 2), round(mid, 2))


def mark_position(entry_price: float, current_mark: float,
                  side: Side, quantity: int) -> float:
    """Calculate unrealized P&L at the current mark.

    Args:
        entry_price: Credit received (positive) or debit paid (positive).
        current_mark: Current mid of spread NBBO.
        side: CREDIT or DEBIT.
        quantity: Number of spreads.

    Returns:
        Unrealized P&L in dollars. Positive = profitable.
    """
    multiplier = 100  # SPX options multiplier

    if side == Side.CREDIT:
        # Credit spread: profit when mark decreases (spread cheapens)
        # P&L = (entry_credit - current_mark) * multiplier * qty
        return (entry_price - current_mark) * multiplier * quantity
    else:
        # Debit spread: profit when mark increases (spread appreciates)
        # P&L = (current_mark - entry_debit) * multiplier * qty
        return (current_mark - entry_price) * multiplier * quantity
