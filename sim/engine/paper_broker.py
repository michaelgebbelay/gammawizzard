"""Paper broker — validates orders, computes fills with slippage, updates accounts."""

from __future__ import annotations

import hashlib
import random
from dataclasses import dataclass
from typing import Optional

from sim.config import MAX_ORDERS_PER_WINDOW
from sim.data.chain_snapshot import ChainSnapshot
from sim.engine.account import Account
from sim.engine.commissions import calculate_commission
from sim.engine.marking import spread_nbbo
from sim.engine.order import Order, OrderStatus, Side
from sim.engine.position import SpreadPosition
from sim.engine.risk_limits import validate_order
from sim.engine.slippage import compute_slippage


@dataclass
class FillResult:
    """Result of attempting to fill an order."""
    order: Order
    filled: bool
    fill_price: float = 0.0
    commission: float = 0.0
    slippage_applied: float = 0.0
    rejection_reason: str = ""
    position: Optional[SpreadPosition] = None


class PaperBroker:
    """Simulated broker that fills orders against chain data."""

    def __init__(self, rng_seed: int = 42):
        self.base_seed = rng_seed
        self.rng = random.Random(rng_seed)

    def _window_rng(self, session_id: int, window: str, agent_id: str) -> random.Random:
        """Create a deterministic RNG for a specific (session, window, agent) triple."""
        key = f"{self.base_seed}:{session_id}:{window}:{agent_id}"
        seed = int(hashlib.sha256(key.encode()).hexdigest()[:16], 16)
        return random.Random(seed)

    def submit_order(self, order: Order, account: Account,
                     chain: ChainSnapshot, track: str,
                     session_id: int,
                     intraday_move_pts: float = 0.0) -> FillResult:
        """Attempt to fill an order.

        Steps:
        1. Compute spread NBBO from chain
        2. Compute slippage
        3. Determine fill price (NBBO + slippage)
        4. Validate against risk limits
        5. If valid, calculate commission, create position, update account

        Args:
            order: The order to fill.
            account: The agent's account.
            chain: Current chain snapshot.
            track: Experimental track ("adaptive", "frozen", "clean").
            session_id: Current session ID.
            intraday_move_pts: SPX move since open (for slippage calculation).

        Returns:
            FillResult with fill details or rejection reason.
        """
        # 0. One order per participant per window
        if order.window:
            window_fills = sum(
                1 for p in account.open_positions
                if p.is_open and p.window == order.window
                and p.session_opened == session_id
            )
            if window_fills >= MAX_ORDERS_PER_WINDOW:
                order.status = OrderStatus.REJECTED
                order.rejection_reason = (
                    f"Already filled {window_fills} order(s) in {order.window} window "
                    f"(max {MAX_ORDERS_PER_WINDOW})"
                )
                return FillResult(order=order, filled=False,
                                  rejection_reason=order.rejection_reason)

        # 1. Compute spread NBBO
        expiration = chain.expirations[0] if chain.expirations else None
        bid, ask, mid = spread_nbbo(order.legs, chain, expiration)

        if bid is None or ask is None or mid is None:
            order.status = OrderStatus.REJECTED
            order.rejection_reason = "Missing quotes for one or more legs"
            return FillResult(order=order, filled=False,
                              rejection_reason=order.rejection_reason)

        # 2. Compute slippage (deterministic per session/window/agent)
        rng = self._window_rng(session_id, order.window or "", order.agent_id) if order.window else self.rng
        slippage = compute_slippage(chain.vix, intraday_move_pts, rng)

        # 3. Determine fill price
        if order.side == Side.CREDIT:
            # Credit: we sell the spread. Natural fill = bid. With slippage, worse = less credit.
            executable_price = bid
            fill_price = max(0.01, executable_price - slippage)
        else:
            # Debit: we buy the spread. Natural fill = ask. With slippage, worse = more debit.
            executable_price = ask
            fill_price = executable_price + slippage

        fill_price = round(fill_price, 2)

        # Check limit price
        if order.limit_price is not None:
            if order.side == Side.CREDIT and fill_price < order.limit_price:
                order.status = OrderStatus.REJECTED
                order.rejection_reason = (
                    f"Limit {order.limit_price:.2f} not achievable. "
                    f"Best fill: {fill_price:.2f} (NBBO bid={bid:.2f}, slippage={slippage:.2f})"
                )
                return FillResult(order=order, filled=False,
                                  rejection_reason=order.rejection_reason)
            if order.side == Side.DEBIT and fill_price > order.limit_price:
                order.status = OrderStatus.REJECTED
                order.rejection_reason = (
                    f"Limit {order.limit_price:.2f} not achievable. "
                    f"Best fill: {fill_price:.2f} (NBBO ask={ask:.2f}, slippage={slippage:.2f})"
                )
                return FillResult(order=order, filled=False,
                                  rejection_reason=order.rejection_reason)

        # 4. Validate risk limits
        is_valid, reject_reason = validate_order(
            order, fill_price, account.balance,
            account.buying_power_used, account.open_positions,
        )
        if not is_valid:
            order.status = OrderStatus.REJECTED
            order.rejection_reason = reject_reason
            return FillResult(order=order, filled=False,
                              rejection_reason=reject_reason)

        # 5. Calculate commission and create position
        commission = calculate_commission(order)

        position = SpreadPosition.from_filled_order(
            order, session_id, track, fill_price, commission
        )

        # 6. Update account
        account.add_position(position)

        # 7. Update order status
        order.status = OrderStatus.FILLED
        order.fill_price = fill_price
        order.commission = commission

        return FillResult(
            order=order,
            filled=True,
            fill_price=fill_price,
            commission=commission,
            slippage_applied=slippage,
            position=position,
        )
