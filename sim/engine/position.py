"""Spread position lifecycle: open → carry overnight → settle at expiration."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import List, Optional
import uuid

from sim.engine.order import Leg, Order, Side, StructureType


@dataclass
class SpreadPosition:
    """A filled spread position tracking its lifecycle across sessions."""

    position_id: str
    agent_id: str
    track: str
    session_opened: int           # session ID when opened
    structure: StructureType
    side: Side
    legs: List[Leg]
    quantity: int
    entry_price: float            # credit received or debit paid
    commission: float             # entry commission
    width: float                  # spread width in points
    window: str = ""              # "open" or "close5"
    dte_at_entry: int = 0         # 0 for 0DTE (same-day settle), 1 for 1DTE (next-day)
    expiration: str = ""          # ISO date of the expiration being traded

    # Settlement fields — filled when position expires
    session_settled: Optional[int] = None
    settlement_price: Optional[float] = None   # SPX close at expiration
    settlement_value: Optional[float] = None   # intrinsic value of spread
    settlement_source: str = ""                # "official_close" | "close5_mark" | etc.
    realized_pnl: Optional[float] = None

    @property
    def is_open(self) -> bool:
        return self.session_settled is None

    @property
    def is_settled(self) -> bool:
        return self.session_settled is not None

    @classmethod
    def from_filled_order(cls, order: Order, session_id: int, track: str,
                          fill_price: float, commission: float) -> "SpreadPosition":
        """Create a position from a filled order."""
        return cls(
            position_id=str(uuid.uuid4())[:8],
            agent_id=order.agent_id,
            track=track,
            session_opened=session_id,
            structure=order.structure,
            side=order.side,
            legs=list(order.legs),
            quantity=order.quantity,
            entry_price=fill_price,
            commission=commission,
            width=order.width,
            window=order.window,
            dte_at_entry=order.dte_at_entry,
            expiration=order.expiration,
        )

    def settle(self, session_id: int, spx_close: float) -> float:
        """Cash-settle the position at expiration.

        Args:
            session_id: The session when settlement occurs.
            spx_close: SPX closing price at expiration.

        Returns:
            Realized P&L in dollars (including commission).
        """
        from sim.engine.payoff import settlement_pnl, spread_settlement_value

        self.session_settled = session_id
        self.settlement_price = spx_close
        self.settlement_value = spread_settlement_value(self.legs, spx_close)
        self.realized_pnl = settlement_pnl(
            self.entry_price, self.side, self.legs, spx_close, self.quantity
        ) - self.commission

        return self.realized_pnl
