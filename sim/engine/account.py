"""Simulated trading account — balance, positions, buying power, P&L."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from sim.config import STARTING_CAPITAL
from sim.engine.margin import buying_power_required, max_loss
from sim.engine.order import Order
from sim.engine.position import SpreadPosition


@dataclass
class Account:
    """A simulated trading account for one agent."""

    agent_id: str
    balance: float = STARTING_CAPITAL
    realized_pnl: float = 0.0
    total_commissions: float = 0.0
    positions: List[SpreadPosition] = field(default_factory=list)

    @property
    def open_positions(self) -> List[SpreadPosition]:
        return [p for p in self.positions if p.is_open]

    @property
    def settled_positions(self) -> List[SpreadPosition]:
        return [p for p in self.positions if p.is_settled]

    @property
    def open_position_count(self) -> int:
        return len(self.open_positions)

    @property
    def buying_power_used(self) -> float:
        """Total buying power held by open positions."""
        total = 0.0
        for pos in self.open_positions:
            # Create a pseudo-order to compute max_loss
            pseudo = Order(
                agent_id=pos.agent_id,
                structure=pos.structure,
                legs=pos.legs,
                quantity=pos.quantity,
            )
            total += max_loss(pseudo, pos.entry_price)
        return total

    @property
    def buying_power_available(self) -> float:
        return max(0.0, self.balance - self.buying_power_used)

    @property
    def net_liquidation(self) -> float:
        """Net liquidation value = balance (realized P&L already reflected)."""
        return self.balance

    def add_position(self, position: SpreadPosition) -> None:
        """Add a new filled position and deduct commission from balance."""
        self.positions.append(position)
        self.total_commissions += position.commission
        self.balance -= position.commission

    def settle_position(self, position: SpreadPosition, session_id: int,
                        spx_close: float) -> float:
        """Settle an expiring position. Returns realized P&L.

        The position.settle() method computes the P&L including commission.
        We add the gross P&L to balance (commission already deducted at entry).
        """
        # Compute P&L from entry to settlement (commission already subtracted at open)
        from sim.engine.payoff import settlement_pnl
        gross_pnl = settlement_pnl(
            position.entry_price, position.side, position.legs,
            spx_close, position.quantity
        )

        position.session_settled = session_id
        position.settlement_price = spx_close
        position.realized_pnl = gross_pnl  # commission already deducted from balance

        self.balance += gross_pnl
        self.realized_pnl += gross_pnl

        return gross_pnl

    def book_settlement(self, position: SpreadPosition) -> float:
        """Book the P&L for an already-settled position.

        Use this when the settlement was performed by settle_0dte_positions()
        or settle_1dte_positions() externally, and the position already has
        its realized_pnl computed via position.settle().

        Note: position.realized_pnl = gross_pnl - commission (set by
        position.settle()). But commission was already deducted from balance
        at add_position() time. So we add back (realized_pnl + commission)
        = gross_pnl to avoid double-counting.

        Returns gross P&L added to balance.
        """
        gross_pnl = (position.realized_pnl or 0.0) + position.commission
        self.balance += gross_pnl
        self.realized_pnl += gross_pnl
        return gross_pnl

    def accrue_risk_free(self, daily_rate: float) -> float:
        """Accrue risk-free interest on uninvested cash. Returns interest earned."""
        # Interest accrues on available buying power (not tied up in positions)
        interest = self.buying_power_available * daily_rate
        self.balance += interest
        return interest

    def to_dict(self) -> dict:
        """Serialize account state for agent context."""
        return {
            "balance": round(self.balance, 2),
            "buying_power_available": round(self.buying_power_available, 2),
            "buying_power_used": round(self.buying_power_used, 2),
            "open_positions": self.open_position_count,
            "realized_pnl": round(self.realized_pnl, 2),
            "total_commissions": round(self.total_commissions, 2),
            "net_liquidation": round(self.net_liquidation, 2),
            "starting_capital": STARTING_CAPITAL,
            "return_pct": round((self.balance - STARTING_CAPITAL) / STARTING_CAPITAL * 100, 2),
        }
