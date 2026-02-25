"""Portfolio Greeks aggregation — net exposure across all open positions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from sim.config import SPX_MULTIPLIER
from sim.data.chain_snapshot import ChainSnapshot
from sim.engine.order import Action
from sim.engine.position import SpreadPosition


@dataclass
class PortfolioGreeks:
    """Aggregate Greeks across all open positions."""
    net_delta: float = 0.0
    net_gamma: float = 0.0
    net_theta: float = 0.0
    net_vega: float = 0.0

    def __str__(self) -> str:
        return (f"Δ={self.net_delta:+.2f} Γ={self.net_gamma:+.4f} "
                f"Θ={self.net_theta:+.2f} ν={self.net_vega:+.2f}")


def aggregate_greeks(positions: List[SpreadPosition],
                     chain: ChainSnapshot,
                     expiration=None) -> PortfolioGreeks:
    """Calculate aggregate portfolio Greeks from all open positions.

    Args:
        positions: All open positions for an agent.
        chain: Current chain snapshot for Greek lookups.
        expiration: Filter contracts by expiration (optional).

    Returns:
        PortfolioGreeks with net exposure.
    """
    greeks = PortfolioGreeks()

    for pos in positions:
        if not pos.is_open:
            continue

        for leg in pos.legs:
            contract = chain.get_contract(leg.strike, leg.put_call, expiration)
            if contract is None:
                continue

            # Sign: +1 for long, -1 for short
            sign = 1.0 if leg.action == Action.BUY else -1.0
            qty = leg.quantity * pos.quantity * sign

            greeks.net_delta += contract.delta * qty
            greeks.net_gamma += contract.gamma * qty
            greeks.net_theta += contract.theta * qty
            greeks.net_vega += contract.vega * qty

    # Round for readability
    greeks.net_delta = round(greeks.net_delta, 4)
    greeks.net_gamma = round(greeks.net_gamma, 6)
    greeks.net_theta = round(greeks.net_theta, 4)
    greeks.net_vega = round(greeks.net_vega, 4)

    return greeks
