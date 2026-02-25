"""Commission calculator — entry only (cash settlement has no closing transaction)."""

from sim.config import COMMISSION_PER_LEG, SEC_TAF_PER_CONTRACT
from sim.engine.order import Order


def calculate_commission(order: Order) -> float:
    """Calculate entry commission for an order.

    Formula: (per_leg_cost × num_unique_legs + sec_taf × num_unique_legs) × quantity

    For a butterfly, the center leg has quantity=2 in the Leg dataclass,
    but commission is per contract, so we count total contracts.
    """
    total_contracts = sum(leg.quantity for leg in order.legs) * order.quantity
    cost = (COMMISSION_PER_LEG + SEC_TAF_PER_CONTRACT) * total_contracts
    return round(cost, 2)
