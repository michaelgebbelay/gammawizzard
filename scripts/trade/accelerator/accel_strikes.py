#!/usr/bin/env python3
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, List

@dataclass
class Leg:
    occ: str
    right: str            # "CALL" | "PUT"
    strike: float
    delta: float
    bid: float
    ask: float

def pick_short_leg(legs: List[Leg], right: str, delta_floor: float = 0.45, delta_ceiling: float = 0.50) -> Optional[Leg]:
    """Pick |Δ| in [0.45, 0.50], nearest to 0.50 on the requested side."""
    cands = [l for l in legs if l.right == right and abs(l.delta) >= delta_floor and abs(l.delta) <= delta_ceiling]
    if not cands:
        return None
    cands.sort(key=lambda l: abs(abs(l.delta) - 0.50))
    return cands[0]

def mate_long_leg(short_leg: Leg, all_legs: List[Leg], width_points: int = 5) -> Optional[Leg]:
    """Find the same-right leg exactly width_points away."""
    target_k = short_leg.strike - width_points if short_leg.right == "PUT" else short_leg.strike + width_points
    for l in all_legs:
        if l.right == short_leg.right and abs(l.strike - target_k) < 1e-6:
            return l
    return None

def vertical_nbbo_mid(short: Leg, long: Leg) -> float:
    """Synthetic mid for credit spread: (short_mid - long_mid)."""
    short_mid = (short.bid + short.ask) / 2.0
    long_mid  = (long.bid  + long.ask)  / 2.0
    return round(short_mid - long_mid, 2)
