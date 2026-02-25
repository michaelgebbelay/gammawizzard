"""Order and leg dataclasses for the paper trading engine."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional
import uuid


class StructureType(str, Enum):
    BULL_PUT_VERTICAL = "bull_put_vertical"
    BEAR_CALL_VERTICAL = "bear_call_vertical"
    IRON_CONDOR = "iron_condor"
    IRON_FLY = "iron_fly"
    CALL_BUTTERFLY = "call_butterfly"
    PUT_BUTTERFLY = "put_butterfly"


class Side(str, Enum):
    CREDIT = "credit"
    DEBIT = "debit"


class Action(str, Enum):
    BUY = "buy"
    SELL = "sell"


class OrderStatus(str, Enum):
    PENDING = "pending"
    FILLED = "filled"
    REJECTED = "rejected"


@dataclass
class Leg:
    """A single option leg in a spread order."""
    strike: float
    put_call: str       # "C" or "P"
    action: Action      # buy or sell
    quantity: int = 1


@dataclass
class Order:
    """A spread order submitted by an agent or baseline."""
    agent_id: str
    structure: StructureType
    legs: List[Leg]
    quantity: int
    limit_price: Optional[float] = None
    thesis: str = ""
    window: str = ""              # "open" or "close5" — which decision window
    dte_at_entry: int = 0         # 0 for 0DTE (OPEN window), 1 for 1DTE (CLOSE+5)
    expiration: str = ""          # ISO date of the expiration being traded

    order_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    status: OrderStatus = OrderStatus.PENDING
    fill_price: Optional[float] = None
    commission: float = 0.0
    rejection_reason: str = ""

    @property
    def side(self) -> Side:
        """Determine if this is a credit or debit structure based on legs."""
        # Credit: net seller (more sells than buys, or sell higher premium)
        # For verticals: sell closer to ATM = credit
        # For ICs: always credit
        # For butterflies: always debit
        if self.structure in (StructureType.CALL_BUTTERFLY, StructureType.PUT_BUTTERFLY):
            return Side.DEBIT
        return Side.CREDIT

    @property
    def num_legs(self) -> int:
        return len(self.legs)

    def sell_legs(self) -> List[Leg]:
        return [l for l in self.legs if l.action == Action.SELL]

    def buy_legs(self) -> List[Leg]:
        return [l for l in self.legs if l.action == Action.BUY]

    @property
    def width(self) -> float:
        """Spread width in points. For ICs, returns the width of one side."""
        strikes = sorted(set(l.strike for l in self.legs))
        if len(strikes) < 2:
            return 0.0
        if self.structure in (StructureType.IRON_CONDOR, StructureType.IRON_FLY):
            # Width of one side (put side or call side — should be equal at 5-wide)
            puts = sorted(l.strike for l in self.legs if l.put_call == "P")
            if len(puts) >= 2:
                return puts[-1] - puts[0]
            calls = sorted(l.strike for l in self.legs if l.put_call == "C")
            if len(calls) >= 2:
                return calls[-1] - calls[0]
        if self.structure in (StructureType.CALL_BUTTERFLY, StructureType.PUT_BUTTERFLY):
            return strikes[-1] - strikes[0]  # full butterfly width
        return strikes[-1] - strikes[0]


def make_bull_put_vertical(agent_id: str, short_strike: float, long_strike: float,
                           qty: int, limit_price: Optional[float] = None,
                           thesis: str = "") -> Order:
    """Create a bull put credit spread (sell higher put, buy lower put)."""
    return Order(
        agent_id=agent_id,
        structure=StructureType.BULL_PUT_VERTICAL,
        legs=[
            Leg(strike=short_strike, put_call="P", action=Action.SELL),
            Leg(strike=long_strike, put_call="P", action=Action.BUY),
        ],
        quantity=qty,
        limit_price=limit_price,
        thesis=thesis,
    )


def make_bear_call_vertical(agent_id: str, short_strike: float, long_strike: float,
                            qty: int, limit_price: Optional[float] = None,
                            thesis: str = "") -> Order:
    """Create a bear call credit spread (sell lower call, buy higher call)."""
    return Order(
        agent_id=agent_id,
        structure=StructureType.BEAR_CALL_VERTICAL,
        legs=[
            Leg(strike=short_strike, put_call="C", action=Action.SELL),
            Leg(strike=long_strike, put_call="C", action=Action.BUY),
        ],
        quantity=qty,
        limit_price=limit_price,
        thesis=thesis,
    )


def make_iron_condor(agent_id: str, put_long: float, put_short: float,
                     call_short: float, call_long: float, qty: int,
                     limit_price: Optional[float] = None,
                     thesis: str = "") -> Order:
    """Create an iron condor (sell both sides)."""
    return Order(
        agent_id=agent_id,
        structure=StructureType.IRON_CONDOR,
        legs=[
            Leg(strike=put_long, put_call="P", action=Action.BUY),
            Leg(strike=put_short, put_call="P", action=Action.SELL),
            Leg(strike=call_short, put_call="C", action=Action.SELL),
            Leg(strike=call_long, put_call="C", action=Action.BUY),
        ],
        quantity=qty,
        limit_price=limit_price,
        thesis=thesis,
    )


def make_iron_fly(agent_id: str, center_strike: float, wing_width: float,
                  qty: int, limit_price: Optional[float] = None,
                  thesis: str = "") -> Order:
    """Create an iron fly (sell ATM straddle, buy wings)."""
    return Order(
        agent_id=agent_id,
        structure=StructureType.IRON_FLY,
        legs=[
            Leg(strike=center_strike - wing_width, put_call="P", action=Action.BUY),
            Leg(strike=center_strike, put_call="P", action=Action.SELL),
            Leg(strike=center_strike, put_call="C", action=Action.SELL),
            Leg(strike=center_strike + wing_width, put_call="C", action=Action.BUY),
        ],
        quantity=qty,
        limit_price=limit_price,
        thesis=thesis,
    )


def make_call_butterfly(agent_id: str, lower: float, center: float, upper: float,
                        qty: int, limit_price: Optional[float] = None,
                        thesis: str = "") -> Order:
    """Create a call butterfly (buy 1 lower, sell 2 center, buy 1 upper)."""
    return Order(
        agent_id=agent_id,
        structure=StructureType.CALL_BUTTERFLY,
        legs=[
            Leg(strike=lower, put_call="C", action=Action.BUY),
            Leg(strike=center, put_call="C", action=Action.SELL, quantity=2),
            Leg(strike=upper, put_call="C", action=Action.BUY),
        ],
        quantity=qty,
        limit_price=limit_price,
        thesis=thesis,
    )


def make_put_butterfly(agent_id: str, lower: float, center: float, upper: float,
                       qty: int, limit_price: Optional[float] = None,
                       thesis: str = "") -> Order:
    """Create a put butterfly (buy 1 lower, sell 2 center, buy 1 upper)."""
    return Order(
        agent_id=agent_id,
        structure=StructureType.PUT_BUTTERFLY,
        legs=[
            Leg(strike=lower, put_call="P", action=Action.BUY),
            Leg(strike=center, put_call="P", action=Action.SELL, quantity=2),
            Leg(strike=upper, put_call="P", action=Action.BUY),
        ],
        quantity=qty,
        limit_price=limit_price,
        thesis=thesis,
    )
