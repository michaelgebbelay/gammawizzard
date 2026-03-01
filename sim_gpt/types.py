"""Core datatypes for the live binary-vertical game."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from typing import Optional

from sim_gpt.config import ALLOWED_WIDTHS, MAX_CONTRACTS, TARGET_DELTAS


class SideAction(str, Enum):
    BUY = "buy"
    SELL = "sell"
    NONE = "none"


@dataclass(frozen=True)
class PublicSnapshot:
    signal_date: date
    tdate: date
    spx: float
    vix: float
    vix_one: float
    rv: float
    rv5: float
    rv10: float
    rv20: float
    r: float
    rx: float
    forward: float
    payload: dict


@dataclass(frozen=True)
class SettlementSnapshot:
    settlement_date: date
    settlement_spx: float


@dataclass(frozen=True)
class OptionQuote:
    strike: float
    put_call: str  # "P" | "C"
    bid: float
    ask: float
    mid: float
    delta: float
    iv: float


@dataclass(frozen=True)
class ChainSnapshot:
    signal_date: date
    tdate: date
    asof_utc: datetime
    underlying_spx: float
    puts: tuple[OptionQuote, ...]
    calls: tuple[OptionQuote, ...]


@dataclass
class Decision:
    put_action: SideAction = SideAction.NONE
    call_action: SideAction = SideAction.NONE
    put_width: Optional[int] = None
    call_width: Optional[int] = None
    put_target_delta: Optional[float] = None
    call_target_delta: Optional[float] = None
    size: int = 1
    thesis: str = ""
    template_id: str = "custom"

    def active_sides(self) -> int:
        total = 0
        if self.put_action != SideAction.NONE:
            total += 1
        if self.call_action != SideAction.NONE:
            total += 1
        return total

    def validate(self) -> tuple[bool, str]:
        if self.size < 1 or self.size > MAX_CONTRACTS:
            return False, f"size must be in [1, {MAX_CONTRACTS}]"

        if self.put_action == SideAction.NONE:
            if self.put_width is not None:
                return False, "put_width must be null when put_action is none"
            if self.put_target_delta is not None:
                return False, "put_target_delta must be null when put_action is none"
        else:
            if self.put_width not in ALLOWED_WIDTHS:
                return False, f"put_width must be one of {sorted(ALLOWED_WIDTHS)}"
            if self.put_target_delta is None:
                return False, "put_target_delta is required when put_action is active"
            if float(self.put_target_delta) not in TARGET_DELTAS:
                return False, f"put_target_delta must be one of {list(TARGET_DELTAS)}"

        if self.call_action == SideAction.NONE:
            if self.call_width is not None:
                return False, "call_width must be null when call_action is none"
            if self.call_target_delta is not None:
                return False, "call_target_delta must be null when call_action is none"
        else:
            if self.call_width not in ALLOWED_WIDTHS:
                return False, f"call_width must be one of {sorted(ALLOWED_WIDTHS)}"
            if self.call_target_delta is None:
                return False, "call_target_delta is required when call_action is active"
            if float(self.call_target_delta) not in TARGET_DELTAS:
                return False, f"call_target_delta must be one of {list(TARGET_DELTAS)}"

        if len(self.thesis) > 300:
            return False, "thesis must be <= 300 chars"

        return True, ""

    def to_dict(self) -> dict:
        return {
            "put_action": self.put_action.value,
            "call_action": self.call_action.value,
            "put_width": self.put_width,
            "call_width": self.call_width,
            "put_target_delta": self.put_target_delta,
            "call_target_delta": self.call_target_delta,
            "size": self.size,
            "thesis": self.thesis,
            "template_id": self.template_id,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Decision":
        put_td = data.get("put_target_delta")
        call_td = data.get("call_target_delta")
        return cls(
            put_action=SideAction(data.get("put_action", SideAction.NONE.value)),
            call_action=SideAction(data.get("call_action", SideAction.NONE.value)),
            put_width=data.get("put_width"),
            call_width=data.get("call_width"),
            put_target_delta=(None if put_td in (None, "") else float(put_td)),
            call_target_delta=(None if call_td in (None, "") else float(call_td)),
            size=int(data.get("size", 1)),
            thesis=str(data.get("thesis", "")),
            template_id=str(data.get("template_id", "custom")),
        )


def build_decision(
    put_action: SideAction,
    call_action: SideAction,
    width: int = 5,
    target_delta: float = 0.16,
    size: int = 1,
    thesis: str = "",
    template_id: str = "custom",
) -> Decision:
    put_width = width if put_action != SideAction.NONE else None
    call_width = width if call_action != SideAction.NONE else None
    put_target_delta = target_delta if put_action != SideAction.NONE else None
    call_target_delta = target_delta if call_action != SideAction.NONE else None
    return Decision(
        put_action=put_action,
        call_action=call_action,
        put_width=put_width,
        call_width=call_width,
        put_target_delta=put_target_delta,
        call_target_delta=call_target_delta,
        size=size,
        thesis=thesis,
        template_id=template_id,
    )
