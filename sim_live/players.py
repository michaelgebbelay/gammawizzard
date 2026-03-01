"""Player implementations for the live binary-vertical game."""

from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass
from datetime import date
from typing import Iterable

from sim_live.types import Decision, PublicSnapshot, SideAction, build_decision


def _vix1d_pct(s: PublicSnapshot) -> float:
    # Leo files usually provide decimals (0.1654 => 16.54%).
    return s.vix_one * 100.0 if s.vix_one < 1.0 else s.vix_one


def _vix_bucket(s: PublicSnapshot) -> str:
    v = _vix1d_pct(s)
    if v < 12:
        return "low"
    if v < 20:
        return "mid"
    return "high"


def _trend_bucket(s: PublicSnapshot) -> str:
    if s.r > 0.002:
        return "up"
    if s.r < -0.002:
        return "down"
    return "flat"


def _context_key(s: PublicSnapshot) -> str:
    return f"{_vix_bucket(s)}|{_trend_bucket(s)}"


def _deterministic_noise(seed: str) -> float:
    h = hashlib.sha256(seed.encode()).hexdigest()
    val = int(h[:8], 16) / 0xFFFFFFFF
    return (val - 0.5) * 0.08


def _template_pool() -> dict[str, Decision]:
    return {
        "flat": build_decision(SideAction.NONE, SideAction.NONE, thesis="No edge today.", template_id="flat"),
        "put_sell_5": build_decision(SideAction.SELL, SideAction.NONE, width=5, thesis="Short put side.", template_id="put_sell_5"),
        "call_sell_5": build_decision(SideAction.NONE, SideAction.SELL, width=5, thesis="Short call side.", template_id="call_sell_5"),
        "put_buy_5": build_decision(SideAction.BUY, SideAction.NONE, width=5, thesis="Long put side.", template_id="put_buy_5"),
        "call_buy_5": build_decision(SideAction.NONE, SideAction.BUY, width=5, thesis="Long call side.", template_id="call_buy_5"),
        "both_sell_5": build_decision(SideAction.SELL, SideAction.SELL, width=5, thesis="Two-sided short 5w.", template_id="both_sell_5"),
        "both_buy_5": build_decision(SideAction.BUY, SideAction.BUY, width=5, thesis="Two-sided long 5w.", template_id="both_buy_5"),
        "both_sell_10": build_decision(SideAction.SELL, SideAction.SELL, width=10, thesis="Two-sided short 10w.", template_id="both_sell_10"),
        "both_buy_10": build_decision(SideAction.BUY, SideAction.BUY, width=10, thesis="Two-sided long 10w.", template_id="both_buy_10"),
    }


TEMPLATES = _template_pool()


@dataclass
class Player:
    player_id: str
    persona: str

    def decide(self, snapshot: PublicSnapshot, state: dict) -> Decision:
        raise NotImplementedError

    def _score_template(
        self,
        snapshot: PublicSnapshot,
        state: dict,
        template_id: str,
        base_score: float,
    ) -> float:
        ctx = _context_key(snapshot)
        hist = state.get("q", {}).get(ctx, {}).get(template_id, {})
        avg = float(hist.get("avg", 0.0))
        n = int(hist.get("n", 0))
        explore = 0.2 / math.sqrt(n + 1)
        noise = _deterministic_noise(f"{snapshot.signal_date}|{self.player_id}|{template_id}")
        # Base preference + online memory + small exploration + tiny tie-break noise.
        return base_score + 0.35 * avg + explore + noise

    def update_state(
        self,
        state: dict,
        snapshot: PublicSnapshot,
        template_id: str,
        pnl: float,
    ) -> dict:
        ctx = _context_key(snapshot)
        q = state.setdefault("q", {})
        q_ctx = q.setdefault(ctx, {})
        item = q_ctx.setdefault(template_id, {"n": 0, "avg": 0.0})
        n = int(item["n"]) + 1
        avg = float(item["avg"])
        item["avg"] = avg + (pnl - avg) / n
        item["n"] = n
        return state


class RegimePlayer(Player):
    def __init__(self):
        super().__init__("player-regime", "Volatility regime trader.")

    def decide(self, snapshot: PublicSnapshot, state: dict) -> Decision:
        v = _vix_bucket(snapshot)
        base = {
            "flat": 0.2,
            "put_sell_5": 0.3,
            "call_sell_5": 0.3,
            "put_buy_5": -0.1,
            "call_buy_5": -0.1,
            "both_sell_5": 0.6 if v in {"low", "mid"} else 0.1,
            "both_buy_5": 0.5 if v == "high" else -0.2,
            "both_sell_10": 0.2 if v == "low" else -0.3,
            "both_buy_10": 0.3 if v == "high" else -0.3,
        }
        return _pick_best(self, snapshot, state, base)


class MomentumPlayer(Player):
    def __init__(self):
        super().__init__("player-momentum", "Directional momentum trader.")

    def decide(self, snapshot: PublicSnapshot, state: dict) -> Decision:
        trend = _trend_bucket(snapshot)
        # Up trend -> prefer call-side buy / put-side sell.
        # Down trend -> prefer put-side buy / call-side sell.
        if trend == "up":
            base = {
                "flat": 0.1,
                "put_sell_5": 0.45,
                "call_buy_5": 0.55,
                "call_sell_5": -0.2,
                "put_buy_5": -0.1,
                "both_sell_5": 0.2,
                "both_buy_5": 0.2,
                "both_sell_10": 0.0,
                "both_buy_10": 0.1,
            }
        elif trend == "down":
            base = {
                "flat": 0.1,
                "put_buy_5": 0.55,
                "call_sell_5": 0.45,
                "put_sell_5": -0.2,
                "call_buy_5": -0.1,
                "both_sell_5": 0.2,
                "both_buy_5": 0.2,
                "both_sell_10": 0.0,
                "both_buy_10": 0.1,
            }
        else:
            base = {
                "flat": 0.3,
                "put_sell_5": 0.35,
                "call_sell_5": 0.35,
                "put_buy_5": 0.1,
                "call_buy_5": 0.1,
                "both_sell_5": 0.4,
                "both_buy_5": -0.1,
                "both_sell_10": 0.1,
                "both_buy_10": -0.2,
            }
        return _pick_best(self, snapshot, state, base)


class VolatilityPlayer(Player):
    def __init__(self):
        super().__init__("player-volatility", "Realized vs implied vol trader.")

    def decide(self, snapshot: PublicSnapshot, state: dict) -> Decision:
        # If implied > realized, bias to selling premium; otherwise buying.
        implied = _vix1d_pct(snapshot) / 100.0
        realized = snapshot.rv5 if snapshot.rv5 > 0 else snapshot.rv
        spread = implied - realized
        sell_bias = spread > 0.015
        buy_bias = spread < -0.005
        base = {
            "flat": 0.1,
            "put_sell_5": 0.35 if sell_bias else 0.0,
            "call_sell_5": 0.35 if sell_bias else 0.0,
            "both_sell_5": 0.65 if sell_bias else -0.1,
            "both_sell_10": 0.2 if sell_bias else -0.25,
            "put_buy_5": 0.3 if buy_bias else -0.05,
            "call_buy_5": 0.3 if buy_bias else -0.05,
            "both_buy_5": 0.55 if buy_bias else -0.1,
            "both_buy_10": 0.25 if buy_bias else -0.25,
        }
        return _pick_best(self, snapshot, state, base)


class ContrarianPlayer(Player):
    def __init__(self):
        super().__init__("player-contrarian", "Anti-consensus trader.")

    def decide(self, snapshot: PublicSnapshot, state: dict) -> Decision:
        v = _vix_bucket(snapshot)
        trend = _trend_bucket(snapshot)
        # Contrarian: fade directional trend and avoid crowding.
        if trend == "up":
            base = {
                "flat": 0.15,
                "call_sell_5": 0.55,
                "put_buy_5": 0.45,
                "put_sell_5": -0.2,
                "call_buy_5": -0.2,
                "both_sell_5": 0.25 if v == "high" else 0.1,
                "both_buy_5": 0.15 if v == "low" else 0.25,
                "both_sell_10": 0.05,
                "both_buy_10": 0.1,
            }
        elif trend == "down":
            base = {
                "flat": 0.15,
                "put_sell_5": 0.55,
                "call_buy_5": 0.45,
                "put_buy_5": -0.2,
                "call_sell_5": -0.2,
                "both_sell_5": 0.25 if v == "high" else 0.1,
                "both_buy_5": 0.15 if v == "low" else 0.25,
                "both_sell_10": 0.05,
                "both_buy_10": 0.1,
            }
        else:
            base = {
                "flat": 0.4,
                "put_sell_5": 0.25,
                "call_sell_5": 0.25,
                "put_buy_5": 0.2,
                "call_buy_5": 0.2,
                "both_sell_5": 0.2,
                "both_buy_5": 0.2,
                "both_sell_10": 0.05,
                "both_buy_10": 0.05,
            }
        return _pick_best(self, snapshot, state, base)


def _pick_best(player: Player, snapshot: PublicSnapshot, state: dict, base_scores: dict[str, float]) -> Decision:
    scored: list[tuple[str, float]] = []
    for template_id, decision in TEMPLATES.items():
        base = float(base_scores.get(template_id, -0.3))
        score = player._score_template(snapshot, state, template_id, base)
        scored.append((template_id, score))
    best = max(scored, key=lambda t: t[1])[0]
    d = TEMPLATES[best]
    # Copy decision so per-round thesis can be personalized.
    return Decision(
        put_action=d.put_action,
        call_action=d.call_action,
        put_width=d.put_width,
        call_width=d.call_width,
        size=d.size,
        template_id=d.template_id,
        thesis=f"{player.persona} | ctx={_context_key(snapshot)} | template={best}",
    )


def build_players() -> list[Player]:
    return [
        RegimePlayer(),
        MomentumPlayer(),
        VolatilityPlayer(),
        ContrarianPlayer(),
    ]


def player_by_id(players: Iterable[Player], player_id: str) -> Player:
    for p in players:
        if p.player_id == player_id:
            return p
    raise KeyError(f"Unknown player_id: {player_id}")

