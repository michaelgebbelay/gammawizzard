"""Self-learning players for the live binary-vertical game."""

from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass
from typing import Iterable

from sim_gpt.template_rules import template_rule
from sim_gpt.types import Decision, PublicSnapshot, SideAction, build_decision


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
    return (val - 0.5) * 0.05


def _template_pool() -> dict[str, Decision]:
    templates = {
        "flat": build_decision(
            SideAction.NONE,
            SideAction.NONE,
            size=1,
            thesis="No trade.",
            template_id="flat",
        )
    }

    widths = (5, 10)
    sizes = (1, 2, 3)
    legs = (
        ("put_sell", SideAction.SELL, SideAction.NONE),
        ("put_buy", SideAction.BUY, SideAction.NONE),
        ("call_sell", SideAction.NONE, SideAction.SELL),
        ("call_buy", SideAction.NONE, SideAction.BUY),
        ("both_sell", SideAction.SELL, SideAction.SELL),
        ("both_buy", SideAction.BUY, SideAction.BUY),
        ("rr_long", SideAction.SELL, SideAction.BUY),
        ("rr_short", SideAction.BUY, SideAction.SELL),
    )

    for width in widths:
        for size in sizes:
            for name, put_side, call_side in legs:
                template_id = f"{name}_{width}x{size}"
                templates[template_id] = build_decision(
                    put_side,
                    call_side,
                    width=width,
                    size=size,
                    thesis=f"Self-learning template {template_id}.",
                    template_id=template_id,
                )
    return templates


TEMPLATES = _template_pool()


def _max_loss_for_template(decision: Decision) -> float:
    rule = template_rule(decision.template_id)
    total = 0.0
    if decision.put_action != SideAction.NONE and decision.put_width:
        total += float(decision.put_width) * float(decision.size) * rule.risk_per_width_dollars
    if decision.call_action != SideAction.NONE and decision.call_width:
        total += float(decision.call_width) * float(decision.size) * rule.risk_per_width_dollars
    return total


def _is_flat(template_id: str) -> bool:
    return template_id == "flat"


@dataclass
class Player:
    player_id: str
    explore_coef: float
    risk_penalty: float
    flat_penalty: float
    cold_start_boost: float
    warmup_rounds: int

    def decide(self, snapshot: PublicSnapshot, state: dict) -> Decision:
        ctx = _context_key(snapshot)
        rounds_seen = int(state.get("meta", {}).get("rounds", 0))

        scored: list[tuple[str, float]] = []
        for template_id in TEMPLATES:
            score = self._score_template(snapshot, state, ctx, template_id)
            scored.append((template_id, score))

        scored.sort(key=lambda t: t[1], reverse=True)
        best_template = scored[0][0]

        # Force active exploration in the bootstrapping phase.
        if rounds_seen < self.warmup_rounds and _is_flat(best_template):
            for template_id, _ in scored:
                if not _is_flat(template_id):
                    best_template = template_id
                    break

        d = TEMPLATES[best_template]
        return Decision(
            put_action=d.put_action,
            call_action=d.call_action,
            put_width=d.put_width,
            call_width=d.call_width,
            size=d.size,
            template_id=d.template_id,
            thesis=f"model=self_learning | id={self.player_id} | ctx={ctx} | template={best_template}",
        )

    def _score_template(self, snapshot: PublicSnapshot, state: dict, ctx: str, template_id: str) -> float:
        item = _template_stats(state, ctx, template_id)
        n = int(item.get("n", 0))
        mean = float(item.get("mean", 0.0))
        m2 = float(item.get("m2", 0.0))
        total_n = int(state.get("meta", {}).get("rounds", 0))

        variance = (m2 / (n - 1)) if n > 1 else 0.0
        std = math.sqrt(max(0.0, variance))

        decision = TEMPLATES[template_id]
        risk = _max_loss_for_template(decision)
        risk_norm = max(100.0, risk)
        mean_per_risk = mean / risk_norm
        std_per_risk = std / risk_norm

        ucb = self.explore_coef * math.sqrt(math.log(total_n + 2.0) / (n + 1.0))
        cold = self.cold_start_boost if (n == 0 and not _is_flat(template_id)) else 0.0
        flat_pen = self.flat_penalty if _is_flat(template_id) else 0.0
        noise = _deterministic_noise(f"{snapshot.signal_date}|{self.player_id}|{template_id}")

        return mean_per_risk - (self.risk_penalty * std_per_risk) + ucb + cold - flat_pen + noise

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
        item = q_ctx.setdefault(template_id, {"n": 0, "mean": 0.0, "m2": 0.0})

        # Backward compatibility for old state shape: {"n":..., "avg":...}
        if "mean" not in item and "avg" in item:
            item["mean"] = float(item.get("avg", 0.0))
            item["m2"] = float(item.get("m2", 0.0))

        n = int(item.get("n", 0))
        mean = float(item.get("mean", 0.0))
        m2 = float(item.get("m2", 0.0))

        n_new = n + 1
        delta = float(pnl) - mean
        mean_new = mean + delta / n_new
        delta2 = float(pnl) - mean_new
        m2_new = m2 + delta * delta2

        item["n"] = n_new
        item["mean"] = mean_new
        item["m2"] = m2_new
        item["avg"] = mean_new

        meta = state.setdefault("meta", {})
        meta["rounds"] = int(meta.get("rounds", 0)) + 1
        return state


def _template_stats(state: dict, ctx: str, template_id: str) -> dict:
    q = state.get("q", {})
    q_ctx = q.get(ctx, {})
    item = q_ctx.get(template_id, {})
    n = int(item.get("n", 0))
    if "mean" in item:
        mean = float(item.get("mean", 0.0))
        m2 = float(item.get("m2", 0.0))
    else:
        # Support old schema.
        mean = float(item.get("avg", 0.0))
        m2 = float(item.get("m2", 0.0))
    return {"n": n, "mean": mean, "m2": m2}


def build_players() -> list[Player]:
    # Same learner family, no personality priors. Diversity comes from search parameters.
    return [
        Player(
            player_id="player-01",
            explore_coef=0.75,
            risk_penalty=0.10,
            flat_penalty=0.08,
            cold_start_boost=0.22,
            warmup_rounds=8,
        ),
        Player(
            player_id="player-02",
            explore_coef=0.65,
            risk_penalty=0.14,
            flat_penalty=0.08,
            cold_start_boost=0.20,
            warmup_rounds=8,
        ),
        Player(
            player_id="player-03",
            explore_coef=0.58,
            risk_penalty=0.18,
            flat_penalty=0.08,
            cold_start_boost=0.18,
            warmup_rounds=8,
        ),
        Player(
            player_id="player-04",
            explore_coef=0.50,
            risk_penalty=0.24,
            flat_penalty=0.08,
            cold_start_boost=0.16,
            warmup_rounds=8,
        ),
        Player(
            player_id="player-05",
            explore_coef=0.42,
            risk_penalty=0.30,
            flat_penalty=0.08,
            cold_start_boost=0.14,
            warmup_rounds=8,
        ),
    ]


def player_by_id(players: Iterable[Player], player_id: str) -> Player:
    for p in players:
        if p.player_id == player_id:
            return p
    raise KeyError(f"Unknown player_id: {player_id}")
