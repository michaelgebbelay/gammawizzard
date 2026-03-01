"""Self-learning players for the live binary-vertical game."""

from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass
from typing import Iterable

from sim_gpt.config import ALLOWED_WIDTHS, MAX_CONTRACTS, TARGET_DELTAS
from sim_gpt.types import Decision, PublicSnapshot, SideAction


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


def _side_token(action: SideAction, width: int | None, target_delta: float | None) -> str:
    if action == SideAction.NONE:
        return "n"
    side = "b" if action == SideAction.BUY else "s"
    delta_token = "" if target_delta is None else f"d{int(round(target_delta * 100)):02d}"
    return f"{side}{width}{delta_token}"


def _template_id(
    put_action: SideAction,
    call_action: SideAction,
    put_width: int | None,
    call_width: int | None,
    put_target_delta: float | None,
    call_target_delta: float | None,
    size: int,
) -> str:
    return (
        f"p{_side_token(put_action, put_width, put_target_delta)}_"
        f"c{_side_token(call_action, call_width, call_target_delta)}_x{size}"
    )


def _template_pool() -> dict[str, Decision]:
    templates: dict[str, Decision] = {
        "flat": Decision(
            put_action=SideAction.NONE,
            call_action=SideAction.NONE,
            put_width=None,
            call_width=None,
            put_target_delta=None,
            call_target_delta=None,
            size=1,
            thesis="No trade.",
            template_id="flat",
        )
    }

    widths = sorted(int(w) for w in ALLOWED_WIDTHS)
    deltas = sorted(float(d) for d in TARGET_DELTAS)
    sizes = range(1, MAX_CONTRACTS + 1)
    actions = (SideAction.NONE, SideAction.BUY, SideAction.SELL)

    for size in sizes:
        for put_action in actions:
            for call_action in actions:
                if put_action == SideAction.NONE and call_action == SideAction.NONE:
                    continue
                put_widths = [None] if put_action == SideAction.NONE else widths
                call_widths = [None] if call_action == SideAction.NONE else widths
                put_deltas = [None] if put_action == SideAction.NONE else deltas
                call_deltas = [None] if call_action == SideAction.NONE else deltas

                for put_width in put_widths:
                    for call_width in call_widths:
                        for put_delta in put_deltas:
                            for call_delta in call_deltas:
                                tid = _template_id(
                                    put_action=put_action,
                                    call_action=call_action,
                                    put_width=put_width,
                                    call_width=call_width,
                                    put_target_delta=put_delta,
                                    call_target_delta=call_delta,
                                    size=size,
                                )
                                templates[tid] = Decision(
                                    put_action=put_action,
                                    call_action=call_action,
                                    put_width=put_width,
                                    call_width=call_width,
                                    put_target_delta=put_delta,
                                    call_target_delta=call_delta,
                                    size=size,
                                    thesis=f"Self-learning template {tid}.",
                                    template_id=tid,
                                )

    return templates


TEMPLATES = _template_pool()


def _max_loss_for_template(decision: Decision) -> float:
    total = 0.0
    if decision.put_action != SideAction.NONE and decision.put_width:
        total += float(decision.put_width) * float(decision.size) * 100.0
    if decision.call_action != SideAction.NONE and decision.call_width:
        total += float(decision.call_width) * float(decision.size) * 100.0
    return total


def _is_flat(template_id: str) -> bool:
    return template_id == "flat"


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


@dataclass(frozen=True)
class MarketFrame:
    expected_move_pts: float
    expected_move_pct: float
    iv_minus_rv: float
    directional_edge: float


def _market_frame(snapshot: PublicSnapshot) -> MarketFrame:
    spot = max(1.0, float(snapshot.spx))
    vix1d = _vix1d_pct(snapshot) / 100.0
    realized = float(snapshot.rv5 if snapshot.rv5 > 0 else snapshot.rv)
    expected_move = max(5.0, spot * vix1d / math.sqrt(252.0))

    # Directional proxy from same-day return + forward basis.
    trend = float(snapshot.r)
    fwd_edge = (float(snapshot.forward) - spot) / spot
    directional_edge = (trend * 120.0) + (fwd_edge * 160.0)

    return MarketFrame(
        expected_move_pts=expected_move,
        expected_move_pct=expected_move / spot,
        iv_minus_rv=vix1d - realized,
        directional_edge=_clamp(directional_edge, -2.0, 2.0),
    )


def _options_prior_score(
    decision: Decision,
    frame: MarketFrame,
    rounds_seen: int,
    warmup_rounds: int,
) -> float:
    if _is_flat(decision.template_id):
        return -0.05

    score = 0.0

    # Volatility carry edge: positive iv-rv favors short premium, negative favors long premium.
    carry = _clamp(frame.iv_minus_rv, -0.04, 0.04) * 28.0
    short_legs = 0
    long_legs = 0

    if decision.put_action == SideAction.SELL:
        short_legs += 1
        score += carry
    elif decision.put_action == SideAction.BUY:
        long_legs += 1
        score -= carry

    if decision.call_action == SideAction.SELL:
        short_legs += 1
        score += carry
    elif decision.call_action == SideAction.BUY:
        long_legs += 1
        score -= carry

    # Expected-move regime (from VixOne only): tighter days favor short premium; larger move days favor long premium.
    em_edge = _clamp((0.0105 - frame.expected_move_pct) * 65.0, -0.80, 0.80)
    if short_legs:
        score += em_edge * short_legs
    if long_legs:
        score -= em_edge * long_legs

    # Directional mapping for cash-settled structures (no assignment risk, pure payoff at close).
    dir_exposure = 0.0
    if decision.put_action == SideAction.SELL:
        dir_exposure += 0.70
    elif decision.put_action == SideAction.BUY:
        dir_exposure -= 0.70
    if decision.call_action == SideAction.BUY:
        dir_exposure += 0.70
    elif decision.call_action == SideAction.SELL:
        dir_exposure -= 0.70
    score += dir_exposure * frame.directional_edge * 0.26

    # Notional discipline: softly penalize larger structures, especially during warmup.
    risk = _max_loss_for_template(decision)
    score -= (risk / 1000.0) * 0.05
    if rounds_seen < warmup_rounds and decision.size > 1:
        score -= 0.12 * (decision.size - 1)

    return score


def _participation_bias(
    decision: Decision,
    trade_rate: float,
    consecutive_holds: int,
    target_trade_rate: float,
) -> float:
    deficit = max(0.0, target_trade_rate - trade_rate)
    streak = max(0, consecutive_holds)
    if _is_flat(decision.template_id):
        # Softly discourage repeated cash-holds, especially when below target.
        return -(deficit * 3.2) - (max(0, streak - 1) * 0.40)
    # Encourage taking risk-defined trades when participation falls.
    return (deficit * 0.70) + (max(0, streak - 1) * 0.08)


@dataclass
class Player:
    player_id: str
    explore_coef: float
    risk_penalty: float
    flat_penalty: float
    cold_start_boost: float
    warmup_rounds: int

    def decide(self, snapshot: PublicSnapshot, state: dict, account_ctx: dict | None = None) -> Decision:
        ctx_input = account_ctx or {}
        ctx = _context_key(snapshot)
        meta = state.get("meta", {})
        rounds_seen = int(meta.get("decision_rounds", meta.get("rounds", 0)))
        frame = _market_frame(snapshot)

        scored: list[tuple[str, float]] = []
        for template_id in TEMPLATES:
            score = self._score_template(
                snapshot=snapshot,
                state=state,
                account_ctx=ctx_input,
                ctx=ctx,
                frame=frame,
                rounds_seen=rounds_seen,
                template_id=template_id,
            )
            scored.append((template_id, score))

        scored.sort(key=lambda t: t[1], reverse=True)
        best_template = scored[0][0]

        # Force active exploration in the bootstrapping phase.
        if rounds_seen < self.warmup_rounds:
            candidates = [tid for tid, _ in scored if not _is_flat(tid)]
            top = candidates[: max(4, min(8, len(candidates)))]
            if top:
                h = hashlib.sha256(
                    f"{snapshot.signal_date}|{self.player_id}|{rounds_seen}|explore".encode()
                ).hexdigest()
                idx = int(h[:8], 16) % len(top)
                best_template = top[idx]
            elif _is_flat(best_template):
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
            put_target_delta=d.put_target_delta,
            call_target_delta=d.call_target_delta,
            size=d.size,
            template_id=d.template_id,
            thesis=(
                f"model=self_learning_options | id={self.player_id} | ctx={ctx} | "
                f"em={frame.expected_move_pts:.1f} | ivrv={frame.iv_minus_rv:.4f} | "
                f"dir={frame.directional_edge:.2f} | tr={float(ctx_input.get('trade_rate', 1.0)):.2f} | "
                f"hold={int(ctx_input.get('consecutive_holds', 0))} | template={best_template}"
            ),
        )

    def _score_template(
        self,
        snapshot: PublicSnapshot,
        state: dict,
        account_ctx: dict,
        ctx: str,
        frame: MarketFrame,
        rounds_seen: int,
        template_id: str,
    ) -> float:
        item = _template_stats(state, ctx, template_id)
        n = int(item.get("n", 0))
        mean = float(item.get("mean", 0.0))
        m2 = float(item.get("m2", 0.0))
        meta = state.get("meta", {})
        total_n = max(
            int(meta.get("rounds", 0)),
            int(meta.get("decision_rounds", meta.get("rounds", 0))),
        )

        variance = (m2 / (n - 1)) if n > 1 else 0.0
        std = math.sqrt(max(0.0, variance))

        decision = TEMPLATES[template_id]
        risk = _max_loss_for_template(decision)
        risk_norm = max(100.0, risk)
        mean_per_risk = mean / risk_norm
        std_per_risk = std / risk_norm

        prior = _options_prior_score(
            decision=decision,
            frame=frame,
            rounds_seen=rounds_seen,
            warmup_rounds=self.warmup_rounds,
        )
        participation = _participation_bias(
            decision=decision,
            trade_rate=float(account_ctx.get("trade_rate", 1.0)),
            consecutive_holds=int(account_ctx.get("consecutive_holds", 0)),
            target_trade_rate=float(account_ctx.get("target_trade_rate", 0.90)),
        )
        ucb = self.explore_coef * math.sqrt(math.log(total_n + 2.0) / (n + 1.0))
        cold = self.cold_start_boost if (n == 0 and not _is_flat(template_id)) else 0.0
        flat_pen = self.flat_penalty if _is_flat(template_id) else 0.0
        noise = _deterministic_noise(f"{snapshot.signal_date}|{self.player_id}|{template_id}")

        return (
            prior
            + participation
            + mean_per_risk
            - (self.risk_penalty * std_per_risk)
            + ucb
            + cold
            - flat_pen
            + noise
        )

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
