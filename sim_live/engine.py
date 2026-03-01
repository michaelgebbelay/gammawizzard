"""Round engine for the live binary-vertical game."""

from __future__ import annotations

from datetime import date

from sim_live.config import LIVE_START_DATE, MAX_RISK_PCT, RISK_BUFFER_PCT, STARTING_ACCOUNT_BALANCE
from sim_live.feed import LeoFeed
from sim_live.judge import Judge
from sim_live.players import Player, build_players, player_by_id
from sim_live.store import Store
from sim_live.types import Decision, RoundOutcomes, SideAction, build_decision


class LiveGameEngine:
    def __init__(self, store: Store, players: list[Player] | None = None, judge: Judge | None = None):
        self.store = store
        self.players = players or build_players()
        self.judge = judge or Judge()

    def run_live_round(self, signal_date: date, feed: LeoFeed, allow_prestart: bool = False) -> dict:
        if signal_date < LIVE_START_DATE and not allow_prestart:
            raise ValueError(
                f"Live game starts on {LIVE_START_DATE.isoformat()}, got {signal_date.isoformat()}"
            )

        # Settle any due rounds first.
        settled = self.settle_due(signal_date, feed)

        snapshot = feed.get_public_snapshot(signal_date)
        if snapshot is None:
            raise ValueError(f"No Leo data found for signal date: {signal_date.isoformat()}")

        self.store.upsert_round(
            signal_date=snapshot.signal_date.isoformat(),
            tdate=snapshot.tdate.isoformat(),
            public_snapshot=snapshot.payload,
        )

        decisions_out = []
        for player in self.players:
            state = self.store.load_player_state(player.player_id)
            decision = player.decide(snapshot, state)
            decision, risk_meta = self._apply_risk_limits(player.player_id, decision)
            valid, err = decision.validate()
            if valid:
                valid, err = validate_risk_defined_structure(decision)
            if valid and risk_meta["max_loss"] > risk_meta["risk_budget"] + 1e-6:
                valid = False
                err = (
                    "max_loss exceeds risk budget: "
                    f"{risk_meta['max_loss']:.2f} > {risk_meta['risk_budget']:.2f}"
                )

            decision_payload = decision.to_dict()
            decision_payload.update(risk_meta)
            self.store.save_decision(
                signal_date=snapshot.signal_date.isoformat(),
                player_id=player.player_id,
                decision=decision_payload,
                valid=valid,
                error=err,
            )
            decisions_out.append(
                {
                    "player_id": player.player_id,
                    "valid": valid,
                    "error": err,
                    "decision": decision_payload,
                }
            )

        return {
            "signal_date": snapshot.signal_date.isoformat(),
            "tdate": snapshot.tdate.isoformat(),
            "settled_rounds": settled,
            "decisions": decisions_out,
        }

    def settle_due(self, settlement_date: date, feed: LeoFeed) -> list[dict]:
        due = self.store.pending_rounds_due(settlement_date.isoformat())
        settled_rows = []

        for row in due:
            signal_date = date.fromisoformat(row["signal_date"])
            outcomes = feed.get_outcomes(signal_date)
            if outcomes is None:
                # Outcome data not available yet; keep pending.
                continue

            decisions = self.store.get_decisions(signal_date.isoformat())
            for drow in decisions:
                if not drow["valid"]:
                    continue
                decision = Decision.from_dict(_json_load(drow["decision_json"]))
                put_pnl, call_pnl, total_pnl = score_decision(decision, outcomes)
                snapshot = feed.get_public_snapshot(signal_date)
                if snapshot is None:
                    continue

                risk = self.store.projected_risk_metrics(
                    player_id=drow["player_id"],
                    pending_pnl=total_pnl,
                )
                judge_score, judge_notes = self.judge.score(total_pnl=total_pnl, metrics=risk)
                self.store.save_result(
                    signal_date=signal_date.isoformat(),
                    player_id=drow["player_id"],
                    put_pnl=put_pnl,
                    call_pnl=call_pnl,
                    total_pnl=total_pnl,
                    equity_pnl=risk["equity_pnl"],
                    drawdown=risk["current_drawdown"],
                    max_drawdown=risk["max_drawdown"],
                    risk_adjusted=risk["risk_adjusted"],
                    judge_score=judge_score,
                    judge_notes=judge_notes,
                )

                # Online learning update.
                player = player_by_id(self.players, drow["player_id"])
                state = self.store.load_player_state(player.player_id)
                state = player.update_state(state, snapshot, decision.template_id, total_pnl)
                self.store.save_player_state(player.player_id, state)

            self.store.mark_settled(signal_date.isoformat())
            settled_rows.append(
                {
                    "signal_date": signal_date.isoformat(),
                    "tdate": row["tdate"],
                    "status": "settled",
                }
            )

        return settled_rows

    def _apply_risk_limits(self, player_id: str, decision: Decision) -> tuple[Decision, dict]:
        equity = float(self.store.projected_risk_metrics(player_id)["equity_pnl"])
        account_value = max(0.0, STARTING_ACCOUNT_BALANCE + equity)
        risk_budget = account_value * MAX_RISK_PCT * RISK_BUFFER_PCT

        adjusted = Decision.from_dict(decision.to_dict())
        max_loss = max_loss_dollars(adjusted)
        risk_note = ""

        if max_loss > risk_budget + 1e-6:
            unit_loss = max_loss_dollars(Decision.from_dict({**adjusted.to_dict(), "size": 1}))
            max_size = int(risk_budget // unit_loss) if unit_loss > 0 else 0

            if max_size >= 1:
                adjusted.size = max_size
                max_loss = max_loss_dollars(adjusted)
                risk_note = f"size_clamped_to_{max_size}"
            else:
                adjusted = build_decision(
                    SideAction.NONE,
                    SideAction.NONE,
                    size=1,
                    thesis="risk guard: no-trade",
                    template_id="risk_guard_flat",
                )
                max_loss = 0.0
                risk_note = "flattened_by_risk_guard"

        risk_used_pct = (max_loss / account_value * 100.0) if account_value > 0 else 0.0
        return adjusted, {
            "account_value": round(account_value, 2),
            "risk_budget": round(risk_budget, 2),
            "max_loss": round(max_loss, 2),
            "risk_used_pct": round(risk_used_pct, 2),
            "max_risk_pct": round(MAX_RISK_PCT * 100.0, 2),
            "risk_buffer_pct": round(RISK_BUFFER_PCT * 100.0, 2),
            "risk_guard": risk_note,
        }


def side_pnl(
    action: SideAction,
    width: int | None,
    base_short_pnl_5w: float,
    size: int,
) -> float:
    if action == SideAction.NONE:
        return 0.0
    if width is None:
        return 0.0

    direction = 1.0 if action == SideAction.SELL else -1.0
    width_mult = width / 5.0
    return direction * base_short_pnl_5w * width_mult * size * 100.0


def score_decision(decision: Decision, outcomes: RoundOutcomes) -> tuple[float, float, float]:
    put_pnl = side_pnl(
        action=decision.put_action,
        width=decision.put_width,
        base_short_pnl_5w=outcomes.put_short_pnl_5w,
        size=decision.size,
    )
    call_pnl = side_pnl(
        action=decision.call_action,
        width=decision.call_width,
        base_short_pnl_5w=outcomes.call_short_pnl_5w,
        size=decision.size,
    )
    total = put_pnl + call_pnl
    return round(put_pnl, 2), round(call_pnl, 2), round(total, 2)


def max_loss_dollars(decision: Decision) -> float:
    total = 0.0
    if decision.put_action != SideAction.NONE and decision.put_width:
        total += float(decision.put_width) * 100.0 * float(decision.size)
    if decision.call_action != SideAction.NONE and decision.call_width:
        total += float(decision.call_width) * 100.0 * float(decision.size)
    return round(total, 2)


def validate_risk_defined_structure(decision: Decision) -> tuple[bool, str]:
    # This game supports only risk-defined spreads (vertical packages) or no-trade.
    if decision.active_sides() == 0:
        return True, ""
    if decision.put_action != SideAction.NONE and decision.put_width is None:
        return False, "put side must use a defined vertical width"
    if decision.call_action != SideAction.NONE and decision.call_width is None:
        return False, "call side must use a defined vertical width"
    return True, ""


def _json_load(s: str) -> dict:
    import json

    try:
        return json.loads(s)
    except json.JSONDecodeError:
        return {}
