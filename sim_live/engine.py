"""Round engine for the live binary-vertical game."""

from __future__ import annotations

from datetime import date

from sim_live.config import LIVE_START_DATE
from sim_live.feed import LeoFeed
from sim_live.judge import Judge
from sim_live.players import Player, build_players, player_by_id
from sim_live.store import Store
from sim_live.types import Decision, RoundOutcomes, SideAction


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
            valid, err = decision.validate()
            self.store.save_decision(
                signal_date=snapshot.signal_date.isoformat(),
                player_id=player.player_id,
                decision=decision.to_dict(),
                valid=valid,
                error=err,
            )
            decisions_out.append(
                {
                    "player_id": player.player_id,
                    "valid": valid,
                    "error": err,
                    "decision": decision.to_dict(),
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


def _json_load(s: str) -> dict:
    import json

    try:
        return json.loads(s)
    except json.JSONDecodeError:
        return {}
