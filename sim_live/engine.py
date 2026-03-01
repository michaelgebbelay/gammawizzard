"""Round engine for the live binary-vertical game."""

from __future__ import annotations

from datetime import date, datetime, timedelta, time, timezone
from zoneinfo import ZoneInfo

from sim_live.config import (
    ASOF_MAX_STALENESS_MINUTES,
    LIVE_START_DATE,
    MARKET_TZ,
    MAX_RISK_PCT,
    POST_CLOSE_DELAY_MINUTES,
    RISK_BUFFER_PCT,
    STARTING_ACCOUNT_BALANCE,
)
from sim_live.feed import LeoFeed
from sim_live.judge import Judge
from sim_live.players import Player, build_players, player_by_id
from sim_live.store import Store
from sim_live.template_rules import template_rule
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

        try:
            raw_row = feed.validate_signal_row(signal_date)
        except ValueError as e:
            # Feed is source-of-truth: no row means no round today.
            if "No Leo data found for signal date" in str(e):
                return {
                    "status": "skipped",
                    "reason": str(e),
                    "signal_date": signal_date.isoformat(),
                    "settled_rounds": settled,
                    "decisions": [],
                }
            raise
        self._validate_live_signal_window(signal_date, raw_row, allow_prestart=allow_prestart)
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
            "status": "created",
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
            try:
                outcomes = feed.get_outcomes(signal_date)
            except ValueError:
                # Settle only when feed has exactly one valid row for this signal date.
                continue
            if outcomes is None:
                # Outcome data not available yet; keep pending.
                continue

            decisions = self.store.get_decisions(signal_date.isoformat())
            for drow in decisions:
                if not drow["valid"]:
                    continue
                decision = Decision.from_dict(_json_load(drow["decision_json"]))
                put_pnl, call_pnl, total_pnl = score_decision(decision, outcomes)
                try:
                    snapshot = feed.get_public_snapshot(signal_date)
                except ValueError:
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

    def _validate_live_signal_window(self, signal_date: date, row: dict, allow_prestart: bool) -> None:
        now_utc = datetime.now(timezone.utc)
        market = ZoneInfo(MARKET_TZ)
        now_et = now_utc.astimezone(market)
        is_same_day = signal_date == now_et.date()

        close_dt_et = datetime.combine(signal_date, time(16, 0), tzinfo=market)
        earliest_entry_et = close_dt_et + timedelta(minutes=POST_CLOSE_DELAY_MINUTES)

        # Apply post-close timing checks for same-day live rounds only.
        if is_same_day and not allow_prestart and now_et < earliest_entry_et:
            raise ValueError(
                "Too early for live entry: "
                f"now={now_et.isoformat()} "
                f"earliest={earliest_entry_et.isoformat()} "
                f"tz={MARKET_TZ}"
            )

        # Guard against accidentally using already-settled columns on same-day runs.
        if is_same_day:
            if row.get("Profit") not in (None, "") or row.get("CProfit") not in (None, ""):
                raise ValueError(
                    "Signal row includes settlement fields (Profit/CProfit) on decision run; refusing to trade"
                )

        asof = _extract_asof_timestamp(row)
        if asof is None:
            return
        asof_et = asof.astimezone(market)

        if asof_et.date() != signal_date:
            raise ValueError(
                f"Data asof date mismatch: asof={asof_et.isoformat()} signal_date={signal_date.isoformat()}"
            )
        if asof_et < close_dt_et:
            raise ValueError(
                "Data asof is pre-close; expected post-close snapshot "
                f"(asof={asof_et.isoformat()}, close={close_dt_et.isoformat()})"
            )
        if is_same_day:
            max_age = timedelta(minutes=ASOF_MAX_STALENESS_MINUTES)
            if now_et - asof_et > max_age:
                raise ValueError(
                    f"Data asof is stale by > {ASOF_MAX_STALENESS_MINUTES} minutes "
                    f"(asof={asof_et.isoformat()}, now={now_et.isoformat()})"
                )

    def _apply_risk_limits(self, player_id: str, decision: Decision) -> tuple[Decision, dict]:
        equity = float(self.store.projected_risk_metrics(player_id)["equity_pnl"])
        account_value = max(0.0, STARTING_ACCOUNT_BALANCE + equity)
        risk_budget = account_value * MAX_RISK_PCT * RISK_BUFFER_PCT

        adjusted = Decision.from_dict(decision.to_dict())
        pre_max_loss = max_loss_dollars(adjusted)
        max_loss = pre_max_loss
        risk_note = "within_budget"

        if max_loss > risk_budget + 1e-6:
            unit_loss = max_loss_dollars(Decision.from_dict({**adjusted.to_dict(), "size": 1}))
            max_size = int(risk_budget // unit_loss) if unit_loss > 0 else 0
            before_size = adjusted.size

            if max_size >= 1:
                adjusted.size = max_size
                max_loss = max_loss_dollars(adjusted)
                risk_note = (
                    f"size_clamped: before_size={before_size} after_size={max_size} "
                    f"pre_max_loss={pre_max_loss:.2f} post_max_loss={max_loss:.2f} "
                    f"budget={risk_budget:.2f}"
                )
            else:
                adjusted = build_decision(
                    SideAction.NONE,
                    SideAction.NONE,
                    size=1,
                    thesis="risk guard: no-trade",
                    template_id="risk_guard_flat",
                )
                max_loss = 0.0
                risk_note = (
                    "flattened_by_risk_guard: "
                    f"pre_max_loss={pre_max_loss:.2f} budget={risk_budget:.2f}"
                )

        risk_used_pct = (max_loss / account_value * 100.0) if account_value > 0 else 0.0
        return adjusted, {
            "account_value": round(account_value, 2),
            "risk_budget": round(risk_budget, 2),
            "pre_max_loss": round(pre_max_loss, 2),
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
    template_id: str,
) -> float:
    if action == SideAction.NONE:
        return 0.0
    if width is None:
        return 0.0

    rule = template_rule(template_id)
    direction = 1.0 if action == SideAction.SELL else -1.0
    width_mult = width * rule.pnl_scale_per_width
    return direction * base_short_pnl_5w * width_mult * size * 100.0


def score_decision(decision: Decision, outcomes: RoundOutcomes) -> tuple[float, float, float]:
    put_pnl = side_pnl(
        action=decision.put_action,
        width=decision.put_width,
        base_short_pnl_5w=outcomes.put_short_pnl_5w,
        size=decision.size,
        template_id=decision.template_id,
    )
    call_pnl = side_pnl(
        action=decision.call_action,
        width=decision.call_width,
        base_short_pnl_5w=outcomes.call_short_pnl_5w,
        size=decision.size,
        template_id=decision.template_id,
    )
    total = put_pnl + call_pnl
    return round(put_pnl, 2), round(call_pnl, 2), round(total, 2)


def max_loss_dollars(decision: Decision) -> float:
    rule = template_rule(decision.template_id)
    total = 0.0
    if decision.put_action != SideAction.NONE and decision.put_width:
        total += float(decision.put_width) * rule.risk_per_width_dollars * float(decision.size)
    if decision.call_action != SideAction.NONE and decision.call_width:
        total += float(decision.call_width) * rule.risk_per_width_dollars * float(decision.size)
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


def _extract_asof_timestamp(row: dict) -> datetime | None:
    candidates = [
        "asof",
        "AsOf",
        "Asof",
        "asof_utc",
        "AsOfUtc",
        "timestamp",
        "Timestamp",
        "ts",
        "ts_utc",
    ]
    raw = None
    for key in candidates:
        val = row.get(key)
        if val not in (None, ""):
            raw = str(val).strip()
            break
    if not raw:
        return None

    raw = raw.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)
