"""Round engine for the live risk-defined SPX game."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from sim_gpt.config import (
    ASOF_MAX_STALENESS_MINUTES,
    COMMISSION_PER_LEG_DOLLARS,
    FILL_HALF_SPREAD_FACTOR,
    LIVE_START_DATE,
    MARKET_TZ,
    MAX_RISK_PCT,
    POST_CLOSE_DELAY_MINUTES,
    RISK_BUFFER_PCT,
    STARTING_ACCOUNT_BALANCE,
    TARGET_DELTA_MAX_ERROR,
)
from sim_gpt.feed import LeoFeed
from sim_gpt.judge import Judge
from sim_gpt.players import Player, build_players, player_by_id
from sim_gpt.store import Store
from sim_gpt.types import ChainSnapshot, Decision, OptionQuote, SideAction, build_decision


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

        settled = self.settle_due(signal_date, feed)

        try:
            raw_row = feed.validate_signal_row(signal_date)
        except ValueError as e:
            if "No data found for signal date" in str(e):
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
            raise ValueError(f"No data found for signal date: {signal_date.isoformat()}")

        chain = feed.get_entry_chain(signal_date=snapshot.signal_date, tdate=snapshot.tdate)

        self.store.upsert_round(
            signal_date=snapshot.signal_date.isoformat(),
            tdate=snapshot.tdate.isoformat(),
            public_snapshot=snapshot.payload,
        )

        decisions_out = []
        for player in self.players:
            state = self.store.load_player_state(player.player_id)
            meta = state.setdefault("meta", {})
            decision_rounds = int(meta.get("decision_rounds", 0))
            activity_ctx = self.store.projected_activity_metrics(player.player_id)

            decision = player.decide(snapshot, state, account_ctx=activity_ctx)
            valid, err = decision.validate()
            if valid:
                valid, err = validate_risk_defined_structure(decision)

            pricing_meta = _empty_pricing_meta(chain)
            risk_meta = self._risk_context(player.player_id, max_loss=0.0, pre_max_loss=0.0, note="n/a")

            if valid:
                try:
                    pricing_meta = price_decision(decision, chain)
                except ValueError as e:
                    valid = False
                    err = str(e)

            if valid:
                decision, pricing_meta, risk_meta = self._apply_risk_limits(
                    player_id=player.player_id,
                    decision=decision,
                    pricing_meta=pricing_meta,
                    chain=chain,
                )
                if risk_meta["max_loss"] > risk_meta["risk_budget"] + 1e-6:
                    valid = False
                    err = (
                        "max_loss exceeds risk budget: "
                        f"{risk_meta['max_loss']:.2f} > {risk_meta['risk_budget']:.2f}"
                    )

            decision_payload = decision.to_dict()
            decision_payload.update(pricing_meta)
            decision_payload.update(risk_meta)
            decision_payload["decision_round"] = decision_rounds + 1
            decision_payload["trade_rate_context"] = round(float(activity_ctx["trade_rate"]), 4)
            decision_payload["consecutive_holds_context"] = int(activity_ctx["consecutive_holds"])
            decision_payload["target_trade_rate"] = round(float(activity_ctx["target_trade_rate"]), 4)

            self.store.save_decision(
                signal_date=snapshot.signal_date.isoformat(),
                player_id=player.player_id,
                decision=decision_payload,
                valid=valid,
                error=err,
            )
            meta["decision_rounds"] = decision_rounds + 1
            self.store.save_player_state(player.player_id, state)
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
            tdate = date.fromisoformat(row["tdate"])
            if settlement_date < tdate:
                continue

            try:
                settlement = feed.get_settlement_snapshot(tdate)
            except ValueError:
                continue
            if settlement is None:
                continue

            decisions = self.store.get_decisions(signal_date.isoformat())
            for drow in decisions:
                if not drow["valid"]:
                    continue

                payload = _json_load(drow["decision_json"])
                if payload.get("pricing_model") != "chain_intrinsic_v1":
                    # Old rows from pre-chain model are intentionally ignored.
                    continue

                decision = Decision.from_dict(payload)
                put_pnl, call_pnl, gross_total_pnl, fees, total_pnl = score_decision(
                    decision_payload=payload,
                    settlement_spx=settlement.settlement_spx,
                    size=decision.size,
                )

                try:
                    snapshot = feed.get_public_snapshot(signal_date)
                except ValueError:
                    continue
                if snapshot is None:
                    continue

                risk = self.store.projected_risk_metrics(
                    player_id=drow["player_id"],
                    pending_pnl=total_pnl,
                )
                activity = self.store.projected_activity_metrics(player_id=drow["player_id"])
                judge_metrics = {**risk, **activity}
                judge_score, judge_notes = self.judge.score(total_pnl=total_pnl, metrics=judge_metrics)

                self.store.save_result(
                    signal_date=signal_date.isoformat(),
                    player_id=drow["player_id"],
                    put_pnl=put_pnl,
                    call_pnl=call_pnl,
                    total_pnl=total_pnl,
                    gross_total_pnl=gross_total_pnl,
                    fees=fees,
                    equity_pnl=risk["equity_pnl"],
                    drawdown=risk["current_drawdown"],
                    max_drawdown=risk["max_drawdown"],
                    risk_adjusted=risk["risk_adjusted"],
                    judge_score=judge_score,
                    judge_notes=(
                        f"{judge_notes} | settle_spx={settlement.settlement_spx:.2f}"
                        if judge_notes
                        else f"settle_spx={settlement.settlement_spx:.2f}"
                    ),
                )

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
                    "settlement_spx": round(settlement.settlement_spx, 2),
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

        if is_same_day and not allow_prestart and now_et < earliest_entry_et:
            raise ValueError(
                "Too early for live entry: "
                f"now={now_et.isoformat()} "
                f"earliest={earliest_entry_et.isoformat()} "
                f"tz={MARKET_TZ}"
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

    def _risk_context(self, player_id: str, max_loss: float, pre_max_loss: float, note: str) -> dict:
        equity = float(self.store.projected_risk_metrics(player_id)["equity_pnl"])
        account_value = max(0.0, STARTING_ACCOUNT_BALANCE + equity)
        risk_budget = account_value * MAX_RISK_PCT * RISK_BUFFER_PCT
        risk_used_pct = (max_loss / account_value * 100.0) if account_value > 0 else 0.0
        return {
            "account_value": round(account_value, 2),
            "risk_budget": round(risk_budget, 2),
            "pre_max_loss": round(pre_max_loss, 2),
            "max_loss": round(max_loss, 2),
            "risk_used_pct": round(risk_used_pct, 2),
            "max_risk_pct": round(MAX_RISK_PCT * 100.0, 2),
            "risk_buffer_pct": round(RISK_BUFFER_PCT * 100.0, 2),
            "risk_guard": note,
        }

    def _apply_risk_limits(
        self,
        player_id: str,
        decision: Decision,
        pricing_meta: dict,
        chain: ChainSnapshot,
    ) -> tuple[Decision, dict, dict]:
        equity = float(self.store.projected_risk_metrics(player_id)["equity_pnl"])
        account_value = max(0.0, STARTING_ACCOUNT_BALANCE + equity)
        risk_budget = account_value * MAX_RISK_PCT * RISK_BUFFER_PCT

        adjusted = Decision.from_dict(decision.to_dict())
        unit_max_loss = float(pricing_meta.get("unit_max_loss", 0.0))
        pre_max_loss = round(unit_max_loss * float(adjusted.size), 2)
        max_loss = pre_max_loss
        risk_note = "within_budget"

        if max_loss > risk_budget + 1e-6:
            max_size = int(risk_budget // unit_max_loss) if unit_max_loss > 0 else 0
            before_size = adjusted.size
            if max_size >= 1:
                adjusted.size = max_size
                max_loss = round(unit_max_loss * float(adjusted.size), 2)
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
                pricing_meta = _empty_pricing_meta(chain)
                pre_max_loss = max_loss
                max_loss = 0.0
                risk_note = (
                    "flattened_by_risk_guard: "
                    f"pre_max_loss={pre_max_loss:.2f} budget={risk_budget:.2f}"
                )

        risk_used_pct = (max_loss / account_value * 100.0) if account_value > 0 else 0.0
        risk_meta = {
            "account_value": round(account_value, 2),
            "risk_budget": round(risk_budget, 2),
            "pre_max_loss": round(pre_max_loss, 2),
            "max_loss": round(max_loss, 2),
            "risk_used_pct": round(risk_used_pct, 2),
            "max_risk_pct": round(MAX_RISK_PCT * 100.0, 2),
            "risk_buffer_pct": round(RISK_BUFFER_PCT * 100.0, 2),
            "risk_guard": risk_note,
        }
        return adjusted, pricing_meta, risk_meta


def _intrinsic(put_call: str, strike: float, settlement_spx: float) -> float:
    if put_call == "C":
        return max(0.0, settlement_spx - strike)
    return max(0.0, strike - settlement_spx)


def _structure_pnl_points(entry_cashflow_points: float, legs: list[dict], settlement_spx: float) -> float:
    value = float(entry_cashflow_points)
    for leg in legs:
        sign = float(leg["sign"])
        value += sign * _intrinsic(str(leg["put_call"]), float(leg["strike"]), settlement_spx)
    return value


def _worst_case_pnl_points(entry_cashflow_points: float, legs: list[dict], spot_hint: float) -> float:
    if not legs:
        return 0.0
    strikes = sorted({float(leg["strike"]) for leg in legs})
    hi = max(strikes) + max(1000.0, abs(float(spot_hint)) * 0.5, 400.0)
    candidates = [0.0] + strikes + [float(spot_hint), hi]
    return min(_structure_pnl_points(entry_cashflow_points, legs, s) for s in candidates)


def _find_target_quote(
    quotes: tuple[OptionQuote, ...],
    target_delta: float,
    put_call: str,
    spot: float,
) -> OptionQuote:
    if not quotes:
        raise ValueError(f"No {put_call} quotes available")

    with_delta = [q for q in quotes if abs(float(q.delta)) > 1e-6]
    if with_delta:
        best = min(
            with_delta,
            key=lambda q: (abs(abs(float(q.delta)) - target_delta), abs(float(q.strike) - spot)),
        )
        if abs(abs(float(best.delta)) - target_delta) > TARGET_DELTA_MAX_ERROR:
            raise ValueError(
                f"Delta match too far for {put_call}: target={target_delta:.2f} matched={best.delta:.4f}"
            )
        return best

    if put_call == "P":
        cands = [q for q in quotes if float(q.strike) <= spot]
        if not cands:
            cands = list(quotes)
        return max(cands, key=lambda q: float(q.strike))

    cands = [q for q in quotes if float(q.strike) >= spot]
    if not cands:
        cands = list(quotes)
    return min(cands, key=lambda q: float(q.strike))


def _rank_target_quotes(quotes: tuple[OptionQuote, ...], target_delta: float, spot: float) -> list[OptionQuote]:
    with_delta = [q for q in quotes if abs(float(q.delta)) > 1e-6]
    if with_delta:
        return sorted(
            with_delta,
            key=lambda q: (abs(abs(float(q.delta)) - target_delta), abs(float(q.strike) - spot)),
        )
    return sorted(quotes, key=lambda q: abs(float(q.strike) - spot))


def _find_wing_quote(quotes: tuple[OptionQuote, ...], target: float, direction: str) -> OptionQuote:
    if direction == "down":
        down = [q for q in quotes if float(q.strike) <= target + 1e-9]
        if down:
            return max(down, key=lambda q: float(q.strike))
    elif direction == "up":
        up = [q for q in quotes if float(q.strike) >= target - 1e-9]
        if up:
            return min(up, key=lambda q: float(q.strike))
    return min(quotes, key=lambda q: abs(float(q.strike) - target))


def _strictly_lower_quote(quotes: tuple[OptionQuote, ...], ref_strike: float) -> OptionQuote | None:
    cands = [q for q in quotes if float(q.strike) < ref_strike - 1e-9]
    if not cands:
        return None
    return max(cands, key=lambda q: float(q.strike))


def _strictly_upper_quote(quotes: tuple[OptionQuote, ...], ref_strike: float) -> OptionQuote | None:
    cands = [q for q in quotes if float(q.strike) > ref_strike + 1e-9]
    if not cands:
        return None
    return min(cands, key=lambda q: float(q.strike))


def _price_credit(short_q: OptionQuote, long_q: OptionQuote) -> dict:
    bid = float(short_q.bid) - float(long_q.ask)
    ask = float(short_q.ask) - float(long_q.bid)
    if ask < bid:
        ask = bid
    mid = (bid + ask) / 2.0
    half_spread = max(0.0, (ask - bid) / 2.0)
    fill = max(0.01, mid - (half_spread * FILL_HALF_SPREAD_FACTOR))
    return {
        "bid": round(bid, 4),
        "ask": round(ask, 4),
        "mid": round(mid, 4),
        "half_spread": round(half_spread, 4),
        "fill": round(fill, 4),
    }


def _price_debit(long_q: OptionQuote, short_q: OptionQuote) -> dict:
    bid = float(long_q.bid) - float(short_q.ask)
    ask = float(long_q.ask) - float(short_q.bid)
    if ask < bid:
        ask = bid
    mid = (bid + ask) / 2.0
    half_spread = max(0.0, (ask - bid) / 2.0)
    fill = max(0.01, mid + (half_spread * FILL_HALF_SPREAD_FACTOR))
    return {
        "bid": round(bid, 4),
        "ask": round(ask, 4),
        "mid": round(mid, 4),
        "half_spread": round(half_spread, 4),
        "fill": round(fill, 4),
    }


def _price_put_side(decision: Decision, chain: ChainSnapshot) -> dict:
    if decision.put_action == SideAction.NONE:
        return {
            "put_action_active": False,
            "put_entry_cashflow_points": 0.0,
            "put_legs": [],
            "put_entry_price_points": 0.0,
            "put_bid_points": 0.0,
            "put_ask_points": 0.0,
            "put_mid_points": 0.0,
            "put_fill_half_spread_points": 0.0,
            "put_width_actual": 0.0,
            "put_target_delta_hit": None,
            "put_target_strike": None,
            "put_lower_strike": None,
            "put_upper_strike": None,
            "put_long_strike": None,
            "put_short_strike": None,
        }

    if decision.put_width is None or decision.put_target_delta is None:
        raise ValueError("Put side missing width/target delta")

    target = float(decision.put_target_delta)
    width_req = float(decision.put_width)
    anchor = None
    for cand in _rank_target_quotes(chain.puts, target_delta=target, spot=chain.underlying_spx):
        wing = _find_wing_quote(chain.puts, target=float(cand.strike) - width_req, direction="down")
        if wing.strike < cand.strike:
            anchor = cand
            break
    if anchor is None:
        anchor = _find_target_quote(chain.puts, target_delta=target, put_call="P", spot=chain.underlying_spx)

    if decision.put_action == SideAction.SELL:
        short_q = anchor
        long_q = _find_wing_quote(chain.puts, target=float(short_q.strike) - width_req, direction="down")
        if long_q.strike >= short_q.strike:
            lower = _strictly_lower_quote(chain.puts, float(short_q.strike))
            if lower is not None:
                long_q = lower
        if long_q.strike >= short_q.strike:
            raise ValueError("Invalid put spread strikes for short put vertical")
        px = _price_credit(short_q, long_q)
        entry_cashflow = float(px["fill"])
        legs = [
            {"put_call": "P", "strike": float(short_q.strike), "sign": -1.0},
            {"put_call": "P", "strike": float(long_q.strike), "sign": 1.0},
        ]
        lower = float(long_q.strike)
        upper = float(short_q.strike)
        long_strike = lower
        short_strike = upper
    else:
        long_q = anchor
        short_q = _find_wing_quote(chain.puts, target=float(long_q.strike) - width_req, direction="down")
        if short_q.strike >= long_q.strike:
            lower = _strictly_lower_quote(chain.puts, float(long_q.strike))
            if lower is not None:
                short_q = lower
        if short_q.strike >= long_q.strike:
            raise ValueError("Invalid put spread strikes for long put vertical")
        px = _price_debit(long_q, short_q)
        entry_cashflow = -float(px["fill"])
        legs = [
            {"put_call": "P", "strike": float(long_q.strike), "sign": 1.0},
            {"put_call": "P", "strike": float(short_q.strike), "sign": -1.0},
        ]
        lower = float(short_q.strike)
        upper = float(long_q.strike)
        long_strike = upper
        short_strike = lower

    width_actual = upper - lower
    if width_actual <= 0:
        raise ValueError("Put side resolved to non-positive width")

    return {
        "put_action_active": True,
        "put_entry_cashflow_points": round(entry_cashflow, 4),
        "put_legs": legs,
        "put_entry_price_points": float(px["fill"]),
        "put_bid_points": float(px["bid"]),
        "put_ask_points": float(px["ask"]),
        "put_mid_points": float(px["mid"]),
        "put_fill_half_spread_points": float(px["half_spread"]),
        "put_width_actual": round(width_actual, 4),
        "put_target_delta_hit": float(anchor.delta),
        "put_target_strike": float(anchor.strike),
        "put_lower_strike": round(lower, 4),
        "put_upper_strike": round(upper, 4),
        "put_long_strike": round(long_strike, 4),
        "put_short_strike": round(short_strike, 4),
    }


def _price_call_side(decision: Decision, chain: ChainSnapshot) -> dict:
    if decision.call_action == SideAction.NONE:
        return {
            "call_action_active": False,
            "call_entry_cashflow_points": 0.0,
            "call_legs": [],
            "call_entry_price_points": 0.0,
            "call_bid_points": 0.0,
            "call_ask_points": 0.0,
            "call_mid_points": 0.0,
            "call_fill_half_spread_points": 0.0,
            "call_width_actual": 0.0,
            "call_target_delta_hit": None,
            "call_target_strike": None,
            "call_lower_strike": None,
            "call_upper_strike": None,
            "call_long_strike": None,
            "call_short_strike": None,
        }

    if decision.call_width is None or decision.call_target_delta is None:
        raise ValueError("Call side missing width/target delta")

    target = float(decision.call_target_delta)
    width_req = float(decision.call_width)
    anchor = None
    for cand in _rank_target_quotes(chain.calls, target_delta=target, spot=chain.underlying_spx):
        wing = _find_wing_quote(chain.calls, target=float(cand.strike) + width_req, direction="up")
        if wing.strike > cand.strike:
            anchor = cand
            break
    if anchor is None:
        anchor = _find_target_quote(chain.calls, target_delta=target, put_call="C", spot=chain.underlying_spx)

    if decision.call_action == SideAction.SELL:
        short_q = anchor
        long_q = _find_wing_quote(chain.calls, target=float(short_q.strike) + width_req, direction="up")
        if long_q.strike <= short_q.strike:
            upper = _strictly_upper_quote(chain.calls, float(short_q.strike))
            if upper is not None:
                long_q = upper
        if long_q.strike <= short_q.strike:
            raise ValueError("Invalid call spread strikes for short call vertical")
        px = _price_credit(short_q, long_q)
        entry_cashflow = float(px["fill"])
        legs = [
            {"put_call": "C", "strike": float(short_q.strike), "sign": -1.0},
            {"put_call": "C", "strike": float(long_q.strike), "sign": 1.0},
        ]
        lower = float(short_q.strike)
        upper = float(long_q.strike)
        long_strike = upper
        short_strike = lower
    else:
        long_q = anchor
        short_q = _find_wing_quote(chain.calls, target=float(long_q.strike) + width_req, direction="up")
        if short_q.strike <= long_q.strike:
            upper = _strictly_upper_quote(chain.calls, float(long_q.strike))
            if upper is not None:
                short_q = upper
        if short_q.strike <= long_q.strike:
            raise ValueError("Invalid call spread strikes for long call vertical")
        px = _price_debit(long_q, short_q)
        entry_cashflow = -float(px["fill"])
        legs = [
            {"put_call": "C", "strike": float(long_q.strike), "sign": 1.0},
            {"put_call": "C", "strike": float(short_q.strike), "sign": -1.0},
        ]
        lower = float(long_q.strike)
        upper = float(short_q.strike)
        long_strike = lower
        short_strike = upper

    width_actual = upper - lower
    if width_actual <= 0:
        raise ValueError("Call side resolved to non-positive width")

    return {
        "call_action_active": True,
        "call_entry_cashflow_points": round(entry_cashflow, 4),
        "call_legs": legs,
        "call_entry_price_points": float(px["fill"]),
        "call_bid_points": float(px["bid"]),
        "call_ask_points": float(px["ask"]),
        "call_mid_points": float(px["mid"]),
        "call_fill_half_spread_points": float(px["half_spread"]),
        "call_width_actual": round(width_actual, 4),
        "call_target_delta_hit": float(anchor.delta),
        "call_target_strike": float(anchor.strike),
        "call_lower_strike": round(lower, 4),
        "call_upper_strike": round(upper, 4),
        "call_long_strike": round(long_strike, 4),
        "call_short_strike": round(short_strike, 4),
    }


def _empty_pricing_meta(chain: ChainSnapshot) -> dict:
    return {
        "pricing_model": "chain_intrinsic_v1",
        "fill_model": "conservative_mid",
        "fill_half_spread_factor": float(FILL_HALF_SPREAD_FACTOR),
        "chain_asof_utc": chain.asof_utc.isoformat(),
        "chain_underlying_spx": round(float(chain.underlying_spx), 4),
        "settlement_model": "expiry_spx_close",
        "put_entry_cashflow_points": 0.0,
        "call_entry_cashflow_points": 0.0,
        "put_entry_price_points": 0.0,
        "call_entry_price_points": 0.0,
        "put_bid_points": 0.0,
        "put_ask_points": 0.0,
        "put_mid_points": 0.0,
        "put_fill_half_spread_points": 0.0,
        "call_bid_points": 0.0,
        "call_ask_points": 0.0,
        "call_mid_points": 0.0,
        "call_fill_half_spread_points": 0.0,
        "put_width_actual": 0.0,
        "call_width_actual": 0.0,
        "put_target_delta_hit": None,
        "call_target_delta_hit": None,
        "put_target_strike": None,
        "call_target_strike": None,
        "put_lower_strike": None,
        "put_upper_strike": None,
        "put_long_strike": None,
        "put_short_strike": None,
        "call_lower_strike": None,
        "call_upper_strike": None,
        "call_long_strike": None,
        "call_short_strike": None,
        "unit_max_loss": 0.0,
    }


def price_decision(decision: Decision, chain: ChainSnapshot) -> dict:
    out = _empty_pricing_meta(chain)
    put_meta = _price_put_side(decision, chain)
    call_meta = _price_call_side(decision, chain)
    out.update(put_meta)
    out.update(call_meta)

    legs = list(put_meta["put_legs"]) + list(call_meta["call_legs"])
    entry_cashflow_points = float(put_meta["put_entry_cashflow_points"]) + float(
        call_meta["call_entry_cashflow_points"]
    )

    worst_points = _worst_case_pnl_points(entry_cashflow_points=entry_cashflow_points, legs=legs, spot_hint=chain.underlying_spx)
    per_unit_fees = (
        executed_legs(decision.put_action, size=1) + executed_legs(decision.call_action, size=1)
    ) * COMMISSION_PER_LEG_DOLLARS
    unit_max_loss = max(0.0, -worst_points * 100.0) + per_unit_fees
    out["unit_max_loss"] = round(unit_max_loss, 2)
    return out


def _side_gross_points(
    action: SideAction,
    put_call: str,
    long_strike: float | None,
    short_strike: float | None,
    entry_price_points: float,
    settlement_spx: float,
) -> float:
    if action == SideAction.NONE:
        return 0.0
    if long_strike is None or short_strike is None:
        return 0.0

    long_iv = _intrinsic(put_call, float(long_strike), settlement_spx)
    short_iv = _intrinsic(put_call, float(short_strike), settlement_spx)
    if action == SideAction.SELL:
        return float(entry_price_points) - (short_iv - long_iv)
    return (long_iv - short_iv) - float(entry_price_points)


def score_decision(decision_payload: dict, settlement_spx: float, size: int) -> tuple[float, float, float, float, float]:
    put_action = SideAction(str(decision_payload.get("put_action", "none")).lower())
    call_action = SideAction(str(decision_payload.get("call_action", "none")).lower())

    put_gross_pts = _side_gross_points(
        action=put_action,
        put_call="P",
        long_strike=_to_float_or_none(decision_payload.get("put_long_strike")),
        short_strike=_to_float_or_none(decision_payload.get("put_short_strike")),
        entry_price_points=float(decision_payload.get("put_entry_price_points", 0.0) or 0.0),
        settlement_spx=settlement_spx,
    )
    call_gross_pts = _side_gross_points(
        action=call_action,
        put_call="C",
        long_strike=_to_float_or_none(decision_payload.get("call_long_strike")),
        short_strike=_to_float_or_none(decision_payload.get("call_short_strike")),
        entry_price_points=float(decision_payload.get("call_entry_price_points", 0.0) or 0.0),
        settlement_spx=settlement_spx,
    )

    put_gross = put_gross_pts * 100.0 * float(size)
    call_gross = call_gross_pts * 100.0 * float(size)
    put_fees = executed_legs(put_action, size) * COMMISSION_PER_LEG_DOLLARS
    call_fees = executed_legs(call_action, size) * COMMISSION_PER_LEG_DOLLARS

    put_pnl = put_gross - put_fees
    call_pnl = call_gross - call_fees
    gross_total = put_gross + call_gross
    fees = put_fees + call_fees
    total = gross_total - fees
    return (
        round(put_pnl, 2),
        round(call_pnl, 2),
        round(gross_total, 2),
        round(fees, 2),
        round(total, 2),
    )


def _to_float_or_none(v) -> float | None:
    try:
        if v in (None, ""):
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def executed_legs(action: SideAction, size: int) -> int:
    if action == SideAction.NONE:
        return 0
    return 2 * int(size)


def validate_risk_defined_structure(decision: Decision) -> tuple[bool, str]:
    if decision.active_sides() == 0:
        return True, ""
    if decision.put_action != SideAction.NONE:
        if decision.put_width is None:
            return False, "put side must use a defined vertical width"
        if decision.put_target_delta is None:
            return False, "put side must define target delta"
    if decision.call_action != SideAction.NONE:
        if decision.call_width is None:
            return False, "call side must use a defined vertical width"
        if decision.call_target_delta is None:
            return False, "call side must define target delta"
    return True, ""


def _json_load(s: str) -> dict:
    import json

    try:
        v = json.loads(s)
        return v if isinstance(v, dict) else {}
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
