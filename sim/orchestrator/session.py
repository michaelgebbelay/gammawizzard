"""Dual-window session lifecycle (v13).

Session flow:
  PRE-MARKET   → Settle 1DTE positions from prior session
  OPEN window  → 0DTE: agents/baselines decide → broker fills → features
  MID          → Mark all open positions (observation only)
  CLOSE        → Settle 0DTE positions → final marks → close FeaturePack
  CLOSE+5      → 1DTE: agents/baselines decide → broker fills → features
  SCORING      → Judge scorecards → rubric evolution → memory compress
  END          → Risk-free accrual → account snapshots
"""

from __future__ import annotations

import json
import logging
from typing import Dict, List, Optional

import anthropic

from sim.config import (
    RISK_FREE_RATE_ANNUAL,
    RNG_SEED,
    TRACK_ADAPTIVE,
    TRACK_CLEAN,
    TRACK_FROZEN,
    WINDOW_OPEN,
    WINDOW_CLOSE5,
)
from sim.data.chain_snapshot import ChainSnapshot
from sim.data.features import enrich, save_feature_pack, FeaturePack
from sim.data.gw_client import load_gw_data, gate_gw_for_window
from sim.engine.account import Account
from sim.engine.order import Order
from sim.engine.paper_broker import FillResult, PaperBroker
from sim.engine.settlement import settle_0dte_positions, settle_1dte_positions
from sim.agents.agent_registry import get_agent_configs
from sim.agents.claude_agent import ClaudeAgent
from sim.agents.memory import build_session_record, compress_session_memory
from sim.agents.prompts.market_context import (
    format_account_context,
    format_chain_context,
    format_positions_context,
)
from sim.baselines import HoldCash, MechanicalIC, RandomEntry, RegimeBot
from sim.judge.judge_agent import JudgeAgent
from sim.judge.rubric import Rubric
from sim.persistence import queries
from sim.persistence.db import init_db

logger = logging.getLogger(__name__)

DAILY_RISK_FREE = RISK_FREE_RATE_ANNUAL / 252


class SessionRunner:
    """Runs a single session (dual-window) for one track."""

    def __init__(self, track: str,
                 anthropic_client: Optional[anthropic.Anthropic] = None,
                 db_path=None):
        self.track = track
        self.client = anthropic_client or anthropic.Anthropic()
        self.conn = init_db(db_path) if db_path else init_db()
        self.broker = PaperBroker(rng_seed=RNG_SEED)
        self.judge = JudgeAgent(client=self.client)

        # Initialize AI agents
        self.agents: Dict[str, ClaudeAgent] = {}
        for agent_id, cfg in get_agent_configs().items():
            self.agents[agent_id] = ClaudeAgent(
                agent_id=agent_id,
                model=cfg["model"],
                personality_seed=cfg["seed"],
                client=self.client,
            )

        # Initialize baselines
        self.baselines = [
            MechanicalIC(),
            RandomEntry(seed=RNG_SEED),
            HoldCash(),
            RegimeBot(),
        ]

        # Account state (loaded/created per session)
        self.accounts: Dict[str, Account] = {}

    def run_session(self, session_id: int, trading_date: str,
                    open_chain: ChainSnapshot,
                    close_chain: ChainSnapshot,
                    close5_chain: Optional[ChainSnapshot] = None,
                    prior_spx_close: Optional[float] = None,
                    rubric: Optional[Rubric] = None,
                    mid_chain: Optional[ChainSnapshot] = None) -> dict:
        """Execute a complete dual-window session lifecycle.

        Args:
            session_id: Session number.
            trading_date: ISO date string.
            open_chain: 0DTE chain snapshot at OPEN (9:31 AM).
            close_chain: Chain snapshot at CLOSE (4:00 PM).
            close5_chain: 1DTE chain snapshot at CLOSE+5 (4:05 PM).
                          If None, CLOSE+5 window is skipped.
            prior_spx_close: SPX close from prior session (for 1DTE settlement).
            rubric: Current scoring rubric.
            mid_chain: Optional MID chain for marking.

        Returns:
            Session summary dict.
        """
        if rubric is None:
            rubric = Rubric()

        all_participants = list(self.agents.keys()) + [b.agent_id for b in self.baselines]
        self._ensure_accounts(all_participants)
        pc = prior_spx_close if prior_spx_close else 0.0

        logger.info("=== Session %d | Track: %s | Date: %s ===",
                     session_id, self.track, trading_date)

        # ===============================================================
        # PRE-MARKET: Settle 1DTE positions from prior session
        # ===============================================================
        if prior_spx_close is not None:
            self._settle_1dte_phase(session_id, prior_spx_close)

        # Record session
        queries.insert_session(
            self.conn, session_id, self.track, trading_date,
            spx_open=open_chain.underlying_price,
            vix_open=open_chain.vix,
        )

        # ===============================================================
        # OPEN WINDOW: 0DTE decisions
        # ===============================================================
        gw_open_raw = load_gw_data(trading_date, "open")
        gw_open = gate_gw_for_window(gw_open_raw, window=WINDOW_OPEN, session_date=trading_date)
        open_features = enrich(open_chain, prev_close=pc, gw_data=gw_open, window=WINDOW_OPEN)
        save_feature_pack(trading_date, "open", open_features)
        queries.save_session_features(
            self.conn, session_id, WINDOW_OPEN,
            open_features.expiration,
            json.dumps(open_features.to_dict()),
        )
        logger.info("[OPEN] FeaturePack: gap=%+.1fpts EM=%.1fpts ATM_IV=%.1f%% VIX1D=%s",
                     open_features.gap_pts, open_features.atm_straddle_mid,
                     open_features.iv_atm, open_features.vix_1d)

        # Judge brief (OPEN window)
        judge_brief_open = ""
        if self.track in (TRACK_ADAPTIVE, TRACK_FROZEN):
            judge_brief_open = self.judge.generate_brief(open_chain)
            logger.info("[OPEN] Judge brief: %s", judge_brief_open[:100])

        # AI agents decide (0DTE)
        open_agent_results = {}
        for agent_id, agent in self.agents.items():
            result = self._run_agent(
                agent, session_id, open_chain, judge_brief_open, rubric,
                window=WINDOW_OPEN, dte=0,
            )
            open_agent_results[agent_id] = result

        # Baselines decide (0DTE)
        open_baseline_results = {}
        for bot in self.baselines:
            if isinstance(bot, RegimeBot) and prior_spx_close is not None:
                bot.set_prior_close(prior_spx_close)
            result = self._run_baseline(bot, session_id, open_chain,
                                        window=WINDOW_OPEN, dte=0)
            open_baseline_results[bot.agent_id] = result

        # ===============================================================
        # MID: Mark positions (observation only)
        # ===============================================================
        if mid_chain:
            self._mark_phase(session_id, mid_chain, "mid")

        # ===============================================================
        # CLOSE: Settle 0DTE positions + final marks
        # ===============================================================
        spx_close = close_chain.underlying_price
        self._settle_0dte_phase(session_id, spx_close)
        self._mark_phase(session_id, close_chain, "close")

        # Close FeaturePack
        gw_close_raw = load_gw_data(trading_date, "close")
        gw_close = gate_gw_for_window(gw_close_raw, window=WINDOW_CLOSE5, session_date=trading_date)
        close_features = enrich(close_chain, prev_close=pc, gw_data=gw_close, window="close")
        save_feature_pack(trading_date, "close", close_features)
        queries.save_session_features(
            self.conn, session_id, "close",
            close_features.expiration,
            json.dumps(close_features.to_dict()),
        )

        # Update session record
        intraday_range = abs(spx_close - open_chain.underlying_price)
        queries.update_session_close(
            self.conn, session_id,
            spx_close=spx_close,
            vix_close=close_chain.vix,
            intraday_range=intraday_range,
        )

        # ===============================================================
        # CLOSE+5 WINDOW: 1DTE decisions (skip if no close5 chain)
        # ===============================================================
        close5_agent_results = {}
        close5_baseline_results = {}

        if close5_chain is not None:
            gw_close5_raw = load_gw_data(trading_date, "close5")
            gw_close5 = gate_gw_for_window(gw_close5_raw, window=WINDOW_CLOSE5,
                                           session_date=trading_date)
            close5_features = enrich(close5_chain, prev_close=pc, gw_data=gw_close5,
                                     window=WINDOW_CLOSE5)
            save_feature_pack(trading_date, "close5", close5_features)
            queries.save_session_features(
                self.conn, session_id, WINDOW_CLOSE5,
                close5_features.expiration,
                json.dumps(close5_features.to_dict()),
            )
            logger.info("[CLOSE+5] FeaturePack: EM=%.1fpts ATM_IV=%.1f%% VIX1D=%s",
                         close5_features.atm_straddle_mid,
                         close5_features.iv_atm, close5_features.vix_1d)

            # Judge brief (CLOSE+5 window)
            judge_brief_close5 = ""
            if self.track in (TRACK_ADAPTIVE, TRACK_FROZEN):
                judge_brief_close5 = self.judge.generate_brief(close5_chain)

            # AI agents decide (1DTE)
            for agent_id, agent in self.agents.items():
                result = self._run_agent(
                    agent, session_id, close5_chain, judge_brief_close5, rubric,
                    window=WINDOW_CLOSE5, dte=1,
                )
                close5_agent_results[agent_id] = result

            # Baselines decide (1DTE)
            for bot in self.baselines:
                result = self._run_baseline(bot, session_id, close5_chain,
                                            window=WINDOW_CLOSE5, dte=1)
                close5_baseline_results[bot.agent_id] = result

        # ===============================================================
        # SCORING: Judge scorecards (covers both windows)
        # ===============================================================
        all_agent_results = {
            **{f"{k}/open": v for k, v in open_agent_results.items()},
            **{f"{k}/close5": v for k, v in close5_agent_results.items()},
        }
        all_baseline_results = {
            **{f"{k}/open": v for k, v in open_baseline_results.items()},
            **{f"{k}/close5": v for k, v in close5_baseline_results.items()},
        }

        scores_summary = {}
        if self.track != TRACK_CLEAN:
            # Score per-participant (aggregate across both windows)
            scores_summary = self._scoring_phase(
                session_id, rubric, open_chain, close_chain,
                open_agent_results, open_baseline_results,
                close5_agent_results, close5_baseline_results,
                open_features=open_features,
            )

        # Risk-free accrual
        for agent_id in all_participants:
            self.accounts[agent_id].accrue_risk_free(DAILY_RISK_FREE)

        # Account snapshots
        for agent_id in all_participants:
            queries.save_account_snapshot(
                self.conn, agent_id, session_id, self.track,
                self.accounts[agent_id],
            )

        # Memory + rubric (adaptive track)
        if self.track == TRACK_ADAPTIVE:
            self._memory_phase(session_id, open_agent_results,
                               close5_agent_results, open_chain, close5_chain)

        new_rubric = rubric
        if self.track == TRACK_ADAPTIVE and scores_summary:
            new_rubric = self._evolve_rubric(session_id, rubric, scores_summary)

        logger.info("=== Session %d complete ===", session_id)

        return {
            "session_id": session_id,
            "track": self.track,
            "trading_date": trading_date,
            "spx_open": open_chain.underlying_price,
            "spx_close": spx_close,
            "vix_open": open_chain.vix,
            "open_agent_results": open_agent_results,
            "open_baseline_results": open_baseline_results,
            "close5_agent_results": close5_agent_results,
            "close5_baseline_results": close5_baseline_results,
            "scores": scores_summary,
            "rubric": new_rubric,
        }

    # -------------------------------------------------------------------
    # Account setup
    # -------------------------------------------------------------------

    def _ensure_accounts(self, agent_ids: List[str]) -> None:
        """Create accounts for any new participants."""
        for agent_id in agent_ids:
            if agent_id not in self.accounts:
                self.accounts[agent_id] = Account(agent_id=agent_id)

    # -------------------------------------------------------------------
    # Settlement
    # -------------------------------------------------------------------

    def _settle_1dte_phase(self, session_id: int, spx_close: float) -> None:
        """Settle 1DTE positions from prior session at prior close."""
        for agent_id, acct in self.accounts.items():
            settled = settle_1dte_positions(
                acct.open_positions, session_id, spx_close,
                settlement_source="official_close",
            )
            for pos in settled:
                acct.book_settlement(pos)
                queries.update_position_settlement(self.conn, pos)
                logger.info("[%s] settled 1DTE %s: P&L=$%.2f",
                            agent_id, pos.structure.value, pos.realized_pnl)

    def _settle_0dte_phase(self, session_id: int, spx_close: float) -> None:
        """Settle 0DTE positions opened this session at today's close."""
        for agent_id, acct in self.accounts.items():
            settled = settle_0dte_positions(
                acct.open_positions, session_id, spx_close,
                settlement_source="official_close",
            )
            for pos in settled:
                acct.book_settlement(pos)
                queries.update_position_settlement(self.conn, pos)
                logger.info("[%s] settled 0DTE %s: P&L=$%.2f",
                            agent_id, pos.structure.value, pos.realized_pnl)

    # -------------------------------------------------------------------
    # Agent / baseline execution
    # -------------------------------------------------------------------

    def _run_agent(self, agent: ClaudeAgent, session_id: int,
                   chain: ChainSnapshot, judge_brief: str,
                   rubric: Rubric,
                   window: str = WINDOW_OPEN,
                   dte: int = 0) -> dict:
        """Run a single AI agent's decision for one window."""
        acct = self.accounts[agent.agent_id]

        # Load memory (adaptive track only)
        memory = None
        if self.track == TRACK_ADAPTIVE:
            memory = queries.get_latest_memory(
                self.conn, agent.agent_id, self.track
            )

        order, raw_response = agent.decide(
            chain, acct, session_id, self.track,
            judge_brief=judge_brief,
            memory=memory,
            window=window,
            dte=dte,
        )

        result = {
            "agent_id": agent.agent_id,
            "window": window,
            "dte": dte,
            "raw_response": raw_response,
            "action": "hold" if order is None else "trade",
        }

        if order is None:
            queries.insert_action(
                self.conn, agent.agent_id, session_id, self.track,
                action_type="hold",
                reasoning=raw_response[:500],
            )
            logger.info("[%s][%s] HOLD", agent.agent_id, window)
            return result

        # Stamp window/dte on the order before submission
        order.window = window
        order.dte_at_entry = dte
        if chain.expirations:
            order.expiration = chain.expirations[0].isoformat()

        # Submit order through broker
        fill = self.broker.submit_order(
            order, acct, chain, self.track, session_id,
        )

        result["order"] = {
            "structure": order.structure.value,
            "strikes": [l.strike for l in order.legs],
            "quantity": order.quantity,
            "thesis": order.thesis,
            "window": window,
            "dte": dte,
        }
        result["filled"] = fill.filled
        result["fill_price"] = fill.fill_price
        result["commission"] = fill.commission
        result["slippage"] = fill.slippage_applied
        result["rejection_reason"] = fill.rejection_reason

        queries.insert_order(self.conn, order, session_id, self.track,
                             slippage=fill.slippage_applied)

        if fill.filled and fill.position:
            queries.insert_position(self.conn, fill.position)
            logger.info("[%s][%s] FILLED %s @ $%.2f (slippage=$%.2f, comm=$%.2f)",
                        agent.agent_id, window, order.structure.value,
                        fill.fill_price, fill.slippage_applied, fill.commission)
        else:
            queries.insert_action(
                self.conn, agent.agent_id, session_id, self.track,
                action_type="rejected",
                details=order.structure.value,
                failure_reason=fill.rejection_reason,
            )
            logger.info("[%s][%s] REJECTED: %s", agent.agent_id, window, fill.rejection_reason)

        return result

    def _run_baseline(self, bot, session_id: int,
                      chain: ChainSnapshot,
                      window: str = WINDOW_OPEN,
                      dte: int = 0) -> dict:
        """Run a single baseline bot's decision for one window."""
        acct = self.accounts[bot.agent_id]
        order = bot.decide(chain, acct, session_id, self.track)

        result = {
            "agent_id": bot.agent_id,
            "window": window,
            "dte": dte,
            "action": "hold" if order is None else "trade",
        }

        if order is None:
            queries.insert_action(
                self.conn, bot.agent_id, session_id, self.track,
                action_type="hold",
                reasoning="Baseline: hold cash.",
            )
            logger.info("[%s][%s] HOLD", bot.agent_id, window)
            return result

        # Stamp window/dte on the order
        order.window = window
        order.dte_at_entry = dte
        if chain.expirations:
            order.expiration = chain.expirations[0].isoformat()

        fill = self.broker.submit_order(
            order, acct, chain, self.track, session_id,
        )

        result["order"] = {
            "structure": order.structure.value,
            "strikes": [l.strike for l in order.legs],
            "quantity": order.quantity,
            "thesis": order.thesis,
            "window": window,
            "dte": dte,
        }
        result["filled"] = fill.filled
        result["fill_price"] = fill.fill_price
        result["commission"] = fill.commission

        queries.insert_order(self.conn, order, session_id, self.track,
                             slippage=fill.slippage_applied)

        if fill.filled and fill.position:
            queries.insert_position(self.conn, fill.position)
            logger.info("[%s][%s] FILLED %s @ $%.2f",
                        bot.agent_id, window, order.structure.value, fill.fill_price)
        else:
            logger.info("[%s][%s] REJECTED: %s", bot.agent_id, window, fill.rejection_reason)

        return result

    # -------------------------------------------------------------------
    # Marking
    # -------------------------------------------------------------------

    def _mark_phase(self, session_id: int, chain: ChainSnapshot,
                    phase: str) -> None:
        """Mark all open positions at current chain prices."""
        from sim.engine.marking import spread_nbbo, mark_position

        expiration = chain.expirations[0] if chain.expirations else None

        for agent_id, acct in self.accounts.items():
            for pos in acct.open_positions:
                bid, ask, mid = spread_nbbo(pos.legs, chain, expiration)
                if mid is not None:
                    unrealized = mark_position(
                        pos.entry_price, mid, pos.side, pos.quantity
                    )
                    queries.insert_position_mark(
                        self.conn, pos.position_id, session_id, phase,
                        mark_price=mid, unrealized_pnl=unrealized,
                    )

    # -------------------------------------------------------------------
    # Scoring
    # -------------------------------------------------------------------

    def _scoring_phase(self, session_id: int, rubric: Rubric,
                       open_chain: ChainSnapshot, close_chain: ChainSnapshot,
                       open_agent_results: dict, open_baseline_results: dict,
                       close5_agent_results: dict = None,
                       close5_baseline_results: dict = None,
                       open_features: Optional[FeaturePack] = None) -> dict:
        """Have the judge score all participants across both windows."""
        close5_agent_results = close5_agent_results or {}
        close5_baseline_results = close5_baseline_results or {}

        market_ctx = format_chain_context(open_chain, feature_pack=open_features)
        scores = {}

        # Merge results per participant
        all_open = {**open_agent_results, **open_baseline_results}
        all_close5 = {**close5_agent_results, **close5_baseline_results}

        # Score per participant
        for agent_id in set(list(all_open.keys()) + list(all_close5.keys())):
            open_result = all_open.get(agent_id, {})
            close5_result = all_close5.get(agent_id, {})

            # Build decision summary across both windows
            decision_parts = []
            for label, result in [("OPEN/0DTE", open_result), ("CLOSE+5/1DTE", close5_result)]:
                if not result:
                    continue
                action = result.get("action", "hold")
                if action == "trade" and result.get("filled"):
                    decision_parts.append(
                        f"{label}: {result.get('order', {}).get('structure', '?')} "
                        f"@ ${result.get('fill_price', 0):.2f}"
                    )
                elif action == "trade":
                    decision_parts.append(f"{label}: REJECTED ({result.get('rejection_reason', '')})")
                else:
                    decision_parts.append(f"{label}: HOLD")

            agent_decision = " | ".join(decision_parts) if decision_parts else "HOLD both windows"

            fill_info_parts = []
            for result in [open_result, close5_result]:
                if result.get("action") == "trade":
                    fill_info_parts.append(
                        f"Filled: {result.get('filled', False)}, "
                        f"Price: ${result.get('fill_price', 0):.2f}, "
                        f"Commission: ${result.get('commission', 0):.2f}"
                    )
            fill_info = " | ".join(fill_info_parts) if fill_info_parts else "No trades placed."

            acct = self.accounts[agent_id]
            account_ctx = format_account_context(acct)

            settled = [p for p in acct.settled_positions
                       if p.session_settled == session_id]
            settlement_info = ", ".join(
                f"{p.structure.value}({p.window}): P&L=${p.realized_pnl:+.2f}"
                for p in settled
            ) if settled else "No settlements this session."

            score = self.judge.score_agent(
                agent_id=agent_id,
                session_id=session_id,
                rubric=rubric,
                market_context=market_ctx,
                agent_decision=agent_decision[:500],
                fill_result=fill_info,
                settlement_result=settlement_info,
                account_state=account_ctx,
            )

            scores[agent_id] = score
            queries.insert_scorecard(
                self.conn, agent_id, session_id, self.track, score,
                notes=score.get("notes", ""),
            )

        return scores

    # -------------------------------------------------------------------
    # Memory
    # -------------------------------------------------------------------

    def _memory_phase(self, session_id: int,
                      open_agent_results: dict,
                      close5_agent_results: dict,
                      open_chain: ChainSnapshot,
                      close5_chain: Optional[ChainSnapshot]) -> None:
        """Compress and save session memory for adaptive track agents."""
        for agent_id in self.agents:
            prior = queries.get_latest_memory(self.conn, agent_id, self.track)
            prior_text = prior["cumulative_summary"] if prior else ""

            open_result = open_agent_results.get(agent_id, {})
            close5_result = close5_agent_results.get(agent_id, {})

            # Build combined record from both windows
            actions = []
            for label, result in [("open/0DTE", open_result), ("close5/1DTE", close5_result)]:
                action = result.get("action", "hold")
                order = result.get("order")
                thesis = order.get("thesis", "") if order else ""
                actions.append(f"{label}: {action}" + (f" — {thesis}" if thesis else ""))

            market_ctx = f"SPX={open_chain.underlying_price}, VIX={open_chain.vix}"
            if close5_chain:
                market_ctx += f" | CLOSE SPX={close5_chain.underlying_price}"

            record = build_session_record(
                action=" | ".join(actions),
                order=open_result.get("order") or close5_result.get("order"),
                thesis="",
                account_state=self.accounts[agent_id].to_dict(),
                market_context=market_ctx,
            )

            summary = compress_session_memory(
                self.client, record, prior_text,
            )

            queries.save_memory(
                self.conn, agent_id, session_id, self.track,
                summary=summary,
                cumulative=summary,
            )

    # -------------------------------------------------------------------
    # Rubric evolution
    # -------------------------------------------------------------------

    def _evolve_rubric(self, session_id: int, rubric: Rubric,
                       scores: dict) -> Rubric:
        """Evolve rubric weights based on score variance."""
        perf_data = {}
        for dim in rubric.weights:
            dim_scores = [s.get(dim, 5) for s in scores.values()
                          if isinstance(s.get(dim), (int, float))]
            perf_data[dim] = dim_scores

        new_rubric = rubric.evolve(session_id, perf_data)
        queries.save_rubric(
            self.conn, session_id, self.track,
            new_rubric.weights, new_rubric.rationale,
        )
        logger.info("Rubric evolved: %s", new_rubric.weights)
        return new_rubric
