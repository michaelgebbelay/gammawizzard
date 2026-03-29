"""Single-window 1DTE session lifecycle (v14).

Session flow:
  1. SETTLE: settle 1DTE positions from prior session using today's SPX close
  2. FEATURES: enrich chain (close5 phase) with GW data
  3. DECIDE: all 4 AI agents + 6 baselines make decisions
  4. RECORD: save accounts, update agent memory, diagnostics
"""

from __future__ import annotations

import json
import logging
from typing import Dict, List, Optional

from sim.config import (
    RISK_FREE_RATE_ANNUAL,
    RNG_SEED,
)
from sim.data.chain_snapshot import ChainSnapshot
from sim.data.features import enrich, save_feature_pack
from sim.data.gw_client import load_gw_data
from sim.engine.account import Account
from sim.engine.order import Order
from sim.engine.paper_broker import FillResult, PaperBroker
from sim.engine.settlement import settle_prior_positions
from sim.agents.agent_registry import get_agent_configs
from sim.agents.claude_agent import ClaudeAgent
from sim.agents.openai_agent import OpenAIAgent
from sim.agents.base_agent import BaseAgent
from sim.agents.memory import build_session_record, update_memory, format_memory
from sim.baselines import (
    HoldCash, NarrowIC, WideIC, DirectionalPut, IronFly,
    DynamicButterfly, DynamicButterflyVix1d,
)
from sim.persistence import queries
from sim.persistence.db import init_db

logger = logging.getLogger(__name__)

DAILY_RISK_FREE = RISK_FREE_RATE_ANNUAL / 252


class SessionRunner:
    """Runs a single 1DTE session for all participants."""

    def __init__(self, anthropic_client=None, openai_client=None,
                 db_path=None):
        self.conn = init_db(db_path) if db_path else init_db()
        self.broker = PaperBroker(rng_seed=RNG_SEED)

        # Initialize AI agents based on provider (skip if no client provided)
        self.agents: Dict[str, BaseAgent] = {}
        for agent_id, cfg in get_agent_configs().items():
            if cfg["provider"] == "anthropic" and anthropic_client is not None:
                self.agents[agent_id] = ClaudeAgent(
                    agent_id=agent_id,
                    model=cfg["model"],
                    trained=cfg.get("trained", False),
                    client=anthropic_client,
                )
            elif cfg["provider"] == "openai" and openai_client is not None:
                self.agents[agent_id] = OpenAIAgent(
                    agent_id=agent_id,
                    model=cfg["model"],
                    trained=cfg.get("trained", False),
                    client=openai_client,
                )

        if not self.agents:
            logger.warning("No AI agents initialized (missing API keys). Running baselines only.")

        # Initialize baselines
        self.baselines = [
            NarrowIC(),
            WideIC(),
            DirectionalPut(),
            IronFly(),
            DynamicButterfly(),
            DynamicButterflyVix1d(),
            HoldCash(),
        ]

        # Account state (loaded/created per session)
        self.accounts: Dict[str, Account] = {}

    def run_session(self, session_id: int, trading_date: str,
                    chain: ChainSnapshot,
                    prior_spx_close: Optional[float] = None) -> dict:
        """Execute a complete 1DTE session lifecycle.

        Args:
            session_id: Session number.
            trading_date: ISO date string.
            chain: 1DTE chain snapshot at CLOSE+5 (4:05 PM).
            prior_spx_close: SPX close from prior session (for settlement).

        Returns:
            Session summary dict.
        """
        all_participants = list(self.agents.keys()) + [b.agent_id for b in self.baselines]
        self._ensure_accounts(all_participants)

        logger.info("=== Session %d | Date: %s ===", session_id, trading_date)

        # Record session first (needed for FK references during settlement)
        queries.insert_session(
            self.conn, session_id, trading_date,
            spx_open=chain.spx_open if chain.spx_open > 0 else chain.underlying_price,
            vix_open=chain.vix,
        )

        # ===================================================================
        # 1. SETTLE: settle 1DTE positions from prior session
        # ===================================================================
        if prior_spx_close is not None:
            self._settle_phase(session_id, prior_spx_close)

        # ===================================================================
        # 2. FEATURES: enrich chain with GW data
        # ===================================================================
        gw_data = load_gw_data(trading_date, "close5")
        features = enrich(chain, prev_close=prior_spx_close or 0.0,
                          gw_data=gw_data, window="close5")
        save_feature_pack(trading_date, "close5", features)
        queries.save_session_features(
            self.conn, session_id, "close5",
            features.expiration,
            json.dumps(features.to_dict()),
        )
        logger.info("[CLOSE5] FeaturePack: EM=%.1fpts ATM_IV=%.1f%% VIX1D=%s",
                     features.atm_straddle_mid, features.iv_atm, features.vix_1d)

        # ===================================================================
        # 3. DECIDE: all agents and baselines
        # ===================================================================
        agent_results = {}
        for agent_id, agent in self.agents.items():
            result = self._run_agent(agent, session_id, chain)
            agent_results[agent_id] = result

        baseline_results = {}
        for bot in self.baselines:
            result = self._run_baseline(bot, session_id, chain)
            baseline_results[bot.agent_id] = result

        # ===================================================================
        # 4. RECORD: risk-free accrual, account snapshots, session close
        # ===================================================================
        # Update session close info
        spx_close = chain.underlying_price
        intraday_range = abs(chain.spx_high - chain.spx_low) if chain.spx_high > 0 else 0.0
        queries.update_session_close(
            self.conn, session_id,
            spx_close=spx_close,
            vix_close=chain.vix,
            intraday_range=intraday_range,
        )

        # Risk-free accrual
        for agent_id in all_participants:
            self.accounts[agent_id].accrue_risk_free(DAILY_RISK_FREE)

        # Account snapshots
        for agent_id in all_participants:
            queries.save_account_snapshot(
                self.conn, agent_id, session_id,
                self.accounts[agent_id],
            )

        logger.info("=== Session %d complete ===", session_id)

        return {
            "session_id": session_id,
            "trading_date": trading_date,
            "spx": spx_close,
            "vix": chain.vix,
            "agent_results": agent_results,
            "baseline_results": baseline_results,
        }

    # -------------------------------------------------------------------
    # Account setup
    # -------------------------------------------------------------------

    def _ensure_accounts(self, agent_ids: List[str]) -> None:
        for agent_id in agent_ids:
            if agent_id not in self.accounts:
                self.accounts[agent_id] = Account(agent_id=agent_id)

    # -------------------------------------------------------------------
    # Settlement
    # -------------------------------------------------------------------

    def _settle_phase(self, session_id: int, spx_close: float) -> None:
        """Settle all open 1DTE positions from prior sessions."""
        for agent_id, acct in self.accounts.items():
            settled = settle_prior_positions(
                acct.open_positions, session_id, spx_close,
                settlement_source="official_close",
            )
            for pos in settled:
                acct.book_settlement(pos)
                queries.update_position_settlement(self.conn, pos)
                logger.info("[%s] settled %s: P&L=$%.2f",
                            agent_id, pos.structure.value, pos.realized_pnl)

            # Update agent memory with settlement results
            for pos in settled:
                state = queries.load_agent_state(self.conn, agent_id)
                strikes = "/".join(f"{l.strike:.0f}" for l in pos.legs)
                record = build_session_record(
                    session_id=pos.session_opened,
                    action="trade",
                    structure=pos.structure.value,
                    strikes=strikes,
                    width=pos.width,
                    fill_price=pos.entry_price,
                    pnl=pos.realized_pnl,
                    balance=acct.balance,
                )
                state = update_memory(state, record)
                queries.save_agent_state(self.conn, agent_id, state)

    # -------------------------------------------------------------------
    # Agent / baseline execution
    # -------------------------------------------------------------------

    def _run_agent(self, agent: BaseAgent, session_id: int,
                   chain: ChainSnapshot) -> dict:
        """Run a single AI agent's decision."""
        acct = self.accounts[agent.agent_id]

        # Load memory
        memory = queries.load_agent_state(self.conn, agent.agent_id)

        order, raw_response = agent.decide(
            chain, acct, session_id, memory=memory,
        )

        result = {
            "agent_id": agent.agent_id,
            "raw_response": raw_response,
            "action": "hold" if order is None else "trade",
        }

        if order is None:
            queries.insert_action(
                self.conn, agent.agent_id, session_id,
                action_type="hold",
                reasoning=raw_response[:500],
            )
            # Record hold in memory
            state = queries.load_agent_state(self.conn, agent.agent_id)
            record = build_session_record(
                session_id=session_id,
                action="hold",
                market_context=f"SPX={chain.underlying_price:.0f} VIX={chain.vix:.1f}",
                balance=acct.balance,
            )
            state = update_memory(state, record)
            queries.save_agent_state(self.conn, agent.agent_id, state)

            logger.info("[%s] HOLD", agent.agent_id)
            return result

        # Stamp 1DTE fields on the order
        order.window = "close5"
        order.dte_at_entry = 1
        if chain.expirations:
            order.expiration = chain.expirations[0].isoformat()

        # Submit order through broker
        fill = self.broker.submit_order(
            order, acct, chain, session_id,
        )

        result["order"] = {
            "structure": order.structure.value,
            "strikes": [l.strike for l in order.legs],
            "width": order.width,
            "quantity": order.quantity,
            "thesis": order.thesis,
        }
        result["filled"] = fill.filled
        result["fill_price"] = fill.fill_price
        result["commission"] = fill.commission
        result["slippage"] = fill.slippage_applied
        result["rejection_reason"] = fill.rejection_reason

        queries.insert_order(self.conn, order, session_id,
                             slippage=fill.slippage_applied)

        if fill.filled and fill.position:
            queries.insert_position(self.conn, fill.position)
            logger.info("[%s] FILLED %s @ $%.2f (width=%s, slippage=$%.2f, comm=$%.2f)",
                        agent.agent_id, order.structure.value,
                        fill.fill_price, order.width,
                        fill.slippage_applied, fill.commission)
        else:
            queries.insert_action(
                self.conn, agent.agent_id, session_id,
                action_type="rejected",
                details=order.structure.value,
                failure_reason=fill.rejection_reason,
            )
            logger.info("[%s] REJECTED: %s", agent.agent_id, fill.rejection_reason)

        return result

    def _run_baseline(self, bot, session_id: int,
                      chain: ChainSnapshot) -> dict:
        """Run a single baseline bot's decision."""
        acct = self.accounts[bot.agent_id]
        order = bot.decide(chain, acct, session_id)

        result = {
            "agent_id": bot.agent_id,
            "action": "hold" if order is None else "trade",
        }

        if order is None:
            queries.insert_action(
                self.conn, bot.agent_id, session_id,
                action_type="hold",
                reasoning="Baseline: hold cash.",
            )
            logger.info("[%s] HOLD", bot.agent_id)
            return result

        # Stamp 1DTE fields
        order.window = "close5"
        order.dte_at_entry = 1
        if chain.expirations:
            order.expiration = chain.expirations[0].isoformat()

        fill = self.broker.submit_order(
            order, acct, chain, session_id,
        )

        result["order"] = {
            "structure": order.structure.value,
            "strikes": [l.strike for l in order.legs],
            "width": order.width,
            "quantity": order.quantity,
            "thesis": order.thesis,
        }
        result["filled"] = fill.filled
        result["fill_price"] = fill.fill_price
        result["commission"] = fill.commission

        queries.insert_order(self.conn, order, session_id,
                             slippage=fill.slippage_applied)

        if fill.filled and fill.position:
            queries.insert_position(self.conn, fill.position)
            logger.info("[%s] FILLED %s @ $%.2f (width=%s)",
                        bot.agent_id, order.structure.value,
                        fill.fill_price, order.width)
        else:
            logger.info("[%s] REJECTED: %s", bot.agent_id, fill.rejection_reason)

        return result
