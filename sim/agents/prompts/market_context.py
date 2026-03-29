"""Compress chain data into concise text for agent context window (v14)."""

from __future__ import annotations

import logging
from typing import List, Optional

from sim.config import CHAIN_ATM_WINDOW
from sim.data.chain_snapshot import ChainSnapshot
from sim.data.features import FeaturePack, enrich, format_feature_pack
from sim.engine.account import Account
from sim.engine.position import SpreadPosition

logger = logging.getLogger(__name__)


def format_chain_context(chain: ChainSnapshot,
                         window: int = CHAIN_ATM_WINDOW,
                         prev_close: float = 0.0,
                         feature_pack: Optional[FeaturePack] = None) -> str:
    """Compress option chain into ATM±window strikes for agent consumption."""
    atm = chain.atm_strike()
    expiration = chain.expirations[0] if chain.expirations else None

    if feature_pack is None:
        pc = prev_close if prev_close > 0 else chain.spx_prev_close
        feature_pack = enrich(chain, prev_close=pc)

    features_text = format_feature_pack(feature_pack)

    nearby = [s for s in chain.strikes if abs(s - atm) <= window * 5]
    nearby.sort()

    lines = [
        f"SPX: {chain.underlying_price:.2f}  |  VIX: {chain.vix:.1f}  |  "
        f"ATM: {atm:.0f}  |  "
        f"Exp: {expiration}  |  Expected Move: +/-{chain.expected_move(expiration):.2f}",
    ]

    if chain.spx_open > 0:
        lines.append(
            f"Open: {chain.spx_open:.2f}  |  High: {chain.spx_high:.2f}  |  "
            f"Low: {chain.spx_low:.2f}  |  Prev Close: {chain.spx_prev_close:.2f}"
        )

    if features_text and "No features" not in features_text:
        lines.append("")
        lines.append(features_text)

    lines.extend([
        "",
        f"{'Strike':>8}  {'P.Bid':>6} {'P.Ask':>6} {'P.D':>6}  |  {'C.Bid':>6} {'C.Ask':>6} {'C.D':>6}",
        f"{'---'*3:>8}  {'---':>6} {'---':>6} {'---':>6}  |  {'---':>6} {'---':>6} {'---':>6}",
    ])

    for strike in nearby:
        put = chain.get_contract(strike, "P", expiration)
        call = chain.get_contract(strike, "C", expiration)

        p_bid = f"{put.bid:.2f}" if put else "  -  "
        p_ask = f"{put.ask:.2f}" if put else "  -  "
        p_delta = f"{put.delta:.3f}" if put else "  -  "
        c_bid = f"{call.bid:.2f}" if call else "  -  "
        c_ask = f"{call.ask:.2f}" if call else "  -  "
        c_delta = f"{call.delta:.3f}" if call else "  -  "

        marker = " <<<" if strike == atm else ""
        lines.append(
            f"{strike:>8.0f}  {p_bid:>6} {p_ask:>6} {p_delta:>6}  |  "
            f"{c_bid:>6} {c_ask:>6} {c_delta:>6}{marker}"
        )

    return "\n".join(lines)


def format_account_context(account: Account) -> str:
    """Format account state for agent consumption."""
    d = account.to_dict()
    lines = [
        f"Balance: ${d['balance']:,.2f}  |  "
        f"BP Available: ${d['buying_power_available']:,.2f}  |  "
        f"BP Used: ${d['buying_power_used']:,.2f}",
        f"Open Positions: {d['open_positions']}  |  "
        f"Realized P&L: ${d['realized_pnl']:+,.2f}  |  "
        f"Commissions: ${d['total_commissions']:,.2f}  |  "
        f"Return: {d['return_pct']:+.2f}%",
    ]
    return "\n".join(lines)


def format_positions_context(positions: List[SpreadPosition]) -> str:
    """Format open positions for agent consumption."""
    if not positions:
        return "No open positions."

    lines = ["Open positions:"]
    for pos in positions:
        strikes = sorted(set(l.strike for l in pos.legs))
        strike_str = "/".join(f"{s:.0f}" for s in strikes)
        lines.append(
            f"  {pos.structure.value} {strike_str} ({pos.width:.0f}w) | "
            f"{pos.side.value} @ {pos.entry_price:.2f} | "
            f"qty={pos.quantity} | opened session {pos.session_opened}"
        )
    return "\n".join(lines)


def format_memory_text(memory: Optional[dict]) -> str:
    """Format agent's accumulated memory for prompt injection."""
    if not memory:
        return "No prior session memory."
    from sim.agents.memory import format_memory
    text = format_memory(memory)
    return text if text else "No prior session memory."
