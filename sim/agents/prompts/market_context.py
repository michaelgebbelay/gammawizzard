"""Compress chain data into concise text for agent context window."""

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
                         feature_pack: Optional[FeaturePack] = None,
                         replay: bool = False) -> str:
    """Compress option chain into ATM±window strikes for agent consumption.

    Args:
        chain: The chain snapshot.
        window: Number of strikes above/below ATM to include.
        prev_close: Previous SPX close for gap calculation.
        feature_pack: Pre-computed FeaturePack. If None and not replay, computes on the fly.
        replay: If True, require a pre-computed feature_pack. Raises if missing.
    """
    atm = chain.atm_strike()
    expiration = chain.expirations[0] if chain.expirations else None

    # Replay mode: persisted features required, never recompute
    if feature_pack is None:
        if replay:
            raise ValueError(
                "replay=True but no persisted FeaturePack provided. "
                "Session data may be incomplete — mark invalid."
            )
        # Live / ad-hoc: compute on the fly
        pc = prev_close if prev_close > 0 else chain.spx_prev_close
        feature_pack = enrich(chain, prev_close=pc)

    features_text = format_feature_pack(feature_pack)

    # Filter strikes within window of ATM
    nearby = [s for s in chain.strikes if abs(s - atm) <= window * 5]
    nearby.sort()

    lines = [
        f"SPX: {chain.underlying_price:.2f}  |  VIX: {chain.vix:.1f}  |  "
        f"ATM: {atm:.0f}  |  Phase: {chain.phase}  |  "
        f"Exp: {expiration}  |  Expected Move: +/-{chain.expected_move(expiration):.2f}",
    ]

    # Add OHLC line if available
    if chain.spx_open > 0:
        lines.append(
            f"Open: {chain.spx_open:.2f}  |  High: {chain.spx_high:.2f}  |  "
            f"Low: {chain.spx_low:.2f}  |  Prev Close: {chain.spx_prev_close:.2f}"
        )

    # Add derived features
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
    return (
        f"Balance: ${d['balance']:,.2f}  |  "
        f"BP Available: ${d['buying_power_available']:,.2f}  |  "
        f"BP Used: ${d['buying_power_used']:,.2f}\n"
        f"Open Positions: {d['open_positions']}  |  "
        f"Realized P&L: ${d['realized_pnl']:+,.2f}  |  "
        f"Commissions: ${d['total_commissions']:,.2f}  |  "
        f"Return: {d['return_pct']:+.2f}%"
    )


def format_positions_context(positions: List[SpreadPosition]) -> str:
    """Format open positions for agent consumption."""
    if not positions:
        return "No open positions."

    lines = ["Open positions:"]
    for pos in positions:
        strikes = sorted(set(l.strike for l in pos.legs))
        strike_str = "/".join(f"{s:.0f}" for s in strikes)
        lines.append(
            f"  {pos.structure.value} {strike_str} | "
            f"{pos.side.value} @ {pos.entry_price:.2f} | "
            f"qty={pos.quantity} | opened session {pos.session_opened}"
        )
    return "\n".join(lines)


def format_memory_context(memory: Optional[dict]) -> str:
    """Format agent's prior session memory."""
    if not memory:
        return "No prior session memory."
    return f"Prior session summary:\n{memory.get('cumulative_summary', memory.get('summary', ''))}"
