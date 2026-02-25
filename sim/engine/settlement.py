"""Cash settlement engine — settles expiring positions at SPX close.

Dual-window settlement (v13):
  - 0DTE positions (window="open"):  settle same-day at CLOSE
  - 1DTE positions (window="close5"): settle next-day at CLOSE

Settlement is automatic — no participant action required.
"""

from __future__ import annotations

from typing import List

from sim.engine.position import SpreadPosition


def settle_0dte_positions(positions: List[SpreadPosition],
                          session_id: int,
                          spx_close: float,
                          settlement_source: str = "official_close") -> List[SpreadPosition]:
    """Settle 0DTE positions opened this session at today's CLOSE.

    Called during the same session's CLOSE phase.

    Args:
        positions: All open positions for an agent.
        session_id: Current session ID.
        spx_close: SPX closing price for today.
        settlement_source: How the settlement price was obtained.

    Returns:
        List of 0DTE positions that were settled.
    """
    settled = []
    for pos in positions:
        if pos.is_open and pos.dte_at_entry == 0 and pos.session_opened == session_id:
            pos.settlement_source = settlement_source
            pos.settle(session_id, spx_close)
            settled.append(pos)
    return settled


def settle_1dte_positions(positions: List[SpreadPosition],
                          session_id: int,
                          spx_close: float,
                          settlement_source: str = "official_close") -> List[SpreadPosition]:
    """Settle 1DTE positions opened in a prior session at today's CLOSE.

    Called at the start of each session (PRE-MARKET) to settle positions
    that carried overnight.

    Args:
        positions: All open positions for an agent.
        session_id: Current session ID.
        spx_close: SPX closing price from the prior session.
        settlement_source: How the settlement price was obtained.

    Returns:
        List of 1DTE positions that were settled.
    """
    settled = []
    for pos in positions:
        if pos.is_open and pos.dte_at_entry == 1 and pos.session_opened < session_id:
            pos.settlement_source = settlement_source
            pos.settle(session_id, spx_close)
            settled.append(pos)
    return settled


def settle_expiring_positions(positions: List[SpreadPosition],
                              session_id: int,
                              spx_close: float) -> List[SpreadPosition]:
    """Legacy: settle all expiring positions (backwards-compatible).

    For the dual-window lifecycle, prefer settle_0dte_positions() and
    settle_1dte_positions() separately.
    """
    settled = []
    for pos in positions:
        if pos.is_open and pos.session_opened < session_id:
            pos.settle(session_id, spx_close)
            settled.append(pos)
    return settled
