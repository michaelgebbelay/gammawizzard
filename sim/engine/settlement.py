"""Cash settlement engine — settles expiring 1DTE positions at SPX close.

v14 settlement:
  - All positions are 1DTE (window="close5", dte_at_entry=1)
  - Settle at next day's official SPX close
  - Called at start of each session (PRE-MARKET phase)
"""

from __future__ import annotations

from typing import List

from sim.engine.position import SpreadPosition


def settle_prior_positions(positions: List[SpreadPosition],
                           session_id: int,
                           spx_close: float,
                           settlement_source: str = "official_close") -> List[SpreadPosition]:
    """Settle all open 1DTE positions from prior sessions at today's SPX close.

    Called at the start of each session to settle positions that carried
    overnight from the prior session.

    Args:
        positions: All open positions for an agent.
        session_id: Current session ID.
        spx_close: SPX closing price (prior day's close = today's settlement).
        settlement_source: How the settlement price was obtained.

    Returns:
        List of positions that were settled.
    """
    settled = []
    for pos in positions:
        if pos.is_open and pos.session_opened < session_id:
            pos.settlement_source = settlement_source
            pos.settle(session_id, spx_close)
            settled.append(pos)
    return settled
