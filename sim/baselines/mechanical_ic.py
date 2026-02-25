"""Mechanical Iron Condor bot — sells 10-delta IC every session."""

from __future__ import annotations

from typing import Optional

from sim.config import SPREAD_WIDTH
from sim.data.chain_snapshot import ChainSnapshot
from sim.engine.account import Account
from sim.engine.order import Order, make_iron_condor
from sim.baselines.base_baseline import BaseBaseline


class MechanicalIC(BaseBaseline):
    """Bot 1: Sells a 10-delta iron condor every session, no exceptions.

    - Short strikes at ~10-delta on each side
    - Wings 5 points wide (SPREAD_WIDTH)
    - Always trades 1 lot
    - No market regime awareness — pure mechanical execution
    """

    def __init__(self, agent_id: str = "bot-mechanical-ic"):
        super().__init__(agent_id)

    def decide(self, chain: ChainSnapshot, account: Account,
               session_id: int, track: str) -> Optional[Order]:
        expiration = chain.expirations[0] if chain.expirations else None

        # Find 10-delta strikes
        put_short_strike = chain.nearest_delta_strike(-0.10, "P", expiration)
        call_short_strike = chain.nearest_delta_strike(0.10, "C", expiration)

        if put_short_strike is None or call_short_strike is None:
            return None

        # Wings at SPREAD_WIDTH points beyond short strikes
        put_long_strike = put_short_strike - SPREAD_WIDTH
        call_long_strike = call_short_strike + SPREAD_WIDTH

        # Verify wing strikes exist in the chain
        if (chain.get_contract(put_long_strike, "P", expiration) is None
                or chain.get_contract(call_long_strike, "C", expiration) is None):
            return None

        return make_iron_condor(
            agent_id=self.agent_id,
            put_long=put_long_strike,
            put_short=put_short_strike,
            call_short=call_short_strike,
            call_long=call_long_strike,
            qty=1,
            thesis="Mechanical 10-delta IC — no market view, pure premium collection.",
        )
