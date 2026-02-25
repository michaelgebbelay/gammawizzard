"""Hold Cash bot — never trades, earns risk-free rate (T-Bill benchmark)."""

from __future__ import annotations

from typing import Optional

from sim.data.chain_snapshot import ChainSnapshot
from sim.engine.account import Account
from sim.engine.order import Order
from sim.baselines.base_baseline import BaseBaseline


class HoldCash(BaseBaseline):
    """Bot 3: Never trades. Earns risk-free rate on full balance.

    Acts as the zero-skill benchmark. If an agent can't beat this,
    they're destroying value through trading costs and bad selection.

    Interest accrual is handled by Account.accrue_risk_free() in the
    orchestrator — this bot simply returns None every session.
    """

    def __init__(self, agent_id: str = "bot-hold-cash"):
        super().__init__(agent_id)

    def decide(self, chain: ChainSnapshot, account: Account,
               session_id: int, track: str) -> Optional[Order]:
        return None
