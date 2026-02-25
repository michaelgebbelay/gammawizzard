"""Abstract interface for baseline (mechanical) trading bots."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional

from sim.data.chain_snapshot import ChainSnapshot
from sim.engine.account import Account
from sim.engine.order import Order


class BaseBaseline(ABC):
    """Base class for all mechanical baseline strategies.

    Baselines are deterministic — they do not use an LLM.
    Each baseline receives the chain and account state, then returns
    an Order to place or None (hold cash).
    """

    def __init__(self, agent_id: str):
        self.agent_id = agent_id

    @abstractmethod
    def decide(self, chain: ChainSnapshot, account: Account,
               session_id: int, track: str) -> Optional[Order]:
        """Decide whether to trade this session.

        Args:
            chain: The OPEN chain snapshot for this session.
            account: The bot's current account state.
            session_id: Current session number.
            track: Experimental track name.

        Returns:
            An Order to submit, or None to hold cash.
        """
        ...

    @property
    def name(self) -> str:
        return self.agent_id
