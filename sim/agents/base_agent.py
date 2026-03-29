"""Abstract interface for AI trading agents (v14)."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional

from sim.data.chain_snapshot import ChainSnapshot
from sim.engine.account import Account
from sim.engine.order import Order


class BaseAgent(ABC):
    """Base class for all AI trading agents (Claude and GPT)."""

    def __init__(self, agent_id: str, model: str, trained: bool = False):
        self.agent_id = agent_id
        self.model = model
        self.trained = trained

    @abstractmethod
    def decide(self, chain: ChainSnapshot, account: Account,
               session_id: int,
               memory: Optional[dict] = None) -> tuple[Optional[Order], str]:
        """Make a trading decision for this session.

        Args:
            chain: Chain snapshot (1DTE, close5 phase).
            account: Current account state.
            session_id: Current session number.
            memory: Accumulated memory dict from prior sessions.

        Returns:
            Tuple of (Order or None for hold, raw_response_text).
        """
        ...
