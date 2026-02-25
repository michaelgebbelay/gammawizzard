"""Abstract interface for AI trading agents."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional

from sim.data.chain_snapshot import ChainSnapshot
from sim.engine.account import Account
from sim.engine.order import Order


class BaseAgent(ABC):
    """Base class for all AI trading agents (Claude-powered)."""

    def __init__(self, agent_id: str, model: str, personality_seed: str):
        self.agent_id = agent_id
        self.model = model
        self.personality_seed = personality_seed

    @abstractmethod
    def decide(self, chain: ChainSnapshot, account: Account,
               session_id: int, track: str,
               judge_brief: str = "",
               memory: Optional[dict] = None,
               window: str = "open",
               dte: int = 0) -> tuple[Optional[Order], str]:
        """Make a trading decision for one window.

        Args:
            chain: Chain snapshot for this decision window.
            account: Current account state.
            session_id: Current session number.
            track: Experimental track name.
            judge_brief: Pre-market brief (adaptive/frozen tracks).
            memory: Prior session memory (adaptive track only).
            window: "open" (0DTE) or "close5" (1DTE).
            dte: Days to expiration (0 or 1).

        Returns:
            Tuple of (Order or None, raw_response_text).
        """
        ...
