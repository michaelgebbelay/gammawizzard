"""Registry of all 6 AI trading agents with their configurations."""

from __future__ import annotations

from sim.config import AGENTS


def get_agent_configs() -> dict:
    """Return the full agent configuration dictionary."""
    return dict(AGENTS)


def get_agent_ids() -> list[str]:
    """Return all agent IDs in seat order."""
    return list(AGENTS.keys())


def get_model_for_agent(agent_id: str) -> str:
    """Look up the model ID for an agent."""
    return AGENTS[agent_id]["model"]


def get_personality_for_agent(agent_id: str) -> str:
    """Look up the personality seed for an agent."""
    return AGENTS[agent_id]["seed"]
