"""Registry of AI trading agents (v14: 2 Opus + 2 GPT-4o)."""

from __future__ import annotations

from sim.config import AGENTS


def get_agent_configs() -> dict:
    """Return the full agent configuration dictionary."""
    return dict(AGENTS)


def get_agent_ids() -> list[str]:
    """Return all agent IDs."""
    return list(AGENTS.keys())


def get_model_for_agent(agent_id: str) -> str:
    """Look up the model ID for an agent."""
    return AGENTS[agent_id]["model"]


def get_provider_for_agent(agent_id: str) -> str:
    """Look up the provider for an agent."""
    return AGENTS[agent_id]["provider"]


def is_trained(agent_id: str) -> bool:
    """Check if an agent has the trained playbook."""
    return AGENTS[agent_id].get("trained", False)
