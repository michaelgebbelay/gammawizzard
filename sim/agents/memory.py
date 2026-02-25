"""Agent memory compression — Haiku-compressed session summaries."""

from __future__ import annotations

import json
import logging
from typing import Optional

import anthropic

from sim.config import MEMORY_COMPRESS_MODEL

logger = logging.getLogger(__name__)

_COMPRESS_PROMPT = """Compress the following trading session record into a concise summary (max 150 words).
Focus on: what was traded, the reasoning, the outcome, and any lessons learned.
Do NOT include JSON formatting — just plain text.

Session record:
{record}

Prior cumulative summary (incorporate key themes):
{prior}"""


def compress_session_memory(
    client: anthropic.Anthropic,
    session_record: dict,
    prior_summary: str = "",
) -> str:
    """Compress a session's trading activity into a concise memory.

    Uses Haiku for cost efficiency (~$0.001 per compression).

    Args:
        client: Anthropic API client.
        session_record: Dict with session details (trades, P&L, market context).
        prior_summary: Cumulative summary from prior sessions.

    Returns:
        Compressed summary string (~100-150 words).
    """
    prompt = _COMPRESS_PROMPT.format(
        record=json.dumps(session_record, indent=2, default=str),
        prior=prior_summary or "No prior history.",
    )

    try:
        response = client.messages.create(
            model=MEMORY_COMPRESS_MODEL,
            max_tokens=256,
            messages=[{"role": "user", "content": prompt}],
        )
        return response.content[0].text.strip()
    except Exception as e:
        logger.warning("Memory compression failed: %s", e)
        # Fallback: simple mechanical summary
        return _fallback_summary(session_record)


def _fallback_summary(record: dict) -> str:
    """Mechanical fallback if Haiku call fails."""
    action = record.get("action", "unknown")
    structure = record.get("structure", "unknown")
    pnl = record.get("realized_pnl", 0)
    thesis = record.get("thesis", "")

    if action == "hold":
        return f"Session: held cash. Reasoning: {thesis[:100]}"
    return (
        f"Session: traded {structure}. "
        f"P&L: ${pnl:+.2f}. "
        f"Reasoning: {thesis[:100]}"
    )


def build_session_record(
    action: str,
    order: Optional[dict] = None,
    fill_result: Optional[dict] = None,
    settlement: Optional[dict] = None,
    account_state: Optional[dict] = None,
    market_context: str = "",
    thesis: str = "",
) -> dict:
    """Build a structured session record for memory compression."""
    record = {
        "action": action,
        "thesis": thesis,
        "market_context_summary": market_context[:300] if market_context else "",
    }
    if order:
        record["order"] = order
    if fill_result:
        record["fill"] = fill_result
    if settlement:
        record["settlement"] = settlement
    if account_state:
        record["account"] = account_state
    return record
