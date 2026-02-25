"""Opus judge agent — pre-market briefs and session scoring."""

from __future__ import annotations

import json
import logging
import re
from typing import Optional

import anthropic

from sim.config import AGENT_MAX_TOKENS, JUDGE_MODEL
from sim.data.chain_snapshot import ChainSnapshot
from sim.agents.prompts.market_context import format_chain_context
from sim.judge.prompts.judge_system_prompt import JUDGE_SYSTEM_PROMPT
from sim.judge.prompts.rubric_template import (
    BRIEF_TEMPLATE,
    RUBRIC_DESCRIPTION,
    SCORECARD_TEMPLATE,
)
from sim.judge.rubric import Rubric

logger = logging.getLogger(__name__)


class JudgeAgent:
    """Opus-powered judge for pre-market briefs and session scoring."""

    def __init__(self, client: Optional[anthropic.Anthropic] = None):
        self.client = client or anthropic.Anthropic()
        self.model = JUDGE_MODEL

    def generate_brief(self, chain: ChainSnapshot) -> str:
        """Generate a pre-market brief for agents.

        Args:
            chain: The OPEN chain snapshot.

        Returns:
            Brief text string (3-5 sentences).
        """
        market_ctx = format_chain_context(chain)
        prompt = BRIEF_TEMPLATE.format(market_context=market_ctx)

        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=AGENT_MAX_TOKENS,
                system=JUDGE_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": prompt}],
            )
            return response.content[0].text.strip()
        except Exception as e:
            logger.error("Judge brief generation failed: %s", e)
            return self._fallback_brief(chain)

    def score_agent(self, agent_id: str, session_id: int,
                    rubric: Rubric,
                    market_context: str,
                    agent_decision: str,
                    fill_result: str,
                    settlement_result: str,
                    account_state: str) -> dict:
        """Score an agent's session performance.

        Args:
            agent_id: The agent being scored.
            session_id: Current session number.
            rubric: The current scoring rubric.
            market_context: Formatted market data string.
            agent_decision: The agent's raw response text.
            fill_result: Fill details or rejection info.
            settlement_result: Settlement P&L details.
            account_state: Current account state.

        Returns:
            Dict with dimension scores, total, and notes.
        """
        rubric_text = RUBRIC_DESCRIPTION.format(**rubric.weights)
        prompt = SCORECARD_TEMPLATE.format(
            rubric=rubric_text,
            agent_id=agent_id,
            session_id=session_id,
            market_context=market_context,
            agent_decision=agent_decision,
            fill_result=fill_result,
            settlement_result=settlement_result,
            account_state=account_state,
        )

        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=AGENT_MAX_TOKENS,
                system=JUDGE_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": prompt}],
            )
            raw = response.content[0].text.strip()
            scores = self._parse_scores(raw, rubric)
            if scores:
                return scores
        except Exception as e:
            logger.error("Judge scoring failed for %s: %s", agent_id, e)

        # Fallback: neutral scores
        return self._fallback_scores(rubric)

    def _parse_scores(self, text: str, rubric: Rubric) -> Optional[dict]:
        """Parse judge's JSON score response."""
        cleaned = re.sub(r"```(?:json)?\s*", "", text)
        cleaned = cleaned.strip().rstrip("`")

        try:
            parsed = json.loads(cleaned)
        except json.JSONDecodeError:
            match = re.search(r"\{[^{}]*\}", cleaned, re.DOTALL)
            if match:
                try:
                    parsed = json.loads(match.group())
                except json.JSONDecodeError:
                    return None
            else:
                return None

        # Validate and clamp scores
        scores = {}
        for dim in rubric.weights:
            val = parsed.get(dim, 5)
            scores[dim] = max(0, min(10, int(val)))

        # Recompute weighted total
        scores["total"] = rubric.weighted_total(scores)
        scores["notes"] = parsed.get("notes", "")
        return scores

    def _fallback_brief(self, chain: ChainSnapshot) -> str:
        """Mechanical fallback brief when API fails."""
        return (
            f"SPX at {chain.underlying_price:.0f}, "
            f"VIX at {chain.vix:.1f}. "
            f"Expected move: ±{chain.expected_move():.1f} points. "
            f"Evaluate conditions carefully before trading."
        )

    def _fallback_scores(self, rubric: Rubric) -> dict:
        """Neutral scores when API fails."""
        scores = {dim: 5 for dim in rubric.weights}
        scores["total"] = rubric.weighted_total(scores)
        scores["notes"] = "Fallback neutral scores — judge API call failed."
        return scores
