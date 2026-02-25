"""Claude-powered trading agent — Anthropic SDK wrapper with retry, parse, fallback."""

from __future__ import annotations

import json
import logging
import re
from typing import Optional

import anthropic

from sim.config import AGENT_MAX_TOKENS, SPREAD_WIDTH
from sim.data.chain_snapshot import ChainSnapshot
from sim.engine.account import Account
from sim.engine.order import (
    Order,
    StructureType,
    make_bear_call_vertical,
    make_bull_put_vertical,
    make_call_butterfly,
    make_iron_condor,
    make_iron_fly,
    make_put_butterfly,
)
from sim.agents.base_agent import BaseAgent
from sim.agents.prompts.market_context import (
    format_account_context,
    format_chain_context,
    format_memory_context,
    format_positions_context,
)
from sim.agents.prompts.system_prompt import build_system_prompt

logger = logging.getLogger(__name__)

MAX_RETRIES = 2


class ClaudeAgent(BaseAgent):
    """AI trading agent powered by Claude via the Anthropic SDK."""

    def __init__(self, agent_id: str, model: str, personality_seed: str,
                 client: Optional[anthropic.Anthropic] = None):
        super().__init__(agent_id, model, personality_seed)
        self.client = client or anthropic.Anthropic()

    def decide(self, chain: ChainSnapshot, account: Account,
               session_id: int, track: str,
               judge_brief: str = "",
               memory: Optional[dict] = None,
               window: str = "open",
               dte: int = 0) -> tuple[Optional[Order], str]:
        """Call Claude to make a trading decision.

        Returns:
            Tuple of (Order or None for hold, raw response text).
        """
        system_prompt = build_system_prompt(
            agent_id=self.agent_id,
            personality_seed=self.personality_seed,
            track=track,
            session_id=session_id,
            judge_brief=judge_brief,
            memory_context=format_memory_context(memory),
            window=window,
            dte=dte,
        )

        user_message = self._build_user_message(chain, account)

        raw_text = ""
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = self.client.messages.create(
                    model=self.model,
                    max_tokens=AGENT_MAX_TOKENS,
                    system=system_prompt,
                    messages=[{"role": "user", "content": user_message}],
                )
                raw_text = response.content[0].text.strip()
                logger.info("[%s] attempt %d raw: %s", self.agent_id, attempt,
                            raw_text[:200])

                parsed = self._parse_response(raw_text)
                if parsed is None:
                    logger.warning("[%s] parse failed attempt %d", self.agent_id, attempt)
                    continue

                if parsed["action"] == "hold":
                    return None, raw_text

                order = self._build_order(parsed, chain)
                if order is not None:
                    return order, raw_text

                logger.warning("[%s] order build failed attempt %d", self.agent_id, attempt)

            except anthropic.APIError as e:
                logger.error("[%s] API error attempt %d: %s", self.agent_id, attempt, e)
            except Exception as e:
                logger.error("[%s] unexpected error attempt %d: %s", self.agent_id, attempt, e)

        # All retries exhausted → fallback to hold
        logger.warning("[%s] all retries exhausted, falling back to hold", self.agent_id)
        return None, raw_text or "FALLBACK_HOLD"

    def _build_user_message(self, chain: ChainSnapshot, account: Account) -> str:
        """Build the user message with market data and account state."""
        parts = [
            "## Current Market Data",
            format_chain_context(chain),
            "",
            "## Your Account",
            format_account_context(account),
            "",
            format_positions_context(account.open_positions),
            "",
            "What is your decision for this session? Respond with JSON only.",
        ]
        return "\n".join(parts)

    def _parse_response(self, text: str) -> Optional[dict]:
        """Parse agent's JSON response, handling common formatting issues."""
        # Strip markdown code fences if present
        cleaned = re.sub(r"```(?:json)?\s*", "", text)
        cleaned = cleaned.strip().rstrip("`")

        # Try direct parse
        try:
            return json.loads(cleaned)
        except json.JSONDecodeError:
            pass

        # Try to extract JSON object from surrounding text
        match = re.search(r"\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}", cleaned, re.DOTALL)
        if match:
            try:
                return json.loads(match.group())
            except json.JSONDecodeError:
                pass

        return None

    def _build_order(self, parsed: dict, chain: ChainSnapshot) -> Optional[Order]:
        """Convert parsed JSON into a validated Order object."""
        try:
            structure = parsed["structure"]
            strikes = parsed.get("strikes", {})
            quantity = parsed.get("quantity", 1)
            thesis = parsed.get("thesis", "")

            # Validate quantity
            quantity = max(1, min(5, int(quantity)))

            if structure == "bull_put_vertical":
                return self._make_vertical_put(strikes, quantity, thesis, chain)
            elif structure == "bear_call_vertical":
                return self._make_vertical_call(strikes, quantity, thesis, chain)
            elif structure == "iron_condor":
                return self._make_ic(strikes, quantity, thesis, chain)
            elif structure == "iron_fly":
                return self._make_fly(strikes, quantity, thesis, chain)
            elif structure == "call_butterfly":
                return self._make_butterfly(strikes, quantity, thesis, chain, "C")
            elif structure == "put_butterfly":
                return self._make_butterfly(strikes, quantity, thesis, chain, "P")
            else:
                logger.warning("[%s] unknown structure: %s", self.agent_id, structure)
                return None
        except (KeyError, TypeError, ValueError) as e:
            logger.warning("[%s] order build error: %s", self.agent_id, e)
            return None

    def _snap_strike(self, strike: float, chain: ChainSnapshot,
                     direction: str = "nearest") -> Optional[float]:
        """Snap a requested strike to the nearest available strike.

        Args:
            strike: Requested strike price.
            chain: Current chain snapshot.
            direction: "nearest" (default), "away_from_atm", or "toward_atm".
                Conservative rounding: short strikes use "away_from_atm",
                long strikes use "toward_atm".

        Returns:
            Snapped strike, or None if no strike within tolerance.
        """
        if not chain.strikes:
            return None

        atm = chain.underlying_price

        if direction == "away_from_atm":
            # Short strike: round further from ATM
            if strike < atm:
                # Put side: round down (further OTM)
                candidates = [s for s in chain.strikes if s <= strike]
                if candidates:
                    nearest = max(candidates)
                else:
                    nearest = min(chain.strikes, key=lambda s: abs(s - strike))
            else:
                # Call side: round up (further OTM)
                candidates = [s for s in chain.strikes if s >= strike]
                if candidates:
                    nearest = min(candidates)
                else:
                    nearest = min(chain.strikes, key=lambda s: abs(s - strike))
        elif direction == "toward_atm":
            # Long strike: round toward ATM
            if strike < atm:
                # Put side: round up (toward ATM)
                candidates = [s for s in chain.strikes if s >= strike]
                if candidates:
                    nearest = min(candidates)
                else:
                    nearest = min(chain.strikes, key=lambda s: abs(s - strike))
            else:
                # Call side: round down (toward ATM)
                candidates = [s for s in chain.strikes if s <= strike]
                if candidates:
                    nearest = max(candidates)
                else:
                    nearest = min(chain.strikes, key=lambda s: abs(s - strike))
        else:
            nearest = min(chain.strikes, key=lambda s: abs(s - strike))

        # Only snap if within 3 points
        if abs(nearest - strike) <= 3:
            return nearest
        return None

    def _validate_width(self, s1: float, s2: float) -> bool:
        """Check that two strikes are exactly SPREAD_WIDTH apart."""
        return abs(abs(s1 - s2) - SPREAD_WIDTH) < 0.01

    def _make_vertical_put(self, strikes: dict, qty: int, thesis: str,
                           chain: ChainSnapshot) -> Optional[Order]:
        short = self._snap_strike(strikes["short_put"], chain, direction="away_from_atm")
        long = self._snap_strike(strikes["long_put"], chain, direction="toward_atm")
        if short is None or long is None:
            return None
        # Ensure correct ordering: short > long for bull put
        if short <= long:
            short, long = max(short, long), min(short, long)
        if not self._validate_width(short, long):
            long = short - SPREAD_WIDTH
            if long not in chain.strikes:
                return None
        return make_bull_put_vertical(self.agent_id, short, long, qty, thesis=thesis)

    def _make_vertical_call(self, strikes: dict, qty: int, thesis: str,
                            chain: ChainSnapshot) -> Optional[Order]:
        short = self._snap_strike(strikes["short_call"], chain, direction="away_from_atm")
        long = self._snap_strike(strikes["long_call"], chain, direction="toward_atm")
        if short is None or long is None:
            return None
        # Ensure correct ordering: long > short for bear call
        if long <= short:
            short, long = min(short, long), max(short, long)
        if not self._validate_width(short, long):
            long = short + SPREAD_WIDTH
            if long not in chain.strikes:
                return None
        return make_bear_call_vertical(self.agent_id, short, long, qty, thesis=thesis)

    def _make_ic(self, strikes: dict, qty: int, thesis: str,
                 chain: ChainSnapshot) -> Optional[Order]:
        lp = self._snap_strike(strikes["long_put"], chain, direction="toward_atm")
        sp = self._snap_strike(strikes["short_put"], chain, direction="away_from_atm")
        sc = self._snap_strike(strikes["short_call"], chain, direction="away_from_atm")
        lc = self._snap_strike(strikes["long_call"], chain, direction="toward_atm")
        if any(s is None for s in [lp, sp, sc, lc]):
            return None
        # Fix widths if needed
        if not self._validate_width(sp, lp):
            lp = sp - SPREAD_WIDTH
        if not self._validate_width(sc, lc):
            lc = sc + SPREAD_WIDTH
        if lp not in chain.strikes or lc not in chain.strikes:
            return None
        return make_iron_condor(self.agent_id, lp, sp, sc, lc, qty, thesis=thesis)

    def _make_fly(self, strikes: dict, qty: int, thesis: str,
                  chain: ChainSnapshot) -> Optional[Order]:
        # Iron fly can be specified as IC-style or with center + wings
        if "center" in strikes:
            center = self._snap_strike(strikes["center"], chain)
            if center is None:
                return None
            return make_iron_fly(self.agent_id, center, SPREAD_WIDTH, qty, thesis=thesis)

        sp = self._snap_strike(strikes.get("short_put", 0), chain)
        sc = self._snap_strike(strikes.get("short_call", 0), chain)
        if sp is None or sc is None:
            return None
        # Iron fly: short strikes should be the same (ATM)
        center = sp if sp == sc else (sp + sc) / 2
        center = self._snap_strike(center, chain)
        if center is None:
            return None
        return make_iron_fly(self.agent_id, center, SPREAD_WIDTH, qty, thesis=thesis)

    def _make_butterfly(self, strikes: dict, qty: int, thesis: str,
                        chain: ChainSnapshot, put_call: str) -> Optional[Order]:
        lower = self._snap_strike(strikes["lower"], chain)
        center = self._snap_strike(strikes["center"], chain)
        upper = self._snap_strike(strikes["upper"], chain)
        if any(s is None for s in [lower, center, upper]):
            return None
        # Validate butterfly geometry: center should be midpoint
        if abs(center - (lower + upper) / 2) > 1:
            return None
        factory = make_call_butterfly if put_call == "C" else make_put_butterfly
        return factory(self.agent_id, lower, center, upper, qty, thesis=thesis)
