"""Rubric template and default weights for the judge's scoring system."""

from __future__ import annotations

# Default rubric weights (must sum to 100)
DEFAULT_WEIGHTS = {
    "structure_selection": 25,   # Did they pick the right structure for the regime?
    "strike_placement": 25,      # How well did they select strike distances?
    "risk_sizing": 20,           # Appropriate position size relative to account?
    "portfolio_exposure": 15,    # Net portfolio risk management across positions
    "pnl": 15,                   # Actual financial outcome
}

RUBRIC_DESCRIPTION = """## Scoring Rubric

Each agent is scored on 5 dimensions (0-10 each), weighted as shown:

### 1. Structure Selection ({structure_selection}%)
- Did the agent choose an appropriate structure for the market regime?
- Low VIX → wide structures (IC) or high-premium (fly) make sense
- High VIX → directional verticals or holding cash may be better
- Holding cash in bad conditions is a VALID and sometimes optimal choice

### 2. Strike Placement ({strike_placement}%)
- How well did the agent select strike distances relative to expected move?
- Short strikes beyond the expected move earn higher scores
- Placing strikes inside the expected move is aggressive and risky
- Delta-based placement shows understanding of probability

### 3. Risk Sizing ({risk_sizing}%)
- Is the position size appropriate for the account?
- Trading 1 lot when the account is small = appropriate
- Using too much buying power = reckless
- Consistent sizing across sessions = disciplined

### 4. Portfolio Exposure ({portfolio_exposure}%)
- Net portfolio risk across all open positions
- Directional bias awareness (all positions on same side = concentrated)
- Correlation of positions (multiple ICs = well-diversified)
- Not overloading the account

### 5. P&L Outcome ({pnl}%)
- Actual financial result of the session
- Max profit = best score, max loss = worst
- Partial losses weighted proportionally
- Commission drag considered"""

BRIEF_TEMPLATE = """You are the judge for an SPX options trading simulation.

## Your Task
Write a concise PRE-MARKET BRIEF (3-5 sentences) for the trading agents.
The brief should summarize:
1. Current market conditions (VIX level, SPX level, recent moves)
2. What the expected move implies for strike selection
3. Any notable observations about the chain (skew, term structure, etc.)

Do NOT give specific trade recommendations. Just describe conditions.

## Market Data
{market_context}

Write the brief now (3-5 sentences, no bullet points):"""

SCORECARD_TEMPLATE = """You are the judge scoring a trading agent's session performance.

## Scoring Rubric
{rubric}

## Agent: {agent_id}
## Session: {session_id}

### Market Conditions
{market_context}

### Agent's Decision
{agent_decision}

### Fill Result
{fill_result}

### Settlement Result
{settlement_result}

### Account State
{account_state}

## Instructions
Score each dimension 0-10 and provide brief justification.
Respond with ONLY a JSON object:
{{
  "structure_selection": <0-10>,
  "strike_placement": <0-10>,
  "risk_sizing": <0-10>,
  "portfolio_exposure": <0-10>,
  "pnl": <0-10>,
  "total": <weighted_total>,
  "notes": "Brief overall assessment (2-3 sentences)."
}}"""
