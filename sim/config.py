"""Central configuration for the SPX 1DTE trading simulation (v14)."""

import os
from pathlib import Path

# --- Paths ---
SIM_ROOT = Path(__file__).resolve().parent
REPO_ROOT = SIM_ROOT.parent
CACHE_DIR = SIM_ROOT / "cache"
DB_PATH = SIM_ROOT / "data" / "simulation_v14.db"

# --- Agent definitions (v14: 2 Opus + 2 GPT-5.2, cold/trained) ---
AGENTS = {
    "opus-cold":    {"model": "claude-opus-4-6",  "provider": "anthropic", "trained": False},
    "opus-trained": {"model": "claude-opus-4-6",  "provider": "anthropic", "trained": True},
    "gpt-cold":     {"model": "gpt-5.2",          "provider": "openai",    "trained": False},
    "gpt-trained":  {"model": "gpt-5.2",          "provider": "openai",    "trained": True},
}

AGENT_MAX_TOKENS = 1024       # output cap for all agents
AGENT_INPUT_TOKEN_CAP = 4000  # approximate input budget

# --- Risk limits ---
MAX_CONCURRENT_SPREADS = 3       # max open positions per agent
MAX_RISK_PER_TRADE_PCT = 0.10    # 10% of account per single trade
MAX_ACCOUNT_RISK_PCT = 0.30      # 30% total exposure at any time
MIN_BP_RESERVE_PCT = 0.10        # retain 10% buying power
STARTING_CAPITAL = 30_000.0
SPX_MULTIPLIER = 100             # dollars per point

# --- Commission (entry only — cash settlement has no closing transaction) ---
COMMISSION_PER_LEG = 0.65        # per contract per leg
SEC_TAF_PER_CONTRACT = 0.04      # per contract (flat, across all legs)

# --- Slippage model ---
SLIPPAGE_BASE = 0.05             # $0.05 per spread in calm markets
SLIPPAGE_VIX_BANDS = [
    (15,  1.0),
    (20,  1.2),
    (25,  1.5),
    (35,  2.0),
    (999, 3.0),
]
SLIPPAGE_MOVE_BANDS = [
    (5,   1.0),
    (15,  1.3),
    (25,  1.6),
    (999, 2.0),
]
SLIPPAGE_NOISE_LO = 0.85
SLIPPAGE_NOISE_HI = 1.15
SLIPPAGE_WIDENED_PROB = 0.05     # 5% chance of doubled slippage

# --- Risk-free rate (T-Bill bot) ---
RISK_FREE_RATE_ANNUAL = 0.053    # 5.3%

# --- RNG seed (deterministic fills) ---
RNG_SEED = 42

# --- Time convention ---
TRADING_DAYS_PER_YEAR = 252

# --- Schwab API ---
SCHWAB_SYMBOL = "$SPX"
SCHWAB_STRIKE_COUNT = 40         # strikes above + below ATM
CHAIN_ATM_WINDOW = 15            # ±15 strikes for agent context compression
