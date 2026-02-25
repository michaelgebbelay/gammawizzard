"""Central configuration for the SPX multi-agent trading simulation."""

import os
from pathlib import Path

# --- Paths ---
SIM_ROOT = Path(__file__).resolve().parent
REPO_ROOT = SIM_ROOT.parent
CACHE_DIR = SIM_ROOT / "cache"
DB_PATH = SIM_ROOT / "data" / "simulation.db"

# --- Agent definitions ---
AGENTS = {
    "opus-1":   {"model": "claude-opus-4-6",            "seed": "You tend to wait for high-conviction setups."},
    "opus-2":   {"model": "claude-opus-4-6",            "seed": "You focus on identifying the current market regime."},
    "sonnet-1": {"model": "claude-sonnet-4-5-20250929", "seed": "You prefer to act quickly when opportunities appear."},
    "sonnet-2": {"model": "claude-sonnet-4-5-20250929", "seed": "You pay close attention to implied volatility dynamics."},
    "haiku-1":  {"model": "claude-haiku-4-5-20251001",  "seed": "You like selling premium when conditions are right."},
    "haiku-2":  {"model": "claude-haiku-4-5-20251001",  "seed": "You look for stretched moves that should revert."},
}
JUDGE_MODEL = "claude-opus-4-6"
MEMORY_COMPRESS_MODEL = "claude-haiku-4-5-20251001"

AGENT_MAX_TOKENS = 1024  # output cap for all agents in all tracks

# --- Risk limits ---
MAX_CONCURRENT_SPREADS = 3
MAX_RISK_PER_TRADE_PCT = 0.05   # 5% of account per structure
MAX_ACCOUNT_RISK_PCT = 0.15     # 15% total
MIN_BP_RESERVE_PCT = 0.20       # retain 20% buying power
STARTING_CAPITAL = 30_000.0
SPREAD_WIDTH = 5.0              # $5 between strikes (all structures)
SPX_MULTIPLIER = 100            # dollars per point

# --- Commission (entry only — cash settlement has no closing transaction) ---
COMMISSION_PER_LEG = 0.65       # per contract per leg
SEC_TAF_PER_CONTRACT = 0.04     # per contract (flat, across all legs)

# --- Slippage model ---
SLIPPAGE_BASE = 0.05            # $0.05 per spread in calm markets
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
SLIPPAGE_WIDENED_PROB = 0.05    # 5% chance of doubled slippage

# --- Risk-free rate (T-Bill bot) ---
RISK_FREE_RATE_ANNUAL = 0.053   # 5.3%

# --- RNG seed (shared across all tracks for reproducibility) ---
RNG_SEED = 42

# --- Session timing (ET) ---
PHASE_OPEN = "09:30"
PHASE_MID = "12:00"
PHASE_CLOSE = "16:00"
PHASE_CLOSE5 = "16:05"

# --- Dual-window lifecycle (v13) ---
# OPEN window: trade 0DTE, settles same-day CLOSE
# CLOSE+5 window: trade 1DTE, settles next-day CLOSE
WINDOW_OPEN = "open"
WINDOW_CLOSE5 = "close5"
VALID_WINDOWS = {WINDOW_OPEN, WINDOW_CLOSE5}

# One order per participant per window
MAX_ORDERS_PER_WINDOW = 1

# Quote staleness thresholds (seconds)
STALENESS_OPEN_SEC = 30     # OPEN window: 30s max quote age
STALENESS_CLOSE5_SEC = 120  # CLOSE+5 window: 120s max quote age

# --- Tracks ---
TRACK_ADAPTIVE = "adaptive"
TRACK_FROZEN = "frozen"
TRACK_CLEAN = "clean"
VALID_TRACKS = {TRACK_ADAPTIVE, TRACK_FROZEN, TRACK_CLEAN}

# --- Time convention ---
# All annualization uses trading days, never calendar days.
# See sim/time_utils.py for the canonical trading_dte() function.
TRADING_DAYS_PER_YEAR = 252

# --- Sessions ---
SESSIONS_PER_TRACK = 80

# --- Schwab API ---
SCHWAB_SYMBOL = "$SPX"
SCHWAB_STRIKE_COUNT = 40        # strikes above + below ATM
CHAIN_ATM_WINDOW = 15           # ±15 strikes for agent context compression
