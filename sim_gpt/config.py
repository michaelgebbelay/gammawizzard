"""Configuration for the live binary-vertical game."""

from __future__ import annotations

from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
DB_PATH = DATA_DIR / "live_game.db"

# Live game starts Monday, March 2, 2026.
LIVE_START_DATE = date(2026, 3, 2)
MARKET_TZ = "America/New_York"
POST_CLOSE_DELAY_MINUTES = 13
ASOF_MAX_STALENESS_MINUTES = 180
DEFAULT_LIVE_API_URL = "https://gandalf.gammawizard.com/rapi/GetUltraPureConstantStable"

ALLOWED_WIDTHS = {5, 10}
MAX_CONTRACTS = 3
TARGET_DELTAS = (0.10, 0.16, 0.25)
STARTING_ACCOUNT_BALANCE = 30_000.0
MAX_RISK_PCT = 0.30
# Safety buffer applied to max risk cap (30% * 0.90 => 27% effective cap).
RISK_BUFFER_PCT = 0.90
# Trade cost model: $1 per executed option leg.
COMMISSION_PER_LEG_DOLLARS = 1.0

# Chain-pricing model (no Profit/CProfit settlement dependency).
CHAIN_SOURCE = "schwab"
SCHWAB_SYMBOL = "$SPX"
SCHWAB_STRIKE_COUNT = 60
TARGET_DELTA_MAX_ERROR = 0.20
MAX_LEG_SPREAD_POINTS = 5.0
# Conservative mid fill = mid +/- half_spread * factor.
FILL_HALF_SPREAD_FACTOR = 0.50

PLAYER_IDS = [
    "player-01",
    "player-02",
    "player-03",
    "player-04",
    "player-05",
]

# Google Sheets defaults (from the URL you shared).
DEFAULT_GSHEET_ID = "1aBRPbPhYO39YvxrHVdDzo9nxrABRpHxaRk-FE5hXypc"
DEFAULT_RESULTS_TAB = "Live_Game_Rounds"
DEFAULT_LEADERBOARD_TAB = "Live_Game_Leaderboard"
DEFAULT_DECISIONS_TAB = "Live_Game_Decisions"

# Columns that are safe to expose to players at decision time.
PUBLIC_COLUMNS = [
    "Date",
    "TDate",
    "SPX",
    "VIX",
    "VixOne",
    "RV",
    "RV5",
    "RV10",
    "RV20",
    "R",
    "RX",
    "Forward",
]

# Columns that can leak labels, targets, or direct outcomes.
SUPPRESSED_COLUMNS = {
    "Limit",
    "CLimit",
    "LImp",
    "RImp",
    "LeftGo",
    "RightGo",
    "LGo",
    "RGo",
    "LReturn",
    "RReturn",
    "Cat",
    "Cat1",
    "Cat2",
    "TX",
    "Win",
    "CWin",
    "Profit",
    "CProfit",
    "Put",
    "Call",
}
