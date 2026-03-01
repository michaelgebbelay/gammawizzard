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
STARTING_ACCOUNT_BALANCE = 30_000.0
MAX_RISK_PCT = 0.30
# Safety buffer applied to max risk cap (30% * 0.90 => 27% effective cap).
RISK_BUFFER_PCT = 0.90
# Trade cost model: $1 per executed option leg.
COMMISSION_PER_LEG_DOLLARS = 1.0

PLAYER_IDS = [
    "player-regime",
    "player-momentum",
    "player-volatility",
    "player-contrarian",
    "player-vixone-bias",
]

# Google Sheets defaults (from the URL you shared).
DEFAULT_GSHEET_ID = "1aBRPbPhYO39YvxrHVdDzo9nxrABRpHxaRk-FE5hXypc"
DEFAULT_RESULTS_TAB = "Live_Game_Rounds"
DEFAULT_LEADERBOARD_TAB = "Live_Game_Leaderboard"

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
    "Limit",
    "CLimit",
]

# Columns that can leak labels, targets, or direct outcomes.
SUPPRESSED_COLUMNS = {
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
