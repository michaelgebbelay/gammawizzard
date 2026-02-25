"""SQLite database connection and schema management."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from sim.config import DB_PATH

SCHEMA_VERSION = 2

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS sessions (
    session_id INTEGER PRIMARY KEY,
    track TEXT NOT NULL,
    trading_date TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    spx_open REAL,
    spx_close REAL,
    vix_open REAL,
    vix_close REAL,
    intraday_range REAL,
    premarket_brief TEXT,
    session_valid INTEGER DEFAULT 1,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS session_regimes (
    session_id INTEGER PRIMARY KEY,
    vix_bucket TEXT,
    market_character TEXT,
    intraday_range REAL,
    FOREIGN KEY (session_id) REFERENCES sessions(session_id)
);

CREATE TABLE IF NOT EXISTS accounts (
    agent_id TEXT NOT NULL,
    session_id INTEGER NOT NULL,
    track TEXT NOT NULL,
    starting_balance REAL NOT NULL,
    ending_balance REAL,
    realized_pnl REAL DEFAULT 0.0,
    unrealized_pnl REAL DEFAULT 0.0,
    total_commissions REAL DEFAULT 0.0,
    buying_power_used REAL DEFAULT 0.0,
    open_position_count INTEGER DEFAULT 0,
    PRIMARY KEY (agent_id, session_id, track),
    FOREIGN KEY (session_id) REFERENCES sessions(session_id)
);

CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    session_id INTEGER NOT NULL,
    track TEXT NOT NULL,
    window TEXT NOT NULL DEFAULT '',
    dte_at_entry INTEGER DEFAULT 0,
    expiration TEXT DEFAULT '',
    structure TEXT NOT NULL,
    side TEXT NOT NULL,
    legs TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    limit_price REAL,
    status TEXT NOT NULL,
    fill_price REAL,
    commission REAL,
    slippage REAL,
    thesis TEXT,
    rejection_reason TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (session_id) REFERENCES sessions(session_id)
);

CREATE TABLE IF NOT EXISTS positions (
    position_id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    track TEXT NOT NULL,
    session_opened INTEGER NOT NULL,
    session_settled INTEGER,
    window TEXT NOT NULL DEFAULT '',
    dte_at_entry INTEGER DEFAULT 0,
    expiration TEXT DEFAULT '',
    structure TEXT NOT NULL,
    legs TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    entry_price REAL NOT NULL,
    commission REAL NOT NULL,
    width REAL NOT NULL,
    settlement_price REAL,
    settlement_value REAL,
    settlement_source TEXT DEFAULT '',
    realized_pnl REAL,
    FOREIGN KEY (session_opened) REFERENCES sessions(session_id),
    FOREIGN KEY (session_settled) REFERENCES sessions(session_id)
);

CREATE TABLE IF NOT EXISTS position_marks (
    position_id TEXT NOT NULL,
    session_id INTEGER NOT NULL,
    phase TEXT NOT NULL,
    mark_price REAL NOT NULL,
    unrealized_pnl REAL NOT NULL,
    delta REAL,
    gamma REAL,
    theta REAL,
    vega REAL,
    PRIMARY KEY (position_id, session_id, phase),
    FOREIGN KEY (position_id) REFERENCES positions(position_id)
);

CREATE TABLE IF NOT EXISTS scorecards (
    agent_id TEXT NOT NULL,
    session_id INTEGER NOT NULL,
    track TEXT NOT NULL,
    structure_selection_score REAL,
    strike_placement_score REAL,
    risk_sizing_score REAL,
    portfolio_exposure_score REAL,
    pnl_score REAL,
    total_score REAL,
    judge_notes TEXT,
    PRIMARY KEY (agent_id, session_id, track),
    FOREIGN KEY (session_id) REFERENCES sessions(session_id)
);

CREATE TABLE IF NOT EXISTS rubric_history (
    session_id INTEGER NOT NULL,
    track TEXT NOT NULL,
    weights TEXT NOT NULL,
    rationale TEXT,
    PRIMARY KEY (session_id, track),
    FOREIGN KEY (session_id) REFERENCES sessions(session_id)
);

CREATE TABLE IF NOT EXISTS agent_actions (
    action_id INTEGER PRIMARY KEY AUTOINCREMENT,
    agent_id TEXT NOT NULL,
    session_id INTEGER NOT NULL,
    track TEXT NOT NULL,
    action_type TEXT NOT NULL,
    details TEXT,
    reasoning TEXT,
    failure_reason TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (session_id) REFERENCES sessions(session_id)
);

CREATE TABLE IF NOT EXISTS agent_memory (
    agent_id TEXT NOT NULL,
    session_id INTEGER NOT NULL,
    track TEXT NOT NULL DEFAULT 'adaptive',
    summary TEXT NOT NULL,
    cumulative_summary TEXT,
    PRIMARY KEY (agent_id, session_id, track)
);

CREATE TABLE IF NOT EXISTS session_features (
    session_id INTEGER NOT NULL,
    phase TEXT NOT NULL,
    expiration TEXT NOT NULL,
    features_json TEXT NOT NULL,
    feature_pack_version TEXT DEFAULT '2',
    data_source_chain TEXT DEFAULT '',
    data_source_gw INTEGER DEFAULT 0,
    PRIMARY KEY (session_id, phase, expiration),
    FOREIGN KEY (session_id) REFERENCES sessions(session_id)
);

CREATE TABLE IF NOT EXISTS diagnostics (
    session_id INTEGER NOT NULL,
    window TEXT NOT NULL,
    field TEXT NOT NULL,
    value TEXT,
    PRIMARY KEY (session_id, window, field),
    FOREIGN KEY (session_id) REFERENCES sessions(session_id)
);
"""

# Schema v1 → v2 migration (add window/dte/settlement_source columns)
MIGRATION_V1_TO_V2 = [
    "ALTER TABLE orders ADD COLUMN window TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE orders ADD COLUMN dte_at_entry INTEGER DEFAULT 0",
    "ALTER TABLE orders ADD COLUMN expiration TEXT DEFAULT ''",
    "ALTER TABLE positions ADD COLUMN window TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE positions ADD COLUMN dte_at_entry INTEGER DEFAULT 0",
    "ALTER TABLE positions ADD COLUMN expiration TEXT DEFAULT ''",
    "ALTER TABLE positions ADD COLUMN settlement_source TEXT DEFAULT ''",
    "ALTER TABLE session_features ADD COLUMN feature_pack_version TEXT DEFAULT '2'",
    "ALTER TABLE session_features ADD COLUMN data_source_chain TEXT DEFAULT ''",
    "ALTER TABLE session_features ADD COLUMN data_source_gw INTEGER DEFAULT 0",
    "CREATE TABLE IF NOT EXISTS diagnostics (session_id INTEGER NOT NULL, window TEXT NOT NULL, field TEXT NOT NULL, value TEXT, PRIMARY KEY (session_id, window, field))",
]


def get_connection(db_path: Path = DB_PATH) -> sqlite3.Connection:
    """Get a SQLite connection with WAL mode and foreign keys enabled."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path: Path = DB_PATH) -> sqlite3.Connection:
    """Initialize the database with the schema. Idempotent.

    Handles migrations from older schema versions.
    """
    conn = get_connection(db_path)
    conn.executescript(SCHEMA_SQL)

    # Check/set schema version
    cur = conn.execute("SELECT version FROM schema_version LIMIT 1")
    row = cur.fetchone()
    if row is None:
        conn.execute("INSERT INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,))
    else:
        current_version = row[0]
        if current_version < 2:
            _migrate_v1_to_v2(conn)
        if current_version < SCHEMA_VERSION:
            conn.execute("UPDATE schema_version SET version = ?", (SCHEMA_VERSION,))

    conn.commit()
    return conn


def _migrate_v1_to_v2(conn: sqlite3.Connection) -> None:
    """Migrate from schema v1 to v2 (add window/dte columns)."""
    for sql in MIGRATION_V1_TO_V2:
        try:
            conn.execute(sql)
        except sqlite3.OperationalError:
            # Column/table already exists — skip
            pass


def _serialize_legs(legs) -> str:
    """Serialize legs to JSON string for storage."""
    return json.dumps([
        {
            "strike": l.strike,
            "put_call": l.put_call,
            "action": l.action.value,
            "quantity": l.quantity,
        }
        for l in legs
    ])
