"""DuckDB connection manager for the canonical portfolio store.

Usage:
    from reporting.db import get_connection, init_schema

    con = get_connection()          # default: reporting/data/portfolio.duckdb
    init_schema(con)                # create tables if not exist
    con.execute("SELECT * FROM positions WHERE lifecycle_state = 'OPEN'")
"""

from __future__ import annotations

import os
from pathlib import Path

import duckdb


# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

_DEFAULT_DB_DIR = Path(__file__).parent / "data"
_DEFAULT_DB_NAME = "portfolio.duckdb"
_SCHEMA_PATH = Path(__file__).parent / "schema.sql"

_connections: dict[str, duckdb.DuckDBPyConnection] = {}


# ---------------------------------------------------------------------------
# Connection management
# ---------------------------------------------------------------------------

def get_connection(
    db_path: str | Path | None = None,
    read_only: bool = False,
) -> duckdb.DuckDBPyConnection:
    """Get or create a DuckDB connection.

    Default path: reporting/data/portfolio.duckdb
    Override with GAMMA_DB_PATH env var or db_path argument.
    """
    if db_path is None:
        db_path = os.environ.get("GAMMA_DB_PATH")

    if db_path is None:
        _DEFAULT_DB_DIR.mkdir(parents=True, exist_ok=True)
        db_path = _DEFAULT_DB_DIR / _DEFAULT_DB_NAME

    db_path = Path(db_path)
    key = f"{db_path}:{read_only}"

    if key not in _connections:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        _connections[key] = duckdb.connect(str(db_path), read_only=read_only)

    return _connections[key]


def close_all():
    """Close all cached connections."""
    for con in _connections.values():
        try:
            con.close()
        except Exception:
            pass
    _connections.clear()


# ---------------------------------------------------------------------------
# Schema initialization
# ---------------------------------------------------------------------------

def init_schema(con: duckdb.DuckDBPyConnection | None = None) -> None:
    """Execute schema.sql to create all tables (IF NOT EXISTS)."""
    if con is None:
        con = get_connection()

    sql = _SCHEMA_PATH.read_text()

    # DuckDB can execute multi-statement SQL directly
    con.execute(sql)


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------

def query_df(sql: str, params: list | None = None, con: duckdb.DuckDBPyConnection | None = None):
    """Execute SQL and return a pandas DataFrame."""
    if con is None:
        con = get_connection()
    if params:
        return con.execute(sql, params).fetchdf()
    return con.execute(sql).fetchdf()


def query_one(sql: str, params: list | None = None, con: duckdb.DuckDBPyConnection | None = None):
    """Execute SQL and return a single row as a tuple, or None."""
    if con is None:
        con = get_connection()
    if params:
        result = con.execute(sql, params).fetchone()
    else:
        result = con.execute(sql).fetchone()
    return result


def execute(sql: str, params: list | None = None, con: duckdb.DuckDBPyConnection | None = None):
    """Execute SQL (INSERT, UPDATE, DELETE) without returning results."""
    if con is None:
        con = get_connection()
    if params:
        con.execute(sql, params)
    else:
        con.execute(sql)
