"""SQLite persistence for the live game."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional

from sim_live.config import DB_PATH

SCHEMA = """
CREATE TABLE IF NOT EXISTS rounds (
    signal_date TEXT PRIMARY KEY,
    tdate TEXT NOT NULL,
    status TEXT NOT NULL,                -- pending|settled
    public_snapshot_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    settled_at TEXT
);

CREATE TABLE IF NOT EXISTS decisions (
    signal_date TEXT NOT NULL,
    player_id TEXT NOT NULL,
    decision_json TEXT NOT NULL,
    valid INTEGER NOT NULL DEFAULT 1,
    error TEXT DEFAULT '',
    created_at TEXT NOT NULL,
    PRIMARY KEY (signal_date, player_id),
    FOREIGN KEY (signal_date) REFERENCES rounds(signal_date)
);

CREATE TABLE IF NOT EXISTS results (
    signal_date TEXT NOT NULL,
    player_id TEXT NOT NULL,
    put_pnl REAL NOT NULL,
    call_pnl REAL NOT NULL,
    total_pnl REAL NOT NULL,
    judge_score REAL NOT NULL,
    judge_notes TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    PRIMARY KEY (signal_date, player_id),
    FOREIGN KEY (signal_date) REFERENCES rounds(signal_date)
);

CREATE TABLE IF NOT EXISTS player_state (
    player_id TEXT PRIMARY KEY,
    state_json TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
"""


def _now() -> str:
    return datetime.utcnow().isoformat(timespec="seconds")


class Store:
    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA busy_timeout=3000")
        self.conn.executescript(SCHEMA)
        self.conn.commit()

    def upsert_round(self, signal_date: str, tdate: str, public_snapshot: dict) -> None:
        self.conn.execute(
            """INSERT INTO rounds
               (signal_date, tdate, status, public_snapshot_json, created_at, settled_at)
               VALUES (?, ?, 'pending', ?, ?, NULL)
               ON CONFLICT(signal_date) DO UPDATE SET
                   tdate=excluded.tdate,
                   public_snapshot_json=excluded.public_snapshot_json""",
            (
                signal_date,
                tdate,
                json.dumps(public_snapshot),
                _now(),
            ),
        )
        self.conn.commit()

    def mark_settled(self, signal_date: str) -> None:
        self.conn.execute(
            "UPDATE rounds SET status='settled', settled_at=? WHERE signal_date=?",
            (_now(), signal_date),
        )
        self.conn.commit()

    def get_round(self, signal_date: str) -> Optional[dict]:
        cur = self.conn.execute(
            "SELECT * FROM rounds WHERE signal_date=?",
            (signal_date,),
        )
        row = cur.fetchone()
        return dict(row) if row else None

    def save_decision(self, signal_date: str, player_id: str, decision: dict, valid: bool, error: str = "") -> None:
        self.conn.execute(
            """INSERT OR REPLACE INTO decisions
               (signal_date, player_id, decision_json, valid, error, created_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (signal_date, player_id, json.dumps(decision), 1 if valid else 0, error, _now()),
        )
        self.conn.commit()

    def get_decisions(self, signal_date: str) -> list[dict]:
        cur = self.conn.execute(
            "SELECT * FROM decisions WHERE signal_date=? ORDER BY player_id",
            (signal_date,),
        )
        return [dict(r) for r in cur.fetchall()]

    def save_result(
        self,
        signal_date: str,
        player_id: str,
        put_pnl: float,
        call_pnl: float,
        total_pnl: float,
        judge_score: float,
        judge_notes: str,
    ) -> None:
        self.conn.execute(
            """INSERT OR REPLACE INTO results
               (signal_date, player_id, put_pnl, call_pnl, total_pnl, judge_score, judge_notes, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                signal_date,
                player_id,
                put_pnl,
                call_pnl,
                total_pnl,
                judge_score,
                judge_notes,
                _now(),
            ),
        )
        self.conn.commit()

    def get_results(self, signal_date: str) -> list[dict]:
        cur = self.conn.execute(
            "SELECT * FROM results WHERE signal_date=? ORDER BY total_pnl DESC",
            (signal_date,),
        )
        return [dict(r) for r in cur.fetchall()]

    def pending_rounds_due(self, settlement_date: str) -> list[dict]:
        cur = self.conn.execute(
            """SELECT * FROM rounds
               WHERE status='pending' AND tdate <= ?
               ORDER BY signal_date""",
            (settlement_date,),
        )
        return [dict(r) for r in cur.fetchall()]

    def load_player_state(self, player_id: str) -> dict:
        cur = self.conn.execute(
            "SELECT state_json FROM player_state WHERE player_id=?",
            (player_id,),
        )
        row = cur.fetchone()
        if not row:
            return {}
        try:
            return json.loads(row["state_json"])
        except json.JSONDecodeError:
            return {}

    def save_player_state(self, player_id: str, state: dict) -> None:
        self.conn.execute(
            """INSERT OR REPLACE INTO player_state
               (player_id, state_json, updated_at)
               VALUES (?, ?, ?)""",
            (player_id, json.dumps(state), _now()),
        )
        self.conn.commit()

    def leaderboard(self) -> list[dict]:
        cur = self.conn.execute(
            """SELECT player_id,
                      COUNT(*) AS rounds,
                      SUM(total_pnl) AS total_pnl,
                      AVG(total_pnl) AS avg_pnl,
                      AVG(judge_score) AS avg_judge
               FROM results
               GROUP BY player_id
               ORDER BY total_pnl DESC"""
        )
        return [dict(r) for r in cur.fetchall()]
