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
    equity_pnl REAL NOT NULL DEFAULT 0,
    drawdown REAL NOT NULL DEFAULT 0,
    max_drawdown REAL NOT NULL DEFAULT 0,
    risk_adjusted REAL NOT NULL DEFAULT 0,
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
        self._migrate_schema()
        self.conn.commit()

    def _table_columns(self, table: str) -> set[str]:
        cur = self.conn.execute(f"PRAGMA table_info({table})")
        return {str(r["name"]) for r in cur.fetchall()}

    def _ensure_column(self, table: str, column: str, column_ddl: str) -> None:
        if column in self._table_columns(table):
            return
        self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {column_ddl}")

    def _migrate_schema(self) -> None:
        # Backward-compatible migration for existing cached DBs.
        self._ensure_column("results", "equity_pnl", "REAL NOT NULL DEFAULT 0")
        self._ensure_column("results", "drawdown", "REAL NOT NULL DEFAULT 0")
        self._ensure_column("results", "max_drawdown", "REAL NOT NULL DEFAULT 0")
        self._ensure_column("results", "risk_adjusted", "REAL NOT NULL DEFAULT 0")

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
        equity_pnl: float,
        drawdown: float,
        max_drawdown: float,
        risk_adjusted: float,
        judge_score: float,
        judge_notes: str,
    ) -> None:
        self.conn.execute(
            """INSERT OR REPLACE INTO results
               (signal_date, player_id, put_pnl, call_pnl, total_pnl, equity_pnl, drawdown,
                max_drawdown, risk_adjusted, judge_score, judge_notes, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                signal_date,
                player_id,
                put_pnl,
                call_pnl,
                total_pnl,
                equity_pnl,
                drawdown,
                max_drawdown,
                risk_adjusted,
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

    def _player_pnls(self, player_id: str) -> list[float]:
        cur = self.conn.execute(
            "SELECT total_pnl FROM results WHERE player_id=? ORDER BY signal_date",
            (player_id,),
        )
        return [float(r["total_pnl"]) for r in cur.fetchall()]

    def projected_risk_metrics(self, player_id: str, pending_pnl: Optional[float] = None) -> dict:
        pnls = self._player_pnls(player_id)
        if pending_pnl is not None:
            pnls.append(float(pending_pnl))

        equity = 0.0
        peak = 0.0
        max_drawdown = 0.0
        wins = 0

        for pnl in pnls:
            equity += pnl
            if pnl > 0:
                wins += 1
            peak = max(peak, equity)
            drawdown = peak - equity
            max_drawdown = max(max_drawdown, drawdown)

        current_drawdown = max(0.0, peak - equity)
        rounds = len(pnls)
        win_rate = (wins / rounds) if rounds else 0.0
        risk_adjusted = equity - (0.60 * max_drawdown)

        return {
            "rounds": rounds,
            "equity_pnl": round(equity, 2),
            "current_drawdown": round(current_drawdown, 2),
            "max_drawdown": round(max_drawdown, 2),
            "risk_adjusted": round(risk_adjusted, 2),
            "win_rate": round(win_rate, 4),
        }

    def leaderboard(self) -> list[dict]:
        cur = self.conn.execute("SELECT DISTINCT player_id FROM results")
        player_ids = [str(r["player_id"]) for r in cur.fetchall()]
        rows: list[dict] = []

        for player_id in player_ids:
            agg = self.conn.execute(
                """SELECT COUNT(*) AS rounds,
                          SUM(total_pnl) AS total_pnl,
                          AVG(total_pnl) AS avg_pnl,
                          AVG(judge_score) AS avg_judge,
                          SUM(CASE WHEN total_pnl > 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS win_rate
                   FROM results
                   WHERE player_id=?""",
                (player_id,),
            ).fetchone()
            if not agg:
                continue
            metrics = self.projected_risk_metrics(player_id)
            rows.append(
                {
                    "player_id": player_id,
                    "rounds": int(agg["rounds"] or 0),
                    "total_pnl": float(agg["total_pnl"] or 0.0),
                    "avg_pnl": float(agg["avg_pnl"] or 0.0),
                    "avg_judge": float(agg["avg_judge"] or 0.0),
                    "win_rate": float(agg["win_rate"] or 0.0),
                    "equity_pnl": float(metrics["equity_pnl"]),
                    "max_drawdown": float(metrics["max_drawdown"]),
                    "risk_adjusted": float(metrics["risk_adjusted"]),
                }
            )

        rows.sort(
            key=lambda r: (
                r["risk_adjusted"],
                r["total_pnl"],
                -r["max_drawdown"],
            ),
            reverse=True,
        )
        return rows
