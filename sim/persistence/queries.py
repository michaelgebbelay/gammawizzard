"""Named queries for all CRUD operations."""

from __future__ import annotations

import json
import sqlite3
from typing import Dict, List, Optional

from sim.persistence.db import _serialize_legs


# --- Sessions ---

def insert_session(conn: sqlite3.Connection, session_id: int, track: str,
                   trading_date: str, spx_open: float = 0, vix_open: float = 0,
                   premarket_brief: str = "") -> None:
    conn.execute(
        """INSERT OR REPLACE INTO sessions
           (session_id, track, trading_date, status, spx_open, vix_open, premarket_brief)
           VALUES (?, ?, ?, 'active', ?, ?, ?)""",
        (session_id, track, trading_date, spx_open, vix_open, premarket_brief),
    )
    conn.commit()


def update_session_close(conn: sqlite3.Connection, session_id: int,
                         spx_close: float, vix_close: float,
                         intraday_range: float, status: str = "completed") -> None:
    conn.execute(
        """UPDATE sessions SET spx_close=?, vix_close=?, intraday_range=?, status=?
           WHERE session_id=?""",
        (spx_close, vix_close, intraday_range, status, session_id),
    )
    conn.commit()


def get_session(conn: sqlite3.Connection, session_id: int) -> Optional[dict]:
    cur = conn.execute("SELECT * FROM sessions WHERE session_id=?", (session_id,))
    row = cur.fetchone()
    return dict(row) if row else None


def count_sessions(conn: sqlite3.Connection, track: str) -> int:
    cur = conn.execute(
        "SELECT COUNT(*) FROM sessions WHERE track=? AND status='completed'",
        (track,),
    )
    return cur.fetchone()[0]


# --- Orders ---

def insert_order(conn: sqlite3.Connection, order, session_id: int, track: str,
                 slippage: float = 0.0) -> None:
    conn.execute(
        """INSERT INTO orders
           (order_id, agent_id, session_id, track, window, dte_at_entry, expiration,
            structure, side, legs, quantity, limit_price, status, fill_price,
            commission, slippage, thesis, rejection_reason)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            order.order_id, order.agent_id, session_id, track,
            order.window, order.dte_at_entry, order.expiration,
            order.structure.value, order.side.value,
            _serialize_legs(order.legs),
            order.quantity, order.limit_price, order.status.value,
            order.fill_price, order.commission, slippage,
            order.thesis, order.rejection_reason,
        ),
    )
    conn.commit()


# --- Positions ---

def insert_position(conn: sqlite3.Connection, pos) -> None:
    conn.execute(
        """INSERT INTO positions
           (position_id, agent_id, track, session_opened, window, dte_at_entry,
            expiration, structure, legs, quantity, entry_price, commission, width)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            pos.position_id, pos.agent_id, pos.track, pos.session_opened,
            pos.window, pos.dte_at_entry, pos.expiration,
            pos.structure.value, _serialize_legs(pos.legs),
            pos.quantity, pos.entry_price, pos.commission, pos.width,
        ),
    )
    conn.commit()


def update_position_settlement(conn: sqlite3.Connection, pos) -> None:
    conn.execute(
        """UPDATE positions SET session_settled=?, settlement_price=?,
           settlement_value=?, settlement_source=?, realized_pnl=?
           WHERE position_id=?""",
        (pos.session_settled, pos.settlement_price,
         pos.settlement_value, pos.settlement_source,
         pos.realized_pnl, pos.position_id),
    )
    conn.commit()


def get_open_positions(conn: sqlite3.Connection, agent_id: str,
                       track: str) -> List[dict]:
    cur = conn.execute(
        """SELECT * FROM positions
           WHERE agent_id=? AND track=? AND session_settled IS NULL""",
        (agent_id, track),
    )
    return [dict(r) for r in cur.fetchall()]


# --- Position Marks ---

def insert_position_mark(conn: sqlite3.Connection, position_id: str,
                         session_id: int, phase: str, mark_price: float,
                         unrealized_pnl: float, delta: float = 0,
                         gamma: float = 0, theta: float = 0,
                         vega: float = 0) -> None:
    conn.execute(
        """INSERT OR REPLACE INTO position_marks
           (position_id, session_id, phase, mark_price, unrealized_pnl,
            delta, gamma, theta, vega)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (position_id, session_id, phase, mark_price, unrealized_pnl,
         delta, gamma, theta, vega),
    )
    conn.commit()


# --- Accounts ---

def save_account_snapshot(conn: sqlite3.Connection, agent_id: str,
                          session_id: int, track: str,
                          account) -> None:
    conn.execute(
        """INSERT OR REPLACE INTO accounts
           (agent_id, session_id, track, starting_balance, ending_balance,
            realized_pnl, total_commissions, buying_power_used, open_position_count)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            agent_id, session_id, track,
            account.balance, account.balance,
            account.realized_pnl, account.total_commissions,
            account.buying_power_used, account.open_position_count,
        ),
    )
    conn.commit()


# --- Scorecards ---

def insert_scorecard(conn: sqlite3.Connection, agent_id: str, session_id: int,
                     track: str, scores: dict, notes: str = "") -> None:
    conn.execute(
        """INSERT OR REPLACE INTO scorecards
           (agent_id, session_id, track, structure_selection_score,
            strike_placement_score, risk_sizing_score,
            portfolio_exposure_score, pnl_score, total_score, judge_notes)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            agent_id, session_id, track,
            scores.get("structure_selection", 0),
            scores.get("strike_placement", 0),
            scores.get("risk_sizing", 0),
            scores.get("portfolio_exposure", 0),
            scores.get("pnl", 0),
            scores.get("total", 0),
            notes,
        ),
    )
    conn.commit()


# --- Agent Memory ---

def save_memory(conn: sqlite3.Connection, agent_id: str, session_id: int,
                track: str, summary: str, cumulative: str = "") -> None:
    conn.execute(
        """INSERT OR REPLACE INTO agent_memory
           (agent_id, session_id, track, summary, cumulative_summary)
           VALUES (?, ?, ?, ?, ?)""",
        (agent_id, session_id, track, summary, cumulative),
    )
    conn.commit()


def get_latest_memory(conn: sqlite3.Connection, agent_id: str,
                      track: str) -> Optional[dict]:
    cur = conn.execute(
        """SELECT * FROM agent_memory
           WHERE agent_id=? AND track=?
           ORDER BY session_id DESC LIMIT 1""",
        (agent_id, track),
    )
    row = cur.fetchone()
    return dict(row) if row else None


# --- Leaderboard ---

def get_leaderboard(conn: sqlite3.Connection, track: str) -> List[dict]:
    """Get financial leaderboard for all agents in a track."""
    cur = conn.execute(
        """SELECT agent_id,
                  MAX(ending_balance) as final_balance,
                  SUM(realized_pnl) as total_pnl,
                  SUM(total_commissions) as total_commissions,
                  COUNT(*) as sessions_played
           FROM accounts
           WHERE track=?
           GROUP BY agent_id
           ORDER BY final_balance DESC""",
        (track,),
    )
    return [dict(r) for r in cur.fetchall()]


# --- Rubric History ---

def save_rubric(conn: sqlite3.Connection, session_id: int, track: str,
                weights: dict, rationale: str = "") -> None:
    conn.execute(
        """INSERT OR REPLACE INTO rubric_history
           (session_id, track, weights, rationale)
           VALUES (?, ?, ?, ?)""",
        (session_id, track, json.dumps(weights), rationale),
    )
    conn.commit()


def get_latest_rubric(conn: sqlite3.Connection, track: str) -> Optional[dict]:
    cur = conn.execute(
        """SELECT * FROM rubric_history
           WHERE track=?
           ORDER BY session_id DESC LIMIT 1""",
        (track,),
    )
    row = cur.fetchone()
    if row:
        result = dict(row)
        result["weights"] = json.loads(result["weights"])
        return result
    return None


# --- Agent Actions ---

def insert_action(conn: sqlite3.Connection, agent_id: str, session_id: int,
                  track: str, action_type: str, details: str = "",
                  reasoning: str = "", failure_reason: str = "") -> None:
    conn.execute(
        """INSERT INTO agent_actions
           (agent_id, session_id, track, action_type, details, reasoning, failure_reason)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (agent_id, session_id, track, action_type, details, reasoning, failure_reason),
    )
    conn.commit()


# --- Session Features ---

def save_session_features(conn: sqlite3.Connection, session_id: int,
                          phase: str, expiration: str,
                          features_json: str) -> None:
    """Persist a FeaturePack JSON blob keyed by (session_id, phase, expiration)."""
    conn.execute(
        """INSERT OR REPLACE INTO session_features
           (session_id, phase, expiration, features_json)
           VALUES (?, ?, ?, ?)""",
        (session_id, phase, expiration, features_json),
    )
    conn.commit()


def get_session_features(conn: sqlite3.Connection, session_id: int,
                         phase: str) -> Optional[dict]:
    """Load FeaturePack from DB as parsed dict."""
    cur = conn.execute(
        """SELECT features_json FROM session_features
           WHERE session_id=? AND phase=?
           LIMIT 1""",
        (session_id, phase),
    )
    row = cur.fetchone()
    if row:
        return json.loads(row["features_json"])
    return None
