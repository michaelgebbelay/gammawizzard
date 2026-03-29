"""Named queries for all CRUD operations (v14 — no tracks)."""

from __future__ import annotations

import json
import math
import sqlite3
from typing import Dict, List, Optional

from sim.persistence.db import _serialize_legs


# --- Sessions ---

def insert_session(conn: sqlite3.Connection, session_id: int,
                   trading_date: str, spx_open: float = 0,
                   vix_open: float = 0) -> None:
    conn.execute(
        """INSERT OR REPLACE INTO sessions
           (session_id, trading_date, status, spx_open, vix_open)
           VALUES (?, ?, 'active', ?, ?)""",
        (session_id, trading_date, spx_open, vix_open),
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


def count_sessions(conn: sqlite3.Connection) -> int:
    cur = conn.execute(
        "SELECT COUNT(*) FROM sessions WHERE status='completed'",
    )
    return cur.fetchone()[0]


# --- Orders ---

def insert_order(conn: sqlite3.Connection, order, session_id: int,
                 slippage: float = 0.0) -> None:
    conn.execute(
        """INSERT INTO orders
           (order_id, agent_id, session_id,
            structure, side, legs, quantity, width, limit_price, status, fill_price,
            commission, slippage, thesis, rejection_reason)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            order.order_id, order.agent_id, session_id,
            order.structure.value, order.side.value,
            _serialize_legs(order.legs),
            order.quantity, order.width, order.limit_price, order.status.value,
            order.fill_price, order.commission, slippage,
            order.thesis, order.rejection_reason,
        ),
    )
    conn.commit()


# --- Positions ---

def insert_position(conn: sqlite3.Connection, pos) -> None:
    conn.execute(
        """INSERT INTO positions
           (position_id, agent_id, session_opened,
            structure, legs, quantity, width, entry_price, commission, expiration)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            pos.position_id, pos.agent_id, pos.session_opened,
            pos.structure.value, _serialize_legs(pos.legs),
            pos.quantity, pos.width, pos.entry_price, pos.commission,
            pos.expiration,
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


def get_open_positions(conn: sqlite3.Connection, agent_id: str) -> List[dict]:
    cur = conn.execute(
        """SELECT * FROM positions
           WHERE agent_id=? AND session_settled IS NULL""",
        (agent_id,),
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
                          session_id: int, account) -> None:
    conn.execute(
        """INSERT OR REPLACE INTO accounts
           (agent_id, session_id, starting_balance, ending_balance,
            realized_pnl, total_commissions, buying_power_used, open_position_count)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            agent_id, session_id,
            account.balance, account.balance,
            account.realized_pnl, account.total_commissions,
            account.buying_power_used, account.open_position_count,
        ),
    )
    conn.commit()


# --- Agent State (v14 memory) ---

def load_agent_state(conn: sqlite3.Connection, agent_id: str) -> Optional[dict]:
    """Load accumulated agent state (memory) from DB."""
    cur = conn.execute(
        "SELECT state_json FROM agent_state WHERE agent_id=?",
        (agent_id,),
    )
    row = cur.fetchone()
    if row:
        return json.loads(row["state_json"])
    return None


def save_agent_state(conn: sqlite3.Connection, agent_id: str,
                     state: dict) -> None:
    """Save accumulated agent state (memory) to DB."""
    conn.execute(
        """INSERT OR REPLACE INTO agent_state
           (agent_id, state_json, updated_at)
           VALUES (?, ?, datetime('now'))""",
        (agent_id, json.dumps(state)),
    )
    conn.commit()


# --- Agent Actions ---

def insert_action(conn: sqlite3.Connection, agent_id: str, session_id: int,
                  action_type: str, details: str = "",
                  reasoning: str = "", failure_reason: str = "") -> None:
    conn.execute(
        """INSERT INTO agent_actions
           (agent_id, session_id, action_type, details, reasoning, failure_reason)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (agent_id, session_id, action_type, details, reasoning, failure_reason),
    )
    conn.commit()


# --- Session Features ---

def save_session_features(conn: sqlite3.Connection, session_id: int,
                          phase: str, expiration: str,
                          features_json: str) -> None:
    conn.execute(
        """INSERT OR REPLACE INTO session_features
           (session_id, phase, expiration, features_json)
           VALUES (?, ?, ?, ?)""",
        (session_id, phase, expiration, features_json),
    )
    conn.commit()


def get_session_features(conn: sqlite3.Connection, session_id: int,
                         phase: str) -> Optional[dict]:
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


# --- Leaderboard / Hard Metrics ---

def get_hard_metrics(conn: sqlite3.Connection) -> List[dict]:
    """Compute hard financial metrics per agent for deterministic ranking.

    Returns list of dicts sorted by final_balance DESC, each containing:
        agent_id, final_balance, total_pnl, total_commissions, sessions,
        max_drawdown, max_drawdown_pct, win_rate, wins, losses, trades,
        return_pct, mar_ratio, sharpe, profit_factor
    """
    from sim.config import STARTING_CAPITAL

    agents = conn.execute(
        "SELECT DISTINCT agent_id FROM accounts"
    ).fetchall()

    results = []
    for row in agents:
        aid = row["agent_id"]

        # Equity curve: ending_balance per session in order
        balances = conn.execute(
            """SELECT ending_balance FROM accounts
               WHERE agent_id=?
               ORDER BY session_id""",
            (aid,),
        ).fetchall()
        curve = [STARTING_CAPITAL] + [r["ending_balance"] for r in balances]

        # Max drawdown from equity curve
        peak = curve[0]
        max_dd = 0.0
        for bal in curve:
            if bal > peak:
                peak = bal
            dd = peak - bal
            if dd > max_dd:
                max_dd = dd
        max_dd_pct = (max_dd / STARTING_CAPITAL * 100) if STARTING_CAPITAL > 0 else 0.0

        # Final balance and return
        final = curve[-1] if curve else STARTING_CAPITAL
        total_pnl = final - STARTING_CAPITAL
        return_pct = total_pnl / STARTING_CAPITAL * 100

        # Per-session returns for Sharpe ratio
        session_returns = []
        for i in range(1, len(curve)):
            prev = curve[i - 1]
            if prev > 0:
                session_returns.append((curve[i] - prev) / prev)

        # Sharpe ratio (annualized: sqrt(252) * mean/std)
        sharpe = 0.0
        if len(session_returns) >= 2:
            mean_r = sum(session_returns) / len(session_returns)
            var_r = sum((r - mean_r) ** 2 for r in session_returns) / (len(session_returns) - 1)
            std_r = math.sqrt(var_r) if var_r > 0 else 0.0
            if std_r > 0:
                sharpe = round(mean_r / std_r * math.sqrt(252), 2)

        # Win rate from settled positions
        wins = conn.execute(
            """SELECT COUNT(*) FROM positions
               WHERE agent_id=? AND realized_pnl IS NOT NULL
               AND realized_pnl > 0""",
            (aid,),
        ).fetchone()[0]
        losses = conn.execute(
            """SELECT COUNT(*) FROM positions
               WHERE agent_id=? AND realized_pnl IS NOT NULL
               AND realized_pnl <= 0""",
            (aid,),
        ).fetchone()[0]
        trades = wins + losses
        win_rate = wins / trades if trades > 0 else 0.0

        # Profit factor: sum(wins) / abs(sum(losses))
        gross_wins = conn.execute(
            """SELECT COALESCE(SUM(realized_pnl), 0) FROM positions
               WHERE agent_id=? AND realized_pnl IS NOT NULL AND realized_pnl > 0""",
            (aid,),
        ).fetchone()[0]
        gross_losses = abs(conn.execute(
            """SELECT COALESCE(SUM(realized_pnl), 0) FROM positions
               WHERE agent_id=? AND realized_pnl IS NOT NULL AND realized_pnl <= 0""",
            (aid,),
        ).fetchone()[0])
        profit_factor = (gross_wins / gross_losses) if gross_losses > 0 else float("inf")

        # Commissions
        comms = conn.execute(
            "SELECT SUM(total_commissions) FROM accounts WHERE agent_id=?",
            (aid,),
        ).fetchone()[0] or 0.0

        # MAR ratio: return / max drawdown (higher = better risk-adjusted)
        mar = (return_pct / max_dd_pct) if max_dd_pct > 0 else float("inf")

        sessions = len(balances)

        results.append({
            "agent_id": aid,
            "final_balance": final,
            "total_pnl": total_pnl,
            "total_commissions": comms,
            "sessions": sessions,
            "max_drawdown": max_dd,
            "max_drawdown_pct": max_dd_pct,
            "win_rate": win_rate,
            "wins": wins,
            "losses": losses,
            "trades": trades,
            "return_pct": return_pct,
            "mar_ratio": mar,
            "sharpe": sharpe,
            "profit_factor": profit_factor,
        })

    results.sort(key=lambda r: r["final_balance"], reverse=True)
    return results
