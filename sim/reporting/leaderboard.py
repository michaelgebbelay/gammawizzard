"""Financial and judge dual leaderboards."""

from __future__ import annotations

import sqlite3
from typing import List

from sim.persistence.queries import get_leaderboard


def financial_leaderboard(conn: sqlite3.Connection, track: str) -> str:
    """Generate financial leaderboard as formatted text."""
    rows = get_leaderboard(conn, track)
    if not rows:
        return f"No data for track: {track}"

    lines = [
        f"=== Financial Leaderboard — Track: {track} ===",
        f"{'Rank':>4}  {'Agent':<22} {'Balance':>12} {'Total P&L':>12} "
        f"{'Commissions':>12} {'Sessions':>8}",
        f"{'─'*4}  {'─'*22} {'─'*12} {'─'*12} {'─'*12} {'─'*8}",
    ]

    for i, row in enumerate(rows, 1):
        lines.append(
            f"{i:>4}  {row['agent_id']:<22} "
            f"${row['final_balance']:>11,.2f} "
            f"${row['total_pnl']:>+11,.2f} "
            f"${row['total_commissions']:>11,.2f} "
            f"{row['sessions_played']:>8}"
        )

    return "\n".join(lines)


def judge_leaderboard(conn: sqlite3.Connection, track: str) -> str:
    """Generate judge scorecard leaderboard."""
    cur = conn.execute(
        """SELECT agent_id,
                  AVG(total_score) as avg_score,
                  AVG(structure_selection_score) as avg_structure,
                  AVG(strike_placement_score) as avg_strike,
                  AVG(risk_sizing_score) as avg_risk,
                  AVG(portfolio_exposure_score) as avg_exposure,
                  AVG(pnl_score) as avg_pnl,
                  COUNT(*) as sessions_scored
           FROM scorecards
           WHERE track=?
           GROUP BY agent_id
           ORDER BY avg_score DESC""",
        (track,),
    )
    rows = [dict(r) for r in cur.fetchall()]

    if not rows:
        return f"No scorecard data for track: {track}"

    lines = [
        f"=== Judge Leaderboard — Track: {track} ===",
        f"{'Rank':>4}  {'Agent':<22} {'Avg Score':>10} {'Structure':>10} "
        f"{'Strikes':>10} {'Sizing':>10} {'Exposure':>10} {'P&L':>10} {'Sessions':>8}",
        f"{'─'*4}  {'─'*22} {'─'*10} {'─'*10} {'─'*10} {'─'*10} {'─'*10} {'─'*10} {'─'*8}",
    ]

    for i, row in enumerate(rows, 1):
        lines.append(
            f"{i:>4}  {row['agent_id']:<22} "
            f"{row['avg_score']:>10.2f} "
            f"{row['avg_structure']:>10.2f} "
            f"{row['avg_strike']:>10.2f} "
            f"{row['avg_risk']:>10.2f} "
            f"{row['avg_exposure']:>10.2f} "
            f"{row['avg_pnl']:>10.2f} "
            f"{row['sessions_scored']:>8}"
        )

    return "\n".join(lines)
