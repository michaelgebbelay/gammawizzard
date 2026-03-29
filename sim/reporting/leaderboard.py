"""Financial leaderboard (v14 — no tracks, no judge)."""

from __future__ import annotations

import sqlite3

from sim.persistence.queries import get_hard_metrics


def hard_metrics_leaderboard(conn: sqlite3.Connection) -> str:
    """Primary leaderboard: ranked by hard financial metrics only.

    Shows: Balance, P&L, Return, Max DD, MAR, Sharpe, PF, Win%, W/L, Trades.
    """
    rows = get_hard_metrics(conn)
    if not rows:
        return "No simulation data yet."

    lines = [
        "=== Leaderboard ===",
        f"{'Rank':>4}  {'Agent':<16} {'Balance':>12} {'P&L':>10} "
        f"{'Return':>8} {'Max DD':>8} {'MAR':>6} {'Sharpe':>7} "
        f"{'PF':>6} {'Win%':>6} {'W/L':>7} {'Trades':>6}",
        f"{'─'*4}  {'─'*16} {'─'*12} {'─'*10} "
        f"{'─'*8} {'─'*8} {'─'*6} {'─'*7} "
        f"{'─'*6} {'─'*6} {'─'*7} {'─'*6}",
    ]

    for i, r in enumerate(rows, 1):
        mar_str = f"{r['mar_ratio']:.2f}" if r['mar_ratio'] < 1000 else "inf"
        pf_str = f"{r['profit_factor']:.2f}" if r['profit_factor'] < 1000 else "inf"
        lines.append(
            f"{i:>4}  {r['agent_id']:<16} "
            f"${r['final_balance']:>11,.2f} "
            f"${r['total_pnl']:>+9,.2f} "
            f"{r['return_pct']:>+7.2f}% "
            f"{r['max_drawdown_pct']:>7.2f}% "
            f"{mar_str:>6} "
            f"{r['sharpe']:>7.2f} "
            f"{pf_str:>6} "
            f"{r['win_rate']:>5.0%} "
            f"{r['wins']:>3}/{r['losses']:<3} "
            f"{r['trades']:>6}"
        )

    return "\n".join(lines)
