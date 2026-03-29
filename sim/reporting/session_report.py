"""Per-session markdown report generator (v14)."""

from __future__ import annotations

import sqlite3


def generate_session_report(conn: sqlite3.Connection, session_id: int) -> str:
    """Generate a markdown report for a single session."""
    cur = conn.execute("SELECT * FROM sessions WHERE session_id=?", (session_id,))
    session = cur.fetchone()
    if not session:
        return f"Session {session_id} not found."
    session = dict(session)

    lines = [
        f"# Session {session_id} Report",
        f"**Date**: {session['trading_date']}",
        f"**SPX**: {session.get('spx_open', 'N/A')} → {session.get('spx_close', 'N/A')}",
        f"**VIX**: {session.get('vix_open', 'N/A')} → {session.get('vix_close', 'N/A')}",
        f"**Intraday Range**: {session.get('intraday_range', 'N/A')} pts",
        "",
        "## Orders",
    ]

    cur = conn.execute(
        "SELECT * FROM orders WHERE session_id=? ORDER BY agent_id",
        (session_id,),
    )
    orders = [dict(r) for r in cur.fetchall()]

    if not orders:
        lines.append("No orders this session.")
    else:
        lines.append(
            "| Agent | Structure | Width | Status | Fill Price | Commission | Slippage | Thesis |"
        )
        lines.append(
            "|-------|-----------|-------|--------|------------|------------|----------|--------|"
        )
        for o in orders:
            thesis = (o.get("thesis") or "")[:50]
            lines.append(
                f"| {o['agent_id']} | {o['structure']} | {o.get('width', 0):.0f} | "
                f"{o['status']} | ${o.get('fill_price', 0):.2f} | "
                f"${o.get('commission', 0):.2f} | ${o.get('slippage', 0):.2f} | {thesis} |"
            )

    # Settlements
    lines.extend(["", "## Settlements"])
    cur = conn.execute(
        "SELECT * FROM positions WHERE session_settled=?",
        (session_id,),
    )
    settlements = [dict(r) for r in cur.fetchall()]

    if not settlements:
        lines.append("No settlements this session.")
    else:
        lines.append("| Agent | Structure | Width | Entry | Settlement | P&L |")
        lines.append("|-------|-----------|-------|-------|------------|-----|")
        for s in settlements:
            lines.append(
                f"| {s['agent_id']} | {s['structure']} | {s.get('width', 0):.0f} | "
                f"${s['entry_price']:.2f} | ${s.get('settlement_price', 0):.2f} | "
                f"${s.get('realized_pnl', 0):+.2f} |"
            )

    # Account balances
    lines.extend(["", "## Account Balances"])
    cur = conn.execute(
        "SELECT * FROM accounts WHERE session_id=? ORDER BY ending_balance DESC",
        (session_id,),
    )
    accounts = [dict(r) for r in cur.fetchall()]

    if accounts:
        lines.append("| Agent | Balance | Realized P&L | Commissions | Open Positions |")
        lines.append("|-------|---------|--------------|-------------|----------------|")
        for a in accounts:
            lines.append(
                f"| {a['agent_id']} | ${a.get('ending_balance', 0):,.2f} | "
                f"${a.get('realized_pnl', 0):+,.2f} | "
                f"${a.get('total_commissions', 0):.2f} | "
                f"{a.get('open_position_count', 0)} |"
            )

    return "\n".join(lines)
