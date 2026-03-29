"""Accumulating agent memory (v14) — structured records, no LLM compression.

Each agent maintains a rolling memory of their trading history:
- Last 20 sessions: full detail
- Older: compressed statistical summary
- Total ~500 token cap
"""

from __future__ import annotations

import json
from typing import Dict, List, Optional


DETAIL_WINDOW = 20  # keep full detail for last N sessions
MAX_TOKEN_ESTIMATE = 500  # approximate cap for memory text


def build_session_record(
    session_id: int,
    action: str,
    structure: str = "",
    strikes: str = "",
    width: float = 0,
    fill_price: float = 0,
    pnl: Optional[float] = None,
    market_context: str = "",
    balance: float = 0,
    thesis: str = "",
) -> dict:
    """Build a structured session record for memory."""
    record = {
        "session": session_id,
        "action": action,
    }
    if structure:
        record["structure"] = structure
    if strikes:
        record["strikes"] = strikes
    if width > 0:
        record["width"] = width
    if fill_price > 0:
        record["fill_price"] = round(fill_price, 2)
    if pnl is not None:
        record["pnl"] = round(pnl, 2)
    if market_context:
        record["market"] = market_context[:100]
    if balance > 0:
        record["balance"] = round(balance, 2)
    if thesis:
        record["thesis"] = thesis[:80]
    return record


def update_memory(state: Optional[dict], new_record: dict) -> dict:
    """Add a session record to the agent's accumulated memory state.

    Args:
        state: Existing memory state dict, or None for first session.
        new_record: Session record from build_session_record().

    Returns:
        Updated state dict ready for persistence.
    """
    if state is None:
        state = {
            "records": [],
            "stats": {
                "total_sessions": 0,
                "trades": 0,
                "holds": 0,
                "wins": 0,
                "losses": 0,
                "total_pnl": 0.0,
                "best_pnl": 0.0,
                "worst_pnl": 0.0,
                "structures": {},
            },
        }

    records = state["records"]
    stats = state["stats"]

    # Update running stats
    stats["total_sessions"] += 1
    action = new_record.get("action", "hold")
    pnl = new_record.get("pnl")

    if action == "trade":
        stats["trades"] += 1
        struct = new_record.get("structure", "unknown")
        stats["structures"][struct] = stats["structures"].get(struct, 0) + 1

        if pnl is not None:
            stats["total_pnl"] += pnl
            if pnl > 0:
                stats["wins"] += 1
            else:
                stats["losses"] += 1
            stats["best_pnl"] = max(stats["best_pnl"], pnl)
            stats["worst_pnl"] = min(stats["worst_pnl"], pnl)
    else:
        stats["holds"] += 1

    # Add new record and trim to detail window
    records.append(new_record)
    if len(records) > DETAIL_WINDOW:
        records[:] = records[-DETAIL_WINDOW:]

    state["records"] = records
    state["stats"] = stats
    return state


def format_memory(state: Optional[dict]) -> str:
    """Format accumulated memory into concise text for agent context.

    Stays within ~500 token budget.
    """
    if state is None:
        return ""

    stats = state.get("stats", {})
    records = state.get("records", [])

    lines = []

    # Running stats summary
    total = stats.get("total_sessions", 0)
    trades = stats.get("trades", 0)
    wins = stats.get("wins", 0)
    losses = stats.get("losses", 0)
    pnl = stats.get("total_pnl", 0)
    win_rate = f"{wins / trades:.0%}" if trades > 0 else "N/A"

    lines.append(
        f"Sessions: {total} | Trades: {trades} | Holds: {stats.get('holds', 0)} | "
        f"Win Rate: {win_rate} | Cumulative P&L: ${pnl:+,.0f}"
    )

    # Structure breakdown
    structs = stats.get("structures", {})
    if structs:
        parts = [f"{k}: {v}" for k, v in sorted(structs.items(), key=lambda x: -x[1])]
        lines.append(f"Structures: {', '.join(parts)}")

    # Best/worst
    if trades > 0:
        lines.append(
            f"Best: ${stats.get('best_pnl', 0):+,.0f} | "
            f"Worst: ${stats.get('worst_pnl', 0):+,.0f}"
        )

    # Recent session details (last 5 for brevity)
    recent = records[-5:]
    if recent:
        lines.append("")
        lines.append("Recent sessions:")
        for r in recent:
            sess = r.get("session", "?")
            act = r.get("action", "?")
            if act == "trade":
                struct = r.get("structure", "?")
                p = r.get("pnl")
                pnl_str = f"P&L=${p:+,.0f}" if p is not None else "pending"
                strike_str = r.get("strikes", "")
                w = r.get("width", 0)
                lines.append(f"  S{sess}: {struct} {strike_str} ({w}w) → {pnl_str}")
            else:
                lines.append(f"  S{sess}: HOLD")

    return "\n".join(lines)
