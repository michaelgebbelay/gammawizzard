"""Position lifecycle engine.

Links fills and legs into positions, tracks lifecycle state transitions,
handles partial fills, expiry, assignment, rolls, and broken states.

The position engine is the heart of the system — it answers "what is open
right now?" from one place.

State machine:
    INTENDED → PARTIALLY_OPEN → OPEN → PARTIALLY_CLOSED → CLOSED
                                     → EXPIRED
                                     → ASSIGNED
                                     → BROKEN

Provenance: STRATEGY | MANUAL | CORRECTION
Closure reasons: TARGET | STOP | EXPIRY | ASSIGNMENT | ROLL | MANUAL | UNKNOWN
"""

from __future__ import annotations

import json
import uuid
from datetime import date, datetime, timezone

from reporting.db import execute, get_connection, init_schema, query_df, query_one


# ---------------------------------------------------------------------------
# Valid state transitions
# ---------------------------------------------------------------------------

VALID_TRANSITIONS = {
    "INTENDED": {"PARTIALLY_OPEN", "OPEN", "BROKEN"},
    "PARTIALLY_OPEN": {"OPEN", "PARTIALLY_CLOSED", "CLOSED", "BROKEN"},
    "OPEN": {"PARTIALLY_CLOSED", "CLOSED", "EXPIRED", "ASSIGNED", "BROKEN"},
    "PARTIALLY_CLOSED": {"CLOSED", "EXPIRED", "ASSIGNED", "BROKEN"},
    # Terminal states — no further transitions
    "CLOSED": set(),
    "EXPIRED": set(),
    "ASSIGNED": set(),
    "BROKEN": {"CLOSED", "EXPIRED", "ASSIGNED"},  # Can be resolved
}


# ---------------------------------------------------------------------------
# Position creation from fills
# ---------------------------------------------------------------------------

def create_position_from_fill(con, fill_row: dict, intent_row: dict | None = None) -> str:
    """Create a new position record from a fill event.

    If an intent exists for the trade_group_id, uses it for metadata.
    Returns the position_id.
    """
    position_id = fill_row["trade_group_id"]

    existing = query_one(
        "SELECT position_id FROM positions WHERE position_id = ?",
        [position_id], con=con,
    )
    if existing:
        return position_id

    # Get strategy run for additional metadata
    run = query_one(
        "SELECT strategy, account, trade_date, signal, config FROM strategy_runs WHERE run_id = ?",
        [fill_row["run_id"]], con=con,
    )

    strategy = run[0] if run else fill_row.get("strategy", "unknown")
    account = run[1] if run else fill_row.get("account", "unknown")
    trade_date = run[2] if run else date.today().isoformat()
    signal = run[3] if run else None
    config = run[4] if run else None

    # Determine expiry from legs
    expiry_date = None
    legs = json.loads(fill_row.get("legs", "[]")) if fill_row.get("legs") else []
    if intent_row:
        intent_legs = json.loads(intent_row.get("legs", "[]")) if intent_row.get("legs") else []
        legs = legs or intent_legs

    for leg in legs:
        osi = leg.get("osi", "").replace(" ", "")  # handle padded roots
        if len(osi) >= 15:
            # Parse expiry from OSI: SPXW260211P06700000 → 2026-02-11
            # Root is variable length (1-6 chars), date starts after it.
            # Find the first digit sequence of 6+ chars as YYMMDD.
            import re
            m = re.search(r"(\d{6})[CP]", osi)
            if m:
                try:
                    yr = int(m.group(1)[:2]) + 2000
                    mo = int(m.group(1)[2:4])
                    dy = int(m.group(1)[4:6])
                    expiry_date = date(yr, mo, dy).isoformat()
                    break
                except (ValueError, IndexError):
                    pass

    now = datetime.now(timezone.utc).isoformat()
    initial_state = fill_row.get("_initial_state", "OPEN")

    execute(
        """INSERT INTO positions
           (position_id, strategy, account, trade_date, expiry_date,
            lifecycle_state, provenance, entry_price, qty,
            signal, config, opened_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, 'STRATEGY', ?, ?, ?, ?, ?, ?)""",
        [
            position_id,
            strategy,
            account,
            trade_date,
            expiry_date,
            initial_state,
            fill_row.get("fill_price", 0),
            fill_row.get("fill_qty", 0),
            signal,
            config,
            now,
            now,
        ],
        con=con,
    )

    # Create position legs
    for leg in legs:
        leg_id = uuid.uuid4().hex[:16]
        execute(
            """INSERT INTO position_legs
               (leg_id, position_id, osi, option_type, strike, expiry_date,
                action, qty, fill_price)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                leg_id,
                position_id,
                leg.get("osi", ""),
                leg.get("option_type", "UNKNOWN"),
                leg.get("strike", 0),
                expiry_date,
                leg.get("action", "UNKNOWN"),
                leg.get("qty", 0),
                leg.get("fill_price"),
            ],
            con=con,
        )

    return position_id


# ---------------------------------------------------------------------------
# State transitions
# ---------------------------------------------------------------------------

def transition_state(
    con,
    position_id: str,
    new_state: str,
    closure_reason: str | None = None,
    exit_price: float | None = None,
    realized_pnl: float | None = None,
) -> bool:
    """Transition a position to a new lifecycle state.

    Validates the transition against the state machine. Returns True if
    the transition was applied, False if invalid.
    """
    row = query_one(
        "SELECT lifecycle_state FROM positions WHERE position_id = ?",
        [position_id], con=con,
    )
    if not row:
        return False

    current_state = row[0]
    if new_state not in VALID_TRANSITIONS.get(current_state, set()):
        return False

    now = datetime.now(timezone.utc).isoformat()
    is_terminal = new_state in ("CLOSED", "EXPIRED", "ASSIGNED")

    updates = ["lifecycle_state = ?", "updated_at = ?"]
    params = [new_state, now]

    if closure_reason:
        updates.append("closure_reason = ?")
        params.append(closure_reason)

    if exit_price is not None:
        updates.append("exit_price = ?")
        params.append(exit_price)

    if realized_pnl is not None:
        updates.append("realized_pnl = ?")
        params.append(realized_pnl)

    if is_terminal:
        updates.append("closed_at = ?")
        params.append(now)

    params.append(position_id)
    execute(
        f"UPDATE positions SET {', '.join(updates)} WHERE position_id = ?",
        params,
        con=con,
    )
    return True


# ---------------------------------------------------------------------------
# Expiry processing
# ---------------------------------------------------------------------------

def process_expiries(con, as_of_date: date | None = None) -> int:
    """Mark all OPEN positions past their expiry_date as EXPIRED.

    Returns count of positions expired.
    """
    if as_of_date is None:
        as_of_date = date.today()

    df = query_df(
        """SELECT position_id FROM positions
           WHERE lifecycle_state IN ('OPEN', 'PARTIALLY_CLOSED')
             AND expiry_date IS NOT NULL
             AND expiry_date < ?""",
        [as_of_date.isoformat()],
        con=con,
    )

    count = 0
    for _, row in df.iterrows():
        if transition_state(con, row["position_id"], "EXPIRED", closure_reason="EXPIRY"):
            count += 1

    return count


# ---------------------------------------------------------------------------
# Roll tracking
# ---------------------------------------------------------------------------

def record_roll(
    con,
    from_position_id: str,
    to_position_id: str,
) -> None:
    """Record a roll relationship between two positions.

    Marks the old position as closed with ROLL reason and links the two.
    """
    # Close the old position with ROLL reason first (before FK insert)
    transition_state(con, from_position_id, "CLOSED", closure_reason="ROLL")

    # ROLLED_FROM on the new position pointing to the old
    rel_id = uuid.uuid4().hex[:16]
    execute(
        """INSERT INTO position_relationships
           (id, from_position_id, to_position_id, relationship_type)
           VALUES (?, ?, ?, 'ROLLED_FROM')""",
        [rel_id, from_position_id, to_position_id],
        con=con,
    )


# ---------------------------------------------------------------------------
# Position queries
# ---------------------------------------------------------------------------

def get_open_positions(con=None, strategy: str | None = None, account: str | None = None):
    """Get all currently open positions as a DataFrame."""
    if con is None:
        con = get_connection()

    sql = """SELECT p.*,
                    (SELECT json_group_array(json_object(
                        'osi', pl.osi, 'option_type', pl.option_type,
                        'strike', pl.strike, 'action', pl.action, 'qty', pl.qty
                    )) FROM position_legs pl WHERE pl.position_id = p.position_id) as legs
             FROM positions p
             WHERE p.lifecycle_state IN ('OPEN', 'PARTIALLY_OPEN', 'PARTIALLY_CLOSED')"""

    params = []
    if strategy:
        sql += " AND p.strategy = ?"
        params.append(strategy)
    if account:
        sql += " AND p.account = ?"
        params.append(account)

    sql += " ORDER BY p.trade_date DESC"
    return query_df(sql, params, con=con)


def get_positions_by_date(con, trade_date: date | str):
    """Get all positions opened on a specific date."""
    if isinstance(trade_date, date):
        trade_date = trade_date.isoformat()

    return query_df(
        """SELECT * FROM positions WHERE trade_date = ?
           ORDER BY strategy, account""",
        [trade_date],
        con=con,
    )


def get_closed_positions(con, since_date: date | str | None = None):
    """Get closed positions, optionally since a date."""
    sql = """SELECT * FROM positions
             WHERE lifecycle_state IN ('CLOSED', 'EXPIRED', 'ASSIGNED')"""
    params = []

    if since_date:
        if isinstance(since_date, date):
            since_date = since_date.isoformat()
        sql += " AND closed_at >= ?"
        params.append(since_date)

    sql += " ORDER BY closed_at DESC"
    return query_df(sql, params, con=con)


# ---------------------------------------------------------------------------
# Materialize positions from fills (batch)
# ---------------------------------------------------------------------------

def materialize_positions(con=None) -> int:
    """Create position records for all fills that don't have one yet.

    Aggregates multiple fills by trade_group_id so partial fills produce a
    single position with the correct total qty and weighted-average price.

    Returns count of new positions created.
    """
    if con is None:
        con = get_connection()

    # Find trade_group_ids with fills but no position, aggregating fills
    df = query_df(
        """SELECT f.trade_group_id,
                  MAX(f.run_id) AS run_id,
                  SUM(f.fill_qty) AS total_qty,
                  SUM(f.fill_qty * f.fill_price) / NULLIF(SUM(f.fill_qty), 0) AS avg_price,
                  MAX(f.legs) AS legs
           FROM fills f
           LEFT JOIN positions p ON f.trade_group_id = p.position_id
           WHERE p.position_id IS NULL
           GROUP BY f.trade_group_id""",
        con=con,
    )

    count = 0
    for _, row in df.iterrows():
        # Check for an intent
        intent = query_one(
            """SELECT intent_id, legs, side, direction, target_qty, limit_price
               FROM intended_trades WHERE trade_group_id = ?""",
            [row["trade_group_id"]], con=con,
        )

        intent_dict = None
        target_qty = None
        if intent:
            intent_dict = {
                "legs": intent[1],
                "side": intent[2],
                "direction": intent[3],
            }
            target_qty = intent[4]

        total_qty = int(row["total_qty"] or 0)
        avg_price = float(row["avg_price"] or 0)

        # Determine lifecycle state based on fill completeness
        if target_qty and total_qty < target_qty:
            initial_state = "PARTIALLY_OPEN"
        else:
            initial_state = "OPEN"

        fill_dict = {
            "trade_group_id": row["trade_group_id"],
            "run_id": row["run_id"],
            "fill_qty": total_qty,
            "fill_price": avg_price,
            "legs": row["legs"],
            "_initial_state": initial_state,
        }

        create_position_from_fill(con, fill_dict, intent_dict)
        count += 1

    return count
