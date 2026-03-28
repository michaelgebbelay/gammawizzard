"""Publish daily portfolio report to Google Sheets.

Writes a compact one-row-per-day summary to the Portfolio_Daily tab,
plus an open-positions snapshot to Portfolio_Positions.

Uses the same GSHEET_ID / GOOGLE_SERVICE_ACCOUNT_JSON env vars as
the existing scripts/lib/sheets.py infrastructure.

Usage:
    from reporting.publish_sheets import publish_daily

    publish_daily(report_date, con=con)  # writes to Sheets
"""

from __future__ import annotations

import os
import sys
from datetime import date
from pathlib import Path

# Ensure repo root on path for scripts/lib imports
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from reporting.db import query_df, query_one


# ---------------------------------------------------------------------------
# Tab names
# ---------------------------------------------------------------------------

SUMMARY_TAB = os.environ.get("PORTFOLIO_DAILY_TAB", "Portfolio_Daily")
POSITIONS_TAB = os.environ.get("PORTFOLIO_POSITIONS_TAB", "Portfolio_Positions")

SUMMARY_HEADERS = [
    "Date", "Banner", "Opened", "Closed", "Open",
    "Realized P&L", "Unrealized P&L", "Issues", "Stale",
]

POSITIONS_HEADERS = [
    "Strategy", "Account", "Trade Date", "Expiry", "Signal",
    "Config", "Entry", "Qty", "State",
]


# ---------------------------------------------------------------------------
# Data extraction
# ---------------------------------------------------------------------------

def _extract_summary_row(report_date: date, con) -> list:
    """Build one summary row from the DB for report_date."""
    date_str = report_date.isoformat()

    # Trust banner
    row = query_one(
        """SELECT trust_banner FROM daily_report_outputs
           WHERE report_date = ? ORDER BY generated_at DESC LIMIT 1""",
        [date_str], con=con,
    )
    banner = row[0] if row else "?"

    # Positions opened today
    opened = query_one(
        "SELECT COUNT(*) FROM positions WHERE trade_date = ?",
        [date_str], con=con,
    )
    n_opened = opened[0] if opened else 0

    # Positions closed today
    closed = query_one(
        """SELECT COUNT(*) FROM positions
           WHERE closed_at::DATE = ?
             AND lifecycle_state IN ('CLOSED', 'EXPIRED', 'ASSIGNED')""",
        [date_str], con=con,
    )
    n_closed = closed[0] if closed else 0

    # Currently open
    open_pos = query_one(
        """SELECT COUNT(*) FROM positions
           WHERE lifecycle_state IN ('OPEN', 'PARTIALLY_OPEN', 'PARTIALLY_CLOSED')""",
        con=con,
    )
    n_open = open_pos[0] if open_pos else 0

    # Realized P&L today
    pnl_row = query_one(
        """SELECT COALESCE(SUM(realized_pnl), 0) FROM positions
           WHERE closed_at::DATE = ?
             AND lifecycle_state IN ('CLOSED', 'EXPIRED', 'ASSIGNED')""",
        [date_str], con=con,
    )
    realized = round(pnl_row[0], 2) if pnl_row and pnl_row[0] else 0.0

    # Unrealized (from account snapshots if available)
    unreal_row = query_one(
        """SELECT COALESCE(SUM(unrealized_pnl), 0) FROM account_snapshots
           WHERE snapshot_date = ?""",
        [date_str], con=con,
    )
    unrealized = round(unreal_row[0], 2) if unreal_row and unreal_row[0] else 0.0

    # Unresolved issues (WARNING+)
    issues_row = query_one(
        """SELECT COUNT(*) FROM reconciliation_items
           WHERE status = 'UNRESOLVED'
             AND severity IN ('WARNING', 'ERROR', 'CRITICAL')""",
        con=con,
    )
    n_issues = issues_row[0] if issues_row else 0

    # Stale sources
    stale_row = query_one(
        "SELECT COUNT(*) FROM source_freshness WHERE is_stale = true",
        con=con,
    )
    n_stale = stale_row[0] if stale_row else 0

    return [
        date_str, banner, n_opened, n_closed, n_open,
        realized, unrealized, n_issues, n_stale,
    ]


def _extract_positions(con) -> list[list]:
    """Extract current open positions for the snapshot tab."""
    df = query_df(
        """SELECT strategy, account, trade_date, expiry_date,
                  signal, config, entry_price, qty, lifecycle_state
           FROM positions
           WHERE lifecycle_state IN ('OPEN', 'PARTIALLY_OPEN', 'PARTIALLY_CLOSED')
           ORDER BY expiry_date, strategy""",
        con=con,
    )
    rows = []
    for _, r in df.iterrows():
        ep = r["entry_price"]
        entry_str = f"{ep:.2f}" if ep and not (isinstance(ep, float) and ep != ep) else ""
        rows.append([
            str(r["strategy"]),
            str(r["account"]),
            str(r["trade_date"]),
            str(r["expiry_date"]) if r["expiry_date"] is not None else "",
            str(r["signal"]) if r["signal"] is not None else "",
            str(r["config"]) if r["config"] is not None else "",
            entry_str,
            int(r["qty"]) if r["qty"] else 0,
            str(r["lifecycle_state"]),
        ])
    return rows


# ---------------------------------------------------------------------------
# Publisher
# ---------------------------------------------------------------------------

def publish_daily(report_date: date, con=None) -> dict:
    """Publish daily summary + positions snapshot to Google Sheets.

    Returns {"summary_rows": int, "position_rows": int} on success,
    or {"skipped": reason} if credentials are missing.
    """
    # Check env vars
    gsheet_id = (os.environ.get("GSHEET_ID") or "").strip()
    sa_json = (os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON") or "").strip()
    if not gsheet_id or not sa_json:
        return {"skipped": "GSHEET_ID or GOOGLE_SERVICE_ACCOUNT_JSON not set"}

    # Late import — only needed when actually publishing
    from scripts.lib.sheets import (
        ensure_tab,
        overwrite_rows,
        read_existing,
        sheets_client,
    )

    if con is None:
        from reporting.db import get_connection, init_schema
        con = get_connection()
        init_schema(con)

    svc, sid = sheets_client()

    # --- Summary tab: append/update today's row ---
    ensure_tab(svc, sid, SUMMARY_TAB, SUMMARY_HEADERS)
    existing = read_existing(svc, sid, SUMMARY_TAB, SUMMARY_HEADERS)

    new_row = _extract_summary_row(report_date, con)
    date_str = report_date.isoformat()

    # Replace existing row for today if present, else append
    updated = False
    for i, row in enumerate(existing):
        if row[0] == date_str:
            existing[i] = new_row
            updated = True
            break
    if not updated:
        existing.append(new_row)

    # Sort by date descending (newest first)
    existing.sort(key=lambda r: r[0] if r[0] else "", reverse=True)

    overwrite_rows(svc, sid, SUMMARY_TAB, SUMMARY_HEADERS, existing)

    # --- Positions tab: full overwrite with current snapshot ---
    ensure_tab(svc, sid, POSITIONS_TAB, POSITIONS_HEADERS)
    pos_rows = _extract_positions(con)
    overwrite_rows(svc, sid, POSITIONS_TAB, POSITIONS_HEADERS, pos_rows)

    return {"summary_rows": len(existing), "position_rows": len(pos_rows)}
