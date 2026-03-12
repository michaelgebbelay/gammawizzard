#!/usr/bin/env python3
"""
Push DualSide trade CSV rows to a Google Sheet tab.

Reads the trade CSV at DS_LOG_PATH, pivots PUT + CALL rows for the same
(trade_date, tdate) into one combined row, and upserts to a "DS_Tracking" tab.

Exit codes: 0 = data written, 1 = failure, 2 = expected skip (no CSV/no rows).

Env:
  GSHEET_ID                    - spreadsheet ID
  GOOGLE_SERVICE_ACCOUNT_JSON  - full JSON string for service account
  DS_LOG_PATH                  - path to trade CSV (default /tmp/logs/dualside_trades.csv)
  DS_TRACKING_TAB              - sheet tab name (default "DS_Tracking")
"""

import os
import re
import sys
import csv
from collections import defaultdict

# --- path setup ---
def _add_scripts_root():
    cur = os.path.abspath(os.path.dirname(__file__))
    while True:
        if os.path.basename(cur) == "scripts":
            if cur not in sys.path:
                sys.path.append(cur)
            return
        parent = os.path.dirname(cur)
        if parent == cur:
            return
        cur = parent

_IMPORT_ERR = None
try:
    _add_scripts_root()
    from lib.sheets import sheets_client, col_letter, ensure_sheet_tab, get_values
except Exception as e:
    sheets_client = None
    _IMPORT_ERR = e

DS_TRACKING_HEADER = [
    "expiry", "trade_date",
    "put_structure", "call_structure",
    "put_strikes", "call_strikes",
    "put_side", "call_side",
    "put_target", "call_target",
    "put_filled", "call_filled",
    "put_fill_price", "call_fill_price",
    "put_nbbo_bid", "put_nbbo_ask", "put_nbbo_mid",
    "call_nbbo_bid", "call_nbbo_ask", "call_nbbo_mid",
    "put_status", "call_status",
    "put_order_ids", "call_order_ids",
    "vix_value",
]

CSV_PATH = os.environ.get("DS_LOG_PATH", "/tmp/logs/dualside_trades.csv")
TAB = os.environ.get("DS_TRACKING_TAB", "DS_Tracking")


def _bail(msg: str, rc: int = 1):
    """Exit with structured code: rc=1 for failures, rc=2 for expected skips."""
    label = "FAIL" if rc == 1 else "SKIP"
    print(f"DS_GSHEET {label}: {msg}")
    sys.exit(rc)


def _osi_strikes(short_osi: str, long_osi: str) -> str:
    """Extract strikes from OSI and format as 'low/high'."""
    def _strike(osi):
        try:
            s = (osi or "").strip()
            if len(s) >= 21:
                return str(int(s[-8:]) / 1000)
            return ""
        except Exception:
            return ""
    lo = _strike(long_osi)
    hi = _strike(short_osi)
    if lo and hi:
        strikes = sorted([float(lo), float(hi)])
        return f"{strikes[0]:g}/{strikes[1]:g}"
    return ""


def _read_csv() -> list[dict]:
    if not os.path.exists(CSV_PATH):
        _bail(f"CSV not found: {CSV_PATH}", rc=2)
    with open(CSV_PATH, newline="") as f:
        return list(csv.DictReader(f))


def _pivot_rows(rows: list[dict]) -> list[list[str]]:
    """Group CSV rows by (trade_date, tdate) and pivot PUT/CALL into one row."""
    groups: dict[tuple, dict] = {}

    for r in rows:
        trade_date = r.get("trade_date", "")
        tdate = r.get("tdate", "")  # expiry
        key = (trade_date, tdate)
        if key not in groups:
            groups[key] = {"put": None, "call": None, "vix": ""}
        kind = (r.get("kind") or "").upper()
        if kind == "PUT":
            groups[key]["put"] = r
        elif kind == "CALL":
            groups[key]["call"] = r
        vix = r.get("vol_value", "")
        if vix:
            groups[key]["vix"] = vix

    out = []
    for (trade_date, tdate), g in sorted(groups.items(), reverse=True):
        p = g["put"] or {}
        c = g["call"] or {}
        row = [
            tdate,
            trade_date,
            # structure
            (p.get("name") or "").upper(),
            (c.get("name") or "").upper(),
            # strikes
            _osi_strikes(p.get("short_osi", ""), p.get("long_osi", "")),
            _osi_strikes(c.get("short_osi", ""), c.get("long_osi", "")),
            # side
            p.get("side", ""),
            c.get("side", ""),
            # target qty
            p.get("qty_requested", ""),
            c.get("qty_requested", ""),
            # filled qty
            p.get("qty_filled", ""),
            c.get("qty_filled", ""),
            # fill price
            p.get("last_price", ""),
            c.get("last_price", ""),
            # put NBBO
            p.get("nbbo_bid", ""),
            p.get("nbbo_ask", ""),
            p.get("nbbo_mid", ""),
            # call NBBO
            c.get("nbbo_bid", ""),
            c.get("nbbo_ask", ""),
            c.get("nbbo_mid", ""),
            # status
            p.get("reason", ""),
            c.get("reason", ""),
            # order IDs
            p.get("order_ids", ""),
            c.get("order_ids", ""),
            # vix
            g["vix"],
        ]
        out.append(row)
    return out


def _upsert(svc, sid: str, tab: str, new_rows: list[list[str]]):
    """Upsert by (expiry, trade_date) — columns 0 and 1."""
    existing = []
    try:
        last_col = col_letter(len(DS_TRACKING_HEADER) - 1)
        vals = (
            svc.spreadsheets()
            .values()
            .get(spreadsheetId=sid, range=f"{tab}!A2:{last_col}")
            .execute()
            .get("values", [])
        )
        for r in vals:
            row = list(r) + [""] * (len(DS_TRACKING_HEADER) - len(r))
            existing.append(row[: len(DS_TRACKING_HEADER)])
    except Exception:
        pass

    # Build lookup: (expiry, trade_date) -> row index in existing
    idx_map = {}
    for i, row in enumerate(existing):
        k = (row[0], row[1])
        idx_map[k] = i

    updates = []
    appends = []
    for nr in new_rows:
        k = (nr[0], nr[1])
        if k in idx_map:
            sheet_row = idx_map[k] + 2  # +1 header, +1 one-based
            # Merge: keep existing values where new values are empty
            # (prevents 2nd Lambda invocation from blanking 1st invocation's data)
            existing_row = existing[idx_map[k]]
            merged = []
            for col_i in range(len(DS_TRACKING_HEADER)):
                new_val = nr[col_i] if col_i < len(nr) else ""
                old_val = existing_row[col_i] if col_i < len(existing_row) else ""
                merged.append(new_val if new_val else old_val)
            updates.append((sheet_row, merged))
        else:
            appends.append(nr)

    # Batch update existing rows
    if updates:
        data = []
        for sheet_row, nr in updates:
            last_col = col_letter(len(DS_TRACKING_HEADER) - 1)
            data.append({
                "range": f"{tab}!A{sheet_row}:{last_col}{sheet_row}",
                "values": [nr],
            })
        svc.spreadsheets().values().batchUpdate(
            spreadsheetId=sid,
            body={"valueInputOption": "RAW", "data": data},
        ).execute()

    # Append new rows
    if appends:
        svc.spreadsheets().values().append(
            spreadsheetId=sid,
            range=f"{tab}!A1",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": appends},
        ).execute()

    return len(updates), len(appends)


def main():
    if sheets_client is None:
        _bail(f"sheets import failed: {_IMPORT_ERR}")

    gsheet_id = os.environ.get("GSHEET_ID", "")
    sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
    if not gsheet_id or not sa_json:
        _bail("GSHEET_ID or GOOGLE_SERVICE_ACCOUNT_JSON not set")

    svc, sid = sheets_client()

    # Read CSV
    rows = _read_csv()
    if not rows:
        print("DS_GSHEET SKIP: no rows in CSV")
        sys.exit(2)

    # Pivot
    pivoted = _pivot_rows(rows)
    print(f"DS_GSHEET: {len(rows)} CSV rows -> {len(pivoted)} tracking rows")

    # Ensure tab + header
    ensure_sheet_tab(svc, sid, TAB)
    got = (
        svc.spreadsheets()
        .values()
        .get(spreadsheetId=sid, range=f"{TAB}!1:1")
        .execute()
        .get("values", [])
    )
    if not got or got[0] != DS_TRACKING_HEADER:
        svc.spreadsheets().values().update(
            spreadsheetId=sid,
            range=f"{TAB}!A1",
            valueInputOption="RAW",
            body={"values": [DS_TRACKING_HEADER]},
        ).execute()

    # Upsert
    updated, appended = _upsert(svc, sid, TAB, pivoted)
    print(f"DS_GSHEET: updated={updated} appended={appended}")


if __name__ == "__main__":
    main()
