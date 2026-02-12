#!/usr/bin/env python3
"""
Write daily trading summary to CS_Summary tab.

Reads all rows from CS_Tracking, builds a daily pivot with GW signal prices +
per-account fills side by side, and writes to CS_Summary.

Layout:
  Rows 1-8:   User-managed summary formulas (Trade Total, Edge, etc.) — never touched
  Row 9:      Section headers (GW, schwab, TT IRA, TT IND)
  Row 10:     Column headers (Date, QTY, price put, price call, Total with comm, ...)
  Row 11+:    Daily data (one row per date, newest first)

"Total with comm" columns (E, I, M, Q) are user-managed formulas — script never
writes to them.

NON-BLOCKING BY DEFAULT (same pattern as other gsheet scripts).

Env:
  GSHEET_ID                    - spreadsheet ID
  GOOGLE_SERVICE_ACCOUNT_JSON  - full JSON string for service account
  CS_TRACKING_TAB              - source tab (default "CS_Tracking")
  CS_GW_SIGNAL_TAB             - GW signal tab (default "GW_Signal")
  CS_SUMMARY_TAB               - target tab (default "CS_Summary")
  CS_GSHEET_STRICT             - "1" to fail hard on errors
"""

import os
import sys
import json
from collections import defaultdict

# --- optional imports (skip if missing) ---
_IMPORT_ERR = None
try:
    from googleapiclient.discovery import build
    from google.oauth2 import service_account
except Exception as e:
    build = None
    service_account = None
    _IMPORT_ERR = e

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

TAG = "CS_SUMMARY"

# --- Layout constants ---
SECTION_HEADER_ROW = 9   # "GW", "schwab", etc.
COLUMN_HEADER_ROW = 10   # "Date", "QTY", "price put", etc.
DATA_START_ROW = 11      # First data row

SECTION_HEADER = [
    "",
    "GW", "", "", "",
    "schwab", "", "", "",
    "TT IRA", "", "", "",
    "TT IND", "", "", "",
]

COLUMN_HEADER = [
    "Date",
    "QTY", "price put", "price call", "Total with comm",
    "QTY", "price put", "price call", "Total with comm",
    "QTY", "price put", "price call", "Total with comm",
    "QTY", "price put", "price call", "Total with comm",
]

ACCOUNT_ORDER = ["schwab", "tt-ira", "tt-individual"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def strict_enabled() -> bool:
    return (os.environ.get("CS_GSHEET_STRICT", "0") or "0").strip().lower() in ("1", "true", "yes", "y")


def log(msg: str):
    print(f"{TAG}: {msg}")


def skip(msg: str) -> int:
    log(f"SKIP — {msg}")
    return 0


def fail(msg: str, code: int = 2) -> int:
    print(f"{TAG}: ERROR — {msg}", file=sys.stderr)
    return code


def creds_from_env():
    raw = (os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON") or "").strip()
    if not raw:
        return None
    info = json.loads(raw)
    return service_account.Credentials.from_service_account_info(info, scopes=SCOPES)


def ensure_sheet_tab(svc, sid: str, title: str) -> int:
    meta = svc.spreadsheets().get(spreadsheetId=sid, fields="sheets.properties").execute()
    for s in (meta.get("sheets") or []):
        p = s.get("properties") or {}
        if (p.get("title") or "") == title:
            return int(p.get("sheetId"))
    req = {"requests": [{"addSheet": {"properties": {"title": title}}}]}
    r = svc.spreadsheets().batchUpdate(spreadsheetId=sid, body=req).execute()
    return int(r["replies"][0]["addSheet"]["properties"]["sheetId"])


def signed_price(price_str: str, side_str: str) -> str:
    """Apply sign based on trade side: CREDIT = positive, DEBIT = negative."""
    if not price_str:
        return ""
    try:
        price = float(price_str)
        if not price:
            return "0"
        if (side_str or "").strip().upper() == "DEBIT":
            price = -price
        return str(price)
    except (ValueError, TypeError):
        return price_str


# ---------------------------------------------------------------------------
# Daily detail computation
# ---------------------------------------------------------------------------

def read_gw_signal(svc, spreadsheet_id: str, tab: str) -> dict:
    """Read GW_Signal tab and return {date: {put_price, call_price}}."""
    try:
        resp = svc.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id, range=f"{tab}!A1:ZZ"
        ).execute()
        all_rows = resp.get("values") or []
        if len(all_rows) < 2:
            return {}
        header = all_rows[0]
        result = {}
        for vals in all_rows[1:]:
            d = {header[i]: (vals[i] if i < len(vals) else "") for i in range(len(header))}
            dt = (d.get("date") or "").strip()
            if dt:
                result[dt] = {
                    "put_price": (d.get("put_price") or "").strip(),
                    "call_price": (d.get("call_price") or "").strip(),
                }
        log(f"read {len(result)} GW signal rows")
        return result
    except Exception as e:
        log(f"WARN — could not read GW_Signal: {e}")
        return {}


def compute_daily_detail(rows: list[dict], gw_signal: dict) -> list[tuple]:
    """Build daily pivot: one row per date with GW + 3 accounts side by side.

    Returns list of (date, gw_group, [schwab_group, ira_group, ind_group])
    where each group is [qty, price_put, price_call].
    """
    by_date = defaultdict(dict)
    for row in rows:
        dt = (row.get("date") or "").strip()
        acct = (row.get("account") or "").strip()
        if dt and acct:
            by_date[dt][acct] = row

    result = []
    for dt in sorted(by_date.keys(), reverse=True):
        accts = by_date[dt]
        any_row = next(iter(accts.values()), {})
        put_side = (any_row.get("put_side") or "").strip()
        call_side = (any_row.get("call_side") or "").strip()

        # GW prices from GW_Signal tab (authoritative source)
        gw = gw_signal.get(dt, {})
        gw_put = signed_price(gw.get("put_price", ""), put_side)
        gw_call = signed_price(gw.get("call_price", ""), call_side)
        gw_group = ["1", gw_put, gw_call]

        account_groups = []
        for acct_name in ACCOUNT_ORDER:
            r = accts.get(acct_name)
            if r:
                qty = r.get("put_filled", "0")
                p_side = (r.get("put_side") or "").strip()
                c_side = (r.get("call_side") or "").strip()
                price_put = signed_price(r.get("put_fill_price", ""), p_side)
                price_call = signed_price(r.get("call_fill_price", ""), c_side)
            else:
                qty = "0"
                price_put = ""
                price_call = ""
            account_groups.append([qty, price_put, price_call])

        result.append((dt, gw_group, account_groups))

    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    strict = strict_enabled()

    if build is None or service_account is None:
        msg = f"google sheets libs not installed ({_IMPORT_ERR})"
        return fail(msg, 2) if strict else skip(msg)

    spreadsheet_id = (os.environ.get("GSHEET_ID") or "").strip()
    if not spreadsheet_id:
        return fail("GSHEET_ID missing", 2) if strict else skip("GSHEET_ID missing")

    raw_sa = (os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON") or "").strip()
    if not raw_sa:
        return fail("SA creds missing", 2) if strict else skip("SA creds missing")

    tracking_tab = (os.environ.get("CS_TRACKING_TAB") or "CS_Tracking").strip()
    gw_signal_tab = (os.environ.get("CS_GW_SIGNAL_TAB") or "GW_Signal").strip()
    summary_tab = (os.environ.get("CS_SUMMARY_TAB") or "CS_Summary").strip()

    try:
        creds = creds_from_env()
        if creds is None:
            return fail("SA creds empty", 2) if strict else skip("SA creds empty")
        svc = build("sheets", "v4", credentials=creds, cache_discovery=False)

        # Read tracking data
        resp = svc.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id, range=f"{tracking_tab}!A1:ZZ"
        ).execute()
        all_rows = resp.get("values") or []
        if len(all_rows) < 2:
            return skip("no tracking data yet")

        header = all_rows[0]
        rows = []
        for vals in all_rows[1:]:
            d = {header[i]: (vals[i] if i < len(vals) else "") for i in range(len(header))}
            rows.append(d)

        log(f"read {len(rows)} tracking rows")

        # Read GW signal prices (authoritative source for GW put_price/call_price)
        gw_signal = read_gw_signal(svc, spreadsheet_id, gw_signal_tab)

        # Compute daily detail
        daily_rows = compute_daily_detail(rows, gw_signal)
        if not daily_rows:
            return skip("no daily data")

        # Write to summary tab
        ensure_sheet_tab(svc, spreadsheet_id, summary_tab)

        # Write section + column headers (rows 9-10)
        header_updates = [
            {"range": f"{summary_tab}!A{SECTION_HEADER_ROW}:Q{SECTION_HEADER_ROW}",
             "values": [SECTION_HEADER]},
            {"range": f"{summary_tab}!A{COLUMN_HEADER_ROW}:Q{COLUMN_HEADER_ROW}",
             "values": [COLUMN_HEADER]},
        ]

        # Clear old data + formulas in all columns A:Q
        clear_to = DATA_START_ROW + len(daily_rows) + 100
        svc.spreadsheets().values().batchClear(
            spreadsheetId=spreadsheet_id,
            body={"ranges": [
                f"{summary_tab}!A{DATA_START_ROW}:Q{clear_to}",
            ]},
        ).execute()

        # Build data updates (4 ranges per row, skipping formula columns E/I/M/Q)
        data_updates = []
        for i, (dt, gw_group, account_groups) in enumerate(daily_rows):
            rnum = DATA_START_ROW + i
            # A:D = date + GW (qty, price put, price call)
            data_updates.append({
                "range": f"{summary_tab}!A{rnum}:D{rnum}",
                "values": [[dt] + gw_group],
            })
            # F:H = Schwab (qty, price put, price call)
            data_updates.append({
                "range": f"{summary_tab}!F{rnum}:H{rnum}",
                "values": [account_groups[0]],
            })
            # J:L = TT IRA
            data_updates.append({
                "range": f"{summary_tab}!J{rnum}:L{rnum}",
                "values": [account_groups[1]],
            })
            # N:P = TT IND
            data_updates.append({
                "range": f"{summary_tab}!N{rnum}:P{rnum}",
                "values": [account_groups[2]],
            })

        # Write headers + data in one batch (RAW so dates stay as strings)
        all_updates = header_updates + data_updates
        svc.spreadsheets().values().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"valueInputOption": "RAW", "data": all_updates},
        ).execute()

        # Write "Total with comm" formulas in E/I/M/Q (USER_ENTERED for formulas)
        formula_updates = []
        for i in range(len(daily_rows)):
            r = DATA_START_ROW + i
            formula_updates.append({
                "range": f"{summary_tab}!E{r}",
                "values": [[f"=B{r}*(C{r}+D{r})"]],
            })
            formula_updates.append({
                "range": f"{summary_tab}!I{r}",
                "values": [[f"=(F{r}*(G{r}+H{r}))+0.97*F{r}*4"]],
            })
            formula_updates.append({
                "range": f"{summary_tab}!M{r}",
                "values": [[f"=(J{r}*(K{r}+L{r}))+1.72*J{r}*4"]],
            })
            formula_updates.append({
                "range": f"{summary_tab}!Q{r}",
                "values": [[f"=(N{r}*(O{r}+P{r}))+1.72*N{r}*4"]],
            })
        svc.spreadsheets().values().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"valueInputOption": "USER_ENTERED", "data": formula_updates},
        ).execute()

        log(f"wrote {len(daily_rows)} daily rows to {summary_tab}")
        return 0

    except Exception as e:
        msg = f"Summary failed: {type(e).__name__}: {e}"
        return fail(msg, 2) if strict else skip(msg)


if __name__ == "__main__":
    sys.exit(main())
