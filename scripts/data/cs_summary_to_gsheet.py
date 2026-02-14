#!/usr/bin/env python3
"""
Write daily trading summary to CS_Summary tab.

Reads all rows from CS_Tracking, builds a daily pivot with GW signal prices +
per-account fills side by side, computes P&L from settlement data, and writes
to CS_Summary.

Layout:
  Rows 1-8:   User-managed summary formulas (Trade Total, Edge, etc.) — never touched
  Row 9:      Section headers (GW, schwab, TT IRA, TT IND)
  Row 10:     Column headers (Date, QTY, price put, price call, P&L, ...)
  Row 11+:    Daily data (one row per date, newest first)

P&L columns (E, I, M, Q):
  - After settlement available: actual P&L in SPX points × qty
  - Before settlement: cost-adjusted net credit (SPX points × qty)

NON-BLOCKING BY DEFAULT (same pattern as other gsheet scripts).

Env:
  GSHEET_ID                    - spreadsheet ID
  GOOGLE_SERVICE_ACCOUNT_JSON  - full JSON string for service account
  CS_TRACKING_TAB              - source tab (default "CS_Tracking")
  CS_GW_SIGNAL_TAB             - GW signal tab (default "GW_Signal")
  CS_SUMMARY_TAB               - target tab (default "CS_Summary")
  CS_TT_CLOSE_TAB              - TT close status tab (default "CS_TT_Close")
  CS_GSHEET_STRICT             - "1" to fail hard on errors
"""

import os
import sys
from collections import defaultdict
from datetime import date

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
    from lib.sheets import sheets_client, ensure_sheet_tab, get_values
    from lib.parsing import safe_float
except Exception as e:
    sheets_client = None
    _IMPORT_ERR = e

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
    "QTY", "price put", "price call", "P&L",
    "QTY", "price put", "price call", "P&L",
    "QTY", "price put", "price call", "P&L",
    "QTY", "price put", "price call", "P&L",
]

ACCOUNT_ORDER = ["schwab", "tt-ira", "tt-individual"]

# Cost per 1 iron condor in SPX points: cost_per_contract × 4_legs / 100
COST_POINTS = {
    "gw": 0.0,
    "schwab": 0.0388,        # $0.97 × 4 / 100
    "tt-ira": 0.0688,        # $1.72 × 4 / 100
    "tt-individual": 0.0688,
}


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


# --- Account group colors (RGB 0-1 scale) ---
COLOR_GW   = {"red": 0.851, "green": 0.918, "blue": 0.827}  # light green
COLOR_SCHW = {"red": 0.788, "green": 0.855, "blue": 0.973}  # light blue
COLOR_IRA  = {"red": 0.851, "green": 0.918, "blue": 0.827}  # light green
COLOR_IND  = {"red": 1.0,   "green": 0.949, "blue": 0.800}  # light yellow


def apply_formatting(svc, spreadsheet_id: str, sheet_id: int, num_data_rows: int):
    """Apply background colors and currency format to the summary tab."""
    last_row = DATA_START_ROW + num_data_rows  # exclusive (0-indexed end)
    top = SECTION_HEADER_ROW - 1               # 0-indexed start for colors

    requests = []

    # Background colors for each account group (section header through data)
    for start_col, end_col, color in [
        (1, 5, COLOR_GW),    # B:E
        (5, 9, COLOR_SCHW),  # F:I
        (9, 13, COLOR_IRA),  # J:M
        (13, 17, COLOR_IND), # N:Q
    ]:
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": top,
                    "endRowIndex": last_row,
                    "startColumnIndex": start_col,
                    "endColumnIndex": end_col,
                },
                "cell": {"userEnteredFormat": {"backgroundColor": color}},
                "fields": "userEnteredFormat.backgroundColor",
            }
        })

    # Currency format ($#,##0.00) for price/P&L columns in data rows
    data_top = COLUMN_HEADER_ROW  # 0-indexed row 10 = data starts at row 11
    for start_col, end_col in [
        (2, 5),   # C:E  (GW price put, price call, P&L)
        (6, 9),   # G:I  (Schwab price put, price call, P&L)
        (10, 13), # K:M  (TT IRA price put, price call, P&L)
        (14, 17), # O:Q  (TT IND price put, price call, P&L)
    ]:
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": data_top,
                    "endRowIndex": last_row,
                    "startColumnIndex": start_col,
                    "endColumnIndex": end_col,
                },
                "cell": {"userEnteredFormat": {
                    "numberFormat": {"type": "CURRENCY", "pattern": "$#,##0.00"}
                }},
                "fields": "userEnteredFormat.numberFormat",
            }
        })

    svc.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests": requests},
    ).execute()


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
# P&L computation
# ---------------------------------------------------------------------------

def parse_strikes(strike_str: str):
    """Parse 'low/high' strike string. Returns (low, high) or (None, None)."""
    parts = (strike_str or "").split("/")
    if len(parts) == 2:
        try:
            return (float(parts[0]), float(parts[1]))
        except (ValueError, TypeError):
            pass
    return (None, None)


def compute_pnl_for_group(signed_put_f, signed_call_f, put_side, call_side,
                          put_strikes_str, call_strikes_str,
                          settlement, cost_points, qty):
    """Compute P&L for one account-day.

    All values in SPX points. Result = SPX points × qty.

    Before settlement: cost-adjusted net credit = qty × (put + call - cost)
    After settlement:  actual P&L using intrinsic at settlement.

    For CREDIT legs: P&L = fill_credit - spread_value_at_expiry
    For DEBIT legs:  P&L = -fill_debit + spread_value_at_expiry

    Returns P&L as string, or "" if no fills.
    """
    if qty == 0:
        return ""

    if signed_put_f is None and signed_call_f is None:
        return ""

    sp = signed_put_f if signed_put_f is not None else 0.0
    sc = signed_call_f if signed_call_f is not None else 0.0

    if settlement is None:
        # No settlement yet — leave P&L blank for open trades
        return ""

    # Post-settlement: actual P&L
    put_lo, put_hi = parse_strikes(put_strikes_str)
    call_lo, call_hi = parse_strikes(call_strikes_str)

    # Put spread value at settlement: max(0, min(width, high - settle))
    put_intrinsic = 0.0
    if put_lo is not None and put_hi is not None:
        width = put_hi - put_lo
        put_intrinsic = max(0.0, min(width, put_hi - settlement))

    # Call spread value at settlement: max(0, min(width, settle - low))
    call_intrinsic = 0.0
    if call_lo is not None and call_hi is not None:
        width = call_hi - call_lo
        call_intrinsic = max(0.0, min(width, settlement - call_lo))

    # Per-leg P&L depends on whether we sold (CREDIT) or bought (DEBIT)
    if (put_side or "").strip().upper() == "DEBIT":
        put_pnl = sp + put_intrinsic   # paid debit, receive intrinsic
    else:
        put_pnl = sp - put_intrinsic   # received credit, owe intrinsic

    if (call_side or "").strip().upper() == "DEBIT":
        call_pnl = sc + call_intrinsic
    else:
        call_pnl = sc - call_intrinsic

    total = (put_pnl + call_pnl - cost_points) * qty
    return str(round(total, 2))


# ---------------------------------------------------------------------------
# Data readers
# ---------------------------------------------------------------------------

def derive_settlements(gw_signal: dict) -> dict:
    """Derive settlement prices from GW_Signal SPX data.

    Settlement for expiry E = SPX price from GW_Signal where date == E.
    Includes today (by the time reporting runs, the market has closed and
    GW_Signal has the SPX close for today's expiry).  Excludes future dates.
    """
    today = date.today().isoformat()
    result = {}
    for dt, data in gw_signal.items():
        if dt > today:
            continue
        spx = safe_float(data.get("spx"))
        if spx is not None and spx > 0:
            result[dt] = spx
    if result:
        log(f"derived {len(result)} settlement prices from GW_Signal")
    return result


def read_gw_signal(svc, spreadsheet_id: str, tab: str) -> dict:
    """Read GW_Signal tab and return {date: {put_price, call_price, spx}}."""
    try:
        all_rows = get_values(svc, spreadsheet_id, f"{tab}!A1:ZZ")
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
                    "spx": (d.get("spx") or "").strip(),
                }
        log(f"read {len(result)} GW signal rows")
        return result
    except Exception as e:
        log(f"WARN — could not read GW_Signal: {e}")
        return {}


def read_tt_close_status(svc, spreadsheet_id: str, tab: str) -> dict:
    """Read CS_TT_Close tab. Returns {expiry: close_net} for filled close orders."""
    try:
        all_rows = get_values(svc, spreadsheet_id, f"{tab}!A1:C")
        if len(all_rows) < 2:
            return {}
        result = {}
        for row in all_rows[1:]:
            if len(row) >= 3:
                expiry = (row[0] or "").strip()
                status = (row[1] or "").strip()
                close_net = safe_float(row[2])
                if expiry and status == "closed" and close_net is not None:
                    result[expiry] = close_net
        if result:
            log(f"read {len(result)} TT close entries")
        return result
    except Exception as e:
        log(f"WARN — could not read {tab}: {e}")
        return {}


# ---------------------------------------------------------------------------
# Daily detail computation
# ---------------------------------------------------------------------------

def compute_daily_detail(rows: list[dict], gw_signal: dict,
                         settlements: dict, tt_close: dict = None) -> list[tuple]:
    """Build daily pivot: one row per date with GW + 3 accounts side by side.

    Returns list of (date, gw_group, [schwab_group, ira_group, ind_group])
    where each group is [qty_str, price_put, price_call, pnl].

    tt_close: {expiry: close_net} from CS_TT_Close tab for TT Individual
              early-close P&L. When an expiry is present, uses close-based P&L
              instead of settlement-based P&L.
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
        expiry = (any_row.get("expiry") or "").strip()

        # Look up settlement by expiry date
        settlement = settlements.get(expiry)

        # GW prices from GW_Signal tab (authoritative source)
        gw = gw_signal.get(dt, {})
        gw_put = signed_price(gw.get("put_price", ""), put_side)
        gw_call = signed_price(gw.get("call_price", ""), call_side)

        # GW P&L: use strikes from any available account (all share the same signal)
        gw_put_strikes = (any_row.get("put_strikes") or "").strip()
        gw_call_strikes = (any_row.get("call_strikes") or "").strip()
        gw_pnl = compute_pnl_for_group(
            safe_float(gw_put), safe_float(gw_call),
            put_side, call_side,
            gw_put_strikes, gw_call_strikes,
            settlement, COST_POINTS["gw"], 1,
        )
        gw_group = ["1", gw_put, gw_call, gw_pnl]

        account_groups = []
        for acct_name in ACCOUNT_ORDER:
            r = accts.get(acct_name)
            if r:
                put_filled_f = safe_float(r.get("put_filled"), 0)
                call_filled_f = safe_float(r.get("call_filled"), 0)
                qty = int(put_filled_f) if put_filled_f else 0

                # QTY mismatch detection
                qty_str = str(qty)
                call_qty = int(call_filled_f) if call_filled_f else 0
                if qty > 0 and call_qty > 0 and qty != call_qty:
                    qty_str += "*"

                p_side = (r.get("put_side") or "").strip()
                c_side = (r.get("call_side") or "").strip()
                price_put = signed_price(r.get("put_fill_price", ""), p_side)
                price_call = signed_price(r.get("call_fill_price", ""), c_side)

                cost = COST_POINTS.get(acct_name, 0.0)

                # TT Individual: use close-based P&L if order filled early
                if (acct_name == "tt-individual" and tt_close
                        and expiry in tt_close and qty > 0):
                    close_net = tt_close[expiry]
                    sp = safe_float(price_put) or 0.0
                    sc = safe_float(price_call) or 0.0
                    # 2× cost: commissions on both open + close trades
                    pnl = str(round((sp + sc + close_net - 2 * cost) * qty, 2))
                else:
                    pnl = compute_pnl_for_group(
                        safe_float(price_put), safe_float(price_call),
                        p_side, c_side,
                        (r.get("put_strikes") or "").strip(),
                        (r.get("call_strikes") or "").strip(),
                        settlement, cost, qty,
                    )
            else:
                qty_str = "0"
                price_put = ""
                price_call = ""
                pnl = ""
            account_groups.append([qty_str, price_put, price_call, pnl])

        result.append((dt, gw_group, account_groups))

    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    strict = strict_enabled()

    if sheets_client is None:
        msg = f"google sheets libs not installed ({_IMPORT_ERR})"
        return fail(msg, 2) if strict else skip(msg)

    if not (os.environ.get("GSHEET_ID") or "").strip():
        return fail("GSHEET_ID missing", 2) if strict else skip("GSHEET_ID missing")

    if not (os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON") or "").strip():
        return fail("SA creds missing", 2) if strict else skip("SA creds missing")

    tracking_tab = (os.environ.get("CS_TRACKING_TAB") or "CS_Tracking").strip()
    gw_signal_tab = (os.environ.get("CS_GW_SIGNAL_TAB") or "GW_Signal").strip()
    summary_tab = (os.environ.get("CS_SUMMARY_TAB") or "CS_Summary").strip()
    tt_close_tab = (os.environ.get("CS_TT_CLOSE_TAB") or "CS_TT_Close").strip()

    try:
        svc, spreadsheet_id = sheets_client()

        # Read tracking data
        all_rows = get_values(svc, spreadsheet_id, f"{tracking_tab}!A1:ZZ")
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

        # Derive settlement prices from GW_Signal SPX column
        settlements = derive_settlements(gw_signal)

        # Read TT Individual close status (for early-close P&L)
        tt_close = read_tt_close_status(svc, spreadsheet_id, tt_close_tab)

        # Compute daily detail with P&L
        daily_rows = compute_daily_detail(rows, gw_signal, settlements, tt_close)
        if not daily_rows:
            return skip("no daily data")

        # Write to summary tab
        sheet_id = ensure_sheet_tab(svc, spreadsheet_id, summary_tab)

        # Write section + column headers (rows 9-10)
        header_updates = [
            {"range": f"{summary_tab}!A{SECTION_HEADER_ROW}:Q{SECTION_HEADER_ROW}",
             "values": [SECTION_HEADER]},
            {"range": f"{summary_tab}!A{COLUMN_HEADER_ROW}:Q{COLUMN_HEADER_ROW}",
             "values": [COLUMN_HEADER]},
        ]

        # Clear old data (generous buffer to remove stale rows)
        clear_to = max(DATA_START_ROW + len(daily_rows) + 100, 5000)
        svc.spreadsheets().values().batchClear(
            spreadsheetId=spreadsheet_id,
            body={"ranges": [
                f"{summary_tab}!A{DATA_START_ROW}:Q{clear_to}",
            ]},
        ).execute()

        # Build data updates — each group now includes P&L (no separate formula pass)
        data_updates = []
        for i, (dt, gw_group, account_groups) in enumerate(daily_rows):
            rnum = DATA_START_ROW + i
            # A:E = date + GW (qty, price put, price call, P&L)
            data_updates.append({
                "range": f"{summary_tab}!A{rnum}:E{rnum}",
                "values": [[dt] + gw_group],
            })
            # F:I = Schwab (qty, price put, price call, P&L)
            data_updates.append({
                "range": f"{summary_tab}!F{rnum}:I{rnum}",
                "values": [account_groups[0]],
            })
            # J:M = TT IRA
            data_updates.append({
                "range": f"{summary_tab}!J{rnum}:M{rnum}",
                "values": [account_groups[1]],
            })
            # N:Q = TT IND
            data_updates.append({
                "range": f"{summary_tab}!N{rnum}:Q{rnum}",
                "values": [account_groups[2]],
            })

        # Write headers + data in one batch (RAW so dates stay as strings)
        all_updates = header_updates + data_updates
        svc.spreadsheets().values().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"valueInputOption": "RAW", "data": all_updates},
        ).execute()

        # Apply formatting (colors + currency)
        apply_formatting(svc, spreadsheet_id, sheet_id, len(daily_rows))

        # Count dates with settlement-based P&L
        date_to_expiry = {}
        for r in rows:
            dt2 = (r.get("date") or "").strip()
            exp = (r.get("expiry") or "").strip()
            if dt2 and exp:
                date_to_expiry[dt2] = exp
        settled = sum(1 for dt, _, _ in daily_rows
                      if settlements.get(date_to_expiry.get(dt, "")))
        log(f"wrote {len(daily_rows)} daily rows to {summary_tab} "
            f"({settled} with settlement P&L)")
        return 0

    except Exception as e:
        msg = f"Summary failed: {type(e).__name__}: {e}"
        return fail(msg, 2) if strict else skip(msg)


if __name__ == "__main__":
    sys.exit(main())
