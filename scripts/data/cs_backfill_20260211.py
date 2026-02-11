#!/usr/bin/env python3
"""
One-time backfill for CS_Tracking sheet:

1. TT-IRA 2026-02-11: fix qty_filled (parse_order_id bug logged 0, real was 6+6)
2. Schwab rows: fix cost_per_contract 0.65 → 0.97 and recalculate costs
3. All rows: add gw_put_price/gw_call_price from GW historical data

Idempotent — skips rows already correct.
Remove from post_steps after confirmed run.

Env: GSHEET_ID, GOOGLE_SERVICE_ACCOUNT_JSON, CS_TRACKING_TAB
"""

import os
import sys
import json

_IMPORT_ERR = None
try:
    from googleapiclient.discovery import build
    from google.oauth2 import service_account
except Exception as e:
    build = None
    service_account = None
    _IMPORT_ERR = e

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
TAG = "CS_BACKFILL"

# --- GW historical prices (from GetUltraPureConstantStable Predictions) ---
# date → (put_price, call_price)
GW_PRICES = {
    "2026-02-11": ("0.95", "0.95"),
    "2026-02-10": ("1.0", "1.0"),
    "2026-02-09": ("1.0", "1.12"),
    "2026-02-06": ("1.0", "0.95"),
    "2026-02-05": ("1.0", "0.95"),
    "2026-02-04": ("0.95", "1.05"),
    "2026-02-03": ("0.9", "1.0"),
    "2026-02-02": ("1.05", "0.93"),
    "2026-01-30": ("1.0", "1.05"),
    "2026-01-29": ("1.0", "0.95"),
}


def main() -> int:
    if build is None:
        print(f"{TAG}: SKIP — google libs missing ({_IMPORT_ERR})")
        return 0

    spreadsheet_id = (os.environ.get("GSHEET_ID") or "").strip()
    if not spreadsheet_id:
        print(f"{TAG}: SKIP — GSHEET_ID missing")
        return 0

    raw_sa = (os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON") or "").strip()
    if not raw_sa:
        print(f"{TAG}: SKIP — SA creds missing")
        return 0

    tab = (os.environ.get("CS_TRACKING_TAB") or "CS_Tracking").strip()

    try:
        info = json.loads(raw_sa)
        creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
        svc = build("sheets", "v4", credentials=creds, cache_discovery=False)

        resp = svc.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id, range=f"{tab}!A1:ZZ"
        ).execute()
        all_rows = resp.get("values") or []
        if len(all_rows) < 2:
            print(f"{TAG}: SKIP — no tracking data")
            return 0

        header = all_rows[0]

        def col_idx(name):
            return header.index(name) if name in header else -1

        def get_cell(vals, col_name):
            idx = col_idx(col_name)
            if idx < 0 or idx >= len(vals):
                return ""
            return vals[idx]

        def set_cell(vals, col_name, value):
            idx = col_idx(col_name)
            if idx < 0:
                return
            while len(vals) < len(header):
                vals.append("")
            vals[idx] = value

        updates = []  # list of (range_str, [row_values])

        for rnum, vals in enumerate(all_rows[1:], start=2):
            row_vals = list(vals) + [""] * max(0, len(header) - len(vals))
            changed = False
            acct = get_cell(vals, "account")
            dt = get_cell(vals, "date")

            # --- Fix 1: TT-IRA 2026-02-11 fills ---
            if (acct == "tt-ira" and dt == "2026-02-11"
                    and get_cell(vals, "expiry") == "2026-02-12"
                    and get_cell(vals, "put_filled") != "6"):
                for col, val in [
                    ("put_filled", "6"), ("put_fill_price", "0.97"), ("put_status", "OK"),
                    ("call_filled", "6"), ("call_fill_price", "0.97"), ("call_status", "OK"),
                    ("cost_per_contract", "1.72"),
                    ("put_cost", "20.64"), ("call_cost", "20.64"), ("total_cost", "41.28"),
                ]:
                    set_cell(row_vals, col, val)
                changed = True
                print(f"{TAG}: FIX tt-ira fills row {rnum}")

            # --- Fix 2: Schwab cost 0.65 → 0.97 ---
            if acct == "schwab" and get_cell(vals, "cost_per_contract") == "0.65":
                set_cell(row_vals, "cost_per_contract", "0.97")
                # Recalculate costs: cost_per_contract * filled * 2 legs
                for side, filled_col, cost_col in [
                    ("put", "put_filled", "put_cost"),
                    ("call", "call_filled", "call_cost"),
                ]:
                    try:
                        filled = int(get_cell(row_vals, filled_col) or "0")
                        new_cost = 0.97 * filled * 2
                        set_cell(row_vals, cost_col, f"{new_cost:.2f}")
                    except (ValueError, TypeError):
                        pass
                # Total cost
                try:
                    pc = float(get_cell(row_vals, "put_cost") or "0")
                    cc = float(get_cell(row_vals, "call_cost") or "0")
                    set_cell(row_vals, "total_cost", f"{pc + cc:.2f}")
                except (ValueError, TypeError):
                    pass
                changed = True
                print(f"{TAG}: FIX schwab cost row {rnum} (date={dt})")

            # --- Fix 3: Add GW prices ---
            if dt in GW_PRICES and col_idx("gw_put_price") >= 0:
                gw_put, gw_call = GW_PRICES[dt]
                if get_cell(row_vals, "gw_put_price") != gw_put:
                    set_cell(row_vals, "gw_put_price", gw_put)
                    set_cell(row_vals, "gw_call_price", gw_call)
                    changed = True
                    print(f"{TAG}: ADD gw_prices row {rnum} (date={dt}, acct={acct})")

            if changed:
                # Column letter helper
                n = len(header)
                if n <= 26:
                    last_col = chr(64 + n)
                else:
                    last_col = chr(64 + (n - 1) // 26) + chr(65 + (n - 1) % 26)
                rng = f"{tab}!A{rnum}:{last_col}{rnum}"
                updates.append({"range": rng, "values": [row_vals[:len(header)]]})

        if not updates:
            print(f"{TAG}: SKIP — all rows already correct")
            return 0

        svc.spreadsheets().values().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"valueInputOption": "RAW", "data": updates},
        ).execute()

        print(f"{TAG}: PATCHED {len(updates)} row(s)")
        return 0

    except Exception as e:
        print(f"{TAG}: WARN — {type(e).__name__}: {e}")
        return 0


if __name__ == "__main__":
    sys.exit(main())
