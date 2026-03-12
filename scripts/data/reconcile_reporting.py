#!/usr/bin/env python3
"""Reconcile reporting — confirm today's expected rows exist in Google Sheets.

Runs as the LAST post-step for each strategy. Checks that the expected tab
has a row with today's date and that required columns are non-empty.

Emits RECONCILE_OK or RECONCILE_MISS log lines for CloudWatch alerting.

Exit codes: 0 = all checks pass, 1 = config/infra failure, 2 = data miss

Env:
  RECONCILE_CHECKS              - comma-separated check specs:
                                  "tab:date_header:required_header1:required_header2"
                                  e.g. "BF_Trades:trade_date:status:signal"
                                  or   "DS_Tracking:trade_date:put_structure"
  GSHEET_ID                     - spreadsheet ID
  GOOGLE_SERVICE_ACCOUNT_JSON   - service account JSON
"""

import os
import sys
from datetime import datetime
from zoneinfo import ZoneInfo


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
    from lib.sheets import sheets_client, col_letter
except Exception as e:
    sheets_client = None
    _IMPORT_ERR = e

ET = ZoneInfo("America/New_York")


def main():
    if _IMPORT_ERR:
        print(f"RECONCILE FAIL: import error: {_IMPORT_ERR}")
        return 1

    checks_raw = os.environ.get("RECONCILE_CHECKS", "").strip()
    if not checks_raw:
        print("RECONCILE FAIL: RECONCILE_CHECKS not configured")
        return 1

    today = datetime.now(ET).date()
    if today.weekday() >= 5:
        print(f"RECONCILE SKIP: weekend ({today})")
        return 2

    today_str = today.isoformat()

    try:
        svc, sid = sheets_client()
    except Exception as e:
        print(f"RECONCILE FAIL: sheets_client failed: {e}")
        return 1

    any_miss = False
    for check in checks_raw.split(","):
        check = check.strip()
        if not check:
            continue
        parts = check.split(":")
        if len(parts) < 2:
            print(f"RECONCILE FAIL: bad check format (need tab:date_header[:required...]): {check}")
            any_miss = True
            continue

        tab = parts[0]
        date_header = parts[1]
        required_headers = parts[2:] if len(parts) > 2 else []

        try:
            # Read header row + all data
            last_col = col_letter(50)  # wide enough for any tab
            all_vals = (
                svc.spreadsheets()
                .values()
                .get(spreadsheetId=sid, range=f"{tab}!A1:{last_col}")
                .execute()
                .get("values", [])
            )
            if not all_vals:
                print(f"RECONCILE_MISS {tab} (sheet is empty)")
                any_miss = True
                continue

            headers = all_vals[0]
            data_rows = all_vals[1:]

            # Find date column index by header name
            if date_header not in headers:
                print(f"RECONCILE FAIL: {tab} has no column '{date_header}' (headers: {headers[:5]}...)")
                any_miss = True
                continue
            date_col = headers.index(date_header)

            # Find required column indices
            req_cols = {}
            for rh in required_headers:
                if rh not in headers:
                    print(f"RECONCILE FAIL: {tab} has no column '{rh}'")
                    any_miss = True
                    continue
                req_cols[rh] = headers.index(rh)

            # Find today's row
            matched_row = None
            for row in data_rows:
                padded = row + [""] * (len(headers) - len(row))
                if padded[date_col] == today_str:
                    matched_row = padded
                    break

            if matched_row is None:
                print(f"RECONCILE_MISS {tab} (no row with {date_header}={today_str})")
                any_miss = True
                continue

            # Check required columns are non-empty
            blanks = [rh for rh, ci in req_cols.items() if not matched_row[ci].strip()]
            if blanks:
                print(f"RECONCILE_MISS {tab} (row found but blank columns: {', '.join(blanks)})")
                any_miss = True
                continue

            print(f"RECONCILE_OK {tab}")

        except Exception as e:
            print(f"RECONCILE FAIL: {tab} read error: {e}")
            any_miss = True
            continue

    return 2 if any_miss else 0


if __name__ == "__main__":
    sys.exit(main())
