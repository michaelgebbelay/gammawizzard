#!/usr/bin/env python3
"""Reconcile reporting — confirm today's expected rows exist in Google Sheets.

Runs as the LAST post-step for each strategy. Checks that the expected tab
has a row with today's date in the specified column.

Emits RECONCILE_OK or RECONCILE_MISS log lines for CloudWatch alerting.

Exit codes: 0 = all checks pass, 2 = at least one miss (soft-fail)

Env:
  RECONCILE_CHECKS              - comma-separated "tab:col_index" pairs
                                  e.g. "BF_Trades:1,DS_Tracking:1"
  GSHEET_ID                     - spreadsheet ID
  GOOGLE_SERVICE_ACCOUNT_JSON   - service account JSON
"""

import os
import sys
from datetime import date, datetime
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
        print("RECONCILE SKIP: no checks configured")
        return 2

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
        if len(parts) != 2:
            print(f"RECONCILE WARN: bad check format: {check}")
            continue
        tab, col_idx_str = parts
        try:
            col_idx = int(col_idx_str)
        except ValueError:
            print(f"RECONCILE WARN: bad col index: {col_idx_str}")
            continue

        try:
            col = col_letter(col_idx)
            vals = (
                svc.spreadsheets()
                .values()
                .get(spreadsheetId=sid, range=f"{tab}!{col}2:{col}")
                .execute()
                .get("values", [])
            )
            found = any(row and row[0] == today_str for row in vals)
        except Exception as e:
            print(f"RECONCILE FAIL: {tab} read error: {e}")
            any_miss = True
            continue

        if found:
            print(f"RECONCILE_OK {tab}")
        else:
            print(f"RECONCILE_MISS {tab} (no row with {today_str} in col {col})")
            any_miss = True

    return 2 if any_miss else 0


if __name__ == "__main__":
    sys.exit(main())
