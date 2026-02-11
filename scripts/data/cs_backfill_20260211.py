#!/usr/bin/env python3
"""
One-time backfill: fix TT-IRA tracking row for 2026-02-11.

The parse_order_id bug caused the CSV to log qty_filled=0 for both sides,
but TastyTrade actually filled 6 put + 6 call contracts. This script
patches the CS_Tracking sheet row with the real execution data.

Runs as a no-op if the row already shows correct data (idempotent).
Remove from post_steps after confirmed run.

Env: same as cs_tracking_to_gsheet.py (GSHEET_ID, GOOGLE_SERVICE_ACCOUNT_JSON, CS_TRACKING_TAB)
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

# Corrected data from TastyTrade transaction history CSV
# TT-IRA 2026-02-11, expiry 2026-02-12
BACKFILL = {
    "key": {"date": "2026-02-11", "expiry": "2026-02-12", "account": "tt-ira"},
    "patches": {
        "put_filled": "6",
        "put_fill_price": "0.97",
        "put_status": "OK",
        "call_filled": "6",
        "call_fill_price": "0.97",
        "call_status": "OK",
        "cost_per_contract": "1.72",
        "put_cost": "20.64",
        "call_cost": "20.64",
        "total_cost": "41.28",
    },
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
        key_cols = ["date", "expiry", "account"]
        key_indices = {k: header.index(k) for k in key_cols if k in header}

        target_row = None
        for rnum, vals in enumerate(all_rows[1:], start=2):
            match = all(
                (vals[key_indices[k]] if key_indices[k] < len(vals) else "") == BACKFILL["key"][k]
                for k in key_cols if k in key_indices
            )
            if match:
                target_row = rnum
                break

        if target_row is None:
            print(f"{TAG}: SKIP — no matching row for {BACKFILL['key']}")
            return 0

        # Check if already correct
        row_vals = all_rows[target_row - 1]
        row_dict = {header[i]: (row_vals[i] if i < len(row_vals) else "") for i in range(len(header))}
        if row_dict.get("put_filled") == BACKFILL["patches"]["put_filled"]:
            print(f"{TAG}: SKIP — row already correct (put_filled={row_dict.get('put_filled')})")
            return 0

        # Apply patches
        new_vals = list(row_vals) + [""] * max(0, len(header) - len(row_vals))
        for col_name, value in BACKFILL["patches"].items():
            if col_name in header:
                idx = header.index(col_name)
                new_vals[idx] = value

        last_col = chr(64 + len(header)) if len(header) <= 26 else "ZZ"
        rng = f"{tab}!A{target_row}:{last_col}{target_row}"
        svc.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=rng,
            valueInputOption="RAW",
            body={"values": [new_vals[:len(header)]]},
        ).execute()

        print(f"{TAG}: PATCHED row {target_row} for {BACKFILL['key']}")
        return 0

    except Exception as e:
        print(f"{TAG}: WARN — {type(e).__name__}: {e}")
        return 0


if __name__ == "__main__":
    sys.exit(main())
