#!/usr/bin/env python3
"""Log butterfly evaluations (SKIP, ERROR, OK/DRY_RUN, OK/FILLED) to Google Sheets.

Reads /tmp/bf_plan.json written by the orchestrator and appends one row per day.
Designed as a post-step in the Lambda handler.

Env:
  GSHEET_ID                    - spreadsheet ID
  GOOGLE_SERVICE_ACCOUNT_JSON  - service account JSON
  BF_GSHEET_TAB                - tab name (default "BF_Trades")
  BF_PLAN_PATH                 - path to plan JSON (default /tmp/bf_plan.json)
"""

import json
import os
import sys
from datetime import datetime
from pathlib import Path
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
    from lib.sheets import sheets_client, ensure_tab
except Exception as e:
    sheets_client = None
    _IMPORT_ERR = e


ET = ZoneInfo("America/New_York")

HEADERS = [
    "ts_et",
    "trade_date",
    "status",
    "reason",
    "signal",
    "config",
    "target_dte",
    "order_side",
    "expiry_date",
    "spot",
    "vix",
    "vix1d",
    "lower_strike",
    "center_strike",
    "upper_strike",
    "width",
    "package_bid",
    "package_ask",
    "package_mid",
    "se_today",
    "se_5d_avg",
    "dry_run",
    "filled_qty",
    "fill_price",
    "order_ids",
]


def _val(plan, key, default=""):
    v = plan.get(key, default)
    if v is None:
        return ""
    return str(v)


def main():
    if _IMPORT_ERR:
        print(f"BF_GSHEET SKIP: import error: {_IMPORT_ERR}")
        return 0

    plan_path = Path(os.environ.get("BF_PLAN_PATH", "/tmp/bf_plan.json"))
    if not plan_path.exists():
        print("BF_GSHEET SKIP: no plan file found")
        return 0

    try:
        plan = json.loads(plan_path.read_text())
    except Exception as e:
        print(f"BF_GSHEET SKIP: cannot read plan: {e}")
        return 0

    tab = os.environ.get("BF_GSHEET_TAB", "BF_Trades")

    try:
        svc, sid = sheets_client()
    except Exception as e:
        print(f"BF_GSHEET SKIP: sheets_client failed: {e}")
        return 0

    ensure_tab(svc, sid, tab, HEADERS)

    result = plan.get("result", {})

    row = [
        datetime.now(ET).strftime("%Y-%m-%d %H:%M:%S"),
        _val(plan, "trade_date"),
        _val(plan, "status"),
        _val(plan, "reason"),
        _val(plan, "signal"),
        _val(plan, "config"),
        _val(plan, "target_dte"),
        _val(plan, "order_side"),
        _val(plan, "expiry_date"),
        _val(plan, "spot"),
        _val(plan, "vix"),
        _val(plan, "vix1d"),
        _val(plan, "lower_strike"),
        _val(plan, "center_strike"),
        _val(plan, "upper_strike"),
        _val(plan, "width"),
        _val(plan, "package_bid"),
        _val(plan, "package_ask"),
        _val(plan, "package_mid"),
        _val(plan, "se_today"),
        _val(plan, "se_5d_avg"),
        str(result.get("dry_run", "")),
        str(result.get("filled_qty", "")),
        str(result.get("last_price", "")),
        ",".join(result.get("order_ids", [])) if isinstance(result.get("order_ids"), list) else str(result.get("order_ids", "")),
    ]

    svc.spreadsheets().values().append(
        spreadsheetId=sid,
        range=f"{tab}!A:A",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": [row]},
    ).execute()

    print(f"BF_GSHEET OK: {plan.get('status')} row appended to {tab}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
