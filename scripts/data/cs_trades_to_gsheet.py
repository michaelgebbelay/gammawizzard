#!/usr/bin/env python3
# Push ConstantStable vertical trade log (CSV) to Google Sheets.
#
# Env:
#   GSHEET_ID (required)                  — target spreadsheet ID
#   GOOGLE_SERVICE_ACCOUNT_JSON (req'd)   — service account JSON (entire JSON string)
#   CS_LOG_PATH (opt)                     — log CSV path; default logs/constantstable_vertical_trades.csv
#   CS_GSHEET_TAB (opt)                  — tab name; default "ConstantStableTrades"
#
# Behavior:
#   - Reads the CSV log written by ConstantStable/place.py
#   - Ensures a tab exists
#   - Writes header if missing
#   - UPSERTS rows keyed by (trade_date, tdate, name, side, short_osi, long_osi)

import os
import sys
import csv
import json
from typing import List, Dict, Tuple

from googleapiclient.discovery import build
from google.oauth2 import service_account

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


# ---------- Google Sheets helpers ----------

def creds_from_env():
    raw = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if not raw:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON env is required")
    try:
        info = json.loads(raw)
    except Exception as e:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON must be a full JSON string") from e
    return service_account.Credentials.from_service_account_info(info, scopes=SCOPES)


def ensure_sheet_tab(svc, spreadsheet_id: str, title: str) -> int:
    meta = svc.spreadsheets().get(
        spreadsheetId=spreadsheet_id,
        fields="sheets.properties"
    ).execute()
    for s in (meta.get("sheets") or []):
        p = s.get("properties") or {}
        if (p.get("title") or "") == title:
            return int(p.get("sheetId"))
    # create
    req = {"requests": [{"addSheet": {"properties": {"title": title}}}]}
    r = svc.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body=req
    ).execute()
    sid = r["replies"][0]["addSheet"]["properties"]["sheetId"]
    return int(sid)


def col_letter(idx_zero_based: int) -> str:
    n = idx_zero_based + 1
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def read_sheet_all(svc, spreadsheet_id: str, title: str):
    r = svc.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=f"{title}!A1:ZZ"
    ).execute()
    return r.get("values") or []


def upsert_log_rows(
    svc,
    spreadsheet_id: str,
    title: str,
    rows: List[Dict[str, str]],
    header: List[str],
):
    existing = read_sheet_all(svc, spreadsheet_id, title)

    # Ensure header in sheet
    if not existing:
        existing = [header]
        svc.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{title}!A1:{col_letter(len(header) - 1)}1",
            valueInputOption="RAW",
            body={"values": [header]},
        ).execute()
    else:
        cur_header = existing[0]
        if cur_header != header:
            # Rewrite header to match CSV header order
            svc.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"{title}!A1:{col_letter(len(header) - 1)}1",
                valueInputOption="RAW",
                body={"values": [header]},
            ).execute()
            existing = [header] + existing[1:]

    # Build index: (trade_date, tdate, name, side, short_osi, long_osi) → row_number
    hdr_index = {h: i for i, h in enumerate(header)}

    key_fields = [
        "trade_date",
        "tdate",
        "name",
        "side",
        "short_osi",
        "long_osi",
    ]
    # Filter key_fields to only those present in the header
    key_fields = [k for k in key_fields if k in hdr_index]

    def make_key(d: Dict[str, str]) -> Tuple[str, ...]:
        if key_fields:
            return tuple(str(d.get(k, "")) for k in key_fields)
        # fallback: full row in header order
        return tuple(str(d.get(h, "")) for h in header)

    existing_map = {}
    for rnum, row in enumerate(existing[1:], start=2):
        d = {header[i]: (row[i] if i < len(row) else "") for i in range(len(header))}
        existing_map[make_key(d)] = rnum

    updates = []
    appends = []

    for d in rows:
        key = make_key(d)
        values = [d.get(h, "") for h in header]
        if key in existing_map:
            rnum = existing_map[key]
            rng = f"{title}!A{rnum}:{col_letter(len(header) - 1)}{rnum}"
            updates.append((rng, values))
        else:
            appends.append(values)

    # Batch updates
    if updates:
        data = [{"range": rng, "values": [vals]} for (rng, vals) in updates]
        svc.spreadsheets().values().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"valueInputOption": "RAW", "data": data},
        ).execute()

    # Appends
    if appends:
        svc.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=f"{title}!A1",
            includeValuesInResponse=False,
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": appends},
        ).execute()

    return {"updated": len(updates), "appended": len(appends)}


# ---------- main ----------

def main():
    spreadsheet_id = os.environ.get("GSHEET_ID", "").strip()
    if not spreadsheet_id:
        print("ERROR: GSHEET_ID env is required", file=sys.stderr)
        return 2

    log_path = os.environ.get(
        "CS_LOG_PATH", "logs/constantstable_vertical_trades.csv"
    ).strip()
    if not os.path.exists(log_path):
        print(f"CS_TRADES_TO_GSHEET: log file not found: {log_path}")
        return 0

    # Read CSV log
    with open(log_path, newline="") as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames or []
        rows = [row for row in reader]

    if not header or not rows:
        print(f"CS_TRADES_TO_GSHEET: nothing to write from {log_path}")
        return 0

    tab_title = (
        os.environ.get("CS_GSHEET_TAB", "").strip() or "ConstantStableTrades"
    )

    creds = creds_from_env()
    svc = build("sheets", "v4", credentials=creds, cache_discovery=False)

    ensure_sheet_tab(svc, spreadsheet_id, tab_title)
    res = upsert_log_rows(svc, spreadsheet_id, tab_title, rows, header)

    print(
        f"CS_TRADES_TO_GSHEET: {log_path} → {tab_title} "
        f"appended={res['appended']} updated={res['updated']}"
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())
