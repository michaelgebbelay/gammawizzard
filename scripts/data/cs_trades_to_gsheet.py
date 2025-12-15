#!/usr/bin/env python3
"""
Push ConstantStable trades CSV to Google Sheets (UPSERT).

Env:
  GSHEET_ID (required)
  GOOGLE_SERVICE_ACCOUNT_JSON (required) - full JSON string
  CS_LOG_PATH (optional) default logs/constantstable_vertical_trades.csv
  CS_GSHEET_TAB (optional) default ConstantStableTrades

UPSERT KEY:
  (trade_date, tdate, name)
"""

import os, sys, json, csv
from googleapiclient.discovery import build
from google.oauth2 import service_account

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def creds_from_env():
    raw = (os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON") or "").strip()
    if not raw:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON env is required")
    info = json.loads(raw)
    return service_account.Credentials.from_service_account_info(info, scopes=SCOPES)


def col_letter(idx_zero_based: int) -> str:
    n = idx_zero_based + 1
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def ensure_sheet_tab(svc, spreadsheet_id: str, title: str) -> int:
    meta = svc.spreadsheets().get(spreadsheetId=spreadsheet_id, fields="sheets.properties").execute()
    for s in (meta.get("sheets") or []):
        p = s.get("properties") or {}
        if (p.get("title") or "") == title:
            return int(p.get("sheetId"))
    req = {"requests": [{"addSheet": {"properties": {"title": title}}}]}
    r = svc.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=req).execute()
    sid = r["replies"][0]["addSheet"]["properties"]["sheetId"]
    return int(sid)


def read_sheet_all(svc, spreadsheet_id: str, title: str):
    r = svc.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=f"{title}!A1:ZZ").execute()
    return r.get("values") or []


def upsert_rows(svc, spreadsheet_id: str, title: str, rows: list[dict], header: list[str]):
    existing = read_sheet_all(svc, spreadsheet_id, title)

    # Ensure header row
    if not existing:
        svc.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{title}!A1:{col_letter(len(header)-1)}1",
            valueInputOption="RAW",
            body={"values": [header]},
        ).execute()
        existing = [header]
    else:
        cur_header = existing[0]
        if cur_header != header:
            svc.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"{title}!A1:{col_letter(len(header)-1)}1",
                valueInputOption="RAW",
                body={"values": [header]},
            ).execute()
            existing = [header] + existing[1:]

    def key_from_dict(d):
        return (str(d.get("trade_date", "")), str(d.get("tdate", "")), str(d.get("name", "")))

    # Build existing key -> row number map
    existing_map = {}
    for rnum, row in enumerate(existing[1:], start=2):
        d = {header[i]: (row[i] if i < len(row) else "") for i in range(len(header))}
        existing_map[key_from_dict(d)] = rnum

    updates = []
    appends = []

    for d in rows:
        key = key_from_dict(d)
        values = [str(d.get(h, "")) for h in header]
        if key in existing_map:
            rnum = existing_map[key]
            rng = f"{title}!A{rnum}:{col_letter(len(header)-1)}{rnum}"
            updates.append((rng, values))
        else:
            appends.append(values)

    if updates:
        data = [{"range": rng, "values": [vals]} for (rng, vals) in updates]
        svc.spreadsheets().values().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"valueInputOption": "RAW", "data": data},
        ).execute()

    if appends:
        svc.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=f"{title}!A1",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": appends},
        ).execute()

    return {"updated": len(updates), "appended": len(appends)}


def main():
    spreadsheet_id = (os.environ.get("GSHEET_ID") or "").strip()
    if not spreadsheet_id:
        print("ERROR: GSHEET_ID env is required", file=sys.stderr)
        return 2

    tab = (os.environ.get("CS_GSHEET_TAB") or "ConstantStableTrades").strip()
    path = (os.environ.get("CS_LOG_PATH") or "logs/constantstable_vertical_trades.csv").strip()

    if not os.path.exists(path):
        print(f"CS_TRADES_TO_GSHEET: {path} missing — nothing to do")
        return 0

    with open(path, "r", newline="") as f:
        rdr = csv.DictReader(f)
        header = rdr.fieldnames or []
        rows = list(rdr)

    if not header:
        print(f"CS_TRADES_TO_GSHEET: {path} has no header — nothing to do")
        return 0

    creds = creds_from_env()
    svc = build("sheets", "v4", credentials=creds, cache_discovery=False)

    ensure_sheet_tab(svc, spreadsheet_id, tab)
    res = upsert_rows(svc, spreadsheet_id, tab, rows, header)

    print(f"CS_TRADES_TO_GSHEET: {path} → {tab} appended={res['appended']} updated={res['updated']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
