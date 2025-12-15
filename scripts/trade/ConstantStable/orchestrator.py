#!/usr/bin/env python3
"""
Push ConstantStable trades CSV to Google Sheets (UPSERT or "append-ish").

Env:
  GSHEET_ID (required)
  GOOGLE_SERVICE_ACCOUNT_JSON (required) - full JSON string
  CS_LOG_PATH (optional) default logs/constantstable_vertical_trades.csv
  CS_GSHEET_TAB (optional) default ConstantStableTrades

UPSERT KEYS:
  By default this preserves one row per (trade_date, tdate, name).
  If you want every execution attempt to show up as a new row, set:
      CS_UPSERT_KEYS=ts_utc,name
  (ts_utc is already in the CSV, so each run becomes unique.)

  Default:
      CS_UPSERT_KEYS=trade_date,tdate,name
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


def parse_upsert_keys(header: list[str]) -> list[str]:
    raw = (os.environ.get("CS_UPSERT_KEYS") or "trade_date,tdate,name").strip()
    keys = [k.strip() for k in raw.split(",") if k.strip()]
    missing = [k for k in keys if k not in header]
    if missing:
        raise RuntimeError(
            f"CS_UPSERT_KEYS refers to missing columns: {missing}. "
            f"Available columns include: {header[:12]}..."
        )
    return keys


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

    upsert_keys = parse_upsert_keys(header)

    def key_from_dict(d):
        return tuple(str(d.get(k, "")) for k in upsert_keys)

    # Build existing key -> row number map
    existing_map = {}
    for rnum, row in enumerate(existing[1:], start=2):
        d = {header[i]: (row[i] if i < len(row) else "") for i in range(len(header))}
        existing_map[key_from_dict(d)] = rnum

    # De-dupe CSV rows by key (keep the LAST occurrence per key)
    last_by_key = {}
    for d in rows:
        last_by_key[key_from_dict(d)] = d
    rows = list(last_by_key.values())

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

    return {"updated": len(updates), "appended": len(appends), "keys": upsert_keys, "dedup_rows": len(rows)}


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

    # Quick sanity output (helps debug "why am I not seeing anything?")
    print(f"CS_TRADES_TO_GSHEET: loaded_csv_rows={len(rows)} cols={len(header)} tab={tab}")
    if rows:
        print("CS_TRADES_TO_GSHEET: first_row_keys_sample:", {k: rows[0].get(k, "") for k in header[:8]})

    creds = creds_from_env()
    svc = build("sheets", "v4", credentials=creds, cache_discovery=False)

    ensure_sheet_tab(svc, spreadsheet_id, tab)
    res = upsert_rows(svc, spreadsheet_id, tab, rows, header)

    print(
        f"CS_TRADES_TO_GSHEET: {path} → {tab} "
        f"appended={res['appended']} updated={res['updated']} "
        f"upsert_keys={','.join(res['keys'])} dedup_rows={res['dedup_rows']}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
