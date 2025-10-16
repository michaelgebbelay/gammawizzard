#!/usr/bin/env python3
# Post GammaWizard API data to Google Sheets (UPSERT by Date+TDate if present).
# Env:
#   GSHEET_ID (required) — target spreadsheet ID
#   GOOGLE_SERVICE_ACCOUNT_JSON (required) — service account JSON (entire JSON string)
#   GW_BASE (opt) default https://gandalf.gammawizard.com
#   GW_TOKEN (opt) — bearer token; or provide GW_EMAIL + GW_PASSWORD to login
#   GW_EMAIL, GW_PASSWORD (opt) — fallback auth if token invalid/missing
#
# Usage examples:
#   python scripts/tools/gw_to_gsheet.py --endpoints GetUltraSupreme
#   python scripts/tools/gw_to_gsheet.py --endpoints GetUltraSupreme,GetLeoCross
#   python scripts/tools/gw_to_gsheet.py --endpoints GetUltraSupreme --sheet UltraSupreme
#   python scripts/tools/gw_to_gsheet.py --endpoints GetLeoCross --sheet LeoCross

import os, sys, json, time, argparse, re
import requests

from googleapiclient.discovery import build
from google.oauth2 import service_account

# ---------- GammaWizard auth + fetch ----------
def _sanitize_token(t: str) -> str:
    t=(t or "").strip().strip('"').strip("'")
    return t.split(None,1)[1] if t.lower().startswith("bearer ") else t

def gw_login_token(base: str, email: str, password: str, timeout: int = 30) -> str:
    r = requests.post(f"{base.rstrip('/')}/goauth/authenticateFireUser",
                      data={"email": email, "password": password}, timeout=timeout)
    r.raise_for_status()
    j = r.json() or {}
    t = j.get("token")
    if not t:
        raise RuntimeError("GW_LOGIN_NO_TOKEN")
    return t

def gw_fetch_endpoint(base: str, endpoint: str, token: str | None, email: str | None, password: str | None, timeout: int = 30):
    url = f"{base.rstrip('/')}/rapi/{endpoint.lstrip('/')}"
    def hit(tok):
        h={"Accept":"application/json"}
        if tok:
            h["Authorization"]=f"Bearer {_sanitize_token(tok)}"
        return requests.get(url, headers=h, timeout=timeout)

    # Try with provided token first; if 401/403, try login
    r = hit(token) if token else None
    if (r is None) or (r.status_code in (401,403)):
        if not (email and password):
            if r is None:
                raise RuntimeError("GW_AUTH_REQUIRED (no token and no credentials)")
            else:
                r.raise_for_status()
        tok = gw_login_token(base, email, password, timeout=timeout)
        r = hit(tok)

    r.raise_for_status()
    try:
        return r.json()
    except Exception:
        # If API returned text (shouldn't), wrap it
        return {"raw": r.text}

def extract_rows(endpoint: str, payload):
    """
    Return list[dict]; for arrays it's passthrough; for dicts pick most recent inner list/dict.
    """
    # Supreme returns list[dict]
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]

    # If nested, try to find a list/dict with 'Date' keys
    if isinstance(payload, dict):
        # Common: { Trade: [...]} or similar
        if "Trade" in payload and isinstance(payload["Trade"], list):
            arr = [x for x in payload["Trade"] if isinstance(x, dict)]
            return arr
        # Otherwise, try values()
        for v in payload.values():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                return v
            if isinstance(v, dict) and "Date" in v:
                return [v]
        # Single dict fallback
        if "Date" in payload or "TDate" in payload:
            return [payload]

    # Fallback: nothing usable
    return []

# ---------- Google Sheets helpers ----------
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def creds_from_env():
    raw = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON","").strip()
    if not raw:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON env is required")
    try:
        info = json.loads(raw)
    except Exception as e:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON must be a full JSON string") from e
    return service_account.Credentials.from_service_account_info(info, scopes=SCOPES)

def ensure_sheet_tab(svc, spreadsheet_id: str, title: str) -> int:
    meta = svc.spreadsheets().get(spreadsheetId=spreadsheet_id, fields="sheets.properties").execute()
    for s in (meta.get("sheets") or []):
        p = s.get("properties") or {}
        if (p.get("title") or "") == title:
            return int(p.get("sheetId"))
    # create
    req = {"requests":[{"addSheet":{"properties":{"title": title}}}]}
    r = svc.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=req).execute()
    sid = r["replies"][0]["addSheet"]["properties"]["sheetId"]
    return int(sid)

def col_letter(idx_zero_based: int) -> str:
    n = idx_zero_based + 1
    s = ""
    while n:
        n, r = divmod(n-1, 26)
        s = chr(65 + r) + s
    return s

def header_for_endpoint(endpoint: str, items: list[dict]) -> list[str]:
    # Preferred order for Supreme
    if re.search(r"Supreme", endpoint, re.IGNORECASE):
        sup = ["Date","TDate","SPX","PlusTwo","MinusTwo","NP1","NT1",
               "P0","T0","P1","T1","P2","T2","P3","T3","M","SD"]
        # Include any extra keys at the end in alpha order
        extra = sorted({k for row in items for k in row.keys() if k not in sup})
        return sup + extra
    # Generic: stable alphabetical of observed keys
    keys = []
    seen = set()
    for row in items:
        for k in row.keys():
            if k not in seen:
                seen.add(k); keys.append(k)
    return sorted(keys)

def read_sheet_all(svc, spreadsheet_id: str, title: str):
    r = svc.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=f"{title}!A1:ZZ").execute()
    return r.get("values") or []

def upsert_rows(svc, spreadsheet_id: str, title: str, rows: list[dict], header: list[str]):
    existing = read_sheet_all(svc, spreadsheet_id, title)
    # Ensure header
    if not existing:
        existing = [header]
        svc.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{title}!A1:{col_letter(len(header)-1)}1",
            valueInputOption="RAW",
            body={"values":[header]}
        ).execute()
    else:
        # If header differs, re-write it to match our header order (minimal)
        cur_header = existing[0]
        if cur_header != header:
            svc.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"{title}!A1:{col_letter(len(header)-1)}1",
                valueInputOption="RAW",
                body={"values":[header]}
            ).execute()
            existing = [header] + existing[1:]

    # Build index (Date,TDate) → row_number (1-based)
    hdr_index = {h:i for i,h in enumerate(header)}
    key_has_date  = ("Date" in hdr_index)
    key_has_tdate = ("TDate" in hdr_index)

    def make_key(d: dict):
        if key_has_date and key_has_tdate:
            return (str(d.get("Date","")), str(d.get("TDate","")))
        if key_has_date:
            return (str(d.get("Date","")),)
        # fallback: full row in header order
        return tuple(str(d.get(h,"")) for h in header)

    existing_map = {}
    for rnum, row in enumerate(existing[1:], start=2):
        d = {header[i]: (row[i] if i < len(row) else "") for i in range(len(header))}
        existing_map[make_key(d)] = rnum

    # Prepare updates + appends
    updates = []
    appends = []
    for d in rows:
        key = make_key(d)
        values = [d.get(h, "") for h in header]
        if key in existing_map:
            rnum = existing_map[key]
            rng = f"{title}!A{rnum}:{col_letter(len(header)-1)}{rnum}"
            updates.append((rng, values))
        else:
            appends.append(values)

    # Batch updates
    if updates:
        data = [{"range": rng, "values":[vals]} for (rng, vals) in updates]
        svc.spreadsheets().values().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"valueInputOption":"RAW","data": data}
        ).execute()

    # Appends
    if appends:
        svc.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=f"{title}!A1",
            includeValuesInResponse=False,
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": appends}
        ).execute()

    return {"updated": len(updates), "appended": len(appends)}

# ---------- CLI ----------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--endpoints", required=True,
                    help="Comma-separated list of /rapi endpoints, e.g. GetUltraSupreme,GetLeoCross")
    ap.add_argument("--sheet", default="", help="Tab title override (single endpoint only). Default: endpoint sans 'Get'")
    args = ap.parse_args()

    spreadsheet_id = os.environ.get("GSHEET_ID","").strip()
    if not spreadsheet_id:
        print("ERROR: GSHEET_ID env is required", file=sys.stderr); return 2

    creds = creds_from_env()
    svc = build("sheets", "v4", credentials=creds, cache_discovery=False)

    base = os.environ.get("GW_BASE","https://gandalf.gammawizard.com")
    token = os.environ.get("GW_TOKEN","")
    email = os.environ.get("GW_EMAIL","")
    password = os.environ.get("GW_PASSWORD","")

    ep_list = [e.strip() for e in args.endpoints.split(",") if e.strip()]
    if not ep_list:
        print("ERROR: no endpoints", file=sys.stderr); return 2

    for endpoint in ep_list:
        payload = gw_fetch_endpoint(base, endpoint, token, email, password)
        items = extract_rows(endpoint, payload)
        if not items:
            print(f"[{endpoint}] nothing to write")
            continue

        title = args.sheet.strip() if (args.sheet.strip() and len(ep_list)==1) else re.sub(r"^Get","", endpoint).strip() or endpoint
        ensure_sheet_tab(svc, spreadsheet_id, title)
        header = header_for_endpoint(endpoint, items)
        res = upsert_rows(svc, spreadsheet_id, title, items, header)
        print(f"[{endpoint} → {title}] appended={res['appended']} updated={res['updated']}")

    return 0

if __name__ == "__main__":
    sys.exit(main())
