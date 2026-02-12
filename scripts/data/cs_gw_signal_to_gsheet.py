#!/usr/bin/env python3
"""
Write daily GammaWizard signal data to a GW_Signal sheet tab.

Fetches the latest ConstantStable payload from GW API and upserts
one row per trading day with the full signal data.

NON-BLOCKING BY DEFAULT (same pattern as other gsheet scripts).

Env:
  GW_BASE                      - GW API base URL
  GW_EMAIL / GW_PASSWORD       - GW credentials
  GSHEET_ID                    - spreadsheet ID
  GOOGLE_SERVICE_ACCOUNT_JSON  - full JSON string for service account
  CS_GW_SIGNAL_TAB             - tab name (default "GW_Signal")
"""

import os
import sys
import json

import requests

_IMPORT_ERR = None
try:
    from googleapiclient.discovery import build
    from google.oauth2 import service_account
except Exception as e:
    build = None
    service_account = None
    _IMPORT_ERR = e

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
TAG = "GW_SIGNAL"

SIGNAL_HEADER = [
    "date", "expiry", "spx", "forward",
    "vix", "vix_one",
    "put_strike", "call_strike",
    "put_price", "call_price",
    "left_go", "right_go",
    "l_imp", "r_imp",
    "l_return", "r_return",
    "fp",
    "rv", "rv5", "rv10", "rv20",
    "r", "ar", "ar2", "ar3",
    "tx",  # user-managed formula column — script never overwrites this
]

# Data columns = everything except user-managed columns at the end
DATA_COLS = SIGNAL_HEADER[:-1]  # write data up to but not including "tx"

UPSERT_KEY = "date"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def log(msg: str):
    print(f"{TAG}: {msg}")


def skip(msg: str) -> int:
    log(f"SKIP — {msg}")
    return 0


def col_letter(idx: int) -> str:
    n = idx + 1
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def ensure_sheet_tab(svc, sid: str, title: str) -> int:
    meta = svc.spreadsheets().get(spreadsheetId=sid, fields="sheets.properties").execute()
    for s in (meta.get("sheets") or []):
        p = s.get("properties") or {}
        if (p.get("title") or "") == title:
            return int(p.get("sheetId"))
    req = {"requests": [{"addSheet": {"properties": {"title": title}}}]}
    r = svc.spreadsheets().batchUpdate(spreadsheetId=sid, body=req).execute()
    return int(r["replies"][0]["addSheet"]["properties"]["sheetId"])


def creds_from_env():
    raw = (os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON") or "").strip()
    if not raw:
        return None
    info = json.loads(raw)
    return service_account.Credentials.from_service_account_info(info, scopes=SCOPES)


# ---------------------------------------------------------------------------
# GW API
# ---------------------------------------------------------------------------

def _sanitize_token(t: str) -> str:
    t = (t or "").strip().strip('"').strip("'")
    return t.split(None, 1)[1] if t.lower().startswith("bearer ") else t


def gw_fetch():
    base = (os.environ.get("GW_BASE", "https://gandalf.gammawizard.com") or "").rstrip("/")
    endpoint = (os.environ.get("GW_ENDPOINT", "rapi/GetUltraPureConstantStable") or "").lstrip("/")
    url = f"{base}/{endpoint}"

    def hit(tok):
        h = {"Accept": "application/json"}
        if tok:
            h["Authorization"] = f"Bearer {_sanitize_token(tok)}"
        return requests.get(url, headers=h, timeout=30)

    tok = _sanitize_token(os.environ.get("GW_TOKEN", "") or "")
    r = hit(tok) if tok else None

    if (r is None) or (r.status_code in (401, 403)):
        email = os.environ.get("GW_EMAIL", "")
        pwd = os.environ.get("GW_PASSWORD", "")
        if not (email and pwd):
            raise RuntimeError("GW_AUTH_REQUIRED")
        rr = requests.post(
            f"{base}/goauth/authenticateFireUser",
            data={"email": email, "password": pwd},
            timeout=30,
        )
        rr.raise_for_status()
        t = rr.json().get("token") or ""
        r = hit(t)

    r.raise_for_status()
    return r.json()


def extract_predictions(j) -> list[dict]:
    """Extract the Predictions array (or Trade if no Predictions)."""
    if isinstance(j, dict):
        preds = j.get("Predictions") or j.get("Trade")
        if isinstance(preds, list):
            return preds
        if isinstance(preds, dict):
            return [preds]
        for v in j.values():
            if isinstance(v, (dict, list)):
                result = extract_predictions(v)
                if result:
                    return result
    return []


def signal_row(tr: dict) -> dict:
    """Map a GW trade dict to our signal row."""
    def s(key):
        v = tr.get(key)
        return "" if v is None else str(v)

    return {
        "date": s("Date"),
        "expiry": s("TDate"),
        "spx": s("SPX"),
        "forward": s("Forward"),
        "vix": s("VIX"),
        "vix_one": s("VixOne"),
        "put_strike": s("Limit"),
        "call_strike": s("CLimit"),
        "put_price": s("Put"),
        "call_price": s("Call"),
        "left_go": s("LeftGo"),
        "right_go": s("RightGo"),
        "l_imp": s("LImp"),
        "r_imp": s("RImp"),
        "l_return": s("LReturn"),
        "r_return": s("RReturn"),
        "fp": s("FP"),
        "rv": s("RV"),
        "rv5": s("RV5"),
        "rv10": s("RV10"),
        "rv20": s("RV20"),
        "r": s("R"),
        "ar": s("AR"),
        "ar2": s("AR2"),
        "ar3": s("AR3"),
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    if build is None or service_account is None:
        msg = f"google sheets libs not installed ({_IMPORT_ERR})"
        return skip(msg)

    spreadsheet_id = (os.environ.get("GSHEET_ID") or "").strip()
    if not spreadsheet_id:
        return skip("GSHEET_ID missing")

    raw_sa = (os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON") or "").strip()
    if not raw_sa:
        return skip("SA creds missing")

    tab = (os.environ.get("CS_GW_SIGNAL_TAB") or "GW_Signal").strip()

    try:
        # Fetch GW data
        api = gw_fetch()
        predictions = extract_predictions(api)
        if not predictions:
            return skip("no predictions from GW")

        rows = [signal_row(tr) for tr in predictions if tr.get("Date")]
        if not rows:
            return skip("no valid signal rows")

        log(f"fetched {len(rows)} signal row(s) from GW")

        # Connect to Sheets
        creds = creds_from_env()
        if creds is None:
            return skip("SA creds empty")
        svc = build("sheets", "v4", credentials=creds, cache_discovery=False)

        ensure_sheet_tab(svc, spreadsheet_id, tab)

        # Read existing data
        resp = svc.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id, range=f"{tab}!A1:ZZ"
        ).execute()
        existing = resp.get("values") or []

        header_col = col_letter(len(SIGNAL_HEADER) - 1)  # full header range (includes tx)
        data_col = col_letter(len(DATA_COLS) - 1)        # data range (excludes tx)

        # Ensure header (write full header including tx)
        if not existing or existing[0][:len(SIGNAL_HEADER)] != SIGNAL_HEADER:
            svc.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"{tab}!A1:{header_col}1",
                valueInputOption="RAW",
                body={"values": [SIGNAL_HEADER]},
            ).execute()
            if existing:
                existing = [SIGNAL_HEADER] + existing[1:]
            else:
                existing = [SIGNAL_HEADER]

        # Build existing date map
        date_idx = 0  # "date" is first column
        existing_dates = {}
        for rnum, row in enumerate(existing[1:], start=2):
            dt = row[date_idx] if date_idx < len(row) else ""
            if dt:
                existing_dates[dt] = rnum

        # Upsert rows (write data columns only — preserve user tx formulas)
        updates = []
        appends = []
        for d in rows:
            values = [str(d.get(h, "")) for h in DATA_COLS]
            dt = d.get("date", "")
            if dt in existing_dates:
                rnum = existing_dates[dt]
                rng = f"{tab}!A{rnum}:{data_col}{rnum}"
                updates.append({"range": rng, "values": [values]})
            else:
                appends.append(values)

        if updates:
            svc.spreadsheets().values().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={"valueInputOption": "RAW", "data": updates},
            ).execute()

        if appends:
            svc.spreadsheets().values().append(
                spreadsheetId=spreadsheet_id,
                range=f"{tab}!A1",
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body={"values": appends},
            ).execute()

        log(f"updated={len(updates)} appended={len(appends)} to {tab}")
        return 0

    except Exception as e:
        log(f"WARN — {type(e).__name__}: {e}")
        return 0


if __name__ == "__main__":
    sys.exit(main())
