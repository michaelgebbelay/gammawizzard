#!/usr/bin/env python3
"""
gw_leo_to_sheet.py
Fetch GammaWizard GetLeoCross → write to Google Sheet tab `gw_leocross`.

Env you must set:
  # Google
  GOOGLE_SERVICE_ACCOUNT_JSON  (the JSON string for a service account with edit access)
  GSHEET_ID                    (spreadsheet ID)

  # GammaWizard: prefer token OR email/password
  GW_TOKEN                     (can be "Bearer …" or raw token)
  -- or --
  GW_EMAIL
  GW_PASSWORD

Optional:
  GW_BASE=https://gandalf.gammawizard.com
  GW_ENDPOINT=/rapi/GetLeoCross
  GW_TIMEOUT=30
  GW_DUMP_DIR=gw_dump            (if set, writes raw JSON dumps here)
  GW_LEO_TAB=gw_leocross         (tab name override)
"""

import os, sys, json, re
from pathlib import Path
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone
import requests

# --------- Config / helpers ---------

HEADERS = [
    "as_of_date","tdate","limit_put","limit_call","spx","forward",
    "cat1","cat2","put_w","call_w","l_imp","r_imp",
    "rv","rv20","rv10","rv5","vix","m","source"
]

def _nonempty(*names: str) -> Optional[str]:
    for n in names:
        v = os.environ.get(n)
        if v is not None and str(v).strip():
            return str(v).strip()
    return None

def _sanitize_token(t: Optional[str]) -> Optional[str]:
    if not t: return None
    t = t.strip().strip('"').strip("'")
    return t.split(None, 1)[1] if t.lower().startswith("bearer ") else t

def _timeout() -> int:
    try: return int(os.environ.get("GW_TIMEOUT", "30"))
    except: return 30

def _slug(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_.-]+","_",s).strip("_")[:120] or "gw"

def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

def _dump_raw(text: str, path: str, dump_dir_env: str = "GW_DUMP_DIR"):
    dd = _nonempty(dump_dir_env)
    if not dd: return
    d = Path(dd); d.mkdir(parents=True, exist_ok=True)
    fp = d / f"{_ts()}_{_slug(path)}.json"
    fp.write_text(text, encoding="utf-8")
    print(f"[GW] dumped raw → {fp} ({len(text.encode('utf-8'))} bytes)")

def _to_float(x):
    try: return float(x)
    except Exception: return None

def _to_int(x):
    try: return int(float(x))
    except Exception: return None

# --------- GW fetch ---------

def gw_login_token(base: str) -> str:
    email = _nonempty("GW_EMAIL"); pwd = _nonempty("GW_PASSWORD")
    if not (email and pwd):
        raise RuntimeError("GW_LOGIN_MISSING_CREDS")
    r = requests.post(f"{base}/goauth/authenticateFireUser",
                      data={"email": email, "password": pwd},
                      timeout=_timeout())
    r.raise_for_status()
    j = r.json()
    tok = j.get("token") or j.get("access_token")
    if not tok:
        raise RuntimeError(f"GW_LOGIN_NO_TOKEN in response: {list(j.keys())}")
    return _sanitize_token(tok) or ""

def gw_get_leocross(base: str, endpoint: str, token: Optional[str]) -> Dict[str, Any]:
    url = f"{base.rstrip('/')}/{endpoint.lstrip('/')}"
    h = {"Accept":"application/json","User-Agent":"gw-leo-to-sheet/1.0"}
    if token: h["Authorization"] = f"Bearer {token}"
    r = requests.get(url, headers=h, timeout=_timeout())
    if r.status_code in (401,403) and not token:
        # try login if no token was provided
        token = gw_login_token(base)
        h["Authorization"] = f"Bearer {token}"
        r = requests.get(url, headers=h, timeout=_timeout())
    elif r.status_code in (401,403) and token:
        # try fresh login if provided token failed
        token = gw_login_token(base)
        h["Authorization"] = f"Bearer {token}"
        r = requests.get(url, headers=h, timeout=_timeout())

    _dump_raw(r.text, endpoint)
    if r.status_code != 200:
        raise RuntimeError(f"GW_HTTP_{r.status_code}: {(r.text or '')[:180]}")
    return r.json()

# --------- Parse LeoCross ---------

def parse_leocross(obj: Any) -> List[List[Any]]:
    """
    Returns rows matching HEADERS.
    Source labels: 'Trade' for the current trade block, 'Pred' for historical predictions.
    """
    rows: List[List[Any]] = []

    def as_row(tr: Dict[str, Any], source: str) -> List[Any]:
        return [
            str(tr.get("Date",""))[:10],
            str(tr.get("TDate",""))[:10],
            _to_int(tr.get("Limit")),
            _to_int(tr.get("CLimit")),
            _to_float(tr.get("SPX")),
            _to_float(tr.get("Forward")),
            _to_float(tr.get("Cat1")),
            _to_float(tr.get("Cat2")),
            _to_float(tr.get("Put")),
            _to_float(tr.get("Call")),
            _to_float(tr.get("LImp")),
            _to_float(tr.get("RImp")),
            _to_float(tr.get("RV")),
            _to_float(tr.get("RV20")),
            _to_float(tr.get("RV10")),
            _to_float(tr.get("RV5")),
            _to_float(tr.get("VIX")),
            _to_int(tr.get("M")),
            source
        ]

    if isinstance(obj, dict):
        # Trade (latest)
        t = obj.get("Trade")
        if isinstance(t, dict):
            rows.append(as_row(t, "Trade"))
        elif isinstance(t, list):
            for tr in t:
                if isinstance(tr, dict):
                    rows.append(as_row(tr, "Trade"))

        # Predictions (history)
        p = obj.get("Predictions")
        if isinstance(p, list):
            for tr in p:
                if isinstance(tr, dict):
                    rows.append(as_row(tr, "Pred"))

    # Dedup by as_of_date keeping the most relevant row for each date.
    dedup: Dict[str, List[Any]] = {}
    idx_as_of = HEADERS.index("as_of_date")
    idx_source = HEADERS.index("source")

    def prefer_new(prev: Optional[List[Any]], new: List[Any]) -> bool:
        """Return True if ``new`` should replace ``prev`` for a date key."""
        if prev is None:
            return True
        src_prev = (prev[idx_source] or "").lower()
        src_new = (new[idx_source] or "").lower()
        if src_prev == src_new:
            # Later occurrence wins to reflect freshest pull.
            return True
        if src_new == "trade" and src_prev != "trade":
            # Prefer the Trade snapshot over historical predictions.
            return True
        if src_prev == "trade" and src_new != "trade":
            return False
        # Otherwise allow the latest row to override.
        return True

    for r in rows:
        key = r[idx_as_of] or ""
        if prefer_new(dedup.get(key), r):
            dedup[key] = r

    # Sort by as_of_date descending so the newest data is first.
    return sorted(
        dedup.values(),
        key=lambda r: (r[idx_as_of] or ""),
        reverse=True
    )

# --------- Google Sheets ---------

def sheets_client():
    sa = _nonempty("GOOGLE_SERVICE_ACCOUNT_JSON")
    sid = _nonempty("GSHEET_ID")
    if not (sa and sid):
        raise RuntimeError("SHEETS_CREDS_MISSING(GOOGLE_SERVICE_ACCOUNT_JSON or GSHEET_ID)")
    from google.oauth2 import service_account
    from googleapiclient.discovery import build as gbuild
    creds = service_account.Credentials.from_service_account_info(
        json.loads(sa),
        scopes=["https://www.googleapis.com/auth/spreadsheets"])
    return gbuild("sheets","v4",credentials=creds), sid

def ensure_tab_and_header(svc, sid: str, tab: str, headers: List[str]):
    meta = svc.spreadsheets().get(spreadsheetId=sid).execute()
    names = {s["properties"]["title"] for s in meta.get("sheets", [])}
    if tab not in names:
        svc.spreadsheets().batchUpdate(
            spreadsheetId=sid,
            body={"requests":[{"addSheet":{"properties":{"title":tab}}}]}
        ).execute()
    got = svc.spreadsheets().values().get(
        spreadsheetId=sid, range=f"{tab}!1:1"
    ).execute().get("values", [])
    if not got or got[0] != headers:
        svc.spreadsheets().values().update(
            spreadsheetId=sid, range=f"{tab}!A1",
            valueInputOption="RAW", body={"values":[headers]}
        ).execute()

def write_rows(svc, sid: str, tab: str, headers: List[str], rows: List[List[Any]]):
    ensure_tab_and_header(svc, sid, tab, headers)
    # Replace entire tab with latest rows (simple, deterministic)
    svc.spreadsheets().values().clear(spreadsheetId=sid, range=tab).execute()
    body = {"values":[headers] + rows}
    svc.spreadsheets().values().update(
        spreadsheetId=sid, range=f"{tab}!A1",
        valueInputOption="RAW", body=body
    ).execute()

# --------- main ---------

def main():
    base = _nonempty("GW_BASE") or "https://gandalf.gammawizard.com"
    endpoint = _nonempty("GW_ENDPOINT") or "/rapi/GetLeoCross"
    tab = _nonempty("GW_LEO_TAB") or "gw_leocross"
    token = _sanitize_token(_nonempty("GW_TOKEN","GW_VAR2_TOKEN"))

    print(f"[GW] base={base} endpoint={endpoint} tab={tab}")
    j = gw_get_leocross(base, endpoint, token)
    rows = parse_leocross(j)
    print(f"[GW] parsed rows: {len(rows)}")

    svc, sid = sheets_client()
    write_rows(svc, sid, tab, HEADERS, rows)
    print(f"[SHEETS] updated spreadsheet={sid} tab={tab} rows={len(rows)}")
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print("FATAL:", repr(e))
        sys.exit(1)
