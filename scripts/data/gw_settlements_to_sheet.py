#!/usr/bin/env python3
# VERSION: 2025-10-07 v1.2 — SPX settlements from GammaWizard (fallback: Schwab)

import os, json, math
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo
import requests

from schwab.auth import client_from_token_file
from google.oauth2 import service_account
from googleapiclient.discovery import build as gbuild

ET = ZoneInfo("America/New_York")
TAB = "sw_settlements"
HEADERS = ["exp_primary","settle"]

# -------- Sheets --------
def sheets_client():
    sid = os.environ["GSHEET_ID"]
    sa = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
    creds = service_account.Credentials.from_service_account_info(
        sa, scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return gbuild("sheets","v4",credentials=creds), sid

def ensure_tab(svc, sid: str):
    meta = svc.spreadsheets().get(spreadsheetId=sid).execute()
    names = {s["properties"]["title"] for s in meta.get("sheets", [])}
    if TAB not in names:
        svc.spreadsheets().batchUpdate(spreadsheetId=sid, body={"requests":[{"addSheet":{"properties":{"title":TAB}}}]}).execute()
    got = svc.spreadsheets().values().get(spreadsheetId=sid, range=f"{TAB}!1:1").execute().get("values",[])
    if not got or got[0] != HEADERS:
        svc.spreadsheets().values().update(spreadsheetId=sid, range=f"{TAB}!A1",
            valueInputOption="RAW", body={"values":[HEADERS]}).execute()

def read_all(svc, sid: str) -> Dict[str, float]:
    resp = svc.spreadsheets().values().get(spreadsheetId=sid, range=f"{TAB}!A2:B").execute()
    out={}
    for r in resp.get("values", []):
        if len(r)>=2 and r[0]:
            try:
                out[str(r[0]).strip()] = float(r[1])
            except Exception:
                pass
    return out

def write_all(svc, sid: str, rows: List[List[Any]]):
    svc.spreadsheets().values().clear(spreadsheetId=sid, range=TAB).execute()
    svc.spreadsheets().values().update(
        spreadsheetId=sid, range=f"{TAB}!A1", valueInputOption="RAW",
        body={"values":[HEADERS] + rows}
    ).execute()

# -------- GW auth & fetch --------
def _sanitize_token(t: str) -> str:
    t=(t or "").strip().strip('"').strip("'")
    return t.split(None,1)[1] if t.lower().startswith("bearer ") else t

def _gw_timeout():
    try: return int(os.environ.get("GW_TIMEOUT","30"))
    except: return 30

def gw_login_token():
    email=os.environ.get("GW_EMAIL",""); pwd=os.environ.get("GW_PASSWORD","")
    if not (email and pwd): raise RuntimeError("GW_LOGIN_MISSING_CREDS")
    base=os.environ.get("GW_BASE","https://gandalf.gammawizard.com").rstrip("/")
    r=requests.post(f"{base}/goauth/authenticateFireUser", data={"email":email,"password":pwd}, timeout=_gw_timeout())
    if r.status_code!=200: raise RuntimeError(f"GW_LOGIN_HTTP_{r.status_code}:{(r.text or '')[:160]}")
    j=r.json(); t=j.get("token")
    if not t: raise RuntimeError("GW_LOGIN_NO_TOKEN")
    return t

def gw_fetch_settles(t0: str, t1: str) -> List[Tuple[str, float]]:
    """
    Try GammaWizard endpoints in this order:
      1) GW_SETTLE_ENDPOINT (env), queried with ?start=YYYY-MM-DD&end=YYYY-MM-DD
      2) /rapi/Market/SpxClose?start=...&end=...
      3) /rapi/SpxClose?start=...&end=...
    Response shapes tolerated:
      - [{"date":"2025-09-24","close":4567.89}, ...]
      - {"data":[{"d":"2025-09-24","v":4567.89}, ...]}
      - {"candles":[{"time":1695590400000,"close":4567.89}, ...]}
    """
    base = os.environ.get("GW_BASE","https://gandalf.gammawizard.com").rstrip("/")
    token = _sanitize_token(os.environ.get("GW_TOKEN","") or "")
    def call(path: str):
        nonlocal token
        def hit(t):
            h={"Accept":"application/json","Authorization":f"Bearer {t}","User-Agent":"gw-settles/1.0"}
            return requests.get(f"{base}/{path.lstrip('/')}", params={"start":t0,"end":t1}, headers=h, timeout=_gw_timeout())
        r = hit(token) if token else None
        if (r is None) or (r.status_code in (401,403)):
            token = _sanitize_token(gw_login_token())
            r = hit(token)
        if r.status_code!=200:
            raise RuntimeError(f"GW_HTTP_{r.status_code}:{(r.text or '')[:120]}")
        return r.json()

    cand = []
    paths = []
    if os.environ.get("GW_SETTLE_ENDPOINT"): paths.append(os.environ["GW_SETTLE_ENDPOINT"])
    paths += ["/rapi/Market/SpxClose", "/rapi/SpxClose"]
    for p in paths:
        try:
            j = call(p)
            cand = _parse_gw_payload(j)
            if cand: break
        except Exception:
            continue
    return cand

def _parse_gw_payload(j: Any) -> List[Tuple[str,float]]:
    out=[]
    if isinstance(j, list):
        for it in j:
            d = it.get("date") or it.get("d") or it.get("Day") or it.get("Date")
            v = it.get("settle") or it.get("Settle") or it.get("SET") or it.get("close") or it.get("Close") or it.get("v") or it.get("Value")
            if d and v is not None:
                out.append((str(d)[:10], float(v)))
    elif isinstance(j, dict):
        # common containers
        arr = j.get("data") or j.get("candles") or j.get("rows") or j.get("list") or []
        if isinstance(arr, list):
            for it in arr:
                d = it.get("date") or it.get("d") or it.get("Day") or it.get("Date")
                v = it.get("settle") or it.get("Settle") or it.get("SET") or it.get("close") or it.get("Close") or it.get("v") or it.get("Value")
                t = it.get("time") or it.get("datetime") or it.get("ts") or it.get("epoch")
                if not d and t is not None:
                    try:
                        d = datetime.fromtimestamp(int(t)/1000.0, tz=timezone.utc).astimezone(ET).date().isoformat()
                    except Exception:
                        pass
                if d and v is not None:
                    out.append((str(d)[:10], float(v)))
    # dedupe by day; last one wins
    dd={}
    for d,v in out: dd[d]=v
    return sorted(dd.items(), key=lambda x: x[0])

# -------- Schwab fallback --------
def schwab_client():
    app_key=os.environ["SCHWAB_APP_KEY"]; app_secret=os.environ["SCHWAB_APP_SECRET"]; token_json=os.environ["SCHWAB_TOKEN_JSON"]
    with open("schwab_token.json","w") as f: f.write(token_json)
    return client_from_token_file(api_key=app_key, app_secret=app_secret, token_path="schwab_token.json")

def schwab_backfill(c, days: int) -> List[Tuple[str,float]]:
    out=[]
    try:
        url = "https://api.schwabapi.com/marketdata/v1/pricehistory"
        params = {"symbol":"SPX","periodType":"day","period":str(days+3),"frequencyType":"day","frequency":"1"}
        r = c.session.get(url, params=params, timeout=15)
        if r.status_code!=200: return out
        data = r.json() or {}
        candles = data.get("candles") or data.get("data") or []
        for cd in candles:
            dt_ms = cd.get("datetime") or cd.get("time")
            close = cd.get("close") or cd.get("closePrice")
            if dt_ms is None or close is None: continue
            d = datetime.fromtimestamp(int(dt_ms)/1000.0, tz=timezone.utc).astimezone(ET).date().isoformat()
            out.append((d, float(close)))
    except Exception:
        return []
    # dedupe + sort
    dd={}
    for d,v in out: dd[d]=v
    return sorted(dd.items(), key=lambda x: x[0])

# -------- main --------
def main() -> int:
    svc, sid = sheets_client()
    ensure_tab(svc, sid)
    existing = read_all(svc, sid)

    days = int(os.environ.get("SETTLE_BACKFILL_DAYS","180"))
    t1 = datetime.now(ET).date()
    t0 = t1 - timedelta(days=days)
    t0s, t1s = t0.isoformat(), t1.isoformat()

    # 1) Try GW
    got = []
    try:
        got = gw_fetch_settles(t0s, t1s)
    except Exception as e:
        print(f"GW settle fetch failed: {e}")

    # 2) Fallback Schwab
    if not got:
        try:
            c = schwab_client()
            got = schwab_backfill(c, days)
        except Exception as e:
            print(f"Schwab fallback failed: {e}")

    # Merge: prefer new values
    dd = dict(existing)
    for d, v in got:
        dd[d] = float(v)

    # rewrite full table sorted asc
    rows = [[d, round(v,2)] for d,v in sorted(dd.items())]
    write_all(svc, sid, rows)
    print(f"SETTLES: wrote {len(rows)} rows total (range {rows[0][0] if rows else 'NA'} → {rows[-1][0] if rows else 'NA'}).")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
