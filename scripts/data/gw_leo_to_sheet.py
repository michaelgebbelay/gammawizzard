#!/usr/bin/env python3
# VERSION: 2025-10-07 v1.0 — Leo→Sheet auto-ingest (NBBO mid), zero manual

import os, sys, json, re, math, time
from datetime import datetime, date, timezone
from zoneinfo import ZoneInfo
import requests
from typing import List, Any, Optional, Tuple

from schwab.auth import client_from_token_file
from google.oauth2 import service_account
from googleapiclient.discovery import build as gbuild

ET = ZoneInfo("America/New_York")
RAW_LEO_TAB = "sw_leo_orders"        # exact schema expected by your summary
AUDIT_TAB   = "gw_leo_ingest_log"

HEADERS_LEO = ["exp_primary","side","short_put","long_put","short_call","long_call","price"]
HEADERS_AUD = ["ingest_ts","exp_primary","side","width","nbbo_bid","nbbo_ask","nbbo_mid","price_used","legs_bp","legs_sp","legs_sc","legs_bc","signal_date","note"]

# ---------- Sheets ----------
def sheets_client():
    sid = os.environ["GSHEET_ID"]
    sa_json = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
    creds = service_account.Credentials.from_service_account_info(
        json.loads(sa_json), scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    svc = gbuild("sheets","v4",credentials=creds)
    return svc, sid

def ensure_tab_with_header(svc, sid: str, tab: str, headers: List[str]):
    meta = svc.spreadsheets().get(spreadsheetId=sid).execute()
    titles = {s["properties"]["title"] for s in meta.get("sheets",[])}
    if tab not in titles:
        svc.spreadsheets().batchUpdate(
            spreadsheetId=sid, body={"requests":[{"addSheet":{"properties":{"title":tab}}}]}
        ).execute()
    got = svc.spreadsheets().values().get(spreadsheetId=sid, range=f"{tab}!1:1").execute().get("values",[])
    if not got or got[0] != headers:
        svc.spreadsheets().values().update(
            spreadsheetId=sid, range=f"{tab}!A1", valueInputOption="RAW", body={"values":[headers]}
        ).execute()

def read_rows(svc, sid: str, tab: str, headers: List[str]) -> List[List[Any]]:
    last_col = chr(ord("A")+len(headers)-1)
    resp = svc.spreadsheets().values().get(spreadsheetId=sid, range=f"{tab}!A2:{last_col}").execute()
    vals = resp.get("values", [])
    out=[]
    for r in vals:
        row = list(r)+[""]*(len(headers)-len(r))
        out.append(row[:len(headers)])
    return out

def overwrite_row(svc, sid: str, tab: str, headers: List[str], row_idx_2based: int, row_vals: List[Any]):
    rng = f"{tab}!A{row_idx_2based}"
    svc.spreadsheets().values().update(
        spreadsheetId=sid, range=rng, valueInputOption="RAW", body={"values":[row_vals]}
    ).execute()

def append_row(svc, sid: str, tab: str, row_vals: List[Any]):
    svc.spreadsheets().values().append(
        spreadsheetId=sid, range=f"{tab}!A1", valueInputOption="RAW",
        insertDataOption="INSERT_ROWS", body={"values":[row_vals]}
    ).execute()

# ---------- Shared utils (from your stack) ----------
def yymmdd(iso: str) -> str:
    d = date.fromisoformat((iso or "")[:10])
    return "{:%y%m%d}".format(d)

def to_osi(sym: str) -> str:
    raw = (sym or "").upper()
    raw = re.sub(r'\s+', '', raw).lstrip('.')
    raw = re.sub(r'[^A-Z0-9.$^]', '', raw)
    m = re.match(r'^([A-Z.$^]{1,6})(\d{6})([CP])(\d{1,5})(?:\.(\d{1,3}))?$', raw) \
        or re.match(r'^([A-Z.$^]{1,6})(\d{6})([CP])(\d{8})$', raw)
    if not m: raise ValueError("Cannot parse option symbol: " + sym)
    root, ymd, cp, strike, frac = (m.groups()+("",))[:5]
    if len(strike)==8 and not frac:
        mills = int(strike)
    else:
        mills = int(strike)*1000 + (int((frac or "0").ljust(3,'0')) if frac else 0)
    return "{:<6s}{}{}{:08d}".format(root, ymd, cp, mills)

def strike_from_osi(osi: str) -> float:
    return int(osi[-8:]) / 1000.0

def _sanitize_token(t: str) -> str:
    t = (t or "").strip().strip('"').strip("'")
    return t.split(None,1)[1] if t.lower().startswith("bearer ") else t

def _gw_timeout():
    try: return int(os.environ.get("GW_TIMEOUT","30"))
    except: return 30

def gw_login_token():
    email=os.environ.get("GW_EMAIL",""); pwd=os.environ.get("GW_PASSWORD","")
    if not (email and pwd): raise RuntimeError("GW_LOGIN_MISSING_CREDS")
    r=requests.post(f"{os.environ.get('GW_BASE','https://gandalf.gammawizard.com')}/goauth/authenticateFireUser",
                    data={"email":email,"password":pwd}, timeout=_gw_timeout())
    if r.status_code!=200: raise RuntimeError(f"GW_LOGIN_HTTP_{r.status_code}:{(r.text or '')[:180]}")
    j=r.json(); t=j.get("token")
    if not t: raise RuntimeError("GW_LOGIN_NO_TOKEN")
    return t

def gw_get_leocross():
    base = os.environ.get("GW_BASE","https://gandalf.gammawizard.com")
    endpoint = os.environ.get("GW_ENDPOINT","/rapi/GetLeoCross")
    tok=_sanitize_token(os.environ.get("GW_TOKEN","") or "")
    def hit(t):
        h={"Accept":"application/json","Authorization":f"Bearer {_sanitize_token(t)}","User-Agent":"gw-leo2sheet/1.0"}
        return requests.get(f"{base.rstrip('/')}/{endpoint.lstrip('/')}", headers=h, timeout=_gw_timeout())
    r=hit(tok) if tok else None
    if (r is None) or (r.status_code in (401,403)): r=hit(gw_login_token())
    if r.status_code!=200: raise RuntimeError(f"GW_HTTP_{r.status_code}:{(r.text or '')[:180]}")
    return r.json()

def extract_trade(j):
    if isinstance(j,dict):
        if "Trade" in j:
            tr=j["Trade"]; 
            return tr[-1] if isinstance(tr,list) and tr else tr if isinstance(tr,dict) else {}
        keys=("Date","TDate","Limit","CLimit","Cat1","Cat2")
        if any(k in j for k in keys): return j
        for v in j.values():
            if isinstance(v,(dict,list)):
                t=extract_trade(v)
                if t: return t
    if isinstance(j,list):
        for it in reversed(j):
            t=extract_trade(it)
            if t: return t
    return {}

# ---------- Schwab ----------
def schwab_client():
    app_key=os.environ["SCHWAB_APP_KEY"]; app_secret=os.environ["SCHWAB_APP_SECRET"]; token_json=os.environ["SCHWAB_TOKEN_JSON"]
    with open("schwab_token.json","w") as f: f.write(token_json)
    c=client_from_token_file(api_key=app_key, app_secret=app_secret, token_path="schwab_token.json")
    return c

def fetch_bid_ask(c, osi: str) -> Tuple[Optional[float], Optional[float]]:
    try:
        r=c.get_quote(osi)
        if r.status_code!=200: return (None,None)
        d=list(r.json().values())[0] if isinstance(r.json(), dict) else {}
        q=d.get("quote", d)
        b=q.get("bidPrice") or q.get("bid") or q.get("bidPriceInDouble")
        a=q.get("askPrice") or q.get("ask") or q.get("askPriceInDouble")
        return (float(b) if b is not None else None, float(a) if a is not None else None)
    except Exception:
        return (None,None)

def condor_nbbo(c, legs) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    bp, sp, sc, bc = legs
    bp_b, bp_a = fetch_bid_ask(c, bp); sp_b, sp_a = fetch_bid_ask(c, sp)
    sc_b, sc_a = fetch_bid_ask(c, sc); bc_b, bc_a = fetch_bid_ask(c, bc)
    if None in (bp_b, bp_a, sp_b, sp_a, sc_b, sc_a, bc_b, bc_a):
        return (None,None,None)
    net_bid = (sp_b + sc_b) - (bp_a + bc_a)
    net_ask = (sp_a + sc_a) - (bp_b + bc_b)
    mid     = (net_bid + net_ask) / 2.0
    def clamp(x): return round(round(x / 0.05) * 0.05 + 1e-12, 2)
    return (clamp(net_bid), clamp(net_ask), clamp(mid))

# ---------- Core ----------
def main() -> int:
    # Sheets ready
    svc, sid = sheets_client()
    ensure_tab_with_header(svc, sid, RAW_LEO_TAB, HEADERS_LEO)
    ensure_tab_with_header(svc, sid, AUDIT_TAB,   HEADERS_AUD)

    # GW fetch
    api = gw_get_leocross()
    tr  = extract_trade(api)
    if not tr:
        append_row(svc, sid, AUDIT_TAB, [datetime.now(ET).isoformat(), "", "", "", "", "", "", "", "", "", "", "", "", "NO_TRADE_PAYLOAD"])
        print("LEO2SHEET: NO_TRADE_PAYLOAD"); return 0

    sig_date=str(tr.get("Date",""))
    exp_iso=str(tr.get("TDate",""))
    exp_ymd=(date.fromisoformat(exp_iso[:10]).isoformat() if exp_iso else "")
    if not exp_ymd:
        append_row(svc, sid, AUDIT_TAB, [datetime.now(ET).isoformat(), "", "", "", "", "", "", "", "", "", "", "", sig_date, "NO_EXPIRY"])
        print("LEO2SHEET: NO_EXPIRY"); return 0

    def fnum(x):
        try: return float(x)
        except: return None
    inner_put=int(float(tr.get("Limit")))
    inner_call=int(float(tr.get("CLimit")))
    cat1=fnum(tr.get("Cat1")); cat2=fnum(tr.get("Cat2"))
    side = "short" if (cat2 is None or cat1 is None or cat2>=cat1) else "long"

    # Build 5-wide legs (Leo)
    width = 5
    exp6  = yymmdd(exp_ymd)
    p_low,p_high = inner_put - width, inner_put
    c_low,c_high = inner_call, inner_call + width
    bp = to_osi(f".SPXW{exp6}P{p_low}")
    sp = to_osi(f".SPXW{exp6}P{p_high}")
    sc = to_osi(f".SPXW{exp6}C{c_low}")
    bc = to_osi(f".SPXW{exp6}C{c_high}")

    # Orient: for short IC, buy lower put & higher call; for long IC reverse
    def orient(bp,sp,sc,bc, credit=True):
        bpS=strike_from_osi(bp); spS=strike_from_osi(sp)
        scS=strike_from_osi(sc); bcS=strike_from_osi(bc)
        if credit:
            if bpS>spS: bp,sp = sp,bp
            if scS>bcS: sc,bc = bc,sc
        else:
            if bpS<spS: bp,sp = sp,bp
            if bcS>scS: sc,bc = bc,sc
        return [bp,sp,sc,bc]
    legs = orient(bp,sp,sc,bc, credit=(side=="short"))

    # Schwab NBBO → price (we use mid)
    c = schwab_client()
    nbbo_bid, nbbo_ask, nbbo_mid = condor_nbbo(c, legs)
    if nbbo_mid is None:
        append_row(svc, sid, AUDIT_TAB, [datetime.now(ET).isoformat(), exp_ymd, side, width, nbbo_bid or "", nbbo_ask or "", "",
                                         "", legs[0],legs[1],legs[2],legs[3], sig_date, "NBBO_UNAVAILABLE"])
        print("LEO2SHEET: NBBO_UNAVAILABLE"); return 0

    price_used = nbbo_mid  # positive number (credit for short, debit for long)

    # Upsert into sw_leo_orders keyed by exp_primary
    rows = read_rows(svc, sid, RAW_LEO_TAB, HEADERS_LEO)
    exp_idx = { (r[0] or "").strip(): (i+2) for i,r in enumerate(rows) if (r and r[0]) }  # row index (2-based)
    short_put = max(strike_from_osi(legs[0]), strike_from_osi(legs[1]))  # SP is higher strike on put wing
    long_put  = min(strike_from_osi(legs[0]), strike_from_osi(legs[1]))
    short_call= min(strike_from_osi(legs[2]), strike_from_osi(legs[3]))  # SC is lower strike on call wing
    long_call = max(strike_from_osi(legs[2]), strike_from_osi(legs[3]))

    out_row = [exp_ymd, side, short_put, long_put, short_call, long_call, price_used]

    if exp_ymd in exp_idx:
        overwrite_row(svc, sid, RAW_LEO_TAB, HEADERS_LEO, exp_idx[exp_ymd], out_row)
        note="UPSERT"
    else:
        append_row(svc, sid, RAW_LEO_TAB, out_row)
        note="INSERT"

    # Audit
    append_row(svc, sid, AUDIT_TAB, [
        datetime.now(ET).isoformat(), exp_ymd, side, width,
        nbbo_bid, nbbo_ask, nbbo_mid, price_used,
        legs[0], legs[1], legs[2], legs[3],
        sig_date, note
    ])

    print(f"LEO2SHEET: {note} {exp_ymd} side={side} 5-wide price={price_used:.2f}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
