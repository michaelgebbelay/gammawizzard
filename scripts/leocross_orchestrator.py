#!/usr/bin/env python3
# VERSION: 2025-10-05 v4.2.0 — Short IC uses fixed 15-wide & 12k-per-contract sizing.
# - Short IC (credit): width = 15 (configurable); qty = ceil(opening_cash/12k, min 1)
# - Long  IC (debit):  legacy 5-wide; qty = floor(opening_cash / PER_UNIT)
# - BYPASS_GUARD: skip NO-CLOSE & PARTIAL-OVERLAP checks, and force rem_qty (default 1 or BYPASS_QTY)
__version__ = "4.2.0"

# LeoCross ORCHESTRATOR — strict overlap guard + dynamic sizing from opening cash.
# - Blocks if any leg would "close" something already in the account (unless BYPASS_GUARD=true).
# - If all 4 legs present and aligned, tops up to target (unless BYPASS_GUARD=true).
# - Logs every decision to Google Sheet tab "guard".
# - Invokes placer with QTY_OVERRIDE=<remainder> to execute ladder with true replace semantics.

PER_UNIT = 5000  # legacy debit sizing: contracts = floor(opening_cash / PER_UNIT)

import os, sys, json, time, re, math, random
from datetime import datetime, date, timezone
from zoneinfo import ZoneInfo
import requests
from schwab.auth import client_from_token_file

# ----- env knobs -----
CREDIT_DOLLARS_PER_CONTRACT = float(os.environ.get("CREDIT_DOLLARS_PER_CONTRACT", "12000"))
CREDIT_SPREAD_WIDTH         = int(os.environ.get("CREDIT_SPREAD_WIDTH", "15"))
CREDIT_MIN_WIDTH            = 5

def _truthy(s: str) -> bool:
    return str(s or "").strip().lower() in {"1","true","t","yes","y","on"}
BYPASS_GUARD = _truthy(os.environ.get("BYPASS_GUARD",""))
BYPASS_QTY   = os.environ.get("BYPASS_QTY","").strip()  # optional; defaults to 1 when bypassing

# Google Sheets
from google.oauth2 import service_account
from googleapiclient.discovery import build as gbuild

ET = ZoneInfo("America/New_York")
GW_BASE = "https://gandalf.gammawizard.com"
GW_ENDPOINT = "/rapi/GetLeoCross"

# ===== Sheets helpers =====
GUARD_TAB = "guard"
GUARD_HEADERS = [
    "ts","source","symbol","signal_date","decision","detail","open_units","rem_qty",
    "occ_buy_put","occ_sell_put","occ_sell_call","occ_buy_call",
    "acct_qty_bp","acct_qty_sp","acct_qty_sc","acct_qty_bc"
]

def ensure_header_and_get_sheetid(svc, spreadsheet_id: str, tab: str, header: list):
    meta = svc.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheet_id_num = None
    for sh in meta["sheets"]:
        if sh["properties"]["title"] == tab:
            sheet_id_num = sh["properties"]["sheetId"]; break
    if sheet_id_num is None:
        svc.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id,
            body={"requests":[{"addSheet":{"properties":{"title":tab}}}]}).execute()
        meta = svc.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheet_id_num = next(sh["properties"]["sheetId"] for sh in meta["sheets"] if sh["properties"]["title"]==tab)
    got = svc.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=f"{tab}!1:1").execute().get("values",[])
    if not got or got[0] != header:
        svc.spreadsheets().values().update(spreadsheetId=spreadsheet_id, range=f"{tab}!1:1",
            valueInputOption="USER_ENTERED", body={"values":[header]}).execute()
    return sheet_id_num

def top_insert(svc, spreadsheet_id: str, sheet_id_num: int):
    svc.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id,
        body={"requests":[{"insertDimension":{"range":{"sheetId":sheet_id_num,"dimension":"ROWS","startIndex":1,"endIndex":2},
                                               "inheritFromBefore": False}}]}).execute()

def guard_log(svc, sheet_id_num, spreadsheet_id: str, row_vals: list):
    try:
        top_insert(svc, spreadsheet_id, sheet_id_num)
        svc.spreadsheets().values().update(spreadsheetId=spreadsheet_id, range=f"{GUARD_TAB}!A2",
            valueInputOption="USER_ENTERED", body={"values":[row_vals]}).execute()
    except Exception as e:
        print("ORCH WARN: guard log failed — {}".format(str(e)[:200]))

# ===== utils =====
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

def osi_canon(osi: str):
    return (osi[6:12], osi[12], osi[-8:])  # (yymmdd, C/P, strike8)

def strike_from_osi(osi: str) -> float:
    return int(osi[-8:]) / 1000.0

def iso_z(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# ===== Schwab helpers =====
def _sleep_for_429(r, attempt):
    ra = r.headers.get("Retry-After")
    if ra:
        try:
            return max(1.0, float(ra))
        except Exception:
            pass
    # exponential backoff + small jitter
    return min(10.0, 0.5 * (2 ** attempt)) + random.uniform(0.0, 0.25)


def schwab_get_json(c, url, params=None, tries=6, tag=""):
    last = ""
    for i in range(tries):
        try:
            r = c.session.get(url, params=(params or {}), timeout=20)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 429:
                time.sleep(_sleep_for_429(r, i))
                continue
            last = f"HTTP_{r.status_code}:{(r.text or '')[:160]}"
        except Exception as e:
            last = f"{type(e).__name__}:{str(e)}"
        time.sleep(0.5 * (2 ** i))
    raise RuntimeError(f"SCHWAB_GET_FAIL({tag}) {last}")

def _osi_from_instrument(ins: dict) -> str | None:
    sym = (ins.get("symbol") or "")
    try:
        return to_osi(sym)
    except Exception:
        pass
    exp = ins.get("optionExpirationDate") or ins.get("expirationDate") or ""
    pc  = (ins.get("putCall") or ins.get("type") or "").upper()
    strike = ins.get("strikePrice") or ins.get("strike")
    try:
        if exp and pc in ("CALL","PUT") and strike is not None:
            ymd = date.fromisoformat(str(exp)[:10]).strftime("%y%m%d")
            cp = "C" if pc.startswith("C") else "P"
            mills = int(round(float(strike)*1000))
            return "{:<6s}{}{}{:08d}".format("SPXW", ymd, cp, mills)
    except Exception:
        pass
    return None

def positions_map(c, acct_hash: str):
    url="https://api.schwabapi.com/trader/v1/accounts/{}".format(acct_hash)
    j=schwab_get_json(c,url,params={"fields":"positions"},tag="POSITIONS")
    sa=j[0]["securitiesAccount"] if isinstance(j,list) else (j.get("securitiesAccount") or j)
    out={}
    for p in (sa.get("positions") or []):
        ins=p.get("instrument",{}) or {}
        atype = (ins.get("assetType") or ins.get("type") or "").upper()
        if atype != "OPTION": continue
        osi = _osi_from_instrument(ins)
        if not osi: continue
        qty=float(p.get("longQuantity",0))-float(p.get("shortQuantity",0))
        if abs(qty)<1e-9: continue
        out[osi_canon(osi)] = out.get(osi_canon(osi), 0.0) + qty
    return out

# ===== opening cash + account =====
def get_primary_acct(c):
    r=c.get_account_numbers(); r.raise_for_status()
    first=r.json()[0]
    return str(first.get("accountNumber")), str(first.get("hashValue"))

def opening_cash_for_account(c, acct_number: str):
    r = c.get_accounts(); r.raise_for_status()
    data = r.json()
    accs = data if isinstance(data, list) else [data]

    def pick(d,*ks):
        for k in ks:
            v = (d or {}).get(k)
            if isinstance(v,(int,float)): return float(v)

    def hunt(a):
        acct_id=None; initial={}; current={}
        stack=[a]
        while stack:
            x=stack.pop()
            if isinstance(x,dict):
                if acct_id is None and x.get("accountNumber"): acct_id=str(x["accountNumber"])
                if "initialBalances" in x and isinstance(x["initialBalances"], dict): initial=x["initialBalances"]
                if "currentBalances" in x and isinstance(x["currentBalances"], dict): current=x["currentBalances"]
                for v in x.values():
                    if isinstance(v,(dict,list)): stack.append(v)
            elif isinstance(x,list):
                stack.extend(x)
        return acct_id, initial, current

    chosen=None
    for a in accs:
        aid, init, curr = hunt(a)
        if acct_number and aid == acct_number:
            chosen=(init,curr); break
        if chosen is None:
            chosen=(init,curr)

    if not chosen: return None
    init, curr = chosen
    oc = pick(init,"cashBalance","cashAvailableForTrading","liquidationValue")
    if oc is None:
        oc = pick(curr,"cashBalance","cashAvailableForTrading","liquidationValue")
    return oc

# ===== width =====
def calc_short_ic_width(opening_cash: float | int) -> int:
    width = max(CREDIT_MIN_WIDTH, int(CREDIT_SPREAD_WIDTH))
    return int(math.ceil(width / 5.0) * 5)

def calc_short_ic_contracts(opening_cash: float | int) -> int:
    try:
        oc = float(opening_cash)
    except Exception:
        oc = 0.0
    denom = CREDIT_DOLLARS_PER_CONTRACT if CREDIT_DOLLARS_PER_CONTRACT > 0 else 1.0
    units = math.ceil(max(0.0, oc) / denom)
    return max(1, int(units))

# ===== condor helpers =====
def condor_units_open(pos_map, legs):
    b1 = max(0.0,  pos_map.get(osi_canon(legs[0]), 0.0))  # long wing put
    b2 = max(0.0,  pos_map.get(osi_canon(legs[3]), 0.0))  # long wing call
    s1 = max(0.0, -pos_map.get(osi_canon(legs[1]), 0.0))  # short inner put
    s2 = max(0.0, -pos_map.get(osi_canon(legs[2]), 0.0))  # short inner call
    return int(min(b1, b2, s1, s2))

def print_guard_snapshot(pos, legs, is_credit, width_used, bypass):
    labels = [("BUY_PUT",legs[0],-1),("SELL_PUT",legs[1],+1),("SELL_CALL",legs[2],+1),("BUY_CALL",legs[3],-1)]
    tag = "CREDIT" if is_credit else "DEBIT"
    print("ORCH GUARD SNAPSHOT ({} width={} BYPASS={}):".format(tag, width_used, "ON" if bypass else "OFF"))
    for name, osi, sign in labels:
        can = osi_canon(osi); cur = pos.get(can, 0.0)
        print("  {:10s} {}  acct_qty={:+g}  sign={:+d}".format(name, osi, cur, sign))

# ===== main =====
def main():
    # Sheets init (non-fatal)
    svc = None; sheet_id = None; guard_sheet_id = None
    try:
        sheet_id=os.environ["GSHEET_ID"]
        sa_json=os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
        creds=service_account.Credentials.from_service_account_info(json.loads(sa_json),
            scopes=["https://www.googleapis.com/auth/spreadsheets"])
        svc=gbuild("sheets","v4",credentials=creds)
        guard_sheet_id = ensure_header_and_get_sheetid(svc, sheet_id, GUARD_TAB, GUARD_HEADERS)
    except Exception as e:
        print("ORCH WARN: Sheets init failed — {}".format(str(e)[:200]))
        svc=None

    def log(decision, detail, legs=None, acct_qty=(0,0,0,0), open_units="", rem_qty=""):
        if not svc: return
        bp,sp,sc,bc = (legs or ("","","",""))
        bpq,spq,scq,bcq = acct_qty
        row=[datetime.utcnow().isoformat()+"Z","ORCH","SPX",sig_date if 'sig_date' in locals() else "",
             decision, detail, open_units, rem_qty,
             bp,sp,sc,bc, bpq,spq,scq,bcq]
        guard_log(svc, guard_sheet_id, sheet_id, row)

    # ---- Schwab auth ----
    try:
        app_key=os.environ["SCHWAB_APP_KEY"]; app_secret=os.environ["SCHWAB_APP_SECRET"]; token_json=os.environ["SCHWAB_TOKEN_JSON"]
        with open("schwab_token.json","w") as f: f.write(token_json)
        c=client_from_token_file(api_key=app_key, app_secret=app_secret, token_path="schwab_token.json")
        acct_num, acct_hash = get_primary_acct(c)
    except Exception as e:
        reason="SCHWAB_CLIENT_INIT_FAILED — " + str(e)[:200]
        print("ORCH ABORT:", reason); log("ABORT", reason)
        return 1

    # ---- Leo signal → prelim ----
    try:
        api=gw_get_leocross()
        tr=extract_trade(api)
        if not tr:
            print("ORCH SKIP: NO_TRADE_PAYLOAD"); log("SKIP","NO_TRADE_PAYLOAD")
            return 0
    except Exception as e:
        reason="GW_FETCH_FAILED — {}".format(str(e)[:200])
        print("ORCH ABORT:", reason); log("ABORT", reason)
        return 1

    sig_date=str(tr.get("Date","")); exp_iso=str(tr.get("TDate","")); exp6=yymmdd(exp_iso)
    inner_put=int(float(tr.get("Limit"))); inner_call=int(float(tr.get("CLimit")))
    def fnum(x):
        try: return float(x)
        except: return None
    cat1=fnum(tr.get("Cat1")); cat2=fnum(tr.get("Cat2"))
    is_credit = True if (cat2 is None or cat1 is None or cat2>=cat1) else False

    # ---- opening cash early (needed for Short IC width) ----
    oc = opening_cash_for_account(c, acct_num)
    if oc is None:
        reason="OPENING_CASH_UNAVAILABLE — aborting to avoid wrong size/width."
        print("ORCH ABORT:", reason); log("ABORT", reason)
        return 1

    # Build planned legs (width depends on side)
    width = calc_short_ic_width(oc) if is_credit else 5
    p_low,p_high = inner_put - width, inner_put
    c_low,c_high = inner_call, inner_call + width
    bp = to_osi(".SPX{}{}{}".format("W",exp6, f"P{p_low}"))   # BUY_TO_OPEN
    sp = to_osi(".SPX{}{}{}".format("W",exp6, f"P{p_high}"))  # SELL_TO_OPEN
    sc = to_osi(".SPX{}{}{}".format("W",exp6, f"C{c_low}"))   # SELL_TO_OPEN
    bc = to_osi(".SPX{}{}{}".format("W",exp6, f"C{c_high}"))  # BUY_TO_OPEN

    def orient(bp,sp,sc,bc):
        bpS=strike_from_osi(bp); spS=strike_from_osi(sp)
        scS=strike_from_osi(sc); bcS=strike_from_osi(bc)
        if is_credit:
            if bpS>spS: bp,sp = sp,bp
            if scS>bcS: sc,bc = bc,sc
        else:
            if bpS<spS: bp,sp = sp,bp
            if bcS>scS: sc,bc = bc,sc
        return [bp,sp,sc,bc]
    legs = orient(bp,sp,sc,bc)

    # ---- positions ----
    try:
        pos = positions_map(c, acct_hash)
    except Exception as e:
        reason="POSITIONS_FAILED — {}".format(str(e)[:200])
        print("ORCH ABORT:", reason); log("ABORT", reason, legs)
        return 1

    print("ORCH START RUN_ID={} SHA={}".format(os.environ.get("GITHUB_RUN_ID",""), os.environ.get("GITHUB_SHA","")[:7]))
    print_guard_snapshot(pos, legs, is_credit, width, BYPASS_GUARD)

    # snapshot quantities (for logging)
    bpq = pos.get(osi_canon(legs[0]), 0.0)
    spq = pos.get(osi_canon(legs[1]), 0.0)
    scq = pos.get(osi_canon(legs[2]), 0.0)
    bcq = pos.get(osi_canon(legs[3]), 0.0)
    acct_snapshot=(bpq,spq,scq,bcq)

    # ---- STRICT NO‑CLOSE check ----
    checks=[("BUY",legs[0],-1),("SELL",legs[1],+1),("SELL",legs[2],+1),("BUY",legs[3],-1)]
    if not BYPASS_GUARD:
        for label, osi, sign in checks:
            cur = pos.get(osi_canon(osi), 0.0)
            if (sign<0 and cur<0) or (sign>0 and cur>0):
                details="WOULD_CLOSE {} acct_qty={:+g}".format(osi, cur)
                print("ORCH SKIP:", details); log("SKIP", details, legs, acct_snapshot, "", "")
                return 0

    # ---- PARTIAL OVERLAP rule ----
    nonzero = sum(1 for _, osi, _ in checks if abs(pos.get(osi_canon(osi),0.0))>1e-9)
    aligned = sum(1 for _, osi, sign in checks
                  if ((sign<0 and pos.get(osi_canon(osi),0.0)>=0) or
                      (sign>0 and pos.get(osi_canon(osi),0.0)<=0)) and
                      abs(pos.get(osi_canon(osi),0.0))>1e-9)
    if not BYPASS_GUARD and 0 < nonzero < 4:
        present = ["{} {} acct_qty={:+g}".format(l, o, pos.get(osi_canon(o),0.0)) for (l,o,_) in checks if abs(pos.get(osi_canon(o),0.0))>1e-9]
        details="PARTIAL_OVERLAP — " + "; ".join(present)
        print("ORCH SKIP:", details); log("SKIP", details, legs, acct_snapshot, "", "")
        return 0

    # ---- target units ----
    if is_credit:
        target_units = calc_short_ic_contracts(oc)
    else:
        target_units = int(max(0, oc // PER_UNIT))  # legacy debit sizing

    # ---- compute remainder ----
    if BYPASS_GUARD:
        # ignore existing units; force place BYPASS_QTY (default 1)
        try:
            rem_qty = int(BYPASS_QTY) if BYPASS_QTY else 1
        except:
            rem_qty = 1
        units_open = 0
        decision = "ALLOW_BYPASS"
        detail = f"BYPASS_GUARD=1 width={width} open_cash={oc:.2f} target={target_units} rem_qty={rem_qty}"
    else:
        if nonzero==0:
            units_open = 0
        elif nonzero==4 and aligned==4:
            units_open = condor_units_open(pos, legs)
        else:
            units_open = 0
        rem_qty = max(0, target_units - units_open)
        decision = ("ALLOW" if rem_qty>0 else "SKIP")
        detail = ("short_ic width={} open_cash={:.2f} target={} units_open={} rem_qty={}"
                  if is_credit else
                  "long_ic width=5 open_cash={:.2f} per_unit={} target={} units_open={} rem_qty={}").format(
                      width, oc, target_units, units_open, rem_qty) if is_credit else \
                  "long_ic width=5 open_cash={:.2f} per_unit={} target={} units_open={} rem_qty={}".format(
                      oc, PER_UNIT, target_units, units_open, rem_qty)

    print("ORCH SIZE", detail)
    log(decision, detail, legs, acct_snapshot, (0 if BYPASS_GUARD else units_open), rem_qty)

    if rem_qty == 0:
        print("ORCH SKIP: At/above target; no remainder to place.")
        return 0

    # ---- call placer with override ----
    env = dict(os.environ)
    env["QTY_OVERRIDE"] = str(rem_qty)
    rc = os.spawnve(os.P_WAIT, sys.executable, [sys.executable, "scripts/leocross_place_simple.py"], env)
    return rc

# ===== GW helpers (kept at end to match your existing structure) =====
def _gw_timeout():
    try: return int(os.environ.get("GW_TIMEOUT","30"))
    except: return 30

def _sanitize_token(t: str) -> str:
    t = (t or "").strip().strip('"').strip("'")
    return t.split(None,1)[1] if t.lower().startswith("bearer ") else t

def gw_login_token():
    email=os.environ.get("GW_EMAIL",""); pwd=os.environ.get("GW_PASSWORD","")
    if not (email and pwd): raise RuntimeError("GW_LOGIN_MISSING_CREDS")
    r=requests.post("{}/goauth/authenticateFireUser".format(GW_BASE), data={"email":email,"password":pwd}, timeout=_gw_timeout())
    if r.status_code!=200: raise RuntimeError("GW_LOGIN_HTTP_{}:{}".format(r.status_code, (r.text or "")[:180]))
    j=r.json(); t=j.get("token")
    if not t: raise RuntimeError("GW_LOGIN_NO_TOKEN")
    return t

def gw_get_leocross():
    tok=_sanitize_token(os.environ.get("GW_TOKEN","") or "")
    def hit(t):
        h={"Accept":"application/json","Authorization":"Bearer {}".format(_sanitize_token(t)),"User-Agent":"gw-orchestrator/1.6"}
        return requests.get("{}/{}".format(GW_BASE.rstrip("/"), GW_ENDPOINT.lstrip("/")), headers=h, timeout=_gw_timeout())
    r=hit(tok) if tok else None
    if (r is None) or (r.status_code in (401,403)): r=hit(gw_login_token())
    if r.status_code!=200: raise RuntimeError("GW_HTTP_{}:{}".format(r.status_code, (r.text or "")[:180]))
    return r.json()

def extract_trade(j):
    if isinstance(j,dict):
        if "Trade" in j:
            tr=j["Trade"]
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

if __name__ == "__main__":
    sys.exit(main())
