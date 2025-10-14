#!/usr/bin/env python3
# VERSION: 2025-10-07 v4.4.1 — Orchestrator
# - Default FAST_HOLD_SECONDS=30 (start shortly after the 16:13 trigger)
# - Logs guard snapshot and calls placer with QTY_OVERRIDE
# - Aligns sizing math with guard/placer (Short IC = $4k/5-wide, Long IC = $4k)

import os, sys, json, time, re, math
from datetime import datetime, date, timezone, timedelta
from zoneinfo import ZoneInfo
import requests
from schwab.auth import client_from_token_file
from google.oauth2 import service_account
from googleapiclient.discovery import build as gbuild
from decimal import Decimal, ROUND_HALF_UP

# ===== Runtime knobs =====
def _env_float(name: str, default: float) -> float:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _env_int(name: str, default: int) -> int:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        return int(float(raw))
    except ValueError:
        return default


CREDIT_DOLLARS_PER_CONTRACT = _env_float("CREDIT_DOLLARS_PER_CONTRACT", 4000.0)
DEBIT_DOLLARS_PER_CONTRACT  = _env_float("DEBIT_DOLLARS_PER_CONTRACT", 4000.0)
# Default to 20-wide; still overridable by env.
CREDIT_SPREAD_WIDTH         = _env_int("CREDIT_SPREAD_WIDTH", 20)
CREDIT_MIN_WIDTH            = max(5, _env_int("CREDIT_MIN_WIDTH", 5))

# Read from ENV (with sensible defaults)
FAST_HOLD_SECONDS  = int(os.environ.get("FAST_HOLD_SECONDS", "30"))
GW_WARM_TIMEOUT    = int(os.environ.get("GW_WARM_TIMEOUT", "6"))
GW_REFRESH_TIMEOUT = int(os.environ.get("GW_REFRESH_TIMEOUT", "3"))
HARD_CUTOFF_HHMM   = os.environ.get("HARD_CUTOFF_HHMM", "16:15").strip()

def _truthy(s: str) -> bool:
    return str(s or "").strip().lower() in {"1","true","t","yes","y","on"}

BYPASS_GUARD = _truthy(os.environ.get("BYPASS_GUARD",""))
BYPASS_QTY   = os.environ.get("BYPASS_QTY","").strip()

ET = ZoneInfo("America/New_York")
GW_BASE = "https://gandalf.gammawizard.com"
GW_ENDPOINT = "/rapi/GetLeoCross"
GUARD_TAB = "guard"
GUARD_HEADERS = [
    "ts","source","symbol","signal_date","decision","detail","open_units","rem_qty",
    "occ_buy_put","occ_sell_put","occ_sell_call","occ_buy_call",
    "acct_qty_bp","acct_qty_sp","acct_qty_sc","acct_qty_bc"
]

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
    mills = int(strike)*1000 + (int((frac or "0").ljust(3,'0')) if frac else 0) if len(strike)<8 else int(strike)
    return "{:<6s}{}{}{:08d}".format(root, ymd, cp, mills)

def osi_canon(osi: str):
    return (osi[6:12], osi[12], osi[-8:])

def strike_from_osi(osi: str) -> float:
    return int(osi[-8:]) / 1000.0

def iso_z(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _backoff(i): return 0.6*(2**i)

# Sheets helpers (same as before)
def ensure_header_and_get_sheetid(svc, spreadsheet_id: str, tab: str, header: list):
    meta = svc.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheet_id_num=None
    for sh in meta["sheets"]:
        if sh["properties"]["title"] == tab:
            sheet_id_num=sh["properties"]["sheetId"]; break
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

# Schwab + positions (same as before)
def schwab_get_json(c, url, params=None, tries=6, tag=""):
    last=""
    for i in range(tries):
        try:
            r=c.session.get(url, params=(params or {}), timeout=20)
            if r.status_code==200: return r.json()
            last=f"HTTP_{r.status_code}:{(r.text or '')[:160]}"
        except Exception as e:
            last=f"{type(e).__name__}:{str(e)}"
        time.sleep(_backoff(i))
    raise RuntimeError(f"SCHWAB_GET_FAIL({tag}) {last}")

def _osi_from_instrument(ins: dict) -> str | None:
    sym = (ins.get("symbol") or "")
    try: return to_osi(sym)
    except Exception: pass
    exp = ins.get("optionExpirationDate") or ins.get("expirationDate") or ""
    pc  = (ins.get("putCall") or ins.get("type") or "").upper()
    strike = ins.get("strikePrice") or ins.get("strike")
    try:
        if exp and pc in ("CALL","PUT") and strike is not None:
            ymd = date.fromisoformat(str(exp)[:10]).strftime("%y%m%d")
            cp = "C" if pc.startswith("C") else "P"
            mills = int(round(float(strike)*1000))
            return "{:<6s}{}{}{:08d}".format("SPXW", ymd, cp, mills)
    except Exception: pass
    return None

def positions_map(c, acct_hash: str):
    url=f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}"
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
            elif isinstance(x,list): stack.extend(x)
        return acct_id, initial, current
    chosen=None
    for a in accs:
        aid, init, curr = hunt(a)
        if acct_number and aid == acct_number: chosen=(init,curr); break
        if chosen is None: chosen=(init,curr)
    if not chosen: return None
    init, curr = chosen
    oc = pick(init,"cashBalance","cashAvailableForTrading","liquidationValue")
    if oc is None: oc = pick(curr,"cashBalance","cashAvailableForTrading","liquidationValue")
    return oc

def _round_half_up(x: float) -> int:
    return int(Decimal(x).quantize(Decimal('1'), rounding=ROUND_HALF_UP))

def calc_short_ic_width(opening_cash: float | int) -> int:
    width = max(CREDIT_MIN_WIDTH, int(CREDIT_SPREAD_WIDTH))
    return int(math.ceil(width / 5.0) * 5)

def calc_short_ic_contracts(opening_cash: float | int) -> int:
    try:
        oc = float(opening_cash)
    except Exception:
        oc = 0.0
    width = calc_short_ic_width(opening_cash)
    base = CREDIT_DOLLARS_PER_CONTRACT if CREDIT_DOLLARS_PER_CONTRACT > 0 else 4000.0
    denom = base * (width / 5.0)
    if denom <= 0:
        denom = 4000.0 * (width / 5.0)
    units = _round_half_up(max(0.0, oc) / denom)
    return max(1, int(units))

def calc_long_ic_contracts(opening_cash: float | int) -> int:
    try:
        oc = float(opening_cash)
    except Exception:
        oc = 0.0
    base = DEBIT_DOLLARS_PER_CONTRACT if DEBIT_DOLLARS_PER_CONTRACT > 0 else 4000.0
    units = math.floor(max(0.0, oc) / base)
    return max(1, int(units))

def condor_units_open(pos_map, legs):
    b1 = max(0.0,  pos_map.get(osi_canon(legs[0]), 0.0))
    b2 = max(0.0,  pos_map.get(osi_canon(legs[3]), 0.0))
    s1 = max(0.0, -pos_map.get(osi_canon(legs[1]), 0.0))
    s2 = max(0.0, -pos_map.get(osi_canon(legs[2]), 0.0))
    return int(min(b1, b2, s1, s2))

def print_guard_snapshot(pos, legs, is_credit, width_used, bypass):
    labels=[("BUY_PUT",legs[0],-1),("SELL_PUT",legs[1],+1),("SELL_CALL",legs[2],+1),("BUY_CALL",legs[3],-1)]
    tag="CREDIT" if is_credit else "DEBIT"
    print(f"ORCH GUARD SNAPSHOT ({tag} width={width_used} BYPASS={'ON' if bypass else 'OFF'}):")
    for name, osi, sign in labels:
        can=osi_canon(osi); cur=pos.get(can,0.0)
        print(f"  {name:10s} {osi}  acct_qty={cur:+g}  sign={sign:+d}")

# GW helpers (same as before)
def _gw_timeout():
    try: return int(os.environ.get("GW_TIMEOUT","30"))
    except: return 30

def _sanitize_token(t: str) -> str:
    t=(t or "").strip().strip('"').strip("'")
    return t.split(None,1)[1] if t.lower().startswith("bearer ") else t

def gw_login_token():
    email=os.environ.get("GW_EMAIL",""); pwd=os.environ.get("GW_PASSWORD","")
    if not (email and pwd): raise RuntimeError("GW_LOGIN_MISSING_CREDS")
    r=requests.post(f"{GW_BASE}/goauth/authenticateFireUser", data={"email":email,"password":pwd}, timeout=_gw_timeout())
    if r.status_code!=200: raise RuntimeError(f"GW_LOGIN_HTTP_{r.status_code}:{(r.text or '')[:180]}")
    j=r.json(); t=j.get("token")
    if not t: raise RuntimeError("GW_LOGIN_NO_TOKEN")
    return t

def gw_get_leocross():
    tok=_sanitize_token(os.environ.get("GW_TOKEN","") or "")
    def hit(t):
        h={"Accept":"application/json","Authorization":f"Bearer {_sanitize_token(t)}","User-Agent":"gw-orchestrator/1.6"}
        return requests.get(f"{GW_BASE.rstrip('/')}/{GW_ENDPOINT.lstrip('/')}", headers=h, timeout=_gw_timeout())
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

def main():
    # Sheets init
    svc=None; sheet_id=None; guard_sheet_id=None
    try:
        sheet_id=os.environ["GSHEET_ID"]
        sa_json=os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
        creds=service_account.Credentials.from_service_account_info(json.loads(sa_json),
            scopes=["https://www.googleapis.com/auth/spreadsheets"])
        svc=gbuild("sheets","v4",credentials=creds)
        guard_sheet_id=ensure_header_and_get_sheetid(svc, sheet_id, GUARD_TAB, GUARD_HEADERS)
    except Exception as e:
        print("ORCH WARN: Sheets init failed — {}".format(str(e)[:200]))
        svc=None

    def log(decision, detail, legs=None, acct_qty=(0,0,0,0), open_units="", rem_qty=""):
        if not svc: return
        bp,sp,sc,bc = (legs or ("","","",""))
        bpq,spq,scq,bcq = acct_qty
        row=[datetime.utcnow().isoformat()+"Z","ORCH","SPX",sig_date if 'sig_date' in locals() else "",
             decision, detail, open_units, rem_qty, bp,sp,sc,bc, bpq,spq,scq,bcq]
        guard_log(svc, guard_sheet_id, sheet_id, row)

    # Schwab auth
    try:
        app_key=os.environ["SCHWAB_APP_KEY"]; app_secret=os.environ["SCHWAB_APP_SECRET"]; token_json=os.environ["SCHWAB_TOKEN_JSON"]
        with open("schwab_token.json","w") as f: f.write(token_json)
        c=client_from_token_file(api_key=app_key, app_secret=app_secret, token_path="schwab_token.json")
        acct_num, acct_hash = get_primary_acct(c)
    except Exception as e:
        reason="SCHWAB_CLIENT_INIT_FAILED — " + str(e)[:200]
        print("ORCH ABORT:", reason); log("ABORT", reason); return 1

    # Warm read
    try:
        os.environ["GW_TIMEOUT"] = str(GW_WARM_TIMEOUT)
        api=gw_get_leocross(); tr=extract_trade(api)
        if not tr: print("ORCH SKIP: NO_TRADE_PAYLOAD"); log("SKIP","NO_TRADE_PAYLOAD"); return 0
    except Exception as e:
        reason="GW_FETCH_FAILED — {}".format(str(e)[:200])
        print("ORCH ABORT:", reason); log("ABORT", reason); return 1

    sig_date=str(tr.get("Date","")); exp_iso=str(tr.get("TDate","")); exp6=yymmdd(exp_iso)
    inner_put=int(float(tr.get("Limit"))); inner_call=int(float(tr.get("CLimit")))
    def fnum(x):
        try: return float(x)
        except: return None
    cat1=fnum(tr.get("Cat1")); cat2=fnum(tr.get("Cat2"))
    is_credit = True if (cat2 is None or cat1 is None or cat2>=cat1) else False

    # Opening cash for width (allow manual override for testing)
    oc_override_raw = os.environ.get("SIZING_DOLLARS_OVERRIDE", "").strip()
    oc_override = None
    if oc_override_raw:
        try:
            oc_override = float(oc_override_raw)
        except Exception:
            oc_override = None
    oc_real = opening_cash_for_account(c, acct_num)
    oc = oc_override if (oc_override is not None and oc_override > 0) else oc_real
    if oc is None:
        reason="OPENING_CASH_UNAVAILABLE — aborting to avoid wrong size/width."
        print("ORCH ABORT:", reason); log("ABORT", reason); return 1

    # SHORT: use configured width (default now 20). LONG: keep your original 5-wide.
    width = calc_short_ic_width(oc) if is_credit else 5

    # legs (unoriented base strikes)
    if is_credit:
        # PUSHED-OUT SHORTS by 5; wings at ± width from those shorts
        sell_put  = inner_put  - 5
        buy_put   = sell_put   - width
        sell_call = inner_call + 5
        buy_call  = sell_call  + width
        p_low, p_high = buy_put, sell_put
        c_low, c_high = sell_call, buy_call
    else:
        # LONG path unchanged (your original behavior)
        p_low, p_high = inner_put - width, inner_put
        c_low, c_high = inner_call, inner_call + width
    bp = to_osi(f".SPXW{exp6}P{p_low}"); sp = to_osi(f".SPXW{exp6}P{p_high}")
    sc = to_osi(f".SPXW{exp6}C{c_low}"); bc = to_osi(f".SPXW{exp6}C{c_high}")

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

    # Optional gate (hold + cutoff)
    now = datetime.now(ET)
    cutoff_dt = None
    if HARD_CUTOFF_HHMM:
        try:
            hh, mm = HARD_CUTOFF_HHMM.split(":")
            cutoff_dt = now.replace(hour=int(hh), minute=int(mm), second=0, microsecond=0)
        except Exception:
            print(f"ORCH WARN: invalid HARD_CUTOFF_HHMM='{HARD_CUTOFF_HHMM}' — ignoring cutoff")
            cutoff_dt = None

    cutoff_reason = f"GATE_AFTER_CUTOFF(HARD_CUTOFF_HHMM={HARD_CUTOFF_HHMM})"
    if cutoff_dt and now >= cutoff_dt:
        print("ORCH SKIP:", cutoff_reason)
        log("SKIP", cutoff_reason, legs)
        return 0

    if FAST_HOLD_SECONDS > 0:
        gate = now.replace(hour=16, minute=13, second=0, microsecond=0) + timedelta(seconds=FAST_HOLD_SECONDS)
        if cutoff_dt and gate >= cutoff_dt:
            print("ORCH SKIP:", cutoff_reason)
            log("SKIP", cutoff_reason, legs)
            return 0
        if now < gate:
            wait_s = int((gate - now).total_seconds())
            print(f"ORCH GATE sleep {wait_s}s (FAST_HOLD_SECONDS={FAST_HOLD_SECONDS})")
            time.sleep((gate - now).total_seconds())
            now = datetime.now(ET)
            if cutoff_dt and now >= cutoff_dt:
                print("ORCH SKIP:", cutoff_reason)
                log("SKIP", cutoff_reason, legs)
                return 0
        else:
            print("ORCH GATE immediate (already past hold window)")
    else:
        print("ORCH GATE disabled (FAST_HOLD_SECONDS<=0)")

    # quick refresh (low latency)
    try:
        os.environ["GW_TIMEOUT"] = str(GW_REFRESH_TIMEOUT)
        api2 = gw_get_leocross(); tr2 = extract_trade(api2)
        if tr2:
            inner_put2 = int(float(tr2.get("Limit"))); inner_call2= int(float(tr2.get("CLimit")))
            new_exp_iso = str(tr2.get("TDate",""))
            if (inner_put2 != inner_put) or (inner_call2 != inner_call) or (new_exp_iso!=exp_iso):
                exp_iso = new_exp_iso; exp6 = yymmdd(exp_iso)
                inner_put, inner_call = inner_put2, inner_call2
                if is_credit:
                    sell_put  = inner_put  - 5
                    buy_put   = sell_put   - width
                    sell_call = inner_call + 5
                    buy_call  = sell_call  + width
                    p_low, p_high = buy_put, sell_put
                    c_low, c_high = sell_call, buy_call
                else:
                    p_low, p_high = inner_put - width, inner_put
                    c_low, c_high = inner_call, inner_call + width
                bp = to_osi(f".SPXW{exp6}P{p_low}"); sp = to_osi(f".SPXW{exp6}P{p_high}")
                sc = to_osi(f".SPXW{exp6}C{c_low}"); bc = to_osi(f".SPXW{exp6}C{c_high}")
                legs = orient(bp,sp,sc,bc)
    except Exception: pass

    # Positions + checks
    try: pos = positions_map(c, acct_hash)
    except Exception as e:
        reason="POSITIONS_FAILED — {}".format(str(e)[:200])
        print("ORCH ABORT:", reason); log("ABORT", reason, legs); return 1

    print("ORCH START RUN_ID={} SHA={}".format(os.environ.get("GITHUB_RUN_ID",""), os.environ.get("GITHUB_SHA","")[:7]))
    print_guard_snapshot(pos, legs, is_credit, width, BYPASS_GUARD)

    bpq = pos.get(osi_canon(legs[0]), 0.0); spq = pos.get(osi_canon(legs[1]), 0.0)
    scq = pos.get(osi_canon(legs[2]), 0.0); bcq = pos.get(osi_canon(legs[3]), 0.0)
    acct_snapshot=(bpq,spq,scq,bcq)

    checks=[("BUY",legs[0],-1),("SELL",legs[1],+1),("SELL",legs[2],+1),("BUY",legs[3],-1)]
    if not BYPASS_GUARD:
        for _, osi, sign in checks:
            cur = pos.get(osi_canon(osi), 0.0)
            if (sign<0 and cur<0) or (sign>0 and cur>0):
                details=f"WOULD_CLOSE {osi} acct_qty={cur:+g}"
                print("ORCH SKIP:", details); log("SKIP", details, legs, acct_snapshot, "", ""); return 0
        nonzero = sum(1 for _, osi, _ in checks if abs(pos.get(osi_canon(osi),0.0))>1e-9)
        aligned = sum(1 for _, osi, sign in checks
                    if ((sign<0 and pos.get(osi_canon(osi),0.0)>=0) or
                        (sign>0 and pos.get(osi_canon(osi),0.0)<=0)) and
                        abs(pos.get(osi_canon(osi),0.0))>1e-9)
        if 0 < nonzero < 4:
            present = ["{} {} acct_qty={:+g}".format(l, o, pos.get(osi_canon(o),0.0))
                       for (l,o,_) in checks if abs(pos.get(osi_canon(o),0.0))>1e-9]
            details="PARTIAL_OVERLAP — " + "; ".join(present)
            print("ORCH SKIP:", details); log("SKIP", details, legs, acct_snapshot, "", ""); return 0

    target_units = calc_short_ic_contracts(oc) if is_credit else calc_long_ic_contracts(oc)
    if BYPASS_GUARD:
        try: rem_qty = int(BYPASS_QTY) if BYPASS_QTY else int(target_units)
        except Exception: rem_qty = int(target_units)
        decision="ALLOW_BYPASS"; detail=f"BYPASS_GUARD=1 width={width} open_cash={oc:.2f} target={target_units} rem_qty={rem_qty}"
        units_open=0
    else:
        def condor_units_open(pos_map, legs):
            b1 = max(0.0,  pos_map.get(osi_canon(legs[0]), 0.0))
            b2 = max(0.0,  pos_map.get(osi_canon(legs[3]), 0.0))
            s1 = max(0.0, -pos_map.get(osi_canon(legs[1]), 0.0))
            s2 = max(0.0, -pos_map.get(osi_canon(legs[2]), 0.0))
            return int(min(b1, b2, s1, s2))
        units_open = condor_units_open(pos, legs)
        rem_qty = max(0, target_units - units_open)
        decision = ("ALLOW" if rem_qty>0 else "SKIP")
        detail = (
            f"short_ic width={width} open_cash={oc:.2f} target={target_units} units_open={units_open} rem_qty={rem_qty}"
            if is_credit
            else
            f"long_ic width=5 open_cash={oc:.2f} dollars_per={DEBIT_DOLLARS_PER_CONTRACT} target={target_units} units_open={units_open} rem_qty={rem_qty}"
        )

    print("ORCH SIZE", detail); log(decision, detail, legs, acct_snapshot, (0 if BYPASS_GUARD else units_open), rem_qty)
    if rem_qty == 0: print("ORCH SKIP: At/above target; no remainder to place."); return 0

    env = dict(os.environ)
    env["QTY_OVERRIDE"] = str(rem_qty)
    env["PLACER_MODE"]  = "MANUAL"
    env["VERBOSE"]      = env.get("VERBOSE","1")
    print(f"ORCH CALL → PLACER QTY_OVERRIDE={env['QTY_OVERRIDE']} MODE={env['PLACER_MODE']}")
    rc = os.spawnve(os.P_WAIT, sys.executable, [sys.executable, "scripts/trade/leocross_place_simple.py"], env)
    return rc

if __name__ == "__main__":
    sys.exit(main())
