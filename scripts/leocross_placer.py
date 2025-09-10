#!/usr/bin/env python3
# LeoCross PLACER (bounded loop): place or reprice until target units are open or timeout.
# Prevent oversizing: remainder = target - condor_units_open(positions) every loop.

import os, sys, json, time, re, signal
from datetime import datetime, timezone, date
from zoneinfo import ZoneInfo
import requests
from schwab.auth import client_from_token_file

ET = ZoneInfo("America/New_York")

# --- config from env (read-only) ---
IS_CREDIT      = (os.environ.get("IS_CREDIT","true").lower() == "true")
QTY_TARGET     = int(os.environ.get("QTY_TARGET","4") or "4")
QTY_OVERRIDE   = os.environ.get("QTY_OVERRIDE","")
OPEN_IDS_INIT  = [x for x in (os.environ.get("OPEN_ORDER_IDS","") or "").split(",") if x.strip()]
LEGS           = json.loads(os.environ.get("LEGS_JSON","[]") or "[]")
CANON_KEY      = os.environ.get("CANON_KEY","")

TICK           = float(os.environ.get("TICK","0.05") or "0.05")
MAX_SEC        = int(os.environ.get("PLACER_MAX_SEC","240") or "240")
SLEEP_SEC      = float(os.environ.get("PLACER_SLEEP_SEC","10") or "10")
EDGE           = float(os.environ.get("PLACER_EDGE","0.05") or "0.05")   # nudge off mid
MIN_CREDIT     = float(os.environ.get("MIN_CREDIT","0.10") or "0.10")
# Initial “reprice only” hint from orchestrator; may change at runtime:
REPRICE_MODE_INIT = (os.environ.get("REPRICE_ONLY","0").strip() in ("1","true","yes","y"))

# --- Schwab client ---
def schwab_client():
    app_key=os.environ["SCHWAB_APP_KEY"]; app_secret=os.environ["SCHWAB_APP_SECRET"]; token_json=os.environ["SCHWAB_TOKEN_JSON"]
    with open("schwab_token.json","w") as f: f.write(token_json)
    c=client_from_token_file(api_key=app_key, app_secret=app_secret, token_path="schwab_token.json")
    r=c.get_account_numbers(); r.raise_for_status()
    acct_hash=r.json()[0]["hashValue"]
    return c, acct_hash

def _backoff(i): return 0.6*(2**i)

def schwab_get_json(c, url, params=None, tries=6, tag=""):
    last=""
    for i in range(tries):
        try:
            r=c.session.get(url, params=(params or {}), timeout=20)
            if r.status_code==200: return r.json()
            last=f"HTTP_{r.status_code}:{(r.text or '')[:200]}"
        except Exception as e:
            last=f"{type(e).__name__}:{e}"
        time.sleep(_backoff(i))
    raise RuntimeError(f"SCHWAB_GET_FAIL({tag}) {last}")

def schwab_post_json(c, url, body, tries=3, tag=""):
    last=""
    for i in range(tries):
        try:
            r=c.session.post(url, json=body, timeout=20)
            if r.status_code in (200,201): return r.json() if r.text else {}
            if r.status_code in (202,): return {}
            last=f"HTTP_{r.status_code}:{(r.text or '')[:240]}"
        except Exception as e:
            last=f"{type(e).__name__}:{e}"
        time.sleep(_backoff(i))
    raise RuntimeError(f"SCHWAB_POST_FAIL({tag}) {last}")

def schwab_put_json(c, url, body, tries=3, tag=""):
    last=""
    for i in range(tries):
        try:
            r=c.session.put(url, json=body, timeout=20)
            if r.status_code in (200,201,204): return {}
            last=f"HTTP_{r.status_code}:{(r.text or '')[:240]}"
        except Exception as e:
            last=f"{type(e).__name__}:{e}"
        time.sleep(_backoff(i))
    raise RuntimeError(f"SCHWAB_PUT_FAIL({tag}) {last}")

def schwab_delete(c, url, tries=3, tag=""):
    last=""
    for i in range(tries):
        try:
            r=c.session.delete(url, timeout=20)
            if r.status_code in (200,201,202,204): return
            last=f"HTTP_{r.status_code}:{(r.text or '')[:240]}"
        except Exception as e:
            last=f"{type(e).__name__}:{e}"
        time.sleep(_backoff(i))
    raise RuntimeError(f"SCHWAB_DELETE_FAIL({tag}) {last}")

# --- symbols / positions / orders ---
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

def osi_canon(osi: str): return (osi[6:12], osi[12], osi[-8:])
def strike_from_osi(osi: str) -> float: return int(osi[-8:]) / 1000.0

def _osi_from_instrument(ins: dict):
    sym = (ins.get("symbol") or "")
    try: return to_osi(sym)
    except: pass
    exp = ins.get("optionExpirationDate") or ins.get("expirationDate") or ""
    pc  = (ins.get("putCall") or ins.get("type") or "").upper()
    strike = ins.get("strikePrice") or ins.get("strike")
    try:
        if exp and pc in ("CALL","PUT") and strike is not None:
            ymd = date.fromisoformat(str(exp)[:10]).strftime("%y%m%d")
            cp = "C" if pc.startswith("C") else "P"
            mills = int(round(float(strike)*1000))
            return "{:<6s}{}{}{:08d}".format("SPXW", ymd, cp, mills)
    except: pass
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

def list_matching_open_ids(c, acct_hash: str, canon_set):
    now = datetime.now(ET)
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    url=f"https://api/schwabapi.com/trader/v1/accounts/{acct_hash}/orders"
    arr = schwab_get_json(c, url, params={
        "fromEnteredTime": start.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "toEnteredTime":   now.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "maxResults": 200}, tag="ORDERS") or []
    out=[]
    for o in arr or []:
        st=str(o.get("status") or "").upper()
        if st not in ("WORKING","QUEUED","PENDING_ACTIVATION","OPEN"): continue
        got=set()
        for leg in (o.get("orderLegCollection") or []):
            ins=(leg.get("instrument",{}) or {})
            sym=(ins.get("symbol") or "")
            osi=None
            try: osi=to_osi(sym)
            except: osi=_osi_from_instrument(ins)
            if osi: got.add(osi_canon(osi))
        if got==canon_set:
            oid=str(o.get("orderId") or "")
            if oid: out.append(oid)
    return out

# --- condor math ---
def condor_units_open(pos_map, legs):
    b1 = max(0.0,  pos_map.get(osi_canon(legs[0]), 0.0))
    b2 = max(0.0,  pos_map.get(osi_canon(legs[3]), 0.0))
    s1 = max(0.0, -pos_map.get(osi_canon(legs[1]), 0.0))
    s2 = max(0.0, -pos_map.get(osi_canon(legs[2]), 0.0))
    return int(min(b1, b2, s1, s2))

# --- quotes & pricing ---
def get_leg_mids(c, symbols):
    url="https://api.schwabapi.com/marketdata/v1/quotes"
    try:
        j = schwab_get_json(c, url, params={"symbols": ",".join(symbols)}, tag="QUOTES") or {}
        mids={}
        for sym in symbols:
            q = j.get(sym) or {}
            bid = float(q.get("bidPrice") or q.get("bid") or 0)
            ask = float(q.get("askPrice") or q.get("ask") or 0)
            if bid>0 and ask>0:
                mids[sym] = (bid+ask)/2.0
        return mids
    except Exception:
        return {}

def round_tick(x: float, tick: float) -> float:
    return round(x / tick) * tick

def compute_net_price(c, legs, is_credit: bool, edge: float, tick: float) -> float:
    mids = get_leg_mids(c, legs)
    bput = mids.get(legs[0]); sput = mids.get(legs[1]); scall = mids.get(legs[2]); bcall = mids.get(legs[3])
    if all(x is not None for x in (bput,sput,scall,bcall)):
        net = (sput + scall) - (bput + bcall)
        if not is_credit: net = -net  # positive debit for debit condor
        px = (net - edge) if is_credit else (net + edge)
        px = max(MIN_CREDIT, px) if is_credit else max(0.05, px)
        return float(f"{round_tick(px, tick):.2f}")
    # Fallback if quotes missing
    return float(f"{(0.75 if is_credit else 1.25):.2f}")

# --- order builders ---
def build_condor_order(legs, qty: int, is_credit: bool, price: float):
    if is_credit:
        instr = ["BUY_TO_OPEN","SELL_TO_OPEN","SELL_TO_OPEN","BUY_TO_OPEN"]
        orderType = "NET_CREDIT"
    else:
        instr = ["SELL_TO_OPEN","BUY_TO_OPEN","BUY_TO_OPEN","SELL_TO_OPEN"]
        orderType = "NET_DEBIT"
    olc=[{"instruction": instr[i], "quantity": qty, "instrument": {"symbol": legs[i], "assetType":"OPTION"}}
         for i in range(4)]
    return {
        "orderType": orderType,
        "session": "NORMAL",
        "price": f"{price:.2f}",
        "duration": "DAY",
        "orderStrategyType": "SINGLE",
        "complexOrderStrategyType": "IRON_CONDOR",
        "orderLegCollection": olc
    }

def place_order(c, acct_hash, legs, qty, is_credit, price):
    url=f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders"
    return schwab_post_json(c, url, build_condor_order(legs, qty, is_credit, price), tag="PLACE")

def replace_order(c, acct_hash, order_id, legs, qty, is_credit, price):
    url=f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{order_id}"
    return schwab_put_json(c, url, build_condor_order(legs, qty, is_credit, price), tag=f"REPLACE_{order_id}")

# --- signal handling ---
def _term(signum, frame):
    print(f"PLACER TERM: signal {signum}, exiting.")
    sys.exit(130)

signal.signal(signal.SIGTERM, _term)

# --- main loop ---
def main():
    if len(LEGS) != 4:
        print("PLACER ABORT: LEGS_JSON missing or invalid.")
        return 1

    try:
        c, acct_hash = schwab_client()
    except Exception as e:
        msg=str(e)
        if ("unsupported_token_type" in msg) or ("refresh_token_authentication_error" in msg):
            print("PLACER ABORT: SCHWAB_OAUTH_REFRESH_FAILED — rotate SCHWAB_TOKEN_JSON")
        else:
            print("PLACER ABORT: SCHWAB_CLIENT_INIT_FAILED —", msg[:200])
        return 1

    canon = {osi_canon(x) for x in LEGS}
    start_ts = time.time()
    last_price = None
    working_ids = OPEN_IDS_INIT[:]

    # *** KEY FIX: runtime flag that we can flip mid-run ***
    reprice_mode = bool(REPRICE_MODE_INIT)

    # Use guard's remainder only for the first PLACE, then always recompute
    first_rem_override = None
    if not reprice_mode and QTY_OVERRIDE.strip().isdigit():
        first_rem_override = int(QTY_OVERRIDE.strip())

    cycle = 0
    while True:
        cycle += 1
        if (time.time() - start_ts) > MAX_SEC:
            print("PLACER ABORT: DEADLINE_REACHED")
            return 1

        pos = positions_map(c, acct_hash)
        units_open = condor_units_open(pos, LEGS)
        rem = max(0, QTY_TARGET - units_open)

        # Always refresh working ids
        working_ids = list_matching_open_ids(c, acct_hash, canon)

        print(f"PLACER LOOP#{cycle}: units_open={units_open} target={QTY_TARGET} rem={rem} working_ids={','.join(working_ids) or '-'} mode={'REPRICE' if reprice_mode else 'NEW'}")

        if rem == 0:
            print("PLACER DONE: target reached.")
            return 0

        # If any working order exists, from now on we operate in repricing mode
        if working_ids:
            reprice_mode = True

        # --- REPRICE MODE ---
        if reprice_mode:
            # Compute an evolving target price and reprice existing orders
            px_base = compute_net_price(c, LEGS, IS_CREDIT, EDGE, TICK)
            # Walk toward market each loop (one tick per loop)
            adj = TICK * max(1, cycle)
            px = max(MIN_CREDIT, px_base - adj) if IS_CREDIT else (px_base + adj)
            px = float(f"{round_tick(px, TICK):.2f}")

            # If we somehow lost the working order (broker canceled), place the remainder anew
            if not working_ids:
                print(f"PLACER ACTION: PLACE remainder qty={rem} price={px:.2f} ({'credit' if IS_CREDIT else 'debit'})")
                try:
                    place_order(c, acct_hash, LEGS, rem, IS_CREDIT, px)
                except Exception as e:
                    print("PLACER WARN: PLACE failed —", str(e)[:200])
            else:
                if (last_price is None) or (abs(px - last_price) >= TICK/2):
                    for oid in working_ids:
                        print(f"PLACER ACTION: REPLACE order_id={oid} qty={rem} price={px:.2f}")
                        try:
                            replace_order(c, acct_hash, oid, LEGS, rem, IS_CREDIT, px)
                        except Exception as e:
                            print(f"PLACER WARN: REPLACE {oid} failed —", str(e)[:200])
                    last_price = px

            time.sleep(SLEEP_SEC)
            continue

        # --- NEW MODE ---
        # No working orders; submit an initial order
        qty = first_rem_override if first_rem_override is not None else rem
        first_rem_override = None
        px = compute_net_price(c, LEGS, IS_CREDIT, EDGE, TICK)
        last_price = px
        print(f"PLACER ACTION: PLACE qty={qty} price={px:.2f} ({'credit' if IS_CREDIT else 'debit'})")
        try:
            place_order(c, acct_hash, LEGS, qty, IS_CREDIT, px)
        except Exception as e:
            print("PLACER WARN: PLACE failed —", str(e)[:200])

        time.sleep(SLEEP_SEC)

if __name__ == "__main__":
    try:
        sys.exit(main())
    except SystemExit:
        raise
    except Exception as e:
        print("PLACER ABORT (unhandled):", str(e)[:300])
        sys.exit(1)
