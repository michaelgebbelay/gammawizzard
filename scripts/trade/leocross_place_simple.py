#!/usr/bin/env python3
# PLACER — CREDIT/DEBIT, asymmetric wings, supports CALL_MULT (ratio condor).
# Uses CUSTOM multi‑leg to allow non‑equal quantities.

import os, sys, time, random
from schwab.auth import client_from_token_file

TICK = 0.05

def clamp_tick(x: float) -> float:
    return round(round(float(x) / TICK) * TICK + 1e-12, 2)

def schwab_client():
    app_key=os.environ["SCHWAB_APP_KEY"]; app_secret=os.environ["SCHWAB_APP_SECRET"]; token_json=os.environ["SCHWAB_TOKEN_JSON"]
    with open("schwab_token.json","w") as f: f.write(token_json)
    return client_from_token_file(api_key=app_key, app_secret=app_secret, token_path="schwab_token.json")

def fetch_bid_ask(c, osi: str):
    r=c.get_quote(osi)
    if r.status_code!=200: return (None,None)
    d=list(r.json().values())[0] if isinstance(r.json(), dict) else {}
    q=d.get("quote", d)
    b=q.get("bidPrice") or q.get("bid") or q.get("bidPriceInDouble")
    a=q.get("askPrice") or q.get("ask") or q.get("askPriceInDouble")
    return (float(b) if b is not None else None, float(a) if a is not None else None)

def condor_nbbo(c, legs, m:int):
    bp, sp, sc, bc = legs
    bp_b, bp_a = fetch_bid_ask(c, bp); sp_b, sp_a = fetch_bid_ask(c, sp)
    sc_b, sc_a = fetch_bid_ask(c, sc); bc_b, bc_a = fetch_bid_ask(c, bc)
    if None in (bp_b, bp_a, sp_b, sp_a, sc_b, sc_a, bc_b, bc_a):
        return (None, None, None)
    if SIDE == "CREDIT":
        bid = (sp_b - bp_a) + m*(sc_b - bc_a)
        ask = (sp_a - bp_b) + m*(sc_a - bc_b)
        mid = (bid + ask)/2.0
    else:
        # debit: pay for wings (opposite signs)
        bid = (bp_b - sp_a) + m*(bc_b - sc_a)   # value if selling package now
        ask = (bp_a - sp_b) + m*(bc_a - sc_b)   # what we pay to buy now
        mid = (bid + ask)/2.0
    return (clamp_tick(bid), clamp_tick(ask), clamp_tick(mid))

def order_payload(legs, price, q_put, q_call, *, side: str):
    bp, sp, sc, bc = legs
    ot = "NET_CREDIT" if side=="CREDIT" else "NET_DEBIT"
    return {
        "orderType": ot,
        "session": "NORMAL",
        "price": f"{clamp_tick(price):.2f}",
        "duration": "DAY",
        "orderStrategyType": "SINGLE",
        "complexOrderStrategyType": "CUSTOM",  # allow ratio/non‑standard
        "orderLegCollection":[
            {"instruction":"BUY_TO_OPEN" if side=="CREDIT" else "SELL_TO_OPEN", "positionEffect":"OPENING","quantity":q_put,  "instrument":{"symbol":bp,"assetType":"OPTION"}},
            {"instruction":"SELL_TO_OPEN" if side=="CREDIT" else "BUY_TO_OPEN", "positionEffect":"OPENING","quantity":q_put,  "instrument":{"symbol":sp,"assetType":"OPTION"}},
            {"instruction":"SELL_TO_OPEN" if side=="CREDIT" else "BUY_TO_OPEN", "positionEffect":"OPENING","quantity":q_call, "instrument":{"symbol":sc,"assetType":"OPTION"}},
            {"instruction":"BUY_TO_OPEN" if side=="CREDIT" else "SELL_TO_OPEN", "positionEffect":"OPENING","quantity":q_call, "instrument":{"symbol":bc,"assetType":"OPTION"}},
        ]
    }

def parse_order_id(r):
    try:
        j=r.json()
        if isinstance(j,dict):
            oid = j.get("orderId") or j.get("order_id")
            if oid: return str(oid)
    except Exception:
        pass
    loc=r.headers.get("Location","")
    return loc.rstrip("/").split("/")[-1] if loc else ""

def post_with_retry(c, url, payload, tag="", tries=6):
    last=""
    for i in range(tries):
        r = c.session.post(url, json=payload, timeout=20)
        if r.status_code in (200,201,202): return r
        if r.status_code == 429:
            wait = min(12.0, 0.6 * (2**i)) + random.uniform(0.0, 0.3)
            print(f"WARN: place failed — HTTP_429 — backoff {wait:.2f}s")
            time.sleep(wait); continue
        last = f"HTTP_{r.status_code}:{(r.text or '')[:200]}"
        time.sleep(min(6.0, 0.4 * (2**i)))
    raise RuntimeError(f"POST_FAIL({tag}) {last or 'unknown'}")

def delete_with_retry(c, url, tag="", tries=6):
    for i in range(tries):
        r = c.session.delete(url, timeout=20)
        if r.status_code in (200,201,202,204): return True
        if r.status_code == 429:
            wait = min(8.0, 0.5 * (2**i)) + random.uniform(0.0, 0.25)
            time.sleep(wait); continue
        time.sleep(min(4.0, 0.3 * (2**i)))
    return False

def get_status(c, acct_hash: str, oid: str):
    url=f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{oid}"
    try:
        r=c.session.get(url, timeout=20)
        if r.status_code!=200: return {}
        return r.json() or {}
    except Exception:
        return {}

def wait_settle(c, acct_hash: str, oid: str, max_wait=2.0):
    t_end=time.time()+max_wait
    while time.time()<t_end:
        st = get_status(c, acct_hash, oid)
        s = str(st.get("status") or st.get("orderStatus") or "").upper()
        if (not s) or s in {"CANCELED","FILLED","REJECTED","EXPIRED"}:
            return True
        time.sleep(0.15)
    return False

if __name__=="__main__":
    SIDE = (os.environ.get("SIDE","CREDIT") or "CREDIT").upper()
    legs = [
        os.environ["OCC_BUY_PUT"],   # bp
        os.environ["OCC_SELL_PUT"],  # sp
        os.environ["OCC_SELL_CALL"], # sc
        os.environ["OCC_BUY_CALL"],  # bc
    ]
    qty_base = max(1, int(os.environ.get("QTY","1")))
    CALL_MULT = max(1, int(os.environ.get("CALL_MULT","1")))

    # Schwab client + acct hash
    app_key=os.environ["SCHWAB_APP_KEY"]; app_secret=os.environ["SCHWAB_APP_SECRET"]; token_json=os.environ["SCHWAB_TOKEN_JSON"]
    with open("schwab_token.json","w") as f: f.write(token_json)
    c=client_from_token_file(api_key=app_key, app_secret=app_secret, token_path="schwab_token.json")
    r=c.get_account_numbers(); r.raise_for_status()
    acct_hash = str(r.json()[0]["hashValue"])

    # Ladder params
    STEP_WAIT = float(os.environ.get("STEP_WAIT_CREDIT","5" if SIDE=="CREDIT" else os.environ.get("STEP_WAIT_DEBIT","5")))
    MIN_RUNG_WAIT = max(1.0, float(os.environ.get("MIN_RUNG_WAIT","5")))
    CANCEL_SETTLE_SECS = float(os.environ.get("CANCEL_SETTLE_SECS","1.0"))
    MAX_CYCLES = int(os.environ.get("MAX_LADDER_CYCLES","2"))

    print(f"PLACER START side={SIDE} structure=CONDOR qty={qty_base}")

    bid, ask, mid = condor_nbbo(c, legs, CALL_MULT)
    label = "CREDIT NBBO (synth)" if SIDE=="CREDIT" else "DEBIT NBBO (synth)"
    print(f"{label}: bid={bid} ask={ask} mid={mid}")
    if mid is None and ask is None:
        print("WARN: no NBBO — abort")
        sys.exit(0)

    # Ladder: ask → mid → mid±ticks (credit: start high; debit: start low)
    ladder=[]
    if SIDE=="CREDIT":
        base = []
        if ask is not None: base.append(ask)
        if mid is not None:
            base += [mid, clamp_tick(mid-0.05), clamp_tick(mid-0.10)]
            if bid is not None:
                base[-2] = max(base[-2], bid)
                base[-1] = max(base[-1], bid)
        seen=set()
        for p in base:
            p = clamp_tick(p)
            if p not in seen:
                seen.add(p); ladder.append(p)
    else:
        # debit: start at bid (cheapest), walk up
        base = []
        if bid is not None: base.append(bid)
        if mid is not None:
            base += [mid, clamp_tick(mid+0.05), clamp_tick(mid+0.10)]
            if ask is not None:
                base[-2] = min(base[-2], ask)
                base[-1] = min(base[-1], ask)
        seen=set()
        for p in base:
            p = clamp_tick(p)
            if p not in seen:
                seen.add(p); ladder.append(p)

    filled=0
    active_oid=None
    q_put = qty_base
    q_call = qty_base * CALL_MULT
    url_post = f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders"

    def post_order(price):
        payload = order_payload(legs, price, q_put, q_call, side=SIDE)
        r = post_with_retry(c, url_post, payload, tag=f"PLACE@{price:.2f}x{q_put}:{q_call}")
        return parse_order_id(r)

    for cycle in range(1, MAX_CYCLES+1):
        print(f"CYCLE ladder: {ladder}")
        for price in ladder:
            if active_oid:
                url_del=f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{active_oid}"
                ok = delete_with_retry(c, url_del, tag=f"CANCEL {active_oid}")
                print(f"CANCEL {active_oid} → {'HTTP_200' if ok else 'FAIL'}")
                wait_settle(c, acct_hash, active_oid, max_wait=CANCEL_SETTLE_SECS)
                active_oid=None
                time.sleep(0.20)

            print(f"RUNG → price={price:.2f} to_place=1")
            try:
                active_oid = post_order(price)
            except Exception as e:
                print(str(e)); continue

            t_end = time.time() + max(STEP_WAIT, MIN_RUNG_WAIT)
            while time.time() < t_end:
                st = get_status(c, acct_hash, active_oid)
                s  = str(st.get("status") or st.get("orderStatus") or "").upper()
                if s == "FILLED":
                    filled=1; break
                time.sleep(0.25)
            if filled>=1: break

        if filled>=1: break
        # refresh nbbo
        bid, ask, mid = condor_nbbo(c, legs, CALL_MULT)

    if active_oid:
        url_del=f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{active_oid}"
        ok = delete_with_retry(c, url_del, tag=f"CANCEL {active_oid}")
        print(f"CANCEL {active_oid} → {'HTTP_200' if ok else 'FAIL'}")
        wait_settle(c, acct_hash, active_oid, max_wait=CANCEL_SETTLE_SECS)

    print("PLACER DONE")
    sys.exit(0)
