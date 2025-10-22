#!/usr/bin/env python3
# PLACER — supports CREDIT (short IC) and DEBIT (long IC). Same‑shorts only.
# Minimal ladder + 429 backoff + cancel/settle waits.

import os, sys, time, random
from schwab.auth import client_from_token_file

TICK = 0.05

def clamp_tick(x: float) -> float:
    return round(round(float(x) / TICK) * TICK + 1e-12, 2)

def schwab_client():
    app_key=os.environ["SCHWAB_APP_KEY"]; app_secret=os.environ["SCHWAB_APP_SECRET"]; token_json=os.environ["SCHWAB_TOKEN_JSON"]
    with open("schwab_token.json","w") as f: f.write(token_json)
    c=client_from_token_file(api_key=app_key, app_secret=app_secret, token_path="schwab_token.json")
    return c

def fetch_bid_ask(c, osi: str):
    r=c.get_quote(osi)
    if r.status_code!=200: return (None,None)
    d=list(r.json().values())[0] if isinstance(r.json(), dict) else {}
    q=d.get("quote", d)
    b=q.get("bidPrice") or q.get("bid") or q.get("bidPriceInDouble")
    a=q.get("askPrice") or q.get("ask") or q.get("askPriceInDouble")
    return (float(b) if b is not None else None, float(a) if a is not None else None)

def condor_nbbo_credit(c, legs):
    # short IC credit = (shorts bid) - (longs ask)
    bp, sp, sc, bc = legs
    bp_b, bp_a = fetch_bid_ask(c, bp); sp_b, sp_a = fetch_bid_ask(c, sp)
    sc_b, sc_a = fetch_bid_ask(c, sc); bc_b, bc_a = fetch_bid_ask(c, bc)
    if None in (bp_b, bp_a, sp_b, sp_a, sc_b, sc_a, bc_b, bc_a): return (None,None,None)
    bid = (sp_b + sc_b) - (bp_a + bc_a)
    ask = (sp_a + sc_a) - (bp_b + bc_b)
    mid = (bid + ask) / 2.0
    return (clamp_tick(bid), clamp_tick(ask), clamp_tick(mid))

def condor_nbbo_debit(c, legs):
    # long IC debit = (longs ask) - (shorts bid)
    bp, sp, sc, bc = legs
    bp_b, bp_a = fetch_bid_ask(c, bp); sp_b, sp_a = fetch_bid_ask(c, sp)
    sc_b, sc_a = fetch_bid_ask(c, sc); bc_b, bc_a = fetch_bid_ask(c, bc)
    if None in (bp_b, bp_a, sp_b, sp_a, sc_b, sc_a, bc_b, bc_a): return (None,None,None)
    bid = (bp_b + bc_b) - (sp_a + sc_a)   # what you could sell the long IC for
    ask = (bp_a + bc_a) - (sp_b + sc_b)   # what you pay to buy it
    mid = (bid + ask) / 2.0
    return (clamp_tick(bid), clamp_tick(ask), clamp_tick(mid))

def order_payload_credit(legs, price, qty):
    return {
        "orderType": "NET_CREDIT",
        "session": "NORMAL",
        "price": f"{clamp_tick(price):.2f}",
        "duration": "DAY",
        "orderStrategyType": "SINGLE",
        "complexOrderStrategyType": "IRON_CONDOR",
        "orderLegCollection":[
            {"instruction":"BUY_TO_OPEN", "positionEffect":"OPENING","quantity":qty,"instrument":{"symbol":legs[0],"assetType":"OPTION"}},
            {"instruction":"SELL_TO_OPEN","positionEffect":"OPENING","quantity":qty,"instrument":{"symbol":legs[1],"assetType":"OPTION"}},
            {"instruction":"SELL_TO_OPEN","positionEffect":"OPENING","quantity":qty,"instrument":{"symbol":legs[2],"assetType":"OPTION"}},
            {"instruction":"BUY_TO_OPEN", "positionEffect":"OPENING","quantity":qty,"instrument":{"symbol":legs[3],"assetType":"OPTION"}},
        ]
    }

def order_payload_debit(legs, price, qty):
    # Legs are already oriented BUY_PUT, SELL_PUT, SELL_CALL, BUY_CALL for long IC
    return {
        "orderType": "NET_DEBIT",
        "session": "NORMAL",
        "price": f"{clamp_tick(price):.2f}",
        "duration": "DAY",
        "orderStrategyType": "SINGLE",
        "complexOrderStrategyType": "IRON_CONDOR",
        "orderLegCollection":[
            {"instruction":"BUY_TO_OPEN", "positionEffect":"OPENING","quantity":qty,"instrument":{"symbol":legs[0],"assetType":"OPTION"}},
            {"instruction":"SELL_TO_OPEN","positionEffect":"OPENING","quantity":qty,"instrument":{"symbol":legs[1],"assetType":"OPTION"}},
            {"instruction":"SELL_TO_OPEN","positionEffect":"OPENING","quantity":qty,"instrument":{"symbol":legs[2],"assetType":"OPTION"}},
            {"instruction":"BUY_TO_OPEN", "positionEffect":"OPENING","quantity":qty,"instrument":{"symbol":legs[3],"assetType":"OPTION"}},
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
        if r.status_code in (200,201,202):
            return r
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
        if r.status_code in (200,201,202,204):
            return True
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

def main():
    side = (os.environ.get("SIDE","CREDIT") or "CREDIT").upper()
    legs = [
        os.environ["OCC_BUY_PUT"],
        os.environ["OCC_SELL_PUT"],
        os.environ["OCC_SELL_CALL"],
        os.environ["OCC_BUY_CALL"],
    ]
    qty   = max(1, int(os.environ.get("QTY","1")))
    width = int(os.environ.get("WIDTH","20"))

    c=schwab_client()
    r=c.get_account_numbers(); r.raise_for_status()
    acct_hash = str(r.json()[0]["hashValue"])

    STEP_WAIT         = float(os.environ.get("STEP_WAIT_CREDIT","5" if side=="CREDIT" else "5"))
    MIN_RUNG_WAIT     = max(1.0, float(os.environ.get("MIN_RUNG_WAIT","5")))
    CANCEL_SETTLE_SECS= float(os.environ.get("CANCEL_SETTLE_SECS","1.0"))
    MAX_CYCLES        = int(os.environ.get("MAX_LADDER_CYCLES","2"))

    print(f"PLACER START side={side} width={width} qty={qty}")

    if side == "CREDIT":
        bid, ask, mid = condor_nbbo_credit(c, legs)
        print(f"CREDIT NBBO: bid={bid} ask={ask} mid={mid}")
        if (bid,ask,mid) == (None,None,None):
            print("WARN: no NBBO — abort"); return 0
        base = []
        if ask is not None: base.append(ask)
        if mid is not None:
            base += [mid, clamp_tick(mid-0.05), clamp_tick(mid-0.10)]
            if bid is not None:
                base[-2] = max(base[-2], bid)
                base[-1] = max(base[-1], bid)
        ladder=[]; seen=set()
        for p in base:
            p=clamp_tick(p)
            if p not in seen: seen.add(p); ladder.append(p)
        make_payload = order_payload_credit
    else:
        bid, ask, mid = condor_nbbo_debit(c, legs)
        print(f"DEBIT NBBO: bid={bid} ask={ask} mid={mid}")
        if (bid,ask,mid) == (None,None,None):
            print("WARN: no NBBO — abort"); return 0
        base = []
        if bid is not None: base.append(bid)                 # try best case first
        if mid is not None: base += [mid, clamp_tick(mid+0.05), clamp_tick(mid+0.10)]
        if ask is not None: base.append(ask)
        ladder=[]; seen=set()
        for p in base:
            p=clamp_tick(p)
            if p not in seen: seen.add(p); ladder.append(p)
        make_payload = order_payload_debit

    filled=0
    active_oid=None
    url_post = f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders"

    # ladder cycles
    for _ in range(MAX_CYCLES):
        print(f"CYCLE ladder: {ladder}")
        for price in ladder:
            to_place = max(0, qty - filled)
            if to_place==0: break

            # cancel previous rung if any
            if active_oid:
                url_del=f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{active_oid}"
                ok = delete_with_retry(c, url_del, tag=f"CANCEL {active_oid}")
                print(f"CANCEL {active_oid} → {'HTTP_200' if ok else 'FAIL'}")
                wait_settle(c, acct_hash, active_oid, max_wait=CANCEL_SETTLE_SECS)
                active_oid=None
                time.sleep(0.20)

            print(f"RUNG → price={price:.2f} to_place={to_place}")
            payload = make_payload(legs, price, to_place)
            try:
                r = post_with_retry(c, url_post, payload, tag=f"PLACE@{price:.2f}x{to_place}")
                active_oid = parse_order_id(r)
            except Exception as e:
                print(str(e)); continue

            # dwell + poll
            t_end = time.time() + max(STEP_WAIT, MIN_RUNG_WAIT)
            while time.time() < t_end:
                st = get_status(c, acct_hash, active_oid)
                s  = str(st.get("status") or st.get("orderStatus") or "").upper()
                fq = int(round(float(st.get("filledQuantity") or st.get("filled_quantity") or 0)))
                if fq > filled: filled=fq
                if s == "FILLED" or filled >= qty:
                    break
                time.sleep(0.25)

            if filled >= qty:
                break

        if filled >= qty:
            break
        # refresh NBBO between cycles
        if side == "CREDIT":
            bid, ask, mid = condor_nbbo_credit(c, legs)
        else:
            bid, ask, mid = condor_nbbo_debit(c, legs)

    # final cleanup
    if active_oid:
        url_del=f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{active_oid}"
        ok = delete_with_retry(c, url_del, tag=f"CANCEL {active_oid}")
        print(f"CANCEL {active_oid} → {'HTTP_200' if ok else 'FAIL'}")
        wait_settle(c, acct_hash, active_oid, max_wait=CANCEL_SETTLE_SECS)

    print("PLACER DONE")
    return 0

if __name__=="__main__":
    sys.exit(main())
