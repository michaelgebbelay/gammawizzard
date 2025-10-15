#!/usr/bin/env python3
# PLACER — CREDIT only, same‑shorts. Minimal ladder + 429 backoff + cancel settle waits.

import os, sys, time, math, json, random
from datetime import timezone, datetime
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

def condor_nbbo_credit(c, legs):
    bp, sp, sc, bc = legs
    bp_b, bp_a = fetch_bid_ask(c, bp); sp_b, sp_a = fetch_bid_ask(c, sp)
    sc_b, sc_a = fetch_bid_ask(c, sc); bc_b, bc_a = fetch_bid_ask(c, bc)
    if None in (bp_b, bp_a, sp_b, sp_a, sc_b, sc_a, bc_b, bc_a):
        return (None, None, None)
    credit_bid = (sp_b + sc_b) - (bp_a + bc_a)
    credit_ask = (sp_a + sc_a) - (bp_b + bc_b)
    credit_mid = (credit_bid + credit_ask) / 2.0
    return (clamp_tick(credit_bid), clamp_tick(credit_ask), clamp_tick(credit_mid))

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
            # exponential + small jitter
            wait = min(12.0, 0.6 * (2**i)) + random.uniform(0.0, 0.3)
            print(f"WARN: place failed — HTTP_429 — backoff {wait:.2f}s")
            time.sleep(wait); continue
        last = f"HTTP_{r.status_code}:{(r.text or '')[:200]}"
        # brief wait before retry
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

def place_one_credit(c, legs, price, qty):
    url_post = f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders"
    payload = order_payload_credit(legs, price, qty)
    r = post_with_retry(c, url_post, payload, tag=f"PLACE@{price:.2f}x{qty}")
    return parse_order_id(r)

# ===== Main =====
if __name__=="__main__":
    side = (os.environ.get("SIDE","CREDIT") or "CREDIT").upper()
    if side != "CREDIT":
        print("PLACER ABORT: side must be CREDIT in this minimal placer.")
        sys.exit(1)

    legs = [
        os.environ["OCC_BUY_PUT"],
        os.environ["OCC_SELL_PUT"],
        os.environ["OCC_SELL_CALL"],
        os.environ["OCC_BUY_CALL"],
    ]
    qty = max(1, int(os.environ.get("QTY","1")))
    width = int(os.environ.get("WIDTH","20"))

    # Schwab client + acct hash
    app_key=os.environ["SCHWAB_APP_KEY"]; app_secret=os.environ["SCHWAB_APP_SECRET"]; token_json=os.environ["SCHWAB_TOKEN_JSON"]
    with open("schwab_token.json","w") as f: f.write(token_json)
    c=client_from_token_file(api_key=app_key, app_secret=app_secret, token_path="schwab_token.json")
    r=c.get_account_numbers(); r.raise_for_status()
    acct_hash = str(r.json()[0]["hashValue"])

    # Ladder params
    STEP_WAIT = float(os.environ.get("STEP_WAIT_CREDIT","5"))
    MIN_RUNG_WAIT = max(1.0, float(os.environ.get("MIN_RUNG_WAIT","5")))
    CANCEL_SETTLE_SECS = float(os.environ.get("CANCEL_SETTLE_SECS","1.0"))
    MAX_CYCLES = int(os.environ.get("MAX_LADDER_CYCLES","2"))

    print(f"PLACER START side=CREDIT width={width} qty={qty}")

    # NBBO & ladder
    bid, ask, mid = condor_nbbo_credit(c, legs)
    print(f"CREDIT NBBO: bid={bid} ask={ask} mid={mid}")
    if mid is None and ask is None:
        print("WARN: no NBBO — abort")
        sys.exit(0)

    base_ladder = []
    if ask is not None: base_ladder.append(ask)
    if mid is not None:
        base_ladder += [mid, clamp_tick(mid-0.05), clamp_tick(mid-0.10)]
        if bid is not None:
            base_ladder[-2] = max(base_ladder[-2], bid)
            base_ladder[-1] = max(base_ladder[-1], bid)
    # dedupe
    ladder=[]; seen=set()
    for p in base_ladder:
        p = clamp_tick(p)
        if p not in seen:
            seen.add(p); ladder.append(p)

    filled=0
    active_oid=None

    for cycle in range(1, MAX_CYCLES+1):
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
            try:
                active_oid = place_one_credit(c, legs, price, to_place)
            except Exception as e:
                print(str(e))
                continue

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
        bid, ask, mid = condor_nbbo_credit(c, legs)

    # Final cleanup
    if active_oid:
        url_del=f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{active_oid}"
        ok = delete_with_retry(c, url_del, tag=f"CANCEL {active_oid}")
        print(f"CANCEL {active_oid} → {'HTTP_200' if ok else 'FAIL'}")
        wait_settle(c, acct_hash, active_oid, max_wait=CANCEL_SETTLE_SECS)

    print("PLACER DONE")
    sys.exit(0)
