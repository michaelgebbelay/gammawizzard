#!/usr/bin/env python3
# PLACER — CREDIT/DEBIT. Supports:
#   - Single 4-leg IRON_CONDOR (qty equal) — widths may differ (unbalanced wings)
#   - SPLIT mode: separate VERTICAL PUT and VERTICAL CALL with independent qtys (ratio)
# Minimal ladder + 429 backoff.

import os
import random
import sys
import time

from schwab.auth import client_from_token_file

TICK = 0.05


def clamp_tick(x: float) -> float:
    return round(round(float(x) / TICK) * TICK + 1e-12, 2)


def schwab_client():
    app_key = os.environ["SCHWAB_APP_KEY"]
    app_secret = os.environ["SCHWAB_APP_SECRET"]
    token_json = os.environ["SCHWAB_TOKEN_JSON"]
    with open("schwab_token.json", "w", encoding="utf-8") as f:
        f.write(token_json)
    return client_from_token_file(api_key=app_key, app_secret=app_secret, token_path="schwab_token.json")


def fetch_bid_ask(c, osi: str):
    r = c.get_quote(osi)
    if r.status_code != 200:
        return (None, None)
    data = list(r.json().values())[0] if isinstance(r.json(), dict) else {}
    quote = data.get("quote", data)
    bid = quote.get("bidPrice") or quote.get("bid") or quote.get("bidPriceInDouble")
    ask = quote.get("askPrice") or quote.get("ask") or quote.get("askPriceInDouble")
    return (
        float(bid) if bid is not None else None,
        float(ask) if ask is not None else None,
    )


def vertical_nbbo_credit(c, short_osi, long_osi):
    short_bid, short_ask = fetch_bid_ask(c, short_osi)
    long_bid, long_ask = fetch_bid_ask(c, long_osi)
    if None in (short_bid, short_ask, long_bid, long_ask):
        return (None, None, None)
    bid = short_bid - long_ask
    ask = short_ask - long_bid
    mid = (bid + ask) / 2.0
    return (clamp_tick(bid), clamp_tick(ask), clamp_tick(mid))


def condor_nbbo_credit(c, bp, sp, sc, bc):
    put_bid, put_ask, put_mid = vertical_nbbo_credit(c, sp, bp)
    call_bid, call_ask, call_mid = vertical_nbbo_credit(c, sc, bc)
    if None in (put_bid, put_ask, put_mid, call_bid, call_ask, call_mid):
        return (None, None, None)
    bid = clamp_tick(put_bid + call_bid)
    ask = clamp_tick(put_ask + call_ask)
    mid = clamp_tick(put_mid + call_mid)
    return (bid, ask, mid)


def order_payload_condor_credit(legs, price, qty):
    bp, sp, sc, bc = legs
    return {
        "orderType": "NET_CREDIT",
        "session": "NORMAL",
        "price": f"{clamp_tick(price):.2f}",
        "duration": "DAY",
        "orderStrategyType": "SINGLE",
        "complexOrderStrategyType": "IRON_CONDOR",
        "orderLegCollection": [
            {
                "instruction": "BUY_TO_OPEN",
                "positionEffect": "OPENING",
                "quantity": qty,
                "instrument": {"symbol": bp, "assetType": "OPTION"},
            },
            {
                "instruction": "SELL_TO_OPEN",
                "positionEffect": "OPENING",
                "quantity": qty,
                "instrument": {"symbol": sp, "assetType": "OPTION"},
            },
            {
                "instruction": "SELL_TO_OPEN",
                "positionEffect": "OPENING",
                "quantity": qty,
                "instrument": {"symbol": sc, "assetType": "OPTION"},
            },
            {
                "instruction": "BUY_TO_OPEN",
                "positionEffect": "OPENING",
                "quantity": qty,
                "instrument": {"symbol": bc, "assetType": "OPTION"},
            },
        ],
    }


def order_payload_vertical_credit(short_osi, long_osi, price, qty):
    return {
        "orderType": "NET_CREDIT",
        "session": "NORMAL",
        "price": f"{clamp_tick(price):.2f}",
        "duration": "DAY",
        "orderStrategyType": "SINGLE",
        "complexOrderStrategyType": "VERTICAL",
        "orderLegCollection": [
            {
                "instruction": "SELL_TO_OPEN",
                "positionEffect": "OPENING",
                "quantity": qty,
                "instrument": {"symbol": short_osi, "assetType": "OPTION"},
            },
            {
                "instruction": "BUY_TO_OPEN",
                "positionEffect": "OPENING",
                "quantity": qty,
                "instrument": {"symbol": long_osi, "assetType": "OPTION"},
            },
        ],
    }


def parse_order_id(r):
    try:
        payload = r.json()
        if isinstance(payload, dict):
            oid = payload.get("orderId") or payload.get("order_id")
            if oid:
                return str(oid)
    except Exception:
        pass
    location = r.headers.get("Location", "")
    return location.rstrip("/").split("/")[-1] if location else ""


def post_with_retry(c, url, payload, tag="", tries=6):
    last = ""
    for i in range(tries):
        r = c.session.post(url, json=payload, timeout=20)
        if r.status_code in (200, 201, 202):
            return r
        if r.status_code == 429:
            wait = min(12.0, 0.6 * (2**i)) + random.uniform(0.0, 0.3)
            print(f"WARN: place failed — HTTP_429 — backoff {wait:.2f}s")
            time.sleep(wait)
            continue
        last = f"HTTP_{r.status_code}:{(r.text or '')[:200]}"
        time.sleep(min(6.0, 0.4 * (2**i)))
    raise RuntimeError(f"POST_FAIL({tag}) {last or 'unknown'}")


def delete_with_retry(c, url, tag="", tries=6):
    for i in range(tries):
        r = c.session.delete(url, timeout=20)
        if r.status_code in (200, 201, 202, 204):
            return True
        if r.status_code == 429:
            wait = min(8.0, 0.5 * (2**i)) + random.uniform(0.0, 0.25)
            time.sleep(wait)
            continue
        time.sleep(min(4.0, 0.3 * (2**i)))
    return False


def get_status(c, acct_hash: str, oid: str):
    url = f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{oid}"
    try:
        r = c.session.get(url, timeout=20)
        if r.status_code != 200:
            return {}
        return r.json() or {}
    except Exception:
        return {}


def wait_settle(c, acct_hash: str, oid: str, max_wait=2.0):
    t_end = time.time() + max_wait
    while time.time() < t_end:
        status = get_status(c, acct_hash, oid)
        state = str(status.get("status") or status.get("orderStatus") or "").upper()
        if (not state) or state in {"CANCELED", "FILLED", "REJECTED", "EXPIRED"}:
            return True
        time.sleep(0.15)
    return False


def ladder_prices(bid, ask, mid):
    out = []
    if ask is not None:
        out.append(clamp_tick(ask))
    if mid is not None:
        out += [clamp_tick(mid), clamp_tick(mid - 0.05), clamp_tick(mid - 0.10)]
        if bid is not None:
            out[-2] = max(out[-2], bid)
            out[-1] = max(out[-1], bid)
    seen = set()
    ladder = []
    for price in out:
        if price not in seen:
            seen.add(price)
            ladder.append(price)
    return ladder


def place_credit_order(c, acct_hash, payload, prices, qty, tag):
    active_oid = None
    url_post = f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders"
    filled = 0
    for price in prices:
        to_place = max(0, qty - filled)
        if to_place == 0:
            break
        if active_oid:
            url_del = f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{active_oid}"
            ok = delete_with_retry(c, url_del, tag=f"CANCEL {active_oid}")
            print(f"CANCEL {active_oid} → {'HTTP_200' if ok else 'FAIL'}")
            wait_settle(c, acct_hash, active_oid, max_wait=1.0)
            active_oid = None
            time.sleep(0.20)

        payload["price"] = f"{clamp_tick(price):.2f}"
        for leg in payload.get("orderLegCollection", []):
            leg["quantity"] = to_place
        try:
            response = post_with_retry(c, url_post, payload, tag=f"{tag}@{price:.2f}x{to_place}")
        except Exception as exc:
            print(str(exc))
            continue
        oid = parse_order_id(response) or ""
        if not oid:
            continue
        active_oid = oid
        t_end = time.time() + 5.0
        while time.time() < t_end:
            status = get_status(c, acct_hash, active_oid)
            state = str(status.get("status") or status.get("orderStatus") or "").upper()
            filled_qty = int(round(float(status.get("filledQuantity") or status.get("filled_quantity") or 0)))
            if filled_qty > filled:
                filled = filled_qty
            if state == "FILLED" or filled >= qty:
                break
            time.sleep(0.25)
        if filled >= qty:
            break

    if active_oid:
        url_del = f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{active_oid}"
        ok = delete_with_retry(c, url_del, tag=f"CANCEL {active_oid}")
        print(f"CANCEL {active_oid} → {'HTTP_200' if ok else 'FAIL'}")
        wait_settle(c, acct_hash, active_oid, max_wait=1.0)


if __name__ == "__main__":
    side = (os.environ.get("SIDE", "CREDIT") or "CREDIT").upper()
    struct = (os.environ.get("STRUCTURE", "CONDOR") or "CONDOR").upper()

    bp = os.environ["OCC_BUY_PUT"]
    sp = os.environ["OCC_SELL_PUT"]
    sc = os.environ["OCC_SELL_CALL"]
    bc = os.environ["OCC_BUY_CALL"]

    qty_base = max(1, int(os.environ.get("QTY", "1")))
    qty_put = max(1, int(os.environ.get("QTY_PUT", str(qty_base))))
    qty_call = max(1, int(os.environ.get("QTY_CALL", str(qty_base))))

    client = schwab_client()
    response = client.get_account_numbers()
    response.raise_for_status()
    acct_hash = str(response.json()[0]["hashValue"])

    if side == "DEBIT":
        print(f"PLACER START side=DEBIT width=5 qty={qty_base}")
        sys.exit(0)

    if struct == "CONDOR":
        print(f"PLACER START side=CREDIT structure=CONDOR qty={qty_base}")
        bid, ask, mid = condor_nbbo_credit(client, bp, sp, sc, bc)
        print(f"CREDIT NBBO (synth): bid={bid} ask={ask} mid={mid}")
        ladder = ladder_prices(bid, ask, mid)
        payload = order_payload_condor_credit([bp, sp, sc, bc], mid or 0.0, qty_base)
        place_credit_order(client, acct_hash, payload, ladder, qty_base, tag="CONDOR")
        print("PLACER DONE")
        sys.exit(0)

    print(f"PLACER START side=CREDIT structure=SPLIT qty_put={qty_put} qty_call={qty_call}")
    put_bid, put_ask, put_mid = vertical_nbbo_credit(client, sp, bp)
    print(f"PUT VERT NBBO: bid={put_bid} ask={put_ask} mid={put_mid}")
    put_ladder = ladder_prices(put_bid, put_ask, put_mid)
    put_payload = order_payload_vertical_credit(sp, bp, put_mid or 0.0, qty_put)
    place_credit_order(client, acct_hash, put_payload, put_ladder, qty_put, tag="PUT_VERTICAL")

    call_bid, call_ask, call_mid = vertical_nbbo_credit(client, sc, bc)
    print(f"CALL VERT NBBO: bid={call_bid} ask={call_ask} mid={call_mid}")
    call_ladder = ladder_prices(call_bid, call_ask, call_mid)
    call_payload = order_payload_vertical_credit(sc, bc, call_mid or 0.0, qty_call)
    place_credit_order(client, acct_hash, call_payload, call_ladder, qty_call, tag="CALL_VERTICAL")

    print("PLACER DONE")
    sys.exit(0)
