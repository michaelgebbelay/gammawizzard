#!/usr/bin/env python3
# CONSTANT STABLE — vertical placer (fast, deterministic)
#
# Ladder (Option B):
#   CREDIT: mid+0.05, mid, mid-0.05 (refresh on last rung)
#   DEBIT : mid-0.05, mid, mid+0.05 (refresh on last rung)
#
# Timing knobs via env:
#   VERT_STEP_WAIT=12
#   VERT_POLL_SECS=1.5
#   VERT_CANCEL_SETTLE=1.0
#   VERT_MAX_LADDER=3
#   VERT_CANCEL_TRIES=4
#   VERT_DRY_RUN=true/false

import os, sys, time, random, csv
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from schwab.auth import client_from_token_file

TICK = 0.05
ET = ZoneInfo("America/New_York")


# ---------- utils ----------
def goutput(name: str, val: str):
    p = os.environ.get("GITHUB_OUTPUT")
    if p:
        with open(p, "a") as fh:
            fh.write(f"{name}={val}\n")


def clamp_tick(x: float) -> float:
    return round(round(float(x) / TICK) * TICK + 1e-12, 2)


def truthy(s: str) -> bool:
    return str(s or "").strip().lower() in ("1", "true", "yes", "y", "on")


# ---------- Schwab ----------
def schwab_client():
    app_key = os.environ["SCHWAB_APP_KEY"]
    app_secret = os.environ["SCHWAB_APP_SECRET"]
    token_json = os.environ["SCHWAB_TOKEN_JSON"]
    with open("schwab_token.json", "w") as f:
        f.write(token_json)
    return client_from_token_file(api_key=app_key, app_secret=app_secret, token_path="schwab_token.json")


def resolve_acct_hash(c):
    ah = (os.environ.get("SCHWAB_ACCT_HASH") or "").strip()
    if ah:
        return ah
    r = c.get_account_numbers()
    r.raise_for_status()
    arr = r.json() or []
    return str((arr[0] or {}).get("hashValue") or "")


def fetch_bid_ask(c, osi: str):
    r = c.get_quote(osi)
    if r.status_code != 200:
        return (None, None)
    d = list(r.json().values())[0] if isinstance(r.json(), dict) else {}
    q = d.get("quote", d)
    b = q.get("bidPrice") or q.get("bid") or q.get("bidPriceInDouble")
    a = q.get("askPrice") or q.get("ask") or q.get("askPriceInDouble")
    return (float(b) if b is not None else None, float(a) if a is not None else None)


def vertical_nbbo(side: str, short_osi: str, long_osi: str, c):
    sb, sa = fetch_bid_ask(c, short_osi)
    lb, la = fetch_bid_ask(c, long_osi)
    if None in (sb, sa, lb, la):
        return (None, None, None)
    if side.upper() == "CREDIT":
        bid = sb - la
        ask = sa - lb
    else:
        bid = lb - sa
        ask = la - sb
    bid = clamp_tick(bid)
    ask = clamp_tick(ask)
    mid = clamp_tick((bid + ask) / 2.0)
    return bid, ask, mid


def order_payload_vertical(side: str, short_osi: str, long_osi: str, price: float, qty: int):
    side = side.upper()
    if side not in ("CREDIT", "DEBIT"):
        raise ValueError("VERT_SIDE must be CREDIT or DEBIT")
    return {
        "orderType": "NET_CREDIT" if side == "CREDIT" else "NET_DEBIT",
        "session": "NORMAL",
        "price": f"{clamp_tick(price):.2f}",
        "duration": "DAY",
        "orderStrategyType": "SINGLE",
        "complexOrderStrategyType": "VERTICAL",
        "orderLegCollection": [
            {"instruction": "BUY_TO_OPEN",  "positionEffect": "OPENING", "quantity": qty,
             "instrument": {"symbol": long_osi,  "assetType": "OPTION"}},
            {"instruction": "SELL_TO_OPEN", "positionEffect": "OPENING", "quantity": qty,
             "instrument": {"symbol": short_osi, "assetType": "OPTION"}},
        ],
    }


def parse_order_id(r):
    try:
        j = r.json()
        if isinstance(j, dict):
            oid = j.get("orderId") or j.get("order_id")
            if oid:
                return str(oid)
    except Exception:
        pass
    loc = r.headers.get("Location", "")
    return loc.rstrip("/").split("/")[-1] if loc else ""


def _retry_after_seconds(resp, default_wait):
    ra = resp.headers.get("Retry-After")
    if ra:
        try:
            return max(1.0, float(ra))
        except Exception:
            pass
    return default_wait


def post_with_retry(c, url, payload, tag="", tries=5):
    last = ""
    for i in range(tries):
        r = c.session.post(url, json=payload, timeout=20)
        if r.status_code in (200, 201, 202):
            return r
        if r.status_code == 429:
            base = min(10.0, 0.7 * (2 ** i))
            wait = _retry_after_seconds(r, base) + random.uniform(0.0, 0.35)
            print(f"WARN: place failed — HTTP_429 — backoff {wait:.2f}s [{tag}]")
            time.sleep(wait)
            continue
        last = f"HTTP_{r.status_code}:{(r.text or '')[:200]}"
        time.sleep(min(6.0, 0.45 * (2 ** i)))
    raise RuntimeError(f"POST_FAIL({tag}) {last or 'unknown'}")


def get_status(c, acct_hash: str, oid: str):
    url = f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{oid}"
    try:
        r = c.session.get(url, timeout=20)
        if r.status_code != 200:
            return {}
        return r.json() or {}
    except Exception:
        return {}


def delete_with_retry(c, url, tag="", tries=4):
    for i in range(tries):
        r = c.session.delete(url, timeout=20)
        if r.status_code in (200, 201, 202, 204):
            return True
        if r.status_code == 429:
            base = min(6.0, 0.5 * (2 ** i))
            wait = _retry_after_seconds(r, base) + random.uniform(0.0, 0.25)
            print(f"WARN: cancel failed — HTTP_429 — backoff {wait:.2f}s [{tag}]")
            time.sleep(wait)
            continue
        time.sleep(min(3.0, 0.35 * (2 ** i)))
    return False


def log_row(row: dict):
    path = os.environ.get("CS_LOG_PATH", "logs/constantstable_vertical_trades.csv")
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

    cols = [
        "ts_utc","ts_et","trade_date","tdate",
        "name","kind","side","direction",
        "short_osi","long_osi",
        "go","strength",
        "qty_rule","vol_field","vol_used","vol_value","vol_bucket","vol_mult",
        "unit_dollars","oc","units",
        "qty_requested","qty_filled",
        "ladder_prices","last_price",
        "nbbo_bid","nbbo_ask","nbbo_mid",
        "order_ids","reason"
    ]

    write_header = not os.path.exists(path)
    with open(path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        if write_header:
            w.writeheader()
        w.writerow({k: row.get(k, "") for k in cols})


# ---------- main ----------
def main():
    placed_reason = "UNKNOWN"
    saw_429 = False

    side = (os.environ.get("VERT_SIDE", "CREDIT") or "CREDIT").upper()
    kind = (os.environ.get("VERT_KIND", "PUT") or "PUT").upper()
    name = os.environ.get("VERT_NAME", "")
    direction = os.environ.get("VERT_DIRECTION", "")
    short_osi = os.environ["VERT_SHORT_OSI"]
    long_osi = os.environ["VERT_LONG_OSI"]
    qty = max(1, int(os.environ.get("VERT_QTY", "1")))

    # passthrough for logging
    go = os.environ.get("VERT_GO", "")
    strength = os.environ.get("VERT_STRENGTH", "")
    trade_date = os.environ.get("VERT_TRADE_DATE", "")
    tdate = os.environ.get("VERT_TDATE", "")
    unit_d = os.environ.get("VERT_UNIT_DOLLARS", "")
    oc = os.environ.get("VERT_OC", "")
    units = os.environ.get("VERT_UNITS", "")

    qty_rule = os.environ.get("VERT_QTY_RULE", "")
    vol_field = os.environ.get("VERT_VOL_FIELD", "")
    vol_used = os.environ.get("VERT_VOL_USED", "")
    vol_value = os.environ.get("VERT_VOL_VALUE", "")
    vol_bucket = os.environ.get("VERT_VOL_BUCKET", "")
    vol_mult = os.environ.get("VERT_VOL_MULT", "")

    # Timing knobs
    STEP_WAIT = float(os.environ.get("VERT_STEP_WAIT", "12"))
    POLL_SECS = float(os.environ.get("VERT_POLL_SECS", "1.5"))
    CANCEL_SETTLE = float(os.environ.get("VERT_CANCEL_SETTLE", "1.0"))
    MAX_LADDER = int(os.environ.get("VERT_MAX_LADDER", "3"))
    CANCEL_TRIES = int(os.environ.get("VERT_CANCEL_TRIES", "4"))
    DRY_RUN = truthy(os.environ.get("VERT_DRY_RUN", "false"))

    print(f"CS_VERT_PLACE START name={name} side={side} kind={kind} short={short_osi} long={long_osi} qty={qty} dry_run={DRY_RUN}")

    c = schwab_client()
    acct_hash = resolve_acct_hash(c)

    # Initial NBBO
    bid, ask, mid = vertical_nbbo(side, short_osi, long_osi, c)
    print(f"CS_VERT_PLACE NBBO: bid={bid} ask={ask} mid={mid}")
    if bid is None or ask is None or mid is None:
        placed_reason = "NBBO_UNAVAILABLE"
        goutput("placed", "0")
        goutput("reason", placed_reason)
        return 0

    # Option B ladder:
    # CREDIT: mid+0.05, mid, mid-0.05 (refresh last)
    # DEBIT : mid-0.05, mid, mid+0.05 (refresh last)
    if side == "CREDIT":
        ladder_spec = [(+0.05, False), (0.00, False), (-0.05, True)]
    else:
        ladder_spec = [(-0.05, False), (0.00, False), (+0.05, True)]

    def price_from_mid(m, off, b, a):
        p = clamp_tick(m + off)
        # Keep within [bid,ask]
        p = max(p, b)
        p = min(p, a)
        return p

    preview = []
    for off, refresh in ladder_spec[:MAX_LADDER]:
        preview.append("REFRESH" if refresh else f"{price_from_mid(mid, off, bid, ask):.2f}")
    print(f"CS_VERT_PLACE ladder_plan={preview}")

    ts_utc = datetime.now(timezone.utc)
    ts_et = ts_utc.astimezone(ET)

    if DRY_RUN:
        placed_reason = "DRY_RUN"
        ladder_str = ",".join(["REFRESH" if r[1] else f"{clamp_tick(mid + r[0]):.2f}" for r in ladder_spec[:MAX_LADDER]])
        row = {
            "ts_utc": ts_utc.isoformat(), "ts_et": ts_et.isoformat(),
            "trade_date": trade_date, "tdate": tdate,
            "name": name, "kind": kind, "side": side, "direction": direction,
            "short_osi": short_osi, "long_osi": long_osi,
            "go": go, "strength": strength,
            "qty_rule": qty_rule,
            "vol_field": vol_field, "vol_used": vol_used, "vol_value": vol_value,
            "vol_bucket": vol_bucket, "vol_mult": vol_mult,
            "unit_dollars": unit_d, "oc": oc, "units": units,
            "qty_requested": qty, "qty_filled": 0,
            "ladder_prices": ladder_str, "last_price": "",
            "nbbo_bid": f"{bid:.2f}", "nbbo_ask": f"{ask:.2f}", "nbbo_mid": f"{mid:.2f}",
            "order_ids": "", "reason": placed_reason,
        }
        log_row(row)
        print("CS_VERT_PLACE DONE (DRY_RUN)")
        goutput("placed", "0")
        goutput("reason", placed_reason)
        goutput("qty_filled", "0")
        goutput("order_ids", "")
        return 0

    url_post = f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders"
    filled, order_ids, last_price = 0, [], None

    for idx, (off, refresh) in enumerate(ladder_spec[:MAX_LADDER], start=1):
        remaining = max(0, qty - filled)
        if remaining <= 0:
            break

        cur_bid, cur_ask, cur_mid = (bid, ask, mid)
        if refresh:
            cur_bid, cur_ask, cur_mid = vertical_nbbo(side, short_osi, long_osi, c)
            print(f"CS_VERT_PLACE REFRESH NBBO: bid={cur_bid} ask={cur_ask} mid={cur_mid}")
            if None in (cur_bid, cur_ask, cur_mid):
                placed_reason = "NBBO_REFRESH_FAIL"
                break

        price = price_from_mid(cur_mid, off, cur_bid, cur_ask)
        last_price = price
        print(f"CS_VERT_PLACE rung#{idx}: price={price:.2f} remaining={remaining} wait={STEP_WAIT:.2f}s poll={POLL_SECS:.2f}s")

        payload = order_payload_vertical(side, short_osi, long_osi, price, remaining)
        try:
            r = post_with_retry(c, url_post, payload, tag=f"{name}@{price:.2f}x{remaining}")
        except Exception as e:
            if "HTTP_429" in str(e):
                saw_429 = True
            print(f"CS_VERT_PLACE ERROR posting order: {e}")
            continue

        oid = parse_order_id(r)
        if oid:
            order_ids.append(oid)

        # Work the rung
        t_end = time.time() + STEP_WAIT
        while time.time() < t_end and oid:
            st = get_status(c, acct_hash, oid) or {}
            fq = st.get("filledQuantity") or st.get("filled_quantity") or 0
            try:
                fq = int(round(float(fq)))
            except Exception:
                fq = 0

            if fq > filled:
                filled = fq

            s = str(st.get("status") or st.get("orderStatus") or "").upper()
            if s in ("FILLED", "CANCELED", "REJECTED", "EXPIRED"):
                break

            time.sleep(POLL_SECS)

        if filled >= qty:
            placed_reason = "OK"
            break

        # Cancel before next rung
        if oid:
            url_del = f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{oid}"
            ok = delete_with_retry(c, url_del, tag=f"CANCEL {oid}", tries=CANCEL_TRIES)
            print(f"CS_VERT_PLACE CANCEL {oid} → {'OK' if ok else 'FAIL'}")

            if not ok:
                # If we can't cancel, do NOT keep going (risk of stray working order)
                st = get_status(c, acct_hash, oid) or {}
                s = str(st.get("status") or st.get("orderStatus") or "").upper()
                placed_reason = f"CANCEL_FAILED_STATUS_{s or 'UNKNOWN'}"
                break

            time.sleep(CANCEL_SETTLE)

    if filled == 0 and placed_reason == "UNKNOWN":
        placed_reason = "HTTP_429_RATE_LIMIT" if saw_429 else "NO_FILL"

    # Final NBBO snapshot (best-effort)
    b2, a2, m2 = vertical_nbbo(side, short_osi, long_osi, c)

    ladder_str = ",".join(
        ["REFRESH" if r[1] else f"{clamp_tick(mid + r[0]):.2f}" for r in ladder_spec[:MAX_LADDER]]
    )

    row = {
        "ts_utc": ts_utc.isoformat(), "ts_et": ts_et.isoformat(),
        "trade_date": trade_date, "tdate": tdate,
        "name": name, "kind": kind, "side": side, "direction": direction,
        "short_osi": short_osi, "long_osi": long_osi,
        "go": go, "strength": strength,
        "qty_rule": qty_rule,
        "vol_field": vol_field, "vol_used": vol_used, "vol_value": vol_value,
        "vol_bucket": vol_bucket, "vol_mult": vol_mult,
        "unit_dollars": unit_d, "oc": oc, "units": units,
        "qty_requested": qty, "qty_filled": filled,
        "ladder_prices": ladder_str,
        "last_price": f"{last_price:.2f}" if last_price is not None else "",
        "nbbo_bid": f"{b2:.2f}" if b2 is not None else "",
        "nbbo_ask": f"{a2:.2f}" if a2 is not None else "",
        "nbbo_mid": f"{m2:.2f}" if m2 is not None else "",
        "order_ids": ",".join(order_ids),
        "reason": placed_reason,
    }
    log_row(row)

    print(f"CS_VERT_PLACE DONE name={name} side={side} kind={kind} qty_req={qty} qty_filled={filled} last_price={last_price} reason={placed_reason}")

    goutput("placed", "1" if filled > 0 else "0")
    goutput("reason", placed_reason)
    goutput("qty_filled", str(filled))
    goutput("order_ids", ",".join(order_ids))

    # Fail workflow if cancel failed (dangerous stray order scenario)
    if placed_reason.startswith("CANCEL_FAILED"):
        return 2

    return 0


if __name__ == "__main__":
    sys.exit(main())
