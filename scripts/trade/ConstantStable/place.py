#!/usr/bin/env python3
# CONSTANT STABLE — placer (vertical or 4-leg bundle)
#
# Ladder (Option B):
#   CREDIT: mid+0.05, mid, mid-0.05 (refresh on last rung)
#   DEBIT : mid-0.05, mid, mid+0.05 (refresh on last rung)
#
# Bundle mode:
#   If VERT_BUNDLE=true and VERT2_NAME present, places one 4-leg NET_DEBIT/NET_CREDIT "CUSTOM" order.
#   Logs TWO rows (one per vertical) sharing the same order_ids / fills.
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

FINAL_STATUSES = {"FILLED", "CANCELED", "REJECTED", "EXPIRED"}


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


def _fnum(x):
    try:
        return float(x)
    except Exception:
        return None


def _retry_after_seconds(resp, default_wait):
    ra = resp.headers.get("Retry-After")
    if ra:
        try:
            return max(1.0, float(ra))
        except Exception:
            pass
    return default_wait


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


def get_quote_json_with_retry(c, osi: str, tries: int = 4):
    last = None
    for i in range(tries):
        r = c.get_quote(osi)
        if r.status_code == 200:
            try:
                return r.json()
            except Exception:
                return None
        if r.status_code == 429:
            base = min(6.0, 0.5 * (2 ** i))
            wait = _retry_after_seconds(r, base) + random.uniform(0.0, 0.25)
            time.sleep(wait)
            continue
        last = r.status_code
        time.sleep(min(2.0, 0.35 * (2 ** i)))
    return None


def fetch_bid_ask(c, osi: str):
    j = get_quote_json_with_retry(c, osi)
    if not j:
        return (None, None)
    d = list(j.values())[0] if isinstance(j, dict) else {}
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


def bundle_nbbo(long_osi_1: str, short_osi_1: str, long_osi_2: str, short_osi_2: str, c):
    """
    Compute NBBO for a 4-leg opening package with instructions:
      BUY_TO_OPEN  long_osi_1, long_osi_2
      SELL_TO_OPEN short_osi_1, short_osi_2

    We compute net_cash range:
      worst = sum(sell_bid) - sum(buy_ask)
      best  = sum(sell_ask) - sum(buy_bid)

    If net_cash is positive -> NET_CREDIT
    If net_cash is negative -> NET_DEBIT  (debit = -net_cash)
    """
    s1b, s1a = fetch_bid_ask(c, short_osi_1)
    l1b, l1a = fetch_bid_ask(c, long_osi_1)
    s2b, s2a = fetch_bid_ask(c, short_osi_2)
    l2b, l2a = fetch_bid_ask(c, long_osi_2)

    if None in (s1b, s1a, l1b, l1a, s2b, s2a, l2b, l2a):
        return (None, None, None, None)

    net_cash_worst = (s1b + s2b) - (l1a + l2a)
    net_cash_best = (s1a + s2a) - (l1b + l2b)

    # Decide credit/debit
    if net_cash_best <= 0:
        # Always a debit
        side = "DEBIT"
        bid = -net_cash_best  # smaller debit
        ask = -net_cash_worst # larger debit
    elif net_cash_worst >= 0:
        # Always a credit
        side = "CREDIT"
        bid = net_cash_worst  # smaller credit
        ask = net_cash_best   # larger credit
    else:
        # Crosses zero; choose by mid sign
        mid_cash = 0.5 * (net_cash_worst + net_cash_best)
        if mid_cash >= 0:
            side = "CREDIT"
            bid = max(0.0, net_cash_worst)
            ask = max(bid, net_cash_best)
        else:
            side = "DEBIT"
            bid = max(0.0, -net_cash_best)
            ask = max(bid, -net_cash_worst)

    bid = clamp_tick(bid)
    ask = clamp_tick(ask)
    mid = clamp_tick((bid + ask) / 2.0)
    return side, bid, ask, mid


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


def order_payload_bundle(side: str, price: float, qty: int,
                        long_osi_1: str, short_osi_1: str,
                        long_osi_2: str, short_osi_2: str):
    side = side.upper()
    if side not in ("CREDIT", "DEBIT"):
        raise ValueError("bundle side must be CREDIT or DEBIT")
    return {
        "orderType": "NET_CREDIT" if side == "CREDIT" else "NET_DEBIT",
        "session": "NORMAL",
        "price": f"{clamp_tick(price):.2f}",
        "duration": "DAY",
        "orderStrategyType": "SINGLE",
        "complexOrderStrategyType": "CUSTOM",
        "orderLegCollection": [
            {"instruction": "BUY_TO_OPEN",  "positionEffect": "OPENING", "quantity": qty,
             "instrument": {"symbol": long_osi_1,  "assetType": "OPTION"}},
            {"instruction": "SELL_TO_OPEN", "positionEffect": "OPENING", "quantity": qty,
             "instrument": {"symbol": short_osi_1, "assetType": "OPTION"}},
            {"instruction": "BUY_TO_OPEN",  "positionEffect": "OPENING", "quantity": qty,
             "instrument": {"symbol": long_osi_2,  "assetType": "OPTION"}},
            {"instruction": "SELL_TO_OPEN", "positionEffect": "OPENING", "quantity": qty,
             "instrument": {"symbol": short_osi_2, "assetType": "OPTION"}},
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


def status_upper(st: dict) -> str:
    return str(st.get("status") or st.get("orderStatus") or "").upper().strip()


def extract_filled_quantity(st: dict) -> int:
    """
    Robust filled qty extractor for Schwab complex orders.
    1) Try top-level filledQuantity-ish fields.
    2) Fall back to summing executions in orderActivityCollection.
       For multi-leg executions, legs share same execution quantity; use min across legs as combo qty.
    """
    for k in (
        "filledQuantity", "filled_quantity", "filledQty", "filledQtyInDouble",
        "filledQuantityInDouble", "filledQtyInDbl"
    ):
        if k in st:
            v = _fnum(st.get(k))
            if v is not None:
                return int(round(v))

    acts = st.get("orderActivityCollection") or st.get("orderActivities") or []
    total = 0.0
    for act in acts:
        if str(act.get("activityType") or "").upper() != "EXECUTION":
            continue
        legs = act.get("executionLegs") or []
        qtys = []
        for leg in legs:
            q = _fnum(leg.get("quantity"))
            if q is not None:
                qtys.append(q)
        if qtys:
            total += min(qtys)

    if total > 0:
        return int(round(total))
    return 0


def get_status(c, acct_hash: str, oid: str, tries: int = 4):
    url = f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{oid}"
    for i in range(tries):
        try:
            r = c.session.get(url, timeout=20)
            if r.status_code == 200:
                return r.json() or {}
            if r.status_code == 429:
                base = min(6.0, 0.5 * (2 ** i))
                wait = _retry_after_seconds(r, base) + random.uniform(0.0, 0.25)
                time.sleep(wait)
                continue
        except Exception:
            pass
        time.sleep(min(2.0, 0.4 * (2 ** i)))
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


# ---------- core placement routine ----------
def place_order_with_ladder(
    c,
    acct_hash: str,
    side: str,
    qty: int,
    nbbo_fn,            # callable() -> (bid,ask,mid) or (side,bid,ask,mid) depending
    payload_fn,         # callable(price, qty) -> payload
    tag_prefix: str,
    ladder_spec=None,   # list of (offset, refresh) tuples; if None, uses default by side
):
    placed_reason = "UNKNOWN"
    saw_429 = False
    danger_stray_order = False

    STEP_WAIT = float(os.environ.get("VERT_STEP_WAIT", "12"))
    POLL_SECS = float(os.environ.get("VERT_POLL_SECS", "1.5"))
    CANCEL_SETTLE = float(os.environ.get("VERT_CANCEL_SETTLE", "1.0"))
    MAX_LADDER = int(os.environ.get("VERT_MAX_LADDER", "3"))
    CANCEL_TRIES = int(os.environ.get("VERT_CANCEL_TRIES", "4"))

    url_post = f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders"

    # Initial NBBO
    bid, ask, mid = nbbo_fn(refresh=False)
    if bid is None or ask is None or mid is None:
        return {
            "filled": 0, "order_ids": [], "last_price": None,
            "nbbo_bid": bid, "nbbo_ask": ask, "nbbo_mid": mid,
            "reason": "NBBO_UNAVAILABLE", "danger": False,
        }

    print(f"CS_VERT_PLACE NBBO: bid={bid} ask={ask} mid={mid}")

    # Ladder spec (Option B)
    if ladder_spec is None:
        if side.upper() == "CREDIT":
            ladder_spec = [(+0.05, False), (0.00, False), (-0.05, True)]
        else:
            ladder_spec = [(-0.05, False), (0.00, False), (+0.05, True)]

    def price_from_mid(m, off, b, a):
        p = clamp_tick(m + off)
        p = max(p, b)
        p = min(p, a)
        return p

    preview = []
    for off, refresh in ladder_spec[:MAX_LADDER]:
        preview.append("REFRESH" if refresh else f"{price_from_mid(mid, off, bid, ask):.2f}")
    preview_str = "[" + ", ".join(preview) + "]"
    print(f"CS_VERT_PLACE ladder_plan={preview}")

    filled_total = 0
    order_ids = []
    last_price = None

    for idx, (off, refresh) in enumerate(ladder_spec[:MAX_LADDER], start=1):
        remaining = max(0, qty - filled_total)
        if remaining <= 0:
            break

        cur_bid, cur_ask, cur_mid = (bid, ask, mid)
        if refresh:
            cur_bid, cur_ask, cur_mid = nbbo_fn(refresh=True)
            print(f"CS_VERT_PLACE REFRESH NBBO: bid={cur_bid} ask={cur_ask} mid={cur_mid}")
            if None in (cur_bid, cur_ask, cur_mid):
                placed_reason = "NBBO_REFRESH_FAIL"
                break

        price = price_from_mid(cur_mid, off, cur_bid, cur_ask)
        last_price = price
        print(f"CS_VERT_PLACE rung#{idx}: price={price:.2f} remaining={remaining} wait={STEP_WAIT:.2f}s poll={POLL_SECS:.2f}s")

        payload = payload_fn(price, remaining)

        try:
            r = post_with_retry(c, url_post, payload, tag=f"{tag_prefix}@{price:.2f}x{remaining}")
        except Exception as e:
            if "HTTP_429" in str(e):
                saw_429 = True
            print(f"CS_VERT_PLACE ERROR posting order: {e}")
            continue

        oid = parse_order_id(r)
        if oid:
            order_ids.append(oid)

        this_order_filled = 0

        # Work the rung
        t_end = time.time() + STEP_WAIT
        while time.time() < t_end and oid:
            st = get_status(c, acct_hash, oid) or {}
            s = status_upper(st)

            fq = extract_filled_quantity(st)
            if s == "FILLED" and fq <= 0:
                fq = remaining  # if FILLED but qty missing, assume full fill for this order

            if fq > this_order_filled:
                filled_total += (fq - this_order_filled)
                this_order_filled = fq

            if s in FINAL_STATUSES:
                break

            time.sleep(POLL_SECS)

        if filled_total >= qty:
            placed_reason = "OK"
            break

        # Cancel before next rung
        if oid:
            url_del = f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{oid}"
            ok = delete_with_retry(c, url_del, tag=f"CANCEL {oid}", tries=CANCEL_TRIES)
            print(f"CS_VERT_PLACE CANCEL {oid} → {'OK' if ok else 'FAIL'}")

            if ok:
                time.sleep(CANCEL_SETTLE)
                continue

            # cancel failed -> resolve status
            s_final = ""
            for j in range(6):
                st = get_status(c, acct_hash, oid) or {}
                s_final = status_upper(st)

                fq = extract_filled_quantity(st)
                if s_final == "FILLED" and fq <= 0:
                    fq = remaining
                if fq > this_order_filled:
                    filled_total += (fq - this_order_filled)
                    this_order_filled = fq

                if s_final in FINAL_STATUSES:
                    break

                time.sleep(min(4.0, 0.6 * (2 ** j)) + random.uniform(0.0, 0.2))

            if s_final == "FILLED" or filled_total > 0:
                placed_reason = "OK" if filled_total >= qty else "PARTIAL_FILL"
                break

            if s_final in ("CANCELED", "REJECTED", "EXPIRED"):
                time.sleep(CANCEL_SETTLE)
                continue

            placed_reason = f"CANCEL_FAILED_STATUS_{s_final or 'UNKNOWN'}"
            danger_stray_order = True
            break

    if filled_total == 0 and placed_reason == "UNKNOWN":
        placed_reason = "HTTP_429_RATE_LIMIT" if saw_429 else "NO_FILL"

    # final nbbo snapshot best-effort
    b2, a2, m2 = nbbo_fn(refresh=True)
    return {
        "filled": filled_total,
        "order_ids": order_ids,
        "last_price": last_price,
        "nbbo_bid": b2 if b2 is not None else bid,
        "nbbo_ask": a2 if a2 is not None else ask,
        "nbbo_mid": m2 if m2 is not None else mid,
        "reason": placed_reason,
        "danger": danger_stray_order,
        "init_bid": bid, "init_ask": ask, "init_mid": mid,
        "ladder_plan": preview_str,
    }


def run_single_vertical(c, acct_hash: str, v: dict, qty: int, tag_prefix: str):
    side = v["side"]
    short_osi = v["short_osi"]
    long_osi = v["long_osi"]

    def nbbo_single(refresh: bool):
        b, a, m = vertical_nbbo(side, short_osi, long_osi, c)
        return (b, a, m)

    def payload_single(price: float, q: int):
        return order_payload_vertical(side, short_osi, long_osi, price, q)

    ladder_spec = [(-0.05, False), (0.00, False), (+0.05, True)]
    return place_order_with_ladder(
        c=c,
        acct_hash=acct_hash,
        side=side,
        qty=qty,
        nbbo_fn=nbbo_single,
        payload_fn=payload_single,
        tag_prefix=tag_prefix,
        ladder_spec=ladder_spec,
    )


# ---------- main ----------
def main():
    DRY_RUN = truthy(os.environ.get("VERT_DRY_RUN", "false"))

    bundle_mode = truthy(os.environ.get("VERT_BUNDLE", "false")) and bool((os.environ.get("VERT2_NAME") or "").strip())
    bundle_fallback = (os.environ.get("VERT_BUNDLE_FALLBACK", "") or "").strip().lower()
    bundle_fallback_separate = bundle_fallback in ("1", "true", "yes", "y", "separate", "split", "verticals")

    # primary vertical (for logging + single mode)
    v1 = {
        "side": (os.environ.get("VERT_SIDE", "CREDIT") or "CREDIT").upper(),
        "kind": (os.environ.get("VERT_KIND", "PUT") or "PUT").upper(),
        "name": os.environ.get("VERT_NAME", ""),
        "direction": os.environ.get("VERT_DIRECTION", ""),
        "short_osi": os.environ.get("VERT_SHORT_OSI", ""),
        "long_osi": os.environ.get("VERT_LONG_OSI", ""),
        "qty": max(1, int(os.environ.get("VERT_QTY", "1"))),
        "go": os.environ.get("VERT_GO", ""),
        "strength": os.environ.get("VERT_STRENGTH", ""),
    }

    # passthrough for logging (shared)
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

    if bundle_mode:
        v2 = {
            "side": (os.environ.get("VERT2_SIDE", "") or "").upper(),
            "kind": (os.environ.get("VERT2_KIND", "") or "").upper(),
            "name": os.environ.get("VERT2_NAME", ""),
            "direction": os.environ.get("VERT2_DIRECTION", ""),
            "short_osi": os.environ.get("VERT2_SHORT_OSI", ""),
            "long_osi": os.environ.get("VERT2_LONG_OSI", ""),
            "go": os.environ.get("VERT2_GO", ""),
            "strength": os.environ.get("VERT2_STRENGTH", ""),
        }
        qty = v1["qty"]  # common qty
        print(
            f"CS_VERT_PLACE START MODE=BUNDLE4 qty={qty} "
            f"V1={v1['name']}({v1['short_osi']}|{v1['long_osi']}) "
            f"V2={v2['name']}({v2['short_osi']}|{v2['long_osi']}) dry_run={DRY_RUN}"
        )
        print("CS_VERT_PLACE LADDER=BUNDLE4 [mid-0.05, mid, mid+0.10]")
    else:
        v2 = None
        qty = v1["qty"]
        print(f"CS_VERT_PLACE START MODE=VERT name={v1['name']} side={v1['side']} kind={v1['kind']} short={v1['short_osi']} long={v1['long_osi']} qty={qty} dry_run={DRY_RUN}")
        print("CS_VERT_PLACE LADDER=VERT [mid-0.05, mid, mid+0.05]")

    c = schwab_client()
    acct_hash = resolve_acct_hash(c)

    ts_utc = datetime.now(timezone.utc)
    ts_et = ts_utc.astimezone(ET)

    if DRY_RUN:
        reason = "DRY_RUN"
        order_ids = ""
        nbbo_bid = nbbo_ask = nbbo_mid = ""
        last_price = ""
        ladder_prices = "[mid-0.05, mid, mid+0.10]" if bundle_mode else "[mid-0.05, mid, mid+0.05]"

        def write_one(v):
            row = {
                "ts_utc": ts_utc.isoformat(), "ts_et": ts_et.isoformat(),
                "trade_date": trade_date, "tdate": tdate,
                "name": v["name"], "kind": v["kind"], "side": v["side"], "direction": v["direction"],
                "short_osi": v["short_osi"], "long_osi": v["long_osi"],
                "go": v.get("go", ""), "strength": v.get("strength", ""),
                "qty_rule": qty_rule,
                "vol_field": vol_field, "vol_used": vol_used, "vol_value": vol_value,
                "vol_bucket": vol_bucket, "vol_mult": vol_mult,
                "unit_dollars": unit_d, "oc": oc, "units": units,
                "qty_requested": qty, "qty_filled": 0,
                "ladder_prices": ladder_prices, "last_price": last_price,
                "nbbo_bid": nbbo_bid, "nbbo_ask": nbbo_ask, "nbbo_mid": nbbo_mid,
                "order_ids": order_ids, "reason": reason,
            }
            log_row(row)

        write_one(v1)
        if bundle_mode and v2:
            write_one(v2)

        goutput("placed", "0")
        goutput("reason", reason)
        goutput("qty_filled", "0")
        goutput("order_ids", "")
        print("CS_VERT_PLACE DONE (DRY_RUN)")
        return 0

    # ---------- run placement ----------
    if bundle_mode and v2:
        # Determine bundle net side + NBBO
        def nbbo_fn(refresh: bool):
            side_pkg, bid, ask, mid = bundle_nbbo(
                long_osi_1=v1["long_osi"], short_osi_1=v1["short_osi"],
                long_osi_2=v2["long_osi"], short_osi_2=v2["short_osi"],
                c=c,
            )
            if side_pkg is None:
                return (None, None, None)
            # bundle_nbbo returns side too, but this ladder engine already has "side" externally.
            return (bid, ask, mid)

        # Need bundle side for ladder + payload
        side_pkg, bid0, ask0, mid0 = bundle_nbbo(
            long_osi_1=v1["long_osi"], short_osi_1=v1["short_osi"],
            long_osi_2=v2["long_osi"], short_osi_2=v2["short_osi"],
            c=c,
        )
        if side_pkg is None or None in (bid0, ask0, mid0):
            reason = "NBBO_UNAVAILABLE"
            # log both rows
            for v in (v1, v2):
                row = {
                    "ts_utc": ts_utc.isoformat(), "ts_et": ts_et.isoformat(),
                    "trade_date": trade_date, "tdate": tdate,
                    "name": v["name"], "kind": v["kind"], "side": v["side"], "direction": v["direction"],
                    "short_osi": v["short_osi"], "long_osi": v["long_osi"],
                    "go": v.get("go", ""), "strength": v.get("strength", ""),
                    "qty_rule": qty_rule,
                    "vol_field": vol_field, "vol_used": vol_used, "vol_value": vol_value,
                    "vol_bucket": vol_bucket, "vol_mult": vol_mult,
                    "unit_dollars": unit_d, "oc": oc, "units": units,
                    "qty_requested": qty, "qty_filled": 0,
                    "ladder_prices": "", "last_price": "",
                    "nbbo_bid": "", "nbbo_ask": "", "nbbo_mid": "",
                    "order_ids": "", "reason": reason,
                }
                log_row(row)

            goutput("placed", "0")
            goutput("reason", reason)
            goutput("qty_filled", "0")
            goutput("order_ids", "")
            print("CS_VERT_PLACE DONE MODE=BUNDLE4 reason=NBBO_UNAVAILABLE")
            return 0

        # Place bundle with ladder
        def nbbo_fn2(refresh: bool):
            s, b, a, m = bundle_nbbo(
                long_osi_1=v1["long_osi"], short_osi_1=v1["short_osi"],
                long_osi_2=v2["long_osi"], short_osi_2=v2["short_osi"],
                c=c,
            )
            return (b, a, m)

        def payload_fn(price: float, q: int):
            return order_payload_bundle(
                side=side_pkg, price=price, qty=q,
                long_osi_1=v1["long_osi"], short_osi_1=v1["short_osi"],
                long_osi_2=v2["long_osi"], short_osi_2=v2["short_osi"],
            )

        # For ladder we use bundle side
        ladder_spec = [(-0.05, False), (0.00, False), (+0.10, True)]
        res = place_order_with_ladder(
            c=c,
            acct_hash=acct_hash,
            side=side_pkg,
            qty=qty,
            nbbo_fn=nbbo_fn2,
            payload_fn=payload_fn,
            tag_prefix=f"BUNDLE4:{v1['name']}+{v2['name']}",
            ladder_spec=ladder_spec,
        )

        filled = int(res["filled"])
        placed_reason = res["reason"]
        order_ids = ",".join(res["order_ids"])
        last_price = res["last_price"]
        b2, a2, m2 = res["nbbo_bid"], res["nbbo_ask"], res["nbbo_mid"]
        ladder_plan = res.get("ladder_plan", "")

        # log BOTH rows (bundle attempt)
        for v in (v1, v2):
            row = {
                "ts_utc": ts_utc.isoformat(), "ts_et": ts_et.isoformat(),
                "trade_date": trade_date, "tdate": tdate,
                "name": v["name"], "kind": v["kind"], "side": v["side"], "direction": v["direction"],
                "short_osi": v["short_osi"], "long_osi": v["long_osi"],
                "go": v.get("go", ""), "strength": v.get("strength", ""),
                "qty_rule": qty_rule,
                "vol_field": vol_field, "vol_used": vol_used, "vol_value": vol_value,
                "vol_bucket": vol_bucket, "vol_mult": vol_mult,
                "unit_dollars": unit_d, "oc": oc, "units": units,
                "qty_requested": qty, "qty_filled": filled,
                "ladder_prices": ladder_plan,
                "last_price": f"{last_price:.2f}" if last_price is not None else "",
                "nbbo_bid": f"{b2:.2f}" if b2 is not None else "",
                "nbbo_ask": f"{a2:.2f}" if a2 is not None else "",
                "nbbo_mid": f"{m2:.2f}" if m2 is not None else "",
                "order_ids": order_ids,
                "reason": placed_reason,
            }
            log_row(row)

        print(f"CS_VERT_PLACE DONE MODE=BUNDLE4 qty_req={qty} qty_filled={filled} last_price={last_price} reason={placed_reason}")

        if bundle_fallback_separate and filled == 0 and placed_reason == "NO_FILL":
            print("CS_VERT_PLACE FALLBACK: bundle no fill — placing verticals separately")
            res_v1 = run_single_vertical(c, acct_hash, v1, qty, tag_prefix=f"{v1['name']}:FALLBACK")
            res_v2 = run_single_vertical(c, acct_hash, v2, qty, tag_prefix=f"{v2['name']}:FALLBACK")

            def log_fallback(v, r):
                r_filled = int(r["filled"])
                r_reason = f"FALLBACK_{r['reason']}"
                r_order_ids = ",".join(r["order_ids"])
                r_last_price = r["last_price"]
                r_b2, r_a2, r_m2 = r["nbbo_bid"], r["nbbo_ask"], r["nbbo_mid"]
                r_ladder = r.get("ladder_plan", "")
                row = {
                    "ts_utc": ts_utc.isoformat(), "ts_et": ts_et.isoformat(),
                    "trade_date": trade_date, "tdate": tdate,
                    "name": v["name"], "kind": v["kind"], "side": v["side"], "direction": v["direction"],
                    "short_osi": v["short_osi"], "long_osi": v["long_osi"],
                    "go": v.get("go", ""), "strength": v.get("strength", ""),
                    "qty_rule": qty_rule,
                    "vol_field": vol_field, "vol_used": vol_used, "vol_value": vol_value,
                    "vol_bucket": vol_bucket, "vol_mult": vol_mult,
                    "unit_dollars": unit_d, "oc": oc, "units": units,
                    "qty_requested": qty, "qty_filled": r_filled,
                    "ladder_prices": r_ladder,
                    "last_price": f"{r_last_price:.2f}" if r_last_price is not None else "",
                    "nbbo_bid": f"{r_b2:.2f}" if r_b2 is not None else "",
                    "nbbo_ask": f"{r_a2:.2f}" if r_a2 is not None else "",
                    "nbbo_mid": f"{r_m2:.2f}" if r_m2 is not None else "",
                    "order_ids": r_order_ids,
                    "reason": r_reason,
                }
                log_row(row)
                return r_filled, r_reason, r_order_ids, r.get("danger")

            f1, _, o1, d1 = log_fallback(v1, res_v1)
            f2, _, o2, d2 = log_fallback(v2, res_v2)

            total_filled = f1 + f2
            fallback_order_ids = ",".join([x for x in (o1, o2) if x])
            goutput("placed", "1" if total_filled > 0 else "0")
            goutput("reason", "FALLBACK_SEPARATE")
            goutput("qty_filled", str(total_filled))
            goutput("order_ids", fallback_order_ids)

            if res.get("danger") or d1 or d2:
                return 2
            return 0

        goutput("placed", "1" if filled > 0 else "0")
        goutput("reason", placed_reason)
        goutput("qty_filled", str(filled))
        goutput("order_ids", order_ids)

        if res.get("danger"):
            return 2
        return 0

    # ---------- single vertical mode ----------
    side = v1["side"]
    short_osi = v1["short_osi"]
    long_osi = v1["long_osi"]

    res = run_single_vertical(c, acct_hash, v1, qty, tag_prefix=f"{v1['name']}")

    filled = int(res["filled"])
    placed_reason = res["reason"]
    order_ids = ",".join(res["order_ids"])
    last_price = res["last_price"]
    b2, a2, m2 = res["nbbo_bid"], res["nbbo_ask"], res["nbbo_mid"]
    ladder_plan = res.get("ladder_plan", "")

    row = {
        "ts_utc": ts_utc.isoformat(), "ts_et": ts_et.isoformat(),
        "trade_date": trade_date, "tdate": tdate,
        "name": v1["name"], "kind": v1["kind"], "side": v1["side"], "direction": v1["direction"],
        "short_osi": v1["short_osi"], "long_osi": v1["long_osi"],
        "go": v1.get("go", ""), "strength": v1.get("strength", ""),
        "qty_rule": qty_rule,
        "vol_field": vol_field, "vol_used": vol_used, "vol_value": vol_value,
        "vol_bucket": vol_bucket, "vol_mult": vol_mult,
        "unit_dollars": unit_d, "oc": oc, "units": units,
        "qty_requested": qty, "qty_filled": filled,
        "ladder_prices": ladder_plan,
        "last_price": f"{last_price:.2f}" if last_price is not None else "",
        "nbbo_bid": f"{b2:.2f}" if b2 is not None else "",
        "nbbo_ask": f"{a2:.2f}" if a2 is not None else "",
        "nbbo_mid": f"{m2:.2f}" if m2 is not None else "",
        "order_ids": order_ids,
        "reason": placed_reason,
    }
    log_row(row)

    print(f"CS_VERT_PLACE DONE MODE=VERT name={v1['name']} qty_req={qty} qty_filled={filled} last_price={last_price} reason={placed_reason}")

    goutput("placed", "1" if filled > 0 else "0")
    goutput("reason", placed_reason)
    goutput("qty_filled", str(filled))
    goutput("order_ids", order_ids)

    if res.get("danger"):
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
