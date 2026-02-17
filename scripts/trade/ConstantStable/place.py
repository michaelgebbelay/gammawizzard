#!/usr/bin/env python3
# CONSTANT STABLE — placer (vertical or 4-leg bundle)
#
# Ladder (Option B):
#   CREDIT: mid, mid-0.05, mid-0.10 (refresh on last rung)
#   DEBIT : mid, mid+0.05, mid+0.10 (refresh on last rung)
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
import requests
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import sys


def _add_scripts_root():
    cur = os.path.abspath(os.path.dirname(__file__))
    while True:
        if os.path.basename(cur) == "scripts":
            if cur not in sys.path:
                sys.path.append(cur)
            return
        parent = os.path.dirname(cur)
        if parent == cur:
            return
        cur = parent


_add_scripts_root()
from schwab_token_keeper import schwab_client

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


def price_from_mid(mid: float, off: float, bid: float, ask: float) -> float:
    p = clamp_tick(mid + off)
    p = max(p, bid)
    p = min(p, ask)
    return p


def aggressive_offset_for_side(off: float, side: str) -> float:
    return abs(off) if side.upper() == "DEBIT" else -abs(off)


def ladder_offsets_for_side(side: str):
    base = [0.00, 0.05, 0.10]
    if side.upper() == "CREDIT":
        return [-x for x in base]
    return base


def _sanitize_token(t: str) -> str:
    t = (t or "").strip().strip('"').strip("'")
    return t.split(None, 1)[1] if t.lower().startswith("bearer ") else t


def gw_fetch_trade():
    base = (os.environ.get("GW_BASE", "") or "").rstrip("/")
    endpoint = (os.environ.get("GW_ENDPOINT", "") or "").lstrip("/")
    if not base or not endpoint:
        return {}
    url = f"{base}/{endpoint}"
    tok = _sanitize_token(os.environ.get("GW_TOKEN", "") or "")

    def hit(tkn):
        h = {"Accept": "application/json"}
        if tkn:
            h["Authorization"] = f"Bearer {_sanitize_token(tkn)}"
        return requests.get(url, headers=h, timeout=20)

    r = hit(tok) if tok else None
    if (r is None) or (r.status_code in (401, 403)):
        email = os.environ.get("GW_EMAIL", "")
        pwd = os.environ.get("GW_PASSWORD", "")
        if not (email and pwd):
            return {}
        rr = requests.post(
            f"{base}/goauth/authenticateFireUser",
            data={"email": email, "password": pwd},
            timeout=20,
        )
        rr.raise_for_status()
        t = rr.json().get("token") or ""
        r = hit(t)
    r.raise_for_status()
    return extract_trade(r.json())


def extract_trade(j):
    if isinstance(j, dict):
        if "Trade" in j:
            tr = j["Trade"]
            return tr[-1] if isinstance(tr, list) and tr else tr if isinstance(tr, dict) else {}
        keys = ("Date", "TDate", "Limit", "CLimit", "LeftGo", "RightGo", "LImp", "RImp", "VIX", "VixOne")
        if any(k in j for k in keys):
            return j
        for v in j.values():
            if isinstance(v, (dict, list)):
                t = extract_trade(v)
                if t:
                    return t
    if isinstance(j, list):
        for it in reversed(j):
            t = extract_trade(it)
            if t:
                return t
    return {}


def osi_strike_int(osi: str):
    s = (osi or "").strip()
    if len(s) < 8:
        return None
    digits = s[-8:]
    if not digits.isdigit():
        return None
    return int(int(digits) / 1000)


def expected_inner_strikes(v_put: dict, v_call: dict):
    put_strikes = []
    call_strikes = []
    if v_put:
        put_strikes = [osi_strike_int(v_put["short_osi"]), osi_strike_int(v_put["long_osi"])]
    if v_call:
        call_strikes = [osi_strike_int(v_call["short_osi"]), osi_strike_int(v_call["long_osi"])]
    put_strikes = [s for s in put_strikes if s is not None]
    call_strikes = [s for s in call_strikes if s is not None]
    inner_put = max(put_strikes) if put_strikes else None
    inner_call = min(call_strikes) if call_strikes else None
    return inner_put, inner_call


def parse_osi(osi: str):
    s = (osi or "")
    if len(s) < 21:
        return None
    root = s[:6]
    exp6 = s[6:12]
    cp = s[12]
    strike8 = s[13:21]
    if not strike8.isdigit():
        return None
    return root, exp6, cp, int(strike8)


def build_osi(root: str, exp6: str, cp: str, strike_int: int):
    mills = int(round(float(strike_int) * 1000))
    return f"{root}{exp6}{cp}{mills:08d}"


def update_vertical_strikes(v: dict, inner_put: int | None, inner_call: int | None):
    if not v:
        return False
    short_parsed = parse_osi(v.get("short_osi", ""))
    long_parsed = parse_osi(v.get("long_osi", ""))
    if not short_parsed or not long_parsed:
        return False
    root, exp6, cp, s_strike = short_parsed
    _, _, _, l_strike = long_parsed
    width = abs(int(s_strike / 1000) - int(l_strike / 1000))
    if width <= 0:
        return False

    if v.get("kind", "").upper() == "PUT":
        if inner_put is None:
            return False
        p_low = int(inner_put - width)
        p_high = int(inner_put)
        if v.get("direction", "").upper() == "LONG":
            short_k = p_low
            long_k = p_high
        else:
            short_k = p_high
            long_k = p_low
        v["short_osi"] = build_osi(root, exp6, "P", short_k)
        v["long_osi"] = build_osi(root, exp6, "P", long_k)
        return True

    if v.get("kind", "").upper() == "CALL":
        if inner_call is None:
            return False
        c_low = int(inner_call)
        c_high = int(inner_call + width)
        if v.get("direction", "").upper() == "LONG":
            short_k = c_high
            long_k = c_low
        else:
            short_k = c_low
            long_k = c_high
        v["short_osi"] = build_osi(root, exp6, "C", short_k)
        v["long_osi"] = build_osi(root, exp6, "C", long_k)
        return True

    return False


# ---------- Schwab ----------
def resolve_acct_hash(c):
    ah = (os.environ.get("SCHWAB_ACCT_HASH") or "").strip()
    if ah:
        return ah
    r = c.get_account_numbers()
    r.raise_for_status()
    arr = r.json() or []
    return str((arr[0] or {}).get("hashValue") or "")


def cancel_all_working_orders(c, acct_hash: str):
    """Pre-flight: cancel all WORKING orders to prevent stacking from concurrent invocations."""
    url = f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders"
    try:
        r = c.session.get(url, timeout=20)
        if r.status_code != 200:
            print(f"CS_VERT_PLACE PREFLIGHT: failed to fetch orders (HTTP {r.status_code})")
            return
        orders = r.json() or []
        working = [o for o in orders if status_upper(o) in ("WORKING", "QUEUED", "PENDING_ACTIVATION")]
        if not working:
            return
        cancelled = 0
        for order in working:
            oid = str(order.get("orderId", ""))
            if not oid:
                continue
            url_del = f"{url}/{oid}"
            ok = delete_with_retry(c, url_del, tag=f"PREFLIGHT {oid}", tries=3)
            print(f"CS_VERT_PLACE PREFLIGHT_CANCEL {oid} → {'OK' if ok else 'FAIL'}")
            if ok:
                cancelled += 1
        print(f"CS_VERT_PLACE PREFLIGHT: cancelled {cancelled}/{len(working)} working orders")
    except Exception as e:
        print(f"CS_VERT_PLACE PREFLIGHT_WARN: {str(e)[:200]}")


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
        "go","strength","gw_price",
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
            ladder_spec = [(0.00, False), (-0.05, False), (-0.10, True)]
        else:
            ladder_spec = [(0.00, False), (+0.05, False), (+0.10, True)]

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


def place_order_at_price(
    c,
    acct_hash: str,
    payload: dict,
    qty: int,
    tag_prefix: str,
    wait_secs: float,
):
    order_ids = []
    filled = 0
    danger = False
    placed_reason = "UNKNOWN"

    POLL_SECS = float(os.environ.get("VERT_POLL_SECS", "1.5"))
    CANCEL_SETTLE = float(os.environ.get("VERT_CANCEL_SETTLE", "1.0"))
    CANCEL_TRIES = int(os.environ.get("VERT_CANCEL_TRIES", "4"))

    url_post = f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders"

    try:
        r = post_with_retry(c, url_post, payload, tag=f"{tag_prefix}")
    except Exception as e:
        print(f"CS_VERT_PLACE ERROR posting order: {e}")
        return {
            "filled": 0,
            "order_ids": [],
            "reason": "POST_FAIL",
            "danger": False,
        }

    oid = parse_order_id(r)
    if oid:
        order_ids.append(oid)
    else:
        return {
            "filled": 0,
            "order_ids": [],
            "reason": "NO_ORDER_ID",
            "danger": False,
        }

    t_end = time.time() + wait_secs
    while time.time() < t_end and oid:
        st = get_status(c, acct_hash, oid) or {}
        s = status_upper(st)

        fq = extract_filled_quantity(st)
        if s == "FILLED" and fq <= 0:
            fq = qty
        if fq > filled:
            filled = fq

        if s in FINAL_STATUSES:
            break

        time.sleep(POLL_SECS)

    remaining = max(0, qty - filled)

    if remaining > 0 and oid:
        url_del = f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{oid}"
        ok = delete_with_retry(c, url_del, tag=f"CANCEL {oid}", tries=CANCEL_TRIES)
        print(f"CS_VERT_PLACE CANCEL {oid} → {'OK' if ok else 'FAIL'}")

        if ok:
            time.sleep(CANCEL_SETTLE)
        else:
            s_final = ""
            for j in range(6):
                st = get_status(c, acct_hash, oid) or {}
                s_final = status_upper(st)

                fq = extract_filled_quantity(st)
                if s_final == "FILLED" and fq <= 0:
                    fq = qty
                if fq > filled:
                    filled = fq

                if s_final in FINAL_STATUSES:
                    break

                time.sleep(min(4.0, 0.6 * (2 ** j)) + random.uniform(0.0, 0.2))

            if s_final in FINAL_STATUSES:
                pass
            else:
                placed_reason = f"CANCEL_FAILED_STATUS_{s_final or 'UNKNOWN'}"
                danger = True

    if filled >= qty:
        placed_reason = "OK"
    elif filled > 0 and placed_reason == "UNKNOWN":
        placed_reason = "PARTIAL_FILL"
    elif placed_reason == "UNKNOWN":
        placed_reason = "NO_FILL"

    return {
        "filled": int(filled),
        "order_ids": order_ids,
        "reason": placed_reason,
        "danger": danger,
    }


def place_two_verticals_simul(
    c,
    acct_hash: str,
    v1: dict,
    v2: dict,
    qty1: int,
    qty2: int,
):
    FIRST_WAIT = float(os.environ.get("VERT_FIRST_WAIT", "20"))
    ADJUST_WAIT = float(os.environ.get("VERT_ADJUST_WAIT", "20"))
    POLL_SECS = float(os.environ.get("VERT_POLL_SECS", "1.5"))
    CANCEL_SETTLE = float(os.environ.get("VERT_CANCEL_SETTLE", "1.0"))
    CANCEL_TRIES = int(os.environ.get("VERT_CANCEL_TRIES", "4"))

    def submit_one(v, price, q, tag):
        payload = order_payload_vertical(v["side"], v["short_osi"], v["long_osi"], price, q)
        try:
            r = post_with_retry(c, f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders", payload, tag=tag)
        except Exception as e:
            print(f"CS_VERT_PLACE ERROR posting order: {e}")
            return ""
        return parse_order_id(r)

    def poll_orders(states, wait_secs):
        t_end = time.time() + wait_secs
        while time.time() < t_end:
            all_done = True
            for st in states:
                if not st["oid"]:
                    continue
                if st["cur_filled"] >= st["cur_qty"]:
                    continue
                all_done = False
                res = get_status(c, acct_hash, st["oid"]) or {}
                s = status_upper(res)
                fq = extract_filled_quantity(res)
                if s == "FILLED" and fq <= 0:
                    fq = st["cur_qty"]
                if fq > st["cur_filled"]:
                    delta = fq - st["cur_filled"]
                    st["cur_filled"] = fq
                    st["total_filled"] += delta
                if s in FINAL_STATUSES:
                    st["done"] = True
            if all_done:
                break
            time.sleep(POLL_SECS)

    def cancel_if_open(st):
        if not st["oid"]:
            return False, ""
        remaining = max(0, st["cur_qty"] - st["cur_filled"])
        if remaining <= 0:
            return True, ""
        url_del = f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{st['oid']}"
        ok = delete_with_retry(c, url_del, tag=f"CANCEL {st['oid']}", tries=CANCEL_TRIES)
        print(f"CS_VERT_PLACE CANCEL {st['oid']} → {'OK' if ok else 'FAIL'}")
        if ok:
            time.sleep(CANCEL_SETTLE)
            return True, ""

        s_final = ""
        for j in range(6):
            res = get_status(c, acct_hash, st["oid"]) or {}
            s_final = status_upper(res)
            fq = extract_filled_quantity(res)
            if s_final == "FILLED" and fq <= 0:
                fq = st["cur_qty"]
            if fq > st["cur_filled"]:
                delta = fq - st["cur_filled"]
                st["cur_filled"] = fq
                st["total_filled"] += delta
            if s_final in FINAL_STATUSES:
                break
            time.sleep(min(4.0, 0.6 * (2 ** j)) + random.uniform(0.0, 0.2))
        return False, s_final

    # ---- first round at mid ----
    b1, a1, m1 = vertical_nbbo(v1["side"], v1["short_osi"], v1["long_osi"], c)
    b2, a2, m2 = vertical_nbbo(v2["side"], v2["short_osi"], v2["long_osi"], c)
    if None in (b1, a1, m1, b2, a2, m2):
        return (
            {"filled": 0, "order_ids": [], "reason": "NBBO_UNAVAILABLE", "danger": False, "ladder_plan": "[mid, mid+0.05, mid+0.10]"},
            {"filled": 0, "order_ids": [], "reason": "NBBO_UNAVAILABLE", "danger": False, "ladder_plan": "[mid, mid+0.05, mid+0.10]"},
        )

    p1 = price_from_mid(m1, 0.0, b1, a1)
    p2 = price_from_mid(m2, 0.0, b2, a2)
    print(f"CS_VERT_PLACE VERT_MID: {v1['name']}@{p1:.2f} qty={qty1} ; {v2['name']}@{p2:.2f} qty={qty2}")

    s1 = {"v": v1, "qty_total": qty1, "total_filled": 0, "cur_qty": qty1, "cur_filled": 0, "oid": "", "done": False, "order_ids": []}
    s2 = {"v": v2, "qty_total": qty2, "total_filled": 0, "cur_qty": qty2, "cur_filled": 0, "oid": "", "done": False, "order_ids": []}

    s1["oid"] = submit_one(v1, p1, qty, f"{v1['name']}:MID@{p1:.2f}x{qty}")
    if s1["oid"]:
        s1["order_ids"].append(s1["oid"])
    s2["oid"] = submit_one(v2, p2, qty, f"{v2['name']}:MID@{p2:.2f}x{qty}")
    if s2["oid"]:
        s2["order_ids"].append(s2["oid"])

    poll_orders([s1, s2], FIRST_WAIT)

    danger = False
    ok1, st1 = cancel_if_open(s1)
    ok2, st2 = cancel_if_open(s2)
    if not ok1 and st1 not in FINAL_STATUSES:
        danger = True
    if not ok2 and st2 not in FINAL_STATUSES:
        danger = True

    aggressive_offs = [
        float(os.environ.get("VERT_AGGRESSIVE_OFFSET1", "0.05")),
        float(os.environ.get("VERT_AGGRESSIVE_OFFSET2", "0.10")),
    ]

    for aggressive_off in aggressive_offs:
        r1 = max(0, s1["qty_total"] - s1["total_filled"])
        r2 = max(0, s2["qty_total"] - s2["total_filled"])
        if r1 <= 0 and r2 <= 0:
            break
        b1, a1, m1 = vertical_nbbo(v1["side"], v1["short_osi"], v1["long_osi"], c)
        b2, a2, m2 = vertical_nbbo(v2["side"], v2["short_osi"], v2["long_osi"], c)
        if r1 > 0 and None not in (b1, a1, m1):
            off1 = aggressive_offset_for_side(aggressive_off, v1["side"])
            p1 = price_from_mid(m1, off1, b1, a1)
            print(f"CS_VERT_PLACE VERT_ADJ: {v1['name']}@{p1:.2f} rem={r1}")
            s1["oid"] = submit_one(v1, p1, r1, f"{v1['name']}:ADJ@{p1:.2f}x{r1}")
            if s1["oid"]:
                s1["order_ids"].append(s1["oid"])
            s1["cur_qty"] = r1
            s1["cur_filled"] = 0
        if r2 > 0 and None not in (b2, a2, m2):
            off2 = aggressive_offset_for_side(aggressive_off, v2["side"])
            p2 = price_from_mid(m2, off2, b2, a2)
            print(f"CS_VERT_PLACE VERT_ADJ: {v2['name']}@{p2:.2f} rem={r2}")
            s2["oid"] = submit_one(v2, p2, r2, f"{v2['name']}:ADJ@{p2:.2f}x{r2}")
            if s2["oid"]:
                s2["order_ids"].append(s2["oid"])
            s2["cur_qty"] = r2
            s2["cur_filled"] = 0

        poll_orders([s1, s2], ADJUST_WAIT)

        ok1, st1 = cancel_if_open(s1)
        ok2, st2 = cancel_if_open(s2)
        if not ok1 and st1 not in FINAL_STATUSES:
            danger = True
        if not ok2 and st2 not in FINAL_STATUSES:
            danger = True

    def finalize(st):
        filled = int(st["total_filled"])
        if filled >= st["qty_total"]:
            reason = "OK"
        elif filled > 0:
            reason = "PARTIAL_FILL"
        else:
            reason = "NO_FILL"
        return {
            "filled": filled,
            "order_ids": st["order_ids"],
            "reason": reason,
            "danger": danger,
            "ladder_plan": "[mid, mid+0.05, mid+0.10]",
        }

    return finalize(s1), finalize(s2)


def place_two_verticals_alternating(
    c,
    acct_hash: str,
    v1: dict,
    v2: dict,
    qty1: int,
    qty2: int,
):
    STEP_WAIT = float(os.environ.get("VERT_STEP_WAIT", "12"))
    POLL_SECS = float(os.environ.get("VERT_POLL_SECS", "1.5"))
    CANCEL_SETTLE = float(os.environ.get("VERT_CANCEL_SETTLE", "1.0"))
    CANCEL_TRIES = int(os.environ.get("VERT_CANCEL_TRIES", "4"))
    STRIKE_CHECK = truthy(os.environ.get("VERT_STRIKE_CHECK", "1"))
    STRIKE_REPEAT_MAX = int(os.environ.get("VERT_STRIKE_REPEAT_MAX", "2"))

    offs1 = ladder_offsets_for_side(v1["side"])
    offs2 = ladder_offsets_for_side(v2["side"])
    ladder_plan_1 = "[" + ", ".join([f"mid{off:+.2f}" if off != 0 else "mid" for off in offs1]) + "]"
    ladder_plan_2 = "[" + ", ".join([f"mid{off:+.2f}" if off != 0 else "mid" for off in offs2]) + "]"

    def submit_one(v, off, remaining, rung_idx, label):
        print(f"CS_VERT_PLACE ALT STEP={rung_idx} SIDE={label}")
        b, a, m = vertical_nbbo(v["side"], v["short_osi"], v["long_osi"], c)
        if None in (b, a, m):
            return {"oid": "", "cur_qty": 0, "reason": "NBBO_UNAVAILABLE", "last_price": None}
        price = price_from_mid(m, off, b, a)
        print(f"CS_VERT_PLACE ALT rung: {v['name']} price={price:.2f} rem={remaining}")
        payload = order_payload_vertical(v["side"], v["short_osi"], v["long_osi"], price, remaining)
        try:
            r = post_with_retry(
                c,
                f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders",
                payload,
                tag=f"{v['name']}@{price:.2f}x{remaining}",
            )
        except Exception as e:
            print(f"CS_VERT_PLACE ERROR posting order: {e}")
            return {"oid": "", "cur_qty": 0, "reason": "POST_FAIL", "last_price": price}
        oid = parse_order_id(r)
        return {"oid": oid, "cur_qty": remaining, "reason": "PLACED", "last_price": price,
                "nbbo_bid": b, "nbbo_ask": a, "nbbo_mid": m}

    s1 = {"qty_total": qty1, "filled": 0, "order_ids": [], "danger": False, "last_price": None,
          "nbbo_bid": None, "nbbo_ask": None, "nbbo_mid": None}
    s2 = {"qty_total": qty2, "filled": 0, "order_ids": [], "danger": False, "last_price": None,
          "nbbo_bid": None, "nbbo_ask": None, "nbbo_mid": None}
    strikes_changed = False
    exp_put, exp_call = expected_inner_strikes(v2 if v2["kind"] == "PUT" else v1, v1 if v1["kind"] == "CALL" else v2)

    i = 0
    repeat_count = 0
    while i < max(len(offs1), len(offs2)):
        rung_idx = i + 1
        o1 = {"oid": "", "cur_qty": 0, "cur_filled": 0}
        o2 = {"oid": "", "cur_qty": 0, "cur_filled": 0}

        if s1["filled"] < s1["qty_total"] and i < len(offs1):
            rem1 = s1["qty_total"] - s1["filled"]
            r1 = submit_one(v1, offs1[i], rem1, rung_idx, v1["name"])
            o1["oid"] = r1["oid"]
            o1["cur_qty"] = r1["cur_qty"]
            s1["last_price"] = r1.get("last_price")
            if s1["nbbo_mid"] is None:
                s1["nbbo_bid"] = r1.get("nbbo_bid")
                s1["nbbo_ask"] = r1.get("nbbo_ask")
                s1["nbbo_mid"] = r1.get("nbbo_mid")
            if o1["oid"]:
                s1["order_ids"].append(o1["oid"])
        if s2["filled"] < s2["qty_total"] and i < len(offs2):
            rem2 = s2["qty_total"] - s2["filled"]
            r2 = submit_one(v2, offs2[i], rem2, rung_idx, v2["name"])
            o2["oid"] = r2["oid"]
            o2["cur_qty"] = r2["cur_qty"]
            s2["last_price"] = r2.get("last_price")
            if s2["nbbo_mid"] is None:
                s2["nbbo_bid"] = r2.get("nbbo_bid")
                s2["nbbo_ask"] = r2.get("nbbo_ask")
                s2["nbbo_mid"] = r2.get("nbbo_mid")
            if o2["oid"]:
                s2["order_ids"].append(o2["oid"])

        # wait once after submitting both
        if o1["oid"] or o2["oid"]:
            t_end = time.time() + STEP_WAIT
            while time.time() < t_end:
                all_done = True
                for o in (o1, o2):
                    if not o["oid"] or o["cur_filled"] >= o["cur_qty"]:
                        continue
                    all_done = False
                    st = get_status(c, acct_hash, o["oid"]) or {}
                    s = status_upper(st)
                    fq = extract_filled_quantity(st)
                    if s == "FILLED" and fq <= 0:
                        fq = o["cur_qty"]
                    if fq > o["cur_filled"]:
                        o["cur_filled"] = fq
                    if s in FINAL_STATUSES:
                        o["cur_filled"] = max(o["cur_filled"], fq)
                if all_done:
                    break
                time.sleep(POLL_SECS)

        # cancel both after wait
        for o in (o1, o2):
            remaining = max(0, o["cur_qty"] - o["cur_filled"])
            if not o["oid"] or remaining <= 0:
                continue
            url_del = f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{o['oid']}"
            ok = delete_with_retry(c, url_del, tag=f"CANCEL {o['oid']}", tries=CANCEL_TRIES)
            print(f"CS_VERT_PLACE CANCEL {o['oid']} → {'OK' if ok else 'FAIL'}")
            if ok:
                time.sleep(CANCEL_SETTLE)
            else:
                s1["danger"] = s1["danger"] or (o is o1)
                s2["danger"] = s2["danger"] or (o is o2)

        s1["filled"] += int(o1["cur_filled"])
        s2["filled"] += int(o2["cur_filled"])

        if STRIKE_CHECK and (s1["filled"] + s2["filled"] == 0):
            try:
                tr = gw_fetch_trade()
                inner_put = int(float(tr.get("Limit"))) if tr.get("Limit") is not None else None
                inner_call = int(float(tr.get("CLimit"))) if tr.get("CLimit") is not None else None
                if (exp_put is not None and inner_put is not None and exp_put != inner_put) or (
                    exp_call is not None and inner_call is not None and exp_call != inner_call
                ):
                    strikes_changed = True
                    print(f"CS_VERT_PLACE STRIKES_CHANGED: put {exp_put}->{inner_put} call {exp_call}->{inner_call}")
                    updated = False
                    updated |= update_vertical_strikes(v1, inner_put, inner_call)
                    updated |= update_vertical_strikes(v2, inner_put, inner_call)
                    exp_put, exp_call = inner_put, inner_call
                    if updated and repeat_count < STRIKE_REPEAT_MAX:
                        repeat_count += 1
                        print(f"CS_VERT_PLACE STRIKES_REPEAT: rung={rung_idx} repeat={repeat_count}")
                        continue
                    break
            except Exception as e:
                print(f"CS_VERT_PLACE STRIKES_CHECK_WARN: {str(e)[:160]}")

        if s1["filled"] >= s1["qty_total"] and s2["filled"] >= s2["qty_total"]:
            break
        repeat_count = 0
        i += 1

    def finalize(st, ladder_plan):
        filled = int(st["filled"])
        if filled >= st["qty_total"]:
            reason = "OK"
        elif filled > 0:
            reason = "PARTIAL_FILL"
        elif strikes_changed:
            reason = "STRIKES_CHANGED"
        else:
            reason = "NO_FILL"
        return {
            "filled": filled,
            "order_ids": st["order_ids"],
            "reason": reason,
            "danger": st["danger"],
            "ladder_plan": ladder_plan,
            "last_price": st["last_price"],
            "nbbo_bid": st["nbbo_bid"],
            "nbbo_ask": st["nbbo_ask"],
            "nbbo_mid": st["nbbo_mid"],
        }

    return finalize(s1, ladder_plan_1), finalize(s2, ladder_plan_2)


def run_single_vertical(c, acct_hash: str, v: dict, qty: int, tag_prefix: str):
    side = v["side"]
    short_osi = v["short_osi"]
    long_osi = v["long_osi"]

    def nbbo_single(refresh: bool):
        b, a, m = vertical_nbbo(side, short_osi, long_osi, c)
        return (b, a, m)

    def payload_single(price: float, q: int):
        return order_payload_vertical(side, short_osi, long_osi, price, q)

    offs = ladder_offsets_for_side(side)
    ladder_spec = [(o, False) for o in offs]
    if ladder_spec:
        ladder_spec[-1] = (ladder_spec[-1][0], True)
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
    pair_mode = truthy(os.environ.get("VERT_PAIR", "false")) and bool((os.environ.get("VERT2_NAME") or "").strip())
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
        "gw_price": os.environ.get("VERT_GW_PRICE", ""),
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

    if bundle_mode or pair_mode:
        v2 = {
            "side": (os.environ.get("VERT2_SIDE", "") or "").upper(),
            "kind": (os.environ.get("VERT2_KIND", "") or "").upper(),
            "name": os.environ.get("VERT2_NAME", ""),
            "direction": os.environ.get("VERT2_DIRECTION", ""),
            "short_osi": os.environ.get("VERT2_SHORT_OSI", ""),
            "long_osi": os.environ.get("VERT2_LONG_OSI", ""),
            "go": os.environ.get("VERT2_GO", ""),
            "strength": os.environ.get("VERT2_STRENGTH", ""),
            "gw_price": os.environ.get("VERT2_GW_PRICE", ""),
        }
        qty = v1["qty"]  # common qty
        mode_label = "PAIR" if pair_mode else "BUNDLE4"
        print(
            f"CS_VERT_PLACE START MODE={mode_label} qty={qty} "
            f"V1={v1['name']}({v1['short_osi']}|{v1['long_osi']}) "
            f"V2={v2['name']}({v2['short_osi']}|{v2['long_osi']}) dry_run={DRY_RUN}"
        )
        if pair_mode:
            print("CS_VERT_PLACE LADDER=PAIR [mid, mid+0.05, mid+0.10] (credit uses opposite)")
        else:
            print("CS_VERT_PLACE LADDER=BUNDLE4 [mid] -> VERT [mid, mid+0.05, mid+0.10]")
    else:
        v2 = None
        qty = v1["qty"]
        print(f"CS_VERT_PLACE START MODE=VERT name={v1['name']} side={v1['side']} kind={v1['kind']} short={v1['short_osi']} long={v1['long_osi']} qty={qty} dry_run={DRY_RUN}")
        print("CS_VERT_PLACE LADDER=VERT [mid, mid+0.05, mid+0.10] (credit uses opposite)")

    c = schwab_client()
    acct_hash = resolve_acct_hash(c)

    if not DRY_RUN:
        cancel_all_working_orders(c, acct_hash)

    ts_utc = datetime.now(timezone.utc)
    ts_et = ts_utc.astimezone(ET)

    if DRY_RUN:
        reason = "DRY_RUN"
        order_ids = ""
        nbbo_bid = nbbo_ask = nbbo_mid = ""
        last_price = ""
        ladder_prices = "[mid]" if bundle_mode else "[mid, mid+0.05, mid+0.10]"

        def write_one(v):
            row = {
                "ts_utc": ts_utc.isoformat(), "ts_et": ts_et.isoformat(),
                "trade_date": trade_date, "tdate": tdate,
                "name": v["name"], "kind": v["kind"], "side": v["side"], "direction": v["direction"],
                "short_osi": v["short_osi"], "long_osi": v["long_osi"],
                "go": v.get("go", ""), "strength": v.get("strength", ""), "gw_price": v.get("gw_price", ""),
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
        if (bundle_mode or pair_mode) and v2:
            write_one(v2)

        goutput("placed", "0")
        goutput("reason", reason)
        goutput("qty_filled", "0")
        goutput("order_ids", "")
        print("CS_VERT_PLACE DONE (DRY_RUN)")
        return 0

    # ---------- run placement ----------
    if pair_mode and v2:
        qty1 = max(1, int(os.environ.get("VERT_QTY", "1")))
        qty2 = max(1, int(os.environ.get("VERT2_QTY", str(qty1))))

        res_v1, res_v2 = place_two_verticals_alternating(
            c=c,
            acct_hash=acct_hash,
            v1=v1,
            v2=v2,
            qty1=qty1,
            qty2=qty2,
        )

        def log_pair(v, r, qty_req):
            r_filled = int(r["filled"])
            r_order_ids = ",".join(r["order_ids"])
            r_last_price = r.get("last_price")
            r_ladder = r.get("ladder_plan", "")
            row = {
                "ts_utc": ts_utc.isoformat(), "ts_et": ts_et.isoformat(),
                "trade_date": trade_date, "tdate": tdate,
                "name": v["name"], "kind": v["kind"], "side": v["side"], "direction": v["direction"],
                "short_osi": v["short_osi"], "long_osi": v["long_osi"],
                "go": v.get("go", ""), "strength": v.get("strength", ""), "gw_price": v.get("gw_price", ""),
                "qty_rule": qty_rule,
                "vol_field": vol_field, "vol_used": vol_used, "vol_value": vol_value,
                "vol_bucket": vol_bucket, "vol_mult": vol_mult,
                "unit_dollars": unit_d, "oc": oc, "units": units,
                "qty_requested": qty_req, "qty_filled": r_filled,
                "ladder_prices": r_ladder,
                "last_price": f"{r_last_price:.2f}" if r_last_price is not None else "",
                "nbbo_bid": f"{r['nbbo_bid']:.2f}" if r.get("nbbo_bid") is not None else "",
                "nbbo_ask": f"{r['nbbo_ask']:.2f}" if r.get("nbbo_ask") is not None else "",
                "nbbo_mid": f"{r['nbbo_mid']:.2f}" if r.get("nbbo_mid") is not None else "",
                "order_ids": r_order_ids,
                "reason": r["reason"],
            }
            log_row(row)
            return r_filled, r_order_ids, r.get("danger")

        f1, o1, d1 = log_pair(v1, res_v1, qty1)
        f2, o2, d2 = log_pair(v2, res_v2, qty2)

        total_filled = min(f1, qty1) + min(f2, qty2)
        all_order_ids = ",".join([x for x in (o1, o2) if x])
        goutput("placed", "1" if total_filled > 0 else "0")
        goutput("reason", "PAIR_ALTERNATING")
        goutput("qty_filled", str(total_filled))
        goutput("order_ids", all_order_ids)

        if d1 or d2:
            return 2
        return 0

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

        # Place bundle at mid only, then split if needed
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

        FIRST_WAIT = float(os.environ.get("VERT_FIRST_WAIT", "20"))
        b2, a2, m2 = bid0, ask0, mid0
        mid_price = price_from_mid(m2, 0.0, b2, a2)
        print(f"CS_VERT_PLACE BUNDLE_MID: price={mid_price:.2f} qty={qty} wait={FIRST_WAIT:.2f}s")
        res = place_order_at_price(
            c=c,
            acct_hash=acct_hash,
            payload=payload_fn(mid_price, qty),
            qty=qty,
            tag_prefix=f"BUNDLE4:{v1['name']}+{v2['name']}@MID",
            wait_secs=FIRST_WAIT,
        )

        filled = int(res["filled"])
        placed_reason = res["reason"]
        order_ids = ",".join(res["order_ids"])
        last_price = mid_price
        ladder_plan = "[mid]"

        # log BOTH rows (bundle attempt)
        for v in (v1, v2):
            row = {
                "ts_utc": ts_utc.isoformat(), "ts_et": ts_et.isoformat(),
                "trade_date": trade_date, "tdate": tdate,
                "name": v["name"], "kind": v["kind"], "side": v["side"], "direction": v["direction"],
                "short_osi": v["short_osi"], "long_osi": v["long_osi"],
                "go": v.get("go", ""), "strength": v.get("strength", ""), "gw_price": v.get("gw_price", ""),
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

        remaining = max(0, qty - filled)
        if bundle_fallback_separate and remaining > 0:
            print("CS_VERT_PLACE FALLBACK: bundle incomplete — placing verticals separately")
            res_v1, res_v2 = place_two_verticals_simul(
                c=c,
                acct_hash=acct_hash,
                v1=v1,
                v2=v2,
                qty=remaining,
            )

            def log_fallback(v, r):
                r_filled = int(r["filled"])
                r_reason = f"FALLBACK_{r['reason']}"
                r_order_ids = ",".join(r["order_ids"])
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
                    "qty_requested": remaining, "qty_filled": r_filled,
                    "ladder_prices": r_ladder,
                    "last_price": "",
                    "nbbo_bid": "",
                    "nbbo_ask": "",
                    "nbbo_mid": "",
                    "order_ids": r_order_ids,
                    "reason": r_reason,
                }
                log_row(row)
                return r_filled, r_reason, r_order_ids, r.get("danger")

            f1, _, o1, d1 = log_fallback(v1, res_v1)
            f2, _, o2, d2 = log_fallback(v2, res_v2)

            total_filled = filled + min(f1, f2)
            fallback_order_ids = ",".join([x for x in (order_ids, o1, o2) if x])
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
