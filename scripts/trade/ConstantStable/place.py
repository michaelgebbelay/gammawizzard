#!/usr/bin/env python3
# CONSTANT STABLE — vertical placer (no gates, fixed waits)
#  - Starts immediately (no not-before/deadline)
#  - Ladder (per spec):
#       CREDIT: mid, mid - 0.05, REFRESH(mid - 0.05)
#       DEBIT : mid, mid + 0.05, REFRESH(mid + 0.05)
#  - Fixed timings: 15s per rung, 2s cancel settle (configurable via env)
#  - Emits GITHUB_OUTPUT: placed, reason, qty_filled, order_ids
#  - Logs CSV to CS_LOG_PATH
#
# Required env (set by orchestrator/workflow):
#   SCHWAB_APP_KEY, SCHWAB_APP_SECRET, SCHWAB_TOKEN_JSON
#   VERT_SIDE, VERT_KIND, VERT_NAME, VERT_DIRECTION, VERT_SHORT_OSI, VERT_LONG_OSI, VERT_QTY
# Optional env (for logging only):
#   VERT_GO, VERT_STRENGTH, VERT_IS_STRONG, VERT_TRADE_DATE, VERT_TDATE,
#   VERT_UNIT_DOLLARS, VERT_OC, VERT_UNITS, CS_LOG_PATH
# Optional timings (defaults shown):
#   VERT_STEP_WAIT=15, VERT_CANCEL_SETTLE=2.0, VERT_MAX_LADDER=3
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
    r = c.get_account_numbers(); r.raise_for_status()
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
        bid = sb - la; ask = sa - lb
    else:
        bid = lb - sa; ask = la - sb
    bid = clamp_tick(bid); ask = clamp_tick(ask)
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
            if oid: return str(oid)
    except Exception:
        pass
    loc = r.headers.get("Location", "")
    return loc.rstrip("/").split("/")[-1] if loc else ""

def _retry_after_seconds(resp, default_wait):
    ra = resp.headers.get("Retry-After")
    if ra:
        try: return max(1.0, float(ra))
        except Exception: pass
    return default_wait

def post_with_retry(c, url, payload, tag="", tries=5):
    last = ""
    for i in range(tries):
        r = c.session.post(url, json=payload, timeout=20)
        if r.status_code in (200, 201, 202): return r
        if r.status_code == 429:
            base = min(12.0, 0.7 * (2 ** i))
            wait = _retry_after_seconds(r, base) + random.uniform(0.0, 0.35)
            print(f"WARN: place failed — HTTP_429 — backoff {wait:.2f}s [{tag}]")
            time.sleep(wait); continue
        last = f"HTTP_{r.status_code}:{(r.text or '')[:200]}"
        time.sleep(min(6.0, 0.45 * (2 ** i)))
    raise RuntimeError(f"POST_FAIL({tag}) {last or 'unknown'}")

def get_status(c, acct_hash: str, oid: str):
    url = f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{oid}"
    try:
        r = c.session.get(url, timeout=20)
        if r.status_code != 200: return {}
        return r.json() or {}
    except Exception:
        return {}

def delete_with_retry(c, url, tag="", tries=6):
    for i in range(tries):
        r = c.session.delete(url, timeout=20)
        if r.status_code in (200, 201, 202, 204): return True
        if r.status_code == 429:
            base = min(8.0, 0.5 * (2 ** i))
            wait = _retry_after_seconds(r, base) + random.uniform(0.0, 0.25)
            print(f"WARN: cancel failed — HTTP_429 — backoff {wait:.2f}s [{tag}]")
            time.sleep(wait); continue
        time.sleep(min(4.0, 0.35 * (2 ** i)))
    return False

def log_row(row: dict):
    path = os.environ.get("CS_LOG_PATH", "logs/constantstable_vertical_trades.csv")
    d = os.path.dirname(path)
    if d and not os.path.exists(d): os.makedirs(d, exist_ok=True)
    cols = [
        "ts_utc","ts_et","trade_date","tdate","name","kind","side","direction",
        "short_osi","long_osi","go","strength","is_strong","unit_dollars","oc","units",
        "qty_requested","qty_filled","ladder_prices","last_price",
        "nbbo_bid","nbbo_ask","nbbo_mid","order_ids","reason"
    ]
    write_header = not os.path.exists(path)
    with open(path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        if write_header: w.writeheader()
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
    is_strong = (os.environ.get("VERT_IS_STRONG", "false") or "").lower() == "true"
    trade_date = os.environ.get("VERT_TRADE_DATE", "")
    tdate = os.environ.get("VERT_TDATE", "")
    unit_d = os.environ.get("VERT_UNIT_DOLLARS", "")
    oc = os.environ.get("VERT_OC", "")
    units = os.environ.get("VERT_UNITS", "")

    # Fixed timings
    STEP_WAIT = float(os.environ.get("VERT_STEP_WAIT", "15"))       # seconds per rung
    CANCEL_SETTLE = float(os.environ.get("VERT_CANCEL_SETTLE", "2.0"))
    MAX_LADDER = int(os.environ.get("VERT_MAX_LADDER", "3"))

    print(f"CS_VERT_PLACE START name={name} side={side} kind={kind} short={short_osi} long={long_osi} qty={qty}")

    c = schwab_client()
    acct_hash = resolve_acct_hash(c)

    # Initial NBBO
    bid, ask, mid = vertical_nbbo(side, short_osi, long_osi, c)
    print(f"CS_VERT_PLACE NBBO: bid={bid} ask={ask} mid={mid}")
    if bid is None and ask is None:
        placed_reason = "NBBO_UNAVAILABLE"
        goutput("placed","0"); goutput("reason", placed_reason); return 0
    if mid is None:
        placed_reason = "NO_MID"
        goutput("placed","0"); goutput("reason", placed_reason); return 0

    # Ladder spec: (offset, refresh_nbbo_on_this_rung)
    if side == "CREDIT":
        ladder_spec = [(0.00, False), (-0.05, False), (-0.05, True)]
    else:
        ladder_spec = [(0.00, False), (+0.05, False), (+0.05, True)]

    def price_from_mid(m, off, b, a):
        p = clamp_tick(m + off)
        if b is not None: p = max(p, b)
        if a is not None: p = min(p, a)
        return p

    preview = []
    for off, refresh in ladder_spec[:MAX_LADDER]:
        preview.append("REFRESH" if refresh else f"{price_from_mid(mid, off, bid, ask):.2f}")
    print(f"CS_VERT_PLACE ladder_plan={preview}")

    url_post = f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders"
    filled, order_ids, last_price = 0, [], None

    for idx, (off, refresh) in enumerate(ladder_spec[:MAX_LADDER], start=1):
        remaining = max(0, qty - filled)
        if remaining <= 0:
            break

        # Refresh NBBO on demand (3rd rung)
        cur_bid, cur_ask, cur_mid = (bid, ask, mid)
        if refresh:
            cur_bid, cur_ask, cur_mid = vertical_nbbo(side, short_osi, long_osi, c)
            print(f"CS_VERT_PLACE REFRESH NBBO: bid={cur_bid} ask={cur_ask} mid={cur_mid}")
            if None in (cur_bid, cur_ask, cur_mid):
                placed_reason = "NBBO_REFRESH_FAIL"
                break

        price = price_from_mid(cur_mid, off, cur_bid, cur_ask)
        last_price = price
        print(f"CS_VERT_PLACE rung#{idx}: price={price:.2f} remaining={remaining} wait={STEP_WAIT:.2f}s")

        payload = order_payload_vertical(side, short_osi, long_osi, price, remaining)
        try:
            r = post_with_retry(c, url_post, payload, tag=f"{name}@{price:.2f}x{remaining}")
        except Exception as e:
            if "HTTP_429" in str(e): saw_429 = True
            print(f"CS_VERT_PLACE ERROR posting order: {e}")
            continue

        oid = parse_order_id(r)
        if oid: order_ids.append(oid)

        # Work the rung
        t_end = time.time() + STEP_WAIT
        while time.time() < t_end and oid:
            st = get_status(c, acct_hash, oid) or {}
            fq = st.get("filledQuantity") or st.get("filled_quantity") or 0
            try: fq = int(round(float(fq)))
            except Exception: fq = 0
            if fq > filled: filled = fq
            s = str(st.get("status") or st.get("orderStatus") or "").upper()
            if s in ("FILLED", "CANCELED", "REJECTED", "EXPIRED"):
                break
            time.sleep(0.4)

        if filled >= qty:
            placed_reason = "OK"
            break

        # Cancel before next rung
        if oid:
            url_del = f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{oid}"
            ok = delete_with_retry(c, url_del, tag=f"CANCEL {oid}")
            print(f"CS_VERT_PLACE CANCEL {oid} → {'OK' if ok else 'FAIL'}")
            time.sleep(CANCEL_SETTLE)

    if filled == 0 and placed_reason == "UNKNOWN":
        placed_reason = "HTTP_429_RATE_LIMIT" if saw_429 else "NO_FILL"

    ts_utc = datetime.now(timezone.utc); ts_et = ts_utc.astimezone(ET)
    ladder_str = ",".join(["REFRESH" if r[1] else f"{clamp_tick(mid + r[0]):.2f}" for r in ladder_spec[:MAX_LADDER]])
    row = {
        "ts_utc": ts_utc.isoformat(), "ts_et": ts_et.isoformat(),
        "trade_date": trade_date, "tdate": tdate, "name": name, "kind": kind,
        "side": side, "direction": direction, "short_osi": short_osi, "long_osi": long_osi,
        "go": go, "strength": strength, "is_strong": str(is_strong).lower(),
        "unit_dollars": unit_d, "oc": oc, "units": units,
        "qty_requested": qty, "qty_filled": filled, "ladder_prices": ladder_str,
        "last_price": f"{last_price:.2f}" if last_price is not None else "",
        "nbbo_bid": "", "nbbo_ask": "", "nbbo_mid": "", "order_ids": ",".join(order_ids),
        "reason": placed_reason,
    }
    b,a,m = vertical_nbbo(side, short_osi, long_osi, c)
    if b is not None: row["nbbo_bid"] = f"{b:.2f}"
    if a is not None: row["nbbo_ask"] = f"{a:.2f}"
    if m is not None: row["nbbo_mid"] = f"{m:.2f}"
    log_row(row)

    print(f"CS_VERT_PLACE DONE name={name} side={side} kind={kind} qty_req={qty} qty_filled={filled} last_price={last_price} reason={placed_reason}")
    goutput("placed", "1" if filled > 0 else "0")
    goutput("reason", placed_reason)
    goutput("qty_filled", str(filled))
    goutput("order_ids", ",".join(order_ids))
    return 0

if __name__ == "__main__":
    sys.exit(main())
