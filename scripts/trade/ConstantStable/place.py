#!/usr/bin/env python3
# CONSTANT STABLE — vertical placer (deadline + refresh-ladder)
#  - Places a single SPX vertical (2 legs) as CREDIT or DEBIT
#  - Ladder pricing (per your spec):
#       CREDIT: mid, mid - 0.05, REFRESH(mid - 0.05)
#       DEBIT : mid, mid + 0.05, REFRESH(mid + 0.05)
#    The REFRESH rung re-pulls NBBO right before sending the order.
#  - Each rung is clamped into NBBO [bid, ask]
#  - Fits inside a time budget or to a hard ET deadline (e.g., 13:15)
#  - Emits GITHUB_OUTPUT tags: placed, reason, qty_filled, order_ids
#
# ENV knobs (all strings):
#   VERT_SIDE            = "CREDIT" | "DEBIT"
#   VERT_KIND            = "PUT" | "CALL" (for logging only)
#   VERT_NAME            = free-form label
#   VERT_DIRECTION       = free-form label
#   VERT_SHORT_OSI       = e.g. "SPXW  251208P06850000"
#   VERT_LONG_OSI        = e.g. "SPXW  251208P06845000"
#   VERT_QTY             = "1" (default)
#   VERT_STEP_WAIT       = seconds to wait per rung if budget allows (default "35")
#   VERT_MAX_LADDER      = max rungs to attempt (default "3")
#   VERT_CANCEL_SETTLE   = seconds to wait after cancel (default "2.0")
#   VERT_BUDGET_SECS     = total seconds budget (e.g., "120"); overrides STEP_WAIT if tighter
#   VERT_MIN_START_HHMM  = e.g., "13:13" (ET). If now<start, sleep until start (or deadline).
#   VERT_DEADLINE_HHMM   = e.g., "13:15" (ET). Rungs auto-shrink to finish before deadline.
#
# Optional pass-throughs for logging:
#   VERT_TRADE_DATE, VERT_TDATE, VERT_UNIT_DOLLARS, VERT_OC, VERT_UNITS,
#   VERT_GO, VERT_STRENGTH, VERT_IS_STRONG
#
# Secrets (required): SCHWAB_APP_KEY, SCHWAB_APP_SECRET, SCHWAB_TOKEN_JSON
#
# Exit semantics: always 0 (so workflow keeps going); use outputs to decide notifications.

import os
import sys
import time
import random
import csv
from datetime import datetime, date, timezone, timedelta
from zoneinfo import ZoneInfo
from schwab.auth import client_from_token_file

TICK = 0.05
ET = ZoneInfo("America/New_York")

# ---------- helpers ----------

def goutput(name: str, val: str):
    path = os.environ.get("GITHUB_OUTPUT")
    if path:
        with open(path, "a") as fh:
            fh.write(f"{name}={val}\n")

def clamp_tick(x: float) -> float:
    return round(round(float(x) / TICK) * TICK + 1e-12, 2)

def _parse_hhmm_local_et(hhmm: str | None) -> float | None:
    """Returns epoch seconds for today at hh:mm ET, or None."""
    if not hhmm:
        return None
    hhmm = hhmm.strip()
    if len(hhmm) < 4 or ":" not in hhmm:
        return None
    try:
        h, m = hhmm.split(":")
        now = datetime.now(ET)
        ts = now.replace(hour=int(h), minute=int(m), second=0, microsecond=0)
        # if we already passed that time today by > 12 hours, assume tomorrow (rare)
        if (now - ts) > timedelta(hours=12):
            ts = ts + timedelta(days=1)
        return ts.astimezone(timezone.utc).timestamp()
    except Exception:
        return None

def now_utc() -> float:
    return time.time()

def seconds_left(deadline_ts: float | None) -> float | None:
    if deadline_ts is None:
        return None
    return max(0.0, deadline_ts - now_utc())

def budgeted_wait(base_wait: float, remaining_rungs: int, cancel_settle: float,
                  budget_secs: float | None) -> float:
    """Pick a rung wait that fits within remaining budget."""
    if budget_secs is None or remaining_rungs <= 0:
        return base_wait
    # leave a small safety margin
    safety = 5.0
    denom = float(remaining_rungs)
    # total per rung includes polling plus a cancel settle
    # approximate rung time = wait + cancel_settle
    per_rung = max(5.0, (budget_secs - safety - (cancel_settle * remaining_rungs)) / denom)
    return max(5.0, min(base_wait, per_rung))

def schwab_client():
    app_key = os.environ["SCHWAB_APP_KEY"]
    app_secret = os.environ["SCHWAB_APP_SECRET"]
    token_json = os.environ["SCHWAB_TOKEN_JSON"]
    with open("schwab_token.json", "w") as f:
        f.write(token_json)
    c = client_from_token_file(api_key=app_key,
                               app_secret=app_secret,
                               token_path="schwab_token.json")
    r = c.get_account_numbers()
    r.raise_for_status()
    info = (r.json() or [{}])[0]
    acct_hash = str(info.get("hashValue") or "")
    return c, acct_hash

def fetch_bid_ask(c, osi: str):
    r = c.get_quote(osi)
    if r.status_code != 200:
        return (None, None)
    d = list(r.json().values())[0] if isinstance(r.json(), dict) else {}
    q = d.get("quote", d)
    b = q.get("bidPrice") or q.get("bid") or q.get("bidPriceInDouble")
    a = q.get("askPrice") or q.get("ask") or q.get("askPriceInDouble")
    return (
        float(b) if b is not None else None,
        float(a) if a is not None else None,
    )

def vertical_nbbo(side: str, short_osi: str, long_osi: str, c):
    """
    NBBO for a vertical built from legs:
      CREDIT: vertical = short - long
      DEBIT : vertical = long - short
    """
    sb, sa = fetch_bid_ask(c, short_osi)
    lb, la = fetch_bid_ask(c, long_osi)
    if None in (sb, sa, lb, la):
        return (None, None, None)

    side = side.upper()
    if side == "CREDIT":
        bid = sb - la
        ask = sa - lb
    else:  # DEBIT
        bid = lb - sa
        ask = la - sb

    bid = clamp_tick(bid)
    ask = clamp_tick(ask)
    mid = clamp_tick((bid + ask) / 2.0)
    return bid, ask, mid

def order_payload_vertical(side: str,
                           short_osi: str,
                           long_osi: str,
                           price: float,
                           qty: int):
    """
    side = "CREDIT" or "DEBIT"
    Always BUY long_osi, SELL short_osi.
    """
    side = side.upper()
    if side not in ("CREDIT", "DEBIT"):
        raise ValueError("VERT_SIDE must be CREDIT or DEBIT")
    order_type = "NET_CREDIT" if side == "CREDIT" else "NET_DEBIT"
    return {
        "orderType": order_type,
        "session": "NORMAL",
        "price": f"{clamp_tick(price):.2f}",
        "duration": "DAY",
        "orderStrategyType": "SINGLE",
        "complexOrderStrategyType": "VERTICAL",
        "orderLegCollection": [
            {
                "instruction": "BUY_TO_OPEN",
                "positionEffect": "OPENING",
                "quantity": qty,
                "instrument": {"symbol": long_osi, "assetType": "OPTION"},
            },
            {
                "instruction": "SELL_TO_OPEN",
                "positionEffect": "OPENING",
                "quantity": qty,
                "instrument": {"symbol": short_osi, "assetType": "OPTION"},
            },
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
            base = min(12.0, 0.7 * (2 ** i))
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

def delete_with_retry(c, url, tag="", tries=6):
    for i in range(tries):
        r = c.session.delete(url, timeout=20)
        if r.status_code in (200, 201, 202, 204):
            return True
        if r.status_code == 429:
            base = min(8.0, 0.5 * (2 ** i))
            wait = _retry_after_seconds(r, base) + random.uniform(0.0, 0.25)
            print(f"WARN: cancel failed — HTTP_429 — backoff {wait:.2f}s [{tag}]")
            time.sleep(wait)
            continue
        time.sleep(min(4.0, 0.35 * (2 ** i)))
    return False

def log_row(row: dict):
    path = os.environ.get("CS_LOG_PATH", "logs/constantstable_vertical_trades.csv")
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)
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

    qty_raw = os.environ.get("VERT_QTY", "1")
    try:
        qty = max(1, int(qty_raw))
    except Exception:
        qty = 1

    go = os.environ.get("VERT_GO", "")
    strength = os.environ.get("VERT_STRENGTH", "")
    is_strong = os.environ.get("VERT_IS_STRONG", "false").lower() == "true"
    trade_date = os.environ.get("VERT_TRADE_DATE", "")
    tdate = os.environ.get("VERT_TDATE", "")
    unit_d = os.environ.get("VERT_UNIT_DOLLARS", "")
    oc = os.environ.get("VERT_OC", "")
    units = os.environ.get("VERT_UNITS", "")

    STEP_WAIT = float(os.environ.get("VERT_STEP_WAIT", "35"))
    MAX_LADDER_RUNGS = int(os.environ.get("VERT_MAX_LADDER", "3"))
    CANCEL_SETTLE = float(os.environ.get("VERT_CANCEL_SETTLE", "2.0"))
    BUDGET_SECS_env = os.environ.get("VERT_BUDGET_SECS", "").strip()
    BUDGET_SECS = float(BUDGET_SECS_env) if BUDGET_SECS_env else None

    start_not_before = _parse_hhmm_local_et(os.environ.get("VERT_MIN_START_HHMM"))
    hard_deadline = _parse_hhmm_local_et(os.environ.get("VERT_DEADLINE_HHMM"))

    # Optional: wait until not-before time (but don't wait past deadline)
    if start_not_before is not None:
        while True:
            now = now_utc()
            if hard_deadline is not None and now >= hard_deadline:
                break
            if now >= start_not_before:
                break
            sleep_for = min(0.5, start_not_before - now)
            if sleep_for <= 0: break
            time.sleep(sleep_for)

    print(
        f"CS_VERT_PLACE START name={name} side={side} kind={kind} "
        f"short={short_osi} long={long_osi} qty={qty}"
    )

    c, acct_hash = schwab_client()

    # Initial NBBO
    bid, ask, mid = vertical_nbbo(side, short_osi, long_osi, c)
    print(f"CS_VERT_PLACE NBBO: bid={bid} ask={ask} mid={mid}")
    if bid is None and ask is None:
        placed_reason = "NBBO_UNAVAILABLE"
        print("CS_VERT_PLACE: no NBBO — abort")
        goutput("placed","0"); goutput("reason", placed_reason)
        return 0
    if mid is None:
        placed_reason = "NO_MID"
        print("CS_VERT_PLACE: no mid price — abort")
        goutput("placed","0"); goutput("reason", placed_reason)
        return 0

    # Build ladder spec: (offset, refresh_nbbo?)
    if side == "CREDIT":
        ladder_spec = [(0.00, False), (-0.05, False), (-0.05, True)]
    else:  # DEBIT
        ladder_spec = [(0.00, False), (+0.05, False), (+0.05, True)]

    # Clamp with current NBBO when not refreshing; refresh rung re-pulls NBBO
    def price_from_mid(m, offset, bid, ask):
        p = clamp_tick(m + offset)
        if bid is not None: p = max(p, bid)
        if ask is not None: p = min(p, ask)
        return p

    prices_preview = []
    for off, refresh in ladder_spec[:MAX_LADDER_RUNGS]:
        if refresh:
            prices_preview.append("REFRESH")
        else:
            prices_preview.append(f"{price_from_mid(mid, off, bid, ask):.2f}")
    print(f"CS_VERT_PLACE ladder_plan={prices_preview}")

    url_post = f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders"

    filled = 0
    order_ids = []
    last_price = None

    started_ts = now_utc()

    for idx, (offset, refresh) in enumerate(ladder_spec[:MAX_LADDER_RUNGS], start=1):
        remaining = max(0, qty - filled)
        if remaining <= 0:
            break

        # Refresh NBBO on demand (3rd rung by design)
        if refresh:
            bid, ask, mid = vertical_nbbo(side, short_osi, long_osi, c)
            print(f"CS_VERT_PLACE REFRESH NBBO: bid={bid} ask={ask} mid={mid}")
            if bid is None or ask is None or mid is None:
                placed_reason = "NBBO_REFRESH_FAIL"
                break

        price = price_from_mid(mid, offset, bid, ask)
        last_price = price

        # Compute time budget left
        left_by_deadline = seconds_left(hard_deadline)
        if left_by_deadline is not None:
            # reserve cancel settle + small safety for this rung
            left_for_rungs = max(0.0, left_by_deadline - (CANCEL_SETTLE + 1.0))
        else:
            left_for_rungs = None

        # Derive budget left total (min of explicit budget and deadline)
        explicit_budget_left = None
        if BUDGET_SECS is not None:
            explicit_budget_left = max(0.0, BUDGET_SECS - (now_utc() - started_ts))
        if left_for_rungs is not None and explicit_budget_left is not None:
            budget_left = min(left_for_rungs, explicit_budget_left)
        else:
            budget_left = left_for_rungs if left_for_rungs is not None else explicit_budget_left

        rung_wait = budgeted_wait(STEP_WAIT, (qty - filled > 0) + (MAX_LADDER_RUNGS - idx), CANCEL_SETTLE, budget_left)

        print(f"CS_VERT_PLACE rung#{idx}: price={price:.2f} remaining={remaining} wait={rung_wait:.2f}s")

        payload = order_payload_vertical(side, short_osi, long_osi, price, remaining)
        try:
            r = post_with_retry(c, url_post, payload, tag=f"{name}@{price:.2f}x{remaining}")
        except Exception as e:
            msg = str(e)
            if "HTTP_429" in msg:
                saw_429 = True
            print(f"CS_VERT_PLACE ERROR posting order: {e}")
            # continue to next rung if possible
            continue

        oid = parse_order_id(r)
        if oid:
            order_ids.append(oid)

        # Poll for fill or status change up to rung_wait
        t_end = now_utc() + rung_wait
        while now_utc() < t_end and oid:
            st = get_status(c, acct_hash, oid) or {}
            fq = st.get("filledQuantity") or st.get("filled_quantity") or 0
            try:
                fq_int = int(round(float(fq)))
            except Exception:
                fq_int = 0
            if fq_int > filled:
                filled = fq_int
            s = str(st.get("status") or st.get("orderStatus") or "").upper()
            if s in ("FILLED", "CANCELED", "REJECTED", "EXPIRED"):
                break
            time.sleep(0.4)

        if filled >= qty:
            placed_reason = "OK"
            break

        # If a hard deadline exists and we've run out of time, stop
        if left_by_deadline is not None and seconds_left(hard_deadline) <= (CANCEL_SETTLE + 1.0):
            placed_reason = "DEADLINE_HIT"
            # attempt cancel if still working
            if oid:
                url_del = f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{oid}"
                ok = delete_with_retry(c, url_del, tag=f"CANCEL {oid}")
                print(f"CS_VERT_PLACE CANCEL {oid} → {'OK' if ok else 'FAIL'}")
            break

        # Cancel unfilled rung before moving to next price
        if oid:
            url_del = f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{oid}"
            ok = delete_with_retry(c, url_del, tag=f"CANCEL {oid}")
            print(f"CS_VERT_PLACE CANCEL {oid} → {'OK' if ok else 'FAIL'}")
            time.sleep(CANCEL_SETTLE)

    if filled == 0 and placed_reason == "UNKNOWN":
        placed_reason = "HTTP_429_RATE_LIMIT" if saw_429 else "NO_FILL"

    ts_utc = datetime.now(timezone.utc)
    ts_et = ts_utc.astimezone(ET)

    ladder_str = ",".join(
        ["REFRESH" if r[1] else f"{clamp_tick(mid + r[0]):.2f}" for r in ladder_spec[:MAX_LADDER_RUNGS]]
    )

    row = {
        "ts_utc": ts_utc.isoformat(),
        "ts_et": ts_et.isoformat(),
        "trade_date": trade_date,
        "tdate": tdate,
        "name": name,
        "kind": kind,
        "side": side,
        "direction": direction,
        "short_osi": short_osi,
        "long_osi": long_osi,
        "go": go,
        "strength": strength,
        "is_strong": str(is_strong).lower(),
        "unit_dollars": unit_d,
        "oc": oc,
        "units": units,
        "qty_requested": qty,
        "qty_filled": filled,
        "ladder_prices": ladder_str,
        "last_price": f"{last_price:.2f}" if last_price is not None else "",
        "nbbo_bid": "" if bid is None else f"{bid:.2f}",
        "nbbo_ask": "" if ask is not None else f"{ask:.2f}",
        "nbbo_mid": "" if mid is None else f"{mid:.2f}",
        "order_ids": ",".join(order_ids),
        "reason": placed_reason,
    }
    log_row(row)

    print(
        f"CS_VERT_PLACE DONE name={name} side={side} kind={kind} "
        f"qty_req={qty} qty_filled={filled} last_price={last_price} reason={placed_reason}"
    )

    # GitHub outputs for downstream notify
    goutput("placed", "1" if filled > 0 else "0")
    goutput("reason", placed_reason)
    goutput("qty_filled", str(filled))
    goutput("order_ids", ",".join(order_ids))

    return 0

if __name__ == "__main__":
    sys.exit(main())
