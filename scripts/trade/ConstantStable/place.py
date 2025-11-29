#!/usr/bin/env python3
# CONSTANT STABLE — vertical placer
#  - Places a single SPX vertical (2 legs) as CREDIT or DEBIT
#  - Ladder pricing:
#       CREDIT: mid+0.05, mid, mid-0.05
#       DEBIT : mid-0.05, mid, mid+0.05
#  - Each rung is clamped into NBBO [bid, ask]
#  - Logs to CS_LOG_PATH

import os
import sys
import time
import random
import csv
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from schwab.auth import client_from_token_file

TICK = 0.05
ET = ZoneInfo("America/New_York")


def clamp_tick(x: float) -> float:
    return round(round(float(x) / TICK) * TICK + 1e-12, 2)


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


def post_with_retry(c, url, payload, tag="", tries=3):
    last = ""
    for i in range(tries):
        r = c.session.post(url, json=payload, timeout=20)
        if r.status_code in (200, 201, 202):
            return r
        if r.status_code == 429:
            wait = min(12.0, 0.6 * (2**i)) + random.uniform(0.0, 0.3)
            print(f"WARN: place failed — HTTP_429 — backoff {wait:.2f}s [{tag}]")
            time.sleep(wait)
            continue
        last = f"HTTP_{r.status_code}:{(r.text or '')[:200]}"
        time.sleep(min(6.0, 0.4 * (2**i)))
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
            wait = min(8.0, 0.5 * (2**i)) + random.uniform(0.0, 0.25)
            print(f"WARN: cancel failed — HTTP_429 — backoff {wait:.2f}s [{tag}]")
            time.sleep(wait)
            continue
        time.sleep(min(4.0, 0.3 * (2**i)))
    return False


def log_row(row: dict):
    path = os.environ.get("CS_LOG_PATH", "logs/constantstable_vertical_trades.csv")
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

    cols = [
        "ts_utc",
        "ts_et",
        "trade_date",
        "tdate",
        "name",
        "kind",
        "side",
        "direction",
        "short_osi",
        "long_osi",
        "go",
        "strength",
        "is_strong",
        "unit_dollars",
        "oc",
        "units",
        "qty_requested",
        "qty_filled",
        "ladder_prices",
        "last_price",
        "nbbo_bid",
        "nbbo_ask",
        "nbbo_mid",
        "order_ids",
    ]

    write_header = not os.path.exists(path)
    with open(path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        if write_header:
            w.writeheader()
        w.writerow({k: row.get(k, "") for k in cols})


def main():
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

    STEP_WAIT = float(os.environ.get("VERT_STEP_WAIT", "6"))
    MAX_LADDER_RUNGS = int(os.environ.get("VERT_MAX_LADDER", "3"))

    print(
        f"CS_VERT_PLACE START name={name} side={side} kind={kind} "
        f"short={short_osi} long={long_osi} qty={qty}"
    )

    c, acct_hash = schwab_client()

    bid, ask, mid = vertical_nbbo(side, short_osi, long_osi, c)
    print(f"CS_VERT_PLACE NBBO: bid={bid} ask={ask} mid={mid}")
    if bid is None and ask is None:
        print("CS_VERT_PLACE: no NBBO — abort")
        return 0
    if mid is None:
        print("CS_VERT_PLACE: no mid price — abort")
        return 0

    # Ladder from mid, as requested
    if side == "CREDIT":
        base_ladder = [mid + 0.05, mid, mid - 0.05]
    else:  # DEBIT
        base_ladder = [mid - 0.05, mid, mid + 0.05]

    ladder = []
    seen = set()
    for p in base_ladder:
        p_adj = p
        # Clamp into [bid, ask] if those exist
        if bid is not None:
            p_adj = max(p_adj, bid)
        if ask is not None:
            p_adj = min(p_adj, ask)
        p_adj = clamp_tick(p_adj)
        if p_adj not in seen:
            seen.add(p_adj)
            ladder.append(p_adj)

    print(f"CS_VERT_PLACE ladder={ladder}")
    if not ladder:
        print("CS_VERT_PLACE: empty ladder after clamping — abort")
        return 0

    url_post = f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders"

    filled = 0
    order_ids = []
    last_price = None

    for price in ladder[:MAX_LADDER_RUNGS]:
        remaining = max(0, qty - filled)
        if remaining <= 0:
            break

        last_price = price
        payload = order_payload_vertical(side, short_osi, long_osi, price, remaining)
        print(f"CS_VERT_PLACE rung: price={price:.2f} remaining={remaining}")

        try:
            r = post_with_retry(
                c, url_post, payload, tag=f"{name}@{price:.2f}x{remaining}"
            )
        except Exception as e:
            print(f"CS_VERT_PLACE ERROR posting order: {e}")
            continue

        oid = parse_order_id(r)
        if oid:
            order_ids.append(oid)

        t_end = time.time() + STEP_WAIT
        while time.time() < t_end and oid:
            st = get_status(c, acct_hash, oid)
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
            break

        # Cancel unfilled rung before moving to next price
        if oid:
            url_del = (
                f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{oid}"
            )
            ok = delete_with_retry(c, url_del, tag=f"CANCEL {oid}")
            print(f"CS_VERT_PLACE CANCEL {oid} → {'OK' if ok else 'FAIL'}")

    ts_utc = datetime.now(timezone.utc)
    ts_et = ts_utc.astimezone(ET)
    ladder_str = ",".join(f"{p:.2f}" for p in ladder)

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
        "nbbo_ask": "" if ask is None else f"{ask:.2f}",
        "nbbo_mid": "" if mid is None else f"{mid:.2f}",
        "order_ids": ",".join(order_ids),
    }

    log_row(row)
    print(
        f"CS_VERT_PLACE DONE name={name} side={side} kind={kind} "
        f"qty_req={qty} qty_filled={filled} last_price={last_price}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
