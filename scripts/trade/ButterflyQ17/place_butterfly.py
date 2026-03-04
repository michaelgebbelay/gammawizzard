#!/usr/bin/env python3
# BUTTERFLY Q17 — placer (3-leg ATM call butterfly)
#
# Reads BF_* env vars from orchestrator_bf.py and places the butterfly via Schwab API.
#
# Ladder (same as CS):
#   BUY  butterfly: mid, mid+0.05, mid+0.10 (debit — pay more each rung)
#   SELL butterfly: mid, mid-0.05, mid-0.10 (credit — accept less each rung)
#
# Timing knobs via env:
#   BF_STEP_WAIT=12
#   BF_POLL_SECS=1.5
#   BF_CANCEL_SETTLE=1.0
#   BF_MAX_LADDER=3
#   BF_CANCEL_TRIES=4
#   BF_DRY_RUN=true/false

import os, sys, time, random, csv
from datetime import datetime, timezone
from zoneinfo import ZoneInfo


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
TAG = "BF_Q17_PLACE"


# ---------- utils ----------

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
    if bid is not None and bid > 0:
        p = max(p, bid)
    if ask is not None and ask > 0:
        p = min(p, ask)
    return max(0.0, clamp_tick(p))


# ---------- Schwab API helpers ----------

def resolve_acct_hash(c):
    ah = (os.environ.get("SCHWAB_ACCT_HASH") or "").strip()
    if ah:
        return ah
    r = c.get_account_numbers()
    r.raise_for_status()
    arr = r.json() or []
    return str((arr[0] or {}).get("hashValue") or "")


def get_quote_json_with_retry(c, osi: str, tries: int = 4):
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


def butterfly_nbbo(c, direction: str, lower_osi: str, center_osi: str, upper_osi: str):
    """
    Compute butterfly spread NBBO from individual leg quotes.

    BUY butterfly (long): BUY 1 lower + SELL 2 center + BUY 1 upper
      debit_bid  = lo_bid + hi_bid - 2*mid_ask  (cheapest, best for buyer)
      debit_ask  = lo_ask + hi_ask - 2*mid_bid  (most expensive)

    SELL butterfly (short): SELL 1 lower + BUY 2 center + SELL 1 upper
      credit_bid = lo_bid + hi_bid - 2*mid_ask  (least credit, worst for seller)
      credit_ask = lo_ask + hi_ask - 2*mid_bid  (most credit, best for seller)

    Returns (bid, ask, mid) — always positive (debit amount or credit amount).
    """
    lb, la = fetch_bid_ask(c, lower_osi)
    mb, ma = fetch_bid_ask(c, center_osi)
    ub, ua = fetch_bid_ask(c, upper_osi)

    if None in (lb, la, mb, ma, ub, ua):
        return (None, None, None)

    # For this 1:-2:1 call fly, long-fly debit and short-fly credit share
    # the same numeric NBBO range; only order side/type differs.
    bid = lb + ub - 2 * ma   # minimum achievable debit / minimum credit
    ask = la + ua - 2 * mb   # maximum achievable debit / maximum credit

    bid = max(0.0, clamp_tick(bid))
    ask = max(bid, clamp_tick(ask))
    mid = clamp_tick((bid + ask) / 2.0)
    return bid, ask, mid


def _has_positive_mid(mid) -> bool:
    return mid is not None and float(mid) > 0.0


def _chain_quote_from_env():
    bid = _fnum(os.environ.get("BF_CHAIN_BID", ""))
    ask = _fnum(os.environ.get("BF_CHAIN_ASK", ""))
    mid = _fnum(os.environ.get("BF_CHAIN_MID", ""))

    if mid is not None and mid > 0:
        mid = clamp_tick(mid)
    else:
        mid = None

    if bid is not None and bid > 0:
        bid = clamp_tick(bid)
    else:
        bid = None

    if ask is not None and ask > 0:
        ask = clamp_tick(ask)
    else:
        ask = None

    if mid is None and bid is not None and ask is not None and ask > 0:
        mid = clamp_tick((bid + ask) / 2.0)
    if bid is None and mid is not None and ask is not None:
        bid = max(0.0, clamp_tick(min(mid, ask - TICK)))
    if ask is None and mid is not None and bid is not None:
        ask = max(mid, clamp_tick(max(mid, bid + TICK)))

    if _has_positive_mid(mid):
        return float(bid), float(ask), float(mid)
    return (bid, ask, None)


def resolve_butterfly_quote(c, direction: str, lower_osi: str, center_osi: str, upper_osi: str):
    bid, ask, mid = butterfly_nbbo(c, direction, lower_osi, center_osi, upper_osi)
    if _has_positive_mid(mid):
        return bid, ask, mid, "LIVE_NBBO"

    cbid, cask, cmid = _chain_quote_from_env()
    if _has_positive_mid(cmid):
        return cbid, cask, cmid, "CHAIN_FALLBACK"

    return bid, ask, mid, "NO_VALID_MID"


def order_payload_butterfly(direction: str, lower_osi: str, center_osi: str,
                            upper_osi: str, price: float, qty: int):
    """
    Build Schwab butterfly order payload.
    direction = "BUY" -> NET_DEBIT, buy wings, sell center
    direction = "SELL" -> NET_CREDIT, sell wings, buy center
    """
    if direction == "BUY":
        order_type = "NET_DEBIT"
        wing_inst = "BUY_TO_OPEN"
        center_inst = "SELL_TO_OPEN"
    else:
        order_type = "NET_CREDIT"
        wing_inst = "SELL_TO_OPEN"
        center_inst = "BUY_TO_OPEN"

    return {
        "orderType": order_type,
        "session": "NORMAL",
        "price": f"{clamp_tick(price):.2f}",
        "duration": "DAY",
        "orderStrategyType": "SINGLE",
        "complexOrderStrategyType": "BUTTERFLY",
        "orderLegCollection": [
            {"instruction": wing_inst, "positionEffect": "OPENING", "quantity": qty,
             "instrument": {"symbol": lower_osi, "assetType": "OPTION"}},
            {"instruction": center_inst, "positionEffect": "OPENING", "quantity": qty * 2,
             "instrument": {"symbol": center_osi, "assetType": "OPTION"}},
            {"instruction": wing_inst, "positionEffect": "OPENING", "quantity": qty,
             "instrument": {"symbol": upper_osi, "assetType": "OPTION"}},
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


# ---------- logging ----------

def log_row(row: dict):
    path = os.environ.get("BF_LOG_PATH", "logs/butterfly_q17_trades.csv")
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

    cols = [
        "ts_utc", "ts_et", "trade_date", "expiration",
        "direction", "action", "bucket",
        "lower_osi", "center_osi", "upper_osi",
        "width", "atm_strike", "spot", "em", "em_mult",
        "vix", "vix1d",
        "unit_dollars", "equity", "units",
        "qty_requested", "qty_filled",
        "ladder_prices", "last_price",
        "nbbo_bid", "nbbo_ask", "nbbo_mid",
        "order_ids", "reason",
    ]

    write_header = not os.path.exists(path)
    with open(path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        if write_header:
            w.writeheader()
        w.writerow({k: row.get(k, "") for k in cols})


# ---------- core placement ----------

def place_butterfly_with_ladder(
    c,
    acct_hash: str,
    direction: str,
    qty: int,
    lower_osi: str,
    center_osi: str,
    upper_osi: str,
):
    placed_reason = "UNKNOWN"
    saw_429 = False
    danger_stray_order = False

    STEP_WAIT = float(os.environ.get("BF_STEP_WAIT", "12"))
    POLL_SECS = float(os.environ.get("BF_POLL_SECS", "1.5"))
    CANCEL_SETTLE = float(os.environ.get("BF_CANCEL_SETTLE", "1.0"))
    MAX_LADDER = int(os.environ.get("BF_MAX_LADDER", "3"))
    CANCEL_TRIES = int(os.environ.get("BF_CANCEL_TRIES", "4"))

    url_post = f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders"

    # Initial quote (live NBBO, then chain fallback if needed)
    bid, ask, mid, quote_src = resolve_butterfly_quote(
        c, direction, lower_osi, center_osi, upper_osi,
    )
    if not _has_positive_mid(mid):
        return {
            "filled": 0, "order_ids": [], "last_price": None,
            "nbbo_bid": bid, "nbbo_ask": ask, "nbbo_mid": mid,
            "reason": "NBBO_UNAVAILABLE_OR_NONPOSITIVE", "danger": False,
        }

    print(f"{TAG} NBBO: bid={bid} ask={ask} mid={mid} direction={direction} source={quote_src}")

    # Ladder: BUY = debit (pay more each rung), SELL = credit (accept less each rung)
    if direction == "SELL":
        ladder_spec = [(0.00, False), (-0.05, False), (-0.10, True)]
    else:
        ladder_spec = [(0.00, False), (+0.05, False), (+0.10, True)]

    preview = []
    for off, refresh in ladder_spec[:MAX_LADDER]:
        preview.append("REFRESH" if refresh else f"{price_from_mid(mid, off, bid, ask):.2f}")
    preview_str = "[" + ", ".join(preview) + "]"
    print(f"{TAG} ladder_plan={preview_str}")

    filled_total = 0
    order_ids = []
    last_price = None

    for idx, (off, refresh) in enumerate(ladder_spec[:MAX_LADDER], start=1):
        remaining = max(0, qty - filled_total)
        if remaining <= 0:
            break

        cur_bid, cur_ask, cur_mid = (bid, ask, mid)
        if refresh:
            r_bid, r_ask, r_mid, cur_src = resolve_butterfly_quote(
                c, direction, lower_osi, center_osi, upper_osi,
            )
            print(f"{TAG} REFRESH NBBO: bid={r_bid} ask={r_ask} mid={r_mid} source={cur_src}")
            if _has_positive_mid(r_mid):
                cur_bid, cur_ask, cur_mid = r_bid, r_ask, r_mid
            else:
                print(f"{TAG} REFRESH WARN: no positive mid from refresh; keeping prior quote")

        price = price_from_mid(cur_mid, off, cur_bid, cur_ask)
        last_price = price
        print(f"{TAG} rung#{idx}: price={price:.2f} remaining={remaining} wait={STEP_WAIT:.2f}s")

        payload = order_payload_butterfly(
            direction, lower_osi, center_osi, upper_osi, price, remaining,
        )

        try:
            r = post_with_retry(c, url_post, payload, tag=f"BF@{price:.2f}x{remaining}")
        except Exception as e:
            if "HTTP_429" in str(e):
                saw_429 = True
            print(f"{TAG} ERROR posting order: {e}")
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
                fq = remaining

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
            print(f"{TAG} CANCEL {oid} -> {'OK' if ok else 'FAIL'}")

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

    # final nbbo snapshot
    b2, a2, m2, _ = resolve_butterfly_quote(c, direction, lower_osi, center_osi, upper_osi)
    return {
        "filled": filled_total,
        "order_ids": order_ids,
        "last_price": last_price,
        "nbbo_bid": b2 if b2 is not None else bid,
        "nbbo_ask": a2 if a2 is not None else ask,
        "nbbo_mid": m2 if m2 is not None else mid,
        "reason": placed_reason,
        "danger": danger_stray_order,
        "ladder_plan": preview_str,
    }


# ---------- main ----------

def main():
    direction = (os.environ.get("BF_DIRECTION") or "").strip().upper()
    lower_osi = (os.environ.get("BF_LOWER_OSI") or "").strip()
    center_osi = (os.environ.get("BF_CENTER_OSI") or "").strip()
    upper_osi = (os.environ.get("BF_UPPER_OSI") or "").strip()
    qty = int(os.environ.get("BF_QTY", "1"))
    dry_run = truthy(os.environ.get("BF_DRY_RUN", ""))

    if direction not in ("BUY", "SELL"):
        print(f"{TAG} FATAL: BF_DIRECTION must be BUY or SELL, got '{direction}'")
        return 1

    if not (lower_osi and center_osi and upper_osi):
        print(f"{TAG} FATAL: BF_LOWER_OSI, BF_CENTER_OSI, BF_UPPER_OSI required")
        return 1

    print(f"{TAG} START: {direction} {qty}x butterfly")
    print(f"{TAG}   lower  = {lower_osi}")
    print(f"{TAG}   center = {center_osi} x2")
    print(f"{TAG}   upper  = {upper_osi}")

    # Context from orchestrator
    width = os.environ.get("BF_WIDTH", "")
    atm_strike = os.environ.get("BF_ATM_STRIKE", "")
    spot = os.environ.get("BF_SPOT", "")
    em = os.environ.get("BF_EM", "")
    em_mult = os.environ.get("BF_EM_MULT", "")
    vix = os.environ.get("BF_VIX", "")
    vix1d = os.environ.get("BF_VIX1D", "")
    bucket = os.environ.get("BF_BUCKET", "")
    action = os.environ.get("BF_ACTION", "")
    expiration = os.environ.get("BF_EXPIRATION", "")
    unit_dollars = os.environ.get("BF_UNIT_DOLLARS_V", "")
    equity = os.environ.get("BF_EQUITY", "")
    units = os.environ.get("BF_UNITS", "")
    dry_run_reason = (os.environ.get("BF_DRY_RUN_REASON", "DRY_RUN") or "DRY_RUN").strip()

    c = schwab_client()
    acct_hash = resolve_acct_hash(c)
    print(f"{TAG} ACCT_HASH: {acct_hash[:8]}...")

    if dry_run:
        bid, ask, mid, quote_src = resolve_butterfly_quote(
            c, direction, lower_osi, center_osi, upper_osi,
        )
        print(f"{TAG} DRY_RUN: NBBO bid={bid} ask={ask} mid={mid} source={quote_src}")
        print(f"{TAG} DRY_RUN: would place {direction} {qty}x @ mid={mid}")
        resolved_reason = dry_run_reason if _has_positive_mid(mid) else f"{dry_run_reason}_NO_VALID_MID"

        # Log DRY_RUN trade for paper tracking
        now_utc = datetime.now(timezone.utc)
        now_et = now_utc.astimezone(ET)
        log_row({
            "ts_utc": now_utc.strftime("%Y-%m-%d %H:%M:%S"),
            "ts_et": now_et.strftime("%Y-%m-%d %H:%M:%S"),
            "trade_date": now_et.strftime("%Y-%m-%d"),
            "expiration": expiration,
            "direction": direction,
            "action": action,
            "bucket": bucket,
            "lower_osi": lower_osi,
            "center_osi": center_osi,
            "upper_osi": upper_osi,
            "width": width,
            "atm_strike": atm_strike,
            "spot": spot,
            "em": em,
            "em_mult": em_mult,
            "vix": vix,
            "vix1d": vix1d,
            "unit_dollars": unit_dollars,
            "equity": equity,
            "units": units,
            "qty_requested": str(qty),
            "qty_filled": "0",
            "ladder_prices": "",
            "last_price": "" if mid is None else f"{mid:.2f}",
            "nbbo_bid": "" if bid is None else f"{bid:.2f}",
            "nbbo_ask": "" if ask is None else f"{ask:.2f}",
            "nbbo_mid": "" if mid is None else f"{mid:.2f}",
            "order_ids": "",
            "reason": resolved_reason,
        })
        return 0

    result = place_butterfly_with_ladder(
        c, acct_hash, direction, qty, lower_osi, center_osi, upper_osi,
    )

    filled = result["filled"]
    reason = result["reason"]
    oids = result["order_ids"]
    danger = result.get("danger", False)

    print(f"{TAG} RESULT: filled={filled}/{qty} reason={reason} orders={oids}")
    if danger:
        print(f"{TAG} DANGER: stray order may still be working!")

    # Log
    now_utc = datetime.now(timezone.utc)
    now_et = now_utc.astimezone(ET)

    log_row({
        "ts_utc": now_utc.strftime("%Y-%m-%d %H:%M:%S"),
        "ts_et": now_et.strftime("%Y-%m-%d %H:%M:%S"),
        "trade_date": now_et.strftime("%Y-%m-%d"),
        "expiration": expiration,
        "direction": direction,
        "action": action,
        "bucket": bucket,
        "lower_osi": lower_osi,
        "center_osi": center_osi,
        "upper_osi": upper_osi,
        "width": width,
        "atm_strike": atm_strike,
        "spot": spot,
        "em": em,
        "em_mult": em_mult,
        "vix": vix,
        "vix1d": vix1d,
        "unit_dollars": unit_dollars,
        "equity": equity,
        "units": units,
        "qty_requested": str(qty),
        "qty_filled": str(filled),
        "ladder_prices": result.get("ladder_plan", ""),
        "last_price": "" if result.get("last_price") is None else f"{result['last_price']:.2f}",
        "nbbo_bid": "" if result.get("nbbo_bid") is None else f"{result['nbbo_bid']:.2f}",
        "nbbo_ask": "" if result.get("nbbo_ask") is None else f"{result['nbbo_ask']:.2f}",
        "nbbo_mid": "" if result.get("nbbo_mid") is None else f"{result['nbbo_mid']:.2f}",
        "order_ids": ",".join(str(o) for o in oids),
        "reason": reason,
    })

    return 0


if __name__ == "__main__":
    sys.exit(main())
