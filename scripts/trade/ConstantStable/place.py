#!/usr/bin/env python3
# PLACER — ConstantStable verticals (per-leg).
#
# Env from orchestrator:
#   CS_SIDE_PUT    = "CREDIT" / "DEBIT" / "" (skip)
#   CS_SIDE_CALL   = same
#   CS_QTY_PUT     = int
#   CS_QTY_CALL    = int
#   CS_PUT_LOW_OSI, CS_PUT_HIGH_OSI
#   CS_CALL_LOW_OSI, CS_CALL_HIGH_OSI
#   CS_STRONG_PUT, CS_STRONG_CALL = "true"/"false"
#   CS_GO_PUT, CS_GO_CALL         = Go values or ""
#   CS_SIGNAL_DATE, CS_EXPIRY_ISO
#   CS_ACCOUNT_EQUITY
#
# Pricing:
#   DEBIT  (long vertical)  → ladder [0.95, 1.00, 1.05]
#   CREDIT (short vertical) → ladder [1.05, 1.00, 0.95]
#
# Logging:
#   CSV at CS10K_LOG_PATH (default ConstantStable_10k_vertical_trades.csv)

import os, sys, time, random
from datetime import datetime, timezone
from schwab.auth import client_from_token_file

TICK = 0.05

DEBIT_LADDER  = [0.95, 1.00, 1.05]
CREDIT_LADDER = [1.05, 1.00, 0.95]

STEP_WAIT_SECS    = float(os.environ.get("STEP_WAIT_SECS", "8"))
MAX_LADDER_STEPS  = int(os.environ.get("MAX_LADDER_STEPS", "3"))
ORDER_CHECK_SLEEP = float(os.environ.get("ORDER_CHECK_SLEEP", "0.5"))

LOG_PATH = os.environ.get("CS10K_LOG_PATH", "ConstantStable_10k_vertical_trades.csv")

def clamp_tick(x: float) -> float:
    return round(round(float(x) / TICK) * TICK + 1e-12, 2)

def schwab_client():
    app_key    = os.environ["SCHWAB_APP_KEY"]
    app_secret = os.environ["SCHWAB_APP_SECRET"]
    token_json = os.environ["SCHWAB_TOKEN_JSON"]
    with open("schwab_token.json","w") as f:
        f.write(token_json)
    c = client_from_token_file(api_key=app_key, app_secret=app_secret, token_path="schwab_token.json")
    r = c.get_account_numbers(); r.raise_for_status()
    acct_info = (r.json() or [])[0]
    acct_hash = str(acct_info["hashValue"])
    acct_num  = str(acct_info.get("accountNumber") or acct_info.get("account_number") or "")
    return c, acct_hash, acct_num

def strike_from_osi(osi: str) -> float:
    return int(osi[-8:]) / 1000.0

def parse_order_id(r):
    try:
        j = r.json()
        if isinstance(j, dict):
            oid = j.get("orderId") or j.get("order_id")
            if oid:
                return str(oid)
    except Exception:
        pass
    loc = r.headers.get("Location","")
    return loc.rstrip("/").split("/")[-1] if loc else ""

def delete_with_retry(c, url, tries=4):
    for i in range(tries):
        r = c.session.delete(url, timeout=20)
        if r.status_code in (200,201,202,204):
            return True
        if r.status_code == 429:
            wait = min(8.0, 0.5*(2**i)) + random.uniform(0.0,0.25)
            print(f"WARN: delete HTTP_429 backoff {wait:.2f}s")
            time.sleep(wait); continue
        time.sleep(min(4.0, 0.3*(2**i)))
    return False

def get_status(c, acct_hash: str, oid: str) -> dict:
    url = f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{oid}"
    try:
        r = c.session.get(url, timeout=20)
        if r.status_code != 200:
            return {}
        return r.json() or {}
    except Exception:
        return {}

def post_with_retry(c, url, payload, tag="", tries=5):
    last = ""
    for i in range(tries):
        r = c.session.post(url, json=payload, timeout=20)
        if r.status_code in (200,201,202):
            return r
        if r.status_code == 429:
            wait = min(12.0, 0.6*(2**i)) + random.uniform(0.0,0.3)
            print(f"WARN: POST_FAIL_429({tag}) backoff {wait:.2f}s")
            time.sleep(wait); continue
        last = f"HTTP_{r.status_code}:{(r.text or '')[:200]}"
        time.sleep(min(6.0, 0.4*(2**i)))
    raise RuntimeError(f"POST_FAIL({tag}) {last or 'unknown'}")

def make_vertical_payload(side: str, kind: str, low_osi: str, high_osi: str, price: float, qty: int):
    """
    side: "CREDIT" / "DEBIT"
    kind: "PUT" / "CALL"

    PUT:
      DEBIT  (long)  → buy high, sell low
      CREDIT (short) → sell high, buy low
    CALL:
      DEBIT  (long)  → buy low, sell high
      CREDIT (short) → sell low, buy high
    """
    side = side.upper()
    kind = kind.upper()
    if side not in ("CREDIT","DEBIT"):
        raise ValueError("side must be CREDIT or DEBIT")

    if kind == "PUT":
        if side == "DEBIT":
            legs = [
                {"instruction":"BUY_TO_OPEN",  "positionEffect":"OPENING","quantity":qty,
                 "instrument":{"symbol":high_osi,"assetType":"OPTION"}},
                {"instruction":"SELL_TO_OPEN", "positionEffect":"OPENING","quantity":qty,
                 "instrument":{"symbol":low_osi,"assetType":"OPTION"}},
            ]
            order_type = "NET_DEBIT"
        else:
            legs = [
                {"instruction":"SELL_TO_OPEN", "positionEffect":"OPENING","quantity":qty,
                 "instrument":{"symbol":high_osi,"assetType":"OPTION"}},
                {"instruction":"BUY_TO_OPEN",  "positionEffect":"OPENING","quantity":qty,
                 "instrument":{"symbol":low_osi,"assetType":"OPTION"}},
            ]
            order_type = "NET_CREDIT"
    else:  # CALL
        if side == "DEBIT":
            legs = [
                {"instruction":"BUY_TO_OPEN",  "positionEffect":"OPENING","quantity":qty,
                 "instrument":{"symbol":low_osi,"assetType":"OPTION"}},
                {"instruction":"SELL_TO_OPEN", "positionEffect":"OPENING","quantity":qty,
                 "instrument":{"symbol":high_osi,"assetType":"OPTION"}},
            ]
            order_type = "NET_DEBIT"
        else:
            legs = [
                {"instruction":"SELL_TO_OPEN", "positionEffect":"OPENING","quantity":qty,
                 "instrument":{"symbol":low_osi,"assetType":"OPTION"}},
                {"instruction":"BUY_TO_OPEN",  "positionEffect":"OPENING","quantity":qty,
                 "instrument":{"symbol":high_osi,"assetType":"OPTION"}},
            ]
            order_type = "NET_CREDIT"

    return {
        "orderType": order_type,
        "session": "NORMAL",
        "price": f"{clamp_tick(price):.2f}",
        "duration": "DAY",
        "orderStrategyType": "SINGLE",
        "complexOrderStrategyType": "VERTICAL",
        "orderLegCollection": legs,
    }

def place_vertical_with_ladder(c, acct_hash: str, side: str, kind: str,
                               low_osi: str, high_osi: str, qty: int,
                               ladder_prices: list[float]):
    """
    Place a single vertical (PUT or CALL) using a simple ladder.
    Returns (final_status, order_id, filled_qty).
    """
    url_post = f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders"
    oid = ""
    filled = 0
    last_status = "NOT_FILLED"

    prices = ladder_prices[:MAX_LADDER_STEPS]

    for price in prices:
        if qty <= 0:
            break

        payload = make_vertical_payload(side, kind, low_osi, high_osi, price, qty)
        print(f"{kind} {side} VERTICAL rung price={price:.2f} qty={qty}")
        try:
            r = post_with_retry(c, url_post, payload, tag=f"{kind}_{side}@{price:.2f}x{qty}")
        except Exception as e:
            print(f"{kind} {side} ERROR posting order:", e)
            last_status = "ERROR"
            continue

        oid = parse_order_id(r)
        if not oid:
            print("WARN: no orderId returned")
            last_status = "ERROR"
            continue

        t_end = time.time() + STEP_WAIT_SECS
        while time.time() < t_end:
            st = get_status(c, acct_hash, oid)
            s  = str(st.get("status") or st.get("orderStatus") or "").upper()
            fq = int(round(float(st.get("filledQuantity") or st.get("filled_quantity") or 0)))
            filled = fq
            if s in ("FILLED","REJECTED","CANCELED","EXPIRED"):
                last_status = s if s != "" else last_status
                break
            time.sleep(ORDER_CHECK_SLEEP)

        if last_status == "FILLED" and filled >= qty:
            return ("FILLED", oid, filled)

        # Cancel and try next rung
        url_del = f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{oid}"
        ok = delete_with_retry(c, url_del)
        print(f"{kind} {side} CANCEL {oid} → {'OK' if ok else 'FAIL'}")
        last_status = "PARTIAL" if filled > 0 else "NOT_FILLED"

    return (last_status, oid, filled)

def log_trade_row(row: dict):
    header = [
        "timestamp_utc",
        "signal_date",
        "expiry_date",
        "leg",
        "direction",
        "strong",
        "go_value",
        "inner_strike",
        "outer_strike",
        "qty",
        "side",
        "ladder",
        "final_status",
        "filled_qty",
        "order_id",
        "account_equity",
    ]
    file_exists = os.path.exists(LOG_PATH)
    with open(LOG_PATH,"a") as f:
        if not file_exists:
            f.write(",".join(header) + "\n")
        vals = [str(row.get(k,"")) for k in header]
        f.write(",".join(vals) + "\n")

def fnum(x):
    try:
        return float(x)
    except Exception:
        return None

def _int_env(key, default=0):
    try:
        return int(os.environ.get(key, str(default)))
    except Exception:
        return default

def main():
    side_put  = (os.environ.get("CS_SIDE_PUT","") or "").upper()
    side_call = (os.environ.get("CS_SIDE_CALL","") or "").upper()

    qty_put   = _int_env("CS_QTY_PUT", 0)
    qty_call  = _int_env("CS_QTY_CALL", 0)

    put_low   = os.environ.get("CS_PUT_LOW_OSI","")
    put_high  = os.environ.get("CS_PUT_HIGH_OSI","")
    call_low  = os.environ.get("CS_CALL_LOW_OSI","")
    call_high = os.environ.get("CS_CALL_HIGH_OSI","")

    strong_put  = (os.environ.get("CS_STRONG_PUT","").lower() == "true")
    strong_call = (os.environ.get("CS_STRONG_CALL","").lower() == "true")

    go_put   = fnum(os.environ.get("CS_GO_PUT",""))
    go_call  = fnum(os.environ.get("CS_GO_CALL",""))

    sig_date = os.environ.get("CS_SIGNAL_DATE","")
    exp_iso  = os.environ.get("CS_EXPIRY_ISO","")
    equity   = fnum(os.environ.get("CS_ACCOUNT_EQUITY","")) or 0.0

    print("PLACER START ConstantStable verticals")
    print(f"  PUT : side={side_put or 'NONE'} qty={qty_put} strong={strong_put} Go={go_put}")
    print(f"  CALL: side={side_call or 'NONE'} qty={qty_call} strong={strong_call} Go={go_call}")

    c, acct_hash, acct_num = schwab_client()
    print(f"PLACER Schwab acct_hash={acct_hash} acct_num={acct_num}")

    # PUT leg
    if side_put in ("CREDIT","DEBIT") and qty_put > 0 and put_low and put_high:
        ladder_put   = CREDIT_LADDER if side_put == "CREDIT" else DEBIT_LADDER
        direction_put = "SHORT" if side_put == "CREDIT" else "LONG"

        print(f"PUT LEG: dir={direction_put} side={side_put} qty={qty_put} ladder={ladder_put}")
        final_status, oid, filled_qty = place_vertical_with_ladder(
            c, acct_hash, side_put, "PUT", put_low, put_high, qty_put, ladder_put
        )

        inner_strike = strike_from_osi(put_high)   # inner = higher strike
        outer_strike = strike_from_osi(put_low)

        log_trade_row({
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "signal_date": sig_date,
            "expiry_date": exp_iso,
            "leg": "PUT",
            "direction": direction_put,
            "strong": str(strong_put),
            "go_value": go_put if go_put is not None else "",
            "inner_strike": inner_strike,
            "outer_strike": outer_strike,
            "qty": qty_put,
            "side": side_put,
            "ladder": ";".join(f"{p:.2f}" for p in ladder_put),
            "final_status": final_status,
            "filled_qty": filled_qty,
            "order_id": oid,
            "account_equity": equity,
        })
    else:
        print("PUT LEG: skipped (no side, qty<=0, or missing OSI).")

    # CALL leg
    if side_call in ("CREDIT","DEBIT") and qty_call > 0 and call_low and call_high:
        ladder_call   = CREDIT_LADDER if side_call == "CREDIT" else DEBIT_LADDER
        direction_call = "SHORT" if side_call == "CREDIT" else "LONG"

        print(f"CALL LEG: dir={direction_call} side={side_call} qty={qty_call} ladder={ladder_call}")
        final_status, oid, filled_qty = place_vertical_with_ladder(
            c, acct_hash, side_call, "CALL", call_low, call_high, qty_call, ladder_call
        )

        inner_strike = strike_from_osi(call_low)   # inner = lower strike for calls
        outer_strike = strike_from_osi(call_high)

        log_trade_row({
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "signal_date": sig_date,
            "expiry_date": exp_iso,
            "leg": "CALL",
            "direction": direction_call,
            "strong": str(strong_call),
            "go_value": go_call if go_call is not None else "",
            "inner_strike": inner_strike,
            "outer_strike": outer_strike,
            "qty": qty_call,
            "side": side_call,
            "ladder": ";".join(f"{p:.2f}" for p in ladder_call),
            "final_status": final_status,
            "filled_qty": filled_qty,
            "order_id": oid,
            "account_equity": equity,
        })
    else:
        print("CALL LEG: skipped (no side, qty<=0, or missing OSI).")

    print("PLACER DONE")
    return 0

if __name__ == "__main__":
    sys.exit(main())
