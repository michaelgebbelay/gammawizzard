#!/usr/bin/env python3
"""
ConstantStable — Automated Profit-Taking Close Orders (A/B test)

Places GTC limit close orders on existing vertical spread positions.
Runs as a post-step for TT Individual only (IRA is the control).

Rules:
  Long IC  → 2 separate vertical close orders at $3.50 credit each
  Short IC → 1 four-leg IC close order at $1.00 debit
  RR       → short vert close at $0.50 debit, long vert close at $2.00 credit

Env:
  CS_CLOSE_ORDERS_ENABLE  - "1" to enable (default "0")
  CS_CLOSE_DRY_RUN        - "1" to log but not place orders
  CS_CLOSE_LONG_IC_PRICE  - credit per vertical for Long IC (default "3.50")
  CS_CLOSE_SHORT_IC_PRICE - debit for 4-leg Short IC (default "1.00")
  CS_CLOSE_RR_SHORT_PRICE - debit for RR short vert (default "0.50")
  CS_CLOSE_RR_LONG_PRICE  - credit for RR long vert (default "2.00")
  TT_ACCOUNT_NUMBER       - TastyTrade account number
"""

import os
import sys
import re
import time
import random
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo


def _add_scripts_root():
    cur = os.path.abspath(os.path.dirname(__file__))
    while True:
        if os.path.basename(cur) in ("scripts", "Script"):
            if cur not in sys.path:
                sys.path.append(cur)
            return
        parent = os.path.dirname(cur)
        if parent == cur:
            return
        cur = parent


_add_scripts_root()
from tt_client import request as tt_request

ET = ZoneInfo("America/New_York")
TAG = "CS_CLOSE"
TICK = 0.05
SPREAD_WIDTH = 5000  # $5 = 5000 in 8-digit strike format


# ---------------------------------------------------------------------------
# Config from env
# ---------------------------------------------------------------------------

def truthy(s):
    return str(s or "").strip().lower() in ("1", "true", "yes", "y", "on")


def env_float(key, default):
    try:
        return float(os.environ.get(key, default))
    except (ValueError, TypeError):
        return float(default)


ENABLED = truthy(os.environ.get("CS_CLOSE_ORDERS_ENABLE", "0"))
DRY_RUN = truthy(os.environ.get("CS_CLOSE_DRY_RUN", "") or os.environ.get("VERT_DRY_RUN", "false"))

LONG_IC_PRICE = env_float("CS_CLOSE_LONG_IC_PRICE", "3.50")
SHORT_IC_PRICE = env_float("CS_CLOSE_SHORT_IC_PRICE", "1.00")
RR_SHORT_PRICE = env_float("CS_CLOSE_RR_SHORT_PRICE", "0.50")
RR_LONG_PRICE = env_float("CS_CLOSE_RR_LONG_PRICE", "2.00")


# ---------------------------------------------------------------------------
# Utility functions (inlined to avoid importing orchestrator/place)
# ---------------------------------------------------------------------------

def clamp_tick(x):
    return round(round(float(x) / TICK) * TICK + 1e-12, 2)


def to_osi(sym):
    raw = (sym or "").strip().upper().lstrip(".").replace("_", "")
    m = (
        re.match(r"^([A-Z.$^]{1,6})(\d{6})([CP])(\d{1,5})(?:\.(\d{1,3}))?$", raw)
        or re.match(r"^([A-Z.$^]{1,6})(\d{6})([CP])(\d{8})$", raw)
    )
    if not m:
        return None
    root, ymd, cp, strike, frac = (m.groups() + ("",))[:5]
    if len(strike) < 8:
        mills = int(strike) * 1000 + (int((frac or "0").ljust(3, "0")) if frac else 0)
    else:
        mills = int(strike)
    return f"{root:<6}{ymd}{cp}{int(mills):08d}"


def osi_canon(osi):
    s = (osi or "")
    if len(s) < 21:
        return ("", "", "")
    return (s[6:12], s[12], s[-8:])


# ---------------------------------------------------------------------------
# TT API helpers
# ---------------------------------------------------------------------------

def _sleep_for_429(resp, attempt):
    ra = resp.headers.get("Retry-After")
    if ra:
        try:
            return max(1.0, float(ra))
        except Exception:
            pass
    return min(8.0, 0.6 * (2 ** attempt)) + random.uniform(0.0, 0.25)


def tt_get_json(url, params=None, tries=6, tag=""):
    import requests
    last = ""
    for i in range(tries):
        try:
            r = tt_request("GET", url, params=(params or {}))
            return r.json()
        except requests.HTTPError as e:
            resp = e.response
            if resp is not None and resp.status_code == 429:
                time.sleep(_sleep_for_429(resp, i))
                continue
            last = f"HTTP_{resp.status_code}" if resp is not None else "HTTP_unknown"
        except Exception as e:
            last = f"{type(e).__name__}:{e}"
        time.sleep(min(6.0, 0.5 * (2 ** i)))
    raise RuntimeError(f"TT_GET_FAIL({tag}) {last}")


def tt_post_json(url, payload, tries=5, tag=""):
    import requests
    last = ""
    for i in range(tries):
        try:
            r = tt_request("POST", url, json=payload)
            return r
        except requests.HTTPError as e:
            resp = e.response
            if resp is not None and resp.status_code == 429:
                wait = _sleep_for_429(resp, i) + random.uniform(0.0, 0.35)
                print(f"{TAG}: WARN POST 429 — backoff {wait:.2f}s [{tag}]")
                time.sleep(wait)
                continue
            last = f"HTTP_{resp.status_code}:{(resp.text or '')[:200]}" if resp is not None else "HTTP_unknown"
            time.sleep(min(6.0, 0.45 * (2 ** i)))
            continue
        except Exception as e:
            last = f"{type(e).__name__}:{e}"
            time.sleep(min(6.0, 0.45 * (2 ** i)))
    raise RuntimeError(f"POST_FAIL({tag}) {last}")


def parse_order_id(r):
    try:
        j = r.json()
        if isinstance(j, dict):
            data = j.get("data") if isinstance(j.get("data"), dict) else j
            if isinstance(data.get("order"), dict):
                oid = data["order"].get("id")
                if oid:
                    return str(oid)
            oid = data.get("id") or data.get("orderId") or data.get("order_id")
            if oid:
                return str(oid)
    except Exception:
        pass
    return ""


# ---------------------------------------------------------------------------
# Position fetching
# ---------------------------------------------------------------------------

def fetch_positions(acct_num):
    """
    Returns list of dicts: {symbol, osi, canon, qty}
    where qty is positive for long, negative for short.
    """
    j = tt_get_json(f"/accounts/{acct_num}/positions", tag="POSITIONS")
    data = j.get("data") if isinstance(j, dict) else {}
    items = data.get("items") or []
    result = []

    for p in items:
        atype = (p.get("instrument-type") or p.get("instrument_type") or "").upper()
        if "OPTION" not in atype:
            continue

        sym = (p.get("symbol") or "").strip()
        if not sym:
            continue

        osi = to_osi(re.sub(r"\s+", "", sym))
        if not osi:
            continue

        try:
            qty = float(p.get("quantity", 0) or 0)
        except (ValueError, TypeError):
            continue

        direction = str(p.get("quantity-direction") or p.get("quantity_direction") or "").lower()
        if direction.startswith("short"):
            qty = -abs(qty)

        if abs(qty) < 1e-9:
            continue

        result.append({
            "symbol": sym,
            "osi": osi,
            "canon": osi_canon(osi),
            "qty": qty,
        })

    return result


# ---------------------------------------------------------------------------
# Vertical pairing and structure classification
# ---------------------------------------------------------------------------

def pair_verticals(legs):
    """
    Pair option legs into $5-wide vertical spreads.

    legs: list of {symbol, osi, canon, qty} all same expiry and type (P or C)
    Returns list of vertical dicts.
    """
    sorted_legs = sorted(legs, key=lambda l: int(l["canon"][2]))
    used = set()
    verticals = []

    for i, l1 in enumerate(sorted_legs):
        if i in used:
            continue
        for j in range(i + 1, len(sorted_legs)):
            if j in used:
                continue
            l2 = sorted_legs[j]
            s1 = int(l1["canon"][2])
            s2 = int(l2["canon"][2])
            if abs(s2 - s1) != SPREAD_WIDTH:
                continue
            # One must be long, one short
            if not ((l1["qty"] > 0 and l2["qty"] < 0) or (l1["qty"] < 0 and l2["qty"] > 0)):
                continue

            long_leg = l1 if l1["qty"] > 0 else l2
            short_leg = l1 if l1["qty"] < 0 else l2
            spread_qty = int(min(abs(l1["qty"]), abs(l2["qty"])))

            # Determine credit vs debit:
            # PUT credit spread: short the HIGHER strike, long the LOWER
            # CALL credit spread: short the LOWER strike, long the HIGHER
            cp = l1["canon"][1]
            short_strike = int(short_leg["canon"][2])
            long_strike = int(long_leg["canon"][2])

            if cp == "P":
                is_credit = short_strike > long_strike
            else:  # C
                is_credit = short_strike < long_strike

            verticals.append({
                "long_leg": long_leg,
                "short_leg": short_leg,
                "qty": spread_qty,
                "is_credit": is_credit,
                "cp": cp,
            })
            used.add(i)
            used.add(j)
            break

    return verticals


def classify_structure(put_verts, call_verts):
    """Classify as LONG_IC, SHORT_IC, RR, or SINGLE_*."""
    has_put = len(put_verts) > 0
    has_call = len(call_verts) > 0

    if has_put and has_call:
        pv = put_verts[0]
        cv = call_verts[0]
        if pv["is_credit"] and cv["is_credit"]:
            return "SHORT_IC"
        elif not pv["is_credit"] and not cv["is_credit"]:
            return "LONG_IC"
        else:
            return "RR"
    elif has_put:
        return "SINGLE_PUT"
    elif has_call:
        return "SINGLE_CALL"
    return "NONE"


# ---------------------------------------------------------------------------
# Duplicate detection
# ---------------------------------------------------------------------------

def fetch_close_signatures(acct_num):
    """
    Fetch live orders and return a set of frozensets (leg canonical keys)
    for all working close orders. Used to prevent placing duplicate orders.
    """
    try:
        j = tt_get_json(f"/accounts/{acct_num}/orders/live", tag="LIVE_ORDERS")
    except Exception as e:
        print(f"{TAG}: WARN — could not fetch live orders: {e}")
        return set()

    data = j.get("data") if isinstance(j, dict) else {}
    items = data.get("items") or []
    signatures = set()

    for order in items:
        status = str(order.get("status") or "").lower()
        if status not in ("received", "routed", "live"):
            continue
        legs = order.get("legs") or []
        is_close = any("Close" in str(leg.get("action") or "") for leg in legs)
        if not is_close:
            continue
        leg_canons = set()
        for leg in legs:
            sym = (leg.get("symbol") or "").strip()
            if sym:
                osi = to_osi(re.sub(r"\s+", "", sym))
                if osi:
                    leg_canons.add(osi_canon(osi))
        if leg_canons:
            signatures.add(frozenset(leg_canons))

    return signatures


# ---------------------------------------------------------------------------
# Close order payloads
# ---------------------------------------------------------------------------

def close_payload_vertical(long_leg, short_leg, price, price_effect, qty):
    """Build a 2-leg close order for a single vertical spread."""
    # To close: reverse the original actions
    # Original long leg (Buy to Open) → Sell to Close
    # Original short leg (Sell to Open) → Buy to Close
    return {
        "order-type": "Limit",
        "price": f"{clamp_tick(price):.2f}",
        "price-effect": price_effect,
        "time-in-force": "GTC",
        "legs": [
            {
                "symbol": long_leg["symbol"],
                "instrument-type": "Equity Option",
                "action": "Sell to Close",
                "quantity": qty,
            },
            {
                "symbol": short_leg["symbol"],
                "instrument-type": "Equity Option",
                "action": "Buy to Close",
                "quantity": qty,
            },
        ],
    }


def close_payload_4leg_ic(put_vert, call_vert, price, price_effect, qty):
    """Build a 4-leg close order for an entire Iron Condor."""
    return {
        "order-type": "Limit",
        "price": f"{clamp_tick(price):.2f}",
        "price-effect": price_effect,
        "time-in-force": "GTC",
        "legs": [
            {
                "symbol": put_vert["long_leg"]["symbol"],
                "instrument-type": "Equity Option",
                "action": "Sell to Close",
                "quantity": qty,
            },
            {
                "symbol": put_vert["short_leg"]["symbol"],
                "instrument-type": "Equity Option",
                "action": "Buy to Close",
                "quantity": qty,
            },
            {
                "symbol": call_vert["long_leg"]["symbol"],
                "instrument-type": "Equity Option",
                "action": "Sell to Close",
                "quantity": qty,
            },
            {
                "symbol": call_vert["short_leg"]["symbol"],
                "instrument-type": "Equity Option",
                "action": "Buy to Close",
                "quantity": qty,
            },
        ],
    }


def leg_signature(payload):
    """Extract canonical key set from a payload for duplicate checking."""
    canons = set()
    for leg in payload.get("legs", []):
        sym = (leg.get("symbol") or "").strip()
        if sym:
            osi = to_osi(re.sub(r"\s+", "", sym))
            if osi:
                canons.add(osi_canon(osi))
    return frozenset(canons)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if not ENABLED:
        print(f"{TAG}: SKIP — CS_CLOSE_ORDERS_ENABLE not set")
        return 0

    acct_num = (os.environ.get("TT_ACCOUNT_NUMBER") or "").strip()
    if not acct_num:
        print(f"{TAG}: SKIP — TT_ACCOUNT_NUMBER missing")
        return 0

    try:
        # 1. Fetch positions
        positions = fetch_positions(acct_num)
        if not positions:
            print(f"{TAG}: SKIP — no option positions")
            return 0

        print(f"{TAG}: found {len(positions)} option leg(s)")

        # 2. Filter to tomorrow's expiry only (1DTE positions opened today)
        now_et = datetime.now(ET)
        tomorrow = now_et.date() + timedelta(days=1)
        tomorrow_6 = tomorrow.strftime("%y%m%d")

        tomorrow_pos = [p for p in positions if p["canon"][0] == tomorrow_6]
        if not tomorrow_pos:
            print(f"{TAG}: SKIP — no positions expiring {tomorrow} (checked {tomorrow_6})")
            return 0

        print(f"{TAG}: {len(tomorrow_pos)} leg(s) expiring {tomorrow}")

        # 3. Separate into puts and calls
        puts = [p for p in tomorrow_pos if p["canon"][1] == "P"]
        calls = [p for p in tomorrow_pos if p["canon"][1] == "C"]

        # 4. Pair into verticals
        put_verts = pair_verticals(puts)
        call_verts = pair_verticals(calls)

        if not put_verts and not call_verts:
            print(f"{TAG}: SKIP — no vertical spreads found (puts={len(puts)} calls={len(calls)})")
            return 0

        for v in put_verts:
            side = "CREDIT" if v["is_credit"] else "DEBIT"
            print(f"{TAG}: PUT vert {side} qty={v['qty']} "
                  f"short={v['short_leg']['canon'][2]} long={v['long_leg']['canon'][2]}")
        for v in call_verts:
            side = "CREDIT" if v["is_credit"] else "DEBIT"
            print(f"{TAG}: CALL vert {side} qty={v['qty']} "
                  f"short={v['short_leg']['canon'][2]} long={v['long_leg']['canon'][2]}")

        # 5. Classify structure
        structure = classify_structure(put_verts, call_verts)
        print(f"{TAG}: structure={structure}")

        if structure == "NONE":
            print(f"{TAG}: SKIP — no classifiable structure")
            return 0

        # 6. Fetch existing close orders for duplicate check
        existing_sigs = fetch_close_signatures(acct_num)
        print(f"{TAG}: {len(existing_sigs)} existing close order(s) found")

        # 7. Build close order payloads
        orders_to_place = []

        if structure == "LONG_IC":
            # Close each vertical separately at $3.50 credit
            for v in put_verts + call_verts:
                qty = v["qty"]
                payload = close_payload_vertical(
                    v["long_leg"], v["short_leg"],
                    price=LONG_IC_PRICE, price_effect="Credit", qty=qty,
                )
                orders_to_place.append((f"LONG_IC_{v['cp']}", payload))

        elif structure == "SHORT_IC":
            # Close entire IC as single 4-leg order at $1.00 debit
            pv = put_verts[0]
            cv = call_verts[0]
            qty = min(pv["qty"], cv["qty"])
            payload = close_payload_4leg_ic(
                pv, cv,
                price=SHORT_IC_PRICE, price_effect="Debit", qty=qty,
            )
            orders_to_place.append(("SHORT_IC_4LEG", payload))

        elif structure == "RR":
            # Each vertical closed separately
            for v in put_verts + call_verts:
                if v["is_credit"]:
                    price = RR_SHORT_PRICE
                    effect = "Debit"
                    label = f"RR_SHORT_{v['cp']}"
                else:
                    price = RR_LONG_PRICE
                    effect = "Credit"
                    label = f"RR_LONG_{v['cp']}"
                payload = close_payload_vertical(
                    v["long_leg"], v["short_leg"],
                    price=price, price_effect=effect, qty=v["qty"],
                )
                orders_to_place.append((label, payload))

        elif structure in ("SINGLE_PUT", "SINGLE_CALL"):
            # Treat like RR — use short/long pricing based on direction
            verts = put_verts if structure == "SINGLE_PUT" else call_verts
            for v in verts:
                if v["is_credit"]:
                    price = RR_SHORT_PRICE
                    effect = "Debit"
                    label = f"SINGLE_SHORT_{v['cp']}"
                else:
                    price = RR_LONG_PRICE
                    effect = "Credit"
                    label = f"SINGLE_LONG_{v['cp']}"
                payload = close_payload_vertical(
                    v["long_leg"], v["short_leg"],
                    price=price, price_effect=effect, qty=v["qty"],
                )
                orders_to_place.append((label, payload))

        if not orders_to_place:
            print(f"{TAG}: SKIP — no close orders to place")
            return 0

        # 8. Place orders (skip duplicates)
        url = f"/accounts/{acct_num}/orders"
        placed = 0
        skipped = 0

        for label, payload in orders_to_place:
            sig = leg_signature(payload)
            if sig in existing_sigs:
                print(f"{TAG}: SKIP {label} — close order already exists")
                skipped += 1
                continue

            legs_desc = ", ".join(
                f"{l['action']} {l['symbol']} x{l['quantity']}"
                for l in payload.get("legs", [])
            )
            print(f"{TAG}: {label} price={payload['price']} {payload['price-effect']} "
                  f"TIF={payload['time-in-force']} legs=[{legs_desc}]")

            if DRY_RUN:
                print(f"{TAG}: DRY_RUN — would place {label}")
                placed += 1
                continue

            try:
                r = tt_post_json(url, payload, tag=label)
                oid = parse_order_id(r)
                print(f"{TAG}: PLACED {label} order_id={oid}")
                placed += 1
                existing_sigs.add(sig)
            except Exception as e:
                print(f"{TAG}: WARN — failed to place {label}: {e}")

        dry_tag = " (DRY_RUN)" if DRY_RUN else ""
        print(f"{TAG}: done — placed={placed} skipped={skipped}{dry_tag}")
        return 0

    except Exception as e:
        print(f"{TAG}: WARN — {type(e).__name__}: {e}")
        return 0


if __name__ == "__main__":
    sys.exit(main())
