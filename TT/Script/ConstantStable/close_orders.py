#!/usr/bin/env python3
"""
ConstantStable — Automated Profit-Taking Close Orders (A/B test)

Places a single GTC limit close order at 50% of max profit for the entire
position (all legs together). Reads actual fill prices from the trade CSV
to compute dynamic close prices.

Runs as a post-step for TT Individual only (IRA is the control).

Max profit (only ONE side of an IC/RR can pay off — SPX goes one direction):
  Credit IC/vert:  max_profit = total credit received
  Debit IC/vert:   max_profit = $5.00 - total debit paid
  RR:              max_profit = $5.00 (spread width, always)

Close order price:
  close_net = max_profit × profit_pct  -  entry_net
  → positive = Credit order; negative = Debit order
  → rounded to nearest $0.05

Examples (at 50%):
  Credit IC  ($2 credit):    close = 2×0.50 - 2      = -1.00  → Debit  $1.00
  Debit IC   ($2 debit):     close = 3×0.50 -(-2)    =  3.50  → Credit $3.50
  RR (net $1.20 debit):      close = 5×0.50 -(-1.20) =  3.70  → Credit $3.70
  RR (net $0.50 credit):     close = 5×0.50 -(+0.50) =  2.00  → Credit $2.00

Env:
  CS_CLOSE_ORDERS_ENABLE  - "1" to enable (default "0")
  CS_CLOSE_DRY_RUN        - "1" to log but not place orders
  CS_CLOSE_PROFIT_PCT     - profit target as decimal (default "0.50" = 50%)
  CS_LOG_PATH             - path to trade CSV
  TT_ACCOUNT_NUMBER       - TastyTrade account number
"""

import os
import sys
import re
import csv
import time
import random
from datetime import date, datetime
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
SPREAD_WIDTH = 5000        # $5 in 8-digit strike format
SPREAD_WIDTH_DOLLARS = 5.0


# ---------------------------------------------------------------------------
# Config from env
# ---------------------------------------------------------------------------

def truthy(s):
    return str(s or "").strip().lower() in ("1", "true", "yes", "y", "on")


ENABLED = truthy(os.environ.get("CS_CLOSE_ORDERS_ENABLE", "0"))
DRY_RUN = truthy(os.environ.get("CS_CLOSE_DRY_RUN", "") or os.environ.get("VERT_DRY_RUN", "false"))
PROFIT_PCT = float(os.environ.get("CS_CLOSE_PROFIT_PCT", "0.50") or "0.50")


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
# Trade CSV reading
# ---------------------------------------------------------------------------

def read_csv_fills():
    """Read trade CSV and return fill prices grouped by expiry.

    Returns: {expiry_yymmdd: {"PUT": {"side": str, "price": float},
                               "CALL": {"side": str, "price": float}}}
    """
    path = (os.environ.get("CS_LOG_PATH") or "logs/constantstable_vertical_trades.csv").strip()
    if not os.path.exists(path):
        print(f"{TAG}: trade CSV not found at {path}")
        return {}

    fills = {}
    with open(path, "r", newline="") as f:
        for row in csv.DictReader(f):
            tdate_str = (row.get("tdate") or "").strip()
            kind = (row.get("kind") or "").strip().upper()
            side = (row.get("side") or "").strip().upper()
            price_str = (row.get("last_price") or "").strip()
            qty_str = (row.get("qty_filled") or "0").strip()

            if not tdate_str or kind not in ("PUT", "CALL"):
                continue
            try:
                price = float(price_str)
                qty = int(qty_str)
            except (ValueError, TypeError):
                continue
            if qty <= 0 or price <= 0:
                continue

            # Convert tdate (ISO) to yymmdd
            try:
                dt = date.fromisoformat(tdate_str)
                yymmdd = dt.strftime("%y%m%d")
            except ValueError:
                continue

            if yymmdd not in fills:
                fills[yymmdd] = {}
            # Keep last occurrence per kind (handles re-runs)
            fills[yymmdd][kind] = {"side": side, "price": price}

    return fills


# ---------------------------------------------------------------------------
# Position fetching
# ---------------------------------------------------------------------------

def fetch_positions(acct_num):
    """Returns list of dicts: {symbol, osi, canon, qty}."""
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
# Vertical pairing
# ---------------------------------------------------------------------------

def pair_verticals(legs):
    """Pair option legs into $5-wide vertical spreads."""
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
            if not ((l1["qty"] > 0 and l2["qty"] < 0) or (l1["qty"] < 0 and l2["qty"] > 0)):
                continue

            long_leg = l1 if l1["qty"] > 0 else l2
            short_leg = l1 if l1["qty"] < 0 else l2
            spread_qty = int(min(abs(l1["qty"]), abs(l2["qty"])))

            cp = l1["canon"][1]
            short_strike = int(short_leg["canon"][2])
            long_strike = int(long_leg["canon"][2])

            if cp == "P":
                is_credit = short_strike > long_strike
            else:
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


# ---------------------------------------------------------------------------
# Close price computation
# ---------------------------------------------------------------------------

def compute_close_price(fills):
    """Compute close order price for 50% (or PROFIT_PCT) of max profit.

    fills: {"PUT": {"side": "CREDIT"|"DEBIT", "price": float},
            "CALL": {"side": "CREDIT"|"DEBIT", "price": float}}

    Max profit rules (only ONE side can pay off — SPX goes one direction):
      Credit IC/vert:  max_profit = total credit received
      Debit IC/vert:   max_profit = $5 - total debit paid
      RR (mixed):      max_profit = $5 (spread width, always)

    Returns (price, price_effect, max_profit) or (None, None, None).
    Price is already clamped to $0.05 ticks.
    """
    entry_net = 0.0   # positive = net credit, negative = net debit
    sides = []

    for kind in ("PUT", "CALL"):
        f = fills.get(kind)
        if not f:
            continue
        if f["side"] == "CREDIT":
            entry_net += f["price"]
            sides.append("CREDIT")
        else:
            entry_net -= f["price"]
            sides.append("DEBIT")

    if not sides:
        return None, None, None

    # Determine max_profit based on structure
    if len(sides) == 2 and sides[0] != sides[1]:
        # RR (one credit, one debit): max_profit = spread width
        max_profit = SPREAD_WIDTH_DOLLARS
    elif all(s == "CREDIT" for s in sides):
        # Credit IC or single credit: max_profit = credit received
        max_profit = entry_net
    else:
        # Debit IC or single debit: max_profit = spread width - debit
        max_profit = SPREAD_WIDTH_DOLLARS + entry_net  # entry_net is negative

    if max_profit <= 0.01:
        return None, None, None

    # close_net > 0 → we receive credit; close_net < 0 → we pay debit
    close_net = max_profit * PROFIT_PCT - entry_net

    price = clamp_tick(abs(close_net))
    effect = "Credit" if close_net > 0 else "Debit"

    if price < 0.01:
        return None, None, None

    return price, effect, max_profit


# ---------------------------------------------------------------------------
# Duplicate detection
# ---------------------------------------------------------------------------

def fetch_close_signatures(acct_num):
    """Return set of frozensets (leg canonical keys) for working close orders."""
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
    return {
        "order-type": "Limit",
        "price": f"{price:.2f}",
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


def close_payload_4leg(put_vert, call_vert, price, price_effect, qty):
    """Build a 4-leg close order for the entire position (IC or RR)."""
    return {
        "order-type": "Limit",
        "price": f"{price:.2f}",
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
        # 1. Read trade CSV for fill prices (keyed by expiry yymmdd)
        csv_fills = read_csv_fills()
        if not csv_fills:
            print(f"{TAG}: SKIP — no fills in trade CSV")
            return 0

        print(f"{TAG}: fill data for expiries: {list(csv_fills.keys())}")

        # 2. Fetch positions
        positions = fetch_positions(acct_num)
        if not positions:
            print(f"{TAG}: SKIP — no option positions")
            return 0

        print(f"{TAG}: found {len(positions)} option leg(s)")

        # 3. Fetch existing close orders (once) for duplicate check
        existing_sigs = fetch_close_signatures(acct_num)
        print(f"{TAG}: {len(existing_sigs)} existing close order(s)")

        # 4. For each expiry with fill data, match to positions and place close order
        placed_total = 0

        for expiry_ymd, fills in csv_fills.items():
            exp_pos = [p for p in positions if p["canon"][0] == expiry_ymd]
            if not exp_pos:
                print(f"{TAG}: no positions for expiry {expiry_ymd} — skipping")
                continue

            print(f"{TAG}: {len(exp_pos)} leg(s) for expiry {expiry_ymd}")

            # 5. Separate puts/calls and pair into verticals
            puts = [p for p in exp_pos if p["canon"][1] == "P"]
            calls = [p for p in exp_pos if p["canon"][1] == "C"]
            put_verts = pair_verticals(puts)
            call_verts = pair_verticals(calls)

            if not put_verts and not call_verts:
                print(f"{TAG}: SKIP — no verticals for {expiry_ymd}")
                continue

            for v in put_verts:
                sd = "CREDIT" if v["is_credit"] else "DEBIT"
                print(f"{TAG}: PUT vert {sd} qty={v['qty']} "
                      f"short={v['short_leg']['canon'][2]} long={v['long_leg']['canon'][2]}")
            for v in call_verts:
                sd = "CREDIT" if v["is_credit"] else "DEBIT"
                print(f"{TAG}: CALL vert {sd} qty={v['qty']} "
                      f"short={v['short_leg']['canon'][2]} long={v['long_leg']['canon'][2]}")

            # 6. Build fills dict for only the sides we have positions for
            fills_for_price = {}
            if put_verts and "PUT" in fills:
                fills_for_price["PUT"] = fills["PUT"]
            if call_verts and "CALL" in fills:
                fills_for_price["CALL"] = fills["CALL"]

            if not fills_for_price:
                print(f"{TAG}: SKIP — no matching fill data for {expiry_ymd}")
                continue

            # 7. Compute close price
            close_price, price_effect, max_profit = compute_close_price(fills_for_price)
            if close_price is None:
                print(f"{TAG}: SKIP — could not compute close price (max_profit <= 0)")
                continue

            # Log the math
            entry_net = sum(
                f["price"] if f["side"] == "CREDIT" else -f["price"]
                for f in fills_for_price.values()
            )
            target_profit = max_profit * PROFIT_PCT

            print(f"{TAG}: entry_net=${entry_net:+.2f} max_profit=${max_profit:.2f} "
                  f"target({PROFIT_PCT:.0%})=${target_profit:.2f}")
            print(f"{TAG}: close_price=${close_price:.2f} {price_effect} GTC")

            # 8. Build close order payload
            all_verts = put_verts + call_verts
            qty = min(v["qty"] for v in all_verts)

            if put_verts and call_verts:
                payload = close_payload_4leg(
                    put_verts[0], call_verts[0], close_price, price_effect, qty)
                label = "4LEG"
            else:
                v = all_verts[0]
                payload = close_payload_vertical(
                    v["long_leg"], v["short_leg"], close_price, price_effect, qty)
                label = f"2LEG_{v['cp']}"

            # 9. Check duplicate
            sig = leg_signature(payload)
            if sig in existing_sigs:
                print(f"{TAG}: SKIP {label} — close order already exists for {expiry_ymd}")
                continue

            legs_desc = ", ".join(
                f"{l['action']} {l['symbol']} x{l['quantity']}"
                for l in payload.get("legs", [])
            )
            print(f"{TAG}: {label} qty={qty} legs=[{legs_desc}]")

            # 10. Place order
            if DRY_RUN:
                print(f"{TAG}: DRY_RUN — would place {label}")
                placed_total += 1
                continue

            try:
                r = tt_post_json(f"/accounts/{acct_num}/orders", payload, tag=label)
                oid = parse_order_id(r)
                print(f"{TAG}: PLACED {label} order_id={oid}")
                placed_total += 1
                existing_sigs.add(sig)
            except Exception as e:
                print(f"{TAG}: WARN — failed to place {label}: {e}")

        dry_tag = " (DRY_RUN)" if DRY_RUN else ""
        print(f"{TAG}: done — placed={placed_total}{dry_tag}")
        return 0

    except Exception as e:
        print(f"{TAG}: WARN — {type(e).__name__}: {e}")
        return 0


if __name__ == "__main__":
    sys.exit(main())
