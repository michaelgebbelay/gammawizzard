#!/usr/bin/env python3
"""LeoProfit placer — TastyTrade.

Submits a single 4-leg complex order:
  CREDIT (asymmetric): qty puts + qty*call_mult calls, NET CREDIT
  DEBIT (symmetric 5-wide): qty all legs, NET DEBIT

Reuses NBBO/symbol/order helpers from TT/Script/ConstantStable/place.py.
"""

import os
import sys
import time
import random

import requests


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

# Reuse TT helpers from CS placer (NBBO, symbol resolution, status polling).
from ConstantStable.place import (  # type: ignore
    TICK,
    clamp_tick,
    truthy,
    fetch_bid_ask,
    order_symbol,
    parse_order_id,
    post_with_retry,
    delete_with_retry,
    get_status,
    status_upper,
    extract_filled_quantity,
    cancel_all_working_orders,
)


def goutput(name: str, val: str):
    p = os.environ.get("GITHUB_OUTPUT")
    if p:
        with open(p, "a") as fh:
            fh.write(f"{name}={val}\n")


def tt_account_number() -> str:
    acct = (os.environ.get("TT_ACCOUNT_NUMBER") or "").strip()
    if not acct:
        raise RuntimeError("TT_ACCOUNT_NUMBER missing")
    return acct


# ---------------- NBBO ----------------

def nbbo_credit_synth(legs, call_mult: int = 1):
    """Synthetic NBBO for an asymmetric IC entered as NET CREDIT.

    legs = [bp, sp, sc, bc] where bp=long put, sp=short put, sc=short call, bc=long call.
    Credit_per_combo = (sp_short - bp_long) + call_mult * (sc_short - bc_long)
    Bid (worst-case credit collected) = (sp.bid - bp.ask) + m*(sc.bid - bc.ask)
    Ask (best-case credit collected)  = (sp.ask - bp.bid) + m*(sc.ask - bc.bid)
    """
    bp, sp, sc, bc = legs
    bp_b, bp_a = fetch_bid_ask(None, bp)
    sp_b, sp_a = fetch_bid_ask(None, sp)
    sc_b, sc_a = fetch_bid_ask(None, sc)
    bc_b, bc_a = fetch_bid_ask(None, bc)
    if None in (bp_b, bp_a, sp_b, sp_a, sc_b, sc_a, bc_b, bc_a):
        return (None, None, None)
    bid = (sp_b - bp_a) + call_mult * (sc_b - bc_a)
    ask = (sp_a - bp_b) + call_mult * (sc_a - bc_b)
    mid = (bid + ask) / 2.0
    return (clamp_tick(bid), clamp_tick(ask), clamp_tick(mid))


def nbbo_debit_synth(legs):
    """Synthetic NBBO for a symmetric long IC entered as NET DEBIT."""
    bp, sp, sc, bc = legs
    bp_b, bp_a = fetch_bid_ask(None, bp)
    sp_b, sp_a = fetch_bid_ask(None, sp)
    sc_b, sc_a = fetch_bid_ask(None, sc)
    bc_b, bc_a = fetch_bid_ask(None, bc)
    if None in (bp_b, bp_a, sp_b, sp_a, sc_b, sc_a, bc_b, bc_a):
        return (None, None, None)
    bid = (bp_b - sp_a) + (bc_b - sc_a)
    ask = (bp_a - sp_b) + (bc_a - sc_b)
    mid = (bid + ask) / 2.0
    return (clamp_tick(bid), clamp_tick(ask), clamp_tick(mid))


# ---------------- order payload ----------------

def order_payload_ic(side: str, legs, price: float, qty: int, call_mult: int = 1):
    """Build a 4-leg TT complex order. CREDIT supports asymmetric qty via call_mult."""
    side = side.upper()
    if side not in ("CREDIT", "DEBIT"):
        raise ValueError("side must be CREDIT or DEBIT")
    bp, sp, sc, bc = legs
    px = clamp_tick(price)
    if side == "CREDIT":
        return {
            "order-type": "Limit",
            "price": f"{px:.2f}",
            "price-effect": "Credit",
            "time-in-force": "Day",
            "legs": [
                {"symbol": order_symbol(bp), "instrument-type": "Equity Option", "action": "Buy to Open", "quantity": qty},
                {"symbol": order_symbol(sp), "instrument-type": "Equity Option", "action": "Sell to Open", "quantity": qty},
                {"symbol": order_symbol(sc), "instrument-type": "Equity Option", "action": "Sell to Open", "quantity": qty * call_mult},
                {"symbol": order_symbol(bc), "instrument-type": "Equity Option", "action": "Buy to Open", "quantity": qty * call_mult},
            ],
        }
    return {
        "order-type": "Limit",
        "price": f"{px:.2f}",
        "price-effect": "Debit",
        "time-in-force": "Day",
        "legs": [
            {"symbol": order_symbol(bp), "instrument-type": "Equity Option", "action": "Buy to Open", "quantity": qty},
            {"symbol": order_symbol(sp), "instrument-type": "Equity Option", "action": "Sell to Open", "quantity": qty},
            {"symbol": order_symbol(sc), "instrument-type": "Equity Option", "action": "Sell to Open", "quantity": qty},
            {"symbol": order_symbol(bc), "instrument-type": "Equity Option", "action": "Buy to Open", "quantity": qty},
        ],
    }


# ---------------- main ----------------

def main():
    side = (os.environ.get("LEO_SIDE", "CREDIT") or "CREDIT").upper()
    qty = max(1, int(os.environ.get("LEO_QTY", "2")))
    call_mult = max(1, int(os.environ.get("LEO_CALL_MULT", "1")))
    dry_run = truthy(os.environ.get("LEO_DRY_RUN", "false"))

    legs = [
        os.environ["LEO_OCC_BUY_PUT"],
        os.environ["LEO_OCC_SELL_PUT"],
        os.environ["LEO_OCC_SELL_CALL"],
        os.environ["LEO_OCC_BUY_CALL"],
    ]

    acct = tt_account_number()

    print(f"LEO PLACE START side={side} structure={(os.environ.get('LEO_STRUCTURE') or '').upper()} "
          f"qty={qty} call_mult={call_mult} dry_run={dry_run}")
    print(f"LEO PLACE LEGS: {legs}")

    # ---- Cancel any stale working orders before placing ----
    if not dry_run:
        try:
            cancel_all_working_orders(acct)
        except Exception as e:
            print(f"LEO PLACE WARN: preflight cancel failed: {e}")

    # ---- NBBO ----
    if side == "CREDIT":
        bid, ask, mid = nbbo_credit_synth(legs, call_mult=call_mult)
        nbbo_label = "CREDIT NBBO (synth)"
    else:
        bid, ask, mid = nbbo_debit_synth(legs)
        nbbo_label = "DEBIT NBBO (synth)"

    print(f"{nbbo_label}: bid={bid} ask={ask} mid={mid}")
    if mid is None:
        print("LEO PLACE WARN: no NBBO — abort")
        goutput("placed", "0")
        goutput("reason", "no_nbbo")
        return 0

    # ---- Build a small ladder ----
    ladder: list[float] = []
    if side == "CREDIT":
        # Start at mid, walk down to widen credit collected
        ladder = [mid]
        if mid is not None:
            ladder += [clamp_tick(mid - 0.05), clamp_tick(mid - 0.10)]
        if bid is not None:
            ladder = [max(p, bid) for p in ladder]
    else:
        ladder = [mid]
        if mid is not None:
            ladder += [clamp_tick(mid + 0.05), clamp_tick(mid + 0.10)]
        if ask is not None:
            ladder = [min(p, ask) for p in ladder]

    # Dedup, preserve order
    seen = set()
    rungs: list[float] = []
    for p in ladder:
        if p is None:
            continue
        pp = clamp_tick(p)
        if pp not in seen:
            seen.add(pp)
            rungs.append(pp)

    if dry_run:
        for p in rungs:
            print(f"LEO PLACE DRY_RUN rung={p:.2f}")
        goutput("placed", "0")
        goutput("reason", "dry_run")
        return 0

    step_wait = float(os.environ.get("LEO_STEP_WAIT", "8"))
    cancel_settle_secs = float(os.environ.get("LEO_CANCEL_SETTLE", "1.0"))
    max_cycles = int(os.environ.get("LEO_MAX_LADDER", "2"))

    url_post = f"/accounts/{acct}/orders"
    filled = 0
    active_oid = ""

    for cycle in range(1, max_cycles + 1):
        print(f"LEO PLACE CYCLE {cycle} ladder: {rungs}")
        for price in rungs:
            to_place = max(0, qty - filled)
            if to_place == 0:
                break

            # Cancel any prior live order from previous rung
            if active_oid:
                url_del = f"/accounts/{acct}/orders/{active_oid}"
                ok = delete_with_retry(None, url_del, tag=f"CANCEL {active_oid}", tries=3)
                print(f"LEO PLACE CANCEL {active_oid} -> {'OK' if ok else 'FAIL'}")
                time.sleep(cancel_settle_secs)
                active_oid = ""

            payload = order_payload_ic(side, legs, price, to_place, call_mult=call_mult)
            print(f"LEO PLACE RUNG -> price={price:.2f} qty={to_place} call_mult={call_mult}")

            try:
                r = post_with_retry(None, url_post, payload, tag=f"PLACE@{price:.2f}x{to_place}")
                active_oid = parse_order_id(r)
                print(f"LEO PLACE OID={active_oid}")
            except Exception as e:
                print(f"LEO PLACE FAIL: {str(e)[:300]}")
                continue

            t_end = time.time() + step_wait
            while time.time() < t_end:
                st = get_status(None, acct, active_oid, tries=3)
                s = status_upper(st)
                fq = extract_filled_quantity(st)
                if fq > filled:
                    filled = fq
                if s == "FILLED" or filled >= qty:
                    break
                time.sleep(0.30)

            if filled >= qty:
                break

        if filled >= qty:
            break

    # ---- Final cleanup: cancel anything still working ----
    if active_oid:
        url_del = f"/accounts/{acct}/orders/{active_oid}"
        ok = delete_with_retry(None, url_del, tag=f"FINAL_CANCEL {active_oid}", tries=3)
        print(f"LEO PLACE FINAL_CANCEL {active_oid} -> {'OK' if ok else 'FAIL'}")
        time.sleep(cancel_settle_secs)

    print(f"LEO PLACE DONE filled={filled}/{qty}")
    goutput("placed", "1" if filled > 0 else "0")
    goutput("filled_quantity", str(filled))
    return 0


if __name__ == "__main__":
    sys.exit(main())
