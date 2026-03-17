#!/usr/bin/env python3
"""IC_LONG deferred morning entry.

Reads the deferred IC_LONG plan saved by the evening orchestrator and places
the trade at morning prices with cost filters:

  1. Total debit <= $2.20  AND  both sides >= $0.40  →  buy full IC (both sides)
  2. Total debit <= $2.20  BUT  one side < $0.40     →  buy expensive side only (limit $2.00)
  3. Total debit >  $2.20                             →  buy expensive side only (limit $2.00)

Invoked via EventBridge at 9:35 AM ET, account="ic-long-morning".
"""

import json
import os
import subprocess
import sys
from datetime import date, datetime, timezone

import boto3

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

S3_BUCKET = os.environ.get("CS_MOVE_STATE_S3_BUCKET", "gamma-sim-cache")
S3_KEY = os.environ.get("CS_IC_DEFER_S3_KEY", "cadence/cs_ic_long_deferred.json")

MAX_TOTAL_DEBIT = float(os.environ.get("CS_MORNING_MAX_TOTAL", "2.20"))
MIN_SIDE_PRICE = float(os.environ.get("CS_MORNING_MIN_SIDE", "0.40"))
MAX_SINGLE_SIDE = float(os.environ.get("CS_MORNING_MAX_SINGLE", "2.00"))

DRY_RUN = os.environ.get("CS_DRY_RUN", "0").strip() in ("1", "true", "yes")
CS_LOG_PATH = os.environ.get("CS_LOG_PATH", "/tmp/logs/cs_trades.csv")


def _add_scripts_root():
    cur = os.path.abspath(os.path.dirname(__file__))
    while True:
        if os.path.isdir(os.path.join(cur, "scripts")):
            if cur not in sys.path:
                sys.path.insert(0, cur)
            return cur
        parent = os.path.dirname(cur)
        if parent == cur:
            return None
        cur = parent


def load_deferred_plan():
    """Load the deferred IC_LONG plan from S3."""
    s3 = boto3.client("s3")
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
        plan = json.loads(obj["Body"].read().decode("utf-8"))
    except Exception as e:
        print(f"CS_MORNING SKIP: cannot read deferred plan ({e})")
        return None

    if plan.get("status") != "pending":
        print(f"CS_MORNING SKIP: plan status={plan.get('status')} (not pending)")
        return None

    # The execute_date should be today
    today_str = date.today().isoformat()
    execute_date = plan.get("execute_date", "")
    if execute_date != today_str:
        print(f"CS_MORNING SKIP: execute_date={execute_date} != today={today_str}")
        return None

    return plan


def mark_plan_executed(plan, result):
    """Update plan status in S3 so it won't be re-executed."""
    plan["status"] = "executed"
    plan["executed_utc"] = datetime.now(timezone.utc).isoformat()
    plan["result"] = result
    try:
        s3 = boto3.client("s3")
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=S3_KEY,
            Body=json.dumps(plan, indent=2),
            ContentType="application/json",
        )
    except Exception as e:
        print(f"CS_MORNING WARN: failed to mark plan executed ({e})")


def get_morning_prices(c, plan):
    """Fetch current mid prices for both spreads from Schwab chain."""
    from schwab.orders.options import OptionSymbol

    # Get quotes for all 4 legs
    symbols = [
        plan["put_low_osi"], plan["put_high_osi"],
        plan["call_low_osi"], plan["call_high_osi"],
    ]

    try:
        resp = c.get_quotes(symbols)
        if hasattr(resp, "json"):
            quotes = resp.json()
        else:
            quotes = resp
    except Exception as e:
        print(f"CS_MORNING ERROR: quote fetch failed ({e})")
        return None, None

    def mid_price(sym):
        q = quotes.get(sym, {})
        ref = q.get("reference", q.get("quote", q))
        bid = ref.get("bidPrice", 0) or 0
        ask = ref.get("askPrice", 0) or 0
        if bid <= 0 and ask <= 0:
            return None
        return round((bid + ask) / 2, 2)

    # Put spread: buy high strike put, sell low strike put (debit)
    put_long_mid = mid_price(plan["put_high_osi"])  # buy
    put_short_mid = mid_price(plan["put_low_osi"])   # sell
    # Call spread: buy high strike call, sell low strike call (debit)
    call_long_mid = mid_price(plan["call_high_osi"])  # buy
    call_short_mid = mid_price(plan["call_low_osi"])   # sell

    if any(x is None for x in [put_long_mid, put_short_mid, call_long_mid, call_short_mid]):
        print(f"CS_MORNING WARN: missing quotes — put={put_long_mid}/{put_short_mid} call={call_long_mid}/{call_short_mid}")
        return None, None

    put_debit = round(put_long_mid - put_short_mid, 2)
    call_debit = round(call_long_mid - call_short_mid, 2)

    # Floor at 0 (shouldn't be negative but just in case)
    put_debit = max(0, put_debit)
    call_debit = max(0, call_debit)

    return put_debit, call_debit


def apply_price_rules(put_price, call_price):
    """Apply the morning price filter rules.

    Returns list of sides to trade: [("PUT", price), ("CALL", price)] etc.
    """
    total = put_price + call_price

    if total <= MAX_TOTAL_DEBIT and put_price >= MIN_SIDE_PRICE and call_price >= MIN_SIDE_PRICE:
        # Rule 1: both sides at fair price → full IC
        print(f"CS_MORNING RULE: FULL_IC total={total:.2f} (put={put_price:.2f} call={call_price:.2f})")
        return [("PUT", put_price), ("CALL", call_price)]

    # Rule 2 or 3: buy expensive side only, limit $2.00
    if put_price >= call_price:
        expensive, cheap = ("PUT", put_price), ("CALL", call_price)
    else:
        expensive, cheap = ("CALL", call_price), ("PUT", put_price)

    reason = "SIDE_BELOW_MIN" if total <= MAX_TOTAL_DEBIT else "TOTAL_OVER_MAX"
    limit_price = min(expensive[1], MAX_SINGLE_SIDE)
    print(
        f"CS_MORNING RULE: SINGLE_SIDE reason={reason} "
        f"buying={expensive[0]} at limit={limit_price:.2f} "
        f"(skipping {cheap[0]}={cheap[1]:.2f})"
    )
    return [(expensive[0], limit_price)]


def place_vertical(plan, side, limit_price):
    """Place a single vertical via place.py."""
    if side == "PUT":
        short_osi = plan["put_low_osi"]
        long_osi = plan["put_high_osi"]
        qty = plan["put_qty"]
        strength = plan.get("put_strength", 0)
        go = plan.get("put_go")
        gw_price = plan.get("put_credit_close")
    else:
        short_osi = plan["call_low_osi"]
        long_osi = plan["call_high_osi"]
        qty = plan["call_qty"]
        strength = plan.get("call_strength", 0)
        go = plan.get("call_go")
        gw_price = plan.get("call_credit_close")

    env = dict(os.environ)
    env.update({
        "VERT_SIDE": "DEBIT",
        "VERT_KIND": side,
        "VERT_NAME": f"{side}_LONG_MORNING",
        "VERT_DIRECTION": "LONG",
        "VERT_SHORT_OSI": short_osi,
        "VERT_LONG_OSI": long_osi,
        "VERT_QTY": str(qty),
        "VERT_GO": "" if go is None else str(go),
        "VERT_STRENGTH": f"{float(strength):.3f}",
        "VERT_TRADE_DATE": plan["trade_date"],
        "VERT_TDATE": plan["execute_date"],
        "VERT_GW_PRICE": "" if gw_price is None else str(gw_price),
        "VERT_LIMIT_PRICE": f"{limit_price:.2f}",
        "VERT_UNIT_DOLLARS": str(plan.get("unit_dollars", 15000)),
        "VERT_OC": str(plan.get("units", 1) * plan.get("unit_dollars", 15000)),
        "VERT_UNITS": str(plan.get("units", 1)),
        "VERT_VOL_FIELD": plan.get("vol_field", ""),
        "VERT_VOL_USED": plan.get("vol_field", ""),
        "VERT_VOL_VALUE": "" if plan.get("vol_value") is None else str(plan["vol_value"]),
        "VERT_VOL_BUCKET": str(plan.get("vol_bucket", 0)),
        "VERT_VOL_MULT": str(plan.get("vol_mult", 0)),
        "VERT_QTY_RULE": "MORNING_DEFERRED",
        "CS_LOG_PATH": CS_LOG_PATH,
    })

    if DRY_RUN:
        print(f"CS_MORNING DRY_RUN: would place {side}_LONG qty={qty} limit={limit_price:.2f} {short_osi}/{long_osi}")
        return 0

    rc = subprocess.call(
        [sys.executable, "scripts/trade/ConstantStable/place.py"],
        env=env,
    )
    return rc


def main():
    repo_root = _add_scripts_root()
    if repo_root:
        os.chdir(repo_root)

    print("=" * 60)
    print(f"CS_MORNING IC_LONG deferred entry — {datetime.now(timezone.utc).isoformat()}")
    print(f"  max_total={MAX_TOTAL_DEBIT}  min_side={MIN_SIDE_PRICE}  max_single={MAX_SINGLE_SIDE}")
    print(f"  dry_run={DRY_RUN}")
    print("=" * 60)

    plan = load_deferred_plan()
    if not plan:
        return 0

    print(f"CS_MORNING PLAN: trade_date={plan['trade_date']} execute_date={plan['execute_date']}")
    print(f"  put {plan['p_low']}/{plan['p_high']}  call {plan['c_low']}/{plan['c_high']}")
    print(f"  close prices: put={plan.get('put_credit_close')} call={plan.get('call_credit_close')}")
    print(f"  qty: put={plan['put_qty']} call={plan['call_qty']}")

    # Get Schwab client
    try:
        from lib.schwab_auth import schwab_client
        c = schwab_client()
    except Exception as e:
        print(f"CS_MORNING ERROR: Schwab init failed ({e})")
        mark_plan_executed(plan, f"ERROR: schwab_init: {e}")
        return 1

    # Fetch morning prices
    put_price, call_price = get_morning_prices(c, plan)
    if put_price is None or call_price is None:
        print("CS_MORNING SKIP: could not get morning prices")
        mark_plan_executed(plan, "SKIP: no_morning_prices")
        return 0

    print(f"CS_MORNING PRICES: put={put_price:.2f} call={call_price:.2f} total={put_price + call_price:.2f}")
    print(f"  vs close: put={plan.get('put_credit_close')} call={plan.get('call_credit_close')}")

    # Apply price rules
    sides_to_trade = apply_price_rules(put_price, call_price)

    if not sides_to_trade:
        print("CS_MORNING SKIP: no sides pass price rules")
        mark_plan_executed(plan, "SKIP: price_rules")
        return 0

    # Place each side
    results = []
    for side, limit in sides_to_trade:
        print(f"\nCS_MORNING PLACING: {side} limit={limit:.2f}")
        rc = place_vertical(plan, side, limit)
        results.append((side, rc))
        print(f"CS_MORNING RESULT: {side} rc={rc}")

    summary = "; ".join(f"{s}:rc={rc}" for s, rc in results)
    mark_plan_executed(plan, f"EXECUTED: {summary}")

    print(f"\nCS_MORNING DONE: {summary}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
