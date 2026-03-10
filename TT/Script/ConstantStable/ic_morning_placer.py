#!/usr/bin/env python3
"""IC_LONG morning placer (TastyTrade) — reads deferred plan from S3, places at open.

Price logic:
  1. Discover current 4-leg mid via bundle_nbbo
  2. If mid > $2.00: set limit at $2.00, single DAY order (fire-and-forget)
  3. If mid <= $2.00: 3-rung ladder at mid, mid+0.05, mid+0.10

Env:
  CS_ACCOUNT_LABEL   — which account's deferred plan to read
  SIM_CACHE_BUCKET   — S3 bucket (default gamma-sim-cache)
  CS_LOG_PATH        — trade log CSV path
  TT_ACCOUNT_NUMBER  — TT account number
  TT_TOKEN_JSON      — TT auth token
"""

import json
import os
import sys
import time
from datetime import date, datetime, timezone
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

# Import from the existing TT placer
from ConstantStable.place import (
    bundle_nbbo,
    cancel_all_working_orders,
    clamp_tick,
    log_row,
    order_payload_bundle,
    place_order_at_price,
    tt_account_number,
)

TAG = "IC_MORNING"
ET = ZoneInfo("America/New_York")
IC_PRICE_CAP = 2.00
STEP_WAIT = float(os.environ.get("VERT_STEP_WAIT", "15"))


def s3_get_json(bucket, key):
    import boto3
    try:
        s3 = boto3.client("s3")
        obj = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except Exception as e:
        if "NoSuchKey" in str(type(e).__name__) or "NoSuchKey" in str(e):
            return None
        print(f"{TAG}: S3 read error ({e})")
        return None


def s3_delete(bucket, key):
    import boto3
    try:
        s3 = boto3.client("s3")
        s3.delete_object(Bucket=bucket, Key=key)
    except Exception as e:
        print(f"{TAG}: S3 delete error ({e})")


def main():
    account_label = os.environ.get("CS_ACCOUNT_LABEL", "")
    bucket = (
        os.environ.get("CS_MOVE_STATE_S3_BUCKET")
        or os.environ.get("SIM_CACHE_BUCKET", "gamma-sim-cache")
    ).strip()
    s3_key = f"cadence/cs_ic_long_deferred_{account_label}.json"
    dry_run = (os.environ.get("VERT_DRY_RUN", "false") or "false").strip().lower() in ("1", "true", "yes")

    print(f"{TAG}: account={account_label} bucket={bucket} key={s3_key} dry_run={dry_run}")

    # 1. Read deferred plan
    plan = s3_get_json(bucket, s3_key)
    if not plan:
        print(f"{TAG}: no deferred plan for {account_label} — nothing to do")
        return 0

    tdate = plan.get("context", {}).get("tdate_iso", "")[:10]
    today = date.today().isoformat()
    if tdate < today:
        print(f"{TAG}: stale plan (tdate={tdate}, today={today}) — deleting")
        s3_delete(bucket, s3_key)
        return 0

    v_put = plan["v_put"]
    v_call = plan["v_call"]
    ctx = plan["context"]
    qty = int(v_put["send_qty"])

    print(
        f"{TAG}: plan date={plan['date']} tdate={tdate} qty={qty} "
        f"put={v_put['name']}({v_put['short_osi']}|{v_put['long_osi']}) "
        f"call={v_call['name']}({v_call['short_osi']}|{v_call['long_osi']})"
    )

    # 2. Init broker (TT uses env-based auth, no client object needed for orders)
    acct = tt_account_number()
    c = None  # TT uses tt_request() globally, not a client object

    # 3. Cancel working orders
    if not dry_run:
        cancel_all_working_orders(acct)

    # 4. Discover current NBBO
    side_pkg, bid, ask, mid = bundle_nbbo(
        long_osi_1=v_call["long_osi"], short_osi_1=v_call["short_osi"],
        long_osi_2=v_put["long_osi"], short_osi_2=v_put["short_osi"],
        c=c,
    )

    if side_pkg is None or mid is None:
        print(f"{TAG}: NBBO unavailable — skipping")
        s3_delete(bucket, s3_key)
        return 0

    print(f"{TAG}: NBBO side={side_pkg} bid={bid:.2f} ask={ask:.2f} mid={mid:.2f}")

    ts_utc = datetime.now(timezone.utc)
    ts_et = ts_utc.astimezone(ET)

    def _log_result(filled, order_ids, ladder_prices, last_price, reason):
        for v in (v_call, v_put):
            row = {
                "ts_utc": ts_utc.isoformat(), "ts_et": ts_et.isoformat(),
                "trade_date": ctx["trade_date"], "tdate": ctx["tdate_iso"],
                "name": v["name"], "kind": v["kind"], "side": v["side"],
                "direction": v["direction"],
                "short_osi": v["short_osi"], "long_osi": v["long_osi"],
                "go": v.get("go", ""), "strength": v.get("strength", ""),
                "gw_price": ctx.get("gw_put_price") if v["kind"] == "PUT" else ctx.get("gw_call_price"),
                "qty_rule": f"IC_MORNING_{ctx.get('qty_rule', '')}",
                "vol_field": ctx.get("vol_field", ""), "vol_used": ctx.get("vol_used", ""),
                "vol_value": ctx.get("vol_value", ""), "vol_bucket": ctx.get("vol_bucket", ""),
                "vol_mult": ctx.get("vol_mult", ""),
                "unit_dollars": ctx.get("unit_dollars", ""), "oc": ctx.get("oc_val", ""),
                "units": ctx.get("units", ""),
                "qty_requested": qty, "qty_filled": filled,
                "ladder_prices": ladder_prices,
                "last_price": f"{last_price:.2f}" if last_price is not None else "",
                "nbbo_bid": f"{bid:.2f}", "nbbo_ask": f"{ask:.2f}", "nbbo_mid": f"{mid:.2f}",
                "order_ids": order_ids, "reason": reason,
            }
            log_row(row)

    if dry_run:
        price_tag = f"CAP@{IC_PRICE_CAP}" if mid > IC_PRICE_CAP else f"LADDER@{mid:.2f}"
        print(f"{TAG}: DRY_RUN — would place {price_tag}")
        _log_result(0, "", price_tag, None, "DRY_RUN")
        s3_delete(bucket, s3_key)
        return 0

    # 5. Place order with custom price logic
    total_filled = 0
    all_order_ids = []
    last_price = None
    ladder_desc = ""

    if mid > IC_PRICE_CAP:
        # Cap at $2.00, single DAY order — fire-and-forget
        price = IC_PRICE_CAP
        ladder_desc = f"[CAP@{price:.2f}]"
        print(f"{TAG}: mid={mid:.2f} > cap={IC_PRICE_CAP} — placing at {price:.2f} (DAY order)")

        payload = order_payload_bundle(
            side="DEBIT", price=price, qty=qty,
            long_osi_1=v_call["long_osi"], short_osi_1=v_call["short_osi"],
            long_osi_2=v_put["long_osi"], short_osi_2=v_put["short_osi"],
        )
        res = place_order_at_price(c, acct, payload, qty,
                                   tag_prefix=f"{TAG}:CAP", wait_secs=0)
        total_filled = int(res["filled"])
        all_order_ids.extend(res["order_ids"])
        last_price = price
        print(f"{TAG}: CAP result filled={total_filled} reason={res['reason']}")
    else:
        # 3-rung ladder: mid, mid+0.05, mid+0.10
        offsets = [0.00, 0.05, 0.10]
        remaining = qty
        prices_used = []

        for rung, offset in enumerate(offsets):
            if remaining <= 0:
                break

            # Refresh NBBO on last rung
            if rung == len(offsets) - 1:
                s2, b2, a2, m2 = bundle_nbbo(
                    long_osi_1=v_call["long_osi"], short_osi_1=v_call["short_osi"],
                    long_osi_2=v_put["long_osi"], short_osi_2=v_put["short_osi"],
                    c=c,
                )
                if m2 is not None:
                    mid = m2

            price = clamp_tick(mid + offset)
            prices_used.append(f"{price:.2f}")
            wait = STEP_WAIT

            print(f"{TAG}: RUNG{rung+1} price={price:.2f} qty={remaining} wait={wait:.0f}s")
            payload = order_payload_bundle(
                side="DEBIT", price=price, qty=remaining,
                long_osi_1=v_call["long_osi"], short_osi_1=v_call["short_osi"],
                long_osi_2=v_put["long_osi"], short_osi_2=v_put["short_osi"],
            )
            res = place_order_at_price(c, acct, payload, remaining,
                                       tag_prefix=f"{TAG}:RUNG{rung+1}",
                                       wait_secs=wait)
            filled_this = int(res["filled"])
            total_filled += filled_this
            all_order_ids.extend(res["order_ids"])
            remaining -= filled_this
            last_price = price

            print(f"{TAG}: RUNG{rung+1} filled={filled_this} total={total_filled} reason={res['reason']}")

            if remaining <= 0:
                break

        ladder_desc = f"[{','.join(prices_used)}]"

    reason = "FILLED" if total_filled >= qty else f"PARTIAL_{total_filled}/{qty}"
    oid_str = ",".join(all_order_ids)
    _log_result(total_filled, oid_str, ladder_desc, last_price, reason)

    print(f"{TAG}: DONE filled={total_filled}/{qty} orders={oid_str}")

    # 6. Cleanup S3 plan
    s3_delete(bucket, s3_key)
    return 0


if __name__ == "__main__":
    sys.exit(main())
