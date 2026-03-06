#!/usr/bin/env python3
"""Schwab placer for 1-2-1 call butterflies."""

from __future__ import annotations

import csv
import json
import os
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo


def _add_scripts_root() -> None:
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


def clamp_tick(value: float) -> float:
    return round(round(float(value) / TICK) * TICK + 1e-12, 2)


def truthy(value: str) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _fnum(value):
    try:
        return float(value)
    except Exception:
        return None


def write_result(result: dict) -> None:
    path = Path(os.environ.get("BF_RESULT_PATH", "")).expanduser()
    if not path:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result, indent=2, sort_keys=True))


def log_row(row: dict) -> None:
    path = Path(os.environ.get("BF_LOG_PATH", "logs/butterfly_tuesday_trades.csv"))
    path.parent.mkdir(parents=True, exist_ok=True)
    cols = [
        "ts_utc",
        "ts_et",
        "trade_date",
        "expiry_date",
        "signal",
        "config",
        "order_side",
        "qty",
        "spot",
        "vix",
        "vix1d",
        "lower_strike",
        "center_strike",
        "upper_strike",
        "width",
        "lower_osi",
        "center_osi",
        "upper_osi",
        "package_bid",
        "package_ask",
        "package_mid",
        "ladder_plan",
        "last_price",
        "filled_qty",
        "order_ids",
        "reason",
    ]
    write_header = not path.exists()
    with path.open("a", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=cols)
        if write_header:
            writer.writeheader()
        writer.writerow({col: row.get(col, "") for col in cols})


def fetch_bid_ask(c, osi: str):
    resp = c.get_quote(osi)
    if resp.status_code != 200:
        return None, None
    data = list(resp.json().values())[0] if isinstance(resp.json(), dict) else {}
    q = data.get("quote", data)
    bid = q.get("bidPrice") or q.get("bid") or q.get("bidPriceInDouble")
    ask = q.get("askPrice") or q.get("ask") or q.get("askPriceInDouble")
    return (_fnum(bid), _fnum(ask))


def butterfly_nbbo(c, lower_osi: str, center_osi: str, upper_osi: str):
    lb, la = fetch_bid_ask(c, lower_osi)
    cb, ca = fetch_bid_ask(c, center_osi)
    ub, ua = fetch_bid_ask(c, upper_osi)
    if None in (lb, la, cb, ca, ub, ua):
        return None, None, None
    bid = clamp_tick(max(0.0, lb + ub - 2.0 * ca))
    ask = clamp_tick(max(bid, la + ua - 2.0 * cb))
    mid = clamp_tick((bid + ask) / 2.0)
    return bid, ask, mid


def order_payload(order_side: str, price: float, qty: int, lower_osi: str, center_osi: str, upper_osi: str) -> dict:
    order_side = order_side.upper()
    if order_side == "DEBIT":
        order_type = "NET_DEBIT"
        legs = [
            {"instruction": "BUY_TO_OPEN", "positionEffect": "OPENING", "quantity": qty, "instrument": {"symbol": lower_osi, "assetType": "OPTION"}},
            {"instruction": "SELL_TO_OPEN", "positionEffect": "OPENING", "quantity": qty * 2, "instrument": {"symbol": center_osi, "assetType": "OPTION"}},
            {"instruction": "BUY_TO_OPEN", "positionEffect": "OPENING", "quantity": qty, "instrument": {"symbol": upper_osi, "assetType": "OPTION"}},
        ]
    elif order_side == "CREDIT":
        order_type = "NET_CREDIT"
        legs = [
            {"instruction": "SELL_TO_OPEN", "positionEffect": "OPENING", "quantity": qty, "instrument": {"symbol": lower_osi, "assetType": "OPTION"}},
            {"instruction": "BUY_TO_OPEN", "positionEffect": "OPENING", "quantity": qty * 2, "instrument": {"symbol": center_osi, "assetType": "OPTION"}},
            {"instruction": "SELL_TO_OPEN", "positionEffect": "OPENING", "quantity": qty, "instrument": {"symbol": upper_osi, "assetType": "OPTION"}},
        ]
    else:
        raise ValueError(f"Unsupported order side: {order_side}")

    return {
        "orderType": order_type,
        "session": "NORMAL",
        "price": f"{clamp_tick(price):.2f}",
        "duration": "DAY",
        "orderStrategyType": "SINGLE",
        "complexOrderStrategyType": "CUSTOM",
        "orderLegCollection": legs,
    }


def parse_order_id(resp) -> str:
    try:
        payload = resp.json()
        if isinstance(payload, dict):
            oid = payload.get("orderId") or payload.get("order_id")
            if oid:
                return str(oid)
    except Exception:
        pass
    loc = resp.headers.get("Location", "")
    return loc.rstrip("/").split("/")[-1] if loc else ""


def extract_filled_quantity(status_payload: dict) -> int:
    for key in (
        "filledQuantity",
        "filled_quantity",
        "filledQty",
        "filledQtyInDouble",
        "filledQuantityInDouble",
    ):
        if key in status_payload:
            value = _fnum(status_payload.get(key))
            if value is not None:
                return int(round(value))

    total = 0.0
    for activity in status_payload.get("orderActivityCollection") or []:
        if str(activity.get("activityType") or "").upper() != "EXECUTION":
            continue
        qtys = []
        for leg in activity.get("executionLegs") or []:
            q = _fnum(leg.get("quantity"))
            if q is not None:
                qtys.append(q)
        if qtys:
            total += min(qtys)
    return int(round(total))


def post_with_retry(c, url: str, payload: dict, tries: int = 5):
    last = ""
    for attempt in range(tries):
        resp = c.session.post(url, json=payload, timeout=20)
        if resp.status_code in (200, 201, 202):
            return resp
        if resp.status_code == 429:
            wait = min(10.0, 0.7 * (2 ** attempt)) + random.uniform(0.0, 0.35)
            time.sleep(wait)
            continue
        last = f"HTTP_{resp.status_code}:{(resp.text or '')[:200]}"
        time.sleep(min(6.0, 0.45 * (2 ** attempt)))
    raise RuntimeError(last or "POST_FAIL")


def delete_with_retry(c, url: str, tries: int = 4) -> bool:
    for attempt in range(tries):
        resp = c.session.delete(url, timeout=20)
        if resp.status_code in (200, 201, 202, 204):
            return True
        if resp.status_code == 429:
            wait = min(6.0, 0.5 * (2 ** attempt)) + random.uniform(0.0, 0.25)
            time.sleep(wait)
            continue
        time.sleep(min(3.0, 0.35 * (2 ** attempt)))
    return False


def get_status(c, acct_hash: str, oid: str) -> dict:
    url = f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{oid}"
    try:
        resp = c.session.get(url, timeout=20)
        if resp.status_code != 200:
            return {}
        return resp.json() or {}
    except Exception:
        return {}


def price_from_mid(mid: float, offset: float, bid: float, ask: float) -> float:
    price = clamp_tick(mid + offset)
    return min(max(price, bid), ask)


def ladder_offsets(order_side: str) -> list[float]:
    base = [0.00, 0.05, 0.10]
    if order_side.upper() == "CREDIT":
        return [-x for x in base]
    return base


def get_account_hash(c) -> str:
    resp = c.get_account_numbers()
    resp.raise_for_status()
    arr = resp.json() or []
    return str(arr[0]["hashValue"]) if arr else ""


def main() -> int:
    order_side = (os.environ.get("BF_ORDER_SIDE") or "").upper()
    qty = max(1, int(os.environ.get("BF_QTY", "1")))
    lower_osi = os.environ["BF_LOWER_OSI"]
    center_osi = os.environ["BF_CENTER_OSI"]
    upper_osi = os.environ["BF_UPPER_OSI"]
    dry_run = truthy(os.environ.get("BF_DRY_RUN", "1"))

    c = schwab_client()
    bid, ask, mid = butterfly_nbbo(c, lower_osi, center_osi, upper_osi)
    if None in (bid, ask, mid):
        result = {"success": False, "reason": "NBBO_UNAVAILABLE"}
        write_result(result)
        return 1

    offsets = ladder_offsets(order_side)
    ladder = [f"{price_from_mid(mid, offset, bid, ask):.2f}" for offset in offsets]

    common = {
        "trade_date": os.environ.get("BF_TRADE_DATE", ""),
        "expiry_date": os.environ.get("BF_EXPIRY_DATE", ""),
        "signal": os.environ.get("BF_SIGNAL", ""),
        "config": os.environ.get("BF_CONFIG", ""),
        "order_side": order_side,
        "qty": qty,
        "spot": os.environ.get("BF_SPOT", ""),
        "vix": os.environ.get("BF_VIX", ""),
        "vix1d": os.environ.get("BF_VIX1D", ""),
        "lower_strike": os.environ.get("BF_LOWER_STRIKE", ""),
        "center_strike": os.environ.get("BF_CENTER_STRIKE", ""),
        "upper_strike": os.environ.get("BF_UPPER_STRIKE", ""),
        "width": os.environ.get("BF_WIDTH", ""),
        "lower_osi": lower_osi,
        "center_osi": center_osi,
        "upper_osi": upper_osi,
        "package_bid": bid,
        "package_ask": ask,
        "package_mid": mid,
        "ladder_plan": "[" + ", ".join(ladder) + "]",
    }

    if dry_run:
        result = {
            "success": True,
            "dry_run": True,
            "reason": "DRY_RUN",
            "filled_qty": 0,
            "order_ids": [],
            "last_price": None,
            "package_bid": bid,
            "package_ask": ask,
            "package_mid": mid,
            "ladder_plan": ladder,
        }
        write_result(result)
        log_row(
            {
                "ts_utc": datetime.now(timezone.utc).isoformat(),
                "ts_et": datetime.now(ET).isoformat(),
                **common,
                "filled_qty": 0,
                "order_ids": "",
                "last_price": "",
                "reason": "DRY_RUN",
            }
        )
        return 0

    acct_hash = get_account_hash(c)
    if not acct_hash:
        write_result({"success": False, "reason": "NO_ACCOUNT_HASH"})
        return 1

    url_post = f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders"
    step_wait = float(os.environ.get("BF_STEP_WAIT", "10"))
    poll_secs = float(os.environ.get("BF_POLL_SECS", "1.5"))
    cancel_settle = float(os.environ.get("BF_CANCEL_SETTLE", "1.0"))

    filled_qty = 0
    order_ids: list[str] = []
    last_price: float | None = None
    reason = "NO_FILL"

    for offset in offsets:
        price = price_from_mid(mid, offset, bid, ask)
        last_price = price
        payload = order_payload(order_side, price, qty, lower_osi, center_osi, upper_osi)
        try:
            resp = post_with_retry(c, url_post, payload)
        except Exception as exc:
            reason = f"POST_FAIL:{exc}"
            continue

        oid = parse_order_id(resp)
        if oid:
            order_ids.append(oid)

        end_time = time.time() + step_wait
        while time.time() < end_time and oid:
            status = get_status(c, acct_hash, oid)
            state = str(status.get("status") or status.get("orderStatus") or "").upper()
            fq = extract_filled_quantity(status)
            if state == "FILLED" and fq <= 0:
                fq = qty
            if fq > filled_qty:
                filled_qty = fq
            if state in FINAL_STATUSES:
                break
            time.sleep(poll_secs)

        if filled_qty >= qty:
            reason = "OK"
            break

        if oid:
            url_del = f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{oid}"
            delete_with_retry(c, url_del)
            time.sleep(cancel_settle)

    result = {
        "success": reason in {"OK", "NO_FILL"},
        "dry_run": False,
        "reason": reason,
        "filled_qty": filled_qty,
        "order_ids": order_ids,
        "last_price": last_price,
        "package_bid": bid,
        "package_ask": ask,
        "package_mid": mid,
        "ladder_plan": ladder,
    }
    write_result(result)
    log_row(
        {
            "ts_utc": datetime.now(timezone.utc).isoformat(),
            "ts_et": datetime.now(ET).isoformat(),
            **common,
            "filled_qty": filled_qty,
            "order_ids": ",".join(order_ids),
            "last_price": "" if last_price is None else f"{last_price:.2f}",
            "reason": reason,
        }
    )
    return 0 if result["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
