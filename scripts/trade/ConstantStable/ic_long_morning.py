#!/usr/bin/env python3
"""IC_LONG deferred morning entry.

Reads the deferred IC_LONG plan saved by the evening orchestrator and places
the trade at morning prices with cost filters:

  1. Total debit <= $2.20  AND  both sides >= $0.40  →  buy full IC (both sides)
  2. Total debit <= $2.20  BUT  one side < $0.40     →  buy expensive side only (limit $2.00)
  3. Total debit >  $2.20                             →  buy expensive side only (limit $2.00)

Invoked via EventBridge at 9:35 AM ET for all three accounts:
  account="ic-long-morning"              (Schwab — uses Schwab placer)
  account="ic-long-morning-tt-ira"       (TT IRA 5WT20360 — uses TT placer)
  account="ic-long-morning-tt-individual" (TT Individual 5WT09219 — uses TT placer)

Each account reads its own deferred plan from a per-account S3 key
(CS_IC_DEFER_S3_KEY).  Quotes always come from Schwab.
"""

import csv
import json
import os
import re
import subprocess
import sys
import time
import random
from datetime import date, datetime, timezone
from pathlib import Path

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
CS_ACCOUNT_LABEL = os.environ.get("CS_ACCOUNT_LABEL", "schwab").strip()

# Placer script depends on account type
_IS_TT_ACCOUNT = CS_ACCOUNT_LABEL.startswith("tt-")
PLACER_SCRIPT = (
    "TT/Script/ConstantStable/place.py" if _IS_TT_ACCOUNT
    else "scripts/trade/ConstantStable/place.py"
)


# ---------------------------------------------------------------------------
# NO-CLOSE guard — re-check positions before morning placement
# ---------------------------------------------------------------------------

def _to_osi(sym: str) -> str:
    raw = (sym or "").strip().upper().lstrip(".").replace("_", "")
    m = (
        re.match(r"^([A-Z.$^]{1,6})(\d{6})([CP])(\d{1,5})(?:\.(\d{1,3}))?$", raw)
        or re.match(r"^([A-Z.$^]{1,6})(\d{6})([CP])(\d{8})$", raw)
    )
    if not m:
        raise ValueError(f"Cannot parse option symbol: {sym}")
    root, ymd, cp, strike, frac = (m.groups() + ("",))[:5]
    if len(strike) < 8:
        mills = int(strike) * 1000 + (int((frac or "0").ljust(3, "0")) if frac else 0)
    else:
        mills = int(strike)
    return f"{root:<6}{ymd}{cp}{int(mills):08d}"


def _osi_canon(osi: str):
    s = (osi or "")
    if len(s) < 21:
        return ("", "", "")
    return (s[6:12], s[12], s[-8:])


def _would_close(plan, side, pos):
    """Return True if placing this side would net/close an existing position."""
    if side == "PUT":
        sell_osi, buy_osi = plan["put_low_osi"], plan["put_high_osi"]
    else:
        sell_osi, buy_osi = plan["call_high_osi"], plan["call_low_osi"]

    buy_key = _osi_canon(buy_osi)
    sell_key = _osi_canon(sell_osi)
    buy_pos = float(pos.get(buy_key, 0.0))
    sell_pos = float(pos.get(sell_key, 0.0))

    unsafe = buy_pos < -1e-9 or sell_pos > 1e-9
    print(
        f"CS_MORNING GUARD {side}: "
        f"BUY {buy_osi} pos={buy_pos:+g} ; "
        f"SELL {sell_osi} pos={sell_pos:+g} → "
        f"{'BLOCK' if unsafe else 'OK'}"
    )
    return unsafe


def _fetch_schwab_positions(c):
    """Fetch Schwab option positions as canon → net_qty dict."""
    try:
        r = c.get_account_numbers()
        r.raise_for_status()
        arr = r.json() or []
        if not arr:
            return None
        acct_hash = str(arr[0].get("hashValue") or arr[0].get("hashvalue") or "")
        if not acct_hash:
            return None

        url = f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}"
        r2 = c.session.get(url, params={"fields": "positions"}, timeout=20)
        r2.raise_for_status()
        j = r2.json()
        sa = j[0]["securitiesAccount"] if isinstance(j, list) else (j.get("securitiesAccount") or j)

        out = {}
        for p in (sa.get("positions") or []):
            ins = p.get("instrument", {}) or {}
            if (ins.get("assetType") or "").upper() != "OPTION":
                continue
            sym = (ins.get("symbol") or "").strip()
            if not sym:
                continue
            try:
                osi = _to_osi(re.sub(r"\s+", "", sym))
            except Exception:
                continue
            qty = float(p.get("longQuantity", 0)) - float(p.get("shortQuantity", 0))
            if abs(qty) < 1e-9:
                continue
            key = _osi_canon(osi)
            out[key] = out.get(key, 0.0) + qty
        return out
    except Exception as e:
        print(f"CS_MORNING GUARD WARN: Schwab positions fetch failed ({e})")
        return None


def _fetch_tt_positions():
    """Fetch TastyTrade option positions as canon → net_qty dict."""
    try:
        # Import TT client (available when TT/Script is on sys.path)
        tt_script_dir = os.path.join(_REPO_ROOT or "", "TT", "Script")
        if tt_script_dir not in sys.path:
            sys.path.append(tt_script_dir)
        from tt_client import request as tt_request

        acct = (os.environ.get("TT_ACCOUNT_NUMBER") or "").strip()
        if not acct:
            print("CS_MORNING GUARD WARN: TT_ACCOUNT_NUMBER missing")
            return None

        last_err = ""
        for i in range(4):
            try:
                r = tt_request("GET", f"/accounts/{acct}/positions")
                j = r.json()
                break
            except Exception as e:
                resp = getattr(e, "response", None)
                if resp is not None and resp.status_code == 429:
                    ra = resp.headers.get("Retry-After")
                    wait = max(1.0, float(ra)) if ra else min(8.0, 0.6 * (2 ** i)) + random.uniform(0, 0.25)
                    time.sleep(wait)
                    continue
                last_err = str(e)
                time.sleep(min(6.0, 0.5 * (2 ** i)))
        else:
            print(f"CS_MORNING GUARD WARN: TT positions fetch failed ({last_err})")
            return None

        data = j.get("data") if isinstance(j, dict) else {}
        items = (data.get("items") if isinstance(data, dict) else None) or []

        out = {}
        for p in items:
            atype = (p.get("instrument-type") or p.get("instrument_type") or "").upper()
            if "OPTION" not in atype:
                continue
            sym = (p.get("symbol") or "").strip()
            if not sym:
                continue
            try:
                osi = _to_osi(re.sub(r"\s+", "", sym))
            except Exception:
                continue
            qty = float(p.get("quantity", 0) or 0)
            direction = str(p.get("quantity-direction") or p.get("quantity_direction") or "").lower()
            if direction.startswith("short"):
                qty = -abs(qty)
            if abs(qty) < 1e-9:
                continue
            key = _osi_canon(osi)
            out[key] = out.get(key, 0.0) + qty
        return out
    except Exception as e:
        print(f"CS_MORNING GUARD WARN: TT positions fetch failed ({e})")
        return None


def _fetch_positions(c):
    """Fetch positions for the current account type."""
    if _IS_TT_ACCOUNT:
        return _fetch_tt_positions()
    return _fetch_schwab_positions(c)


def _add_scripts_root():
    cur = os.path.abspath(os.path.dirname(__file__))
    while True:
        if os.path.basename(cur) == "scripts":
            repo_root = os.path.dirname(cur)
            if cur not in sys.path:
                sys.path.append(cur)
            if repo_root not in sys.path:
                sys.path.insert(0, repo_root)
            return repo_root
        parent = os.path.dirname(cur)
        if parent == cur:
            return None
        cur = parent


_REPO_ROOT = _add_scripts_root()
from schwab_token_keeper import schwab_client

_ew = None
try:
    if _REPO_ROOT:
        repo_root = str(Path(_REPO_ROOT))
        if repo_root not in sys.path:
            sys.path.insert(0, repo_root)
    from reporting.events import EventWriter
    _EVENTS_AVAILABLE = True
except ImportError:
    _EVENTS_AVAILABLE = False


def _init_events(exec_date: date):
    global _ew
    if not _EVENTS_AVAILABLE:
        return None
    try:
        _ew = EventWriter(strategy="constantstable", account=CS_ACCOUNT_LABEL, trade_date=exec_date)
        return _ew
    except Exception as e:
        print(f"CS_MORNING WARN: EventWriter init failed ({e})")
        return None


def _emit(method: str, **kwargs):
    if _ew is None:
        return
    try:
        getattr(_ew, method)(**kwargs)
    except Exception as e:
        print(f"CS_MORNING WARN: event emit failed ({method}): {e}")


def _close_events():
    if _ew is None:
        return
    try:
        _ew.close()
    except Exception:
        pass


def _csv_row_count() -> int:
    try:
        with open(CS_LOG_PATH) as f:
            return sum(1 for _ in csv.DictReader(f))
    except Exception:
        return 0


def _read_back_fills(group_ids: list[tuple[str, str]], rows_before: int):
    if _ew is None:
        return
    try:
        with open(CS_LOG_PATH) as f:
            all_rows = list(csv.DictReader(f))
        new_rows = all_rows[rows_before:]
        if not new_rows:
            return

        name_to_group = {name: gid for name, gid in group_ids}
        for row in new_rows:
            name = row.get("name", "")
            saved_gid = name_to_group.get(name)
            if saved_gid and _ew is not None:
                _ew.trade_group_id = saved_gid

            oids = [x for x in (row.get("order_ids") or "").split(",") if x]
            filled = int(row.get("qty_filled") or 0)
            price = float(row.get("last_price") or 0) if row.get("last_price") else 0
            short_osi = row.get("short_osi", "")
            long_osi = row.get("long_osi", "")
            kind = row.get("kind", "")
            requested = int(row.get("qty_requested") or 0)

            for oid in oids:
                _emit(
                    "order_submitted",
                    order_id=oid,
                    legs=[
                        {"osi": short_osi, "option_type": kind, "action": "SELL_TO_OPEN", "qty": requested},
                        {"osi": long_osi, "option_type": kind, "action": "BUY_TO_OPEN", "qty": requested},
                    ],
                    limit_price=price,
                )
            if filled > 0:
                _emit(
                    "fill",
                    order_id=oids[0] if oids else "",
                    fill_qty=filled,
                    fill_price=price,
                    legs=[
                        {"osi": short_osi, "option_type": kind, "qty": filled},
                        {"osi": long_osi, "option_type": kind, "qty": filled},
                    ],
                )
    except Exception as e:
        print(f"CS_MORNING WARN: could not read placement result ({e})")


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


def mark_plan_status(plan, status, result, **extra):
    """Update plan status in S3 so it won't be re-executed silently."""
    plan["status"] = status
    plan["updated_utc"] = datetime.now(timezone.utc).isoformat()
    plan["result"] = result
    for key, value in extra.items():
        plan[key] = value
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


QUOTE_RETRIES = 4


def _get_quote_with_retry(c, osi: str) -> tuple:
    """Fetch bid/ask for a single OSI with retry (same pattern as placer)."""
    for i in range(QUOTE_RETRIES):
        try:
            r = c.get_quote(osi)
            if r.status_code == 200:
                j = r.json()
                d = list(j.values())[0] if isinstance(j, dict) else {}
                q = d.get("quote", d)
                b = q.get("bidPrice") or q.get("bid") or q.get("bidPriceInDouble")
                a = q.get("askPrice") or q.get("ask") or q.get("askPriceInDouble")
                return (float(b) if b is not None else None,
                        float(a) if a is not None else None)
            if r.status_code == 429:
                wait = min(6.0, 0.5 * (2 ** i)) + random.uniform(0, 0.25)
                time.sleep(wait)
                continue
            time.sleep(min(2.0, 0.35 * (2 ** i)))
        except Exception as e:
            print(f"CS_MORNING WARN: quote retry {i+1}/{QUOTE_RETRIES} for {osi}: {e}")
            time.sleep(min(2.0, 0.35 * (2 ** i)))
    return (None, None)


def get_morning_prices(c, plan):
    """Fetch current mid prices for both spreads from Schwab (per-leg with retry)."""
    def mid_price(osi):
        bid, ask = _get_quote_with_retry(c, osi)
        if bid is None or ask is None or (bid <= 0 and ask <= 0):
            return None
        return round((bid + ask) / 2, 2)

    # Put spread: buy high strike put, sell low strike put (debit)
    put_long_mid = mid_price(plan["put_high_osi"])  # buy
    put_short_mid = mid_price(plan["put_low_osi"])   # sell
    # Call spread: buy low strike call, sell high strike call (debit)
    call_long_mid = mid_price(plan["call_low_osi"])   # buy
    call_short_mid = mid_price(plan["call_high_osi"])  # sell

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
    name = f"{side}_LONG_MORNING"
    if side == "PUT":
        short_osi = plan["put_low_osi"]
        long_osi = plan["put_high_osi"]
        qty = plan["put_qty"]
        strength = plan.get("put_strength", 0)
        go = plan.get("put_go")
        gw_price = plan.get("put_credit_close")
    else:
        short_osi = plan["call_high_osi"]
        long_osi = plan["call_low_osi"]
        qty = plan["call_qty"]
        strength = plan.get("call_strength", 0)
        go = plan.get("call_go")
        gw_price = plan.get("call_credit_close")

    env = dict(os.environ)
    env.update({
        "VERT_SIDE": "DEBIT",
        "VERT_KIND": side,
        "VERT_NAME": name,
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

    # TT placer needs TT-specific env vars
    if _IS_TT_ACCOUNT:
        env.update({
            "TT_TOKEN_JSON": os.environ.get("TT_TOKEN_JSON", ""),
            "TT_ACCOUNT_NUMBER": os.environ.get("TT_ACCOUNT_NUMBER", ""),
            "TT_BASE_URL": os.environ.get("TT_BASE_URL", "https://api.tastyworks.com"),
            "TT_CLIENT_AUTH": os.environ.get("TT_CLIENT_AUTH", ""),
        })

    if DRY_RUN:
        print(f"CS_MORNING DRY_RUN: would place {side}_LONG qty={qty} limit={limit_price:.2f} {short_osi}/{long_osi} placer={PLACER_SCRIPT}")
        return 0

    rc = subprocess.call(
        [sys.executable, PLACER_SCRIPT],
        env=env,
    )
    return rc


def place_full_ic(plan, put_limit, call_limit):
    """Place a full IC as a 4-leg debit bundle when quantities match."""
    if int(plan["put_qty"]) != int(plan["call_qty"]):
        print("CS_MORNING WARN: asymmetric qty; falling back to separate verticals for full IC")
        put_rc = place_vertical(plan, "PUT", put_limit)
        call_rc = place_vertical(plan, "CALL", call_limit)
        return [("PUT", put_rc), ("CALL", call_rc)]

    total_limit = round(put_limit + call_limit, 2)
    env = dict(os.environ)
    env.update({
        "VERT_BUNDLE": "true",
        "VERT_SIDE": "DEBIT",
        "VERT_KIND": "CALL",
        "VERT_NAME": "CALL_LONG_MORNING",
        "VERT_DIRECTION": "LONG",
        "VERT_SHORT_OSI": plan["call_high_osi"],
        "VERT_LONG_OSI": plan["call_low_osi"],
        "VERT_QTY": str(plan["call_qty"]),
        "VERT_GO": "" if plan.get("call_go") is None else str(plan.get("call_go")),
        "VERT_STRENGTH": f"{float(plan.get('call_strength', 0)):.3f}",
        "VERT_GW_PRICE": "" if plan.get("call_credit_close") is None else str(plan.get("call_credit_close")),
        "VERT2_SIDE": "DEBIT",
        "VERT2_KIND": "PUT",
        "VERT2_NAME": "PUT_LONG_MORNING",
        "VERT2_DIRECTION": "LONG",
        "VERT2_SHORT_OSI": plan["put_low_osi"],
        "VERT2_LONG_OSI": plan["put_high_osi"],
        "VERT2_QTY": str(plan["put_qty"]),
        "VERT2_GO": "" if plan.get("put_go") is None else str(plan.get("put_go")),
        "VERT2_STRENGTH": f"{float(plan.get('put_strength', 0)):.3f}",
        "VERT2_GW_PRICE": "" if plan.get("put_credit_close") is None else str(plan.get("put_credit_close")),
        "VERT_TRADE_DATE": plan["trade_date"],
        "VERT_TDATE": plan["execute_date"],
        "VERT_LIMIT_PRICE": f"{total_limit:.2f}",
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

    # TT placer needs TT-specific env vars
    if _IS_TT_ACCOUNT:
        env.update({
            "TT_TOKEN_JSON": os.environ.get("TT_TOKEN_JSON", ""),
            "TT_ACCOUNT_NUMBER": os.environ.get("TT_ACCOUNT_NUMBER", ""),
            "TT_BASE_URL": os.environ.get("TT_BASE_URL", "https://api.tastyworks.com"),
            "TT_CLIENT_AUTH": os.environ.get("TT_CLIENT_AUTH", ""),
        })

    if DRY_RUN:
        print(
            "CS_MORNING DRY_RUN: would place FULL_IC "
            f"qty={plan['put_qty']} total_limit={total_limit:.2f} placer={PLACER_SCRIPT}"
        )
        return [("FULL_IC", 0)]

    rc = subprocess.call(
        [sys.executable, PLACER_SCRIPT],
        env=env,
    )
    return [("FULL_IC", rc)]


def main():
    if _REPO_ROOT:
        os.chdir(_REPO_ROOT)

    exec_date = date.today()

    # Weekend guard — market is closed, quotes will fail, don't waste the plan
    if exec_date.weekday() >= 5:  # 5=Saturday, 6=Sunday
        print(f"CS_MORNING SKIP: weekend ({exec_date.isoformat()}, weekday={exec_date.weekday()})")
        return 0

    print("=" * 60)
    print(f"CS_MORNING IC_LONG deferred entry — {datetime.now(timezone.utc).isoformat()}")
    print(f"  account={CS_ACCOUNT_LABEL}  placer={PLACER_SCRIPT}")
    print(f"  max_total={MAX_TOTAL_DEBIT}  min_side={MIN_SIDE_PRICE}  max_single={MAX_SINGLE_SIDE}")
    print(f"  dry_run={DRY_RUN}")
    print("=" * 60)

    plan = load_deferred_plan()
    _init_events(exec_date)
    if not plan:
        _emit(
            "strategy_run",
            signal="IC_LONG_MORNING",
            config="IC_LONG_MORNING",
            reason="NO_PENDING_PLAN",
            extra={"execute_date": exec_date.isoformat()},
        )
        _emit("skip", reason="NO_PENDING_PLAN", signal="SKIP")
        _close_events()
        return 0

    _emit(
        "strategy_run",
        signal="IC_LONG_MORNING",
        config="IC_LONG_MORNING",
        reason="PENDING_PLAN",
        extra={
            "execute_date": plan["execute_date"],
            "signal_trade_date": plan["trade_date"],
            "signal_tdate": plan["execute_date"],
            "plan_status": plan.get("status", ""),
        },
    )

    print(f"CS_MORNING PLAN: trade_date={plan['trade_date']} execute_date={plan['execute_date']}")
    print(f"  put {plan['p_low']}/{plan['p_high']}  call {plan['c_low']}/{plan['c_high']}")
    print(f"  close prices: put={plan.get('put_credit_close')} call={plan.get('call_credit_close')}")
    print(f"  qty: put={plan['put_qty']} call={plan['call_qty']}")

    # Get Schwab client
    try:
        c = schwab_client()
    except Exception as e:
        print(f"CS_MORNING ERROR: Schwab init failed ({e})")
        _emit("error", message=str(e), stage="schwab_init")
        mark_plan_status(plan, "error", f"ERROR: schwab_init: {e}")
        _close_events()
        return 1

    # Fetch morning prices
    put_price, call_price = get_morning_prices(c, plan)
    if put_price is None or call_price is None:
        print("CS_MORNING SKIP: could not get morning prices — plan stays pending for retry")
        _emit("skip", reason="NO_MORNING_PRICES", signal="SKIP")
        # Do NOT mark plan as skipped — keep "pending" so the next account
        # invocation (or a manual retry) can try again with fresh quotes.
        _close_events()
        return 0

    print(f"CS_MORNING PRICES: put={put_price:.2f} call={call_price:.2f} total={put_price + call_price:.2f}")
    print(f"  vs close: put={plan.get('put_credit_close')} call={plan.get('call_credit_close')}")

    # Apply price rules
    sides_to_trade = apply_price_rules(put_price, call_price)

    if not sides_to_trade:
        print("CS_MORNING SKIP: no sides pass price rules")
        _emit("skip", reason="PRICE_RULES", signal="SKIP")
        mark_plan_status(plan, "skipped", "SKIP: price_rules")
        _close_events()
        return 0

    # NO-CLOSE guard: re-check positions before morning placement.
    # Overnight assignments or close-order fills could make placement unsafe.
    pos = _fetch_positions(c)
    if pos is not None:
        print(f"CS_MORNING GUARD: loaded {len(pos)} option positions")
        safe_sides = []
        for side, limit in sides_to_trade:
            if not _would_close(plan, side, pos):
                safe_sides.append((side, limit))
            else:
                print(f"CS_MORNING GUARD SKIP: {side} blocked — would close existing position")
                _emit("skip", reason=f"GUARD_WOULD_CLOSE_{side}", signal="SKIP")
        sides_to_trade = safe_sides
        if not sides_to_trade:
            print("CS_MORNING SKIP: all sides blocked by guard")
            mark_plan_status(plan, "skipped", "SKIP: guard_would_close")
            _close_events()
            return 0
    else:
        # If we can't fetch positions, skip all trades (safe default).
        print("CS_MORNING SKIP: cannot fetch positions for guard check — skipping all")
        _emit("skip", reason="GUARD_POSITIONS_UNAVAILABLE", signal="SKIP")
        mark_plan_status(plan, "skipped", "SKIP: guard_positions_unavailable")
        _close_events()
        return 0

    # Emit trade_intent(s) before placement.
    saved_groups = []
    for side, limit in sides_to_trade:
        if _ew is not None:
            name = "FULL_IC" if side == "FULL_IC" else f"{side}_LONG_MORNING"
            _ew.new_trade_group()
            saved_groups.append((name, _ew.trade_group_id))
        if side == "PUT":
            legs = [
                {"osi": plan["put_low_osi"], "option_type": "PUT", "action": "SELL_TO_OPEN", "qty": int(plan["put_qty"])},
                {"osi": plan["put_high_osi"], "option_type": "PUT", "action": "BUY_TO_OPEN", "qty": int(plan["put_qty"])},
            ]
            target_qty = int(plan["put_qty"])
        else:
            legs = [
                {"osi": plan["call_high_osi"], "option_type": "CALL", "action": "SELL_TO_OPEN", "qty": int(plan["call_qty"])},
                {"osi": plan["call_low_osi"], "option_type": "CALL", "action": "BUY_TO_OPEN", "qty": int(plan["call_qty"])},
            ]
            target_qty = int(plan["call_qty"])
        _emit(
            "trade_intent",
            side="DEBIT",
            direction="LONG",
            legs=legs,
            target_qty=target_qty,
            limit_price=limit,
            extra={"name": f"{side}_LONG_MORNING"},
        )

    rows_before = _csv_row_count()

    if len(sides_to_trade) == 2 and {side for side, _ in sides_to_trade} == {"PUT", "CALL"}:
        limit_map = {side: limit for side, limit in sides_to_trade}
        print(f"\nCS_MORNING PLACING: FULL_IC total_limit={limit_map['PUT'] + limit_map['CALL']:.2f}")
        results = place_full_ic(plan, limit_map["PUT"], limit_map["CALL"])
    else:
        results = []
        for side, limit in sides_to_trade:
            print(f"\nCS_MORNING PLACING: {side} limit={limit:.2f}")
            rc = place_vertical(plan, side, limit)
            results.append((side, rc))
            print(f"CS_MORNING RESULT: {side} rc={rc}")

    if all(rc == 0 for _, rc in results):
        _read_back_fills(saved_groups, rows_before)
        _emit("post_step_result", step_name="morning_place", outcome="OK")
        summary = "; ".join(f"{s}:rc={rc}" for s, rc in results)
        mark_plan_status(plan, "executed", f"EXECUTED: {summary}")
        print(f"\nCS_MORNING DONE: {summary}")
        _close_events()
        return 0

    summary = "; ".join(f"{s}:rc={rc}" for s, rc in results)
    failed_sides = [side for side, rc in results if rc != 0]
    _emit("error", message=f"placement failure: {summary}", stage="morning_place")
    mark_plan_status(plan, "error", f"ERROR: {summary}", failed_sides=failed_sides)
    print(f"\nCS_MORNING ERROR: {summary}")
    _close_events()
    return 1


if __name__ == "__main__":
    sys.exit(main())
