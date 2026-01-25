#!/usr/bin/env python3
# Manual trade ticket + execution wrapper (SPX 0DTE)
# Captures structured intent, computes max loss/profit, then executes via existing Schwab flow.

import csv
import json
import os
import sys
import time
import subprocess
from datetime import datetime, timezone, date
from zoneinfo import ZoneInfo

def _add_scripts_root():
    cur = os.path.abspath(os.path.dirname(__file__))
    while True:
        if os.path.basename(cur) == "scripts":
            if cur not in sys.path:
                sys.path.append(cur)
            return
        scripts_dir = os.path.join(cur, "scripts")
        if os.path.isdir(scripts_dir):
            if scripts_dir not in sys.path:
                sys.path.append(scripts_dir)
            return
        parent = os.path.dirname(cur)
        if parent == cur:
            return
        cur = parent


_add_scripts_root()
from schwab_token_keeper import schwab_client as schwab_client_base

ET = ZoneInfo("America/New_York")
TICK = 0.05


def clamp_tick(x: float) -> float:
    return round(round(float(x) / TICK) * TICK + 1e-12, 2)


def prompt_choice(label: str, options: list[str]) -> str:
    print(f"{label}:")
    for i, opt in enumerate(options, start=1):
        print(f"  {i}) {opt}")
    while True:
        raw = input("> ").strip()
        if raw.isdigit():
            idx = int(raw)
            if 1 <= idx <= len(options):
                return options[idx - 1]
        print("Choose a number from the list.")


def prompt_float(label: str) -> float:
    while True:
        raw = input(f"{label}: ").strip()
        try:
            return float(raw)
        except Exception:
            print("Enter a number.")


def prompt_int(label: str) -> int:
    while True:
        raw = input(f"{label}: ").strip()
        try:
            return int(float(raw))
        except Exception:
            print("Enter an integer.")


def prompt_yes_no(label: str) -> bool:
    while True:
        raw = input(f"{label} [y/n]: ").strip().lower()
        if raw in ("y", "yes"):
            return True
        if raw in ("n", "no"):
            return False
        print("Enter y or n.")


def yymmdd(iso: str) -> str:
    d = date.fromisoformat((iso or "")[:10])
    return f"{d:%y%m%d}"


def to_osi(sym: str) -> str:
    raw = (sym or "").strip().upper().lstrip(".").replace("_", "")
    import re
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


def schwab_client():
    return schwab_client_base()


def get_quote_json_with_retry(c, osi: str, tries: int = 4):
    last = None
    for i in range(tries):
        r = c.get_quote(osi)
        if r.status_code == 200:
            try:
                return r.json()
            except Exception:
                return None
        if r.status_code == 429:
            time.sleep(min(6.0, 0.5 * (2 ** i)))
            continue
        last = r.status_code
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


def vertical_nbbo(side: str, short_osi: str, long_osi: str, c):
    sb, sa = fetch_bid_ask(c, short_osi)
    lb, la = fetch_bid_ask(c, long_osi)
    if None in (sb, sa, lb, la):
        return (None, None, None)
    if side.upper() == "CREDIT":
        bid = sb - la
        ask = sa - lb
    else:
        bid = lb - sa
        ask = la - sb
    bid = clamp_tick(bid)
    ask = clamp_tick(ask)
    mid = clamp_tick((bid + ask) / 2.0)
    return bid, ask, mid


def ensure_dir(path: str):
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)


def append_jsonl(path: str, row: dict):
    ensure_dir(path)
    with open(path, "a") as f:
        f.write(json.dumps(row, separators=(",", ":"), sort_keys=False) + "\n")


def read_last_csv_row(path: str):
    if not os.path.exists(path):
        return None
    with open(path, newline="") as f:
        rows = list(csv.DictReader(f))
    return rows[-1] if rows else None


def main():
    print("Manual SPX 0DTE ticket (no strategy hook)")

    side = prompt_choice("Side", ["DEBIT", "CREDIT"])
    kind = prompt_choice("Type", ["PUT", "CALL"])
    qty = prompt_int("Quantity (contracts)")
    short_strike = int(prompt_float("Short strike"))
    long_strike = int(prompt_float("Long strike"))

    width = abs(int(short_strike) - int(long_strike))
    if width <= 0:
        print("Width must be > 0.")
        return 2

    strike_methods = [
        "gamma_level",
        "previous_range",
        "trendline",
        "key_level",
        "other_rule_based",
    ]
    invalidations = [
        "breaks_level",
        "time_decay",
        "vol_spike",
        "invalidates_thesis",
        "other_rule_based",
    ]

    strike_method = prompt_choice("Strike selection method", strike_methods)
    invalidation = prompt_choice("Invalidation trigger", invalidations)

    exit_profit = prompt_float("Profit target (%)")
    exit_stop = prompt_float("Stop loss (%)")
    exit_cutoff = input("Time cutoff (HH:MM ET): ").strip()

    calm = prompt_yes_no("Calm")
    reenter = prompt_yes_no("Re-entering")
    up_day = prompt_yes_no("Up on the day")

    today_et = datetime.now(ET).date()
    exp6 = yymmdd(today_et.isoformat())
    root = "SPXW"

    cp = "P" if kind == "PUT" else "C"
    short_osi = to_osi(f".{root}{exp6}{cp}{short_strike}")
    long_osi = to_osi(f".{root}{exp6}{cp}{long_strike}")

    c = schwab_client()
    bid, ask, mid = vertical_nbbo(side, short_osi, long_osi, c)
    if bid is None or ask is None or mid is None:
        print("NBBO unavailable. Aborting.")
        return 2

    credit = mid if side == "CREDIT" else None
    debit = mid if side == "DEBIT" else None
    mult = 100 * qty
    if side == "CREDIT":
        max_profit = round(credit * mult, 2)
        max_loss = round((width - credit) * mult, 2)
    else:
        max_loss = round(debit * mult, 2)
        max_profit = round((width - debit) * mult, 2)

    print("")
    print("Ticket preview:")
    print(f"  {side} {kind} short={short_strike} long={long_strike} width={width} qty={qty}")
    print(f"  NBBO: bid={bid:.2f} ask={ask:.2f} mid={mid:.2f}")
    print(f"  Max profit=${max_profit:.2f}  Max loss=${max_loss:.2f}")

    ticket_id = f"ticket_{int(time.time())}"
    now_utc = datetime.now(timezone.utc).isoformat()
    ticket_log = os.environ.get("TRADE_MONITOR_LOG", "logs/trade_monitors/tickets.jsonl")

    ticket = {
        "event": "ticket_created",
        "ticket_id": ticket_id,
        "ts_utc": now_utc,
        "side": side,
        "kind": kind,
        "short_strike": short_strike,
        "long_strike": long_strike,
        "width": width,
        "qty": qty,
        "nbbo_bid": bid,
        "nbbo_ask": ask,
        "nbbo_mid": mid,
        "max_profit": max_profit,
        "max_loss": max_loss,
        "strike_method": strike_method,
        "invalidation": invalidation,
        "exit_profit_pct": exit_profit,
        "exit_stop_pct": exit_stop,
        "exit_cutoff_et": exit_cutoff,
        "calm": calm,
        "reentering": reenter,
        "up_on_day": up_day,
    }
    append_jsonl(ticket_log, ticket)

    confirm = input("Type CONFIRM to execute: ").strip().upper()
    if confirm != "CONFIRM":
        append_jsonl(ticket_log, {"event": "ticket_canceled", "ticket_id": ticket_id, "ts_utc": datetime.now(timezone.utc).isoformat()})
        print("Canceled.")
        return 0

    env = dict(os.environ)
    env.update({
        "VERT_SIDE": side,
        "VERT_KIND": kind,
        "VERT_NAME": f"MANUAL_{kind}",
        "VERT_DIRECTION": "LONG" if side == "DEBIT" else "SHORT",
        "VERT_SHORT_OSI": short_osi,
        "VERT_LONG_OSI": long_osi,
        "VERT_QTY": str(qty),
        "VERT_TRADE_DATE": today_et.isoformat(),
        "VERT_TDATE": today_et.isoformat(),
        "CS_LOG_PATH": "logs/trade_monitors/manual_vertical_trades.csv",
    })

    rc = subprocess.call([sys.executable, "scripts/trade/ConstantStable/place.py"], env=env)

    last = read_last_csv_row(env["CS_LOG_PATH"])
    exec_row = {
        "event": "execution_result",
        "ticket_id": ticket_id,
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        "exit_code": rc,
    }
    if last:
        exec_row.update({
            "qty_filled": last.get("qty_filled"),
            "last_price": last.get("last_price"),
            "order_ids": last.get("order_ids"),
            "reason": last.get("reason"),
        })
    append_jsonl(ticket_log, exec_row)
    return 0


if __name__ == "__main__":
    sys.exit(main())
