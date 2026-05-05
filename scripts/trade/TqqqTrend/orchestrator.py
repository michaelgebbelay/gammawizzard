#!/usr/bin/env python3
"""
TQQQ Trend daily rebalance orchestrator.

Designed to run once per weekday morning (~9:31 AM ET, just after the open)
on GitHub Actions. Computes the C1-HYST signal from the most recent CLOSED
QQQ session ("EOD" signal), reads current Schwab holdings, and flips the
position if the target sleeve != current sleeve. Uses a 3-step limit-chase
ladder for execution; falls back to MARKET if the ladder doesn't fill.

Idempotency: if current_state == target_state, no orders are placed. Safe to
run multiple times per day (DST transition weeks fire twice; second run no-ops).

Spec: scripts/conviction/backtest/C1_HYST_LOCKED_SPEC.md
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
SCRIPTS_ROOT = HERE.parent.parent
if str(SCRIPTS_ROOT) not in sys.path:
    sys.path.append(str(SCRIPTS_ROOT))

# pull the helpers we already wrote in place.py
sys.path.insert(0, str(HERE))
from place import (   # noqa: E402
    schwab, resolve_acct_hash, get_positions, get_quote,
    compute_signal_today, target_state, infer_current_state,
    build_market_order,
)

LADDER_STEP_SECS = int(os.environ.get("TQQQ_LADDER_STEP_SECS", "30"))
LADDER_POLL_SECS = float(os.environ.get("TQQQ_LADDER_POLL_SECS", "2.0"))
LADDER_MAX_STEPS = int(os.environ.get("TQQQ_LADDER_MAX_STEPS", "3"))
NY_TZ = "America/New_York"


# ---------------------------------------------------------------------------
# Schwab order helpers (LIMIT + cancel + status)
# ---------------------------------------------------------------------------

def build_limit_order(symbol: str, instruction: str, quantity: int, limit: float) -> dict:
    return {
        "orderType": "LIMIT",
        "session": "NORMAL",
        "duration": "DAY",
        "orderStrategyType": "SINGLE",
        "price": f"{limit:.2f}",
        "orderLegCollection": [{
            "instruction": instruction,
            "quantity": int(quantity),
            "instrument": {"symbol": symbol, "assetType": "EQUITY"},
        }],
    }


def submit_order(c, acct_hash: str, payload: dict) -> dict:
    url = f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders"
    r = c.session.post(url, json=payload, timeout=20)
    if r.status_code not in (200, 201):
        return {"ok": False, "status": r.status_code, "body": r.text[:500]}
    loc = r.headers.get("Location") or ""
    order_id = loc.rsplit("/", 1)[-1] if loc else ""
    return {"ok": True, "status": r.status_code, "order_id": order_id}


def get_order(c, acct_hash: str, order_id: str) -> dict:
    url = f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{order_id}"
    r = c.session.get(url, timeout=15)
    if r.status_code != 200:
        return {"status": "UNKNOWN", "filled": 0}
    j = r.json() or {}
    leg = (j.get("orderLegCollection") or [{}])[0]
    qty = float(leg.get("quantity") or 0)
    filled = float(j.get("filledQuantity") or 0)
    status = (j.get("status") or "").upper()
    avg_px = None
    activities = j.get("orderActivityCollection") or []
    if activities:
        legs = (activities[0] or {}).get("executionLegs") or []
        if legs:
            avg_px = float((legs[0] or {}).get("price") or 0) or None
    return {"status": status, "filled": filled, "qty": qty, "avg_px": avg_px}


def cancel_order(c, acct_hash: str, order_id: str) -> bool:
    url = f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{order_id}"
    r = c.session.delete(url, timeout=15)
    return r.status_code in (200, 204)


# ---------------------------------------------------------------------------
# Limit-chase ladder
# ---------------------------------------------------------------------------

def chase_limit(c, acct_hash: str, symbol: str, side: str, qty: int, log: list) -> dict:
    """
    side: 'BUY' or 'SELL'.
    3-step ladder, then market fallback.
    Step 0: limit at NBBO mid.
    Step 1: cross to ASK (buy) or BID (sell).
    Step 2: ASK + half-spread (buy) or BID - half-spread (sell).
    Step 3 (final): MARKET order.
    """
    for step in range(LADDER_MAX_STEPS):
        q = get_quote(c, symbol)
        bid, ask = q["bid"], q["ask"]
        if bid <= 0 or ask <= 0 or ask < bid:
            log.append(f"step {step}: bad NBBO bid={bid} ask={ask}; skipping to market")
            break
        mid = (bid + ask) / 2.0
        spread = ask - bid
        if step == 0:
            limit = mid
        elif step == 1:
            limit = ask if side == "BUY" else bid
        else:
            limit = (ask + spread / 2.0) if side == "BUY" else (bid - spread / 2.0)
        limit = round(limit, 2)
        log.append(f"step {step}: {side} {symbol} {qty} @ ${limit:.2f}  "
                   f"(bid ${bid:.2f}, ask ${ask:.2f}, spread {spread*100:.1f}c)")

        payload = build_limit_order(symbol, side, qty, limit)
        res = submit_order(c, acct_hash, payload)
        if not res["ok"]:
            log.append(f"  submit failed: {res}")
            continue
        order_id = res["order_id"]
        log.append(f"  order_id={order_id}")

        deadline = time.time() + LADDER_STEP_SECS
        last_status = None
        while time.time() < deadline:
            time.sleep(LADDER_POLL_SECS)
            st = get_order(c, acct_hash, order_id)
            last_status = st
            if st["status"] in ("FILLED",) and st["filled"] >= qty:
                log.append(f"  FILLED {st['filled']} @ avg ${st['avg_px']:.2f}")
                return {"ok": True, "filled": st["filled"], "avg_px": st["avg_px"],
                        "step": step, "method": "limit"}
            if st["status"] in ("CANCELED", "REJECTED", "EXPIRED"):
                log.append(f"  status={st['status']}; advancing ladder")
                break
        else:
            # deadline hit, cancel
            cancelled = cancel_order(c, acct_hash, order_id)
            log.append(f"  step {step} expired (cancel ok={cancelled})")
            time.sleep(1.0)

    # Final fallback: market
    log.append(f"market fallback: {side} {symbol} {qty}")
    payload = build_market_order(symbol, side, qty)
    res = submit_order(c, acct_hash, payload)
    if not res["ok"]:
        log.append(f"  market submit failed: {res}")
        return {"ok": False, "filled": 0, "method": "market_failed"}
    order_id = res["order_id"]
    deadline = time.time() + 30
    while time.time() < deadline:
        time.sleep(LADDER_POLL_SECS)
        st = get_order(c, acct_hash, order_id)
        if st["status"] == "FILLED":
            log.append(f"  market FILLED {st['filled']} @ avg ${st['avg_px']:.2f}")
            return {"ok": True, "filled": st["filled"], "avg_px": st["avg_px"],
                    "step": -1, "method": "market"}
        if st["status"] in ("CANCELED", "REJECTED"):
            log.append(f"  market status={st['status']}; abandoning")
            return {"ok": False, "filled": 0, "method": "market_failed"}
    log.append(f"  market did not fill within 30s; check Schwab manually")
    return {"ok": False, "filled": 0, "method": "market_timeout"}


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------

def time_window_ok() -> tuple[bool, str]:
    try:
        from zoneinfo import ZoneInfo
        now_et = datetime.now(ZoneInfo(NY_TZ))
    except Exception:
        return True, "no zoneinfo; skipping window check"
    weekday = now_et.weekday()  # 0=Mon, 6=Sun
    if weekday >= 5:
        return False, f"weekend ({now_et:%A})"
    hh, mm = now_et.hour, now_et.minute
    minutes = hh * 60 + mm
    # Acceptable window: 9:30 AM .. 10:30 AM ET
    if 9*60 + 30 <= minutes <= 10*60 + 30:
        return True, f"{now_et:%Y-%m-%d %H:%M %Z}"
    return False, f"outside 9:30-10:30 ET ({now_et:%H:%M %Z})"


def run(args) -> int:
    log = []
    log.append(f"TqqqTrend rebalance @ {datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S UTC}")
    if args.skip_window_check:
        log.append("(--skip-window-check) bypassing time-of-day guard")
    else:
        ok, msg = time_window_ok()
        log.append(f"time window: {msg}  ok={ok}")
        if not ok:
            log.append("not in execution window; exiting cleanly.")
            print("\n".join(log))
            return 0

    sig = compute_signal_today()
    log.append("")
    log.append(f"signal as_of_close={sig['as_of_close']}  "
               f"qqq=${sig['qqq_close']:.2f}  score={sig['score']}  "
               f"A={sig['A_close_gt_sma150']} B={sig['B_sma50_gt_sma200']} "
               f"C={sig['C_ret63_positive']}")

    c = schwab()
    acct_hash = resolve_acct_hash(c)
    pos = get_positions(c, acct_hash)
    cur = infer_current_state(pos)
    tgt = target_state(cur, sig["score"])
    log.append(f"account ...{acct_hash[-6:]}  positions: TQQQ={pos.get('TQQQ',0):.0f}  "
               f"BIL={pos.get('BIL',0):.0f}  current_state={cur}")
    log.append(f"target_state={tgt}")

    if cur == tgt:
        log.append("no flip needed — clean exit.")
        emit_log_and_notify(log, subject_tag="no-op")
        return 0

    if cur == "MIXED":
        log.append("MIXED holdings detected — manual intervention required, NOT trading.")
        emit_log_and_notify(log, subject_tag="MIXED ERROR")
        return 2

    # Build the flip plan
    sell_sym = cur if cur in ("TQQQ", "BIL") else None
    buy_sym = tgt
    sell_qty = int(pos.get(sell_sym, 0)) if sell_sym else 0

    if args.dry_run:
        log.append(f"DRY-RUN: would SELL {sell_sym} {sell_qty}, then BUY {buy_sym}")
        emit_log_and_notify(log, subject_tag="DRY-RUN flip")
        return 0

    # 1) sell the existing sleeve, if any
    if sell_sym and sell_qty > 0:
        log.append(f"\n--- chase SELL {sell_sym} {sell_qty} ---")
        sell_res = chase_limit(c, acct_hash, sell_sym, "SELL", sell_qty, log)
        if not sell_res["ok"]:
            log.append("SELL leg failed; aborting before BUY to avoid leverage mismatch.")
            emit_log_and_notify(log, subject_tag="SELL FAILED")
            return 3
        # sleep briefly so cash settles internally
        time.sleep(2)

    # 2) determine buy quantity from cash + sell proceeds
    sell_proceeds = (sell_res["filled"] * sell_res["avg_px"]) if sell_sym and sell_qty > 0 else 0.0
    buy_quote = get_quote(c, buy_sym)
    buy_ref = buy_quote["last"] or buy_quote["close_prev"] or buy_quote["ask"]
    if buy_ref <= 0:
        log.append(f"could not get reference price for {buy_sym}")
        emit_log_and_notify(log, subject_tag="QUOTE FAILED")
        return 4

    # leave $5 buffer to avoid insufficient-funds rejection from price drift
    budget = sell_proceeds - 5.0
    buy_qty = int(budget // buy_ref)
    if buy_qty <= 0:
        log.append(f"buy budget ${budget:.2f} too small at ${buy_ref:.2f}")
        emit_log_and_notify(log, subject_tag="BUY SIZE 0")
        return 5

    log.append(f"\n--- chase BUY {buy_sym} {buy_qty} (~${buy_qty*buy_ref:,.2f}) ---")
    buy_res = chase_limit(c, acct_hash, buy_sym, "BUY", buy_qty, log)
    if not buy_res["ok"]:
        emit_log_and_notify(log, subject_tag="BUY FAILED")
        return 6

    log.append("")
    log.append(f"FLIP COMPLETE: {sell_sym}->{buy_sym}  "
               f"sold {sell_qty} @ ~${sell_res['avg_px']:.2f}, "
               f"bought {buy_qty} @ ~${buy_res['avg_px']:.2f}")
    emit_log_and_notify(log, subject_tag=f"flip {sell_sym}->{buy_sym}")
    return 0


def emit_log_and_notify(log: list, subject_tag: str) -> None:
    body = "\n".join(log)
    print(body)
    if os.environ.get("SMTP_USER") and os.environ.get("SMTP_TO"):
        os.environ.setdefault("SMTP_SUBJECT", f"TqqqTrend: {subject_tag}")
        os.environ["SMTP_BODY"] = body
        try:
            import importlib
            mod = importlib.import_module("notify.smtp_notify")
            if hasattr(mod, "main"):
                mod.main()
        except Exception as e:
            print(f"(smtp notify failed: {e})")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true",
                   help="compute intent, do not place orders")
    p.add_argument("--skip-window-check", action="store_true",
                   help="bypass the 9:30-10:30 ET guard (testing only)")
    args = p.parse_args()
    try:
        return run(args)
    except SystemExit:
        raise
    except Exception:
        tb = traceback.format_exc()
        print(tb)
        if os.environ.get("SMTP_TO"):
            emit_log_and_notify([f"TqqqTrend orchestrator EXCEPTION", tb],
                                subject_tag="EXCEPTION")
        return 99


if __name__ == "__main__":
    raise SystemExit(main())
