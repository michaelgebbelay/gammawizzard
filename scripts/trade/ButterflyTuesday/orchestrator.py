#!/usr/bin/env python3
"""Daily Schwab orchestrator for the hybrid butterfly strategy with BUY vol filter.

Behavior:
  - Runs every weekday at 4:01 PM ET
  - Calls strategy.build_trade_plan() which handles:
    VIX1D signal (BUY/SELL/SKIP), vol filter, strike selection
  - Skips if the planned expiry already has an open position (no dup expiry)
  - Tracks state in S3 (Lambda) or local file (fallback)

Default safety:
  - BF_DRY_RUN=1 by default
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from datetime import date, datetime, time as dt_time
from pathlib import Path
from zoneinfo import ZoneInfo

import boto3


ROOT = Path(__file__).resolve().parent
PLACE_PATH = ROOT / "place.py"
RESULT_PATH = Path(os.environ.get("BF_RESULT_PATH", "/tmp/bf_last_result.json"))
PLAN_PATH = Path(os.environ.get("BF_PLAN_PATH", "/tmp/bf_plan.json"))
ET = ZoneInfo("America/New_York")
CHECK_TIME_ET = dt_time(16, 1)

S3_BUCKET = (
    os.environ.get("BF_STATE_S3_BUCKET")
    or os.environ.get("SIM_CACHE_BUCKET", "")
).strip()
S3_STATE_KEY = os.environ.get("BF_STATE_S3_KEY", "cadence/bf_daily_state.json")
LOCAL_STATE_PATH = Path(os.environ.get("BF_STATE_PATH", ROOT / "state.json"))


def _truthy(value: str) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _now_et() -> datetime:
    override = os.environ.get("BF_NOW_OVERRIDE", "").strip()
    if override:
        return datetime.fromisoformat(override).astimezone(ET)
    return datetime.now(ET)


def _today_et() -> date:
    override = os.environ.get("BF_TRADE_DATE_OVERRIDE", "").strip()
    if override:
        return date.fromisoformat(override)
    return _now_et().date()


def _load_strategy():
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    import strategy

    return strategy


# ---------- S3-backed state ----------

def load_state() -> dict:
    if S3_BUCKET:
        try:
            s3 = boto3.client("s3")
            obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_STATE_KEY)
            return json.loads(obj["Body"].read().decode("utf-8"))
        except Exception:
            pass
    if LOCAL_STATE_PATH.exists():
        with LOCAL_STATE_PATH.open() as handle:
            return json.load(handle)
    return {}


def save_state(state: dict) -> None:
    body = json.dumps(state, indent=2, sort_keys=True)
    if S3_BUCKET:
        try:
            s3 = boto3.client("s3")
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=S3_STATE_KEY,
                Body=body.encode("utf-8"),
                ContentType="application/json",
            )
            return
        except Exception as e:
            print(f"BF_DAILY WARN: S3 state save failed: {e}")
    LOCAL_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    LOCAL_STATE_PATH.write_text(body)


def write_plan(plan: dict) -> None:
    """Write plan summary for post-step scripts to read."""
    try:
        PLAN_PATH.parent.mkdir(parents=True, exist_ok=True)
        PLAN_PATH.write_text(json.dumps(plan, indent=2))
    except Exception as e:
        print(f"BF_DAILY WARN: write plan failed: {e}")


def main() -> int:
    strategy = _load_strategy()
    now_et = _now_et()
    today = _today_et()
    dry_run = _truthy(os.environ.get("BF_DRY_RUN", "1"))

    # Weekend guard
    if today.weekday() >= 5:
        print(f"BF_DAILY WAIT: today={today} is weekend")
        return 0

    # Time guard
    if now_et.time() < CHECK_TIME_ET:
        print(
            f"BF_DAILY WAIT: now_et={now_et.strftime('%Y-%m-%d %H:%M:%S %Z')} "
            f"check_time=16:01 ET"
        )
        return 0

    state = load_state()

    # Skip if already evaluated today
    if state.get("last_evaluated_date") == today.isoformat():
        print(f"BF_DAILY SKIP: already evaluated today={today}")
        return 0

    # Clean up expired positions from open_expiries
    open_expiries = [e for e in state.get("open_expiries", []) if e >= today.isoformat()]
    state["open_expiries"] = open_expiries

    plan = strategy.build_trade_plan(today)
    state["last_evaluated_date"] = today.isoformat()

    if plan["status"] == "SKIP":
        state["last_signal"] = "SKIP"
        state["last_action"] = plan["reason"]
        if not dry_run:
            save_state(state)
        write_plan(plan)
        print(
            f"BF_DAILY SKIP: reason={plan['reason']} vix1d={plan.get('vix1d')} "
            f"se_today={plan.get('se_today')} se_5d={plan.get('se_5d_avg')}"
        )
        return 0

    if plan["status"] != "OK":
        write_plan(plan)
        print(f"BF_DAILY ERROR: {plan['reason']}")
        return 1

    # Skip if we already have a position expiring on this date
    if plan["expiry_date"] in open_expiries:
        state["last_action"] = f"DUP_EXPIRY:{plan['expiry_date']}"
        plan["status"] = "SKIP"
        plan["reason"] = f"DUP_EXPIRY:{plan['expiry_date']}"
        if not dry_run:
            save_state(state)
        write_plan(plan)
        print(
            f"BF_DAILY SKIP: duplicate expiry {plan['expiry_date']} "
            f"signal={plan['signal']} dte={plan.get('target_dte')}"
        )
        return 0

    env = os.environ.copy()
    env.update(
        {
            "BF_RESULT_PATH": str(RESULT_PATH),
            "BF_TRADE_DATE": plan["trade_date"],
            "BF_EXPIRY_DATE": plan["expiry_date"],
            "BF_SIGNAL": plan["signal"],
            "BF_CONFIG": plan["config"],
            "BF_ORDER_SIDE": plan["order_side"],
            "BF_SPOT": str(plan["spot"]),
            "BF_VIX": str(plan["vix"]),
            "BF_VIX1D": str(plan["vix1d"]),
            "BF_LOWER_STRIKE": str(plan["lower_strike"]),
            "BF_CENTER_STRIKE": str(plan["center_strike"]),
            "BF_UPPER_STRIKE": str(plan["upper_strike"]),
            "BF_WIDTH": str(plan["width"]),
            "BF_TARGET_DTE": str(plan.get("target_dte", "")),
            "BF_LOWER_OSI": plan["lower_osi"],
            "BF_CENTER_OSI": plan["center_osi"],
            "BF_UPPER_OSI": plan["upper_osi"],
        }
    )

    print(
        "BF_DAILY PLAN: "
        f"{plan['config']} {plan['order_side']} dte={plan.get('target_dte')} "
        f"{plan['lower_strike']}/{plan['center_strike']}/{plan['upper_strike']} "
        f"spot={plan['spot']} vix={plan['vix']} vix1d={plan['vix1d']} "
        f"pkg={plan['package_bid']}/{plan['package_ask']} mid={plan['package_mid']} "
        f"se_today={plan.get('se_today')} se_5d={plan.get('se_5d_avg')}"
    )

    proc = subprocess.run([sys.executable, str(PLACE_PATH)], env=env, cwd=str(ROOT))
    if proc.returncode != 0:
        print(f"BF_DAILY ERROR: place.py returned {proc.returncode}")
        return proc.returncode

    result = {}
    if RESULT_PATH.exists():
        with RESULT_PATH.open() as handle:
            result = json.load(handle)

    plan["result"] = result
    write_plan(plan)

    if dry_run:
        print(
            f"BF_DAILY DRY_RUN: would hold until {plan['expiry_date']}"
        )
        return 0

    state["last_signal"] = plan["signal"]
    state["last_trade_date"] = plan["trade_date"]
    state["last_expiration_date"] = plan["expiry_date"]
    state["last_action"] = result.get("reason", "PLACED")
    if plan["expiry_date"] not in open_expiries:
        open_expiries.append(plan["expiry_date"])
    state["open_expiries"] = open_expiries
    save_state(state)
    print(
        f"BF_DAILY DONE: reason={state['last_action']} "
        f"expiry={plan['expiry_date']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
