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

# ── Event reporting (best-effort, never blocks trading) ──
_ew = None
try:
    # Add repo root to path so reporting package is importable
    _repo_root = str(Path(__file__).resolve().parent.parent.parent.parent)
    if _repo_root not in sys.path:
        sys.path.insert(0, _repo_root)
    from reporting.events import EventWriter
    _EVENTS_AVAILABLE = True
except ImportError:
    _EVENTS_AVAILABLE = False


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


def _init_events(today: date):
    """Initialize EventWriter for this run. Returns None on failure."""
    global _ew
    if not _EVENTS_AVAILABLE:
        return None
    try:
        _ew = EventWriter(strategy="butterfly", account="schwab", trade_date=today)
        return _ew
    except Exception as e:
        print(f"BF_DAILY WARN: EventWriter init failed: {e}")
        return None


def _emit(method: str, **kwargs):
    """Best-effort event emission. Never raises."""
    if _ew is None:
        return
    try:
        getattr(_ew, method)(**kwargs)
    except Exception as e:
        print(f"BF_DAILY WARN: event emit failed ({method}): {e}")


def main() -> int:
    strategy = _load_strategy()
    now_et = _now_et()
    today = _today_et()
    dry_run = _truthy(os.environ.get("BF_DRY_RUN", "1"))

    ew = _init_events(today)

    # Weekend guard
    if today.weekday() >= 5:
        _emit("strategy_run", signal="SKIP", config="", reason="WEEKEND")
        _emit("skip", reason="WEEKEND", signal="SKIP")
        if ew:
            try: ew.close()
            except Exception: pass
        print(f"BF_DAILY WAIT: today={today} is weekend")
        return 0

    # Time guard
    if now_et.time() < CHECK_TIME_ET:
        _emit("strategy_run", signal="SKIP", config="", reason="BEFORE_CHECK_TIME")
        _emit("skip", reason="BEFORE_CHECK_TIME", signal="SKIP")
        if ew:
            try: ew.close()
            except Exception: pass
        print(
            f"BF_DAILY WAIT: now_et={now_et.strftime('%Y-%m-%d %H:%M:%S %Z')} "
            f"check_time=16:01 ET"
        )
        return 0

    state = load_state()

    # Skip if already evaluated today
    if state.get("last_evaluated_date") == today.isoformat():
        _emit("strategy_run", signal="SKIP", config="", reason="ALREADY_EVALUATED")
        _emit("skip", reason="ALREADY_EVALUATED", signal="SKIP")
        if ew:
            try: ew.close()
            except Exception: pass
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
        _emit("strategy_run", signal="SKIP", config=plan.get("config", ""),
              reason=plan["reason"], spot=float(plan.get("spot") or 0),
              vix=float(plan.get("vix") or 0), vix1d=float(plan.get("vix1d") or 0),
              filters={"se_today": plan.get("se_today"), "se_5d_avg": plan.get("se_5d_avg")})
        _emit("skip", reason=plan["reason"], signal="SKIP")
        if ew:
            try:
                ew.close()
            except Exception:
                pass
        print(
            f"BF_DAILY SKIP: reason={plan['reason']} vix1d={plan.get('vix1d')} "
            f"se_today={plan.get('se_today')} se_5d={plan.get('se_5d_avg')}"
        )
        return 0

    if plan["status"] != "OK":
        write_plan(plan)
        _emit("strategy_run", signal=plan.get("signal", "ERROR"),
              config=plan.get("config", ""), reason=plan["reason"])
        _emit("error", message=plan["reason"], stage="strategy_evaluation")
        if ew:
            try:
                ew.close()
            except Exception:
                pass
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
        _emit("strategy_run", signal=plan["signal"], config=plan.get("config", ""),
              reason=f"DUP_EXPIRY:{plan['expiry_date']}",
              spot=float(plan.get("spot") or 0), vix=float(plan.get("vix") or 0),
              vix1d=float(plan.get("vix1d") or 0))
        _emit("skip", reason=f"DUP_EXPIRY:{plan['expiry_date']}", signal=plan["signal"])
        if ew:
            try:
                ew.close()
            except Exception:
                pass
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

    # Emit strategy_run + trade_intent before placement
    bf_opt_type = "PUT" if plan.get("flipped_to_puts") else "CALL"
    _emit("strategy_run", signal=plan["signal"], config=plan.get("config", ""),
          reason="OK", spot=float(plan.get("spot") or 0),
          vix=float(plan.get("vix") or 0), vix1d=float(plan.get("vix1d") or 0),
          filters={"se_today": plan.get("se_today"), "se_5d_avg": plan.get("se_5d_avg")},
          extra={"expiry_date": plan["expiry_date"], "target_dte": plan.get("target_dte"),
                 "flipped_to_puts": plan.get("flipped_to_puts", False)})
    _emit("trade_intent", side=plan["order_side"], direction="LONG" if plan["order_side"] == "DEBIT" else "SHORT",
          legs=[
              {"osi": plan["lower_osi"], "strike": plan["lower_strike"], "option_type": bf_opt_type,
               "action": "BUY_TO_OPEN" if plan["order_side"] == "DEBIT" else "SELL_TO_OPEN", "qty": 1},
              {"osi": plan["center_osi"], "strike": plan["center_strike"], "option_type": bf_opt_type,
               "action": "SELL_TO_OPEN" if plan["order_side"] == "DEBIT" else "BUY_TO_OPEN", "qty": 2},
              {"osi": plan["upper_osi"], "strike": plan["upper_strike"], "option_type": bf_opt_type,
               "action": "BUY_TO_OPEN" if plan["order_side"] == "DEBIT" else "SELL_TO_OPEN", "qty": 1},
          ],
          target_qty=1, limit_price=float(plan.get("package_mid") or 0),
          extra={"package_bid": plan.get("package_bid"), "package_ask": plan.get("package_ask")})

    flip_tag = " [FLIPPED→PUT]" if plan.get("flipped_to_puts") else ""
    print(
        "BF_DAILY PLAN: "
        f"{plan['config']} {plan['order_side']} dte={plan.get('target_dte')} "
        f"{plan['lower_strike']}/{plan['center_strike']}/{plan['upper_strike']}{flip_tag} "
        f"spot={plan['spot']} vix={plan['vix']} vix1d={plan['vix1d']} "
        f"pkg={plan['package_bid']}/{plan['package_ask']} mid={plan['package_mid']} "
        f"se_today={plan.get('se_today')} se_5d={plan.get('se_5d_avg')}"
    )

    # Write plan BEFORE placement so post-steps always have data
    plan["result"] = {"pending": True}
    write_plan(plan)

    proc = subprocess.run([sys.executable, str(PLACE_PATH)], env=env, cwd=str(ROOT))
    if proc.returncode != 0:
        plan["status"] = "ERROR"
        plan["reason"] = f"place.py rc={proc.returncode}"
        plan["result"] = {"error": True, "rc": proc.returncode}
        write_plan(plan)
        _emit("error", message=f"place.py rc={proc.returncode}", stage="placement")
        if ew:
            try:
                ew.close()
            except Exception:
                pass
        print(f"BF_DAILY ERROR: place.py returned {proc.returncode}")
        return proc.returncode

    result = {}
    if RESULT_PATH.exists():
        with RESULT_PATH.open() as handle:
            result = json.load(handle)

    plan["result"] = result
    write_plan(plan)

    # Emit order + fill events from placement result
    for oid in (result.get("order_ids") or []):
        _emit("order_submitted", order_id=str(oid), legs=[], limit_price=float(result.get("last_price") or 0))
    filled = result.get("filled_qty", 0)
    if filled and filled > 0:
        _emit("fill", order_id=str((result.get("order_ids") or [""])[0]),
              fill_qty=filled, fill_price=float(result.get("last_price") or 0),
              legs=[
                  {"osi": plan["lower_osi"], "strike": plan["lower_strike"], "option_type": bf_opt_type, "qty": 1},
                  {"osi": plan["center_osi"], "strike": plan["center_strike"], "option_type": bf_opt_type, "qty": 2},
                  {"osi": plan["upper_osi"], "strike": plan["upper_strike"], "option_type": bf_opt_type, "qty": 1},
              ])

    if dry_run:
        if ew:
            try:
                ew.close()
            except Exception:
                pass
        print(
            f"BF_DAILY DRY_RUN: would hold until {plan['expiry_date']}"
        )
        return 0

    state["last_signal"] = plan["signal"]
    state["last_trade_date"] = plan["trade_date"]
    state["last_expiration_date"] = plan["expiry_date"]
    state["last_action"] = result.get("reason", "PLACED")
    if filled and filled > 0 and plan["expiry_date"] not in open_expiries:
        open_expiries.append(plan["expiry_date"])
    state["open_expiries"] = open_expiries
    save_state(state)
    if ew:
        try:
            ew.close()
        except Exception:
            pass
    print(
        f"BF_DAILY DONE: reason={state['last_action']} "
        f"expiry={plan['expiry_date']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
