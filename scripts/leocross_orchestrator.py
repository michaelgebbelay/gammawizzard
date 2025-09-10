#!/usr/bin/env python3
# LeoCross ORCHESTRATOR (single-shot): enforce time window; route to PLACER with correct env.

import os, sys, json, signal, subprocess
from datetime import datetime
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")

MODE     = (os.environ.get("PLACER_MODE","SCHEDULED") or "SCHEDULED").strip().upper()
ACTION   = os.environ.get("ACTION","").strip().upper()
REASON   = os.environ.get("REASON","")
REM_QTY  = os.environ.get("REM_QTY","")
LEGS_JSON= os.environ.get("LEGS_JSON","[]")
OPEN_IDS = os.environ.get("OPEN_ORDER_IDS","")
IS_CREDIT= os.environ.get("IS_CREDIT","").lower()
CANON_KEY= os.environ.get("CANON_KEY","")
SIG_DATE = os.environ.get("SIGNAL_DATE","")
TIMEOUT  = int(os.environ.get("PLACER_TIMEOUT_SEC","180"))
QTY_TARGET = os.environ.get("QTY_TARGET","4")

def _term(signum, frame):
    print(f"ORCH TERM: received signal {signum}, exiting.")
    sys.exit(130)

signal.signal(signal.SIGTERM, _term)

def _in_sched_window_et() -> bool:
    now = datetime.now(ET)
    return (now.hour == 16) and (8 <= now.minute <= 14)

def main():
    print(f"ORCH START MODE={MODE} ACTION={ACTION} CANON={CANON_KEY} SIG_DATE={SIG_DATE}")

    # Time gate for scheduled runs
    if MODE == "SCHEDULED" and not _in_sched_window_et():
        print("ORCH SKIP: OUTSIDE_SCHEDULE_WINDOW_ET (16:08–16:14)")
        return 0

    if ACTION == "SKIP":
        print(f"ORCH SKIP: {REASON or 'GUARD'}")
        return 0
    if ACTION not in ("NEW","REPRICE_EXISTING"):
        print(f"ORCH ABORT: unknown ACTION '{ACTION}'")
        return 1

    # Prepare env for placer
    env = dict(os.environ)
    env["LEGS_JSON"] = LEGS_JSON
    env["IS_CREDIT"] = IS_CREDIT
    env["OPEN_ORDER_IDS"] = OPEN_IDS
    env["QTY_TARGET"] = QTY_TARGET
    env["CANON_KEY"] = CANON_KEY
    env["PLACER_MODE"] = MODE

    if ACTION == "NEW":
        env["REPRICE_ONLY"] = "0"
        env["QTY_OVERRIDE"] = REM_QTY or ""
    else:
        env["REPRICE_ONLY"] = "1"
        env["QTY_OVERRIDE"] = ""  # placer will compute remainder from positions

    # Call placer with a hard timeout
    try:
        proc = subprocess.run([sys.executable, "scripts/leocross_placer.py"],
                              env=env, check=False, timeout=TIMEOUT)
        print(f"ORCH DONE: placer_rc={proc.returncode}")
        return proc.returncode
    except subprocess.TimeoutExpired:
        print("ORCH ABORT: PLACER_TIMEOUT")
        return 1

if __name__ == "__main__":
    sys.exit(main())
