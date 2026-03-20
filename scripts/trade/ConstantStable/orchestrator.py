#!/usr/bin/env python3
# CONSTANT STABLE — vertical orchestrator (with NO-CLOSE guard, TOPUP, and optional 4-leg bundling)
#
# What it does:
# - Fetches ConstantStable payload from GammaWizard (GetUltraPureConstantStable).
# - Builds 5-wide SPX verticals around Limit / CLimit.
# - PUT leg:
#     LeftGo < 0  -> short put vertical (credit)
#     LeftGo > 0  -> long  put vertical (debit)
# - CALL leg:
#     RightGo < 0 -> short call vertical (credit)
#     RightGo > 0 -> long  call vertical (debit)
#
# Sizing:
# - units = floor(account_value / CS_UNIT_DOLLARS)   (min 1)
# - Vol bucket multiplier from CS_VIX_MULTS (len = len(CS_VIX_BREAKS)+1).
# - target_qty = max(1, units * vix_mult)  (if vix_mult == 0 -> skip)
#
# TOPUP (default ON):
# - If you already have open spreads for today's strikes, only top up to target_qty.
#   (Avoids "adding more just because".)
#
# Guard (NO-CLOSE, default ON):
# - If an opening vertical would net/close an existing option position leg, SKIP that vertical.
#   (Adding to existing same-direction is OK.)
#
# 4-leg bundling (default ON):
# - If both PUT and CALL legs are eligible AND have the same send_qty, place as ONE 4-leg order.
# - If asymmetric, fallback to placing each vertical separately.
#
# Delegates placement + logging to scripts/trade/ConstantStable/place.py via VERT_* envs.

import os
import sys
import re
import json
import time
import random
import subprocess
from datetime import date, datetime, timezone
from typing import Any, Dict, Tuple
from zoneinfo import ZoneInfo

import requests


def _add_scripts_root():
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
from pathlib import Path

__version__ = "2.4.0"

# ── Event reporting (best-effort, never blocks trading) ──
_ew = None
try:
    _repo_root = str(Path(__file__).resolve().parent.parent.parent.parent)
    if _repo_root not in sys.path:
        sys.path.insert(0, _repo_root)
    from reporting.events import EventWriter
    _EVENTS_AVAILABLE = True
except ImportError:
    _EVENTS_AVAILABLE = False


def _init_events(today: date):
    global _ew
    if not _EVENTS_AVAILABLE:
        return None
    try:
        _ew = EventWriter(strategy="constantstable", account="schwab", trade_date=today)
        return _ew
    except Exception as e:
        print(f"CS_VERT_RUN WARN: EventWriter init failed: {e}")
        return None


def _emit(method: str, **kwargs):
    """Best-effort event emission. Never raises."""
    if _ew is None:
        return
    try:
        getattr(_ew, method)(**kwargs)
    except Exception as e:
        print(f"CS_VERT_RUN WARN: event emit failed ({method}): {e}")


def _close_events():
    """Close EventWriter. Best-effort."""
    if _ew is not None:
        try:
            _ew.close()
        except Exception:
            pass


def _csv_row_count() -> int:
    """Count current rows in CS_LOG_PATH. Returns 0 if file doesn't exist."""
    try:
        import csv as _csv
        with open(CS_LOG_PATH) as f:
            return sum(1 for _ in _csv.DictReader(f))
    except Exception:
        return 0


def _read_back_fills(group_ids: list[str], rows_before: int):
    """Read placement results from CS_LOG_PATH and emit order_submitted + fill events.

    Uses rows_before to identify exactly which CSV rows were written by this
    invocation (handles bundle fallback writing 4 rows instead of 2).
    Each new row is paired with its corresponding trade_group_id from group_ids
    by matching on the vertical name.

    Best-effort — never blocks trading.
    """
    if _ew is None:
        return
    try:
        import csv as _csv
        with open(CS_LOG_PATH) as f:
            all_rows = list(_csv.DictReader(f))
        new_rows = all_rows[rows_before:]
        if not new_rows:
            return

        # Build name → group_id mapping from the saved pairs
        # group_ids is a list of (name, group_id) tuples
        name_to_group = {name: gid for name, gid in group_ids}

        # Emit one order_submitted + fill per CSV row (per real broker order).
        # Bundle+fallback writes multiple rows for the same vertical; each row
        # is a distinct broker execution and must be preserved as-is for
        # order-level reconciliation.
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
                _emit("order_submitted", order_id=oid,
                      legs=[
                          {"osi": short_osi, "option_type": kind, "action": "SELL_TO_OPEN", "qty": requested},
                          {"osi": long_osi, "option_type": kind, "action": "BUY_TO_OPEN", "qty": requested},
                      ],
                      limit_price=price)
            if filled > 0:
                _emit("fill", order_id=oids[0] if oids else "",
                      fill_qty=filled, fill_price=price,
                      legs=[
                          {"osi": short_osi, "option_type": kind, "qty": filled},
                          {"osi": long_osi, "option_type": kind, "qty": filled},
                      ])
    except Exception as e:
        print(f"CS_VERT_RUN WARN: could not read placement result: {e}")

GW_BASE = os.environ.get("GW_BASE", "https://gandalf.gammawizard.com").rstrip("/")
GW_ENDPOINT = os.environ.get("GW_ENDPOINT", "rapi/GetUltraPureConstantStable").lstrip("/")

CS_UNIT_DOLLARS = float(os.environ.get("CS_UNIT_DOLLARS", "10000"))
CS_LOG_PATH = os.environ.get("CS_LOG_PATH", "logs/constantstable_vertical_trades.csv")

# Vol bucket config
CS_VOL_FIELD = (os.environ.get("CS_VOL_FIELD", "VixOne") or "VixOne").strip()  # default changed to VixOne
CS_VIX_BREAKS = os.environ.get("CS_VIX_BREAKS", "0.089,0.111,0.131,0.158,0.192,0.253")
CS_VIX_MULTS = os.environ.get("CS_VIX_MULTS", "1,1,1,2,3,4,6")
CS_RR_CREDIT_RATIOS = os.environ.get("CS_RR_CREDIT_RATIOS", "")
CS_IC_SHORT_MULTS = os.environ.get("CS_IC_SHORT_MULTS", "")
CS_IC_LONG_MULTS = os.environ.get("CS_IC_LONG_MULTS", "")

# --- NO-CLOSE guard config ---
CS_GUARD_NO_CLOSE = (os.environ.get("CS_GUARD_NO_CLOSE", "1") or "1").strip().lower() in ("1", "true", "yes", "y")
CS_GUARD_FAIL_ACTION = (os.environ.get("CS_GUARD_FAIL_ACTION", "SKIP_ALL") or "SKIP_ALL").strip().upper()
#   SKIP_ALL  -> safest: if we cannot load positions, skip everything
#   CONTINUE  -> proceed without guard (not recommended)

# --- TOPUP config ---
CS_TOPUP = (os.environ.get("CS_TOPUP", "1") or "1").strip().lower() in ("1", "true", "yes", "y")
CS_TOPUP_FAIL_ACTION = (os.environ.get("CS_TOPUP_FAIL_ACTION", "SKIP_ALL") or "SKIP_ALL").strip().upper()
#   SKIP_ALL  -> safest: if we cannot load positions, skip everything
#   CONTINUE  -> proceed without topup

# --- 4-leg bundle config ---
CS_BUNDLE_4LEG = (os.environ.get("CS_BUNDLE_4LEG", "1") or "1").strip().lower() in ("1", "true", "yes", "y")
CS_BUNDLE_REQUIRE_EQUAL_QTY = (os.environ.get("CS_BUNDLE_REQUIRE_EQUAL_QTY", "1") or "1").strip().lower() in ("1", "true", "yes", "y")
CS_BUNDLE_FALLBACK = (os.environ.get("CS_BUNDLE_FALLBACK", "1") or "1").strip().lower() in ("1", "true", "yes", "y")
CS_PAIR_ALTERNATE = (os.environ.get("CS_PAIR_ALTERNATE", "1") or "1").strip().lower() in ("1", "true", "yes", "y")

# --- IC_LONG regime filter config ---
# Skip IC_LONG when trailing 5-day avg |SPX move|% < nearest anchor distance%.
# Backtested: IC_LONG in this regime is 0 EV (40% WR, -$1/trade over 169 trades).
# Decision is pre-computed by ic_long_filter.py and written to S3.
CS_IC_LONG_FILTER = (os.environ.get("CS_IC_LONG_FILTER", "0") or "0").strip().lower() in ("1", "true", "yes", "y")
CS_MOVE_STATE_S3_BUCKET = (
    os.environ.get("CS_MOVE_STATE_S3_BUCKET")
    or os.environ.get("SIM_CACHE_BUCKET", "")
).strip()


# ---------- Utility helpers ----------

def yymmdd(iso: str) -> str:
    d = date.fromisoformat((iso or "")[:10])
    return f"{d:%y%m%d}"


def to_osi(sym: str) -> str:
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


def osi_canon(osi: str) -> Tuple[str, str, str]:
    """
    Canonical key for an OSI-like string:
      (YYMMDD, 'C'/'P', strike_8digits)
    Assumes OSI formatted like: ROOT(6) + YYMMDD(6) + C/P(1) + STRIKE(8).
    """
    s = (osi or "")
    if len(s) < 21:
        return ("", "", "")
    return (s[6:12], s[12], s[-8:])


def fnum(x):
    try:
        return float(x)
    except Exception:
        return None


def parse_csv_floats(s: str):
    out = []
    for part in (s or "").split(","):
        part = part.strip()
        if not part:
            continue
        out.append(float(part))
    return out


def get_case_insensitive(d: Dict[str, Any], key: str):
    if key in d:
        return d.get(key)
    lk = key.lower()
    for k, v in d.items():
        if str(k).lower() == lk:
            return v
    return None


def pick_vol_value(tr: Dict[str, Any], vol_field: str) -> Tuple[str, float | None]:
    """
    Returns (field_used, value_or_None).
    vol_field supports:
      - "VIX" / "VixOne" / etc
      - "AUTO": try common candidates
    """
    if (vol_field or "").strip().upper() == "AUTO":
        candidates = ["VixOne", "VIXONE", "Vix1", "VIX1", "VIX", "Vix"]
    else:
        candidates = [vol_field]

    for k in candidates:
        v = get_case_insensitive(tr, k)
        fv = fnum(v)
        if fv is not None:
            return (k, fv)
    return (vol_field, None)


def vix_bucket_and_mult(vix_val: float | None, breaks_csv: str, mults_csv: str) -> Tuple[int, int]:
    """
    N buckets => (N-1) breaks and N multipliers.
    Example (7 buckets):
      breaks = 0.089,0.111,0.131,0.158,0.192,0.253
      mults  = 1,1,1,2,3,4,6
    Buckets are 1..N.
    """
    breaks = parse_csv_floats(breaks_csv)
    mults = parse_csv_floats(mults_csv)

    if len(breaks) < 1:
        raise ValueError("CS_VIX_BREAKS must contain at least 1 cutoff")
    if len(mults) != len(breaks) + 1:
        raise ValueError("CS_VIX_MULTS must have exactly len(CS_VIX_BREAKS)+1 values")

    if vix_val is None:
        mid = len(mults) // 2
        return (mid + 1, int(mults[mid]))

    for i, b in enumerate(breaks):
        if vix_val < b:
            return (i + 1, int(mults[i]))
    return (len(mults), int(mults[-1]))


# ---------- GammaWizard ----------

def _sanitize_token(t: str) -> str:
    t = (t or "").strip().strip('"').strip("'")
    return t.split(None, 1)[1] if t.lower().startswith("bearer ") else t


def gw_fetch():
    base = GW_BASE
    endpoint = GW_ENDPOINT
    tok = _sanitize_token(os.environ.get("GW_TOKEN", "") or "")

    url = f"{base}/{endpoint}"
    print("CS_VERT_RUN GW URL:", url)

    def hit(tkn):
        h = {"Accept": "application/json"}
        if tkn:
            h["Authorization"] = f"Bearer {_sanitize_token(tkn)}"
        return requests.get(url, headers=h, timeout=30)

    r = hit(tok) if tok else None
    if (r is None) or (r.status_code in (401, 403)):
        email = os.environ.get("GW_EMAIL", "")
        pwd = os.environ.get("GW_PASSWORD", "")
        if not (email and pwd):
            raise RuntimeError("GW_AUTH_REQUIRED")
        rr = requests.post(
            f"{base}/goauth/authenticateFireUser",
            data={"email": email, "password": pwd},
            timeout=30,
        )
        rr.raise_for_status()
        t = rr.json().get("token") or ""
        r = hit(t)

    r.raise_for_status()
    return r.json()


def extract_trade(j):
    if isinstance(j, dict):
        if "Trade" in j:
            tr = j["Trade"]
            return tr[-1] if isinstance(tr, list) and tr else tr if isinstance(tr, dict) else {}
        keys = ("Date", "TDate", "Limit", "CLimit", "LeftGo", "RightGo", "LImp", "RImp", "VIX", "VixOne")
        if any(k in j for k in keys):
            return j
        for v in j.values():
            if isinstance(v, (dict, list)):
                t = extract_trade(v)
                if t:
                    return t
    if isinstance(j, list):
        for it in reversed(j):
            t = extract_trade(it)
            if t:
                return t
    return {}


# ---------- Schwab helpers ----------

def opening_cash_for_account(c, prefer_number=None):
    """
    Returns (value_or_None, source_key, acct_number).
    Ignores 0/negative values; prefers liquidationValue first.
    """
    r = c.get_accounts()
    r.raise_for_status()
    data = r.json()
    arr = data if isinstance(data, list) else [data]

    if prefer_number is None:
        try:
            rr = c.get_account_numbers()
            rr.raise_for_status()
            prefer_number = str((rr.json() or [{}])[0].get("accountNumber") or "")
        except Exception:
            prefer_number = None

    def hunt(a):
        acct_id = None
        init = {}
        curr = {}
        stack = [a]
        while stack:
            x = stack.pop()
            if isinstance(x, dict):
                if acct_id is None and x.get("accountNumber"):
                    acct_id = str(x["accountNumber"])
                if "initialBalances" in x and isinstance(x["initialBalances"], dict):
                    init = x["initialBalances"]
                if "currentBalances" in x and isinstance(x["currentBalances"], dict):
                    curr = x["currentBalances"]
                for v in x.values():
                    if isinstance(v, (dict, list)):
                        stack.append(v)
            elif isinstance(x, list):
                stack.extend(x)
        return acct_id, init, curr

    chosen = None
    acct_num = ""
    for a in arr:
        aid, init, curr = hunt(a)
        if prefer_number and aid == prefer_number:
            chosen = (init, curr)
            acct_num = aid
            break
        if chosen is None:
            chosen = (init, curr)
            acct_num = aid

    if not chosen:
        return None, "none", ""

    init, curr = chosen

    keys = [
        "liquidationValue",
        "cashAvailableForTrading",
        "cashBalance",
        "availableFundsNonMarginableTrade",
        "buyingPowerNonMarginableTrade",
        "optionBuyingPower",
        "buyingPower",
    ]

    def pick(src):
        for k in keys:
            v = (src or {}).get(k)
            if isinstance(v, (int, float)):
                fv = float(v)
                if fv > 0:
                    return fv, k
        return None

    for src in (init, curr):
        got = pick(src)
        if got:
            return got[0], got[1], acct_num

    return None, "none", acct_num


def get_account_hash(c):
    r = c.get_account_numbers()
    r.raise_for_status()
    arr = r.json() or []
    if not arr:
        return ""
    info = arr[0]
    return str(info.get("hashValue") or info.get("hashvalue") or "")


# ----- Positions map (for NO-CLOSE guard + TOPUP) -----

def _sleep_for_429(resp, attempt: int) -> float:
    ra = resp.headers.get("Retry-After")
    if ra:
        try:
            return max(1.0, float(ra))
        except Exception:
            pass
    return min(8.0, 0.6 * (2 ** attempt)) + random.uniform(0.0, 0.25)


def schwab_get_json(c, url: str, params=None, tries: int = 6, tag: str = ""):
    last = ""
    for i in range(tries):
        try:
            r = c.session.get(url, params=(params or {}), timeout=20)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 429:
                time.sleep(_sleep_for_429(r, i))
                continue
            last = f"HTTP_{r.status_code}:{(r.text or '')[:200]}"
        except Exception as e:
            last = f"{type(e).__name__}:{str(e)}"
        time.sleep(min(6.0, 0.5 * (2 ** i)))
    raise RuntimeError(f"SCHWAB_GET_FAIL({tag}) {last or 'unknown'}")


def _osi_from_instrument(ins: Dict[str, Any]) -> str | None:
    sym = (ins.get("symbol") or "").strip()
    if sym:
        try:
            sym_clean = re.sub(r"\s+", "", sym)
            return to_osi(sym_clean)
        except Exception:
            pass

    exp = ins.get("optionExpirationDate") or ins.get("expirationDate") or ""
    pc = (ins.get("putCall") or ins.get("type") or "").upper()
    strike = ins.get("strikePrice") or ins.get("strike")

    try:
        if exp and strike is not None and pc:
            ymd = date.fromisoformat(str(exp)[:10]).strftime("%y%m%d")
            cp = "C" if pc.startswith("C") else "P"
            mills = int(round(float(strike) * 1000))
            return f"{'SPXW':<6}{ymd}{cp}{mills:08d}"
    except Exception:
        return None

    return None


def positions_map(c, acct_hash: str) -> Dict[Tuple[str, str, str], float]:
    """
    Returns dict: canon -> net_qty (positive=long, negative=short)
    """
    url = f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}"
    j = schwab_get_json(c, url, params={"fields": "positions"}, tag="POSITIONS")

    sa = j[0]["securitiesAccount"] if isinstance(j, list) else (j.get("securitiesAccount") or j)
    out: Dict[Tuple[str, str, str], float] = {}

    for p in (sa.get("positions") or []):
        ins = p.get("instrument", {}) or {}
        atype = (ins.get("assetType") or ins.get("type") or "").upper()
        if atype != "OPTION":
            continue

        osi = _osi_from_instrument(ins)
        if not osi:
            continue

        try:
            qty = float(p.get("longQuantity", 0)) - float(p.get("shortQuantity", 0))
        except Exception:
            continue

        if abs(qty) < 1e-9:
            continue

        key = osi_canon(osi)
        out[key] = out.get(key, 0.0) + qty

    return out


def open_spreads_for_vertical(v: Dict[str, Any], pos: Dict[Tuple[str, str, str], float]) -> int:
    """
    Computes open spread count for a vertical from option leg net positions.
    For a properly opened spread:
      BUY leg should be > 0
      SELL leg should be < 0
    Spread count ~= min(buy_qty, abs(sell_qty))
    """
    buy_key = osi_canon(v["long_osi"])   # BUY_TO_OPEN
    sell_key = osi_canon(v["short_osi"]) # SELL_TO_OPEN

    buy_pos = float(pos.get(buy_key, 0.0))
    sell_pos = float(pos.get(sell_key, 0.0))

    b = buy_pos if buy_pos > 0 else 0.0
    s = (-sell_pos) if sell_pos < 0 else 0.0
    return int(min(b, s) + 1e-9)


def would_close_guard(v: Dict[str, Any], pos: Dict[Tuple[str, str, str], float]) -> bool:
    """
    Returns True if this opening order would net/close an existing position (unsafe).
    """
    buy_key = osi_canon(v["long_osi"])   # BUY_TO_OPEN
    sell_key = osi_canon(v["short_osi"]) # SELL_TO_OPEN

    buy_leg_pos = float(pos.get(buy_key, 0.0))
    sell_leg_pos = float(pos.get(sell_key, 0.0))

    print(
        f"CS_VERT_RUN GUARD_CHECK {v['name']}: "
        f"BUY_TO_OPEN {v['long_osi']} pos={buy_leg_pos:+g} ; "
        f"SELL_TO_OPEN {v['short_osi']} pos={sell_leg_pos:+g}"
    )

    if buy_leg_pos < -1e-9:
        return True
    if sell_leg_pos > 1e-9:
        return True
    return False


# ---------- IC_LONG filter (reads pre-computed decision from S3) ----------

CS_IC_DECISION_S3_KEY = os.environ.get(
    "CS_IC_DECISION_S3_KEY", "cadence/cs_ic_long_decision.json"
)

# --- Signal readiness config ---
# Wait until this ET time before fetching GW signal (e.g. "16:13:31").
# Allows broker prep to run in parallel with waiting for the signal.
CS_GW_READY_ET = os.environ.get("CS_GW_READY_ET", "").strip()


def _wait_for_gw_ready():
    """Sleep until CS_GW_READY_ET if set and in the future (max 120s)."""
    if not CS_GW_READY_ET:
        return
    try:
        parts = CS_GW_READY_ET.split(":")
        hh, mm = int(parts[0]), int(parts[1])
        ss = int(parts[2]) if len(parts) > 2 else 0
        now_et = datetime.now(ZoneInfo("America/New_York"))
        target = now_et.replace(hour=hh, minute=mm, second=ss, microsecond=0)
        wait = (target - now_et).total_seconds()
        if 0 < wait <= 120:
            print(f"CS_VERT_RUN WAIT: {wait:.1f}s until GW ready at {CS_GW_READY_ET} ET")
            time.sleep(wait)
        elif wait <= 0:
            print(f"CS_VERT_RUN WAIT: already past {CS_GW_READY_ET} ET ({-wait:.1f}s ago)")
    except Exception as e:
        print(f"CS_VERT_RUN WARN: CS_GW_READY_ET parse error ({e}), proceeding immediately")


def _read_ic_long_decision(today_str: str) -> Tuple[bool, bool, str]:
    """Read the pre-computed IC_LONG decision from S3.

    Returns (skip, switch_to_rr_short, reason).
    The decision is written by ic_long_filter.py which runs at 4:01 PM ET,
    before the orchestrators execute at 4:13 PM ET.
    """
    if not CS_IC_LONG_FILTER:
        return False, False, ""
    bucket = CS_MOVE_STATE_S3_BUCKET
    if not bucket:
        print("CS_VERT_RUN IC_FILTER: no S3 bucket, allowing trade")
        return False, False, ""
    try:
        import boto3
        s3 = boto3.client("s3")
        obj = s3.get_object(Bucket=bucket, Key=CS_IC_DECISION_S3_KEY)
        decision = json.loads(obj["Body"].read().decode("utf-8"))
    except Exception as e:
        print(f"CS_VERT_RUN IC_FILTER: cannot read decision ({e}), allowing trade")
        return False, False, ""

    if decision.get("date") != today_str:
        print(
            f"CS_VERT_RUN IC_FILTER: stale decision "
            f"(file={decision.get('date')}, today={today_str}), allowing trade"
        )
        return False, False, ""

    skip = decision.get("ic_long_skip", False)
    switch = decision.get("switch_to_rr_short", False)
    reason = decision.get("reason", "")
    regime_reason = decision.get("regime_reason", "")
    trail = decision.get("trail_move_pct")
    anchor = decision.get("anchor_pct")
    vix_rv10 = decision.get("vix_rv10_ratio")
    rv5_rv20 = decision.get("rv5_rv20_ratio")

    # Switch supersedes skip
    if switch:
        skip = False

    display_reason = regime_reason if switch else reason
    print(
        f"CS_VERT_RUN IC_FILTER: skip={'SKIP' if skip else 'no'} "
        f"switch={'RR_SHORT' if switch else 'no'} "
        f"trail={trail} anchor={anchor} "
        f"VIX/RV10={vix_rv10} RV5/RV20={rv5_rv20} "
        f"reason={display_reason}"
    )
    return skip, switch, display_reason


# ---------- main ----------

def main():
    today = date.today()
    ew = _init_events(today)

    # --- Schwab + equity (with override & fallback) ---
    try:
        c = schwab_client()
        oc_val, oc_src, acct_num = opening_cash_for_account(c)
        acct_hash = get_account_hash(c)
    except Exception as e:
        _emit("strategy_run", signal="SKIP", config="", reason=f"Schwab init failed: {e}")
        _emit("error", message=str(e), stage="schwab_init")
        _close_events()
        print(f"CS_VERT_RUN SKIP: Schwab init failed: {e}")
        return 1

    ov_raw = (os.environ.get("SIZING_DOLLARS_OVERRIDE", "") or "").strip()
    if ov_raw:
        try:
            oc_val = float(ov_raw)
            oc_src = "SIZING_DOLLARS_OVERRIDE"
            print(f"CS_VERT_RUN INFO: using SIZING_DOLLARS_OVERRIDE={oc_val}")
        except Exception:
            print("CS_VERT_RUN WARN: bad SIZING_DOLLARS_OVERRIDE, ignoring override.")

    print(f"CS_VERT_RUN EQUITY_RAW: {oc_val} (src={oc_src}, acct={acct_num})")

    if CS_UNIT_DOLLARS <= 0:
        print("CS_VERT_RUN FATAL: CS_UNIT_DOLLARS must be > 0")
        return 1

    if oc_val is None or oc_val <= 0:
        print("CS_VERT_RUN WARN: equity unavailable/<=0 — defaulting to CS_UNIT_DOLLARS for sizing")
        oc_val = CS_UNIT_DOLLARS
        units = 1
    else:
        units = max(1, int(oc_val // CS_UNIT_DOLLARS))

    print(f"CS_VERT_RUN UNITS: {units} (CS_UNIT_DOLLARS={CS_UNIT_DOLLARS}, oc_val={oc_val})")

    # --- PHASE 1b: Load positions early (while waiting for GW signal) ---
    need_positions = CS_GUARD_NO_CLOSE or CS_TOPUP
    pos = None
    if need_positions:
        try:
            pos = positions_map(c, acct_hash)
            print(
                f"CS_VERT_RUN POSITIONS: loaded count={len(pos)} "
                f"(guard={'on' if CS_GUARD_NO_CLOSE else 'off'}, topup={'on' if CS_TOPUP else 'off'})"
            )
        except Exception as e:
            msg = str(e)[:220]
            guard_ok = (not CS_GUARD_NO_CLOSE) or (CS_GUARD_FAIL_ACTION == "CONTINUE")
            topup_ok = (not CS_TOPUP) or (CS_TOPUP_FAIL_ACTION == "CONTINUE")
            if guard_ok and topup_ok:
                print(f"CS_VERT_RUN POSITIONS WARN: fetch failed ({msg}) — continuing WITHOUT positions (guard/topup degraded).")
                pos = None
            else:
                _emit("strategy_run", signal="SKIP", config="", reason=f"POSITIONS_FETCH_FAILED: {msg}")
                _emit("skip", reason="POSITIONS_FETCH_FAILED", signal="SKIP")
                _close_events()
                print(f"CS_VERT_RUN POSITIONS SKIP: fetch failed ({msg}) — skipping ALL trades.")
                return 0

    # --- PHASE 2: Wait for GW signal readiness ---
    _wait_for_gw_ready()

    # --- PHASE 3: Fetch GW signal + build + place ---
    signal_override = os.environ.get("CS_SIGNAL_JSON", "").strip()
    if signal_override:
        import json as _json
        tr = _json.loads(signal_override)
        print("CS_VERT_RUN SIGNAL_SOURCE: MANUAL (CS_SIGNAL_JSON)")
    else:
        try:
            api = gw_fetch()
            tr = extract_trade(api)
        except Exception as e:
            _emit("strategy_run", signal="SKIP", config="", reason=f"GW_FETCH_FAILED: {e}")
            _emit("skip", reason="GW_FETCH_FAILED", signal="SKIP")
            _close_events()
            print(f"CS_VERT_RUN SKIP: GW fetch failed: {e}")
            return 0

    if not tr:
        _emit("strategy_run", signal="SKIP", config="", reason="NO_TRADE_PAYLOAD")
        _emit("skip", reason="NO_TRADE_PAYLOAD", signal="SKIP")
        _close_events()
        print("CS_VERT_RUN SKIP: NO_TRADE_PAYLOAD")
        return 0

    trade_date = str(tr.get("Date", ""))
    tdate_iso = str(tr.get("TDate", ""))

    exp6 = yymmdd(tdate_iso)
    inner_put = int(float(tr.get("Limit")))
    inner_call = int(float(tr.get("CLimit")))
    width = 5

    # strikes
    p_low = inner_put - width
    p_high = inner_put
    c_low = inner_call
    c_high = inner_call + width

    put_low_osi = to_osi(f".SPXW{exp6}P{p_low}")
    put_high_osi = to_osi(f".SPXW{exp6}P{p_high}")
    call_low_osi = to_osi(f".SPXW{exp6}C{c_low}")
    call_high_osi = to_osi(f".SPXW{exp6}C{c_high}")

    left_go = fnum(tr.get("LeftGo"))
    right_go = fnum(tr.get("RightGo"))
    left_imp = fnum(tr.get("LImp"))
    right_imp = fnum(tr.get("RImp"))

    put_strength = left_imp if left_imp is not None else (abs(left_go) if left_go is not None else 0.0)
    call_strength = right_imp if right_imp is not None else (abs(right_go) if right_go is not None else 0.0)

    # GW recommended spread prices (for fill quality comparison)
    gw_put_price = fnum(tr.get("Put"))
    gw_call_price = fnum(tr.get("Call"))

    # Vol bucket sizing
    field_used, vol_val = pick_vol_value(tr, CS_VOL_FIELD)
    bucket, vix_mult = vix_bucket_and_mult(vol_val, CS_VIX_BREAKS, CS_VIX_MULTS)
    print(f"CS_VERT_RUN VOL: field={CS_VOL_FIELD} used={field_used} value={vol_val} bucket={bucket} mult={vix_mult}")
    print(f"CS_VERT_RUN VIX_BREAKS={CS_VIX_BREAKS} VIX_MULTS={CS_VIX_MULTS} RR_CREDIT_RATIOS={CS_RR_CREDIT_RATIOS}")

    print(f"CS_VERT_RUN TRADE: Date={trade_date} TDate={tdate_iso}")
    print(f"  PUT strikes : {p_low} / {p_high}  OSI=({put_low_osi},{put_high_osi})  LeftGo={left_go} LImp={left_imp}")
    print(f"  CALL strikes: {c_low} / {c_high}  OSI=({call_low_osi},{call_high_osi})  RightGo={right_go} RImp={right_imp}")
    print(f"CS_VERT_RUN RAW_STRENGTH: put_strength={put_strength:.3f} call_strength={call_strength:.3f}")

    # Build candidate verticals
    v_put = None
    v_call = None

    if left_go is not None and left_go != 0.0 and vix_mult != 0:
        target = max(1, units * int(vix_mult))
        if left_go < 0:
            v_put = {
                "name": "PUT_SHORT", "kind": "PUT", "side": "CREDIT", "direction": "SHORT",
                "short_osi": put_high_osi, "long_osi": put_low_osi,
                "go": left_go, "strength": put_strength, "target_qty": target,
            }
        else:
            v_put = {
                "name": "PUT_LONG", "kind": "PUT", "side": "DEBIT", "direction": "LONG",
                "short_osi": put_low_osi, "long_osi": put_high_osi,
                "go": left_go, "strength": put_strength, "target_qty": target,
            }

    if right_go is not None and right_go != 0.0 and vix_mult != 0:
        target = max(1, units * int(vix_mult))
        if right_go < 0:
            v_call = {
                "name": "CALL_SHORT", "kind": "CALL", "side": "CREDIT", "direction": "SHORT",
                "short_osi": call_low_osi, "long_osi": call_high_osi,
                "go": right_go, "strength": call_strength, "target_qty": target,
            }
        else:
            v_call = {
                "name": "CALL_LONG", "kind": "CALL", "side": "DEBIT", "direction": "LONG",
                "short_osi": call_high_osi, "long_osi": call_low_osi,
                "go": right_go, "strength": call_strength, "target_qty": target,
            }

    if not v_put and not v_call:
        _emit("strategy_run", signal="SKIP", config="",
              reason="NO_CANDIDATES", spot=0.0,
              vix=float(vol_val or 0), vix1d=float(vol_val or 0))
        _emit("skip", reason="NO_CANDIDATES", signal="SKIP")
        _close_events()
        print("CS_VERT_RUN SKIP: no verticals to trade (LeftGo/RightGo zero or vix_mult=0).")
        return 0

    # --- Regime rule: IC_LONG → RR_SHORT switch ---
    regime_switched = False
    is_ic_long_candidate = (v_put and v_call
                            and v_put["side"] == "DEBIT" and v_call["side"] == "DEBIT")
    if is_ic_long_candidate:
        skip_ic, switch_rr, regime_reason = _read_ic_long_decision(today.isoformat())
        if skip_ic:
            _emit("strategy_run", signal="SKIP", config="",
                  reason=f"IC_LONG_SKIP: {regime_reason}")
            _emit("skip", reason="IC_LONG_SKIP", signal="SKIP")
            _close_events()
            print(f"CS_VERT_RUN IC_LONG_SKIP: {regime_reason}")
            return 0
        if switch_rr:
            regime_switched = True
            print(f"CS_VERT_RUN REGIME_SWITCH: IC_LONG → RR_SHORT | {regime_reason}")
            # Flip call side: DEBIT → CREDIT (CALL_LONG → CALL_SHORT)
            # Put side stays DEBIT (PUT_LONG) — result is RR_SHORT
            # RR_SHORT = buy put spread + sell call spread (bearish)
            v_call = {
                "name": "CALL_SHORT", "kind": "CALL", "side": "CREDIT", "direction": "SHORT",
                "short_osi": call_low_osi, "long_osi": call_high_osi,
                "go": right_go, "strength": call_strength, "target_qty": v_call["target_qty"],
            }

    # Structure-based sizing adjustments
    if v_put and v_call:
        base_mults = parse_csv_floats(CS_VIX_MULTS)

        if v_put["side"] != v_call["side"] and CS_RR_CREDIT_RATIOS and not regime_switched:
            # RR: per-bucket credit ratio for the credit side
            ratios = parse_csv_floats(CS_RR_CREDIT_RATIOS)
            if len(ratios) == len(base_mults):
                ratio = ratios[bucket - 1]
                full_target = max(1, units * int(vix_mult))
                credit_target = max(1, round(full_target * ratio))
                for v in (v_put, v_call):
                    if v["side"] == "CREDIT":
                        print(f"CS_VERT_RUN RR_CREDIT_ADJ: {v['name']} target {full_target} → {credit_target} (ratio={ratio} bucket={bucket} mult={vix_mult})")
                        v["target_qty"] = credit_target

        elif v_put["side"] == v_call["side"]:
            # IC: separate multiplier arrays for Short IC vs Long IC
            ic_mults_csv = CS_IC_SHORT_MULTS if v_put["side"] == "CREDIT" else CS_IC_LONG_MULTS
            ic_label = "IC_SHORT" if v_put["side"] == "CREDIT" else "IC_LONG"
            if ic_mults_csv:
                ic_mults = parse_csv_floats(ic_mults_csv)
                if len(ic_mults) == len(base_mults):
                    ic_mult = int(ic_mults[bucket - 1])
                    ic_target = max(1, units * ic_mult)
                    for v in (v_put, v_call):
                        print(f"CS_VERT_RUN {ic_label}_SIZE: {v['name']} target {v['target_qty']} → {ic_target} (ic_mult={ic_mult} bucket={bucket})")
                        v["target_qty"] = ic_target

    # Apply TOPUP + GUARD to determine send_qty
    def finalize(v: Dict[str, Any]) -> Dict[str, Any] | None:
        if not v:
            return None

        target_qty = int(v["target_qty"])
        open_qty = 0

        if CS_TOPUP:
            if pos is None:
                # If topup is enabled but we couldn't load positions, safest is to skip
                if CS_TOPUP_FAIL_ACTION != "CONTINUE":
                    print(f"CS_VERT_RUN SKIP {v['name']}: TOPUP_NEEDS_POSITIONS")
                    return None
            else:
                open_qty = open_spreads_for_vertical(v, pos)

        rem = max(0, target_qty - open_qty) if CS_TOPUP else target_qty
        if CS_TOPUP:
            print(f"CS_VERT_RUN TOPUP {v['name']}: target={target_qty} open={open_qty} rem={rem}")

        if rem <= 0:
            print(f"CS_VERT_RUN SKIP {v['name']}: AT_OR_ABOVE_TARGET")
            return None

        # Guard check
        if CS_GUARD_NO_CLOSE:
            if pos is None:
                if CS_GUARD_FAIL_ACTION != "CONTINUE":
                    print(f"CS_VERT_RUN SKIP {v['name']}: GUARD_NEEDS_POSITIONS")
                    return None
            else:
                if would_close_guard(v, pos):
                    print(f"CS_VERT_RUN GUARD_SKIP {v['name']}: WOULD_CLOSE")
                    return None

        v2 = dict(v)
        v2["send_qty"] = int(rem)
        return v2

    v_put_f = finalize(v_put) if v_put else None
    v_call_f = finalize(v_call) if v_call else None

    if not v_put_f and not v_call_f:
        _emit("strategy_run", signal="SKIP", config="",
              reason="TOPUP_GUARD_FILTERED", vix=float(vol_val or 0))
        _emit("skip", reason="TOPUP_GUARD_FILTERED", signal="SKIP")
        _close_events()
        print("CS_VERT_RUN SKIP: nothing to place after TOPUP/GUARD.")
        return 0

    # --- IC_LONG deferred morning entry ---
    # If IC_LONG (both sides DEBIT) and NOT regime-switched to RR_SHORT,
    # save the plan to S3 and skip evening placement.  A morning Lambda
    # trigger (9:35 AM ET next day) will read the plan, fetch fresh quotes,
    # and place with price filters ($2.20 max total, $0.40 min per side).
    CS_IC_LONG_DEFER = os.environ.get("CS_IC_LONG_DEFER", "1").strip() in ("1", "true", "yes")
    CS_IC_DEFER_S3_KEY = os.environ.get("CS_IC_DEFER_S3_KEY", "cadence/cs_ic_long_deferred.json")

    is_ic_long_final = (v_put_f and v_call_f
                        and v_put_f["side"] == "DEBIT" and v_call_f["side"] == "DEBIT"
                        and not regime_switched)
    if CS_IC_LONG_DEFER and is_ic_long_final:
        defer_plan = {
            "trade_date": trade_date,
            "execute_date": tdate_iso,
            "inner_put": inner_put,
            "inner_call": inner_call,
            "p_low": p_low, "p_high": p_high,
            "c_low": c_low, "c_high": c_high,
            "exp6": exp6,
            "put_low_osi": put_low_osi, "put_high_osi": put_high_osi,
            "call_low_osi": call_low_osi, "call_high_osi": call_high_osi,
            "put_credit_close": gw_put_price,
            "call_credit_close": gw_call_price,
            "put_qty": v_put_f["send_qty"],
            "call_qty": v_call_f["send_qty"],
            "put_strength": v_put_f["strength"],
            "call_strength": v_call_f["strength"],
            "put_go": v_put_f.get("go"),
            "call_go": v_call_f.get("go"),
            "vol_field": field_used, "vol_value": vol_val,
            "vol_bucket": bucket, "vol_mult": vix_mult,
            "units": units, "unit_dollars": CS_UNIT_DOLLARS,
            "status": "pending",
            "saved_utc": datetime.now(timezone.utc).isoformat(),
        }
        bucket_name = CS_MOVE_STATE_S3_BUCKET
        if bucket_name:
            try:
                import boto3
                s3 = boto3.client("s3")
                s3.put_object(
                    Bucket=bucket_name,
                    Key=CS_IC_DEFER_S3_KEY,
                    Body=json.dumps(defer_plan, indent=2),
                    ContentType="application/json",
                )
                print(f"CS_VERT_RUN IC_LONG_DEFERRED: saved plan to s3://{bucket_name}/{CS_IC_DEFER_S3_KEY}")
                print(f"  strikes: put {p_low}/{p_high}  call {c_low}/{c_high}  exp={tdate_iso}")
                print(f"  close prices: put={gw_put_price} call={gw_call_price}")
                print(f"  qty: put={v_put_f['send_qty']} call={v_call_f['send_qty']}")
                _emit("strategy_run", signal="IC_LONG_DEFERRED", config=f"{v_put_f['name']}+{v_call_f['name']}",
                      reason="DEFERRED_TO_MORNING", vix=float(vol_val or 0))
                _close_events()
                return 0
            except Exception as e:
                print(f"CS_VERT_RUN IC_LONG_DEFER WARN: S3 write failed ({e}), placing at close instead")
        else:
            print("CS_VERT_RUN IC_LONG_DEFER WARN: no S3 bucket, placing at close instead")

    qty_rule = "VIX_BUCKET_TOPUP" if CS_TOPUP else "VIX_BUCKET"

    def env_for_vertical(v: Dict[str, Any]) -> Dict[str, str]:
        strength_s = f"{float(v['strength']):.3f}"
        # GW price for this vertical's kind
        gw_px = gw_put_price if v["kind"] == "PUT" else gw_call_price
        e = dict(os.environ)
        e.update({
            "VERT_SIDE":         v["side"],
            "VERT_KIND":         v["kind"],
            "VERT_NAME":         v["name"],
            "VERT_DIRECTION":    v["direction"],
            "VERT_SHORT_OSI":    v["short_osi"],
            "VERT_LONG_OSI":     v["long_osi"],
            "VERT_QTY":          str(v["send_qty"]),
            "VERT_GO":           "" if v.get("go") is None else str(v["go"]),
            "VERT_STRENGTH":     strength_s,
            "VERT_TRADE_DATE":   trade_date,
            "VERT_TDATE":        tdate_iso,
            "VERT_GW_PRICE":     "" if gw_px is None else str(gw_px),

            # sizing context
            "VERT_UNIT_DOLLARS": str(CS_UNIT_DOLLARS),
            "VERT_OC":           str(oc_val),
            "VERT_UNITS":        str(units),

            # vol context
            "VERT_VOL_FIELD":    CS_VOL_FIELD,
            "VERT_VOL_USED":     field_used,
            "VERT_VOL_VALUE":    "" if vol_val is None else str(vol_val),
            "VERT_VOL_BUCKET":   str(bucket),
            "VERT_VOL_MULT":     str(vix_mult),
            "VERT_QTY_RULE":     qty_rule,

            # needed by placer
            "SCHWAB_APP_KEY":     os.environ["SCHWAB_APP_KEY"],
            "SCHWAB_APP_SECRET":  os.environ["SCHWAB_APP_SECRET"],
            "SCHWAB_TOKEN_JSON":  os.environ["SCHWAB_TOKEN_JSON"],
            "SCHWAB_ACCT_HASH":   acct_hash,
            "CS_LOG_PATH":        CS_LOG_PATH,
        })
        return e

    # ----- Place CALL + PUT together (bundle for IC_SHORT, pair-alternate otherwise) -----
    is_ic_short = (v_put_f and v_call_f
                   and v_put_f["side"] == "CREDIT" and v_call_f["side"] == "CREDIT")

    if CS_PAIR_ALTERNATE and v_put_f and v_call_f:
        q_put = int(v_put_f["send_qty"])
        q_call = int(v_call_f["send_qty"])
        if q_put > 0 and q_call > 0:
            mode = "BUNDLE4" if is_ic_short else "PAIR_ALT"

            # Determine structure label for events
            if is_ic_short:
                struct_label = "IC_SHORT"
            elif v_put_f["side"] != v_call_f["side"]:
                struct_label = "RR"
            else:
                struct_label = "IC_LONG"

            _emit("strategy_run", signal=struct_label, config=f"{v_put_f['name']}+{v_call_f['name']}",
                  reason="OK", vix=float(vol_val or 0),
                  extra={"mode": mode, "trade_date": trade_date, "tdate": tdate_iso,
                         "vol_field": field_used, "vol_bucket": bucket, "vol_mult": vix_mult})

            # Emit trade_intent for each vertical, capturing trade_group_id per name
            _saved_groups = []
            for v_side in (v_call_f, v_put_f):
                if _ew is not None:
                    _ew.new_trade_group()
                    _saved_groups.append((v_side["name"], _ew.trade_group_id))
                _emit("trade_intent", side=v_side["side"], direction=v_side["direction"],
                      legs=[
                          {"osi": v_side["short_osi"], "strike": 0,
                           "option_type": v_side["kind"], "action": "SELL_TO_OPEN", "qty": v_side["send_qty"]},
                          {"osi": v_side["long_osi"], "strike": 0,
                           "option_type": v_side["kind"], "action": "BUY_TO_OPEN", "qty": v_side["send_qty"]},
                      ],
                      target_qty=v_side["send_qty"],
                      extra={"name": v_side["name"]})

            print(
                f"CS_VERT_RUN {mode}: "
                f"CALL={v_call_f['name']}({v_call_f['short_osi']}|{v_call_f['long_osi']}) qty={q_call} "
                f"PUT={v_put_f['name']}({v_put_f['short_osi']}|{v_put_f['long_osi']}) qty={q_put}"
            )

            _rows_before = _csv_row_count()

            env = env_for_vertical({**v_call_f, "send_qty": q_call})
            if is_ic_short:
                env["VERT_BUNDLE"] = "true"
                env["VERT_BUNDLE_FALLBACK"] = "separate"
            else:
                env["VERT_PAIR"] = "true"
            env.update({
                "VERT2_SIDE":       v_put_f["side"],
                "VERT2_KIND":       v_put_f["kind"],
                "VERT2_NAME":       v_put_f["name"],
                "VERT2_DIRECTION":  v_put_f["direction"],
                "VERT2_SHORT_OSI":  v_put_f["short_osi"],
                "VERT2_LONG_OSI":   v_put_f["long_osi"],
                "VERT2_QTY":        str(q_put),
                "VERT2_GO":         "" if v_put_f.get("go") is None else str(v_put_f["go"]),
                "VERT2_STRENGTH":   f"{float(v_put_f['strength']):.3f}",
                "VERT2_GW_PRICE":   "" if gw_put_price is None else str(gw_put_price),
            })

            rc = subprocess.call([sys.executable, "scripts/trade/ConstantStable/place.py"], env=env)
            if rc != 0:
                _emit("error", message=f"placer rc={rc}", stage=f"placement_{mode}")
                print(f"CS_VERT_RUN PAIR_ALT: placer rc={rc}")
            else:
                _read_back_fills(_saved_groups, _rows_before)
            _close_events()
            return 0

        print(f"CS_VERT_RUN PAIR_ALT SKIP: qty missing (put={q_put} call={q_call}) — placing separately")

    # ----- Fallback: place separately -----
    # Emit strategy_run once for the whole run
    active_names = [v["name"] for v in (v_put_f, v_call_f) if v]
    _emit("strategy_run", signal="+".join(active_names), config="+".join(active_names),
          reason="OK", vix=float(vol_val or 0),
          extra={"mode": "SEPARATE", "trade_date": trade_date, "tdate": tdate_iso,
                 "vol_field": field_used, "vol_bucket": bucket, "vol_mult": vix_mult})

    for v in (v_put_f, v_call_f):
        if not v:
            continue

        if _ew is not None:
            _ew.new_trade_group()

        _saved_gid = _ew.trade_group_id if _ew else ""

        _emit("trade_intent", side=v["side"], direction=v["direction"],
              legs=[
                  {"osi": v["short_osi"], "strike": 0,
                   "option_type": v["kind"], "action": "SELL_TO_OPEN", "qty": v["send_qty"]},
                  {"osi": v["long_osi"], "strike": 0,
                   "option_type": v["kind"], "action": "BUY_TO_OPEN", "qty": v["send_qty"]},
              ],
              target_qty=v["send_qty"],
              extra={"name": v["name"]})

        print(
            f"CS_VERT_RUN {v['name']}: side={v['side']} kind={v['kind']} "
            f"short={v['short_osi']} long={v['long_osi']} "
            f"go={v['go']} target_qty={v['target_qty']} send_qty={v['send_qty']} "
            f"(units={units} vix_mult={vix_mult} bucket={bucket})"
        )
        _rows_before = _csv_row_count()
        env = env_for_vertical(v)
        rc = subprocess.call([sys.executable, "scripts/trade/ConstantStable/place.py"], env=env)
        if rc != 0:
            _emit("error", message=f"placer rc={rc}", stage=f"placement_{v['name']}")
            print(f"CS_VERT_RUN {v['name']}: placer rc={rc}")
        else:
            _read_back_fills([(v["name"], _saved_gid)], _rows_before)

    _close_events()
    return 0


if __name__ == "__main__":
    sys.exit(main())
