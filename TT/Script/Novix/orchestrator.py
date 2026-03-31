#!/usr/bin/env python3
# NOVIX — vertical orchestrator (TastyTrade)
#
# Same core trade logic as ConstantStable but driven by the Novix
# GammaWizard signal (rapi/GetNovix).
#
# Differences from ConstantStable:
#   - No IC_LONG regime filter / skip
#   - No IC_LONG deferred morning entry
#   - No IC_LONG → RR_SHORT regime switching
#
# Everything else is identical: GW fetch → LeftGo/RightGo → 5-wide
# verticals → vol sizing → topup/guard → bundle/pair-alt placement.
#
# Delegates placement to TT/Script/ConstantStable/place.py via VERT_* envs.

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
        if os.path.basename(cur) in ("scripts", "Script"):
            if cur not in sys.path:
                sys.path.append(cur)
            return
        parent = os.path.dirname(cur)
        if parent == cur:
            return
        cur = parent


_add_scripts_root()
from tt_client import request as tt_request
from pathlib import Path

__version__ = "1.0.0"
_TAG = "NX_VERT_RUN"

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


def _init_events(today: date, acct_label: str):
    global _ew
    if not _EVENTS_AVAILABLE:
        return None
    try:
        _ew = EventWriter(strategy="novix", account=acct_label, trade_date=today)
        return _ew
    except Exception as e:
        print(f"{_TAG} WARN: EventWriter init failed: {e}")
        return None


def _emit(method: str, **kwargs):
    if _ew is None:
        return
    try:
        getattr(_ew, method)(**kwargs)
    except Exception as e:
        print(f"{_TAG} WARN: event emit failed ({method}): {e}")


def _close_events():
    if _ew is not None:
        try:
            _ew.close()
        except Exception:
            pass


def _csv_row_count() -> int:
    try:
        import csv as _csv
        with open(CS_LOG_PATH) as f:
            return sum(1 for _ in _csv.DictReader(f))
    except Exception:
        return 0


def _read_back_fills(group_ids: list, rows_before: int):
    if _ew is None:
        return
    try:
        import csv as _csv
        with open(CS_LOG_PATH) as f:
            all_rows = list(_csv.DictReader(f))
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
        print(f"{_TAG} WARN: could not read placement result: {e}")


# ── Config ──

GW_BASE = os.environ.get("GW_BASE", "https://gandalf.gammawizard.com").rstrip("/")
GW_ENDPOINT = os.environ.get("GW_ENDPOINT", "rapi/GetNovix").lstrip("/")

CS_UNIT_DOLLARS = float(os.environ.get("CS_UNIT_DOLLARS", "10000"))
CS_LOG_PATH = os.environ.get("CS_LOG_PATH", "/tmp/nx_trades.csv")

CS_VOL_FIELD = (os.environ.get("CS_VOL_FIELD", "VixOne") or "VixOne").strip()
CS_VIX_BREAKS = os.environ.get("CS_VIX_BREAKS", "0.089,0.111,0.131,0.158,0.192,0.253")
CS_VIX_MULTS = os.environ.get("CS_VIX_MULTS", "1,1,1,2,3,4,6")
CS_RR_CREDIT_RATIOS = os.environ.get("CS_RR_CREDIT_RATIOS", "")
CS_IC_SHORT_MULTS = os.environ.get("CS_IC_SHORT_MULTS", "")
CS_IC_LONG_MULTS = os.environ.get("CS_IC_LONG_MULTS", "")

CS_GUARD_NO_CLOSE = (os.environ.get("CS_GUARD_NO_CLOSE", "1") or "1").strip().lower() in ("1", "true", "yes", "y")
CS_GUARD_FAIL_ACTION = (os.environ.get("CS_GUARD_FAIL_ACTION", "SKIP_ALL") or "SKIP_ALL").strip().upper()

CS_TOPUP = (os.environ.get("CS_TOPUP", "1") or "1").strip().lower() in ("1", "true", "yes", "y")
CS_TOPUP_FAIL_ACTION = (os.environ.get("CS_TOPUP_FAIL_ACTION", "SKIP_ALL") or "SKIP_ALL").strip().upper()

CS_BUNDLE_4LEG = (os.environ.get("CS_BUNDLE_4LEG", "1") or "1").strip().lower() in ("1", "true", "yes", "y")
CS_BUNDLE_REQUIRE_EQUAL_QTY = (os.environ.get("CS_BUNDLE_REQUIRE_EQUAL_QTY", "1") or "1").strip().lower() in ("1", "true", "yes", "y")
CS_BUNDLE_FALLBACK = (os.environ.get("CS_BUNDLE_FALLBACK", "1") or "1").strip().lower() in ("1", "true", "yes", "y")
CS_PAIR_ALTERNATE = (os.environ.get("CS_PAIR_ALTERNATE", "1") or "1").strip().lower() in ("1", "true", "yes", "y")

# Signal readiness wait
CS_GW_READY_ET = os.environ.get("CS_GW_READY_ET", "").strip()

# Placer script (shared TT placer)
PLACER_SCRIPT = os.environ.get(
    "NX_PLACER_SCRIPT",
    "TT/Script/ConstantStable/place.py",
)


# ── Utility helpers ──

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


# ── GammaWizard ──

def _sanitize_token(t: str) -> str:
    t = (t or "").strip().strip('"').strip("'")
    return t.split(None, 1)[1] if t.lower().startswith("bearer ") else t


def gw_fetch():
    base = GW_BASE
    endpoint = GW_ENDPOINT
    tok = _sanitize_token(os.environ.get("GW_TOKEN", "") or "")

    url = f"{base}/{endpoint}"
    print(f"{_TAG} GW URL: {url}")

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
        for it in j:
            t = extract_trade(it)
            if t:
                return t
    return {}


# ── TastyTrade helpers ──

def tt_account_number():
    acct = (os.environ.get("TT_ACCOUNT_NUMBER") or "").strip()
    if not acct:
        raise RuntimeError("TT_ACCOUNT_NUMBER missing")
    return acct


def opening_cash_for_account(prefer_number=None):
    acct_num = prefer_number or tt_account_number()
    j = tt_request("GET", f"/accounts/{acct_num}/balances").json()
    data = j.get("data") if isinstance(j, dict) else {}
    src = data or {}
    keys = [
        "net-liquidating-value",
        "cash-balance",
        "cash-available-to-withdraw",
        "equity-buying-power",
        "derivative-buying-power",
    ]

    def pick(src):
        for k in keys:
            v = (src or {}).get(k)
            try:
                fv = float(v)
                if fv > 0:
                    return fv, k
            except Exception:
                continue
        return None

    got = pick(src)
    if got:
        return got[0], got[1], acct_num
    return None, "none", acct_num


# ── Positions map (for NO-CLOSE guard + TOPUP) ──

def _sleep_for_429(resp, attempt: int) -> float:
    ra = resp.headers.get("Retry-After")
    if ra:
        try:
            return max(1.0, float(ra))
        except Exception:
            pass
    return min(8.0, 0.6 * (2 ** attempt)) + random.uniform(0.0, 0.25)


def tt_get_json(url: str, params=None, tries: int = 6, tag: str = ""):
    last = ""
    for i in range(tries):
        try:
            r = tt_request("GET", url, params=(params or {}))
            return r.json()
        except requests.HTTPError as e:
            resp = e.response
            if resp is not None and resp.status_code == 429:
                time.sleep(_sleep_for_429(resp, i))
                continue
            last = f"HTTP_{resp.status_code}:{(resp.text or '')[:200]}" if resp is not None else "HTTP_unknown"
        except Exception as e:
            last = f"{type(e).__name__}:{str(e)}"
        time.sleep(min(6.0, 0.5 * (2 ** i)))
    raise RuntimeError(f"TT_GET_FAIL({tag}) {last or 'unknown'}")


def _osi_from_symbol(sym: str) -> str | None:
    sym = (sym or "").strip()
    if not sym:
        return None
    try:
        return to_osi(re.sub(r"\s+", "", sym))
    except Exception:
        return None


def positions_map(acct_num: str) -> Dict[Tuple[str, str, str], float]:
    j = tt_get_json(f"/accounts/{acct_num}/positions", tag="POSITIONS")
    data = j.get("data") if isinstance(j, dict) else {}
    items = data.get("items") or []
    out: Dict[Tuple[str, str, str], float] = {}

    for p in items:
        atype = (p.get("instrument-type") or p.get("instrument_type") or "").upper()
        if "OPTION" not in atype:
            continue

        osi = _osi_from_symbol(p.get("symbol", ""))
        if not osi:
            continue

        try:
            qty = float(p.get("quantity", 0) or 0)
        except Exception:
            continue

        direction = str(p.get("quantity-direction") or p.get("quantity_direction") or "").lower()
        if direction.startswith("short"):
            qty = -abs(qty)

        if abs(qty) < 1e-9:
            continue

        key = osi_canon(osi)
        out[key] = out.get(key, 0.0) + qty

    return out


def open_spreads_for_vertical(v: Dict[str, Any], pos: Dict[Tuple[str, str, str], float]) -> int:
    buy_key = osi_canon(v["long_osi"])
    sell_key = osi_canon(v["short_osi"])
    buy_pos = float(pos.get(buy_key, 0.0))
    sell_pos = float(pos.get(sell_key, 0.0))
    b = buy_pos if buy_pos > 0 else 0.0
    s = (-sell_pos) if sell_pos < 0 else 0.0
    return int(min(b, s) + 1e-9)


def would_close_guard(v: Dict[str, Any], pos: Dict[Tuple[str, str, str], float]) -> bool:
    buy_key = osi_canon(v["long_osi"])
    sell_key = osi_canon(v["short_osi"])
    buy_leg_pos = float(pos.get(buy_key, 0.0))
    sell_leg_pos = float(pos.get(sell_key, 0.0))

    print(
        f"{_TAG} GUARD_CHECK {v['name']}: "
        f"BUY_TO_OPEN {v['long_osi']} pos={buy_leg_pos:+g} ; "
        f"SELL_TO_OPEN {v['short_osi']} pos={sell_leg_pos:+g}"
    )

    if buy_leg_pos < -1e-9:
        return True
    if sell_leg_pos > 1e-9:
        return True
    return False


# ── GW signal readiness wait ──

def _wait_for_gw_ready():
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
            print(f"{_TAG} WAIT: {wait:.1f}s until GW ready at {CS_GW_READY_ET} ET")
            time.sleep(wait)
        elif wait <= 0:
            print(f"{_TAG} WAIT: already past {CS_GW_READY_ET} ET ({-wait:.1f}s ago)")
    except Exception as e:
        print(f"{_TAG} WARN: CS_GW_READY_ET parse error ({e}), proceeding immediately")


# ── main ──

def main():
    today = date.today()
    acct_label = os.environ.get("CS_ACCOUNT_LABEL", "novix-tt")
    ew = _init_events(today, acct_label)

    # --- TastyTrade + equity ---
    try:
        acct_num = tt_account_number()
        oc_val, oc_src, acct_num = opening_cash_for_account(acct_num)
    except Exception as e:
        _emit("strategy_run", signal="SKIP", config="", reason=f"TT init failed: {e}")
        _emit("error", message=str(e), stage="tt_init")
        _close_events()
        print(f"{_TAG} SKIP: TastyTrade init failed: {e}")
        return 1

    ov_raw = (os.environ.get("SIZING_DOLLARS_OVERRIDE", "") or "").strip()
    if ov_raw:
        try:
            oc_val = float(ov_raw)
            oc_src = "SIZING_DOLLARS_OVERRIDE"
            print(f"{_TAG} INFO: using SIZING_DOLLARS_OVERRIDE={oc_val}")
        except Exception:
            print(f"{_TAG} WARN: bad SIZING_DOLLARS_OVERRIDE, ignoring override.")

    print(f"{_TAG} EQUITY_RAW: {oc_val} (src={oc_src}, acct={acct_num})")

    if CS_UNIT_DOLLARS <= 0:
        print(f"{_TAG} FATAL: CS_UNIT_DOLLARS must be > 0")
        return 1

    if oc_val is None or oc_val <= 0:
        print(f"{_TAG} WARN: equity unavailable/<=0 — defaulting to CS_UNIT_DOLLARS for sizing")
        oc_val = CS_UNIT_DOLLARS
        units = 1
    else:
        units = max(1, int(oc_val // CS_UNIT_DOLLARS))

    print(f"{_TAG} UNITS: {units} (CS_UNIT_DOLLARS={CS_UNIT_DOLLARS}, oc_val={oc_val})")

    # --- Load positions early ---
    need_positions = CS_GUARD_NO_CLOSE or CS_TOPUP
    pos = None
    if need_positions:
        try:
            pos = positions_map(acct_num)
            print(
                f"{_TAG} POSITIONS: loaded count={len(pos)} "
                f"(guard={'on' if CS_GUARD_NO_CLOSE else 'off'}, topup={'on' if CS_TOPUP else 'off'})"
            )
        except Exception as e:
            msg = str(e)[:220]
            guard_ok = (not CS_GUARD_NO_CLOSE) or (CS_GUARD_FAIL_ACTION == "CONTINUE")
            topup_ok = (not CS_TOPUP) or (CS_TOPUP_FAIL_ACTION == "CONTINUE")
            if guard_ok and topup_ok:
                print(f"{_TAG} POSITIONS WARN: fetch failed ({msg}) — continuing WITHOUT positions.")
                pos = None
            else:
                _emit("strategy_run", signal="SKIP", config="", reason=f"POSITIONS_FETCH_FAILED: {msg}")
                _emit("skip", reason="POSITIONS_FETCH_FAILED", signal="SKIP")
                _close_events()
                print(f"{_TAG} POSITIONS SKIP: fetch failed ({msg}) — skipping ALL trades.")
                return 0

    # --- Wait for GW signal readiness ---
    _wait_for_gw_ready()

    # --- Fetch GW signal ---
    signal_override = os.environ.get("CS_SIGNAL_JSON", "").strip()
    if signal_override:
        tr = json.loads(signal_override)
        print(f"{_TAG} SIGNAL_SOURCE: MANUAL (CS_SIGNAL_JSON)")
    else:
        try:
            api = gw_fetch()
            tr = extract_trade(api)
        except Exception as e:
            _emit("strategy_run", signal="SKIP", config="", reason=f"GW_FETCH_FAILED: {e}")
            _emit("skip", reason="GW_FETCH_FAILED", signal="SKIP")
            _close_events()
            print(f"{_TAG} SKIP: GW fetch failed: {e}")
            return 0

    if not tr:
        _emit("strategy_run", signal="SKIP", config="", reason="NO_TRADE_PAYLOAD")
        _emit("skip", reason="NO_TRADE_PAYLOAD", signal="SKIP")
        _close_events()
        print(f"{_TAG} SKIP: NO_TRADE_PAYLOAD")
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

    gw_put_price = fnum(tr.get("Put"))
    gw_call_price = fnum(tr.get("Call"))

    # Vol bucket sizing
    field_used, vol_val = pick_vol_value(tr, CS_VOL_FIELD)
    bucket, vix_mult = vix_bucket_and_mult(vol_val, CS_VIX_BREAKS, CS_VIX_MULTS)
    print(f"{_TAG} VOL: field={CS_VOL_FIELD} used={field_used} value={vol_val} bucket={bucket} mult={vix_mult}")
    print(f"{_TAG} VIX_BREAKS={CS_VIX_BREAKS} VIX_MULTS={CS_VIX_MULTS} RR_CREDIT_RATIOS={CS_RR_CREDIT_RATIOS}")

    print(f"{_TAG} TRADE: Date={trade_date} TDate={tdate_iso}")
    print(f"  PUT strikes : {p_low} / {p_high}  OSI=({put_low_osi},{put_high_osi})  LeftGo={left_go} LImp={left_imp}")
    print(f"  CALL strikes: {c_low} / {c_high}  OSI=({call_low_osi},{call_high_osi})  RightGo={right_go} RImp={right_imp}")
    print(f"{_TAG} RAW_STRENGTH: put_strength={put_strength:.3f} call_strength={call_strength:.3f}")

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
        print(f"{_TAG} SKIP: no verticals to trade (LeftGo/RightGo zero or vix_mult=0).")
        return 0

    # Structure-based sizing adjustments
    if v_put and v_call:
        base_mults = parse_csv_floats(CS_VIX_MULTS)

        if v_put["side"] != v_call["side"] and CS_RR_CREDIT_RATIOS:
            ratios = parse_csv_floats(CS_RR_CREDIT_RATIOS)
            if len(ratios) == len(base_mults):
                ratio = ratios[bucket - 1]
                full_target = max(1, units * int(vix_mult))
                credit_target = max(1, round(full_target * ratio))
                for v in (v_put, v_call):
                    if v["side"] == "CREDIT":
                        print(f"{_TAG} RR_CREDIT_ADJ: {v['name']} target {full_target} → {credit_target} (ratio={ratio} bucket={bucket} mult={vix_mult})")
                        v["target_qty"] = credit_target

        elif v_put["side"] == v_call["side"]:
            ic_mults_csv = CS_IC_SHORT_MULTS if v_put["side"] == "CREDIT" else CS_IC_LONG_MULTS
            ic_label = "IC_SHORT" if v_put["side"] == "CREDIT" else "IC_LONG"
            if ic_mults_csv:
                ic_mults = parse_csv_floats(ic_mults_csv)
                if len(ic_mults) == len(base_mults):
                    ic_mult = int(ic_mults[bucket - 1])
                    ic_target = max(1, units * ic_mult)
                    for v in (v_put, v_call):
                        print(f"{_TAG} {ic_label}_SIZE: {v['name']} target {v['target_qty']} → {ic_target} (ic_mult={ic_mult} bucket={bucket})")
                        v["target_qty"] = ic_target

    # Apply TOPUP + GUARD to determine send_qty
    def finalize(v: Dict[str, Any]) -> Dict[str, Any] | None:
        if not v:
            return None

        target_qty = int(v["target_qty"])
        open_qty = 0

        if CS_TOPUP:
            if pos is None:
                if CS_TOPUP_FAIL_ACTION != "CONTINUE":
                    print(f"{_TAG} SKIP {v['name']}: TOPUP_NEEDS_POSITIONS")
                    return None
            else:
                open_qty = open_spreads_for_vertical(v, pos)

        rem = max(0, target_qty - open_qty) if CS_TOPUP else target_qty
        if CS_TOPUP:
            print(f"{_TAG} TOPUP {v['name']}: target={target_qty} open={open_qty} rem={rem}")

        if rem <= 0:
            print(f"{_TAG} SKIP {v['name']}: AT_OR_ABOVE_TARGET")
            return None

        if CS_GUARD_NO_CLOSE:
            if pos is None:
                if CS_GUARD_FAIL_ACTION != "CONTINUE":
                    print(f"{_TAG} SKIP {v['name']}: GUARD_NEEDS_POSITIONS")
                    return None
            else:
                if would_close_guard(v, pos):
                    print(f"{_TAG} GUARD_SKIP {v['name']}: WOULD_CLOSE")
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
        print(f"{_TAG} SKIP: nothing to place after TOPUP/GUARD.")
        return 0

    qty_rule = "VIX_BUCKET_TOPUP" if CS_TOPUP else "VIX_BUCKET"

    def env_for_vertical(v: Dict[str, Any]) -> Dict[str, str]:
        strength_s = f"{float(v['strength']):.3f}"
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

            "VERT_UNIT_DOLLARS": str(CS_UNIT_DOLLARS),
            "VERT_OC":           str(oc_val),
            "VERT_UNITS":        str(units),

            "VERT_VOL_FIELD":    CS_VOL_FIELD,
            "VERT_VOL_USED":     field_used,
            "VERT_VOL_VALUE":    "" if vol_val is None else str(vol_val),
            "VERT_VOL_BUCKET":   str(bucket),
            "VERT_VOL_MULT":     str(vix_mult),
            "VERT_QTY_RULE":     qty_rule,

            # needed by TT placer
            "TT_TOKEN_JSON":      os.environ["TT_TOKEN_JSON"],
            "TT_ACCOUNT_NUMBER":  acct_num,
            "TT_BASE_URL":        os.environ.get("TT_BASE_URL", "https://api.tastyworks.com"),
            "TT_CLIENT_AUTH":     os.environ.get("TT_CLIENT_AUTH", ""),
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
                f"{_TAG} {mode}: "
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

            rc = subprocess.call([sys.executable, PLACER_SCRIPT], env=env)
            if rc != 0:
                _emit("error", message=f"placer rc={rc}", stage=f"placement_{mode}")
                print(f"{_TAG} PAIR_ALT: placer rc={rc}")
            else:
                _read_back_fills(_saved_groups, _rows_before)
            _close_events()
            return 0

        print(f"{_TAG} PAIR_ALT SKIP: qty missing (put={q_put} call={q_call}) — placing separately")

    # ----- Fallback: place separately -----
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
            f"{_TAG} {v['name']}: side={v['side']} kind={v['kind']} "
            f"short={v['short_osi']} long={v['long_osi']} "
            f"go={v['go']} target_qty={v['target_qty']} send_qty={v['send_qty']} "
            f"(units={units} vix_mult={vix_mult} bucket={bucket})"
        )
        _rows_before = _csv_row_count()
        env = env_for_vertical(v)
        rc = subprocess.call([sys.executable, PLACER_SCRIPT], env=env)
        if rc != 0:
            _emit("error", message=f"placer rc={rc}", stage=f"placement_{v['name']}")
            print(f"{_TAG} {v['name']}: placer rc={rc}")
        else:
            _read_back_fills([(v["name"], _saved_gid)], _rows_before)

    _close_events()
    return 0


if __name__ == "__main__":
    sys.exit(main())
