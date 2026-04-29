#!/usr/bin/env python3
"""LeoProfit orchestrator — TastyTrade execution.

Strategy logic ported from scripts/trade/leoprofit/leoprofit_orchestrator.py
(Schwab-based original). Execution layer follows the patterns in
TT/Script/ConstantStable/orchestrator.py (TT API + tt_client).

Decision flow:
  1. Fetch GW LeoProfit signal at /rapi/GetLeoProfit
  2. Cat1 vs Cat2 -> CREDIT (asymmetric IC) or DEBIT (symmetric 5-wide IC)
  3. Build the 4 OSI legs around inner put/call strikes from the signal
  4. Sizing: fixed qty (default 2) — same as original; not balance-driven
  5. Subprocess into place.py with leg + sizing env vars
"""

import os
import sys
import re
import math
import subprocess
import time
from datetime import date

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


# ---------------- helpers ----------------

def yymmdd(iso: str) -> str:
    d = date.fromisoformat((iso or "")[:10])
    return f"{d:%y%m%d}"


def to_osi(sym: str) -> str:
    raw = (sym or "").strip().upper().lstrip(".").replace("_", "")
    m = re.match(r"^([A-Z.$^]{1,6})(\d{6})([CP])(\d{1,5})(?:\.(\d{1,3}))?$", raw) or \
        re.match(r"^([A-Z.$^]{1,6})(\d{6})([CP])(\d{8})$", raw)
    if not m:
        raise ValueError(f"Cannot parse option symbol: {sym}")
    root, ymd, cp, strike, frac = (m.groups() + ("",))[:5]
    if len(strike) < 8:
        mills = int(strike) * 1000 + (int((frac or "0").ljust(3, "0")) if frac else 0)
    else:
        mills = int(strike)
    return f"{root:<6}{ymd}{cp}{mills:08d}"


def strike_from_osi(osi: str) -> float:
    return int(osi[-8:]) / 1000.0


def orient_credit(bp, sp, sc, bc):
    bpS = strike_from_osi(bp); spS = strike_from_osi(sp)
    scS = strike_from_osi(sc); bcS = strike_from_osi(bc)
    if bpS > spS:
        bp, sp = sp, bp
    if scS > bcS:
        sc, bc = bc, sc
    return [bp, sp, sc, bc]


def build_legs_credit(exp6: str, inner_put: int, inner_call: int, Wp: int, Wc: int):
    p_low, p_high = inner_put - Wp, inner_put
    c_low, c_high = inner_call, inner_call + Wc
    return orient_credit(
        to_osi(f".SPXW{exp6}P{p_low}"),
        to_osi(f".SPXW{exp6}P{p_high}"),
        to_osi(f".SPXW{exp6}C{c_low}"),
        to_osi(f".SPXW{exp6}C{c_high}"),
    )


def build_legs_debit(exp6: str, inner_put: int, inner_call: int, W: int):
    """Long IC (DEBIT) with 5-wide legs, oriented for DEBIT."""
    p_low, p_high = inner_put - W, inner_put
    c_low, c_high = inner_call, inner_call + W
    bp = to_osi(f".SPXW{exp6}P{p_high}")  # buy higher put (long)
    sp = to_osi(f".SPXW{exp6}P{p_low}")   # sell lower put (short)
    sc = to_osi(f".SPXW{exp6}C{c_high}")  # sell higher call (short)
    bc = to_osi(f".SPXW{exp6}C{c_low}")   # buy lower call (long)
    return [bp, sp, sc, bc]


def floor5(x: float) -> int:
    return int(math.floor(float(x) / 5.0) * 5)


# ---------------- GammaWizard signal ----------------

def _sanitize_token(t: str) -> str:
    t = (t or "").strip().strip('"').strip("'")
    return t.split(None, 1)[1] if t.lower().startswith("bearer ") else t


def gw_fetch():
    base = os.environ.get("GW_BASE", "https://gandalf.gammawizard.com").rstrip("/")
    endpoint = os.environ.get("LEO_GW_ENDPOINT", "/rapi/GetLeoProfit")
    tok = _sanitize_token(os.environ.get("GW_TOKEN", "") or "")

    def hit(t):
        h = {"Accept": "application/json"}
        if t:
            h["Authorization"] = f"Bearer {_sanitize_token(t)}"
        return requests.get(f"{base}/{endpoint.lstrip('/')}", headers=h, timeout=30)

    r = hit(tok) if tok else None
    if (r is None) or (r.status_code in (401, 403)):
        email = os.environ.get("GW_EMAIL", "")
        pwd = os.environ.get("GW_PASSWORD", "")
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
            if isinstance(tr, list) and tr:
                return tr[-1]
            if isinstance(tr, dict):
                return tr
            return {}
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


# ---------------- TT account ----------------

def tt_account_number() -> str:
    acct = (os.environ.get("TT_ACCOUNT_NUMBER") or "").strip()
    if not acct:
        raise RuntimeError("TT_ACCOUNT_NUMBER missing")
    return acct


def opening_cash_for_account():
    """Mirrors the TT CS orchestrator helper. Returns (value, src, acct)."""
    acct_num = tt_account_number()
    j = tt_request("GET", f"/accounts/{acct_num}/balances").json()
    src = (j.get("data") if isinstance(j, dict) else {}) or {}
    keys = [
        "net-liquidating-value",
        "cash-balance",
        "cash-available-to-withdraw",
        "equity-buying-power",
        "derivative-buying-power",
    ]
    for k in keys:
        v = src.get(k)
        try:
            fv = float(v)
            if fv > 0:
                return fv, k, acct_num
        except Exception:
            continue
    return None, "none", acct_num


# ---------------- Wait for GW signal to be ready ----------------

def _wait_for_gw_ready():
    """Wait until the GW endpoint reports today's signal (TDate matches today).
    Mirrors the CS orchestrator gate logic but with shorter retry window."""
    ready_et = (os.environ.get("CS_GW_READY_ET") or "16:13:31").strip()
    try:
        ready_h, ready_m, ready_s = [int(x) for x in ready_et.split(":")]
    except Exception:
        ready_h, ready_m, ready_s = 16, 13, 31

    from datetime import datetime
    from zoneinfo import ZoneInfo
    ET = ZoneInfo("America/New_York")
    now = datetime.now(tz=ET)
    target = now.replace(hour=ready_h, minute=ready_m, second=ready_s, microsecond=0)
    if target > now:
        wait_s = (target - now).total_seconds()
        print(f"LEO ORCH: waiting {wait_s:.1f}s for GW ready ({ready_et} ET)")
        time.sleep(min(wait_s, 60.0))


# ---------------- main ----------------

def main():
    dry_run = (os.environ.get("VERT_DRY_RUN", "false") or "false").lower() in ("1", "true", "yes")

    _wait_for_gw_ready()

    # ---- 1) Signal ----
    try:
        tr = extract_trade(gw_fetch())
    except Exception as e:
        print(f"LEO ORCH FATAL: GW fetch failed: {e}")
        return 1

    if not tr:
        print("LEO ORCH FATAL: empty Trade payload")
        return 1

    try:
        exp6 = yymmdd(str(tr.get("TDate", "")))
        inner_put = int(float(tr.get("Limit")))
        inner_call = int(float(tr.get("CLimit")))
    except Exception as e:
        print(f"LEO ORCH FATAL: bad signal fields ({e}): {tr}")
        return 1

    def fnum(x):
        try:
            return float(x)
        except Exception:
            return None

    cat1 = fnum(tr.get("Cat1"))
    cat2 = fnum(tr.get("Cat2"))
    is_credit = True if (cat2 is None or cat1 is None or cat2 >= cat1) else False

    side_override = (os.environ.get("LEO_SIDE_OVERRIDE", "AUTO") or "AUTO").upper()
    if side_override == "CREDIT":
        is_credit = True
    elif side_override == "DEBIT":
        is_credit = False

    # ---- 2) Width / structure ----
    Wp_env = (os.environ.get("LEO_CREDIT_PUT_WIDTH", "") or "").strip()
    Wc_env = (os.environ.get("LEO_CREDIT_CALL_WIDTH", "") or "").strip()
    m_env = (os.environ.get("LEO_CALL_MULT", "") or "").strip()

    if is_credit:
        Wp = int(Wp_env) if Wp_env.isdigit() else int(os.environ.get("LEO_CREDIT_SPREAD_WIDTH", "20"))
        Wp = max(5, int(math.ceil(Wp / 5.0) * 5))
        Wc = int(Wc_env) if Wc_env.isdigit() else max(5, floor5(Wp / 2.0))
        m = int(m_env) if (m_env.isdigit() and int(m_env) >= 1) else 2
        legs = build_legs_credit(exp6, inner_put, inner_call, Wp, Wc)
    else:
        Wp, Wc, m = 5, 5, 1
        legs = build_legs_debit(exp6, inner_put, inner_call, 5)

    # ---- 3) Equity (informational; sizing is fixed-qty by default) ----
    try:
        oc_val, oc_src, acct_num = opening_cash_for_account()
    except Exception as e:
        print(f"LEO ORCH WARN: TT balance lookup failed: {e}")
        oc_val, oc_src, acct_num = None, "none", tt_account_number()

    # ---- 4) Quantity ----
    fixed_qty_raw = (os.environ.get("LEO_FIXED_QTY", "2") or "").strip()
    try:
        qty = max(1, int(fixed_qty_raw))
    except Exception:
        qty = 2

    qov = (os.environ.get("LEO_BYPASS_QTY", "") or "").strip()
    if qov:
        try:
            qty = max(1, int(qov))
        except Exception:
            pass

    side_txt = "CREDIT" if is_credit else "DEBIT"
    struct = "CONDOR_RATIO" if (is_credit and m > 1) else "CONDOR"

    print(f"LEO ORCH SIGNAL: TDate={tr.get('TDate')} inner_put={inner_put} inner_call={inner_call} "
          f"Cat1={cat1} Cat2={cat2}")
    print(f"LEO ORCH CONFIG: side={side_txt} Wp={Wp} Wc={Wc} call_mult={m} structure={struct}")
    print(f"LEO ORCH SIZE: qty={qty} oc={oc_val if oc_val is not None else 'NA'} "
          f"src={oc_src} acct={acct_num} dry_run={dry_run}")
    print(f"LEO ORCH LEGS: bp={legs[0]} sp={legs[1]} sc={legs[2]} bc={legs[3]}")

    # ---- 5) Hand off to placer ----
    env = dict(os.environ)
    env.update({
        "LEO_SIDE": side_txt,
        "LEO_STRUCTURE": struct,
        "LEO_QTY": str(qty),
        "LEO_PUT_WIDTH": str(Wp),
        "LEO_CALL_WIDTH": str(Wc),
        "LEO_CALL_MULT": str(m),
        "LEO_OCC_BUY_PUT": legs[0],
        "LEO_OCC_SELL_PUT": legs[1],
        "LEO_OCC_SELL_CALL": legs[2],
        "LEO_OCC_BUY_CALL": legs[3],
        "LEO_DRY_RUN": "true" if dry_run else "false",
    })

    placer = os.path.join(os.path.dirname(__file__), "place.py")
    rc = subprocess.call([sys.executable, placer], env=env)
    return rc


if __name__ == "__main__":
    sys.exit(main())
