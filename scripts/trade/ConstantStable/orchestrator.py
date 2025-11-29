#!/usr/bin/env python3
# CONSTANT STABLE — vertical orchestrator (10k sizing)
#  - Uses GammaWizard ConstantStable payload (GetLeoCross by default).
#  - Builds 4 SPX 5‑wide verticals:
#       PUT_SHORT, PUT_LONG, CALL_SHORT, CALL_LONG
#  - Sizing:
#       CS_UNIT_DOLLARS (default 10,000) → units = floor(equity / CS_UNIT_DOLLARS)
#       per‑leg qty = units * (1x weak, 2x strong) where |Go| >= CS_STRONG_THRESHOLD
#  - If equity is unavailable / <= 0 → fall back to units=1 (so it doesn’t perma‑skip).
#  - Delegates NBBO + ladder + logging to ConstantStable/place.py via VERT_* envs.

import os
import sys
import re
import subprocess
from datetime import date
import requests
from schwab.auth import client_from_token_file

# ---------- Config ----------
GW_BASE = os.environ.get("GW_BASE", "https://gandalf.gammawizard.com").rstrip("/")
# ConstantStable endpoint (override if you have a dedicated one)
GW_ENDPOINT = os.environ.get("GW_ENDPOINT", "/rapi/GetLeoCross").lstrip("/")

CS_UNIT_DOLLARS     = float(os.environ.get("CS_UNIT_DOLLARS", "10000"))   # 10k per "unit"
CS_STRONG_THRESHOLD = float(os.environ.get("CS_STRONG_THRESHOLD", "0.66"))
CS_LOG_PATH         = os.environ.get("CS_LOG_PATH", "logs/constantstable_vertical_trades.csv")

# ---------- Utility helpers ----------

def yymmdd(iso: str) -> str:
    d = date.fromisoformat((iso or "")[:10])
    return f"{d:%y%m%d}"

def to_osi(sym: str) -> str:
    """
    Normalize an option symbol into OCC OSI.
    Expects things like .SPXW250321P5000 etc.
    """
    raw = (sym or "").strip().upper().lstrip(".").replace("_", "")
    m = (
        re.match(r'^([A-Z.$^]{1,6})(\d{6})([CP])(\d{1,5})(?:\.(\d{1,3}))?$', raw)
        or re.match(r'^([A-Z.$^]{1,6})(\d{6})([CP])(\d{8})$', raw)
    )
    if not m:
        raise ValueError(f"Cannot parse option symbol: {sym}")
    root, ymd, cp, strike, frac = (m.groups() + ("",))[:5]
    if len(strike) < 8:
        mills = int(strike) * 1000 + (int((frac or "0").ljust(3, "0")) if frac else 0)
    else:
        mills = int(strike)
    return f"{root:<6}{ymd}{cp}{int(mills):08d}"

def fnum(x):
    try:
        return float(x)
    except Exception:
        return None

# ---------- GammaWizard (ConstantStable) ----------

def _sanitize_token(t: str) -> str:
    t = (t or "").strip().strip('"').strip("'")
    return t.split(None, 1)[1] if t.lower().startswith("bearer ") else t

def gw_fetch():
    base = GW_BASE
    endpoint = GW_ENDPOINT
    tok = _sanitize_token(os.environ.get("GW_TOKEN", "") or "")

    def hit(tkn):
        h = {"Accept": "application/json"}
        if tkn:
            h["Authorization"] = f"Bearer {_sanitize_token(tkn)}"
        return requests.get(f"{base}/{endpoint}", headers=h, timeout=30)

    r = hit(tok) if tok else None
    if (r is None) or (r.status_code in (401, 403)):
        email = os.environ.get("GW_EMAIL", "")
        pwd   = os.environ.get("GW_PASSWORD", "")
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
    """
    Walk arbitrary GW payloads and extract the innermost ConstantStable trade dict.
    """
    if isinstance(j, dict):
        if "Trade" in j:
            tr = j["Trade"]
            return tr[-1] if isinstance(tr, list) and tr else tr if isinstance(tr, dict) else {}
        keys = ("Date", "TDate", "Limit", "CLimit", "Cat1", "Cat2", "LeftGo", "RightGo")
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

def schwab_client():
    app_key    = os.environ["SCHWAB_APP_KEY"]
    app_secret = os.environ["SCHWAB_APP_SECRET"]
    token_json = os.environ["SCHWAB_TOKEN_JSON"]
    with open("schwab_token.json", "w") as f:
        f.write(token_json)
    # NOTE: the correct keyword is app_secret, NOT api_secret
    return client_from_token_file(api_key=app_key, app_secret=app_secret, token_path="schwab_token.json")

def opening_cash_for_account(c):
    """
    Try to find a reasonable "equity / cash available" number.

    Returns (value_or_None, acct_number_or_empty).
    """
    r = c.get_accounts()
    r.raise_for_status()
    data = r.json()
    arr = data if isinstance(data, list) else [data]

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

    def pick(src):
        keys = [
            "cashAvailableForTrading",
            "cashBalance",
            "availableFundsNonMarginableTrade",
            "buyingPowerNonMarginableTrade",
            "buyingPower",
            "optionBuyingPower",
            "liquidationValue",
        ]
        for k in keys:
            v = (src or {}).get(k)
            if isinstance(v, (int, float)):
                return float(v)
        return None

    oc_val = None
    acct_num = ""
    for a in arr:
        aid, init, curr = hunt(a)
        oc_candidate = pick(init)
        if oc_candidate is None:
            oc_candidate = pick(curr)
        if oc_candidate is not None:
            oc_val = oc_candidate
            acct_num = aid
            break
        if acct_num == "":
            acct_num = aid or ""

    return oc_val, acct_num

def get_account_hash(c):
    r = c.get_account_numbers()
    r.raise_for_status()
    arr = r.json() or []
    if not arr:
        return ""
    info = arr[0]
    return str(info.get("hashValue") or info.get("hashvalue") or "")

# ---------- Vertical building ----------

def build_verticals(tr: dict):
    """
    Build the 4 ConstantStable verticals, each 5‑wide around Limit / CLimit.

    Returns a list of dicts with:
      name, kind, side, direction, short_osi, long_osi, go, strength, width
    """
    exp_iso = str(tr.get("TDate", ""))
    exp6    = yymmdd(exp_iso)

    inner_put  = int(float(tr.get("Limit")))
    inner_call = int(float(tr.get("CLimit")))
    W = 5

    # Put strikes
    p_low  = inner_put - W
    p_high = inner_put

    # Call strikes
    c_low  = inner_call
    c_high = inner_call + W

    put_low_osi   = to_osi(f".SPXW{exp6}P{p_low}")
    put_high_osi  = to_osi(f".SPXW{exp6}P{p_high}")
    call_low_osi  = to_osi(f".SPXW{exp6}C{c_low}")
    call_high_osi = to_osi(f".SPXW{exp6}C{c_high}")

    left_go  = fnum(tr.get("LeftGo"))
    right_go = fnum(tr.get("RightGo"))
    lg_abs = abs(left_go)  if left_go  is not None else 0.0
    rg_abs = abs(right_go) if right_go is not None else 0.0

    return [
        {
            "name": "PUT_SHORT",
            "kind": "PUT",
            "side": "CREDIT",
            "direction": "SHORT",
            "short_osi": put_high_osi,
            "long_osi":  put_low_osi,
            "go": left_go,
            "strength": lg_abs,
            "width": W,
        },
        {
            "name": "PUT_LONG",
            "kind": "PUT",
            "side": "DEBIT",
            "direction": "LONG",
            "short_osi": put_low_osi,
            "long_osi":  put_high_osi,
            "go": left_go,
            "strength": lg_abs,
            "width": W,
        },
        {
            "name": "CALL_SHORT",
            "kind": "CALL",
            "side": "CREDIT",
            "direction": "SHORT",
            "short_osi": call_high_osi,
            "long_osi":  call_low_osi,
            "go": right_go,
            "strength": rg_abs,
            "width": W,
        },
        {
            "name": "CALL_LONG",
            "kind": "CALL",
            "side": "DEBIT",
            "direction": "LONG",
            "short_osi": call_low_osi,
            "long_osi":  call_high_osi,
            "go": right_go,
            "strength": rg_abs,
            "width": W,
        },
    ]

def ensure_log_dir():
    d = os.path.dirname(CS_LOG_PATH)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

# ---------- main ----------

def main():
    # --- Schwab + equity (with override & fallback) ---
    try:
        c = schwab_client()
        oc_val, acct_num = opening_cash_for_account(c)
        acct_hash = get_account_hash(c)
    except Exception as e:
        print(f"CS_VERT_RUN SKIP: Schwab init failed: {e}")
        return 0

    # Optional manual override for sizing (e.g. in CI / if Schwab fields are funky)
    ov_raw = (os.environ.get("SIZING_DOLLARS_OVERRIDE", "") or "").strip()
    if ov_raw:
        try:
            oc_val = float(ov_raw)
            print(f"CS_VERT_RUN INFO: using SIZING_DOLLARS_OVERRIDE={oc_val}")
        except Exception:
            print("CS_VERT_RUN WARN: bad SIZING_DOLLARS_OVERRIDE, ignoring override.")

    print(f"CS_VERT_RUN EQUITY_RAW: {oc_val} (acct={acct_num})")

    # Fallback: if equity missing /<=0, force 1 unit so we don't perma‑skip
    if oc_val is None or oc_val <= 0:
        print("CS_VERT_RUN WARN: equity unavailable/<=0 — defaulting to 1 unit for sizing")
        oc_val = 0.0
        units = 1
    else:
        if CS_UNIT_DOLLARS <= 0:
            print("CS_VERT_RUN FATAL: CS_UNIT_DOLLARS must be > 0")
            return 1
        units = max(1, int(oc_val // CS_UNIT_DOLLARS))

    print(f"CS_VERT_RUN UNITS: {units} (CS_UNIT_DOLLARS={CS_UNIT_DOLLARS}, oc_val={oc_val})")

    # --- ConstantStable payload from GammaWizard ---
    try:
        api = gw_fetch()
        tr  = extract_trade(api)
    except Exception as e:
        print(f"CS_VERT_RUN SKIP: GW fetch failed: {e}")
        return 0

    if not tr:
        print("CS_VERT_RUN SKIP: NO_TRADE_PAYLOAD")
        return 0

    trade_date = str(tr.get("Date", ""))
    tdate_iso  = str(tr.get("TDate", ""))

    verts = build_verticals(tr)

    print(f"CS_VERT_RUN TRADE: Date={trade_date} TDate={tdate_iso}")
    print(f"CS_VERT_RUN STRONG_THRESHOLD={CS_STRONG_THRESHOLD:.3f} LOG_PATH={CS_LOG_PATH}")

    ensure_log_dir()

    # --- Per‑vertical sizing and spawn placer ---
    for v in verts:
        strength   = v["strength"]
        is_strong  = (strength is not None) and (strength >= CS_STRONG_THRESHOLD)
        mult       = 2 if is_strong else 1
        qty        = units * mult
        strength_s = f"{strength:.3f}" if strength is not None else "0.000"

        if qty <= 0:
            print(f"CS_VERT_RUN {v['name']}: qty=0 — skip")
            continue

        print(
            f"CS_VERT_RUN {v['name']}: side={v['side']} kind={v['kind']} "
            f"short={v['short_osi']} long={v['long_osi']} "
            f"go={v['go']} strength={strength_s} is_strong={is_strong} qty={qty}"
        )

        env = dict(os.environ)
        env.update({
            "VERT_SIDE":        v["side"],
            "VERT_KIND":        v["kind"],
            "VERT_NAME":        v["name"],
            "VERT_DIRECTION":   v["direction"],
            "VERT_SHORT_OSI":   v["short_osi"],
            "VERT_LONG_OSI":    v["long_osi"],
            "VERT_QTY":         str(qty),
            "VERT_GO":          "" if v["go"] is None else str(v["go"]),
            "VERT_STRENGTH":    strength_s,
            "VERT_IS_STRONG":   "true" if is_strong else "false",
            "VERT_TRADE_DATE":  trade_date,
            "VERT_TDATE":       tdate_iso,
            "VERT_UNIT_DOLLARS": str(CS_UNIT_DOLLARS),
            "VERT_OC":          str(oc_val),
            "VERT_UNITS":       str(units),
            "SCHWAB_APP_KEY":     os.environ["SCHWAB_APP_KEY"],
            "SCHWAB_APP_SECRET":  os.environ["SCHWAB_APP_SECRET"],
            "SCHWAB_TOKEN_JSON":  os.environ["SCHWAB_TOKEN_JSON"],
            "SCHWAB_ACCT_HASH":   acct_hash,
            "CS_LOG_PATH":        CS_LOG_PATH,
        })

        rc = subprocess.call(
            [sys.executable, "scripts/trade/ConstantStable/place.py"],
            env=env,
        )
        if rc != 0:
            print(f"CS_VERT_RUN {v['name']}: placer rc={rc}")

    return 0

if __name__ == "__main__":
    sys.exit(main())
