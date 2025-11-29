#!/usr/bin/env python3
# CONSTANT STABLE — vertical orchestrator
#  - Uses GammaWizard ConstantStable payload (default: GetLeoCross)
#  - Builds 4 SPX 5‑wide verticals (PUT_SHORT, PUT_LONG, CALL_SHORT, CALL_LONG)
#  - Sizing: units = floor(equity / 10_000), 1x weak vs 2x strong per leg
#  - Delegates to place.py for NBBO + pricing (0.95 / 1.00 / 1.05) + logging

import os, sys, re, subprocess, math
from datetime import date
from zoneinfo import ZoneInfo
import requests
from schwab.auth import client_from_token_file

__version__ = "1.0.0"

ET = ZoneInfo("America/New_York")

GW_BASE = os.environ.get("GW_BASE", "https://gandalf.gammawizard.com").rstrip("/")
# Default ConstantStable endpoint; override via GW_ENDPOINT if needed
GW_ENDPOINT = os.environ.get("GW_ENDPOINT", "/rapi/GetLeoCross").lstrip("/")

UNIT_DOLLARS     = float(os.environ.get("CS_UNIT_DOLLARS", "10000"))     # 10k per unit
STRONG_THRESHOLD = float(os.environ.get("CS_STRONG_THRESHOLD", "0.66"))  # |Go| ≥ this → strong leg
LOG_PATH         = os.environ.get("CS_LOG_PATH", "logs/constantstable_vertical_trades.csv")

# --------- basic utils ----------

def yymmdd(iso: str) -> str:
    d = date.fromisoformat((iso or "")[:10])
    return f"{d:%y%m%d}"

def to_osi(sym: str) -> str:
    raw = (sym or "").strip().upper().lstrip(".").replace("_", "")
    m = re.match(r'^([A-Z.$^]{1,6})(\d{6})([CP])(\d{1,5})(?:\.(\d{1,3}))?$', raw) or \
        re.match(r'^([A-Z.$^]{1,6})([CP])(\d{8})$', raw)
    if not m:
        raise ValueError(f"Cannot parse option symbol: {sym}")
    if len(m.groups()) == 4:
        root, ymd, cp, strike = (m.groups()+("",))[:4]
        mills = int(strike) * 1000
    else:
        root, cp, mills = (m.groups()+("",))[:3]
        ymd = ""  # not used here
    root = root[:6]
    return f"{root:<6}{ymd}{cp}{int(mills):08d}"

def fnum(x):
    try:
        return float(x)
    except Exception:
        return None

# --------- GammaWizard (ConstantStable) ----------

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
        rr = requests.post(f"{base}/goauth/authenticateFireUser",
                           data={"email": email, "password": pwd}, timeout=30)
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

# --------- Schwab helpers ----------

def schwab_client():
    app_key    = os.environ["SCHWAB_APP_KEY"]
    app_secret = os.environ["SCHWAB_APP_SECRET"]
    token_json = os.environ["SCHWAB_TOKEN_JSON"]
    with open("schwab_token.json", "w") as f:
        f.write(token_json)
    return client_from_token_file(api_key=app_key,
                                  app_secret=app_secret,
                                  token_path="schwab_token.json")

def opening_cash_for_account(c):
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

    def pick(d, *ks):
        for k in ks:
            v = (d or {}).get(k)
            if isinstance(v, (int, float)):
                return float(v)

    chosen = None
    acct_num = ""
    for a in arr:
        aid, init, curr = hunt(a)
        if chosen is None:
            chosen = (init, curr)
            acct_num = aid

    if not chosen:
        return None, ""
    init, curr = chosen
    oc = pick(init, "cashAvailableForTrading", "cashBalance", "liquidationValue")
    if oc is None:
        oc = pick(curr, "cashAvailableForTrading", "cashBalance", "liquidationValue")
    return oc, acct_num

def get_account_hash(c):
    r = c.get_account_numbers()
    r.raise_for_status()
    j = r.json() or []
    if not j:
        return ""
    return str(j[0]["hashValue"])

# --------- vertical building ----------

def build_verticals(tr: dict):
    """
    Build 4 verticals (5‑wide) from ConstantStable payload:
      PUT_SHORT, PUT_LONG, CALL_SHORT, CALL_LONG
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

    # OSI symbols
    put_low_osi   = to_osi(f".SPXW{exp6}P{p_low}")
    put_high_osi  = to_osi(f".SPXW{exp6}P{p_high}")
    call_low_osi  = to_osi(f".SPXW{exp6}C{c_low}")
    call_high_osi = to_osi(f".SPXW{exp6}C{c_high}")

    left_go  = fnum(tr.get("LeftGo"))
    right_go = fnum(tr.get("RightGo"))
    lg_abs = abs(left_go)  if left_go  is not None else 0.0
    rg_abs = abs(right_go) if right_go is not None else 0.0

    return [
        # Short put vertical (credit)
        {
            "name": "PUT_SHORT",
            "kind": "PUT",
            "side": "CREDIT",
            "direction": "SHORT",
            "short_osi": put_high_osi,   # short nearer strike
            "long_osi":  put_low_osi,    # long further OTM
            "go": left_go,
            "strength": lg_abs,
            "width": W,
        },
        # Long put vertical (debit)
        {
            "name": "PUT_LONG",
            "kind": "PUT",
            "side": "DEBIT",
            "direction": "LONG",
            "short_osi": put_low_osi,    # short further OTM
            "long_osi":  put_high_osi,   # long nearer
            "go": left_go,
            "strength": lg_abs,
            "width": W,
        },
        # Short call vertical (credit)
        {
            "name": "CALL_SHORT",
            "kind": "CALL",
            "side": "CREDIT",
            "direction": "SHORT",
            "short_osi": call_high_osi,  # short higher strike
            "long_osi":  call_low_osi,   # long lower
            "go": right_go,
            "strength": rg_abs,
            "width": W,
        },
        # Long call vertical (debit)
        {
            "name": "CALL_LONG",
            "kind": "CALL",
            "side": "DEBIT",
            "direction": "LONG",
            "short_osi": call_low_osi,   # short lower
            "long_osi":  call_high_osi,  # long higher
            "go": right_go,
            "strength": rg_abs,
            "width": W,
        },
    ]

def ensure_log_dir():
    d = os.path.dirname(LOG_PATH)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

# --------- main ----------

def main():
    # Schwab client + equity
    try:
        c = schwab_client()
        oc, acct_num = opening_cash_for_account(c)
        acct_hash = get_account_hash(c)
    except Exception as e:
        print(f"CS_VERT_RUN SKIP: Schwab init failed: {e}")
        return 0

    if oc is None or oc <= 0:
        print("CS_VERT_RUN SKIP: opening cash unavailable or <=0")
        return 0

    if UNIT_DOLLARS <= 0:
        print("CS_VERT_RUN FATAL: CS_UNIT_DOLLARS must be > 0")
        return 1

    units = int(oc // UNIT_DOLLARS)
    if units <= 0:
        print(f"CS_VERT_RUN SKIP: oc={oc:.2f} < UNIT_DOLLARS={UNIT_DOLLARS:.2f}")
        return 0

    # GammaWizard ConstantStable payload
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

    print(f"CS_VERT_RUN: oc={oc:.2f} units={units} trade_date={trade_date} TDate={tdate_iso}")
    print(f"CS_VERT_RUN: STRONG_THRESHOLD={STRONG_THRESHOLD:.3f} UNIT_DOLLARS={UNIT_DOLLARS:.2f}")

    ensure_log_dir()

    # For each vertical, compute qty (1x weak, 2x strong) and delegate to place.py
    for v in verts:
        strength = v["strength"]
        is_strong = (strength is not None) and (strength >= STRONG_THRESHOLD)
        mult = 2 if is_strong else 1
        qty = units * mult

        if qty <= 0:
            print(f"CS_VERT_RUN {v['name']}: qty=0 — skip")
            continue

        strength_str = f"{strength:.3f}" if strength is not None else "0.000"
        print(
            f"CS_VERT_RUN {v['name']}: side={v['side']} kind={v['kind']} "
            f"short={v['short_osi']} long={v['long_osi']} "
            f"go={v['go']} strength={strength_str} "
            f"is_strong={is_strong} qty={qty}"
        )

        env = dict(os.environ)
        env.update({
            "VERT_SIDE":       v["side"],
            "VERT_KIND":       v["kind"],
            "VERT_NAME":       v["name"],
            "VERT_DIRECTION":  v["direction"],
            "VERT_SHORT_OSI":  v["short_osi"],
            "VERT_LONG_OSI":   v["long_osi"],
            "VERT_QTY":        str(qty),
            "VERT_GO":         "" if v["go"] is None else str(v["go"]),
            "VERT_STRENGTH":   strength_str,
            "VERT_IS_STRONG":  "true" if is_strong else "false",
            "VERT_TRADE_DATE": trade_date,
            "VERT_TDATE":      tdate_iso,
            "VERT_UNIT_DOLLARS": str(UNIT_DOLLARS),
            "VERT_OC":         str(oc),
            "VERT_UNITS":      str(units),
            "SCHWAB_APP_KEY":     os.environ["SCHWAB_APP_KEY"],
            "SCHWAB_APP_SECRET":  os.environ["SCHWAB_APP_SECRET"],
            "SCHWAB_TOKEN_JSON":  os.environ["SCHWAB_TOKEN_JSON"],
            "SCHWAB_ACCT_HASH":   acct_hash,
            "CS_LOG_PATH":        LOG_PATH,
        })

        rc = subprocess.call([sys.executable, "scripts/trade/ConstantStable/place.py"], env=env)
        if rc != 0:
            print(f"CS_VERT_RUN {v['name']}: placer rc={rc}")

    return 0

if __name__ == "__main__":
    sys.exit(main())
