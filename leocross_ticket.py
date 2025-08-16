#!/usr/bin/env python3
import os, json, math, datetime as dt, requests
from dataclasses import dataclass
from typing import Dict

# ========= CONFIG =========
GW_AUTH_URL   = "https://gandalf.gammawizard.com/goauth/authenticateFireUser"
LEOCROSS_URL  = os.getenv("LEOCROSS_URL", "https://gandalf.gammawizard.com/rapi/GetLeoCross")
GW_EMAIL      = os.getenv("GW_EMAIL", "gouser@go.com")    # or your own login
GW_PASSWORD   = os.getenv("GW_PASSWORD")                  # Leo's daily password
TOKEN_FILE    = os.getenv("GW_TOKEN_FILE", "gw_token.json")

WIDE          = 5                 # $5 wide wings
QTY_SHORT     = 10                # 10 contracts when SHORT IC
QTY_LONG      = 3                 # 3 contracts when LONG IC
RISK_CAP      = 5000              # optional guardrail

# ========= MODELS =========
@dataclass
class Signal:
    date:   dt.date
    limit:  int          # inner put short strike
    climit: int          # inner call short strike
    cat1:   float        # P(outside) → LONG IC win prob (per your definition)
    cat2:   float        # P(inside)  → SHORT IC win prob
    tdate:  dt.date      # next expiry if provided; else next business day

# ========= HELPERS =========
def _load_token():
    if not os.path.exists(TOKEN_FILE): return None
    try:
        obj = json.load(open(TOKEN_FILE))
        # token is valid for 7 calendar days per Leo; refresh after 6
        iat = dt.datetime.fromisoformat(obj["iat"])
        if (dt.datetime.utcnow() - iat).days < 6:
            return obj["token"]
    except Exception:
        pass
    return None

def _save_token(tok: str):
    with open(TOKEN_FILE, "w") as f:
        json.dump({"token": tok, "iat": dt.datetime.utcnow().isoformat()}, f)

def gw_token() -> str:
    tok = _load_token()
    if tok: return tok
    r = requests.post(GW_AUTH_URL, data={"email": GW_EMAIL, "password": GW_PASSWORD}, timeout=10)
    r.raise_for_status()
    tok = r.json()["token"]
    _save_token(tok)
    return tok

def next_business_day(d: dt.date) -> dt.date:
    # weekend-only skip; holiday logic can be added later
    n = d + dt.timedelta(days=1)
    while n.weekday() >= 5:  # 5=Sat, 6=Sun
        n += dt.timedelta(days=1)
    return n

def fetch_leocross(tok: str) -> Signal:
    r = requests.get(LEOCROSS_URL, headers={"Authorization": f"Bearer {tok}"}, timeout=10)
    r.raise_for_status()
    js = r.json()

    # Accept either a single object or an array; pick the most recent
    row = js[0] if isinstance(js, list) else js

    # Common field names used in your CSV/screenshot
    date   = dt.date.fromisoformat(row.get("Date") or row.get("date"))
    limit  = int(round(float(row.get("Limit")  or row.get("limit"))))
    climit = int(round(float(row.get("CLimit") or row.get("climit"))))
    cat1   = float(row.get("Cat1") or row.get("cat1"))
    cat2   = float(row.get("Cat2") or row.get("cat2"))
    tdates = row.get("TDate") or row.get("tdate")
    tdate  = dt.date.fromisoformat(tdates) if tdates else next_business_day(date)

    return Signal(date, limit, climit, cat1, cat2, tdate)

def occ_symbol_spxw(expiry: dt.date, right: str, strike: int) -> str:
    # OCC: SPXW_YYMMDD{P/C}{strike * 100}
    yymmdd = expiry.strftime("%y%m%d")
    return f"SPXW_{yymmdd}{right}{strike:05d}"

def build_ticket(sig: Signal) -> Dict:
    # Decision: SHORT if Cat2>Cat1 else LONG (your rule)
    side = "SHORT" if sig.cat2 > sig.cat1 else "LONG"
    qty  = QTY_SHORT if side == "SHORT" else QTY_LONG

    # 5-wide: inner strikes are provided (Limit=put short, CLimit=call short)
    sp = sig.limit               # short put
    lp = sp - WIDE               # long put
    sc = sig.climit              # short call
    lc = sc + WIDE               # long call

    # Worst-case dollar risk per condor under your payoff mapping
    worst_per = 300 if side == "SHORT" else 200
    worst_day = qty * worst_per
    if worst_day > RISK_CAP:
        raise RuntimeError(f"Worst-case ${worst_day:,} exceeds cap ${RISK_CAP:,} (side={side}, qty={qty})")

    legs = {
        # instruction reflects net position (no broker schema here yet)
        "puts":  [{"instr": "BUY" if side == "SHORT" else "SELL", "right":"P","strike": lp, "symbol": occ_symbol_spxw(sig.tdate, "P", lp), "qty": qty},
                  {"instr": "SELL" if side == "SHORT" else "BUY",  "right":"P","strike": sp, "symbol": occ_symbol_spxw(sig.tdate, "P", sp), "qty": qty}],
        "calls": [{"instr": "SELL" if side == "SHORT" else "BUY",  "right":"C","strike": sc, "symbol": occ_symbol_spxw(sig.tdate, "C", sc), "qty": qty},
                  {"instr": "BUY" if side == "SHORT" else "SELL", "right":"C","strike": lc, "symbol": occ_symbol_spxw(sig.tdate, "C", lc), "qty": qty}]
    }

    ticket = {
        "date":      sig.date.isoformat(),
        "expiry":    sig.tdate.isoformat(),
        "side":      f"{side}_IRON_CONDOR",
        "qty":       qty,
        "inner_put": sp,
        "inner_call":sc,
        "width":     WIDE,
        "worst_case_loss": worst_day,
        "legs":      legs,
        "notes":     "Enter near close; hold to expiry. Pricing is broker-specific and added later."
    }
    return ticket

def main():
    tok = gw_token()
    sig = fetch_leocross(tok)
    ticket = build_ticket(sig)
    print(json.dumps(ticket, indent=2))

if __name__ == "__main__":
    main()
