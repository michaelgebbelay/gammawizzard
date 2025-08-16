#!/usr/bin/env python3
import os, json, datetime as dt, sys, requests
from dataclasses import dataclass

# ---- config ----
LEOCROSS_URL = "https://gandalf.gammawizard.com/rapi/GetLeoCross"
WIDE = 5
QTY_SHORT = 10   # when Cat2 > Cat1 (SHORT IC)
QTY_LONG  = 3    # when Cat1 > Cat2 (LONG  IC)

@dataclass
class Signal:
    date: dt.date
    tdate: dt.date
    limit: int
    climit: int
    cat1: float
    cat2: float

def _parse_date(s: str) -> dt.date:
    return dt.date.fromisoformat(s)

def get_token() -> str:
    tok = os.getenv("GW_TOKEN")
    if not tok:
        sys.exit("GW_TOKEN is not set. In GitHub Actions, add it as a secret; locally: export GW_TOKEN='...'.")
    return tok

def fetch_leocross(token: str) -> Signal:
    r = requests.get(LEOCROSS_URL, headers={"Authorization": f"Bearer {token}"}, timeout=10)
    r.raise_for_status()
    js = r.json()
    # API shape: {"Trade":[{...}], "Predictions":[...]}
    row = js["Trade"][0] if isinstance(js, dict) and "Trade" in js else (js[0] if isinstance(js, list) else js)

    date   = _parse_date(row["Date"])
    tdate  = _parse_date(row.get("TDate") or row["Date"])
    limit  = int(round(float(row.get("Limit") or row.get("PLimit"))))
    climit = int(round(float(row["CLimit"])))
    cat1   = float(row["Cat1"])   # prob(outside) → long IC win prob
    cat2   = float(row["Cat2"])   # prob(inside)  → short IC win prob
    return Signal(date, tdate, limit, climit, cat1, cat2)

def occ_symbol_spxw(expiry: dt.date, right: str, strike: int) -> str:
    yymmdd = expiry.strftime("%y%m%d")
    strike_code = f"{int(strike*1000):08d}"  # 6430 -> 06430000
    return f"SPXW_{yymmdd}{right}{strike_code}"

def build_ticket(sig: Signal):
    side = "SHORT" if sig.cat2 > sig.cat1 else "LONG"
    qty  = QTY_SHORT if side == "SHORT" else QTY_LONG

    sp = sig.limit
    lp = sp - WIDE
    sc = sig.climit
    lc = sc + WIDE

    worst_per = 300 if side == "SHORT" else 200
    worst_day = qty * worst_per

    legs = [
        {"instr": "BUY" if side=="SHORT" else "SELL", "symbol": occ_symbol_spxw(sig.tdate, "P", lp), "strike": lp},
        {"instr": "SELL" if side=="SHORT" else "BUY",  "symbol": occ_symbol_spxw(sig.tdate, "P", sp), "strike": sp},
        {"instr": "SELL" if side=="SHORT" else "BUY",  "symbol": occ_symbol_spxw(sig.tdate, "C", sc), "strike": sc},
        {"instr": "BUY" if side=="SHORT" else "SELL",  "symbol": occ_symbol_spxw(sig.tdate, "C", lc), "strike": lc},
    ]
    return {
        "signal_date": sig.date.isoformat(),
        "expiry":      sig.tdate.isoformat(),
        "side":        f"{side}_IRON_CONDOR",
        "qty":         qty,
        "width":       WIDE,
        "inner_put":   sp,
        "inner_call":  sc,
        "probs":       {"Cat1": sig.cat1, "Cat2": sig.cat2},
        "worst_case":  {"per_condor": worst_per, "day": worst_day},
        "legs":        legs,
        "enter_rule":  "enter near the close; hold to expiry (no management)"
    }

def main():
    token = get_token()
    sig = fetch_leocross(token)
    t = build_ticket(sig)

    print(f"{t['signal_date']} → {t['expiry']} : {t['side']} qty={t['qty']} width={t['width']}")
    print(f" Strikes  P {t['legs'][0]['strike']}/{t['legs'][1]['strike']}  "
          f"C {t['legs'][2]['strike']}/{t['legs'][3]['strike']}")
    print(f" Probs    Cat1={t['probs']['Cat1']:.3f}  Cat2={t['probs']['Cat2']:.3f}")
    print(f" Worst‑case day loss: ${t['worst_case']['day']:,}")
    print(" OCC legs:")
    for leg in t["legs"]:
        print(f"  {leg['instr']:4s} {leg['symbol']}")
    print("\nJSON ticket ↓\n" + json.dumps(t, indent=2))

if __name__ == "__main__":
    main()
