#!/usr/bin/env python3
import os, json, datetime as dt, sys, requests
from dataclasses import dataclass

GW_EMAIL    = os.getenv("GW_EMAIL")
GW_PASSWORD = os.getenv("GW_PASSWORD")
GW_TOKEN_ENV= os.getenv("GW_TOKEN")

def gw_token() -> str:
    # 1) CI path: use the pre-issued JWT from env/secret
    if GW_TOKEN_ENV:
        return GW_TOKEN_ENV

    # 2) Local fallback: do the login flow if you provided creds
    if GW_EMAIL and GW_PASSWORD:
        r = requests.post(
            "https://gandalf.gammawizard.com/goauth/authenticateFireUser",
            data={"email": GW_EMAIL, "password": GW_PASSWORD},
            timeout=10
        )
        r.raise_for_status()
        tok = r.json().get("token")
        if not tok:
            raise SystemExit("Auth returned no token. Check GW_EMAIL/GW_PASSWORD.")
        return tok

    # 3) Otherwise, stop with a clear message
    raise SystemExit("No GW_TOKEN set. In GitHub, add it under Settings → Secrets → Actions → GW_TOKEN")

# sizing you asked for
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

def fetch_leocross(token: str) -> Signal:
    if not token:
        sys.exit("Set GW_TOKEN env var (your JWT).")

    r = requests.get(LEOCROSS_URL, headers={"Authorization": f"Bearer {token}"}, timeout=10)
    r.raise_for_status()
    js = r.json()

    # API returns {"Trade":[{...}], "Predictions":[...]} – we want the current Trade[0]
    row = js["Trade"][0] if isinstance(js, dict) and "Trade" in js else (js[0] if isinstance(js, list) else js)

    # field names observed in your response/screenshot
    date   = _parse_date(row["Date"])
    tdate  = _parse_date(row.get("TDate") or row["Date"])  # fallback if absent
    limit  = int(round(float(row.get("Limit") or row.get("PLimit"))))
    climit = int(round(float(row["CLimit"])))
    cat1   = float(row["Cat1"])   # probability of OUTSIDE (→ long IC win)
    cat2   = float(row["Cat2"])   # probability of INSIDE  (→ short IC win)

    return Signal(date, tdate, limit, climit, cat1, cat2)

def occ_symbol_spxw(expiry: dt.date, right: str, strike: int) -> str:
    # OCC format Schwab expects: SPXW_YYMMDD{P/C}{strike*1000 padded to 8}
    yymmdd = expiry.strftime("%y%m%d")
    strike_code = f"{int(strike*1000):08d}"
    return f"SPXW_{yymmdd}{right}{strike_code}"

def build_ticket(sig: Signal):
    side = "SHORT" if sig.cat2 > sig.cat1 else "LONG"
    qty  = QTY_SHORT if side == "SHORT" else QTY_LONG

    sp = sig.limit              # short put
    lp = sp - WIDE              # long put
    sc = sig.climit             # short call
    lc = sc + WIDE              # long call

    # your 5‑wide mapping: short risk ≈ $300, long risk ≈ $200 (per condor)
    worst_per = 300 if side == "SHORT" else 200
    worst_day = qty * worst_per

    legs = [
        {"instr": "BUY" if side=="SHORT" else "SELL", "symbol": occ_symbol_spxw(sig.tdate, "P", lp), "strike": lp},
        {"instr": "SELL" if side=="SHORT" else "BUY",  "symbol": occ_symbol_spxw(sig.tdate, "P", sp), "strike": sp},
        {"instr": "SELL" if side=="SHORT" else "BUY",  "symbol": occ_symbol_spxw(sig.tdate, "C", sc), "strike": sc},
        {"instr": "BUY" if side=="SHORT" else "SELL",  "symbol": occ_symbol_spxw(sig.tdate, "C", lc), "strike": lc},
    ]

    ticket = {
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
    return ticket

def main():
    sig = fetch_leocross(GW_TOKEN)
    t = build_ticket(sig)

    # concise human print + raw JSON (for the broker step next)
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
