#!/usr/bin/env python3
"""
Schwab SPX-only data test for a specific trading date (ET):
- Auth
- Quotes ($SPX.X → SPX)
- Intraday price history (SPX only: $SPX.X, SPX.X, SPX; 1m then 5m; 09:30–16:00 ET; both path and query styles)
- SPXW 0DTE option quotes via OCC symbols (no /chains)

Run:
  FOR_DATE=2025-11-07 python scripts/trade/accelerator/test_schwab_spx_only.py

Env:
  SCHWAB_APP_KEY, SCHWAB_APP_SECRET, SCHWAB_TOKEN_JSON
"""
from __future__ import annotations
import os, sys, json, time, argparse, datetime as dt
import pandas as pd
import pytz
from schwab.auth import client_from_token_file

BASE = "https://api.schwabapi.com/marketdata/v1"
ET = pytz.timezone("America/New_York")

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", dest="for_date", default=os.environ.get("FOR_DATE"),
                    help="Trading date YYYY-MM-DD (ET). If omitted on weekend, script picks last weekday.")
    return ap.parse_args()

def last_weekday(d: dt.date) -> dt.date:
    while d.weekday() >= 5:  # 5=Sat,6=Sun
        d -= dt.timedelta(days=1)
    return d

def et_epoch_ms(y, m, d, hh, mm) -> int:
    return int(ET.localize(dt.datetime(y, m, d, hh, mm)).timestamp() * 1000)

def schwab_client():
    app_key    = os.environ["SCHWAB_APP_KEY"]
    app_secret = os.environ["SCHWAB_APP_SECRET"]
    token_json = os.environ["SCHWAB_TOKEN_JSON"]
    with open("schwab_token.json","w") as f:
        f.write(token_json)
    return client_from_token_file(api_key=app_key, app_secret=app_secret, token_path="schwab_token.json")

def get_json(sess, url, params=None, tag=""):
    r = sess.get(url, params=params, timeout=25)
    if r.status_code != 200:
        print(f"[{tag}] HTTP_{r.status_code} → {r.text[:250]}", flush=True)
        return False, {}
    try:
        return True, r.json()
    except Exception:
        return True, {}

def test_auth(c):
    r = c.get_account_numbers(); r.raise_for_status()
    print(f"[OK] auth → account hash: {r.json()[0]['hashValue']}", flush=True)
    return True

def get_spx_last(sess):
    # SPX only: try $SPX.X then SPX
    for sym in ["$SPX.X", "SPX"]:
        ok, data = get_json(sess, f"{BASE}/quotes", {"symbols": sym}, tag=f"quotes {sym}")
        if ok and isinstance(data, dict) and data:
            payload = list(data.values())[0] if data else {}
            q = payload.get("quote") or payload
            last = q.get("lastPrice") or q.get("regularMarketLastPrice") or q.get("last")
            if last is not None:
                print(f"[OK] underlying → {sym} last={last}", flush=True)
                return sym, float(last)
    print("[FAIL] underlying → no $SPX.X or SPX last price returned", flush=True)
    return None, None

def request_pricehistory(sess, sym, start_ms, end_ms, freq):
    # Try both "query" and "path" styles; some tenants differ
    routes = [
        (f"{BASE}/pricehistory",      {"symbol": sym}),
        (f"{BASE}/pricehistory/{sym}", {}),
    ]
    add = {"startDate": start_ms, "endDate": end_ms,
           "frequencyType": "minute", "frequency": freq,
           "needExtendedHoursData": "false"}
    for url, base_params in routes:
        ok, data = get_json(sess, url, {**base_params, **add}, tag=f"pricehistory {sym} f{freq}")
        candles = (data.get("candles") or data.get("data", {}).get("candles") or []) if ok else []
        if ok and candles:
            df = pd.DataFrame(candles)
            tcol = "datetime" if "datetime" in df.columns else ("time" if "time" in df.columns else None)
            if not tcol:
                continue
            df["ts"] = pd.to_datetime(df[tcol], unit="ms", utc=True)
            df = df.set_index("ts").sort_index()
            return df
    return None

def test_spx_intraday_for_date(sess, d: dt.date):
    y, m, dd = d.year, d.month, d.day
    start_ms = et_epoch_ms(y, m, dd, 9, 30)
    end_ms   = et_epoch_ms(y, m, dd, 16, 0)
    symbols  = ["$SPX.X", "SPX.X", "SPX"]  # SPX-only variants worth trying
    for freq in [1, 5]:
        for sym in symbols:
            df = request_pricehistory(sess, sym, start_ms, end_ms, freq)
            if df is not None and not df.empty:
                print(f"[OK] pricehistory → {sym} {d}: {len(df)} x {freq}m bars (first {df.index[0]}, last {df.index[-1]})", flush=True)
                return True
    tried = ", ".join(symbols)
    print(f"[FAIL] pricehistory → SPX-only: no intraday bars for {d} on [{tried}] (1m and 5m)", flush=True)
    return False

# ---------- OCC builder for SPXW options ----------
def occ_spxw_symbol(d: dt.date, right: str, strike: int) -> str:
    """
    OCC 21-char format: root(6), YYMMDD(6), C/P(1), strike*1000 (8)
    Root for weeklies is 'SPXW' padded to 6 chars: 'SPXW  '
    Example: SPXW  251107C05000000
    """
    root6 = f"{'SPXW':<6}"        # 'SPXW  '
    yymmdd = d.strftime("%y%m%d")  # YYMMDD
    cp = right.upper()[0]
    strike_int = int(round(strike * 1000))
    return f"{root6}{yymmdd}{cp}{strike_int:08d}"

def round5(x: float) -> int:
    return int(round(x / 5.0) * 5)

def test_spxw_quotes_for_date(sess, d: dt.date, ref_last: float | None):
    if ref_last is None:
        print("[WARN] spxw_quotes → no SPX last; defaulting ATM to 5000 for probe", flush=True)
        ref_last = 5000.0
    atm = round5(ref_last)
    strikes = [atm + 5*k for k in range(-5, 6)]  # ±25 around ATM in 5-pt steps

    occs = []
    for k in strikes:
        occs.append(occ_spxw_symbol(d, "C", k))
        occs.append(occ_spxw_symbol(d, "P", k))

    symbols = ",".join(occs)
    ok, data = get_json(sess, f"{BASE}/quotes", {"symbols": symbols}, tag=f"quotes OCC {d}")
    if not ok or not isinstance(data, dict) or not data:
        print(f"[FAIL] spxw_quotes → no option quotes payload for {d}", flush=True)
        return False

    count = 0
    printed = 0
    for sym, payload in data.items():
        q = payload.get("quote") or payload
        # Ignore non-options if any
        if str(q.get("assetMainType","")).upper() != "OPTION":
            continue
        bid = q.get("bidPrice") or q.get("bid") or q.get("bidPriceInDouble")
        ask = q.get("askPrice") or q.get("ask") or q.get("askPriceInDouble")
        delta = q.get("delta")
        count += 1
        if printed < 10:
            print(f"  [OPT] {sym}: bid={bid} ask={ask} delta={delta}", flush=True)
            printed += 1

    if count == 0:
        print(f"[FAIL] spxw_quotes → parsed 0 options for {d}", flush=True)
        return False

    print(f"[OK] spxw_quotes → returned {count} SPXW option entries for {d}", flush=True)
    return True

def main():
    args = parse_args()
    if args.for_date:
        y, m, d = map(int, args.for_date.split("-"))
        target = dt.date(y, m, d)
    else:
        target = last_weekday(dt.datetime.now(ET).date())

    c = schwab_client()
    ok_auth = test_auth(c)

    sym, last = get_spx_last(c.session)  # SPX only
    ok_hist   = test_spx_intraday_for_date(c.session, target)  # SPX only
    ok_opts   = test_spxw_quotes_for_date(c.session, target, last)

    print(
        f"\nRESULTS: DATE={target} | AUTH={'OK' if ok_auth else 'FAIL'} | "
        f"SPX_QUOTE={'OK' if last is not None else 'FAIL'} | "
        f"SPX_INTRADAY={'OK' if ok_hist else 'FAIL'} | "
        f"SPXW_QUOTES={'OK' if ok_opts else 'FAIL'}",
        flush=True
    )

if __name__ == "__main__":
    try:
        main()
    except KeyError as e:
        print(f"Missing env: {e}", flush=True); sys.exit(2)
    except Exception as e:
        print("Unhandled:", e, flush=True); sys.exit(1)
