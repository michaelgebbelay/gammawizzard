#!/usr/bin/env python3
"""
Schwab data test for a specific trading date (ET):
- Auth sanity
- Quotes ($SPX.X / SPX)
- 1m price history for the requested date (09:30–16:00 ET)
- SPXW 0DTE option chain for that date (quotes+greeks)

Usage:
  FOR_DATE=2025-11-07 python scripts/trade/accelerator/test_schwab_for_date.py
or:
  python scripts/trade/accelerator/test_schwab_for_date.py --date 2025-11-07

Env required (same as your placer):
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
    while d.weekday() >= 5:  # 5=Sat, 6=Sun
        d -= dt.timedelta(days=1)
    return d

def et_epoch_ms(y, m, d, hh, mm) -> int:
    return int(ET.localize(dt.datetime(y, m, d, hh, mm)).timestamp() * 1000)

def client():
    app_key    = os.environ["SCHWAB_APP_KEY"]
    app_secret = os.environ["SCHWAB_APP_SECRET"]
    token_json = os.environ["SCHWAB_TOKEN_JSON"]
    with open("schwab_token.json","w") as f: f.write(token_json)
    return client_from_token_file(api_key=app_key, app_secret=app_secret, token_path="schwab_token.json")

def get_json(sess, url, params=None, tag=""):
    r = sess.get(url, params=params, timeout=25)
    if r.status_code != 200:
        print(f"[{tag}] HTTP_{r.status_code} → {r.text[:200]}")
        return False, {}
    try:
        return True, r.json()
    except Exception:
        return True, {}

def test_auth(c):
    r = c.get_account_numbers(); r.raise_for_status()
    print(f"[OK] auth → account hash: {r.json()[0]['hashValue']}"); return True

def test_quotes(sess):
    for sym in ["$SPX.X", "SPX"]:
        ok, data = get_json(sess, f"{BASE}/quotes", {"symbols": sym}, tag=f"quotes {sym}")
        if ok and isinstance(data, dict) and data:
            print(f"[OK] quotes → {sym}"); return True
    print("[FAIL] quotes → neither $SPX.X nor SPX returned data"); return False

def test_price_history_for_date(sess, d: dt.date):
    y, m, dd = d.year, d.month, d.day
    start_ms = et_epoch_ms(y, m, dd, 9, 30)
    end_ms   = et_epoch_ms(y, m, dd, 16, 0)
    params = {
        "startDate": start_ms, "endDate": end_ms,
        "frequencyType": "minute", "frequency": 1,
        "needExtendedHoursData": "false"
    }
    # Try SPX index first; if tenant doesn’t return 1m for SPX, fall back to SPY
    for sym in ["$SPX.X", "SPX", "SPY"]:
        ok, data = get_json(sess, f"{BASE}/pricehistory", {"symbol": sym, **params}, tag=f"pricehistory {sym} {d}")
        candles = (data.get("candles") or data.get("data", {}).get("candles") or []) if ok else []
        if ok and candles:
            df = pd.DataFrame(candles)
            tcol = "datetime" if "datetime" in df.columns else ("time" if "time" in df.columns else None)
            if not tcol:
                continue
            df["ts"] = pd.to_datetime(df[tcol], unit="ms", utc=True)
            df = df.set_index("ts").sort_index()
            print(f"[OK] pricehistory → {sym} {d}: {len(df)} x 1m bars (first {df.index[0]}, last {df.index[-1]})")
            return True
    print(f"[FAIL] pricehistory → no 1m bars for {d} on $SPX.X/SPX/SPY")
    return False

def test_chain_for_date(sess, d: dt.date):
    ds = d.strftime("%Y-%m-%d")
    for endpoint in ["chains", "optionchains"]:
        ok, data = get_json(sess, f"{BASE}/{endpoint}", {
            "symbol": "SPX",
            "fromDate": ds, "toDate": ds,
            "includeQuotes": "TRUE", "includeGreeks": "TRUE",
            "contractType": "ALL", "strategy": "SINGLE", "range": "ALL"
        }, tag=f"{endpoint} {ds}")
        if ok and isinstance(data, dict) and data:
            txt = json.dumps(data)
            has_delta = "delta" in txt.lower()
            print(f"[OK] chain ({endpoint}) {ds} → payload {len(txt):,} bytes; greeks_present={has_delta}")
            return True
    print(f"[FAIL] chain → no payload for {ds} (check entitlements or endpoint)")
    return False

def main():
    args = parse_args()
    if args.for_date:
        y, m, d = map(int, args.for_date.split("-"))
        target = dt.date(y, m, d)
    else:
        target = last_weekday(dt.datetime.now(ET).date())  # if weekend, use last Friday

    c = client()
    ok_auth = test_auth(c)
    ok_q    = test_quotes(c.session)
    ok_ph   = test_price_history_for_date(c.session, target)
    ok_ch   = test_chain_for_date(c.session, target)

    print("\nRESULTS:",
          f"DATE={target} |",
          f"AUTH={'OK' if ok_auth else 'FAIL'} |",
          f"QUOTES={'OK' if ok_q else 'FAIL'} |",
          f"PRICE_HISTORY={'OK' if ok_ph else 'FAIL'} |",
          f"CHAIN_0DTE={'OK' if ok_ch else 'FAIL'}")

if __name__ == "__main__":
    try:
        main()
    except KeyError as e:
        print(f"Missing env: {e}"); sys.exit(2)
    except Exception as e:
        print("Unhandled:", e); sys.exit(1)
