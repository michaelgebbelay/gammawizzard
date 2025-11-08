#!/usr/bin/env python3
"""
Schwab smoke test:
- Auth
- Quotes (tries $SPX.X, SPX, SPY)
- Intraday 1m price history using start/end epoch ms (tries $SPX.X, SPX, SPY)
- SPXW 0DTE option chain (today) with quotes+greeks

Env required:
  SCHWAB_APP_KEY
  SCHWAB_APP_SECRET
  SCHWAB_TOKEN_JSON   # JSON string (same as your placer)

pip install: pandas schwab-py
"""
from __future__ import annotations
import os, sys, json, time, datetime as dt
import pandas as pd
from schwab.auth import client_from_token_file

BASE_MD = "https://api.schwabapi.com/marketdata/v1"

def client():
    app_key    = os.environ["SCHWAB_APP_KEY"]
    app_secret = os.environ["SCHWAB_APP_SECRET"]
    token_json = os.environ["SCHWAB_TOKEN_JSON"]
    with open("schwab_token.json","w") as f:
        f.write(token_json)
    return client_from_token_file(api_key=app_key, app_secret=app_secret, token_path="schwab_token.json")

def get_json(sess, url, params=None, tag=""):
    r = sess.get(url, params=params, timeout=25)
    if r.status_code != 200:
        print(f"[{tag}] HTTP_{r.status_code} → {r.text[:180]}")
        return False, {}
    try:
        return True, r.json()
    except Exception:
        return True, {}

def test_auth(c):
    r = c.get_account_numbers()
    r.raise_for_status()
    data = r.json()
    print(f"[OK] auth → account hash: {data[0]['hashValue']}")
    return True

def test_quotes(sess):
    for sym in ["$SPX.X", "SPX", "SPY"]:
        ok, data = get_json(sess, f"{BASE_MD}/quotes", {"symbols": sym}, tag=f"quotes {sym}")
        if ok and isinstance(data, dict) and data:
            print(f"[OK] quotes → got data for {sym}")
            return True
    print("[FAIL] quotes → none of $SPX.X/SPX/SPY returned data")
    return False

def test_price_history(sess):
    # request by explicit time window (last ~8 hours) to avoid empty 'period=1' corner cases
    end_ms = int(time.time() * 1000)
    start_ms = end_ms - 8 * 60 * 60 * 1000
    base_params = {
        "startDate": start_ms, "endDate": end_ms,
        "frequencyType": "minute", "frequency": 1,
        "needExtendedHoursData": "false"
    }
    for sym in ["$SPX.X", "SPX", "SPY"]:
        ok, data = get_json(sess, f"{BASE_MD}/pricehistory", {"symbol": sym, **base_params}, tag=f"pricehistory {sym}")
        candles = (data.get("candles") or data.get("data", {}).get("candles") or []) if ok else []
        if ok and candles:
            df = pd.DataFrame(candles)
            tcol = "datetime" if "datetime" in df.columns else ("time" if "time" in df.columns else None)
            if tcol:
                df["ts"] = pd.to_datetime(df[tcol], unit="ms", utc=True)
                df = df.set_index("ts").sort_index()
                print(f"[OK] pricehistory → {sym} : {len(df)} x 1m bars (last {df.index[-1]})")
                return True
    print("[FAIL] pricehistory → no 1m candles for $SPX.X/SPX/SPY in last 8h")
    return False

def test_chain(sess):
    # Today (PT date) – SPXW 0DTE
    today_pt = dt.datetime.now(dt.timezone(dt.timedelta(hours=-8))).date()
    d = today_pt.strftime("%Y-%m-%d")
    for path in ["chains", "optionchains"]:
        ok, data = get_json(sess, f"{BASE_MD}/{path}", {
            "symbol": "SPX",
            "fromDate": d, "toDate": d,
            "includeQuotes": "TRUE", "includeGreeks": "TRUE",
            "contractType": "ALL", "strategy": "SINGLE", "range": "ALL"
        }, tag=f"{path} today")
        if ok and isinstance(data, dict) and data:
            text = json.dumps(data)
            has_delta = ("delta" in text.lower())
            print(f"[OK] chain ({path}) → payload size {len(text):,} bytes; greeks_present={has_delta}")
            return True
    print("[FAIL] chain → neither /chains nor /optionchains returned a payload for today")
    return False

def main():
    c = client()
    ok_auth = test_auth(c)
    ok_q    = test_quotes(c.session)
    ok_ph   = test_price_history(c.session)
    ok_ch   = test_chain(c.session)

    print("\nRESULTS:",
          f"AUTH={'OK' if ok_auth else 'FAIL'} |",
          f"QUOTES={'OK' if ok_q else 'FAIL'} |",
          f"PRICE_HISTORY={'OK' if ok_ph else 'FAIL'} |",
          f"CHAIN_0DTE={'OK' if ok_ch else 'FAIL'}")

if __name__ == "__main__":
    try:
        main()
    except KeyError as e:
        print(f"Missing env: {e}")
        sys.exit(2)
    except Exception as e:
        print("Unhandled:", e)
        sys.exit(1)
