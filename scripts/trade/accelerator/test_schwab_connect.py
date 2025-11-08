#!/usr/bin/env python3
"""
Smoke test for Schwab Trader API:
- Auth (get_account_numbers)
- SPX quote via client.get_quote()
- Intraday price history via raw HTTPS (tries two common endpoints)

Requires env:
  SCHWAB_APP_KEY
  SCHWAB_APP_SECRET
  SCHWAB_TOKEN_JSON          # JSON string (what your placer already uses)

Optional:
  TEST_OCC                   # e.g., 'SPXW  12/31/24 5000 P' or your OCC format
"""

import os, json, sys
from schwab.auth import client_from_token_file

def norm_quote_payload(j):
    # Schwab quote payloads sometimes nest under "quote"
    if isinstance(j, dict):
        if "quote" in j: return j["quote"]
        if len(j) == 1 and isinstance(list(j.values())[0], dict):
            return norm_quote_payload(list(j.values())[0])
    return j

def main():
    # ---- auth ----
    app_key     = os.environ["SCHWAB_APP_KEY"]
    app_secret  = os.environ["SCHWAB_APP_SECRET"]
    token_json  = os.environ["SCHWAB_TOKEN_JSON"]

    with open("schwab_token.json", "w") as f:
        f.write(token_json)

    c = client_from_token_file(api_key=app_key, app_secret=app_secret, token_path="schwab_token.json")

    # ---- account numbers (auth sanity) ----
    r = c.get_account_numbers()
    r.raise_for_status()
    accounts = r.json()
    acct_hash = str(accounts[0]["hashValue"])
    print(f"[OK] auth → account hash: {acct_hash}")

    # ---- SPX quote via client wrapper ----
    rq = c.get_quote("SPX")
    if rq.status_code != 200:
        print(f"[WARN] SPX quote HTTP_{rq.status_code}: {rq.text[:200]}")
    else:
        q = norm_quote_payload(rq.json())
        last = q.get("lastPrice") or q.get("last") or q.get("mark") or q.get("regularMarketLastPrice")
        print(f"[OK] SPX quote → last={last}")

    # ---- Optional: single option quote if you provide TEST_OCC ----
    test_occ = os.environ.get("TEST_OCC", "").strip()
    if test_occ:
        ro = c.get_quote(test_occ)
        if ro.status_code == 200:
            oq = norm_quote_payload(ro.json())
            bid = oq.get("bidPrice") or oq.get("bid") or oq.get("bidPriceInDouble")
            ask = oq.get("askPrice") or oq.get("ask") or oq.get("askPriceInDouble")
            delta = oq.get("delta")
            print(f"[OK] OPTION {test_occ} → bid={bid} ask={ask} delta={delta}")
        else:
            print(f"[WARN] option quote for '{test_occ}' → HTTP_{ro.status_code}: {ro.text[:200]}")

    # ---- Intraday price history (SPX 1‑minute). Try two common endpoints ----
    ok_ph = False
    tried = []
    for url in [
        # Query‑string style:
        "https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=SPX&periodType=day&period=1&frequencyType=minute&frequency=1",
        # Path style:
        "https://api.schwabapi.com/marketdata/v1/pricehistory/SPX?periodType=day&period=1&frequencyType=minute&frequency=1",
    ]:
        try:
            rp = c.session.get(url, timeout=15)
            tried.append((url, rp.status_code))
            if rp.status_code == 200:
                j = rp.json()
                bars = j.get("candles") or j.get("bars") or j.get("data") or []
                print(f"[OK] price history → {len(bars)} intraday bars from {url}")
                ok_ph = True
                break
        except Exception as e:
            tried.append((url, f"EXC:{e}"))

    if not ok_ph:
        print("[WARN] price history not confirmed; tried:", tried)

    print("[DONE] Schwab connectivity test finished.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
