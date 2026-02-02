#!/usr/bin/env python3
"""
Fetch sample Tastytrade responses for mapping fields.

Env:
  TT_BASE_URL
  TT_TOKEN_JSON
  TT_ACCOUNT_NUMBER
"""

import json
import os
import sys

from tt_client import request
from tt_token_keeper import save_token


def die(msg: str):
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(1)


def main():
    acct = (os.environ.get("TT_ACCOUNT_NUMBER") or "").strip()
    token_env = (os.environ.get("TT_TOKEN_JSON") or "").strip()
    if not acct:
        die("TT_ACCOUNT_NUMBER missing")
    if not token_env:
        die("TT_TOKEN_JSON missing")

    try:
        token_obj = json.loads(token_env)
    except Exception as e:
        die(f"TT_TOKEN_JSON invalid: {e}")

    save_token(token_obj)

    out = {}

    def safe_get(label, method, path, **kwargs):
        try:
            out[label] = request(method, path, **kwargs).json()
        except Exception as e:
            out[label] = {"error": str(e)}

    safe_get("balances", "GET", f"/accounts/{acct}/balances")
    safe_get("positions", "GET", f"/accounts/{acct}/positions")
    safe_get("orders", "GET", f"/accounts/{acct}/orders")
    safe_get("transactions", "GET", f"/accounts/{acct}/transactions")
    safe_get("quotes", "GET", "/market-data/quotes", params={"symbols": "SPXW"})

    print(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
