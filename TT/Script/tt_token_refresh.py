#!/usr/bin/env python3
"""
Refresh Tastytrade token from TT_TOKEN_JSON env and write to TT/Token/tt_token.json.
"""

import base64
import json
import os
import sys

from tt_token_keeper import refresh_token, save_token


def _decode_token_env(raw: str) -> str:
    raw = (raw or "").strip()
    if not raw:
        return ""
    if raw.lstrip().startswith("{"):
        return raw
    try:
        dec = base64.b64decode(raw).decode("utf-8")
        if dec.strip().startswith("{"):
            return dec
    except Exception:
        pass
    return raw


def main():
    token_env = _decode_token_env(os.environ.get("TT_TOKEN_JSON", "") or "")
    if not token_env:
        print("TT_TOKEN_JSON missing", file=sys.stderr)
        return 1

    try:
        token_obj = json.loads(token_env)
    except Exception as e:
        print(f"Invalid TT_TOKEN_JSON: {e}", file=sys.stderr)
        return 2

    save_token(token_obj)
    refresh_token(token_obj)
    print("TT token refreshed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
