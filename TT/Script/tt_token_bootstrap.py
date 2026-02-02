#!/usr/bin/env python3
"""
Bootstrap Tastytrade OAuth token (prod).

Env:
  TT_CLIENT_ID
  TT_CLIENT_SECRET
  TT_REDIRECT_URI
  TT_BASE_URL       (default https://api.tastyworks.com)
  TT_AUTH_URL       (default https://my.tastytrade.com/auth.html)
  TT_TOKEN_URL      (default {TT_BASE_URL}/oauth/token)
  TT_SCOPE          (optional)
  TT_TOKEN_PATH     (optional)
"""

import json
import os
import sys
from urllib.parse import urlencode, urlparse, parse_qs

import requests


def die(msg: str):
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(1)


def parse_code(s: str) -> str:
    if not s:
        return ""
    if "code=" in s:
        try:
            q = urlparse(s).query
            return (parse_qs(q).get("code") or [""])[0]
        except Exception:
            pass
    return s.strip()


def main():
    client_id = os.environ.get("TT_CLIENT_ID", "").strip()
    client_secret = os.environ.get("TT_CLIENT_SECRET", "").strip()
    redirect_uri = os.environ.get("TT_REDIRECT_URI", "").strip()
    base = os.environ.get("TT_BASE_URL", "https://api.tastyworks.com").rstrip("/")
    auth_url = os.environ.get("TT_AUTH_URL", "https://my.tastytrade.com/auth.html").strip()
    token_url = os.environ.get("TT_TOKEN_URL", f"{base}/oauth/token").strip()
    scope = (os.environ.get("TT_SCOPE") or "").strip()

    if not (client_id and client_secret and redirect_uri):
        die("Set TT_CLIENT_ID, TT_CLIENT_SECRET, TT_REDIRECT_URI and re-run.")

    params = {
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": redirect_uri,
    }
    if scope:
        params["scope"] = scope
    url = f"{auth_url}?{urlencode(params)}"

    print("\nOpen this URL in your browser and authorize:")
    print(url)
    print("\nPaste the full redirect URL (or just the code):")
    code_raw = input("> ").strip()
    code = parse_code(code_raw)
    if not code:
        die("Missing authorization code.")

    data = {
        "grant_type": "authorization_code",
        "code": code,
        "client_id": client_id,
        "client_secret": client_secret,
        "redirect_uri": redirect_uri,
    }
    r = requests.post(token_url, data=data, timeout=20)
    r.raise_for_status()
    token = r.json()

    token_path = os.environ.get("TT_TOKEN_PATH", os.path.join("TT", "Token", "tt_token.json"))
    os.makedirs(os.path.dirname(token_path), exist_ok=True)
    with open(token_path, "w") as f:
        json.dump(token, f)

    print(f"\nToken saved to: {token_path}")
    print("=== COPY THIS JSON INTO GitHub Secret: TT_TOKEN_JSON ===\n")
    print(json.dumps(token))
    print("\n=== END ===")


if __name__ == "__main__":
    main()
