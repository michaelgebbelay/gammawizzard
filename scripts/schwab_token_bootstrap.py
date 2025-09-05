"""
Generate a fresh Schwab token JSON locally.

Usage:
  export SCHWAB_APP_KEY=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
  export SCHWAB_APP_SECRET=yyyyyyyyyyyyyyyyyyyyyyyyyyyy
  export SCHWAB_REDIRECT_URI=http://localhost:8765     # must match your app's registered redirect URI
  python scripts/schwab_token_bootstrap.py
"""

import os, sys, json
from schwab.auth import client_from_login_flow

def main():
    app_key = os.environ.get("SCHWAB_APP_KEY")
    app_secret = os.environ.get("SCHWAB_APP_SECRET")
    redirect_uri = os.environ.get("SCHWAB_REDIRECT_URI")

    if not (app_key and app_secret and redirect_uri):
        print("Set SCHWAB_APP_KEY, SCHWAB_APP_SECRET, SCHWAB_REDIRECT_URI and re-run.", file=sys.stderr)
        sys.exit(1)

    token_path = "schwab_token.json"
    client_from_login_flow(api_key=app_key, app_secret=app_secret, redirect_uri=redirect_uri, token_path=token_path)

    with open(token_path, "r") as f:
        token_json = f.read()

    print("\n=== WRITE THIS INTO YOUR GitHub secret SCHWAB_TOKEN_JSON ===\n")
    print(token_json)
    print("\n=== END ===\nSaved to ./schwab_token.json as well.\n")

if __name__ == "__main__":
    main()
