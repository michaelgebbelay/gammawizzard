"""
Generate a fresh Schwab token JSON locally.

Usage:
  export SCHWAB_APP_KEY=wtdRgK6ENV2R2NQ0aAcBAc9Ux8Vihb4hQiTnymAlS23FDgwS
  export SCHWAB_APP_SECRET=Y4JhG3N18mlZTnGRXifVZFfCc7mg51k3bOZZiQj9pC3Y79wZ1EH2eOXiT1tETBIn
  export SCHWAB_REDIRECT_URI=https://127.0.0.1:8182     # must match your app's registered redirect URI
  python scripts/schwab_token_bootstrap.py
"""

import os, sys, json, inspect, importlib.util
from schwab.auth import client_from_login_flow

def main():
    app_key = os.environ.get("SCHWAB_APP_KEY")
    app_secret = os.environ.get("SCHWAB_APP_SECRET")
    redirect_uri = os.environ.get("SCHWAB_REDIRECT_URI")

    if not (app_key and app_secret and redirect_uri):
        fallback = os.path.join(os.path.dirname(__file__), "..", "Token", "schwab_token_bootstrap.py")
        fallback = os.path.abspath(fallback)
        if os.path.exists(fallback):
            spec = importlib.util.spec_from_file_location("schwab_bootstrap_fallback", fallback)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)  # type: ignore[union-attr]
            app_key = app_key or getattr(mod, "APP_KEY", None)
            app_secret = app_secret or getattr(mod, "APP_SECRET", None)
            redirect_uri = redirect_uri or getattr(mod, "CALLBACK_URL", None)

    if not (app_key and app_secret and redirect_uri):
        print("Set SCHWAB_APP_KEY, SCHWAB_APP_SECRET, SCHWAB_REDIRECT_URI or use Token/schwab_token_bootstrap.py.", file=sys.stderr)
        sys.exit(1)

    token_path = os.environ.get("SCHWAB_TOKEN_PATH", os.path.join("Token", "schwab_token.json"))
    kwargs = {"api_key": app_key, "app_secret": app_secret, "token_path": token_path}
    try:
        sig = inspect.signature(client_from_login_flow)
    except Exception:
        sig = None
    if sig and "redirect_uri" in sig.parameters:
        kwargs["redirect_uri"] = redirect_uri
    else:
        kwargs["callback_url"] = redirect_uri
    client_from_login_flow(**kwargs)

    with open(token_path, "r") as f:
        token_json = f.read()

    print("\n=== WRITE THIS INTO YOUR GitHub secret SCHWAB_TOKEN_JSON ===\n")
    print(token_json)
    print(f"\n=== END ===\nSaved to {token_path} as well.\n")

if __name__ == "__main__":
    main()
