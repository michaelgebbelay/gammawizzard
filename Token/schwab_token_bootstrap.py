
import os, sys
from schwab.auth import client_from_login_flow

APP_KEY = "wtdRgK6ENV2R2NQ0aAcBAc9Ux8Vihb4hQiTnymAlS23FDgwS"
APP_SECRET = "Y4JhG3N18mlZTnGRXifVZFfCc7mg51k3bOZZiQj9pC3Y79wZ1EH2eOXiT1tETBIn"
CALLBACK_URL = "https://127.0.0.1:8182"
TOKEN_PATH  = "schwab_token.json"

def die(msg):
    print("ERROR:", msg, file=sys.stderr)
    sys.exit(1)

def main():
    if not APP_KEY or not APP_SECRET:
        die("Set SCHWAB_APP_KEY and SCHWAB_APP_SECRET env vars (or hardcode them).")

    print(f"Starting local OAuth on {CALLBACK_URL} …")
    print("This opens a browser; log in and approve. If you see a certificate warning, click through (self‑signed).\n")

    try:
        client_from_login_flow(
            api_key=APP_KEY,
            app_secret=APP_SECRET,
            callback_url=CALLBACK_URL,   # <-- correct kwarg
            token_path=TOKEN_PATH,
        )
    except Exception as e:
        msg = str(e)
        if "redirect" in msg.lower() and "mismatch" in msg.lower():
            die(f"Callback mismatch. In Schwab dev portal, register EXACTLY: {CALLBACK_URL}")
        if "Address already in use" in msg or "Errno 48" in msg or "Errno 98" in msg:
            die("Port busy. Pick another (e.g., 8765), update BOTH Schwab app + CALLBACK_URL, rerun.")
        die(f"Login flow failed: {msg[:300]}")

    # Print token so you can copy to GitHub secret
    try:
        with open(TOKEN_PATH, "r", encoding="utf-8") as f:
            token_json = f.read()
    except Exception as e:
        die(f"Token not written: {e}")

    print(f"\n✅ Token saved to: {TOKEN_PATH}\n")
    print("=== COPY THIS JSON INTO GitHub Secret: SCHWAB_TOKEN_JSON ===\n")
    print(token_json)
    print("\n=== END ===")
    print("Note: Schwab tokens are short‑lived; plan to refresh weekly. See schwab‑py docs for details.",)

if __name__ == "__main__":
    sys.exit(main())