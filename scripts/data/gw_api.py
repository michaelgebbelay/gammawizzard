#!/usr/bin/env python3
# Minimal GammaWizard API client:
# - Auth with GW_TOKEN or GW_EMAIL/GW_PASSWORD
# - GET any endpoint (e.g., "rapi/GetUltraSupreme", "GetLeoCross", "/rapi/GetX")
# - Auto re-auth on 401/403
import os, sys, re, json
import requests

GW_BASE = os.environ.get("GW_BASE", "https://gandalf.gammawizard.com").rstrip("/")

def _sanitize_token(t: str) -> str:
    t = (t or "").strip().strip('"').strip("'")
    return t.split(None, 1)[1] if t.lower().startswith("bearer ") else t

def _auth(email: str, password: str, timeout: int = 30) -> str:
    r = requests.post(f"{GW_BASE}/goauth/authenticateFireUser",
                      data={"email": email, "password": password},
                      timeout=timeout)
    r.raise_for_status()
    j = r.json() or {}
    tok = j.get("token") or j.get("Token") or ""
    if not tok:
        raise RuntimeError("GW_AUTH_NO_TOKEN")
    return _sanitize_token(tok)

def _normalize_endpoint(ep: str) -> str:
    ep = (ep or "").strip()
    ep = ep[1:] if ep.startswith("/") else ep
    if not ep.lower().startswith("rapi/"):
        ep = "rapi/" + ep
    return ep

def gw_get(endpoint: str, params: dict | None = None, timeout: int = 30) -> dict | list:
    ep = _normalize_endpoint(endpoint)
    base_tok = _sanitize_token(os.environ.get("GW_TOKEN", "") or "")
    email = os.environ.get("GW_EMAIL", "")
    pwd   = os.environ.get("GW_PASSWORD", "")
    def hit(tok: str | None):
        headers = {"Accept": "application/json"}
        if tok:
            headers["Authorization"] = f"Bearer {_sanitize_token(tok)}"
        return requests.get(f"{GW_BASE}/{ep}", headers=headers, params=(params or {}), timeout=timeout)
    # first try with provided token (if any)
    r = hit(base_tok if base_tok else None)
    if r.status_code in (401, 403):
        if not (email and pwd):
            r.raise_for_status()
        # re-auth and retry once
        fresh = _auth(email, pwd, timeout=timeout)
        r = hit(fresh)
    r.raise_for_status()
    try:
        return r.json()
    except Exception:
        # if not json, return text envelope
        return {"raw": r.text}

# CLI: python scripts/trade/gw_api.py rapi/GetUltraSupreme [rapi/OtherEp ...]
if __name__ == "__main__":
    eps = sys.argv[1:] or ["rapi/GetUltraSupreme", "rapi/GetLeoCross"]
    for ep in eps:
        try:
            data = gw_get(ep)
            print(f"===== {ep} =====")
            print(json.dumps(data, indent=2, sort_keys=False))
            print()
        except Exception as e:
            print(f"ERROR {ep}: {e}", file=sys.stderr)
            sys.exit(2)
