#!/usr/bin/env python3
# Fetch one or more GammaWizard endpoints and save JSON locally.
# Env:
#   GW_BASE        default https://gandalf.gammawizard.com
#   GW_TOKEN       optional bearer (preferred)
#   GW_EMAIL       optional (fallback)
#   GW_PASSWORD    optional (fallback)
#   GW_ENDPOINTS   comma-separated: e.g. "rapi/GetUltraConstantStable,rapi/GetLeoCross,rapi/GetUltraSVJ"
#   GW_SAVE_DIR    optional dir to write files (created if missing)

import os, sys, json, time, pathlib, requests

BASE = os.environ.get("GW_BASE","https://gandalf.gammawizard.com").rstrip("/")
ENDPOINTS_RAW = os.environ.get("GW_ENDPOINTS","rapi/GetUltraConstantStable,rapi/GetLeoCross,rapi/GetUltraSVJ")
SAVE_DIR = os.environ.get("GW_SAVE_DIR","").strip()
TOKEN = os.environ.get("GW_TOKEN","").strip()
EMAIL = os.environ.get("GW_EMAIL","").strip()
PASSWORD = os.environ.get("GW_PASSWORD","").strip()

def sanitize_token(t: str) -> str:
    t = (t or "").strip().strip('"').strip("'")
    return t.split(None,1)[1] if t.lower().startswith("bearer ") else t

def login_token() -> str:
    if not (EMAIL and PASSWORD):
        raise RuntimeError("GW_AUTH_REQUIRED (no token and no creds)")
    r = requests.post(f"{BASE}/goauth/authenticateFireUser", data={"email":EMAIL,"password":PASSWORD}, timeout=30)
    r.raise_for_status()
    j = r.json() or {}
    tok = j.get("token")
    if not tok:
        raise RuntimeError("GW_LOGIN_NO_TOKEN")
    return tok

def build_url(ep: str) -> str:
    e = (ep or "").strip()
    if e.startswith("http://") or e.startswith("https://"):
        return e
    if e.startswith("rapi/") or e.startswith("/rapi/"):
        return f"{BASE}/{e.lstrip('/')}"
    # allow bare names like "GetLeoCross"
    return f"{BASE}/rapi/{e.lstrip('/')}"

def fetch_endpoint(ep: str, tok: str | None):
    url = build_url(ep)
    def hit(t):
        h={"Accept":"application/json"}
        if t:
            h["Authorization"]=f"Bearer {sanitize_token(t)}"
        return requests.get(url, headers=h, timeout=45)
    r = hit(tok) if tok else None
    if (r is None) or (r.status_code in (401,403)):
        t2 = login_token()
        r = hit(t2)
    r.raise_for_status()
    try:
        return r.json()
    except Exception:
        return {"raw": r.text}

def save_json(obj, ep: str, root: pathlib.Path) -> pathlib.Path:
    ts = time.strftime("%Y%m%d_%H%M%S")
    safe = ep.strip().strip("/").replace("/", "_")
    if not safe:
        safe = "endpoint"
    fn = f"{safe}_{ts}.json"
    path = root / fn
    path.write_text(json.dumps(obj, ensure_ascii=False, separators=(",",":")), encoding="utf-8")
    return path

def main():
    eps = [e.strip() for e in (ENDPOINTS_RAW or "").split(",") if e.strip()]
    if not eps:
        print("ERROR: GW_ENDPOINTS is empty", file=sys.stderr)
        return 2

    outdir = pathlib.Path(SAVE_DIR) if SAVE_DIR else pathlib.Path.cwd()
    outdir.mkdir(parents=True, exist_ok=True)

    tok = TOKEN or ""
    ok = 0
    for ep in eps:
        try:
            data = fetch_endpoint(ep, tok)
            p = save_json(data, ep, outdir)
            print(f"SAVED {ep} -> {p}")
            ok += 1
        except Exception as e:
            print(f"ERROR {ep}: {type(e).__name__}: {e}", file=sys.stderr)
            # continue to next endpoint
            continue

    return 0 if ok > 0 else 1

if __name__ == "__main__":
    sys.exit(main())
