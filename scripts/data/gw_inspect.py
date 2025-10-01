#!/usr/bin/env python3
# Inspect GammaWizard responses and dump raw payloads.
import os, re, json, sys
from pathlib import Path
from datetime import datetime, timedelta, timezone
import requests

def _now_utc():
    return datetime.now(timezone.utc)

def _ts():
    return _now_utc().strftime("%Y%m%d_%H%M%S")

def _sanitize_token(t: str | None) -> str | None:
    if not t: return None
    t = t.strip().strip('"').strip("'")
    if t.lower().startswith("bearer "):
        t = t.split(None, 1)[1]
    return t

def _timeout() -> int:
    try: return int(os.environ.get("GW_TIMEOUT", "30"))
    except: return 30

def _slug(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_.-]+", "_", s).strip("_")[:120] or "gw"

def _bearer_header(tok: str | None) -> dict:
    h = {"Accept": "application/json", "User-Agent": "gw-inspect/1.0"}
    if tok:
        h["Authorization"] = f"Bearer {tok}"
    return h

def _login_token(base: str) -> str:
    email = os.environ.get("GW_EMAIL", "")
    pwd   = os.environ.get("GW_PASSWORD", "")
    if not (email and pwd):
        raise RuntimeError("GW_LOGIN_MISSING_CREDS")
    r = requests.post(
        f"{base}/goauth/authenticateFireUser",
        data={"email": email, "password": pwd},
        timeout=_timeout(),
    )
    r.raise_for_status()
    j = r.json()
    tok = j.get("token") or j.get("access_token")
    if not tok:
        raise RuntimeError(f"GW_LOGIN_NO_TOKEN in response: {list(j.keys())}")
    return _sanitize_token(tok)

def _call(base: str, path: str, start: str, end: str, tok: str | None):
    url = f"{base.rstrip('/')}/{path.lstrip('/')}"
    return requests.get(
        url,
        params={"start": start, "end": end},
        headers=_bearer_header(tok),
        timeout=_timeout(),
    )

def _dump_response(text: str, dump_dir: Path, path: str) -> Path:
    dump_dir.mkdir(parents=True, exist_ok=True)
    name = f"{_ts()}_{_slug(path)}.json"
    fp = dump_dir / name
    fp.write_text(text, encoding="utf-8")
    print(f"→ Dumped {len(text.encode('utf-8'))} bytes to {fp}")
    return fp

def _preview_json(obj):
    def pv(o, depth=0):
        pad = "  " * depth
        if isinstance(o, dict):
            ks = list(o.keys())
            print(pad + f"dict keys ({len(ks)}): {ks[:12]}{' …' if len(ks)>12 else ''}")
            for k in ("data","rows","list","candles","items","result"):
                v = o.get(k)
                if isinstance(v, list):
                    print(pad + f"  {k}: list len={len(v)}")
                    if v and isinstance(v[0], dict):
                        print(pad + "  first item keys:", list(v[0].keys())[:20])
                    break
        elif isinstance(o, list):
            print(pad + f"list len={len(o)}")
            if o:
                if isinstance(o[0], dict):
                    print(pad + "first item keys:", list(o[0].keys())[:20])
                else:
                    print(pad + "first 3 items:", o[:3])
        else:
            print(pad + f"type={type(o).__name__}")
    pv(obj)

def main():
    # ---- Config from env
    base  = os.environ.get("GW_BASE", "https://gandalf.gammawizard.com").rstrip("/")
    paths = [p.strip() for p in os.environ.get("GW_PATH", "").split(",") if p.strip()]
    if not paths:
        paths = ["/rapi/Market/SpxClose", "/rapi/SpxClose"]

    # date window
    try:
        days_env = os.environ.get("GW_DAYS") or os.environ.get("SETTLE_BACKFILL_DAYS") or "14"
        days = int(days_env)
    except Exception:
        days = 14
    t1 = datetime.now(timezone.utc).date()
    t0 = t1 - timedelta(days=days)
    start_iso = os.environ.get("GW_START", t0.isoformat())
    end_iso   = os.environ.get("GW_END",   t1.isoformat())

    dump_dir = Path(os.environ.get("GW_DUMP_DIR", "gw_dump"))

    # creds
    token = _sanitize_token(os.environ.get("GW_TOKEN") or os.environ.get("GW_VAR2_TOKEN"))
    email = os.environ.get("GW_EMAIL")
    pwd   = os.environ.get("GW_PASSWORD")

    print(f"Base={base}  window={start_iso}→{end_iso}  paths={paths}")

    for path in paths:
        print(f"\n=== Hitting {path} start={start_iso} end={end_iso} ===")
        r = _call(base, path, start_iso, end_iso, token)
        if r.status_code in (401, 403):
            if email and pwd:
                print("Auth failed → trying email/password login…")
                try:
                    token = _login_token(base)
                    r = _call(base, path, start_iso, end_iso, token)
                except Exception as e:
                    print(f"!! Unable to obtain login token: {e}. Skipping {path}.")
            else:
                print("Auth failed or no token → logging in…")
                try:
                    token = _login_token(base)
                    r = _call(base, path, start_iso, end_iso, token)
                except Exception as e:
                    print(f"!! Unable to obtain login token: {e}. Skipping {path}.")

        print(f"HTTP {r.status_code}  content-type={r.headers.get('content-type','?')}  bytes={len(r.content)}")
        _dump_response(r.text, dump_dir, path)

        # Try to preview JSON shape
        try:
            j = r.json()
        except Exception as e:
            print(f"!! Not JSON parseable: {e}\nFirst 500 chars:\n{r.text[:500]}")
            continue

        print("Structure preview:")
        _preview_json(j)
        if isinstance(j, list) and j:
            print("Sample[0]:", json.dumps(j[0], indent=2)[:1000])
        elif isinstance(j, dict):
            for k in ("data","rows","list","candles","items","result"):
                if isinstance(j.get(k), list) and j[k]:
                    print(f"Sample {k}[0]:", json.dumps(j[k][0], indent=2)[:1000])
                    break
    return 0

if __name__ == "__main__":
    sys.exit(main())
