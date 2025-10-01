#!/usr/bin/env python3
import os, re, json, sys
from pathlib import Path
from datetime import datetime, timedelta, timezone
import requests

def _now_utc(): return datetime.now(timezone.utc)
def _ts(): return _now_utc().strftime("%Y%m%d_%H%M%S")

def _nonempty(*names):
    for n in names:
        v = os.environ.get(n)
        if v is not None and str(v).strip():
            return str(v).strip()
    return None

def _sanitize_token(t: str | None) -> str | None:
    if not t: return None
    t = t.strip().strip('"').strip("'")
    return t.split(None, 1)[1] if t.lower().startswith("bearer ") else t

def _timeout() -> int:
    try: return int(os.environ.get("GW_TIMEOUT", "30"))
    except: return 30

def _slug(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_.-]+", "_", s).strip("_")[:120] or "gw"

def _hdr(tok: str | None) -> dict:
    h = {"Accept": "application/json", "User-Agent": "gw-inspect/1.1"}
    if tok: h["Authorization"] = f"Bearer {tok}"
    return h

def _login_token(base: str) -> str:
    email = _nonempty("GW_EMAIL")
    pwd   = _nonempty("GW_PASSWORD")
    if not (email and pwd):
        raise RuntimeError("GW_LOGIN_MISSING_CREDS")
    r = requests.post(f"{base}/goauth/authenticateFireUser",
                      data={"email": email, "password": pwd},
                      timeout=_timeout())
    r.raise_for_status()
    j = r.json()
    tok = j.get("token") or j.get("access_token")
    if not tok:
        raise RuntimeError(f"GW_LOGIN_NO_TOKEN in response: {list(j.keys())}")
    return _sanitize_token(tok)

def _params_for(path: str, start: str | None, end: str | None) -> dict:
    # Some endpoints (e.g., GetLeoCross) don’t take start/end; only send if both present.
    if start and end:
        return {"start": start, "end": end}
    return {}

def _get(base: str, path: str, start: str | None, end: str | None, tok: str | None):
    url = f"{base.rstrip('/')}/{path.lstrip('/')}"
    qs = _params_for(path, start, end)
    print(f"→ GET {path} params={qs if qs else '{}'}")
    return requests.get(url, params=qs, headers=_hdr(tok), timeout=_timeout())

def _dump(text: str, dump_dir: Path, path: str) -> Path:
    dump_dir.mkdir(parents=True, exist_ok=True)
    fp = dump_dir / f"{_ts()}_{_slug(path)}.json"
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
    base  = _nonempty("GW_BASE") or "https://gandalf.gammawizard.com"
    raw_paths = _nonempty("GW_PATH")
    paths = [p.strip() for p in raw_paths.split(",")] if raw_paths else ["/rapi/Market/SpxClose", "/rapi/SpxClose", "/rapi/GetLeoCross"]

    # Compute window with sane fallbacks
    try:
        days = int(_nonempty("GW_DAYS", "SETTLE_BACKFILL_DAYS") or "14")
    except Exception:
        days = 14
    t1_default = datetime.now(timezone.utc).date()
    t0_default = t1_default - timedelta(days=days)
    start = _nonempty("GW_START") or t0_default.isoformat()
    end   = _nonempty("GW_END")   or t1_default.isoformat()

    dump_dir = Path(_nonempty("GW_DUMP_DIR") or "gw_dump")

    tok = _sanitize_token(_nonempty("GW_TOKEN", "GW_VAR2_TOKEN"))

    print(f"Base={base}  window={start}→{end}  paths={paths}")

    for path in paths:
        r = _get(base, path, start, end, tok)
        if r.status_code in (401, 403):
            print("Auth failed → trying email/password login…")
            try:
                tok = _login_token(base)
                r = _get(base, path, start, end, tok)
            except Exception as e:
                print(f"!! Unable to obtain login token: {e}. Skipping {path}.")

        print(f"HTTP {r.status_code}  content-type={r.headers.get('content-type','?')}  bytes={len(r.content)}")
        _dump(r.text, dump_dir, path)

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
