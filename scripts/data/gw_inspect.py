#!/usr/bin/env python3
import os, json, re
from datetime import datetime, timedelta, timezone
import requests

def _sanitize_token(t: str) -> str:
    t=(t or "").strip().strip('"').strip("'")
    return t.split(None,1)[1] if t.lower().startswith("bearer ") else t

def _gw_timeout() -> int:
    try: return int(os.environ.get("GW_TIMEOUT","30"))
    except: return 30

def gw_login_token():
    email=os.environ.get("GW_EMAIL",""); pwd=os.environ.get("GW_PASSWORD","")
    if not (email and pwd): raise RuntimeError("GW_LOGIN_MISSING_CREDS")
    base=os.environ.get("GW_BASE","https://gandalf.gammawizard.com").rstrip("/")
    r=requests.post(f"{base}/goauth/authenticateFireUser",
                    data={"email":email,"password":pwd},
                    timeout=_gw_timeout())
    r.raise_for_status()
    j=r.json()
    t=j.get("token")
    if not t: raise RuntimeError(f"GW_LOGIN_NO_TOKEN in: {j}")
    return t

def _slug(s: str) -> str:
    return re.sub(r'[^a-zA-Z0-9_.-]+','_', s).strip('_')[:120]

def gw_inspect(paths, start_iso: str, end_iso: str):
    base=os.environ.get("GW_BASE","https://gandalf.gammawizard.com").rstrip("/")
    token=_sanitize_token(os.environ.get("GW_TOKEN","") or "")
    dumpdir=os.environ.get("GW_DUMP_DIR","gw_dump")
    os.makedirs(dumpdir, exist_ok=True)
    params={"start":start_iso, "end":end_iso}

    def _hit(tok, path):
        h={"Accept":"application/json","User-Agent":"gw-inspect/1.0"}
        if tok: h["Authorization"]=f"Bearer {tok}"
        return requests.get(f"{base}/{path.lstrip('/')}", params=params, headers=h, timeout=_gw_timeout())

    for path in paths:
        print(f"\n=== Hitting {path} start={start_iso} end={end_iso} ===")
        r=_hit(token, path) if token else None
        if (r is None) or (r.status_code in (401,403)):
            print("Auth failed or no token → logging in…")
            token=gw_login_token()
            r=_hit(token, path)

        print(f"HTTP {r.status_code}  content-type={r.headers.get('content-type','?')}  bytes={len(r.content)}")
        body_text=r.text

        # Dump full body to file (even if not valid JSON)
        ts=datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        fp=os.path.join(dumpdir, f"{ts}_{_slug(path)}.json")
        with open(fp,"w",encoding="utf-8") as f: f.write(body_text)
        print(f"→ Full response dumped to: {fp}")

        # Try to parse and summarize structure
        try:
            j=r.json()
        except Exception as e:
            print(f"!! Not JSON parseable: {e}\nFirst 500 chars:\n{body_text[:500]}")
            continue

        def preview(obj, depth=0):
            pad="  "*depth
            if isinstance(obj, dict):
                print(pad+f"dict keys ({len(obj)}): {list(obj.keys())[:12]}{' …' if len(obj)>12 else ''}")
                # find a likely array to preview
                for k in ("data","rows","list","candles","items","result"):
                    if k in obj and isinstance(obj[k], list):
                        print(pad+f"  {k}: list len={len(obj[k])}")
                        if obj[k]:
                            print(pad+"  first item keys:", list(obj[k][0].keys())[:20])
                        break
            elif isinstance(obj, list):
                print(pad+f"list len={len(obj)}")
                if obj:
                    if isinstance(obj[0], dict):
                        print(pad+"first item keys:", list(obj[0].keys())[:20])
                    else:
                        print(pad+"first 3 items:", obj[:3])
            else:
                print(pad+f"type={type(obj).__name__}")

        print("Structure preview:")
        preview(j)
        # Optional: show first item
        if isinstance(j, list) and j:
            print("Sample[0]:", json.dumps(j[0], indent=2)[:1000])
        elif isinstance(j, dict):
            for k in ("data","rows","list","candles"):
                if isinstance(j.get(k), list) and j[k]:
                    print(f"Sample {k}[0]:", json.dumps(j[k][0], indent=2)[:1000])
                    break

if __name__ == "__main__":
    days=int(os.environ.get("SETTLE_BACKFILL_DAYS","14"))
    t1=datetime.now(timezone.utc).date()
    t0=t1 - timedelta(days=days)
    start_iso=t0.isoformat()
    end_iso=t1.isoformat()

    # Provide your own path with GW_PATH, otherwise try known ones
    paths = [p.strip() for p in os.environ.get("GW_PATH","").split(",") if p.strip()]
    if not paths:
        paths = ["/rapi/Market/SpxClose", "/rapi/SpxClose"]  # add others to taste
    gw_inspect(paths, start_iso, end_iso)
