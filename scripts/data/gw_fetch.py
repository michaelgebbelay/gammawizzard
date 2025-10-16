#!/usr/bin/env python3
# Batch fetcher for multiple GammaWizard endpoints.
# Reads list from CLI args or env GW_ENDPOINTS (comma-separated).
import os, sys, json, re, datetime as dt
from gw_api import gw_get

def sanitize_name(s: str) -> str:
    s = (s or "").strip().strip("/")
    s = s.replace("rapi/", "")
    return re.sub(r'[^A-Za-z0-9_.-]+', '_', s)

def main():
    eps = sys.argv[1:]
    if not eps:
        env_list = os.environ.get("GW_ENDPOINTS", "")
        if env_list:
            eps = [e.strip() for e in env_list.split(",") if e.strip()]
    if not eps:
        eps = ["rapi/GetUltraSupreme", "rapi/GetLeoCross"]

    save_dir = os.environ.get("GW_SAVE_DIR", "").strip()
    stamp = dt.datetime.utcnow().strftime("%Y%m%d")

    for ep in eps:
        data = gw_get(ep)
        print(f"===== {ep} =====")
        print(json.dumps(data, indent=2, sort_keys=False))
        print()
        if save_dir:
            os.makedirs(save_dir, exist_ok=True)
            name = sanitize_name(ep)
            path = os.path.join(save_dir, f"{name}_{stamp}.json")
            with open(path, "w") as f:
                json.dump(data, f, indent=2)
            print(f"[saved] {path}")

if __name__ == "__main__":
    sys.exit(main())
