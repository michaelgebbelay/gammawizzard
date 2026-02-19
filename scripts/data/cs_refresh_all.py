#!/usr/bin/env python3
"""
Run all CS reporting scripts in correct dependency order.

One command = full reporting pipeline refresh.

Dependency order:
  1. cs_gw_signal_to_gsheet.py    - populates GW_Signal
  2. cs_api_to_tracking.py        - fills CS_Tracking gaps from APIs
  3. cs_tt_close_status.py        - populates CS_TT_Close for all accounts
  4. cs_summary_to_gsheet.py      - reads CS_Tracking + GW_Signal + CS_TT_Close
  5. cs_performance_to_gsheet.py  - reads CS_Summary

Usage:
  # With .env file sourced:
  python scripts/data/cs_refresh_all.py

  # Load env vars from SSM:
  python scripts/data/cs_refresh_all.py --from-ssm

  # Skip steps or strict mode:
  python scripts/data/cs_refresh_all.py --skip gw_signal --strict
"""

import argparse
import os
import subprocess
import sys
import time

TAG = "CS_REFRESH"

STEPS = [
    {
        "name": "gw_signal",
        "script": "scripts/data/cs_gw_signal_to_gsheet.py",
        "description": "GW Signal -> GW_Signal tab",
        "timeout": 30,
    },
    {
        "name": "api_to_tracking",
        "script": "scripts/data/cs_api_to_tracking.py",
        "description": "API fills -> CS_Tracking (gap fill)",
        "timeout": 60,
    },
    {
        "name": "tt_close_status",
        "script": "scripts/data/cs_tt_close_status.py",
        "description": "Close fills -> CS_TT_Close",
        "timeout": 30,
    },
    {
        "name": "summary",
        "script": "scripts/data/cs_summary_to_gsheet.py",
        "description": "CS_Tracking + GW_Signal -> CS_Summary",
        "timeout": 30,
    },
    {
        "name": "performance",
        "script": "scripts/data/cs_performance_to_gsheet.py",
        "description": "CS_Summary -> CS_Performance",
        "timeout": 30,
    },
]


def find_repo_root():
    cur = os.path.abspath(os.path.dirname(__file__))
    while True:
        if os.path.isdir(os.path.join(cur, "scripts")):
            return cur
        parent = os.path.dirname(cur)
        if parent == cur:
            raise RuntimeError("Could not find repo root")
        cur = parent


def load_ssm_env():
    """Load key env vars from SSM Parameter Store."""
    import boto3
    ssm = boto3.client("ssm")

    params_to_fetch = {
        "GSHEET_ID": "/gamma/shared/gsheet_id",
        "GOOGLE_SERVICE_ACCOUNT_JSON": "/gamma/shared/google_sa_json",
        "GW_EMAIL": "/gamma/shared/gw_email",
        "GW_PASSWORD": "/gamma/shared/gw_password",
        "SCHWAB_APP_KEY": "/gamma/schwab/app_key",
        "SCHWAB_APP_SECRET": "/gamma/schwab/app_secret",
        "TT_CLIENT_ID": "/gamma/tt/client_id",
        "TT_CLIENT_SECRET": "/gamma/tt/client_secret",
    }

    token_params = {
        "/gamma/schwab/token_json": "/tmp/schwab_token.json",
        "/gamma/tt/token_json": "/tmp/tt_token.json",
    }

    all_paths = list(params_to_fetch.values()) + list(token_params.keys())
    fetched = {}
    for i in range(0, len(all_paths), 10):
        batch = all_paths[i:i + 10]
        resp = ssm.get_parameters(Names=batch, WithDecryption=True)
        for p in resp["Parameters"]:
            fetched[p["Name"]] = p["Value"]

    for env_key, ssm_path in params_to_fetch.items():
        val = fetched.get(ssm_path)
        if val:
            os.environ[env_key] = val

    for ssm_path, file_path in token_params.items():
        val = fetched.get(ssm_path)
        if val:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, "w") as f:
                f.write(val)

    os.environ.setdefault("SCHWAB_TOKEN_PATH", "/tmp/schwab_token.json")
    os.environ.setdefault("TT_TOKEN_PATH", "/tmp/tt_token.json")
    os.environ.setdefault("TT_ACCOUNT_NUMBERS", "5WT09219:tt-individual,5WT20360:tt-ira")

    print(f"{TAG}: loaded {len(fetched)} SSM params")


def run_step(repo_root, step, env):
    script_path = os.path.join(repo_root, step["script"])
    if not os.path.isfile(script_path):
        print(f"{TAG}: SKIP {step['name']} -- {script_path} not found")
        return -1

    print(f"\n{TAG}: === {step['name']}: {step['description']} ===")
    t0 = time.time()

    try:
        result = subprocess.run(
            [sys.executable, script_path],
            env=env,
            cwd=repo_root,
            capture_output=True,
            text=True,
            timeout=step["timeout"],
        )
    except subprocess.TimeoutExpired:
        print(f"{TAG}: TIMEOUT {step['name']} after {step['timeout']}s")
        return 124

    if result.stdout:
        for line in result.stdout.rstrip().split("\n"):
            print(f"  {line}")
    if result.stderr:
        for line in result.stderr.rstrip().split("\n"):
            print(f"  ERR: {line}")

    elapsed = round(time.time() - t0, 1)
    status = "OK" if result.returncode == 0 else f"EXIT {result.returncode}"
    print(f"{TAG}: {step['name']} {status} ({elapsed}s)")
    return result.returncode


def main():
    parser = argparse.ArgumentParser(description="Refresh all CS reporting")
    parser.add_argument("--from-ssm", action="store_true",
                        help="Load env vars from SSM Parameter Store")
    parser.add_argument("--strict", action="store_true",
                        help="Abort on first failure")
    parser.add_argument("--skip", type=str, default="",
                        help="Comma-separated step names to skip")
    args = parser.parse_args()

    repo_root = find_repo_root()

    if args.from_ssm:
        load_ssm_env()

    skip_set = {s.strip() for s in args.skip.split(",") if s.strip()}
    strict = args.strict

    env = dict(os.environ)
    env["PYTHONUNBUFFERED"] = "1"
    env.setdefault("GW_BASE", "https://gandalf.gammawizard.com")
    env.setdefault("GW_ENDPOINT", "rapi/GetUltraPureConstantStable")
    env.setdefault("CS_VOL_FIELD", "VixOne")
    env.setdefault("CS_VIX_BREAKS", "0.1636779,0.3276571,0.3702533,0.4514141")
    env.setdefault("CS_VIX_MULTS", "2,3,4,4,10")
    env.setdefault("TT_ACCOUNT_NUMBERS", "5WT09219:tt-individual,5WT20360:tt-ira")

    results = {}
    t0 = time.time()

    for step in STEPS:
        if step["name"] in skip_set:
            print(f"\n{TAG}: SKIP {step['name']} (user excluded)")
            continue

        rc = run_step(repo_root, step, env)
        results[step["name"]] = rc

        if rc != 0 and strict:
            print(f"\n{TAG}: ABORT -- {step['name']} failed (strict mode)")
            return 1

    elapsed = round(time.time() - t0, 1)
    failed = [name for name, rc in results.items() if rc != 0]

    print(f"\n{TAG}: === DONE ({elapsed}s) ===")
    for name, rc in results.items():
        status = "OK" if rc == 0 else f"FAILED (exit {rc})"
        print(f"  {name}: {status}")

    if failed:
        print(f"\n{TAG}: {len(failed)} step(s) failed: {', '.join(failed)}")
        return 1 if strict else 0

    return 0


if __name__ == "__main__":
    sys.exit(main())
