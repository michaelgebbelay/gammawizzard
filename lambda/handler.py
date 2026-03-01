#!/usr/bin/env python3
"""AWS Lambda handler for ConstantStableVerticals trading.

Wraps existing orchestrator scripts via subprocess — no trading code is modified.
EventBridge Scheduler invokes this with {"account": "schwab"|"tt-ira"|"tt-individual"}.
"""

import hashlib
import json
import os
import subprocess
import sys
import time

import boto3

TASK_ROOT = os.environ.get("LAMBDA_TASK_ROOT", "/var/task")
REPORT_STEPS = {"cs_summary_to_gsheet.py", "cs_performance_to_gsheet.py"}
DEFAULT_REPORT_OWNER = "tt-individual"
DEFAULT_REPORT_DELAY_SECS = 90

# ---------------------------------------------------------------------------
# Account configurations
# ---------------------------------------------------------------------------

ACCOUNTS = {
    "schwab": {
        "orchestrator": "scripts/trade/ConstantStable/orchestrator.py",
        "post_steps": [
            "scripts/data/cs_gw_signal_to_gsheet.py",
            "scripts/data/cs_trades_to_gsheet.py",
            "scripts/data/cs_tracking_to_gsheet.py",
            "scripts/data/cs_backfill_20260211.py",
            "scripts/data/cs_summary_to_gsheet.py",
            "scripts/data/cs_performance_to_gsheet.py",
            "scripts/trade/ConstantStable/edge_guard.py",
        ],
        "token_ssm_path": "/gamma/schwab/token_json",
        "token_file": "/tmp/schwab_token.json",
        "env_from_ssm": {
            "SCHWAB_APP_KEY": "/gamma/schwab/app_key",
            "SCHWAB_APP_SECRET": "/gamma/schwab/app_secret",
        },
        "static_env": {
            "SCHWAB_TOKEN_PATH": "/tmp/schwab_token.json",
            "CS_UNIT_DOLLARS": "10000",
            "CS_ACCOUNT_LABEL": "schwab",
            "CS_COST_PER_CONTRACT": "0.97",
        },
    },
    "tt-ira": {
        "orchestrator": "TT/Script/ConstantStable/orchestrator.py",
        "post_steps": [
            "scripts/data/cs_gw_signal_to_gsheet.py",
            "TT/data/cs_trades_to_gsheet.py",
            "scripts/data/cs_tracking_to_gsheet.py",
            "scripts/data/cs_tt_close_status.py",
            "scripts/data/cs_backfill_20260211.py",
            "scripts/data/cs_summary_to_gsheet.py",
            "scripts/data/cs_performance_to_gsheet.py",
            "TT/Script/ConstantStable/edge_guard.py",
        ],
        "token_ssm_path": "/gamma/tt/token_json",
        "token_file": "/tmp/tt_token.json",
        "env_from_ssm": {
            # TT credentials
            "TT_CLIENT_ID": "/gamma/tt/client_id",
            "TT_CLIENT_SECRET": "/gamma/tt/client_secret",
            # Schwab creds (needed by edge guard / data scripts)
            "SCHWAB_APP_KEY": "/gamma/schwab/app_key",
            "SCHWAB_APP_SECRET": "/gamma/schwab/app_secret",
        },
        "static_env": {
            "TT_ACCOUNT_NUMBER": "5WT20360",
            "TT_TOKEN_PATH": "/tmp/tt_token.json",
            "TT_QUOTE_TOKEN_PATH": "/tmp/tt_quote_token.json",
            "SCHWAB_TOKEN_PATH": "/tmp/schwab_token.json",
            "CS_UNIT_DOLLARS": "10000",
            "CS_ACCOUNT_LABEL": "tt-ira",
            "CS_COST_PER_CONTRACT": "1.72",
        },
    },
    "tt-individual": {
        "orchestrator": "TT/Script/ConstantStable/orchestrator.py",
        "post_steps": [
            "scripts/data/cs_gw_signal_to_gsheet.py",
            "TT/data/cs_trades_to_gsheet.py",
            "scripts/data/cs_tracking_to_gsheet.py",
            "scripts/data/cs_tt_close_status.py",
            "scripts/data/cs_summary_to_gsheet.py",
            "scripts/data/cs_performance_to_gsheet.py",
            "TT/Script/ConstantStable/close_orders.py",
            "TT/Script/ConstantStable/edge_guard.py",
        ],
        "token_ssm_path": "/gamma/tt/token_json",
        "token_file": "/tmp/tt_token.json",
        "env_from_ssm": {
            "TT_CLIENT_ID": "/gamma/tt/client_id",
            "TT_CLIENT_SECRET": "/gamma/tt/client_secret",
            "SCHWAB_APP_KEY": "/gamma/schwab/app_key",
            "SCHWAB_APP_SECRET": "/gamma/schwab/app_secret",
        },
        "static_env": {
            "TT_ACCOUNT_NUMBER": "5WT09219",
            "TT_TOKEN_PATH": "/tmp/tt_token.json",
            "TT_QUOTE_TOKEN_PATH": "/tmp/tt_quote_token.json",
            "SCHWAB_TOKEN_PATH": "/tmp/schwab_token.json",
            "CS_UNIT_DOLLARS": "10000",
            "CS_ACCOUNT_LABEL": "tt-individual",
            "CS_COST_PER_CONTRACT": "1.72",
            "CS_CLOSE_ORDERS_ENABLE": "1",
        },
    },
    "manual": {
        "orchestrator": "scripts/trade/manual_trades.py",
        "post_steps": [],
        "token_ssm_path": "/gamma/schwab/token_json",
        "token_file": "/tmp/schwab_token.json",
        "env_from_ssm": {
            "SCHWAB_APP_KEY": "/gamma/schwab/app_key",
            "SCHWAB_APP_SECRET": "/gamma/schwab/app_secret",
            "TT_CLIENT_ID": "/gamma/tt/client_id",
            "TT_CLIENT_SECRET": "/gamma/tt/client_secret",
        },
        "static_env": {
            "SCHWAB_TOKEN_PATH": "/tmp/schwab_token.json",
            "TT_TOKEN_PATH": "/tmp/tt_token.json",
            "TT_QUOTE_TOKEN_PATH": "/tmp/tt_quote_token.json",
            "CS_UNIT_DOLLARS": "10000",
            "CS_ACCOUNT_LABEL": "manual",
            "CS_COST_PER_CONTRACT": "0.97",
            "CS_MANUAL_TAB": "Manual_Trades",
        },
    },
    "morning-check": {
        "orchestrator": "scripts/trade/ConstantStable/morning_check.py",
        "post_steps": [],
        "token_ssm_path": "/gamma/schwab/token_json",
        "token_file": "/tmp/schwab_token.json",
        "env_from_ssm": {
            "SCHWAB_APP_KEY": "/gamma/schwab/app_key",
            "SCHWAB_APP_SECRET": "/gamma/schwab/app_secret",
            "SMTP_USER": "/gamma/shared/smtp_user",
            "SMTP_PASS": "/gamma/shared/smtp_pass",
        },
        "static_env": {
            "SCHWAB_TOKEN_PATH": "/tmp/schwab_token.json",
            "CS_ACCOUNT_LABEL": "morning-check",
            "CS_MORNING_DD_ALERT_PCT": "0.20",
        },
    },
}

# Env vars shared across all accounts (match current workflow defaults)
COMMON_ENV = {
    "PYTHONUNBUFFERED": "1",
    "GW_BASE": "https://gandalf.gammawizard.com",
    "GW_ENDPOINT": "rapi/GetUltraPureConstantStable",
    "CS_VOL_FIELD": "VixOne",
    "CS_VIX_BREAKS": "0.1636779,0.3276571,0.3702533,0.4514141",
    "CS_VIX_MULTS": "2,3,4,4,10",
    "CS_RR_CREDIT_RATIOS": "0.50,0.6667,0.75,1.00,1.00",
    "CS_IC_SHORT_MULTS": "",
    "CS_IC_LONG_MULTS": "",
    "CS_LOG_PATH": "/tmp/cs_trades.csv",
    "CS_GUARD_NO_CLOSE": "1",
    "CS_GUARD_FAIL_ACTION": "SKIP_ALL",
    "CS_TOPUP_ENABLE": "1",
    "CS_GW_SIGNAL_TAB": "GW_Signal",
    "CS_EDGE_GSHEET_TAB": "ConstantStableEdge",
    "CS_TRACKING_TAB": "CS_Tracking",
    "TT_ACCOUNT_NUMBERS": "5WT09219:tt-individual,5WT20360:tt-ira",
    "CS_SUMMARY_TAB": "CS_Summary",
    "CS_PERFORMANCE_TAB": "CS_Performance",
    "CS_REPORT_OWNER": DEFAULT_REPORT_OWNER,
    "CS_REPORT_DELAY_SECS": str(DEFAULT_REPORT_DELAY_SECS),
    "VERT_STEP_WAIT": "15",
    "VERT_POLL_SECS": "2.0",
    "VERT_CANCEL_SETTLE": "1.0",
    "VERT_MAX_LADDER": "3",
    "VERT_DRY_RUN": "false",
    "VERT_CANCEL_TRIES": "4",
}

# Shared SSM params (same for all accounts)
SHARED_SSM = {
    "GSHEET_ID": "/gamma/shared/gsheet_id",
    "GOOGLE_SERVICE_ACCOUNT_JSON": "/gamma/shared/google_sa_json",
    "GW_EMAIL": "/gamma/shared/gw_email",
    "GW_PASSWORD": "/gamma/shared/gw_password",
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_ssm = None


def ssm_client():
    global _ssm
    if _ssm is None:
        _ssm = boto3.client("ssm")
    return _ssm


def get_ssm_param(name):
    """Fetch a single SSM parameter (decrypted)."""
    resp = ssm_client().get_parameter(Name=name, WithDecryption=True)
    return resp["Parameter"]["Value"]


def get_ssm_params(names):
    """Fetch multiple SSM parameters in batches of 10."""
    result = {}
    name_list = list(names)
    for i in range(0, len(name_list), 10):
        batch = name_list[i : i + 10]
        resp = ssm_client().get_parameters(Names=batch, WithDecryption=True)
        for p in resp["Parameters"]:
            result[p["Name"]] = p["Value"]
    return result


def seed_file(path, content):
    """Write content to a file, creating parent dirs as needed."""
    os.makedirs(os.path.dirname(path) or "/tmp", exist_ok=True)
    with open(path, "w") as f:
        f.write(content)


def file_hash(path):
    """SHA-256 of file contents, or None if missing."""
    try:
        with open(path, "rb") as f:
            return hashlib.sha256(f.read()).hexdigest()
    except FileNotFoundError:
        return None


def persist_token_if_changed(ssm_path, file_path, original_hash):
    """Write token file back to SSM if its hash changed."""
    current_hash = file_hash(file_path)
    if current_hash and current_hash != original_hash:
        with open(file_path, "r") as f:
            content = f.read()
        ssm_client().put_parameter(
            Name=ssm_path,
            Value=content,
            Type="SecureString",
            Overwrite=True,
        )
        print(f"Token persisted to SSM: {ssm_path}")
        return True
    print(f"Token unchanged: {ssm_path}")
    return False


def run_script(script, env, timeout_s=100, label=""):
    """Run a Python script as a subprocess from the task root."""
    full_path = os.path.join(TASK_ROOT, script)
    if not os.path.isfile(full_path):
        print(f"SKIP {label or script}: file not found")
        return -1
    print(f"RUN  {label or script}")
    try:
        result = subprocess.run(
            [sys.executable, full_path],
            env=env,
            cwd=TASK_ROOT,
            capture_output=True,
            text=True,
            timeout=timeout_s,
        )
    except subprocess.TimeoutExpired as e:
        print(f"TIMEOUT {label or script} after {timeout_s}s")
        if e.stdout:
            print(e.stdout[-2000:])
        if e.stderr:
            print(e.stderr[-2000:])
        return 124  # standard timeout exit code
    if result.stdout:
        for line in result.stdout.rstrip().split("\n"):
            print(f"  {line}")
    if result.stderr:
        for line in result.stderr.rstrip().split("\n"):
            print(f"  ERR: {line}")
    if result.returncode != 0:
        print(f"EXIT {result.returncode}: {label or script}")
    return result.returncode


# ---------------------------------------------------------------------------
# Sim chain collection (S3-backed)
# ---------------------------------------------------------------------------


def _handle_sim_collect(event):
    """Collect SPX chain data and persist to S3 for the trading simulation.

    Uses Schwab API for chain data (full Greeks/IV) with GammaWizard overlay.
    Event payload: {"account": "sim-collect", "phase": "open|mid|close|close5"}
    """
    import logging
    logging.basicConfig(level=logging.INFO, force=True)

    t0 = time.time()
    phase = event.get("phase", "")
    valid_phases = ("open", "mid", "close", "close5")

    if phase not in valid_phases:
        msg = f"Invalid phase: {phase!r}. Must be one of {valid_phases}"
        print(msg)
        return {"status": "error", "message": msg}

    print(f"=== sim-collect | phase={phase} ===")

    # 1. Seed credentials from SSM
    ssm_paths = [
        "/gamma/schwab/token_json",
        "/gamma/schwab/app_key",
        "/gamma/schwab/app_secret",
        "/gamma/shared/gw_email",
        "/gamma/shared/gw_password",
    ]
    params = get_ssm_params(ssm_paths)
    print(f"Fetched {len(params)} SSM params")

    # Seed Schwab token file
    schwab_token = params.get("/gamma/schwab/token_json", "")
    if schwab_token:
        seed_file("/tmp/schwab_token.json", schwab_token)
    else:
        print("WARNING: no Schwab token from SSM")

    token_hash = file_hash("/tmp/schwab_token.json")

    # Set env vars for Schwab API + GW API
    os.environ["SCHWAB_TOKEN_PATH"] = "/tmp/schwab_token.json"
    os.environ["SCHWAB_APP_KEY"] = params.get("/gamma/schwab/app_key", "")
    os.environ["SCHWAB_APP_SECRET"] = params.get("/gamma/schwab/app_secret", "")
    os.environ["GW_EMAIL"] = params.get("/gamma/shared/gw_email", "")
    os.environ["GW_PASSWORD"] = params.get("/gamma/shared/gw_password", "")
    os.environ.setdefault("GW_BASE", "https://gandalf.gammawizard.com")
    os.environ.setdefault("GW_ENDPOINT", "rapi/GetUltraPureConstantStable")

    # 2. Fetch chain via Schwab API
    from datetime import date as date_type, datetime, timedelta
    from zoneinfo import ZoneInfo
    from scripts.schwab_token_keeper import schwab_client
    from sim.data.chain_snapshot import parse_schwab_chain
    from sim.data.features import enrich
    from sim.data.gw_client import fetch_gw_data
    from sim.data.s3_cache import s3_put_json

    today = date_type.today()
    today_str = today.isoformat()
    print(f"Collecting: date={today_str}, phase={phase}")

    try:
        c = schwab_client()

        # Fetch SPX option chain (1-3 DTE window to capture 1DTE)
        from_date = today
        to_date = today + timedelta(days=3)
        resp = c.get_option_chain(
            "$SPX",
            contract_type=c.Options.ContractType.ALL,
            strike_count=40,
            include_underlying_quote=True,
            from_date=from_date,
            to_date=to_date,
            option_type=c.Options.Type.ALL,
        )
        resp.raise_for_status()
        raw = resp.json()

        # Fetch VIX
        vix = 0.0
        try:
            vix_resp = c.get_quote("$VIX")
            vix_resp.raise_for_status()
            vix_data = vix_resp.json()
            for key, val in vix_data.items():
                if isinstance(val, dict):
                    q = val.get("quote", val)
                    v = q.get("lastPrice") or q.get("last") or q.get("mark")
                    if v is not None:
                        vix = float(v)
                        break
        except Exception as e:
            print(f"VIX fetch failed (non-fatal): {e}")

        # Parse into ChainSnapshot
        snapshot = parse_schwab_chain(raw, phase=phase, vix=vix)

        if not snapshot.expirations:
            return {"status": "error", "message": "No PM-settled expirations found",
                    "duration_s": round(time.time() - t0, 1)}

        if not snapshot.contracts:
            return {"status": "error", "message": "No contracts in chain",
                    "duration_s": round(time.time() - t0, 1)}

        # Log diagnostics
        has_greeks = sum(1 for oc in snapshot.contracts.values() if oc.delta != 0)
        has_bid = sum(1 for oc in snapshot.contracts.values() if oc.bid > 0)
        print(f"Chain: {len(snapshot.contracts)} contracts, "
              f"SPX={snapshot.underlying_price:.0f}, VIX={vix:.1f}, "
              f"greeks={has_greeks}/{len(snapshot.contracts)}, "
              f"bids={has_bid}/{len(snapshot.contracts)}")

        # 3. Save raw chain to S3
        timestamp = datetime.now(ZoneInfo("America/New_York")).isoformat()
        s3_put_json(today_str, f"{phase}.json", {
            "trading_date": today_str,
            "phase": phase,
            "source": "schwab",
            "vix": vix,
            "fetched_at": timestamp,
            "chain": raw,
        })

        # 4. Fetch + save GW data (non-fatal)
        gw_data = None
        try:
            gw_data = fetch_gw_data(today_str)
            if gw_data:
                s3_put_json(today_str, f"gw_{phase}.json", gw_data)
                print(f"GW: VIX1D={gw_data.get('vix_1d')}, RV={gw_data.get('rv')}")
            else:
                print("GW data unavailable (non-fatal)")
        except Exception as e:
            print(f"GW fetch failed (non-fatal): {e}")

        # 5. Save FeaturePack to S3
        underlying_quote = raw.get("underlying", {})
        prev_close = float(underlying_quote.get("previousClose", 0) or 0)
        fp = enrich(snapshot, prev_close=prev_close, gw_data=gw_data)
        s3_put_json(today_str, f"features_{phase}.json", fp.to_dict())

        print(f"FeaturePack: EM={fp.atm_straddle_mid:.1f}, IV={fp.iv_atm:.1f}%")

    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        return {"status": "error", "message": str(e),
                "duration_s": round(time.time() - t0, 1)}

    # 6. Persist Schwab token if refreshed
    try:
        persist_token_if_changed("/gamma/schwab/token_json",
                                 "/tmp/schwab_token.json", token_hash)
    except Exception as e:
        print(f"ERROR persisting Schwab token: {e}")

    duration = round(time.time() - t0, 1)
    print(f"=== DONE sim-collect | phase={phase} | {duration}s ===")
    return {
        "status": "ok",
        "account": "sim-collect",
        "phase": phase,
        "date": today_str,
        "contracts": len(snapshot.contracts),
        "spx": snapshot.underlying_price,
        "greeks": has_greeks,
        "duration_s": duration,
    }


# ---------------------------------------------------------------------------
# Lambda entry point
# ---------------------------------------------------------------------------


def lambda_handler(event, context):
    t0 = time.time()
    account = event.get("account", "")
    dry_run = event.get("dry_run", False)

    # Warm-up ping — just loads the container, no work done
    if account == "warmup":
        print("WARMUP ping — container is warm")
        return {"status": "ok", "account": "warmup", "duration_s": 0}

    # Sim chain collection — separate flow (no subprocess, writes to S3)
    if account == "sim-collect":
        return _handle_sim_collect(event)

    if account not in ACCOUNTS:
        msg = f"Unknown account: {account!r}. Expected one of {list(ACCOUNTS)}"
        print(msg)
        return {"status": "error", "message": msg}

    cfg = ACCOUNTS[account]
    print(f"=== {account} | orchestrator={cfg['orchestrator']} ===")

    # -- 1. Collect all SSM param names we need --
    ssm_names = {}
    ssm_names.update(cfg["env_from_ssm"])          # env_var -> ssm_path
    ssm_names.update(SHARED_SSM)                    # env_var -> ssm_path
    ssm_names["_token"] = cfg["token_ssm_path"]     # primary token

    # For TT accounts, also fetch Schwab token (edge guard needs it)
    if account.startswith("tt-"):
        ssm_names["_schwab_token"] = "/gamma/schwab/token_json"

    # Manual account needs both Schwab (primary) + TT token
    if account == "manual":
        ssm_names["_tt_token"] = "/gamma/tt/token_json"

    all_ssm_paths = list(set(ssm_names.values()))
    params = get_ssm_params(all_ssm_paths)
    print(f"Fetched {len(params)}/{len(all_ssm_paths)} SSM params")

    # -- 2. Build subprocess environment --
    env = dict(os.environ)
    env.update(COMMON_ENV)
    env.update(cfg["static_env"])

    if dry_run:
        env["VERT_DRY_RUN"] = "true"

    # Map SSM values to env vars
    for env_key, ssm_path in cfg["env_from_ssm"].items():
        env[env_key] = params.get(ssm_path, "")

    for env_key, ssm_path in SHARED_SSM.items():
        env[env_key] = params.get(ssm_path, "")

    # -- 3. Seed token files --
    token_content = params.get(cfg["token_ssm_path"], "")
    if token_content:
        seed_file(cfg["token_file"], token_content)
    else:
        print(f"WARNING: no token content from {cfg['token_ssm_path']}")

    # Schwab token keeper reads SCHWAB_TOKEN_JSON env var to auto-seed
    if account in ("schwab", "morning-check"):
        env["SCHWAB_TOKEN_JSON"] = token_content
    elif account == "manual":
        # Manual needs both Schwab + TT tokens
        env["SCHWAB_TOKEN_JSON"] = token_content
        tt_token = params.get("/gamma/tt/token_json", "")
        if tt_token:
            seed_file("/tmp/tt_token.json", tt_token)
            env["TT_TOKEN_JSON"] = tt_token
    else:
        # TT: set token content as env var (orchestrator passes to placer)
        env["TT_TOKEN_JSON"] = token_content
        # Also seed Schwab token for edge guard
        sw_token = params.get("/gamma/schwab/token_json", "")
        if sw_token:
            seed_file("/tmp/schwab_token.json", sw_token)
            env["SCHWAB_TOKEN_JSON"] = sw_token

    token_hash = file_hash(cfg["token_file"])
    tt_token_hash = file_hash("/tmp/tt_token.json") if account == "manual" else None
    schwab_hash = file_hash("/tmp/schwab_token.json") if account.startswith("tt-") else None

    # Ensure /tmp writability for logs
    os.makedirs("/tmp/logs", exist_ok=True)

    # -- 4. Wait for signal to stabilize (scheduled accounts only) --
    if account in ("schwab", "tt-ira", "tt-individual"):
        delay = int(os.environ.get("CS_TRADE_DELAY_SECS", "30"))
        if delay > 0:
            print(f"Signal delay: sleeping {delay}s")
            time.sleep(delay)

    # -- 4b. Run orchestrator (critical path) --
    orch_timeout = 330 if account == "manual" else 100
    orch_rc = run_script(cfg["orchestrator"], env, timeout_s=orch_timeout, label="orchestrator")

    # -- 5. Post-trade steps (best-effort, time-permitting) --
    remaining_ms = context.get_remaining_time_in_millis() if context else 30000
    remaining_s = max(5, int(remaining_ms / 1000) - 5)
    report_owner = (env.get("CS_REPORT_OWNER") or DEFAULT_REPORT_OWNER).strip()
    report_delay_secs = int(env.get("CS_REPORT_DELAY_SECS") or str(DEFAULT_REPORT_DELAY_SECS))
    report_delay_applied = False

    for step in cfg.get("post_steps", []):
        try:
            step_name = os.path.basename(step)
            is_report_step = step_name in REPORT_STEPS

            # Prevent concurrent full-sheet rewrites by allowing only one account
            # invocation to run reporting scripts.
            if is_report_step and account != report_owner:
                print(f"SKIP {step_name}: reporting owner is {report_owner}, current={account}")
                continue

            # Give other account invocations time to finish tracking updates first.
            if is_report_step and not report_delay_applied and report_delay_secs > 0:
                print(f"Reporting delay: sleeping {report_delay_secs}s before {step_name}")
                time.sleep(report_delay_secs)
                report_delay_applied = True

            step_timeout = min(30, remaining_s)
            run_script(step, env, timeout_s=step_timeout, label=step_name)
            remaining_ms = context.get_remaining_time_in_millis() if context else 10000
            remaining_s = max(5, int(remaining_ms / 1000) - 5)
        except Exception as e:
            print(f"WARN post-step {step}: {e}")

    # -- 6. Persist tokens back to SSM if refreshed --
    try:
        persist_token_if_changed(cfg["token_ssm_path"], cfg["token_file"], token_hash)
    except Exception as e:
        print(f"ERROR persisting token: {e}")

    if account.startswith("tt-") and schwab_hash:
        try:
            persist_token_if_changed(
                "/gamma/schwab/token_json", "/tmp/schwab_token.json", schwab_hash
            )
        except Exception as e:
            print(f"ERROR persisting schwab token: {e}")

    if account == "manual" and tt_token_hash:
        try:
            persist_token_if_changed(
                "/gamma/tt/token_json", "/tmp/tt_token.json", tt_token_hash
            )
        except Exception as e:
            print(f"ERROR persisting tt token: {e}")

    # -- 7. Return result --
    duration = round(time.time() - t0, 1)
    status = "ok" if orch_rc == 0 else "error"
    print(f"=== DONE {account} | status={status} | {duration}s ===")

    return {
        "status": status,
        "account": account,
        "orchestrator_rc": orch_rc,
        "duration_s": duration,
    }
