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
from datetime import datetime, timezone

import boto3

TASK_ROOT = os.environ.get("LAMBDA_TASK_ROOT", "/var/task")
REPORT_STEPS = {"cs_summary_to_gsheet.py", "cs_performance_to_gsheet.py"}
DEFAULT_REPORT_OWNER = "tt-individual"
DEFAULT_REPORT_DELAY_SECS = 90
DISABLE_SCHWAB_CS_DEFAULT = False

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
            "CS_UNIT_DOLLARS": "20000",
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
        ],
        "token_ssm_path": "/gamma/tt/token_json",
        "token_file": "/tmp/tt_token.json",
        "env_from_ssm": {
            "TT_CLIENT_ID": "/gamma/tt/client_id",
            "TT_CLIENT_SECRET": "/gamma/tt/client_secret",
        },
        "static_env": {
            "TT_ACCOUNT_NUMBER": "5WT20360",
            "TT_TOKEN_PATH": "/tmp/tt_token.json",
            "TT_QUOTE_TOKEN_PATH": "/tmp/tt_quote_token.json",
            "CS_UNIT_DOLLARS": "20000",
            "CS_ACCOUNT_LABEL": "tt-ira",
            "CS_COST_PER_CONTRACT": "1.72",
            "CS_VIX_MULTS": "1,1,1,1,1",
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
        ],
        "token_ssm_path": "/gamma/tt/token_json",
        "token_file": "/tmp/tt_token.json",
        "env_from_ssm": {
            "TT_CLIENT_ID": "/gamma/tt/client_id",
            "TT_CLIENT_SECRET": "/gamma/tt/client_secret",
        },
        "static_env": {
            "TT_ACCOUNT_NUMBER": "5WT09219",
            "TT_TOKEN_PATH": "/tmp/tt_token.json",
            "TT_QUOTE_TOKEN_PATH": "/tmp/tt_quote_token.json",
            "CS_UNIT_DOLLARS": "3000",
            "CS_ACCOUNT_LABEL": "tt-individual",
            "CS_COST_PER_CONTRACT": "1.72",
            "CS_VIX_MULTS": "1,1,1,1,1",
            # "CS_CLOSE_ORDERS_ENABLE": "1",  # 50% profit-take disabled 2026-03-15
            "CS_CLOSE_ORDERS_ENABLE": "0",
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
            "CS_UNIT_DOLLARS": "20000",
            "CS_ACCOUNT_LABEL": "manual",
            "CS_COST_PER_CONTRACT": "0.97",
            "CS_MANUAL_TAB": "Manual_Trades",
        },
    },
    "butterfly": {
        "orchestrator": "scripts/trade/ButterflyTuesday/orchestrator.py",
        "post_steps": [
            "scripts/data/bf_trades_to_gsheet.py",
            "scripts/data/bf_eod_tracking.py",
            "scripts/data/reconcile_reporting.py",
        ],
        "token_ssm_path": "/gamma/schwab/token_json",
        "token_file": "/tmp/schwab_token.json",
        "env_from_ssm": {
            "SCHWAB_APP_KEY": "/gamma/schwab/app_key",
            "SCHWAB_APP_SECRET": "/gamma/schwab/app_secret",
        },
        "static_env": {
            "SCHWAB_TOKEN_PATH": "/tmp/schwab_token.json",
            "SIM_CACHE_BUCKET": "gamma-sim-cache",
            "BF_DRY_RUN": "0",
            "BF_LOG_PATH": "/tmp/logs/bf_trades.csv",
            "RECONCILE_CHECKS": "BF_Trades:trade_date:status:signal",
        },
    },
    "dualside": {
        "orchestrator": "scripts/trade/DualSide/orchestrator.py",
        "post_steps": [
            "scripts/data/ds_tracking_to_gsheet.py",
            "scripts/data/reconcile_reporting.py",
        ],
        "token_ssm_path": "/gamma/schwab/token_json",
        "token_file": "/tmp/schwab_token.json",
        "env_from_ssm": {
            "SCHWAB_APP_KEY": "/gamma/schwab/app_key",
            "SCHWAB_APP_SECRET": "/gamma/schwab/app_secret",
        },
        "static_env": {
            "SCHWAB_TOKEN_PATH": "/tmp/schwab_token.json",
            "DS_LOG_PATH": "/tmp/logs/dualside_trades.csv",
            "DS_DRY_RUN": "false",
            "CS_UNIT_DOLLARS": "15000",
            "CS_ACCOUNT_LABEL": "dualside",
            "RECONCILE_CHECKS": "DS_Tracking:trade_date:put_structure",
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
    "CS_VIX_MULTS": "1,1,1,1,1",
    "CS_RR_CREDIT_RATIOS": "",
    "CS_IC_SHORT_MULTS": "",
    "CS_LOG_PATH": "/tmp/cs_trades.csv",
    "CS_GUARD_NO_CLOSE": "1",
    "CS_GUARD_FAIL_ACTION": "SKIP_ALL",
    "CS_TOPUP": "1",
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
    "CS_GW_READY_ET": "16:13:31",
    "GAMMA_EVENT_DIR": "/tmp/gamma_events",
    "GAMMA_EVENT_PREFIX": "reporting/events",
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


def upload_event_files(env, trade_date: str) -> dict:
    """Upload EventWriter JSONL files from Lambda /tmp to S3.

    Without this step, runs that skip or place no orders vanish from reporting
    because EventWriter only writes local files under /tmp/gamma_events.
    """
    bucket = (
        env.get("GAMMA_EVENT_BUCKET")
        or os.environ.get("GAMMA_EVENT_BUCKET")
        or env.get("SIM_CACHE_BUCKET")
        or os.environ.get("SIM_CACHE_BUCKET")
        or ""
    ).strip()
    prefix = (env.get("GAMMA_EVENT_PREFIX") or os.environ.get("GAMMA_EVENT_PREFIX") or "reporting/events").strip("/")
    event_dir = env.get("GAMMA_EVENT_DIR") or os.environ.get("GAMMA_EVENT_DIR") or "/tmp/gamma_events"
    trade_path = os.path.join(event_dir, trade_date)

    stats = {
        "bucket": bucket,
        "prefix": prefix,
        "trade_date": trade_date,
        "files": 0,
        "uploaded": 0,
        "errors": 0,
    }

    if not bucket:
        print("EVENT_UPLOAD SKIP: no GAMMA_EVENT_BUCKET/SIM_CACHE_BUCKET configured")
        return stats
    if not os.path.isdir(trade_path):
        print(f"EVENT_UPLOAD SKIP: no event dir at {trade_path}")
        return stats

    s3 = boto3.client("s3")
    for name in sorted(os.listdir(trade_path)):
        if not name.endswith(".jsonl"):
            continue
        stats["files"] += 1
        local_path = os.path.join(trade_path, name)
        key = f"{prefix}/{trade_date}/{name}"
        try:
            s3.upload_file(local_path, bucket, key)
            stats["uploaded"] += 1
            print(f"EVENT_UPLOAD OK: s3://{bucket}/{key}")
        except Exception as e:
            stats["errors"] += 1
            print(f"EVENT_UPLOAD FAIL: {local_path} -> s3://{bucket}/{key} ({e})")

    return stats


def _finalize_bf_plan(env, orch_rc):
    """Patch a pending BF plan to ERROR when orchestrator failed/timed out.

    Prevents bf_trades_to_gsheet from appending a stale pending-plan row
    as if the day was successfully handled.
    """
    plan_path = env.get("BF_PLAN_PATH", "/tmp/bf_plan.json")
    try:
        with open(plan_path, "r") as f:
            plan = json.load(f)
        result = plan.get("result", {})
        if isinstance(result, dict) and result.get("pending"):
            reason = "TIMEOUT" if orch_rc == 124 else f"orch_rc={orch_rc}"
            plan["status"] = "ERROR"
            plan["reason"] = reason
            plan["result"] = {"error": True, "rc": orch_rc}
            with open(plan_path, "w") as f:
                json.dump(plan, f, indent=2)
            print(f"BF_PLAN finalized: pending -> ERROR ({reason})")
    except FileNotFoundError:
        pass  # No plan written yet — nothing to finalize
    except Exception as e:
        print(f"WARN BF_PLAN finalize: {e}")


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

        # Fetch SPX option chain (wide window to capture up to ~7 DTE)
        from_date = today
        to_date = today + timedelta(days=10)
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

        # Parse full chain into ChainSnapshot for validation
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
              f"{len(snapshot.expirations)} expirations ({snapshot.expirations}), "
              f"SPX={snapshot.underlying_price:.0f}, VIX={vix:.1f}, "
              f"greeks={has_greeks}/{len(snapshot.contracts)}, "
              f"bids={has_bid}/{len(snapshot.contracts)}")

        # 3. Save raw chain to S3 — split into per-DTE files
        #    close5.json = 1DTE (0-day expiration), close5_2dte.json = 2DTE, etc.
        #    This matches the naming convention used by ThetaData downloads
        #    and expected by all backtesting code.
        timestamp = datetime.now(ZoneInfo("America/New_York")).isoformat()

        # Build per-DTE chain dicts by splitting on expiration date.
        # ThetaData convention: DTE = Nth SPX expiration (trading days),
        # NOT calendar days.  Schwab exp_key suffix is calendar DTE which
        # diverges over weekends.  So we sort unique expiration dates and
        # assign ordinal positions: 1st exp → close5.json, 2nd → close5_2dte, etc.
        all_exp_dates = set()
        for side in ("callExpDateMap", "putExpDateMap"):
            for exp_key in raw.get(side, {}):
                exp_date_str = exp_key.split(":")[0]
                all_exp_dates.add(exp_date_str)
        # Skip today's expiration (0DTE) — these contracts are near-worthless
        # at close and break SE computation.  1st remaining exp = 1DTE = close5.json.
        all_exp_dates.discard(today_str)
        sorted_exps = sorted(all_exp_dates)
        # Map each expiration date to its ordinal DTE (1-based)
        exp_to_dte = {exp: i + 1 for i, exp in enumerate(sorted_exps)}

        dte_chains = {}  # ordinal_dte -> filtered chain dict
        for side in ("callExpDateMap", "putExpDateMap"):
            exp_map = raw.get(side, {})
            for exp_key, strikes_data in exp_map.items():
                exp_date_str = exp_key.split(":")[0]
                dte = exp_to_dte.get(exp_date_str, 0)
                if dte not in dte_chains:
                    # Copy top-level metadata (underlyingPrice, volatility, etc.)
                    dte_chains[dte] = {k: v for k, v in raw.items()
                                       if k not in ("callExpDateMap", "putExpDateMap")}
                    dte_chains[dte]["callExpDateMap"] = {}
                    dte_chains[dte]["putExpDateMap"] = {}
                dte_chains[dte][side][exp_key] = strikes_data

        # Save each DTE as a separate file (matches ThetaData naming)
        for dte, dte_raw in sorted(dte_chains.items()):
            if dte <= 1:
                filename = f"{phase}.json"
            else:
                filename = f"{phase}_{dte}dte.json"
            s3_put_json(today_str, filename, {
                "trading_date": today_str,
                "phase": phase,
                "source": "schwab",
                "vix": vix,
                "fetched_at": timestamp,
                "chain": dte_raw,
            })
            exp_date = sorted_exps[dte - 1] if dte <= len(sorted_exps) else "?"
            n_calls = sum(len(v) for v in dte_raw.get("callExpDateMap", {}).values())
            n_puts = sum(len(v) for v in dte_raw.get("putExpDateMap", {}).values())
            print(f"Saved {filename}: exp={exp_date}, {n_calls}C + {n_puts}P strikes")

        # Also save the full combined chain for reference
        s3_put_json(today_str, f"{phase}_full.json", {
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
# Daily P&L email handler
# ---------------------------------------------------------------------------


def _handle_cs_refresh(event, t0):
    """Run cs_refresh_all.py as a standalone post-close reporting job.

    Event payload: {"account": "cs-refresh", "skip": "gw_signal,chart_data"}
    The script loads its own SSM params via --from-ssm.
    """
    skip = event.get("skip", "")
    print(f"=== cs-refresh | skip={skip or 'none'} ===")

    script = os.path.join(TASK_ROOT, "scripts/data/cs_refresh_all.py")
    cmd = [sys.executable, script, "--from-ssm"]
    if skip:
        cmd += ["--skip", skip]

    env = dict(os.environ)
    env["PYTHONUNBUFFERED"] = "1"

    try:
        result = subprocess.run(
            cmd,
            env=env,
            cwd=TASK_ROOT,
            capture_output=True,
            text=True,
            timeout=300,
        )
        for line in (result.stdout or "").rstrip().split("\n"):
            print(line)
        if result.stderr:
            for line in result.stderr.rstrip().split("\n"):
                print(f"ERR: {line}")
        rc = result.returncode
    except subprocess.TimeoutExpired:
        rc = 124
        print("CS_REFRESH: TIMEOUT after 300s")
    except Exception as e:
        rc = 1
        print(f"CS_REFRESH: ERROR: {e}")

    duration = round(time.time() - t0, 1)
    status = "ok" if rc == 0 else "error"
    print(f"=== DONE cs-refresh | status={status} | {duration}s ===")
    return {
        "status": status,
        "account": "cs-refresh",
        "returncode": rc,
        "duration_s": duration,
    }


def _handle_daily_pnl(event, t0):
    """Pull Schwab + TT orders, compute P&L, send email with trade summary.

    Event payload: {"account": "daily-pnl", "dry_run": true/false,
                    "report_date": "YYYY-MM-DD"}
    Needs SSM: Schwab creds (orders) + TT creds (orders) + SMTP creds (email).
    """
    dry_run = event.get("dry_run", False)
    report_date = event.get("report_date")
    print(f"=== daily-pnl | dry_run={dry_run} | report_date={report_date or 'auto'} ===")

    # Fetch SSM params (Schwab + TT + SMTP)
    ssm_paths = {
        "SCHWAB_APP_KEY": "/gamma/schwab/app_key",
        "SCHWAB_APP_SECRET": "/gamma/schwab/app_secret",
        "_schwab_token": "/gamma/schwab/token_json",
        "_tt_token": "/gamma/tt/token_json",
        "TT_CLIENT_ID": "/gamma/tt/client_id",
        "TT_CLIENT_SECRET": "/gamma/tt/client_secret",
        "SMTP_USER": "/gamma/shared/smtp_user",
        "SMTP_PASS": "/gamma/shared/smtp_pass",
    }
    all_paths = list(set(ssm_paths.values()))
    params = get_ssm_params(all_paths)
    print(f"Fetched {len(params)}/{len(all_paths)} SSM params")

    # Set env vars for Schwab client + SMTP
    os.environ["SCHWAB_APP_KEY"] = params.get("/gamma/schwab/app_key", "")
    os.environ["SCHWAB_APP_SECRET"] = params.get("/gamma/schwab/app_secret", "")
    os.environ["SMTP_USER"] = params.get("/gamma/shared/smtp_user", "")
    os.environ["SMTP_PASS"] = params.get("/gamma/shared/smtp_pass", "")

    # Seed Schwab token
    token_content = params.get("/gamma/schwab/token_json", "")
    if token_content:
        token_path = "/tmp/schwab_token.json"
        with open(token_path, "w") as f:
            f.write(token_content)
        os.environ["SCHWAB_TOKEN_PATH"] = token_path

    # Seed TT token + creds
    tt_token = params.get("/gamma/tt/token_json", "")
    if tt_token:
        tt_token_path = "/tmp/tt_token.json"
        with open(tt_token_path, "w") as f:
            f.write(tt_token)
        os.environ["TT_TOKEN_PATH"] = tt_token_path
        os.environ["TT_TOKEN_JSON"] = tt_token
    os.environ["TT_CLIENT_ID"] = params.get("/gamma/tt/client_id", "")
    os.environ["TT_CLIENT_SECRET"] = params.get("/gamma/tt/client_secret", "")

    # Run P&L email
    try:
        from reporting.daily_pnl_email import run_daily_pnl_email
        kwargs = {"dry_run": dry_run}
        if report_date:
            kwargs["report_date"] = datetime.fromisoformat(str(report_date)).date()
        result = run_daily_pnl_email(**kwargs)
        print(f"daily-pnl result: {result}")
    except Exception as e:
        print(f"daily-pnl ERROR: {e}")
        import traceback
        traceback.print_exc()
        result = {"error": str(e)}

    duration = round(time.time() - t0, 1)
    print(f"=== DONE daily-pnl | {duration}s ===")
    return {
        "status": "ok" if "error" not in result else "error",
        "account": "daily-pnl",
        "result": result,
        "duration_s": duration,
    }


def _handle_weekly_pnl(event, t0):
    """Generate Schwab weekly P&L report and send via email.

    Event payload: {"account": "weekly-pnl", "dry_run": true/false,
                    "week_start": "YYYY-MM-DD", "week_end": "YYYY-MM-DD"}
    Needs SSM: Schwab creds (orders + SPX prices) + SMTP creds (email).
    """
    dry_run = event.get("dry_run", False)
    week_start = event.get("week_start")
    week_end = event.get("week_end")
    print(f"=== weekly-pnl | dry_run={dry_run} | start={week_start or 'auto'} | end={week_end or 'auto'} ===")

    # Fetch SSM params (Schwab + SMTP)
    ssm_paths = {
        "SCHWAB_APP_KEY": "/gamma/schwab/app_key",
        "SCHWAB_APP_SECRET": "/gamma/schwab/app_secret",
        "_schwab_token": "/gamma/schwab/token_json",
        "SMTP_USER": "/gamma/shared/smtp_user",
        "SMTP_PASS": "/gamma/shared/smtp_pass",
    }
    all_paths = list(set(ssm_paths.values()))
    params = get_ssm_params(all_paths)
    print(f"Fetched {len(params)}/{len(all_paths)} SSM params")

    # Set env vars
    os.environ["SCHWAB_APP_KEY"] = params.get("/gamma/schwab/app_key", "")
    os.environ["SCHWAB_APP_SECRET"] = params.get("/gamma/schwab/app_secret", "")
    os.environ["SMTP_USER"] = params.get("/gamma/shared/smtp_user", "")
    os.environ["SMTP_PASS"] = params.get("/gamma/shared/smtp_pass", "")

    # Seed Schwab token
    token_content = params.get("/gamma/schwab/token_json", "")
    if token_content:
        token_path = "/tmp/schwab_token.json"
        with open(token_path, "w") as f:
            f.write(token_content)
        os.environ["SCHWAB_TOKEN_PATH"] = token_path

    # Run weekly P&L report
    try:
        from reporting.weekly_pnl import run
        import io
        import sys as _sys

        # Capture stdout for email body
        buf = io.StringIO()
        old_stdout = _sys.stdout
        _sys.stdout = buf
        try:
            run(week_start=week_start, week_end=week_end)
        finally:
            _sys.stdout = old_stdout
        report_text = buf.getvalue()
        print(report_text)

        # Send via email (if not dry run)
        if not dry_run:
            from datetime import date as _date
            today = _date.today()
            subject = f"[Gamma] Weekly P&L — {week_start or 'this week'}"
            smtp_user = os.environ.get("SMTP_USER", "")
            smtp_pass = os.environ.get("SMTP_PASS", "")
            smtp_to = os.environ.get("SMTP_TO", "") or smtp_user
            if smtp_user and smtp_pass:
                import smtplib
                from email.message import EmailMessage
                msg = EmailMessage()
                msg["From"] = smtp_user
                msg["To"] = smtp_to
                msg["Subject"] = subject
                msg.set_content(report_text)
                with smtplib.SMTP(
                    os.environ.get("SMTP_HOST", "smtp.gmail.com"),
                    int(os.environ.get("SMTP_PORT", "587")),
                    timeout=20,
                ) as s:
                    s.starttls()
                    s.login(smtp_user, smtp_pass)
                    s.send_message(msg)
                print(f"Weekly P&L email sent to {smtp_to}")
                result = {"sent": True, "to": smtp_to}
            else:
                print("SMTP creds not set — skipping email")
                result = {"sent": False, "reason": "no SMTP creds"}
        else:
            result = {"sent": False, "dry_run": True}

    except Exception as e:
        print(f"weekly-pnl ERROR: {e}")
        import traceback
        traceback.print_exc()
        result = {"error": str(e)}

    duration = round(time.time() - t0, 1)
    print(f"=== DONE weekly-pnl | {duration}s ===")
    return {
        "status": "ok" if "error" not in result else "error",
        "account": "weekly-pnl",
        "result": result,
        "duration_s": duration,
    }


# ---------------------------------------------------------------------------
# Lambda entry point
# ---------------------------------------------------------------------------


def lambda_handler(event, context):
    t0 = time.time()
    trade_date = datetime.now(timezone.utc).date().isoformat()
    account = event.get("account", "")
    dry_run = event.get("dry_run", False)
    disable_schwab_cs = str(
        os.environ.get(
            "DISABLE_SCHWAB_CS",
            "1" if DISABLE_SCHWAB_CS_DEFAULT else "0",
        )
    ).strip().lower() in ("1", "true", "yes", "y", "on")

    # Warm-up ping — pre-warm containers for parallel trade invocations.
    # The first warmup invocation spawns additional async self-invocations
    # so that Lambda provisions enough containers for all concurrent accounts.
    if account == "warmup":
        depth = event.get("warmup_depth", 0)
        # Count how many concurrent trade schedules fire at the same time
        # (schwab + tt-ira + tt-individual = 3)
        target_containers = int(os.environ.get("WARMUP_CONTAINERS", "5"))
        if depth == 0 and target_containers > 1:
            # Spawn (N-1) additional invocations to force parallel containers
            fn_name = context.function_name if context else os.environ.get("AWS_LAMBDA_FUNCTION_NAME", "")
            if fn_name:
                lam = boto3.client("lambda")
                for i in range(1, target_containers):
                    try:
                        lam.invoke(
                            FunctionName=fn_name,
                            InvocationType="Event",  # async, non-blocking
                            Payload=json.dumps({"account": "warmup", "warmup_depth": i}).encode(),
                        )
                    except Exception as e:
                        print(f"WARMUP spawn {i} failed: {e}")
                print(f"WARMUP: spawned {target_containers - 1} additional containers")
        print(f"WARMUP ping — container is warm (depth={depth})")
        # Hold the container alive briefly so Lambda doesn't reclaim it
        # before the sibling warmups finish initializing
        if depth == 0:
            time.sleep(3)
        return {"status": "ok", "account": "warmup", "depth": depth, "duration_s": round(time.time() - t0, 1)}

    # Sim chain collection — separate flow (no subprocess, writes to S3)
    if account == "sim-collect":
        return _handle_sim_collect(event)

    # Daily P&L email — pulls Schwab orders, computes P&L, sends email
    if account == "daily-pnl":
        return _handle_daily_pnl(event, t0)

    # Weekly P&L report — Saturday morning summary email
    if account == "weekly-pnl":
        return _handle_weekly_pnl(event, t0)

    # CS reporting refresh — runs full pipeline independently of trading accounts
    if account == "cs-refresh":
        return _handle_cs_refresh(event, t0)

    # Safety switch: keep ConstantStable disabled on Schwab while TT stays active.
    if account == "schwab" and disable_schwab_cs:
        print("SKIP schwab: ConstantStable disabled by DISABLE_SCHWAB_CS")
        return {
            "status": "ok",
            "account": "schwab",
            "skipped": True,
            "reason": "SCHWAB_CS_DISABLED",
            "duration_s": round(time.time() - t0, 1),
        }

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
        env["DS_DRY_RUN"] = "true"
        env["BF_DRY_RUN"] = "1"

    # Allow event payload to inject env overrides (e.g. BF_NOW_OVERRIDE for testing)
    for k, v in event.get("env_override", {}).items():
        env[k] = str(v)

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
    if account in ("schwab", "morning-check", "butterfly", "dualside"):
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

    token_hash = file_hash(cfg["token_file"])
    tt_token_hash = file_hash("/tmp/tt_token.json") if account == "manual" else None

    # Ensure /tmp writability for logs
    os.makedirs("/tmp/logs", exist_ok=True)

    # -- 4. Signal wait moved into orchestrator (prep runs while waiting) --

    # -- 4b. Run orchestrator (critical path) --
    orch_timeout = 330 if account == "manual" else 100
    orch_rc = run_script(cfg["orchestrator"], env, timeout_s=orch_timeout, label="orchestrator")

    # -- 4c. Finalize pending BF plan if orchestrator failed/timed out --
    if account == "butterfly" and orch_rc != 0:
        _finalize_bf_plan(env, orch_rc)

    # -- 5. Post-trade steps (best-effort, time-permitting) --
    remaining_ms = context.get_remaining_time_in_millis() if context else 30000
    remaining_s = max(5, int(remaining_ms / 1000) - 5)
    report_owner = (env.get("CS_REPORT_OWNER") or DEFAULT_REPORT_OWNER).strip()
    report_delay_secs = int(env.get("CS_REPORT_DELAY_SECS") or str(DEFAULT_REPORT_DELAY_SECS))
    report_delay_applied = False

    post_results = {}
    if dry_run:
        print("DRY_RUN: skipping all post-steps")
    for step in ([] if dry_run else cfg.get("post_steps", [])):
        step_name = os.path.basename(step)
        try:
            is_report_step = step_name in REPORT_STEPS

            # Prevent concurrent full-sheet rewrites by allowing only one account
            # invocation to run reporting scripts.
            if is_report_step and account != report_owner:
                post_results[step_name] = "SKIP:not_owner"
                print(f"SKIP {step_name}: reporting owner is {report_owner}, current={account}")
                continue

            # Give other account invocations time to finish tracking updates first.
            if is_report_step and not report_delay_applied and report_delay_secs > 0:
                print(f"Reporting delay: sleeping {report_delay_secs}s before {step_name}")
                time.sleep(report_delay_secs)
                report_delay_applied = True

            step_timeout = min(30, remaining_s)
            rc = run_script(step, env, timeout_s=step_timeout, label=step_name)
            if rc == 0:
                post_results[step_name] = "OK"
            elif rc == 2:
                post_results[step_name] = "SOFT_SKIP"
            elif rc == 124:
                post_results[step_name] = "TIMEOUT"
            elif rc == -1:
                post_results[step_name] = "NOT_FOUND"
            else:
                post_results[step_name] = f"FAIL:rc={rc}"
            remaining_ms = context.get_remaining_time_in_millis() if context else 10000
            remaining_s = max(5, int(remaining_ms / 1000) - 5)
        except Exception as e:
            post_results[step_name] = f"ERROR:{e}"
            print(f"WARN post-step {step}: {e}")

    # Structured log line for CloudWatch Insights queries
    summary = {"account": account, "orchestrator_rc": orch_rc, "steps": post_results}
    print(f"REPORT_SUMMARY {json.dumps(summary)}")

    if dry_run:
        event_upload = {"skipped": "dry_run"}
    else:
        event_upload = upload_event_files(env, trade_date)
    print(f"EVENT_UPLOAD_SUMMARY {json.dumps(event_upload)}")

    # -- 6. Persist tokens back to SSM if refreshed --
    try:
        persist_token_if_changed(cfg["token_ssm_path"], cfg["token_file"], token_hash)
    except Exception as e:
        print(f"ERROR persisting token: {e}")

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
        "post_results": post_results,
        "event_upload": event_upload,
        "duration_s": duration,
    }
