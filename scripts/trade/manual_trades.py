#!/usr/bin/env python3
"""
Execute manual trades from the Manual_Trades Google Sheet tab.

Reads pending rows (status column empty), builds a GW-compatible signal dict,
and calls the existing orchestrator for each row via subprocess with CS_SIGNAL_JSON
env var override.  All existing logic (VIX sizing, TOPUP, GUARD, IC/RR detection,
pair mode, CSV logging) works unchanged.

Sheet columns:
  account | date | expiry | put_strike | call_strike | left_go | right_go
  | l_imp | r_imp | vix_one | status

Trigger:
  aws lambda invoke --payload '{"account": "manual"}' ...
"""

import json
import os
import subprocess
import sys

# --- path setup (same pattern as data scripts) ---
def _add_scripts_root():
    cur = os.path.abspath(os.path.dirname(__file__))
    while True:
        if os.path.basename(cur) == "scripts":
            if cur not in sys.path:
                sys.path.append(cur)
            return
        parent = os.path.dirname(cur)
        if parent == cur:
            return
        cur = parent

_add_scripts_root()

from lib.sheets import sheets_client, ensure_sheet_tab, get_values

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

HEADER = [
    "account", "date", "expiry", "put_strike", "call_strike",
    "left_go", "right_go", "l_imp", "r_imp", "vix_one", "status",
]

# Account → orchestrator script (relative to repo root / LAMBDA_TASK_ROOT)
ORCHESTRATORS = {
    "schwab": "scripts/trade/ConstantStable/orchestrator.py",
    "tt-ira": "TT/Script/ConstantStable/orchestrator.py",
    "tt-individual": "TT/Script/ConstantStable/orchestrator.py",
}

# Per-account env overrides (TT account number, label, etc.)
ACCOUNT_ENV = {
    "schwab": {"CS_ACCOUNT_LABEL": "schwab"},
    "tt-ira": {"TT_ACCOUNT_NUMBER": "5WT20360", "CS_ACCOUNT_LABEL": "tt-ira"},
    "tt-individual": {"TT_ACCOUNT_NUMBER": "5WT09219", "CS_ACCOUNT_LABEL": "tt-individual"},
}

VALID_ACCOUNTS = set(ORCHESTRATORS.keys())

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def build_signal_json(row: dict) -> str:
    """Build a GW-compatible signal dict from a sheet row."""
    return json.dumps({
        "Date": row["date"],
        "TDate": row["expiry"],
        "Limit": row["put_strike"],
        "CLimit": row["call_strike"],
        "LeftGo": row["left_go"],
        "RightGo": row["right_go"],
        "LImp": row["l_imp"],
        "RImp": row["r_imp"],
        "VixOne": row["vix_one"],
        "VIX": row["vix_one"],
    })


def resolve_accounts(acct_field: str) -> list:
    """Expand 'all' to all three accounts, or return single account."""
    acct = acct_field.strip().lower()
    if acct == "all":
        return ["schwab", "tt-ira", "tt-individual"]
    if acct in VALID_ACCOUNTS:
        return [acct]
    return []


def run_orchestrator(acct: str, signal_json: str, task_root: str) -> int:
    """Run the orchestrator for a single account with CS_SIGNAL_JSON override."""
    orch = ORCHESTRATORS[acct]
    full_path = os.path.join(task_root, orch)
    if not os.path.isfile(full_path):
        print(f"MANUAL_TRADES: orchestrator not found: {full_path}")
        return 1

    env = dict(os.environ)
    env["CS_SIGNAL_JSON"] = signal_json
    env.update(ACCOUNT_ENV.get(acct, {}))

    print(f"MANUAL_TRADES: running {orch} for {acct}")
    try:
        result = subprocess.run(
            [sys.executable, full_path],
            env=env,
            cwd=task_root,
            capture_output=True,
            text=True,
            timeout=120,
        )
    except subprocess.TimeoutExpired:
        print(f"MANUAL_TRADES: TIMEOUT {acct} after 120s")
        return 124

    if result.stdout:
        for line in result.stdout.rstrip().split("\n"):
            print(f"  {line}")
    if result.stderr:
        for line in result.stderr.rstrip().split("\n"):
            print(f"  ERR: {line}")
    return result.returncode


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    tab = os.environ.get("CS_MANUAL_TAB", "Manual_Trades")
    task_root = os.environ.get("LAMBDA_TASK_ROOT", os.getcwd())

    svc, sid = sheets_client()
    ensure_sheet_tab(svc, sid, tab)

    rows = get_values(svc, sid, f"{tab}!A1:K")
    if not rows or len(rows) < 2:
        print("MANUAL_TRADES: no rows found")
        return 0

    header = rows[0]
    # Validate header matches expected columns
    if header != HEADER:
        # Write header if missing
        svc.spreadsheets().values().update(
            spreadsheetId=sid,
            range=f"{tab}!A1",
            valueInputOption="RAW",
            body={"values": [HEADER]},
        ).execute()
        print(f"MANUAL_TRADES: wrote header row")
        if len(rows) < 2:
            return 0
        header = HEADER

    processed = 0
    for i, vals in enumerate(rows[1:], start=2):
        row = {header[j]: (vals[j] if j < len(vals) else "") for j in range(len(header))}

        # Skip already-processed rows
        if row.get("status", "").strip():
            continue

        accounts = resolve_accounts(row.get("account", ""))
        if not accounts:
            status_str = f"SKIPPED: invalid account '{row.get('account', '')}'"
            print(f"MANUAL_TRADES: row {i} → {status_str}")
            svc.spreadsheets().values().update(
                spreadsheetId=sid,
                range=f"{tab}!K{i}",
                valueInputOption="RAW",
                body={"values": [[status_str]]},
            ).execute()
            continue

        signal_json = build_signal_json(row)
        print(f"MANUAL_TRADES: row {i} | accounts={accounts} | signal={signal_json}")

        statuses = []
        for acct in accounts:
            rc = run_orchestrator(acct, signal_json, task_root)
            statuses.append(f"{acct}:{'OK' if rc == 0 else f'ERR({rc})'}")

        status_str = " | ".join(statuses)
        svc.spreadsheets().values().update(
            spreadsheetId=sid,
            range=f"{tab}!K{i}",
            valueInputOption="RAW",
            body={"values": [[status_str]]},
        ).execute()
        print(f"MANUAL_TRADES: row {i} → {status_str}")
        processed += 1

    print(f"MANUAL_TRADES: processed {processed} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
