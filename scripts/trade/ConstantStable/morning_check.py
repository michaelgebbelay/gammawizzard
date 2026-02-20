#!/usr/bin/env python3
"""Morning equity/drawdown check with email alert.

Runs before market (e.g. 9:35 AM ET) to give early warning about:
  - Drawdown exceeding threshold (CS_MORNING_DD_ALERT_PCT, default 20%)
  - Edge test pausing trading (pause_until set in ConstantStableState)
  - Schwab equity unavailable

Read-only against ConstantStableState — does NOT append rows or advance SPRT.
Sends email via scripts/notify/smtp_notify.py only when alert conditions are met.
"""

import json
import os
import subprocess
import sys
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo


def _add_scripts_root():
    cur = os.path.abspath(os.path.dirname(__file__))
    while True:
        if os.path.basename(cur) in ("scripts", "Script"):
            if cur not in sys.path:
                sys.path.append(cur)
            return
        parent = os.path.dirname(cur)
        if parent == cur:
            return
        cur = parent


_add_scripts_root()
from schwab_token_keeper import schwab_client

ET = ZoneInfo("America/New_York")
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


# ---------- Schwab equity ----------

def get_equity(c):
    """Returns (equity_value, source_key, acct_number) or (None, 'none', '')."""
    r = c.get_accounts()
    r.raise_for_status()
    data = r.json()
    arr = data if isinstance(data, list) else [data]

    acct_num = ""
    try:
        rr = c.get_account_numbers()
        rr.raise_for_status()
        acct_num = str((rr.json() or [{}])[0].get("accountNumber") or "")
    except Exception:
        pass

    def hunt(a):
        acct_id = None
        init = {}
        curr = {}
        stack = [a]
        while stack:
            x = stack.pop()
            if isinstance(x, dict):
                if acct_id is None and x.get("accountNumber"):
                    acct_id = str(x["accountNumber"])
                if "initialBalances" in x and isinstance(x["initialBalances"], dict):
                    init = x["initialBalances"]
                if "currentBalances" in x and isinstance(x["currentBalances"], dict):
                    curr = x["currentBalances"]
                for v in x.values():
                    if isinstance(v, (dict, list)):
                        stack.append(v)
            elif isinstance(x, list):
                stack.extend(x)
        return acct_id, init, curr

    chosen = None
    for a in arr:
        aid, init, curr = hunt(a)
        if acct_num and aid == acct_num:
            chosen = (init, curr)
            break
        if chosen is None:
            chosen = (init, curr)

    if not chosen:
        return None, "none", acct_num

    init, curr = chosen
    keys = ["liquidationValue", "cashAvailableForTrading", "cashBalance"]
    for src in (init, curr):
        for k in keys:
            v = (src or {}).get(k)
            if isinstance(v, (int, float)) and float(v) > 0:
                return float(v), k, acct_num

    return None, "none", acct_num


# ---------- Google Sheets ----------

def sheets_read_last_row(tab):
    """Read the last row from a Google Sheet tab. Returns (header, row_dict) or ([], {})."""
    raw = (os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON") or "").strip()
    sid = (os.environ.get("GSHEET_ID") or "").strip()
    if not raw or not sid:
        return [], {}

    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    creds = service_account.Credentials.from_service_account_info(
        json.loads(raw), scopes=SCOPES
    )
    svc = build("sheets", "v4", credentials=creds, cache_discovery=False)

    r = svc.spreadsheets().values().get(
        spreadsheetId=sid, range=f"{tab}!A1:ZZ"
    ).execute()
    rows = r.get("values") or []
    if len(rows) < 2:
        return [], {}

    header = rows[0]
    last = rows[-1]
    row = {}
    for i, h in enumerate(header):
        row[h] = last[i] if i < len(last) else ""
    return header, row


# ---------- Alert logic ----------

def safe_float(v, default=None):
    try:
        s = (v or "").strip()
        if s == "":
            return default
        return float(s)
    except Exception:
        return default


def check_alerts(equity, prev_state, threshold_pct):
    """Returns list of alert reasons (empty = no alert)."""
    alerts = []

    if equity is None:
        alerts.append("Schwab equity unavailable")
        return alerts

    # Check edge pause
    pause_until = (prev_state.get("pause_until") or "").strip()
    if pause_until:
        try:
            pu = date.fromisoformat(pause_until[:10])
            now_et = datetime.now(ET).date()
            if now_et <= pu:
                alerts.append(f"Edge test PAUSED until {pause_until}")
        except Exception:
            pass

    # Compute live drawdown from stored peak
    peak = safe_float(prev_state.get("peak_equity"))
    if peak and peak > 0:
        dd = max(0.0, peak - equity)
        dd_pct = dd / peak
        if dd_pct >= threshold_pct:
            alerts.append(f"Drawdown {dd_pct:.1%} exceeds threshold {threshold_pct:.0%}")

    return alerts


def build_status(equity, eq_src, prev_state, threshold_pct, alerts):
    """Build human-readable status string."""
    ts_et = datetime.now(ET)
    peak = safe_float(prev_state.get("peak_equity"))
    dd = max(0.0, peak - equity) if (peak and peak > 0 and equity) else 0.0
    dd_pct = (dd / peak) if (peak and peak > 0) else 0.0
    decision = prev_state.get("decision") or "UNKNOWN"
    pause_until = prev_state.get("pause_until") or "(none)"

    lines = [
        f"Morning Check — {ts_et.strftime('%Y-%m-%d %H:%M')} ET",
        "",
        f"Equity:     ${equity:,.2f}" if equity else "Equity:     UNAVAILABLE",
        f"Peak:       ${peak:,.2f}" if peak else "Peak:       UNKNOWN",
        f"Drawdown:   ${dd:,.2f} ({dd_pct:.2%})",
        f"Threshold:  {threshold_pct:.0%}",
        "",
        f"Edge Status: {decision}",
        f"Pause Until: {pause_until}",
    ]

    if alerts:
        lines.append("")
        for a in alerts:
            lines.append(f"  * {a}")

    return "\n".join(lines)


def send_email(subject, body):
    """Send email via smtp_notify.py subprocess."""
    # Find smtp_notify.py relative to this script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # Walk up to find scripts/notify/smtp_notify.py
    root = script_dir
    while root != os.path.dirname(root):
        candidate = os.path.join(root, "notify", "smtp_notify.py")
        if os.path.isfile(candidate):
            break
        # Also check scripts/notify/
        candidate2 = os.path.join(root, "scripts", "notify", "smtp_notify.py")
        if os.path.isfile(candidate2):
            candidate = candidate2
            break
        root = os.path.dirname(root)
    else:
        # Fallback: try LAMBDA_TASK_ROOT
        task_root = os.environ.get("LAMBDA_TASK_ROOT", "/var/task")
        candidate = os.path.join(task_root, "scripts", "notify", "smtp_notify.py")

    if not os.path.isfile(candidate):
        print(f"MORNING_CHECK: smtp_notify.py not found at {candidate}")
        return False

    rc = subprocess.call(
        [sys.executable, candidate, subject, body],
        env=dict(os.environ),
        timeout=30,
    )
    if rc == 0:
        print("MORNING_CHECK: alert email sent")
        return True
    elif rc == 1:
        print("MORNING_CHECK: SMTP secrets missing (SMTP_USER/SMTP_PASS) — skipping email")
        return False
    else:
        print(f"MORNING_CHECK: smtp_notify.py returned rc={rc}")
        return False


# ---------- Main ----------

def main():
    threshold_pct = float(os.environ.get("CS_MORNING_DD_ALERT_PCT", "0.20"))
    state_tab = (os.environ.get("CS_STATE_TAB") or "ConstantStableState").strip()

    # 1. Schwab equity
    try:
        c = schwab_client()
        equity, eq_src, acct = get_equity(c)
    except Exception as e:
        print(f"MORNING_CHECK ERROR: Schwab init failed: {e}")
        equity, eq_src, acct = None, "error", ""

    print(f"MORNING_CHECK: equity={equity} src={eq_src} acct={acct}")

    # 2. Read last edge guard state from sheet
    try:
        header, prev_state = sheets_read_last_row(state_tab)
    except Exception as e:
        print(f"MORNING_CHECK WARN: sheets read failed: {e}")
        prev_state = {}

    if prev_state:
        print(
            f"MORNING_CHECK: last_state date={prev_state.get('run_date')} "
            f"equity={prev_state.get('equity')} dd_pct={prev_state.get('dd_pct')} "
            f"decision={prev_state.get('decision')} pause_until={prev_state.get('pause_until')}"
        )

    # 3. Check alert conditions
    alerts = check_alerts(equity, prev_state, threshold_pct)

    # 4. Build status
    status = build_status(equity, eq_src, prev_state, threshold_pct, alerts)
    print(f"MORNING_CHECK STATUS:\n{status}")

    # 5. Send email if alert triggered
    if alerts:
        peak = safe_float(prev_state.get("peak_equity"))
        dd_pct = max(0.0, peak - equity) / peak if (peak and peak > 0 and equity) else 0.0
        subject = f"[Gamma] ALERT: dd={dd_pct:.1%} equity=${equity:,.0f}" if equity else "[Gamma] ALERT: equity unavailable"
        send_email(subject, status)
    else:
        print("MORNING_CHECK: no alert conditions — skipping email")

    return 0


if __name__ == "__main__":
    sys.exit(main())
