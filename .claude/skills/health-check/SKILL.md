---
name: health-check
description: Monitor and diagnose Gamma trading system health — scheduled jobs, alerts, drawdown, account integrations.
---

# Health Check Skill

Monitor and diagnose the Gamma trading system's health across all scheduled jobs, alert pipelines, and account integrations.

## System Overview

All trading and reporting runs on a single AWS Lambda function (`gamma-trading-TradingFunction-O0rzFPNn3umX`) in `us-east-1`, invoked by EventBridge Scheduler with different `{"account": "<name>"}` payloads. The SAM stack is `gamma-trading`. CloudWatch log group: `/aws/lambda/gamma-trading-TradingFunction-O0rzFPNn3umX`.

---

## 1. Morning Check (9:35 AM ET)

**Schedule:** `gamma-cs-morning-check` -- `cron(35 9 ? * MON-FRI *)`
**Payload:** `{"account": "morning-check"}`
**Script:** `scripts/trade/ConstantStable/morning_check.py`

### What it monitors

- **Schwab equity:** Fetches live account equity via Schwab API (`liquidationValue`, `cashAvailableForTrading`, or `cashBalance`).
- **Drawdown vs peak:** Computes `(peak_equity - current_equity) / peak_equity`. Peak is stored in the `ConstantStableState` Google Sheet tab.
- **Edge test pause:** Checks if `pause_until` is set in the state sheet (edge guard paused trading).

### Alert conditions (any triggers email)

- Schwab equity unavailable (API failure).
- Drawdown exceeds `CS_MORNING_DD_ALERT_PCT` (default 20%, set in `static_env`).
- Edge test has paused trading (`pause_until` date is today or in the future).

### Email delivery

Sends via `scripts/notify/smtp_notify.py` subprocess. Subject format: `[Gamma] ALERT: dd=X.X% equity=$X,XXX` or `[Gamma] ALERT: equity unavailable`. No email is sent when all checks pass.

---

## 2. Daily P&L Email (4:45 PM ET)

**Schedule:** `gamma-daily-pnl` -- `cron(45 16 ? * MON-FRI *)`
**Payload:** `{"account": "daily-pnl"}`
**Script:** `reporting/daily_pnl_email.py`

### What it reports

- **Automated P&L** by strategy (ConstantStable, DualSide, Butterfly Tuesday) across 5-day, MTD, and YTD windows.
- **Win rate and avg trade** per window.
- **Last 5 sessions grid:** CS / DS / BF / Total columns per day.
- **Strategy breakdown:** YTD P&L, trade count, win rate, current streak per strategy group.
- **Drawdown stats:** Current and max drawdown for both MTD and YTD windows (realized P&L based).
- **Today's trade summary:** Table with Account, Strategy, Type (IC Long, IC Short, RR Long, RR Short, Bear Put, etc.), and signed Puts/Calls columns (+N=long/debit, -N=short/credit). Anomaly notes appended below the table flagging qty mismatches (e.g. duplicate fills from failed cancels) and other issues.
- **Discretionary P&L:** Separately computed P&L for manual/non-automated trades, with auto-adjustment detection (identifies when a manual order modifies an automated position).
- **Missing strategies check:** Flags if expected daily schedules (Schwab CS, Schwab DualSide, TT-IRA Novix, TT-Indv CS) had no fills.

### Email format

- Subject: `[Gamma] Today +$X | MTD +$X | YTD +$X — YYYY-MM-DD` (Today includes auto + discretionary P&L combined).
- Separate behavior-check email sent only when anomalies detected.

### Data sources

- Schwab: `load_orders_from_schwab()` via Schwab API.
- TastyTrade: `_load_tt_orders_for_date()` pulls from both TT accounts (5WT20360, 5WT09219).
- SPX spot: Schwab price history API for settlement calculations.

---

## 3. Weekly P&L Report (Saturday 9:00 AM ET)

**Schedule:** `gamma-weekly-pnl` -- `cron(0 9 ? * SAT *)`
**Payload:** `{"account": "weekly-pnl"}`
**Script:** `reporting/weekly_pnl.py`

### What it reports

- **Expired position P&L** using net position method: sums net qty per (strike, option_type) at each expiry, computes settlement value from SPX spot.
- **Strategy breakdown:** ConstantStable, DualSide, CS Morning, Butterfly Tuesday, Discretionary, Discretionary Butterfly.
- **Open positions:** Fetches current Schwab positions to show unrealized exposure.
- **Week window:** Defaults to current week; overridable via `--start YYYY-MM-DD`.

### Strategy label order

`constantstable` > `dualside` > `cs_morning` > `butterfly` > `manual` > `butterfly_manual`

---

## 4. Alert Email System

**Script:** `reporting/alerts_email.py`
**Entry point:** `send_alert_if_needed(report_date, con=con)`

### Alert conditions (email sent when ANY is true)

1. **ERROR/CRITICAL reconciliation items:** Queries `reconciliation_items` table for `UNRESOLVED` items with severity `ERROR` or `CRITICAL` (up to 10, CRITICAL first).
2. **Stale broker sources:** Queries `source_freshness` table for entries where `is_stale = true`, showing `source_name`, `last_success_at`, `sla_minutes`, and `error_message`.
3. **Missing nightly run:** If current ET time is past 4:45 PM on a weekday and no `format='nightly'` row exists in `daily_report_outputs` for today.

### Deduplication

Uses SHA-256 fingerprint of date + banner + issue IDs + stale sources + missing_run flag. Same fingerprint within a process lifetime skips re-send.

### Email format

- Subject: `[Gamma] <trust_banner> | <N> CRITICAL | <N> ERROR | <N> stale source(s) | MISSING RUN -- YYYY-MM-DD`
- Body: Sections for actionable issues, stale sources, missing run.
- Skips email entirely when no actionable conditions exist (returns `{"skipped": "no actionable issues"}`).

---

## 5. SMTP Configuration

All email sending uses the same SMTP pattern across all scripts:

| Env var      | SSM path                   | Default          |
|-------------|---------------------------|------------------|
| `SMTP_USER` | `/gamma/shared/smtp_user`  | (required)       |
| `SMTP_PASS` | `/gamma/shared/smtp_pass`  | (required)       |
| `SMTP_TO`   | `/gamma/shared/smtp_to`    | falls back to `SMTP_USER` |
| `SMTP_HOST` | --                        | `smtp.gmail.com` |
| `SMTP_PORT` | --                        | `587`            |

Connection: SMTP with STARTTLS on port 587, 20-second timeout. The Lambda handler fetches SMTP credentials from SSM Parameter Store and injects them as env vars before invoking each script.

Scripts that send email:
- `scripts/notify/smtp_notify.py` -- standalone CLI sender (used by morning_check via subprocess).
- `reporting/alerts_email.py` -- direct smtplib in-process.
- `reporting/daily_pnl_email.py` -- direct smtplib in-process (`_send_email()` function).
- `reporting/weekly_pnl.py` -- uses same pattern.

---

## 6. EventBridge Schedules (All Times ET, Weekdays Unless Noted)

### Health-critical execution sequence

| Time     | Schedule name                    | Payload                              | State    | Purpose                                    |
|----------|----------------------------------|--------------------------------------|----------|--------------------------------------------|
| 9:00 AM  | `gamma-cs-refresh-morning`       | `{"account": "cs-refresh"}`          | ENABLED  | Morning reporting retry (catch missed fills)|
| 9:31 AM  | `gamma-sim-collect-open`         | `{"account": "sim-collect", "phase": "open"}` | ENABLED | Collect SPX 0DTE chain at open |
| 9:35 AM  | `gamma-cs-morning-check`         | `{"account": "morning-check"}`       | ENABLED  | Morning equity/drawdown alert              |
| 9:35 AM  | `gamma-cs-ic-long-morning`       | `{"account": "ic-long-morning"}`     | ENABLED  | IC_LONG deferred morning entry (Schwab)    |
| 9:35 AM  | `gamma-cs-ic-long-morning-tt-individual` | `{"account": "ic-long-morning-tt-individual"}` | ENABLED | IC_LONG morning entry (TT Individual) |
| 12:00 PM | `gamma-sim-collect-mid`          | `{"account": "sim-collect", "phase": "mid"}` | ENABLED | Collect SPX chain midday |
| 3:58 PM  | `gamma-cs-warmup`                | `{"account": "warmup"}`              | ENABLED  | Pre-warm Lambda containers (spawns N async self-invocations) |
| 4:00 PM  | `gamma-sim-collect-close`        | `{"account": "sim-collect", "phase": "close"}` | ENABLED | Collect SPX chain at close |
| 4:01 PM  | `gamma-bf-daily`                 | `{"account": "butterfly"}`           | ENABLED  | Butterfly Tuesday hybrid strategy          |
| 4:01 PM  | `gamma-cs-ic-long-filter`        | `{"account": "ic-long-filter"}`      | ENABLED  | IC_LONG regime filter pre-check            |
| 4:05 PM  | `gamma-sim-collect-close5`       | `{"account": "sim-collect", "phase": "close5"}` | ENABLED | Collect SPX 1DTE chain |
| 4:05 PM  | `gamma-ds-schwab`                | `{"account": "dualside"}`            | ENABLED  | DualSide vertical strategy (Schwab)        |
| 4:13 PM  | `gamma-cs-schwab`                | `{"account": "schwab"}`              | ENABLED  | ConstantStable (Schwab)                    |
| 4:13 PM  | `gamma-cs-tt-individual`         | `{"account": "tt-individual"}`       | ENABLED  | ConstantStable (TT Individual 5WT09219)    |
| 4:13 PM  | `gamma-nx-tt-ira`                | `{"account": "novix-tt-ira"}`        | ENABLED  | Novix signal strategy (TT IRA 5WT20360)    |
| 4:45 PM  | `gamma-daily-pnl`                | `{"account": "daily-pnl"}`           | ENABLED  | Daily P&L email                            |
| 5:00 PM  | `gamma-cs-refresh`               | `{"account": "cs-refresh"}`          | ENABLED  | CS full reporting refresh                  |
| Sat 9 AM | `gamma-weekly-pnl`               | `{"account": "weekly-pnl"}`          | ENABLED  | Weekly P&L report (Saturday)               |

### Disabled schedules (kept for reference)

| Schedule name                         | Payload                              | Reason                        |
|---------------------------------------|--------------------------------------|-------------------------------|
| `gamma-cs-tt-ira`                     | `{"account": "tt-ira"}`             | Switched to Novix             |
| `gamma-nx-tt-individual`              | `{"account": "novix-tt-individual"}`| Staying with CS               |
| `gamma-cs-ic-long-morning-tt-ira`     | `{"account": "ic-long-morning-tt-ira"}` | Switched to Novix         |

---

## 7. Manual Health Check via Lambda Invocation

Use the AWS CLI to manually trigger any schedule's payload:

```bash
# Morning check
aws lambda invoke --function-name gamma-trading-TradingFunction-O0rzFPNn3umX \
  --payload '{"account": "morning-check"}' \
  --cli-binary-format raw-in-base64-out /tmp/out.json && cat /tmp/out.json

# Daily P&L (dry run)
aws lambda invoke --function-name gamma-trading-TradingFunction-O0rzFPNn3umX \
  --payload '{"account": "daily-pnl", "dry_run": true}' \
  --cli-binary-format raw-in-base64-out /tmp/out.json && cat /tmp/out.json

# Weekly P&L (specific week)
aws lambda invoke --function-name gamma-trading-TradingFunction-O0rzFPNn3umX \
  --payload '{"account": "weekly-pnl", "week_start": "2026-03-23", "week_end": "2026-03-28"}' \
  --cli-binary-format raw-in-base64-out /tmp/out.json && cat /tmp/out.json

# Warmup (test container init)
aws lambda invoke --function-name gamma-trading-TradingFunction-O0rzFPNn3umX \
  --payload '{"account": "warmup"}' \
  --cli-binary-format raw-in-base64-out /tmp/out.json && cat /tmp/out.json

# CS reporting refresh (skip specific steps)
aws lambda invoke --function-name gamma-trading-TradingFunction-O0rzFPNn3umX \
  --payload '{"account": "cs-refresh", "skip": "gw_signal,chart_data"}' \
  --cli-binary-format raw-in-base64-out /tmp/out.json && cat /tmp/out.json
```

---

## 8. CloudWatch Logs

**Log group:** `/aws/lambda/gamma-trading-TradingFunction-O0rzFPNn3umX`
**Region:** `us-east-1`

### Useful log queries

```bash
# Tail recent logs
aws logs tail /aws/lambda/gamma-trading-TradingFunction-O0rzFPNn3umX \
  --since 1h --follow --region us-east-1

# Search for errors in last 2 hours
aws logs filter-log-events \
  --log-group-name /aws/lambda/gamma-trading-TradingFunction-O0rzFPNn3umX \
  --start-time $(date -v-2H +%s000) \
  --filter-pattern "ERROR" \
  --region us-east-1

# Find a specific account's run
aws logs filter-log-events \
  --log-group-name /aws/lambda/gamma-trading-TradingFunction-O0rzFPNn3umX \
  --start-time $(date -v-1d +%s000) \
  --filter-pattern '"account": "morning-check"' \
  --region us-east-1
```

### CloudWatch Insights query for daily run audit

```
fields @timestamp, @message
| filter @message like /=== DONE/
| parse @message "=== DONE * |" as account
| sort @timestamp desc
| limit 20
```

---

## 9. Checking If Today's Runs Completed

### Quick check via CloudWatch

```bash
# Look for all "=== DONE" markers from today
aws logs filter-log-events \
  --log-group-name /aws/lambda/gamma-trading-TradingFunction-O0rzFPNn3umX \
  --start-time $(date -d "today 00:00" +%s000 2>/dev/null || date -v0H -v0M -v0S +%s000) \
  --filter-pattern "DONE" \
  --region us-east-1 \
  --query 'events[].message' --output text
```

### Expected weekday completion markers

After 5:00 PM ET on a normal weekday, you should see "DONE" log lines for all of these accounts:

1. `morning-check` (9:35 AM)
2. `ic-long-morning` (9:35 AM)
3. `ic-long-morning-tt-individual` (9:35 AM)
4. `warmup` (3:58 PM) -- plus N depth-1+ warmup siblings
5. `butterfly` (4:01 PM)
6. `ic-long-filter` (4:01 PM)
7. `dualside` (4:05 PM)
8. `schwab` (4:13 PM)
9. `tt-individual` (4:13 PM)
10. `novix-tt-ira` (4:13 PM)
11. `daily-pnl` (4:45 PM)
12. `cs-refresh` (5:00 PM)

Saturday: `weekly-pnl` (9:00 AM).

### Via Lambda response inspection

Each handler returns a JSON object with `"status"` field. Check the return payload:
- `"status": "ok"` -- completed successfully.
- `"status": "error"` -- check `returncode` and CloudWatch logs for details.

---

## 10. Common Failure Modes

### Token expiry

- **Schwab:** OAuth2 refresh token expires after 7 days of inactivity. Symptoms: 401 errors on equity fetch, order placement, or price history. Fix: run `scripts/refresh_schwab_token.sh` locally and update SSM `/gamma/schwab/token_json`.
- **TastyTrade:** Session token stored at `/gamma/tt/token_json` in SSM. The handler seeds it to `/tmp/tt_token.json`. Expiry causes login failures. Fix: re-authenticate via TT API and update SSM.
- **Token persistence logic:** Handler reads token from SSM, writes to `/tmp`, computes SHA-256 hash before and after the run, and writes back to SSM only if the hash changed (token was refreshed during execution).

### API rate limits

- **Schwab API:** Rate-limited on order placement and account queries. Concurrent invocations (schwab + dualside + butterfly all hit Schwab) can trigger 429s. The warmup at 3:58 PM and staggered schedule (4:01, 4:05, 4:13) mitigate this.
- **TastyTrade API:** Less aggressive rate limiting, but session tokens can be invalidated if multiple concurrent requests use the same token.

### Lambda timeout

- **Configured:** 360 seconds (6 minutes) per invocation.
- **Memory:** 1024 MB.
- **Symptoms:** Task timed out after 360.00 seconds in CloudWatch. Usually caused by: hung Schwab API call, network issues, or Docker container cold start + heavy post_steps.
- **Post-steps risk:** The `schwab` account has 7 post_steps (signal, trades, tracking, backfill, summary, performance, edge_guard). If any one hangs, the entire invocation can timeout.

### SSM fetch failures

- **Symptoms:** Lambda cannot read `/gamma/*` parameters. Usually IAM permission issue or SSM throttling.
- **Critical params:** `/gamma/schwab/token_json`, `/gamma/schwab/app_key`, `/gamma/schwab/app_secret`, `/gamma/tt/token_json`, `/gamma/tt/client_id`, `/gamma/tt/client_secret`, `/gamma/shared/smtp_user`, `/gamma/shared/smtp_pass`.
- **Diagnosis:** Check CloudWatch for "ParameterNotFound" or "AccessDeniedException" in the first few log lines of a failed invocation.

### Google Sheets API failures

- **Morning check** reads `ConstantStableState` tab for peak equity and edge guard state. If Google API is down or `GOOGLE_SERVICE_ACCOUNT_JSON` is missing, the check proceeds but drawdown comparison is skipped (peak = None).
- **Post-steps** write trade data to Google Sheets. Failures here do not block trading but leave reporting gaps.

### SMTP failures

- Missing `SMTP_USER`/`SMTP_PASS` env vars: email silently skipped (returns `{"skipped": "SMTP_USER or SMTP_PASS not set"}`).
- Gmail app password revoked or expired: SMTP login fails with authentication error.
- 20-second timeout on SMTP connection: network issues between Lambda and Gmail.

### Cold start latency

- Container image Lambda has ~5-10 second cold starts. The warmup schedule at 3:58 PM pre-spawns containers to avoid cold starts during the 4:01-4:13 PM trading window.
- If warmup fails, first trade invocations may run slower but will still execute.

---

## 11. Drawdown Thresholds and Alert Logic

### Morning check drawdown (live equity based)

- **Threshold:** `CS_MORNING_DD_ALERT_PCT` = 20% (0.20), configured in `lambda/template.yaml` under the `morning-check` account.
- **Calculation:** `dd_pct = max(0, peak_equity - current_equity) / peak_equity`
- **Peak source:** `peak_equity` field from the last row of the `ConstantStableState` Google Sheet tab.
- **Alert fires when:** `dd_pct >= threshold_pct`
- **Email subject:** `[Gamma] ALERT: dd=X.X% equity=$X,XXX`

### Daily P&L drawdown (realized P&L based)

- **Current drawdown:** `peak_cumulative_pnl - current_cumulative_pnl` for positions settled in the window.
- **Max drawdown:** Maximum peak-to-trough in cumulative daily realized P&L within the window.
- **Computed for:** Both MTD and YTD windows.
- **No threshold alert:** These are informational metrics included in the daily email body; they do not trigger separate alerts.

### Edge guard pause

- The edge guard (`scripts/trade/ConstantStable/edge_guard.py`) can set `pause_until` in the state sheet, which halts trading.
- Morning check detects this and emails an alert: `Edge test PAUSED until <date>`.
- Trading resumes automatically when current date > `pause_until`.

---

## Key Files

| Purpose                | Path                                                    |
|------------------------|---------------------------------------------------------|
| Lambda handler         | `lambda/handler.py`                                     |
| SAM template           | `lambda/template.yaml`                                  |
| Morning check          | `scripts/trade/ConstantStable/morning_check.py`         |
| Daily P&L email        | `reporting/daily_pnl_email.py`                          |
| Weekly P&L report      | `reporting/weekly_pnl.py`                               |
| Alert email system     | `reporting/alerts_email.py`                              |
| SMTP sender            | `scripts/notify/smtp_notify.py`                         |
| Broker P&L engine      | `reporting/broker_pnl.py`                               |
| Edge guard             | `scripts/trade/ConstantStable/edge_guard.py`            |
| CS refresh pipeline    | `scripts/data/cs_refresh_all.py`                        |
