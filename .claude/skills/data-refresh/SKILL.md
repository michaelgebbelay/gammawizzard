---
name: data-refresh
description: Post-trade data refresh pipeline — Google Sheets sync, reconciliation, reporting. Use when the user asks to refresh data, sync sheets, or run the post-trade pipeline.
disable-model-invocation: true
---

# Post-Trade Data Refresh Pipeline

Skill for understanding and operating the CS (ConstantStable) reporting pipeline that refreshes Google Sheets after each trading day.

## Pipeline Overview

The orchestrator `scripts/data/cs_refresh_all.py` runs 7 steps in strict dependency order. Each step is a standalone Python script that reads from upstream tabs and writes to its own target tab.

## Execution Order (cs_refresh_all.py STEPS)

| # | Step Name        | Script                              | Writes To                                  | Reads From                                   | Timeout |
|---|------------------|-------------------------------------|--------------------------------------------|----------------------------------------------|---------|
| 1 | gw_signal        | cs_gw_signal_to_gsheet.py           | GW_Signal                                  | GammaWizard API (ConstantStable endpoint)    | 30s     |
| 2 | api_to_tracking  | cs_api_to_tracking.py               | CS_Tracking (gap fill)                     | Schwab API, TT API, GW_Signal                | 60s     |
| 3 | tt_close_status  | cs_tt_close_status.py               | CS_TT_Close                                | TT API (IRA + Individual), Schwab API        | 30s     |
| 4 | summary          | cs_summary_to_gsheet.py             | CS_Summary                                 | CS_Tracking, GW_Signal, CS_TT_Close          | 30s     |
| 5 | performance      | cs_performance_to_gsheet.py         | CS_Performance                             | CS_Summary                                   | 30s     |
| 6 | chart_data       | cs_chart_data_to_gsheet.py          | CS_Chart_Daily, CS_Chart_Weekly, CS_Chart_Monthly | CS_Summary                            | 30s     |
| 7 | dashboard        | cs_build_dashboard_tab.py           | CS_Dashboard, CS_Dashboard_Data            | Chart tabs                                   | 30s     |

### Dependency DAG

```
GW API ──> [1] GW_Signal ──┐
                            ├──> [4] CS_Summary ──> [5] CS_Performance
Schwab/TT APIs ──> [2] CS_Tracking ──┤                   │
                                     │              [6] CS_Chart_Daily / Weekly / Monthly
TT/Schwab APIs ──> [3] CS_TT_Close ──┘                   │
                                                     [7] CS_Dashboard
```

Steps 1-3 are independent (all pull from APIs). Steps 4-7 are sequential (each depends on prior outputs).

## Step Details

### 1. cs_gw_signal_to_gsheet.py -- GW_Signal

Fetches the latest ConstantStable neural-net payload from the GammaWizard API and upserts one row per trading day. This is the "what should we trade?" signal source.

- **Tab:** `GW_Signal` (env: `CS_GW_SIGNAL_TAB`)
- **API:** `GW_BASE/GW_ENDPOINT` (default `https://gandalf.gammawizard.com/rapi/GetUltraPureConstantStable`)
- **Auth:** `GW_EMAIL`, `GW_PASSWORD`
- **Key fields:** signal prices, VIX values, vol metrics, recommended side

### 2. cs_api_to_tracking.py -- CS_Tracking (Gap Fill)

Pulls filled OPEN orders from all 3 broker accounts (Schwab, TT IRA, TT Individual), joins with GW_Signal for signal metadata, and upserts to CS_Tracking. Smart merge: only fills gaps -- does not overwrite existing CSV-sourced data.

- **Tab:** `CS_Tracking` (env: `CS_TRACKING_TAB`)
- **APIs:** Schwab (via schwab client), TT (via TT client for both accounts)
- **Lookback:** `CS_API_DAYS_BACK` days (default 7)
- **Path setup:** adds both `scripts/` and `TT/Script/` to sys.path

### 3. cs_tt_close_status.py -- CS_TT_Close

Checks close order status across all accounts. For positions with profit-taking close orders, records which orders have filled and the close prices. Used by CS_Summary for accurate P&L: early-close uses close price, expiry uses settlement price.

- **Tab:** `CS_TT_Close` (env: `CS_TT_CLOSE_TAB`)
- **Accounts:** `TT_ACCOUNT_NUMBERS` ("5WT09219:tt-individual,5WT20360:tt-ira") + Schwab

### 4. cs_summary_to_gsheet.py -- CS_Summary

Builds a daily pivot combining GW signal prices with per-account fills side by side, then computes P&L from settlement data.

- **Tab:** `CS_Summary` (env: `CS_SUMMARY_TAB`)
- **Layout:**
  - Rows 1-8: user-managed summary formulas (Trade Total, Edge, etc.) -- NEVER touched by code
  - Row 9: section headers (GW, schwab, TT IRA, TT IND)
  - Row 10: column headers (Date, QTY, price put, price call, P&L, ...)
  - Row 11+: daily data (newest first)
- **P&L columns** (E, I, M, Q): after settlement = actual P&L in SPX points x qty; before settlement = cost-adjusted net credit
- **Also reads:** sw_txn_raw tab (env: `SW_RAW_TAB`)

### 5. cs_performance_to_gsheet.py -- CS_Performance

Aggregates daily P&L from CS_Summary into monthly dollar P&L and rolling trade statistics (last 10/20 trades) for all 4 account views.

- **Tab:** `CS_Performance` (env: `CS_PERFORMANCE_TAB`)
- **Layout:**
  - Rows 3-16: monthly P&L grid (Jan-Dec) with year columns, per-account + Total
  - Rows 18-31: rolling stats blocks (Leo, Schwab, TT IRA, TT Individual) with win rate, avg P&L, etc.

### 6. cs_chart_data_to_gsheet.py -- Chart Tabs

Builds chart-friendly normalized series (bar/line/drawdown) from CS_Summary, similar to Power BI visuals.

- **Tabs:** `CS_Chart_Daily`, `CS_Chart_Weekly`, `CS_Chart_Monthly`
- **Env overrides:** `CS_CHART_DAILY_TAB`, `CS_CHART_WEEKLY_TAB`, `CS_CHART_MONTHLY_TAB`
- **Account selector:** `CS_DASH_ACCOUNT` (default "schwab-adjusted"; options: total, schwab, schwab-adjusted, tt-ira, tt-individual, gw)

### 7. cs_build_dashboard_tab.py -- Dashboard

Builds CS_Dashboard with side-by-side account charts (daily last 20, weekly last 10, monthly last 6, drawdown).

- **Tabs:** `CS_Dashboard`, `CS_Dashboard_Data` (hidden helper tab)

## Separate Pipelines (Not in cs_refresh_all.py)

These scripts run independently as Lambda post-steps for their respective strategies:

### bf_trades_to_gsheet.py -- BF_Trades

Logs butterfly evaluations (SKIP, ERROR, OK/DRY_RUN, OK/FILLED) to Google Sheets. Reads `/tmp/bf_plan.json` written by the ButterflyTuesday orchestrator.

- **Tab:** `BF_Trades` (env: `BF_GSHEET_TAB`)
- **Input:** `BF_PLAN_PATH` (default `/tmp/bf_plan.json`)
- **Trigger:** runs as post-step in Lambda handler after butterfly execution

### bf_eod_tracking.py -- BF_Trades (EOD columns)

EOD butterfly price tracking -- fills `eod_3dte`/`2dte`/`1dte` columns in BF_Trades. For each OK row whose expiry is still in the future, fetches the current butterfly package mid from Schwab.

- **Tab:** `BF_Trades` (same tab, updates EOD columns)

### ds_tracking_to_gsheet.py -- DS_Tracking

Pushes DualSide trade CSV rows to Google Sheets. Pivots PUT + CALL rows for the same (trade_date, tdate) into one combined row.

- **Tab:** `DS_Tracking` (env: `DS_TRACKING_TAB`)
- **Input:** `DS_LOG_PATH` (default `/tmp/logs/dualside_trades.csv`)

### reconcile_reporting.py -- Reconciliation

Runs as the LAST post-step for each strategy. Checks that the expected tab has a row with today's date and required columns are non-empty. Emits `RECONCILE_OK` or `RECONCILE_MISS` log lines for CloudWatch alerting.

- **Config:** `RECONCILE_CHECKS` env var with comma-separated specs: `"tab:date_header:required_header1:required_header2"`
- **Example:** `"BF_Trades:trade_date:status:signal"` or `"DS_Tracking:trade_date:put_structure"`
- **Exit codes:** 0 = all pass, 1 = config/infra failure, 2 = data miss

## How to Run

### Full Refresh (All 7 Steps)

```bash
# Local with .env sourced:
python scripts/data/cs_refresh_all.py

# Local loading secrets from SSM:
python scripts/data/cs_refresh_all.py --from-ssm

# Strict mode (abort on first failure):
python scripts/data/cs_refresh_all.py --from-ssm --strict
```

### Skip Specific Steps

```bash
# Skip signal fetch and chart generation:
python scripts/data/cs_refresh_all.py --skip gw_signal,chart_data
```

Valid step names to skip: `gw_signal`, `api_to_tracking`, `tt_close_status`, `summary`, `performance`, `chart_data`, `dashboard`.

### Individual Steps

Each script runs standalone:

```bash
python scripts/data/cs_gw_signal_to_gsheet.py
python scripts/data/cs_api_to_tracking.py
python scripts/data/cs_summary_to_gsheet.py
python scripts/data/cs_performance_to_gsheet.py
python scripts/data/cs_chart_data_to_gsheet.py
python scripts/data/cs_build_dashboard_tab.py
```

### Separate Strategy Post-Steps

```bash
python scripts/data/bf_trades_to_gsheet.py
python scripts/data/bf_eod_tracking.py
python scripts/data/ds_tracking_to_gsheet.py
python scripts/data/reconcile_reporting.py
```

## Lambda Execution (Production)

The Lambda handler (`lambda/handler.py`) routes `{"account": "cs-refresh"}` to `_handle_cs_refresh()`, which shells out to `cs_refresh_all.py --from-ssm`.

### EventBridge Schedules (template.yaml)

| Schedule Name              | Cron (UTC)                      | ET Equivalent   | Purpose                                  |
|----------------------------|---------------------------------|-----------------|------------------------------------------|
| `gamma-cs-refresh`         | `cron(0 17 ? * MON-FRI *)`     | 5:00 PM ET (*)  | Primary post-close refresh               |
| `gamma-cs-refresh-morning` | `cron(0 9 ? * MON-FRI *)`      | 9:00 AM ET (*)  | Morning retry for overnight fill updates |

(*) UTC offset shifts with DST. During EDT these fire at 1 PM / 5 AM ET -- the cron expressions may need seasonal review.

### Lambda Event Payload

```json
{"account": "cs-refresh"}
{"account": "cs-refresh", "skip": "gw_signal,chart_data"}
```

The handler has a 300-second timeout for the full pipeline subprocess.

## Shared Library Pattern

### scripts/lib/sheets.py

All data scripts import from `scripts/lib/sheets.py` for Google Sheets auth and CRUD:

```python
from lib.sheets import sheets_client, ensure_tab, overwrite_rows, read_existing, get_values, col_letter, ensure_sheet_tab
```

**Key functions:**
- `sheets_client()` -- returns `(sheets_service, spreadsheet_id)`. Handles `GOOGLE_SERVICE_ACCOUNT_JSON` as raw JSON or base64-encoded.
- `ensure_tab(svc, sid, tab, headers)` -- creates tab if missing, sets header row.
- `overwrite_rows(svc, sid, tab, headers, rows)` -- clears tab then writes headers + rows.
- `read_existing(svc, sid, tab, headers)` -- reads all data rows (row 2+), pads to header length.
- `get_values(svc, sid, range_str)` -- raw `spreadsheets.values.get` wrapper.
- `col_letter(idx)` -- zero-based column index to letter (0='A', 26='AA').
- `ensure_sheet_tab(svc, sid, title)` -- creates sheet tab if missing, returns sheetId.

### scripts/lib/parsing.py

Shared parsing helpers:

- `safe_float(x, default)` -- converts to float, returns default on failure.
- `iso_fix(s)` -- normalizes ISO-8601 offset (Z to +00:00, +HHMM to +HH:MM).

### Path Setup Pattern

Every data script uses the same `_add_scripts_root()` pattern to find the `scripts/` directory and add it to `sys.path`:

```python
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
```

Scripts that also need TT client access (cs_api_to_tracking, cs_tt_close_status) additionally add `TT/Script/` to sys.path.

## Google Sheets Tab Reference

| Tab Name           | Writer Script                | Strategy    | Description                                |
|--------------------|------------------------------|-------------|--------------------------------------------|
| GW_Signal          | cs_gw_signal_to_gsheet.py   | CS          | Daily GammaWizard neural-net signal data   |
| CS_Tracking        | cs_api_to_tracking.py        | CS          | Per-account fill records (gap-filled)      |
| CS_TT_Close        | cs_tt_close_status.py        | CS          | Close order fill status and prices         |
| CS_Summary         | cs_summary_to_gsheet.py      | CS          | Daily pivot: signals + fills + P&L         |
| CS_Performance     | cs_performance_to_gsheet.py  | CS          | Monthly P&L grid + rolling stats           |
| CS_Chart_Daily     | cs_chart_data_to_gsheet.py   | CS          | Daily equity/P&L series for charts         |
| CS_Chart_Weekly    | cs_chart_data_to_gsheet.py   | CS          | Weekly aggregated series                   |
| CS_Chart_Monthly   | cs_chart_data_to_gsheet.py   | CS          | Monthly aggregated series                  |
| CS_Dashboard       | cs_build_dashboard_tab.py    | CS          | Side-by-side account comparison charts     |
| CS_Dashboard_Data  | cs_build_dashboard_tab.py    | CS          | Hidden helper data for dashboard charts    |
| sw_txn_raw         | (external)                   | CS          | Schwab raw transaction data (read-only)    |
| BF_Trades          | bf_trades_to_gsheet.py       | Butterfly   | Butterfly evaluation log + EOD tracking    |
| DS_Tracking        | ds_tracking_to_gsheet.py     | DualSide    | DualSide combined PUT+CALL trade rows      |

## Environment Variables

### Required for All Scripts

| Variable                      | Description                              | Source in Lambda     |
|-------------------------------|------------------------------------------|----------------------|
| `GSHEET_ID`                   | Google Spreadsheet ID                    | SSM `/gamma/shared/gsheet_id` |
| `GOOGLE_SERVICE_ACCOUNT_JSON` | Full JSON string for Google SA creds     | SSM `/gamma/shared/google_sa_json` |

### GW Signal (Step 1)

| Variable       | Description                    | Default                                          |
|----------------|--------------------------------|--------------------------------------------------|
| `GW_BASE`      | GammaWizard API base URL       | `https://gandalf.gammawizard.com`                |
| `GW_ENDPOINT`  | API endpoint path              | `rapi/GetUltraPureConstantStable`                |
| `GW_EMAIL`     | GW login email                 | SSM `/gamma/shared/gw_email`                     |
| `GW_PASSWORD`  | GW login password              | SSM `/gamma/shared/gw_password`                  |

### Broker APIs (Steps 2-3)

| Variable              | Description                        | Default / Source                      |
|-----------------------|------------------------------------|---------------------------------------|
| `SCHWAB_APP_KEY`      | Schwab API key                     | SSM `/gamma/schwab/app_key`           |
| `SCHWAB_APP_SECRET`   | Schwab API secret                  | SSM `/gamma/schwab/app_secret`        |
| `SCHWAB_TOKEN_PATH`   | Path to Schwab OAuth token file    | `/tmp/schwab_token.json`              |
| `TT_CLIENT_ID`        | TastyTrade client ID               | SSM `/gamma/tt/client_id`             |
| `TT_CLIENT_SECRET`    | TastyTrade client secret           | SSM `/gamma/tt/client_secret`         |
| `TT_TOKEN_PATH`       | Path to TT token file              | `/tmp/tt_token.json`                  |
| `TT_ACCOUNT_NUMBERS`  | Account map                        | `5WT09219:tt-individual,5WT20360:tt-ira` |

### CS Orchestrator Defaults (set by cs_refresh_all.py)

| Variable         | Description                           | Default                                   |
|------------------|---------------------------------------|-------------------------------------------|
| `CS_VOL_FIELD`   | Which vol field to use                | `VixOne`                                  |
| `CS_VIX_BREAKS`  | VIX bucket breakpoints               | `0.1636779,0.3276571,0.3702533,0.4514141` |
| `CS_VIX_MULTS`   | Position sizing multipliers per bucket | `1,1,1,1,1` (USC scaling disabled)       |

### Tab Name Overrides (Optional)

| Variable             | Default           |
|----------------------|-------------------|
| `CS_GW_SIGNAL_TAB`   | `GW_Signal`       |
| `CS_TRACKING_TAB`    | `CS_Tracking`     |
| `CS_TT_CLOSE_TAB`    | `CS_TT_Close`     |
| `CS_SUMMARY_TAB`     | `CS_Summary`      |
| `CS_PERFORMANCE_TAB`  | `CS_Performance`  |
| `CS_CHART_DAILY_TAB`  | `CS_Chart_Daily`  |
| `CS_CHART_WEEKLY_TAB` | `CS_Chart_Weekly` |
| `CS_CHART_MONTHLY_TAB`| `CS_Chart_Monthly`|
| `CS_DASH_ACCOUNT`     | `schwab-adjusted` |
| `BF_GSHEET_TAB`       | `BF_Trades`       |
| `DS_TRACKING_TAB`     | `DS_Tracking`     |
| `SW_RAW_TAB`          | `sw_txn_raw`      |

### Strict Mode

| Variable           | Description                              |
|--------------------|------------------------------------------|
| `CS_GSHEET_STRICT` | Set to `"1"` to fail hard on errors (default: non-blocking, errors are logged but do not abort) |

## Common Issues and Troubleshooting

### Stale Data / Missing Today's Row

- **Symptom:** CS_Summary or CS_Performance shows yesterday's data after 5 PM refresh.
- **Cause:** GW API may not have published today's signal yet, or broker APIs returned no fills.
- **Fix:** Wait for the 9 AM morning retry, or run manually: `python scripts/data/cs_refresh_all.py --from-ssm`
- **Check:** Look at GW_Signal tab -- if today's date is missing, the GW API is the bottleneck.

### Missing Columns / Schema Drift

- **Symptom:** Script crashes with KeyError or IndexError on column access.
- **Cause:** CS_Summary rows 1-8 are user-managed formulas. If someone edits the header row (row 10) or shifts columns, downstream scripts break.
- **Fix:** Check CS_Summary row 9-10 layout matches the expected schema. Never edit rows 9-10 manually.

### Google API Quota Exceeded

- **Symptom:** `HttpError 429` or `Quota exceeded for quota metric 'Write requests'`.
- **Cause:** Google Sheets API has a 300 write requests per minute limit per project.
- **Fix:** The scripts are designed to batch writes, but if multiple pipelines run simultaneously (e.g., CS refresh + BF post-step + DS post-step), quota can be hit. Space out manual runs. The morning retry at 9 AM naturally avoids the 5 PM congestion window.

### SSM Parameter Missing

- **Symptom:** `ParameterNotFound` error when running with `--from-ssm`.
- **Cause:** A required SSM parameter under `/gamma/` is missing or has wrong path.
- **Fix:** Check SSM Parameter Store in us-east-1 for the expected paths listed in `load_ssm_env()`.

### Token Expiry

- **Symptom:** Schwab or TT API calls return 401/403.
- **Cause:** OAuth tokens have expired. Schwab tokens expire every 7 days; TT tokens expire on session timeout.
- **Fix:** For Schwab, run the token refresh script (`scripts/refresh_schwab_token.sh`). For TT, the client auto-refreshes if `TT_CLIENT_ID` and `TT_CLIENT_SECRET` are set. In Lambda, tokens are seeded from SSM on each invocation.

### Lambda Timeout

- **Symptom:** `CS_REFRESH: TIMEOUT after 300s` in CloudWatch logs.
- **Cause:** One of the 7 steps hung (usually API calls). Individual step timeouts are 30-60s, but if multiple steps are slow, total can exceed 300s.
- **Fix:** Identify the slow step from CloudWatch logs (each step prints timing). Consider skipping non-critical steps: `{"account": "cs-refresh", "skip": "chart_data,dashboard"}`.

### Reconciliation Failures

- **Symptom:** `RECONCILE_MISS` in CloudWatch logs.
- **Cause:** Expected row for today's date not found in the specified tab, or required columns are empty.
- **Fix:** Check the tab directly. If the trade legitimately did not happen (holiday, skip signal), the miss is expected. Otherwise, re-run the appropriate data script.

## File Locations

- Orchestrator: `scripts/data/cs_refresh_all.py`
- Individual scripts: `scripts/data/cs_*.py`, `scripts/data/bf_*.py`, `scripts/data/ds_*.py`
- Reconciliation: `scripts/data/reconcile_reporting.py`
- Shared lib: `scripts/lib/sheets.py`, `scripts/lib/parsing.py`
- Lambda handler: `lambda/handler.py` (function `_handle_cs_refresh`)
- SAM template schedules: `lambda/template.yaml` (CsRefreshSchedule, CsRefreshMorningSchedule)
