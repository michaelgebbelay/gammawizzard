# GammaWizzard Trading System

Automated options trading across Schwab and TastyTrade accounts, triggered daily via AWS Lambda + EventBridge.

---

## Architecture

```
EventBridge Scheduler (America/New_York)
  4:10 PM  -->  Lambda warmup ping
  4:13 PM  -->  Lambda x3 (schwab, tt-ira, tt-individual) in parallel
                  |
                  +-- Read secrets from SSM Parameter Store
                  +-- Seed token files to /tmp
                  +-- Run orchestrator.py via subprocess
                  +-- Post-trade: push to Google Sheets, edge guard
                  +-- Persist refreshed tokens back to SSM
```

All trading scripts run unmodified inside a container-image Lambda. EventBridge handles DST natively via `ScheduleExpressionTimezone`.

---

## Strategies

### ConstantStable Verticals (primary)
- Iron condor strategy driven by GammaWizard signal
- Sizing based on VIX regime buckets (VixOne field)
- Per-account sizing overrides via `TT/data/account_sizing.json`
- Ladder pricing with configurable step/poll/cancel timing

---

## Accounts

| Account | Broker | ID | Unit Dollars |
|---------|--------|----|-------------|
| Schwab | Charles Schwab | (primary) | $10,000 |
| TT IRA | TastyTrade | 5WT20360 | $6,000 |
| TT Individual | TastyTrade | 5WT09219 | $7,500 |

---

## Repository Structure

```
scripts/
  trade/ConstantStable/    # Schwab orchestrator + place.py
  trade/leocross/          # LeoCross orchestrator + placer
  data/                    # Data scripts (dump, summarize, backfill)
  lib/                     # Shared modules (sheets.py, parsing.py)
  notify/                  # Email notifications
  schwab_token_keeper.py   # Schwab OAuth token management

TT/
  Script/ConstantStable/   # TT orchestrator + place.py + DXLink quotes
  Script/tt_token_keeper.py
  Script/tt_client.py      # TT REST wrapper
  Script/tt_dxlink.py      # TT WebSocket streaming
  data/                    # TT data scripts (mirror of scripts/data/)

lambda/
  handler.py               # Lambda entry point (wraps orchestrators)
  template.yaml            # SAM template (Lambda + EventBridge + IAM)
  Dockerfile               # Container image definition
  requirements-lambda.txt  # Python dependencies
  seed_ssm.sh              # One-time SSM parameter seeding
  test_event_*.json        # Dry-run test payloads

.github/workflows/
  deploy_lambda.yml                    # SAM build + deploy (manual)
  constantstable_verticals.yml         # Schwab CS (DISABLED - replaced by Lambda)
  tt_constantstable_verticals.yml      # TT CS (DISABLED - replaced by Lambda)
  schwab_raw_data.yml                  # Schwab transaction data to Sheets
  tt_raw_data.yml                      # TT transaction data to Sheets
  constantstable_edge_backfill.yml     # Edge/transfer backfill
  tt_constantstable_edge_backfill.yml  # TT edge backfill
  gw_fetch.yml                         # GammaWizard data fetch
  tt_probe.yml                         # TT connectivity test
  tt_token_keepalive.yml               # TT token refresh
```

### Shared Library (`scripts/lib/`)

Consolidated utilities used by all data scripts:
- **sheets.py**: `sheets_client()`, `ensure_tab()`, `overwrite_rows()`, `read_existing()`, `get_values()`
- **parsing.py**: `safe_float()`, `iso_fix()`, `fmt_ts_et()`, `parse_sheet_datetime()`, `parse_sheet_date()`, `contract_multiplier()`, `ET` timezone

---

## Infrastructure

### AWS Lambda + EventBridge (production trading)
- **Lambda**: Container image (Python 3.12), 1024 MB, 120s timeout
- **Schedules**: 4:10 PM warmup + 4:13 PM x3 accounts (Mon-Fri ET)
- **Secrets**: SSM Parameter Store SecureStrings under `/gamma/`
- **Token persistence**: Tokens auto-refresh during execution; handler detects changes via SHA-256 and writes back to SSM
- **Cost**: < $1/month (free tier)

### GitHub Actions (deployment + data workflows)
- **deploy_lambda.yml**: Manual dispatch to build + deploy SAM stack
- **schwab_raw_data.yml**: Schwab transaction data to Google Sheets
- **tt_raw_data.yml**: TT transaction data to Google Sheets
- Trading workflows are **disabled** -- replaced by Lambda

---

## Deployment

Deploy or update the Lambda stack via GitHub Actions:

```bash
# Deploy with schedules enabled
gh workflow run "Deploy Trading Lambda" \
  -f schedule_state=ENABLED -f dry_run=false

# Deploy with dry run (no real orders)
gh workflow run "Deploy Trading Lambda" \
  -f schedule_state=ENABLED -f dry_run=true

# Disable schedules without deleting
gh workflow run "Deploy Trading Lambda" \
  -f schedule_state=DISABLED -f dry_run=false
```

### First-time setup

1. Create AWS account, install AWS CLI: `brew install awscli`
2. Create IAM user `gamma-deploy` with AdministratorAccess
3. `aws configure` with the access key
4. Run `./lambda/seed_ssm.sh` to populate SSM secrets
5. Set `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` as GitHub repo secrets
6. Trigger the deploy workflow

---

## Secrets

### SSM Parameter Store (Lambda)
```
/gamma/schwab/app_key
/gamma/schwab/app_secret
/gamma/schwab/token_json
/gamma/tt/client_id
/gamma/tt/client_secret
/gamma/tt/token_json
/gamma/shared/gsheet_id
/gamma/shared/google_sa_json
/gamma/shared/gw_email
/gamma/shared/gw_password
/gamma/shared/smtp_user       # optional
/gamma/shared/smtp_pass       # optional
/gamma/shared/smtp_to         # optional
```

### GitHub Secrets (Actions workflows)
SCHWAB_APP_KEY, SCHWAB_APP_SECRET, SCHWAB_TOKEN_JSON, GSHEET_ID, GOOGLE_SERVICE_ACCOUNT_JSON, GW_EMAIL, GW_PASSWORD, TT_CLIENT_ID, TT_CLIENT_SECRET, TT_TOKEN_JSON, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

---

## Monitoring

```bash
# Tail Lambda logs (real-time)
aws logs tail /aws/lambda/gamma-trading-TradingFunction-O0rzFPNn3umX \
  --follow --region us-east-1

# Last hour of logs
aws logs tail /aws/lambda/gamma-trading-TradingFunction-O0rzFPNn3umX \
  --since 1h --region us-east-1

# Manual invoke (dry run)
aws lambda invoke --function-name gamma-trading-TradingFunction-O0rzFPNn3umX \
  --payload "$(echo '{"account":"schwab","dry_run":true}' | base64)" \
  --region us-east-1 /dev/stdout

# Check schedule status
aws scheduler list-schedules --region us-east-1 \
  --query 'Schedules[].{Name:Name,State:State}'
```

### Google Sheets tabs
- **sw_txn_raw**: Raw Schwab transactions
- **sw_orig_orders**: Classified iron condor orders
- **ConstantStableEdge**: Edge guard daily stats
- **ConstantStableTrades**: Trade execution log

---

## Runbook

| Issue | Fix |
|-------|-----|
| Schwab token expired | Re-run `Token/schwab_token_bootstrap.py`, then update SSM: `aws ssm put-parameter --name /gamma/schwab/token_json --value file://Token/schwab_token.json --type SecureString --overwrite --region us-east-1` |
| TT token expired | Refresh via `TT/Script/tt_token_refresh.py`, then update SSM similarly |
| Lambda timeout | Check CloudWatch logs; increase timeout in template.yaml if needed |
| Need to disable trading | `gh workflow run "Deploy Trading Lambda" -f schedule_state=DISABLED -f dry_run=false` |
| Need to update code | Push to main, then run deploy workflow |
| GammaWizard auth failure | Falls back to email/password automatically |
