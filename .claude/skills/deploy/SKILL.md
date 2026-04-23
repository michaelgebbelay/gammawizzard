---
name: deploy
description: Deploy the Lambda trading infrastructure via GitHub Actions. Use when the user asks to deploy, update schedules, push infrastructure changes, or validate the SAM template.
disable-model-invocation: true
---

# Deploy Skill

You deploy the **gamma-trading** Lambda stack to AWS via GitHub Actions. Docker is NOT available locally -- all deploys go through the `deploy_lambda.yml` workflow.

## Infrastructure Overview

- **Stack**: `gamma-trading` (AWS SAM / CloudFormation)
- **Region**: `us-east-1`
- **AWS Account**: `326730684923`, IAM user: `gamma-deploy`
- **Lambda Function**: `gamma-trading-TradingFunction-O0rzFPNn3umX`
- **S3 Bucket**: `gamma-sim-cache` (chain cache, event files, state)
- **Template**: `lambda/template.yaml`
- **Handler**: `lambda/handler.py` (dispatches by `account` key in event payload)
- **Dockerfile**: `lambda/Dockerfile` (container-image Lambda, built during SAM build)

## SAM Template Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `ScheduleTime` | `cron(13 16 ? * MON-FRI *)` | Cron for CS trade schedules (ET) |
| `DryRun` | `false` | Pass `dry_run=true` to handler (no real orders) |

## EventBridge Schedules (All Times ET)

All schedules use `ScheduleExpressionTimezone: "America/New_York"` and `FlexibleTimeWindow: OFF`.

### Trading Schedules

| Schedule Name | Time (ET) | Cron | State | Event Payload | Description |
|---------------|-----------|------|-------|---------------|-------------|
| `gamma-cs-warmup` | 3:58 PM | `cron(58 15 ? * MON-FRI *)` | ENABLED | `{"account": "warmup"}` | Container warmup, 3 min before trades |
| `gamma-cs-schwab` | 4:13 PM | `ScheduleTime` param | ENABLED | `{"account": "schwab"}` | ConstantStable Schwab |
| `gamma-cs-tt-ira` | 4:13 PM | `ScheduleTime` param | DISABLED | `{"account": "tt-ira"}` | CS TT IRA (switched to Novix) |
| `gamma-cs-tt-individual` | 4:13 PM | `ScheduleTime` param | ENABLED | `{"account": "tt-individual"}` | CS TT Individual |
| `gamma-bf-daily` | 4:01 PM | `cron(1 16 ? * MON-FRI *)` | ENABLED | `{"account": "butterfly"}` | ButterflyTuesday hybrid strategy |
| `gamma-nx-tt-ira` | 4:13 PM | `cron(13 16 ? * MON-FRI *)` | ENABLED | `{"account": "novix-tt-ira"}` | Novix TT IRA |
| `gamma-nx-tt-individual` | 4:13 PM | `cron(13 16 ? * MON-FRI *)` | DISABLED | `{"account": "novix-tt-individual"}` | Novix TT Individual (staying with CS) |
| `gamma-ds-schwab` | 4:05 PM | `cron(5 16 ? * MON-FRI *)` | ENABLED | `{"account": "dualside"}` | DualSide verticals Schwab |
| `gamma-cs-ic-long-filter` | 4:01 PM | `cron(1 16 ? * MON-FRI *)` | ENABLED | `{"account": "ic-long-filter"}` | IC_LONG regime filter pre-check |
| `gamma-cs-ic-long-morning` | 9:35 AM | `cron(35 9 ? * MON-FRI *)` | ENABLED | `{"account": "ic-long-morning"}` | IC_LONG deferred morning entry (Schwab) |
| `gamma-cs-ic-long-morning-tt-ira` | 9:35 AM | `cron(35 9 ? * MON-FRI *)` | DISABLED | `{"account": "ic-long-morning-tt-ira"}` | IC_LONG morning TT IRA (switched to Novix) |
| `gamma-cs-ic-long-morning-tt-individual` | 9:35 AM | `cron(35 9 ? * MON-FRI *)` | ENABLED | `{"account": "ic-long-morning-tt-individual"}` | IC_LONG morning TT Individual |

### Reporting & Monitoring Schedules

| Schedule Name | Time (ET) | Cron | State | Event Payload | Description |
|---------------|-----------|------|-------|---------------|-------------|
| `gamma-cs-morning-check` | 9:35 AM | `cron(35 9 ? * MON-FRI *)` | ENABLED | `{"account": "morning-check"}` | Morning equity/drawdown email alert |
| `gamma-daily-pnl` | 4:45 PM | `cron(45 16 ? * MON-FRI *)` | ENABLED | `{"account": "daily-pnl"}` | Daily P&L email after settlement |
| `gamma-weekly-pnl` | Sat 9:00 AM | `cron(0 9 ? * SAT *)` | ENABLED | `{"account": "weekly-pnl"}` | Weekly P&L report Saturday morning |
| `gamma-cs-refresh` | 5:00 PM | `cron(0 17 ? * MON-FRI *)` | ENABLED | `{"account": "cs-refresh"}` | CS full reporting refresh |
| `gamma-cs-refresh-morning` | 9:00 AM | `cron(0 9 ? * MON-FRI *)` | ENABLED | `{"account": "cs-refresh"}` | CS morning retry for missed fills |

### Sim Chain Collection Schedules

| Schedule Name | Time (ET) | Cron | State | Event Payload | Description |
|---------------|-----------|------|-------|---------------|-------------|
| `gamma-sim-collect-open` | 9:31 AM | `cron(31 9 ? * MON-FRI *)` | ENABLED | `{"account": "sim-collect", "phase": "open"}` | SPX 0DTE chain at open |
| `gamma-sim-collect-mid` | 12:00 PM | `cron(0 12 ? * MON-FRI *)` | ENABLED | `{"account": "sim-collect", "phase": "mid"}` | SPX chain at midday |
| `gamma-sim-collect-close` | 4:00 PM | `cron(0 16 ? * MON-FRI *)` | ENABLED | `{"account": "sim-collect", "phase": "close"}` | SPX chain at close |
| `gamma-sim-collect-close5` | 4:05 PM | `cron(5 16 ? * MON-FRI *)` | ENABLED | `{"account": "sim-collect", "phase": "close5"}` | SPX 1DTE chain at close+5 |

## Handler Account Dispatch

The Lambda handler (`lambda/handler.py`) uses the `account` key from the event payload to select from the `ACCOUNTS` dict. Each entry specifies:
- `orchestrator`: the Python script to run
- `post_steps`: list of post-trade scripts (gsheet sync, tracking, reconciliation)
- `token_ssm_path`: SSM path for token seeding
- `env_from_ssm`: per-account SSM params to inject
- `static_env`: hardcoded env vars for the account

Special dispatch routes (not in `ACCOUNTS` dict):
- `warmup` -- multi-container warmup via self-invoke
- `sim-collect` -- SPX chain collection to S3 (`_handle_sim_collect`)
- `daily-pnl` -- daily P&L email
- `weekly-pnl` -- weekly P&L report
- `cs-refresh` -- CS full reporting pipeline

## Step 1: Validate the SAM Template

Before pushing changes, validate the template locally:

```bash
cd /Users/mgebremichael/Documents/Gamma/lambda && sam validate
```

This catches YAML syntax errors and CloudFormation schema issues. You do NOT need Docker for validation.

## Step 2: Commit and Push Changes

Commit the relevant files (`lambda/template.yaml`, `lambda/handler.py`, any new scripts) and push to the `main` branch. The deploy workflow is `workflow_dispatch` only -- pushing alone does NOT trigger a deploy.

## Step 3: Trigger the Deploy

```bash
gh workflow run deploy_lambda.yml --ref main
```

To deploy with dry run enabled (no real orders):
```bash
gh workflow run deploy_lambda.yml --ref main -f dry_run=true
```

To deploy for live trading:
```bash
gh workflow run deploy_lambda.yml --ref main -f dry_run=false
```

## Step 4: Monitor the Deploy

Check workflow status:
```bash
gh run list --workflow=deploy_lambda.yml --limit 3
```

Watch a specific run to completion:
```bash
gh run watch <run-id> --exit-status
```

View logs on failure:
```bash
gh run view <run-id> --log-failed
```

## Step 5: Verify Post-Deploy

After a successful deploy, confirm the Lambda was updated:
```bash
aws lambda get-function-configuration \
  --function-name gamma-trading-TradingFunction-O0rzFPNn3umX \
  --region us-east-1 \
  --query '{LastModified: LastModified, MemorySize: MemorySize, Timeout: Timeout, State: State}'
```

## Common Issues

### Circular Dependency (Self-Invoke Policy)
The Lambda needs permission to invoke itself (for multi-container warmup). Using `!GetAtt TradingFunction.Arn` in the self-invoke policy creates a circular reference. The fix is to use `!Sub` with a wildcard:
```yaml
Resource: !Sub "arn:aws:lambda:${AWS::Region}:${AWS::AccountId}:function:*"
```
This is already in place in `template.yaml`. Do NOT change it to reference the function directly.

### Docker Not Available Locally
SAM build for container-image Lambdas requires Docker. The user's Mac does not have Docker installed. **Always deploy via GitHub Actions**, never try `sam build` or `sam deploy` locally.

### Schedule State Changes
To enable/disable a schedule, change the `State` field in `template.yaml` (ENABLED/DISABLED) and redeploy. Do not use the AWS console -- it will be overwritten on next deploy.

### Adding a New Schedule
1. Add a new `AWS::Scheduler::Schedule` resource in `template.yaml`
2. Add a matching account entry in `handler.py` `ACCOUNTS` dict (or handle in `lambda_handler` for special routes)
3. The schedule must target `TradingFunction.Arn` with `SchedulerRole.Arn`
4. Use `ScheduleExpressionTimezone: "America/New_York"` for all schedules
5. Validate, commit, push, and trigger deploy

### DryRun Parameter
The `DryRun` SAM parameter is passed through to the Lambda environment as `DRY_RUN`. When `true`, the handler still runs the full orchestrator pipeline but individual strategy scripts check this env var to skip order placement. To deploy with dry run:
```bash
gh workflow run deploy_lambda.yml --ref main -f dry_run=true
```
After verifying, redeploy with `dry_run=false` to go live.

### Rollback
If a deploy breaks things, either:
- Fix forward: commit a fix, push, and redeploy
- Rollback via CloudFormation: `aws cloudformation rollback-stack --stack-name gamma-trading --region us-east-1`
- Or use the AWS console to roll back the `gamma-trading` stack to the previous version
