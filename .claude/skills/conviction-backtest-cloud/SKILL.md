---
name: conviction-backtest-cloud
description: Offload conviction backtest sweeps to GitHub Actions instead of running them locally. Use when the user has a (skew-z, days, or other-knob) sweep that would tie up local CPU for hours, or any time multiple replay.py invocations are independent and parallelizable. Each matrix leg runs on a free GitHub-hosted runner, syncs parquet data from S3, and uploads results as artifacts.
---

# Conviction Backtest — Cloud Sweep Skill

Wraps `.github/workflows/conviction_backtest.yml`. Fans a `(skew_z_min × days)` matrix across parallel GitHub Actions runners, no local compute.

## When to use this vs running locally

**Use cloud:**
- Sweeping ≥4 parameter combinations (matrix legs run in parallel for free).
- Single run takes >30min and the user is also doing other work on their laptop.
- User explicitly asks to offload, save CPU, or run "in the cloud."

**Stay local:**
- One-off run for quick iteration on logic — pushing a workflow + waiting for runner spinup is slower than a local 30s run.
- Debugging the replay engine itself (inspect intermediate state, attach a debugger).
- Strategy other than `pathS` with Schwab data source — workflow currently assumes `--source massive` (no Schwab API call).

## Prerequisites (verify before kicking off a run)

1. **Data staged in S3.** The workflow pulls from `s3://gamma-sim-cache/conviction-backtest-data/`. Check it exists:
   ```
   aws s3 ls s3://gamma-sim-cache/conviction-backtest-data/
   ```
   Should show `aggs_daily_adjusted.parquet`, `aggs_daily.parquet`, `skew_daily.parquet`, `skew_daily_shard_*.parquet`, `splits.parquet`, `ticker_metadata.parquet`. **If empty or missing files, sync first:**
   ```
   aws s3 sync "scripts/conviction/backtest/data/" \
     s3://gamma-sim-cache/conviction-backtest-data/ \
     --exclude "day_aggs_raw/*" --exclude "*.json"
   ```
   3GB upload. Re-sync only when parquets are regenerated (after `massive_ingest.py` etc.).

2. **`gh` CLI on PATH** — lives at `~/Downloads/gh_2.91-2.0_macOS_amd64/bin/gh`, may not be on PATH. Prefix with that dir if `gh` not found.

3. **Repo secrets `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`** must have S3 read on `gamma-sim-cache`. They do (same secrets used by `leoprofit_test.yml`, etc.).

## How to fire a sweep

Default 8-leg sweep (4 z-values × 2 lookback windows):

```
gh workflow run conviction_backtest.yml \
  -f z_values=1.5,2.0,2.5,3.0 \
  -f days_values=1460,730 \
  -f end_date=2026-04-29
gh run watch
```

All workflow inputs (defaults in parentheses):

| Input | Default | Maps to replay.py flag |
|---|---|---|
| `z_values` | `1.5,2.0,2.5,3.0` | `--skew-z-min` (matrix axis) |
| `days_values` | `1460,730` | `--days` (matrix axis) |
| `end_date` | `2026-04-29` | `--end-date` |
| `strategy` | `pathS` | `--strategy` |
| `skew_direction` | `bullish` | `--skew-direction` |
| `universe_top_n` | `2000` | `--universe-top-n` |
| `positions` | `2` | `--positions` |
| `max_hold_days` | `90` | `--max-hold-days` |
| `trailing_pct` | `20` | `--trailing-pct` |

**Hardcoded** (would need workflow edits to change): `--source massive`, `--ignore-themes`, `--exit-rule trailing_pct`, `--regime-gate spy`. If a sweep needs to vary one of those, edit the YAML — don't try to wedge it into another input.

## Retrieving results

Each matrix leg uploads an artifact named `zmin_<z>_<label>` (where `label` = `4y` for days=1460, `2y` for days=730, else `<days>d`). Contains:
- `zmin_<z>_<label>.log` — full replay.py stdout/stderr
- `results/` — anything replay.py wrote to `scripts/conviction/backtest/results/` during that run

Download all artifacts for a run:

```
gh run list --workflow=conviction_backtest.yml --limit 1
gh run download <run-id>            # downloads all artifacts to ./
# or
gh run download <run-id> -n zmin_2.0_4y    # one specific leg
```

Artifacts retained 14 days.

## Failure modes & fixes

- **`Sync backtest data from S3` step fails with `(403)` or `(404)`:**
  Either S3 prefix is empty (run the sync) or the IAM identity behind repo secrets lacks `s3:GetObject` on `gamma-sim-cache`. Check by running `aws s3 ls s3://gamma-sim-cache/conviction-backtest-data/` locally.

- **`Run replay` step fails with `ImportError`:**
  Pinned versions in the workflow (`pandas==3.0.2` etc.) may have drifted from local `.venv`. Compare:
  ```
  .venv/bin/python -c "import pandas, numpy, pyarrow, scipy, boto3, yaml, requests; \
    print(pandas.__version__, numpy.__version__, pyarrow.__version__, \
          scipy.__version__, boto3.__version__, yaml.__version__, requests.__version__)"
  ```
  Update the `pip install` block in [.github/workflows/conviction_backtest.yml](.github/workflows/conviction_backtest.yml).

- **Job hits 350-min timeout:**
  Single replay leg ran longer than expected. Either reduce `--days` or split into smaller matrix combos. The 350m limit is just under GH's 360m hard cap.

- **Empty results/log artifacts:**
  Step failed before upload. Check the run page: `gh run view <run-id> --log-failed`.

## Pitfalls / things that bite

- **Don't push secrets via workflow inputs.** All inputs become visible in the run UI. Keep secrets in repo secrets, reference via `${{ secrets.X }}` in YAML.
- **Re-syncing parquets after schema changes** isn't automatic. If `massive_ingest.py` regenerates `aggs_daily_adjusted.parquet`, you must re-run the `aws s3 sync` before the next cloud sweep, or runs will use stale data.
- **Free runners are 4 vCPU / 16 GB.** Replay is mostly single-threaded so vCPU count doesn't matter, but watch RAM if loading huge skew shards alongside aggs — current setup fits fine.
- **Cost ceiling.** 2000 free runner-min/mo on private repos. 8 legs × 3hr = 1440 min/sweep. Two full sweeps in a month and you start paying $0.008/min. Budget accordingly.
