# Path-S Post-90 Continuation Test

## Hypothesis

The 90-day max hold may be too blunt for winners. After day 90, if there
is no fresh qualifying signal needing capital, keep the current winner
alive with a tighter trailing stop.

This is structurally different from removing exits. It says: do not
force cash if the current trade is still working and nothing better is
available.

## Spec (locked)

Baseline (X0): production candidate.

```
z=3.0 Path S single
top-2000 universe (CS / non-pharma / optionable / major exchange)
SPY > 200d regime gate
trend filter: close > 50d SMA, ret_60d > 0
eligibility: $25M $vol, ret_12m in [0,5], trend_status in {INTACT,PULLBACK}
20% trailing stop
90-day max hold
no displacement
no earnings blackout
no additional trend floors
15bps cost
```

Variants (all share baseline; diff is exit logic only):

| Mode | exit_mode flag value | Behavior at day 90+ |
|---|---|---|
| **X0 baseline** | `baseline` | forced exit at MAX_HOLD_90D |
| **X1 primary** | `post90_green_no_candidate_trail10` | if green AND no fresh z>=3 candidate, extend with 10% trail |
| **X2 diagnostic** | `post90_green_always_trail10` | if green, extend (ignore fresh candidates) |
| **X3 diagnostic** | `post90_any_no_candidate_trail10` | if no fresh candidate, extend (ignore return sign) |
| **X4 diagnostic** | `no_max_hold_trail20` | no max hold, just standard 20% trail |

X1 rules in detail:

```
before day 90:
  active trailing stop = 20%

on/after day 90:
  if unrealized_return <= 0:
      exit at max hold      # don't extend losers
  elif fresh z>=3 candidate exists and != current_ticker:
      exit current; rotate (engine picks fresh on next bar)
  else:
      continue holding current
      active trailing stop = 10% (on peak_close since entry, NOT day-90 reset)

SPY regime-off exit always overrides.
costs unchanged.
```

## Acceptance bar

Promote X1 to production only if:

- Sharpe improves or stays close (within 0.05 of baseline)
- MDD does not materially worsen (within 5pp)
- Calmar improves
- Total return improves WITHOUT being one-trade dependent
- Post-90 extension does not block better fresh candidates (X1 vs X2 delta)
- Losers are not routinely extended past 90 days (X1 vs X3 delta)

Reject X1 if:

- Most of the gain comes from one trade (drop-one INTC sensitivity must
  still show edge preservation)
- MDD worsens meaningfully (>5pp vs baseline)
- It blocks strong fresh z>=3 entries (compare missed_candidate_log)
- It turns slow losers into longer slow losers
- X4 looks tempting only because of one monster

## Drop-one sensitivity

Each variant runs twice: full universe and INTC-excluded. If X1 only
works because of INTC, reject it as low-N shrine-building. We've been
through enough of that with F3 and the displacement variants.

## Diagnostics required

Each run produces:

- `summary.json` — headline metrics
- `trade_log.csv` — one row per closed trade
- `daily_equity.csv` — full equity curve
- `post90_extension_log.csv` — one row per day a position was extended
  past max-hold (only when exit_mode != baseline)
- `missed_candidate_log.csv` — fresh z>=3 candidates passed up due to
  extension (only when exit_mode = post90_green_always_trail10)

## Comparison

After all 10 runs complete, compute:

| variant | TR | CAGR | Sharpe | MDD | Calmar | trades | avg hold | post90_ext | drop-INTC TR delta |
|---|---|---|---|---|---|---|---|---|---|

Plus per-trade table for the 6 max-hold trades in baseline:

| ticker | entry_date | day90_price | day90_return | baseline_exit_return | X1_exit_date | X1_exit_return | delta_vs_baseline |
|---|---|---|---|---|---|---|---|

## Workflow

```
gh workflow run path_s_post90_test.yml -f end_date=2026-04-29
gh run watch
gh run list --workflow path_s_post90_test.yml --limit 1
gh run download <run-id>
```

10 parallel runs on free GHA runners; total wall time ~30-60 min.

## Doctrine boundary

This test must clear the verifier and the acceptance bar before any
live change. The existing production spec is clear:
20% trailing stop, 90-day max hold, SPY regime exit, no discretionary
swap logic. Any post-90 continuation rule must beat the frozen baseline
cleanly, drop-one robust, and survive the diagnostic logs before
getting near production.
