# TS_6 Candidate Overlay Spec

Status: research candidate only as of 2026-05-06. Production remains plain
`B0` / `C1-HYST`. The TS_6 shadow state machine is now implemented and
persisted, but the overlay remains disabled for trading until explicitly
reviewed and promoted.

This document locks the basis and timing contract for the candidate trailing
stop overlay that survived the strict-timing research.

## Evidence

- Strict robustness run:
  `out/ts_band_strict_robustness_20260505/summary.txt`
- Pure broker-history revalidation:
  `out/ts_band_schwab_revalidation_20260505/summary.txt`
- Hybrid live-style revalidation:
  `out/ts_band_hybrid_revalidation_20260505/summary.txt`
- Historical replay parity:
  `out/tqqq_ts6_replay_parity_20260505/parity_summary.txt`

## Confirmed live baseline plumbing

1. Live `C1-HYST` currently computes QQQ from adjusted closes via
   `yfinance(..., auto_adjust=True)` in `scripts/trade/TqqqTrend/place.py`.
2. Live execution runs after the open and acts on the most recent closed QQQ
   session. See `scripts/trade/TqqqTrend/orchestrator.py`.
3. Current `TqqqTrend` live code persists TS_6 shadow state at
   `scripts/trade/TqqqTrend/state/ts6_overlay_state.json`, hydrated from and
   persisted back to the workflow secret `TQQQ_TS6_STATE_JSON`.
4. Live orders still follow plain `B0` unless `TQQQ_TS6_ENABLED` is explicitly
   set truthy in the rebalance workflow environment.

## Locked decision

If a TS overlay is added to the current live `C1-HYST` stack, the stop trigger
must use the same adjusted QQQ close series as the live signal.

This is not optional style preference. The raw Schwab QQQ close path changes
the historical stop sequence enough to invalidate `TS_6`:

- On the raw Schwab basis, `TS_6` is stopped out on 2013-06-24 and stays out
  through 2014-04-11, missing a large TQQQ advance.
- On the adjusted-QQQ signal basis, `TS_6` remains the all-window winner.

## Candidate selection

- Default candidate under adjusted QQQ basis: `TS_6`
- Backup candidate if policy later requires raw Schwab QQQ stop basis:
  `TS_6.5`
- `TS_5.5` is not the lead candidate and should not be revived as the default.

## Timing contract

At each market close `t`:

1. Compute the baseline `C1-HYST` state from adjusted QQQ through `close[t]`.
2. Compute the TS overlay state from adjusted QQQ through `close[t]`.
3. Emit the target sleeve for the earliest next executable session.

Rules:

- No same-day exposure mutation.
- No intraday stop trigger.
- Schwab tradable prices are for execution and realized sleeve returns only.
- If the next scheduled run is delayed, it still acts on the last completed
  close-driven target state. It must not recompute using partial intraday QQQ.

## Overlay state machine

The overlay sits on top of the baseline `C1-HYST` state.

Definitions:

- `baseline_state[t]` is the hysteresis state after evaluating `C1-HYST` on
  adjusted QQQ close `t`
- `adj_close[t]` is the adjusted QQQ close at `t`
- `peak` is the highest adjusted QQQ close seen since the current overlay
  risk-on entry
- `blocked` means the stop has fired during an ongoing baseline risk-on regime

For the plain `TS_6` candidate:

```text
If baseline_state[t] != RISKON:
    target[t] = BIL
    holding = False
    blocked = False
    peak = None

Else if holding:
    peak = max(peak, adj_close[t])
    if adj_close[t] / peak - 1 <= -0.06:
        target[t] = BIL
        holding = False
        blocked = True
    else:
        target[t] = TQQQ

Else if not blocked:
    target[t] = TQQQ
    holding = True
    peak = adj_close[t]

Else:
    target[t] = BIL
```

## Re-entry rule

Plain `TS_6` has no extra repair signal and no same-regime re-entry.

After a stop exit during an ongoing baseline risk-on regime:

- remain in `BIL`
- keep `blocked = True`
- do not re-enter while baseline remains `RISKON`

`blocked` clears only when the baseline `C1-HYST` state itself leaves
`RISKON` and returns to `BIL`. The next eligible re-entry is then the next
future baseline transition back to `RISKON`.

That is the exact re-entry rule for the current candidate.

## Required persisted state

Any live implementation must persist overlay state across days. Recommended
fields:

- `as_of_close`
- `signal_source`
- `execution_source`
- `baseline_state`
- `overlay_target_state`
- `overlay_holding`
- `stop_blocked`
- `peak_adjusted_close_since_entry`
- `stop_threshold_pct`
- `last_stop_trigger_date`
- `last_stop_trigger_close`

Recommended values for the basis fields:

- `signal_source = "adjusted_qqq_close"`
- `execution_source = "schwab_tqqq_bil"`

## Fail-closed rules

Do not silently substitute another data basis.

Abort or no-op if any of the following is true:

- adjusted QQQ close series is unavailable, stale, or incomplete
- overlay is enabled and persisted state is missing or malformed
- current live sleeve is ambiguous (`MIXED`) and cannot be reconciled
- code attempts to evaluate the stop on raw Schwab QQQ closes while this spec
  is active

When the overlay is disabled:

- shadow state is still hydrated, advanced, and persisted
- shadow-state failures must be surfaced in logs / monitor output
- shadow-state failures must NOT block plain `B0` production behavior
- shadow-state existence must NOT imply live enablement

If production policy ever requires raw Schwab QQQ stop basis, that is a spec
change. The candidate changes with it:

- reject `TS_6`
- promote `TS_6.5` to the revalidation path
- rerun implementation-parity checks before any live change

## Operational conclusion

The current candidate is:

```text
Baseline signal basis: adjusted QQQ close
Overlay stop basis:    adjusted QQQ close
Execution basis:       Schwab TQQQ/BIL
Timing:                close[t] decision, earliest next-session execution
Primary overlay:       TS_6
Fallback if raw basis: TS_6.5
```
