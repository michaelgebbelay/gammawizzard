---
name: backtest-pitfalls
description: Sanity checks and known traps for equity strategy backtests in this repo. Covers split-adjustment bugs (NVDA-style phantom losses), survivorship bias from curated universes (themes.yaml carrying hindsight), in-sample vs out-of-sample inflation (rules derived from same data they're tested on), reverse-split phantom gainers contaminating screens, distressed-equity contamination of winner cohorts, end-of-window position leaks, exit-code traps in cron wrappers, **same-day decision leakage** (close[t] signal applied to return[t] — silently inflates trailing-stop / vol-target / drawdown-rule results). Use when designing a new backtest, evaluating results that look too good to be true (>30% CAGR or Sharpe > 1.5), interpreting p-values from cohort studies, or debugging surprising trade logs.
---

# Backtest Pitfalls Skill

Specific traps we've hit running equity backtests in this repo, with concrete fixes and detection signatures. If a backtest result looks great, work through this list before quoting it as forward expectation.

## 1. Unadjusted bars cause split-day phantom losses

**Trap**: Massive's `day_aggs_v1` flat files are UNADJUSTED. NVDA's 2024-06-07 10:1 split shows as $1208 → $121 in raw data. Any backtest holding NVDA through that day records a phantom 90% drop, fires the trailing stop, logs a -73% loss.

**Detection signatures**:
- A single trade with -50%+ loss that lasted only a few days → check for splits in the holding window
- Top-15 winners include +900%-ish moves on names you don't recognize → reverse splits look like gainers in raw data
- Equity curve has a one-day cliff drop that recovers immediately

**Fix**: Always use `aggs_daily_adjusted.parquet`. `massive_ingest.load_parquet()` auto-loads it and warns when it falls back to raw. After any data refresh, run:
```bash
bash scripts/conviction/backtest/run_massive_ingest.sh --fetch-splits
```

## 2. Reverse splits show as +900% gainers in screens

**Trap**: A 1:10 reverse split looks like a +900% same-day gainer to a price-change screen. It trivially passes any "+10% gainer" threshold and contaminates the cohort.

**Detection**: gainer cohort with `pct_change_today > 0.50` is almost always reverse splits or post-bankruptcy emergence — not real gainers.

**Fix**: Same as #1 (adjusted data resolves it). Belt-and-suspenders: cap same-day moves at +30-50% in the screen filter, drop forward returns > +500% (data artifacts).

## 3. Survivorship bias from forward-looking universes

**Trap**: `themes.yaml` is curated TODAY based on which names have done well. Any backtest restricted to it is forward-looking biased. Names that imploded in 2022 aren't there.

**Concrete cost in this repo**: themes.yaml + Path A backtest = +447% / 5y. Same logic on broader cleaned universe = +159%. **The 288pp gap was pure curation premium, not algorithmic edge.**

**Fix**: Use the cleaned broad universe (`massive_reference.allowed_ticker_set()`) for any backtest you want to claim is honest. Reserve `themes.yaml` for the *live* system where the human curation edge is intentional. NEVER bake themes.yaml into a forward-looking strategy claim.

## 4. Distressed-equity contamination at high return thresholds

**Trap**: Defining "winner" as `fwd_90d > +50%` catches mostly bankruptcy-bounce / penny-pump names — WOLF (Wolfspeed reorg), BBBY (Bed Bath & Beyond meme rally), DBD (Diebold), OPEN (Opendoor distress). The pattern that emerges is "high-vol name near 52w low with 0% volume" — real but uninvestable.

**Detection**: top-15 winner examples include names you've never heard of, or industries like "Gold Ores", "Aluminum", "Fabricated Plate Work / Boiler Shops", "Finance Services" (SPACs).

**Fix for cohort-pattern analysis**:
- Lower threshold to +25% / 90d (catches real compounders, drops lottery tickets)
- Add price floor: `min_price >= $10` at sample date
- Add liquidity floor: `min_dollar_volume_today >= $25M`
- Cap forward returns at +500% (data artifacts)

This is what `winner_pattern.py` does between v1 and v2 settings.

## 5. In-sample evaluation inflates results

**Trap**: Derive a rule from data X, test on data X. The rule "fits" because it was built from the data. Forward performance is much worse.

**Concrete example in this repo**: Path P entry rule was derived from full 5y data via `winner_pattern.py`. In-sample 5y replay produced **+722% / CAGR 53% / Sharpe 1.03**. Real forward expectation is probably 30-50% of headline.

**Fix — walk-forward validation**:
- Train on first 60% of window (first 3 years of 5y)
- Test on last 40% (last 2 years)
- If rule holds out of sample → real edge
- If it collapses → was overfit

**Quick test in this repo**:
```bash
# Re-derive rule on training subset
bash scripts/conviction/backtest/run_winner_pattern.sh \
    --start-date 2021-05-01 --end-date 2024-04-30

# Test rule on held-out 2 years
bash scripts/conviction/backtest/run.sh --days 730 --strategy pathP
```

## 6. Open positions at end-of-window inflate (or hide) results

**Trap**: Naive backtest doesn't force-close positions when window ends. Trade log silently drops the final trade. Activity stats (n_trades, avg holding, win rate) are biased. Equity curve marks-to-market correctly but the trade-by-trade narrative misses an entire trade.

**Fix**: Already shipped in `replay.py` and `run_replay_multi`. Force-close all open positions at last-day close with slippage applied. Exit reason logged as `exit_reason=END_OF_WINDOW`.

**Detection**: trade count is suspiciously low; "% time invested" doesn't match holding pattern; final equity comes from a phantom name not in trade log.

## 7. Exit code 1 from valid signals kills cron jobs

**Trap**: Original `monitor.py` returned exit 1 on EJECT. Wrappers using `set -e` killed the job on a valid sell signal — exactly when you most need the alert.

**Fix**: Already shipped. Both HOLD and EJECT return exit 0; state lives in `cache/monitor_state.json`. Schedulers should read state file, not exit code.

**Reserve non-zero exits for**: actual infrastructure failures (data fetch failed, computation errored). These should kill the job.

## 8. Universe drift: top-N today ≠ top-N historically

**Trap**: "Top 500 by dollar volume" computed across the full 5y window includes names that were active in 2025 but illiquid in 2022. They get factor scores in 2022 backtest dates but weren't realistically tradeable then.

**Fix**: For strict realism, add per-day liquidity filter (`dollar_vol_20d >= $25M` at the actual flag date, not just window-aggregate). For most backtests this is small marginal correctness vs more code complexity — document the limitation.

## 9. yfinance returns CURRENT data, not historical

**Trap**: `yf.Ticker(T).info["shortPercentOfFloat"]` returns TODAY's short interest, not historical. Using it for past-date features inflates results because past-date trades benefit from knowing today's positioning.

**Fix**: yfinance is fine for live system enrichment. NEVER use it for historical backtest feature snapshots. For historical short interest, you'd need FINRA archives (paid) or a paid data vendor.

## 10. NaN-as-float in pandas after merges

**Trap**: After `pd.merge`, missing values are `np.nan` (float), not `None`. Code like `if isinstance(x, str): x.upper()` works, but `if x is not None: x.upper()` doesn't — `nan is not None` is True, then `nan.upper()` fails.

**Fix**: Always `isinstance(x, str) and x` before string ops on merged columns.

## 11. Same-day decision leakage (THE HARD RULE)

**Trap**: A rule uses information from close[t] to set a position, and that position is then applied to return[t] (close[t-1] → close[t]). The strategy "decides" to be flat on the same day it observes the bad close. That's not a backtest, it's clairvoyance wearing a pandas DataFrame.

This bug killed a full DD-reduction research conclusion in May 2026. Vol-targeting went from "ROBUST in all 3 sub-windows, Calmar 1.07" → "rejected in WL, Calmar 0.53" once timing was corrected. A trailing-stop sweep that named TS_15 the winner inverted to TS_5/TS_6 after the fix. Three of the original "winners" did not survive. **The bug was load-bearing on the entire result set, not a small adjustment.**

**The hard rule**:

```
decision timestamp < execution timestamp < return measurement endpoint
```

For close-based daily systems:

```python
# CORRECT — the position decided at close[t] earns the return through close[t+1]
target_weight[t]    = signal_using_data_through_close_t
effective_weight[t] = target_weight.shift(1)              # what you actually held today
strategy_return[t]  = effective_weight[t] * asset_return[t]
```

```python
# WRONG — same-day decision leakage
target_weight[t]    = signal_using_data_through_close_t
strategy_return[t]  = target_weight[t] * asset_return[t]   # used close[t] to dodge return[t]
```

The two lines that look almost identical produce dramatically different results because the second one lets the strategy avoid the day it just observed.

**Most toxic for these rule types**:

| rule type | how it cheats |
|---|---|
| **Trailing stops** | sees the bad close, exits before that day's close-to-close loss is applied |
| **Vol-targeting** | sees today's vol shock, sizes down before today's shock hits the equity curve |
| **Gap / crash filters** | reacts to a move that already happened |
| **Drawdown-from-peak rules** | exits on the day that creates the drawdown |
| **Breadth / RS using close** | same issue if close data sets same-day exposure |

The C1-HYST signal itself (close[t] state lagged by `shift(1)` inside the variant) was clean. The overlay variants (TS_*, VT_*) bypassed that lag and pulled close[t] directly into weight[t], which the simulator then applied to return[t]. The mixed convention is what created the bug — not the variant logic per se.

**Detection signatures**:
- A trailing stop or vol-target rule shows much better numbers than the baseline at every window AND levels  → suspicious; rerun with strict shift(1) and compare
- "Worst single-day loss while invested" is dramatically lower than the underlying's worst-day move → strategy is dodging losses it should have eaten
- The Calmar improvement is large (e.g., +0.4 or more) AND smooth across the parameter range → too clean to be real
- Removing same-day-close from the decision causes the entire ranking to shuffle (not just shrink magnitudes) → original ranking was an artifact

**Required diagnostic before reporting any rule that touches same-day exposure**:

Re-run the backtest under both conventions and compare:

| Result of strict-vs-buggy comparison | Interpretation |
|---|---|
| Similar performance | More trustworthy |
| Mild degradation (~5-10% return diff, similar Calmar) | Normal |
| Huge collapse (Calmar drops by half or more) | Likely lookahead-dependent — old result invalid |
| Entire ranking flips (different variants win) | Original research finding is invalid; redo from clean simulator |

**Fix in code**:

Centralize the lag in the simulator, not inside each variant. Every variant emits `weight[t]` = "position decided at close[t] for tomorrow"; simulator does `effective = weights.shift(1)` exactly once before applying to returns. This eliminates the cross-variant inconsistency that creates the bug.

```python
def simulate_strict(weights, returns, slippage_bps):
    effective = weights.shift(1).fillna(0.0)   # the only timing line in the codebase
    daily = (effective * returns).sum(axis=1)
    cost = effective.diff().abs().sum(axis=1).fillna(0) * (slippage_bps / 1e4)
    return (1 + daily - cost).cumprod()
```

**The blunt rule**: any close-based rule that changes same-day exposure is guilty until proven innocent. Don't accept a backtest result that "looked great" without seeing the strict-timing variant alongside it.

Reference rerun: `scripts/conviction/backtest/dd_clean_timing_rerun.py` shows the corrected convention and the diagnostic comparison against the buggy version.

---

## Sanity-check checklist before quoting any backtest number

- [ ] **Strict timing**: every overlay variant emits `weight[t]` decided at close[t]; simulator does `shift(1)` exactly once before applying to return[t]. No close-based rule changes same-day exposure.
- [ ] **Strict-vs-buggy diagnostic** run for any rule using `close[t]` to size `weight[t]`. If results differ materially, the buggy version is invalid (see #11).
- [ ] Loaded SPLIT-ADJUSTED parquet (`load_parquet()` doesn't print warning)?
- [ ] Universe is the cleaned set (CS / non-pharma / optionable)?
- [ ] No `themes.yaml` in the forward-looking path (only in live system)?
- [ ] Forward-return cap applied if cohort study (fwd > +500% → NaN)?
- [ ] Same-day gainer cap if cohort study (pct > +50% → drop)?
- [ ] Held positions force-closed at end-of-window (`exit_reason=END_OF_WINDOW` rows present)?
- [ ] Top-5 trade winners are recognizable real names (NVDA / SMCI / META, not WOLF / BBBY)?
- [ ] If rule was derived from data → walk-forward done? Discount in-sample by 30-50%?
- [ ] Sharpe > 1.5? Suspicious — double-check no leakage.
- [ ] Win rate > 70%? Suspicious — double-check no leakage.
- [ ] CAGR > 50%? Could be real (Path P style) but apply 50% in-sample discount before claiming.

## Result-quality red flags

If you see any of these in a result, stop and audit before reporting:

- Top-3 trades responsible for >70% of total return → over-concentration, single-stock luck, not strategy edge
- Trade log has names you can't identify → contamination
- Avg winner +50% with avg loser <-3% → asymmetric capture is real but check for split artifacts
- Equity curve is monotonic up with no drawdown → too good, look for leakage or unrealistic execution
- Sharpe higher than QQQ's during the same period → possible but warrants double-check

## Don't do this

- Don't paste backtest numbers into the final memo without working through the checklist above.
- Don't claim "real edge" from one in-sample run. Walk-forward or it didn't happen.
- Don't dismiss a bad result as "just bad luck" without examining the trade log — sometimes the strategy correctly identified that nothing was working.
- Don't mix backtest universe with live system universe. The live system can use themes.yaml deliberately; the backtest must use the cleaned broad universe.
- **Don't write a rule that uses `close[t]` to set `weight[t]` and then apply that to `return[t]`.** That's same-day decision leakage (#11). It looks great in backtest and impossible to execute live. Centralize the lag in the simulator (`weights.shift(1)` exactly once); never let any variant function bypass it.
