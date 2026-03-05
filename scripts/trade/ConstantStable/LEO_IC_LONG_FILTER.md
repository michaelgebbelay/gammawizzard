# Leo IC_LONG Regime Filter

## Problem

IC_LONG trades (buying both put and call verticals) have been consistently losing in early 2026. Jan-Feb 2026: 16 trades, 25% WR, -$1,323. Every regime indicator (EM2d, VIX1D, VV ratio) looks identical to profitable years — the standard filters don't explain it.

## Root Cause

SPX daily moves have shrunk relative to Leo's anchor strike placement:

| Year | Avg SPX | Avg Daily Move | Avg Anchor Dist | Move/Dist | IC_LONG WR |
|------|---------|---------------|-----------------|-----------|------------|
| 2024 | 5,597 | 43.5 pts (0.78%) | 33.1 pts (0.60%) | 1.38 | 57% |
| 2025 | 6,334 | 47.4 pts (0.76%) | 33.6 pts (0.54%) | 1.26 | 53% |
| 2026 | 6,907 | 36.7 pts (0.53%) | 40.0 pts (0.58%) | 0.95 | 25% |

2026 is the first year where average daily move < average anchor distance. IC_LONG needs SPX to move past an anchor to win. When it doesn't, both debit verticals expire worthless.

## Calculations

### SPX Daily Move %

```
move_pct = |SPX_close - SPX_prev_close| / SPX_prev_close * 100
```

- Uses Schwab $SPX quote: `lastPrice` for current, `closePrice` for previous close
- Computed daily at 4:01 PM ET regardless of trade structure
- Stored in S3: `cadence/cs_spx_move_state.json`

### Trailing 5-Day Average Move %

```
trail5_move_pct = average of last 5 trading days' move_pct values
```

- Minimum 3 days required to produce a value
- Updated every trading day (not just IC_LONG days)
- The 5-day window balances responsiveness vs noise (tested 1-10 day windows)

### Anchor Distance %

```
dist_to_put  = SPX - Limit          (how far SPX must drop to reach put anchor)
dist_to_call = CLimit - SPX          (how far SPX must rise to reach call anchor)
min_dist     = min(dist_to_put, dist_to_call)
anchor_pct   = min_dist / SPX * 100
```

- `Limit` and `CLimit` come from Leo's GammaWizard signal
- `min_dist` = distance to the nearest anchor (best case for IC_LONG)
- Known at entry time — both values available before trade placement

### Filter Rule

```
IF trail5_move_pct < anchor_pct AND structure == IC_LONG:
    SKIP (do not trade)
ELSE:
    TRADE normally
```

## Backtest Results (2016-2026, 1DTE SPX verticals, real ATM straddle prices from ThetaData)

### IC_LONG by filter condition

| Condition | Trades | Total PnL | Win Rate | $/Trade |
|-----------|--------|-----------|----------|---------|
| Trail5 >= anchor (TRADE) | 280 | +$33,518 | 64.4% | +$120 |
| Trail5 < anchor (SKIP) | 169 | -$202 | 40.2% | -$1 |

### 2026 IC_LONG separation

| Condition | Trades | Wins | Losses | PnL |
|-----------|--------|------|--------|-----|
| Caught by filter (SKIP) | 12 | 1 | 11 | -$2,045 |
| Not caught (TRADE) | 8 | 4 | 4 | +$722 |

### Impact on other structures (trail5 < anchor zone)

| Structure | Trades | PnL | Win Rate | $/Trade | Action |
|-----------|--------|-----|----------|---------|--------|
| IC_LONG | 169 | -$202 | 40.2% | -$1 | SKIP |
| IC_SHORT | 47 | +$7,124 | 89.4% | +$152 | Trade normally |
| RR_LONG | 248 | +$7,490 | 43.5% | +$30 | Trade normally |
| RR_SHORT | 38 | +$668 | 52.6% | +$18 | Trade normally |

IC_SHORT thrives in the exact regime that kills IC_LONG (small moves = straddle seller wins). RR structures remain profitable in both regimes.

### Flip IC_LONG to IC_SHORT?

Tested but rejected. The flip adds only +$202 over skip (+$1/trade) across full history, while creating $5-7K downside in years like 2021/2022 where low-move regimes suddenly produce big moves. Skip is the safer play.

## What We Tested That Didn't Work

| Filter | Problem |
|--------|---------|
| EM2d fixed thresholds (0.5-1.3) | All IC_LONG buckets profitable historically |
| EM2d relative percentiles | No percentile consistently separates winners from losers |
| VIX1D level thresholds | 2026 VIX1D (15.6 median) identical to profitable years |
| VV ratio (VIX1D/VIX) | 2026 VV (0.86) identical to profitable years |
| Vol overpricing (straddle > actual move) | IC_LONG historically profitable even under heavy overpricing |
| Multi-factor combos (EM2d + VIX1D + VV) | Only 19 trades caught across 10 years — useless |
| Consecutive loss streaks | Next trade after 2+ losses wins 56% — mean reversion holds |

## EM2d (used by butterfly system, tested on Leo)

```
EM Ratio    = |actual SPX daily move in points| / (ATM 1DTE call mid * 2)
EM2d        = average of last 2 trading days' EM ratios
```

- Denominator is the 1DTE ATM straddle price (ATM call mid * 2)
- ATM straddle prices extracted from ThetaData cache (sim/cache/), NOT from VIX1D
- VIX1D proxy for straddle has only 57% bucket agreement with real straddle — not interchangeable
- EM2d works well for butterfly regime classification but does NOT predict IC_LONG outcomes

## Strategy C: Profit IC_LONG Substitution

When the trail5 filter fires (Stable IC_LONG would be skipped), the system checks Leo Profit's signal. If Profit is also calling IC_LONG, we use Profit's anchor strikes instead of skipping entirely.

### Why Profit IC_LONG works where Stable IC_LONG fails

Stable IC_LONG and Profit IC_LONG are mirror images in low-move regimes:

| Metric | Stable IC_LONG | Profit IC_LONG |
|--------|---------------|----------------|
| EM2d < 0.80 (calm) | 47% WR, -$22/trade | 74% WR, +$147/trade |
| EM2d > 1.30 (volatile) | 60% WR, +$176/trade | 44% WR, -$60/trade |

Profit uses a VIX-based neural net that places anchors differently — closer to SPX in calm markets, so the daily move is more likely to reach them.

### Backtest: Strategy comparison (2016-2026, live = Jul 2024+)

| Strategy | Total PnL | Live PnL | Live $/Trade |
|----------|-----------|----------|--------------|
| A: Stable as-is | $126,179 | $14,322 | $27 |
| B: Stable + skip filter | $126,381 | $16,211 | $31 |
| C: Hybrid (Profit sub on skip) | $142,586 | $20,487 | $39 |
| D: Full IC_LONG replacement | $144,644 | $22,998 | $44 |

Strategy C adds +$6,165 in the live period (+43% vs Strategy B) and wins in 10 of 11 years.

### How Profit side is determined

```
Cat1 = Profit neural net output for IC_LONG confidence
Cat2 = Profit neural net output for IC_SHORT confidence

IF Cat2 < Cat1:  → IC_LONG  (debit)
IF Cat2 >= Cat1: → IC_SHORT (credit)
```

### Substitution logic

```
IF trail5_move_pct < anchor_pct AND structure == IC_LONG:
    Fetch Leo Profit signal from /rapi/GetLeoProfit
    IF Profit is also IC_LONG (Cat2 < Cat1):
        USE Profit's Limit/CLimit as anchor strikes
        TRADE IC_LONG with Profit anchors
    ELSE:
        SKIP (Profit is calling IC_SHORT — no IC_LONG substitute)
```

### Fail-safe behavior for Strategy C

- If Profit endpoint fails → fall back to skip (Strategy B)
- If Profit is calling IC_SHORT → skip (no substitute available)
- Profit signal is fetched once by `ic_long_filter.py` and cached in S3 decision

## Production Implementation

### Schedule

- **4:01 PM ET**: `ic_long_filter.py` runs — fetches SPX quote + GW signal, computes filter, writes decision to S3
- **4:13 PM ET**: All 3 orchestrators (Schwab, TT IRA, TT Individual) read the S3 decision

### S3 Files

- `cadence/cs_spx_move_state.json` — trailing move history (last 20 days)
- `cadence/cs_ic_long_decision.json` — today's go/no-go decision with full diagnostics

### Config (env vars)

| Variable | Default | Description |
|----------|---------|-------------|
| `CS_IC_LONG_FILTER` | `1` | Enable/disable the filter (set `0` to disable) |
| `CS_IC_LONG_TRAIL_DAYS` | `5` | Trailing window for avg move % |
| `CS_MOVE_STATE_S3_BUCKET` | `gamma-sim-cache` | S3 bucket for state files |
| `CS_IC_DECISION_S3_KEY` | `cadence/cs_ic_long_decision.json` | S3 key for decision |
| `GW_PROFIT_ENDPOINT` | `rapi/GetLeoProfit` | GW endpoint for Profit signal (Strategy C) |

### Fail-safe behavior

- If SPX quote unavailable → ALLOW (trade normally)
- If insufficient move history (<3 days) → ALLOW
- If S3 decision file missing or stale (wrong date) → ALLOW
- Filter only affects IC_LONG — all other structures always trade
- If Profit endpoint fails → SKIP (fall back to Strategy B)

## Data Sources

- **Leo trades**: `leo_stable_March_02_26.csv` (1,835 trades, 2016-01 to 2026-02)
- **Enriched with straddle data**: `leo_stable_March_02_26_with_em2d.csv` (1,755 rows with real ATM straddle prices)
- **ATM straddle prices**: ThetaData cache at `sim/cache/` (2,514 dates, 1DTE contracts)
- **Leo signal reference**: `Leo_data_reference.md`

## Files Modified

- `scripts/trade/ConstantStable/ic_long_filter.py` — pre-check script (new)
- `scripts/trade/ConstantStable/orchestrator.py` — Schwab orchestrator (v2.4.0)
- `TT/Script/ConstantStable/orchestrator.py` — TT orchestrator (v2.4.0)
- `lambda/handler.py` — new `ic-long-filter` account, Schwab re-enabled
- `lambda/template.yaml` — new 4:01 PM schedule, Schwab schedule enabled
- `scripts/trade/ConstantStable/LEO_IC_LONG_FILTER.md` — this document
