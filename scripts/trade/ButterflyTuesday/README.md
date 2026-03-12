# Butterfly

Daily hybrid butterfly strategy on SPX via Schwab.

## Base signal (VIX1D at 4:01 PM ET)

| VIX1D         | Signal | Structure                          | DTE | Wings        |
|---------------|--------|------------------------------------|-----|--------------|
| <= 10         | SKIP   | no trade                           | -   | -            |
| 10 < x <= 20  | BUY    | long call butterfly (DEBIT)        | 4   | D20P         |
| > 20          | SELL   | short call butterfly (CREDIT)      | 3   | D35P         |

## Current production candidate

The live strategy keeps the base VIX1D split above, then applies side-specific overlays.

### BUY overlay

Require all of:
- `10 < VIX1D <= 20`
- `4DTE`, `D20P` wings
- `rv_5d_ann_pct <= 12`
- `straddle_eff_5d_avg <= 0.9`
- `dVIX < 0`
- `dVIX1D < 0`

Notes:
- `BOTH_DOWN` is strict. Flat does not qualify.
- If there is no prior-day VIX/VIX1D reading, skip the BUY.
- Both RV5 and SE5d must be present. If either is `None` (data missing), the BUY is skipped.

### SELL overlay

Require all of:
- `VIX1D > 20`
- `3DTE`, `D35P` wings
- `straddle_eff_5d_avg <= 1.00`
- `ER3 >= 0.60` (3-day price efficiency ratio — filters choppy/mean-reverting markets)

Notes:
- Both SE5d and ER3 must be present. If either is `None` (data missing), the sell is skipped.
- `ER3 = |Close[t] - Close[t-3]| / sum(|daily moves|)` — low ER3 = choppy, high ER3 = directional.
- Sell butterflies need directional displacement through expiry, not chop that mean-reverts.
- `BF_SELL_SE_THRESHOLD` can override the default `1.00`.
- `BF_SELL_ER3_THRESHOLD` can override the default `0.60`.

### Skip

Skip when the base VIX1D signal is not active or the side-specific overlay fails.

## Filter inputs

- `VIX1D` comes from GammaWizard at evaluation time.
- `RV5` is 5-day annualized realized vol from Schwab SPX daily candles.
- `ER3` is 3-day price efficiency ratio from the same Schwab daily candles (single API call for both).
- `straddle_eff_5d_avg` is the trailing 5-day average of straddle efficiency.
- Straddle efficiency = `|SPX move| / ATM straddle mid`.
- VIX/VIX1D regime history is stored in S3 at `cadence/bf_vol_regime_history.json`.
- Straddle efficiency history is stored in S3 at `cadence/bf_straddle_eff_history.json`.

## Backtest snapshot

Research now uses multiple path models for the same trades:
- `trade_date_stamped`: old optimistic method, final trade P&L booked on entry date
- `expiry_realized`: final trade P&L booked on expiration date
- `mtm_mid`: open trades repriced daily at package mid
- `mtm_liq`: open trades repriced daily at liquidation value

Use `mtm_liq` as the safer live-risk reference. The old `trade_date_stamped` numbers understated path risk when short butterflies overlapped.

Current hybrid comparison (pre-ER3 backtest, data through 2026-03-02):

**Note:** This table was computed before the ER3 sell filter was added. The live strategy now also requires `ER3 >= 0.60` on sells, which would have blocked the March 2026 sell losses (~-$9k) that are not reflected in these numbers.

| Variant | Trades | PF | Total P&L | 2026 P&L | Old Min to Survive | MTM Mid Min to Survive | MTM Liq Min to Survive | MTM Liq Max DD |
|---------|-------:|---:|----------:|---------:|-------------------:|-----------------------:|-----------------------:|---------------:|
| Baseline `4BUY + 3SELL` | 283 | 1.830 | $152,407.5 | $37,575.0 | $21,003.5 | $21,073.5 | $21,653.5 | -$25,853.0 |
| Candidate `BOTH_DOWN buy + 3SELL SE<=1.00` | 104 | 2.300 | $78,019.5 | $33,039.0 | $6,135.0 | $6,985.0 | $8,280.0 | -$8,849.0 |
| Control `BOTH_DOWN buy + 2SELL SE<=1.00` | 103 | 2.026 | $66,600.0 | $33,019.0 | $7,097.0 | $7,947.0 | $9,242.0 | -$9,811.0 |

Interpretation:
- Both baselines still fail a `$20k` start once realistic path accounting is used.
- The candidate still survives a `$20k` start under `mtm_liq`, but the real buffer is closer to `$8.3k` than the older `$6.1k` figure.
- `3DTE` sell plus the `SE<=1.00` veto still beats the analogous `2DTE` control on both PF and conservative path risk.
- The regime filter on buys does most of the drawdown cleanup, but open-position MTM is still materially worse than settlement-stamped research.

Supporting files:
- `sim/data/butterfly_hybrid_mtm_summary.csv`
- `sim/data/butterfly_hybrid_mtm_daily.csv`

## Strike selection

1. Find ATM strike for the target expiry
2. Find the OTM put nearest to the target delta (D20P or D35P) below ATM
3. Width = ATM - lower strike
4. Upper strike = ATM + width (symmetric butterfly)
5. Reject if any leg's bid-ask spread > `BF_MAX_LEG_SPREAD` (default 30)

## Order placement (ladder)

3-step ladder starting at mid, stepping by $0.05:
- DEBIT (BUY): mid, mid+0.05, mid+0.10
- CREDIT (SELL): mid, mid-0.05, mid-0.10

Each step waits up to `BF_STEP_WAIT` seconds (default 10) for a fill.
Unfilled orders are cancelled before the next step. Tick size = $0.05.

## Cadence

- Runs every weekday at 4:01 PM ET via Lambda EventBridge (`gamma-bf-daily`)
- Skips weekends and pre-4:01 PM runs
- Skips if already evaluated today (dedup via `last_evaluated_date` in state)
- Skips if a position already exists for the target expiry (`open_expiries` check)
- Skips if any butterfly strike overlaps an existing Schwab position on the same expiry (prevents accidental close of DualSide/CS legs)
- State persisted in S3 (`cadence/bf_daily_state.json`) with local file fallback
- Buy-side regime history persisted in S3 (`cadence/bf_vol_regime_history.json`)
- Straddle efficiency history persisted in S3 (`cadence/bf_straddle_eff_history.json`)

## Post-trade steps

- `bf_trades_to_gsheet.py` - logs trade to Google Sheets (BF_Trades tab)
- `bf_eod_tracking.py` - tracks EOD butterfly pricing

## Files

- `orchestrator.py` - schedule gate, live data fetch, trade selection, state handling
- `place.py` - Schwab CUSTOM butterfly order placer (1-2-1 call legs)
- `strategy.py` - VIX1D signal, BOTH_DOWN buy regime filter, RV5/SE buy veto, SE+ER3 sell veto, strike selection, chain parsing

## Env vars

| Variable                | Default                   | Description                        |
|-------------------------|---------------------------|------------------------------------|
| `BF_DRY_RUN`           | `1` (Lambda sets `0`)    | Dry-run mode                       |
| `BF_QTY`               | `1`                       | Number of butterfly contracts       |
| `BF_STRIKE_COUNT`      | `120`                     | Schwab chain strike count           |
| `BF_MAX_LEG_SPREAD`    | `30`                      | Max bid-ask spread per leg ($)      |
| `BF_BUY_TARGET_DTE`    | `4`                       | DTE for BUY signal                  |
| `BF_SELL_TARGET_DTE`   | `3`                       | DTE for SELL signal                 |
| `BF_SELL_SE_THRESHOLD` | `1.00`                    | Max allowed `straddle_eff_5d_avg` for SELL |
| `BF_SELL_ER3_THRESHOLD`| `0.60`                    | Min allowed ER3 for SELL (chop filter)     |
| `BF_STEP_WAIT`         | `10`                      | Seconds to wait per ladder step     |
| `BF_STATE_PATH`        | `state.json`              | Local state file path               |
| `BF_LOG_PATH`          | `logs/butterfly_tuesday_trades.csv` | Trade log CSV path       |
| `BF_TRADE_DATE_OVERRIDE` | -                       | Force a specific trade date         |
| `BF_NOW_OVERRIDE`      | -                         | Force a specific timestamp          |

## Known limitations

- DTE uses weekday counting (`add_business_days`), not exchange calendar. Holiday weeks may pick the wrong expiry.

## Examples

Dry run:
```bash
python scripts/trade/ButterflyTuesday/orchestrator.py
```

Live:
```bash
BF_DRY_RUN=0 BF_QTY=1 python scripts/trade/ButterflyTuesday/orchestrator.py
```
