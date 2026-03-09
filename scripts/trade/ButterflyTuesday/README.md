# Butterfly

Daily hybrid butterfly strategy on SPX via Schwab.

## Signal rules (VIX1D at 4:01 PM ET)

| VIX1D         | Signal | Structure                          | DTE | Wings        |
|---------------|--------|------------------------------------|-----|--------------|
| <= 10         | SKIP   | no trade                           | -   | -            |
| 10 < x <= 20  | BUY    | long call butterfly (DEBIT)        | 4   | D20P         |
| > 20          | SELL   | short call butterfly (CREDIT)      | 3   | D35P         |

## Vol filter (BUY only)

Skip the BUY when **either** fires:
- Straddle efficiency 5-day avg > 0.9
- Straddle efficiency today > 1.0

Straddle efficiency = |SPX move today| / ATM straddle mid.
History stored in S3 (`cadence/bf_straddle_eff_history.json`), last 10 entries.

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
- State persisted in S3 (`cadence/bf_daily_state.json`) with local file fallback

## Post-trade steps

- `bf_trades_to_gsheet.py` - logs trade to Google Sheets (BF_Trades tab)
- `bf_eod_tracking.py` - tracks EOD butterfly pricing

## Files

- `orchestrator.py` - schedule gate, live data fetch, trade selection, state handling
- `place.py` - Schwab CUSTOM butterfly order placer (1-2-1 call legs)
- `strategy.py` - VIX1D signal, vol filter, strike selection, chain parsing

## Env vars

| Variable                | Default                   | Description                        |
|-------------------------|---------------------------|------------------------------------|
| `BF_DRY_RUN`           | `1` (Lambda sets `0`)    | Dry-run mode                       |
| `BF_QTY`               | `1`                       | Number of butterfly contracts       |
| `BF_STRIKE_COUNT`      | `120`                     | Schwab chain strike count           |
| `BF_MAX_LEG_SPREAD`    | `30`                      | Max bid-ask spread per leg ($)      |
| `BF_BUY_TARGET_DTE`    | `4`                       | DTE for BUY signal                  |
| `BF_SELL_TARGET_DTE`   | `3`                       | DTE for SELL signal                 |
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
