# DualSide Strategy

SPX dual-side vertical spread strategy with vol-regime filters.

## Changelog

### v1.1.0 (2026-03-19) — Regime-Follow + 5-Wide

**Changes:**
1. **Width reduced from $10 to $5** on both sides
2. **50-delta (call side) now follows the regime switch** instead of always bullish

**Why — Width reduction ($10 → $5):**
On bearish days, the strategy places a bear_put_debit at 25-delta and a bull_put_credit at 50-delta.
These positions are only 15-25 SPX points apart and oppose each other. With $10-wide spreads, the
combined max loss is ~$820 (after credits) for a max win of ~$180. Cutting to $5 halves the per-trade
capital at risk from ~$900 to ~$400, making the risk/reward structure less lopsided.

**Why — Regime-follow on 50-delta:**
The 50-delta trade was always bullish (bull_put_credit), even on days the regime switch said bearish.
Analysis of 864 backtest trades (2023-06 through 2026-02-20):

| 50-delta scenario       | Trades | Total P&L | Avg/trade |
|-------------------------|--------|-----------|-----------|
| On BULL regime days     | 260    | $34,047   | +$131     |
| On BEAR regime days     | 93     | -$1,755   | -$19      |

The 50-delta trade averages +$131 on bull days and -$19 on bear days. On bear days it directly
contradicts the 25-delta bearish position, creating opposing bets a few strikes apart.

March 2026 CloudWatch data confirmed all 7 trading days were bearish (iv_minus_vix ranging from
-0.70 to -4.31, well below the -0.05 threshold). The 50-delta was bullish every day, fighting the
25-delta bear_put_debit.

Flipping the 50-delta to follow the regime means both sides point the same direction: both bearish
in selloffs (profiting from downside), both bullish in calm markets (collecting premium). The
estimated improvement from flipping vs always-bullish is ~$3,500 over the backtest period.

**What did NOT work (tested and rejected):**
- **L1 trend filter** (VIX/RV10 >= 1.95 AND RV5/RV20 <= 1.10): identifies range-bound markets,
  which is BPC's best environment. Applying it cost $4,000+ in skipped profitable trades.
- **MA5 trend filter** (skip BPC when 5-day return < -1%): saved $1,400 in bad months but
  cost $2,100 in good months. Net negative.
- **Capping the 50-delta**: BPC cap of 3 only ever cuts 25-delta trades. The 50-delta at
  5DTE expires before stacking past the cap, making it ineffective.

### v1.0.0 (initial)
- 10-wide spreads, 50-delta always bullish, BPC cap of 3.

---

## Strategy Rules

### Put Side — 25-delta (6DTE, 5-wide)
- Strike placement: `spot - 0.75 * EM` (expected move = ATM straddle mid)
- **Direction switch** based on 6DTE chain signals:
  - If `iv_minus_vix >= -0.0502` AND `rr25 <= -0.9976`: **bull put credit** (sell higher put, buy 5 lower)
  - Else: **bear put debit** (buy higher put, sell 5 lower)

### Call Side — 50-delta (5DTE, 5-wide)
- Strike at 50-delta (ATM put from 5DTE chain)
- **Follows same regime switch** as put side:
  - Bullish: **bull put credit** (sell higher put, buy 5 lower)
  - Bearish: **bear put debit** (buy higher put, sell 5 lower)

### Signal Computation
- `iv_minus_vix` = ATM IV (vol points) - VIX. Both in percentage (e.g., 17.5 - 15.2 = +2.3)
- `rr25` = (25-delta call IV - 25-delta put IV) * 100. In vol points.
- `EM` = ATM call mid + ATM put mid (straddle mid in SPX points)
- ATM IV = average of ATM call and ATM put implied vol from 6DTE chain

## Filters

### 1. VIX1D Veto (skip ALL trades)
Skip the entire setup when `10.0 <= VIX1D < 11.5`.
If VIX1D is unavailable, do NOT veto.

### 2. VIX < 10 Call Skip
Skip the call side when VIX < 10.

### 3. RV5/RV20 Band Skip (0.70 - 0.85)
When `0.70 <= RV5/RV20 < 0.85`, skip **bullish** legs:
- Skip bull_put_credit (on either side)
- **Always keep bear_put_debit** (profits from the selloffs this filter anticipates)

### RV Computation
- RV5 = annualized 5-day realized vol: `std(log_returns, ddof=1) * sqrt(252) * 100`
- RV20 = same for 20 days
- Computed from SPX daily closes via Schwab price history API

## Execution

### Manual (local)
```bash
# Load Schwab credentials from SSM
export SCHWAB_APP_KEY=$(aws ssm get-parameter --name /gamma/schwab/app_key --with-decryption --query 'Parameter.Value' --output text --region us-east-1)
export SCHWAB_APP_SECRET=$(aws ssm get-parameter --name /gamma/schwab/app_secret --with-decryption --query 'Parameter.Value' --output text --region us-east-1)
export SCHWAB_TOKEN_PATH=Token/schwab_token.json

# Dry run (no orders placed)
DS_DRY_RUN=true python scripts/trade/DualSide/orchestrator.py

# Live
python scripts/trade/DualSide/orchestrator.py
```

### Order Placement
Uses the same ladder placer as ConstantStable (`place.py`):
- CREDIT spreads: start at mid, step down by $0.05 each rung
- DEBIT spreads: start at mid, step up by $0.05 each rung
- 3 rungs by default, 15s wait between rungs
- Configurable via `VERT_STEP_WAIT`, `VERT_POLL_SECS`, `VERT_MAX_LADDER`

### Guards
- **TOPUP**: Only places `target - open_spreads` contracts (default ON)
- **NO_CLOSE**: Skips if placing would net/close existing positions (default ON)
- **BPC CAP**: Skip new bull_put_credit if >= 3 already open (counts both $5 and legacy $10 widths)

## Sizing
Currently fixed at 1 contract per side. Configurable via code (`qty` in `main()`).

## Backtest Results (v1.0.0, 10-wide, 2023-06 to 2026-03-13)

> These results are from v1.0.0 (10-wide, 50-delta always bullish).
> v1.1.0 has not been backtested yet.

### Cap3 (production: bull_put_credit capped at 3 open)

| Metric | Value |
|--------|-------|
| Settled Trades | 790 |
| Total P&L | $63,749 |
| Avg/trade | $81 |
| Sharpe | 2.96 |
| Max DD | -$9,409 |
| Win Rate | 64% |
| Profit Factor | 1.51 |
| Cadence | 5.4 trades/week |
| Peak Capital at Risk | $6,066 |

### By Year (cap3)
| Year | Trades | Put | Call | Total | WR |
|------|--------|-----|------|-------|----|
| 2023 | 173 | $13,669 | $1,916 | $15,585 | 61% |
| 2024 | 281 | $4,518 | $18,774 | $23,291 | 65% |
| 2025 | 288 | $4,363 | $15,040 | $19,404 | 65% |
| 2026 | 48 | $6,353 | -$884 | $5,468 | 67% |

### By Structure (cap3, all years)
| Structure | Trades | Total P&L | Avg/trade | WR |
|-----------|--------|-----------|-----------|-----|
| bull_put_credit | 244 | $15,905 | $65 | 86% |
| bear_put_debit | 162 | $12,997 | $80 | 31% |
| bull_call_debit | 384 | $34,847 | $91 | 64% |

## Files

| File | Purpose |
|------|---------|
| `orchestrator.py` | Main strategy: signal computation, filters, vertical construction |
| `place.py` | Order placement: NBBO ladder, Schwab API (copied from ConstantStable) |
| `README.md` | This file |

## Data Sources
- **SPX spot**: Schwab option chain underlying price
- **VIX**: Schwab `$VIX` quote
- **VIX1D**: Schwab `$VIX1D` quote
- **Option chains**: Schwab `get_option_chain("$SPX")` for 5DTE and 6DTE
- **RV5/RV20**: Computed from Schwab `get_price_history_every_day("$SPX")`
