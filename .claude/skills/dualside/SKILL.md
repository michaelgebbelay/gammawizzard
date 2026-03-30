---
name: dualside
description: DualSide production strategy — diagnose, tune, backtest, and analyze the live SPX dual-side vertical spread system. Use when working on DualSide signal logic, regime switching, filters, execution, or performance analysis.
---

# DualSide Strategy Skill

You are the expert on the DualSide SPX vertical spread strategy — a live production system trading daily at 4:05 PM ET on Schwab via AWS Lambda.

## Strategy Identity

- **Version**: v1.4.0 (2026-03-30)
- **Lambda account**: `dualside` in `gamma-trading` stack
- **Schedule**: `gamma-ds-schwab`, `cron(5 16 ? * MON-FRI *)` (4:05 PM ET)
- **Orchestrator**: `scripts/trade/DualSide/orchestrator.py`
- **Placer**: `scripts/trade/DualSide/place.py` (same ladder as ConstantStable)
- **Tracking**: `scripts/data/ds_tracking_to_gsheet.py` → Google Sheets `DS_Tracking` tab
- **Live DB**: `sim/data/dualside_live.db`

## Production Rules (v1.3.1)

### Put Side — 25-delta (6DTE, 10-wide)
- Strike: `spot - 0.75 * EM` (EM = ATM straddle mid)
- **Regime switch** on 6DTE chain signals:
  - `iv_minus_vix >= -0.0502` AND `rr25 <= -0.9976` → **bull_put_credit** (sell higher put, buy 10 lower)
  - Else → **bear_put_debit** (buy higher put, sell 10 lower)

### Call Side — 50-delta (5DTE, 10-wide)
- Strike: 50-delta put from 5DTE chain
- **Bull regime only**: places bull_put_credit (sell 50d put, buy 10 lower)
- **Bear regime**: SKIP entire call side (avoids opposing bets that create a losing long IC)

### Filters (applied before any trade)
1. **VIX1D veto**: Skip ALL when `10.0 <= VIX1D < 11.5`
2. **VIX < 10**: Skip call side only
3. **RV5/RV20 band** `[0.70, 0.85)`: Skip bullish legs (bull_put_credit) on either side. Bear_put_debit always trades.
4. **Bear regime**: Skip call side (Filter 4, call-specific)

### Signal Computation
- `iv_minus_vix` = ATM IV (vol points from 6DTE chain) - VIX
- `rr25` = (25-delta call IV - 25-delta put IV) * 100, in vol points
- `EM` = ATM call mid + ATM put mid (straddle mid in SPX points)
- RV5, RV20 = annualized realized vol from Schwab SPX daily closes

### Position Scaling (v1.4.0) — VIX1D Gentle Curve
- `units = floor(equity / DS_UNIT_DOLLARS)` where `DS_UNIT_DOLLARS=15000`
- **Credit trades** (bull_put_credit, call helper): `qty = units * credit_mult`
- **Bear debit trades** (bear_put_debit): `qty = units * bear_mult`
- VIX1D breaks: `8.9, 11.1, 13.1, 15.8, 19.2, 25.3`
- Credit mults: `1, 1, 1, 1, 2, 3, 4` — no scaling below VIX1D 15.8
- Bear mults: `1, 1, 1, 1, 1, 2, 2` — flat until VIX1D 19.2, max 2x
- Falls back to VIX if VIX1D unavailable
- **Backtest**: Sharpe 2.83 (vs 2.44 flat), DD -20% (vs -18% flat), P&L +$99k (vs +$60k flat)
- Env overrides: `DS_VIX1D_BREAKS`, `DS_CREDIT_MULTS`, `DS_BEAR_MULTS`, `DS_UNIT_DOLLARS`

### Execution
- 3-step ladder: CREDIT starts at mid, steps down $0.05; DEBIT starts at mid, steps up $0.05
- 15s wait between rungs
- TOPUP guard: only places `target - open_spreads` contracts
- NO_CLOSE guard: skips if placement would net/close existing positions

## Backtest Performance (v1.3.1, 10-wide, Jun 2023 – Mar 2026)

Source: `scripts/trade/DualSide/backtest/dualside_v2_backtest.csv` variant `50d_skip_bear` + `put_regime`

| Year | Combos | Put P&L | Call P&L | Total | $/combo |
|------|--------|---------|----------|-------|---------|
| 2023 | 138 | +$19,694 | +$7,227 | +$26,921 | +$195 |
| 2024 | 234 | +$4,893 | +$11,627 | +$16,520 | +$71 |
| 2025 | 209 | +$3,762 | +$11,526 | +$15,288 | +$73 |
| 2026 | 38 | +$4,973 | -$2,786 | +$2,187 | +$58 |
| **Total** | **619** | **+$33,322** | **+$27,594** | **+$60,916** | **+$98** |

Zero negative years. Put side is the consistent earner; call side adds alpha on bull days.

### Trade Type Breakdown (from evaluator)
- **IC_SHORT** (51%): +$43,806 — the core engine, 66% rWR
- **RR_LP** (34%): +$14,067 — bear trades, low WR (33%) but high avg win ($675)
- **RR_LC** (14%): +$3,043 — small contribution, 84% rWR

### Cost Model (from vertical_dualside_10w_regime_switch_documentation.md)
- Commission: $0.97 per vertical
- Slippage: 0.20 pts per spread (mid +/- 0.20)
- Total: ~$20.97 per vertical, ~$33-42 per combo depending on 1 or 2 verticals
- **Net $/combo after costs: ~$65**

### VIX1D Profile
Profitable at every VIX1D level. Sweet spots:
- **VIX1D 8-10**: +$1.28/trade
- **VIX1D 12-14**: +$1.38/trade
- **VIX1D 14-16**: +$1.65/trade
- **VIX1D 18-20**: +$1.94/trade
- **VIX1D 20-24**: +$1.81/trade
- **VIX1D 30+**: +$2.26/trade
- Dead zone: VIX1D 10-12 (-$0.06/trade)

### Market Direction
- **UP days** (57%): +$0.06/trade — near break-even
- **DOWN days** (43%): +$0.53/trade — where the strategy makes its money
- The bear_put_debit on down days drives the P&L

### Known Weak Spots (from evaluator analysis)
- **RV5/RV20 = 0.70-0.85**: Already filtered in production (skip bull legs). Confirmed as a disaster zone.
- **VIX/RV10 > 2.5**: 48% rWR — extreme fear premium hurts the bullish bias
- **VIX1D 10-12**: Near-zero edge, partially covered by VIX1D veto [10.0, 11.5)

## v1.0 vs v1.3.1 Comparison

| Metric | V1.0 (call always bull) | V1.3.1 (call skip bear) |
|--------|------------------------|------------------------|
| Total P&L | $59,192 | **$60,916** (+$1,724) |
| $/combo | $95 | **$98** |
| Execution cost | Higher (2 verticals every day) | Lower (skip call on bear) |
| Net $/combo | ~$59 | **~$65** |

V1.3.1 wins: more gross, lower costs, no opposing bets on bear days.

## Version History

| Version | Date | Change |
|---------|------|--------|
| v1.0.0 | Initial | 10-wide, 50d always bull, BPC cap of 3 |
| v1.1.0 | 2026-03-19 | Regime-follow on 50d (tested 5-wide, reverted to 10-wide) |
| v1.4.0 | 2026-03-30 | VIX1D-based position scaling (Gentle curve: credit 1/1/1/1/2/3/4, bear 1/1/1/1/1/2/2) |
| v1.3.1 | 2026-03-20 | Removed BPC cap, finalized skip-bear on call side |

## Key Files

| File | Purpose |
|------|---------|
| `scripts/trade/DualSide/orchestrator.py` | Production orchestrator (v1.3.1) |
| `scripts/trade/DualSide/place.py` | Schwab order placer (ladder) |
| `scripts/trade/DualSide/README.md` | Strategy documentation |
| `scripts/trade/DualSide/backtest/dualside_v2_backtest.csv` | Multi-variant backtest (v1.0 vs skip_bear vs regime_follow) |
| `scripts/trade/DualSide/backtest/call_side_variants.py` | Call-side variant analysis |
| `scripts/trade/DualSide/backtest/width_comparison.py` | 5-wide vs 10-wide comparison |
| `sim/data/dualside_v11_backtest_corrected.csv` | v1.0 vs v1.1 (5-wide) side-by-side (historical, not production) |
| `sim/data/vertical_final_recommended_trade_ledger.csv` | 864-trade deep signal study (v1.0 era) |
| `sim/data/vertical_dualside_10w_regime_switch_documentation.md` | Full research progression |
| `sim/data/dualside_live.db` | Live trade database |
| `scripts/data/ds_tracking_to_gsheet.py` | Google Sheets sync |
| `lambda/handler.py` (line 303) | Lambda account config |
| `lambda/template.yaml` (line 171) | EventBridge schedule |

## When Answering Questions

- **"How is DualSide doing?"** → Check live DB (`sim/data/dualside_live.db`), Google Sheets DS_Tracking tab, or `logs/dualside_trades.csv`
- **"Should we change X filter?"** → Reference the backtest data, run the evaluator script against filtered variants
- **"What's the regime today?"** → The regime depends on real-time `iv_minus_vix` and `rr25` from the 6DTE chain. Check CloudWatch logs for `gamma-ds-schwab`.
- **"Compare to ConstantStable/LeoProfit"** → DualSide nets ~$65/combo after costs. CS nets ~$56/trade, LP ~$39/trade (same cost basis). DualSide is the best vertical strategy by net $/trade.
- **"Why no IC_SHORT in the evaluator output?"** → Because both sides are always the same direction (both bull or both bear). This creates IC_SHORT (both sell) or RR (mixed buy/sell) combos, never IC_LONG in the pure evaluator classification.
