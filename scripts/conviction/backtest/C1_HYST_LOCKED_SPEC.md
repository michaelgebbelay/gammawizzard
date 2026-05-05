# C1-HYST — Locked Strategy Specification

**Also known as: TQQQ Trend, 3xQQQ, QQQ 3x, HYST.** All names refer to the same
strategy. User-facing name is "TQQQ Trend" (skill: `tqqq-trend`); internal/git
codename is "C1-HYST". Live placement script: `scripts/trade/TqqqTrend/place.py`.

**Status:** primary candidate, locked 2026-05-05. **Live on Schwab from 2026-05-05
at $5k initial position.**
**Reference implementation:** `c1_locked_baseline_backtest.py` (variant `C1-HYST`).
**Attribution evidence:** GHA run 25358962345 (`entry_exit_attribution.yml`).
**Non-MA confirmation:** GHA run 25358646580 (`non_ma_regime_backtest.yml`).

This document is the canonical contract. Code and live-trading logic must
match it exactly. Changes require a new attribution test, not a code edit.

---

## Signal definition

Signal asset: **QQQ** (adjusted close, total-return basis).
Sleeves: **TQQQ** (risk-on) and **BIL** (cash).

After each official close, compute on QQQ:

```
SMA50  = 50-day simple moving average of QQQ close
SMA150 = 150-day simple moving average of QQQ close
SMA200 = 200-day simple moving average of QQQ close
ret63  = QQQ close[t] / QQQ close[t-63] - 1     (63 trading days)
```

Then evaluate three booleans:

```
A = QQQ close > SMA150
B = QQQ SMA50  > QQQ SMA200          ← load-bearing entry gate
C = QQQ ret63 > 0

score = int(A) + int(B) + int(C)     # range 0..3
```

## State machine

```
If currently in BIL:
    Enter TQQQ at next open  iff  score == 3

If currently in TQQQ:
    Hold TQQQ                iff  score >= 2
    Exit  to BIL at next open iff  score <= 1
```

The `score == 2` "hold zone" is the hysteresis. It eliminates whipsaw
flips on single-condition false alarms.

## Execution policy

- **Signal time:** after official US equity close, on finalized QQQ data.
- **Execution:** next trading day at open.
  Acceptable alternates: VWAP for the same session, or close — must be
  declared in the live-trading config and held constant; mixing fill
  conventions invalidates backtest comparability.
- **Cost convention:** one slippage event per flip (sell + buy combined).
  Stress tests at **3, 10, 25, and 50 bps**.
- **Rebalance frequency:** evaluate every trading day. Flip on signal change.
- **Cash sleeve:** BIL. Total-return adjusted (yield baked into NAV).
- **Discretionary override:** none. Risk overrides only via separately
  defined and backtested rules; **untested overlays are not allowed.**

## What this strategy is NOT

- Not a buy-and-hold replacement. Long-window max DD = -48.1%, worst
  single-day = -20.3%. Treat as a high-risk tactical sleeve.
- Not multi-asset. Signal is QQQ-only; sleeves are TQQQ+BIL only.
- Not regime-detecting in the bear-prediction sense. The SMA50/SMA200
  gate works because it's a *slow-moving trend qualifier*, not because
  it predicts bears. Don't reframe the rationale.
- Not a substitute for portfolio-level risk budgeting. Position sizing
  and account-level DD limits are *separate* layers.

## Replacement bar

Any future variant that proposes to *replace* C1-HYST as primary must:

1. Pass 2x SPY total return in **all** of windows L / A / B / C at **50 bps** slippage.
2. Have window-L max DD no worse than **-48.5%** (no relaxation).
3. Demonstrably **stay out** during the 387-day window-L exposure block
   where D2 ate -62.8% of TQQQ drawdown that HH avoided
   (see `attribution_L.csv` from GHA run 25358962345). A new entry rule
   must be either inactive or exit-active on those days. Otherwise it's
   D2 in different clothes.
4. Show non-redundant value vs HYST in a 2x2 attribution swap.

## Rejected variants — do not re-pitch

| variant | why rejected |
|---|---|
| C1 (original) | passed 3bp, failed at 10/25 bps; flip-rate too high |
| C1-WEEKLY | underperformed daily HYST on key 2022 catch |
| C1-COOLDOWN | re-entry delay missed too many recoveries |
| C1-VEL21 / C1-VEL63 | velocity exits added flips, didn't add return |
| D1 (short drawdown) | failed 2x SPY in all four windows |
| V1 (vol expansion) | failed 2x SPY in all four windows |
| VX1 (VIX level) | failed 2x SPY in all four windows |
| DH (D2 entry + HYST exit) | failed verdict gate: max DD -53.4% > gate -53%, B+C miss 2x SPY at 50bp, flip count exploded to 61 |
| Monthly dual momentum | underperformed C1-HYST on every metric tested |
| D2 (long drawdown) as primary | passes 2x SPY but max DD -66.8% disqualifies it as primary; retained as academic confirmation only |

## Operational checklist (pre-deployment)

- [ ] Re-run on **independent historical data source** (e.g. Stooq, Schwab
      adjusted, Polygon flat files) to confirm the result is not a
      yfinance-specific data artifact. Reproduction within ±2pp
      cumulative is acceptable.
- [ ] Generate **live-trading ledger logic** that emits the same target
      state from production data feeds (Schwab snapshot or equivalent).
- [ ] Define **portfolio-level risk limits** separately from the strategy
      (e.g. account-level DD trip, position-size cap relative to NAV).
      Backtest any overlay before adding it.
- [ ] Run **paper / shadow mode** for at least one full quarter before
      committing capital. Compare daily target state vs production
      ledger; any mismatch is a stop-deploy until reconciled.
- [ ] Document escalation: who is responsible for halting the strategy
      and on what trigger.

## Reference numbers (from GHA run 25358962345, 10 bps slippage)

| window | period | C1-HYST total | DD | Sharpe | flips |
|---|---|---|---|---|---|
| L | 2011-01-03 → 2026-04-29 | +6,609.78% | -48.14% | 0.87 | 49 |
| A | 2021-01-04 → 2026-04-29 | +433.97%   | -36.87% | 0.95 | 15 |
| B | 2022-01-03 → 2026-04-29 | +192.84%   | -36.87% | 0.83 | 15 |
| C | 2022-02-11 → 2026-04-29 | +265.25%   | -36.87% | 0.98 | 14 |

Buy-and-hold on window L: SPY +634%, QQQ +1,265%, **TQQQ +16,031%**.
2x-SPY target: +1,268%. C1-HYST clears the target by ~5x while accepting
~31% of TQQQ B&H's volatility profile.
