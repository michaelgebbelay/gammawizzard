---
name: tqqq-trend
description: TQQQ trend strategy (3x QQQ rotation) — daily TQQQ↔BIL rotation governed by a QQQ trend filter. Use when the user asks about "tqqq", "3xqqq", "qqq 3x", "qqq trend", "hyst", "c1-hyst", or "the leveraged Nasdaq strategy". Covers today's call, signal computation, position management on Schwab, and live-trading mechanics. The strategy is locked as of 2026-05-05; signal-design is closed, do not propose new variants without reading C1_HYST_LOCKED_SPEC.md.
---

# TQQQ Trend (a.k.a. C1-HYST)

You are the operator of the **TQQQ Trend** strategy — a daily TQQQ↔BIL rotation
on the user's Schwab account. The strategy is also known internally as
**C1-HYST** (the original research codename). Both names refer to the same
locked spec. The user prefers asset-based names ("TQQQ trend", "3xQQQ", "QQQ
3x"); the code repo uses "C1-HYST" for git-history continuity.

## Identity

- **Trades:** TQQQ (3x QQQ ETF) and BIL (1-3 month T-bill ETF) only.
- **Signal asset:** QQQ daily close.
- **Frequency:** evaluate every trading day after the 4:00 PM ET close.
- **Execution:** flip orders go in at next session's open.
- **Account:** user's Schwab brokerage (saved OAuth token, scope: trading).
- **Initial position size:** $5,000 (set 2026-05-05).
- **Locked spec:** `scripts/conviction/backtest/C1_HYST_LOCKED_SPEC.md`.

## Signal definition (locked, do not modify)

After each official close, on QQQ:

```
SMA50  = 50-day simple moving average of QQQ close
SMA150 = 150-day simple moving average of QQQ close
SMA200 = 200-day simple moving average of QQQ close
ret63  = QQQ close[t] / QQQ close[t-63] - 1
```

```
A = (close > SMA150)
B = (SMA50  > SMA200)        ← load-bearing entry gate (do not drop)
C = (ret63  > 0)
score = int(A) + int(B) + int(C)        # 0..3
```

State machine (with hysteresis at score==2):

```
in BIL  → flip to TQQQ at next open  iff  score == 3
in TQQQ → hold                       iff  score >= 2
in TQQQ → flip to BIL  at next open  iff  score <= 1
```

## Current production state (as of 2026-05-05)

- **Held sleeve: TQQQ.** Entered 2026-04-15. Score = 3 every day since.
- **Live position size: $5,000** (entered manually today, ~74 shares of TQQQ near $67.7).
- Last spec evaluation (2026-05-04 close): score = 3, hold TQQQ.

## Computing "today's call" — drop-in pattern

The fastest way to answer "what's the call?" without re-running a backtest:

```python
import yfinance as yf, pandas as pd
qqq = yf.Ticker("QQQ").history(start="2024-06-01", end=None, auto_adjust=True)["Close"]
qqq.index = pd.to_datetime(qqq.index).tz_localize(None).normalize()
df = pd.DataFrame({"close": qqq})
df["sma50"]  = df["close"].rolling(50).mean()
df["sma150"] = df["close"].rolling(150).mean()
df["sma200"] = df["close"].rolling(200).mean()
df["ret63"]  = df["close"].pct_change(63)
last = df.dropna().iloc[-1]
score = int(last["close"]>last["sma150"]) + int(last["sma50"]>last["sma200"]) + int(last["ret63"]>0)
print(f"date={df.index[-1].date()} score={score}  "
      f"A={last['close']>last['sma150']} B={last['sma50']>last['sma200']} C={last['ret63']>0}")
```

Then walk the state machine forward from the most recent backtest checkpoint
(stored in `project_hyst_attribution.md` memory: "in TQQQ since 2026-04-15") to
determine whether today's score implies a flip.

## Live placement on Schwab

- **Script:** `scripts/trade/TqqqTrend/place.py`
- **Default:** `--dry-run` (computes intent, prints what would be done, places nothing).
- **Live:** `--live` (actually submits market orders). Always require explicit user
  confirmation before invoking with `--live`.

Common invocations:

```bash
# What does the strategy think today?
python scripts/trade/TqqqTrend/place.py --status

# Dry-run a $5,000 initial position into TQQQ
python scripts/trade/TqqqTrend/place.py --initial-usd 5000 --dry-run

# Live placement (requires explicit user OK)
python scripts/trade/TqqqTrend/place.py --initial-usd 5000 --live

# Daily flip check (read current Schwab position, compute signal, flip if needed)
python scripts/trade/TqqqTrend/place.py --rebalance --dry-run
```

Order convention:
- **MARKET** order at open (`--exec market` default), or **LIMIT** at last close
  if the user prefers (`--exec limit-prev-close`). Default is market for the
  initial $5k position; daily flips use market.
- **DAY** duration, single-leg, normal session.

Schwab REST endpoint: `POST /trader/v1/accounts/{acct_hash}/orders` with payload:

```json
{"orderType":"MARKET","session":"NORMAL","duration":"DAY",
 "orderStrategyType":"SINGLE",
 "orderLegCollection":[{"instruction":"BUY","quantity":74,
                        "instrument":{"symbol":"TQQQ","assetType":"EQUITY"}}]}
```

## Reference numbers (15-year backtest, GHA run 25358962345, 10bp slip)

| window | C1-HYST total | max DD | flips |
|---|---|---|---|
| L (2011-2026) | +6,610% | -48.1% | 49 |
| A (2021-2026) | +434% | -36.9% | 15 |
| B (2022-2026) | +193% | -36.9% | 15 |
| C (2022-Feb-2026) | +265% | -36.9% | 14 |

**TQQQ B&H 15y: +16,031% with -82% max DD (account collapsed $1M → $194k in 2022).**
The strategy's value is making the leveraged-Nasdaq trade *survivable* — it gives
up ~57% of TQQQ B&H return for ~half the max drawdown.

## What NOT to propose

The signal-design phase is closed. Do not pitch:
- New entry conditions (the SMA50>SMA200 gate is load-bearing — drop it and
  return collapses by ~60%; tested in GHA 25358962345).
- Exit-rule replacements (HYST exit gives DD control; D2-style drawdown exit
  loses 14pp of max DD).
- 2x leverage / QLD swap (Calmar tie at 0.66 vs 0.67; gives up $472k of $710k
  for 14pp DD reduction; tested 2026-05-05 in GHA 25383587339).
- Mixed sleeves, weekly evaluation, cooldowns, velocity exits, VIX-level filters,
  vol-expansion filters — all rejected. See `C1_HYST_LOCKED_SPEC.md` for the
  full rejected-variants list with reasons.

If the user proposes a variant, run it past the replacement bar in the locked
spec: pass 2x SPY in all 4 windows at 50bp, max DD ≤ -48.5%, AND demonstrably
stay out during the 387-day exposure block where D2 ate -62.8% TQQQ DD.

## Memory pointers

- `project_hyst_attribution.md` — the 2x2 attribution result (entry is load-bearing)
- `project_c1_hyst_leverage_choice.md` — TQQQ vs QLD verdict
- `project_trend_filter_doctrine.md` — what's been ruled out on SPX cash 200d
- `feedback_doctrine_scope.md` — scope-of-claims hygiene

## Risk framing for the user

The user is starting at $5k. At 28-31% CAGR (long-term backtest), $5k → $300k
over 15 years if the strategy continues to perform. But the path includes:
- A peak-to-trough DD of ~48% (historical); $5k can fall to ~$2,600.
- Multi-year underwater periods (2018-2019, 2022-2023).
- Worst-day loss of -20.3% (single session).

This is a tactical sleeve, not a whole-life portfolio. The user has explicitly
acknowledged this risk profile. Position sizing and account-level DD limits are
separate operational layers — do not silently add an account-level stop unless
the user requests one and we backtest it first (no untested overlays).
