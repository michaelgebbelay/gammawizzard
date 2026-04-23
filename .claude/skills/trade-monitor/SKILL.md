---
name: trade-monitor
description: Check today's trade execution across all accounts — did we trade, what was the signal, did it fill? Use when the user asks about today's trades, fills, execution status, or wants a daily trade check.
---

# Trade Monitor Skill

You answer the user's daily question: **"Did we trade today? What? Did it fill?"** across all 3 brokerage accounts and all active strategies.

## Important: Trust Hierarchy

The user does NOT fully trust Google Sheets trade logs because they sometimes miss entries. Always use this priority order:

1. **Broker API** (ground truth) — what actually happened at the broker
2. **DuckDB event log** — what the automation recorded
3. **S3 state files** — what the Lambda decided
4. **Google Sheets** — convenient but unreliable. Use only to confirm, never as sole source.
5. **CloudWatch logs** — last resort for debugging execution failures

When results conflict between sources, **always trust the broker API** and flag the discrepancy.

## Accounts & Strategies

| Account | Broker | Strategies |
|---------|--------|------------|
| Schwab (hashed) | Charles Schwab | ConstantStable, ButterflyTuesday, DualSide |
| 5WT20360 | TastyTrade IRA | **Novix only** (CS disabled) |
| 5WT09219 | TastyTrade Individual | **ConstantStable only** (Novix disabled) |

## Step-by-Step: Daily Trade Check

### Step 1: Pull Broker Orders (Ground Truth)

**Schwab:**
```python
from scripts.schwab_token_keeper import schwab_client
from datetime import datetime, timedelta
import pytz

c = schwab_client()
acct_hash = c.get_account_numbers().json()[0]["hashValue"]
et = pytz.timezone("America/New_York")
today = datetime.now(et).date()
start = datetime(today.year, today.month, today.day, tzinfo=et)
end = start + timedelta(days=1)

orders = c.get_orders_for_account(
    acct_hash,
    from_entered_datetime=start,
    to_entered_datetime=end
).json()
```

**TastyTrade (both accounts):**
```python
import sys, os
sys.path.insert(0, os.path.join(os.environ.get("GAMMA_ROOT", "/Users/mgebremichael/Documents/Gamma"), "TT/Script"))
from tt_client import request

for acct in ["5WT20360", "5WT09219"]:
    os.environ["TT_ACCOUNT_NUMBER"] = acct
    orders = request("GET", f"/accounts/{acct}/orders/live").json()
    # Also check recent filled:
    all_orders = request("GET", f"/accounts/{acct}/orders").json()
```

### Step 2: Classify Each Order

**By strategy (Schwab):**

| Strategy | Identifier | Structure | Typical Time (ET) |
|----------|-----------|-----------|-------------------|
| ConstantStable | API tag present | 2-leg vertical, 5-wide | 4:08-4:45 PM |
| DualSide | API tag present | 2-leg vertical, 10-wide | 4:00-4:15 PM |
| ButterflyTuesday | API tag present | 3-strike butterfly | 3:55-4:25 PM |
| Novix | API tag present | 2-leg vertical | ~4:13 PM |
| Manual | No API tag | Any | Any |

API tag value: `TA_1michaelbelaygmailcom1755679459`

**Signal type classification (from order legs):**
- Both legs same side (SELL/SELL or BUY/BUY): **IC_SHORT** or **IC_LONG**
- Mixed (SELL put spread + BUY call spread): **RR_LONG_CALL**
- Mixed (BUY put spread + SELL call spread): **RR_LONG_PUT**
- 3-leg butterfly: check if net debit (BUY) or net credit (SELL)
- Look at `complexOrderStrategyType`: VERTICAL, BUTTERFLY, IRON_CONDOR, CUSTOM

**Fill status:**
- Schwab: `status` field — FILLED, CANCELED, REJECTED, EXPIRED, WORKING
- Schwab: `filledQuantity` vs expected quantity → FULL, PARTIAL, NONE
- TastyTrade: `status` field — Filled, Cancelled, Rejected, Expired, Live
- TastyTrade: `filled-quantity` vs `remaining-quantity`

### Step 3: Parse Order Details

**Schwab OSI symbol parsing:**
```python
import re
symbol = leg["instrument"]["symbol"]  # e.g., "SPXW  260319P06535000"
m = re.match(r"(?:SPXW|SPX)\s+(\d{6})([PC])(\d{8})", symbol)
raw_date, put_call, raw_strike = m.groups()
expiry = f"20{raw_date[:2]}-{raw_date[2:4]}-{raw_date[4:6]}"
strike = int(raw_strike) / 1000
```

**TastyTrade symbol parsing:**
```python
# TT uses OCC format: "SPXW  260319P05800000" or similar
# Same regex works. TT uses kebab-case JSON keys.
```

**Price interpretation:**
- `price` field is per-share (multiply × 100 for per-contract dollars)
- Credit: `orderType = "NET_CREDIT"` or `price-effect = "Credit"`
- Debit: `orderType = "NET_DEBIT"` or `price-effect = "Debit"`

### Step 4: Cross-Reference with Automation Log

**DuckDB (if available):**
```python
from reporting.db import get_connection, init_schema
con = get_connection()
init_schema(con)

runs = con.execute("""
    SELECT strategy, account, signal, status, started_at, completed_at
    FROM strategy_runs WHERE trade_date = ?
""", [str(today)]).fetchdf()

intents = con.execute("""
    SELECT strategy, account, side, target_qty, outcome
    FROM intended_trades WHERE trade_date = ?
""", [str(today)]).fetchdf()
```

**Trigger audit script:**
```bash
python scripts/reporting/run_trade_audit.py --date 2026-03-29
cat reporting/data/reports/trigger_audit_2026-03-29.md
```

**S3 state (ButterflyTuesday):**
```python
import boto3, json
s3 = boto3.client("s3")
obj = s3.get_object(Bucket="gamma-sim-cache", Key="cadence/bf_daily_state.json")
state = json.loads(obj["Body"].read())
# Also check /tmp/bf_plan.json if running locally
```

### Step 5: Check Google Sheets (Confirm, Not Trust)

```python
from scripts.lib.sheets import get_worksheet
ws = get_worksheet("ConstantStableTrades")
rows = ws.get_all_records()
today_rows = [r for r in rows if r.get("trade_date") == str(today)]
```

Tabs to check:
- `ConstantStableTrades` — CS signal and fills per account
- `BF_Trades` — butterfly decisions (SKIP/ERROR/OK/FILLED)
- `CS_TT_Close` — TT close fills

## Output Format

Present results as a clear daily summary:

```
## Trade Monitor — {date} ({day_of_week})

### ConstantStable
| Account | Signal | Put Side | Call Side | Fill Status | Fill Price | Qty |
|---------|--------|----------|-----------|-------------|------------|-----|
| Schwab  | IC_LONG | BUY 5800/5795P | BUY 5920/5925C | FILLED | $0.95 + $1.05 | 3 |
| TT-IRA  | IC_LONG | BUY 5800/5795P | BUY 5920/5925C | FILLED | $0.90 + $1.00 | 2 |
| TT-Indv | IC_LONG | BUY 5800/5795P | BUY 5920/5925C | PARTIAL (1/2) | $0.92 | 1 |

### ButterflyTuesday
| Account | Signal | VIX1D | Structure | Fill Status | Fill Price | Qty |
|---------|--------|-------|-----------|-------------|------------|-----|
| Schwab  | BUY    | 14.2  | 5850/5830/5810 butterfly | FILLED | $0.45 debit | 2 |

### DualSide
| Account | Signal | Put Side | Call Side | Fill Status |
|---------|--------|----------|-----------|-------------|
| Schwab  | SELL   | ... | ... | FILLED |

### Novix
| Account | Signal | Structure | Fill Status |
|---------|--------|-----------|-------------|
| TT-IRA  | RR_LONG_PUT | SELL 5900/5910C + BUY 5800/5795P | FILLED |
| TT-Indv | RR_LONG_PUT | Same | FILLED |

### Discrepancies
- ⚠ TT-Individual CS: broker shows FILLED but Google Sheet has no entry
- ⚠ BF_Trades sheet shows SKIP but broker has a filled butterfly order (manual?)

### Source: Schwab API + TastyTrade API (as of {timestamp} ET)
```

## Key Rules

1. **Always check ALL 3 accounts** — don't stop at Schwab
2. **Always identify the signal type** — IC_LONG vs IC_SHORT vs RR matters for P&L expectations
3. **Flag partial fills explicitly** — "2 of 3 filled" not just "PARTIAL"
4. **Flag discrepancies** between broker and sheets/DuckDB
5. **Include strike details** — the user wants to know WHAT was traded, not just that something filled
6. **Show fill prices** — per-share AND per-contract dollars
7. **Show VIX1D** for butterfly decisions (explains BUY vs SELL vs SKIP)
8. **Separate automated vs manual** trades using the API tag
9. **All times in ET** (America/New_York)
10. **If a strategy didn't trade, say WHY** — was it SKIP (signal said no), ERROR (execution failed), or NOT_SCHEDULED (not a trading day)?

## When There's No Trade

Don't just say "no trades." Explain why:
- "ConstantStable: IC_LONG signal, but Schwab order REJECTED (insufficient buying power)"
- "ButterflyTuesday: SKIP — VIX1D = 8.2 (below 10 threshold)"
- "DualSide: Not scheduled today (weekend)"
- "Novix: Signal fired but TT API returned error (check CloudWatch)"

## Debugging Failed Executions

If trades were expected but missing:
1. Check Lambda CloudWatch logs: `gamma-trading-TradingFunction-O0rzFPNn3umX`
2. Check EventBridge schedule was enabled
3. Check token freshness (expired Schwab token = silent failure)
4. Check buying power / margin limits
5. Check if dry-run flag was accidentally set (`BF_DRY_RUN`, `DS_DRY_RUN`, `CS_DRY_RUN`)

## Production Portfolio Overview

When the user asks "what are we running" or "why do we trade X" — use this section.

### Daily Execution Timeline (all times ET)

| Time | Strategy | Account | Action |
|------|----------|---------|--------|
| 3:58 PM | Warmup | all | Lambda container pre-warm |
| 4:01 PM | CS IC_LONG Filter | Schwab | Regime check → S3 decision |
| 4:01 PM | ButterflyTuesday | Schwab | **DISABLED** (pending sell filter review since 2026-03-29) |
| 4:05 PM | **DualSide** | Schwab | Regime switch verticals (v1.4.0) |
| 4:13 PM | **ConstantStable** | Schwab | GammaWizard signal verticals (v2.4.0) |
| 4:13 PM | **ConstantStable** | TT Individual | Same signal, TT execution |
| Next 9:35 AM | CS IC_LONG Morning | Schwab + TT | Deferred IC_LONG entry |

### Why Each Strategy Exists

**ConstantStable** — The Foundation
- Neural net picks direction + strikes for 1DTE, 5-wide SPX verticals daily
- Live since Jul 2024. +$0.56 net/trade after costs. 60% RR_LC (bullish bias)
- Profits on UP days (+$1.54/trade), loses on DOWN (-$0.33)
- IC_LONG struggling in 2026 → L1 regime filter flips to RR_SHORT when triggered

**DualSide** — The Diversifier
- Chain-derived IV/skew regime switch for 10-wide, 5-6DTE verticals
- Uncorrelated with CS (r = 0.097). +$65 net/combo after costs
- Profits on DOWN days (+$0.53/trade), flat on UP (+$0.06)
- Together with CS: positive in both directions, combined Sharpe 30% better

**ButterflyTuesday** — The Alpha Generator (Paused)
- Highest $/trade ($550+ gross). Cost absorption at 2% vs 18-24% for verticals
- Disabled 2026-03-29 pending sell filter analysis

### How They Interact
- CS and DualSide run on the same Schwab account but different DTEs (1DTE vs 5-6DTE), different widths (5 vs 10), different signals → positions don't overlap
- r = 0.097 correlation — essentially independent daily P&L
- CS profits UP days, DualSide profits DOWN days → combined portfolio positive in both directions
- NO_CLOSE guards on both prevent accidental position netting

### Net Performance (Cost-Adjusted)

| Strategy | Net $/trade | Trades/year | Est. Annual $ |
|----------|------------|-------------|---------------|
| DualSide (Schwab) | $65/combo | ~240 | ~$15,600 |
| CS (Schwab) | $56/trade | ~250 | ~$14,000 |
| CS (TT Individual) | $56/trade | ~250 | ~$14,000 |
| ButterflyTuesday* | $737/trade | ~40 | ~$29,500 |

*Disabled but shows why re-enabling matters.

### Accounts & Capital

| Account | Broker | Strategies Active | Unit $ |
|---------|--------|------------------|--------|
| Schwab | Charles Schwab | CS + DualSide | $15,000 |
| TT Individual (5WT09219) | TastyTrade | CS only | $15,000 |
| TT IRA (5WT20360) | TastyTrade | Novix only | $15,000 |

## Known Issues & Past Bugs

### Novix `extract_trade` picked oldest signal (fixed 2026-03-31)
- **Symptom**: Novix placed orders for expired options (QUOTE_EMPTY), no fills, rc=0 silent success
- **Root cause**: GetNovix returns a **list of 10 signals sorted newest-first**. `extract_trade()` used `reversed()` iteration (designed for CS's `{"Trade": [...]}` oldest-first format), so it picked the oldest entry.
- **Fix**: Changed `for it in reversed(j)` → `for it in j` in both `TT/Script/Novix/orchestrator.py` and `scripts/trade/Novix/orchestrator.py`
- **Key difference**: CS endpoint (`GetUltraPureConstantStable`) returns `{"Trade": {single_dict}}`, Novix endpoint (`GetNovix`) returns a top-level `[list]` sorted newest-first

### BUNDLE4 fallback parameter bug (fixed 2026-04-06)
- **Symptom**: Novix IC trades (both sides SHORT) failed to execute on April 2-3. BUNDLE4 placed at mid → NO_FILL → fallback crashed with rc=1.
- **Root cause**: `place_two_verticals_simul()` expects `qty1, qty2` but BUNDLE4 fallback passed `qty=remaining`. Also, `submit_one()` inside the function used undefined `qty` instead of `qty1`/`qty2`.
- **Fix**: Changed `qty=remaining` → `qty1=remaining, qty2=remaining` in `TT/Script/ConstantStable/place.py:1688-1694`, and fixed `submit_one` calls at lines 1120/1123.
- **When it triggers**: Only when both signal sides are SHORT (both CREDIT), which selects BUNDLE4 mode. Mixed directions (LONG+SHORT) use PAIR_ALT and were unaffected.

### Schwab API timeout on daily report (fixed 2026-04-02)
- **Symptom**: Daily P&L email fails with `httpx.ReadTimeout` — Schwab `get_orders_for_account` times out at default 5s.
- **Fix**: Extended httpx timeout to 60s and added 3 retries with backoff in `reporting/broker_pnl.py:load_orders_from_schwab()`.

### TT filled-quantity is null (fixed 2026-04-07)
- **Symptom**: TT trades show `0x` in the daily report. TT API does not populate `filled-quantity` on order objects.
- **Fix**: Fall back to `size` field when order status is `Filled` in `reporting/daily_pnl_email.py:_load_tt_orders_for_date()`.

### Schwab API rate limiting (HTTP 429)
- **Symptom**: Many canceled orders, retries with exponential backoff in logs
- **Context**: Normal during high-activity windows. The placer retries and eventually fills. Not a bug — just noisy logs.
