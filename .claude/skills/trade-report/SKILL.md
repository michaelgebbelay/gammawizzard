---
name: trade-report
description: Generate accurate trade activity, fill, and P&L reports across Schwab and TastyTrade accounts — both automated strategies AND discretionary trades. Use when the user asks about trades, fills, positions, P&L, reconciliation, reporting, or discretionary trade analysis.
---

# Trade Report Skill

You are an expert at reporting on options trading activity across **Schwab** and **TastyTrade (TT)** brokerage accounts. You handle **both automated strategy trades AND discretionary (manual) trades**. Your job is to produce accurate, trustworthy reports by pulling real data from broker APIs and the local DuckDB store.

## Accounts

| Label | Broker | Account | Strategies |
|-------|--------|---------|------------|
| `schwab` | Charles Schwab | (hashed via API) | ConstantStable, ButterflyTuesday, DualSide + Discretionary |
| `tt-ira` | TastyTrade | 5WT20360 | ConstantStable + Discretionary |
| `tt-individual` | TastyTrade | 5WT09219 | ConstantStable + Discretionary |

## Available Data Sources

### 1. Schwab API (via `scripts/schwab_token_keeper.py`)
```python
from scripts.schwab_token_keeper import schwab_client
c = schwab_client()
acct_hash = c.get_account_numbers().json()[0]["hashValue"]

# Orders (last N days)
orders = c.get_orders_for_account(acct_hash,
    from_entered_datetime=start, to_entered_datetime=end).json()

# Positions
c.session.get(f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}",
              params={"fields": "positions"}).json()

# Balances
c.get_account(acct_hash).json()
```

**Schwab order structure:**
- `orderId`, `status` (FILLED, CANCELED, REJECTED, EXPIRED, WORKING)
- `orderLegCollection[].instrument.symbol` — OSI format: `SPXW  260319P06535000`
- `orderLegCollection[].instrument.putCall` — PUT or CALL
- `orderLegCollection[].instrument.strikePrice` — float
- `orderLegCollection[].instruction` — BUY_TO_OPEN, SELL_TO_OPEN, BUY_TO_CLOSE, SELL_TO_CLOSE
- `orderLegCollection[].quantity` — int
- `orderActivityCollection[].executionLegs[]` — fill details: `legId`, `quantity`, `price`, `time`
- `complexOrderStrategyType` — VERTICAL, BUTTERFLY, CUSTOM, NONE
- `orderType` — NET_CREDIT or NET_DEBIT
- `price` — limit/fill price per share (multiply by 100 for per-contract)
- `filledQuantity` — total filled qty
- `tag` — API tag `TA_1michaelbelaygmailcom1755679459` means automated (vs manual)
- `closeTime` / `enteredTime` — timestamps in UTC

**OSI parsing:**
```python
import re
m = re.match(r"(?:SPXW|SPX)\s+(\d{6})([PC])(\d{8})", symbol)
yy, mm, dd = raw_date[:2], raw_date[2:4], raw_date[4:6]
expiry = f"20{yy}-{mm}-{dd}"
strike = int(raw_strike) / 1000
```

**Schwab balance fields:**
- `securitiesAccount.currentBalances.cashBalance`
- `securitiesAccount.currentBalances.liquidationValue` (net liq)
- `securitiesAccount.currentBalances.buyingPower`
- NOTE: `underlying` field on chain data is None — use `underlyingPrice` for spot

### 2. TastyTrade API (via `TT/Script/tt_client.py`)
```python
# From within TT/Script/ directory:
from tt_client import request

acct = os.environ["TT_ACCOUNT_NUMBER"]  # 5WT20360 or 5WT09219

# Orders
request("GET", f"/accounts/{acct}/orders").json()

# Live orders
request("GET", f"/accounts/{acct}/orders/live").json()

# Positions
request("GET", f"/accounts/{acct}/positions").json()

# Balances
request("GET", f"/accounts/{acct}/balances").json()

# Transactions (fills, fees, etc.)
request("GET", f"/accounts/{acct}/transactions").json()
```

**TT order structure (kebab-case JSON):**
- `id` — order ID
- `status` — Filled, Cancelled, Rejected, Expired, Live
- `filled-quantity`, `remaining-quantity`
- `legs[].symbol` — OCC symbol
- `legs[].instrument-type` — Equity Option
- `legs[].action` — Buy to Open, Sell to Open, Buy to Close, Sell to Close
- `legs[].quantity`
- `price` — limit price
- `price-effect` — Credit or Debit
- NOTE: TT uses kebab-case field names, NOT camelCase. Handle both formats: `filledQuantity`, `filled_quantity`, `filled-quantity`

**TT balance fields:**
- `cash-balance`
- `net-liquidating-value`
- `maintenance-excess` (buying power)

### 3. DuckDB Portfolio Store
```python
from reporting.db import get_connection, init_schema, query_df, query_one, execute
con = get_connection()  # reporting/data/portfolio.duckdb
init_schema(con)
```

Key tables: `broker_raw_orders`, `broker_raw_fills`, `broker_raw_positions`, `broker_raw_cash`, `positions`, `fills`, `account_snapshots`, `strategy_daily`, `portfolio_daily`. See `reporting/schema.sql` for full schema.

### 4. Broker Sync Modules
```python
# Schwab
from reporting.broker_sync_schwab import sync_schwab
stats = sync_schwab()  # pulls orders, fills, positions, cash into DuckDB

# TastyTrade (both accounts)
from reporting.broker_sync_tt import sync_tt
stats = sync_tt()  # syncs tt-ira (5WT20360) + tt-individual (5WT09219)
```

`reporting/broker_sync_tt.py` mirrors the Schwab sync pattern:
- SHA-256 idempotency keys, source freshness tracking, raw payload storage
- Pulls orders, transactions, positions, balances for each account
- Uses `TT/Script/tt_client.py` via `_get_tt_request()` with sys.path resolution
- Handles TT kebab-case fields via `_get(obj, *keys, default=None)` helper
- Materializes account snapshots with cash, net_liq, buying_power

### 5. P&L Module
```python
from reporting.broker_pnl import load_orders_from_schwab, parse_filled_orders, load_settlements, build_positions, print_report
```
Currently Schwab-only. For TT P&L, you'll need to normalize TT orders into the same structure.

### 6. CSV Trade Logs
- `logs/constantstable_vertical_trades.csv` — CS trade log
- `logs/butterfly_tuesday_trades.csv` — BF trade log
- Headers include: `trade_date, expiry, account, put_filled, call_filled, put_fill_price, call_fill_price, put_status, call_status`

### 7. Google Sheets (via `scripts/lib/sheets.py`)
Tabs: `ConstantStableTrades`, `BF_Trades`, `CS_Tracking`, `GW_Signal`

## Strategy Classification

When looking at Schwab orders, classify by:

| Signal | Tag | Structure | Time (ET) |
|--------|-----|-----------|-----------|
| ConstantStable | API-tagged | 2-leg vertical, 5-wide | 16:08-16:45 |
| DualSide | API-tagged | 2-leg vertical, 10-wide | 16:00-16:15 |
| Butterfly | API-tagged | 3-strike butterfly | 15:55-16:25 |
| CS Morning | API-tagged | 2-leg vertical | 09:30-10:00 |
| Manual | No API tag | Any | Any |

API tag value: `TA_1michaelbelaygmailcom1755679459`

## Discretionary Trade Classification & Reporting

The user actively trades discretionary positions alongside the automated strategies. **Any order WITHOUT the API tag is discretionary.** These are high-volume: typically 200-300+ manual fills per month on Schwab alone.

### Discretionary Trade Structures (by frequency)

| Structure | `complexOrderStrategyType` | Distinct Strikes | Description |
|-----------|---------------------------|------------------|-------------|
| **Vertical spread** | VERTICAL | 2 | Most common (~70%). Various widths (5, 10, 15 wide). Both credit and debit. |
| **Naked/single leg** | NONE | 1 | ~27%. Single calls or puts — often 0DTE scalps. |
| **Butterfly** | BUTTERFLY | 3 | Occasional. Manual butterfly adjustments or new positions. |
| **Iron condor** | CONDOR | 4 | Rare. |
| **Custom multi-leg** | CUSTOM | 3+ | Rare. |

### Discretionary Sub-Classification

When reporting discretionary trades, further classify by **trading style** using time and structure:

| Style | Time (ET) | Expiry | Typical Structure | Description |
|-------|-----------|--------|-------------------|-------------|
| **0DTE Scalp** | 09:30-15:59 | Same day | Vertical or naked | Intraday open + close same session |
| **0DTE Swing** | 09:30-15:59 | Same day | Vertical or naked | Opened but held to expiry (no close order) |
| **Short-dated** | Any | 1-5 DTE | Vertical | Multi-day hold, typically credit spreads |
| **Butterfly Manual** | Any | Any | Butterfly (3 strikes) | Manual butterfly trades (not from ButterflyTuesday) |
| **Adjustment** | Any | Matches existing | Any close order | Closing/adjusting an existing position |

### Matching Discretionary Opens to Closes

Discretionary P&L requires matching open orders to their corresponding close orders. This is harder than automated trades because:

1. **Partial closes** — may close 2 of 4 contracts
2. **Multiple opens at different prices** — may scale into a position across several fills
3. **Single legs** — naked options need individual leg tracking, not spread-level
4. **Same-day turnarounds** — open at 10:00, close at 13:00, same strikes

**Matching algorithm for discretionary trades:**
1. Group all manual fills by (expiry, strikes_tuple, option_types)
2. Within each group, pair opens with closes chronologically (FIFO)
3. For single legs: match by (expiry, strike, option_type) — a BUY_TO_OPEN pairs with a SELL_TO_CLOSE
4. Handle partial fills: if open qty=4 and close qty=2, split the open into matched (2) and remaining (2)
5. Remaining unmatched opens → either still OPEN or EXPIRED (check expiry vs today)

**P&L for single legs (naked options):**
- **Long call:** `(exit_price - entry_price) * qty * 100` (closed early) or `(max(0, settlement - strike) - entry_price) * qty * 100` (expired)
- **Long put:** `(exit_price - entry_price) * qty * 100` (closed early) or `(max(0, strike - settlement) - entry_price) * qty * 100` (expired)
- **Short call:** `(entry_price - exit_price) * qty * 100` (closed early) or `(entry_price - max(0, settlement - strike)) * qty * 100` (expired)
- **Short put:** `(entry_price - exit_price) * qty * 100` (closed early) or `(entry_price - max(0, strike - settlement)) * qty * 100` (expired)

### Discretionary Report Sections

When reporting discretionary trades, always include these sections:

**1. Activity Summary**
- Total orders (filled / canceled / expired / working)
- Breakdown by structure (verticals, naked, butterflies)
- Breakdown by style (0DTE scalp, swing, short-dated)
- Busiest trading days and times

**2. P&L by Day**
- Net realized P&L per trading day
- Win rate per day
- Gross profit vs gross loss
- Largest single win and loss

**3. P&L by Structure**
- Verticals: credit vs debit, width distribution, win rate
- Single legs: calls vs puts, directional bias, win rate
- Butterflies: separate from automated BF strategy

**4. 0DTE Analysis** (when relevant)
- Round-trip count (opened and closed same day)
- Hold time (minutes between open and close fills)
- Average P&L per round-trip
- Win rate by time of day (morning vs afternoon)
- Unrealized losses on positions held to expiry vs closed early

**5. Open Discretionary Positions**
- Any manual positions not yet expired or closed
- Current DTE, structure, and entry price

**6. Risk Metrics**
- Max concurrent discretionary exposure (sum of max loss on open positions)
- Largest single-day discretionary loss
- Correlation with automated strategy P&L (do discretionary trades help or hurt on days when automated strategies lose?)

## P&L Computation Rules

### Spread types and their intrinsic values at settlement:

**Vertical (2 legs):**
- PUT vertical: `max(0, high_strike - settlement) - max(0, low_strike - settlement)`
- CALL vertical: `max(0, settlement - low_strike) - max(0, settlement - high_strike)`

**Butterfly (3 strikes):**
- PUT: `max(0, low - settlement) - 2*max(0, mid - settlement) + max(0, high - settlement)`
- CALL: `max(0, settlement - low) - 2*max(0, settlement - mid) + max(0, settlement - high)`

**Iron Condor (4 legs):**
- `put_val + call_val` where:
  - `put_val = max(0, s[1] - settlement) - max(0, s[0] - settlement)`
  - `call_val = max(0, settlement - s[2]) - max(0, settlement - s[3])`

### P&L per position:
- **SHORT (credit):** `(entry_credit - exit_cost) * qty * 100`
- **LONG (debit):** `(exit_value - entry_debit) * qty * 100`
- For expired positions: exit value = intrinsic value at SPX settlement
- For closed-early: exit value = close order fill price
- Commissions: Schwab $0.97/contract, TT $1.72/contract

### Settlement prices:

**Primary source — Schwab price history:**
```python
from reporting.daily_pnl_email import _load_spx_spot
spx = _load_spx_spot(for_date=date(2026, 3, 26))  # specific date's close
spx = _load_spx_spot()  # today's live quote
```
`_load_spx_spot(for_date)` uses `Client.PriceHistory` with THREE_MONTHS period to build a date→close map, then returns the specific date's close. Falls back to live quote for today.

**Fallbacks (in order):**
1. `sim/cache/{date}/close5_features.json` (spot field)
2. `close5.json` (chain._underlying_price)
3. DuckDB `strategy_signal_rows.forward` (Leo forward price)

### CRITICAL: Net Position Method for Expired P&L

**Do NOT compute expired P&L by matching individual open orders to close orders.** Discretionary trades are often entered as separate legs (e.g., buy a put and sell a put as two orders, not one spread order). Per-order matching treats these as independent naked positions and produces wildly incorrect P&L.

Instead, use the **net position method**:

```python
# For each expiry date:
# 1. Net all order legs → net qty per (strike, option_type)
# 2. Sum all premiums (credits positive, debits negative)
# 3. Compute settlement value: sum(intrinsic * net_qty * 100) for all legs
# 4. P&L = net_premiums + settlement_value
```

This naturally handles:
- Separate-leg entries that form spreads
- Partial closes (reduce net qty)
- Multiple opens at different prices (averaged in cash flow)
- Complex multi-leg positions

Per-order matching is only reliable for **closed-early trades** where there's an explicit close order (BUY_TO_CLOSE / SELL_TO_CLOSE) matched to an open. But even then, the net position method is preferred when mixing with expired positions.

See `reporting/weekly_pnl.py` for the implementation.

### Auto Adjustment Detection (Discretionary P&L)

**Problem:** Manual closes of automated positions (e.g., BUY_TO_CLOSE an auto-opened IC) appear as manual orders (no API tag) but are NOT pure discretionary trades. Including them in discretionary P&L gives incorrect results.

**Solution — Directional adjustment detection:**

```python
from reporting.daily_pnl_email import _compute_discretionary_pnl

disc_result = _compute_discretionary_pnl(raw_orders, report_date, spx_spot)
# Returns:
#   total_pnl        — pure discretionary P&L (excludes adjustments)
#   trade_count       — total manual trades
#   pure_disc_count   — pure discretionary trades
#   adjustment_count  — manual closes of automated positions
#   adjustment_pnl    — P&L impact of adjustments (via subtraction method)
#   trades            — list of trade details with is_adjustment flag
```

**How `_is_auto_adjustment()` works:**
1. Build the auto net position: net qty per (strike, option_type) from API-tagged orders only
2. For each manual close-only order (all legs are BUY_TO_CLOSE or SELL_TO_CLOSE):
   - Check if ALL legs REDUCE the auto net position (move qty toward zero)
   - If yes → auto adjustment (tagged `[adj]` in reports)
   - If no → pure discretionary trade

**Adjustment P&L uses the subtraction method:**
```
adjustment_pnl = total_pnl(all orders) - auto_pnl(API-tagged) - disc_pnl(pure manual)
```

**Reusable net position helper:**
```python
from reporting.daily_pnl_email import _net_position_pnl_for_expiry
pnl = _net_position_pnl_for_expiry(orders, expiry_str="2026-03-26", spx_close=5750.0)
```

## Report Guidelines

1. **Always state the data source** — "Source: Schwab API (30-day lookback)" or "Source: DuckDB broker_raw_orders"
2. **Always state the as-of date** — Reports are point-in-time
3. **Always separate automated vs discretionary** — Use the API tag to distinguish. Never mix them in a single section.
4. **Flag data gaps explicitly** — "3 expired positions missing SPX settlement", "TT data not synced since X"
5. **Show per-account AND portfolio totals** — Always break down by schwab / tt-ira / tt-individual
6. **Include open positions** — Show positions not yet expired with DTE remaining
7. **Cross-reference when possible** — Compare internal event log vs broker fills to catch discrepancies
8. **Prices are per-share** — Multiply by 100 for per-contract dollar amounts
9. **All timestamps should be shown in ET** (America/New_York) for the user
10. **Show discretionary P&L honestly** — Include commissions, don't cherry-pick, show both wins and losses
11. **Always show the combined bottom line** — After showing automated and discretionary separately, show the total across both
12. **For 0DTE trades, match opens to closes within the same session** — Use chronological FIFO matching by (expiry, strikes, option_type)

## Common Report Types

### Daily P&L Email
```python
from reporting.daily_pnl_email import run_daily_pnl_email
run_daily_pnl_email()  # generates and sends via SMTP
```
Automated via Lambda EventBridge `gamma-daily-pnl` (5 PM + 9 AM ET).
Sections: automated strategy P&L, risk state, **discretionary section** (trade table with `[adj]` tags, disc P&L, adj P&L), combined bottom line.

### Daily Activity Report
Show what happened today across all accounts:
- Automated: Signals generated (BUY/SELL/SKIP per strategy), orders submitted, filled, canceled
- Discretionary: All manual fills with structure classification and `[adj]` tags for auto adjustments
- Fill quality (fill price vs mid at time of entry)
- Open positions summary with DTE (both automated and discretionary)

### Weekly P&L Report
```
python -m reporting.weekly_pnl                        # current week
python -m reporting.weekly_pnl --start 2026-03-23     # specific week
```
Sections:
1. **Expired Positions** — net position method per expiry (SPX close, premiums, settlement value, P&L, residual legs)
2. **Discretionary Trades** — per-day breakdown: date, SPX, trade count, adj count, disc P&L, adj P&L
3. **Summary** — Automated (expired) + Discretionary = WEEK TOTAL

Scheduled in production: **Lambda EventBridge `gamma-weekly-pnl`**, Saturday 9:00 AM ET.
Handler: `lambda/handler.py → _handle_weekly_pnl()` — captures stdout, emails via SMTP.
Event payload: `{"account": "weekly-pnl"}`, supports `dry_run`, `week_start`, `week_end` overrides.

### Discretionary Trade Report
Show all manual trading activity:
- P&L by day, by structure, by time-of-day
- 0DTE round-trip analysis (hold time, win rate)
- Largest wins and losses
- Open discretionary positions
- Weekly and monthly totals
- Comparison: discretionary P&L vs automated P&L

### Position Reconciliation
- Compare internal event log (`raw_events`) vs broker data
- Flag orphan positions (in broker but not in event log = likely manual/discretionary)
- Flag phantom positions (in event log but not in broker = execution failure)
- Use trigger windows to classify unknowns
- Reconcile discretionary opens with closes (unmatched = data gap)

### Fill Quality Audit
- For each fill: compare limit price vs actual fill price
- Track improvement (filled better than limit) vs slippage
- Aggregate by strategy and time of day
- Separate automated vs discretionary fill quality

## When Writing Scripts

- Use `reporting/db.py` for DuckDB access (not raw duckdb imports)
- Use `scripts/schwab_token_keeper.py` for Schwab API access
- Use `TT/Script/tt_client.py` for TastyTrade API access
- Store outputs in `reporting/data/` for persistence
- Follow the idempotency pattern from existing sync modules (SHA-256 hash keys)
- All money values in USD, prices per-share unless labeled per-contract
- All timestamps stored in UTC, displayed in ET
