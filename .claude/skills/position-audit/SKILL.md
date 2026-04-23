---
name: position-audit
description: Audit open/closed positions, reconcile broker data, trace trades end-to-end, query the DuckDB portfolio store across Schwab and TastyTrade.
---

# Skill: Position Audit

Audit open/closed positions, reconcile broker data, trace trades end-to-end, and query the canonical DuckDB portfolio store across Schwab and TastyTrade accounts.

## DuckDB Database

Location: `reporting/data/portfolio.duckdb`
Connection module: `reporting/db.py` (`get_connection()`, `init_schema()`)
Schema definition: `reporting/schema.sql`

### Key Tables

| Table | Purpose |
|---|---|
| `raw_events` | Immutable append-only event log from Lambda runs (strategy_run, trade_intent, fill, skip, error, etc.) |
| `strategy_runs` | Materialized from raw_events: one row per strategy execution (date, signal, config, spot, vix, vix1d, filters) |
| `intended_trades` | What the strategy wanted to trade (legs, target_qty, limit_price, outcome: PENDING/FILLED/PARTIAL/REJECTED/CANCELED/TIMEOUT) |
| `order_events` | Broker-facing order lifecycle (submitted, working, partial_fill, filled, canceled, rejected, expired) |
| `fills` | Actual fill executions (fill_qty, fill_price, legs, source: internal or broker_sync) |
| `positions` | Lifecycle-tracked positions (the canonical "what is open now?" table) |
| `position_legs` | Per-leg detail: OSI symbol, option_type, strike, expiry, action, qty, fill_price |
| `position_relationships` | Rolls, adjustments, splits (ROLLED_FROM, ROLLED_TO, ADJUSTED_FROM, SPLIT_FROM, MERGED_INTO) |
| `broker_raw_orders` | Raw Schwab/TT order payloads stored unchanged, keyed by SHA-256 idempotency |
| `broker_raw_fills` | Raw broker fill payloads |
| `broker_raw_positions` | Raw broker position snapshots (latest per account per fetch) |
| `broker_raw_cash` | Raw broker balance/cash payloads |
| `account_snapshots` | Daily account-level: cash, net_liq, buying_power, open_positions, realized_pnl_day, unrealized_pnl |
| `position_snapshots` | Daily mark-to-market per position: mark_price, dte, unrealized_pnl, assignment_risk |
| `cash_events` | Ledger of cash flows: trade, fee, commission, interest, transfer, dividend, assignment, expiry |
| `reconciliation_runs` | One row per reconciliation execution (checks_run, issues_found, auto_resolved) |
| `reconciliation_items` | Individual discrepancies: check_type, severity (INFO/WARNING/ERROR/CRITICAL), status, internal_value vs broker_value |
| `source_freshness` | Per-source SLA tracking (schwab_orders, schwab_positions, tt_orders, etc.) |
| `strategy_daily` | Materialized scorecard: trades opened/closed/skipped, realized/unrealized P&L, win_rate_20d, expectancy_20d |
| `strategy_signal_rows` | Leo/GW signal data: spot, forward, vix, vix1d, strikes, greeks, rv metrics |
| `portfolio_daily` | Portfolio rollup: total cash, net_liq, drawdown, peak, unresolved issues |
| `strategy_trigger_windows` | Allowed execution windows per strategy/account/weekday in ET (for discretionary classification) |

## Position Lifecycle

State machine defined in `reporting/position_engine.py`:

```
INTENDED --> PARTIALLY_OPEN --> OPEN --> PARTIALLY_CLOSED --> CLOSED
                                    \-> EXPIRED
                                    \-> ASSIGNED
                                    \-> BROKEN (can resolve to CLOSED/EXPIRED/ASSIGNED)
```

Valid transitions:
- INTENDED: can go to PARTIALLY_OPEN, OPEN, BROKEN
- PARTIALLY_OPEN: can go to OPEN, PARTIALLY_CLOSED, CLOSED, BROKEN
- OPEN: can go to PARTIALLY_CLOSED, CLOSED, EXPIRED, ASSIGNED, BROKEN
- PARTIALLY_CLOSED: can go to CLOSED, EXPIRED, ASSIGNED, BROKEN
- BROKEN: can be resolved to CLOSED, EXPIRED, ASSIGNED
- CLOSED, EXPIRED, ASSIGNED: terminal states (no further transitions)

Provenance: STRATEGY (automated), MANUAL (discretionary), CORRECTION
Closure reasons: TARGET, STOP, EXPIRY, ASSIGNMENT, ROLL, MANUAL, UNKNOWN

Trust status on positions: TRUSTED, UNVERIFIED, STALE, DISPUTED

## Broker Sync (Schwab + TastyTrade)

### SHA-256 Idempotency Keys

Both `broker_sync_schwab.py` and `broker_sync_tt.py` use the same idempotency pattern:

```python
def _idem_key(broker, entity_type, payload):
    canonical = json.dumps({"b": broker, "t": entity_type, "p": payload},
                           separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(canonical.encode()).hexdigest()[:20]
```

The first 20 hex chars of the SHA-256 hash serve as a dedup key. If a `broker_raw_orders` row with the same `idempotency_key` already exists, the insert is skipped. This makes sync idempotent -- running it multiple times produces no duplicates.

### Source Freshness Tracking

The `source_freshness` table tracks last successful sync per source (e.g., `schwab_orders`, `schwab_positions`, `tt_orders`). Each source has an `sla_minutes` threshold. If `last_success_at` exceeds the SLA window, `is_stale` is set to true. Stale sources degrade the daily report trust banner from GREEN to YELLOW.

### Account Mapping

| Account | Broker | Label |
|---|---|---|
| Schwab (single account) | schwab | `schwab` |
| TT IRA 5WT20360 | tastytrade | `tt-ira` |
| TT Individual 5WT09219 | tastytrade | `tt-individual` |

Schwab client resolution: tries `schwab_token_keeper.schwab_client()` first, then falls back to direct SDK `client_from_token_file`.
TT client resolution: imports `TT/Script/tt_client.py` request function. TT wraps API responses in `{"data": {...}}`.

## Querying Positions

### By lifecycle state
```sql
SELECT position_id, strategy, account, trade_date, expiry_date,
       entry_price, qty, lifecycle_state, signal, config
FROM positions
WHERE lifecycle_state = 'OPEN'
ORDER BY trade_date DESC;
```

### By account
```sql
SELECT * FROM positions
WHERE account = 'schwab'
  AND lifecycle_state IN ('OPEN', 'PARTIALLY_OPEN')
ORDER BY trade_date DESC;
```

### By strategy
```sql
SELECT * FROM positions
WHERE strategy = 'butterfly'
  AND lifecycle_state = 'OPEN'
ORDER BY trade_date DESC;
```

### By date range
```sql
SELECT * FROM positions
WHERE trade_date BETWEEN '2026-03-01' AND '2026-03-29'
ORDER BY trade_date;
```

### Positions with legs
```sql
SELECT p.position_id, p.strategy, p.account, p.trade_date,
       p.lifecycle_state, p.entry_price, p.qty,
       pl.osi, pl.option_type, pl.strike, pl.expiry_date, pl.action
FROM positions p
JOIN position_legs pl ON pl.position_id = p.position_id
WHERE p.lifecycle_state = 'OPEN'
ORDER BY p.trade_date DESC, pl.strike;
```

### Recently closed with P&L
```sql
SELECT position_id, strategy, account, trade_date, expiry_date,
       entry_price, exit_price, realized_pnl, closure_reason
FROM positions
WHERE lifecycle_state IN ('CLOSED', 'EXPIRED')
  AND closed_at >= current_date - INTERVAL 7 DAY
ORDER BY closed_at DESC;
```

## Tracing a Trade: Order to Fill to Position to P&L

To trace a specific trade end-to-end, use the `trade_group_id` as the join key:

```sql
-- 1. Find the intent
SELECT * FROM intended_trades WHERE trade_group_id = '<ID>';

-- 2. Find orders
SELECT * FROM order_events WHERE trade_group_id = '<ID>' ORDER BY ts_utc;

-- 3. Find fills
SELECT * FROM fills WHERE trade_group_id = '<ID>';

-- 4. Find the position (position_id = trade_group_id)
SELECT * FROM positions WHERE position_id = '<ID>';

-- 5. Find position legs
SELECT * FROM position_legs WHERE position_id = '<ID>' ORDER BY strike;

-- 6. Check strategy run context
SELECT sr.* FROM strategy_runs sr
JOIN fills f ON f.run_id = sr.run_id
WHERE f.trade_group_id = '<ID>';
```

To find the trade_group_id for a given date/strategy:
```sql
SELECT DISTINCT trade_group_id, strategy, account, trade_date
FROM intended_trades
WHERE trade_date = '2026-03-28' AND strategy = 'constantstable';
```

## broker_pnl.py Utilities

### parse_osi(symbol)

Parses OCC option symbols like `SPXW  260319P06535000` or `SPX   260320P06520000`:

```python
from reporting.broker_pnl import parse_osi

expiry, option_type, strike = parse_osi("SPXW  260319P06535000")
# expiry = "2026-03-19", option_type = "PUT", strike = 6535.0
```

Returns `(None, None, None)` if the symbol cannot be parsed.

### classify_order(order)

Classifies a Schwab order dict into a strategy based on:
1. **API tag**: checks for `TA_1michaelbelaygmailcom1755679459` (automated vs manual)
2. **Fill time (ET)**: CS morning window 09:00-10:30, DualSide 16:00-16:15, CS afternoon 16:08-16:45, BF 15:55-16:25
3. **Spread structure**: number of distinct strikes, width, complexOrderStrategyType

Returns one of: `butterfly`, `butterfly_manual`, `constantstable`, `cs_morning`, `dualside`, `manual`

Non-API-tagged orders with 3 strikes or BUTTERFLY type are classified as `butterfly_manual`. All other non-API-tagged orders are `manual`.

For API-tagged orders:
- 3 strikes or BUTTERFLY type -> `butterfly`
- 09:00-10:30 ET -> `cs_morning`
- Width=10 and 16:00-16:15 ET -> `dualside`
- Anything else -> `constantstable`

## Reconciliation

### How Issues Are Detected

`reporting/reconciliation.py` runs four checks:

1. **fill_match**: Compares internal `fills` table against `broker_raw_fills`. Flags fills that exist in one system but not the other.

2. **position_match**: Compares internal open positions (OPEN, PARTIALLY_OPEN, PARTIALLY_CLOSED) against `broker_raw_positions`. Flags:
   - Positions open internally but missing from broker (possibly expired/closed without our knowledge)
   - Positions at broker but missing internally (likely discretionary trades)
   - Broker-only positions are classified via `strategy_trigger_windows`: if the first fill falls outside all allowed execution windows, it is tagged `likely_discretionary` with severity INFO (does not degrade trust banner)

3. **cash_match**: Compares `account_snapshots` against `broker_raw_cash` for net_liq, cash, buying_power discrepancies.

4. **freshness**: Checks `source_freshness` SLA thresholds. If any source exceeds its `sla_minutes`, it is flagged as stale.

### Reconciliation Storage

Each run creates a `reconciliation_runs` row. Individual issues go into `reconciliation_items` with:
- `check_type`: fill_match, position_match, cash_match, freshness
- `severity`: INFO, WARNING, ERROR, CRITICAL
- `status`: UNRESOLVED, AUTO_RESOLVED, MANUALLY_RESOLVED, SUPPRESSED
- `internal_value` vs `broker_value`: the discrepant values
- `classification` / `classification_reason`: e.g., `likely_discretionary` / `OUTSIDE_TRIGGER_WINDOW(time=14:32ET,weekday=2)`

Prior UNRESOLVED items are auto-resolved at the start of each new reconciliation run. If the issue persists, the check re-creates a fresh row.

### Querying Reconciliation Issues
```sql
-- All unresolved issues
SELECT id, check_type, severity, entity_type, entity_id, message,
       internal_value, broker_value, classification
FROM reconciliation_items
WHERE status = 'UNRESOLVED'
ORDER BY severity DESC, opened_at DESC;

-- Actionable issues only (excludes INFO-level discretionary)
SELECT * FROM reconciliation_items
WHERE status = 'UNRESOLVED'
  AND severity IN ('WARNING', 'ERROR', 'CRITICAL')
ORDER BY severity DESC;

-- History of resolved issues
SELECT * FROM reconciliation_items
WHERE status IN ('AUTO_RESOLVED', 'MANUALLY_RESOLVED')
  AND resolved_at >= current_date - INTERVAL 7 DAY
ORDER BY resolved_at DESC;
```

## Reporting Pipeline Order

Defined in `reporting/run_pipeline.py`. The full chain runs in this order:

```
1. S3 event sync       -- Download JSONL event files from S3 (gamma-sim-cache bucket)
2. Event ingestion     -- Parse JSONL into raw_events, materialize strategy_runs/intended_trades/order_events/fills
3. Schwab broker sync  -- Pull orders, fills, positions, balances from Schwab API into broker_raw_* tables
4. TT broker sync      -- Pull orders, positions, balances from TastyTrade API into broker_raw_* tables
5. Position engine     -- materialize_positions(): link fills to positions; process_expiries(): mark expired
6. P&L calculation     -- compute_all_pnl(): settlement lookup (Leo signal rows -> Schwab quote), compute per-position realized P&L
7. Strategy summaries  -- materialize_all(): build strategy_daily and portfolio_daily rollups
8. Daily report        -- generate_report(): Markdown report with trust banner, open/closed positions, account snapshots, recon issues
```

Run the pipeline:
```bash
python -m reporting.run_pipeline                        # today
python -m reporting.run_pipeline --date 2026-03-28      # specific date
python -m reporting.run_pipeline --since 2026-03-01     # backfill range
python -m reporting.run_pipeline --no-broker-sync       # skip broker API calls
python -m reporting.run_pipeline --save-report          # write markdown to disk
```

## Cross-Broker Reconciliation Patterns

### Comparing Schwab vs TT Positions for the Same Strategy

ConstantStable runs on both Schwab and TT-IRA. To compare:

```sql
-- Open positions by account for the same strategy
SELECT account, COUNT(*) as open_count,
       SUM(realized_pnl) as total_pnl,
       MIN(trade_date) as oldest_open,
       MAX(trade_date) as newest_open
FROM positions
WHERE strategy = 'constantstable'
  AND lifecycle_state = 'OPEN'
GROUP BY account;
```

### Broker-reported positions not tracked internally

```sql
-- Latest broker position snapshot per account
SELECT account, broker, fetched_at, raw_payload
FROM broker_raw_positions
WHERE fetched_at = (
    SELECT MAX(fetched_at) FROM broker_raw_positions bp
    WHERE bp.account = broker_raw_positions.account
)
ORDER BY account;
```

### Matching broker fills to internal fills by order_id

```sql
SELECT f.fill_id, f.order_id, f.fill_price, f.fill_qty, f.source,
       brf.broker, brf.fill_id as broker_fill_id
FROM fills f
LEFT JOIN broker_raw_fills brf ON brf.order_id = f.order_id
WHERE f.ts_utc >= current_date - INTERVAL 7 DAY;
```

## Common Audit Queries

### Daily trade count by strategy and account
```sql
SELECT trade_date, strategy, account,
       COUNT(*) FILTER (WHERE lifecycle_state != 'INTENDED') as trades,
       SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) as wins,
       SUM(CASE WHEN realized_pnl < 0 THEN 1 ELSE 0 END) as losses,
       ROUND(SUM(realized_pnl), 2) as total_pnl
FROM positions
WHERE trade_date >= current_date - INTERVAL 30 DAY
GROUP BY trade_date, strategy, account
ORDER BY trade_date DESC, strategy;
```

### Account snapshot trend
```sql
SELECT snapshot_date, account, net_liq, cash, buying_power,
       open_positions, realized_pnl_day
FROM account_snapshots
WHERE snapshot_date >= current_date - INTERVAL 30 DAY
ORDER BY snapshot_date DESC, account;
```

### Positions expiring today
```sql
SELECT p.position_id, p.strategy, p.account, p.entry_price, p.qty,
       pl.osi, pl.strike, pl.option_type
FROM positions p
JOIN position_legs pl ON pl.position_id = p.position_id
WHERE p.expiry_date = current_date
  AND p.lifecycle_state = 'OPEN'
ORDER BY pl.strike;
```

### Strategy run outcomes (last 14 days)
```sql
SELECT trade_date, strategy, account, signal, config, status, reason,
       spot, vix, vix1d
FROM strategy_runs
WHERE trade_date >= current_date - INTERVAL 14 DAY
ORDER BY trade_date DESC, strategy;
```

### Fills with no matching position (orphans)
```sql
SELECT f.fill_id, f.trade_group_id, f.order_id, f.fill_price, f.fill_qty
FROM fills f
LEFT JOIN positions p ON p.position_id = f.trade_group_id
WHERE p.position_id IS NULL
  AND f.ts_utc >= current_date - INTERVAL 14 DAY;
```

### Broken positions needing resolution
```sql
SELECT position_id, strategy, account, trade_date, trust_status, trust_reasons
FROM positions
WHERE lifecycle_state = 'BROKEN'
ORDER BY trade_date DESC;
```

### P&L by strategy (monthly)
```sql
SELECT DATE_TRUNC('month', trade_date) as month,
       strategy,
       COUNT(*) as trades,
       SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) as wins,
       ROUND(SUM(realized_pnl), 2) as total_pnl,
       ROUND(AVG(realized_pnl), 2) as avg_pnl,
       ROUND(SUM(CASE WHEN realized_pnl > 0 THEN realized_pnl ELSE 0 END), 2) as gross_wins,
       ROUND(SUM(CASE WHEN realized_pnl < 0 THEN realized_pnl ELSE 0 END), 2) as gross_losses
FROM positions
WHERE lifecycle_state IN ('CLOSED', 'EXPIRED')
  AND realized_pnl IS NOT NULL
GROUP BY month, strategy
ORDER BY month DESC, strategy;
```

### Source freshness check
```sql
SELECT source_name, last_success_at, source_asof, sla_minutes, is_stale, error_message
FROM source_freshness
ORDER BY is_stale DESC, last_success_at;
```

### Intended trades that never filled
```sql
SELECT it.intent_id, it.strategy, it.account, it.trade_date,
       it.side, it.direction, it.target_qty, it.limit_price, it.outcome
FROM intended_trades it
WHERE it.outcome IN ('TIMEOUT', 'REJECTED', 'CANCELED')
  AND it.trade_date >= current_date - INTERVAL 14 DAY
ORDER BY it.trade_date DESC;
```

## P&L Computation

SPX is cash-settled, European-style (no assignment risk). P&L is computed in `reporting/pnl.py`:

- **Vertical spreads**: entry_price vs settlement intrinsic. SHORT credit spread profits when settlement stays OTM.
- **Butterflies**: 3-strike intrinsic calculation at settlement.
- **Settlement lookup chain**: `strategy_signal_rows.forward` (Leo CSV data) -> Schwab API SPX close quote.
- **Multiplier**: all P&L is per-share * qty * 100 (SPX multiplier).
- **Closed early**: uses matched close order price instead of settlement.
