# Gamma Portfolio Intelligence System — V1 Spec

## Overview

A deterministic portfolio tracking system that replaces Google Sheets as the source of truth. AI sits on top as a narrative layer; it does not compute P&L, positions, or cash.

**Pipeline:** `raw events + broker sync → normalized ledger → position engine → reconciliation → trusted state → daily report → AI narrative`

---

## 1. Position State Machine

```
INTENDED ──→ PARTIALLY_OPEN ──→ OPEN ──→ PARTIALLY_CLOSED ──→ CLOSED
                                   │                              ↑
                                   ├──→ EXPIRED ─────────────────┘
                                   ├──→ ASSIGNED ────────────────┘
                                   └──→ BROKEN ──→ (any terminal)
```

### States

| State | Meaning |
|-------|---------|
| `INTENDED` | Strategy decided to trade, no fill yet |
| `PARTIALLY_OPEN` | Some legs filled, not all |
| `OPEN` | Fully filled, position active |
| `PARTIALLY_CLOSED` | Some legs closed, remainder open |
| `CLOSED` | All legs closed or netted to zero |
| `EXPIRED` | All legs expired worthless or at intrinsic |
| `ASSIGNED` | One or more legs assigned |
| `BROKEN` | Engine cannot determine state — requires manual review |

### Provenance

| Value | Meaning |
|-------|---------|
| `STRATEGY` | Opened by BF/DS/CS orchestrator |
| `MANUAL` | Opened via manual_trades.py or broker UI |
| `CORRECTION` | Created by a correction event |

### Closure Reasons

| Value | Meaning |
|-------|---------|
| `TARGET` | Hit profit target |
| `STOP` | Hit stop loss |
| `EXPIRY` | Expired (worthless or settled) |
| `ASSIGNMENT` | Assigned by broker |
| `ROLL` | Closed as part of a roll |
| `MANUAL` | Manually closed |
| `UNKNOWN` | Engine cannot determine |

### Position Relationships

| Type | Meaning |
|------|---------|
| `ROLLED_FROM` | New position rolled from old |
| `ROLLED_TO` | Old position rolled into new |
| `ADJUSTED_FROM` | Position was adjusted |
| `SPLIT_FROM` | Position was split |
| `MERGED_INTO` | Position was merged |

---

## 2. Event Schema

Every event is an immutable JSON record with this envelope:

```json
{
  "event_id": "hex16",
  "event_type": "strategy_run|trade_intent|order_submitted|order_update|fill|skip|error|post_step_result|position_close|correction|manual_adjustment",
  "ts_utc": "ISO-8601",
  "strategy": "butterfly|constantstable|dualside",
  "account": "schwab|tt-ira|tt-individual",
  "trade_group_id": "hex16",
  "run_id": "hex16",
  "config_version": "git-sha-12",
  "payload": { ... },
  "idempotency_key": "sha256-16"
}
```

### Event Types

| Type | When | Key Payload Fields |
|------|------|--------------------|
| `strategy_run` | Start of each orchestrator run | signal, config, reason, spot, vix, vix1d, filters |
| `trade_intent` | Before any order placed | side, direction, legs, target_qty, limit_price |
| `order_submitted` | Order sent to broker | order_id, legs, limit_price, order_type |
| `order_update` | Any order status change | order_id, status, filled_qty, remaining_qty |
| `fill` | Order filled (full/partial) | order_id, fill_qty, fill_price, legs |
| `skip` | Strategy decides not to trade | reason, signal |
| `error` | Any error during run | message, stage |
| `post_step_result` | Post-trade step done | step_name, outcome |
| `position_close` | Position closed | reason, fill_price, pnl |
| `correction` | Corrects a prior event | original_event_id, correction |
| `manual_adjustment` | Manual override | description, adjustment |

### Idempotency

Every event gets a deterministic `idempotency_key = sha256(event_type + sorted_payload)[:16]`. On ingest, duplicates are silently dropped. This makes the entire pipeline rerunnable.

### trade_group_id

Generated before any order goes out. All events for a single trade (intent, orders, fills, close) share the same `trade_group_id`. For DS which places PUT and CALL separately, each side gets its own `trade_group_id` via `new_trade_group()`.

---

## 3. Canonical Store Schema

See `schema.sql` for full DDL. Key tables:

### Core
- `raw_events` — immutable event log, deduplicated by idempotency_key
- `strategy_runs` — one row per BF/DS/CS run
- `intended_trades` — what the strategy wanted to do
- `order_events` — submitted, working, canceled, rejected, filled
- `fills` — actual executions

### Position Engine
- `positions` — lifecycle-tracked positions with state machine
- `position_legs` — individual option legs per position
- `position_relationships` — rolls, adjustments, splits

### Broker Sync
- `broker_raw_orders` — raw Schwab/TT order payloads, unchanged
- `broker_raw_fills` — raw fill payloads
- `broker_raw_positions` — raw position snapshots
- `broker_raw_cash` — raw cash/balance snapshots

### Account & Portfolio
- `account_snapshots` — daily cash, net liq, buying power per account
- `position_snapshots` — daily mark, DTE, unrealized P&L per position
- `cash_events` — trade, fee, commission, interest, transfer, dividend, assignment, expiry

### Reconciliation
- `reconciliation_runs` — audit trail of each recon pass
- `reconciliation_items` — individual mismatches with status workflow
- `source_freshness` — staleness tracking per data source

### Reporting
- `strategy_daily` — per-strategy daily scorecard
- `portfolio_daily` — whole-book rollup
- `daily_diary` — narrative + structured facts
- `daily_report_outputs` — rendered reports (markdown, sheets, email)
- `strategy_config_versions` — track config changes for anomaly baselines

---

## 4. Reconciliation Rules

### Check Types

| Check | Entity | Logic |
|-------|--------|-------|
| `fill_match` | fill | Internal fill ↔ broker fill by order_id, qty, price (±$0.01) |
| `position_match` | position | Internal open positions ↔ broker open positions by OSI + qty |
| `cash_match` | account | Internal cash ledger sum ↔ broker cash balance (±$1.00) |
| `expiry_match` | position | Expired positions ↔ broker settlement confirms |
| `assignment_match` | position | Assigned legs ↔ broker assignment notices |
| `freshness` | source | Each source within SLA (default 60 min for broker, 5 min for events) |

### Status Workflow

```
UNRESOLVED → AUTO_RESOLVED   (system matched on retry or after lag)
           → MANUALLY_RESOLVED (operator confirmed or corrected)
           → SUPPRESSED        (known benign, e.g. rounding, timing)
```

### Severity

| Level | Meaning | Action |
|-------|---------|--------|
| `CRITICAL` | Missing fills, unexplained cash delta > $100 | Report turns RED |
| `ERROR` | Position count mismatch, stale source > 2× SLA | Report turns YELLOW |
| `WARNING` | Minor price rounding, fill timing lag | Logged, no banner change |
| `INFO` | Expected differences (e.g. pending settlement) | Logged only |

---

## 5. Canonicalization Rules

Before idempotency hashing, normalize:

### Schwab
- Timestamps: convert ET → UTC
- Money: round to 2 decimal places
- Order IDs: string, not int
- OSI symbols: strip whitespace, uppercase
- Order status: map to canonical enum (FILLED, CANCELED, REJECTED, WORKING, EXPIRED)
- Legs: sort by (option_type ASC, strike ASC)

### TastyTrade
- Timestamps: already UTC from API
- Money: round to 2 decimal places
- Order IDs: string
- OSI symbols: map TT streamer format → OCC format
- Legs: sort by (option_type ASC, strike ASC)

---

## 6. Trust Propagation

Every entity carries its own trust status:

| Status | Meaning |
|--------|---------|
| `TRUSTED` | Reconciled against broker, no mismatches |
| `UNVERIFIED` | No broker data to compare yet |
| `STALE` | Source data is past SLA |
| `DISPUTED` | Active reconciliation mismatch |

Trust flows bottom-up:
- `positions` trust depends on fill_match and position_match
- `account_snapshots` trust depends on cash_match and freshness
- `strategy_daily` trust = min(positions trust, account trust)
- `portfolio_daily` trust = min(all strategy_daily trust)
- Report banner: GREEN (all TRUSTED/UNVERIFIED), YELLOW (any STALE), RED (any DISPUTED/CRITICAL)

---

## 7. Phased Implementation

| Phase | Deliverable | Files |
|-------|-------------|-------|
| 1 | Event writer + schema | `reporting/events.py`, `reporting/schema.sql` |
| 2 | DuckDB store + ingest | `reporting/db.py`, `reporting/ingest.py` |
| 3 | Position engine | `reporting/position_engine.py` |
| 4 | Daily report | `reporting/daily_report.py` |
| 5 | Schwab broker sync | `reporting/broker_sync_schwab.py` |
| 6 | Reconciliation engine | `reporting/reconciliation.py` |
| 7 | TT broker sync | `reporting/broker_sync_tt.py` |
| 8 | Account snapshots | `reporting/account_snapshots.py` |
| 9 | Anomaly / drift detection | `reporting/anomaly_engine.py` |
| 10 | AI narrative | `reporting/ai_narrative.py` |

### Current Status (Phase 1-4 complete)

- [x] Event schema and writer (`events.py`)
- [x] DuckDB schema (`schema.sql`)
- [x] Connection manager (`db.py`)
- [x] Event ingest/materializer (`ingest.py`)
- [x] Position lifecycle engine (`position_engine.py`)
- [x] Daily report generator (`daily_report.py`)
- [ ] Instrument BF/DS/CS orchestrators to emit events
- [ ] Schwab broker sync
- [ ] Reconciliation engine
- [ ] TT broker sync
- [ ] Account snapshots from broker
- [ ] Anomaly detection
- [ ] AI narrative layer

### Next Steps

1. **Instrument orchestrators** — Add `EventWriter` calls to ButterflyTuesday, DualSide, and ConstantStable orchestrators. This is Phase 1 delivery: real events flowing from real trades.
2. **Nightly materializer script** — `scripts/reporting/run_daily_portfolio_report.py` that ingests events, materializes positions, and generates the report.
3. **Schwab broker sync** — Pull orders/fills/positions/balances from Schwab API, store raw payloads, normalize into canonical tables.

---

## 8. Folder Structure

```
reporting/
├── __init__.py
├── SPEC.md                    # This file
├── events.py                  # Event schema + writer
├── schema.sql                 # DuckDB DDL
├── db.py                      # Connection manager
├── ingest.py                  # JSONL → DuckDB materializer
├── position_engine.py         # Position lifecycle state machine
├── daily_report.py            # Markdown report generator
├── broker_sync_schwab.py      # Phase 5 — Schwab API sync
├── broker_sync_tt.py          # Phase 7 — TastyTrade API sync
├── reconciliation.py          # Phase 6 — recon engine
├── account_snapshots.py       # Phase 8 — account NAV/cash
├── anomaly_engine.py          # Phase 9 — drift detection
├── ai_narrative.py            # Phase 10 — AI narrative
└── data/
    └── portfolio.duckdb       # Canonical store (gitignored)
```
