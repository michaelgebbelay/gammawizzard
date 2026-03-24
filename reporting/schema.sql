-- Gamma Portfolio Intelligence — Canonical DuckDB Schema
-- Version: 1.0
-- Convention: all timestamps UTC, all money in USD, all prices per-share (not per-contract)

-- =========================================================================
-- RAW EVENT LOG (Phase 1 — immutable, append-only)
-- =========================================================================

CREATE TABLE IF NOT EXISTS raw_events (
    event_id          VARCHAR PRIMARY KEY,
    event_type        VARCHAR NOT NULL,      -- strategy_run, trade_intent, order_submitted, order_update, fill, skip, error, post_step_result, position_close, correction, manual_adjustment
    ts_utc            TIMESTAMP NOT NULL,
    strategy          VARCHAR NOT NULL,      -- butterfly, constantstable, dualside
    account           VARCHAR NOT NULL,      -- schwab, tt-ira, tt-individual
    trade_group_id    VARCHAR NOT NULL,
    run_id            VARCHAR NOT NULL,
    config_version    VARCHAR NOT NULL,
    payload           JSON NOT NULL,
    idempotency_key   VARCHAR NOT NULL,
    ingested_at       TIMESTAMP DEFAULT current_timestamp
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_raw_events_idem
    ON raw_events (idempotency_key);

-- =========================================================================
-- STRATEGY RUNS (Phase 2 — materialized from raw_events)
-- =========================================================================

CREATE TABLE IF NOT EXISTS strategy_runs (
    run_id            VARCHAR PRIMARY KEY,
    strategy          VARCHAR NOT NULL,
    account           VARCHAR NOT NULL,
    trade_date        DATE NOT NULL,
    config_version    VARCHAR NOT NULL,
    signal            VARCHAR,               -- BUY, SELL, SKIP, PUT_CREDIT, CALL_DEBIT, etc.
    config            VARCHAR,               -- 4DTE_D20P_WINGS, 3DTE_D35P_WINGS, etc.
    reason            VARCHAR,
    spot              DOUBLE,
    vix               DOUBLE,
    vix1d             DOUBLE,
    filters           JSON,
    status            VARCHAR DEFAULT 'RUNNING',  -- RUNNING, COMPLETED, ERROR, SKIPPED
    started_at        TIMESTAMP,
    completed_at      TIMESTAMP,
    post_results      JSON
);

CREATE INDEX IF NOT EXISTS idx_strategy_runs_date
    ON strategy_runs (trade_date, strategy, account);

-- =========================================================================
-- INTENDED TRADES
-- =========================================================================

CREATE TABLE IF NOT EXISTS intended_trades (
    intent_id         VARCHAR PRIMARY KEY,   -- event_id of trade_intent event
    run_id            VARCHAR NOT NULL REFERENCES strategy_runs(run_id),
    trade_group_id    VARCHAR NOT NULL,
    strategy          VARCHAR NOT NULL,
    account           VARCHAR NOT NULL,
    trade_date        DATE NOT NULL,
    side              VARCHAR NOT NULL,      -- CREDIT, DEBIT
    direction         VARCHAR NOT NULL,      -- SHORT, LONG
    legs              JSON NOT NULL,         -- [{osi, strike, option_type, action, qty}, ...]
    target_qty        INTEGER NOT NULL,
    limit_price       DOUBLE,
    outcome           VARCHAR DEFAULT 'PENDING'  -- PENDING, FILLED, PARTIAL, REJECTED, CANCELED, TIMEOUT
);

-- =========================================================================
-- ORDER EVENTS (broker-facing)
-- =========================================================================

CREATE TABLE IF NOT EXISTS order_events (
    event_id          VARCHAR PRIMARY KEY,
    trade_group_id    VARCHAR NOT NULL,
    run_id            VARCHAR NOT NULL,
    order_id          VARCHAR NOT NULL,      -- broker order ID
    ts_utc            TIMESTAMP NOT NULL,
    event_type        VARCHAR NOT NULL,      -- submitted, working, partial_fill, filled, canceled, rejected, expired
    legs              JSON,
    limit_price       DOUBLE,
    filled_qty        INTEGER DEFAULT 0,
    remaining_qty     INTEGER DEFAULT 0,
    order_type        VARCHAR DEFAULT 'LIMIT'
);

CREATE INDEX IF NOT EXISTS idx_order_events_order
    ON order_events (order_id, ts_utc);

-- =========================================================================
-- FILLS (actual executions)
-- =========================================================================

CREATE TABLE IF NOT EXISTS fills (
    fill_id           VARCHAR PRIMARY KEY,   -- event_id of fill event
    trade_group_id    VARCHAR NOT NULL,
    run_id            VARCHAR NOT NULL,
    order_id          VARCHAR NOT NULL,
    ts_utc            TIMESTAMP NOT NULL,
    fill_qty          INTEGER NOT NULL,
    fill_price        DOUBLE NOT NULL,
    legs              JSON,                  -- per-leg fill details
    source            VARCHAR DEFAULT 'internal'  -- internal, broker_sync
);

CREATE INDEX IF NOT EXISTS idx_fills_order
    ON fills (order_id);

CREATE INDEX IF NOT EXISTS idx_fills_group
    ON fills (trade_group_id);

-- =========================================================================
-- POSITIONS (lifecycle-tracked)
-- =========================================================================

-- Lifecycle states:
--   INTENDED → PARTIALLY_OPEN → OPEN → PARTIALLY_CLOSED → CLOSED
--                                    → EXPIRED
--                                    → ASSIGNED
--                                    → BROKEN (engine cannot resolve)

CREATE TABLE IF NOT EXISTS positions (
    position_id       VARCHAR PRIMARY KEY,   -- = trade_group_id (one position per trade group)
    strategy          VARCHAR NOT NULL,
    account           VARCHAR NOT NULL,
    trade_date        DATE NOT NULL,
    expiry_date       DATE,
    lifecycle_state   VARCHAR NOT NULL DEFAULT 'INTENDED',
    provenance        VARCHAR NOT NULL DEFAULT 'STRATEGY',  -- STRATEGY, MANUAL, CORRECTION
    closure_reason    VARCHAR,               -- TARGET, STOP, EXPIRY, ASSIGNMENT, ROLL, MANUAL, UNKNOWN
    entry_price       DOUBLE,                -- net credit/debit per spread
    exit_price        DOUBLE,
    qty               INTEGER DEFAULT 0,
    max_loss          DOUBLE,                -- theoretical max loss
    realized_pnl      DOUBLE,
    config            VARCHAR,
    signal            VARCHAR,
    opened_at         TIMESTAMP,
    closed_at         TIMESTAMP,
    updated_at        TIMESTAMP DEFAULT current_timestamp,
    trust_status      VARCHAR DEFAULT 'UNVERIFIED',  -- TRUSTED, UNVERIFIED, STALE, DISPUTED
    trust_reasons     JSON
);

CREATE INDEX IF NOT EXISTS idx_positions_state
    ON positions (lifecycle_state, strategy, account);

CREATE INDEX IF NOT EXISTS idx_positions_date
    ON positions (trade_date, strategy);

-- =========================================================================
-- POSITION LEGS
-- =========================================================================

CREATE TABLE IF NOT EXISTS position_legs (
    leg_id            VARCHAR PRIMARY KEY,
    position_id       VARCHAR NOT NULL,       -- references positions(position_id)
    osi               VARCHAR NOT NULL,      -- OCC option symbol
    option_type       VARCHAR NOT NULL,      -- CALL, PUT
    strike            DOUBLE NOT NULL,
    expiry_date       DATE,                  -- nullable: derived from OSI when available
    action            VARCHAR NOT NULL,      -- BUY_TO_OPEN, SELL_TO_OPEN, BUY_TO_CLOSE, SELL_TO_CLOSE
    qty               INTEGER NOT NULL,
    fill_price        DOUBLE
);

CREATE INDEX IF NOT EXISTS idx_position_legs_pos
    ON position_legs (position_id);

-- =========================================================================
-- POSITION RELATIONSHIPS (rolls, adjustments, splits)
-- =========================================================================

CREATE TABLE IF NOT EXISTS position_relationships (
    id                VARCHAR PRIMARY KEY,
    from_position_id  VARCHAR NOT NULL,       -- references positions(position_id)
    to_position_id    VARCHAR NOT NULL,       -- references positions(position_id)
    relationship_type VARCHAR NOT NULL,      -- ROLLED_FROM, ROLLED_TO, ADJUSTED_FROM, SPLIT_FROM, MERGED_INTO
    created_at        TIMESTAMP DEFAULT current_timestamp
);

-- =========================================================================
-- BROKER RAW DATA (Phase 3 — stored unchanged)
-- =========================================================================

CREATE TABLE IF NOT EXISTS broker_raw_orders (
    id                VARCHAR PRIMARY KEY,
    broker            VARCHAR NOT NULL,      -- schwab, tastytrade
    account           VARCHAR NOT NULL,
    order_id          VARCHAR NOT NULL,
    fetched_at        TIMESTAMP NOT NULL,
    as_of             TIMESTAMP,
    raw_payload       JSON NOT NULL,
    idempotency_key   VARCHAR NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_broker_raw_orders_idem
    ON broker_raw_orders (idempotency_key);

CREATE TABLE IF NOT EXISTS broker_raw_fills (
    id                VARCHAR PRIMARY KEY,
    broker            VARCHAR NOT NULL,
    account           VARCHAR NOT NULL,
    order_id          VARCHAR NOT NULL,
    fill_id           VARCHAR,               -- broker's fill ID if available
    fetched_at        TIMESTAMP NOT NULL,
    as_of             TIMESTAMP,
    raw_payload       JSON NOT NULL,
    idempotency_key   VARCHAR NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_broker_raw_fills_idem
    ON broker_raw_fills (idempotency_key);

CREATE TABLE IF NOT EXISTS broker_raw_positions (
    id                VARCHAR PRIMARY KEY,
    broker            VARCHAR NOT NULL,
    account           VARCHAR NOT NULL,
    fetched_at        TIMESTAMP NOT NULL,
    as_of             TIMESTAMP,
    raw_payload       JSON NOT NULL
);

CREATE TABLE IF NOT EXISTS broker_raw_cash (
    id                VARCHAR PRIMARY KEY,
    broker            VARCHAR NOT NULL,
    account           VARCHAR NOT NULL,
    fetched_at        TIMESTAMP NOT NULL,
    as_of             TIMESTAMP,
    raw_payload       JSON NOT NULL
);

-- =========================================================================
-- ACCOUNT SNAPSHOTS (Phase 4)
-- =========================================================================

CREATE TABLE IF NOT EXISTS account_snapshots (
    id                VARCHAR PRIMARY KEY,
    account           VARCHAR NOT NULL,
    snapshot_date     DATE NOT NULL,
    as_of             TIMESTAMP NOT NULL,
    cash              DOUBLE,
    net_liq           DOUBLE,
    buying_power      DOUBLE,
    open_positions    INTEGER,
    realized_pnl_day  DOUBLE,
    unrealized_pnl    DOUBLE,
    transfers_day     DOUBLE,
    fees_day          DOUBLE,
    source            VARCHAR,               -- broker_sync, edge_guard, manual
    trust_status      VARCHAR DEFAULT 'UNVERIFIED',
    trust_reasons     JSON
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_account_snapshots_date
    ON account_snapshots (account, snapshot_date);

-- =========================================================================
-- POSITION SNAPSHOTS (daily marks)
-- =========================================================================

CREATE TABLE IF NOT EXISTS position_snapshots (
    id                VARCHAR PRIMARY KEY,
    position_id       VARCHAR NOT NULL,       -- references positions(position_id)
    snapshot_date     DATE NOT NULL,
    mark_price        DOUBLE,
    dte               INTEGER,
    unrealized_pnl    DOUBLE,
    max_loss_estimate DOUBLE,
    assignment_risk   VARCHAR,               -- NONE, LOW, MEDIUM, HIGH
    trust_status      VARCHAR DEFAULT 'UNVERIFIED'
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_position_snapshots_date
    ON position_snapshots (position_id, snapshot_date);

-- =========================================================================
-- CASH EVENTS LEDGER (Phase 4)
-- =========================================================================

CREATE TABLE IF NOT EXISTS cash_events (
    id                VARCHAR PRIMARY KEY,
    account           VARCHAR NOT NULL,
    event_date        DATE NOT NULL,
    ts_utc            TIMESTAMP,
    event_type        VARCHAR NOT NULL,      -- trade, fee, commission, interest, transfer, dividend, assignment, expiry
    amount            DOUBLE NOT NULL,       -- positive = inflow, negative = outflow
    description       VARCHAR,
    position_id       VARCHAR,               -- link to position if trade-related
    source            VARCHAR,               -- broker_sync, manual
    idempotency_key   VARCHAR NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_cash_events_idem
    ON cash_events (idempotency_key);

-- =========================================================================
-- RECONCILIATION (Phase 5)
-- =========================================================================

CREATE TABLE IF NOT EXISTS reconciliation_runs (
    id                VARCHAR PRIMARY KEY,
    run_date          DATE NOT NULL,
    started_at        TIMESTAMP NOT NULL,
    completed_at      TIMESTAMP,
    checks_run        INTEGER DEFAULT 0,
    issues_found      INTEGER DEFAULT 0,
    auto_resolved     INTEGER DEFAULT 0,
    status            VARCHAR DEFAULT 'RUNNING'  -- RUNNING, COMPLETED, FAILED
);

CREATE TABLE IF NOT EXISTS reconciliation_items (
    id                VARCHAR PRIMARY KEY,
    recon_run_id      VARCHAR NOT NULL REFERENCES reconciliation_runs(id),
    check_type        VARCHAR NOT NULL,      -- fill_match, position_match, cash_match, expiry_match, freshness
    entity_type       VARCHAR NOT NULL,      -- order, fill, position, account, cash_event
    entity_id         VARCHAR,
    severity          VARCHAR NOT NULL,      -- INFO, WARNING, ERROR, CRITICAL
    status            VARCHAR NOT NULL DEFAULT 'UNRESOLVED',  -- UNRESOLVED, AUTO_RESOLVED, MANUALLY_RESOLVED, SUPPRESSED
    message           VARCHAR NOT NULL,
    internal_value    VARCHAR,
    broker_value      VARCHAR,
    opened_at         TIMESTAMP DEFAULT current_timestamp,
    resolved_at       TIMESTAMP,
    resolution_type   VARCHAR,               -- auto_match, manual_override, correction_event
    resolution_event_id VARCHAR,
    suppression_reason VARCHAR,
    classification      VARCHAR,            -- likely_discretionary, unexpected, NULL for non-position checks
    classification_reason VARCHAR            -- e.g. OUTSIDE_TRIGGER_WINDOW(time=14:32ET,weekday=2)
);

CREATE INDEX IF NOT EXISTS idx_recon_items_status
    ON reconciliation_items (status, severity);

-- =========================================================================
-- SOURCE FRESHNESS (Phase 5)
-- =========================================================================

CREATE TABLE IF NOT EXISTS source_freshness (
    source_name       VARCHAR PRIMARY KEY,   -- schwab_orders, schwab_positions, tt_orders, etc.
    last_success_at   TIMESTAMP,
    source_asof       TIMESTAMP,
    sla_minutes       INTEGER NOT NULL DEFAULT 60,
    is_stale          BOOLEAN DEFAULT false,
    error_message     VARCHAR
);

-- =========================================================================
-- STRATEGY CONFIG VERSIONS (Phase 5)
-- =========================================================================

CREATE TABLE IF NOT EXISTS strategy_config_versions (
    id                VARCHAR PRIMARY KEY,
    strategy          VARCHAR NOT NULL,
    config_hash       VARCHAR NOT NULL,
    config_data       JSON NOT NULL,
    active_from       TIMESTAMP NOT NULL,
    active_to         TIMESTAMP,
    deploy_sha        VARCHAR
);

-- =========================================================================
-- MATERIALIZED VIEWS (Phase 6)
-- =========================================================================

-- Strategy daily scorecard
CREATE TABLE IF NOT EXISTS strategy_daily (
    id                VARCHAR PRIMARY KEY,
    strategy          VARCHAR NOT NULL,
    account           VARCHAR NOT NULL,
    report_date       DATE NOT NULL,
    trades_opened     INTEGER DEFAULT 0,
    trades_closed     INTEGER DEFAULT 0,
    trades_skipped    INTEGER DEFAULT 0,
    realized_pnl      DOUBLE DEFAULT 0,
    unrealized_pnl    DOUBLE DEFAULT 0,
    win_rate_20d      DOUBLE,
    expectancy_20d    DOUBLE,
    avg_credit        DOUBLE,
    avg_fill_quality  DOUBLE,                -- fill vs NBBO mid
    trust_status      VARCHAR DEFAULT 'UNVERIFIED',
    trust_reasons     JSON,
    reconciliation_status VARCHAR DEFAULT 'UNCHECKED'
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_strategy_daily_date
    ON strategy_daily (report_date, strategy, account);

-- Strategy signal rows (e.g. Leo / GW daily trade rows)
CREATE TABLE IF NOT EXISTS strategy_signal_rows (
    signal_id         VARCHAR PRIMARY KEY,
    strategy          VARCHAR NOT NULL,
    source            VARCHAR NOT NULL,      -- leo_csv, gw_api, manual
    trade_date        DATE NOT NULL,
    expiry_date       DATE,
    spot              DOUBLE,
    forward           DOUBLE,
    vix               DOUBLE,
    vix_one           DOUBLE,
    put_strike        DOUBLE,
    call_strike       DOUBLE,
    put_price         DOUBLE,
    call_price        DOUBLE,
    left_go           DOUBLE,
    right_go          DOUBLE,
    l_imp             DOUBLE,
    r_imp             DOUBLE,
    l_return          DOUBLE,
    r_return          DOUBLE,
    fp                DOUBLE,
    rv                DOUBLE,
    rv5               DOUBLE,
    rv10              DOUBLE,
    rv20              DOUBLE,
    r                 DOUBLE,
    ar                DOUBLE,
    ar2               DOUBLE,
    ar3               DOUBLE,
    tx                DOUBLE,
    year_num          INTEGER,
    month_num         INTEGER,
    raw_payload       JSON,
    ingested_at       TIMESTAMP DEFAULT current_timestamp
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_strategy_signal_rows_key
    ON strategy_signal_rows (strategy, source, trade_date, expiry_date);

-- Portfolio daily rollup
CREATE TABLE IF NOT EXISTS portfolio_daily (
    id                VARCHAR PRIMARY KEY,
    report_date       DATE NOT NULL,
    total_cash        DOUBLE,
    total_net_liq     DOUBLE,
    total_buying_power DOUBLE,
    total_open_positions INTEGER,
    realized_pnl_day  DOUBLE,
    unrealized_pnl    DOUBLE,
    drawdown_pct      DOUBLE,
    drawdown_from_peak DOUBLE,
    peak_net_liq      DOUBLE,
    trust_status      VARCHAR DEFAULT 'UNVERIFIED',
    trust_reasons     JSON,
    unresolved_issues INTEGER DEFAULT 0
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_portfolio_daily_date
    ON portfolio_daily (report_date);

-- Daily diary
CREATE TABLE IF NOT EXISTS daily_diary (
    id                VARCHAR PRIMARY KEY,
    report_date       DATE NOT NULL,
    generated_at      TIMESTAMP DEFAULT current_timestamp,
    narrative_md      TEXT,                  -- human-readable markdown
    facts             JSON,                  -- machine-readable structured facts
    ai_generated      BOOLEAN DEFAULT false,
    trust_status      VARCHAR
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_daily_diary_date
    ON daily_diary (report_date);

-- Daily report outputs
-- =========================================================================
-- STRATEGY TRIGGER WINDOWS (Phase 5 — reconciliation inference)
-- =========================================================================
-- Maps each strategy/account to its allowed execution windows in US/Eastern.
-- Used by reconciliation to classify broker-only positions as likely
-- discretionary when their first fill falls outside all allowed windows.

CREATE TABLE IF NOT EXISTS strategy_trigger_windows (
    id                VARCHAR PRIMARY KEY,
    strategy          VARCHAR NOT NULL,
    account           VARCHAR NOT NULL,
    weekday           INTEGER NOT NULL,       -- 0=Mon, 1=Tue, ..., 4=Fri
    start_et          VARCHAR NOT NULL,       -- HH:MM in US/Eastern, e.g. '16:10'
    end_et            VARCHAR NOT NULL,       -- HH:MM in US/Eastern, e.g. '16:30'
    rule_name         VARCHAR NOT NULL        -- e.g. 'bf_daily_trigger', 'cs_daily_trigger'
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_trigger_windows_lookup
    ON strategy_trigger_windows (strategy, account, weekday, rule_name);

-- Daily report outputs
CREATE TABLE IF NOT EXISTS daily_report_outputs (
    id                VARCHAR PRIMARY KEY,
    report_date       DATE NOT NULL,
    format            VARCHAR NOT NULL,      -- markdown, sheets, email
    content           TEXT,
    generated_at      TIMESTAMP DEFAULT current_timestamp,
    trust_banner      VARCHAR               -- GREEN, YELLOW, RED
);
