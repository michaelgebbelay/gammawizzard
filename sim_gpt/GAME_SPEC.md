# Live SPX Risk-Defined Game Spec (`sim_gpt`)

Canonical spec for:

- `/Users/mgebremichael/Documents/Gamma/sim_gpt`

## 1. Objective

Daily post-close 1DTE SPX competition with objective scoring:

- maximize cumulative P/L
- minimize drawdown

Leaderboard is sorted by:

1. highest `total_pnl`
2. lowest `max_drawdown` (tie-break)
3. highest `win_rate` (tie-break)

`risk_adjusted = equity_pnl - 0.60 * max_drawdown` is kept as a descriptive metric only.

## 2. Timeline and Session Semantics

- One round per `Date` row.
- Decision run is after close (`16:00 ET + delay`, default 13 min).
- Expiry/settlement is `TDate` close.
- `run-live` always settles due rounds first, then opens the new round.
- Re-runs are idempotent at round level (`rounds.signal_date` is unique).

Live start guard:

- `2026-03-02`

## 3. Feed Truth Rules

Feed is source-of-truth for "should we trade today".

Round creation requires:

- exactly one feed row for `Date=YYYY-MM-DD`
- valid `TDate > Date`

Settlement requires:

- exactly one feed row for `Date=TDate`
- usable `SPX` settlement value on that `TDate` row
- settle command date `>= TDate`

No exchange-holiday/early-close calendar logic is used. If feed has no row, nothing happens.

## 4. Data Contract (No Leo Payoff Dependency)

### 4.1 Public feature row (from ConstantStable feed)

Decision-time public fields exposed to players:

- `Date`, `TDate`, `SPX`, `VIX`, `VixOne`, `RV`, `RV5`, `RV10`, `RV20`, `R`, `RX`, `Forward`

Suppressed fields are never sent to players:

- `Limit`, `CLimit`, `Put`, `Call`, `LImp`, `RImp`, `LeftGo`, `RightGo`, `LGo`, `RGo`,
  `LReturn`, `RReturn`, `Cat`, `Cat1`, `Cat2`, `TX`, `Win`, `CWin`, `Profit`, `CProfit`

### 4.2 Entry pricing input (real chain)

At decision time, engine fetches SPX option chain snapshot from Schwab for expiry `TDate`:

- strikes
- bid/ask/mark(mid proxy)
- delta
- iv
- chain as-of timestamp

### 4.3 Settlement input

Settlement uses `SPX` from feed row on `TDate` as cash-settlement underlying value.

## 5. Allowed Trade Space

Risk-defined structures only (current implementation):

- put vertical side: `buy` or `sell` or `none`
- call vertical side: `buy` or `sell` or `none`
- widths: `5` or `10`
- target delta per active side: `0.10`, `0.16`, `0.25`
- size: integer `1..3`

Butterflies are not implemented in `sim_gpt` yet.

## 6. Strike Selection (Deterministic)

For each active side:

1. choose anchor strike by nearest target delta on that option type (`put`/`call`)
2. compute wing target by requested width
3. snap to nearest available strike in required direction (down for put wings, up for call wings)

Directional templates:

- put `sell`: short higher put, long lower put
- put `buy`: long higher put, short lower put
- call `sell`: short lower call, long higher call
- call `buy`: long lower call, short higher call

## 7. Entry Fill Model

Fill model: conservative mid (`conservative_mid`)

For each vertical:

- compute spread bid/ask from leg bid/ask
- mid = `(bid + ask) / 2`
- half_spread = `(ask - bid) / 2`
- credit fill = `mid - half_spread * fill_half_spread_factor`
- debit fill = `mid + half_spread * fill_half_spread_factor`

Default factor:

- `fill_half_spread_factor = 0.50`

## 8. Settlement P/L Model (Intrinsic)

At expiry settlement `S = SPX(TDate)`:

- call intrinsic = `max(0, S - K)`
- put intrinsic = `max(0, K - S)`

Per vertical side:

- short spread P/L points = `entry_credit - intrinsic_spread_value`
- long spread P/L points = `intrinsic_spread_value - entry_debit`

Dollar conversion:

- `P/L($) = P/L(points) * 100 * size`

Commission:

- `$1` per leg
- one vertical side has 2 legs
- fees are subtracted from realized P/L

## 9. Risk Model

Starting account per player:

- `$30,000`

Risk budget per round:

- `account_value * 0.30 * 0.90` (effective 27%)

`account_value`:

- `max(0, 30000 + cumulative_equity_pnl)`

Max-loss computation is chain-priced:

- derive structure entry cashflow from fills
- evaluate payoff envelope over strike/boundary knots
- compute exact worst-case loss per 1x (`unit_max_loss`)
- scale by size and include commissions

Guard behavior:

1. if max loss <= budget: accept
2. else clamp size down to largest valid size
3. if nothing fits: flatten to no-trade (`risk_guard_flat`)

## 10. Participation Control

Soft participation objective:

- players should trade at least 90% of sessions

Enforcement mechanism:

- trade-rate and hold-streak context are injected into decisions
- no hard trade block

## 11. Performance Metrics

Tracked metrics:

- `equity_pnl`
- `current_drawdown`
- `max_drawdown`
- `risk_adjusted`

## 12. Storage Model (SQLite)

Default DB:

- `/Users/mgebremichael/Documents/Gamma/sim_gpt/data/live_game.db`

Tables:

- `rounds`
- `decisions`
- `results`
- `player_state`

Important persisted fields:

- UTC round timestamps:
  - `signal_timestamp_utc`
  - `settlement_timestamp_utc`
- decision pricing metadata:
  - target deltas
  - selected strikes
  - entry prices
  - chain as-of
  - `unit_max_loss`

## 13. Google Sheets Output

Default sheet:

- `1aBRPbPhYO39YvxrHVdDzo9nxrABRpHxaRk-FE5hXypc`

Tabs:

- `Live_Game_Rounds`
- `Live_Game_Leaderboard`
- `Live_Game_Decisions`

Sheets include:

- decision settings (actions, widths, target deltas, size, template)
- resolved strikes and entry prices
- risk metadata and guard reason
- realized outcomes and risk-adjusted metrics
- integrity columns (`round_id`, checksums)

## 14. Operations

Core commands:

- `python3 -m sim_gpt.cli run-live --date YYYY-MM-DD [--csv ...|--api-url ...]`
- `python3 -m sim_gpt.cli settle --date YYYY-MM-DD [--csv ...|--api-url ...]`
- `python3 -m sim_gpt.cli leaderboard`
- `python3 -m sim_gpt.cli round --date YYYY-MM-DD`
- `python3 -m sim_gpt.cli sync-sheet`

## 15. Required Environment

For feed API access (if not using CSV):

- `LEO_LIVE_URL` (optional override)
- `LEO_LIVE_TOKEN` or `GW_EMAIL` + `GW_PASSWORD`

For Schwab chain pricing:

- `SCHWAB_APP_KEY`
- `SCHWAB_APP_SECRET`
- `SCHWAB_TOKEN_JSON` or `SCHWAB_TOKEN_PATH`

For sheet sync:

- `GOOGLE_SERVICE_ACCOUNT_JSON` (or file-based equivalent)
- `GSHEET_ID` (optional; defaults baked in)

## 16. Current Gaps

- butterfly template/settlement support not implemented
- no intraday exits (hold-to-expiry only)
- no mark-to-market equity curve between entry and settlement
