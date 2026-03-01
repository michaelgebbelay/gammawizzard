# Live SPX Risk-Defined Game Spec

This document is the canonical specification for the `sim_gpt` game in:

- `/Users/mgebremichael/Documents/Gamma/sim_gpt`

It reflects the current implemented behavior.

## 1. Purpose

Run a daily, post-close, 1DTE SPX options competition where multiple players submit one risk-defined trade decision and are ranked by risk-adjusted performance.

Primary objective:

- maximize cumulative P/L
- minimize drawdown

## 2. Scope and Start Date

- Game type: live daily simulation on Leo feed rows.
- Live start guard date: `2026-03-02` (Monday).
- One decision cycle per signal day.
- No intraday adjustments.

## 3. Daily Timeline

1. Signal day (`Date`): players decide after close (target around 4:13 PM ET).
2. Expiry day (`TDate`): round is settled on/after close when outcomes are available.

Runtime behavior:

- `run-live` for a date first settles older due rounds, then creates the current round decisions.
- `settle` settles all pending rounds with `tdate <= settlement_date`.
- If feed has no row for the requested `Date`, `run-live` is a no-op (skip, no round created).

## 4. Data Contract

Source options:

- CSV (`--csv`)
- API (`--api-url` or `LEO_LIVE_URL`; default endpoint is `https://gandalf.gammawizard.com/rapi/GetUltraPureConstantStable`)

Required date fields:

- `Date`
- `TDate`

Row validity rules:

- there must be exactly one row for the requested `Date`
- `TDate` must be strictly greater than `Date`
- settlement requires settlement fields (`Profit`, `CProfit`) to be present

Outcome fields used for settlement:

- `Profit` (put-side 5-wide short baseline)
- `CProfit` (call-side 5-wide short baseline)
- `TX` (stored but not used in decision scoring math)

## 5. Information Leakage Controls

Players only receive public fields:

- `Date`, `TDate`, `SPX`, `VIX`, `VixOne`, `RV`, `RV5`, `RV10`, `RV20`, `R`, `RX`, `Forward`, `Limit`, `CLimit`

Suppressed from players:

- `LeftGo`, `RightGo`, `LGo`, `RGo`, `LReturn`, `RReturn`, `Cat`, `Cat1`, `Cat2`, `TX`, `Win`, `CWin`, `Profit`, `CProfit`, `Put`, `Call`

## 6. Players

Active players:

- `player-regime`
- `player-momentum`
- `player-volatility`
- `player-contrarian`
- `player-vixone-bias`

Each player chooses from a template set and learns online by context bucket (`vix_bucket|trend_bucket`) using historical average P/L per template.

## 7. Trade Universe and Allowed Actions

Per side (put and call), actions are:

- `buy`
- `sell`
- `none`

Allowed width:

- `5`
- `10`

Allowed size:

- integer `1..3`

Risk-defined rule:

- Trades must be packaged as defined-width vertical structures (or no-trade).
- Naked option buying/selling is not allowed.

Current implementation note:

- Butterfly is not implemented in settlement/decision templates yet.
- To support butterfly, Leo feed needs additional leg-level pricing/outcome fields.

## 8. Account and Risk Constraints

Per-player account settings:

- starting balance: `$30,000`
- hard cap: `30%` of current account value per round
- safety buffer: `90%` applied to cap
- commission: `$1` per executed option leg

Effective per-round risk budget formula:

- `risk_budget = account_value * 0.30 * 0.90` (effective 27% of account value)

Where:

- `account_value = max(0, 30000 + cumulative_equity_pnl)`

Max loss estimation (before entry):

- put side max loss = `put_width * risk_per_width_dollars * size` when put action is not `none`
- call side max loss = `call_width * risk_per_width_dollars * size` when call action is not `none`
- total `max_loss = put_max_loss + call_max_loss`

Template rule source of truth:

- each template has explicit metadata (`risk_per_width_dollars`, `pnl_scale_per_width`)
- risk guard and settlement P/L both reference this same template metadata

Risk guard behavior:

1. If `max_loss <= risk_budget`, trade is accepted.
2. If `max_loss > risk_budget`, size is clamped down to the highest valid size.
3. If no size can fit budget, trade is flattened to no-trade (`risk_guard_flat`).

Risk metadata stored with every decision:

- `account_value`
- `risk_budget`
- `max_loss`
- `risk_used_pct`
- `max_risk_pct`
- `risk_buffer_pct`
- `risk_guard`

## 9. P/L Model

Assumptions:

- `Profit` and `CProfit` represent points for 5-wide short vertical baselines.
- SPX multiplier is `100`.

Per-side P/L:

- `none`: `0`
- `sell`: `+ base_short_pnl_5w * (width * pnl_scale_per_width) * size * 100`
- `buy`: sign-flipped sell P/L

Round total:

- `gross_total_pnl = put_gross + call_gross`
- `fees = executed_legs * $1` (2 legs per active vertical side, scaled by size)
- `total_pnl = gross_total_pnl - fees`

## 10. Judge and Ranking

Judge is objective and style-agnostic.

Tracked risk metrics:

- `equity_pnl`
- `current_drawdown`
- `max_drawdown`
- `risk_adjusted = equity_pnl - 0.60 * max_drawdown`

Per-round judge score (`0..10`):

- `base = 5 + (risk_adjusted / 250)`
- `dd_penalty = min(3, current_drawdown / 200)`
- `round_term = clamp(total_pnl / 200, -1.5, +1.5)`
- `judge_score = clamp(base - dd_penalty + round_term, 0, 10)`

Leaderboard sort order:

1. highest `risk_adjusted`
2. highest `total_pnl`
3. lowest `max_drawdown`

## 11. One-Trade-Per-Day Semantics

- The system records one round per `signal_date` (`rounds.signal_date` is primary key).
- A rerun on the same day does not create a second position row; it updates/replaces the same day decision record.
- Operationally this is still one daily decision set, held to expiry.

## 11A. Session Clock and Stale Guards

- Market timezone is explicit: `America/New_York`.
- For same-day live rounds, execution is blocked until `16:00 ET + delay` (default `+13 minutes`).
- No exchange-calendar gating is used; feed row availability is the only session source-of-truth.
- If `asof` is present from API feed, it must be same-day, post-close, and within max staleness window.
- Same-day decision runs reject rows that already contain settlement fields (`Profit`, `CProfit`).

## 12. Storage Model (SQLite)

Database default:

- `/Users/mgebremichael/Documents/Gamma/sim_gpt/data/live_game.db`

Tables:

- `rounds`
- `decisions`
- `results`
- `player_state`

`results` includes risk analytics fields:

- `equity_pnl`, `drawdown`, `max_drawdown`, `risk_adjusted`

`rounds` includes UTC clock fields:

- `signal_timestamp_utc`
- `settlement_timestamp_utc`

## 13. Google Sheets Output

Sheet ID default:

- `1aBRPbPhYO39YvxrHVdDzo9nxrABRpHxaRk-FE5hXypc`

Tabs:

- `Live_Game_Rounds`
- `Live_Game_Leaderboard`

`Live_Game_Rounds` includes:

- decision attributes (`put/call action`, widths, size, template)
- risk metadata (`account_value`, `risk_budget`, `max_loss`, `risk_used_pct`, `risk_guard`)
- outcomes (`put_pnl`, `call_pnl`, `total_pnl`, `equity_pnl`, `drawdown`, `max_drawdown`, `risk_adjusted`, `judge_score`)
- integrity metadata (`round_id`, `decision_checksum`)

## 14. Operations

Core commands:

- `python3 -m sim_gpt.cli run-live --date YYYY-MM-DD [--csv ...|--api-url ...]`
- `python3 -m sim_gpt.cli settle --date YYYY-MM-DD [--csv ...|--api-url ...]`
- `python3 -m sim_gpt.cli settle --date YYYY-MM-DD --push-sheet ...`
- `python3 -m sim_gpt.cli leaderboard`
- `python3 -m sim_gpt.cli round --date YYYY-MM-DD`
- `python3 -m sim_gpt.cli sync-sheet`

GitHub Actions workflow:

- `/Users/mgebremichael/Documents/Gamma/.github/workflows/live_game_sync.yml`

Supported workflow modes:

- `run_live_and_settle`
- `settle_only`
- `sync_only`

## 15. Required Secrets/Env

For sheet sync:

- `GOOGLE_SERVICE_ACCOUNT_JSON` or `GOOGLE_SERVICE_ACCOUNT_FILE` or `GOOGLE_APPLICATION_CREDENTIALS`
- `GSHEET_ID` (optional if using default)

For API feed:

- `LEO_LIVE_URL` (optional override)
- `LEO_LIVE_TOKEN` (optional bearer token)
- `GW_EMAIL` + `GW_PASSWORD` (optional fallback auth when bearer token is not provided)

## 16. Non-Goals and Known Gaps

- No butterfly payoff support yet.
- No slippage model yet (commission is modeled).
- No explicit daily kill switch yet (risk cap is per round).
- No exchange holiday/early-close calendar logic by design (feed controls run/no-run).

## 17. Recommended Next Buffers

1. Add slippage haircut in max-loss and realized P/L accounting.
2. Add daily stop rule (skip new round after configurable daily equity hit).
3. Add stale-data guard (reject snapshots not matching expected post-close freshness).
4. Add equity floor mode (force one-side-only or flat under account stress).
