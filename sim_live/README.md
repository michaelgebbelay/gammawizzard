# Live Binary Vertical Game

Separate game engine for live Leo rounds (independent of `sim/`).

## Core Rules

- Decision time: `Date` at ~4:13 PM ET.
- Expiry/settlement: `TDate` close.
- Market timezone: `America/New_York` (explicit session clock).
- Each player can choose per side:
  - `buy`, `sell`, or `none` for put side
  - `buy`, `sell`, or `none` for call side
- Allowed widths: `5` or `10` (risk-defined verticals only).
- No intraday adjustments; one decision per round.
- Active players: regime, momentum, volatility-spread, contrarian, and `VIXone-bias`.
- Starting account per player: `$30,000`.
- Risk limit per round: `30%` of account value, with a `90%` safety buffer on that cap
  (`effective trade budget = 27%` of account value).

## Session/Data Guards

- Skip non-trading days (weekends + built-in 2026 US market holidays).
- Respect early-close sessions for post-close entry timing.
- Require exactly one source row for each signal date.
- Require `TDate > Date`.
- If an `asof` timestamp exists, require same-day post-close and non-stale freshness.
- Decision-time runs reject rows with `Profit/CProfit` already populated for same-day live rounds.

## Safety / Leakage Control

Players only receive a public feature view. Signal/outcome columns are suppressed:

- `LeftGo`, `RightGo`, `Cat*`, `TX`, `Win`, `CWin`, `Profit`, `CProfit`, etc.

Engine/judge can use private outcome fields only during settlement.

## Start Date Guard

Live rounds are blocked before:

- `2026-03-02` (Monday)

Use `--allow-prestart` only for local validation.

## CLI

```bash
cd /Users/mgebremichael/Documents/Gamma

# Create today's round (and auto-settle any due rounds first)
python3 -m sim_live.cli run-live --csv /Users/mgebremichael/Downloads/leo_profit_Dec25.csv

# Validate prestart behavior using historical date
python3 -m sim_live.cli run-live \
  --date 2025-12-23 \
  --csv /Users/mgebremichael/Downloads/leo_profit_Dec25.csv \
  --allow-prestart

# Settle all due rounds as of a date
python3 -m sim_live.cli settle \
  --date 2025-12-24 \
  --csv /Users/mgebremichael/Downloads/leo_profit_Dec25.csv

# Leaderboard
python3 -m sim_live.cli leaderboard

# Round detail
python3 -m sim_live.cli round --date 2025-12-23

# Push results to Google Sheets (defaults to your shared sheet ID)
python3 -m sim_live.cli sync-sheet

# Or settle + push in one step
python3 -m sim_live.cli settle \
  --csv /Users/mgebremichael/Downloads/leo_profit_Dec25.csv \
  --push-sheet
```

## Notes

- The settlement model assumes `Profit` and `CProfit` are side-level 5-wide short-vertical P/L.
- Buy-side P/L is modeled as the sign-flipped side P/L.
- 10-wide is modeled with a `2x` width multiplier from 5-wide outcomes.
- Butterfly support can be added, but needs additional leg-level pricing fields from the Leo feed.
- Round storage includes UTC timestamps:
  - `signal_timestamp_utc`
  - `settlement_timestamp_utc`
- Judge and leaderboard are objective risk/reward:
  - maximize cumulative P/L
  - minimize drawdown
  - rank by risk-adjusted score (`equity - 0.60 * max_drawdown`)
- Sheets results include integrity fields:
  - `round_id` (`signal_date|player_id`)
  - `decision_checksum`
- Google Sheets export auth can use:
  - `GOOGLE_SERVICE_ACCOUNT_JSON` (raw JSON or base64)
  - or `GOOGLE_SERVICE_ACCOUNT_FILE`
  - or `GOOGLE_APPLICATION_CREDENTIALS`

## GitHub Secrets Path

If your secrets are only in GitHub, use workflow:

- [live_game_sync.yml](/Users/mgebremichael/Documents/Gamma/.github/workflows/live_game_sync.yml)

Expected secrets:

- `GSHEET_ID`
- `GOOGLE_SERVICE_ACCOUNT_JSON`
- `LEO_LIVE_URL` (optional but recommended)
- `LEO_LIVE_TOKEN` (optional)

From GitHub Actions UI:

1. Run **Live Game Sync**
2. `mode=run_live_and_settle`
3. leave dates blank for "today"
