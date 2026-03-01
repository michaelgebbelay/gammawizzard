# sim_gpt (Live SPX Risk-Defined Game)

Isolated live game engine under:

- `/Users/mgebremichael/Documents/Gamma/sim_gpt`

This game is separate from `sim/` and uses:

- ConstantStable feed rows for session truth + public features
- Schwab SPX option chain for entry pricing
- `SPX` on `TDate` row for cash settlement

No `Profit/CProfit` payoff dependency.

## Core Rules

- One decision set per signal day.
- Post-close run window in `America/New_York` (default guard: 16:13 ET).
- Hold to expiry (1DTE).
- Risk-defined verticals only (put/call sides can be `buy`, `sell`, `none`).
- Widths: `5` or `10`.
- Target deltas per active side: `0.10`, `0.16`, `0.25`.
- Size: `1..3`.
- Starting balance: `$30,000`.
- Risk budget: `30%` cap with `90%` buffer (effective `27%`).
- Commission: `$1` per leg.

## Leakage Control

Players only receive public fields:

- `Date,TDate,SPX,VIX,VixOne,RV,RV5,RV10,RV20,R,RX,Forward`

Engine suppresses label/outcome fields (including `Limit/CLimit`, `LeftGo/RightGo`, `Profit/CProfit`).

## CLI

```bash
cd /Users/mgebremichael/Documents/Gamma

# Create a round for a date (settles due rounds first)
python3 -m sim_gpt.cli run-live --date 2026-03-02 --api-url https://gandalf.gammawizard.com/rapi/GetUltraPureConstantStable

# Settle due rounds
python3 -m sim_gpt.cli settle --date 2026-03-03 --api-url https://gandalf.gammawizard.com/rapi/GetUltraPureConstantStable

# Inspect one round
python3 -m sim_gpt.cli round --date 2026-03-02

# Leaderboard
python3 -m sim_gpt.cli leaderboard

# Push results/leaderboard/decisions to Google Sheets
python3 -m sim_gpt.cli sync-sheet
```

## Environment

Required for live chain pricing:

- `SCHWAB_APP_KEY`
- `SCHWAB_APP_SECRET`
- `SCHWAB_TOKEN_JSON` or `SCHWAB_TOKEN_PATH`

For feed API:

- `LEO_LIVE_URL` (optional override)
- `LEO_LIVE_TOKEN` or `GW_EMAIL` + `GW_PASSWORD`

For Google Sheets:

- `GOOGLE_SERVICE_ACCOUNT_JSON` (or file equivalent)
- `GSHEET_ID` (optional; default is set in config)

## Google Sheets Tabs

- `Live_Game_Rounds`
- `Live_Game_Leaderboard`
- `Live_Game_Decisions`

Decisions tab includes:

- chosen target deltas
- resolved strikes
- entry prices (points)
- chain as-of timestamp
- risk metadata and guard outcome
