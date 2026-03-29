# TastyTrade Operations

Reference for all TastyTrade broker integration: accounts, API client, token management, strategies, streaming, sync, and troubleshooting.

---

## 1. TT Accounts

Two TastyTrade accounts, both trading SPX 5-wide verticals via the same core engine:

| Account | Number | Label | Active Strategies | Schedule |
|---------|--------|-------|-------------------|----------|
| IRA | 5WT20360 | `tt-ira` | **Novix** (ENABLED) | 4:13 PM ET weekdays |
| Individual | 5WT09219 | `tt-individual` | **ConstantStable** (ENABLED) | 4:13 PM ET weekdays |

### Current state (as of 2026-03):
- **IRA (5WT20360)**: Runs **Novix** signal (`rapi/GetNovix`). ConstantStable schedule `gamma-cs-tt-ira` is DISABLED.
- **Individual (5WT09219)**: Runs **ConstantStable** signal (`rapi/GetUltraPureConstantStable`). Novix schedule `gamma-nx-tt-individual` is DISABLED.
- Both accounts also have IC_LONG deferred morning entry schedules (9:35 AM ET) that read plans from S3.
- Cost per contract: $1.72 (TT fee structure for SPX options).

---

## 2. tt_client.py — API Client

**File**: `TT/Script/tt_client.py`

Minimal HTTP client wrapping `requests` with auto-refresh on 401:

```python
from tt_client import request as tt_request

# Usage:
resp = tt_request("GET", "/accounts/5WT20360/positions")
data = resp.json()
```

### How it works:
1. `_base_url()` reads `TT_BASE_URL` env var (default: `https://api.tastyworks.com`)
2. `_auth_header()` calls `get_access_token()` from `tt_token_keeper` and sets `Authorization: Bearer <token>`
3. `request(method, path, **kwargs)` makes the HTTP call with 20s timeout
4. On **HTTP 401**: automatically calls `refresh_token()`, gets a new access token, retries once
5. Raises `requests.HTTPError` on any other failure

### Key endpoints used throughout the codebase:
- `GET /accounts/{acct}/positions` — current positions
- `GET /accounts/{acct}/orders` — order history
- `GET /accounts/{acct}/balances` — cash, net liq, buying power
- `GET /accounts/{acct}/transactions` — fills, fees, commissions
- `POST /accounts/{acct}/orders` — place new orders
- `DELETE /accounts/{acct}/orders/{order_id}` — cancel order
- `GET /api-quote-tokens` — get DXLink streaming quote token
- `GET /market-data/quotes` — snapshot quotes

---

## 3. Token Management

### tt_token_keeper.py

**File**: `TT/Script/tt_token_keeper.py`

Handles token persistence with file locking and atomic writes:

1. **`load_token()`**: Reads from `TT_TOKEN_PATH` env var (default: `TT/Token/tt_token.json`). In Lambda, this is `/tmp/tt_token.json` (seeded from SSM).
2. **`save_token(token)`**: Atomic write via `.tmp` + `os.replace()`, protected by `fcntl.flock()` file lock (`.lock` file).
3. **`refresh_token(token)`**: POSTs to `/oauth/token` with `grant_type=refresh_token`. Supports two auth modes:
   - `body` (default): client_id + client_secret in POST body
   - `basic`: HTTP Basic Auth (set `TT_CLIENT_AUTH=basic`)
4. **`get_access_token()`**: Loads token, returns access_token. If empty, triggers refresh first.

### Required env vars for refresh:
- `TT_CLIENT_ID` — OAuth client ID
- `TT_CLIENT_SECRET` — OAuth client secret
- `TT_TOKEN_PATH` — path to token JSON file (optional, has default)
- `TT_BASE_URL` — API base (optional, defaults to `https://api.tastyworks.com`)
- `TT_CLIENT_AUTH` — `body` or `basic` (optional, defaults to `body`)

### SSM Storage (Lambda)

In production (Lambda), tokens are stored in AWS SSM Parameter Store:
- **Path**: `/gamma/tt/token_json`
- **Other SSM params**: `/gamma/tt/client_id`, `/gamma/tt/client_secret`
- Lambda handler seeds token from SSM to `/tmp/tt_token.json` on cold start
- After each run, handler detects SHA-256 changes in the token file and writes back to SSM

### GitHub Actions Token Keepalive

**File**: `.github/workflows/tt_token_keepalive.yml`

Manual-dispatch workflow (`workflow_dispatch` only) that refreshes the TT token:

1. Seeds token from `TT_TOKEN_JSON` GitHub secret into `TT/Token/tt_token.json`
2. Runs `TT/Script/tt_token_refresh.py` which:
   - Decodes `TT_TOKEN_JSON` env (supports both raw JSON and base64)
   - Calls `save_token()` then `refresh_token()`
3. Persists refreshed token back to `TT_TOKEN_JSON` GitHub secret via `gh secret set`
4. Requires `GH_SECRET_TOKEN` (PAT with secrets write access)

### Token Expiry
- TT access tokens expire after ~24 hours
- Refresh tokens have longer validity but still rotate on each refresh
- The merged token preserves the refresh_token if the API response omits it

---

## 4. Token Bootstrap (Initial Setup)

**File**: `TT/Script/tt_token_bootstrap.py`

Interactive script for initial OAuth authorization code flow:

1. Set env vars: `TT_CLIENT_ID`, `TT_CLIENT_SECRET`, `TT_REDIRECT_URI`
2. Run: `python TT/Script/tt_token_bootstrap.py`
3. Script prints an authorization URL to open in browser
4. User authorizes and pastes the redirect URL (or just the code)
5. Script exchanges code for token via `POST /oauth/token`
6. Saves to `TT_TOKEN_PATH` (and mirrors to `TT/Token/tt_token.json`)
7. Prints the JSON to copy into GitHub secret `TT_TOKEN_JSON`

Use this when:
- Setting up TT integration for the first time
- Refresh token has fully expired (need to re-authorize)

---

## 5. Strategy Orchestrators on TT

All TT strategies share the same core engine: fetch GammaWizard signal, build 5-wide SPX verticals, apply vol sizing, guard checks, and place via `place.py`.

### ConstantStable (Individual — 5WT09219)

**File**: `TT/Script/ConstantStable/orchestrator.py` (v2.4.0)

- **Signal**: `rapi/GetUltraPureConstantStable` from GammaWizard
- **Schedule**: `gamma-cs-tt-individual` at 4:13 PM ET (ENABLED)
- **Trade logic**:
  - `LeftGo < 0` -> short put vertical (credit); `LeftGo > 0` -> long put vertical (debit)
  - `RightGo < 0` -> short call vertical (credit); `RightGo > 0` -> long call vertical (debit)
- **Sizing**: `units = floor(account_value / CS_UNIT_DOLLARS)` with vol bucket multipliers from `CS_VIX_MULTS` keyed off `CS_VOL_FIELD` (VixOne)
- **Guards**:
  - NO-CLOSE guard (`CS_GUARD_NO_CLOSE=1`): skips if opening would net/close existing position
  - TOPUP (`CS_TOPUP=1`): only tops up to target qty if same-strike spreads already open
  - 4-leg bundling (`CS_BUNDLE_4LEG=1`): places PUT+CALL as single 4-leg order when quantities match
  - IC_LONG filter (`CS_IC_LONG_FILTER=1`): regime filter for IC_LONG trades, reads pre-computed go/no-go from S3

### Novix (IRA — 5WT20360)

**File**: `TT/Script/Novix/orchestrator.py` (v1.0.0)

- **Signal**: `rapi/GetNovix` from GammaWizard
- **Schedule**: `gamma-nx-tt-ira` at 4:13 PM ET (ENABLED)
- **Differences from ConstantStable**:
  - No IC_LONG regime filter
  - No IC_LONG deferred morning entry
  - No IC_LONG -> RR_SHORT regime switching
- Everything else identical: GW fetch, LeftGo/RightGo, 5-wide verticals, vol sizing, topup/guard, bundle/pair-alt placement
- Uses EventWriter for structured trade event logging

### IC_LONG Morning Entry

**File**: `TT/Script/ConstantStable/ic_morning_placer.py`

- Runs at 9:35 AM ET on both TT accounts (when deferred plan exists)
- Reads deferred plan from S3 (`cadence/cs_ic_long_deferred_tt_*.json`)
- Price logic:
  - If 4-leg mid > $2.00: single DAY order at $2.00 (fire-and-forget)
  - If mid <= $2.00: 3-rung ladder at mid, mid+0.05, mid+0.10
- Imports placement functions from `ConstantStable/place.py`

### LeoCross

No TT-specific LeoCross orchestrator exists. The `GetLeoCross` endpoint returns 404 (disabled on GammaWizard side).

---

## 6. close_orders.py — 50% Profit-Take Automation

**File**: `TT/Script/ConstantStable/close_orders.py`

**Status**: DISABLED since 2026-03-15 (`CS_CLOSE_ORDERS_ENABLE: "0"` in handler.py)

Previously ran as a post-step on TT Individual only (IRA was the control group for A/B test):

- Places GTC limit close order at 50% of max profit for entire position
- Max profit calculation:
  - Credit IC/vert: `max_profit = total_credit_received`
  - Debit IC/vert: `max_profit = $5.00 - total_debit_paid`
  - RR: `max_profit = $5.00` (spread width, always)
- Close price: `close_net = max_profit * profit_pct - entry_net` (rounded to nearest $0.05)
- Reads actual fill prices from the trade CSV (`CS_LOG_PATH`)

### Env vars:
- `CS_CLOSE_ORDERS_ENABLE` — "1" to enable (default "0")
- `CS_CLOSE_DRY_RUN` — "1" to log without placing
- `CS_CLOSE_PROFIT_PCT` — target as decimal (default "0.50")

---

## 7. edge_guard.py — Equity Logger + Sequential Edge Test

**File**: `TT/Script/ConstantStable/edge_guard.py`

Runs as a post-step for TT strategies. Two functions:

1. **Equity logging**: Writes one row per run to Google Sheet tab (`ConstantStableState`/`ConstantStableEdge`) with daily P&L based on Schwab liquidation value, adjusted for transfers.

2. **Edge guard (SPRT)**: Sequential probability ratio test on returns:
   - Compares H1 (edge exists, mean = `CS_EDGE_MU1`) vs H0 (no edge, mean = 0)
   - Uses configurable alpha/beta/sigma/min_samples
   - Can pause trading for `CS_EDGE_PAUSE_DAYS` if evidence of no edge
   - Outputs `can_trade=1/0` + reason to `GITHUB_OUTPUT`
   - `CS_EDGE_FAIL_OPEN=true` (default): if Sheets unavailable, allows trading

**Note**: Uses **Schwab** liquidation value (not TT) for P&L computation. Requires Schwab credentials even though it guards TT trading.

---

## 8. DXLink Streamer — Real-Time Quotes

**Files**: `TT/Script/tt_dxlink.py`, `TT/Script/tt_streamer.py`

### tt_dxlink.py (Primary — used by place.py)

WebSocket streaming via DXLink protocol for real-time bid/ask quotes:

1. **Quote token**: `GET /api-quote-tokens` returns a streaming token + DXLink WebSocket URL
   - Cached to `TT/Token/tt_quote_token.json` (or `TT_QUOTE_TOKEN_PATH`)
   - Min TTL of 7200s before re-fetching (`TT_QUOTE_TOKEN_MIN_TTL`)
   - Retries up to 4 times with exponential backoff
2. **WebSocket flow**: SETUP -> AUTH -> CHANNEL_REQUEST(FEED) -> FEED_SETUP -> FEED_SUBSCRIPTION -> receive FEED_DATA
3. **`get_quotes_once(symbols, timeout_s=6.0)`**: synchronous wrapper that opens WebSocket, subscribes, collects bid/ask for all requested symbols, returns `Dict[str, Tuple[float, float]]`
4. Supports COMPACT data format with field mapping from FEED_CONFIG response
5. Debug logging controlled by `TT_DXLINK_DEBUG` and `TT_DXLINK_RAW` env vars

### tt_streamer.py (Legacy)

Older streaming helper, similar structure but without token caching. Uses `_get_quote_token()` directly and supports custom message sequences via `TT_STREAM_MESSAGES` env var.

### Usage in place.py:
```python
from tt_dxlink import get_quotes_once
quotes = get_quotes_once([".SPXW250401P5800", ".SPXW250401P5795"])
# Returns: {".SPXW250401P5800": (bid, ask), ...}
```

---

## 9. tt_probe.py — Connectivity Check

**File**: `TT/Script/tt_probe.py`

Diagnostic script that fetches sample responses from all key TT endpoints:

- Balances, positions, orders, transactions, quotes
- Seeds token from `TT_TOKEN_JSON` env var
- Outputs JSON to stdout (workflow uploads as artifact)

**Workflow**: `.github/workflows/tt_probe.yml` (manual dispatch)
- Uses `TT_CLIENT_AUTH=basic` for auth
- Uploads output as `tt-probe-{run_id}` artifact (7-day retention)

Useful for:
- Verifying TT API connectivity after token refresh
- Debugging field name changes in TT API responses
- Checking account state manually

---

## 10. Broker Sync (broker_sync_tt.py)

**File**: `reporting/broker_sync_tt.py`

Pulls TT data into the reporting DuckDB database. Mirrors the Schwab sync pattern.

### What it syncs:
| Entity | API Path | DuckDB Table |
|--------|----------|-------------|
| Orders | `/accounts/{acct}/orders` | `broker_raw_orders` |
| Fills | Extracted from order legs | `broker_raw_fills` |
| Transactions | `/accounts/{acct}/transactions` | `broker_raw_fills` |
| Positions | `/accounts/{acct}/positions` | `broker_raw_positions` |
| Balances | `/accounts/{acct}/balances` | `broker_raw_cash` + `account_snapshots` |

### Key features:
- **Idempotency**: SHA-256 key on (broker, entity_type, payload) prevents duplicate inserts
- **Kebab-case handler**: `_get(obj, *keys)` tries multiple key variants (kebab-case, snake_case, camelCase) because TT API uses kebab-case field names
- **Account snapshots**: Materializes net_liq, cash, buying_power into `account_snapshots` table (one row per account per day)
- **Source freshness**: Tracks last success/failure per source in `source_freshness` table
- Syncs both accounts in one call, iterating `TT_ACCOUNTS = {"5WT20360": "tt-ira", "5WT09219": "tt-individual"}`
- Default lookback: 7 days for orders/transactions

### Usage:
```python
from reporting.broker_sync_tt import sync_tt
stats = sync_tt()  # syncs both accounts
stats = sync_tt(accounts={"5WT20360": "tt-ira"})  # single account
```

CLI: `python reporting/broker_sync_tt.py --lookback 14 --account 5WT20360`

---

## 11. Common Issues & Troubleshooting

### Token Expiry (most common)
- **Symptom**: HTTP 401 errors, "TT refresh failed: HTTP 401"
- **Cause**: Access token expired (24h TTL) and refresh token also expired or was revoked
- **Fix**: Run `tt_token_keepalive.yml` workflow manually. If refresh token is dead, re-bootstrap with `tt_token_bootstrap.py`.
- **Prevention**: The Lambda handler auto-refreshes on 401 and writes back to SSM. GitHub workflows persist refreshed tokens to secrets.

### Session Refresh Race Condition
- **Symptom**: Two concurrent Lambda invocations both try to refresh the same token
- **Mitigation**: `fcntl.flock()` file lock on token file. In Lambda, invocations are serialized per function instance. SSM writes use SHA-256 change detection (no-op if unchanged).

### API Rate Limits
- **Symptom**: HTTP 429 or slow responses
- **Mitigation**: Placer uses configurable timing knobs:
  - `VERT_STEP_WAIT=20` seconds between ladder rungs
  - `VERT_POLL_SECS=2.0` seconds between order status polls
  - `VERT_CANCEL_SETTLE=1.0` seconds after cancel before next attempt
  - Random jitter added to avoid thundering herd

### Kebab-Case Field Names
- **Symptom**: KeyError on TT API response fields
- **Cause**: TT API returns kebab-case (`cash-balance`, `net-liquidating-value`, `filled-quantity`) while Python code typically uses snake_case
- **Fix**: Always use the `_get(obj, "kebab-key", "snake_key", "camelKey")` pattern from broker_sync_tt.py, or handle both variants

### DXLink Quote Token Expiry
- **Symptom**: WebSocket connection fails or returns no quotes
- **Cause**: Cached quote token in `TT/Token/tt_quote_token.json` expired
- **Fix**: Delete the cached file; `get_quote_token()` will re-fetch. Min TTL check (`TT_QUOTE_TOKEN_MIN_TTL=7200`) prevents using nearly-expired tokens.

### Order Placement Failures
- **Symptom**: Order rejected or no fills
- **Debugging**: Check trade CSV at `CS_LOG_PATH`, look for REJECTED/CANCELED statuses
- **Common causes**: Insufficient buying power, position limits, market closed
- **Ladder behavior**: Placer tries up to `VERT_MAX_LADDER=3` rungs, widening price each time

---

## 12. Manual Operations via tt_client

### Check Positions
```python
import sys; sys.path.insert(0, "TT/Script")
from tt_client import request as tt_request
from tt_token_keeper import save_token
import json, os

# Seed token
token = json.loads(os.environ["TT_TOKEN_JSON"])
save_token(token)

# Fetch
acct = "5WT20360"  # or "5WT09219"
resp = tt_request("GET", f"/accounts/{acct}/positions")
positions = resp.json().get("data", {}).get("items", [])
for p in positions:
    sym = p.get("symbol", "")
    qty = p.get("quantity", 0)
    print(f"{sym}: {qty}")
```

### Check Orders
```python
resp = tt_request("GET", f"/accounts/{acct}/orders")
orders = resp.json().get("data", {}).get("items", [])
for o in orders:
    print(f"ID={o.get('id')} status={o.get('status')} legs={len(o.get('legs', []))}")
```

### Check Balances
```python
resp = tt_request("GET", f"/accounts/{acct}/balances")
bal = resp.json().get("data", {})
print(f"Cash: {bal.get('cash-balance')}")
print(f"Net Liq: {bal.get('net-liquidating-value')}")
print(f"Buying Power: {bal.get('maintenance-excess')}")
```

### Cancel All Working Orders
```python
resp = tt_request("GET", f"/accounts/{acct}/orders")
orders = resp.json().get("data", {}).get("items", [])
for o in orders:
    if o.get("status") in ("LIVE", "RECEIVED", "CONTINGENT"):
        tt_request("DELETE", f"/accounts/{acct}/orders/{o['id']}")
        print(f"Cancelled {o['id']}")
```

### Get Streaming Quotes
```python
from tt_dxlink import get_quotes_once
quotes = get_quotes_once([".SPXW250401P5800", ".SPXW250401P5795"], timeout_s=8.0)
for sym, (bid, ask) in quotes.items():
    print(f"{sym}: bid={bid} ask={ask} mid={round((bid+ask)/2, 2)}")
```

---

## Key File Paths

| Purpose | Path |
|---------|------|
| API client | `TT/Script/tt_client.py` |
| Token keeper | `TT/Script/tt_token_keeper.py` |
| Token bootstrap | `TT/Script/tt_token_bootstrap.py` |
| Token refresh (CI) | `TT/Script/tt_token_refresh.py` |
| DXLink streamer | `TT/Script/tt_dxlink.py` |
| Probe | `TT/Script/tt_probe.py` |
| CS orchestrator | `TT/Script/ConstantStable/orchestrator.py` |
| CS placer | `TT/Script/ConstantStable/place.py` |
| CS close orders | `TT/Script/ConstantStable/close_orders.py` |
| CS edge guard | `TT/Script/ConstantStable/edge_guard.py` |
| IC morning placer | `TT/Script/ConstantStable/ic_morning_placer.py` |
| Novix orchestrator | `TT/Script/Novix/orchestrator.py` |
| Broker sync | `reporting/broker_sync_tt.py` |
| Lambda handler | `lambda/handler.py` |
| Lambda template | `lambda/template.yaml` |
| Token keepalive workflow | `.github/workflows/tt_token_keepalive.yml` |
| Probe workflow | `.github/workflows/tt_probe.yml` |
| CS verticals workflow | `.github/workflows/tt_constantstable_verticals.yml` |
| Token file (local) | `TT/Token/tt_token.json` |
| Token file (Lambda) | `/tmp/tt_token.json` |
| Quote token cache | `TT/Token/tt_quote_token.json` |
| SSM token path | `/gamma/tt/token_json` |
| SSM client ID | `/gamma/tt/client_id` |
| SSM client secret | `/gamma/tt/client_secret` |
