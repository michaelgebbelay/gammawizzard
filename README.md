Here’s the full picture—what this system is, what it does, and how it does it. No fluff.

⸻

1) Objective

Automate your daily SPXW LeoCross iron‑condor trade so you don’t have to be at the screen:
	•	Input: LeoCross signal (strikes + expiry + “credit vs debit”).
	•	Action: Place a single complex IRON_CONDOR order at Schwab with a price‑ladder that tries to fill within a few minutes.
	•	Safety: Never place an order that would close any existing leg, and never place when there’s partial overlap (1–3 legs already on). Only allow top‑ups to target when all 4 legs already exist.
	•	Sizing: Fixed count (default 4 contracts). No OBP checks.
	•	Observability: Log every guard decision and every placement attempt to Google Sheets.

⸻

2) Components

A. Orchestrator (guard + runner) — scripts/leocross_orchestrator.py
	•	Fetches latest LeoCross trade payload from GammaWizard.
	•	Derives the four SPXW legs (.SPXW yymmdd P/C strikes), correctly oriented:
	•	Short IC (credit): BUY outer wings, SELL inner strikes.
	•	Long IC (debit): SELL outer wings, BUY inner strikes (inverted orientation).
	•	Checks Schwab for:
	1.	Would‑close guard: It refuses to place if BUY_TO_OPEN would hit a short already in the account, or SELL_TO_OPEN would hit a long already in the account (i.e., the new order would functionally close anything).
	2.	Partial‑overlap guard: If any (1–3) of the intended legs already exist in the same direction, it skips. If all 4 exist and are aligned, it tops up to the target.
	3.	Duplicate working orders: Skips if an identical 4‑leg working order is already open.
	•	Computes remaining quantity to trade:
	•	rem_qty = QTY_TARGET - units_open (units_open = min of aligned leg counts).
	•	If allowed and rem_qty > 0, it runs the placer and injects QTY_OVERRIDE=rem_qty into the environment.
	•	Logs every decision to your Google Sheet in a tab named guard.
	•	Hardenings:
	•	Backoff/retry on Schwab HTTP calls.
	•	Per‑run lock (based on GITHUB_RUN_ID) + duplicate‑main sentinel so it can’t run twice inside a single job even if the file is ever pasted twice by mistake.

B. Placer (ladder) — scripts/leocross_place_simple.py
	•	Takes the four legs from LeoCross.
	•	Quantity: uses QTY_OVERRIDE if present; otherwise a hard‑coded constant at the top of the file (set to 4 as you asked). No OBP sizing. No balance checks.
	•	Cancel/replace ladder (one cycle):
	•	Credit IC:
	•	Start 2.10 → wait ~30s.
	•	If not filled, use mid once, then refresh mid again → wait each time.
	•	If still not filled, try prev mid − 0.05 (bounded by floor 1.90).
	•	Final bound: 1.90, then cancel.
	•	Debit IC mirrors the above (start 1.90, step toward 2.10 ceiling).
	•	Before each placement, cancels any matching open order (same 4 legs).
	•	Tracks partial fills and cancellations; if partially filled, it continues the ladder for the remaining size only.
	•	Logs to Google Sheet tab schwab:
	•	Timestamp, source, signal date, side (short/long IC), size, order type (net credit/debit), limit used, four OCC symbols, Schwab order id, and a ladder trace (e.g., STEPS 2.10@4→1.95@4 | FILLED 3/4 | CANCELED 1).

C. Workflow — .github/workflows/leocross.yml
	•	Triggered manually (workflow_dispatch inputs.mode) and/or by schedule or external scheduler.
	•	Single step runs the orchestrator; the orchestrator decides whether to run the placer.
	•	Uses concurrency (group: leocross-min) with cancel-in-progress: true so only one job runs at a time.

D. External scheduler (optional)
	•	You can trigger via cron-job.com with a single POST to GitHub’s workflow‑dispatch at 16:12 ET (Mon–Fri).
	•	Expected 204 response; no retries; one job only.

⸻

3) Data flow & decisions
	1.	Trigger fires (GitHub manual, scheduled, or cron‑job.com -> workflow_dispatch).
	2.	Orchestrator:
	•	Get LeoCross JSON → extract last/active trade.
	•	Build legs (expiry, inner strikes, width=5), determine credit/debit using Cat1/Cat2 when present.
	•	Orient legs so BUY are wings for credit; inverted for debit.
	•	Positions snapshot (Schwab “positions?fields=positions”).
	•	Guards:
	•	Would‑close? If BUY leg already short, or SELL leg already long → SKIP.
	•	Partial overlap? If 1–3 legs present → SKIP.
	•	All four aligned? Top‑up only → compute rem_qty.
	•	No legs present? Set rem_qty = QTY_TARGET.
	•	Working order duplicate? SKIP.
	•	Log decision to Sheet tab guard.
	•	If allowed and rem_qty > 0, run placer with QTY_OVERRIDE=rem_qty.
	3.	Placer:
	•	Cancels matching working orders for those legs.
	•	Ladder submissions (wait ~30s per rung); poll status; handle partial fills.
	•	Final cleanup: cancel lingering working orders if not filled.
	•	Log to Sheet tab schwab with full ladder trace and fill/cancel counts.
	4.	Done.

⸻

4) Configuration (what you set once)

Secrets (GitHub → Settings → Secrets and variables → Actions):
	•	SCHWAB_APP_KEY, SCHWAB_APP_SECRET
	•	SCHWAB_TOKEN_JSON (valid refresh token JSON; rotate when you see refresh_token_authentication_error / unsupported_token_type)
	•	GSHEET_ID (the target spreadsheet)
	•	GOOGLE_SERVICE_ACCOUNT_JSON (service account JSON with edit rights on that sheet)
	•	GammaWizard: GW_TOKEN (optional), GW_EMAIL, GW_PASSWORD (fallback auth)

Placer constants (top of leocross_place_simple.py):
	•	QTY_FIXED = 4 (your hard‑coded quantity when QTY_OVERRIDE not set)
	•	Ladder tuning:
	•	STEP_WAIT = 30 (seconds between checks)
	•	CREDIT_START = 2.10, CREDIT_FLOOR = 1.90
	•	DEBIT_START = 1.90, DEBIT_CEIL = 2.10
	•	TICK = 0.05, WIDTH = 5

Orchestrator constant:
	•	QTY_TARGET = 4

Optional env for dry‑run:
	•	GUARD_ONLY=1 → runs guard + logs, does not call placer.

⸻

5) Triggers you can use
	•	Manual: Actions → “Place LeoCross” → Run workflow → mode=NOW.
	•	GitHub schedule (commented/uncommented in YAML): two crons around 16:10–16:12 ET; the placer itself enforces a narrow window if you use the scheduled path.
	•	cron-job.com external: Single POST to
https://api.github.com/repos/<owner>/<repo>/actions/workflows/leocross.yml/dispatches
with body {"ref":"main","inputs":{"mode":"NOW"}}, headers Authorization: Bearer <PAT>, Accept: application/vnd.github+json, X-GitHub-Api-Version: 2022-11-28, Content-Type: application/json.
Schedule: Mon–Fri 16:12:00 ET; no retries; treat 204 as success.

(Tip: while using cron‑job.com, comment out the schedule: block in YAML to avoid two trigger sources. If you keep both, add a “debounce” step to skip if another run started within ~90s.)

⸻

6) What we explicitly do not do
	•	No OBP / balance checks. You asked to remove them; quantity is fixed or override via orchestrator.
	•	No strike hunting or price discovery beyond the ladder. We follow the exact strikes from LeoCross and ladder prices to fill quickly.
	•	No multi‑cycle re‑laddering beyond the single 3–5 minute cycle; after the final bound the order is canceled.

⸻

7) Reliability & safety rails
	•	Hardened HTTP (GET/POST/DELETE) with backoff/retries for Schwab.
	•	Must‑include time window for listing open orders (Schwab requires fromEnteredTime/toEnteredTime).
	•	Per‑run lock (/tmp/leocross-orch-run-<RUN_ID>) to prevent duplicate execution inside one GitHub run.
	•	Duplicate‑main sentinel so even if someone pastes the orchestrator file twice, only the first main() executes.
	•	Actions concurrency group to prevent two jobs from running at once.
	•	Optional debounce in YAML to ignore a second dispatch within 90 seconds.

⸻

8) Observability
	•	Console: clear prints for guard snapshot, decision, and ladder steps.
	•	Google Sheets:
	•	guard tab: every run’s decision (ALLOW/TOP_UP/SKIP reason/ABORT) + planned legs + account leg quantities + open_units/rem_qty.
	•	schwab tab: placement steps, price(s) used, fill/cancel counts, final order id and status.

⸻

9) Runbook (what to do when X breaks)
	•	Schwab OAuth error (refresh_token_authentication_error / unsupported_token_type):
refresh and replace SCHWAB_TOKEN_JSON Secret.
	•	Working orders list returns 400 “fromEnteredTime missing”:
we call with fromEnteredTime/toEnteredTime already; if Schwab glitches, placer continues with warn and still tries to place.
	•	GammaWizard 401/403: falls back to email/password to fetch a fresh token.
	•	cron‑job.com double‑fires: ensure only one job, disable retries; treat 204 as success. Add YAML debounce if needed.

⸻

10) Acceptance criteria
	•	At 16:12 ET (or on manual trigger):
	•	Guard logs a row in guard with ALLOW (or SKIP with clear reason).
	•	If ALLOW and rem_qty>0, placer runs once, posts 1 order, ladders prices for ≤ ~3–5 minutes, and either fills or cancels.
	•	schwab tab shows one row per placement with ladder trace and final status (FILLED X/Y, CANCELED N).
	•	No order ever posts that would close an existing leg; no partial‑overlap placements.

⸻

That’s the system. If anything in this description doesn’t match the current behavior you’re seeing in logs, call it out and we’ll lock it down.
