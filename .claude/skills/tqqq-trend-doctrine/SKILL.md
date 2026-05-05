---
name: tqqq-trend-doctrine
description: TQQQ Trend (a.k.a. C1-HYST) production doctrine — the locked spec, what is rejected, the load-bearing entry gate, and the replacement bar that any new variant must clear. Use whenever the user proposes a tweak to the strategy ("what if we add X", "could we use a different MA window", "test a 2x version", "drop the SMA50/SMA200 condition"), suggests a new entry/exit rule, asks "could we tune Y", or asks "is variant Z worth trying". Read this BEFORE recommending any change to the live system. The signal-design phase is closed; this skill exists to prevent re-litigating settled questions.
---

# TQQQ Trend Doctrine

Production doctrine for **TQQQ Trend** (internal codename **C1-HYST**) — the
daily TQQQ↔BIL rotation governed by a QQQ trend filter that went live on the
user's Schwab account on 2026-05-05 at $5,000 initial position.

This skill is the boundary-keeping document. The companion operational skill
is `tqqq-trend` (signal computation, placement script, daily mechanics).
This one exists to answer the question: **"is this proposed change worth
running?"** — usually no, with a specific empirical reason.

Source of truth: [scripts/conviction/backtest/C1_HYST_LOCKED_SPEC.md](../../../scripts/conviction/backtest/C1_HYST_LOCKED_SPEC.md).
If this skill disagrees with that file, the file wins — update this skill.

---

## The spec (frozen)

```
Signal asset:  QQQ daily close
Sleeves:       TQQQ (3x QQQ) on risk-on, BIL (1-3m T-bill) on risk-off

Inputs (computed on QQQ close):
  SMA50, SMA150, SMA200
  ret63 = close[t]/close[t-63] - 1

Conditions:
  A = close > SMA150
  B = SMA50 > SMA200            ← LOAD-BEARING entry gate (do not drop)
  C = ret63 > 0
  score = int(A) + int(B) + int(C)

State machine:
  in BIL  → flip to TQQQ at next open  iff  score == 3
  in TQQQ → hold                       iff  score >= 2   (hysteresis zone)
  in TQQQ → flip to BIL  at next open  iff  score <= 1

Cadence:       evaluate every trading day after 4:00 PM ET close
Execution:     next trading day at open via 3-step LIMIT-chase ladder
               (mid → cross → cross+spread/2 → MARKET fallback)
Slippage assumption (backtest): 10 bp baseline, 50 bp stress
Cost convention: one slippage event per flip, sell+buy combined
```

---

## What's rejected — do not re-pitch

Each row below is a tested-and-failed proposal. The "why rejected" includes
the empirical evidence, not just an opinion. If the user proposes any of these
variants, point them at the row and the run ID.

| variant | what it tried | why rejected |
|---|---|---|
| **C1 (original)** | naive entry/exit, no hysteresis | passed only 3bp slippage; failed at 10/25bp; flip rate too high |
| **C1-NORETEXIT** | drop ret63 from exit | preserved as fallback only; HYST dominates on Sharpe + Calmar |
| **C1-WEEKLY** | evaluate Friday only | underperformed daily HYST on key 2022 catch |
| **C1-COOLDOWN** | enforce ≥10td gap before re-entry | re-entry delay missed too many recoveries |
| **C1-VEL21 / VEL63** | extra exit at -8% / -10% from N-day high | added flips, did not add return |
| **D1** (drawdown -5%/-10% from 63d high) | non-MA short drawdown filter | failed 2x SPY in all 4 windows |
| **V1** (vol expansion: vol21 vs vol63) | non-MA vol filter | failed 2x SPY in all 4 windows; vol confirms after damage |
| **VX1** (VIX <25 enter, >30 exit) | VIX-level filter | failed 2x SPY in all 4 windows; same lateness problem |
| **DH** (D2 entry + HYST exit) | proposed hybrid after D2 emerged | failed verdict gate: max DD -53.4% > -53% gate; B+C miss 2x SPY at 50bp; flip count exploded to 61 |
| **D2 as primary** | non-MA dd-from-126d-high filter | passes 2x SPY in all 4 windows but max DD -66.8% disqualifies as primary; retained as academic confirmation |
| **HD** (HYST entry + D2 exit) | inverse control, simpler exit | matches HH return but DD widens from -48% to -62%; not worth the simplicity |
| **QLD swap (2x)** | replace TQQQ with 2x leverage | Calmar tie at 0.66 vs 0.67; gives up $472k of $710k for 14pp DD reduction; tested GHA 25383587339 |
| **Mixed sleeve (50/50 TQQQ/QLD)** | blend the leverages | no Calmar headroom to capture per the QLD test; adds operational complexity |
| **Monthly dual momentum** | period swap | underperformed C1-HYST on every metric tested |

The pattern across most rejected variants: **they react to volatility or price
damage AFTER the move is underway.** Vol spikes, drawdowns from short-window
highs, and VIX-level thresholds all fire too late to avoid the damage and too
early to avoid whipsaws on the way back up. The HYST entry's `SMA50 > SMA200`
gate is uniquely load-bearing because it's a *slow-moving regime qualifier*
that prevents re-entering during fragile post-bear rallies.

---

## The load-bearing finding (don't propose changes that break this)

From the 2x2 entry/exit attribution test (GHA run 25358962345):

```
HH = HYST entry + HYST exit:    +6610%, DD -48.1%, 49 flips
HD = HYST entry + D2 exit:      +6334%, DD -61.8%, 21 flips
DD = D2 entry + D2 exit:        +4165%, DD -66.8%, 21 flips
DH = D2 entry + HYST exit:      +2440%, DD -51.6%, 61 flips
```

**Hold entry fixed, swap exit:** return changes by 4%, DD changes by 14pp.
**Hold exit fixed, swap entry:** return changes by 63%, DD changes by 4pp.

Therefore: **HYST entry contributes most of the return; HYST exit contributes
DD control.** Both rules are doing real, complementary work. Neither is
redundant. Most importantly:

**The 387 days where HH was in BIL while DD was in TQQQ contributed 97.9% of
the HH-vs-DD edge.** Any variant that re-enters TQQQ during those days
(specifically: when SMA50 < SMA200, even if QQQ is near a 126d high)
inherits D2's worse drawdown profile.

---

## Replacement bar (must clear ALL four)

Any proposed variant that wants to displace C1-HYST as primary must:

1. **Pass 2x SPY total return in all four windows** (L=2011-, A=2021-, B=2022-,
   C=2022-Feb-) at 50bp slippage. C1-HYST clears this with margin.
2. **Window-L max DD ≤ -48.5%** (no relaxation of the historical worst).
3. **Demonstrably stay out** during the 387-day attribution block where D2 ate
   −62.8% of TQQQ drawdown that HH avoided. New entry rule must be inactive
   or exit-active on those specific dates. See `attribution_L.csv` from
   GHA run 25358962345.
4. **Show non-redundant value vs HYST in a 2x2 attribution swap** — same test
   we ran on D2. If swapping the new rule into HH degrades return ≥10% or
   widens DD ≥5pp, the rule is doing different work than HYST and the
   comparison is real. If it produces an ≈identical curve, it's a re-skin.

If a proposal can't articulate how it clears all four, the answer is no. Don't
make the user re-litigate.

---

## What is research-only (NOT for production)

These are documented because they came up in conversation, but they have
NEVER been backtested in this sleeve and should not be treated as deployable:

- **D2 + position-size-stop overlay** — hypothetical "use D2 with an account-DD
  stop". Untested. The doctrine: do not silently add risk overlays without
  backtesting them first. Adding "soft" rules to dampen DD can change the
  edge in non-obvious ways.
- **Multi-asset trend baskets** (e.g. AQR-style time-series momentum across
  20+ markets) — academically supported phenomenon but a different research
  agenda from this single-sleeve TQQQ rotation. Not a substitute or extension.
- **Stop-loss orders on the underlying TQQQ position** — never tested. The
  spec uses signal-driven exits, not price-stop exits. Adding a stop introduces
  a path-dependent rule with its own DD profile that has to be re-validated.

---

## Boundary rule

Research findings (e.g. "D2 also passes 2x SPY", "non-MA filters can work")
do NOT mean those filters should be added to C1-HYST. The strategy is the
specific rule set above. Treat anything not in the locked spec as either
*rejected* (see the table above) or *research-only* (different conversation).

The user has explicitly closed the signal-design phase as of 2026-05-05.
The next phase is operational: live monitoring, P&L tracking, position-level
risk limits as a *separate layer outside* the strategy spec.

---

## When to update this skill

- Update only after a backtest has been run that meets the replacement bar
- Always update both this skill AND `C1_HYST_LOCKED_SPEC.md` together; if
  they disagree, the locked spec wins
- Note the GHA run ID + date in the rejected-variants table when adding rows
- Don't remove rejected entries from the table — they're the institutional
  memory that prevents re-running

---

## Memory pointers

- `project_hyst_attribution.md` — the 2x2 attribution result + live status
- `project_c1_hyst_leverage_choice.md` — TQQQ vs QLD verdict
- `project_trend_filter_doctrine.md` — what's been ruled out on SPX cash 200d
- `feedback_doctrine_scope.md` — scope-of-claims hygiene
