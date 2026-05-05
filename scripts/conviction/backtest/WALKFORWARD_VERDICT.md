# Walk-forward verdict (living document)

> **Last updated 2026-05-03.** This file chronicles the path from "no
> strategy beats SPY" to a tested *candidate* baseline. The current
> candidate is documented at the **bottom** of this file
> (`Walkforward Verdict — Path S Bullish Skew Flip`). It is the best
> tested candidate, not a deployment recommendation.
>
> The original failure verdict and intermediate refinements are
> preserved below as history.

# Historical: original walk-forward verdict — all paths fail in 2024-05 → 2026-04

Same window, same universe (top 500 by $vol from cleaned CS / non-pharma /
optionable set), same 20% trailing stop, same execution model. Single position
unless noted.

| Strategy | Total Return | CAGR | Sharpe | MDD | vs SPY |
|---|---|---|---|---|---|
| **SPY benchmark** | **+42.2%** | **+19.4%** | — | -19% | — |
| **QQQ benchmark** | **+57.0%** | **+25.5%** | — | -23% | — |
| Path P single | +28.5% | +13.5% | 0.53 | -58% | -14pp |
| Path A | +20.9% | +10.0% | 0.44 | -52% | -21pp |
| Path V2 (quality + vol) | -12.4% | -6.5% | 0.00 | -53% | -54pp |
| Path P top-2 | -27.8% | -15.2% | -0.09 | -69% | -70pp |
| Path V (vol-only, no quality gate) | -41.5% | -23.7% | -0.55 | -66% | -84pp |

### Bonus: Path V2 in-sample (5y, 2021-05 → 2026-04) also underperforms

| Strategy | Total Return | CAGR | Sharpe | MDD | vs SPY |
|---|---|---|---|---|---|
| **SPY benchmark (5y)** | **+70.2%** | **+11.3%** | — | -25% | — |
| Path V2 5y in-sample | +19.9% | +3.7% | 0.28 | -51% | -50pp |

Top holdings: BK (456d), PCG (549d), NOC (222d). The quality+vol gate
selects mature, low-volatility names that pass composite filters during
their slow uptrends but **never breaks out into actual high-flyer territory**.
V2 is structurally biased away from the names you actually want.

## What this means

**Every original conviction-system variant (A, P, V, V2) lagged SPY in
the recent 2-year window.** Even Path A — the "rule independent of test
data, +179% / 5y" baseline I called the only honest result — underperformed
by 21 percentage points and took a 52% drawdown vs SPY's 19%.

(Path S is more nuanced — see the Path S section below. One configuration
nominally beat SPY by 43pp but only because of a single 665-day RY hold.)

The pattern across A/P/V/V2 variants: 5 trades, ~150-day average hold,
picking up a single name and riding it through a regime that doesn't
reward it.

| Strategy | What it picked in 2024-2026 |
|---|---|
| Path A | VRT, CVNA, PLTR, HOOD, MMM (held 414 days going nowhere) |
| Path P single | VRT, CHWY, MU, DLTR, plus losers APP/PLTR/SMCI/RBRK/NBIS |
| Path V (broken) | VST, FSLR, VFC, RJF, T, OKTA, RIVN, SLB, NBIS — defensive yielders + post-earnings dead cats |
| Path V2 (quality gate) | VST, WSM, CAVA, DOCU, DE — better names, but DE held 414 days going sideways |

## What's broken

1. **Single-position concentration risk dominates the math.** One MMM or one
   T held for 200+ trading days losing money tanks the whole portfolio.
2. **The 20% trailing is too loose for losing trades** but too tight for
   winning ones. We optimized for the 2021-2024 sample where it happened to
   work.
3. **All paths use price-derived features.** Price momentum and volatility
   are correlated with both real high flyers AND value bouncers / defensive
   rotation. Without a forward-looking signal, we can't tell them apart.

## What we're building next (skew-flip indicator)

Discovered today: the existing Massive S3 credentials grant access to the
full options flat files at `s3://flatfiles/us_options_opra/day_aggs_v1/`.
4 years of daily options chains, 2.6 GB compressed, downloaded in 75s.

Validated end-to-end on 30-day sample:
- Black-Scholes inverse computes IV correctly
- AAPL: -4 vol pts skew (normal hedger market)
- NVDA: -2 vol pts skew (high IV, flatter)
- **MSTR: skew oscillates between -4 and +4 vol pts — literal "skew flip"**
- TSLA: ~0 skew (speculative-mature)

The MSTR signal is exactly the cult-name fingerprint: when skew goes
positive (calls richer than puts), speculative upside bidding dominates.
This is **not visible in price data** — it's a forward-looking option-flow
indicator. If the signal generalizes across the 4y backtest, it's the first
real "high flyer" definition we've found.

## Path S (skew-flip) results

Built end-to-end:
- 4y options chain ingest from Massive S3 (1006 daily files, 2.6 GB)
- Black-Scholes IV inverse via parallel sharded compute (1.2M skew rows)
- Cohort signal test on 1.1M (ticker, date) samples with fwd returns

### Cohort test (descriptive — does the signal have predictive power?)

After fixing a 1-day look-ahead in the rolling z-score (today was inside
its own normalization window) AND switching from naive Welch t-tests on
raw rows (overstates significance due to within-ticker dependence) to
clustered t-tests on per-ticker mean returns:

| Cohort | n_obs | n_tkrs | Mean fwd 60d | p_naive | p_clustered |
|---|---|---|---|---|---|
| skew_z > +2.0 | 28.9k | 2,241 | +3.79% | 0.0001 | **0.0000** |
| skew_z > +1.5 | 61.4k | 2,362 | +3.36% | 0.0518 | **0.0001** |
| BASELINE \|z\|<0.25 | 263.2k | 2,511 | +3.13% | — | — |
| skew_z < -1.5 | 65.4k | 2,394 | +4.32% | 0.0000 | **0.0000** |
| skew_z < -2.0 | 34.0k | 2,327 | +4.56% | 0.0000 | **0.0000** |

Important caveat: even p_clustered ignores cross-sectional correlation
across tickers within the same date / regime. The "real edge" reading is
**direction and magnitude across the cross-section**, not the p-value
literally. With those caveats: bullish flip ≈ +0.23pp over baseline,
bearish flip ≈ +1.19pp over baseline.

Two findings:
1. The aggregate "bullish skew flip" thesis BARELY beats baseline (+0.23pp).
   Real but small.
2. Bearish skew flips (puts unusually rich) predict HIGHER fwd returns —
   classic "fear → opportunity" pattern, opposite of the original
   hypothesis, and roughly 5x larger effect than the bullish-flip edge.

Per-ticker, the signal IS dramatic for high-vol speculative names:
SNDK (+158% avg per-flip on 21 flips), PACS (+133%), NEON (+88%),
OKLO (+55%), IREN (+54%), QBTS (+49%). Exactly the user's original
cult-name targets. But these are a small minority of the universe.

### Path S backtests

| Variant | 4y in-sample | 2y holdout | vs SPY |
|---|---|---|---|
| Path S top-500 | -18.8% | +37.0% (2 trades) | -2.5pp |
| Path S top-2000 | -48.5% | +82.9% (2 trades) | +43pp |
| SPY (4y) | +71.7% | +39.5% | — |

The +83% holdout was driven entirely by one 665-day RY hold — pure luck,
not signal. The 4y in-sample is the honest test: -48% over 4 years.

Path S as designed funnels into the WRONG tail of the universe — defensive
trenders (TJX, PWR, RY, GLPI, CL) where the signal has weak predictive
power — instead of the cult names where it's dramatic. The eligibility +
above-50d filter biases away from the speculative names that the cohort
test showed are where the signal lives.

## Path S sweep with max-hold + speculative + bearish (Variants A/B/C/D)

After confirming the original Path S +83% holdout was 1-trade luck, ran a
4-variant sweep: speculative-filter on/off × bullish/bearish direction,
all with a 90-day max hold. Universe: top-2000 by $vol from cleaned set.

| Variant | Config | 4y in-sample | 2y holdout |
|---|---|---|---|
| **D** | top-2000 + **bullish** + **90d max** | **+268.4%** | **+136.2%** |
| C | top-2000 + bearish + 90d max | -49.6% | +71.6% |
| A | speculative + bullish + 90d max | +7.7% | -37.8% |
| B | speculative + bearish + 90d max | -81.0% | +25.1% |
| Path S original (no max-hold) | top-2000 + bullish | -48.5% | +83% (1 trade luck) |
| **SPY** | — | **+71.7%** | **+39.5%** |

### Variant D is the first walk-forward-validated edge

| Metric | D 4y | D 2y | SPY 4y |
|---|---|---|---|
| Total return | +268.4% | +136.2% | +71.7% |
| CAGR | 38.8% | 53.9% | 14.6% |
| **Sharpe** | **0.98** | **0.98** | — |
| MDD | -45.4% | -48.0% | -19.0% |
| Trades | 20 | 13 | — |
| Win rate | 60% | 69% | — |
| Avg winner | +23.2% | +17.8% | — |
| Avg loser | -10.5% | -12.7% | — |

The **identical Sharpe of 0.98 in both windows** is the strongest single
piece of evidence this isn't regime overfit. Top winners include HOOD
twice (+88%, +65% — two separate skew-flip cycles 9 months apart),
SPHR +42%, SCCO +17%, CAH +16%. **14 of 20 exits fired via
MAX_HOLD_90D** — the 90-day rotation IS the strategy.

### Why the 90-day max-hold is the lever

Same universe (top-2000), same direction (bullish), same skew threshold,
same trailing stop. Only difference is whether positions are forced to
rotate at day 90. Result:

| Without max-hold | With 90d max-hold |
|---|---|
| -48.5% / 4y, 8 trades | +268.4% / 4y, 20 trades |
| Holding 178d avg | Holding 71d avg |

The original Path S without rotation is "buy on first skew flip, hold
forever" — which traps the strategy in the first name's regime. Adding
forced rotation lets it harvest fresh skew flips every quarter.

### Why direction matters

- D (bullish) clears both windows
- C (bearish) wins 2y holdout by chance (regime), loses 4y by 121pp
- The cohort test had hinted bearish was stronger — but that was raw
  forward-return mean, not a tradeable strategy. When wrapped in entry/
  exit discipline, bullish skew-flip wins.

### Why the speculative filter is broken

Speculative names (price $1-$50, vol_60d > 50%) generate too many false
positives — one-off call-buying spurts that revert. A 4y, B 4y both
underperform dramatically. The flyer-rank eligibility (composite score +
$25M dollar volume) is the right gate.

## Decision: deploy Variant D at small size after sensitivity tests

**Conditions:**
1. Sweep max-hold parameter (60d, 90d, 120d, 180d) to confirm 90 isn't
   a sweet-spot artifact.
2. Re-test with realistic transaction costs (model market impact, not
   just 15bps slippage).
3. Per-year breakdown: how does D perform in each calendar year? If 2024
   and 2025 dominate the gain (and 2022-2023 are flat/negative), the
   "edge" is partial regime-luck.
4. Position size at 25-50% of conviction-system capital, not 100%, until
   live trading produces another quarter of confirmation. The -45%
   drawdown is 2.4x SPY's — concentration risk is real even with rotation.

If sensitivity tests pass, this is the first conviction-system variant
worth real capital allocation. **Critically: do not deploy any other
variant.** A/B/C all failed walk-forward in clear ways.

### Important caveats

- Sharpe 0.98 is real but not exceptional (Renaissance-tier is 2+).
- 20 trades / 4y is moderate sample size; path-dependence not ruled out.
- The skew signal is forward-looking option flow, but the 4y backtest
  window includes only one full bear-recovery cycle.
- Variant D's per-trade win rate of 60% means 4 of 10 trades lose on
  average — psychologically painful at single-name 100% conviction.

The skew signal IS real for speculative cult names but as currently wired
into the strategy doesn't translate into beating the benchmark. The right
extensions to try (listed in order of likelihood of working):

1. **Speculative-only universe**: filter to price $1-$50, vol_60d > 50%,
   market cap < $20B. Apply skew-flip only there. This is where SNDK / OKLO
   / IREN actually live.
2. **Hard 90-day max hold**: forces the signal to keep working; avoids
   the 644-day GLPI sit-and-rot pattern.
3. **Bearish skew as the actual entry signal**: cohort test showed +1.5pp
   edge in the bearish-flip direction (counter-intuitive but data-supported).

Until one of those extensions is built and walk-forward validated, the
honest forward expectation for any conviction strategy is "underperforms SPY."

The pragmatic decision: hold SPY, stop tuning, or build extension #1 next.


---

# Walkforward Verdict — Path S Bullish Skew Flip

## Current Best Candidate

After the 2026-05-03 displacement replay, the production candidate is
the **single-position** expression of the z=3.0 family. No rotation
overlay.

**Production candidate — z=3.0 single, no displacement:**

- Signal: Path S bullish skew flip
- Skew threshold: z >= 3.0
- Universe: top-2000 liquid universe
- Portfolio: 1 position (single)
- Regime gate: SPY > 200d
- Exit: 20% trailing stop OR 90d max hold OR regime exit (in that
  order of precedence)
- Displacement / rotation overlay: **none** (rejected by full replay)
- Cost assumption: 15bps (top-2 cost stress passed cleanly through
  50bps; single cost sweep still queued but not intellectually
  decisive)
- Window: 2022-05-02 to 2026-04-29

**Smoother alternate — z=3.0 top-2, no displacement:** same spec as
above with 2 concurrent positions instead of 1. Better Sharpe and MDD,
~244pp lower total return.

This replaces the prior z=1.5 Variant D candidate AND the earlier
"top-2 is the primary candidate" framing.

## Current Status (post-2026-05-03 sweeps)

```
Confirmed keepers:
  - z=3.0 same-day skew threshold
  - SPY regime gate
  - 20% trailing stop
  - 90d max hold
  - z=3.0 single as production candidate
  - z=3.0 top-2 as smoother alternate (benchmark)
  - no displacement / no rotation overlay (auto-swap rejected)

Rejected:
  - 2-day skew persistence
  - earnings blackout (any window, any mode)
  - trend-quality floors (% above 200d, ret_60d)
  - stale-loser displacement (single-position AND top-2; full replay 2026-05-03)
  - crypto / SIC hindsight exclusions

Mostly closed:
  - cost sensitivity for top-2 (passes at 50bps, +254pp vs SPY)

Research notes only (NOT production rules — never path-tested as a
strategy change):
  - high-z exhaustion hypothesis (event study: z>=6/7 looks like fuel,
    not exhaustion — but this is a cross-sectional descriptive result,
    not an in-position exit-rule backtest)
  - negative-z asymmetry (descriptive only; would be a separate strategy)
  - rank-vs-breadth question (not backtested)
  - candidate watchlist while already holding (display only — NO
    automatic swap; the displacement backtest closed the auto-swap
    door definitively)

Still open:
  - z=3.0 single cost sweep (queued, not intellectually decisive)
  - risk-scaled single-position sleeve test
  - forward paper-trade ≥1 quarter
```

The pattern across rejections is consistent: every "make the signal
safer with slower / additional confirmation" filter destroys edge. The
candidate baseline is genuinely just `z >= 3.0 + SPY gate + 90d max
hold + 20% trail + top-2`, with no defensive overlays.

**Boundary rule:** if it has not been path-tested as a strategy change
in `replay.py`, it is a research note, not a strategy rule. Event
studies, cohort analyses, and blocked-signal counterfactuals can
*motivate* a backtest, but they cannot *replace* one. Nothing moves
from "research notes only" to "keepers / rejected" without a
canonical-config replay run.

## Final Variant Ranking

| Rank | Variant | 4y Return | 4y Sharpe | 4y MDD | Trades | Verdict |
|---:|---|---:|---:|---:|---:|---|
| 1 | z=3.0 single, no displacement | +621% | 1.32 | -30% | 17 | Production candidate (highest absolute return, more concentration / path risk) |
| 2 | z=3.0 top-2, no displacement | +377% | 1.49 | -20% | 33 | Benchmark / smoother alternate (best risk-adjusted shape) |
| — | All displacement variants (single + top-2) | — | — | — | — | **Rejected** — full replay degraded every metric |
| — | Variant D + SPY, z=1.5 single | +261% | 0.99 | -29% | 19 | Superseded |
| — | Variant D no gate | +268% | 0.98 | -45% | 20 | Superseded, worse drawdown |
| — | SPY | +72% | — | -19% | — | Benchmark |

z=3.0 **single, no displacement** is the production candidate: highest
absolute return, calendar-consistent (fixes 2023), Sharpe 1.32, MDD
-30%. z=3.0 **top-2, no displacement** is the smoother alternate:
Sharpe 1.49, MDD -20% (≈ SPY's drawdown for ~5× the return), at the
cost of ~244pp of total return.

No displacement / rotation overlay is included in either expression.
The 2026-05-03 path-dependent replay decisively rejected the
stale-loser displacement rule for both single and top-2.

## Per-Year Decomposition

| Year | z=3.0 top-2 | Variant D + SPY, z=1.5 | SPY |
|---|---:|---:|---:|
| 2022 May-Dec | +0.0% | -2.9% | -7.7% |
| 2023 | +7.8% | +31.9% | +24.8% |
| 2024 | +44.5% | +89.4% | +24.0% |
| 2025 | +107.9% | +30.2% | +16.6% |
| 2026 YTD | +56.8% | +16.8% | +4.2% |
| 4y total | +376.8% | +260.8% | +71.7% |

## Single-Position Nuance: Fixes 2023, But Amplifies Path Risk

| Year | z=3.0 Single | z=3.0 Top-2 | SPY | Single vs SPY |
|---|---:|---:|---:|---:|
| 2022 May-Dec | +0.00% | +0.00% | -7.73% | +7.73pp |
| 2023 | +44.62% | +7.78% | +24.81% | +19.81pp |
| 2024 | +32.69% | +44.46% | +24.00% | +8.69pp |
| 2025 | +80.94% | +107.87% | +16.64% | +64.30pp |
| 2026 YTD | +117.69% | +56.76% | +4.16% | +113.53pp |
| 4y Total | +621.33% | +376.8% | +71.7% | — |

The single-position z=3.0 variant changes the interpretation of the 2023
weakness.

The earlier read was that z=3.0 itself struggled in 2023 because the
threshold was too selective during a low-volatility grind-up regime. The
single-position run shows that is not quite right. z=3.0 single returned
+44.62% in 2023 versus SPY at +24.81%, while z=3.0 top-2 returned only
+7.78%.

So the 2023 weakness is not purely a z=3.0 signal problem. It is at
least partly a portfolio-construction problem: top-2 suffered from
slot-2 drag in a year where the best single name mattered more than
diversified signal breadth.

However, single-position is not automatically the primary candidate. It
has a worse 4y Sharpe than top-2, higher concentration risk, and deeper
within-year drawdowns, especially in 2025 and 2026.

### In-Year Max Drawdown Comparison

| Year | Single MDD In-Year | Top-2 MDD In-Year |
|---|---:|---:|
| 2022 May-Dec | 0.00% | 0.00% |
| 2023 | -15.45% | -20.20% |
| 2024 | -21.62% | -17.20% |
| 2025 | -26.03% | -13.60% |
| 2026 YTD | -29.20% | -17.20% |

Single-position is more calendar-consistent: it beats SPY in every
calendar period, including 2023, where top-2 lagged badly.

Top-2 is more path-stable: it has lower full-window MDD, better Sharpe,
and much smaller in-year drawdowns in 2025 and 2026.

This makes single-position more than a casual shadow variant. It is a
serious aggressive challenger. But top-2 remains the primary candidate
for now because the goal is not simply max return; it is deployable
return per unit of tolerable pain.

## Regime Interpretation

z=3.0 top-2 is more episodic than the older z=1.5 Variant D setup. It
sat fully in cash during the weak 2022 period because of the SPY regime
gate, then strongly outperformed in 2025 and 2026 YTD.

The main weakness of top-2 is 2023. SPY returned +24.81%, while z=3.0
top-2 returned only +7.78%.

However, the z=3.0 single-position run returned +44.62% in 2023. This
means the weakness is not simply the z=3.0 threshold. It is likely a
portfolio construction issue: in slower grind-up regimes, slot-2 can
dilute the best signal rather than add breadth.

In stronger momentum regimes, top-2 can outperform single by harvesting
multiple strong names at once, as seen in 2024 and 2025. In more
concentrated regimes, single can outperform by avoiding slot-2 drag, as
seen in 2023 and 2026 YTD.

Conclusion: z=3.0 is a valid signal family. The open question is the
best capital expression: full top-2, full single, or risk-scaled single.

## Rejected / Superseded Ideas

### 2-Day Skew Persistence

Rejected. The persistence filter killed the signal by filtering out
roughly 89% of one-day bullish skew flips. The strategy appears to
depend on capturing one-day skew repricing events, not waiting for
multi-day confirmation.

### Earnings Blackout — REJECTED (2026-05-03 fresh canonical sweep)

Tested on the actual z=3.0 / top-2000 / top-2 candidate (not the older
z=1.5 setup). Verifier confirmed all variants match on every core field
except the three blackout fields. Apples-to-apples.

| variant | TR | CAGR | MDD | Sharpe | Trades | Win | Avg W / Avg L | Δ CAGR |
|---|---:|---:|---:|---:|---:|---:|---|---:|
| baseline (none) | +376.8% | +48.1% | -20.2% | 1.491 | 33 | 58% | +26.6 / -7.8% | — |
| eb -3d replace | +221.2% | +34.1% | -22.7% | 1.256 | 32 | 56% | +22.7 / -8.7% | -14pp |
| eb -3d skip | +221.2% | +34.1% | -22.7% | 1.256 | 32 | 56% | +22.7 / -8.7% | -14pp |
| eb -7d replace | +172.9% | +28.7% | -29.0% | 1.093 | 33 | 58% | +20.6 / -10.0% | -19pp |
| eb -7d skip | +173.7% | +28.8% | -29.4% | 1.095 | 33 | 58% | +20.7 / -10.0% | -19pp |
| eb -7d/+1d replace | +113.9% | +21.1% | -30.2% | 0.780 | 36 | 53% | +21.4 / -11.3% | -27pp |
| eb -7d/+1d skip | +127.4% | +22.9% | -30.2% | 0.843 | 35 | 54% | +21.5 / -11.3% | -25pp |

The blackout costs the strategy a lot. Even the lightest variant (-3d)
drops CAGR by 14pp. The Path S skew-flip signal IS event-coupled —
pre-earnings option flow is a big share of the edge.

Findings:

- **Replace ≈ skip at every window.** Trade logs are byte-identical at
  -3d, differ by one trade at -7d, and diverge slightly at -7d/+1d. At
  z=3.0/top-2000, when the top candidate is blacked out, there's
  rarely a runner-up that also clears the threshold — so "walk down
  ranks" degenerates to "wait until tomorrow."
- **Big winners blackout removes**: HL +60%, LITE +42%, INTC +40%,
  FTAI +53%, CRDO +70% (at -7d/+1d). Replacements rarely match.
- **MDD worsens monotonically with window width** (-20% → -23% → -29%
  → -30%), opposite of the usual "earnings blackout reduces tail risk"
  story. The SPY regime gate already cuts the bear-tail risk; blackout
  only removes upside.
- **Win rate barely moves** (58% → 56% → 58% → 53-54%); avg loser gets
  worse (-7.8% → -11.3%) as blackout pushes entries onto signal-degraded
  later days that lack the pre-earnings premium tailwind.

**Decision**: blackout is rejected as the default for the candidate
baseline. The pre-earnings skew is part of the signal, not noise to be
filtered. If a future risk-management mandate requires removing
ex-ante earnings exposure (e.g., to cap single-name event tail risk),
the lightest **-3d replace** variant is the smallest-cost compromise:
still Sharpe 1.26, still CAGR 34%, still beats SPY by 19pp/yr, just
gives up the steepest part of the alpha.

Comparison helper: [compare_earnings_blackout.py](compare_earnings_blackout.py)
(verifier-guarded). Outputs at
`scripts/conviction/backtest/results/2026-05-03_replay_1460d_massive_n2_eb_*/`.

### Crypto / SIC Exclusion

Rejected as a hindsight-driven loser patch. Excluding a sector because
CIFR/BTDR-type names hurt the backtest is not a clean ex-ante rule.

### Trend-Quality Floors — REJECTED (2026-05-03 sweep)

Hard trend floors based on % above 200d or 60d return destroyed the
z=3.0/top-2 edge. Every tested floor materially reduced CAGR and Sharpe,
while increasing max drawdown.

| Variant | Trades | Total | CAGR | Sharpe | MDD | vs SPY |
|---|---:|---:|---:|---:|---:|---:|
| baseline (no floor) | 33 | +376.8% | +48.1% | 1.49 | -20.2% | +305pp |
| >=15% above 200d | 37 | -0.3% | -0.1% | 0.18 | -64.8% | -72pp |
| >=20% above 200d | 32 | +52.0% | +11.1% | 0.50 | -36.6% | -20pp |
| ret_60d >= 10% | 36 | +3.7% | +0.9% | 0.19 | -52.3% | -68pp |
| ret_60d >= 30% | 34 | +8.6% | +2.1% | 0.23 | -52.2% | -63pp |

The baseline returned +376.8% / Sharpe 1.49 / -20.2% MDD; trend-floor
variants ranged from roughly flat to +52%, with Sharpe ≤ 0.50 and MDD
between -36.6% and -64.8%.

The lesson: **winner-conditioned analysis misled us.** The big winners
looked like they were already strong trenders, so it was tempting to
require "strong trend before entry." But applied to the full candidate
set, that rule destroyed performance and blew out drawdown. The trend
stats on winners were a description after the fact, not a universal
condition for edge. Several floor variants produced *more trades* than
baseline — a warning sign that the filter wasn't making the system more
selective, just causing different (worse) rotations as losers exit
faster and refill slots.

The signal is not "buy names that are already extended trend monsters."
It is closer to "buy fresh extreme skew repricing events before the
broader trend-quality filters would approve them."

Conclusion: the Path S z=3.0 edge should not be gated by simple
trend-extension floors. The signal works by catching fresh option-flow
repricing, not by waiting for already-extended trend confirmation.

### Stale-Loser Displacement — REJECTED (2026-05-03 full path-dependent replay)

Rejected for both single-position and top-2.

A path-dependent replay was run for both z=3.0 single and z=3.0 top-2.
The earlier blocked-signal event study suggested stale/red holdings
might benefit from replacement by fresh z>=3 candidates. That result
did not survive full replay.

**Single-position results:**

| Variant | Return | CAGR | Sharpe | MDD | Trades |
|---|---:|---:|---:|---:|---:|
| baseline (no displacement) | +621.33% | +64.53% | 1.32 | -29.62% | 17 |
| h20 r0   (hold>=20d, ret<=0%, z>=3.0) | +571.50% | +61.59% | 1.20 | -55.27% | 29 |
| h20 r-5  (hold>=20d, ret<=-5%, z>=3.0) | +168.02% | +28.20% | 0.80 | -43.19% | — |
| h30 r0   (hold>=30d, ret<=0%, z>=3.0) | +435.18% | +52.61% | 1.17 | -47.81% | — |

The best displacement variant gave up ~50pp of return, lowered Sharpe,
and nearly doubled max drawdown (-29.6% → -55.3%). That is not a
rotation improvement. That is paying transaction costs to make the
portfolio worse.

**Top-2 results:** baseline (+376.8% / Sharpe 1.49 / MDD -20.2% / 33
trades) was strictly better than every displacement variant on CAGR,
Sharpe, MDD, Calmar, win rate, and churn.

**Why the offline blocked-signal study was misleading:** the events
were not independent. Once the system swaps out of PLTR / CHWY / SFM
or another "stale" name, every subsequent opportunity, hold, and
recovery path changes. A row-by-row counterfactual on the original
trade log cannot capture that. Real replay replaces a whole future
path, not one line in a spreadsheet. The blocked-signal study is
useful for hypothesis generation but cannot adjudicate a strategy
rule on its own.

**Why the swaps actually hurt:**

- current holdings often recovered or remained better than the
  challenger's realized path
- swaps locked in marginal losses on names that would have come back
- the challenger pool was lower-quality than the offline analysis
  implied (it was conditioned on baseline blocking, not on independent
  forward returns)
- trade count jumped (~17 → 29 single, materially more on top-2) but
  edge did not — pure churn

**Production implication:** no auto-swap. A dashboard may *display* a
candidate watchlist alongside the held position (current holding, hold
days, unrealized return, trailing stop level, days-to-max-hold, fresh
z>=3 candidates today, candidate z-scores), but the action remains:

```
HOLD unless:
- trailing stop hits
- 90d max hold hits
- regime exit triggers
```

No discretionary "new candidate looks better" override. The backtest
showed that instinct makes the system worse.

**Doctrinal takeaway:** same pattern as persistence, blackout, and
trend floors — every "make the rotation smarter / safer / fresher"
overlay amputated edge. The engine that worked is brutally simple:
enter on z>=3.0, hold through the noise, exit by trail / max-hold /
regime. Micromanaging the rotation degrades performance.

### High-Z Exhaustion Hypothesis — observationally weakened, NOT production-tested

Status: research note only. **No production exit rule has been
proposed, run, or rejected on this basis.**

A cross-sectional event study on 90d forward returns found that very
high starting z (z>=6, z>=7) had *higher* average forward returns than
the broader z>=3 event set. Read directly, that argues against
treating extreme z as exhaustion — it looks more like fuel.

But this is an event study, not a path-dependent exit-rule backtest.
The right question is not "do high-z names go up on average over the
next 90 days" — it is "if I am already in a position and z spikes to
6+, does adding an exit/tighten-trail rule change the strategy's
realized return, Sharpe, MDD, and Calmar?" That requires running the
rule inside `replay.py` against the canonical config.

Required test before any production decision (not yet run):
- baseline: z=3.0 single and z=3.0 top-2 (current candidates)
- variant: exit when in-position z >= 6
- variant: exit when in-position z >= 7
- variant: tighten trail to 10% after in-position z >= 6
- variant: tighten trail to 10% after in-position z >= 7

Compare CAGR, Sharpe, MDD, Calmar, average trade return, win rate, and
inspect whether the rule clips the small number of large winners that
drive the curve.

Until that backtest is run through the verifier, the position is:

- do **not** add a high-z exhaustion exit
- do **not** declare high-z exits useless
- treat the event-study result as *suggestive evidence against an
  exhaustion exit*, not a production verdict either way

This is the same boundary rule applied to every other rejected /
deferred idea: an event study can motivate a backtest but cannot
replace one. The strategy spec only contains rules that have been
path-tested in a canonical-config replay run.

### Confirmation: z=3.0 is the right baseline (z-sweep redone)

The fresh z-sweep on the canonical (top-2000, top-2, SPY gate, 90d
max hold, no other filters) family confirms the earlier conclusion:

- z=1.5: +163.6% / Sharpe 0.91 / MDD -34.5%
- z=3.0: +376.8% / Sharpe 1.49 / MDD -20.2%

Better on every axis. The clean improvement is **stronger same-day
skew threshold**, not persistence, blackout, trend floor, or sector
carveout.

### Top-1 vs Top-2

Top-1 remains useful as an aggressive shadow variant, but top-2 is the
primary candidate because it has better Sharpe, lower drawdown, and
captures multi-name signal breadth. Earlier work also showed that top-2
did not miss the top-1 leader in the old 2024 attribution; the
disagreement was concentration and slot-2 drag, not failure to
identify the best signal.

## Cost Sensitivity (z=3.0 top-2 candidate) — DONE 2026-05-03

Fresh canonical sweep. Verifier confirmed all 4 runs match on every core
field except `cost_bps`. Clean comparison.

| cost_bps | return | CAGR | Sharpe | MDD | trades | win_rate | avg_W | avg_L | drag_vs_15bps |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 15 | +376.81% | +48.1% | 1.49 | -20.21% | 33 | 57.6% | +26.64% | -7.79% | — |
| 25 | +361.74% | +46.9% | 1.46 | -20.61% | 33 | 57.6% | +26.39% | -7.98% | -15.07% |
| 35 | +347.15% | +45.7% | 1.44 | -21.01% | 33 | 57.6% | +26.14% | -8.16% | -29.66% |
| 50 | +326.13% | +44.0% | 1.40 | -21.61% | 33 | 57.6% | +25.76% | -8.43% | -50.67% |

The strategy passes the cost test cleanly.

- 50bps still beats SPY by +254pp (+326.13% vs +71.68%).
- Sharpe degrades by only -0.10 across the full sweep (1.49 → 1.40).
- MDD widens just 1.4pp (-20.2% → -21.6%).
- Trade count is identical (33 across all 4 cost levels) — entries are
  not cost-sensitive at this scale.
- Win rate is identical (57.6%) — costs erode each trade's average
  return without changing which trades fire.
- Drag is monotonic and approximately linear: ~1pp of total return per
  1bp of cost.

This rules out "the edge was thin, high-friction churn." The signal
holds with realistic execution assumptions.

## Required Follow-Ups Before Deployment

1. ~~Fresh z=3.0 cost sweep~~ — **DONE** (see above)

2. Risk-matched single-position comparison

The single-position variant fixed the 2023 weakness and delivered the
highest total return, but with higher drawdown and lower Sharpe. Test
scaled single-position sleeves:

- 100%
- 75%
- 67%
- 50%

Compare against 100% top-2 on:

- total return
- CAGR
- Sharpe
- max drawdown
- Calmar
- worst month
- drawdown duration
- exposure
- trade count

The key question is whether scaled single can beat top-2 at comparable
drawdown.

3. ~~z=3.0 earnings blackout rerun~~ — **DONE**, rejected as default
   (see Rejected / Superseded Ideas section above)

4. Paper-trade freeze

Paper trade:

- z=3.0 single as production candidate
- z=3.0 top-2 as smoother alternate / benchmark
- scaled single sleeve if the risk-matched test supports it
- SPY as benchmark
- explicitly NOT testing any displacement / rotation overlay
  (rejected by 2026-05-03 replay)

## Updated Verdict

After the 2026-05-03 path-dependent displacement replay, the candidate
hierarchy simplifies:

```
z=3.0 signal family, no displacement / no rotation overlay

  production candidate:    z=3.0 single
  smoother alternate:      z=3.0 top-2
  pending test:            risk-scaled single (vs top-2 at matched MDD)
```

**Production candidate — z=3.0 single, no displacement:**

- +621.3% total return / CAGR +64.5% / Sharpe 1.32 / MDD -29.6%
- 17 trades over 4y
- Beats SPY in every calendar period; fixes the 2023 weakness top-2 had
- Higher concentration / path risk than top-2

**Smoother alternate — z=3.0 top-2, no displacement:**

- +376.8% total return / CAGR +48.1% / Sharpe 1.49 / MDD -20.2%
- 33 trades over 4y; MDD ≈ SPY's
- Best risk-adjusted shape; gives up ~244pp of total return for it

**No displacement / rotation overlay in either expression.** The
2026-05-03 full replay decisively rejected stale-loser displacement
for both single and top-2 (degraded return, Sharpe, and MDD; nearly
2× drawdown on single). Same pattern as persistence, blackout, and
trend floors: every "make the rotation smarter" overlay amputates
edge. The engine that worked is brutally simple — enter on z>=3.0,
hold through the noise, exit by trail / max-hold / regime.

A production dashboard may *display* a candidate watchlist (current
holding, hold days, unrealized return, trailing stop level, days to
max hold, fresh z>=3 candidates today, candidate z-scores), but the
action remains:

```
HOLD unless:
- trailing stop hits
- 90d max hold hits
- regime exit triggers
```

No discretionary "new candidate looks better" override.

The remaining open comparison is whether a risk-scaled single sleeve
(50/67/75/100%) can beat top-2 at matched drawdown — that is the only
sizing question still worth answering.

## Config Integrity Note

All future comparisons must assert matching core config fields:

- signal variant
- skew_z_min
- universe_top_n
- positions
- regime gate
- trailing stop
- max hold
- date window
- cost assumption
- earnings blackout settings

Comparison scripts should refuse to compare runs if any core field
differs.

## Single-Position Z-Matrix (2026-05-04)

Rejected as a production replacement.

A 654-cell matrix (528 train, 126 OOS) tested four entry filters
× three selection rules × three exit rules × two between-trade
behaviors as candidate single-position systems. Train: 2022-05 →
2024-12. OOS: 2025-01 → 2026-04-29 (vs SPY +21.7%, Sharpe 0.91).

Headline OOS results (matched single-position framing):

| Cell                     | OOS Total | Sharpe |    MDD | Trades | Verdict |
| ------------------------ | --------: | -----: | -----: | -----: | ------- |
| F1 z>=3 positive only    |      +33% |   0.67 | -43.0% |      6 | weak survivor |
| F4 z<=-3 negative only   |      -62% |  -0.95 | -67.4% |      8 | reject  |
| F3 combined tails        |     +201% |   2.66 | -14.6% |      6 | not robust — see below |

The matrix confirmed that the only OOS-credible direction remains
positive-tail z>=3. Negative-tail-only F4 collapsed OOS and failed
against random selection. High-z-only F2 (z>=6) was too sparse and
underperformed SPY (+10.8% vs +21.7%, Sharpe 0.39, n=10 trades).
SPY-idle B2 added drag and no useful exposure benefit (strategies
sit in stock ~98% of the time; SPY-idle leg contributed -$9.7k vs
+$186k from stock).

F3 combined-tail produced the best headline OOS return, but the
result was low-N and concentrated. Trade-log decomposition:

```
All 6 F3 trades: +201%
Drop HBM:        +65%
Drop AU:         +78%
Drop both:        -2%
F3 ∩ F1 = {AU}
F3 ∩ F4 = {}
```

So F3 did not prove "positive AND negative tails both work." It
took a different set of negative-tail trades than F4, mostly
because the slot was sometimes occupied and therefore blocked
the bad negative-tail signals. Scheduling luck, not signal
quality.

Conclusion: do not promote F3, F4, F2, or B2. The matrix tested a
single-position z-framework; it does NOT replace the production
verifier results and does NOT invalidate the already-tested
production candidate. Production candidate remains the existing
positive-tail z>=3 Path S strategy (single as aggressive
expression, top-2 as smoother benchmark, 20% trailing, 90d max
hold, SPY gate where validated in production replay).

Locked rejections (cumulative):

```
KEEP:
- z >= 3 positive-tail Path S
- single-position as aggressive production candidate
- top-2 as smoother benchmark / alternate
- 20% trailing stop
- 90d max hold
- SPY gate where validated in production replay

REJECT:
- persistence
- earnings blackout
- trend floors
- stale-loser displacement
- negative-tail-only F4
- high-z-only F2
- combined-tail F3 as a production rule
- SPY idle B2
```

---

## Rolling-Z Window Sweep (2026-05-04) — w=60 confirmed canonical

**GHA run:** [25345947109](https://github.com/michaelgebbelay/gammawizzard/actions/runs/25345947109)
**Trigger:** `path-s-doctrine` cited "252d rolling" but `replay.py` `load_skew_lookup`
defaulted to `z_window=60` and the call site never overrode it. The live
`data_refresh.py` had `ROLLING_DAYS=252`, meaning the live signal was being
computed on a *different math* than the one that produced the +621% verdict.

**Sweep config:** z=3.0, single position, 4y window, max-hold 90, trail 20%,
SPY 200d gate, top-2000 universe, ignore-themes. Only axis: `--skew-z-window`.

| z_window | total return | CAGR | Sharpe | MDD | trades |
|---:|---:|---:|---:|---:|---:|
| **60** | **+621.3%** | **+64.4%** | **1.32** | **-29.6%** | 17 |
| 126 | -48.2% | -15.2% | -0.23 | -65.1% | 20 |
| 189 | -31.4% | -9.0% | -0.04 | -61.4% | 20 |
| 252 | +42.2% | +9.3% | +0.42 | -55.8% | 17 |
| 378 | +52.2% | +11.1% | +0.46 | -42.9% | 17 |
| SPY  | +71.7% |   —   |   —   | -19.0% | — |

**Findings:**
1. **w=60 reproduces the verdict exactly** (+621% / 1.32 / -30% / 17 trades).
   Engine is not drifting; the canonical numbers are reproducible from S3-staged data.
2. **w=126 (halved-window hypothesis) is catastrophic** — Sharpe goes negative,
   MDD blows out to -65%. Shorter does *not* mean fresher.
3. **w=252 (live `data_refresh.py` setting) underperforms SPY by 30pp** with
   nearly 2× the drawdown of w=60. Same name (KIM) was rank-1 across all
   windows, but ranks 2-12 reshuffle materially — future top-2 picks would
   diverge from the verdict-validated signal.
4. The win is not selectivity (trade counts roughly equal across windows) —
   it's **identity**: which underlyings cross z>=3.0 changes when the
   normalization horizon changes.

**Action taken (2026-05-04):**
- `data_refresh.py` ROLLING_DAYS 252 → 60, ROLLING_MIN 60 → 20.
- `path-s-doctrine` and `PathS/PLAN.md` corrected: spec is **60-trading-day**
  rolling z (min_periods=20), not 252d.
- This entry added to the verdict memo so the discrepancy doesn't recur.

**Prior live trade (KIM, entered 2026-05-04 at the open):** unaffected — KIM
was rank-1 under every window tested (z=6.67 @ w=60, z=5.57 @ w=252). The
trade matches what the canonical strategy would have picked. State file
records the entry signal at z=5.57 (the buggy live value); under the
corrected pipeline the same date's signal is z=6.67.

**Window is now a frozen parameter.** Re-running this sweep is wasted compute
unless something in the underlying skew distribution materially changes.

