# Conflict Review Summary

## GitHub Actions Workflows
- Reviewed all workflows under `.github/workflows/` (`gw_leocross_sheet.yml`, `leocross.yml`, `run_gw_inspect.yml`, `schwab_raw_data.yml`).
- No merge conflict markers or incompatible trigger definitions were found.

## Trading Scripts (`scripts/trade`)
- Inspected `leocross_guard.py`, `leocross_orchestrator.py`, and `leocross_place_simple.py` for merge conflict markers or divergent behaviors.
- Identified a sizing inconsistency:
  - `leocross_guard.py` and `leocross_place_simple.py` size short iron condors at **$4,000 per 5-wide** (scaled by width) and long iron condors at **$4,000 per condor**.
  - `leocross_orchestrator.py` previously used a different default (short IC: $12,000 flat per contract; long IC: $5,000), leading to mismatched remainder calculations.
- Updated `leocross_orchestrator.py` so that both short and long sizing match the guard/placer logic while still allowing environment overrides.

## Outcome
- Sizing decisions made by the orchestrator now align with the guard and placer, preventing contradictory workflow outputs.
- No other conflicts detected.
