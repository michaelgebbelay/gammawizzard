#!/usr/bin/env python3
"""Chain collection script — run via cron on trading days.

Usage:
    python -m sim.data.collect_chain open
    python -m sim.data.collect_chain mid
    python -m sim.data.collect_chain close

    # Force TastyTrade source:
    python -m sim.data.collect_chain open --source tt

Crontab (ET timezone — adjust for your local TZ):
    31 9  * * 1-5  ... python -m sim.data.collect_chain open       # 9:31 ET
    0  12 * * 1-5  ... python -m sim.data.collect_chain mid        # 12:00 ET
    0  16 * * 1-5  ... python -m sim.data.collect_chain close      # 16:00 ET
    5  16 * * 1-5  ... python -m sim.data.collect_chain close5     # 16:05 ET

Each run:
1. Fetches the full SPX options chain via TastyTrade (default) or Schwab API
2. Fetches current VIX quote
3. Fetches VIX1D + realized vol from GammaWizard API
4. Validates PM-settled SPXW expirations are present
5. Saves raw JSON + GW data to sim/cache/YYYY-MM-DD/{phase}.json
6. Logs status for contiguity tracking
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import date, datetime

# Ensure repo root is on path
_repo_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)

from sim.data.cache import cache_path, save_to_cache, has_complete_day, list_cached_dates

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


def collect_tt(phase: str) -> bool:
    """Fetch and cache the SPX chain from TastyTrade + GW market data."""
    from sim.data.tt_market_data import fetch_tt_spx_chain
    from sim.data.chain_snapshot import parse_tt_chain
    from sim.data.features import enrich, save_feature_pack
    from sim.data.gw_client import fetch_gw_data, save_gw_data

    today = date.today()
    today_str = today.isoformat()
    log.info("Collecting SPX chain via TastyTrade: date=%s, phase=%s", today, phase)

    path = cache_path(today, phase)
    if path.exists():
        log.info("Already cached: %s", path)
        return True

    try:
        raw = fetch_tt_spx_chain(phase=phase)
        if raw is None:
            log.error("TT chain fetch returned None")
            return False

        # Validate by parsing
        snapshot = parse_tt_chain(raw)

        if not snapshot.contracts:
            log.error("No contracts in TT chain response!")
            return False

        if not snapshot.expirations:
            log.error("No expirations in TT chain response!")
            return False

        # --- Diagnostic logging ---
        _log_chain_diagnostics(snapshot)

        log.info(
            "Chain stats: %d contracts, %d strikes, SPX=%.2f, VIX=%.1f, exp=%s",
            len(snapshot.contracts), len(snapshot.strikes),
            snapshot.underlying_price, snapshot.vix, snapshot.expirations,
        )

        # Save raw chain to cache
        saved_path = save_to_cache(today, phase, raw, raw.get("_vix", 0.0))
        log.info("Saved to: %s", saved_path)

        # Fetch GW data (VIX1D + RV) — non-fatal if unavailable
        gw_data = None
        try:
            gw_data = fetch_gw_data(today_str)
            if gw_data:
                save_gw_data(today_str, phase, gw_data)
                log.info("GW data: VIX1D=%s, RV=%s, RV5=%s",
                         gw_data.get("vix_1d"), gw_data.get("rv"),
                         gw_data.get("rv5"))
            else:
                log.warning("GW data unavailable for %s (non-fatal)", today_str)
        except Exception as e:
            log.warning("GW fetch failed (non-fatal): %s", e)

        # Compute + persist FeaturePack alongside snapshot
        prev_close = raw.get("spx_prev_close", 0.0)
        fp = enrich(snapshot, prev_close=prev_close, gw_data=gw_data)
        fp_path = save_feature_pack(today, phase, fp)
        log.info("FeaturePack saved to: %s (EM=%.1f, ATM IV=%.1f%%)",
                 fp_path, fp.atm_straddle_mid, fp.iv_atm)

        # Log contiguity status
        cached = list_cached_dates()
        complete = [d for d in cached if has_complete_day(d)]
        log.info("Total cached dates: %d, complete days: %d", len(cached), len(complete))

        return True

    except Exception as e:
        log.error("TT collection failed: %s", e, exc_info=True)
        return False


def _log_chain_diagnostics(snapshot) -> None:
    """Log data quality diagnostics for the chain snapshot."""
    from sim.config import CHAIN_ATM_WINDOW

    contracts = list(snapshot.contracts.values())
    total = len(contracts)
    if total == 0:
        return

    # Filter to ATM window for focused diagnostics
    atm = snapshot.atm_strike()
    nearby = [c for c in contracts if abs(c.strike - atm) <= CHAIN_ATM_WINDOW * 5]
    nearby_n = len(nearby)

    # Count non-null fields across nearby contracts
    has_delta = sum(1 for c in nearby if c.delta != 0)
    has_gamma = sum(1 for c in nearby if c.gamma != 0)
    has_iv = sum(1 for c in nearby if c.implied_vol > 0)
    has_oi = sum(1 for c in nearby if c.open_interest > 0)
    has_volume = sum(1 for c in nearby if c.volume > 0)
    has_bid = sum(1 for c in nearby if c.bid > 0)
    has_ask = sum(1 for c in nearby if c.ask > 0)

    log.info(
        "DIAGNOSTICS (ATM±%d, %d contracts): "
        "delta=%d/%d  gamma=%d/%d  IV=%d/%d  OI=%d/%d  "
        "volume=%d/%d  bid=%d/%d  ask=%d/%d",
        CHAIN_ATM_WINDOW, nearby_n,
        has_delta, nearby_n, has_gamma, nearby_n, has_iv, nearby_n,
        has_oi, nearby_n, has_volume, nearby_n, has_bid, nearby_n,
        has_ask, nearby_n,
    )

    # ATM pricing fallback tracking
    exp = snapshot.expirations[0] if snapshot.expirations else None
    for pc_label, pc in [("call", "C"), ("put", "P")]:
        c = snapshot.get_contract(atm, pc, exp)
        if c is None:
            log.warning("DIAGNOSTICS: ATM %s missing entirely", pc_label)
            continue
        mid = (c.bid + c.ask) / 2.0 if c.bid > 0 and c.ask > 0 else 0.0
        if mid > 0:
            source = "mid"
        elif c.mark > 0:
            source = "mark"
        elif c.bid > 0 and c.ask > 0:
            source = "(bid+ask)/2"
        else:
            source = "NULL"
        log.info("DIAGNOSTICS: ATM %s pricing source=%s (bid=%.2f ask=%.2f mark=%.2f)",
                 pc_label, source, c.bid, c.ask, c.mark)

    # PM-settled validation
    for exp_d in snapshot.expirations:
        log.info("DIAGNOSTICS: Expiration %s passes PM-settled filter", exp_d)


def collect_schwab(phase: str) -> bool:
    """Fetch and cache the SPX chain from Schwab (legacy fallback)."""
    from scripts.schwab_token_keeper import schwab_client
    from sim.data.chain_snapshot import parse_schwab_chain
    from datetime import timedelta

    today = date.today()
    log.info("Collecting SPX chain via Schwab: date=%s, phase=%s", today, phase)

    path = cache_path(today, phase)
    if path.exists():
        log.info("Already cached: %s", path)
        return True

    try:
        c = schwab_client()

        from_date = today
        to_date = today + timedelta(days=3)
        resp = c.get_option_chain(
            "$SPX",
            contract_type=c.Options.ContractType.ALL,
            strike_count=40,
            include_underlying_quote=True,
            from_date=from_date,
            to_date=to_date,
            option_type=c.Options.Type.ALL,
        )
        resp.raise_for_status()
        raw = resp.json()

        # Fetch VIX
        vix = 0.0
        try:
            vix_resp = c.get_quote("$VIX")
            vix_resp.raise_for_status()
            vix_data = vix_resp.json()
            for key, val in vix_data.items():
                if isinstance(val, dict):
                    q = val.get("quote", val)
                    v = q.get("lastPrice") or q.get("last") or q.get("mark")
                    if v is not None:
                        vix = float(v)
                        break
        except Exception as e:
            log.warning("VIX fetch failed (non-fatal): %s", e)

        snapshot = parse_schwab_chain(raw, phase=phase, vix=vix)

        if not snapshot.expirations:
            log.error("No PM-settled expirations found!")
            return False

        if not snapshot.contracts:
            log.error("No contracts after PM-settled filter!")
            return False

        log.info(
            "Chain stats: %d contracts, %d strikes, SPX=%.2f, VIX=%.2f",
            len(snapshot.contracts), len(snapshot.strikes),
            snapshot.underlying_price, vix,
        )

        saved_path = save_to_cache(today, phase, raw, vix)
        log.info("Saved to: %s", saved_path)

        cached = list_cached_dates()
        complete = [d for d in cached if has_complete_day(d)]
        log.info("Total cached dates: %d, complete days: %d", len(cached), len(complete))

        return True

    except Exception as e:
        log.error("Schwab collection failed: %s", e, exc_info=True)
        return False


def collect(phase: str, source: str = "tt") -> bool:
    """Collect chain from specified source, with fallback."""
    if source == "tt":
        ok = collect_tt(phase)
        if not ok:
            log.warning("TT failed, trying Schwab fallback...")
            return collect_schwab(phase)
        return ok
    elif source == "schwab":
        ok = collect_schwab(phase)
        if not ok:
            log.warning("Schwab failed, trying TT fallback...")
            return collect_tt(phase)
        return ok
    else:
        log.error("Unknown source: %s", source)
        return False


def main():
    valid_phases = ("open", "mid", "close", "close5")
    if len(sys.argv) < 2 or sys.argv[1] not in valid_phases:
        print(f"Usage: python -m sim.data.collect_chain <{'|'.join(valid_phases)}> [--source tt|schwab]")
        sys.exit(1)

    phase = sys.argv[1]
    source = "tt"  # Default to TastyTrade
    if "--source" in sys.argv:
        idx = sys.argv.index("--source")
        if idx + 1 < len(sys.argv):
            source = sys.argv[idx + 1]

    success = collect(phase, source=source)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
