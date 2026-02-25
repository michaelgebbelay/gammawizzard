"""Feature enrichment module — deterministic FeaturePack from a ChainSnapshot.

Computes all derived features agents/baselines/judge use for decisions.
Persisted alongside the snapshot so replay is identical.

Usage:
    from sim.data.features import enrich
    pack = enrich(chain, prev_close=5870.0)
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from sim.config import CACHE_DIR, SPX_MULTIPLIER
from sim.data.chain_snapshot import ChainSnapshot, OptionContract


# ---------------------------------------------------------------------------
# Pricing fallback: mid > mark > (bid+ask)/2 > None
# ---------------------------------------------------------------------------

def _mid(c: OptionContract) -> Optional[float]:
    """Contract mid with fallback chain: mid > mark > (bid+ask)/2 > None."""
    m = (c.bid + c.ask) / 2.0 if c.bid > 0 and c.ask > 0 else 0.0
    if m > 0:
        return m
    if c.mark > 0:
        return c.mark
    if c.bid > 0 and c.ask > 0:
        return (c.bid + c.ask) / 2.0
    return None


# ---------------------------------------------------------------------------
# Delta fallback: if greeks missing, approximate using moneyness bands
# ---------------------------------------------------------------------------

def _effective_delta(c: OptionContract, spot: float) -> float:
    """Return contract delta, or moneyness-based approximation if missing."""
    if c.delta != 0.0:
        return c.delta
    if spot <= 0:
        return 0.0
    # Rough approximation: delta ~ N(moneyness / sigma)
    # sigma_approx = ~1.5% per trading day.  For 0DTE this overstates
    # sigma (less time remaining) — acceptable for a fallback proxy.
    # TODO: scale with sqrt(trading_dte) when DTE context is threaded through.
    moneyness = (c.strike - spot) / spot
    sigma_approx = 0.015  # ~1.5% per trading day
    if c.put_call == "C":
        # Call delta decreases as strike rises above spot
        d = max(0.01, min(0.99, 0.5 - moneyness / (2 * sigma_approx)))
        return d
    else:
        # Put delta = call delta - 1
        d = max(0.01, min(0.99, 0.5 - moneyness / (2 * sigma_approx)))
        return d - 1.0


def _find_nearest_delta(contracts: List[OptionContract], target: float,
                        spot: float) -> Optional[OptionContract]:
    """Find contract with delta closest to target. Falls back to moneyness."""
    if not contracts:
        return None
    return min(contracts, key=lambda c: abs(_effective_delta(c, spot) - target))


# ---------------------------------------------------------------------------
# FeaturePack dataclass
# ---------------------------------------------------------------------------

@dataclass
class FeaturePack:
    """All derived features for one decision window."""

    # A) Spot / day context
    spot: float = 0.0
    prev_close: float = 0.0
    gap_pts: float = 0.0               # open - prev_close
    gap_pct: float = 0.0               # gap / prev_close * 100
    day_range_pts: float = 0.0         # high - low
    range_position_pct: Optional[float] = None  # (spot - low) / (high - low); null if high==low
    range_is_full_day: bool = False     # true only at close phase (complete range)
    range_source_phase: str = ""        # "open" or "close"
    spx_open: float = 0.0
    spx_high: float = 0.0
    spx_low: float = 0.0

    # B) ATM identification
    atm_strike: float = 0.0
    atm_call_mid: Optional[float] = None
    atm_put_mid: Optional[float] = None
    atm_straddle_mid: float = 0.0      # expected move in points

    # C) Expected move levels (points)
    em_0p5_up: float = 0.0
    em_1p0_up: float = 0.0
    em_0p5_dn: float = 0.0
    em_1p0_dn: float = 0.0
    em_0p5_up_strike: float = 0.0      # nearest available strike
    em_1p0_up_strike: float = 0.0
    em_0p5_dn_strike: float = 0.0
    em_1p0_dn_strike: float = 0.0

    # D) IV / skew surface
    iv_atm: float = 0.0                # mean of ATM call/put IV
    risk_reversal_25d: float = 0.0     # iv_call_25d - iv_put_25d
    put_skew_slope: float = 0.0        # OTM put IV regression slope
    call_skew_slope: float = 0.0       # OTM call IV regression slope
    wing_iv_spread: float = 0.0        # iv_put_10d - iv_put_25d

    # E) OI / gamma concentration
    oi_walls_puts: List[Tuple[float, int]] = field(default_factory=list)   # [(strike, OI), ...]
    oi_walls_calls: List[Tuple[float, int]] = field(default_factory=list)
    gex_peaks: List[Tuple[float, float]] = field(default_factory=list)     # [(strike, gex), ...]
    gex_total: float = 0.0
    put_call_oi_ratio: float = 0.0
    put_call_volume_ratio: float = 0.0
    total_put_oi: int = 0
    total_call_oi: int = 0
    total_put_volume: int = 0
    total_call_volume: int = 0

    # F) Term structure (requires both expirations)
    iv_term: Optional[float] = None    # iv_atm_1dte - iv_atm_0dte
    em_term: Optional[float] = None    # straddle_1dte - straddle_0dte

    # G) VIX1D + realized vol (sourced from GammaWizard API, not computed)
    vix_1d: Optional[float] = None     # CBOE VIX1D (1-day implied vol), decimal (e.g. 0.1424 = 14.24%)
    rv: Optional[float] = None         # realized vol (current)
    rv5: Optional[float] = None        # 5-day realized vol
    rv10: Optional[float] = None       # 10-day realized vol
    rv20: Optional[float] = None       # 20-day realized vol

    # VIX context
    vix: float = 0.0
    phase: str = ""
    window: str = ""                    # "open" or "close5"
    expiration: str = ""
    snapshot_date: str = ""             # ISO date of the snapshot

    # H) Versioning + provenance (v13)
    feature_pack_version: str = "2"     # schema version (bumped on field changes)
    data_source_chain: str = ""         # "tt" or "schwab"
    data_source_gw: bool = False        # True if GW data was injected
    gw_date_used: str = ""              # actual date from GW row (may differ from session)
    gw_match_mode: str = ""             # "exact" or "latest_fallback"
    gw_fields_gated: bool = False       # True if anti-lookahead gating removed RV fields

    def to_dict(self) -> dict:
        """Serialize to JSON-safe dict."""
        d = asdict(self)
        # Tuple lists need conversion
        d["oi_walls_puts"] = [list(t) for t in self.oi_walls_puts]
        d["oi_walls_calls"] = [list(t) for t in self.oi_walls_calls]
        d["gex_peaks"] = [list(t) for t in self.gex_peaks]
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "FeaturePack":
        """Deserialize from dict."""
        d = dict(d)
        d["oi_walls_puts"] = [tuple(t) for t in d.get("oi_walls_puts", [])]
        d["oi_walls_calls"] = [tuple(t) for t in d.get("oi_walls_calls", [])]
        d["gex_peaks"] = [tuple(t) for t in d.get("gex_peaks", [])]
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


# ---------------------------------------------------------------------------
# Core enrichment function
# ---------------------------------------------------------------------------

def enrich(chain: ChainSnapshot,
           prev_close: float = 0.0,
           other_expiry_chain: Optional[ChainSnapshot] = None,
           gw_data: Optional[Dict] = None,
           window: str = "") -> FeaturePack:
    """Compute FeaturePack from a ChainSnapshot. Deterministic.

    Args:
        chain: The parsed chain for the decision window.
        prev_close: Previous session SPX close (for gap). Falls back to chain.spx_prev_close.
        other_expiry_chain: Optional second expiry chain for term structure.
        gw_data: Optional dict from GammaWizard API with VIX1D + RV fields.
                 Keys: vix_1d, rv, rv5, rv10, rv20 (all float, decimal form).
                 Should be pre-gated via gate_gw_for_window() for anti-lookahead.
        window: Decision window ("open" or "close5").

    Returns:
        FeaturePack with all computed features.
    """
    fp = FeaturePack()
    fp.vix = chain.vix
    fp.phase = chain.phase
    fp.window = window or chain.phase
    fp.expiration = chain.expirations[0].isoformat() if chain.expirations else ""
    fp.snapshot_date = chain.timestamp.date().isoformat()
    fp.data_source_chain = "tt" if "tt" in chain.underlying_symbol.lower() or chain.spx_open > 0 else "schwab"

    # --- G) VIX1D + RV (populated early — independent of chain contracts) ---
    if gw_data:
        fp.vix_1d = gw_data.get("vix_1d")
        fp.rv = gw_data.get("rv")
        fp.rv5 = gw_data.get("rv5")
        fp.rv10 = gw_data.get("rv10")
        fp.rv20 = gw_data.get("rv20")
        fp.data_source_gw = True
        fp.gw_date_used = gw_data.get("date", "")
        session_date = fp.snapshot_date
        if fp.gw_date_used == session_date:
            fp.gw_match_mode = "exact"
        elif fp.gw_date_used:
            fp.gw_match_mode = "latest_fallback"
        if fp.vix_1d is not None and all(
            gw_data.get(k) is None for k in ("rv", "rv5", "rv10", "rv20")
        ):
            fp.gw_fields_gated = True

    if not chain.contracts or not chain.strikes:
        return fp

    exp = chain.expirations[0] if chain.expirations else None
    spot = chain.underlying_price
    fp.spot = spot

    # --- A) Spot / day context ---
    fp.spx_open = chain.spx_open
    fp.spx_high = chain.spx_high
    fp.spx_low = chain.spx_low
    pc = prev_close if prev_close > 0 else chain.spx_prev_close
    fp.prev_close = pc

    if pc > 0 and chain.spx_open > 0:
        fp.gap_pts = chain.spx_open - pc
        fp.gap_pct = fp.gap_pts / pc * 100.0
    elif pc > 0:
        # No open price yet — use current spot vs prev close
        fp.gap_pts = spot - pc
        fp.gap_pct = fp.gap_pts / pc * 100.0

    if chain.spx_high > 0 and chain.spx_low > 0:
        fp.day_range_pts = chain.spx_high - chain.spx_low
        fp.range_source_phase = chain.phase
        fp.range_is_full_day = chain.phase in ("close", "close5")
        # Range position: null when high==low (zero range), otherwise 0=low, 1=high
        if fp.day_range_pts > 0:
            fp.range_position_pct = (spot - chain.spx_low) / fp.day_range_pts

    # --- B) ATM identification ---
    fp.atm_strike = chain.atm_strike()
    atm_call = chain.get_contract(fp.atm_strike, "C", exp)
    atm_put = chain.get_contract(fp.atm_strike, "P", exp)

    if atm_call:
        fp.atm_call_mid = _mid(atm_call)
    if atm_put:
        fp.atm_put_mid = _mid(atm_put)

    if fp.atm_call_mid is not None and fp.atm_put_mid is not None:
        fp.atm_straddle_mid = fp.atm_call_mid + fp.atm_put_mid

    # --- C) Expected move levels ---
    if fp.atm_straddle_mid > 0 and spot > 0:
        half = 0.5 * fp.atm_straddle_mid
        full = fp.atm_straddle_mid

        fp.em_0p5_up = spot + half
        fp.em_1p0_up = spot + full
        fp.em_0p5_dn = spot - half
        fp.em_1p0_dn = spot - full

        # Map to nearest available strikes (conservative: round toward ATM)
        fp.em_0p5_up_strike = _snap_strike_conservative(chain.strikes, fp.em_0p5_up, direction="up")
        fp.em_1p0_up_strike = _snap_strike_conservative(chain.strikes, fp.em_1p0_up, direction="up")
        fp.em_0p5_dn_strike = _snap_strike_conservative(chain.strikes, fp.em_0p5_dn, direction="down")
        fp.em_1p0_dn_strike = _snap_strike_conservative(chain.strikes, fp.em_1p0_dn, direction="down")

    # --- D) IV / skew surface ---
    _compute_iv_skew(fp, chain, exp, spot)

    # --- E) OI / gamma concentration ---
    _compute_oi_gamma(fp, chain, exp, spot)

    # --- F) Term structure ---
    if other_expiry_chain is not None:
        _compute_term_structure(fp, chain, other_expiry_chain)

    return fp


# ---------------------------------------------------------------------------
# Section computers
# ---------------------------------------------------------------------------

def _snap_strike_conservative(strikes: List[float], target: float,
                              direction: str) -> float:
    """Snap target to nearest available strike, rounding toward ATM.

    For 'up' targets: round down (conservative = closer to ATM).
    For 'down' targets: round up (conservative = closer to ATM).
    """
    if not strikes:
        return target

    if direction == "up":
        # Find largest strike <= target, or nearest if none
        below = [s for s in strikes if s <= target]
        return max(below) if below else min(strikes, key=lambda s: abs(s - target))
    else:
        # Find smallest strike >= target, or nearest if none
        above = [s for s in strikes if s >= target]
        return min(above) if above else min(strikes, key=lambda s: abs(s - target))


def _compute_iv_skew(fp: FeaturePack, chain: ChainSnapshot,
                     exp, spot: float):
    """Compute IV/skew features."""
    calls = chain.calls(exp)
    puts = chain.puts(exp)

    # ATM IV
    atm_call = chain.get_contract(fp.atm_strike, "C", exp)
    atm_put = chain.get_contract(fp.atm_strike, "P", exp)
    ivs = []
    if atm_call and atm_call.implied_vol > 0:
        ivs.append(atm_call.implied_vol)
    if atm_put and atm_put.implied_vol > 0:
        ivs.append(atm_put.implied_vol)
    if ivs:
        fp.iv_atm = sum(ivs) / len(ivs)

    # 25-delta risk reversal
    call_25d = _find_nearest_delta(calls, 0.25, spot)
    put_25d = _find_nearest_delta(puts, -0.25, spot)
    if call_25d and put_25d:
        c_iv = call_25d.implied_vol if call_25d.implied_vol > 0 else 0.0
        p_iv = put_25d.implied_vol if put_25d.implied_vol > 0 else 0.0
        if c_iv > 0 and p_iv > 0:
            fp.risk_reversal_25d = c_iv - p_iv

    # Wing IV spread: iv_put_10d - iv_put_25d
    put_10d = _find_nearest_delta(puts, -0.10, spot)
    if put_10d and put_25d:
        p10_iv = put_10d.implied_vol if put_10d.implied_vol > 0 else 0.0
        p25_iv = put_25d.implied_vol if put_25d.implied_vol > 0 else 0.0
        if p10_iv > 0 and p25_iv > 0:
            fp.wing_iv_spread = p10_iv - p25_iv

    # Put skew slope: OTM puts from spot-25 to spot-100 in $5 increments
    fp.put_skew_slope = _iv_slope_regression(
        puts, spot,
        strike_lo=spot - 100, strike_hi=spot - 25,
        otm_side="put"
    )

    # Call skew slope
    fp.call_skew_slope = _iv_slope_regression(
        calls, spot,
        strike_lo=spot + 25, strike_hi=spot + 100,
        otm_side="call"
    )


def _iv_slope_regression(contracts: List[OptionContract], spot: float,
                         strike_lo: float, strike_hi: float,
                         otm_side: str) -> float:
    """OLS regression of IV vs strike within [strike_lo, strike_hi].

    Requires >= 5 data points with non-zero IV.
    Returns slope (IV change per 1 strike point).
    """
    points: List[Tuple[float, float]] = []
    for c in contracts:
        if c.implied_vol <= 0:
            continue
        if c.strike < strike_lo or c.strike > strike_hi:
            continue
        points.append((c.strike, c.implied_vol))

    if len(points) < 5:
        return 0.0

    n = len(points)
    x_mean = sum(p[0] for p in points) / n
    y_mean = sum(p[1] for p in points) / n

    num = sum((x - x_mean) * (y - y_mean) for x, y in points)
    den = sum((x - x_mean) ** 2 for x, _ in points)

    if den == 0:
        return 0.0
    return num / den


def _compute_oi_gamma(fp: FeaturePack, chain: ChainSnapshot,
                      exp, spot: float):
    """Compute OI walls and gamma concentration."""
    puts = chain.puts(exp)
    calls = chain.calls(exp)

    # Filter to ±200 points of spot
    nearby_puts = [c for c in puts if abs(c.strike - spot) <= 200]
    nearby_calls = [c for c in calls if abs(c.strike - spot) <= 200]

    # OI walls: top 3 strikes by OI
    if nearby_puts:
        sorted_puts = sorted(nearby_puts, key=lambda c: c.open_interest, reverse=True)
        fp.oi_walls_puts = [(c.strike, c.open_interest) for c in sorted_puts[:3] if c.open_interest > 0]
        fp.total_put_oi = sum(c.open_interest for c in nearby_puts)
        fp.total_put_volume = sum(c.volume for c in nearby_puts)

    if nearby_calls:
        sorted_calls = sorted(nearby_calls, key=lambda c: c.open_interest, reverse=True)
        fp.oi_walls_calls = [(c.strike, c.open_interest) for c in sorted_calls[:3] if c.open_interest > 0]
        fp.total_call_oi = sum(c.open_interest for c in nearby_calls)
        fp.total_call_volume = sum(c.volume for c in nearby_calls)

    # Ratios
    if fp.total_call_oi > 0:
        fp.put_call_oi_ratio = fp.total_put_oi / fp.total_call_oi
    if fp.total_call_volume > 0:
        fp.put_call_volume_ratio = fp.total_put_volume / fp.total_call_volume

    # GEX by strike: |OI * gamma * multiplier| per strike
    gex_by_strike: Dict[float, float] = {}
    for c in chain.contracts.values():
        if exp is not None and c.expiration != exp:
            continue
        if abs(c.strike - spot) > 200:
            continue
        gex = abs(c.gamma) * c.open_interest * SPX_MULTIPLIER
        gex_by_strike[c.strike] = gex_by_strike.get(c.strike, 0.0) + gex

    fp.gex_total = sum(gex_by_strike.values())

    # Top 3 GEX peaks
    if gex_by_strike:
        sorted_gex = sorted(gex_by_strike.items(), key=lambda kv: kv[1], reverse=True)
        fp.gex_peaks = [(s, g) for s, g in sorted_gex[:3] if g > 0]


def _compute_term_structure(fp: FeaturePack, chain: ChainSnapshot,
                            other: ChainSnapshot):
    """Compute term structure between two expiry chains."""
    if not other.contracts or not other.strikes:
        return

    # ATM IV of the other expiry
    other_atm = other.atm_strike()
    other_exp = other.expirations[0] if other.expirations else None
    other_call = other.get_contract(other_atm, "C", other_exp)
    other_put = other.get_contract(other_atm, "P", other_exp)

    other_iv = 0.0
    ivs = []
    if other_call and other_call.implied_vol > 0:
        ivs.append(other_call.implied_vol)
    if other_put and other_put.implied_vol > 0:
        ivs.append(other_put.implied_vol)
    if ivs:
        other_iv = sum(ivs) / len(ivs)

    # Term structure: 1DTE IV - 0DTE IV
    if fp.iv_atm > 0 and other_iv > 0:
        fp.iv_term = other_iv - fp.iv_atm

    # Straddle term structure
    other_straddle = 0.0
    if other_call and other_put:
        cm = _mid(other_call)
        pm = _mid(other_put)
        if cm is not None and pm is not None:
            other_straddle = cm + pm

    if fp.atm_straddle_mid > 0 and other_straddle > 0:
        fp.em_term = other_straddle - fp.atm_straddle_mid


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def save_feature_pack(trading_date: date, phase: str, fp: FeaturePack) -> Path:
    """Save FeaturePack to cache alongside the snapshot.

    Path: sim/cache/YYYY-MM-DD/{phase}_features.json
    """
    d = trading_date.isoformat() if isinstance(trading_date, date) else trading_date
    path = CACHE_DIR / d / f"{phase}_features.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(fp.to_dict(), f)
    return path


def load_feature_pack(trading_date: date, phase: str) -> Optional[FeaturePack]:
    """Load a persisted FeaturePack, or None if not cached."""
    d = trading_date.isoformat() if isinstance(trading_date, date) else trading_date
    path = CACHE_DIR / d / f"{phase}_features.json"
    if not path.exists():
        return None
    with open(path) as f:
        return FeaturePack.from_dict(json.load(f))


# ---------------------------------------------------------------------------
# Compact text format for agent context
# ---------------------------------------------------------------------------

def format_feature_pack(fp: FeaturePack) -> str:
    """Format FeaturePack as concise text for agent context window."""
    lines = []

    # Spot context
    if fp.prev_close > 0:
        lines.append(f"Gap: {fp.gap_pts:+.2f}pts ({fp.gap_pct:+.2f}%)")
    if fp.day_range_pts > 0:
        range_line = f"Day Range: {fp.day_range_pts:.2f}pts"
        # Only show range position at close (full day data)
        if fp.range_is_full_day and fp.range_position_pct is not None:
            range_line += f"  |  Close Pos: {fp.range_position_pct:.0%}"
        lines.append(range_line)

    # Expected move
    if fp.atm_straddle_mid > 0:
        em_pct = fp.atm_straddle_mid / fp.spot * 100 if fp.spot > 0 else 0
        lines.append(
            f"EM (straddle): +/-{fp.atm_straddle_mid:.2f}pts ({em_pct:.2f}%)"
        )
        lines.append(
            f"  0.5 EM: {fp.em_0p5_dn_strike:.0f} / {fp.em_0p5_up_strike:.0f}"
            f"  |  1.0 EM: {fp.em_1p0_dn_strike:.0f} / {fp.em_1p0_up_strike:.0f}"
        )

    # IV
    iv_parts = []
    if fp.iv_atm > 0:
        iv_parts.append(f"ATM IV: {fp.iv_atm:.1f}%")
    if fp.vix_1d is not None:
        iv_parts.append(f"VIX1D: {fp.vix_1d * 100:.1f}%")
    if iv_parts:
        lines.append("  |  ".join(iv_parts))

    # Realized vol
    rv_parts = []
    if fp.rv is not None:
        rv_parts.append(f"RV: {fp.rv * 100:.2f}%")
    if fp.rv5 is not None:
        rv_parts.append(f"RV5: {fp.rv5 * 100:.2f}%")
    if fp.rv10 is not None:
        rv_parts.append(f"RV10: {fp.rv10 * 100:.2f}%")
    if fp.rv20 is not None:
        rv_parts.append(f"RV20: {fp.rv20 * 100:.2f}%")
    if rv_parts:
        lines.append("  |  ".join(rv_parts))

    # Skew
    skew_parts = []
    if fp.risk_reversal_25d != 0:
        skew_parts.append(f"25d RR: {fp.risk_reversal_25d:+.2f}")
    if fp.put_skew_slope != 0:
        skew_parts.append(f"Put Slope: {fp.put_skew_slope:+.4f}")
    if fp.wing_iv_spread != 0:
        skew_parts.append(f"Wing Spread: {fp.wing_iv_spread:+.2f}")
    if skew_parts:
        lines.append("  |  ".join(skew_parts))

    # Term structure
    if fp.iv_term is not None:
        lines.append(f"IV Term (1DTE-0DTE): {fp.iv_term:+.2f}")
    if fp.em_term is not None:
        lines.append(f"EM Term (1DTE-0DTE): {fp.em_term:+.2f}pts")

    # OI walls
    wall_parts = []
    if fp.oi_walls_puts:
        top = fp.oi_walls_puts[0]
        wall_parts.append(f"Put Wall: {top[0]:.0f} ({top[1]:,})")
    if fp.oi_walls_calls:
        top = fp.oi_walls_calls[0]
        wall_parts.append(f"Call Wall: {top[0]:.0f} ({top[1]:,})")
    if fp.gex_peaks:
        top = fp.gex_peaks[0]
        wall_parts.append(f"GEX Peak: {top[0]:.0f}")
    if wall_parts:
        lines.append("  |  ".join(wall_parts))

    # Ratios
    ratio_parts = []
    if fp.put_call_oi_ratio > 0:
        ratio_parts.append(f"P/C OI: {fp.put_call_oi_ratio:.2f}")
    if fp.put_call_volume_ratio > 0:
        ratio_parts.append(f"P/C Vol: {fp.put_call_volume_ratio:.2f}")
    if ratio_parts:
        lines.append("  |  ".join(ratio_parts))

    return "\n".join(lines) if lines else "No features (skeleton chain)."
