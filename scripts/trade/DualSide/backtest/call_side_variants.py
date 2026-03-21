#!/usr/bin/env python3
"""
DualSide v2 backtest — all $10-wide, testing call-side variations.

Put side (fixed): $10 regime switch at 0.75*EM (6DTE)

Call side variants (all $10 wide, 5DTE):
  1) 50-delta BPC always (v1.0.0 baseline)
  2) 50-delta regime follow (BPC bull / BPD bear)
  3) 50-delta skip bear (BPC bull only)
  4) 0.75*EM call spread — always bull (buy call at anchor, sell anchor+10)
  5) 0.75*EM call spread — regime follow (long call debit bull / short call credit bear)
  6) 0.75*EM call spread — skip bear
  7) 0.75*EM call spread — always short (sell call credit)
"""
from __future__ import annotations

import csv
import json
import math
from collections import defaultdict
from pathlib import Path

import numpy as np
import yfinance as yf

from sim.config import CACHE_DIR, SPX_MULTIPLIER
from sim.data.chain_snapshot import (
    ChainSnapshot, OptionContract,
    parse_cboe_chain, parse_schwab_chain, parse_tt_chain,
)

SLIPPAGE = 0.20
COMMISSION = 0.97
EM_FACTOR = 0.75
WIDTH = 10

IV_MINUS_VIX_THRESH = -0.0502
RR25_THRESH = -0.9976
VIX1D_VETO_LO = 10.0
VIX1D_VETO_HI = 11.5
VIX_CALL_SKIP = 10.0
RV_BAND_LO = 0.70
RV_BAND_HI = 0.85

START_DATE = "2023-06-01"


def _load(path):
    try:
        with path.open() as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


def _parse(wrapper, phase):
    raw = wrapper.get("chain", wrapper)
    source = (raw.get("_source") or "").lower()
    if source == "tastytrade":
        return parse_tt_chain(raw)
    if source in ("cboe", "thetadata"):
        return parse_cboe_chain(raw)
    vix = raw.get("_vix", wrapper.get("vix", 0.0))
    return parse_schwab_chain(raw, phase, vix=vix)


def _norm_iv(chain):
    positive = [c.implied_vol for c in chain.contracts.values() if c.implied_vol > 0]
    nearby = [c.implied_vol for c in chain.contracts.values()
              if c.implied_vol > 0 and abs(c.strike - chain.underlying_price) <= 200]
    sample = nearby or positive
    if not sample or float(np.median(sample)) <= 3.0:
        return chain
    contracts = {}
    for sym, c in chain.contracts.items():
        iv = c.implied_vol / 100.0 if c.implied_vol > 0 else c.implied_vol
        contracts[sym] = OptionContract(
            symbol=c.symbol, strike=c.strike, expiration=c.expiration,
            put_call=c.put_call, bid=c.bid, ask=c.ask, last=c.last,
            mark=c.mark, volume=c.volume, open_interest=c.open_interest,
            implied_vol=iv, delta=c.delta, gamma=c.gamma, theta=c.theta,
            vega=c.vega, rho=c.rho, days_to_exp=c.days_to_exp,
            in_the_money=c.in_the_money,
        )
    return ChainSnapshot(
        timestamp=chain.timestamp, phase=chain.phase,
        underlying_price=chain.underlying_price,
        underlying_symbol=chain.underlying_symbol,
        vix=chain.vix, contracts=contracts,
        expirations=list(chain.expirations), strikes=list(chain.strikes),
        vix1d=chain.vix1d, spx_open=chain.spx_open, spx_high=chain.spx_high,
        spx_low=chain.spx_low, spx_prev_close=chain.spx_prev_close,
    )


def _mid(c):
    if c and c.bid > 0 and c.ask > 0:
        return (c.bid + c.ask) / 2.0
    if c and c.mark > 0:
        return c.mark
    if c and c.last > 0:
        return c.last
    return None


def compute_rv(closes, window):
    if len(closes) < window + 1:
        return None
    recent = closes[-(window + 1):]
    lr = [math.log(recent[i] / recent[i - 1]) for i in range(1, len(recent))]
    return float(np.std(lr, ddof=1) * math.sqrt(252) * 100)


def build_put_spread(chain, exp, ss, ls):
    sc = chain.get_contract(ss, "P", exp)
    lc = chain.get_contract(ls, "P", exp)
    sm = _mid(sc)
    lm = _mid(lc)
    if sm is not None and lm is not None and sm > 0 and lm > 0:
        return sm, lm
    return None, None


def build_call_spread(chain, exp, ss, ls):
    """Build call spread: ss = short call strike, ls = long call strike."""
    sc = chain.get_contract(ss, "C", exp)
    lc = chain.get_contract(ls, "C", exp)
    sm = _mid(sc)
    lm = _mid(lc)
    if sm is not None and lm is not None and sm > 0 and lm > 0:
        return sm, lm
    return None, None


def put_pnl(entry, is_credit, settle_spot, short_strike, long_strike):
    """P&L for a put vertical at settlement."""
    short_iv = max(0, short_strike - settle_spot)
    long_iv = max(0, long_strike - settle_spot)
    settle_val = long_iv - short_iv
    if is_credit:
        pnl_pts = entry + settle_val
    else:
        pnl_pts = settle_val - entry
    return pnl_pts * SPX_MULTIPLIER - COMMISSION


def call_pnl(entry, is_debit, settle_spot, long_strike, short_strike):
    """P&L for a call vertical at settlement.

    Debit (bull call debit): buy call at long_strike, sell call at short_strike (short > long).
    Credit (bear call credit): sell call at short_strike, buy call at long_strike (long > short).
    """
    long_iv = max(0, settle_spot - long_strike)
    short_iv = max(0, settle_spot - short_strike)
    settle_val = long_iv - short_iv  # value of our long - short
    if is_debit:
        pnl_pts = settle_val - entry
    else:
        pnl_pts = entry + settle_val  # credit: received entry, settle_val is negative when ITM
    return pnl_pts * SPX_MULTIPLIER - COMMISSION


def main():
    print("Loading SPX closes...")
    spx_df = yf.download("^GSPC", start="2000-01-01", progress=False)
    spx_arr = spx_df[("Close", "^GSPC")].values.astype(float)
    spx_dates = [d.strftime("%Y-%m-%d") for d in spx_df.index]
    date_idx = {d: i for i, d in enumerate(spx_dates)}
    settle = {d: float(spx_arr[i]) for d, i in date_idx.items()}

    daily_cl = {}
    for i, d in enumerate(spx_dates):
        if i >= 30:
            daily_cl[d] = [float(spx_arr[j]) for j in range(max(0, i - 30), i + 1)]

    cache = Path(CACHE_DIR)
    date_dirs = sorted(p.name for p in cache.iterdir() if p.is_dir() and p.name >= START_DATE)
    print(f"{len(date_dirs)} cache dates from {START_DATE}")

    trades = []
    skips = defaultdict(int)

    for ds in date_dirs:
        f6 = cache / ds / "close5_6dte.json"
        f5 = cache / ds / "close5_5dte.json"

        w6 = _load(f6) if f6.exists() else None
        w5 = _load(f5) if f5.exists() else None

        if not w6 and not w5:
            skips["no_chain"] += 1
            continue

        vix = None
        if w6: vix = w6.get("vix")
        if vix is None and w5: vix = w5.get("vix")

        chain6 = chain5 = None
        if w6:
            try: chain6 = _norm_iv(_parse(w6, "close5"))
            except: pass
        if w5:
            try: chain5 = _norm_iv(_parse(w5, "close5"))
            except: pass

        vix1d = None
        if chain6: vix1d = getattr(chain6, 'vix1d', None)
        if vix1d is None and chain5: vix1d = getattr(chain5, 'vix1d', None)

        # VIX1D veto
        if vix1d and VIX1D_VETO_LO <= vix1d < VIX1D_VETO_HI:
            skips["vix1d_veto"] += 1
            continue

        # RV
        rv_in_band = False
        if ds in daily_cl:
            rv5 = compute_rv(daily_cl[ds], 5)
            rv20 = compute_rv(daily_cl[ds], 20)
            if rv5 and rv20 and rv20 > 0:
                rv_in_band = RV_BAND_LO <= rv5 / rv20 < RV_BAND_HI

        # ═══════════════════════════════════════════════════
        # REGIME from 6DTE chain
        # ═══════════════════════════════════════════════════
        is_bull = True  # default
        em = None
        spot6 = None

        if chain6 and chain6.expirations:
            exp6 = chain6.expirations[0]
            exp6s = str(exp6)
            spot6 = chain6.underlying_price

            calls6 = [c for c in chain6.calls(exp6) if c.implied_vol > 0 and c.delta and c.delta > 0]
            puts6 = [c for c in chain6.puts(exp6) if c.implied_vol > 0 and c.delta and c.delta < 0]

            if calls6 and puts6:
                c_atm = min(calls6, key=lambda c: abs(c.strike - spot6))
                p_atm = min(puts6, key=lambda c: abs(c.strike - spot6))
                iv_atm = (c_atm.implied_vol * 100 + p_atm.implied_vol * 100) / 2

                c25 = min(calls6, key=lambda c: abs(abs(c.delta) - 0.25))
                p25 = min(puts6, key=lambda c: abs(abs(c.delta) - 0.25))
                rr25 = (c25.implied_vol - p25.implied_vol) * 100

                iv_minus_vix = iv_atm - (vix or 0)
                is_bull = iv_minus_vix >= IV_MINUS_VIX_THRESH and rr25 <= RR25_THRESH

                c_mid_atm = _mid(c_atm)
                p_mid_atm = _mid(p_atm)
                if c_mid_atm and p_mid_atm:
                    em = c_mid_atm + p_mid_atm

        # ═══════════════════════════════════════════════════
        # PUT SIDE (6DTE, $10 wide, regime switch)
        # ═══════════════════════════════════════════════════
        if chain6 and chain6.expirations and em:
            exp6 = chain6.expirations[0]
            exp6s = str(exp6)

            target = spot6 - em * EM_FACTOR
            anchor = min(chain6.puts(exp6), key=lambda c: abs(c.strike - target))

            rv_skip_put = rv_in_band and is_bull

            if not rv_skip_put and exp6s in settle:
                if is_bull:
                    ss_w = anchor.strike
                    ls_w = ss_w - WIDTH
                else:
                    ls_w = anchor.strike
                    ss_w = ls_w - WIDTH

                sm, lm = build_put_spread(chain6, exp6, ss_w, ls_w)
                if sm is not None:
                    if is_bull:
                        ep = sm - lm - SLIPPAGE  # credit
                    else:
                        ep = lm - sm + SLIPPAGE  # debit

                    if ep > 0:
                        pnl = put_pnl(ep, is_bull, settle[exp6s], ss_w, ls_w)
                        trades.append({
                            "date": ds, "side": "put", "variant": "put_regime",
                            "regime": "BULL" if is_bull else "BEAR",
                            "structure": "BPC" if is_bull else "BPD",
                            "ss": ss_w, "ls": ls_w,
                            "entry": round(ep, 4), "pnl": round(pnl, 2),
                            "spot": round(spot6, 2),
                            "settle": round(settle[exp6s], 2), "exp": exp6s,
                            "sm": round(sm, 2), "lm": round(lm, 2),
                        })

        # ═══════════════════════════════════════════════════
        # CALL SIDE (5DTE)
        # ═══════════════════════════════════════════════════
        if chain5 and chain5.expirations:
            exp5 = chain5.expirations[0]
            exp5s = str(exp5)
            spot5 = chain5.underlying_price

            vix_skip = vix is not None and vix < VIX_CALL_SKIP

            puts5 = [c for c in chain5.puts(exp5) if c.delta and c.delta < 0]
            calls5 = [c for c in chain5.calls(exp5) if c.delta and c.delta > 0]

            if puts5 and calls5 and not vix_skip and exp5s in settle:
                # ── Variant 1: 50-delta BPC (put spreads at ATM) ──
                p50 = min(puts5, key=lambda c: abs(abs(c.delta) - 0.50))

                # Always-bull BPC
                ss_50 = p50.strike
                ls_50 = ss_50 - WIDTH
                sm_50, lm_50 = build_put_spread(chain5, exp5, ss_50, ls_50)
                if sm_50 is not None:
                    ep_50_bpc = sm_50 - lm_50 - SLIPPAGE
                    if ep_50_bpc > 0 and not rv_in_band:
                        pnl_50_bpc = put_pnl(ep_50_bpc, True, settle[exp5s], ss_50, ls_50)
                        trades.append({
                            "date": ds, "side": "call", "variant": "50d_always_bull",
                            "regime": "BULL" if is_bull else "BEAR",
                            "structure": "BPC",
                            "ss": ss_50, "ls": ls_50,
                            "entry": round(ep_50_bpc, 4), "pnl": round(pnl_50_bpc, 2),
                            "spot": round(spot5, 2),
                            "settle": round(settle[exp5s], 2), "exp": exp5s,
                            "sm": round(sm_50, 2), "lm": round(lm_50, 2),
                        })

                # Regime follow at 50-delta
                if is_bull:
                    # Same BPC
                    if sm_50 is not None and ep_50_bpc > 0 and not rv_in_band:
                        trades.append({
                            "date": ds, "side": "call", "variant": "50d_regime_follow",
                            "regime": "BULL", "structure": "BPC",
                            "ss": ss_50, "ls": ls_50,
                            "entry": round(ep_50_bpc, 4),
                            "pnl": round(pnl_50_bpc, 2),
                            "spot": round(spot5, 2),
                            "settle": round(settle[exp5s], 2), "exp": exp5s,
                            "sm": round(sm_50, 2), "lm": round(lm_50, 2),
                        })
                else:
                    # BPD at 50-delta
                    ls_bear50 = p50.strike
                    ss_bear50 = ls_bear50 - WIDTH
                    sm_b50, lm_b50 = build_put_spread(chain5, exp5, ss_bear50, ls_bear50)
                    if sm_b50 is not None:
                        ep_50_bpd = lm_b50 - sm_b50 + SLIPPAGE
                        if ep_50_bpd > 0:
                            pnl_50_bpd = put_pnl(ep_50_bpd, False, settle[exp5s], ss_bear50, ls_bear50)
                            trades.append({
                                "date": ds, "side": "call", "variant": "50d_regime_follow",
                                "regime": "BEAR", "structure": "BPD",
                                "ss": ss_bear50, "ls": ls_bear50,
                                "entry": round(ep_50_bpd, 4),
                                "pnl": round(pnl_50_bpd, 2),
                                "spot": round(spot5, 2),
                                "settle": round(settle[exp5s], 2), "exp": exp5s,
                                "sm": round(sm_b50, 2), "lm": round(lm_b50, 2),
                            })

                # Skip-bear at 50-delta (only BPC on bull days)
                if is_bull and sm_50 is not None and ep_50_bpc > 0 and not rv_in_band:
                    trades.append({
                        "date": ds, "side": "call", "variant": "50d_skip_bear",
                        "regime": "BULL", "structure": "BPC",
                        "ss": ss_50, "ls": ls_50,
                        "entry": round(ep_50_bpc, 4),
                        "pnl": round(pnl_50_bpc, 2),
                        "spot": round(spot5, 2),
                        "settle": round(settle[exp5s], 2), "exp": exp5s,
                        "sm": round(sm_50, 2), "lm": round(lm_50, 2),
                    })

                # ── Variant 2: 0.75*EM call spreads (20-30 delta) ──
                # Compute EM from 5DTE chain for call-side anchor
                c_atm5 = min(calls5, key=lambda c: abs(c.strike - spot5))
                p_atm5 = min(puts5, key=lambda c: abs(c.strike - spot5))
                c_mid5 = _mid(c_atm5)
                p_mid5 = _mid(p_atm5)

                if c_mid5 and p_mid5:
                    em5 = c_mid5 + p_mid5
                    call_target = spot5 + em5 * EM_FACTOR
                    call_anchor = min(calls5, key=lambda c: abs(c.strike - call_target))
                    anchor_k = call_anchor.strike

                    # Bull: bull_call_debit (buy call at anchor, sell call at anchor+WIDTH)
                    buy_k = anchor_k
                    sell_k = anchor_k + WIDTH
                    sm_bcd, lm_bcd = build_call_spread(chain5, exp5, sell_k, buy_k)
                    # sm_bcd = short call mid (higher strike), lm_bcd = long call mid (lower strike)
                    if sm_bcd is not None:
                        ep_bcd = lm_bcd - sm_bcd + SLIPPAGE  # debit: pay for long - receive for short
                        if ep_bcd > 0:
                            pnl_bcd = call_pnl(ep_bcd, True, settle[exp5s], buy_k, sell_k)

                            # Always bull (long call spread every day)
                            if not rv_in_band:  # RV band blocks bullish
                                trades.append({
                                    "date": ds, "side": "call", "variant": "em75_always_bull",
                                    "regime": "BULL" if is_bull else "BEAR",
                                    "structure": "BCD",
                                    "ss": sell_k, "ls": buy_k,
                                    "entry": round(ep_bcd, 4), "pnl": round(pnl_bcd, 2),
                                    "spot": round(spot5, 2),
                                    "settle": round(settle[exp5s], 2), "exp": exp5s,
                                    "sm": round(sm_bcd, 2), "lm": round(lm_bcd, 2),
                                })

                            # Regime follow: BCD on bull
                            if is_bull and not rv_in_band:
                                trades.append({
                                    "date": ds, "side": "call", "variant": "em75_regime_follow",
                                    "regime": "BULL", "structure": "BCD",
                                    "ss": sell_k, "ls": buy_k,
                                    "entry": round(ep_bcd, 4), "pnl": round(pnl_bcd, 2),
                                    "spot": round(spot5, 2),
                                    "settle": round(settle[exp5s], 2), "exp": exp5s,
                                    "sm": round(sm_bcd, 2), "lm": round(lm_bcd, 2),
                                })

                            # Skip-bear: BCD on bull only
                            if is_bull and not rv_in_band:
                                trades.append({
                                    "date": ds, "side": "call", "variant": "em75_skip_bear",
                                    "regime": "BULL", "structure": "BCD",
                                    "ss": sell_k, "ls": buy_k,
                                    "entry": round(ep_bcd, 4), "pnl": round(pnl_bcd, 2),
                                    "spot": round(spot5, 2),
                                    "settle": round(settle[exp5s], 2), "exp": exp5s,
                                    "sm": round(sm_bcd, 2), "lm": round(lm_bcd, 2),
                                })

                    # Bear: bear_call_credit (sell call at anchor, buy call at anchor+WIDTH)
                    # Short call at lower strike, long call at higher strike
                    short_k_bcc = anchor_k
                    long_k_bcc = anchor_k + WIDTH
                    sm_bcc, lm_bcc = build_call_spread(chain5, exp5, short_k_bcc, long_k_bcc)
                    if sm_bcc is not None:
                        ep_bcc = sm_bcc - lm_bcc - SLIPPAGE  # credit
                        if ep_bcc > 0:
                            pnl_bcc = call_pnl(ep_bcc, False, settle[exp5s], long_k_bcc, short_k_bcc)

                            # Always short (premium selling call credit every day)
                            trades.append({
                                "date": ds, "side": "call", "variant": "em75_always_short",
                                "regime": "BULL" if is_bull else "BEAR",
                                "structure": "BCC",
                                "ss": short_k_bcc, "ls": long_k_bcc,
                                "entry": round(ep_bcc, 4), "pnl": round(pnl_bcc, 2),
                                "spot": round(spot5, 2),
                                "settle": round(settle[exp5s], 2), "exp": exp5s,
                                "sm": round(sm_bcc, 2), "lm": round(lm_bcc, 2),
                            })

                            # Regime follow: BCC on bear days
                            if not is_bull:
                                trades.append({
                                    "date": ds, "side": "call", "variant": "em75_regime_follow",
                                    "regime": "BEAR", "structure": "BCC",
                                    "ss": short_k_bcc, "ls": long_k_bcc,
                                    "entry": round(ep_bcc, 4), "pnl": round(pnl_bcc, 2),
                                    "spot": round(spot5, 2),
                                    "settle": round(settle[exp5s], 2), "exp": exp5s,
                                    "sm": round(sm_bcc, 2), "lm": round(lm_bcc, 2),
                                })

    print(f"Total trade records: {len(trades)}")
    print(f"Skips: {dict(skips)}")

    # ═══════════════════════════════════════════════════════
    # BUILD SCENARIO P&Ls
    # ═══════════════════════════════════════════════════════

    def scenario(label, call_variant):
        put_t = [t for t in trades if t["side"] == "put" and t["variant"] == "put_regime"]
        call_t = [t for t in trades if t["side"] == "call" and t["variant"] == call_variant]
        all_t = put_t + call_t

        put_total = sum(t["pnl"] for t in put_t)
        call_total = sum(t["pnl"] for t in call_t)
        total = put_total + call_total
        wins = sum(1 for t in all_t if t["pnl"] > 0)

        eq = pk = 0
        dd = 0
        for t in sorted(all_t, key=lambda x: x["date"]):
            eq += t["pnl"]
            pk = max(pk, eq)
            dd = max(dd, pk - eq)

        return {
            "label": label, "n": len(all_t),
            "n_put": len(put_t), "n_call": len(call_t),
            "put_pnl": put_total, "call_pnl": call_total, "total": total,
            "wr": wins / len(all_t) * 100 if all_t else 0, "dd": dd,
            "put_trades": put_t, "call_trades": call_t,
        }

    scenarios = [
        scenario("A: 50d always bull (v1.0.0)", "50d_always_bull"),
        scenario("B: 50d regime follow", "50d_regime_follow"),
        scenario("C: 50d skip bear", "50d_skip_bear"),
        scenario("D: 0.75EM always bull (long call debit)", "em75_always_bull"),
        scenario("E: 0.75EM regime follow", "em75_regime_follow"),
        scenario("F: 0.75EM skip bear", "em75_skip_bear"),
        scenario("G: 0.75EM always short (call credit)", "em75_always_short"),
    ]

    print("\n" + "=" * 120)
    print("DUALSIDE $10-WIDE BACKTEST — CALL-SIDE VARIATIONS")
    print("Put side: $10 regime switch at 0.75*EM (6DTE) — same across all scenarios")
    print("=" * 120)

    print(f"\n{'Scenario':>46s} | {'N':>5s} |{'Puts':>5s} |{'Calls':>5s} | {'Put P&L':>10s} | {'Call P&L':>10s} | {'Total':>10s} | {'Avg':>6s} | {'WR':>5s} | {'Max DD':>8s}")
    print("-" * 120)
    for s in scenarios:
        avg = s['total'] / s['n'] if s['n'] else 0
        print(f"{s['label']:>46s} | {s['n']:>5d} |{s['n_put']:>5d} |{s['n_call']:>5d} | ${s['put_pnl']:>8,.0f} | ${s['call_pnl']:>8,.0f} | ${s['total']:>8,.0f} | ${avg:>4,.0f} | {s['wr']:>4.0f}% | ${s['dd']:>6,.0f}")

    # --- Yearly ---
    print(f"\n### By Year")
    years = sorted(set(t["date"][:4] for t in trades))
    for s in scenarios:
        print(f"\n  {s['label']}:")
        print(f"  {'Year':>6s} | {'Puts':>4s} | {'Calls':>5s} | {'Put P&L':>8s} | {'Call P&L':>8s} | {'Total':>8s}")
        print(f"  {'-' * 55}")
        for y in years:
            yp_t = [t for t in s["put_trades"] if t["date"][:4] == y]
            yc_t = [t for t in s["call_trades"] if t["date"][:4] == y]
            yp = sum(t["pnl"] for t in yp_t)
            yc = sum(t["pnl"] for t in yc_t)
            print(f"  {y:>6s} | {len(yp_t):>4d} | {len(yc_t):>5d} | ${yp:>7,.0f} | ${yc:>7,.0f} | ${yp + yc:>7,.0f}")

    # --- Call-side breakdown by regime ---
    print(f"\n### Call-Side Breakdown by Regime")
    call_variants = [
        "50d_always_bull", "50d_regime_follow", "50d_skip_bear",
        "em75_always_bull", "em75_regime_follow", "em75_skip_bear", "em75_always_short",
    ]
    for cv in call_variants:
        ct = [t for t in trades if t["side"] == "call" and t["variant"] == cv]
        if not ct:
            continue
        print(f"\n  {cv}:")
        for regime in ["BULL", "BEAR"]:
            rt = [t for t in ct if t["regime"] == regime]
            if rt:
                total = sum(t["pnl"] for t in rt)
                wins = sum(1 for t in rt if t["pnl"] > 0)
                avg = total / len(rt)
                print(f"    {regime}: {len(rt):>4d} trades  ${total:>+10,.0f}  avg ${avg:>+6.0f}  WR {wins / len(rt) * 100:.0f}%")

    # --- Monthly 2025+ ---
    print(f"\n### Monthly 2025+ (A vs D vs F vs G)")
    focus = [s for s in scenarios if s["label"].startswith(("A:", "D:", "F:", "G:"))]
    months = sorted(set(t["date"][:7] for t in trades if t["date"] >= "2025"))
    headers = [s["label"].split(":")[0] + ":" + s["label"].split(":")[1][:15] for s in focus]
    print(f"  {'Month':>8s} |" + "|".join(f"{h:>16s}" for h in headers))
    print(f"  {'-' * (8 + 17 * len(focus))}")
    for m in months:
        vals = []
        for s in focus:
            all_t = s["put_trades"] + s["call_trades"]
            mp = sum(t["pnl"] for t in all_t if t["date"][:7] == m)
            vals.append(mp)
        print(f"  {m:>8s} |" + "|".join(f"${v:>14,.0f}" for v in vals))

    # --- Bad months ---
    print(f"\n### Bad Month Analysis (months where A < 0)")
    s_a = scenarios[0]
    all_a = s_a["put_trades"] + s_a["call_trades"]
    all_months = sorted(set(t["date"][:7] for t in all_a))
    bad_months = [m for m in all_months if sum(t["pnl"] for t in all_a if t["date"][:7] == m) < 0]

    print(f"  {'Month':>8s} |" + "|".join(f"{s['label'][:16]:>16s}" for s in scenarios))
    print(f"  {'-' * (8 + 17 * len(scenarios))}")
    totals = [0] * len(scenarios)
    for m in bad_months:
        vals = []
        for i, s in enumerate(scenarios):
            all_t = s["put_trades"] + s["call_trades"]
            mp = sum(t["pnl"] for t in all_t if t["date"][:7] == m)
            vals.append(mp)
            totals[i] += mp
        print(f"  {m:>8s} |" + "|".join(f"${v:>14,.0f}" for v in vals))
    print(f"  {'TOTAL':>8s} |" + "|".join(f"${t:>14,.0f}" for t in totals))

    # --- Spot check ---
    print(f"\n### Entry Spot-Check (first 3 dates)")
    seen = set()
    for t in sorted(trades, key=lambda x: x["date"]):
        if t["date"] in seen:
            continue
        if len(seen) >= 3:
            break
        seen.add(t["date"])
        same = [x for x in trades if x["date"] == t["date"]]
        print(f"\n  {t['date']} (spot={t['spot']}, regime={'BULL' if is_bull else 'BEAR'})")
        for x in sorted(same, key=lambda z: (z["variant"], z["structure"])):
            print(f"    {x['variant']:>22s} {x['structure']:>3s}  "
                  f"ss={x['ss']:.0f} ls={x['ls']:.0f}  "
                  f"sm={x['sm']:.2f} lm={x['lm']:.2f}  "
                  f"entry={x['entry']:.2f}  pnl=${x['pnl']:>+8.0f}")

    # Save CSV
    out = Path("/Users/mgebremichael/Documents/Gamma/sim/data/dualside_v2_backtest.csv")
    with open(out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=[
            "date", "side", "variant", "regime", "structure",
            "ss", "ls", "sm", "lm", "entry", "pnl", "spot", "settle", "exp"])
        w.writeheader()
        for t in sorted(trades, key=lambda x: (x["date"], x["variant"])):
            row = {k: t.get(k, "") for k in w.fieldnames}
            w.writerow(row)
    print(f"\nSaved: {out}")


if __name__ == "__main__":
    main()
