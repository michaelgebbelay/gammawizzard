#!/usr/bin/env python3
"""
DualSide CLEAN backtest from real chain data.

No approximations. Actual $5 and $10 spread mids from cached option chains.

Scenarios:
  A) v1.0.0: Put $10 regime + Call $10 always-bull BPC
  B) v1.1.0: Put $5 regime + Call $5 regime-follow
  C) revised: Put $5 regime + Call $5 always-bull BPC
  D) skip:    Put $5 regime + Call skip-on-bear, $5 BPC on bull
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

DATA_DIR = Path(__file__).resolve().parent.parent / "sim" / "data"
SLIPPAGE = 0.20
COMMISSION = 0.97  # per vertical (matches production)
EM_FACTOR = 0.75

# Thresholds
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
    lr = [math.log(recent[i] / recent[i-1]) for i in range(1, len(recent))]
    return float(np.std(lr, ddof=1) * math.sqrt(252) * 100)


def build_spread(chain, exp, ss, ls):
    """Get spread mid from actual chain contracts."""
    sc = chain.get_contract(ss, "P", exp)
    lc = chain.get_contract(ls, "P", exp)
    sm = _mid(sc)
    lm = _mid(lc)
    if sm is not None and lm is not None and sm > 0 and lm > 0:
        return sm, lm
    return None, None


def compute_pnl(entry, is_credit, settle_spot, short_strike, long_strike):
    """P&L for a vertical spread at settlement."""
    short_iv = max(0, short_strike - settle_spot)
    long_iv = max(0, long_strike - settle_spot)
    # Settlement from our perspective
    settle_val = long_iv - short_iv  # positive = good for us (debit buyer)
    if is_credit:
        pnl_pts = entry + settle_val  # entry is credit received (positive)
    else:
        pnl_pts = settle_val - entry  # entry is debit paid (positive)
    return pnl_pts * SPX_MULTIPLIER - COMMISSION


def main():
    print("Loading SPX closes...")
    spx_df = yf.download("^GSPC", start="2000-01-01", progress=False)
    spx_arr = spx_df[("Close", "^GSPC")].values.astype(float)
    spx_dates = [d.strftime("%Y-%m-%d") for d in spx_df.index]
    date_idx = {d: i for i, d in enumerate(spx_dates)}
    settle = {d: float(spx_arr[i]) for d, i in date_idx.items()}

    # Daily closes for RV computation
    daily_cl = {}
    for i, d in enumerate(spx_dates):
        if i >= 30:
            daily_cl[d] = [float(spx_arr[j]) for j in range(max(0, i-30), i+1)]

    cache = Path(CACHE_DIR)
    date_dirs = sorted(p.name for p in cache.iterdir() if p.is_dir() and p.name >= START_DATE)
    print(f"{len(date_dirs)} cache dates from {START_DATE}")

    # Collect trades for each scenario
    # Each trade: {date, side, regime, structure, width, entry, pnl, ...}
    trades = []  # all individual trade records
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
                rv_in_band = RV_BAND_LO <= rv5/rv20 < RV_BAND_HI

        # ═══════════════════════════════════════════════════
        # PUT SIDE (6DTE, regime switch)
        # ═══════════════════════════════════════════════════
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
                    target = spot6 - em * EM_FACTOR
                    anchor = min(chain6.puts(exp6), key=lambda c: abs(c.strike - target))

                    # Determine strikes for both widths
                    if is_bull:
                        ss = anchor.strike
                    else:
                        ls_anchor = anchor.strike
                        ss = ls_anchor  # for bear: long is anchor (higher)

                    # RV skip for bull put
                    rv_skip_put = rv_in_band and is_bull

                    if not rv_skip_put and exp6s in settle:
                        for width in [10, 5]:
                            if is_bull:
                                ss_w = ss; ls_w = ss - width
                            else:
                                ls_w = ls_anchor; ss_w = ls_anchor - width

                            sm, lm = build_spread(chain6, exp6, ss_w, ls_w)
                            if sm is None:
                                continue

                            if is_bull:
                                ep = sm - lm - SLIPPAGE  # credit
                            else:
                                ep = lm - sm + SLIPPAGE  # debit

                            if ep <= 0:
                                continue

                            pnl = compute_pnl(ep, is_bull, settle[exp6s], ss_w, ls_w)

                            trades.append({
                                "date": ds, "side": "put", "width": width,
                                "regime": "BULL" if is_bull else "BEAR",
                                "structure": "BPC" if is_bull else "BPD",
                                "ss": ss_w, "ls": ls_w,
                                "entry": round(ep, 4),
                                "pnl": round(pnl, 2),
                                "spot": round(spot6, 2),
                                "settle": round(settle[exp6s], 2),
                                "exp": exp6s,
                                "sm": round(sm, 2), "lm": round(lm, 2),
                            })

        # ═══════════════════════════════════════════════════
        # CALL SIDE (5DTE, 50-delta)
        # ═══════════════════════════════════════════════════
        if chain5 and chain5.expirations:
            exp5 = chain5.expirations[0]
            exp5s = str(exp5)
            spot5 = chain5.underlying_price

            vix_skip = vix is not None and vix < VIX_CALL_SKIP

            puts5 = [c for c in chain5.puts(exp5) if c.delta and c.delta < 0]

            if puts5 and not vix_skip and exp5s in settle:
                p50 = min(puts5, key=lambda c: abs(abs(c.delta) - 0.50))

                # Need regime from put side (already computed if chain6 existed)
                # Use the same is_bull from 6DTE chain
                call_is_bull = is_bull if (chain6 and chain6.expirations and calls6 and puts6) else True

                for width in [10, 5]:
                    # Always-bull BPC
                    ss_bull = p50.strike; ls_bull = ss_bull - width
                    sm_b, lm_b = build_spread(chain5, exp5, ss_bull, ls_bull)
                    if sm_b is not None:
                        ep_bull = sm_b - lm_b - SLIPPAGE
                        if ep_bull > 0:
                            pnl_bull = compute_pnl(ep_bull, True, settle[exp5s], ss_bull, ls_bull)

                            # RV skip: always-bull is always blocked by RV band
                            if not rv_in_band:
                                trades.append({
                                    "date": ds, "side": "call", "width": width,
                                    "regime": "BULL" if call_is_bull else "BEAR",
                                    "structure": "BPC",
                                    "call_scenario": "always_bull",
                                    "ss": ss_bull, "ls": ls_bull,
                                    "entry": round(ep_bull, 4),
                                    "pnl": round(pnl_bull, 2),
                                    "spot": round(spot5, 2),
                                    "settle": round(settle[exp5s], 2),
                                    "exp": exp5s,
                                    "sm": round(sm_b, 2), "lm": round(lm_b, 2),
                                })

                    # Regime-follow: BPC on bull days, BPD on bear days
                    if call_is_bull:
                        # Same as always-bull
                        if sm_b is not None and ep_bull > 0 and not (rv_in_band):
                            trades.append({
                                "date": ds, "side": "call", "width": width,
                                "regime": "BULL",
                                "structure": "BPC",
                                "call_scenario": "regime_follow",
                                "ss": ss_bull, "ls": ls_bull,
                                "entry": round(ep_bull, 4),
                                "pnl": round(pnl_bull, 2),
                                "spot": round(spot5, 2),
                                "settle": round(settle[exp5s], 2),
                                "exp": exp5s,
                                "sm": round(sm_b, 2), "lm": round(lm_b, 2),
                            })
                    else:
                        # Bear: BPD — buy p50 put, sell p50-width put
                        ls_bear = p50.strike; ss_bear = ls_bear - width
                        sm_br, lm_br = build_spread(chain5, exp5, ss_bear, ls_bear)
                        if sm_br is not None:
                            ep_bear = lm_br - sm_br + SLIPPAGE  # debit
                            if ep_bear > 0:
                                pnl_bear = compute_pnl(ep_bear, False, settle[exp5s], ss_bear, ls_bear)
                                # Bear not blocked by RV band
                                trades.append({
                                    "date": ds, "side": "call", "width": width,
                                    "regime": "BEAR",
                                    "structure": "BPD",
                                    "call_scenario": "regime_follow",
                                    "ss": ss_bear, "ls": ls_bear,
                                    "entry": round(ep_bear, 4),
                                    "pnl": round(pnl_bear, 2),
                                    "spot": round(spot5, 2),
                                    "settle": round(settle[exp5s], 2),
                                    "exp": exp5s,
                                    "sm": round(sm_br, 2), "lm": round(lm_br, 2),
                                })

    print(f"Total trade records: {len(trades)}")
    print(f"Skips: {dict(skips)}")

    # ═══════════════════════════════════════════════════════
    # BUILD SCENARIO P&Ls
    # ═══════════════════════════════════════════════════════

    def scenario_pnl(label, put_width, call_width, call_mode):
        """Sum P&L for a scenario.
        call_mode: 'always_bull', 'regime_follow', 'skip_bear'
        """
        put_t = [t for t in trades if t["side"] == "put" and t["width"] == put_width]

        if call_mode == "skip_bear":
            call_t = [t for t in trades if t["side"] == "call" and t["width"] == call_width
                       and t.get("call_scenario") == "always_bull" and t["regime"] == "BULL"]
        else:
            call_t = [t for t in trades if t["side"] == "call" and t["width"] == call_width
                       and t.get("call_scenario") == call_mode]

        put_total = sum(t["pnl"] for t in put_t)
        call_total = sum(t["pnl"] for t in call_t)
        all_t = put_t + call_t
        total = put_total + call_total
        wins = sum(1 for t in all_t if t["pnl"] > 0)

        # Max drawdown
        eq = pk = 0; dd = 0
        for t in sorted(all_t, key=lambda x: x["date"]):
            eq += t["pnl"]; pk = max(pk, eq); dd = max(dd, pk - eq)

        return {
            "label": label, "n_put": len(put_t), "n_call": len(call_t),
            "put_pnl": put_total, "call_pnl": call_total,
            "total": total, "n": len(all_t),
            "wr": wins / len(all_t) * 100 if all_t else 0,
            "dd": dd,
            "worst": min(t["pnl"] for t in all_t) if all_t else 0,
            "best": max(t["pnl"] for t in all_t) if all_t else 0,
            "put_trades": put_t, "call_trades": call_t,
        }

    scenarios = [
        scenario_pnl("A: v1.0.0 ($10/$10, always bull)", 10, 10, "always_bull"),
        scenario_pnl("B: v1.1.0 ($5/$5, regime follow)", 5, 5, "regime_follow"),
        scenario_pnl("C: revised ($5/$5, always bull)",   5, 5, "always_bull"),
        scenario_pnl("D: skip-bear ($5/$5, skip bear)",   5, 5, "skip_bear"),
    ]

    print("\n" + "=" * 110)
    print("DUALSIDE CLEAN BACKTEST — REAL CHAIN DATA")
    print("=" * 110)

    print(f"\n{'Scenario':>42s} | {'N':>5s} | {'Put P&L':>10s} | {'Call P&L':>10s} | {'Total':>10s} | {'Avg':>6s} | {'WR':>5s} | {'Max DD':>8s}")
    print("-" * 110)
    for s in scenarios:
        print(f"{s['label']:>42s} | {s['n']:>5d} | ${s['put_pnl']:>8,.0f} | ${s['call_pnl']:>8,.0f} | ${s['total']:>8,.0f} | ${s['total']/s['n'] if s['n'] else 0:>4,.0f} | {s['wr']:>4.0f}% | ${s['dd']:>6,.0f}")

    # --- Yearly ---
    print(f"\n### By Year")
    years = sorted(set(t["date"][:4] for t in trades))
    for s in scenarios:
        print(f"\n  {s['label']}:")
        print(f"  {'Year':>6s} | {'Put':>8s} | {'Call':>8s} | {'Total':>8s}")
        print(f"  {'-'*40}")
        all_t = s["put_trades"] + s["call_trades"]
        for y in years:
            yp = sum(t["pnl"] for t in s["put_trades"] if t["date"][:4] == y)
            yc = sum(t["pnl"] for t in s["call_trades"] if t["date"][:4] == y)
            print(f"  {y:>6s} | ${yp:>6,.0f} | ${yc:>6,.0f} | ${yp+yc:>6,.0f}")

    # --- Call side: regime analysis ---
    print(f"\n### Call Side by Regime (width=5)")
    for call_mode in ["always_bull", "regime_follow"]:
        print(f"\n  Mode: {call_mode}")
        call_t = [t for t in trades if t["side"] == "call" and t["width"] == 5
                   and t.get("call_scenario") == call_mode]
        for regime in ["BULL", "BEAR"]:
            rt = [t for t in call_t if t["regime"] == regime]
            if rt:
                total = sum(t["pnl"] for t in rt)
                wins = sum(1 for t in rt if t["pnl"] > 0)
                print(f"    {regime}: {len(rt):>4d} trades  ${total:>+10,.0f}  avg ${total/len(rt):>+6.0f}  WR {wins/len(rt)*100:.0f}%")

    # --- Monthly (2025+) ---
    print(f"\n### Monthly 2025+ (scenario A vs B vs C)")
    months = sorted(set(t["date"][:7] for t in trades if t["date"] >= "2025"))
    print(f"  {'Month':>8s} | {'A: v1.0.0':>10s} | {'B: v1.1.0':>10s} | {'C: revised':>10s} | {'D: skip':>10s}")
    print(f"  {'-'*60}")
    for m in months:
        vals = []
        for s in scenarios:
            all_t = s["put_trades"] + s["call_trades"]
            mp = sum(t["pnl"] for t in all_t if t["date"][:7] == m)
            vals.append(mp)
        print(f"  {m:>8s} | ${vals[0]:>8,.0f} | ${vals[1]:>8,.0f} | ${vals[2]:>8,.0f} | ${vals[3]:>8,.0f}")

    # --- Bad months analysis ---
    print(f"\n### Bad Month Analysis (months where v1.0.0 total < 0)")
    s_a = scenarios[0]
    all_a = s_a["put_trades"] + s_a["call_trades"]
    all_months = sorted(set(t["date"][:7] for t in all_a))

    bad_months = []
    for m in all_months:
        mp = sum(t["pnl"] for t in all_a if t["date"][:7] == m)
        if mp < 0:
            bad_months.append(m)

    print(f"  {'Month':>8s} | {'A: v1.0.0':>10s} | {'B: v1.1.0':>10s} | {'C: revised':>10s} | {'D: skip':>10s}")
    print(f"  {'-'*60}")
    totals = [0, 0, 0, 0]
    for m in bad_months:
        vals = []
        for i, s in enumerate(scenarios):
            all_t = s["put_trades"] + s["call_trades"]
            mp = sum(t["pnl"] for t in all_t if t["date"][:7] == m)
            vals.append(mp)
            totals[i] += mp
        print(f"  {m:>8s} | ${vals[0]:>8,.0f} | ${vals[1]:>8,.0f} | ${vals[2]:>8,.0f} | ${vals[3]:>8,.0f}")
    print(f"  {'TOTAL':>8s} | ${totals[0]:>8,.0f} | ${totals[1]:>8,.0f} | ${totals[2]:>8,.0f} | ${totals[3]:>8,.0f}")

    # --- Entry spot-check ---
    print(f"\n### Entry Spot-Check (real mids, first 4 dates)")
    seen = set()
    for t in sorted(trades, key=lambda x: x["date"]):
        if t["date"] in seen: continue
        if len(seen) >= 4: break
        seen.add(t["date"])
        same_date = [x for x in trades if x["date"] == t["date"]]
        print(f"\n  {t['date']} (spot={t['spot']}, regime={t['regime']})")
        for x in same_date:
            cs = x.get("call_scenario", "")
            print(f"    {x['side']:>5s} w{x['width']:>2d} {x['structure']:>3s} {cs:>14s} "
                  f"ss={x['ss']:.0f} ls={x['ls']:.0f} "
                  f"sm={x['sm']:.2f} lm={x['lm']:.2f} "
                  f"entry={x['entry']:.2f} pnl=${x['pnl']:>+8.0f}")

    # --- Save CSV ---
    out = Path("/Users/mgebremichael/Documents/Gamma/sim/data/dualside_clean_backtest.csv")
    with open(out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=[
            "date", "side", "width", "regime", "structure", "call_scenario",
            "ss", "ls", "sm", "lm", "entry", "pnl", "spot", "settle", "exp"])
        w.writeheader()
        for t in sorted(trades, key=lambda x: (x["date"], x["side"], x["width"])):
            row = {k: t.get(k, "") for k in w.fieldnames}
            w.writerow(row)
    print(f"\nSaved: {out}")


if __name__ == "__main__":
    main()
