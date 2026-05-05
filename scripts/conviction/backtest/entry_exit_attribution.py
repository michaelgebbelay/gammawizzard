#!/usr/bin/env python3
"""
2x2 entry/exit attribution between C1-HYST and D2.

Strategies:
  HH = HYST entry + HYST exit   (reproduces C1-HYST baseline)
  DD = D2   entry + D2   exit   (reproduces D2 baseline)
  DH = D2   entry + HYST exit   (proposed hybrid)
  HD = HYST entry + D2   exit   (inverse control — necessary)

Conflict control (applied uniformly to all four):
  in BIL  : flip to TQQQ only when entry_signal AND NOT exit_signal
  in TQQQ : flip to BIL  only when exit_signal

Signal definitions (preserve EXACTLY from prior runs):
  D2 entry  = (dd_from_126d_high > -0.06) AND (ret63 > 0)
  D2 exit   = dd_from_126d_high <= -0.12
  HYST score= (close>sma150) + (sma50>sma200) + (ret63>0)
  HYST entry= score == 3
  HYST exit = score <= 1   (score == 2 = hold)

Reports:
  metrics.csv              one row per (strategy, window, slippage)
  attribution_L.csv        daily state comparison HH vs DD on window L
  attribution_buckets.csv  4-bucket roll-up of where HH-vs-DD diff comes from
  bil_episodes_HH.csv      BIL holding periods for HH on window L
  bil_episodes_DD.csv      same for DD
  bil_episode_pairs.csv    each HH BIL period paired to closest DD BIL period
                           with TQQQ return between exit/re-entry dates
  summary.txt              human-readable report including verdict gate

Trigger: gh workflow run entry_exit_attribution.yml
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

# Import the working helpers from the prior non-MA script so HH/DD reproduce
# exactly without re-implementing data loading or simulation.
from non_ma_regime_backtest import (   # noqa: E402
    load_bars,
    compute_indicators,
    simulate,
    TICKERS,
    START_VALUE,
)


WINDOWS = {
    "L": ("2011-01-03", "2026-04-29"),
    "A": ("2021-01-04", "2026-04-29"),
    "B": ("2022-01-03", "2026-04-29"),
    "C": ("2022-02-11", "2026-04-29"),
}
SLIPPAGE_BPS = [10, 50]
VARIANTS = ["HH", "DD", "DH", "HD"]


# ---------------------------------------------------------------------------
# raw entry/exit signal computation (one row per day)
# ---------------------------------------------------------------------------

def compute_raw_signals(ind: pd.DataFrame) -> pd.DataFrame:
    """Compute the four primitive booleans + readiness flag."""
    out = pd.DataFrame({"date": ind["date"]}).reset_index(drop=True)

    # readiness — only after all indicators are valid
    needed = ["sma200", "sma150", "sma50", "ret63", "dd126"]
    ready = ~ind[needed].isna().any(axis=1)
    out["ready"] = ready.values

    # D2 primitives
    out["d2_entry"] = ((ind["dd126"] > -0.06) & (ind["ret63"] > 0)).values
    out["d2_exit"] = (ind["dd126"] <= -0.12).values

    # HYST primitives
    A = (ind["close"] > ind["sma150"]).astype(int)
    B = (ind["sma50"] > ind["sma200"]).astype(int)
    C = (ind["ret63"] > 0).astype(int)
    score = (A + B + C).values
    out["hyst_score"] = score
    out["hyst_entry"] = score == 3
    out["hyst_exit"] = score <= 1

    return out


def state_machine(sigs: pd.DataFrame, entry_col: str, exit_col: str) -> list:
    """Generic state machine with conflict control.

    in BIL: flip to TQQQ if entry_signal AND NOT exit_signal
    in TQQQ: flip to BIL if exit_signal
    """
    out = []
    s = None
    for _, r in sigs.iterrows():
        if not r["ready"]:
            out.append(None)
            continue
        entry = bool(r[entry_col])
        ex = bool(r[exit_col])
        if s is None:
            # initial state: TQQQ if entry conditions met (and not exit), else BIL
            s = "TQQQ" if (entry and not ex) else "BIL"
        else:
            if s == "TQQQ":
                if ex:
                    s = "BIL"
            else:  # BIL
                if entry and not ex:
                    s = "TQQQ"
        out.append(s)
    return out


def compute_states(sigs: pd.DataFrame) -> dict[str, list]:
    return {
        "HH": state_machine(sigs, "hyst_entry", "hyst_exit"),
        "DD": state_machine(sigs, "d2_entry", "d2_exit"),
        "DH": state_machine(sigs, "d2_entry", "hyst_exit"),
        "HD": state_machine(sigs, "hyst_entry", "d2_exit"),
    }


# ---------------------------------------------------------------------------
# attribution: daily HH vs DD bucket comparison on window L
# ---------------------------------------------------------------------------

BUCKET_NAMES = {
    (1, 1): "both_TQQQ",
    (0, 0): "both_BIL",
    (1, 0): "HH_TQQQ_DD_BIL",   # HH in / DD out
    (0, 1): "HH_BIL_DD_TQQQ",   # HH out / DD in
}


def daily_bucket_compare(states: dict[str, list],
                         ind: pd.DataFrame,
                         bars: dict[str, pd.DataFrame],
                         start: pd.Timestamp,
                         end: pd.Timestamp) -> tuple[pd.DataFrame, pd.DataFrame]:
    mask = (ind["date"] >= start) & (ind["date"] <= end)
    sub_ind = ind[mask].reset_index(drop=True)
    idx_in_full = list(ind.index[mask])

    hh = [states["HH"][i] or "BIL" for i in idx_in_full]
    dd = [states["DD"][i] or "BIL" for i in idx_in_full]

    tqqq = bars["TQQQ"].set_index("date")["close"].reindex(sub_ind["date"]).ffill()
    tqqq_ret = tqqq.pct_change().fillna(0).values
    tqqq_log = np.log(tqqq).diff().fillna(0).values

    bil = bars["BIL"].set_index("date")["close"].reindex(sub_ind["date"]).ffill()
    bil_ret = bil.pct_change().fillna(0).values
    bil_log = np.log(bil).diff().fillna(0).values

    df = pd.DataFrame({
        "date": sub_ind["date"].values,
        "hh_state": hh,
        "dd_state": dd,
        "tqqq_ret": tqqq_ret,
        "bil_ret": bil_ret,
        "tqqq_log": tqqq_log,
        "bil_log": bil_log,
    })

    df["hh_in_tqqq"] = (df["hh_state"] == "TQQQ").astype(int)
    df["dd_in_tqqq"] = (df["dd_state"] == "TQQQ").astype(int)
    df["bucket"] = df.apply(
        lambda r: BUCKET_NAMES[(r["hh_in_tqqq"], r["dd_in_tqqq"])], axis=1)

    # log-return contribution to (HH - DD): HH log-return - DD log-return per day
    df["hh_log"] = np.where(df["hh_in_tqqq"] == 1, df["tqqq_log"], df["bil_log"])
    df["dd_log"] = np.where(df["dd_in_tqqq"] == 1, df["tqqq_log"], df["bil_log"])
    df["log_diff_hh_minus_dd"] = df["hh_log"] - df["dd_log"]

    bucket_rows = []
    total_diff = float(df["log_diff_hh_minus_dd"].sum())
    for b in ["both_TQQQ", "both_BIL", "HH_TQQQ_DD_BIL", "HH_BIL_DD_TQQQ"]:
        sub = df[df["bucket"] == b]
        cum_tqqq_ret = float(np.expm1(sub["tqqq_log"].sum())) if not sub.empty else 0.0
        log_diff_sum = float(sub["log_diff_hh_minus_dd"].sum())
        worst_day_tqqq = float(sub["tqqq_ret"].min()) if not sub.empty else 0.0
        bucket_rows.append({
            "bucket": b,
            "n_days": int(len(sub)),
            "pct_days": round(len(sub) / max(len(df), 1) * 100, 2),
            "cum_tqqq_return_in_bucket": round(cum_tqqq_ret, 4),
            "log_diff_contribution_HH_minus_DD": round(log_diff_sum, 4),
            "share_of_total_diff_pct": round(log_diff_sum / total_diff * 100, 2)
                                       if total_diff != 0 else None,
            "worst_day_tqqq_in_bucket": round(worst_day_tqqq, 4),
        })

    return df, pd.DataFrame(bucket_rows)


# ---------------------------------------------------------------------------
# BIL episodes for a strategy on window L (and pairing)
# ---------------------------------------------------------------------------

def bil_episodes(daily_df: pd.DataFrame, state_col: str,
                 tqqq_close: pd.Series) -> pd.DataFrame:
    """Find each consecutive run where state == 'BIL'."""
    states = (daily_df[state_col] == "BIL").astype(int).values
    edges = np.diff(np.r_[0, states, 0])
    starts = np.where(edges == 1)[0]
    ends = np.where(edges == -1)[0] - 1

    rows = []
    dates = daily_df["date"].values
    for s_i, e_i in zip(starts, ends):
        entry_to_bil = pd.Timestamp(dates[s_i])
        exit_from_bil = pd.Timestamp(dates[e_i])
        # the "exit-to-BIL day" carries TQQQ price = price at that day's close
        # (we use the close price for diagnosis only)
        p_at_exit = float(tqqq_close.loc[entry_to_bil]) \
            if entry_to_bil in tqqq_close.index else float("nan")
        p_at_reenter = float(tqqq_close.loc[exit_from_bil]) \
            if exit_from_bil in tqqq_close.index else float("nan")
        rows.append({
            "exit_to_BIL": entry_to_bil.date().isoformat(),
            "reenter_TQQQ": exit_from_bil.date().isoformat(),
            "BIL_days": int(e_i - s_i + 1),
            "tqqq_at_exit": round(p_at_exit, 2),
            "tqqq_at_reenter": round(p_at_reenter, 2),
            "tqqq_return_during_BIL": round(p_at_reenter / p_at_exit - 1.0, 4)
                                       if p_at_exit and not np.isnan(p_at_exit) else None,
        })
    return pd.DataFrame(rows)


def pair_bil_episodes(hh_eps: pd.DataFrame, dd_eps: pd.DataFrame,
                      tqqq_close: pd.Series, max_gap_days: int = 90) -> pd.DataFrame:
    """For each HH BIL episode, find the closest-in-time DD BIL episode
    by exit-to-BIL date. Report the gap in trading days and TQQQ return between."""
    if hh_eps.empty or dd_eps.empty:
        return pd.DataFrame()

    dd_dates = pd.to_datetime(dd_eps["exit_to_BIL"]).values

    rows = []
    for _, h in hh_eps.iterrows():
        h_exit = pd.Timestamp(h["exit_to_BIL"])
        gaps = (dd_dates - np.datetime64(h_exit)).astype("timedelta64[D]").astype(int)
        best_idx = int(np.argmin(np.abs(gaps)))
        d = dd_eps.iloc[best_idx]
        d_exit = pd.Timestamp(d["exit_to_BIL"])
        h_reenter = pd.Timestamp(h["reenter_TQQQ"])
        d_reenter = pd.Timestamp(d["reenter_TQQQ"])

        # TQQQ return between exit dates (HH exit date -> DD exit date)
        try:
            p_h_exit = float(tqqq_close.loc[h_exit]) if h_exit in tqqq_close.index else float("nan")
            p_d_exit = float(tqqq_close.loc[d_exit]) if d_exit in tqqq_close.index else float("nan")
            ret_between_exits = p_d_exit / p_h_exit - 1.0 if p_h_exit and not np.isnan(p_h_exit) else None
        except Exception:
            ret_between_exits = None
        try:
            p_h_re = float(tqqq_close.loc[h_reenter]) if h_reenter in tqqq_close.index else float("nan")
            p_d_re = float(tqqq_close.loc[d_reenter]) if d_reenter in tqqq_close.index else float("nan")
            ret_between_reenters = p_d_re / p_h_re - 1.0 if p_h_re and not np.isnan(p_h_re) else None
        except Exception:
            ret_between_reenters = None

        rows.append({
            "hh_exit": h["exit_to_BIL"],
            "dd_exit": d["exit_to_BIL"],
            "exit_gap_calendar_days": int((d_exit - h_exit).days),
            "tqqq_return_between_exits": round(ret_between_exits, 4)
                                          if ret_between_exits is not None else None,
            "hh_reenter": h["reenter_TQQQ"],
            "dd_reenter": d["reenter_TQQQ"],
            "reenter_gap_calendar_days": int((d_reenter - h_reenter).days),
            "tqqq_return_between_reenters": round(ret_between_reenters, 4)
                                              if ret_between_reenters is not None else None,
            "hh_BIL_days": h["BIL_days"],
            "dd_BIL_days": d["BIL_days"],
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--out-dir", default="out")
    p.add_argument("--no-cache", action="store_true")
    args = p.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    bars, vix = load_bars(use_cache=not args.no_cache)
    ind = compute_indicators(bars["QQQ"], vix)
    sigs = compute_raw_signals(ind)
    states_by_variant = compute_states(sigs)

    needed = ["sma200", "sma150", "sma50", "ret63", "dd126"]
    first_valid = ind.dropna(subset=needed)["date"].iloc[0]
    print(f"[run] first day all indicators valid: {first_valid.date()}", file=sys.stderr)

    metrics_rows, annual_rows, monthly_rows = [], [], []
    full_results = {}

    for variant in VARIANTS:
        full_results[variant] = {}
        for w_label, (s, e) in WINDOWS.items():
            full_results[variant][w_label] = {}
            start = max(pd.Timestamp(s), first_valid)
            end = pd.Timestamp(e)
            for slip in SLIPPAGE_BPS:
                m = simulate(states_by_variant[variant], ind, bars, start, end, slip)
                key = f"{variant} | window {w_label} ({start.date()}->{end.date()}) | slip {slip}bp"
                print(f"[done] {key}: total={m['total_return']*100:+.2f}%  DD={m['max_drawdown']*100:.2f}%  "
                      f"flips={m['flips']}  pass_2x={m['pass_2x_spy_return']}", file=sys.stderr)
                full_results[variant][w_label][slip] = m
                metrics_rows.append({
                    "variant": variant, "window": w_label, "slippage_bps": slip,
                    "start_date": str(start.date()), "end_date": str(end.date()),
                    **{k: v for k, v in m.items()
                       if k not in ("annual_returns", "monthly_returns", "trades")}
                })
                for y, v in m["annual_returns"].items():
                    annual_rows.append({"variant": variant, "window": w_label,
                                        "slippage_bps": slip, "year": y, "return": v})
                for ym, v in m["monthly_returns"].items():
                    monthly_rows.append({"variant": variant, "window": w_label,
                                         "slippage_bps": slip, "month": ym, "return": v})
                tdf = pd.DataFrame(m["trades"])
                if not tdf.empty:
                    tdf.to_csv(out_dir / f"trades_{variant}_{w_label}_{slip}bp.csv", index=False)

    pd.DataFrame(metrics_rows).to_csv(out_dir / "metrics.csv", index=False)
    pd.DataFrame(annual_rows).to_csv(out_dir / "annual_returns.csv", index=False)
    pd.DataFrame(monthly_rows).to_csv(out_dir / "monthly_returns.csv", index=False)

    # --- attribution on window L (the longer test) ---------------------------
    L_start = max(pd.Timestamp(WINDOWS["L"][0]), first_valid)
    L_end = pd.Timestamp(WINDOWS["L"][1])
    daily_df, bucket_df = daily_bucket_compare(
        states_by_variant, ind, bars, L_start, L_end)
    daily_df.to_csv(out_dir / "attribution_L.csv", index=False)
    bucket_df.to_csv(out_dir / "attribution_buckets.csv", index=False)

    tqqq_close = bars["TQQQ"].set_index("date")["close"]
    hh_eps = bil_episodes(daily_df.assign(date=pd.to_datetime(daily_df["date"])),
                          "hh_state", tqqq_close)
    dd_eps = bil_episodes(daily_df.assign(date=pd.to_datetime(daily_df["date"])),
                          "dd_state", tqqq_close)
    hh_eps.to_csv(out_dir / "bil_episodes_HH.csv", index=False)
    dd_eps.to_csv(out_dir / "bil_episodes_DD.csv", index=False)
    pairs = pair_bil_episodes(hh_eps, dd_eps, tqqq_close)
    pairs.to_csv(out_dir / "bil_episode_pairs.csv", index=False)

    # --- verdict gate for DH ------------------------------------------------
    DH = full_results["DH"]
    pass_2x_50bp = all(DH[w][50]["pass_2x_spy_return"] for w in WINDOWS)
    L_dd_50bp = DH["L"][50]["max_drawdown"]
    A_total_50bp = DH["A"][50]["total_return"]
    L_flips_50bp = DH["L"][50]["flips"]
    HH_L_flips_50bp = full_results["HH"]["L"][50]["flips"]
    DD_L_flips_50bp = full_results["DD"]["L"][50]["flips"]

    verdict = []
    verdict.append(f"DH passes 2x SPY in all 4 windows at 50bp: {pass_2x_50bp}")
    verdict.append(f"DH L max DD at 50bp: {L_dd_50bp*100:.2f}%  (gate: <= -53%)  "
                   f"{'PASS' if L_dd_50bp >= -0.53 else 'FAIL'}")
    verdict.append(f"DH window A total at 50bp: {A_total_50bp*100:+.2f}%")
    verdict.append(f"DH L flips at 50bp: {L_flips_50bp}  (HH={HH_L_flips_50bp}, DD={DD_L_flips_50bp})")

    # --- summary --------------------------------------------------------------
    write_summary(out_dir, full_results, bucket_df, pairs, verdict, daily_df)
    print(f"[done] wrote {out_dir}", file=sys.stderr)


def write_summary(out_dir: Path, results: dict, bucket_df: pd.DataFrame,
                  pairs: pd.DataFrame, verdict: list[str],
                  daily_df: pd.DataFrame) -> None:
    lines = []
    lines.append("=" * 100)
    lines.append("Entry/Exit Attribution: HYST × D2 (2x2 matrix)")
    lines.append("=" * 100)
    lines.append("HH = HYST entry + HYST exit  (= prior C1-HYST)")
    lines.append("DD = D2   entry + D2   exit  (= prior D2)")
    lines.append("DH = D2   entry + HYST exit  (proposed hybrid)")
    lines.append("HD = HYST entry + D2   exit  (inverse control)")
    lines.append("Conflict control: in BIL flip iff entry AND NOT exit; in TQQQ exit iff exit")
    lines.append("")

    # benchmarks per window from any variant (they share the BH series)
    lines.append("Benchmarks (buy & hold):")
    lines.append(f"  {'window':<6} {'SPY':>10} {'QQQ':>10} {'TQQQ':>10} {'2x SPY':>12}")
    for w in WINDOWS.keys():
        m = results["HH"][w][10]
        lines.append(f"  {w:<6} {m['spy_bh_total_return']*100:>9.2f}% {m['qqq_bh_total_return']*100:>9.2f}% "
                     f"{m['tqqq_bh_total_return']*100:>9.2f}% {m['target_2x_spy_return']*100:>11.2f}%")
    lines.append("")

    for w in WINDOWS.keys():
        lines.append(f"--- Window {w}: total / DD / Sharpe / flips / pass_2x at 10bp & 50bp ---")
        lines.append(f"  {'var':<4} {'10bp_tot':>10} {'10bp_DD':>10} {'10bp_Shp':>9} "
                     f"{'10bp_flips':>11} {'pass10':>7}  {'50bp_tot':>10} {'50bp_DD':>10} "
                     f"{'50bp_Shp':>9} {'50bp_flips':>11} {'pass50':>7}")
        for v in VARIANTS:
            m10 = results[v][w][10]
            m50 = results[v][w][50]
            sh10 = "n/a" if m10["sharpe"] is None else f"{m10['sharpe']:>9.2f}"
            sh50 = "n/a" if m50["sharpe"] is None else f"{m50['sharpe']:>9.2f}"
            lines.append(f"  {v:<4} {m10['total_return']*100:>+9.2f}% {m10['max_drawdown']*100:>+9.2f}% "
                         f"{sh10:>9} {m10['flips']:>11d} {str(m10['pass_2x_spy_return']):>7}  "
                         f"{m50['total_return']*100:>+9.2f}% {m50['max_drawdown']*100:>+9.2f}% "
                         f"{sh50:>9} {m50['flips']:>11d} {str(m50['pass_2x_spy_return']):>7}")
        lines.append("")

    # sanity: HH should reproduce C1-HYST and DD should reproduce D2
    lines.append("--- Reproducibility check (compare to prior non-MA run, 10bp) ---")
    for v in ("HH", "DD"):
        for w in WINDOWS.keys():
            m = results[v][w][10]
            lines.append(f"  {v} | {w} | total {m['total_return']*100:+.2f}%  "
                         f"DD {m['max_drawdown']*100:.2f}%  flips {m['flips']}")
    lines.append("")

    lines.append("--- Window L bucket attribution (HH state vs DD state, daily) ---")
    lines.append(f"  total log-return diff (HH - DD) over window L = "
                 f"{daily_df['log_diff_hh_minus_dd'].sum():+.4f}")
    lines.append("  (expm1 of that ≈ multiplicative edge of HH over DD)")
    lines.append(f"  {'bucket':<22} {'days':>6} {'pct_days':>9} {'cum_TQQQ':>10} "
                 f"{'log_diff':>10} {'share_pct':>11} {'worst_day_TQQQ':>16}")
    for _, r in bucket_df.iterrows():
        lines.append(f"  {r['bucket']:<22} {r['n_days']:>6d} {r['pct_days']:>8.2f}% "
                     f"{r['cum_tqqq_return_in_bucket']*100:>+9.2f}% "
                     f"{r['log_diff_contribution_HH_minus_DD']:>+9.4f} "
                     f"{(r['share_of_total_diff_pct'] or 0):>+10.2f}% "
                     f"{r['worst_day_tqqq_in_bucket']*100:>+15.2f}%")
    lines.append("")

    if not pairs.empty:
        lines.append("--- BIL episode pairs HH<->DD on window L (TQQQ return between exit/re-entry dates) ---")
        lines.append(f"  {'hh_exit':<12} {'dd_exit':<12} {'gap':>5} {'tqqq_btw_exits':>15} "
                     f"{'hh_reent':<12} {'dd_reent':<12} {'gap':>5} {'tqqq_btw_re':>13}")
        for _, r in pairs.iterrows():
            tbe = (f"{r['tqqq_return_between_exits']*100:+.2f}%"
                   if r['tqqq_return_between_exits'] is not None else "    -")
            tbr = (f"{r['tqqq_return_between_reenters']*100:+.2f}%"
                   if r['tqqq_return_between_reenters'] is not None else "    -")
            lines.append(f"  {r['hh_exit']:<12} {r['dd_exit']:<12} "
                         f"{r['exit_gap_calendar_days']:>+5d} {tbe:>15}  "
                         f"{r['hh_reenter']:<12} {r['dd_reenter']:<12} "
                         f"{r['reenter_gap_calendar_days']:>+5d} {tbr:>13}")
        lines.append("")

    lines.append("--- DH verdict gate ---")
    for v in verdict:
        lines.append(f"  {v}")
    lines.append("")

    (out_dir / "summary.txt").write_text("\n".join(lines))


if __name__ == "__main__":
    main()
