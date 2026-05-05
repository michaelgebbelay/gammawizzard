#!/usr/bin/env python3
"""
Test (b): does VIX/VIX3M flipping into backwardation within 10td of a 200d
cross-down filter the false-positive rate?

Population: post-2007-12-04 below-200d cross-downs (VIX3M starts then).
Label: depth_bucket == "bear"  ⇔ intra-episode max distance below MA >= 15%.
Filter: max(VIX/VIX3M) over the window [cross_day .. cross_day+10td] > 1.0.

Output is a 2x2 contingency, precision/recall/FPR, and the full daily
VIX/VIX3M trajectories for every bear and every flipped-but-not-bear case
(so the failure modes can be inspected).

Re-fetches ^GSPC, ^VIX, ^VIX3M from yfinance — independent of the earlier
study so it can run as a standalone job.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import yfinance as yf


SMA_SLOW = 200
POST_CROSS_TD = 10
DEPTH_BEAR_THRESHOLD = 0.15
BACKWARDATION_LEVEL = 1.0


def fetch(ticker: str, start: str, end: str) -> pd.Series:
    df = yf.download(ticker, start=start, end=end, auto_adjust=True,
                     progress=False, threads=False)
    if df is None or df.empty:
        raise SystemExit(f"yfinance: no data for {ticker}")
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    s = df["Close"].astype(float)
    s.index = pd.to_datetime(s.index).tz_localize(None).normalize()
    s = s[~s.index.duplicated(keep="first")].sort_index()
    s.name = ticker
    return s


def find_below200_episodes(spx: pd.Series) -> pd.DataFrame:
    sma = spx.rolling(SMA_SLOW, min_periods=SMA_SLOW).mean()
    df = pd.concat({"spx": spx, "sma": sma}, axis=1).dropna(subset=["sma"])
    below = (df["spx"] < df["sma"]).astype(int).values
    edges = np.diff(np.r_[0, below, 0])
    starts = np.where(edges == 1)[0]
    ends = np.where(edges == -1)[0] - 1

    rows = []
    for s_i, e_i in zip(starts, ends):
        seg_spx = df["spx"].values[s_i : e_i + 1]
        seg_ma = df["sma"].values[s_i : e_i + 1]
        depth = ((seg_ma - seg_spx) / seg_ma).max()
        peak = np.maximum.accumulate(seg_spx)
        intra_dd = ((peak - seg_spx) / peak).max()
        rows.append({
            "start": df.index[s_i],
            "end": df.index[e_i],
            "duration_td": int(e_i - s_i + 1),
            "depth_pct_below_ma": float(depth),
            "intra_drawdown": float(intra_dd),
            "is_bear": float(depth) >= DEPTH_BEAR_THRESHOLD,
        })
    return pd.DataFrame(rows)


def attach_post_cross_trajectory(eps: pd.DataFrame, ratio: pd.Series) -> pd.DataFrame:
    """For each cross-down, pull VIX/VIX3M for cross_day .. cross_day+10td."""
    out = eps.copy()
    idx = ratio.index
    cols_d = [f"d{k}" for k in range(POST_CROSS_TD + 1)]
    for c in cols_d:
        out[c] = np.nan

    for i, row in out.iterrows():
        pos = idx.get_indexer([row["start"]], method="ffill")[0]
        if pos < 0:
            continue
        for k in range(POST_CROSS_TD + 1):
            q = pos + k
            if 0 <= q < len(idx):
                out.at[i, f"d{k}"] = float(ratio.iloc[q])

    out["max_ratio_in_window"] = out[cols_d].max(axis=1)
    out["min_ratio_in_window"] = out[cols_d].min(axis=1)
    out["flipped_backwardation"] = out["max_ratio_in_window"] > BACKWARDATION_LEVEL
    out["days_to_first_flip"] = out[cols_d].apply(
        lambda r: int(np.argmax(r.values > BACKWARDATION_LEVEL))
                  if (r.values > BACKWARDATION_LEVEL).any() else -1,
        axis=1,
    )
    return out


def confusion_matrix(eps: pd.DataFrame) -> dict:
    flipped = eps["flipped_backwardation"]
    bear = eps["is_bear"]
    tp = int(((flipped) & (bear)).sum())
    fp = int(((flipped) & (~bear)).sum())
    fn = int(((~flipped) & (bear)).sum())
    tn = int(((~flipped) & (~bear)).sum())
    n = tp + fp + fn + tn
    precision = tp / (tp + fp) if (tp + fp) else float("nan")
    recall = tp / (tp + fn) if (tp + fn) else float("nan")
    fpr = fp / (fp + tn) if (fp + tn) else float("nan")
    base_rate_bear = (tp + fn) / n if n else float("nan")
    return dict(tp=tp, fp=fp, fn=fn, tn=tn, n=n,
                precision=precision, recall=recall, fpr=fpr,
                base_rate_bear=base_rate_bear)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2007-01-01")
    ap.add_argument("--end", default=pd.Timestamp.today().strftime("%Y-%m-%d"))
    ap.add_argument("--out-dir", default="out")
    args = ap.parse_args()

    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)

    print(f"[load] ^GSPC + ^VIX + ^VIX3M  {args.start}..{args.end}", file=sys.stderr)
    # Fetch SPX from earlier so the 200d MA is defined by the time VIX3M starts.
    spx = fetch("^GSPC", "2006-01-01", args.end)
    vix = fetch("^VIX", "2006-01-01", args.end)
    try:
        vix3m = fetch("^VIX3M", "2006-01-01", args.end)
    except SystemExit:
        vix3m = fetch("^VXV", "2006-01-01", args.end)
    panel = pd.concat({"vix": vix, "vix3m": vix3m}, axis=1).dropna()
    ratio = (panel["vix"] / panel["vix3m"]).rename("vix_vix3m")
    print(f"[load] ratio panel: {ratio.index[0].date()} .. {ratio.index[-1].date()}",
          file=sys.stderr)

    eps = find_below200_episodes(spx)
    eps = eps[eps["start"] >= ratio.index[0]].reset_index(drop=True)
    print(f"[eps] post-VIX3M below-200 episodes: {len(eps)}  "
          f"bear count: {int(eps['is_bear'].sum())}", file=sys.stderr)

    eps = attach_post_cross_trajectory(eps, ratio)

    eps["start"] = eps["start"].dt.date.astype(str)
    eps["end"] = eps["end"].dt.date.astype(str)

    cm = confusion_matrix(eps)
    cm_df = pd.DataFrame([cm])
    cm_df.to_csv(out / "test_b_confusion.csv", index=False)

    bear_traj = eps[eps["is_bear"]].copy()
    fp_traj = eps[(eps["flipped_backwardation"]) & (~eps["is_bear"])].copy()
    fn_traj = eps[(~eps["flipped_backwardation"]) & (eps["is_bear"])].copy()

    bear_traj.to_csv(out / "test_b_bear_trajectories.csv", index=False)
    fp_traj.to_csv(out / "test_b_false_positive_trajectories.csv", index=False)
    fn_traj.to_csv(out / "test_b_missed_bear_trajectories.csv", index=False)
    eps.to_csv(out / "test_b_all_episodes.csv", index=False)

    # human readable
    lines = ["# Test (b): VIX/VIX3M backwardation flip within 10td of 200d cross-down\n"]
    lines.append(f"- panel: ratio data starts {ratio.index[0].date()}, ends {ratio.index[-1].date()}")
    lines.append(f"- below-200 episodes in window: **{cm['n']}**")
    lines.append(f"- bear-bucket episodes (depth >= {int(DEPTH_BEAR_THRESHOLD*100)}% below MA): **{cm['tp']+cm['fn']}**  "
                 f"(base rate {cm['base_rate_bear']:.1%})")
    lines.append("")
    lines.append("## 2x2 contingency\n")
    lines.append("|                            | bear | non-bear |")
    lines.append("|----------------------------|------|----------|")
    lines.append(f"| flipped backwardation      | {cm['tp']}    | {cm['fp']}        |")
    lines.append(f"| didn't flip                | {cm['fn']}    | {cm['tn']}        |")
    lines.append("")
    lines.append("## Filter performance\n")
    lines.append(f"- **precision** (P(bear | flipped))   = **{cm['precision']:.3f}**  "
                 f"({cm['tp']} / {cm['tp']+cm['fp']})")
    lines.append(f"- **recall** (P(flipped | bear))      = **{cm['recall']:.3f}**  "
                 f"({cm['tp']} / {cm['tp']+cm['fn']})")
    lines.append(f"- false-positive rate (P(flipped | non-bear)) = {cm['fpr']:.3f}  "
                 f"({cm['fp']} / {cm['fp']+cm['tn']})")
    lines.append("")

    lines.append("## Bear cohort — daily VIX/VIX3M day 0..10\n")
    cols_d = [f"d{k}" for k in range(POST_CROSS_TD + 1)]
    if bear_traj.empty:
        lines.append("(none)")
    else:
        for _, r in bear_traj.iterrows():
            traj = "  ".join(f"{r[c]:.3f}" if pd.notna(r[c]) else "  -  " for c in cols_d)
            flip = "FLIPPED" if r["flipped_backwardation"] else "no flip"
            d2f = r["days_to_first_flip"]
            lines.append(f"- **{r['start']}** dur={r['duration_td']}td "
                         f"depth={r['depth_pct_below_ma']:.1%}  intra_dd={r['intra_drawdown']:.1%}  "
                         f"[{flip}, first flip d{d2f if d2f>=0 else '-'}]")
            lines.append(f"  - days 0..10: {traj}")
    lines.append("")

    lines.append("## False positives — flipped but no bear (failure modes)\n")
    if fp_traj.empty:
        lines.append("(none)")
    else:
        # sort by max_ratio (deepest backwardation first)
        fp_sorted = fp_traj.sort_values("max_ratio_in_window", ascending=False)
        for _, r in fp_sorted.iterrows():
            traj = "  ".join(f"{r[c]:.3f}" if pd.notna(r[c]) else "  -  " for c in cols_d)
            d2f = r["days_to_first_flip"]
            lines.append(f"- {r['start']} dur={r['duration_td']}td "
                         f"depth={r['depth_pct_below_ma']:.1%}  intra_dd={r['intra_drawdown']:.1%}  "
                         f"[max ratio {r['max_ratio_in_window']:.3f}, first flip d{d2f}]")
            lines.append(f"  - days 0..10: {traj}")
    lines.append("")

    if not fn_traj.empty:
        lines.append("## Missed bears — bear but did not flip in 10td (recall failures)\n")
        for _, r in fn_traj.iterrows():
            traj = "  ".join(f"{r[c]:.3f}" if pd.notna(r[c]) else "  -  " for c in cols_d)
            lines.append(f"- {r['start']} dur={r['duration_td']}td "
                         f"depth={r['depth_pct_below_ma']:.1%}  intra_dd={r['intra_drawdown']:.1%}  "
                         f"[max ratio {r['max_ratio_in_window']:.3f}]")
            lines.append(f"  - days 0..10: {traj}")
        lines.append("")

    lines.append("## Verdict gate\n")
    if cm["precision"] >= 0.30:
        verdict = "**Filter passes (precision >= 30%).** Worth operationalizing."
    elif cm["precision"] >= 0.20:
        verdict = "**Filter is borderline (20-30% precision).** Possibly useful with a second confirmation."
    else:
        verdict = (f"**Filter fails (precision = {cm['precision']:.1%} < 20%).** "
                   "Doctrine: trend filters are coin flips on this universe. "
                   "Position sizing and DD stops are the only real defense.")
    lines.append(verdict)

    (out / "summary.md").write_text("\n".join(lines) + "\n")
    print((out / "summary.md").read_text())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
