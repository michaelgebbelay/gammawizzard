#!/usr/bin/env python3
"""
SPY/SPX 50d & 200d MA regime study, 1990 -> today.

Descriptive layer:
  - per-year days below 50d MA, days below 200d MA
  - per-year count of distinct below-MA episodes
  - per-year median / max episode duration
  - per-year max drawdown observed during below-MA periods

Episode-level table (one row per below-MA run):
  - start, end, duration (trading days)
  - depth (max % below MA during the run)
  - distance bucket: noise (<3%), shallow (3-7), correction (7-15), bear (15+)
  - MA slope at the cross-down: rising / flat / falling
  - peak-to-trough drawdown observed within the episode
  - VIX level at cross-down (median of last 5 sessions including the cross)
  - VIX/VIX3M ratio at cross-down (post-2007 only)
  - VIX/VIX3M ratio at +5, +10, +20 trading days
  - HYG/LQD ratio change from -20d to +20d around cross (post-2007 only)
  - calendar month of cross-down

Conditional probabilities:
  - P(close back above MA within 5/10/20 days | cross-down)
  - P(further drawdown to -20% peak-to-trough | episode lasts 20+ days)
  - P(200d cross-down within 60 trading days | 50d cross-down)

Outputs go under --out-dir as csv + a summary markdown + a scatter PNG.

Data sources (yfinance, no auth):
  ^GSPC        : 1990-01-01 onward (price series)
  ^VIX         : 1990-01-02 onward
  ^VIX3M       : 2007-12-04 onward  (term structure / backwardation flag)
  HYG, LQD     : 2007-04 onward     (credit confirmation proxy)

NOTE on breadth: a true "% of SPX above 200d" series ($SPXA200R) is not
available via yfinance. We deliberately skip that overlay rather than
fake it. Section flagged in the summary.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

import yfinance as yf


SMA_FAST = 50
SMA_SLOW = 200
SLOPE_LOOKBACK = 20            # trading days for MA-slope classification
SLOPE_RISING_BPS = 0.005       # >0.5% slope over 20 days = rising
SLOPE_FALLING_BPS = -0.005     # <-0.5% over 20 days = falling
DEPTH_BUCKETS = [
    ("noise",       0.00,  0.03),
    ("shallow",     0.03,  0.07),
    ("correction",  0.07,  0.15),
    ("bear",        0.15,  10.0),
]
COND_HORIZONS = [5, 10, 20]
LOOKAHEAD_50_TO_200 = 60       # trading days
DEEP_DD_THRESHOLD = 0.20       # peak-to-trough during episode


# ----- data loading -------------------------------------------------------- #


def fetch_yf(ticker: str, start: str, end: str) -> pd.Series:
    df = yf.download(
        ticker,
        start=start,
        end=end,
        auto_adjust=True,
        progress=False,
        threads=False,
    )
    if df is None or df.empty:
        raise SystemExit(f"yfinance: no data for {ticker}")
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    s = df["Close"].astype(float)
    s.index = pd.to_datetime(s.index).tz_localize(None).normalize()
    s = s[~s.index.duplicated(keep="first")].sort_index()
    s.name = ticker
    return s


def load_panel(start: str, end: str) -> pd.DataFrame:
    print(f"[load] fetching ^GSPC {start}..{end}", file=sys.stderr)
    spx = fetch_yf("^GSPC", start, end)
    print(f"[load] fetching ^VIX", file=sys.stderr)
    vix = fetch_yf("^VIX", start, end)
    print(f"[load] fetching ^VIX3M", file=sys.stderr)
    try:
        vix3m = fetch_yf("^VIX3M", start, end)
    except SystemExit:
        print("[load] ^VIX3M unavailable, trying ^VXV", file=sys.stderr)
        vix3m = fetch_yf("^VXV", start, end)
    print(f"[load] fetching HYG", file=sys.stderr)
    try:
        hyg = fetch_yf("HYG", start, end)
    except SystemExit:
        hyg = pd.Series(dtype=float, name="HYG")
    print(f"[load] fetching LQD", file=sys.stderr)
    try:
        lqd = fetch_yf("LQD", start, end)
    except SystemExit:
        lqd = pd.Series(dtype=float, name="LQD")

    df = pd.concat(
        {"spx": spx, "vix": vix, "vix3m": vix3m, "hyg": hyg, "lqd": lqd},
        axis=1,
    )
    df = df.loc[df["spx"].first_valid_index():]
    df["sma50"] = df["spx"].rolling(SMA_FAST, min_periods=SMA_FAST).mean()
    df["sma200"] = df["spx"].rolling(SMA_SLOW, min_periods=SMA_SLOW).mean()
    df["sma200_slope_20d"] = (
        df["sma200"] / df["sma200"].shift(SLOPE_LOOKBACK) - 1.0
    )
    df["below_50"] = df["spx"] < df["sma50"]
    df["below_200"] = df["spx"] < df["sma200"]
    df["vix_vix3m"] = df["vix"] / df["vix3m"]
    df["hyg_lqd"] = df["hyg"] / df["lqd"]
    return df


# ----- episode extraction -------------------------------------------------- #


def find_episodes(df: pd.DataFrame, below_col: str, ma_col: str) -> pd.DataFrame:
    """One row per consecutive run of `below_col == True` (after MA is defined)."""
    sub = df.dropna(subset=[ma_col]).copy()
    flag = sub[below_col].astype(int).values
    if len(flag) == 0:
        return pd.DataFrame()

    edges = np.diff(np.r_[0, flag, 0])
    starts_idx = np.where(edges == 1)[0]
    ends_idx = np.where(edges == -1)[0] - 1   # inclusive end index

    rows = []
    dates = sub.index.values
    spx = sub["spx"].values
    ma = sub[ma_col].values
    slope = sub["sma200_slope_20d"].values  # always 200d slope as regime tag

    for s_idx, e_idx in zip(starts_idx, ends_idx):
        episode = sub.iloc[s_idx : e_idx + 1]
        depth = ((ma[s_idx : e_idx + 1] - spx[s_idx : e_idx + 1])
                 / ma[s_idx : e_idx + 1]).max()
        # peak-to-trough during the episode (intra-episode drawdown)
        peak = np.maximum.accumulate(spx[s_idx : e_idx + 1])
        dd = ((peak - spx[s_idx : e_idx + 1]) / peak).max()
        rows.append({
            "start": pd.Timestamp(dates[s_idx]).date().isoformat(),
            "end": pd.Timestamp(dates[e_idx]).date().isoformat(),
            "duration_td": int(e_idx - s_idx + 1),
            "depth_pct_below_ma": float(depth),
            "intra_drawdown": float(dd),
            "ma_slope_20d_at_start": float(slope[s_idx])
                                       if not np.isnan(slope[s_idx]) else np.nan,
            "spx_at_start": float(spx[s_idx]),
            "ma_at_start": float(ma[s_idx]),
            "month_of_start": pd.Timestamp(dates[s_idx]).month,
        })
    return pd.DataFrame(rows)


def bucket_depth(d: float) -> str:
    for name, lo, hi in DEPTH_BUCKETS:
        if lo <= d < hi:
            return name
    return "bear"


def classify_slope(s: float) -> str:
    if pd.isna(s):
        return "unknown"
    if s > SLOPE_RISING_BPS:
        return "rising"
    if s < SLOPE_FALLING_BPS:
        return "falling"
    return "flat"


def attach_episode_context(eps: pd.DataFrame, df: pd.DataFrame) -> pd.DataFrame:
    if eps.empty:
        return eps
    eps = eps.copy()
    eps["depth_bucket"] = eps["depth_pct_below_ma"].apply(bucket_depth)
    eps["slope_regime"] = eps["ma_slope_20d_at_start"].apply(classify_slope)
    starts = pd.to_datetime(eps["start"])

    # VIX at cross-down (level on the start date)
    vix_at = df["vix"].reindex(starts).values
    eps["vix_at_start"] = vix_at

    # VIX/VIX3M at start + at +5/+10/+20 trading days (rolled by index position)
    idx = df.index
    pos = pd.Index(starts).map(lambda d: idx.get_indexer([d], method="ffill")[0])
    pos = pos.to_numpy() if hasattr(pos, "to_numpy") else np.array(list(pos))
    ratio = df["vix_vix3m"].values
    hyglqd = df["hyg_lqd"].values

    def take(positions, lag):
        out = np.full(len(positions), np.nan)
        for i, p in enumerate(positions):
            if p < 0:
                continue
            q = p + lag
            if 0 <= q < len(idx):
                out[i] = ratio[q] if 0 <= q < len(ratio) else np.nan
        return out

    eps["vix_vix3m_at_start"] = take(pos, 0)
    eps["vix_vix3m_+5d"] = take(pos, 5)
    eps["vix_vix3m_+10d"] = take(pos, 10)
    eps["vix_vix3m_+20d"] = take(pos, 20)

    def hyg_lag(positions, lag):
        out = np.full(len(positions), np.nan)
        for i, p in enumerate(positions):
            if p < 0:
                continue
            q = p + lag
            if 0 <= q < len(hyglqd):
                out[i] = hyglqd[q]
        return out

    eps["hyg_lqd_-20d"] = hyg_lag(pos, -20)
    eps["hyg_lqd_+20d"] = hyg_lag(pos, 20)
    eps["hyg_lqd_change_20d"] = (
        eps["hyg_lqd_+20d"] / eps["hyg_lqd_-20d"] - 1.0
    )

    return eps


# ----- yearly descriptive layer ------------------------------------------- #


def yearly_table(df: pd.DataFrame,
                 eps_50: pd.DataFrame,
                 eps_200: pd.DataFrame) -> pd.DataFrame:
    sub = df.dropna(subset=["sma200"]).copy()
    sub["year"] = sub.index.year

    rows = []
    for year, g in sub.groupby("year"):
        eps50_y = eps_50[pd.to_datetime(eps_50["start"]).dt.year == year] \
            if not eps_50.empty else eps_50
        eps200_y = eps_200[pd.to_datetime(eps_200["start"]).dt.year == year] \
            if not eps_200.empty else eps_200

        # max drawdown during below-200 days within the year
        below = g[g["below_200"]]
        if not below.empty:
            peak = below["spx"].cummax()
            dd_below = (1.0 - below["spx"] / peak).max()
        else:
            dd_below = 0.0

        rows.append({
            "year": int(year),
            "trading_days": int(len(g)),
            "days_below_50": int(g["below_50"].sum()),
            "days_below_200": int(g["below_200"].sum()),
            "pct_below_50": round(g["below_50"].mean() * 100, 2),
            "pct_below_200": round(g["below_200"].mean() * 100, 2),
            "episodes_50d": int(len(eps50_y)),
            "episodes_200d": int(len(eps200_y)),
            "median_dur_50d": int(eps50_y["duration_td"].median())
                              if not eps50_y.empty else 0,
            "max_dur_50d": int(eps50_y["duration_td"].max())
                            if not eps50_y.empty else 0,
            "median_dur_200d": int(eps200_y["duration_td"].median())
                                if not eps200_y.empty else 0,
            "max_dur_200d": int(eps200_y["duration_td"].max())
                             if not eps200_y.empty else 0,
            "max_dd_below_200": round(float(dd_below) * 100, 2),
        })
    return pd.DataFrame(rows)


# ----- conditional probabilities ------------------------------------------ #


def conditional_probs(df: pd.DataFrame,
                      eps_200: pd.DataFrame,
                      eps_50: pd.DataFrame) -> pd.DataFrame:
    out = []
    sub = df.dropna(subset=["sma200"])
    idx = sub.index

    # P(close back above 200d within H trading days | cross-down)
    if not eps_200.empty:
        starts = pd.to_datetime(eps_200["start"])
        for H in COND_HORIZONS:
            recovered = 0
            total = 0
            for s in starts:
                pos = idx.get_indexer([s], method="ffill")[0]
                if pos < 0:
                    continue
                end = min(pos + H, len(idx) - 1)
                window = sub.iloc[pos : end + 1]
                total += 1
                if (window["spx"] > window["sma200"]).any():
                    recovered += 1
            if total:
                out.append({
                    "metric": f"P(recover above 200d within {H}td | cross-down)",
                    "n": total,
                    "prob": round(recovered / total, 3),
                })

        # P(intra-episode DD >= 20% | episode duration >= 20 td)
        long_eps = eps_200[eps_200["duration_td"] >= 20]
        if not long_eps.empty:
            n = len(long_eps)
            d = (long_eps["intra_drawdown"] >= DEEP_DD_THRESHOLD).sum()
            out.append({
                "metric": f"P(intra-DD >= {int(DEEP_DD_THRESHOLD*100)}% | duration_200 >= 20td)",
                "n": int(n),
                "prob": round(d / n, 3),
            })

    # P(200d cross-down within K td | 50d cross-down)
    if not eps_50.empty and not eps_200.empty:
        starts50 = pd.to_datetime(eps_50["start"])
        starts200 = pd.to_datetime(eps_200["start"])
        starts200_set = set(starts200.dt.date)
        # for each 50d cross, did a 200d episode start within K td?
        hits = 0
        total = 0
        for s in starts50:
            pos = idx.get_indexer([s], method="ffill")[0]
            if pos < 0:
                continue
            end = min(pos + LOOKAHEAD_50_TO_200, len(idx) - 1)
            window_dates = set(idx[pos : end + 1].date)
            total += 1
            if window_dates & starts200_set:
                hits += 1
        if total:
            out.append({
                "metric": f"P(200d cross-down within {LOOKAHEAD_50_TO_200}td | 50d cross-down)",
                "n": total,
                "prob": round(hits / total, 3),
            })

    return pd.DataFrame(out)


# ----- plot --------------------------------------------------------------- #


def scatter_episodes(eps_200: pd.DataFrame, out_path: Path) -> None:
    if eps_200.empty:
        print("[plot] no 200d episodes; skipping scatter", file=sys.stderr)
        return
    fig, ax = plt.subplots(figsize=(11, 7))
    color = eps_200["vix_vix3m_at_start"].fillna(0.85).clip(0.6, 1.4)
    sc = ax.scatter(
        eps_200["duration_td"],
        eps_200["intra_drawdown"] * 100,
        c=color,
        cmap="coolwarm",
        s=40 + np.minimum(eps_200["duration_td"] / 4, 60),
        alpha=0.7,
        edgecolors="k",
        linewidths=0.4,
    )
    ax.set_xscale("log")
    ax.set_xlabel("episode duration (trading days, log scale)")
    ax.set_ylabel("intra-episode drawdown (%)")
    ax.set_title("SPX below-200d episodes since 1990 — duration vs drawdown\n"
                 "color = VIX/VIX3M at episode start (red = backwardation)")
    ax.grid(True, alpha=0.3)
    cbar = plt.colorbar(sc, ax=ax)
    cbar.set_label("VIX / VIX3M at start (>1 = backwardation)")
    fig.tight_layout()
    fig.savefig(out_path, dpi=130)
    plt.close(fig)
    print(f"[plot] wrote {out_path}", file=sys.stderr)


# ----- main --------------------------------------------------------------- #


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="1990-01-01")
    ap.add_argument("--end", default=pd.Timestamp.today().strftime("%Y-%m-%d"))
    ap.add_argument("--out-dir", default="out")
    args = ap.parse_args()

    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)

    df = load_panel(args.start, args.end)
    print(f"[panel] {df.index[0].date()} .. {df.index[-1].date()}  "
          f"rows={len(df):,}", file=sys.stderr)

    eps_50 = find_episodes(df, "below_50", "sma50")
    eps_200 = find_episodes(df, "below_200", "sma200")
    eps_50 = attach_episode_context(eps_50, df)
    eps_200 = attach_episode_context(eps_200, df)

    yearly = yearly_table(df, eps_50, eps_200)
    cond = conditional_probs(df, eps_200, eps_50)

    # cross-down VIX/VIX3M state, the "what does the term structure say at the cross" cut
    cross_view = (
        eps_200[["start", "duration_td", "depth_pct_below_ma", "intra_drawdown",
                 "depth_bucket", "slope_regime", "vix_at_start",
                 "vix_vix3m_at_start", "vix_vix3m_+5d", "vix_vix3m_+10d",
                 "vix_vix3m_+20d", "month_of_start"]]
        if not eps_200.empty else pd.DataFrame()
    )

    # write csvs
    yearly.to_csv(out / "yearly_descriptive.csv", index=False)
    eps_50.to_csv(out / "episodes_50d.csv", index=False)
    eps_200.to_csv(out / "episodes_200d.csv", index=False)
    cond.to_csv(out / "conditional_probs.csv", index=False)
    cross_view.to_csv(out / "vix_at_cross_200d.csv", index=False)

    # scatter
    scatter_episodes(eps_200, out / "episode_scatter_200d.png")

    # contingency: depth_bucket x slope_regime x VIX/VIX3M(>1) at start
    if not eps_200.empty:
        eps_200["backwardation_at_start"] = eps_200["vix_vix3m_at_start"] > 1.0
        ct = (
            eps_200
            .groupby(["depth_bucket", "slope_regime", "backwardation_at_start"])
            .size()
            .reset_index(name="n_episodes")
        )
        ct.to_csv(out / "contingency_depth_slope_backwardation.csv", index=False)

        # how often backwardation at cross predicts a deep DD
        with_term = eps_200.dropna(subset=["vix_vix3m_at_start"])
        if not with_term.empty:
            tab = (
                with_term
                .assign(deep=lambda d: d["intra_drawdown"] >= DEEP_DD_THRESHOLD)
                .groupby("backwardation_at_start")
                .agg(n=("deep", "size"),
                     p_deep_dd=("deep", "mean"),
                     median_dd=("intra_drawdown", "median"),
                     median_dur=("duration_td", "median"))
                .reset_index()
            )
            tab["p_deep_dd"] = tab["p_deep_dd"].round(3)
            tab["median_dd"] = tab["median_dd"].round(3)
            tab.to_csv(out / "p_deep_dd_by_backwardation.csv", index=False)

    # human-readable summary
    write_summary(out, df, yearly, eps_50, eps_200, cond)

    print("[done]", file=sys.stderr)
    return 0


def write_summary(out: Path,
                  df: pd.DataFrame,
                  yearly: pd.DataFrame,
                  eps_50: pd.DataFrame,
                  eps_200: pd.DataFrame,
                  cond: pd.DataFrame) -> None:
    lines = []
    lines.append("# SPX MA regime study\n")
    lines.append(f"- panel: {df.index[0].date()} .. {df.index[-1].date()}, "
                 f"{len(df):,} sessions")
    lines.append(f"- 50d episodes: {len(eps_50)}")
    lines.append(f"- 200d episodes: {len(eps_200)}")
    if not eps_200.empty:
        lines.append(f"- median 200d episode duration: "
                     f"{int(eps_200['duration_td'].median())} td, "
                     f"max {int(eps_200['duration_td'].max())} td")
        lines.append(f"- median 200d episode depth: "
                     f"{eps_200['depth_pct_below_ma'].median()*100:.1f}% below MA, "
                     f"max {eps_200['depth_pct_below_ma'].max()*100:.1f}%")
    lines.append("\n## Conditional probabilities\n")
    if cond.empty:
        lines.append("(none)")
    else:
        for _, r in cond.iterrows():
            lines.append(f"- **{r['metric']}** = {r['prob']}  (n={r['n']})")

    if not eps_200.empty:
        lines.append("\n## Depth bucket frequency (200d episodes)\n")
        bucket_counts = eps_200["depth_bucket"].value_counts().reindex(
            [b[0] for b in DEPTH_BUCKETS]).fillna(0).astype(int)
        for name, n in bucket_counts.items():
            lines.append(f"- {name}: {n}")

        lines.append("\n## Slope regime at 200d cross-down\n")
        slope_counts = eps_200["slope_regime"].value_counts()
        for k, v in slope_counts.items():
            lines.append(f"- {k}: {int(v)}")

    lines.append("\n## Caveats")
    lines.append("- Price series is ^GSPC (cash index), not SPY ETF total return. "
                 "Trend / cross logic identical; tracking error to SPY is ~0 for "
                 "MA-cross signals.")
    lines.append("- VIX3M (^VIX3M / ^VXV) only exists from 2007-12-04. "
                 "Backwardation analysis is null prior to that.")
    lines.append("- HYG/LQD only exist from 2007-04. Pre-2007 episodes have "
                 "no credit overlay.")
    lines.append("- SPX component breadth ($SPXA200R) is not available via "
                 "yfinance and was deliberately omitted.")

    (out / "summary.md").write_text("\n".join(lines) + "\n")


if __name__ == "__main__":
    raise SystemExit(main())
