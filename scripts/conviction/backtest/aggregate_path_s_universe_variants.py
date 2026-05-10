#!/usr/bin/env python3
"""Aggregate Path S universe-variant artifacts into comparison tables."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
from pandas.errors import EmptyDataError


HERE = Path(__file__).resolve().parent
DATA_DIR = HERE / "data"


def _calmar(cagr: float | None, max_drawdown: float | None) -> float | None:
    if cagr is None or max_drawdown is None or max_drawdown >= 0:
        return None
    return float(cagr / abs(max_drawdown))


def _norm_date(value) -> pd.Timestamp:
    return pd.Timestamp(value).normalize()


def _load_packages(artifacts_dir: Path) -> list[dict]:
    packages: list[dict] = []
    for meta_path in sorted(artifacts_dir.rglob("variant_meta.json")):
        pkg_dir = meta_path.parent
        meta = json.loads(meta_path.read_text())
        replay_dir = pkg_dir / "replay_run"
        summary = json.loads((replay_dir / "summary.json").read_text())
        if (replay_dir / "trade_log.csv").exists():
            try:
                trades = pd.read_csv(replay_dir / "trade_log.csv")
            except EmptyDataError:
                trades = pd.DataFrame()
        else:
            trades = pd.DataFrame()
        daily = pd.read_csv(replay_dir / "daily_equity.csv") if (replay_dir / "daily_equity.csv").exists() else pd.DataFrame()
        diag = pd.read_csv(pkg_dir / "daily_signal_diagnostics.csv") if (pkg_dir / "daily_signal_diagnostics.csv").exists() else pd.DataFrame()
        packages.append(
            {
                "pkg_dir": pkg_dir,
                "meta": meta,
                "summary": summary,
                "trades": trades,
                "daily": daily,
                "diag": diag,
            }
        )
    return packages


def _load_industry_map() -> dict[str, str]:
    meta_path = DATA_DIR / "ticker_metadata.parquet"
    if not meta_path.exists():
        return {}
    df = pd.read_parquet(meta_path, columns=["ticker", "sic_description"])
    df["ticker"] = df["ticker"].astype(str).str.upper()
    out = {}
    for row in df.itertuples():
        if isinstance(row.sic_description, str) and row.sic_description.strip():
            out[row.ticker] = row.sic_description.strip()
    return out


def _load_benchmark_bars() -> pd.DataFrame | None:
    bars_path = DATA_DIR / "aggs_daily_adjusted.parquet"
    if not bars_path.exists():
        return None
    df = pd.read_parquet(bars_path, columns=["ticker", "date", "close"])
    df["ticker"] = df["ticker"].astype(str).str.upper()
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    return df


def _benchmark_metrics(
    bench_df: pd.DataFrame | None,
    symbol: str,
    start_date: str,
    end_date: str,
    n_sessions: int,
) -> dict:
    if bench_df is None:
        return {"benchmark_total_return": None, "benchmark_cagr": None, "benchmark_mdd": None}
    sub = bench_df[
        (bench_df["ticker"] == symbol)
        & (bench_df["date"] >= _norm_date(start_date))
        & (bench_df["date"] <= _norm_date(end_date))
    ].copy()
    sub = sub.sort_values("date")
    if sub.empty:
        return {"benchmark_total_return": None, "benchmark_cagr": None, "benchmark_mdd": None}
    first = float(sub["close"].iloc[0])
    last = float(sub["close"].iloc[-1])
    tr = last / first - 1.0
    years = max(1e-9, n_sessions / 252.0)
    cagr = (last / first) ** (1.0 / years) - 1.0
    mdd = float((sub["close"] / sub["close"].cummax() - 1.0).min())
    return {
        "benchmark_total_return": round(tr, 5),
        "benchmark_cagr": round(cagr, 5),
        "benchmark_mdd": round(mdd, 5),
    }


def _sector_exposure(trades: pd.DataFrame, industry_map: dict[str, str]) -> str:
    if trades.empty:
        return ""
    tmp = trades.copy()
    tmp["ticker"] = tmp["ticker"].astype(str).str.upper()
    tmp["industry"] = tmp["ticker"].map(industry_map).fillna("UNKNOWN")
    counts = (
        tmp.groupby("industry").size().sort_values(ascending=False).head(3)
    )
    return "; ".join(f"{idx}:{int(val)}" for idx, val in counts.items())


def _winner_capture(
    *,
    baseline_trades: pd.DataFrame,
    variant_trades: pd.DataFrame,
) -> tuple[int, int, int, int]:
    if baseline_trades.empty:
        return 0, 0, 0, 0
    winners = baseline_trades[baseline_trades["return_pct"] > 0].copy()
    winners["ticker"] = winners["ticker"].astype(str).str.upper()
    variant_tickers = set(variant_trades["ticker"].astype(str).str.upper()) if not variant_trades.empty else set()
    winner_tickers = set(winners["ticker"])
    preserved = len(winner_tickers & variant_tickers)
    missed = len(winner_tickers - variant_tickers)
    top5 = set(winners.nlargest(min(5, len(winners)), "return_pct")["ticker"]) if not winners.empty else set()
    top5_captured = len(top5 & variant_tickers)
    top5_missed = len(top5 - variant_tickers)
    return preserved, missed, top5_captured, top5_missed


def build_rows(packages: list[dict]) -> pd.DataFrame:
    industry_map = _load_industry_map()
    bench_df = _load_benchmark_bars()
    rows: list[dict] = []
    baseline_by_pos = {
        int(pkg["meta"]["positions"]): pkg
        for pkg in packages
        if pkg["meta"]["variant"] == "CORE_2000"
    }

    for pkg in packages:
        meta = pkg["meta"]
        summary = pkg["summary"]
        perf = summary["performance"]
        act = summary["activity"]
        diag = pkg["diag"]
        trades = pkg["trades"]
        pos = int(meta["positions"])
        baseline_pkg = baseline_by_pos.get(pos)
        preserved = missed = top5_captured = top5_missed = 0
        if baseline_pkg is not None:
            preserved, missed, top5_captured, top5_missed = _winner_capture(
                baseline_trades=baseline_pkg["trades"],
                variant_trades=trades,
            )

        avg_winner = (
            float(trades.loc[trades["return_pct"] > 0, "return_pct"].mean())
            if not trades.empty and (trades["return_pct"] > 0).any()
            else None
        )
        avg_loser = (
            float(trades.loc[trades["return_pct"] <= 0, "return_pct"].mean())
            if not trades.empty and (trades["return_pct"] <= 0).any()
            else None
        )
        win_rate = (
            float((trades["return_pct"] > 0).mean())
            if not trades.empty
            else None
        )
        calmar = _calmar(perf.get("cagr"), perf.get("max_drawdown"))
        bench = _benchmark_metrics(
            bench_df,
            meta["benchmark_symbol"],
            summary["window"]["start"],
            summary["window"]["end"],
            int(summary["window"]["n_sessions"]),
        )
        rows.append(
            {
                "variant": meta["variant"],
                "positions": pos,
                "diagnostic_only": bool(meta.get("diagnostic_only", False)),
                "universe_type": meta["universe_type"],
                "benchmark_symbol": meta["benchmark_symbol"],
                "implemented_universe_size": meta["implemented_universe_size"],
                "base_universe_size": meta["base_universe_size"],
                "TR": perf.get("total_return"),
                "CAGR": perf.get("cagr"),
                "Sharpe": perf.get("sharpe"),
                "MDD": perf.get("max_drawdown"),
                "Calmar": round(calmar, 5) if calmar is not None else None,
                "trades": act.get("n_trades"),
                "avg_winner": round(avg_winner, 5) if avg_winner is not None else None,
                "avg_loser": round(avg_loser, 5) if avg_loser is not None else None,
                "win_rate": round(win_rate, 5) if win_rate is not None else None,
                "pct_days_in_position": act.get("pct_time_invested"),
                "valid_signal_days": int(diag["valid_signal"].fillna(False).sum()) if not diag.empty else 0,
                "avg_allowed_universe_size": round(float(diag["allowed_universe_size"].mean()), 2) if not diag.empty else None,
                "max_allowed_universe_size": int(diag["allowed_universe_size"].max()) if not diag.empty else None,
                "runtime_seconds": meta.get("runtime_seconds"),
                "contracts_processed_est": meta.get("contracts_processed_est"),
                "baseline_winners_preserved": preserved,
                "baseline_winners_missed": missed,
                "baseline_top5_winners_captured": top5_captured,
                "baseline_top5_winners_missed": top5_missed,
                "sector_exposure": _sector_exposure(trades, industry_map),
                "selection_note": meta.get("selection_note"),
                "run_name": meta.get("run_name"),
                **bench,
            }
        )

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    return df.sort_values(["positions", "variant"]).reset_index(drop=True)


def add_baseline_deltas(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    default_false_cols = [
        "pass_runtime_reduction_25pct",
        "pass_contracts_reduction_25pct",
        "pass_cagr_within_2pp",
        "pass_sharpe_within_0_05",
        "pass_mdd_within_2pp",
        "pass_baseline_winners_preserved",
        "pass_top5_contributors_preserved",
        "pass_all_thresholds",
    ]
    default_nan_cols = [
        "delta_TR_vs_core",
        "delta_CAGR_vs_core",
        "delta_Sharpe_vs_core",
        "delta_MDD_vs_core",
        "delta_runtime_seconds_vs_core",
        "delta_contracts_processed_vs_core",
    ]
    for col in default_false_cols:
        out[col] = False
    for col in default_nan_cols:
        out[col] = np.nan

    for pos, sub in out.groupby("positions"):
        baseline = sub[sub["variant"] == "CORE_2000"]
        if baseline.empty:
            continue
        base = baseline.iloc[0]
        mask = out["positions"] == pos
        out.loc[mask, "delta_TR_vs_core"] = out.loc[mask, "TR"] - base["TR"]
        out.loc[mask, "delta_CAGR_vs_core"] = out.loc[mask, "CAGR"] - base["CAGR"]
        out.loc[mask, "delta_Sharpe_vs_core"] = out.loc[mask, "Sharpe"] - base["Sharpe"]
        out.loc[mask, "delta_MDD_vs_core"] = out.loc[mask, "MDD"] - base["MDD"]
        out.loc[mask, "delta_runtime_seconds_vs_core"] = out.loc[mask, "runtime_seconds"] - base["runtime_seconds"]
        out.loc[mask, "delta_contracts_processed_vs_core"] = (
            out.loc[mask, "contracts_processed_est"] - base["contracts_processed_est"]
        )
        out.loc[mask, "pass_runtime_reduction_25pct"] = (
            out.loc[mask, "runtime_seconds"] <= (0.75 * base["runtime_seconds"])
        )
        out.loc[mask, "pass_contracts_reduction_25pct"] = (
            out.loc[mask, "contracts_processed_est"] <= (0.75 * base["contracts_processed_est"])
        )
        out.loc[mask, "pass_cagr_within_2pp"] = (
            out.loc[mask, "CAGR"] >= (base["CAGR"] - 0.02)
        )
        out.loc[mask, "pass_sharpe_within_0_05"] = (
            out.loc[mask, "Sharpe"] >= (base["Sharpe"] - 0.05)
        )
        out.loc[mask, "pass_mdd_within_2pp"] = (
            out.loc[mask, "MDD"] >= (base["MDD"] - 0.02)
        )
        out.loc[mask, "pass_baseline_winners_preserved"] = (
            out.loc[mask, "baseline_winners_missed"] <= 0
        )
        out.loc[mask, "pass_top5_contributors_preserved"] = (
            out.loc[mask, "baseline_top5_winners_missed"] <= 0
        )
        out.loc[mask, "pass_all_thresholds"] = (
            out.loc[mask, "pass_runtime_reduction_25pct"].fillna(False)
            & out.loc[mask, "pass_contracts_reduction_25pct"].fillna(False)
            & out.loc[mask, "pass_cagr_within_2pp"].fillna(False)
            & out.loc[mask, "pass_sharpe_within_0_05"].fillna(False)
            & out.loc[mask, "pass_mdd_within_2pp"].fillna(False)
            & out.loc[mask, "pass_baseline_winners_preserved"].fillna(False)
            & out.loc[mask, "pass_top5_contributors_preserved"].fillna(False)
        )
    return out


def write_report(out_dir: Path, df: pd.DataFrame) -> None:
    lines = [
        f"# Path S universe variants - {pd.Timestamp.now('UTC').date()}",
        "",
    ]
    if df.empty:
        lines.append("_No artifacts loaded._")
        (out_dir / "report.md").write_text("\n".join(lines) + "\n")
        return

    for pos, sub in df.groupby("positions"):
        lines.extend(
            [
                f"## Positions = {pos}",
                "",
                "```text",
                sub[
                    [
                        "variant",
                        "TR",
                        "CAGR",
                        "Sharpe",
                        "MDD",
                        "Calmar",
                        "trades",
                        "valid_signal_days",
                        "contracts_processed_est",
                        "baseline_winners_preserved",
                        "baseline_winners_missed",
                        "baseline_top5_winners_captured",
                        "baseline_top5_winners_missed",
                        "diagnostic_only",
                        "pass_all_thresholds",
                    ]
                ].to_string(index=False),
                "```",
                "",
            ]
        )
        contenders = sub[
            (sub["variant"] != "CORE_2000")
            & ~sub["diagnostic_only"].fillna(False)
            & sub["pass_all_thresholds"].fillna(False)
        ].copy()
        if not contenders.empty:
            best = contenders.sort_values(
                ["delta_contracts_processed_vs_core", "delta_runtime_seconds_vs_core"],
                ascending=[True, True],
                na_position="last",
            ).iloc[0]
            lines.extend(
                [
                    f"- First read: best contender for p{pos} is `{best['variant']}`.",
                    f"- Contracts vs CORE_2000: {int(best['delta_contracts_processed_vs_core']):,}",
                    f"- Runtime vs CORE_2000: {best['delta_runtime_seconds_vs_core']:+.2f}s",
                    f"- CAGR vs CORE_2000: {best['delta_CAGR_vs_core']:+.4f}",
                    f"- Sharpe vs CORE_2000: {best['delta_Sharpe_vs_core']:+.3f}",
                    f"- MDD vs CORE_2000: {best['delta_MDD_vs_core']:+.3f}",
                    "",
                ]
            )
        else:
            lines.extend(
                [
                    f"- First read: no p{pos} non-diagnostic contender cleared the PIT pass bar yet.",
                    "",
                ]
            )
    (out_dir / "report.md").write_text("\n".join(lines) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifacts", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()

    args.out.mkdir(parents=True, exist_ok=True)
    packages = _load_packages(args.artifacts)
    print(f"Loaded {len(packages)} variant packages from {args.artifacts}")
    df = build_rows(packages)
    df = add_baseline_deltas(df)
    df.to_csv(args.out / "variant_summary_all.csv", index=False)
    for pos, sub in df.groupby("positions"):
        sub.to_csv(args.out / f"variant_summary_p{pos}.csv", index=False)
    shortlist = df[
        (df["variant"] != "CORE_2000")
        & ~df["diagnostic_only"].fillna(False)
        & df["pass_all_thresholds"].fillna(False)
    ].copy()
    shortlist.to_csv(args.out / "jitter_shortlist.csv", index=False)
    write_report(args.out, df)
    print(df.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
