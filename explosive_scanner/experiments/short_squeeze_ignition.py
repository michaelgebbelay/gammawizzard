#!/usr/bin/env python3
"""Run the five short-squeeze ignition variants and compare them to Path S."""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from explosive_scanner.short_squeeze import (
    BenzingaNewsCache,
    CATALYST_CONFIRMED_IGNITION,
    CORE_IGNITION,
    DEFAULT_PATH_S_DAILY_EQUITY,
    NUCLEAR_IGNITION,
    RISING_SHORT_INTEREST_IGNITION,
    SECOND_DAY_CONFIRMATION,
    VARIANT_SPECS,
    attach_short_interest,
    build_variant_candidates,
    curve_metrics,
    evaluate_variant,
    load_finra_short_interest,
    load_path_s_daily_equity,
    prepare_price_volume_panel,
    variant_summary_row,
)

OUT = ROOT / "reports"
OUT.mkdir(exist_ok=True)


def _t(msg: str, t0: float) -> float:
    now = time.perf_counter()
    print(f"[{now - t0:6.1f}s] {msg}", flush=True)
    return now


def _default(obj):
    if isinstance(obj, (pd.Timestamp, datetime)):
        return obj.isoformat()
    if hasattr(obj, "item"):
        return obj.item()
    raise TypeError(type(obj).__name__)


def _write_report(
    summary: pd.DataFrame,
    path_s_metrics: dict[str, float],
    out_path: Path,
    *,
    analysis_start: str,
    analysis_end: str,
    hold_days: int,
    max_positions: int,
) -> None:
    def _fmt(value: float, spec: str = ".2f") -> str:
        return format(value, spec) if pd.notna(value) else "nan"

    ordered = summary.sort_values(
        ["pass_sleeve_20_or_30_improves_path_s", "sleeve_20_calmar", "standalone_sharpe"],
        ascending=[False, False, False],
    )

    lines = [
        f"# Short-Squeeze Ignition — {datetime.now():%Y-%m-%d}",
        "",
        f"- Window: {analysis_start} to {analysis_end}",
        f"- Exit: fixed {hold_days}-trading-day hold",
        f"- Capacity model: max_positions={max_positions}, equal weight",
        f"- Path S baseline: total={path_s_metrics['total_return']:+.2%}, sharpe={path_s_metrics['sharpe']:.2f}, calmar={path_s_metrics['calmar']:.2f}, max_dd={path_s_metrics['max_drawdown']:+.2%}",
        "",
        "## Variant Summary",
        "",
        "| Variant | Trades | Avg Trade | Median | Payoff | Standalone Sharpe | Standalone Calmar | 20% Sleeve Sharpe | 20% Sleeve Calmar | Remove Top 5 | Pass 20/30 Sleeve |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for row in ordered.itertuples(index=False):
        lines.append(
            "| "
            f"{row.variant} | "
            f"{row.n_trades} | "
            f"{row.avg_trade_return:+.2%} | "
            f"{row.median_trade_return:+.2%} | "
            f"{_fmt(row.payoff_ratio)} | "
            f"{_fmt(row.standalone_sharpe)} | "
            f"{_fmt(row.standalone_calmar)} | "
            f"{_fmt(row.sleeve_20_sharpe)} | "
            f"{_fmt(row.sleeve_20_calmar)} | "
            f"{row.return_ex_top5_winners:+.2%} | "
            f"{'yes' if row.pass_sleeve_20_or_30_improves_path_s else 'no'} |"
        )
    out_path.write_text("\n".join(lines))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--analysis-start", default="2022-05-02")
    parser.add_argument("--analysis-end", default="2026-04-29")
    parser.add_argument("--hold-days", type=int, default=10)
    parser.add_argument("--max-positions", type=int, default=2)
    parser.add_argument("--refresh-short-interest", action="store_true")
    parser.add_argument("--skip-news", action="store_true")
    parser.add_argument("--path-s-daily-equity", default=str(DEFAULT_PATH_S_DAILY_EQUITY))
    parser.add_argument("--out-prefix", default=f"short_squeeze_ignition_{datetime.now():%Y%m%d}")
    args = parser.parse_args()

    variant_order = [
        CORE_IGNITION,
        NUCLEAR_IGNITION,
        RISING_SHORT_INTEREST_IGNITION,
        CATALYST_CONFIRMED_IGNITION,
        SECOND_DAY_CONFIRMATION,
    ]

    t0 = time.perf_counter()
    panel, bars, trading_dates = prepare_price_volume_panel(
        analysis_start=args.analysis_start,
        analysis_end=args.analysis_end,
    )
    t = _t(f"price/volume panel ready: rows={len(panel):,}  symbols={panel['symbol'].nunique():,}", t0)

    short_interest = load_finra_short_interest(
        trading_dates,
        start_date=panel["date"].min(),
        end_date=args.analysis_end,
        refresh=args.refresh_short_interest,
    )
    t = _t(f"FINRA short-interest loaded: rows={len(short_interest):,}", t)

    enriched = attach_short_interest(panel, short_interest, trading_dates)
    t = _t(f"short-interest joined: rows={len(enriched):,}", t)

    news_cache = None if args.skip_news else BenzingaNewsCache()
    if news_cache is not None and news_cache.enabled:
        print("[news] Massive Benzinga history enabled", flush=True)
    elif news_cache is not None:
        print("[news] MASSIVE_API_KEY missing — catalyst variant will be empty", flush=True)

    candidates = build_variant_candidates(
        enriched,
        trading_dates,
        analysis_start=args.analysis_start,
        analysis_end=args.analysis_end,
        news_cache=news_cache,
    )
    if news_cache is not None:
        news_cache.save()
    t = _t("variant candidate sets built", t)

    path_s = load_path_s_daily_equity(args.path_s_daily_equity)
    path_s = path_s[path_s["date"].between(pd.Timestamp(args.analysis_start), pd.Timestamp(args.analysis_end))].reset_index(drop=True)
    path_s_metrics = curve_metrics(path_s.set_index("date")["path_s_return"])

    summary_rows = []
    results: dict[str, dict[str, object]] = {}

    for name in variant_order:
        print(f"\n=== {name} ===", flush=True)
        result = evaluate_variant(
            name,
            candidates.get(name, pd.DataFrame()),
            bars,
            trading_dates,
            path_s,
            hold_days=args.hold_days,
            max_positions=args.max_positions,
            max_exit_date=path_s["date"].max(),
        )
        results[name] = result
        summary_rows.append(variant_summary_row(result, path_s_metrics))
        accepted = result["accepted"]
        baseline_ledger = result["baseline_ledger"]
        portfolio = result["portfolio"]

        base = OUT / f"{args.out_prefix}_{name}"
        if not baseline_ledger.empty:
            baseline_ledger.to_csv(base.with_name(base.name + "_ledger.csv"), index=False)
        if not accepted.empty:
            accepted.to_csv(base.with_name(base.name + "_accepted.csv"), index=False)
        if not portfolio.empty:
            portfolio.to_csv(base.with_name(base.name + "_portfolio.csv"), index=False)

        baseline = result["baseline_metrics"]
        trade_stats = result["trade_metrics"]
        print(
            "  "
            f"signals={len(baseline_ledger):>4}  trades={trade_stats['n_trades']:>4}  "
            f"avg={trade_stats['avg_return']:+.2%}  sharpe={baseline['sharpe']:.2f}  "
            f"dd={baseline['max_drawdown']:+.2%}",
            flush=True,
        )

    summary = pd.DataFrame(summary_rows)
    summary_path = OUT / f"{args.out_prefix}_summary.csv"
    summary.to_csv(summary_path, index=False)

    json_path = OUT / f"{args.out_prefix}_summary.json"
    json_payload = {
        "analysis_start": args.analysis_start,
        "analysis_end": args.analysis_end,
        "hold_days": args.hold_days,
        "max_positions": args.max_positions,
        "path_s_metrics": path_s_metrics,
        "variants": summary.to_dict(orient="records"),
    }
    json_path.write_text(json.dumps(json_payload, indent=2, default=_default))

    report_path = OUT / f"{args.out_prefix}_report.md"
    _write_report(
        summary,
        path_s_metrics,
        report_path,
        analysis_start=args.analysis_start,
        analysis_end=args.analysis_end,
        hold_days=args.hold_days,
        max_positions=args.max_positions,
    )

    t = _t(f"wrote summary → {summary_path}", t)
    print()
    print(summary.sort_values("variant").to_string(index=False))
    _t(f"complete — report {report_path.name}", t)


if __name__ == "__main__":
    main()
