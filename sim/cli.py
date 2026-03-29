"""CLI entry point for the SPX 1DTE trading simulation (v14).

Usage:
    python -m sim.cli run-all --max-sessions 200
    python -m sim.cli leaderboard
    python -m sim.cli report --session 5
    python -m sim.cli collect-chain --phase close5
    python -m sim.cli export-sheets
    python -m sim.cli status
    python -m sim.cli sync-cache
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

from sim.config import DB_PATH


def setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )


def _load_dotenv() -> None:
    """Load .env file from project root if it exists."""
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if env_path.exists():
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, _, value = line.partition("=")
                    os.environ.setdefault(key.strip(), value.strip())


def cmd_run_all(args) -> None:
    """Run all sessions."""
    from sim.orchestrator.scheduler import Scheduler

    _load_dotenv()

    # Create API clients if keys are available
    anthropic_client = None
    openai_client = None

    if os.environ.get("ANTHROPIC_API_KEY"):
        import anthropic
        anthropic_client = anthropic.Anthropic()

    if os.environ.get("OPENAI_API_KEY"):
        import openai
        openai_client = openai.OpenAI()

    db_path = Path(args.db) if args.db else DB_PATH

    scheduler = Scheduler(
        anthropic_client=anthropic_client,
        openai_client=openai_client,
        db_path=db_path,
        max_sessions=args.max_sessions,
    )

    dates = None
    if args.dates:
        dates = args.dates.split(",")

    result = scheduler.run(trading_dates=dates)
    print(f"\nCompleted {result.get('sessions_completed', 0)} sessions "
          f"({result.get('total_completed', 0)} total)")


def cmd_leaderboard(args) -> None:
    """Display leaderboard."""
    from sim.persistence.db import get_connection
    from sim.reporting.leaderboard import hard_metrics_leaderboard

    db_path = Path(args.db) if args.db else DB_PATH
    conn = get_connection(db_path)
    print(hard_metrics_leaderboard(conn))
    conn.close()


def cmd_report(args) -> None:
    """Generate a session report."""
    from sim.persistence.db import get_connection
    from sim.reporting.session_report import generate_session_report

    db_path = Path(args.db) if args.db else DB_PATH
    conn = get_connection(db_path)
    report = generate_session_report(conn, args.session)
    print(report)
    conn.close()


def cmd_collect_chain(args) -> None:
    """Collect chain data (wrapper around collect_chain.py)."""
    from sim.data.collect_chain import main as collect_main
    sys.argv = ["collect_chain", args.phase]
    collect_main()


def cmd_sync_cache(args) -> None:
    """Sync chain data from S3 to local cache."""
    from sim.config import CACHE_DIR
    from sim.data.s3_cache import sync_s3_to_local, s3_list_dates, S3_BUCKET

    bucket = args.bucket or S3_BUCKET
    dates = None
    if args.dates:
        dates = args.dates.split(",")

    print(f"Syncing from s3://{bucket}/ to {CACHE_DIR}/")

    if dates is None:
        all_dates = s3_list_dates(bucket)
        print(f"Found {len(all_dates)} dates in S3")
    else:
        all_dates = dates
        print(f"Syncing {len(all_dates)} specified dates")

    downloaded = sync_s3_to_local(CACHE_DIR, bucket=bucket, dates=all_dates)
    print(f"Downloaded {downloaded} new files")


def cmd_export_sheets(args) -> None:
    """Export simulation results to Google Sheets."""
    from sim.persistence.db import init_db
    from sim.reporting.export_sheets import export_all, DEFAULT_SHEET_ID

    db_path = Path(args.db) if args.db else DB_PATH
    conn = init_db(db_path)
    sheet_id = args.sheet_id or os.environ.get("SIM_GSHEET_ID", DEFAULT_SHEET_ID)

    print(f"Exporting to Google Sheets ...")
    written = export_all(conn, spreadsheet_id=sheet_id)
    print(f"Done — {written} tabs written to sheet {sheet_id}")
    conn.close()


def cmd_regime_labels(args) -> None:
    """Build offline SPX/VIX regime labels from cached history."""
    from pathlib import Path
    from sim.regime_switching import DEFAULT_OUT_CSV, DEFAULT_OUT_JSON, export_regime_labels

    artifacts = export_regime_labels(
        out_csv=Path(args.out_csv) if args.out_csv else DEFAULT_OUT_CSV,
        out_json=Path(args.out_json) if args.out_json else DEFAULT_OUT_JSON,
        min_date=args.min_date,
        max_date=args.max_date,
        n_states=args.states,
    )

    counts = artifacts.summary.get("counts", {})
    print(f"Wrote labels to {args.out_csv or DEFAULT_OUT_CSV}")
    print(f"Wrote summary to {args.out_json or DEFAULT_OUT_JSON}")
    print(
        "Regime counts: "
        + ", ".join(f"{name}={count}" for name, count in counts.items())
    )


def cmd_volatility_study(args) -> None:
    """Run standalone volatility forecasting analytics."""
    from pathlib import Path
    from sim.volatility_analytics import DEFAULT_OUT_DIR, export_volatility_study

    horizons = tuple(int(part.strip()) for part in args.horizons.split(",") if part.strip())
    cache_dir = Path(args.cache_dir) if args.cache_dir else None
    out_dir = Path(args.out_dir) if args.out_dir else DEFAULT_OUT_DIR

    kwargs = {
        "out_dir": out_dir,
        "csv_path": Path(args.csv) if args.csv else None,
        "min_date": args.min_date,
        "max_date": args.max_date,
        "horizons": horizons,
        "n_states": args.states,
        "covid_start": args.covid_start,
        "post_covid_start": args.post_covid_start,
        "train_ratio": args.train_ratio,
        "min_train_rows": args.min_train_rows,
    }
    if cache_dir is not None:
        kwargs["cache_dir"] = cache_dir

    artifacts = export_volatility_study(**kwargs)

    print(f"Wrote labels to {out_dir / 'labels.csv'}")
    print(f"Wrote forecasts to {out_dir / 'forecasts.csv'}")
    print(f"Wrote metrics to {out_dir / 'metrics.csv'}")
    print(f"Wrote summary to {out_dir / 'summary.json'}")

    if not artifacts.metrics.empty:
        best = (
            artifacts.metrics.sort_values(["period", "horizon_days", "mse", "mape", "model"])
            .groupby(["period", "horizon_days"], as_index=False)
            .first()
        )
        for _, row in best.iterrows():
            print(
                f"{row['period']} horizon={int(row['horizon_days'])}d "
                f"best={row['model']} mse={row['mse']:.4f} mape={row['mape']:.2f}%"
            )


def cmd_vol_vov_states(args) -> None:
    """Build standalone VOL/VOV state labels and summaries."""
    from pathlib import Path
    from sim.vol_vov_state_system import (
        DEFAULT_OUT_CSV,
        DEFAULT_OUT_JSON,
        export_vol_vov_state_system,
    )

    artifacts = export_vol_vov_state_system(
        out_csv=Path(args.out_csv) if args.out_csv else DEFAULT_OUT_CSV,
        out_json=Path(args.out_json) if args.out_json else DEFAULT_OUT_JSON,
        min_date=args.min_date,
        max_date=args.max_date,
        vov_window=args.vov_window,
        threshold_window=args.threshold_window,
        min_history=args.min_history,
        max_gap_days=args.max_gap_days,
    )

    paper_alignment = artifacts.summary.get("paper_alignment", {})
    state_counts = artifacts.summary.get("state_counts", {})
    print(f"Wrote daily states to {args.out_csv or DEFAULT_OUT_CSV}")
    print(f"Wrote summary to {args.out_json or DEFAULT_OUT_JSON}")
    print(
        "State counts: "
        + ", ".join(f"{name}={count}" for name, count in state_counts.items())
    )
    print(
        "Most negative corr state: "
        f"{paper_alignment.get('observed_most_negative_corr_state')}"
    )
    print(
        "Least negative corr state: "
        f"{paper_alignment.get('observed_least_negative_corr_state')}"
    )


def cmd_status(args) -> None:
    """Show simulation status."""
    from sim.persistence.db import get_connection
    from sim.persistence.queries import count_sessions

    db_path = Path(args.db) if args.db else DB_PATH

    if not db_path.exists():
        print("No database found. Run 'run-all' to start.")
        return

    conn = get_connection(db_path)
    n = count_sessions(conn)
    print(f"  Sessions completed: {n}")

    # Show agent count
    cur = conn.execute("SELECT COUNT(DISTINCT agent_id) FROM accounts")
    agents = cur.fetchone()[0]
    print(f"  Participants: {agents}")

    conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="SPX 1DTE Trading Simulation (v14)",
        prog="sim",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("--db", help="Custom database path")
    sub = parser.add_subparsers(dest="command")

    # run-all
    p_run = sub.add_parser("run-all", help="Run all sessions")
    p_run.add_argument("--max-sessions", type=int, default=200)
    p_run.add_argument("--dates", help="Comma-separated trading dates")

    # leaderboard
    sub.add_parser("leaderboard", help="Show leaderboard")

    # report
    p_rpt = sub.add_parser("report", help="Generate session report")
    p_rpt.add_argument("--session", type=int, required=True)

    # collect-chain
    p_cc = sub.add_parser("collect-chain", help="Collect chain data")
    p_cc.add_argument("--phase", required=True, choices=["open", "mid", "close", "close5"])

    # sync-cache
    p_sync = sub.add_parser("sync-cache", help="Sync chain data from S3 to local cache")
    p_sync.add_argument("--bucket", help="S3 bucket name (default: gamma-sim-cache)")
    p_sync.add_argument("--dates", help="Comma-separated dates to sync (default: all)")

    # export-sheets
    p_exp = sub.add_parser("export-sheets", help="Export results to Google Sheets")
    p_exp.add_argument("--sheet-id", dest="sheet_id",
                       help="Google Sheet ID (default: SIM_GSHEET_ID env or built-in)")

    # regime-labels
    p_regime = sub.add_parser("regime-labels", help="Build offline SPX/VIX regime labels")
    p_regime.add_argument("--min-date", help="Earliest cache date to include (YYYY-MM-DD)")
    p_regime.add_argument("--max-date", help="Latest cache date to include (YYYY-MM-DD)")
    p_regime.add_argument("--states", type=int, default=3, help="Number of latent states")
    p_regime.add_argument("--out-csv", help="Output CSV path")
    p_regime.add_argument("--out-json", help="Output summary JSON path")

    # volatility-study
    p_vol = sub.add_parser("volatility-study", help="Run standalone volatility forecasting analytics")
    p_vol.add_argument("--csv", help="Optional external CSV input instead of sim/cache")
    p_vol.add_argument("--cache-dir", help="Optional cache directory override")
    p_vol.add_argument("--min-date", help="Earliest input date to include (YYYY-MM-DD)")
    p_vol.add_argument("--max-date", help="Latest input date to include (YYYY-MM-DD)")
    p_vol.add_argument("--horizons", default="5,10", help="Comma-separated forecast horizons in trading days")
    p_vol.add_argument("--states", type=int, default=3, help="Number of latent regimes")
    p_vol.add_argument("--covid-start", default="2020-02-24", help="COVID window start date (YYYY-MM-DD)")
    p_vol.add_argument("--post-covid-start", default="2023-01-03", help="Post-COVID window start date (YYYY-MM-DD)")
    p_vol.add_argument("--train-ratio", type=float, default=0.7, help="Fraction of each period used for training")
    p_vol.add_argument("--min-train-rows", type=int, default=126, help="Minimum train rows per period/horizon")
    p_vol.add_argument("--out-dir", help="Output directory for labels, forecasts, metrics, and summary")

    # vol-vov-states
    p_vol_vov = sub.add_parser(
        "vol-vov-states",
        help="Build standalone VOL/VOV state labels",
    )
    p_vol_vov.add_argument("--min-date", help="Earliest cache date to include (YYYY-MM-DD)")
    p_vol_vov.add_argument("--max-date", help="Latest cache date to include (YYYY-MM-DD)")
    p_vol_vov.add_argument(
        "--vov-window",
        type=int,
        default=10,
        help="Trailing window for realized VIX vol proxy",
    )
    p_vol_vov.add_argument(
        "--threshold-window",
        type=int,
        default=252,
        help="Trailing window for high/low threshold medians",
    )
    p_vol_vov.add_argument(
        "--min-history",
        type=int,
        default=60,
        help="Minimum history required before assigning a state",
    )
    p_vol_vov.add_argument(
        "--max-gap-days",
        type=int,
        default=5,
        help="Ignore return jumps across larger calendar gaps",
    )
    p_vol_vov.add_argument("--out-csv", help="Output CSV path")
    p_vol_vov.add_argument("--out-json", help="Output summary JSON path")

    # status
    sub.add_parser("status", help="Show simulation status")

    args = parser.parse_args()
    setup_logging(args.verbose)

    commands = {
        "run-all": cmd_run_all,
        "leaderboard": cmd_leaderboard,
        "report": cmd_report,
        "collect-chain": cmd_collect_chain,
        "sync-cache": cmd_sync_cache,
        "export-sheets": cmd_export_sheets,
        "regime-labels": cmd_regime_labels,
        "volatility-study": cmd_volatility_study,
        "vol-vov-states": cmd_vol_vov_states,
        "status": cmd_status,
    }

    if args.command in commands:
        commands[args.command](args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
