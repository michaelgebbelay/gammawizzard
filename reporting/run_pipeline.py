"""End-to-end reporting pipeline.

Runs the full chain: S3 sync → ingest → broker sync → position engine →
P&L calculation → strategy summaries → daily report.

Usage:
    python -m reporting.run_pipeline                        # today
    python -m reporting.run_pipeline --date 2026-03-18      # specific date
    python -m reporting.run_pipeline --since 2026-03-01     # backfill range
    python -m reporting.run_pipeline --no-broker-sync       # skip Schwab API
    python -m reporting.run_pipeline --save-report          # write markdown to disk
"""

from __future__ import annotations

import sys
import time
from datetime import date, timedelta
from pathlib import Path


def run_pipeline(
    report_date: date | None = None,
    since_date: date | None = None,
    broker_sync: bool = True,
    save_report: bool = False,
) -> dict:
    """Run the full reporting pipeline. Returns aggregate stats."""

    from reporting.db import get_connection, init_schema

    if report_date is None:
        report_date = date.today()
    if since_date is None:
        since_date = report_date - timedelta(days=14)

    con = get_connection()
    init_schema(con)

    results = {}
    t0 = time.time()

    # ── 1. Sync events from S3 ──────────────────────────────────────────
    _step("S3 event sync")
    try:
        from reporting.ingest import sync_events_from_s3
        s3_stats = sync_events_from_s3(since_date, report_date)
        results["s3_sync"] = s3_stats
        print(f"  downloaded {s3_stats.get('files_downloaded', 0)} files")
    except Exception as e:
        print(f"  WARN: S3 sync failed ({e}), continuing with local data")
        results["s3_sync"] = {"error": str(e)}

    # ── 2. Ingest events ────────────────────────────────────────────────
    _step("Event ingestion")
    try:
        from reporting.ingest import ingest_all_pending
        ingest_stats = ingest_all_pending(con)
        results["ingest"] = ingest_stats
        print(f"  {ingest_stats.get('inserted', 0)} new events, "
              f"{ingest_stats.get('duplicates', 0)} duplicates")
    except Exception as e:
        print(f"  WARN: Ingestion failed ({e})")
        results["ingest"] = {"error": str(e)}

    # ── 3. Broker sync (optional) ───────────────────────────────────────
    if broker_sync:
        _step("Schwab broker sync")
        try:
            from reporting.broker_sync_schwab import sync_schwab
            broker_stats = sync_schwab(con, as_of_date=report_date)
            results["broker_sync"] = broker_stats
            print(f"  orders: {broker_stats.get('orders', 0)}, "
                  f"fills: {broker_stats.get('fills', 0)}, "
                  f"positions: {broker_stats.get('positions', 0)}")
        except Exception as e:
            print(f"  WARN: Broker sync failed ({e})")
            results["broker_sync"] = {"error": str(e)}
    else:
        print("[skip] Broker sync disabled")

    # ── 4. Position engine ──────────────────────────────────────────────
    _step("Position engine")
    from reporting.position_engine import materialize_positions, process_expiries

    new_positions = materialize_positions(con)
    expired = process_expiries(con, as_of_date=report_date)
    results["positions"] = {"new": new_positions, "expired": expired}
    print(f"  {new_positions} new positions, {expired} expired")

    # ── 5. P&L calculation ──────────────────────────────────────────────
    _step("P&L calculation")
    from reporting.pnl import compute_all_pnl

    pnl_stats = compute_all_pnl(con, as_of_date=report_date)
    results["pnl"] = pnl_stats
    print(f"  {pnl_stats['computed']} computed, "
          f"{pnl_stats['skipped_no_settlement']} missing settlement")

    # ── 6. Strategy summaries ───────────────────────────────────────────
    _step("Strategy summaries")
    from reporting.strategy_summary import materialize_all

    summary_stats = materialize_all(con, since_date=since_date)
    results["summaries"] = summary_stats
    print(f"  {summary_stats['strategy_daily_rows']} strategy rows, "
          f"{summary_stats['portfolio_daily_rows']} portfolio rows")

    # ── 7. Daily report ─────────────────────────────────────────────────
    _step("Daily report")
    try:
        from reporting.daily_report import generate_report
        report_md = generate_report(report_date, con)
        results["report"] = {"generated": True, "length": len(report_md)}
        print(f"  generated ({len(report_md)} chars)")

        if save_report:
            out_dir = Path(__file__).parent / "data" / "reports"
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = out_dir / f"daily_{report_date.isoformat()}.md"
            out_path.write_text(report_md)
            print(f"  saved to {out_path}")
    except Exception as e:
        print(f"  WARN: Report generation failed ({e})")
        results["report"] = {"error": str(e)}

    # ── Summary ─────────────────────────────────────────────────────────
    elapsed = time.time() - t0
    results["elapsed_seconds"] = round(elapsed, 1)
    print(f"\n{'='*50}")
    print(f"Pipeline complete in {elapsed:.1f}s")
    print(f"{'='*50}")

    return results


def _step(name: str):
    print(f"\n[{name}]")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Gamma reporting pipeline")
    parser.add_argument("--date", type=str, default=None,
                        help="Report date (YYYY-MM-DD, default: today)")
    parser.add_argument("--since", type=str, default=None,
                        help="Start date for backfill (YYYY-MM-DD)")
    parser.add_argument("--no-broker-sync", action="store_true",
                        help="Skip Schwab API sync")
    parser.add_argument("--save-report", action="store_true",
                        help="Save markdown report to disk")
    args = parser.parse_args()

    report_date = date.fromisoformat(args.date) if args.date else None
    since_date = date.fromisoformat(args.since) if args.since else None

    run_pipeline(
        report_date=report_date,
        since_date=since_date,
        broker_sync=not args.no_broker_sync,
        save_report=args.save_report,
    )


if __name__ == "__main__":
    main()
