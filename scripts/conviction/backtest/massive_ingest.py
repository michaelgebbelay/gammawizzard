#!/usr/bin/env python3
"""
Massive flat-files ingest for daily US-equity aggregates.

Pulls from `s3://flatfiles/us_stocks_sip/day_aggs_v1/YYYY/MM/YYYY-MM-DD.csv.gz`
into a local parquet, ready for the gainer-discovery backtest.

Why flat files instead of REST:
    REST: ~3000 tickers × 5y × 1 API call/ticker = thousands of calls, slow,
          rate-limited.
    Flat: ~1260 daily files × ~1MB each = 1.3GB download, ~10-15 min over a
          ThreadPoolExecutor, all tickers per file (including delisted names
          that still traded that day — survivorship bias auto-solved).

Setup (one time):
    1. In your Massive dashboard, generate S3 access credentials.
    2. Add to .env:
           MASSIVE_S3_KEY=<access-key>
           MASSIVE_S3_SECRET=<secret-key>
           # Optional, defaults to https://files.massive.com:
           # MASSIVE_S3_ENDPOINT=https://files.massive.com

Run:
    bash scripts/conviction/backtest/run_massive_ingest.sh --probe       # 30 days
    bash scripts/conviction/backtest/run_massive_ingest.sh --days 1825   # 5y
    bash scripts/conviction/backtest/run_massive_ingest.sh --merge-only  # skip download
"""
from __future__ import annotations

import argparse
import gzip
import io
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path

import boto3
import botocore
import botocore.exceptions
import pandas as pd
from botocore.config import Config


HERE = Path(__file__).resolve().parent
DATA_DIR = HERE / "data"
RAW_DIR = DATA_DIR / "day_aggs_raw"
PARQUET_PATH = DATA_DIR / "aggs_daily.parquet"
SPLITS_PATH = DATA_DIR / "splits.parquet"
ADJUSTED_PARQUET_PATH = DATA_DIR / "aggs_daily_adjusted.parquet"


# ---------------------------------------------------------------------------
# S3 client
# ---------------------------------------------------------------------------

def _s3_client():
    try:
        access_key = os.environ["MASSIVE_S3_KEY"]
        secret_key = os.environ["MASSIVE_S3_SECRET"]
    except KeyError as e:
        raise SystemExit(
            f"missing env var {e.args[0]}. Add to .env:\n"
            "  MASSIVE_S3_KEY=<access-key>\n"
            "  MASSIVE_S3_SECRET=<secret-key>"
        ) from None
    endpoint = os.environ.get("MASSIVE_S3_ENDPOINT", "https://files.massive.com")
    return boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url=endpoint,
        config=Config(signature_version="s3v4", retries={"max_attempts": 5, "mode": "standard"}),
    )


# ---------------------------------------------------------------------------
# Date utilities
# ---------------------------------------------------------------------------

def _is_likely_trading_day(d: date) -> bool:
    """Cheap weekday filter. Holidays still get skipped server-side (404)."""
    return d.weekday() < 5


def _enumerate_days(start: date, end: date) -> list[date]:
    out = []
    cur = start
    while cur <= end:
        if _is_likely_trading_day(cur):
            out.append(cur)
        cur += timedelta(days=1)
    return out


def _key_for_day(d: date) -> str:
    return f"us_stocks_sip/day_aggs_v1/{d.year}/{d.month:02d}/{d.isoformat()}.csv.gz"


def _local_path_for_day(d: date) -> Path:
    return RAW_DIR / f"{d.isoformat()}.csv.gz"


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------

def _download_one(s3, d: date) -> tuple[date, str]:
    """Returns (date, status). Status ∈ {downloaded, cached, missing, error}."""
    local = _local_path_for_day(d)
    if local.exists() and local.stat().st_size > 0:
        return d, "cached"
    key = _key_for_day(d)
    try:
        s3.download_file("flatfiles", key, str(local))
        return d, "downloaded"
    except botocore.exceptions.ClientError as e:
        # Holidays / weekends / pre-data dates return 404
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey"):
            # Don't leave an empty file behind
            if local.exists():
                local.unlink(missing_ok=True)
            return d, "missing"
        return d, f"error:{code}"
    except Exception as e:  # noqa: BLE001
        return d, f"error:{type(e).__name__}"


def download_window(start: date, end: date, *, max_workers: int = 10) -> dict:
    s3 = _s3_client()
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    days = _enumerate_days(start, end)
    print(f"[ingest] {start} → {end}: {len(days)} candidate days, "
          f"{max_workers} workers", file=sys.stderr)

    counts = {"downloaded": 0, "cached": 0, "missing": 0, "error": 0}
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=max_workers) as exe:
        futures = {exe.submit(_download_one, s3, d): d for d in days}
        done = 0
        for fut in as_completed(futures):
            d, status = fut.result()
            done += 1
            if status.startswith("error"):
                counts["error"] += 1
                print(f"[ingest] {d} ERROR: {status}", file=sys.stderr)
            else:
                counts[status] += 1
            if done % 50 == 0:
                elapsed = time.time() - t0
                rate = done / elapsed
                eta = (len(days) - done) / rate if rate > 0 else 0
                print(f"[ingest] {done}/{len(days)}  "
                      f"({elapsed:.0f}s elapsed, ~{eta:.0f}s remaining)",
                      file=sys.stderr)
    elapsed = time.time() - t0
    print(f"[ingest] done in {elapsed:.0f}s — "
          f"{counts['downloaded']} downloaded, {counts['cached']} cached, "
          f"{counts['missing']} missing (holidays), {counts['error']} errored",
          file=sys.stderr)
    return counts


# ---------------------------------------------------------------------------
# Merge to parquet
# ---------------------------------------------------------------------------

EXPECTED_COLUMNS = {"ticker", "volume", "open", "close", "high", "low", "window_start"}


def merge_to_parquet(*, drop_after_year: int | None = None) -> Path:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    files = sorted(RAW_DIR.glob("*.csv.gz"))
    if not files:
        raise SystemExit(f"no files to merge in {RAW_DIR}. Run download first.")
    print(f"[merge] reading {len(files)} files...", file=sys.stderr)

    frames = []
    for i, path in enumerate(files):
        try:
            df = pd.read_csv(path, compression="gzip")
        except Exception as e:  # noqa: BLE001
            print(f"[merge] skipping {path.name}: {type(e).__name__}: {e}", file=sys.stderr)
            continue
        if not EXPECTED_COLUMNS.issubset(df.columns):
            print(f"[merge] {path.name}: missing columns {EXPECTED_COLUMNS - set(df.columns)}",
                  file=sys.stderr)
            continue
        frames.append(df)
        if (i + 1) % 250 == 0:
            print(f"[merge] {i+1}/{len(files)}", file=sys.stderr)
    if not frames:
        raise SystemExit("no parseable files")

    big = pd.concat(frames, ignore_index=True)
    print(f"[merge] {len(big):,} raw rows, normalizing...", file=sys.stderr)

    # window_start is a nanosecond Unix epoch at the bar's start (typically
    # midnight ET for daily aggs). Convert to a tz-naive normalized date in
    # America/New_York.
    big["date"] = (
        pd.to_datetime(big["window_start"], unit="ns", utc=True)
          .dt.tz_convert("America/New_York")
          .dt.normalize()
          .dt.tz_localize(None)
    )
    if drop_after_year:
        big = big[big["date"].dt.year >= drop_after_year]

    big["ticker"] = big["ticker"].astype(str).str.upper()
    cols = ["ticker", "date", "open", "high", "low", "close", "volume"]
    if "transactions" in big.columns:
        cols.append("transactions")
    big = big[cols]
    big = big.dropna(subset=["ticker", "date", "close", "volume"])
    big = big.sort_values(["ticker", "date"]).drop_duplicates(["ticker", "date"]).reset_index(drop=True)

    print(f"[merge] {len(big):,} rows, "
          f"{big['ticker'].nunique():,} unique tickers, "
          f"{big['date'].min().date()} → {big['date'].max().date()}",
          file=sys.stderr)

    big.to_parquet(PARQUET_PATH, index=False)
    size_mb = PARQUET_PATH.stat().st_size / 1e6
    print(f"[merge] wrote {PARQUET_PATH}  ({size_mb:.1f} MB)", file=sys.stderr)
    return PARQUET_PATH


# ---------------------------------------------------------------------------
# Splits — pull + apply backward adjustment
# ---------------------------------------------------------------------------
# Massive's flat-file day_aggs are UNADJUSTED. NVDA's 10:1 split on
# 2024-06-07 shows up as a 90% one-day drop in the raw data, which makes any
# backtest of a name through its split window produce phantom losses. We
# pull /v3/reference/splits and apply a standard backward-adjustment:
# pre-split prices are multiplied by (split_from / split_to) so the entire
# series is comparable to post-split shares.

def _api_key() -> str:
    try:
        return os.environ["MASSIVE_API_KEY"]
    except KeyError:
        raise SystemExit("MASSIVE_API_KEY not set in environment / .env") from None


def fetch_all_splits(*, start_date: str = "2020-01-01") -> "pd.DataFrame":
    """Walk paginated /v3/reference/splits since `start_date`."""
    import requests
    base = os.environ.get("MASSIVE_API_BASE", "https://api.massive.com")
    s = requests.Session()
    s.params = {"apiKey": _api_key()}
    url = f"{base}/v3/reference/splits"
    params = {"execution_date.gte": start_date, "limit": 1000, "order": "asc"}
    rows: list[dict] = []
    page = 0
    next_url = url
    next_params = params
    while next_url:
        page += 1
        r = s.get(next_url, params=next_params)
        r.raise_for_status()
        data = r.json()
        rows.extend(data.get("results") or [])
        nxt = data.get("next_url")
        next_url = nxt if nxt else None
        next_params = {}
        if page % 5 == 0:
            print(f"[splits] page {page}: {len(rows)} splits so far", file=sys.stderr)
    print(f"[splits] done — {len(rows)} splits since {start_date}", file=sys.stderr)
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["execution_date"] = pd.to_datetime(df["execution_date"]).dt.normalize()
    df["ticker"] = df["ticker"].astype(str).str.upper()
    df["split_from"] = df["split_from"].astype(float)
    df["split_to"] = df["split_to"].astype(float)
    df = df[(df["split_from"] > 0) & (df["split_to"] > 0)].copy()
    df["adjustment_factor"] = df["split_from"] / df["split_to"]
    return df[["ticker", "execution_date", "split_from", "split_to", "adjustment_factor"]]


def apply_splits_to_parquet(
    *,
    aggs_path: Path | None = None,
    splits_path: Path | None = None,
    out_path: Path | None = None,
) -> Path:
    """Read raw aggregates parquet, apply backward split adjustment, write
    adjusted parquet. For each split (ticker T, exec_date D, factor f):
        - Bars with ticker==T and date < D get prices *= f, volume /= f
    Multiple splits compound naturally because we apply oldest-first.
    """
    aggs_path = aggs_path or PARQUET_PATH
    splits_path = splits_path or SPLITS_PATH
    out_path = out_path or ADJUSTED_PARQUET_PATH
    if not aggs_path.exists():
        raise SystemExit(f"raw aggs parquet missing: {aggs_path}")
    if not splits_path.exists():
        raise SystemExit(f"splits parquet missing: {splits_path}. Run --fetch-splits.")

    print(f"[adjust] reading {aggs_path.name}...", file=sys.stderr)
    df = pd.read_parquet(aggs_path)
    splits = pd.read_parquet(splits_path)
    print(f"[adjust] {len(df):,} bar rows, {len(splits):,} splits to apply",
          file=sys.stderr)

    # Group splits by ticker for efficiency
    n_adjusted = 0
    for tkr, group in splits.groupby("ticker"):
        if tkr not in df["ticker"].values:
            continue
        group = group.sort_values("execution_date")
        ticker_mask = df["ticker"] == tkr
        for _, split in group.iterrows():
            exec_date = split["execution_date"]
            factor = float(split["adjustment_factor"])
            pre_split_mask = ticker_mask & (df["date"] < exec_date)
            if not pre_split_mask.any():
                continue
            df.loc[pre_split_mask, ["open", "high", "low", "close"]] *= factor
            df.loc[pre_split_mask, "volume"] /= factor
            n_adjusted += int(pre_split_mask.sum())

    print(f"[adjust] adjusted {n_adjusted:,} bar rows for splits", file=sys.stderr)
    df.to_parquet(out_path, index=False)
    size_mb = out_path.stat().st_size / 1e6
    print(f"[adjust] wrote {out_path}  ({size_mb:.1f} MB)", file=sys.stderr)
    return out_path


def fetch_and_apply_splits() -> Path:
    """End-to-end: fetch splits via REST, save, apply to parquet."""
    splits = fetch_all_splits()
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    splits.to_parquet(SPLITS_PATH, index=False)
    print(f"[splits] wrote {SPLITS_PATH}", file=sys.stderr)
    return apply_splits_to_parquet()


# ---------------------------------------------------------------------------
# Loaders for downstream consumers (gainer_discovery.py / replay.py)
# ---------------------------------------------------------------------------

def load_parquet(parquet_path: Path | None = None) -> pd.DataFrame:
    """Default: load the split-adjusted parquet if it exists (preferred for
    backtests). Fall back to the raw parquet only if explicitly requested
    or if adjusted version hasn't been built. Caller can override via path."""
    if parquet_path is not None:
        p = parquet_path
    elif ADJUSTED_PARQUET_PATH.exists():
        p = ADJUSTED_PARQUET_PATH
        print(f"[load_parquet] using SPLIT-ADJUSTED data: {p.name}", file=sys.stderr)
    else:
        p = PARQUET_PATH
        print(f"[load_parquet] WARNING: using UNADJUSTED data ({p.name}). "
              f"Run massive_ingest.py --fetch-splits to fix.", file=sys.stderr)
    if not p.exists():
        raise SystemExit(f"{p} not found. Run massive_ingest.py first.")
    return pd.read_parquet(p)


def to_bars_by_ticker(df: pd.DataFrame, *, tickers: list[str] | None = None) -> dict:
    """Convert merged parquet into the {ticker: bars_df} dict used by the
    rest of the system. Compatible with `data.fetch_daily_bars` output shape."""
    if tickers is not None:
        df = df[df["ticker"].isin([t.upper() for t in tickers])]
    out = {}
    for tkr, g in df.groupby("ticker"):
        out[str(tkr)] = g[["date", "open", "high", "low", "close", "volume"]].copy().reset_index(drop=True)
    return out


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--start", type=str, default=None, help="YYYY-MM-DD")
    ap.add_argument("--end", type=str, default=None, help="YYYY-MM-DD")
    ap.add_argument("--days", type=int, default=None, help="lookback in days")
    ap.add_argument("--probe", action="store_true",
                    help="30-day probe (fast smoke test before full pull)")
    ap.add_argument("--full", action="store_true",
                    help="5-year pull (1825 days)")
    ap.add_argument("--merge-only", action="store_true",
                    help="skip download, only merge existing files into parquet")
    ap.add_argument("--max-workers", type=int, default=10)
    ap.add_argument("--no-merge", action="store_true",
                    help="download only, don't build parquet")
    ap.add_argument("--fetch-splits", action="store_true",
                    help="pull splits via REST and build the SPLIT-ADJUSTED parquet "
                         "(aggs_daily_adjusted.parquet). Runs after merge if combined.")
    ap.add_argument("--apply-splits-only", action="store_true",
                    help="re-apply splits to the existing raw parquet without re-pulling")
    args = ap.parse_args()

    if args.apply_splits_only:
        apply_splits_to_parquet()
        return 0
    if args.fetch_splits and args.merge_only:
        merge_to_parquet()
        fetch_and_apply_splits()
        return 0
    if args.fetch_splits and not (args.start or args.end or args.days or args.probe or args.full):
        fetch_and_apply_splits()
        return 0
    if args.merge_only:
        merge_to_parquet()
        return 0

    end = pd.Timestamp(args.end).date() if args.end else (datetime.now() - timedelta(days=1)).date()
    if args.probe:
        start = end - timedelta(days=30)
    elif args.full:
        start = end - timedelta(days=5 * 365)
    elif args.days is not None:
        start = end - timedelta(days=args.days)
    elif args.start:
        start = pd.Timestamp(args.start).date()
    else:
        # Default: 1 year (matches the existing discovery test window)
        start = end - timedelta(days=365)

    counts = download_window(start, end, max_workers=args.max_workers)
    if counts["error"] > 0:
        print(f"[ingest] WARNING: {counts['error']} errored downloads. "
              f"Re-run to retry.", file=sys.stderr)
    if not args.no_merge and (counts["downloaded"] > 0 or counts["cached"] > 0):
        merge_to_parquet()
    if args.fetch_splits:
        fetch_and_apply_splits()
    return 0


if __name__ == "__main__":
    raise SystemExit(main() or 0)
