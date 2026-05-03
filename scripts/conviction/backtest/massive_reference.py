#!/usr/bin/env python3
"""
Massive REST reference-data pull for ticker filtering.

Two-stage fetch:
    1. Bulk paginated `/v3/reference/tickers?market=stocks&active=true` —
       gets `type`, `primary_exchange`, `name` for every active US stock.
       ~20 calls (1000 results per page), takes seconds.
    2. Per-ticker detail `/v3/reference/tickers/{ticker}` — gets `sic_code`
       and `sic_description` so we can exclude pharma/biotech industries.
       Concurrent with retries; ~13K calls, takes 3-10 minutes depending
       on plan rate limits.

Output: `backtest/data/ticker_metadata.parquet` with columns:
    ticker, name, type, primary_exchange, active, sic_code, sic_description,
    is_pharma_biotech, list_date, market_cap

Run:
    bash scripts/conviction/backtest/run_massive_reference.sh         # full pull
    bash scripts/conviction/backtest/run_massive_reference.sh --probe # 100 tickers
    bash scripts/conviction/backtest/run_massive_reference.sh --bulk-only  # skip detail
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

HERE = Path(__file__).resolve().parent
DATA_DIR = HERE / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
META_PATH = DATA_DIR / "ticker_metadata.parquet"
RAW_BULK_PATH = DATA_DIR / "ticker_metadata_bulk.json"

API_BASE = os.environ.get("MASSIVE_API_BASE", "https://api.massive.com")


# ---------------------------------------------------------------------------
# SIC codes for pharma / biotech exclusion
# ---------------------------------------------------------------------------
# Reference: SEC Standard Industrial Classification index.
PHARMA_BIOTECH_SIC_CODES = {
    "2833",  # Medicinal Chemicals & Botanical Products
    "2834",  # Pharmaceutical Preparations
    "2835",  # In Vitro & In Vivo Diagnostic Substances
    "2836",  # Biological Products (no diagnostics)
    "8731",  # Commercial Physical & Biological Research
}
# Backup keyword check on sic_description in case codes vary by data source
PHARMA_BIOTECH_KEYWORDS = (
    "PHARMACEUTICAL",
    "BIOLOGICAL",
    "BIOTECH",
    "MEDICINAL",
)


def is_pharma_biotech(sic_code, sic_description) -> bool:
    # Tolerate NaN, None, ints, floats from pandas merges
    if sic_code is not None and (isinstance(sic_code, str) or not (isinstance(sic_code, float) and sic_code != sic_code)):
        if str(sic_code).strip() in PHARMA_BIOTECH_SIC_CODES:
            return True
    if isinstance(sic_description, str) and sic_description:
        upper = sic_description.upper()
        if any(kw in upper for kw in PHARMA_BIOTECH_KEYWORDS):
            return True
    return False


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def _api_key() -> str:
    try:
        return os.environ["MASSIVE_API_KEY"]
    except KeyError:
        raise SystemExit("MASSIVE_API_KEY not set in environment / .env") from None


def _session() -> requests.Session:
    s = requests.Session()
    s.params = {"apiKey": _api_key()}
    s.headers["User-Agent"] = "conviction-backtest/1.0"
    return s


# ---------------------------------------------------------------------------
# Stage 1 — bulk paginated pull
# ---------------------------------------------------------------------------

def pull_bulk_tickers(*, limit: int = 1000) -> list[dict]:
    """Walk every page of /v3/reference/tickers?market=stocks&active=true."""
    s = _session()
    url = f"{API_BASE}/v3/reference/tickers"
    params = {
        "market": "stocks",
        "active": "true",
        "limit": limit,
    }
    out: list[dict] = []
    page = 0
    next_url = url
    next_params = params
    while next_url:
        page += 1
        r = s.get(next_url, params=next_params)
        if r.status_code != 200:
            print(f"[bulk] page {page} HTTP {r.status_code}: {r.text[:200]}", file=sys.stderr)
            raise SystemExit(f"bulk pull failed at page {page}")
        data = r.json()
        results = data.get("results", []) or []
        out.extend(results)
        nxt = data.get("next_url")
        if nxt:
            # Massive's next_url already encodes the cursor; we just need the
            # apiKey appended (the SDK handles this; with raw HTTP we add it).
            next_url = nxt
            next_params = {}  # apiKey is auto-added by session.params
        else:
            next_url = None
        if page % 5 == 0:
            print(f"[bulk] page {page}: {len(out):,} tickers so far", file=sys.stderr)
    print(f"[bulk] done — {page} pages, {len(out):,} active stock tickers", file=sys.stderr)
    return out


# ---------------------------------------------------------------------------
# Stage 2 — per-ticker detail (for SIC code)
# ---------------------------------------------------------------------------

def _fetch_one_detail(session: requests.Session, ticker: str) -> dict | None:
    """Returns dict with sic_code, sic_description, list_date, market_cap;
    None on miss / rate-limit-after-retries."""
    url = f"{API_BASE}/v3/reference/tickers/{ticker}"
    for attempt in range(4):
        r = session.get(url)
        if r.status_code == 200:
            data = r.json().get("results") or {}
            return {
                "ticker": ticker,
                "sic_code": data.get("sic_code"),
                "sic_description": data.get("sic_description"),
                "list_date": data.get("list_date"),
                "market_cap": data.get("market_cap"),
                "share_class_shares_outstanding": data.get("share_class_shares_outstanding"),
            }
        if r.status_code == 404:
            return {"ticker": ticker}  # active=true but no detail → keep, no SIC
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(min(2 ** attempt, 8))
            continue
        # Other errors — log and skip
        print(f"[detail] {ticker} HTTP {r.status_code}: {r.text[:120]}", file=sys.stderr)
        return None
    return None


def _check_optionable(session: requests.Session, ticker: str) -> bool | None:
    """Returns True if there is at least one listed options contract on this
    underlying. None means lookup failed."""
    url = f"{API_BASE}/v3/reference/options/contracts"
    params = {"underlying_ticker": ticker, "limit": 1, "expired": "false"}
    for attempt in range(4):
        r = session.get(url, params=params)
        if r.status_code == 200:
            return bool(r.json().get("results"))
        if r.status_code == 404:
            return False
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(min(2 ** attempt, 8))
            continue
        return None
    return None


def pull_optionable(tickers: list[str], *, max_workers: int = 8) -> dict[str, bool]:
    """ticker -> True/False/None (None = lookup failed)."""
    s = _session()
    print(f"[options] checking optionable status for {len(tickers):,} tickers, "
          f"{max_workers} workers", file=sys.stderr)
    out: dict[str, bool] = {}
    t0 = time.time()
    done = 0
    failed = 0
    with ThreadPoolExecutor(max_workers=max_workers) as exe:
        futures = {exe.submit(_check_optionable, s, t): t for t in tickers}
        for fut in as_completed(futures):
            tkr = futures[fut]
            res = fut.result()
            done += 1
            if res is None:
                failed += 1
            else:
                out[tkr] = res
            if done % 500 == 0:
                elapsed = time.time() - t0
                rate = done / max(elapsed, 0.01)
                eta = (len(tickers) - done) / rate if rate > 0 else 0
                print(f"[options] {done}/{len(tickers)}  "
                      f"({rate:.1f}/sec, ~{eta:.0f}s left, {failed} failed)",
                      file=sys.stderr)
    n_opt = sum(1 for v in out.values() if v)
    print(f"[options] done in {time.time()-t0:.0f}s — {n_opt:,} optionable, "
          f"{failed} failed", file=sys.stderr)
    return out


def pull_details(tickers: list[str], *, max_workers: int = 8) -> list[dict]:
    s = _session()
    print(f"[detail] fetching SIC for {len(tickers):,} tickers, "
          f"{max_workers} workers", file=sys.stderr)
    out: list[dict] = []
    t0 = time.time()
    done = 0
    failed = 0
    with ThreadPoolExecutor(max_workers=max_workers) as exe:
        futures = {exe.submit(_fetch_one_detail, s, t): t for t in tickers}
        for fut in as_completed(futures):
            res = fut.result()
            done += 1
            if res is None:
                failed += 1
            else:
                out.append(res)
            if done % 500 == 0:
                elapsed = time.time() - t0
                rate = done / max(elapsed, 0.01)
                eta = (len(tickers) - done) / rate if rate > 0 else 0
                print(f"[detail] {done}/{len(tickers)}  "
                      f"({rate:.1f}/sec, ~{eta:.0f}s left, {failed} failed)",
                      file=sys.stderr)
    print(f"[detail] done in {time.time()-t0:.0f}s, {failed} failed", file=sys.stderr)
    return out


# ---------------------------------------------------------------------------
# Compose + save
# ---------------------------------------------------------------------------

def build_metadata(*, probe: bool = False, bulk_only: bool = False, max_workers: int = 8) -> Path:
    bulk = pull_bulk_tickers()
    bulk_df = pd.DataFrame(bulk)
    # Keep only the columns that vary across pages
    keep_cols = [c for c in ("ticker", "name", "type", "primary_exchange", "active",
                              "currency_name", "cik", "composite_figi") if c in bulk_df.columns]
    bulk_df = bulk_df[keep_cols].copy()

    # Also persist raw bulk for reproducibility / debugging
    RAW_BULK_PATH.write_text(json.dumps(bulk, indent=2, default=str))

    if bulk_only:
        bulk_df["sic_code"] = None
        bulk_df["sic_description"] = None
        bulk_df["is_pharma_biotech"] = False
        bulk_df.to_parquet(META_PATH, index=False)
        print(f"[meta] wrote {META_PATH} (bulk only, {len(bulk_df):,} rows)", file=sys.stderr)
        return META_PATH

    # Detail pull is per-ticker. Restrict to type=CS (common stocks) — that
    # alone kills ETFs/ETNs/funds/warrants and is the biggest cleanup.
    cs_tickers = bulk_df[bulk_df["type"] == "CS"]["ticker"].astype(str).str.upper().unique().tolist()
    if probe:
        cs_tickers = cs_tickers[:100]
        print(f"[meta] probe mode — restricting to first 100 CS tickers", file=sys.stderr)
    print(f"[meta] {len(cs_tickers):,} CS tickers; pulling SIC details...", file=sys.stderr)

    details = pull_details(cs_tickers, max_workers=max_workers)
    detail_df = pd.DataFrame(details)

    # Merge back with bulk
    bulk_df["ticker"] = bulk_df["ticker"].astype(str).str.upper()
    if not detail_df.empty:
        detail_df["ticker"] = detail_df["ticker"].astype(str).str.upper()
        merged = bulk_df.merge(detail_df, on="ticker", how="left")
    else:
        merged = bulk_df.copy()
        for c in ("sic_code", "sic_description", "list_date", "market_cap",
                  "share_class_shares_outstanding"):
            merged[c] = None

    merged["is_pharma_biotech"] = merged.apply(
        lambda r: is_pharma_biotech(r.get("sic_code"), r.get("sic_description")),
        axis=1,
    )

    # Optionable check — do this for CS tickers that aren't already excluded
    # as pharma/biotech (saves API calls).
    candidate_tickers = merged[
        (merged["type"] == "CS") & (~merged["is_pharma_biotech"].fillna(False))
    ]["ticker"].astype(str).str.upper().unique().tolist()
    if probe:
        candidate_tickers = candidate_tickers[:100]

    optionable_map = pull_optionable(candidate_tickers, max_workers=max_workers)
    merged["is_optionable"] = merged["ticker"].map(optionable_map)

    merged.to_parquet(META_PATH, index=False)
    n_pb = int(merged["is_pharma_biotech"].fillna(False).sum())
    n_cs = int((merged["type"] == "CS").sum()) if "type" in merged.columns else len(merged)
    n_opt = int(merged["is_optionable"].fillna(False).sum())
    print(f"[meta] wrote {META_PATH}  rows={len(merged):,}  CS={n_cs:,}  "
          f"pharma/biotech={n_pb:,}  optionable={n_opt:,}", file=sys.stderr)
    return META_PATH


# ---------------------------------------------------------------------------
# Loader for downstream consumers
# ---------------------------------------------------------------------------

def load_metadata(path: Path | None = None) -> pd.DataFrame:
    p = path or META_PATH
    if not p.exists():
        raise SystemExit(f"{p} not found. Run massive_reference.py first.")
    return pd.read_parquet(p)


def allowed_ticker_set(
    *,
    require_type: str | None = "CS",
    exclude_pharma_biotech: bool = True,
    require_optionable: bool = True,
    primary_exchanges: tuple[str, ...] | None = ("XNYS", "XNAS", "ARCX", "BATS"),
    require_active: bool = False,  # the bars include delisted names too
) -> set[str]:
    df = load_metadata()
    mask = pd.Series(True, index=df.index)
    if require_type:
        mask &= df["type"] == require_type
    if exclude_pharma_biotech:
        mask &= ~df["is_pharma_biotech"].fillna(False)
    if require_optionable and "is_optionable" in df.columns:
        mask &= df["is_optionable"].fillna(False)
    if primary_exchanges:
        mask &= df["primary_exchange"].isin(primary_exchanges)
    if require_active and "active" in df.columns:
        mask &= df["active"].fillna(False)
    return set(df.loc[mask, "ticker"].astype(str).str.upper())


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--probe", action="store_true",
                    help="only fetch detail for first 100 CS tickers (smoke test)")
    ap.add_argument("--bulk-only", action="store_true",
                    help="skip detail pull (no SIC, no pharma filter possible)")
    ap.add_argument("--max-workers", type=int, default=8)
    args = ap.parse_args()
    build_metadata(probe=args.probe, bulk_only=args.bulk_only, max_workers=args.max_workers)
    return 0


if __name__ == "__main__":
    raise SystemExit(main() or 0)
