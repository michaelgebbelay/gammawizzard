"""S3 cache backend for chain snapshots.

Used by Lambda to persist collected chains to S3, and by the CLI
to sync S3 data to local cache for sim runs.

Bucket layout mirrors local cache:
  s3://{bucket}/{date}/open.json
  s3://{bucket}/{date}/mid.json
  s3://{bucket}/{date}/close.json
  s3://{bucket}/{date}/close5.json
  s3://{bucket}/{date}/gw_open.json   (GammaWizard data)
  ...
"""

from __future__ import annotations

import json
import logging
import os
from datetime import date
from pathlib import Path
from typing import Optional, Union

logger = logging.getLogger(__name__)

S3_BUCKET = os.environ.get("SIM_CACHE_BUCKET", "gamma-sim-cache")


def _s3_client():
    import boto3
    return boto3.client("s3")


def _s3_key(trading_date: Union[str, date], filename: str) -> str:
    d = trading_date if isinstance(trading_date, str) else trading_date.isoformat()
    return f"{d}/{filename}"


def s3_put_json(trading_date: Union[str, date], filename: str,
                data: dict, bucket: str = "") -> str:
    """Write a JSON object to S3 cache.

    Returns the S3 key written.
    """
    bucket = bucket or S3_BUCKET
    key = _s3_key(trading_date, filename)
    body = json.dumps(data)
    _s3_client().put_object(Bucket=bucket, Key=key, Body=body,
                            ContentType="application/json")
    logger.info("S3 PUT s3://%s/%s (%d bytes)", bucket, key, len(body))
    return key


def s3_get_json(trading_date: Union[str, date], filename: str,
                bucket: str = "") -> Optional[dict]:
    """Read a JSON object from S3 cache. Returns None if not found."""
    bucket = bucket or S3_BUCKET
    key = _s3_key(trading_date, filename)
    try:
        resp = _s3_client().get_object(Bucket=bucket, Key=key)
        return json.loads(resp["Body"].read())
    except _s3_client().exceptions.NoSuchKey:
        return None
    except Exception as e:
        logger.warning("S3 GET failed for %s: %s", key, e)
        return None


def s3_list_dates(bucket: str = "") -> list[str]:
    """List all trading dates in the S3 cache bucket."""
    bucket = bucket or S3_BUCKET
    s3 = _s3_client()
    paginator = s3.get_paginator("list_objects_v2")
    dates = set()
    for page in paginator.paginate(Bucket=bucket, Delimiter="/"):
        for prefix in page.get("CommonPrefixes", []):
            d = prefix["Prefix"].rstrip("/")
            # Validate it looks like a date
            if len(d) == 10 and d[4] == "-" and d[7] == "-":
                dates.add(d)
    return sorted(dates)


def sync_s3_to_local(local_cache_dir: Path, bucket: str = "",
                     dates: Optional[list[str]] = None) -> int:
    """Download all chain data from S3 to local cache directory.

    Args:
        local_cache_dir: Local sim/cache/ directory.
        bucket: S3 bucket name.
        dates: Optional list of dates to sync. If None, syncs all.

    Returns:
        Number of files downloaded.
    """
    bucket = bucket or S3_BUCKET
    s3 = _s3_client()

    if dates is None:
        dates = s3_list_dates(bucket)

    downloaded = 0
    for d in dates:
        prefix = f"{d}/"
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                filename = key.split("/", 1)[1] if "/" in key else key
                local_path = local_cache_dir / d / filename

                if local_path.exists():
                    continue

                local_path.parent.mkdir(parents=True, exist_ok=True)
                s3.download_file(bucket, key, str(local_path))
                downloaded += 1
                logger.debug("Downloaded %s -> %s", key, local_path)

        logger.info("Synced %s", d)

    logger.info("S3 sync complete: %d new files from %d dates", downloaded, len(dates))
    return downloaded
