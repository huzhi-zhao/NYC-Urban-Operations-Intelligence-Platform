#!/usr/bin/env python3
"""
Backfill Script — NYC 311 Service Requests (SRC-NYC-001)

One-time script to backfill the last 3 months of 311 data to GCS Bronze layer.

Storage layout (flat per month, no month= subdirectory):
  bronze/raw/SRC-NYC-001/nyc_311/data_YYYY-MM.json
  bronze/raw/SRC-NYC-001/nyc_311/manifest_YYYY-MM.json

Re-run is idempotent: overwrites both data and manifest files.
manifest.fetch_timestamp reflects the time of the latest upload.

Usage:
    export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
    export GCS_BUCKET_NAME=your-bucket-name
    export SOCRATA_APP_TOKEN=your_token   # optional
    python scripts/backfill/backfill_nyc_311.py

Exit codes:
    0  — success
    1  — configuration error
    2  — fetch error
    3  — GCS write error
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import date, datetime
from typing import Any

# Absolute import — project root is added to path below
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from ingestion.clients.socrata_client import SocrataClient
from ingestion.loaders.gcs_loader import GCSBronzeLoader

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("backfill_nyc_311")


# ── Constants ─────────────────────────────────────────────────────────────────

RESOURCE_ID = "erm2-nwe9"
DOMAIN = "data.cityofnewyork.us"
SOURCE_ID = "SRC-NYC-001"
DATASET_NAME = "nyc_311"
TIMESTAMP_FIELD = "created_date"
PAGE_SIZE = 1000  # Socrata max


def load_config() -> dict[str, str]:
    """Load required config from environment."""
    errors: list[str] = []
    cfg: dict[str, str] = {}

    bucket = os.environ.get("GCS_BUCKET_NAME", "").strip()
    if not bucket:
        errors.append("GCS_BUCKET_NAME is required")
    else:
        cfg["GCS_BUCKET_NAME"] = bucket

    app_token = os.environ.get("SOCRATA_APP_TOKEN", "").strip()
    if app_token:
        cfg["SOCRATA_APP_TOKEN"] = app_token

    creds = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    if not creds:
        errors.append("GOOGLE_APPLICATION_CREDENTIALS not set")

    if errors:
        for e in errors:
            logger.error(e)
        sys.exit(1)

    return cfg


def monthly_ranges(n_months: int = 3) -> list[tuple[date, date]]:
    """
    Return [(month_start, month_end), ...] for the last n_months.

    Example (today=2026-06-06, n=3):
        [(2026-03-01, 2026-04-01), (2026-04-01, 2026-05-01), (2026-05-01, 2026-06-01)]
    """
    today = date.today()
    # First day of the month n_months ago
    start_month_num = ((today.month - 1 - n_months) % 12) + 1
    start_year = today.year - ((today.month - 1 - n_months) // 12)
    current = date(start_year, start_month_num, 1)

    ranges: list[tuple[date, date]] = []
    while current <= today:
        if current.month == 12:
            next_month = date(current.year + 1, 1, 1)
        else:
            next_month = date(current.year, current.month + 1, 1)
        ranges.append((current, next_month))
        current = next_month

    return ranges


def fetch_month(
    client: SocrataClient,
    ts_field: str,
    month_start: date,
    month_end: date,
) -> list[dict[str, Any]]:
    """Fetch all records where ts_field falls in [month_start, month_end)."""
    start_dt = datetime.combine(month_start, datetime.min.time())
    end_dt = datetime.combine(month_end, datetime.min.time())

    logger.info("Fetching %s in [%s, %s)", ts_field, month_start, month_end)

    records: list[dict[str, Any]] = []
    for page in client.fetch_all_paginated(
        timestamp_field=ts_field,
        start_dt=start_dt,
        end_dt=end_dt,
        page_size=PAGE_SIZE,
    ):
        records.append(page)
        if len(records) % 5000 == 0:
            logger.info("  ... %d records fetched", len(records))

    logger.info("  -> %d records", len(records))
    return records


def main() -> None:
    logger.info("=" * 60)
    logger.info("NYC 311 Backfill  SRC-NYC-001  (last 3 months)")
    logger.info("=" * 60)

    cfg = load_config()

    client = SocrataClient(
        resource_id=RESOURCE_ID,
        domain=DOMAIN,
        app_token=cfg.get("SOCRATA_APP_TOKEN"),
    )

    loader = GCSBronzeLoader(
        bucket_name=cfg["GCS_BUCKET_NAME"],
        timestamp_field=TIMESTAMP_FIELD,
    )

    total = 0
    for month_start, month_end in monthly_ranges(n_months=3):
        month_str = month_start.strftime("%Y-%m")
        logger.info("─── Month: %s ───", month_str)

        records = fetch_month(client, TIMESTAMP_FIELD, month_start, month_end)

        if not records:
            logger.warning("  No records for %s, skipping", month_str)
            continue

        manifest = loader.write_monthly_shard(
            source_id=SOURCE_ID,
            dataset_name=DATASET_NAME,
            month_partition=month_str,
            records=records,
        )

        logger.info(
            "  Wrote gs://%s/bronze/raw/%s/%s/data_%s.json  "
            "records=%d  fetch_timestamp=%s",
            cfg["GCS_BUCKET_NAME"], SOURCE_ID, DATASET_NAME, month_str,
            manifest.record_count, manifest.fetch_timestamp,
        )
        total += manifest.record_count

    logger.info("=" * 60)
    logger.info("Backfill complete. Total records written: %d", total)
    logger.info(
        "GCS prefix: gs://%s/bronze/raw/%s/%s/",
        cfg["GCS_BUCKET_NAME"], SOURCE_ID, DATASET_NAME,
    )
    logger.info("=" * 60)


if __name__ == "__main__":
    main()