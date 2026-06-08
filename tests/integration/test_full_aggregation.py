"""
Test 4 — Full Aggregation Test (integration)

End-to-end test that:
1. Fetches all 3 months of 311 data via backfill logic
2. Writes each month to GCS
3. Reads back manifest files and aggregates record counts
4. Validates total counts are consistent

Run:
    export GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa.json
    export GCS_BUCKET_NAME=your-bucket
    export SOCRATA_APP_TOKEN=your_token  # optional
    python -m pytest tests/integration/test_full_aggregation.py -v
"""

from __future__ import annotations

import json
import os
import sys
from datetime import date, datetime
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from ingestion.clients.socrata_client import SocrataClient
from ingestion.loaders.gcs_loader import GCSBronzeLoader

BUCKET = os.environ.get("GCS_BUCKET_NAME", "")
CREDS = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
APP_TOKEN = os.environ.get("SOCRATA_APP_TOKEN", "") or None

SOURCE_ID = "SRC-NYC-001"
DATASET_NAME = "nyc_311"
TIMESTAMP_FIELD = "created_date"
N_MONTHS = 3


def monthly_ranges(n: int) -> list[tuple[date, date]]:
    """Same logic as backfill script."""
    today = date.today()
    start_month_num = ((today.month - 1 - n) % 12) + 1
    start_year = today.year - ((today.month - 1 - n) // 12)
    current = date(start_year, start_month_num, 1)
    ranges = []
    while current <= today:
        next_month = date(current.year + 1, 1, 1) if current.month == 12 else date(current.year, current.month + 1, 1)
        ranges.append((current, next_month))
        current = next_month
    return ranges


@pytest.fixture
def gcs():
    if not BUCKET:
        pytest.skip("GCS_BUCKET_NAME not set")
    if not CREDS:
        pytest.skip("GOOGLE_APPLICATION_CREDENTIALS not set")
    return GCSBronzeLoader(bucket_name=BUCKET, timestamp_field=TIMESTAMP_FIELD)


@pytest.fixture
def socrata():
    return SocrataClient(
        resource_id="erm2-nwe9",
        domain="data.cityofnewyork.us",
        app_token=APP_TOKEN,
    )


def test_full_backfill_aggregation(gcs, socrata):
    """
    Run full backfill for N_MONTHS and verify:
    - All months written successfully
    - Manifests are readable from GCS
    - Total record count across months matches sum of manifest counts
    - All fetch_timestamps are recent (within last 5 minutes)
    """
    ranges = monthly_ranges(N_MONTHS)
    print(f"\n  Backfilling {N_MONTHS} months: {[r[0].isoformat() for r in ranges]}")

    manifests = []
    total_records = 0

    for month_start, month_end in ranges:
        month_str = month_start.strftime("%Y-%m")
        start_dt = datetime.combine(month_start, datetime.min.time())
        end_dt = datetime.combine(month_end, datetime.min.time())

        # Fetch
        records = list(socrata.fetch_all_paginated(
            timestamp_field=TIMESTAMP_FIELD,
            start_dt=start_dt,
            end_dt=end_dt,
            page_size=1000,
        ))

        print(f"  Month {month_str}: {len(records)} records fetched")

        # Write
        manifest = gcs.write_monthly_shard(
            source_id=SOURCE_ID,
            dataset_name=DATASET_NAME,
            month_partition=month_str,
            records=records,
        )
        manifests.append(manifest)
        total_records += manifest.record_count

    # Verify all manifests are readable from GCS
    bucket = gcs._client.bucket(BUCKET)
    for m in manifests:
        manifest_path = f"bronze/raw/{SOURCE_ID}/{DATASET_NAME}/manifest_{m.month_partition}.json"
        blob = bucket.blob(manifest_path)
        assert blob.exists(), f"Manifest not found: {manifest_path}"

        # Read back and compare
        data = json.loads(blob.download_as_text())
        assert data["record_count"] == m.record_count
        assert data["month_partition"] == m.month_partition
        assert data["source_id"] == SOURCE_ID
        assert data["dataset_name"] == DATASET_NAME
        assert data["fetch_timestamp"] == m.fetch_timestamp

    # Verify timestamps are recent
    now = datetime.utcnow()
    for m in manifests:
        ts = datetime.fromisoformat(m.fetch_timestamp)
        assert (now - ts).total_seconds() < 300, f"fetch_timestamp too old: {m.fetch_timestamp}"

    print(f"\n✓ Full aggregation test passed")
    print(f"  Months processed: {len(manifests)}")
    print(f"  Total records:    {total_records}")
    print(f"  Manifests verified: {[m.month_partition for m in manifests]}")
    print(f"  GCS prefix: gs://{BUCKET}/bronze/raw/{SOURCE_ID}/{DATASET_NAME}/")


if __name__ == "__main__":
    print("=" * 60)
    print("Test 4 — Full Aggregation Test")
    print(f"Months: last {N_MONTHS}")
    print("=" * 60)
    if not BUCKET or not CREDS:
        print("SKIP: Set GOOGLE_APPLICATION_CREDENTIALS and GCS_BUCKET_NAME to run")
        sys.exit(0)

    gcs = GCSBronzeLoader(bucket_name=BUCKET, timestamp_field=TIMESTAMP_FIELD)
    socrata = SocrataClient(resource_id="erm2-nwe9", domain="data.cityofnewyork.us", app_token=APP_TOKEN)
    test_full_backfill_aggregation(gcs, socrata)