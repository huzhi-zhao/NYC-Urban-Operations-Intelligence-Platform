"""
Test 3 — Single Month Retrieval Test (integration)

Fetches one full month of 311 data from Socrata API and writes to GCS.
This validates the complete fetch → write pipeline for a single partition.

Run:
    export GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa.json
    export GCS_BUCKET_NAME=your-bucket
    export SOCRATA_APP_TOKEN=your_token  # optional
    python -m pytest tests/integration/test_single_month.py -v

Uses a hard-coded past month (2026-02) to ensure stable, reproducible results.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from datetime import datetime, date

from dotenv import load_dotenv
load_dotenv()

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from ingestion.clients.socrata_client import SocrataClient
from ingestion.loaders.gcs_loader import GCSBronzeLoader

BUCKET = os.environ.get("GCS_BUCKET_NAME", "")
CREDS = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
APP_TOKEN = os.environ.get("SOCRATA_APP_TOKEN", "") or None

SOURCE_ID = "SRC-NYC-311"
DATASET_NAME = "nyc_311"
TIMESTAMP_FIELD = "created_date"
TEST_MONTH = "2026-02"  # Fixed month for stable test

# [start, end) of the test month
TEST_START = date(2026, 2, 1)
TEST_END = date(2026, 3, 1)


@pytest.fixture
def clients():
    if not BUCKET:
        pytest.skip("GCS_BUCKET_NAME not set")
    # if not CREDS:
    #     pytest.skip("GOOGLE_APPLICATION_CREDENTIALS not set")
    socrata = SocrataClient(
        resource_id="erm2-nwe9",
        domain="data.cityofnewyork.us",
        app_token=APP_TOKEN,
    )
    gcs = GCSBronzeLoader(bucket_name=BUCKET, timestamp_field=TIMESTAMP_FIELD)
    return socrata, gcs


def test_single_month_fetch_and_write(clients):
    """Fetch one full month from API and write to GCS."""
    socrata, gcs = clients

    start_dt = datetime.combine(TEST_START, datetime.min.time())
    end_dt = datetime.combine(TEST_END, datetime.min.time())

    records = []
    for page in socrata.fetch_all_paginated(
        timestamp_field=TIMESTAMP_FIELD,
        start_dt=start_dt,
        end_dt=end_dt,
        page_size=1000,
    ):
        records.append(page)

    assert len(records) > 0, f"No records returned for {TEST_MONTH}"
    print(f"\n  Fetched {len(records)} records for {TEST_MONTH}")

    # Verify all records belong to the test month
    out_of_range = []
    for r in records:
        raw = r.get(TIMESTAMP_FIELD, "")
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.date() < TEST_START or dt.date() >= TEST_END:
            out_of_range.append((r.get("unique_key"), dt.date()))

    assert not out_of_range, f"Records outside test month: {out_of_range[:5]}"

    # Write to GCS
    manifest = gcs.write_monthly_shard(
        source_id=SOURCE_ID,
        dataset_name=DATASET_NAME,
        month_partition=TEST_MONTH,
        records=records,
    )

    assert manifest.record_count == len(records)
    assert manifest.month_partition == TEST_MONTH
    assert manifest.data_date_min is not None
    assert manifest.data_date_max is not None
    assert manifest.fetch_timestamp  # upload time recorded

    # Verify GCS files exist
    bucket = gcs._client.bucket(BUCKET)
    data_path = f"bronze/raw/{SOURCE_ID}/{DATASET_NAME}/data_{TEST_MONTH}.json"
    manifest_path = f"bronze/raw/{SOURCE_ID}/{DATASET_NAME}/manifest_{TEST_MONTH}.json"

    assert bucket.blob(data_path).exists(), f"Data not found: {data_path}"
    assert bucket.blob(manifest_path).exists(), f"Manifest not found: {manifest_path}"

    print(f"✓ Single month test passed")
    print(f"  Month:        {TEST_MONTH}")
    print(f"  Records:      {manifest.record_count}")
    print(f"  Data range:   {manifest.data_date_min} → {manifest.data_date_max}")
    print(f"  Uploaded at:  {manifest.fetch_timestamp}")
    print(f"  GCS path:     gs://{BUCKET}/{data_path}")


if __name__ == "__main__":
    print("=" * 60)
    print("Test 3 — Single Month Retrieval Test")
    print(f"Month: {TEST_MONTH}")
    print("=" * 60)
    if not BUCKET or not CREDS:
        print("SKIP: Set GOOGLE_APPLICATION_CREDENTIALS and GCS_BUCKET_NAME to run")
        sys.exit(0)

    socrata = SocrataClient(resource_id="erm2-nwe9", domain="data.cityofnewyork.us", app_token=APP_TOKEN)
    gcs = GCSBronzeLoader(bucket_name=BUCKET, timestamp_field=TIMESTAMP_FIELD)
    test_single_month_fetch_and_write((socrata, gcs))