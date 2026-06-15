"""
Test 3 — Single Month Retrieval Test (integration)

Fetches one full month of 311 data from Socrata API and writes to GCS.
This validates the complete fetch → write pipeline for a single partition.

NYC 311 uses ``partition_strategy: daily`` — records are split by their
``created_date`` into per-day files inside the month folder.

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
from datetime import date
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from ingestion.backfill import BackfillFacade
from ingestion.config import load_source_config

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
def facade():
    if not BUCKET:
        pytest.skip("GCS_BUCKET_NAME not set")
    # if not CREDS:
    #     pytest.skip("GOOGLE_APPLICATION_CREDENTIALS not set")
    cfg = load_source_config(SOURCE_ID)
    return BackfillFacade(cfg, gcs_bucket=BUCKET)


def test_single_month_fetch_and_write(facade):
    """Fetch one full month from API and write per-day files to GCS."""
    manifests = facade.upload(
        start=TEST_START,
        end=TEST_END,
        dataset_name=DATASET_NAME,
    )

    assert len(manifests) > 0, f"No manifests produced for {TEST_MONTH}"
    total_records = sum(m.record_count for m in manifests)
    print(f"\n  Fetched {total_records} records for {TEST_MONTH} into {len(manifests)} daily file(s)")

    # Every daily manifest is in the test month
    for m in manifests:
        assert m.month_partition == TEST_MONTH, (
            f"month_partition {m.month_partition!r} != {TEST_MONTH!r}"
        )
        assert m.filename.startswith("data_2026-02-"), (
            f"Unexpected daily filename {m.filename!r}"
        )
        # data_date_min == data_date_max for a daily group
        assert m.data_date_min == m.data_date_max, (
            f"Daily group {m.filename!r} spans multiple dates"
        )
        assert m.fetch_timestamp  # upload time recorded

    # Verify GCS files exist
    bucket = facade._gcs_client.bucket(BUCKET)
    seen_data_paths: set[str] = set()
    seen_manifest_paths: set[str] = set()
    for m in manifests:
        data_path = f"bronze/raw/{SOURCE_ID}/{DATASET_NAME}/{m.month_partition}/{m.filename}"
        manifest_path = f"bronze/raw/{SOURCE_ID}/{DATASET_NAME}/{m.month_partition}/manifest.json"
        seen_data_paths.add(data_path)
        seen_manifest_paths.add(manifest_path)
        assert bucket.blob(data_path).exists(), f"Data not found: {data_path}"

    # Every month folder that received a daily write has a manifest.json
    for path in seen_manifest_paths:
        assert bucket.blob(path).exists(), f"Manifest not found: {path}"

    print("✓ Single month test passed")
    print(f"  Month:           {TEST_MONTH}")
    print(f"  Daily files:     {len(manifests)}")
    print(f"  Total records:   {total_records}")
    print(f"  GCS prefix:      gs://{BUCKET}/bronze/raw/{SOURCE_ID}/{DATASET_NAME}/{TEST_MONTH}/")


if __name__ == "__main__":
    print("=" * 60)
    print("Test 3 — Single Month Retrieval Test")
    print(f"Month: {TEST_MONTH}")
    print("=" * 60)
    if not BUCKET or not CREDS:
        print("SKIP: Set GOOGLE_APPLICATION_CREDENTIALS and GCS_BUCKET_NAME to run")
        sys.exit(0)

    cfg = load_source_config(SOURCE_ID)
    facade = BackfillFacade(cfg, gcs_bucket=BUCKET)
    test_single_month_fetch_and_write(facade)
