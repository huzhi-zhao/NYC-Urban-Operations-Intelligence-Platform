"""
Test 2 — Small Upload Test (integration, requires GCS)

Uploads a tiny (2-record) payload to GCS Bronze and verifies:
- data file is written
- manifest file is written
- manifest fields are populated correctly

Run (requires GCS credentials):
    export GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa.json
    export GCS_BUCKET_NAME=your-test-bucket
    python -m pytest tests/integration/test_small_upload.py -v

This test is safe to re-run: it overwrites the same files each time.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from ingestion.loaders.gcs_loader import GCSBronzeLoader

BUCKET = os.environ.get("GCS_BUCKET_NAME", "")
CREDS = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")

# Minimal 2-record payload
SMALL_PAYLOAD = [
    {
        "unique_key": "TEST001",
        "created_date": "2026-03-01T09:00:00.000",
        "complaint_type": "Test Complaint",
        "descriptor": "Test descriptor",
        "borough": "MANHATTAN",
        "incident_zip": "10001",
        "latitude": "40.714",
        "longitude": "-74.006",
        "status": "Closed",
    },
    {
        "unique_key": "TEST002",
        "created_date": "2026-03-15T14:30:00.000",
        "complaint_type": "Test Heating",
        "descriptor": "No heat",
        "borough": "BROOKLYN",
        "incident_zip": "11213",
        "latitude": "40.678",
        "longitude": "-73.944",
        "status": "Open",
    },
]

TEST_SOURCE = "SRC-NYC-001"
TEST_DATASET = "nyc_311"
TEST_MONTH = "2026-03"


@pytest.fixture
def gcs_loader():
    """Skip test if GCS credentials are not configured."""
    if not BUCKET:
        pytest.skip("GCS_BUCKET_NAME not set")
    # if not CREDS:
    #     pytest.skip("GOOGLE_APPLICATION_CREDENTIALS not set")
    return GCSBronzeLoader(bucket_name=BUCKET, timestamp_field="created_date")


def test_small_upload_overwrites_data_and_manifest(gcs_loader):
    """
    Upload 2 records as a monthly shard.
    Verify data + manifest are written, then re-upload and verify overwrite.
    """
    # First upload
    manifest1 = gcs_loader.write_monthly_shard(
        source_id=TEST_SOURCE,
        dataset_name=TEST_DATASET,
        month_partition=TEST_MONTH,
        records=SMALL_PAYLOAD,
    )

    assert manifest1.record_count == 2
    assert manifest1.month_partition == TEST_MONTH
    assert manifest1.filename == f"data_{TEST_MONTH}.json"
    assert manifest1.sha256_checksum  # non-empty
    assert manifest1.data_date_min is not None
    assert manifest1.data_date_max is not None
    assert manifest1.fetch_timestamp  # upload time recorded

    from datetime import datetime
    ts1 = datetime.fromisoformat(manifest1.fetch_timestamp)

    # Second upload (same data) — should overwrite
    import time
    time.sleep(1.1)  # ensure fetch_timestamp differs by at least 1 second

    manifest2 = gcs_loader.write_monthly_shard(
        source_id=TEST_SOURCE,
        dataset_name=TEST_DATASET,
        month_partition=TEST_MONTH,
        records=SMALL_PAYLOAD,
    )

    # fetch_timestamp should be newer
    ts2 = datetime.fromisoformat(manifest2.fetch_timestamp)
    assert ts2 > ts1, "Re-upload should update fetch_timestamp"

    # Verify GCS objects actually exist
    bucket = gcs_loader._client.bucket(BUCKET)
    data_path = f"bronze/raw/{TEST_SOURCE}/{TEST_DATASET}/data_{TEST_MONTH}.json"
    manifest_path = f"bronze/raw/{TEST_SOURCE}/{TEST_DATASET}/manifest_{TEST_MONTH}.json"

    assert bucket.blob(data_path).exists(), f"Data file not found: {data_path}"
    assert bucket.blob(manifest_path).exists(), f"Manifest file not found: {manifest_path}"

    # Verify manifest content via GCS read-back
    manifest_blob = bucket.blob(manifest_path)
    manifest_data = json.loads(manifest_blob.download_as_text())
    assert manifest_data["source_id"] == TEST_SOURCE
    assert manifest_data["record_count"] == 2
    assert manifest_data["month_partition"] == TEST_MONTH
    assert manifest_data["fetch_timestamp"] == manifest2.fetch_timestamp

    print(f"\n✓ Small upload test passed")
    print(f"  Data:     gs://{BUCKET}/{data_path}")
    print(f"  Manifest: gs://{BUCKET}/{manifest_path}")
    print(f"  Records:  {manifest2.record_count}")
    print(f"  Uploaded: {manifest2.fetch_timestamp}")


if __name__ == "__main__":
    print("=" * 60)
    print("Test 2 — Small Upload Test")
    print("=" * 60)
    if not BUCKET or not CREDS:
        print("SKIP: Set GOOGLE_APPLICATION_CREDENTIALS and GCS_BUCKET_NAME to run")
        sys.exit(0)
    test_small_upload_overwrites_data_and_manifest(
        GCSBronzeLoader(bucket_name=BUCKET, timestamp_field="created_date")
    )