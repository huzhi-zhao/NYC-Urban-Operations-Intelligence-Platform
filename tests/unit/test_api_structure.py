"""
Test 1 — API Structure Test (unit, no GCS required)

Calls the Socrata API with limit=1 to get a minimal sample,
then validates the response structure against expected fields.

 "记录 API 在某个时间点的响应格式"，用作回归检测——如果未来 API 字段变了，这个测试会失败。

Run:
    python -m pytest tests/unit/test_api_structure.py -v

No credentials required. Uses real API (公开免密 endpoint).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

# Ensure project root on path for absolute imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from ingestion.clients.socrata_client import SocrataClient

# Expected Socrata fields for SRC-NYC-311 (311)
REQUIRED_FIELDS = [
    "unique_key",
    "created_date",
    "complaint_type",
    "descriptor",
    "borough",
    "incident_zip",
    "latitude",
    "longitude",
    "status",
]

OPTIONAL_FIELDS = [
    "closed_date",
    "agency",
    "agency_name",
    "location",
]

EXPECTED_BOROUGHS = {
    "MANHATTAN", "BROOKLYN", "BRONX", "QUEENS", "STATEN ISLAND", "Unspecified"
}


def test_311_api_returns_valid_structure():
    """Fetch 1 record from 311 API and validate its shape."""
    client = SocrataClient(resource_id="erm2-nwe9", domain="data.cityofnewyork.us")

    records = client.fetch_page(limit=1)
    assert len(records) >= 1, "API should return at least 1 record"

    record = records[0]

    # Required fields must be present
    missing = [f for f in REQUIRED_FIELDS if f not in record]
    assert not missing, f"Missing required fields: {missing}"

    # created_date should be parseable as ISO datetime
    from datetime import datetime
    try:
        dt = datetime.fromisoformat(record["created_date"].replace("Z", "+00:00"))
        assert dt.year >= 2010, "created_date year should be >= 2010"
    except ValueError as e:
        raise AssertionError(f"created_date not ISO parseable: {e}")

    # latitude/longitude should be numeric strings (or convertible)
    lat = record.get("latitude", "")
    lon = record.get("longitude", "")
    if lat and lon:
        float(lat), float(lon)  # raises if not numeric

    # borough value should be known NYC borough or Unspecified
    boro = record.get("borough", "").upper()
    assert boro in EXPECTED_BOROUGHS, f"Unknown borough: {boro}"

    print(f"✓ API structure valid — record has {len(record)} fields")
    print(f"  created_date : {record['created_date']}")
    print(f"  borough      : {record.get('borough')}")
    print(f"  complaint    : {record.get('complaint_type')}")
    print(f"  status       : {record.get('status')}")


def test_sample_fixture_structure():
    """Validate the local fixture file matches expected 311 schema."""
    fixture_path = Path(__file__).parent.parent / "fixtures" / "sample_311_response.json"
    assert fixture_path.exists(), f"Fixture not found: {fixture_path}"

    records = json.loads(fixture_path.read_text())
    assert len(records) == 2, "Fixture should have 2 sample records"

    for record in records:
        missing = [f for f in REQUIRED_FIELDS if f not in record]
        assert not missing, f"Fixture record missing fields: {missing}"

    print(f"✓ Fixture structure valid — {len(records)} records")


if __name__ == "__main__":
    print("=" * 60)
    print("Test 1 — API Structure Test")
    print("=" * 60)
    test_311_api_returns_valid_structure()
    test_sample_fixture_structure()
    print("\nAll structure tests passed ✓")
