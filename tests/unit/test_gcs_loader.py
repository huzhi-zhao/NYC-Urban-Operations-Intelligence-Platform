"""
Unit tests for ingestion.loaders.gcs_loader.GCSBronzeLoader.

The GCS loader is exercised in two flavors:
- ``write_daily()`` — splits records by their ``timestamp_field`` date and
  writes per-day files inside a month folder. The unit tests below mock
  ``storage.Client`` so they run without GCS credentials.
- ``write_monthly_shard()`` — integration-tested elsewhere (requires GCS).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from ingestion.loaders.gcs_loader import GCSBronzeLoader


def _make_loader(timestamp_field: str = "created_date") -> tuple[GCSBronzeLoader, MagicMock]:
    """Build a GCSBronzeLoader with a mocked storage client + bucket.

    Returns:
        (loader, bucket_mock) — ``bucket_mock.uploaded`` is a list of
        (path, content_bytes, metadata) tuples captured for assertions.
    """
    bucket_mock = MagicMock(name="bucket")
    uploaded: list[tuple[str, bytes, dict]] = []
    blob_mock = MagicMock()
    blob_mock.upload_from_string.side_effect = (
        lambda data, content_type=None: uploaded.append(
            (blob_mock._path, data, dict(blob_mock.metadata or {})),
        )
    )

    def _blob(path: str) -> MagicMock:
        b = MagicMock()
        b._path = path
        b.metadata = None
        b.upload_from_string = lambda data, content_type=None: uploaded.append(
            (path, data, dict(b.metadata or {})),
        )
        return b

    bucket_mock.blob.side_effect = _blob
    bucket_mock.uploaded = uploaded  # type: ignore[attr-defined]

    client_mock = MagicMock(name="client")
    client_mock.bucket.return_value = bucket_mock

    loader = GCSBronzeLoader(
        bucket_name="test-bucket",
        timestamp_field=timestamp_field,
        client=client_mock,
    )
    return loader, bucket_mock


def _sample_311_records() -> list[dict]:
    return [
        {"unique_key": "A", "created_date": "2026-03-21T09:00:00.000"},
        {"unique_key": "B", "created_date": "2026-03-21T18:30:00.000"},
        {"unique_key": "C", "created_date": "2026-03-22T03:15:00.000"},
        {"unique_key": "D", "created_date": "2026-04-01T00:00:00.000"},
    ]


# ── write_daily() — grouping & path layout ──────────────────────────────────


def test_write_daily_splits_records_into_per_day_files():
    loader, bucket = _make_loader("created_date")

    manifests = loader.write_daily(
        source_id="SRC-NYC-311",
        dataset_name="nyc_311",
        records=_sample_311_records(),
    )

    # 3 days, 3 manifests in chronological order
    assert [m.filename for m in manifests] == [
        "data_2026-03-21.ndjson",
        "data_2026-03-22.ndjson",
        "data_2026-04-01.ndjson",
    ]
    # Record counts per day: A+B, C, D
    assert [m.record_count for m in manifests] == [2, 1, 1]
    # data_date_min == data_date_max for a daily group
    for m in manifests:
        assert m.data_date_min == m.data_date_max

    paths = [p for (p, _, _) in bucket.uploaded]
    # 3 data files + 3 per-day manifests = 6 uploads
    assert len(paths) == 6
    assert "bronze/raw/SRC-NYC-311/nyc_311/2026-03/data_2026-03-21.ndjson" in paths
    assert "bronze/raw/SRC-NYC-311/nyc_311/2026-03/data_2026-03-22.ndjson" in paths
    assert "bronze/raw/SRC-NYC-311/nyc_311/2026-04/data_2026-04-01.ndjson" in paths
    # Per-day manifest files (one per data file)
    assert "bronze/raw/SRC-NYC-311/nyc_311/2026-03/manifest_2026-03-21.json" in paths
    assert "bronze/raw/SRC-NYC-311/nyc_311/2026-03/manifest_2026-03-22.json" in paths
    assert "bronze/raw/SRC-NYC-311/nyc_311/2026-04/manifest_2026-04-01.json" in paths


def test_write_daily_writes_only_records_within_their_day():
    loader, bucket = _make_loader("created_date")
    loader.write_daily(
        source_id="SRC-NYC-311",
        dataset_name="nyc_311",
        records=_sample_311_records(),
    )
    # Find the day-1 data file and confirm it contains only the day-1 records
    day1_path = "bronze/raw/SRC-NYC-311/nyc_311/2026-03/data_2026-03-21.ndjson"
    day1_content = next(
        content for (path, content, _) in bucket.uploaded if path == day1_path
    )
    payload = [json.loads(line) for line in day1_content.decode("utf-8").splitlines()]
    keys = sorted(r["unique_key"] for r in payload)
    assert keys == ["A", "B"]


def test_write_daily_per_day_manifest_describes_that_day():
    """Each day gets its own manifest_YYYY-MM-DD.json describing its data file."""
    loader, bucket = _make_loader("created_date")
    loader.write_daily(
        source_id="SRC-NYC-311",
        dataset_name="nyc_311",
        records=_sample_311_records(),
    )

    # Two days in 2026-03 → two distinct manifest files (one per day).
    day21_manifest = "bronze/raw/SRC-NYC-311/nyc_311/2026-03/manifest_2026-03-21.json"
    day22_manifest = "bronze/raw/SRC-NYC-311/nyc_311/2026-03/manifest_2026-03-22.json"

    day21_content = next(
        content for (path, content, _) in bucket.uploaded if path == day21_manifest
    )
    day22_content = next(
        content for (path, content, _) in bucket.uploaded if path == day22_manifest
    )

    day21 = json.loads(day21_content)
    assert day21["filename"] == "data_2026-03-21.ndjson"
    assert day21["record_count"] == 2
    assert day21["data_date_min"] == "2026-03-21"
    assert day21["data_date_max"] == "2026-03-21"
    assert day21["month_partition"] == "2026-03"

    day22 = json.loads(day22_content)
    assert day22["filename"] == "data_2026-03-22.ndjson"
    assert day22["record_count"] == 1
    assert day22["data_date_min"] == "2026-03-22"
    assert day22["data_date_max"] == "2026-03-22"


def test_write_daily_handles_open_meteo_time_field():
    """Open-Meteo records have an ISO time like '2026-03-21T00:00' (no microseconds)."""
    records = [
        {"time": "2026-03-21T00:00", "temperature_2m": 1.0},
        {"time": "2026-03-21T12:00", "temperature_2m": 5.5},
        {"time": "2026-03-22T00:00", "temperature_2m": 0.5},
    ]
    loader, bucket = _make_loader("time")
    manifests = loader.write_daily(
        source_id="SRC-Open-Meteo",
        dataset_name="nyc_weather_forecast",
        records=records,
    )
    assert [m.filename for m in manifests] == [
        "data_2026-03-21.ndjson",
        "data_2026-03-22.ndjson",
    ]
    paths = [p for (p, _, _) in bucket.uploaded]
    assert "bronze/raw/SRC-Open-Meteo/nyc_weather_forecast/2026-03/manifest_2026-03-21.json" in paths
    assert "bronze/raw/SRC-Open-Meteo/nyc_weather_forecast/2026-03/manifest_2026-03-22.json" in paths


def test_write_daily_handles_z_suffix_iso_timestamps():
    """Timestamps with a 'Z' suffix (UTC) are parsed correctly."""
    records = [
        {"created_date": "2026-03-21T23:30:00Z", "x": 1},
        {"created_date": "2026-03-22T00:30:00Z", "x": 2},
    ]
    loader, _ = _make_loader("created_date")
    manifests = loader.write_daily(
        source_id="SRC-NYC-311",
        dataset_name="nyc_311",
        records=records,
    )
    assert [m.filename for m in manifests] == [
        "data_2026-03-21.ndjson",
        "data_2026-03-22.ndjson",
    ]


def test_write_daily_drops_records_with_missing_or_bad_timestamp():
    records = [
        {"created_date": "2026-03-21T09:00:00.000", "x": 1},
        {"created_date": None, "x": 2},          # missing
        {"x": 3},                                # missing
        {"created_date": "not-a-date", "x": 4},  # unparseable
    ]
    loader, _ = _make_loader("created_date")
    manifests = loader.write_daily(
        source_id="SRC-NYC-311",
        dataset_name="nyc_311",
        records=records,
    )
    assert len(manifests) == 1
    assert manifests[0].record_count == 1
    assert manifests[0].filename == "data_2026-03-21.ndjson"


def test_write_daily_returns_empty_list_when_no_usable_records():
    loader, bucket = _make_loader("created_date")
    manifests = loader.write_daily(
        source_id="SRC-NYC-311",
        dataset_name="nyc_311",
        records=[{"x": 1}, {"created_date": "garbage"}],
    )
    assert manifests == []
    assert bucket.uploaded == []


def test_write_daily_raises_if_timestamp_field_not_configured():
    """Calling write_daily() on a loader with empty timestamp_field is a programming error."""
    loader, _ = _make_loader(timestamp_field="")
    with pytest.raises(ValueError, match="timestamp_field"):
        loader.write_daily(
            source_id="SRC-NYC-311",
            dataset_name="nyc_311",
            records=[{"created_date": "2026-03-21T00:00:00"}],
        )


# ── _group_by_date() helper ─────────────────────────────────────────────────


def test_group_by_date_preserves_record_order_within_a_day():
    loader = _make_loader("created_date")[0]
    records = [
        {"created_date": "2026-03-21T10:00:00.000", "i": 0},
        {"created_date": "2026-03-21T09:00:00.000", "i": 1},  # earlier same day
        {"created_date": "2026-03-22T00:00:00.000", "i": 2},
    ]
    groups = loader._group_by_date(records)
    # Day-1 records should be in input order, not sorted by timestamp
    assert [r["i"] for r in groups["2026-03-21"]] == [0, 1]
    assert [r["i"] for r in groups["2026-03-22"]] == [2]
