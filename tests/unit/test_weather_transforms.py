"""Unit tests for spark.transforms.weather (Bronze -> Silver, no GCS/cluster needed).

Uses a local in-process SparkSession (master=local[1]) — no Spark cluster or
cloud credentials required, so this stays in tests/unit per Makefile's
test-unit target.
"""

from __future__ import annotations

from datetime import datetime

import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import Row, SparkSession  # noqa: E402

from spark.transforms.weather import (  # noqa: E402
    dedupe_by_freshness,
    normalize_timestamps,
    parse_ingest_date,
    split_by_validity,
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test_weather_transforms")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_parse_ingest_date_extracts_date_from_bronze_path(spark):
    df = spark.createDataFrame(
        [Row(_source_file="gs://bucket/bronze/raw/SRC-Open-Meteo/nyc_weather_forecast/2026-06/data_2026-06-28.ndjson")]
    )
    result = parse_ingest_date(df).collect()
    assert result[0]["ingest_date"] == "2026-06-28"


def test_dedupe_by_freshness_keeps_latest_ingest_date(spark):
    df = spark.createDataFrame(
        [
            Row(time="2026-06-28T10:00", ingest_date="2026-06-27", temperature_2m=20.0),
            Row(time="2026-06-28T10:00", ingest_date="2026-06-28", temperature_2m=21.0),
        ]
    )
    result = dedupe_by_freshness(df).collect()
    assert len(result) == 1
    assert result[0]["temperature_2m"] == 21.0
    assert result[0]["ingest_date"] == "2026-06-28"


def test_normalize_timestamps_converts_ny_local_to_utc(spark):
    # 2026-06-28T10:00 America/New_York (EDT, UTC-4) -> 2026-06-28T14:00 UTC
    df = spark.createDataFrame([Row(time="2026-06-28T10:00:00")])
    result = normalize_timestamps(df, source_id="SRC-Open-Meteo").collect()
    assert result[0]["time_utc"] == datetime(2026, 6, 28, 14, 0, 0)
    assert result[0]["date"] == "2026-06-28"
    assert result[0]["source_id"] == "SRC-Open-Meteo"


def test_split_by_validity_rejects_out_of_range_and_null_timestamp(spark):
    df = spark.createDataFrame(
        [
            Row(
                time_utc=datetime(2026, 6, 28, 14, 0),
                temperature_2m=20.0, precipitation=1.0, snowfall=0.0, windspeed_10m=10.0,
            ),
            Row(
                time_utc=datetime(2026, 6, 28, 15, 0),
                temperature_2m=999.0, precipitation=1.0, snowfall=0.0, windspeed_10m=10.0,
            ),
            Row(
                time_utc=None,
                temperature_2m=20.0, precipitation=1.0, snowfall=0.0, windspeed_10m=10.0,
            ),
        ]
    )
    valid, rejected = split_by_validity(df)
    assert valid.count() == 1
    assert rejected.count() == 2
    reasons = {row["_reject_reason"] for row in rejected.collect()}
    assert reasons == {"temperature_2m_out_of_range", "null_time_utc"}
