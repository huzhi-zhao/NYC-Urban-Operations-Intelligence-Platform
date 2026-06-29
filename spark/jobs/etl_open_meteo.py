"""Bronze -> Silver ETL for SRC-Open-Meteo / nyc_weather_forecast.

Reads the daily-split Bronze NDJSON files for a 7-day lookback window ending
at `execution_date` (to absorb late forecast revisions, matching the Socrata
lookback convention in AGENTS.md), dedupes by forecast freshness, normalizes
timestamps to UTC, and writes:

  gs://{bucket}/silver/weather/date=YYYY-MM-DD/*.parquet      (valid rows)
  gs://{bucket}/silver/_rejects/weather/date=YYYY-MM-DD/*.parquet  (quarantined rows)

Idempotent: re-running the same execution_date overwrites only the partitions
it touches (`partitionOverwriteMode=dynamic`), never the whole table.

Usage:
    spark-submit spark/jobs/etl_open_meteo.py \
        --bucket nyc-uoip-bronze --execution-date 2026-06-28
"""

from __future__ import annotations

import argparse
import logging
from datetime import date, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from spark.schemas.weather_schemas import WEATHER_RAW_SCHEMA
from spark.transforms.weather import (
    dedupe_by_freshness,
    normalize_timestamps,
    parse_ingest_date,
    split_by_validity,
)

logger = logging.getLogger(__name__)

SOURCE_ID = "SRC-Open-Meteo"
DATASET = "nyc_weather_forecast"
LOOKBACK_DAYS = 7
MIN_EXPECTED_ROWS_PER_DAY = 12  # alert threshold: half of the ~24/day baseline


def _bronze_paths(bucket: str, execution_date: date) -> list[str]:
    """Build the glob of daily Bronze files covering the lookback window.

    Spans month boundaries by deriving each day's own YYYY-MM folder rather
    than assuming the whole window falls in execution_date's month.
    """
    paths = []
    for offset in range(LOOKBACK_DAYS):
        day = execution_date - timedelta(days=offset)
        month_folder = day.strftime("%Y-%m")
        paths.append(
            f"gs://{bucket}/bronze/raw/{SOURCE_ID}/{DATASET}/{month_folder}/data_{day.isoformat()}.ndjson"
        )
    return paths


def run(spark: SparkSession, bucket: str, execution_date: date) -> None:
    paths = _bronze_paths(bucket, execution_date)
    raw = (
        spark.read.schema(WEATHER_RAW_SCHEMA)
        .json(paths)
        .withColumn("_source_file", F.input_file_name())
    )

    raw = parse_ingest_date(raw)
    deduped = dedupe_by_freshness(raw)
    normalized = normalize_timestamps(deduped, source_id=SOURCE_ID)
    valid, rejected = split_by_validity(normalized)

    silver_path = f"gs://{bucket}/silver/weather"
    rejects_path = f"gs://{bucket}/silver/_rejects/weather"

    (
        valid.write.partitionBy("date")
        .option("partitionOverwriteMode", "dynamic")
        .mode("overwrite")
        .parquet(silver_path)
    )
    if rejected.take(1):
        (
            rejected.write.partitionBy("date")
            .option("partitionOverwriteMode", "dynamic")
            .mode("overwrite")
            .parquet(rejects_path)
        )

    row_count = valid.count()
    logger.info(
        "%s/%s: execution_date=%s lookback_days=%d valid_rows=%d rejected_rows=%d",
        SOURCE_ID, DATASET, execution_date, LOOKBACK_DAYS, row_count, rejected.count(),
    )
    if row_count < MIN_EXPECTED_ROWS_PER_DAY:
        raise RuntimeError(
            f"{SOURCE_ID}/{DATASET}: only {row_count} valid Silver rows for "
            f"execution_date={execution_date} (baseline ~24/day) — possible API "
            f"outage or upstream schema change. Escalate per CLAUDE.md."
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--execution-date", required=True, type=date.fromisoformat)
    args = parser.parse_args()

    spark = SparkSession.builder.appName(f"etl_open_meteo_{args.execution_date}").getOrCreate()
    try:
        run(spark, bucket=args.bucket, execution_date=args.execution_date)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
