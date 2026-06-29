"""Bronze → Silver transforms for SRC-Open-Meteo / nyc_weather_forecast.

Pipeline order (see spark/jobs/etl_open_meteo.py for orchestration):
  1. parse_ingest_date    — recover the source file's ingest_date for dedup ranking
  2. dedupe_by_freshness   — keep the most recently re-forecast value per UTC hour
  3. normalize_timestamps  — local America/New_York -> UTC, derive partition date
  4. split_by_validity     — range/null checks; valid rows vs. quarantined rejects

Valid ranges are frozen in contracts/api-contracts/open-meteo.yaml.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from pyspark.sql.window import Window

from spark.transforms.timestamp_normalizer import localize_naive_to_utc, to_partition_date

SOURCE_TZ = "America/New_York"

VALID_RANGES = {
    "temperature_2m": (-40.0, 50.0),
    "precipitation": (0.0, None),
    "snowfall": (0.0, None),
    "windspeed_10m": (0.0, 200.0),
}


def parse_ingest_date(df: DataFrame, source_path_column: str = "_source_file") -> DataFrame:
    """Extract the YYYY-MM-DD ingest date from the Bronze filename.

    Expects paths shaped like ``.../YYYY-MM/data_YYYY-MM-DD.ndjson`` (the
    daily-split Bronze layout used by SRC-Open-Meteo).
    """
    return df.withColumn(
        "ingest_date",
        F.regexp_extract(F.col(source_path_column), r"data_(\d{4}-\d{2}-\d{2})\.ndjson", 1),
    )


def dedupe_by_freshness(df: DataFrame) -> DataFrame:
    """Keep one row per raw ``time`` value: the one from the freshest ingest_date.

    Each daily Bronze file re-forecasts the next 7 days, so the same hour can
    appear in several files with different forecast values. The file with the
    later ingest_date always reflects the more recent model run and wins.
    """
    ranked = df.withColumn(
        "_rank",
        F.row_number().over(
            Window.partitionBy("time").orderBy(F.col("ingest_date").desc())
        ),
    )
    return ranked.filter(F.col("_rank") == 1).drop("_rank")


def normalize_timestamps(df: DataFrame, source_id: str) -> DataFrame:
    """Convert raw local-time `time` to UTC `time_utc` and derive partition `date`.

    Adds `source_id` and `loaded_at` (job run time) audit columns.
    """
    df = localize_naive_to_utc(df, "time", SOURCE_TZ)
    df = df.withColumnRenamed("time", "time_utc")
    df = df.withColumn("date", to_partition_date(F.col("time_utc")))
    df = df.withColumn("source_id", F.lit(source_id))
    df = df.withColumn("loaded_at", F.current_timestamp())
    return df


def split_by_validity(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split rows into (valid, rejected) based on null timestamps and range checks.

    Rejected rows are tagged with `_reject_reason` rather than dropped, so they
    can be written to a quarantine path for audit instead of silently lost.
    """
    reasons = [F.when(F.col("time_utc").isNull(), F.lit("null_time_utc"))]
    for column, (lo, hi) in VALID_RANGES.items():
        out_of_range = F.lit(False)
        if lo is not None:
            out_of_range = out_of_range | (F.col(column) < F.lit(lo))
        if hi is not None:
            out_of_range = out_of_range | (F.col(column) > F.lit(hi))
        cond = F.col(column).isNotNull() & out_of_range
        reasons.append(F.when(cond, F.lit(f"{column}_out_of_range")))

    df = df.withColumn("_reject_reason", F.coalesce(*reasons))
    valid = df.filter(F.col("_reject_reason").isNull()).drop("_reject_reason")
    rejected = df.filter(F.col("_reject_reason").isNotNull())
    return valid, rejected


def enforce_schema(df: DataFrame, schema: StructType) -> DataFrame:
    """Align `df` to exactly `schema` before writing — fail fast on drift.

    Raises if the transform pipeline's output columns don't exactly match the
    declared Silver schema (spark/schemas/weather_schemas.py), so a code
    change that silently drops/adds/retypes a column breaks the job instead
    of writing an undocumented table shape.
    """
    expected = {f.name for f in schema.fields}
    actual = set(df.columns)
    missing = expected - actual
    unexpected = actual - expected
    if missing or unexpected:
        raise ValueError(
            f"Silver output columns don't match WEATHER_SILVER_SCHEMA: "
            f"missing={sorted(missing)} unexpected={sorted(unexpected)}"
        )
    return df.select([F.col(f.name).cast(f.dataType) for f in schema.fields])
