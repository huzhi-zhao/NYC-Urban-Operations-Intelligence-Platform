"""StructType definitions for SRC-Open-Meteo / nyc_weather_forecast.

Frozen against contracts/api-contracts/open-meteo.yaml. Spark must never read
this source with ``spark.read.json()`` schema inference (AGENTS.md rule) —
import ``WEATHER_RAW_SCHEMA`` instead.
"""

from __future__ import annotations

from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

# Bronze NDJSON shape, one record per hour.
# `time` is a local America/New_York string with no UTC offset — see contract.
WEATHER_RAW_SCHEMA = StructType(
    [
        StructField("time", StringType(), nullable=False),
        StructField("temperature_2m", DoubleType(), nullable=True),
        StructField("precipitation", DoubleType(), nullable=True),
        StructField("snowfall", DoubleType(), nullable=True),
        StructField("windspeed_10m", DoubleType(), nullable=True),
    ]
)

# Silver grain: one row per UTC hour, citywide. No borough_id — that join
# happens in the Gold layer against dim_geography.
WEATHER_SILVER_SCHEMA = StructType(
    [
        StructField("time_utc", TimestampType(), nullable=False),
        StructField("date", StringType(), nullable=False),  # partition column, YYYY-MM-DD
        StructField("temperature_2m", DoubleType(), nullable=True),
        StructField("precipitation", DoubleType(), nullable=True),
        StructField("snowfall", DoubleType(), nullable=True),
        StructField("windspeed_10m", DoubleType(), nullable=True),
        StructField("source_id", StringType(), nullable=False),
        StructField("ingest_date", StringType(), nullable=False),  # YYYY-MM-DD of source file
        StructField("loaded_at", TimestampType(), nullable=False),
    ]
)
