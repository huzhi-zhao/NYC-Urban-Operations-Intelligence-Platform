"""Shared timestamp normalization for all Silver transforms.

CLAUDE.md requires every Silver timestamp to be UTC and routed through this
module. Sources differ in how their raw timestamp strings carry (or omit)
timezone information — each source-specific helper below documents that.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


def localize_naive_to_utc(df: DataFrame, column: str, source_tz: str) -> DataFrame:
    """Convert a naive (no-offset) local-time string/timestamp column to UTC.

    Use this when the raw value has no UTC offset and the source's wall-clock
    timezone is known out-of-band (e.g. an API ``timezone`` query param).
    Spark's ``to_utc_timestamp`` first parses the value as a naive timestamp,
    then reinterprets it as having been recorded in ``source_tz``.

    Args:
        df: input DataFrame.
        column: name of the column to convert in place.
        source_tz: IANA timezone name the raw values were recorded in
            (e.g. ``"America/New_York"``).
    """
    return df.withColumn(column, F.to_utc_timestamp(F.col(column), source_tz))


def to_partition_date(column: Column) -> Column:
    """Derive the Silver partition date (UTC, ``YYYY-MM-DD``) from a UTC timestamp column."""
    return F.date_format(column, "yyyy-MM-dd")
