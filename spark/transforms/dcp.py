"""Bronze → Silver transforms for SRC-DCP / borough_boundaries.

Pipeline order (see spark/jobs/etl_dcp.py for orchestration):
  1. geojson_to_wkt_udf  — convert the_geom nested struct → WKT string via Shapely
  2. cast_scalars         — borocode→int, shape_area/leng→double
  3. validate_boroughs    — reject any row whose borough_id is not in {1..5}
  4. enforce_schema       — align columns to DCP_SILVER_SCHEMA before write
"""

from __future__ import annotations

import json

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType

VALID_BOROUGH_IDS = {1, 2, 3, 4, 5}


def _geojson_struct_to_wkt(geom_json: str | None) -> str | None:
    """Convert a GeoJSON string (serialised from Spark struct) to WKT.

    Shapely 2.x: shape(geojson_dict).wkt produces e.g.
    "MULTIPOLYGON (((-74.05 40.56, ...)))"
    """
    if geom_json is None:
        return None
    from shapely.geometry import shape  # deferred: not available on driver at import time

    return shape(json.loads(geom_json)).wkt


_geojson_to_wkt_udf = F.udf(_geojson_struct_to_wkt, StringType())


def add_geometry_wkt(df: DataFrame) -> DataFrame:
    """Serialise the nested the_geom struct to JSON, then convert to WKT.

    Spark reads MultiPolygon coordinates as a nested ARRAY struct; to_json()
    turns it back into a GeoJSON string that Shapely can parse.
    """
    return df.withColumn(
        "geometry_wkt",
        _geojson_to_wkt_udf(F.to_json(F.col("the_geom"))),
    )


def cast_scalars(df: DataFrame, source_id: str) -> DataFrame:
    """Cast string fields to typed columns and add audit columns."""
    return (
        df.withColumn("borough_id",      F.col("borocode").cast("int"))
        .withColumn("borough_name",    F.col("boroname"))
        .withColumn("shape_area_sqft", F.col("shape_area").cast("double"))
        .withColumn("shape_leng_ft",   F.col("shape_leng").cast("double"))
        .withColumn("source_id",       F.lit(source_id))
        .withColumn("loaded_at",       F.current_timestamp())
    )


def split_by_validity(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split into (valid, rejected) based on borough_id range and null geometry."""
    valid_ids = F.array(*[F.lit(i) for i in sorted(VALID_BOROUGH_IDS)])

    reject_reason = F.when(
        F.col("borough_id").isNull() | ~F.array_contains(valid_ids, F.col("borough_id")),
        F.lit("invalid_borough_id"),
    ).when(
        F.col("geometry_wkt").isNull(),
        F.lit("null_geometry"),
    )

    df = df.withColumn("_reject_reason", reject_reason)
    valid    = df.filter(F.col("_reject_reason").isNull()).drop("_reject_reason")
    rejected = df.filter(F.col("_reject_reason").isNotNull())
    return valid, rejected


def enforce_schema(df: DataFrame, schema: StructType) -> DataFrame:
    """Project df to exactly the columns declared in DCP_SILVER_SCHEMA.

    Extra columns from the raw pipeline (borocode, the_geom, etc.) are silently
    dropped by the select. Only missing columns cause a hard failure — that means
    a required transform step was skipped.
    """
    expected = {f.name for f in schema.fields}
    missing  = expected - set(df.columns)
    if missing:
        raise ValueError(
            f"Silver output columns don't match DCP_SILVER_SCHEMA: "
            f"missing={sorted(missing)}"
        )
    return df.select([F.col(f.name).cast(f.dataType) for f in schema.fields])
