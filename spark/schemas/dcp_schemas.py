"""StructType definitions for SRC-DCP / borough_boundaries.

Static reference table: 5 rows, one per NYC borough. Never infer schema
from raw NDJSON — always use DCP_RAW_SCHEMA to prevent silent type coercion.
Frozen against contracts/api-contracts/dcp-borough-boundaries.yaml.
"""

from __future__ import annotations

from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType, TimestampType

# Bronze NDJSON shape — all numeric fields arrive as strings from the API.
# the_geom is a nested struct: {type: STRING, coordinates: ARRAY<...>}
# We read it as a raw JSON string via spark.read.json; Spark infers the_geom
# as a StructType automatically, so we only declare the scalar fields here
# and handle the_geom via to_json() in the transform.
DCP_RAW_SCHEMA = StructType(
    [
        StructField("borocode", StringType(), nullable=False),
        StructField("boroname", StringType(), nullable=False),
        StructField("shape_area", StringType(), nullable=True),
        StructField("shape_leng", StringType(), nullable=True),
        # the_geom intentionally omitted: Spark infers the nested MultiPolygon
        # struct automatically; the transform serialises it back to JSON then
        # converts to WKT via Shapely.
    ]
)

# Silver grain: one row per borough, geometry stored as WKT string.
# BigQuery consumes geometry_wkt via ST_GEOGFROMTEXT(geometry_wkt).
DCP_SILVER_SCHEMA = StructType(
    [
        StructField("borough_id",      IntegerType(), nullable=False),   # borocode cast to int
        StructField("borough_name",    StringType(),  nullable=False),
        StructField("shape_area_sqft", DoubleType(),  nullable=True),
        StructField("shape_leng_ft",   DoubleType(),  nullable=True),
        StructField("geometry_wkt",    StringType(),  nullable=False),   # MultiPolygon WKT, WGS84
        StructField("source_id",       StringType(),  nullable=False),
        StructField("loaded_at",       TimestampType(), nullable=False),
    ]
)
