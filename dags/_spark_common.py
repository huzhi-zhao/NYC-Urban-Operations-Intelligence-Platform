"""
Shared Spark-submit config for all Silver DAGs (daily incremental + backfill).

Import pattern:
    from _spark_common import GCS_CONNECTOR_PACKAGE, SPARK_CONF
"""

from __future__ import annotations

GCS_KEY_PATH = "/opt/airflow/keys/nyc-uoip-sa-key.json"
GCS_CONNECTOR_PACKAGE = "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.21"

SPARK_CONF = {
    "spark.hadoop.fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
    "spark.hadoop.fs.AbstractFileSystem.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
    "spark.hadoop.google.cloud.auth.service.account.json.keyfile": GCS_KEY_PATH,
}
