"""
Shared Spark-submit config for all Silver DAGs (daily incremental + backfill).

Import pattern:
    from _spark_common import GCS_CONNECTOR_JAR, SPARK_CONF
"""

from __future__ import annotations

GCS_KEY_PATH = "/opt/airflow/keys/nyc-uoip-sa-key.json"

# Use the SHADED connector jar via --jars, not --packages.
# --packages pulls the unshaded artifact plus ~90 transitive deps (Guava,
# gRPC, protobuf, ...) whose versions collide with the versions already
# bundled in Spark's own Hadoop client on the classpath — manifests as
# java.lang.NoSuchMethodError: 'void com.google.common.base.Preconditions
# .checkState(boolean, String, long)' at runtime. The shaded jar relocates
# all of those dependencies under its own package namespace, so it can't
# collide with anything already on the classpath.
#
# Served from Maven Central (the legacy storage.googleapis.com/hadoop-lib
# mirror Google used to publish this under has been taken down — 404s now).
GCS_CONNECTOR_JAR = (
    "https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/"
    "gcs-connector/hadoop3-2.2.21/gcs-connector-hadoop3-2.2.21-shaded.jar"
)

SPARK_CONF = {
    "spark.hadoop.fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
    "spark.hadoop.fs.AbstractFileSystem.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
    "spark.hadoop.google.cloud.auth.service.account.json.keyfile": GCS_KEY_PATH,
}
