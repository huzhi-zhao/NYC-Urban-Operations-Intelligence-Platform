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
    # PYTHON_VERSION_MISMATCH (worker 3.8 vs driver 3.11) fix.
    #
    # spark.executorEnv.PYSPARK_PYTHON alone does NOT work: it only injects an
    # OS env var into the Executor JVM's own process environment. The actual
    # command used to spawn the Python worker subprocess (`pythonExec`) is a
    # literal string embedded by the Driver into the serialized UDF closure
    # at build time (from spark.pyspark.python / PYSPARK_PYTHON, default
    # "python3") and shipped to the Executor as-is — the Executor's local env
    # vars never come into play. Since the Driver (airflow-scheduler) never
    # set PYSPARK_PYTHON, the embedded value defaulted to bare "python3",
    # which resolves on the spark-worker container's PATH to the base image's
    # Ubuntu Focal Python 3.8.
    #
    # spark.pyspark.python sets that embedded value correctly for the
    # Executor, but it also governs the Driver's own interpreter unless
    # overridden — and the Driver (airflow-scheduler) doesn't have
    # /usr/local/bin/python3.11 (that path only exists in the spark-worker
    # image), which broke the Driver with "Cannot run program
    # /usr/local/bin/python3.11: No such file". spark.pyspark.driver.python
    # overrides it back to a path that exists in airflow-scheduler.
    #
    # Harmless for jobs with no Python UDFs (e.g. weather): these confs are
    # only consulted when a Python worker subprocess actually gets spawned.
    "spark.pyspark.python": "/usr/local/bin/python3.11",
    "spark.pyspark.driver.python": "python3",
}
