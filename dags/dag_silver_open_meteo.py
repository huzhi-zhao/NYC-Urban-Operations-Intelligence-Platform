"""
Daily Bronze -> Silver Spark job for SRC-Open-Meteo (hourly weather).

Schedule        : 07:00 UTC every day — 1 hour after dag_ingest_open_meteo (06:00 UTC),
                  so the previous day's Bronze files are confirmed before Silver reads them.
Engine          : standalone Spark cluster (spark-master:7077) on this host, deploy-mode
                  client — driver runs inside the Airflow container via SparkSubmitOperator,
                  using the spark_default connection (see infra/docker/docker-compose.yml).
Storage         : unchanged — Bronze and Silver both live on GCS. Only the compute engine
                  moved off Dataproc.
Catchup         : enabled — missed days are auto-backfilled on scheduler restart.
max_active_runs : 1 — one Spark job at a time is enough for this data volume.

Infra prerequisites this DAG depends on (outside this repo's docker-compose.yml):
  - spark-master / spark-worker containers already on the `bigdata-net` network.
  - spark-worker must mount the same GCS service-account key at
    /opt/airflow/keys/nyc-uoip-sa-key.json (executors read/write GCS too).
"""

from __future__ import annotations

from datetime import timedelta

from _dag_common import DEFAULT_ARGS, get_bucket
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

GCS_KEY_PATH = "/opt/airflow/keys/nyc-uoip-sa-key.json"
GCS_CONNECTOR_PACKAGE = "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.21"

SPARK_CONF = {
    "spark.hadoop.fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
    "spark.hadoop.fs.AbstractFileSystem.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
    "spark.hadoop.google.cloud.auth.service.account.json.keyfile": GCS_KEY_PATH,
}

try:
    _DEFAULT_BUCKET = get_bucket({})
except ValueError:
    # GCS_BUCKET_NAME not set in this environment yet — let the DAG parse anyway;
    # triggering a run with an empty bucket will fail visibly inside the Spark job.
    _DEFAULT_BUCKET = ""

with DAG(
    dag_id="dag_silver_open_meteo",
    description="Daily Bronze -> Silver: Open-Meteo weather, via spark-submit on the local Spark cluster",
    default_args=DEFAULT_ARGS,
    schedule="0 7 * * *",
    catchup=True,
    max_active_runs=1,
    tags=["silver", "open-meteo", "spark", "weather", "daily"],
) as dag:

    run_silver_etl = SparkSubmitOperator(
        task_id="run_silver_etl",
        application="/opt/airflow/plugins/spark/jobs/etl_open_meteo.py",
        conn_id="spark_default",
        packages=GCS_CONNECTOR_PACKAGE,
        conf=SPARK_CONF,
        application_args=[
            "--bucket",
            _DEFAULT_BUCKET,
            # data_interval_start is the previous schedule period's start, i.e.
            # "yesterday" relative to this run's trigger day — matches the
            # get_yesterday() convention used by dag_ingest_open_meteo.
            "--execution-date",
            "{{ data_interval_start.date().isoformat() }}",
        ],
        verbose=True,
        execution_timeout=timedelta(minutes=30),
    )
