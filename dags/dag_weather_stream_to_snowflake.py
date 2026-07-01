"""
Submits the long-running weather_stream_to_snowflake.py Structured Streaming
job to the standalone Spark cluster in `cluster` deploy mode.

Deploy mode is `cluster` (not the `client` mode used by the batch DAGs in
this repo) because the job runs forever via query.awaitTermination() — in
client mode the submitting process (and therefore this Airflow task) would
block for as long as the streaming job runs. In cluster mode, spark-submit
hands the driver off to a worker and returns immediately, so this task
finishes in seconds while the streaming job keeps running independently on
the Spark cluster.

schedule=None: this DAG is meant to be triggered manually/once to (re)launch
the streaming job, not run on a recurring schedule like the batch DAGs.

Snowflake credentials: the job's driver runs on a spark-worker container in
`cluster` deploy mode, which never sees Airflow's `.env` (env_file is only
wired into airflow-common in docker-compose.yml). SparkSubmitOperator's
`conf` dict, however, is shipped to the remote driver as part of the
SparkConf regardless of deploy mode — so we resolve the ONE existing
`snowflake_default` Airflow connection here (the same
AIRFLOW_CONN_SNOWFLAKE_DEFAULT used by the Q2 ETL DAG) and pass its parts
through as `spark.mini_pro.sf.*` conf keys. The Spark Snowflake connector
needs discrete sfURL/sfUser/... options rather than a single URI, which is
why we split the connection apart here rather than forwarding the URI.
`spark/jobs/etl_weather_stream_to_snowflake.py` reads them via
spark.conf.get().

TARGET_SCHEMA overrides the connection's default schema so the stream lands
in the Q3 schema (whether_q3) regardless of what the shared connection's
default schema is set to.

Dedup + monitoring: SparkSubmitOperator in cluster mode returns as soon as
spark-submit hands the driver off — Airflow doesn't otherwise know or care
whether the streaming job ends up actually running, and nothing stops you
from triggering this DAG twice and getting two concurrent queries writing
to the same Snowflake table. Two extra tasks close that gap by polling the
Spark standalone master's own JSON status endpoint (no extra service to
run — it's already exposed on spark-master:8080 for the web UI):
  - check_not_already_running: before submitting, skip the whole run
    (AirflowSkipException, not a failure) if APP_NAME is already an
    active app or driver on the cluster.
  - verify_job_running: after submitting, poll until APP_NAME shows up as
    active, or fail the task if it never does within VERIFY_TIMEOUT_SECONDS
    — this is what makes "monitors" in the architecture diagram real,
    instead of the task just declaring success the moment spark-submit
    returns.
"""

import time
from datetime import datetime

import requests
from airflow import DAG
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models import Connection
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

SNOWFLAKE_CONN_ID = "snowflake_default"
TARGET_SCHEMA = "whether_q3"

# Passed to the job below as the `spark.app.name` conf — this is the one
# place the name is defined; spark/jobs/etl_weather_stream_to_snowflake.py
# takes it from SparkConf rather than hardcoding it too.
APP_NAME = "weather_kafka_to_snowflake"
SPARK_MASTER_STATUS_URL = "http://spark-master:8080/json/"
VERIFY_TIMEOUT_SECONDS = 120
VERIFY_POLL_INTERVAL_SECONDS = 10


def _spark_master_state() -> dict:
    response = requests.get(SPARK_MASTER_STATUS_URL, timeout=10)
    response.raise_for_status()
    return response.json()


def _app_is_active(state: dict) -> bool:
    """True if APP_NAME shows up as an active app or an active driver.

    activedrivers can lag behind activeapps right after submission (the
    driver process has to start before it registers a SparkContext under
    APP_NAME), so both lists are checked. Field access is defensive
    (.get) since the exact JSON shape isn't a stable public API.
    """
    active_app_names = {a.get("name") for a in state.get("activeapps", [])}
    active_driver_names = {
        d.get("desc", {}).get("name") for d in state.get("activedrivers", [])
    }
    return APP_NAME in active_app_names or APP_NAME in active_driver_names


def check_not_already_running(**context):
    if _app_is_active(_spark_master_state()):
        raise AirflowSkipException(
            f"{APP_NAME} is already an active app/driver on spark-master — "
            "skipping resubmission to avoid duplicate concurrent streams."
        )


def verify_job_running(**context):
    deadline = time.time() + VERIFY_TIMEOUT_SECONDS
    while time.time() < deadline:
        if _app_is_active(_spark_master_state()):
            return
        time.sleep(VERIFY_POLL_INTERVAL_SECONDS)
    raise AirflowFailException(
        f"{APP_NAME} did not appear as an active app/driver on spark-master "
        f"within {VERIFY_TIMEOUT_SECONDS}s of submission — the driver likely "
        "failed to start. Check spark-master:8080 and spark-worker logs."
    )


def _snowflake_conf() -> dict:
    """Resolve the shared snowflake_default connection into Spark connector conf.

    get_connection_from_secrets reads the AIRFLOW_CONN_SNOWFLAKE_DEFAULT env
    var backend directly — no metadata-DB round trip at DAG-parse time.
    """
    conn = Connection.get_connection_from_secrets(SNOWFLAKE_CONN_ID)
    extra = conn.extra_dejson
    account = extra.get("account") or conn.host
    return {
        "spark.mini_pro.sf.url": f"{account}.snowflakecomputing.com",
        "spark.mini_pro.sf.user": conn.login,
        "spark.mini_pro.sf.password": conn.password,
        "spark.mini_pro.sf.database": extra.get("database") or conn.schema,
        "spark.mini_pro.sf.schema": TARGET_SCHEMA,
        "spark.mini_pro.sf.warehouse": extra.get("warehouse", "COMPUTE_WH"),
        "spark.mini_pro.sf.role": extra.get("role", "ACCOUNTADMIN"),
    }


with DAG(
    dag_id="weather_stream_to_snowflake",
    description="Launch the Kafka -> Spark Structured Streaming -> Snowflake RAW job",
    schedule=None,
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["weather", "streaming", "spark", "snowflake"],
) as dag:

    check_not_already_running = PythonOperator(
        task_id="check_not_already_running",
        python_callable=check_not_already_running,
    )

    launch_streaming_job = SparkSubmitOperator(
        task_id="launch_streaming_job",
        application="/opt/airflow/plugins/spark/jobs/etl_weather_stream_to_snowflake.py",
        conn_id="spark_default",
        deploy_mode="cluster",
        conf={
            "spark.app.name": APP_NAME,
            "spark.driver.supervise": "true",  # auto-restart the driver if the worker running it dies
            **_snowflake_conf(),
        },
        packages=(
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
            "net.snowflake:spark-snowflake_2.12:2.16.0-spark_3.5,"
            "net.snowflake:snowflake-jdbc:3.16.1"
        ),
    )

    verify_job_running = PythonOperator(
        task_id="verify_job_running",
        python_callable=verify_job_running,
    )

    check_not_already_running >> launch_streaming_job >> verify_job_running
