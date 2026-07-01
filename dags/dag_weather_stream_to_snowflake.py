"""
Monitors the Kafka -> Spark Structured Streaming -> Snowflake job, which
this DAG does NOT launch.

Why not: the job runs forever (query.awaitTermination()), and Spark
standalone's cluster manager rejects Python applications in `cluster`
deploy mode outright ("Cluster deploy mode is currently not supported for
python applications on standalone clusters" — a hard Spark limitation).
That leaves only `client` deploy mode for a PySpark streaming job here,
where the submitting process IS the driver — so an Airflow task submitting
it would block for as long as the stream runs, the same problem the
producer's old subprocess.Popen + PID-file DAG had.

The job instead runs as its own long-running compose service
(weather-stream-job in infra/docker/docker-compose.yml, restart:
unless-stopped, spark-submit --deploy-mode client in the foreground) —
same pattern as weather-producer. Airflow's role here is reduced to what
it's actually good at: periodically checking the job is still alive and
raising a visible, alertable failure if it isn't.

Polls Spark standalone master's own JSON status endpoint (already exposed
on spark-master:8080 for the web UI, no extra service needed) for
APP_NAME among activeapps/activedrivers.

APP_NAME must match `--conf spark.app.name=...` in the weather-stream-job
service command in docker-compose.yml.
"""

from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator

APP_NAME = "weather_kafka_to_snowflake"
SPARK_MASTER_STATUS_URL = "http://spark-master:8080/json/"


def _spark_master_state() -> dict:
    response = requests.get(SPARK_MASTER_STATUS_URL, timeout=10)
    response.raise_for_status()
    return response.json()


def _app_is_active(state: dict) -> bool:
    """True if APP_NAME shows up as an active app or an active driver.

    Both lists are checked since activedrivers can lag activeapps right
    after a (re)start — the driver process has to come up before it
    registers a SparkContext under APP_NAME. Field access is defensive
    (.get) since the exact JSON shape isn't a stable public API.
    """
    active_app_names = {a.get("name") for a in state.get("activeapps", [])}
    active_driver_names = {
        d.get("desc", {}).get("name") for d in state.get("activedrivers", [])
    }
    return APP_NAME in active_app_names or APP_NAME in active_driver_names


def check_streaming_job_running(**context):
    if not _app_is_active(_spark_master_state()):
        raise AirflowFailException(
            f"{APP_NAME} is not an active app/driver on spark-master — the "
            "weather-stream-job compose service is likely down or crash-looping. "
            "Check `docker logs weather-stream-job` and spark-master:8080."
        )


with DAG(
    dag_id="weather_stream_to_snowflake",
    description="Monitor the Kafka -> Spark Structured Streaming -> Snowflake job",
    schedule=timedelta(minutes=10),
    start_date=datetime(2026, 6, 1),
    catchup=False,
    max_active_runs=1,
    tags=["weather", "streaming", "spark", "snowflake", "monitoring"],
) as dag:
    PythonOperator(
        task_id="check_streaming_job_running",
        python_callable=check_streaming_job_running,
    )
