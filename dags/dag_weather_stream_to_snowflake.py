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
"""

from datetime import datetime

from airflow import DAG
from airflow.models import Connection
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

SNOWFLAKE_CONN_ID = "snowflake_default"
TARGET_SCHEMA = "whether_q3"


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

    launch_streaming_job = SparkSubmitOperator(
        task_id="launch_streaming_job",
        application="/opt/airflow/plugins/spark/jobs/etl_weather_stream_to_snowflake.py",
        conn_id="spark_default",
        deploy_mode="cluster",
        conf={
            "spark.driver.supervise": "true",  # auto-restart the driver if the worker running it dies
            **_snowflake_conf(),
        },
        packages=(
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
            "net.snowflake:spark-snowflake_2.12:2.16.0-spark_3.5,"
            "net.snowflake:snowflake-jdbc:3.16.1"
        ),
    )
