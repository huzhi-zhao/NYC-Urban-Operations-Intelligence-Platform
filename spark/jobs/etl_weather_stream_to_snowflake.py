"""
Spark Structured Streaming job: consumes the `weather_data` Kafka topic
and writes each micro-batch into the Snowflake RAW table `weather_stream_raw`.

Submitted via spark-submit in `client` deploy mode:
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,net.snowflake:spark-snowflake_2.12:3.1.3,net.snowflake:snowflake-jdbc:3.16.1

Not `cluster` deploy mode: Spark standalone's cluster manager rejects
Python applications in cluster mode outright ("Cluster deploy mode is
currently not supported for python applications on standalone clusters" —
a hard Spark limitation, unrelated to any config here). client mode means
this process IS the driver, so it has to be the main process of a
long-running container, not something an Airflow task submits and waits on
— see infra/docker/docker-compose.yml's weather-stream-job service
(restart: unless-stopped), which runs this exact command in the
foreground. Airflow's role for this job is monitor-only now (see
dags/dag_weather_stream_to_snowflake.py).

Because this now runs as its own compose service rather than being
launched by Airflow, it reads Snowflake credentials directly from its own
process env via AIRFLOW_CONN_SNOWFLAKE_DEFAULT (docker-compose's env_file
loads the same .env Airflow uses) — no SparkConf indirection needed since
nothing separate is doing the submitting.
"""

import os
from urllib.parse import parse_qs, unquote, urlparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"  # container name on bigdata-net (KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092)
KAFKA_TOPIC = "weather_data"
SNOWFLAKE_TABLE = "weather_stream_raw"
TARGET_SCHEMA = "whether_q3"


def _snowflake_options() -> dict:
    """Parse AIRFLOW_CONN_SNOWFLAKE_DEFAULT (same connection URI the Q2 ETL
    DAG uses) into the discrete sfURL/sfUser/... options the Spark Snowflake
    connector needs. TARGET_SCHEMA overrides the connection's own schema so
    this always lands in whether_q3 regardless of what the shared
    connection defaults to.
    """
    uri = os.environ["AIRFLOW_CONN_SNOWFLAKE_DEFAULT"]
    parsed = urlparse(uri)
    query = parse_qs(parsed.query)
    account = query.get("account", [parsed.hostname])[0]
    return {
        "sfURL": f"{account}.snowflakecomputing.com",
        "sfUser": unquote(parsed.username or ""),
        "sfPassword": unquote(parsed.password or ""),
        "sfDatabase": query.get("database", [parsed.path.lstrip("/").split("/")[0]])[0],
        "sfSchema": TARGET_SCHEMA,
        "sfWarehouse": query.get("warehouse", ["COMPUTE_WH"])[0],
        "sfRole": query.get("role", ["ACCOUNTADMIN"])[0],
        "dbtable": SNOWFLAKE_TABLE,
        # weather_stream_raw has a 6th column, ingested_at, with a
        # DEFAULT CURRENT_TIMESTAMP() that this job deliberately never
        # writes (see write_to_snowflake below). The connector's default
        # column_mapping="order" requires the staged file's column count to
        # match the table exactly (5 vs 6 -> "Number of columns in file
        # does not match that of the corresponding table"), so writes have
        # to be matched by name instead — that also lets the DEFAULT fire
        # for the column this DataFrame doesn't supply.
        "column_mapping": "name",
    }


schema = (
    StructType()
    .add("city", StringType())
    .add("temperature", DoubleType())
    .add("humidity", IntegerType())
    .add("wind_speed", DoubleType())
)


def _make_write_to_snowflake(sf_options: dict):
    def write_to_snowflake(batch_df, batch_id):
        """foreachBatch sink: one JDBC write per micro-batch."""
        if batch_df.rdd.isEmpty():
            return
        (
            batch_df.write.format("net.snowflake.spark.snowflake")
            .options(**sf_options)
            .mode("append")
            .save()
        )

    return write_to_snowflake


def main():
    # spark.app.name is set via --conf in the weather-stream-job service
    # command (docker-compose.yml) — dags/dag_weather_stream_to_snowflake.py
    # polls spark-master for a running app under that exact name, so keep
    # both in sync if it ever changes.
    spark = (
        SparkSession.builder.config(
            "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
            "/opt/airflow/keys/pace-lab-bdp-sa-key.json",
        ).getOrCreate()
    )

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = (
        raw_stream.selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), schema).alias("data"))
        .select("data.*")
        .withColumn("event_time", current_timestamp())
    )

    query = (
        parsed.writeStream.foreachBatch(_make_write_to_snowflake(_snowflake_options()))
        .option("checkpointLocation", "gs://pace-lab-bdp-checkpoints/weather_stream")
        .trigger(processingTime="30 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
