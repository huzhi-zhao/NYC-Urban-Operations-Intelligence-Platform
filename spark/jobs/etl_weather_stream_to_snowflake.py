"""
Spark Structured Streaming job: consumes the `weather_data` Kafka topic
and writes each micro-batch into the Snowflake RAW table `weather_stream_raw`.

Submitted via spark-submit (or Airflow's SparkSubmitOperator) with:
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,net.snowflake:spark-snowflake_2.12:2.16.0-spark_3.5,net.snowflake:snowflake-jdbc:3.16.1

Snowflake credentials are never hardcoded here. deploy-mode is `cluster`
(dags/dag_weather_stream_to_snowflake.py), so the driver runs on a
spark-worker container that never sees Airflow's `.env` — env vars set by
Airflow's env_file don't reach it. Credentials are instead passed as
namespaced `spark.mini_pro.sf.*` values in the SparkConf, which spark-submit
ships to the remote driver along with the application regardless of deploy
mode (see SPARK_CONF in dags/_spark_common.py for the equivalent pattern
used for GCS credentials).
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"  # container name on bigdata-net (KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092)
KAFKA_TOPIC = "weather_data"
SNOWFLAKE_TABLE = "weather_stream_raw"

# spark.mini_pro.sf.* keys are set on the SparkConf by
# dags/dag_weather_stream_to_snowflake.py, sourced from the Airflow
# container's own env (AIRFLOW_CONN_SNOWFLAKE_DEFAULT / SNOWFLAKE_* in .env).
SF_CONF_KEYS = {
    "sfURL": "spark.mini_pro.sf.url",
    "sfUser": "spark.mini_pro.sf.user",
    "sfPassword": "spark.mini_pro.sf.password",
    "sfDatabase": "spark.mini_pro.sf.database",
    "sfSchema": "spark.mini_pro.sf.schema",
    "sfWarehouse": "spark.mini_pro.sf.warehouse",
    "sfRole": "spark.mini_pro.sf.role",
}


def _snowflake_options(spark: SparkSession) -> dict:
    options = {opt: spark.conf.get(conf_key) for opt, conf_key in SF_CONF_KEYS.items()}
    options["dbtable"] = SNOWFLAKE_TABLE
    return options


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
    # spark.app.name is set via --conf by dags/dag_weather_stream_to_snowflake.py
    # (APP_NAME there), not hardcoded here — that DAG also polls spark-master
    # for a running app under this exact name to dedup submissions and verify
    # the job actually started, so there's one source of truth for the name.
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
        parsed.writeStream.foreachBatch(_make_write_to_snowflake(_snowflake_options(spark)))
        .option("checkpointLocation", "gs://pace-lab-bdp-checkpoints/weather_stream")
        .trigger(processingTime="30 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
