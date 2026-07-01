import os
import shutil
import pandas as pd
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
"""
这是PACE课堂MINI Project作业
"""
SNOWFLAKE_CONN_ID = "snowflake_default"
STAGE_NAME = "weather_stage_af"
RAW_TABLE = "weather_raw_af"
CLEAN_TABLE = "weather_clean_af"


def extract(**context):
    source_dir = Variable.get("weather_source_dir")
    work_dir = Variable.get("weather_work_dir")
    source_path = os.path.join(source_dir, "weather_raw.csv")
    extracted_path = os.path.join(work_dir, "weather_extracted.csv")

    os.makedirs(work_dir, exist_ok=True)
    shutil.copy(source_path, extracted_path)
    context["ti"].xcom_push(key="extracted_path", value=extracted_path)


def transform(**context):
    work_dir = Variable.get("weather_work_dir")
    extracted_path = context["ti"].xcom_pull(task_ids="extract", key="extracted_path")
    cleaned_path = os.path.join(work_dir, "weather_cleaned.csv")

    df = pd.read_csv(extracted_path, dtype={"temperature_2m": "float64"})
    df = df.dropna(subset=["temperature_2m"])
    df["temperature_2m_f"] = df["temperature_2m"] * 9 / 5 + 32

    df.to_csv(cleaned_path, index=False)
    context["ti"].xcom_push(key="cleaned_path", value=cleaned_path)


def load(**context):
    cleaned_path = context["ti"].xcom_pull(task_ids="transform", key="cleaned_path")

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(f"PUT file://{cleaned_path} @{STAGE_NAME} AUTO_COMPRESS=TRUE OVERWRITE=TRUE")
        cursor.execute(f"""
            COPY INTO {RAW_TABLE} (time, temperature_2m)
            FROM (
                SELECT $1, $2
                FROM @{STAGE_NAME}/{os.path.basename(cleaned_path)}.gz
            )
            FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 NULL_IF = ('', 'NULL'))
            ON_ERROR = 'ABORT_STATEMENT'
        """)
        cursor.execute(f"""
            INSERT INTO {CLEAN_TABLE} (time, temperature_2m, temperature_2m_f)
            SELECT time, temperature_2m, temperature_2m * 9/5 + 32
            FROM {RAW_TABLE}
        """)
    finally:
        cursor.close()
        conn.close()


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="weather_etl_to_snowflake",
    default_args=default_args,
    description="Extract weather_raw.csv, clean it, load into Snowflake",
    schedule_interval="@daily",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["weather", "snowflake"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load,
    )

    extract_task >> transform_task >> load_task
