# ==========================================
# Hello World demo DAG — PythonOperator, Airflow 3.x style
# Reference: https://airflow.apache.org/docs/apache-airflow/3.2.2/tutorial/fundamentals.html
# ==========================================
from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator


def extract_data():
    print("Extracting data...")


def clean_data():
    print("Cleaning data...")


def transform_data():
    print("Transforming data...")


def load_data():
    print("Loading data...")


default_args = {
    "owner": "Temi",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="hello_airflow_python",
    default_args=default_args,
    description="Hello-world demo DAG using PythonOperator",
    schedule="@daily",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["demo", "hello-world", "python-operator"],
) as dag:

    extract_task = PythonOperator(
        task_id="data_extraction",
        python_callable=extract_data,
    )

    cleaning_task = PythonOperator(
        task_id="data_cleaning",
        python_callable=clean_data,
    )

    transform_task = PythonOperator(
        task_id="data_transformation",
        python_callable=transform_data,
    )

    loading_task = PythonOperator(
        task_id="data_loading",
        python_callable=load_data,
    )

    extract_task >> cleaning_task >> transform_task >> loading_task
