# ==========================================
# Hello World demo DAG — Airflow 3.x style
# Reference: https://airflow.apache.org/docs/apache-airflow/3.2.2/tutorial/fundamentals.html
# ==========================================
from datetime import datetime, timedelta

# Airflow 3.x: DAG comes from the Task SDK, not the `airflow` package directly.
from airflow.sdk import DAG
# Airflow 3.x: core operators (Bash, Python, Empty, ...) moved into the
# "standard" provider package, bundled by default in the apache/airflow image.
from airflow.providers.standard.operators.bash import BashOperator

default_args = {
    "owner": "James Zhao",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="hello_airflow",
    default_args=default_args,
    description="Hello-world demo DAG using BashOperator",
    schedule=timedelta(days=1),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["demo", "hello-world"],
) as dag:

    extract = BashOperator(
        task_id="extract",
        bash_command='echo "extract work"',
    )

    transform = BashOperator(
        task_id="transform",
        bash_command='echo "transform work"',
    )

    load = BashOperator(
        task_id="load",
        bash_command='echo "load work"',
    )

    extract >> transform >> load
