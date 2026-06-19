# ==========================================
# Hello World demo DAG — TaskFlow API, Airflow 3.x style
# Demonstrates: dynamic task mapping (the Airflow way to "loop"),
#               branching, and a fan-in join.
# Reference:
#   https://airflow.apache.org/docs/apache-airflow/3.2.2/tutorial/taskflow.html
#   https://airflow.apache.org/docs/apache-airflow/3.2.2/authoring-and-scheduling/dynamic-task-mapping.html
# ==========================================
from datetime import datetime, timedelta

from airflow.sdk import DAG, task
from airflow.providers.standard.operators.empty import EmptyOperator

default_args = {
    "owner": "Temi",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="hello_airflow_empty",
    default_args=default_args,
    description="Hello-world demo DAG: loop (dynamic task mapping) + branch + join",
    schedule="@daily",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["demo", "hello-world", "empty"],
) as dag:

    @task
    def extract_data() -> list[int]:
        items = [1, 2, 3, 4, 5]
        print(f"Extracting data... {items}")
        return items

    # Airflow DAGs must stay acyclic — there is no "for" loop that jumps
    # back to an earlier task. The idiomatic way to repeat work over a
    # list is dynamic task mapping: .expand() spins up one task instance
    # per item, all running in parallel.
    @task
    def clean_item(item: int) -> int:
        cleaned = item * 10
        print(f"Cleaning item {item} -> {cleaned}")
        return cleaned

    @task
    def sum_cleaned(values: list[int]) -> int:
        total = sum(values)
        print(f"Cleaned total: {total}")
        return total

    @task.branch
    def check_quality(total: int) -> str:
        if total >= 100:
            return "transform_data"
        return "skip_transform"

    @task
    def transform_data(total: int) -> None:
        print(f"Transforming data... total={total}")

    @task
    def skip_transform() -> None:
        print("Data quality too low — skipping transform")

    # Branches converge here. Default trigger rule (all_success) would
    # fail this join because the unchosen branch is "skipped", not
    # "success" — none_failed_min_one_success accepts that.
    join = EmptyOperator(task_id="join", trigger_rule="none_failed_min_one_success")

    @task
    def load_data() -> None:
        print("Loading data...")

    extracted = extract_data()
    cleaned = clean_item.expand(item=extracted)
    total = sum_cleaned(cleaned)
    branch = check_quality(total)
    branch >> [transform_data(total), skip_transform()] >> join >> load_data()
