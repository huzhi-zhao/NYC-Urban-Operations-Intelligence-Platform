"""
Starts/stops the long-running weather_producer.py as a detached background
process, rather than running it directly inside an Airflow task.

weather_producer.py loops forever (produces a message every 5s). If it ran
as a normal PythonOperator/BashOperator, the task -- and the worker slot it
occupies -- would never finish. Instead, "start" launches it with
subprocess.Popen and returns immediately once the PID is recorded; "stop"
reads that PID and sends SIGTERM. The producer process itself keeps running
in the container's OS process table, independent of any Airflow task run,
until explicitly stopped.

Trigger `start_weather_producer` once to begin producing, and
`stop_weather_producer` to stop. Both are schedule=None / manual-trigger only.
"""

import os
import signal
import subprocess
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

PRODUCER_SCRIPT = "/opt/airflow/plugins/scripts/weather_producer.py"
PID_FILE = "/tmp/weather_producer.pid"


def start_producer(**context):
    if os.path.exists(PID_FILE):
        with open(PID_FILE) as f:
            pid = int(f.read().strip())
        if _process_alive(pid):
            print(f"Producer already running (pid={pid}), nothing to do.")
            return
        os.remove(PID_FILE)

    log_file = open("/opt/airflow/logs/weather_producer.out", "a")
    proc = subprocess.Popen(
        ["python", PRODUCER_SCRIPT],
        stdout=log_file,
        stderr=subprocess.STDOUT,
        start_new_session=True,  # detach from this task's process group
    )
    with open(PID_FILE, "w") as f:
        f.write(str(proc.pid))
    print(f"Started weather_producer.py with pid={proc.pid}")


def stop_producer(**context):
    if not os.path.exists(PID_FILE):
        print("No pid file found, producer is not running (as far as we know).")
        return

    with open(PID_FILE) as f:
        pid = int(f.read().strip())

    if _process_alive(pid):
        os.kill(pid, signal.SIGTERM)
        print(f"Sent SIGTERM to pid={pid}")
    else:
        print(f"pid={pid} was not running.")

    os.remove(PID_FILE)


def _process_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


with DAG(
    dag_id="start_weather_producer",
    description="Launch weather_producer.py as a detached background process",
    schedule=None,
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["weather", "kafka", "producer"],
) as start_dag:
    PythonOperator(task_id="start_producer", python_callable=start_producer)

with DAG(
    dag_id="stop_weather_producer",
    description="Stop the background weather_producer.py process",
    schedule=None,
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["weather", "kafka", "producer"],
) as stop_dag:
    PythonOperator(task_id="stop_producer", python_callable=stop_producer)
