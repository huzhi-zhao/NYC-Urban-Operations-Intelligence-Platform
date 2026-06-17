"""
Daily incremental ingest DAG for SRC-NYC-311 (311 Service Requests).

Schedule  : 06:00 UTC every day
Window    : data_interval_start.date() — i.e. yesterday's data
Strategy  : daily partition (one Socrata query, one GCS file per day)

The DAG pulls yesterday's 311 records and writes them to:
    bronze/raw/SRC-NYC-311/nyc_311/{YYYY-MM}/data_{YYYY-MM-DD}.json

Idempotent: re-triggering the same DAG Run overwrites the same GCS objects.
"""

from __future__ import annotations

import logging
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from _dag_common import DEFAULT_ARGS, get_bucket, get_yesterday

logger = logging.getLogger(__name__)

SOURCE_ID = "SRC-NYC-311"

# 7-day lookback: on failures, re-fetch the last 7 days to catch late-arriving records
LOOKBACK_DAYS = 7


def _run_ingest(**context) -> None:
    from scripts.backfill.bulk import backfill_daily_window

    target_date = get_yesterday(context)
    # Lookback window: [target_date - 6 days, target_date + 1 day)
    # Ensures late-arriving 311 records (updated up to 7 days after creation) are captured.
    start = target_date - timedelta(days=LOOKBACK_DAYS - 1)
    end = target_date + timedelta(days=1)
    bucket = get_bucket({})

    logger.info("%s incremental ingest: target_date=%s, window=[%s, %s)", SOURCE_ID, target_date, start, end)

    results = backfill_daily_window(SOURCE_ID, start=start, end=end, bucket=bucket)

    failed = [r for r in results if r.status == "failed"]
    total_records = sum(r.manifest_count for r in results if r.status == "ok")
    logger.info(
        "%s: %d days written, %d total records, %d failures",
        SOURCE_ID, len(results), total_records, len(failed),
    )
    if failed:
        for r in failed:
            logger.error("  FAILED day=%s: %s", r.document, r.error)
        raise RuntimeError(f"{len(failed)} day(s) failed for {SOURCE_ID}.")


with DAG(
    dag_id="dag_ingest_nyc_311",
    description="Daily incremental: NYC 311 Service Requests → GCS Bronze (7-day lookback window)",
    default_args=DEFAULT_ARGS,
    schedule="0 6 * * *",
    catchup=False,
    tags=["ingest", "nyc-311", "bronze", "socrata", "daily"],
) as dag:

    run_ingest = PythonOperator(
        task_id="run_ingest",
        python_callable=_run_ingest,
    )
