"""
Monthly incremental ingest DAG for SRC-NYPD (NYPD Public Safety).

Schedule  : 06:00 UTC on the 1st of every month
Window    : last calendar month (data_interval_start month)
Strategy  : monthly partition (one Socrata query per dataset per month, 4 datasets)

For a run triggered on 2026-06-01:
    data_interval_start = 2026-05-01
    → fetches May 2026 data for all 4 NYPD datasets
    → writes to bronze/raw/SRC-NYPD/{dataset}/data_2026-05.json

Idempotent: re-triggering the same DAG Run overwrites the same GCS objects.
"""

from __future__ import annotations

import logging

from airflow import DAG
from airflow.operators.python import PythonOperator

from _dag_common import DEFAULT_ARGS, get_bucket, get_last_month

logger = logging.getLogger(__name__)

SOURCE_ID = "SRC-NYPD"


def _run_ingest(**context) -> None:
    from scripts.backfill.bulk import backfill_monthly_window

    month_start, month_end = get_last_month(context)
    bucket = get_bucket({})

    logger.info("%s incremental ingest: month=[%s, %s)", SOURCE_ID, month_start, month_end)

    results = backfill_monthly_window(SOURCE_ID, start=month_start, end=month_end, bucket=bucket)

    failed = [r for r in results if r.status == "failed"]
    total_records = sum(r.manifest_count for r in results if r.status == "ok")
    logger.info(
        "%s: %d dataset shards written, %d total records, %d failures",
        SOURCE_ID, len(results), total_records, len(failed),
    )
    if failed:
        for r in failed:
            logger.error("  FAILED month=%s dataset=%s: %s", r.document, getattr(r, "dataset", "?"), r.error)
        raise RuntimeError(f"{len(failed)} shard(s) failed for {SOURCE_ID}.")


with DAG(
    dag_id="dag_ingest_nypd",
    description="Monthly incremental: NYPD Public Safety (4 datasets) → GCS Bronze",
    default_args=DEFAULT_ARGS,
    schedule="0 6 1 * *",
    catchup=False,
    tags=["ingest", "nypd", "bronze", "socrata", "monthly"],
) as dag:

    run_ingest = PythonOperator(
        task_id="run_ingest",
        python_callable=_run_ingest,
    )
