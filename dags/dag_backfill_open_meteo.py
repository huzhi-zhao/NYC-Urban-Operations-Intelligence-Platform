"""
Backfill DAG for SRC-Open-Meteo (hourly weather history).

Partition strategy : daily, wide-fetch (1 Open-Meteo API call covers the entire window;
                     the response is split into per-day files by the facade)
Trigger            : manual only (schedule=None)
Params             : start (inclusive), end (exclusive), bucket

Open-Meteo API constraint: past_days ≤ 92 (free tier). Large windows are
automatically split into ≤92-day chunks and executed sequentially.

Trigger example:
    {"start": "2024-01-01", "end": "2026-06-16", "bucket": "nyc-uoip-prod"}
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from _dag_common import DEFAULT_ARGS, backfill_params, get_bucket

logger = logging.getLogger(__name__)

SOURCE_ID = "SRC-Open-Meteo"
_CHUNK_DAYS = 90  # safely under the 92-day API limit


def _check_params(**context) -> None:
    params = context["params"]
    start = datetime.strptime(params["start"], "%Y-%m-%d").date()
    end = datetime.strptime(params["end"], "%Y-%m-%d").date()
    if end <= start:
        raise ValueError(f"end ({end}) must be after start ({start})")
    get_bucket(params)
    window_days = (end - start).days
    chunks = (window_days + _CHUNK_DAYS - 1) // _CHUNK_DAYS
    logger.info(
        "%s backfill: [%s, %s) = %d days, will run %d chunk(s) of ≤%d days",
        SOURCE_ID, start, end, window_days, chunks, _CHUNK_DAYS,
    )


def _run_backfill(**context) -> None:
    from scripts.backfill.bulk import backfill_daily_window

    params = context["params"]
    start = datetime.strptime(params["start"], "%Y-%m-%d").date()
    end = datetime.strptime(params["end"], "%Y-%m-%d").date()
    bucket = get_bucket(params)

    # Split large windows into ≤90-day chunks (Open-Meteo past_days ≤ 92).
    cursor = start
    all_results = []
    while cursor < end:
        chunk_end = min(cursor + timedelta(days=_CHUNK_DAYS), end)
        logger.info("Fetching chunk [%s, %s)", cursor, chunk_end)
        results = backfill_daily_window(SOURCE_ID, start=cursor, end=chunk_end, bucket=bucket)
        all_results.extend(results)
        cursor = chunk_end

    failed = [r for r in all_results if r.status == "failed"]
    if failed:
        for r in failed:
            logger.error("  FAILED: %s", r.error)
        raise RuntimeError(f"Open-Meteo fetch failed: {failed[0].error}")

    total_files = sum(r.manifest_count for r in all_results)
    logger.info("%s: %d daily files written to GCS Bronze", SOURCE_ID, total_files)


with DAG(
    dag_id="dag_backfill_open_meteo",
    description="One-time backfill: Open-Meteo weather history → GCS Bronze (daily partition, wide-fetch)",
    default_args=DEFAULT_ARGS,
    schedule=None,
    catchup=False,
    params=backfill_params,
    tags=["backfill", "open-meteo", "bronze", "weather"],
) as dag:

    check_params = PythonOperator(
        task_id="check_params",
        python_callable=_check_params,
    )

    run_backfill = PythonOperator(
        task_id="run_backfill",
        python_callable=_run_backfill,
        execution_timeout=None,
    )

    check_params >> run_backfill
