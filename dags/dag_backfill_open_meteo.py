"""
Backfill DAG for SRC-Open-Meteo (hourly weather history).

Partition strategy : daily, wide-fetch (1 Open-Meteo API call covers the entire window;
                     the response is split into per-day files by the facade)
Trigger            : manual only (schedule=None)
Params             : start (inclusive), end (exclusive), bucket

Open-Meteo API constraint: the API expresses date ranges as relative offsets
(past_days / forecast_days from today), not arbitrary calendar dates. Fetching
very old history requires large past_days values. Keep windows ≤ 365 days per run.

Trigger example:
    {"start": "2024-01-01", "end": "2025-01-01", "bucket": "nyc-uoip"}
"""

from __future__ import annotations

import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from _dag_common import DEFAULT_ARGS, backfill_params, get_bucket

logger = logging.getLogger(__name__)

SOURCE_ID = "SRC-Open-Meteo"

_MAX_WINDOW_DAYS = 365


def _check_params(**context) -> None:
    params = context["params"]
    start = datetime.strptime(params["start"], "%Y-%m-%d").date()
    end = datetime.strptime(params["end"], "%Y-%m-%d").date()
    if end <= start:
        raise ValueError(f"end ({end}) must be after start ({start})")
    window_days = (end - start).days
    if window_days > _MAX_WINDOW_DAYS:
        raise ValueError(
            f"Open-Meteo window too large ({window_days} days). "
            f"Split into ≤{_MAX_WINDOW_DAYS}-day runs."
        )
    get_bucket(params)
    logger.info("%s backfill: [%s, %s) (%d days, 1 API call)", SOURCE_ID, start, end, window_days)


def _run_backfill(**context) -> None:
    from scripts.backfill.bulk import backfill_daily_window

    params = context["params"]
    start = datetime.strptime(params["start"], "%Y-%m-%d").date()
    end = datetime.strptime(params["end"], "%Y-%m-%d").date()
    bucket = get_bucket(params)

    # Wide-fetch: bulk dispatches to facade.upload_window(start, end) — 1 API call total.
    results = backfill_daily_window(SOURCE_ID, start=start, end=end, bucket=bucket)

    failed = [r for r in results if r.status == "failed"]
    if failed:
        for r in failed:
            logger.error("  FAILED: %s", r.error)
        raise RuntimeError(f"Open-Meteo fetch failed: {failed[0].error}")

    total_files = sum(r.manifest_count for r in results)
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
