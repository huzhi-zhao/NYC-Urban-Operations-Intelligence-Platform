"""
Daily incremental ingest DAG for SRC-Open-Meteo (hourly weather).

Schedule  : 06:00 UTC every day
Window    : yesterday → +8 days (covers yesterday's confirmed data + 7-day forecast)
Strategy  : daily partition, wide-fetch (1 Open-Meteo API call covers the full window;
            the facade splits the response into per-day GCS files)

For a run triggered on 2026-06-17:
    target_date = 2026-06-16  (yesterday — 24h of confirmed observations)
    fetch_end   = 2026-06-24  (7 days of forecast beyond today)
    → writes bronze/raw/SRC-Open-Meteo/nyc_weather_forecast/2026-06/data_2026-06-16.json
                                                                    ...data_2026-06-23.json

Re-running overwrites existing files, so forecast files are refreshed daily with the
latest model output (Open-Meteo updates forecasts multiple times per day).

Idempotent: same DAG Run always fetches the same logical window.
"""

from __future__ import annotations

import logging
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from _dag_common import DEFAULT_ARGS, get_bucket, get_yesterday

logger = logging.getLogger(__name__)

SOURCE_ID = "SRC-Open-Meteo"

# 1 confirmed day (yesterday) + 7 forecast days = 8 days total per run
FORECAST_DAYS = 7


def _run_ingest(**context) -> None:
    from scripts.backfill.bulk import backfill_daily_window

    target_date = get_yesterday(context)
    fetch_end = target_date + timedelta(days=1 + FORECAST_DAYS)
    bucket = get_bucket({})

    logger.info(
        "%s incremental ingest: confirmed=%s, forecast until %s, window=[%s, %s)",
        SOURCE_ID, target_date, fetch_end - timedelta(days=1), target_date, fetch_end,
    )

    # OpenMeteoFetcher automatically uses forecast API for recent/future dates
    results = backfill_daily_window(SOURCE_ID, start=target_date, end=fetch_end, bucket=bucket)

    failed = [r for r in results if r.status == "failed"]
    total_files = sum(r.manifest_count for r in results if r.status == "ok")
    logger.info(
        "%s: %d daily files written (%d confirmed + up to %d forecast), %d failures",
        SOURCE_ID, total_files, 1, FORECAST_DAYS, len(failed),
    )
    if failed:
        for r in failed:
            logger.error("  FAILED: %s", r.error)
        raise RuntimeError(f"Open-Meteo ingest failed: {failed[0].error}")


with DAG(
    dag_id="dag_ingest_open_meteo",
    description="Daily incremental: Open-Meteo weather (yesterday confirmed + 7-day forecast) → GCS Bronze",
    default_args=DEFAULT_ARGS,
    schedule="0 6 * * *",
    catchup=False,
    tags=["ingest", "open-meteo", "bronze", "weather", "daily"],
) as dag:

    run_ingest = PythonOperator(
        task_id="run_ingest",
        python_callable=_run_ingest,
    )
