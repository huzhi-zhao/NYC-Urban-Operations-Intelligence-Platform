"""Open-Meteo fetcher — converts start/end to past_days/forecast_days."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import date
from typing import Any

import requests

from ingestion.backfill.fetchers.base import Fetcher
from ingestion.config import DatasetConfig

logger = logging.getLogger(__name__)

# Open-Meteo limits as of 2026-06-13 — see https://open-meteo.com/en/docs
MAX_PAST_DAYS = 92
MAX_FORECAST_DAYS = 16


def _window_to_past_forecast(
    start: date,
    end: date,
    today: date,
) -> tuple[int, int]:
    """Translate a ``[start, end)`` window to ``(past_days, forecast_days)``.

    - If the window is entirely in the past, ``forecast_days = 0``.
    - If entirely in the future, ``past_days = 0``.
    - If it straddles today, both are > 0.
    """
    if end <= today:
        past = (end - start).days
        return past, 0
    if start >= today:
        forecast = (end - start).days
        return 0, forecast
    return (today - start).days, (end - today).days


class OpenMeteoFetcher(Fetcher):
    """Fetch Open-Meteo data for the ``[start, end)`` window.

    Open-Meteo's API takes ``past_days`` and ``forecast_days`` query params
    (not arbitrary start/end). This fetcher translates the caller's
    ``[start, end)`` window into those two values, relative to today.
    """

    def __init__(self, ds: DatasetConfig, start: date, end: date) -> None:
        if not ds.endpoint:
            raise ValueError(
                f"Open-Meteo dataset {ds.name!r} missing endpoint",
            )
        self.endpoint = ds.endpoint
        self.query_params = dict(ds.query_params or {})
        self.start = start
        self.end = end
        self.dataset_name = ds.name

    def fetch(self) -> Iterator[dict[str, Any]]:
        from datetime import date as _date

        past_days, forecast_days = _window_to_past_forecast(
            self.start, self.end, today=_date.today(),
        )
        if past_days > MAX_PAST_DAYS:
            raise ValueError(
                f"Window starts {past_days} days before today; "
                f"Open-Meteo allows at most {MAX_PAST_DAYS} past_days",
            )
        if forecast_days > MAX_FORECAST_DAYS:
            raise ValueError(
                f"Window ends {forecast_days} days after today; "
                f"Open-Meteo allows at most {MAX_FORECAST_DAYS} forecast_days",
            )

        params = dict(self.query_params)
        params["past_days"] = past_days
        params["forecast_days"] = forecast_days

        logger.info(
            "Open-Meteo fetch: dataset=%s window=[%s, %s) "
            "-> past_days=%d forecast_days=%d",
            self.dataset_name, self.start, self.end, past_days, forecast_days,
        )

        resp = requests.get(self.endpoint, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        # Open-Meteo returns {"hourly": {"time": [...], "temperature_2m": [...], ...}}
        # Flatten into one record per hour.
        hourly = data.get("hourly", {})
        times = hourly.get("time", [])
        for i, t in enumerate(times):
            record: dict[str, Any] = {"time": t}
            for k, v in hourly.items():
                if k == "time" or not isinstance(v, list):
                    continue
                if i < len(v):
                    record[k] = v[i]
            yield record
