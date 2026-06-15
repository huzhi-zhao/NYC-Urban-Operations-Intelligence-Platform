"""Socrata fetcher — paginated fetch with timestamp-window filter."""

from __future__ import annotations

import logging
import os
from collections.abc import Iterator
from datetime import date, datetime
from typing import Any

from ingestion.backfill.fetchers.base import Fetcher
from ingestion.clients.socrata_client import SocrataClient, SocrataFetchError
from ingestion.config import DatasetConfig

logger = logging.getLogger(__name__)


class SocrataFetcher(Fetcher):
    """Fetch Socrata dataset records in the ``[start, end)`` timestamp window."""

    def __init__(self, ds: DatasetConfig, start: date, end: date) -> None:
        if not ds.resource_id or not ds.domain:
            raise ValueError(
                f"Socrata dataset {ds.name!r} missing resource_id/domain",
            )
        if not ds.timestamp_field:
            raise ValueError(
                f"Socrata dataset {ds.name!r} missing timestamp_field",
            )

        app_token = os.environ.get("SOCRATA_APP_TOKEN") or None
        self.client = SocrataClient(
            resource_id=ds.resource_id,
            domain=ds.domain,
            app_token=app_token,
        )
        self.timestamp_field = ds.timestamp_field
        self.start = start
        self.end = end
        self.dataset_name = ds.name

    def fetch(self) -> Iterator[dict[str, Any]]:
        start_dt = datetime.combine(self.start, datetime.min.time())
        end_dt = datetime.combine(self.end, datetime.min.time())
        logger.info(
            "Socrata fetch: dataset=%s field=%s window=[%s, %s)",
            self.dataset_name, self.timestamp_field, self.start, self.end,
        )
        try:
            yield from self.client.fetch_all_paginated(
                timestamp_field=self.timestamp_field,
                start_dt=start_dt,
                end_dt=end_dt,
            )
        except SocrataFetchError as e:
            raise RuntimeError(
                f"Socrata fetch failed for {self.dataset_name!r} "
                f"[{self.start}, {self.end}): {e}",
            ) from e
