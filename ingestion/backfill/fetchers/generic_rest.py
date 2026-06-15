"""Generic REST fetcher — fallback for any REST API returning JSON."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import date
from typing import Any

import requests

from ingestion.backfill.fetchers.base import Fetcher
from ingestion.config import DatasetConfig

logger = logging.getLogger(__name__)


class GenericRestFetcher(Fetcher):
    """Generic REST GET fetcher.

    Passes ``start``/``end`` as ``start_date``/``end_date`` query params if
    not already set in the dataset's configured ``query_params``. Subclass
    this for REST APIs that need custom request shapes.
    """

    def __init__(self, ds: DatasetConfig, start: date, end: date) -> None:
        if not ds.endpoint:
            raise ValueError(
                f"Generic REST dataset {ds.name!r} missing endpoint",
            )
        self.endpoint = ds.endpoint
        self.query_params = dict(ds.query_params or {})
        self.start = start
        self.end = end
        self.dataset_name = ds.name

    def fetch(self) -> Iterator[dict[str, Any]]:
        params = dict(self.query_params)
        params.setdefault("start_date", self.start.isoformat())
        params.setdefault("end_date", self.end.isoformat())

        logger.info(
            "Generic REST fetch: dataset=%s url=%s params=%s",
            self.dataset_name, self.endpoint, params,
        )

        resp = requests.get(self.endpoint, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list):
            yield from data
        else:
            yield data
