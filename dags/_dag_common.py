"""
Shared defaults for all NYC-UOIP backfill DAGs.

Import pattern in every DAG:
    from _dag_common import DEFAULT_ARGS, backfill_params, get_bucket
"""

from __future__ import annotations

import logging
import os
from datetime import timedelta

from airflow.models.param import Param

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "nyc-uoip",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,  # set to True and configure SMTP in Composer if needed
}

# Standard Params for all date-range backfill DAGs.
# In the Airflow UI: Trigger DAG w/ Config → fill these fields.
backfill_params = {
    "start": Param(
        "2024-01-01",
        type="string",
        description="Inclusive start date (YYYY-MM-DD)",
        format="date",
    ),
    "end": Param(
        "2025-01-01",
        type="string",
        description="Exclusive end date (YYYY-MM-DD)",
        format="date",
    ),
    "bucket": Param(
        "",
        type=["string", "null"],
        description="GCS bucket name. Empty = use GCS_BUCKET_NAME env var.",
    ),
}


def get_bucket(params) -> str:
    """Resolve GCS bucket from DAG Param or GCS_BUCKET_NAME env var."""
    bucket = (params.get("bucket") or "").strip()
    if not bucket:
        bucket = os.environ.get("GCS_BUCKET_NAME", "").strip()
    if not bucket:
        raise ValueError(
            "GCS bucket not set. Pass 'bucket' Param when triggering the DAG "
            "or set the GCS_BUCKET_NAME environment variable in the Composer environment."
        )
    return bucket
