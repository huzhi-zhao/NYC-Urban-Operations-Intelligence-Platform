"""
Shared CLI helpers for per-source backfill scripts.

Per-source scripts accept a ``[start, end)`` window and delegate to
``scripts.backfill.bulk``. The bulk layer splits the window into
day-sized or month-sized chunks and calls the atomic ``BackfillFacade``
methods.
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, datetime


def parse_date(s: str) -> date:
    """Parse ``YYYY-MM-DD`` into a :class:`date`. Used as argparse ``type=``."""
    return datetime.strptime(s, "%Y-%m-%d").date()


def parse_args(description: str) -> argparse.Namespace:
    """Standard backfill CLI flags shared by every per-source script.

    Every per-source script accepts ``[--start, --end)`` and delegates
    the day/month splitting to ``scripts.backfill.bulk``. Static
    sources (DCP) ignore ``--start`` / ``--end``.
    """
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "--start", required=True, type=parse_date,
        help="Start date (inclusive), format YYYY-MM-DD",
    )
    parser.add_argument(
        "--end", required=True, type=parse_date,
        help="End date (exclusive), format YYYY-MM-DD",
    )
    parser.add_argument(
        "--action", choices=["upload", "fetch"], default="upload",
        help="upload (write to GCS) or fetch (return data, do not write)",
    )
    parser.add_argument(
        "--bucket", default=None,
        help="GCS bucket name. Defaults to env GCS_BUCKET_NAME. "
             "Required for --action upload.",
    )
    parser.add_argument(
        "--dataset", default=None,
        help="Specific dataset name to backfill (default: all datasets in the source)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Alias for --action fetch; logs record counts without writing",
    )
    parser.add_argument(
        "--max-workers", type=int, default=None,
        help="Thread-pool size for parallel day/month backfill (default: "
             "4 for daily, 2 for monthly). 1 = serial.",
    )
    return parser.parse_args()


def require_bucket(args: argparse.Namespace) -> str:
    """Resolve the GCS bucket from ``--bucket`` or the ``GCS_BUCKET_NAME`` env.

    Exits with code 1 and a clear message if neither is set, matching the
    pre-refactor behavior of ``backfill_nyc_311.load_config``.
    """
    bucket = args.bucket or _env_bucket()
    if not bucket:
        print(
            "Error: GCS bucket is required for upload. "
            "Set --bucket or the GCS_BUCKET_NAME env var.",
            file=sys.stderr,
        )
        sys.exit(1)
    return bucket


def _env_bucket() -> str:
    import os
    return os.environ.get("GCS_BUCKET_NAME", "").strip()


def default_max_workers(partition_strategy: str) -> int:
    """Default thread-pool size per partition strategy.

    Socrata has per-token rate limits, so 4 is a safe default for daily.
    NYPD has 4 datasets sharing one token → 2 is safer.
    """
    return {"daily": 4, "monthly": 2, "static": 1}.get(partition_strategy, 4)


# ── Dispatch tables: strategy → bulk function ────────────────────────────────
#
# Per-source scripts use these to pick the right bulk helper for their
# source's strategy. Adding a 4th strategy = adding one row to each table.


from scripts.backfill.bulk import (  # noqa: E402  — local import to avoid cycles
    backfill_daily_window,
    backfill_monthly_window,
    backfill_static,
    fetch_daily_window,
    fetch_monthly_window,
    fetch_static,
)

UPLOAD_DISPATCH = {
    "daily": backfill_daily_window,
    "monthly": backfill_monthly_window,
    "static": backfill_static,
}

FETCH_DISPATCH = {
    "daily": fetch_daily_window,
    "monthly": fetch_monthly_window,
    "static": fetch_static,
}
