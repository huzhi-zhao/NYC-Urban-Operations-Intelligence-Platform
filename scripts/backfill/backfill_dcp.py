"""Per-source backfill: NYC Spatial Boundaries (SRC-DCP, static GeoJSON).

DCP is a static snapshot — ``--start`` / ``--end`` are accepted for CLI
uniformity but the underlying call is a one-shot ``upload_static()``.
"""

from __future__ import annotations

import argparse
import logging

from ingestion.config import load_source_config
from scripts.backfill._common import (
    FETCH_DISPATCH,
    UPLOAD_DISPATCH,
    parse_args,
    require_bucket,
)
from scripts.backfill._registry import register_backfill

logger = logging.getLogger(__name__)
SOURCE_ID = "SRC-DCP"


@register_backfill(SOURCE_ID)
def run(args: argparse.Namespace) -> None:
    cfg = load_source_config(SOURCE_ID)
    strategy = cfg.source.partition_strategy
    logger.info(
        "DCP is a static GeoJSON; --start=%s --end=%s are accepted but ignored",
        args.start, args.end,
    )

    if args.dry_run or args.action == "fetch":
        results = FETCH_DISPATCH[strategy](SOURCE_ID)
        _log_results(results, dry_run=True)
        return

    bucket = require_bucket(args)
    results = UPLOAD_DISPATCH[strategy](SOURCE_ID, bucket=bucket)
    _log_results(results, dry_run=False)
    failures = [r for r in results if r.status == "failed"]
    if failures:
        logger.error("%s: %d/%d chunks failed", SOURCE_ID, len(failures), len(results))
        raise SystemExit(2)


def _log_results(results, *, dry_run: bool) -> None:
    tag = "DRY-RUN" if dry_run else "WROTE"
    for r in results:
        if r.status == "ok":
            logger.info("  %s static: %d records %s", tag, r.manifest_count, "ok")
        else:
            logger.error("  %s static FAILED: %s", tag, r.error)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    run(parse_args("NYC Spatial Boundaries backfill (static GeoJSON)"))
