"""Per-source backfill: Open-Meteo Weather (SRC-Open-Meteo, daily)."""

from __future__ import annotations

import argparse
import logging

from ingestion.config import load_source_config
from scripts.backfill._common import (
    FETCH_DISPATCH,
    UPLOAD_DISPATCH,
    default_max_workers,
    parse_args,
    require_bucket,
)
from scripts.backfill._registry import register_backfill

logger = logging.getLogger(__name__)
SOURCE_ID = "SRC-Open-Meteo"


@register_backfill(SOURCE_ID)
def run(args: argparse.Namespace) -> None:
    cfg = load_source_config(SOURCE_ID)
    strategy = cfg.source.partition_strategy

    if args.dry_run or args.action == "fetch":
        results = FETCH_DISPATCH[strategy](
            SOURCE_ID, start=args.start, end=args.end,
            max_workers=args.max_workers or default_max_workers(strategy),
        )
        _log_results(results, dry_run=True)
        return

    bucket = require_bucket(args)
    results = UPLOAD_DISPATCH[strategy](
        SOURCE_ID, start=args.start, end=args.end, bucket=bucket,
        max_workers=args.max_workers or default_max_workers(strategy),
    )
    _log_results(results, dry_run=False)
    failures = [r for r in results if r.status == "failed"]
    if failures:
        logger.error("%s: %d/%d chunks failed", SOURCE_ID, len(failures), len(results))
        raise SystemExit(2)


def _log_results(results, *, dry_run: bool) -> None:
    tag = "DRY-RUN" if dry_run else "WROTE"
    for r in results:
        if r.status == "ok":
            logger.info("  %s %s: %d records %s", tag, r.document, r.manifest_count, "ok")
        else:
            logger.error("  %s %s FAILED: %s", tag, r.document, r.error)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    run(parse_args("Open-Meteo Weather backfill (daily)"))
