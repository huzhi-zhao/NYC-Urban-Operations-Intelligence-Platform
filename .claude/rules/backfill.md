# Backfill Layer — Architecture Notes

> Read this before touching `scripts/backfill/`, `ingestion/backfill.py`, or any backfill DAG.

---

## Three-layer design

```
scripts/backfill/backfill_*.py   ← CLI entry points (argparse, one file per source)
        ↓ calls
scripts/backfill/bulk.py         ← window slicing + ThreadPoolExecutor
        ↓ calls
ingestion/backfill.py            ← BackfillFacade: one atomic pull+write per document
        ↓ writes
GCS Bronze                       ← bronze/raw/{sid}/{ds}/{YYYY-MM}/data_{date}.json
```

**Rule**: business logic lives only in the facade and bulk layers. Per-source scripts
and DAG files are pure dispatch — no API calls, no date arithmetic inline.

---

## Dispatch by partition strategy

Each source YAML (`config/sources/*.yaml`) declares `partition_strategy`.
`bulk.py` and `_common.py` dispatch on it:

| strategy | source | bulk function | facade method |
|---|---|---|---|
| `daily` + socrata | SRC-NYC-311 | `backfill_daily_window` (per-day loop) | `upload_day(date)` |
| `daily` + open_meteo | SRC-Open-Meteo | `backfill_daily_window` (1 wide call) | `upload_window(start, end)` |
| `monthly` | SRC-NYPD | `backfill_monthly_window` | `upload_month(date)` |
| `static` | SRC-DCP | `backfill_static` | `upload_static()` |

`_is_wide_fetch_source()` in `bulk.py` checks `cfg.datasets[0].api_type == ApiType.OPEN_METEO`
to choose between per-day slicing and the single wide-fetch path.

---

## Auto-discovery of per-source scripts

`scripts/backfill/main.py` calls `pkgutil.iter_modules` to find every `backfill_*.py`
file and imports it. Importing triggers the `@register_backfill` decorator (defined in
`_registry.py`), which populates `BACKFILL_REGISTRY`. To add a new source, drop a
`backfill_<slug>.py` file — no edits to `main.py` needed.

---

## CLI invocation pattern

```bash
# Daily source (311), upload mode
python -m scripts.backfill.main --source SRC-NYC-311 \
    --start 2024-01-01 --end 2025-01-01 --bucket nyc-uoip-bronze

# Monthly source (NYPD), dry-run
python -m scripts.backfill.main --source SRC-NYPD \
    --start 2024-01-01 --end 2025-01-01 --dry-run

# Static (DCP), upload
python -m scripts.backfill.main --source SRC-DCP \
    --start 2024-01-01 --end 2024-01-01 --bucket nyc-uoip-bronze
```

`--bucket` falls back to `GCS_BUCKET_NAME` env var. `--dry-run` calls `fetch_*`
instead of `upload_*`, no GCS writes.

---

## Calling bulk functions from a DAG (copy-paste pattern)

```python
from scripts.backfill.bulk import backfill_daily_window, backfill_monthly_window, backfill_static
from datetime import date

# 311
results = backfill_daily_window("SRC-NYC-311", start=date(2024,1,1), end=date(2025,1,1), bucket="nyc-uoip-bronze")

# NYPD
results = backfill_monthly_window("SRC-NYPD", start=date(2024,1,1), end=date(2025,1,1), bucket="nyc-uoip-bronze")

# Open-Meteo (1 wide call, returns list of 1 BulkResult)
results = backfill_daily_window("SRC-Open-Meteo", start=date(2024,1,1), end=date(2025,1,1), bucket="nyc-uoip-bronze")

# DCP (static, no dates)
results = backfill_static("SRC-DCP", bucket="nyc-uoip-bronze")
```

`BulkResult` fields: `document` (date|None), `status` ("ok"|"failed"), `manifest_count`, `error`.
Failures on one slice do **not** abort others — check `any(r.status=="failed" for r in results)`.

---

## DAG status (as of 2026-06-15)

`dags/` directory created. 4 backfill DAGs implemented:
- `dags/dag_backfill_nyc_311.py` — daily, Socrata
- `dags/dag_backfill_nypd.py` — monthly, Socrata
- `dags/dag_backfill_open_meteo.py` — daily, wide-fetch (max 365-day window per run)
- `dags/dag_backfill_dcp.py` — static (no date params needed)

Shared helpers: `dags/_dag_common.py` (DEFAULT_ARGS, backfill_params, get_bucket).
DAG import test: `tests/unit/test_dag_imports.py` (skips if airflow not installed locally).

Design: 1 DAG Run = 1 time window. Airflow does NOT slice — `bulk.py` does.
Schedule = None (manual trigger only).

---

## Cloud Composer deployment (Phase 1)

Infra: `google_composer_environment.main` added to `infra/terraform/main.tf`.
Composer 2 Small, us-central1, Airflow 2.x. GCP project: `pace-lab-bdp`.
COST: ~$10/day — delete after backfill: `terraform destroy -target=google_composer_environment.main`

Deploy workflow:
```bash
make terraform-apply      # provision (~20 min first time)
make deploy-composer      # sync dags/ + ingestion/ + scripts/ + config/ to Composer GCS
# Then trigger in Airflow UI (URL: terraform output composer_airflow_uri)
# {"start": "2024-01-01", "end": "2025-01-01", "bucket": "nyc-uoip"}
```

Composer adds `gs://<bucket>/plugins/` to PYTHONPATH automatically.
Our packages (ingestion/, scripts/, config/) land there via `deploy-composer`.

Env vars injected via Terraform: GCS_BUCKET_NAME, SOCRATA_APP_TOKEN, DEPLOYMENT_PHASE=1.
Set `socrata_app_token` in `terraform.tfvars` (not committed).
