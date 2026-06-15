# NYC-UOIP — Claude Code Instructions

> This file is read by Claude Code at the start of every session.
> Keep it under 200 lines. Move long procedures to `.claude/rules/`.

@AGENTS.md

---

## Project identity

NYC Urban Operations Intelligence Platform (NYC-UOIP).
A production-grade Lakehouse pipeline that ingests NYC Open Data (311 requests,
NYPD collisions, Open-Meteo weather, Borough boundaries) and produces a daily
Operational Load Score + resource allocation recommendations per Borough.

Two delivery phases run in the same repo:
- **Phase 1** — GCP stack: GCS · Dataproc · BigQuery · Cloud Composer
- **Phase 2** — Self-hosted stack: MinIO · Spark+Iceberg · Trino · Airflow (Docker)

---

## Build & run commands

```bash
# Install all Python deps (uses uv)
make install

# Lint Python (ruff) + SQL (sqlfluff)
make lint

# Run unit tests only (no Spark / cloud needed)
make test-unit

# Run full integration tests (requires local Docker stack)
make test-integration

# Submit a Spark job locally (Phase 2)
make spark-submit JOB=spark/jobs/etl_nyc_311.py

# Trigger a specific Airflow DAG locally
make dag-trigger DAG=dag_ingest_nyc_311

# Bring up the Phase 2 Docker stack (MinIO + Airflow + Trino + Spark)
docker compose -f infra/docker/docker-compose.yml up -d
```

---

## Repository layout (critical paths)

```
dags/                   Airflow DAG definitions — scheduling logic only
ingestion/clients/      Thin API wrappers (Socrata, Open-Meteo, GeoJSON)
ingestion/loaders/      Write raw files to Bronze (gcs_loader / minio_loader)
ingestion/schemas/      Pydantic models — validate raw API shape before write
spark/jobs/             PySpark entry points, one file per dataset
spark/transforms/       Reusable transform functions imported by jobs
spark/schemas/          Silver layer StructType definitions
sql/ddl/                CREATE TABLE statements — run once at setup
sql/dml/                Daily incremental loads (MERGE / INSERT OVERWRITE)
sql/intelligence/       Load score + driver + recommendation SQL
contracts/              Source registry and data contracts
infra/terraform/        GCP resources (Phase 1)
infra/docker/           Self-hosted stack config (Phase 2)
tests/unit/             Pure Python tests, no Spark or cloud deps
tests/fixtures/         Sample JSON/GeoJSON for mocking API responses
```

---

## Coding conventions

- **Python**: ruff enforced. Line length 100. Type hints required on all public functions.
- **SQL**: sqlfluff, dialect `bigquery` for Phase 1, `trino` for Phase 2.
  Keywords UPPERCASE. Table/column names `snake_case`.
- **Naming**:
  - DAG files: `dag_<action>_<dataset>.py` (e.g. `dag_ingest_nyc_311.py`)
  - Spark jobs: `etl_<dataset>.py`
  - SQL DDL: `<table_name>.sql` (matches BigQuery table name exactly)
  - Tests: `test_<module_being_tested>.py`
- **Imports**: absolute paths within the package (`from ingestion.clients.socrata_client import ...`).
  Never relative imports at the top level.
- **Secrets**: loaded via `python-dotenv` from `.env`. Never hardcode credentials.
  Reference `.env.example` for all required keys.

---

## Data architecture rules

- Bronze = immutable raw JSON/GeoJSON. Never overwrite a Bronze file.
  Partition path: `bronze/<dataset>/year=YYYY/month=MM/day=DD/`
- Silver = cleaned Parquet, partitioned by date.
  All timestamps must be UTC. Use `timestamp_normalizer.py` for all conversions.
- Gold = BigQuery managed tables (Phase 1) or Iceberg tables via Trino (Phase 2).
  `fact_` tables are partitioned by date and clustered by `borough_id`.
- `dim_geography` stores `GEOGRAPHY` type (BigQuery) or WKT string (Iceberg).
  Use `ST_CONTAINS` for borough spatial fill — never manual zip-code lookup alone.
- All ETL jobs must be **idempotent**: re-running the same `execution_date`
  produces identical output, no duplicates. Use MERGE or INSERT OVERWRITE PARTITION.

### Bronze partitioning strategies

Each source declares `partition_strategy: daily|monthly` in its YAML
(`config/sources/<id>.yaml`). The `BackfillFacade` uses it to choose the
GCS path layout:

| Strategy | Used by | GCS path |
|---|---|---|
| `daily` | SRC-NYC-311, SRC-Open-Meteo | `bronze/raw/{sid}/{ds}/{YYYY-MM}/data_{YYYY-MM-DD}.json` + `manifest_{YYYY-MM-DD}.json` (per day) |
| `monthly` (default) | SRC-NYPD | `bronze/raw/{sid}/{ds}/data_{YYYY-MM}.json` + `manifest_{YYYY-MM}.json` |
| `static` | SRC-DCP | `bronze/raw/{sid}/{ds}/data_static.json` + `manifest_static.json` |

`daily` requires every dataset to declare a `timestamp_field` (Pydantic
validates this in `ingestion/config/source_config.py`). Records are split
by the date portion of that field; records with missing/unparseable
timestamps are dropped. Each daily shard has a paired
`manifest_YYYY-MM-DD.json` file in the same month folder describing that
day's data.

---

## Airflow conventions

- DAG files contain scheduling logic only. No business logic, no API calls inline.
- All heavy work is delegated to: `ingestion/`, `spark/jobs/`, or `sql/` scripts.
- Use `execution_date` (not `datetime.now()`) for all incremental window logic.
- Every DAG must have: `retries=3`, `retry_delay=timedelta(minutes=5)`,
  `on_failure_callback` pointing to the Slack/email alert utility.
- Socrata DAGs must implement a 7-day lookback window for late-arriving facts.

---

## What NOT to do

- Do not put business logic inside DAG files.
- Do not use `SELECT *` in any Gold-layer SQL.
- Do not hardcode `execution_date` or date strings in SQL — always use parameters.
- Do not create new utility functions in `spark/jobs/` — put them in `spark/transforms/`.
- Do not commit `.env`, `CLAUDE.local.md`, or any `*.json` credentials file.
- Do not run Dataproc clusters (Phase 1) without auto-delete configured.

---

## Escalate to human when

- The upstream Socrata API schema has changed (new/renamed fields).
- A Spark job produces a Silver partition with 0 rows (possible API outage).
- Any `dim_geography` spatial join returns NULL for > 10% of records.
- GCP billing alert fires.
