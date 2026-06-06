---
name: project
description: NYC Urban Operations Intelligence Platform — architecture, phases, data sources, business goals
metadata:
  type: project
---

# NYC-UOIP — Project Memory

## What it is
Production-grade **Lakehouse pipeline** that ingests NYC Open Data (311 requests, NYPD collisions, Open-Meteo weather, Borough boundaries) and produces a daily **Operational Load Score + resource allocation recommendations** per Borough.

## Business goal
- Predict future 24h operational load per NYC Borough
- Output: Load Score ranking · Operational Drivers · Resource Allocation Recommendations
- Users: NYC 311 call center, traffic/EMS dispatch, emergency response planners

## Two delivery phases (same repo)

| | Phase 1 — GCP Demo | Phase 2 — Self-hosted |
|---|---|---|
| Storage | GCS | MinIO (S3-compatible) |
| Processing | Spark on Dataproc | Spark + Apache Iceberg |
| Warehouse | BigQuery | Trino + Iceberg tables |
| Orchestration | Cloud Composer (Airflow) | Docker Airflow |
| GIS | BigQuery GIS (ST_GEOG) | GeoSpark via Trino |

Switch via `DEPLOYMENT_PHASE=1` or `DEPLOYMENT_PHASE=2`.

## Data sources

| ID | Source | Key fields |
|---|---|---|
| SRC-NYC-001 | NYC 311 Socrata API (`erm2-nwe9.json`) | `created_date`, `complaint_type`, `borough`, `latitude`, `longitude` |
| SRC-NYC-002 | NYPD Collisions Socrata API (`h9gi-nx95.json`) | `crash_date`, `borough`, `latitude`, `longitude`, `number_of_persons_injured` |
| SRC-NYC-003 | Open-Meteo forecast API | `time`, `temperature_2m`, `snowfall`, `precipitation`, `windspeed_10m` |
| SRC-NYC-004 | NYC Borough GeoJSON (static) | `boro_code`, `boro_name`, `geometry` (polygon) |

All contracts live in `contracts/source-registry.md` and `contracts/api-contracts/`.

## Lakehouse layers

```
Bronze  → Silver  → Gold
Raw JSON  Cleaned   Star schema
(immutable) Parquet  BigQuery / Trino
```

- **Bronze**: Immutable raw JSON/GeoJSON, partitioned `bronze/<dataset>/year=YYYY/month=MM/day=DD/`
- **Silver**: Cleaned Parquet, UTC timestamps, deduplicated, partitioned by date
- **Gold**: Star schema — `fact_311_requests`, `fact_vehicle_collisions`, `dim_date`, `dim_weather`, `dim_geography`

## Operational Load Score formula
```
Score = 0.4 × 311_Request_Volume + 0.4 × Collision_Factor + 0.2 × Weather_Factor
```
Per Borough per day. Rules-based (no ML). SQL-driven in `sql/intelligence/`.

## Key coding rules
- Python: `ruff`, line length 100, type hints required
- SQL: `sqlfluff`, dialect `bigquery` (P1) / `trino` (P2), keywords UPPERCASE, no `SELECT *` in Gold
- DAGs: scheduling logic ONLY, no business logic inline, use `execution_date` not `datetime.now()`
- Imports: absolute paths within package — `from ingestion.clients.socrata_client import ...`
- Secrets: all via `.env` / env vars, never hardcoded

## Build commands
```bash
make install        # uv install
make lint           # ruff + sqlfluff
make test-unit      # pytest (no Spark/cloud)
make test-integration
make spark-submit JOB=spark/jobs/etl_nyc_311.py
docker compose -f infra/docker/docker-compose.yml up -d  # Phase 2 stack
```

## Escalate to human when
- Upstream Socrata API schema changed
- Spark job produces 0 rows (possible API outage)
- `dim_geography` spatial join returns NULL for >10% of records
- GCP billing alert fires