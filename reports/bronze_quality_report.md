# Bronze Data Quality Report

**Bucket**: `nyc-uoip-prod`  
**Generated**: 2026-06-16 20:56 UTC

---
## SRC-NYC-311

### nyc_311

**Coverage (from manifests)**

| Metric | Value |
|--------|-------|
| Total records | 1,659,723 |
| Partitions found | 151 |
| Min records/partition | 7,334 |
| Max records/partition | 22,806 |
| Median records/partition | 10,761 |


**Field Profiling** (sampled 53,791 records across 5 file(s))

**Fields with >5% nulls:**

| Field | Null % |
|-------|--------|
| `taxi_company_borough` | 99.9% |
| `bridge_highway_direction` | 99.7% |
| `road_ramp` | 99.7% |
| `due_date` | 99.6% |
| `bridge_highway_name` | 99.5% |
| `bridge_highway_segment` | 99.5% |
| `taxi_pick_up_location` | 99.1% |
| `vehicle_type` | 96.0% |
| `facility_type` | 92.7% |
| `descriptor_2` | 47.7% |
| `landmark` | 47.3% |
| `intersection_street_2` | 40.6% |
| `intersection_street_1` | 40.6% |
| `cross_street_2` | 37.0% |
| `cross_street_1` | 37.0% |
| `location_type` | 12.2% |
| `bbl` | 11.2% |
| `closed_date` | 5.1% |

**Timestamp analysis:**

| Check | Value |
|-------|-------|
| Missing | 0.00% |
| Parse errors | 0.00% |
| Epoch-zero anomalies | 0 |
| Far-future anomalies | 0 |
| Date range | 2026-01-01 → 2026-05-01 |

**Borough values:**

| Borough | Count |
|---------|-------|
| `BROOKLYN` | 16,510 |
| `QUEENS` | 12,784 |
| `BRONX` | 11,272 |
| `MANHATTAN` | 10,935 |
| `STATEN ISLAND` | 2,209 |
| `Unspecified` | 81 |
