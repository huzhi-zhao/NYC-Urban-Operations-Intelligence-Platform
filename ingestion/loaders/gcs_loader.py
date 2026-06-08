"""
GCS Loader — writes raw JSON records to Bronze layer on Google Cloud Storage.

Storage layouts:

  # Daily incremental (DAG loads) — partition by ingest_date
  {bucket}/bronze/raw/{source_id}/{dataset_name}/ingest_date=YYYY-MM-DD/data.json
  {bucket}/bronze/raw/{source_id}/{dataset_name}/ingest_date=YYYY-MM-DD/manifest.json

  # Monthly backfill / shard — flat files, month encoded in filename
  {bucket}/bronze/raw/{source_id}/{dataset_name}/data_YYYY-MM.json
  {bucket}/bronze/raw/{source_id}/{dataset_name}/manifest_YYYY-MM.json

Manifest contents:
  - source_id, dataset_name, ingest_date, month_partition
  - record_count, file_size_bytes, sha256_checksum
  - data_date_range (min/max of the timestamp field)
  - fetch_timestamp (ISO datetime of this upload — overwrites on re-run)
  - timestamp_field

Re-upload behavior: data and manifest are always overwritten (GCS PUT is idempotent).

Phase: Phase 1 (GCP)
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, asdict
from datetime import date, datetime
from typing import Any

from google.cloud import storage


@dataclass
class ManifestEntry:
    """Metadata manifest entry written alongside each Bronze data file."""

    source_id: str
    dataset_name: str
    ingest_date: str  # YYYY-MM-DD (date of this upload)
    month_partition: str  # YYYY-MM (calendar month of the data)
    filename: str
    record_count: int
    file_size_bytes: int
    sha256_checksum: str
    data_date_min: str | None  # ISO date of earliest record
    data_date_max: str | None  # ISO date of latest record
    fetch_timestamp: str  # ISO datetime of this upload (re-written on re-run)
    timestamp_field: str  # field used for date range (e.g. "created_date")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class GCSBronzeLoader:
    """Writes raw JSON records to GCS Bronze layer with manifest."""

    # Daily incremental filenames
    DATA_FILE = "data.json"
    MANIFEST_FILE = "manifest.json"

    def __init__(
        self,
        bucket_name: str,
        timestamp_field: str = "created_date",
        client: storage.Client | None = None,
    ) -> None:
        """
        Initialize Bronze loader.

        Args:
            bucket_name: GCS bucket name (e.g. "nyc-uoip").
            timestamp_field: Field name for date range extraction in manifest.
            client: Optional GCS client. If None, uses default credentials.
        """
        self.bucket_name = bucket_name
        self.timestamp_field = timestamp_field
        self._client = client or storage.Client()

    def _sha256(self, data: bytes) -> str:
        return hashlib.sha256(data).hexdigest()

    def _date_range(
        self, records: list[dict[str, Any]]
    ) -> tuple[str | None, str | None]:
        """Extract min/max date from records using timestamp_field."""
        dates = []
        for r in records:
            raw = r.get(self.timestamp_field)
            if not raw:
                continue
            try:
                dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
                dates.append(dt.date())
            except ValueError:
                continue
        if not dates:
            return None, None
        return min(dates).isoformat(), max(dates).isoformat()

    def _make_manifest(
        self,
        source_id: str,
        dataset_name: str,
        ingest_date: date,
        month_partition: str,
        filename: str,
        records: list[dict[str, Any]],
        content_bytes: bytes,
    ) -> ManifestEntry:
        data_min, data_max = self._date_range(records)
        return ManifestEntry(
            source_id=source_id,
            dataset_name=dataset_name,
            ingest_date=ingest_date.isoformat(),
            month_partition=month_partition,
            filename=filename,
            record_count=len(records),
            file_size_bytes=len(content_bytes),
            sha256_checksum=self._sha256(content_bytes),
            data_date_min=data_min,
            data_date_max=data_max,
            fetch_timestamp=datetime.utcnow().isoformat(),
            timestamp_field=self.timestamp_field,
        )

    def _upload(self, bucket: storage.Bucket, path: str, content: bytes, meta: dict[str, str]) -> None:
        """Upload bytes to GCS; overwrites existing object at path."""
        blob = bucket.blob(path)
        blob.upload_from_string(content, content_type="application/json")
        blob.metadata = meta

    # ── Daily incremental write (DAG loads) ────────────────────────────────────

    def write(
        self,
        source_id: str,
        dataset_name: str,
        ingest_date: date,
        records: list[dict[str, Any]],
    ) -> ManifestEntry:
        """
        Write records using daily ingest_date partition.

        Path: bronze/raw/{source_id}/{dataset_name}/ingest_date={date}/data.json
              bronze/raw/{source_id}/{dataset_name}/ingest_date={date}/manifest.json

        Idempotent: re-running for the same ingest_date overwrites both files.
        """
        bucket = self._client.bucket(self.bucket_name)
        content = json.dumps(records, indent=2, ensure_ascii=False).encode("utf-8")
        month = ingest_date.strftime("%Y-%m")

        manifest = self._make_manifest(
            source_id=source_id,
            dataset_name=dataset_name,
            ingest_date=ingest_date,
            month_partition=month,
            filename=self.DATA_FILE,
            records=records,
            content_bytes=content,
        )

        data_path = f"bronze/raw/{source_id}/{dataset_name}/ingest_date={ingest_date.isoformat()}/{self.DATA_FILE}"
        self._upload(bucket, data_path, content, {
            "source_id": source_id,
            "dataset_name": dataset_name,
            "ingest_date": ingest_date.isoformat(),
            "record_count": str(len(records)),
        })

        manifest_path = f"bronze/raw/{source_id}/{dataset_name}/ingest_date={ingest_date.isoformat()}/{self.MANIFEST_FILE}"
        manifest_bytes = json.dumps(manifest.to_dict(), indent=2).encode("utf-8")
        self._upload(bucket, manifest_path, manifest_bytes, {
            "source_id": source_id,
            "dataset_name": dataset_name,
            "ingest_date": ingest_date.isoformat(),
        })

        return manifest

    # ── Monthly shard write (backfill / historical loads) ─────────────────────

    def write_monthly_shard(
        self,
        source_id: str,
        dataset_name: str,
        month_partition: str,
        records: list[dict[str, Any]],
    ) -> ManifestEntry:
        """
        Write a flat monthly shard — no month= subdirectory.

        Path: bronze/raw/{source_id}/{dataset_name}/data_{YYYY-MM}.json
              bronze/raw/{source_id}/{dataset_name}/manifest_{YYYY-MM}.json

        month_partition: YYYY-MM string (e.g. "2026-03").

        Idempotent: re-running for the same month overwrites both files.
        fetch_timestamp in the manifest reflects the time of the latest upload.
        """
        bucket = self._client.bucket(self.bucket_name)
        content = json.dumps(records, indent=2, ensure_ascii=False).encode("utf-8")
        ingest_date = date.today()

        manifest = self._make_manifest(
            source_id=source_id,
            dataset_name=dataset_name,
            ingest_date=ingest_date,
            month_partition=month_partition,
            filename=f"data_{month_partition}.json",
            records=records,
            content_bytes=content,
        )

        data_path = f"bronze/raw/{source_id}/{dataset_name}/data_{month_partition}.json"
        self._upload(bucket, data_path, content, {
            "source_id": source_id,
            "dataset_name": dataset_name,
            "month_partition": month_partition,
            "record_count": str(len(records)),
        })

        manifest_path = f"bronze/raw/{source_id}/{dataset_name}/manifest_{month_partition}.json"
        manifest_bytes = json.dumps(manifest.to_dict(), indent=2).encode("utf-8")
        self._upload(bucket, manifest_path, manifest_bytes, {
            "source_id": source_id,
            "dataset_name": dataset_name,
            "month_partition": month_partition,
        })

        return manifest


def load_gcs_credentials() -> str:
    """Load GCS credentials path from environment."""
    import os
    cred_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
    if not cred_path:
        raise EnvironmentError(
            "GOOGLE_APPLICATION_CREDENTIALS not set. "
            "Set path to GCP service account JSON key file."
        )
    return cred_path