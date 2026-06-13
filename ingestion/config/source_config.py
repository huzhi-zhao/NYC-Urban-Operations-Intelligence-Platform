"""
Pydantic models for source registry YAML files.

Each YAML file under ``config/sources/`` describes one upstream data source,
optionally with multiple datasets. The models in this module are the schema
contract — validation runs on every load and rejects unknown fields,
malformed IDs, and cross-field inconsistencies.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class SourceType(StrEnum):
    """High-level source type — drives the client/loader dispatch in step #3."""

    REST_API_SOCRATA = "rest_api_socrata"
    REST_API = "rest_api"
    GEOJSON_STATIC = "geojson_static"


class ApiType(StrEnum):
    """Dataset-level API protocol — drives dataset client construction."""

    SOCRATA = "socrata"
    SOCRATA_GEOJSON = "socrata_geojson"
    OPEN_METEO = "open_meteo"
    GENERIC_REST = "generic_rest"


# Stable set of allowed values for source.status.
SourceStatus = Literal["production", "staging", "deprecated"]


class SourceMetadata(BaseModel):
    """Top-level metadata for a single data source."""

    model_config = ConfigDict(extra="forbid")

    id: str = Field(
        pattern=r"^SRC-[A-Za-z0-9-]+$",
        description="Stable source identifier, e.g. SRC-NYC-001, SRC-Open-Meteo",
    )
    name: str = Field(min_length=1)
    type: SourceType
    owner: str = Field(min_length=1, description="Owning team slug, e.g. city_operations")
    priority: str = Field(pattern=r"^P[0-3]$")
    status: SourceStatus
    description: str | None = None


class DatasetConfig(BaseModel):
    """A single dataset within a source. Validated against its api_type."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(pattern=r"^[a-z0-9_]+$")
    description: str | None = None
    api_type: ApiType

    # Common
    timestamp_field: str | None = Field(
        default=None,
        description="Field used for incremental windowing. null for static datasets.",
    )

    # Socrata / Socrata-GeoJSON
    resource_id: str | None = None
    domain: str | None = None

    # Socrata-GeoJSON only
    format: str | None = None

    # Open-Meteo / generic REST
    endpoint: str | None = None
    query_params: dict[str, Any] | None = None

    @model_validator(mode="after")
    def _check_field_combinations(self) -> DatasetConfig:
        """Enforce required/forbidden field combinations per api_type."""
        api = self.api_type

        if api == ApiType.SOCRATA:
            self._require("resource_id", "domain")
            self._forbid("endpoint", "format")

        elif api == ApiType.SOCRATA_GEOJSON:
            self._require("resource_id", "domain")
            if self.format != "geojson":
                raise ValueError(
                    "api_type=socrata_geojson requires format='geojson'",
                )
            self._forbid("endpoint")

        elif api == ApiType.OPEN_METEO:
            self._require("endpoint")
            self._forbid("resource_id", "domain", "format")

        elif api == ApiType.GENERIC_REST:
            self._require("endpoint")
            self._forbid("resource_id", "domain", "format")

        return self

    def _require(self, *field_names: str) -> None:
        missing = [f for f in field_names if not getattr(self, f)]
        if missing:
            raise ValueError(
                f"api_type={self.api_type.value} requires: {', '.join(missing)}",
            )

    def _forbid(self, *field_names: str) -> None:
        present = [f for f in field_names if getattr(self, f) is not None]
        if present:
            raise ValueError(
                f"api_type={self.api_type.value} forbids: {', '.join(present)}",
            )


class SourceConfig(BaseModel):
    """A complete source definition (metadata + one or more datasets)."""

    model_config = ConfigDict(extra="forbid")

    source: SourceMetadata
    datasets: list[DatasetConfig] = Field(min_length=1)
