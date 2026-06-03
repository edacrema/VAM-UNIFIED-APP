from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Optional


SQLITE_BACKEND = "sqlite"
POSTGRES_BACKEND = "postgres"


@dataclass(frozen=True)
class PriceCacheConfig:
    backend: str
    sqlite_path: Path
    database_url: Optional[str]
    retain_versions: int
    gcp_project: Optional[str]
    gcp_region: str
    gcp_cloud_sql_instance: Optional[str]


def load_price_cache_config(
    env: Optional[Mapping[str, str]] = None,
    *,
    validate: bool = True,
) -> PriceCacheConfig:
    values = env or os.environ
    backend = _normalize_backend(values.get("PRICE_CACHE_BACKEND", "sqlite"))
    sqlite_path = Path(values.get("PRICE_CACHE_SQLITE_PATH", ".tmp/price_cache.sqlite3"))
    database_url = _blank_to_none(values.get("PRICE_CACHE_DATABASE_URL"))
    retain_versions = _int_or_default(values.get("PRICE_CACHE_RETAIN_VERSIONS"), 3, minimum=1)

    config = PriceCacheConfig(
        backend=backend,
        sqlite_path=sqlite_path,
        database_url=database_url,
        retain_versions=retain_versions,
        gcp_project=_blank_to_none(values.get("PRICE_CACHE_GCP_PROJECT")),
        gcp_region=values.get("PRICE_CACHE_GCP_REGION", "europe-west1").strip() or "europe-west1",
        gcp_cloud_sql_instance=_blank_to_none(values.get("PRICE_CACHE_GCP_CLOUD_SQL_INSTANCE")),
    )
    if validate:
        validate_price_cache_config(config)
    return config


def validate_price_cache_config(config: PriceCacheConfig) -> None:
    if config.backend == POSTGRES_BACKEND and not config.database_url:
        raise ValueError("PRICE_CACHE_DATABASE_URL is required when PRICE_CACHE_BACKEND is postgres.")
    if config.backend == SQLITE_BACKEND and not str(config.sqlite_path).strip():
        raise ValueError("PRICE_CACHE_SQLITE_PATH is required when PRICE_CACHE_BACKEND is sqlite.")


def _normalize_backend(raw: str) -> str:
    value = (raw or "sqlite").strip().lower()
    if value in {"sqlite", "local", "file"}:
        return SQLITE_BACKEND
    if value in {"postgres", "postgresql", "cloud_sql_postgres", "cloud-sql-postgres"}:
        return POSTGRES_BACKEND
    raise ValueError(
        "Unsupported PRICE_CACHE_BACKEND. Expected sqlite, postgres, or cloud_sql_postgres."
    )


def _blank_to_none(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def _int_or_default(value: Optional[str], default: int, *, minimum: int) -> int:
    if value is None or not str(value).strip():
        return default
    try:
        parsed = int(str(value).strip())
    except ValueError as exc:
        raise ValueError(f"Expected integer value, got {value!r}.") from exc
    if parsed < minimum:
        raise ValueError(f"Expected integer value >= {minimum}, got {parsed}.")
    return parsed
