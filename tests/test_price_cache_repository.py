from pathlib import Path

import pytest
from sqlalchemy import inspect

from app.services.price_cache.config import load_price_cache_config
from app.services.price_cache.fixtures import seed_cache_snapshot
from app.services.price_cache.migrations import apply_migrations
from app.services.price_cache.sql_repository import SqlPriceCacheRepository, create_price_cache_engine


def _repo(tmp_path: Path) -> SqlPriceCacheRepository:
    config = load_price_cache_config(
        {
            "PRICE_CACHE_BACKEND": "sqlite",
            "PRICE_CACHE_SQLITE_PATH": str(tmp_path / "price_cache.sqlite3"),
        }
    )
    engine = create_price_cache_engine(config)
    apply_migrations(engine, config.backend)
    return SqlPriceCacheRepository(engine)


def test_config_defaults_to_local_sqlite(monkeypatch):
    for name in (
        "PRICE_CACHE_BACKEND",
        "PRICE_CACHE_SQLITE_PATH",
        "PRICE_CACHE_RETAIN_VERSIONS",
        "PRICE_CACHE_DATABASE_URL",
        "PRICE_CACHE_GCP_PROJECT",
        "PRICE_CACHE_GCP_REGION",
        "PRICE_CACHE_GCP_CLOUD_SQL_INSTANCE",
    ):
        monkeypatch.delenv(name, raising=False)

    config = load_price_cache_config()

    assert config.backend == "sqlite"
    assert config.sqlite_path == Path(".tmp/price_cache.sqlite3")
    assert config.retain_versions == 3
    assert config.gcp_region == "europe-west1"


def test_config_rejects_postgres_without_database_url(monkeypatch):
    monkeypatch.setenv("PRICE_CACHE_BACKEND", "cloud_sql_postgres")
    monkeypatch.delenv("PRICE_CACHE_DATABASE_URL", raising=False)

    with pytest.raises(ValueError, match="PRICE_CACHE_DATABASE_URL"):
        load_price_cache_config()


def test_migrations_create_sqlite_schema(tmp_path):
    repo = _repo(tmp_path)
    inspector = inspect(repo.engine)

    tables = set(inspector.get_table_names())

    assert "price_cache_schema_migrations" in tables
    assert "price_cache_versions" in tables
    assert "price_cache_active_version" in tables
    assert "cached_countries" in tables
    assert "cached_commodities" in tables
    assert "cached_units" in tables
    assert "cached_markets" in tables
    assert "cached_currencies" in tables
    assert "cached_price_monthly" in tables


def test_empty_cache_returns_clear_empty_status(tmp_path):
    repo = _repo(tmp_path)

    status = repo.get_cache_status()

    assert status.has_active_cache is False
    assert status.active_version_id is None
    assert repo.get_active_version_id() is None
    assert repo.list_countries() == []
    assert repo.get_country_metadata("SSD") is None
    assert repo.get_price_window("SSD", "2025-01-01", "2025-02-01") == []


def test_fixture_snapshot_seeds_country_metadata(tmp_path):
    repo = _repo(tmp_path)
    version_id = seed_cache_snapshot(repo)

    status = repo.get_cache_status()
    countries = repo.list_countries()
    metadata = repo.get_country_metadata("SSD")

    assert status.has_active_cache is True
    assert status.active_version_id == version_id
    assert status.rows_prices == 4
    assert countries[0].country_name == "South Sudan"
    assert metadata is not None
    assert metadata.country.country_iso3 == "SSD"
    assert [item.commodity_name for item in metadata.commodities] == ["Beans", "Maize"]
    assert [item.market_name for item in metadata.markets] == ["Juba", "Wau"]
    assert [item.commodity_unit_name for item in metadata.units] == ["kg"]
    assert [item.currency_code for item in metadata.currencies] == ["SSP"]


def test_price_window_filters_by_country_date_commodity_market_and_admin1(tmp_path):
    repo = _repo(tmp_path)
    seed_cache_snapshot(repo)

    all_rows = repo.get_price_window("SSD", "2025-01-01", "2025-02-01")
    maize_rows = repo.get_price_window("SSD", "2025-01-01", "2025-02-01", commodity_ids=[1])
    wau_rows = repo.get_price_window("SSD", "2025-01-01", "2025-02-01", market_ids=[11])
    juba_rows = repo.get_price_window(
        "SSD",
        "2025-01-01",
        "2025-02-01",
        admin1_names=["Central Equatoria"],
    )
    feb_rows = repo.get_price_window("SSD", "2025-02-01", "2025-02-01")

    assert len(all_rows) == 4
    assert {row.commodity_name for row in maize_rows} == {"Maize"}
    assert {row.market_name for row in wau_rows} == {"Wau"}
    assert {row.admin1_name for row in juba_rows} == {"Central Equatoria"}
    assert len(feb_rows) == 2
    assert {row.price for row in feb_rows} == {6.0, 12.0}


def test_active_version_pointer_controls_visible_rows(tmp_path):
    repo = _repo(tmp_path)
    old_version = seed_cache_snapshot(
        repo,
        cache_version_id="11111111-1111-1111-1111-111111111111",
        price_offset=0,
    )
    old_rows = repo.get_price_window("SSD", "2025-01-01", "2025-01-01", commodity_ids=[1])

    new_version = seed_cache_snapshot(
        repo,
        cache_version_id="22222222-2222-2222-2222-222222222222",
        price_offset=100,
    )
    new_rows = repo.get_price_window("SSD", "2025-01-01", "2025-01-01", commodity_ids=[1])

    assert old_version != new_version
    assert repo.get_active_version_id() == new_version
    assert old_rows[0].price == 10.0
    assert new_rows[0].price == 110.0
