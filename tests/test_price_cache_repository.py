from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy import inspect

from app.services.price_cache.config import load_price_cache_config
from app.services.price_cache.fixtures import seed_cache_snapshot
from app.services.price_cache.migrations import (
    MIGRATIONS_ROOT,
    _ensure_migration_table,
    _split_sql,
    apply_migrations,
)
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
    assert "price_cache_country_active_versions" in tables
    assert "price_cache_version_countries" in tables
    assert "price_cache_refresh_locks" in tables
    assert "country_food_basket_current" in tables
    assert "country_food_basket_versions" in tables
    assert "country_food_basket_items" in tables


def test_migration_003_backfills_admin_metadata_on_legacy_databases(tmp_path):
    config = load_price_cache_config(
        {
            "PRICE_CACHE_BACKEND": "sqlite",
            "PRICE_CACHE_SQLITE_PATH": str(tmp_path / "legacy_cache.sqlite3"),
        }
    )
    engine = create_price_cache_engine(config)

    # Build a database at schema version 002 with rows shaped like the broken
    # deployment: price rows whose admin1/admin2 are NULL.
    with engine.begin() as conn:
        _ensure_migration_table(conn, "sqlite")
        for version in ("001_initial_cache", "002_phase3_refresh_worker"):
            for statement in _split_sql((MIGRATIONS_ROOT / "sqlite" / f"{version}.sql").read_text(encoding="utf-8")):
                conn.execute(text(statement))
            conn.execute(
                text("INSERT INTO price_cache_schema_migrations(version) VALUES (:version)"),
                {"version": version},
            )
        conn.execute(
            text(
                """
                INSERT INTO price_cache_versions (cache_version_id, status, refresh_type, started_at)
                VALUES ('legacy-version', 'active', 'weekly_full', '2026-06-01T00:00:00+00:00')
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO cached_markets (cache_version_id, country_iso3, market_id, market_name, admin1_name, admin2_name)
                VALUES ('legacy-version', 'SSD', 10, 'Juba', 'Central Equatoria', 'Juba County')
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO cached_price_monthly (
                    cache_version_id, country_iso3, commodity_id, market_id, price_date, price,
                    price_type_name, price_flag, admin1_name, admin2_name, market_name
                ) VALUES (
                    'legacy-version', 'SSD', 1, 10, '2025-01-01', 20.0,
                    'Retail', 'actual', NULL, NULL, NULL
                )
                """
            )
        )

    applied = apply_migrations(engine, config.backend)

    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT admin1_name, admin2_name, market_name FROM cached_price_monthly")
        ).mappings().first()
    assert "003_admin_backfill_and_price_key" in applied
    assert row["admin1_name"] == "Central Equatoria"
    assert row["admin2_name"] == "Juba County"
    assert row["market_name"] == "Juba"


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
    availability = repo.get_country_availability("SSD")

    assert status.has_active_cache is True
    assert status.active_version_id == version_id
    assert status.rows_prices == 4
    assert countries[0].country_name == "South Sudan"
    assert metadata is not None
    assert availability is not None
    assert availability.cache_version_id == version_id
    assert availability.date_start.isoformat() == "2025-01-01"
    assert availability.date_end.isoformat() == "2025-02-01"
    assert availability.latest_price_date.isoformat() == "2025-02-01"
    assert availability.priced_commodity_ids == [1, 2]
    assert availability.admin1_names == ["Central Equatoria", "Western Bahr el Ghazal"]
    assert [unit.commodity_unit_name for unit in availability.units] == ["kg"]
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


def test_country_active_pointer_preserves_failed_country_fallback(tmp_path):
    repo = _repo(tmp_path)
    old_version = seed_cache_snapshot(
        repo,
        cache_version_id="11111111-1111-1111-1111-111111111111",
        country_iso3="SSD",
        country_name="South Sudan",
        price_offset=0,
    )
    new_version = seed_cache_snapshot(
        repo,
        cache_version_id="22222222-2222-2222-2222-222222222222",
        country_iso3="ETH",
        country_name="Ethiopia",
        price_offset=100,
    )

    countries = repo.list_countries()
    ssd_rows = repo.get_price_window("SSD", "2025-01-01", "2025-01-01", commodity_ids=[1])
    eth_rows = repo.get_price_window("ETH", "2025-01-01", "2025-01-01", commodity_ids=[1])

    assert repo.get_active_version_id() == new_version
    assert repo.get_active_version_id_for_country("SSD") == old_version
    assert repo.get_active_version_id_for_country("ETH") == new_version
    assert {country.country_iso3 for country in countries} == {"ETH", "SSD"}
    assert ssd_rows[0].price == 10.0
    assert eth_rows[0].price == 110.0


def test_refresh_lock_blocks_concurrent_refreshes(tmp_path):
    repo = _repo(tmp_path)

    assert repo.acquire_refresh_lock("weekly_full_refresh", "owner-a", 30) is True
    assert repo.acquire_refresh_lock("weekly_full_refresh", "owner-b", 30) is False

    repo.release_refresh_lock("weekly_full_refresh", "owner-a")

    assert repo.acquire_refresh_lock("weekly_full_refresh", "owner-b", 30) is True


def test_publish_cache_version_is_atomic_when_final_update_fails(tmp_path):
    repo = _repo(tmp_path)
    old_version = seed_cache_snapshot(
        repo,
        cache_version_id="11111111-1111-1111-1111-111111111111",
        country_iso3="SSD",
        country_name="South Sudan",
    )
    new_version = repo.create_cache_version(cache_version_id="22222222-2222-2222-2222-222222222222")
    repo.insert_country_snapshot(
        cache_version_id=new_version,
        country_iso3="SSD",
        country_name="South Sudan",
        commodities=[
            {
                "commodity_id": 1,
                "commodity_name": "Maize",
                "commodity_unit_id": 100,
                "commodity_unit_name": "kg",
            }
        ],
        markets=[{"market_id": 10, "market_name": "Juba", "admin1_name": "Central Equatoria"}],
        prices=[
            {
                "country_iso3": "SSD",
                "commodity_id": 1,
                "commodity_name": "Maize",
                "market_id": 10,
                "market_name": "Juba",
                "admin1_name": "Central Equatoria",
                "price_date": "2025-03-01",
                "price": 99,
                "commodity_unit_id": 100,
                "commodity_unit_name": "kg",
                "price_flag": "actual",
            }
        ],
        latest_price_date="2025-03-01",
        currency_code="SSP",
        currency_name="South Sudanese Pound",
    )
    repo.record_country_result(
        cache_version_id=new_version,
        country_iso3="SSD",
        status="success",
        rows_prices=1,
        rows_commodities=1,
        rows_markets=1,
        latest_price_date="2025-03-01",
    )
    with repo.engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TRIGGER force_publish_failure
                BEFORE UPDATE ON price_cache_versions
                WHEN NEW.cache_version_id = '22222222-2222-2222-2222-222222222222'
                BEGIN
                    SELECT RAISE(ABORT, 'forced publish failure');
                END
                """
            )
        )

    with pytest.raises(Exception, match="forced publish failure"):
        repo.publish_cache_version(
            new_version,
            country_iso3s=["SSD"],
            status="active",
            validation_summary={"test": True},
        )

    assert repo.get_active_version_id() == old_version
    assert repo.get_active_version_id_for_country("SSD") == old_version
    assert repo.get_cache_refresh(new_version).status == "building"
