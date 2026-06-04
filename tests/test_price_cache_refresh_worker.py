from pathlib import Path
from types import SimpleNamespace

import pytest

from app.services.price_cache.config import load_price_cache_config
from app.services.price_cache.fixtures import seed_cache_snapshot
from app.services.price_cache.migrations import apply_migrations
from app.services.price_cache.refresh_worker import PriceCacheRefreshWorker, RefreshLockUnavailable
from app.services.price_cache.sql_repository import SqlPriceCacheRepository, create_price_cache_engine
from app.services.price_cache.validation import validate_country_snapshot


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


def _config():
    return load_price_cache_config(
        {
            "PRICE_CACHE_BACKEND": "sqlite",
            "PRICE_CACHE_REFRESH_ENABLED": "true",
            "PRICE_CACHE_REFRESH_LOCK_TIMEOUT_MINUTES": "30",
            "PRICE_CACHE_VALIDATE_MAX_COUNTRY_DROP_RATIO": "0.25",
        }
    )


class FakeAdapter:
    def __init__(self, *, fail_countries=None, duplicate_countries=None, fail_units=False):
        self.config = SimpleNamespace(base_url="https://databridges.test", env="prod")
        self.fail_countries = set(fail_countries or [])
        self.duplicate_countries = set(duplicate_countries or [])
        self.fail_units = fail_units

    def fetch_units(self):
        if self.fail_units:
            raise RuntimeError("403 Forbidden for CommodityUnits/List")
        return [{"commodity_unit_id": 100, "commodity_unit_name": "kg", "active": True}]

    def fetch_currencies(self):
        return [{"currency_id": 200, "currency_code": "SSP", "currency_name": "Pound"}]

    def fetch_commodities(self, country_iso3):
        self._maybe_fail(country_iso3)
        return [
            {
                "country_iso3": country_iso3,
                "commodity_id": 1,
                "commodity_name": "Maize",
                "commodity_unit_id": 100,
                "commodity_unit_name": "kg",
                "active": True,
            }
        ]

    def fetch_markets(self, country_iso3):
        self._maybe_fail(country_iso3)
        return [
            {
                "country_iso3": country_iso3,
                "market_id": 10,
                "market_name": f"{country_iso3} Market",
                "admin1_name": "Central",
                "active": True,
            }
        ]

    def fetch_monthly_price_rows(self, country_iso3, **kwargs):
        self._maybe_fail(country_iso3)
        rows = [
            {
                "country_iso3": country_iso3,
                "commodity_id": 1,
                "commodity_name": "Maize",
                "market_id": 10,
                "market_name": f"{country_iso3} Market",
                "admin1_name": "Central",
                "price_date": "2025-01-01",
                "price": 20.0,
                "currency_id": 200,
                "currency_code": "SSP",
                "currency_name": "Pound",
                "commodity_unit_id": 100,
                "commodity_unit_name": "kg",
                "price_type_id": 1,
                "price_type_name": "Retail",
                "price_flag": "actual",
                "source_payload_hash": f"{country_iso3}-1",
                "data_source": "fake",
            }
        ]
        if country_iso3 in self.duplicate_countries:
            rows.append(dict(rows[0], source_payload_hash=f"{country_iso3}-duplicate"))
        return rows

    def _maybe_fail(self, country_iso3):
        if country_iso3 in self.fail_countries:
            raise RuntimeError(f"{country_iso3} failed")


def _worker(repo, adapter, country_options):
    return PriceCacheRefreshWorker(
        repository=repo,
        adapter=adapter,
        config=_config(),
        country_options=country_options,
    )


def test_refresh_worker_promotes_all_successful_countries(tmp_path):
    repo = _repo(tmp_path)
    worker = _worker(
        repo,
        FakeAdapter(),
        [
            {"iso3": "AAA", "name": "Alpha", "currency_code": "AAA", "currency_name": "Alpha Currency"},
            {"iso3": "BBB", "name": "Beta", "currency_code": "BBB", "currency_name": "Beta Currency"},
        ],
    )

    summary = worker.run(triggered_by="pytest")

    assert summary["status"] == "active"
    assert summary["countries_successful"] == 2
    assert repo.get_cache_status().active_country_count == 2
    assert repo.get_active_version_id_for_country("AAA") == summary["cache_version_id"]
    assert repo.get_price_window("BBB", "2025-01-01", "2025-01-01")[0].price == 20.0


def test_refresh_worker_partial_success_keeps_failed_country_previous_data(tmp_path):
    repo = _repo(tmp_path)
    old_bbb = seed_cache_snapshot(
        repo,
        cache_version_id="11111111-1111-1111-1111-111111111111",
        country_iso3="BBB",
        country_name="Beta",
        price_offset=0,
    )
    worker = _worker(
        repo,
        FakeAdapter(fail_countries={"BBB"}),
        [
            {"iso3": "AAA", "name": "Alpha", "currency_code": "AAA", "currency_name": "Alpha Currency"},
            {"iso3": "BBB", "name": "Beta", "currency_code": "BBB", "currency_name": "Beta Currency"},
        ],
    )

    summary = worker.run(triggered_by="pytest")

    assert summary["status"] == "partial_active"
    assert summary["countries_successful"] == 1
    assert summary["countries_failed"] == 1
    assert repo.get_active_version_id_for_country("AAA") == summary["cache_version_id"]
    assert repo.get_active_version_id_for_country("BBB") == old_bbb
    assert repo.get_price_window("BBB", "2025-01-01", "2025-01-01", commodity_ids=[1])[0].price == 10.0


def test_refresh_worker_total_failure_leaves_active_cache_unchanged(tmp_path):
    repo = _repo(tmp_path)
    old_aaa = seed_cache_snapshot(
        repo,
        cache_version_id="11111111-1111-1111-1111-111111111111",
        country_iso3="AAA",
        country_name="Alpha",
        price_offset=0,
    )
    worker = _worker(
        repo,
        FakeAdapter(fail_countries={"AAA"}),
        [{"iso3": "AAA", "name": "Alpha", "currency_code": "AAA", "currency_name": "Alpha Currency"}],
    )

    summary = worker.run(triggered_by="pytest")

    assert summary["status"] == "failed"
    assert summary["countries_successful"] == 0
    assert repo.get_active_version_id_for_country("AAA") == old_aaa


def test_refresh_worker_unit_endpoint_failure_derives_units_and_warns(tmp_path):
    repo = _repo(tmp_path)
    worker = _worker(
        repo,
        FakeAdapter(fail_units=True),
        [{"iso3": "AAA", "name": "Alpha", "currency_code": "AAA", "currency_name": "Alpha Currency"}],
    )

    summary = worker.run(triggered_by="pytest")
    metadata = repo.get_country_metadata("AAA")
    refresh = repo.get_cache_refresh(summary["cache_version_id"])

    assert summary["status"] == "active"
    assert summary["rows_units"] == 1
    assert "CommodityUnits/List" in summary["warnings"][0]["warning"]
    assert metadata is not None
    assert [unit.commodity_unit_name for unit in metadata.units] == ["kg"]
    assert "CommodityUnits/List" in refresh.validation_summary["warnings"][0]["warning"]


def test_refresh_worker_lock_contention_raises(tmp_path):
    repo = _repo(tmp_path)
    repo.acquire_refresh_lock("weekly_full_refresh", "other-owner", 30)
    worker = _worker(
        repo,
        FakeAdapter(),
        [{"iso3": "AAA", "name": "Alpha", "currency_code": "AAA", "currency_name": "Alpha Currency"}],
    )

    with pytest.raises(RefreshLockUnavailable):
        worker.run(triggered_by="pytest")


def test_validation_catches_duplicate_canonical_keys():
    adapter = FakeAdapter(duplicate_countries={"AAA"})
    result = validate_country_snapshot(
        country_iso3="AAA",
        prices=adapter.fetch_monthly_price_rows("AAA"),
        commodities=adapter.fetch_commodities("AAA"),
        markets=adapter.fetch_markets("AAA"),
    )

    assert result.valid is False
    assert "duplicate canonical" in result.errors[0]
