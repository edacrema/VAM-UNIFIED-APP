from datetime import date, datetime, timezone
from importlib import import_module

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.services.market_monitor import data_loader
from app.services.price_cache.schemas import (
    CacheRefreshSummary,
    CacheStatus,
    CommodityRecord,
    CountryAvailability,
    CountryMetadata,
    CountryRecord,
    CountryRefreshStatus,
    CurrencyRecord,
    MarketRecord,
    UnitRecord,
)


market_router = import_module("app.services.market_monitor.router")


class FakeRepo:
    def get_cache_status(self):
        return CacheStatus(
            has_active_cache=True,
            active_version_id="11111111-1111-1111-1111-111111111111",
            status="partial_active",
            active_country_count=2,
            rows_prices=10,
            validation_summary={
                "warnings": [
                    {
                        "scope": "units",
                        "warning": "CommodityUnits/List could not be fetched; units were derived.",
                    }
                ]
            },
        )

    def list_countries(self):
        return [
            CountryRecord(
                cache_version_id="11111111-1111-1111-1111-111111111111",
                country_iso3="SSD",
                country_name="South Sudan",
                currency_code="SSP",
                currency_name="South Sudanese Pound",
                latest_price_date=date(2025, 2, 1),
            )
        ]

    def get_country_availability(self, country_iso3):
        if str(country_iso3).upper() != "SSD":
            return None
        return CountryAvailability(
            cache_version_id="11111111-1111-1111-1111-111111111111",
            country_iso3="SSD",
            date_start=date(2025, 1, 1),
            date_end=date(2025, 2, 1),
            latest_price_date=date(2025, 2, 1),
            priced_commodity_ids=[1, 2],
            admin1_names=["Central Equatoria", "Western Bahr el Ghazal"],
            units=[
                UnitRecord(
                    cache_version_id="11111111-1111-1111-1111-111111111111",
                    commodity_unit_id=100,
                    commodity_unit_name="kg",
                )
            ],
        )

    def get_country_metadata(self, country_iso3):
        if str(country_iso3).upper() != "SSD":
            return None
        version_id = "11111111-1111-1111-1111-111111111111"
        return CountryMetadata(
            country=CountryRecord(
                cache_version_id=version_id,
                country_iso3="SSD",
                country_name="South Sudan",
                currency_code="SSP",
                currency_name="South Sudanese Pound",
                latest_price_date=date(2025, 2, 1),
            ),
            commodities=[
                CommodityRecord(version_id, "SSD", 1, "Maize", 100, "kg", "Cereals"),
                CommodityRecord(version_id, "SSD", 2, "Beans", 100, "kg", "Pulses"),
            ],
            units=[UnitRecord(version_id, 100, "kg")],
            markets=[
                MarketRecord(version_id, "SSD", 10, "Juba", "Central Equatoria"),
                MarketRecord(version_id, "SSD", 11, "Wau", "Western Bahr el Ghazal"),
            ],
            currencies=[CurrencyRecord(version_id, 200, "SSP", "South Sudanese Pound")],
        )

    def list_cache_refreshes(self, *, limit=20):
        return [
            CacheRefreshSummary(
                cache_version_id="11111111-1111-1111-1111-111111111111",
                status="partial_active",
                refresh_type="weekly_full",
                started_at=datetime(2026, 6, 3, tzinfo=timezone.utc),
                rows_prices=10,
            )
        ]

    def get_cache_refresh(self, cache_version_id):
        if cache_version_id == "missing":
            return None
        return CacheRefreshSummary(
            cache_version_id=cache_version_id,
            status="partial_active",
            refresh_type="weekly_full",
            started_at=datetime(2026, 6, 3, tzinfo=timezone.utc),
            rows_prices=10,
            countries=[
                CountryRefreshStatus(
                    cache_version_id=cache_version_id,
                    country_iso3="SSD",
                    status="success",
                    rows_prices=10,
                )
            ],
        )


def _client(monkeypatch):
    app = FastAPI()
    app.include_router(market_router.router)
    monkeypatch.setattr(market_router, "_get_price_cache_repository", lambda: FakeRepo())
    monkeypatch.setattr(data_loader, "_get_price_cache_repository", lambda: FakeRepo())
    data_loader.reset_market_monitor_caches_for_tests()
    return TestClient(app)


def test_cache_status_endpoint(monkeypatch):
    client = _client(monkeypatch)

    response = client.get("/cache/status")

    assert response.status_code == 200
    assert response.json()["status"] == "partial_active"
    assert response.json()["active_country_count"] == 2


def test_cache_refresh_history_endpoints(monkeypatch):
    client = _client(monkeypatch)

    list_response = client.get("/cache/refreshes")
    detail_response = client.get("/cache/refreshes/11111111-1111-1111-1111-111111111111")
    missing_response = client.get("/cache/refreshes/missing")

    assert list_response.status_code == 200
    assert list_response.json()["refreshes"][0]["status"] == "partial_active"
    assert detail_response.status_code == 200
    assert detail_response.json()["countries"][0]["country_iso3"] == "SSD"
    assert missing_response.status_code == 404


def test_countries_endpoint_reads_cached_countries(monkeypatch):
    client = _client(monkeypatch)

    response = client.get("/countries")

    assert response.status_code == 200
    payload = response.json()
    assert payload["cache_status"]["source"] == "PriceCache"
    assert payload["warnings"][0].startswith("CommodityUnits/List")
    assert payload["countries"] == [
        {
            "name": "South Sudan",
            "iso3": "SSD",
            "currency_code": "SSP",
            "currency_name": "South Sudanese Pound",
            "has_data": True,
            "source": "PriceCache",
            "cache_version_id": "11111111-1111-1111-1111-111111111111",
            "latest_cached_date": "2025-02-01",
            "date_range": {"start": "2025-01-01", "end": "2025-02-01"},
        }
    ]


def test_country_metadata_endpoint_reads_cache_without_databridges(monkeypatch):
    def fail_if_live_client_is_used():
        raise AssertionError("DataBridges should not be called by cache-first metadata.")

    monkeypatch.setattr(data_loader, "get_databridges_client", fail_if_live_client_is_used, raising=False)
    client = _client(monkeypatch)

    response = client.get("/countries/South%20Sudan/metadata")

    assert response.status_code == 200
    payload = response.json()
    assert payload["source"] == "PriceCache"
    assert payload["cache_version_id"] == "11111111-1111-1111-1111-111111111111"
    assert payload["latest_cached_date"] == "2025-02-01"
    assert payload["date_range"] == {"start": "2025-01-01", "end": "2025-02-01"}
    assert [item["name"] for item in payload["commodities"]] == ["Beans", "Maize"]
    assert payload["units"] == [
        {"id": 100, "name": "kg", "conversion_to_kg_l": None, "source": "cached_units"}
    ]
    assert payload["markets"][0]["market_name"] == "Juba"
