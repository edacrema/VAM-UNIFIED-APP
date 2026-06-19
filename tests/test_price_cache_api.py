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
                CommodityRecord(version_id, "SSD", 3, "Rice", 100, "kg", "Cereals"),
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
    payload = response.json()
    assert payload["status"] == "partial_active"
    assert payload["active_country_count"] == 2
    assert payload["warnings"] == []
    assert payload["operator_warnings"][0].startswith("CommodityUnits/List")


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
    assert payload["warnings"] == []
    assert payload["operator_warnings"][0].startswith("CommodityUnits/List")
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
    assert all(item["priced"] is True for item in payload["commodities"])
    assert payload["unpriced_commodities"] == [
        {
            "id": 3,
            "name": "Rice",
            "category": "Cereals",
            "unit_id": 100,
            "unit": "kg",
            "unit_name": "kg",
            "priced": False,
        }
    ]
    assert payload["units"] == [
        {"id": 100, "name": "kg", "conversion_to_kg_l": None, "source": "cached_units"}
    ]
    assert payload["markets"][0]["market_name"] == "Juba"


def test_country_basket_endpoint_returns_needs_setup(monkeypatch):
    monkeypatch.setattr(
        market_router,
        "get_country_basket_response",
        lambda country: {
            "country": country,
            "iso3": "SSD",
            "needs_setup": True,
            "active_basket": None,
        },
    )
    client = _client(monkeypatch)

    response = client.get("/countries/South%20Sudan/basket")

    assert response.status_code == 200
    assert response.json()["needs_setup"] is True


def test_country_basket_save_endpoint_validates_payload_shape(monkeypatch):
    captured = {}

    def fake_save(country, input_data):
        captured["country"] = country
        captured["commodity_id"] = input_data.items[0].commodity_id
        captured["weight_quantity"] = input_data.items[0].weight_quantity
        captured["created_by_user_id"] = input_data.created_by_user_id
        return {
            "country": country,
            "iso3": "SSD",
            "needs_setup": False,
            "active_basket": {
                "basket_version_id": "new-version",
                "version_number": 1,
                "items": [],
            },
        }

    monkeypatch.setattr(market_router, "save_country_basket", fake_save)
    client = _client(monkeypatch)

    response = client.post(
        "/countries/South%20Sudan/basket",
        json={
            "items": [{"commodity_id": 1, "weight_quantity": 2}],
            "created_by_user_id": "tester",
        },
    )

    assert response.status_code == 200
    assert response.json()["active_basket"]["basket_version_id"] == "new-version"
    assert captured == {
        "country": "South Sudan",
        "commodity_id": 1,
        "weight_quantity": 2.0,
        "created_by_user_id": "tester",
    }


def test_country_basket_history_endpoint_returns_newest_first(monkeypatch):
    monkeypatch.setattr(
        market_router,
        "list_country_basket_history",
        lambda country, limit=20: {
            "country": country,
            "iso3": "SSD",
            "versions": [
                {"basket_version_id": "v2", "version_number": 2},
                {"basket_version_id": "v1", "version_number": 1},
            ],
        },
    )
    client = _client(monkeypatch)

    response = client.get("/countries/South%20Sudan/basket/history?limit=2")

    assert response.status_code == 200
    assert [item["version_number"] for item in response.json()["versions"]] == [2, 1]


def test_generate_async_returns_conflict_for_stale_basket_version(monkeypatch):
    def stale_basket(_country, _basket_version_id=None):
        raise market_router.BasketVersionConflict("refresh basket")

    monkeypatch.setattr(market_router, "get_active_basket_for_report", stale_basket)
    client = _client(monkeypatch)

    response = client.post(
        "/generate-async",
        json={
            "country": "South Sudan",
            "time_period": "2025-02",
            "commodity_list": ["Maize"],
            "admin1_list": [],
            "currency_code": "SSP",
            "enabled_modules": [],
            "basket_version_id": "old-version",
        },
    )

    assert response.status_code == 409
    assert "refresh basket" in response.json()["detail"]
