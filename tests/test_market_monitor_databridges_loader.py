import pandas as pd

from app.services.market_monitor import data_loader


class FakePriceClient:
    def __init__(
        self,
        *,
        reverse_prices=False,
        include_latest_beans=True,
        duplicate_commodities=False,
    ):
        self.reverse_prices = reverse_prices
        self.include_latest_beans = include_latest_beans
        self.duplicate_commodities = duplicate_commodities
        self.price_calls = []

    def list_commodities(self, country_code, commodity_name=None, commodity_id=None):
        rows = [
            {"id": 1, "name": "Maize"},
            {"id": 2, "name": "Beans"},
        ]
        if self.duplicate_commodities:
            rows = [
                {"id": 3, "name": "Maize"},
                {"id": 1, "name": "Maize"},
                {"id": 2, "name": "Beans"},
            ]
        if commodity_id is not None:
            return [row for row in rows if row["id"] == commodity_id]
        return rows

    def list_markets(self, country_code):
        return [
            {"marketId": 10, "marketName": "Juba", "admin1Name": "Central Equatoria"},
            {"marketId": 11, "marketName": "Wau", "admin1Name": "Western Bahr el Ghazal"},
        ]

    def list_monthly_prices(
        self,
        country_code,
        commodity_id=None,
        start_date=None,
        end_date=None,
        latest_value_only=False,
        price_flag=None,
        price_type_name=None,
    ):
        self.price_calls.append(
            {
                "country_code": country_code,
                "commodity_id": commodity_id,
                "start_date": start_date,
                "end_date": end_date,
                "latest_value_only": latest_value_only,
            }
        )
        rows = [
            _price(1, "Maize", 10, "2024-01-01", 5),
            _price(2, "Beans", 11, "2024-01-01", 2),
            _price(1, "Maize", 10, "2025-01-01", 10),
            *([_price(2, "Beans", 11, "2025-01-01", 5)] if self.include_latest_beans else []),
            _price(3, "Maize", 10, "2025-01-01", 30),
            _price(1, "Maize", 10, "2025-01-01", 99, flag="forecasted"),
        ]
        if commodity_id is not None:
            rows = [row for row in rows if row["commodityId"] == commodity_id]
        if latest_value_only:
            rows = [row for row in rows if row["commodityPriceDate"] == "2025-01-01"]
        if self.reverse_prices:
            rows = list(reversed(rows))
        return rows


def _price(commodity_id, commodity, market_id, date, price, flag="actual"):
    return {
        "countryName": "South Sudan",
        "countryIso3": "SSD",
        "commodityId": commodity_id,
        "commodityName": commodity,
        "marketId": market_id,
        "marketName": "market",
        "commodityPriceDate": date,
        "commodityPrice": price,
        "commodityPriceFlag": flag,
        "priceTypeName": "Retail",
        "commodityUnitName": "kg",
        "currencyName": "SSP",
    }


def test_country_metadata_uses_databridges(monkeypatch):
    data_loader.reset_market_monitor_caches_for_tests()
    client = FakePriceClient()
    monkeypatch.setattr(data_loader, "get_databridges_client", lambda: client)
    monkeypatch.setattr(data_loader, "_recent_price_window", lambda months=36: ("2023-04-01", "2026-04-15"))

    metadata = data_loader.get_country_metadata("South Sudan")

    assert metadata["iso3"] == "SSD"
    assert [item["name"] for item in metadata["commodities"]] == ["Beans", "Maize"]
    assert metadata["regions"] == ["Central Equatoria", "Western Bahr el Ghazal"]
    assert metadata["date_range"] == {"start": "2024-01-01", "end": "2025-01-01"}
    assert metadata["metadata_price_window"]["bounded"] is True
    assert client.price_calls[0]["start_date"] == "2023-04-01"
    assert client.price_calls[0]["end_date"] == "2026-04-15"


def test_time_series_preserves_missing_months_and_current_statistics(monkeypatch):
    data_loader.reset_market_monitor_caches_for_tests()
    monkeypatch.setattr(data_loader, "get_databridges_client", lambda: FakePriceClient())

    national, regional = data_loader.extract_time_series_from_csv(
        "South Sudan",
        "2025-01",
        ["Maize", "Beans"],
        [],
    )
    stats = data_loader.calculate_statistics_from_csv(national, ["Maize", "Beans"])

    assert len(national) == 13
    assert national.index[0].strftime("%Y-%m") == "2024-01"
    assert national.index[-1].strftime("%Y-%m") == "2025-01"
    assert national.loc["2024-02-01", "FoodBasket"] != national.loc["2024-02-01", "FoodBasket"]
    assert national.loc["2025-01-01", "FoodBasket"] == 15
    assert regional["Region"].nunique() == 2
    assert stats["food_basket"]["current_price"] == 15
    assert stats["food_basket"]["yoy_change_pct"] == 114.3
    assert stats["food_basket"]["latest_component_names"] == ["Maize", "Beans"]


def test_availability_reports_missing_commodities_without_mock(monkeypatch):
    data_loader.reset_market_monitor_caches_for_tests()
    monkeypatch.setattr(data_loader, "get_databridges_client", lambda: FakePriceClient())

    availability = data_loader.check_data_availability(
        "South Sudan",
        "2025-01",
        ["Rice"],
    )

    assert availability["available"] is True
    assert availability["missing_commodities"] == ["Rice"]
    assert "Rice" in availability["warnings"][0]


def test_reversed_api_row_order_produces_identical_time_series(monkeypatch):
    data_loader.reset_market_monitor_caches_for_tests()
    normal_client = FakePriceClient(reverse_prices=False)
    monkeypatch.setattr(data_loader, "get_databridges_client", lambda: normal_client)
    normal_national, normal_regional = data_loader.extract_time_series_from_csv(
        "South Sudan",
        "2025-01",
        ["Maize", "Beans"],
        [],
    )

    data_loader.reset_market_monitor_caches_for_tests()
    reversed_client = FakePriceClient(reverse_prices=True)
    monkeypatch.setattr(data_loader, "get_databridges_client", lambda: reversed_client)
    reversed_national, reversed_regional = data_loader.extract_time_series_from_csv(
        "South Sudan",
        "2025-01",
        ["Maize", "Beans"],
        [],
    )

    pd.testing.assert_frame_equal(normal_national, reversed_national)
    pd.testing.assert_frame_equal(normal_regional, reversed_regional)


def test_duplicate_commodity_name_prefers_lower_id(monkeypatch):
    data_loader.reset_market_monitor_caches_for_tests()
    client = FakePriceClient(duplicate_commodities=True)
    monkeypatch.setattr(data_loader, "get_databridges_client", lambda: client)

    national, _regional = data_loader.extract_time_series_from_csv(
        "South Sudan",
        "2025-01",
        ["Maize"],
        [],
    )

    assert national.loc["2025-01-01", "Maize"] == 10
    commodity_calls = [call["commodity_id"] for call in client.price_calls if call["commodity_id"] is not None]
    assert commodity_calls == [1]


def test_food_basket_coverage_only_counts_latest_month_contributors(monkeypatch):
    data_loader.reset_market_monitor_caches_for_tests()
    monkeypatch.setattr(data_loader, "get_databridges_client", lambda: FakePriceClient(include_latest_beans=False))

    national, _regional = data_loader.extract_time_series_from_csv(
        "South Sudan",
        "2025-01",
        ["Maize", "Beans"],
        [],
    )
    stats = data_loader.calculate_statistics_from_csv(national, ["Maize", "Beans"])

    assert stats["food_basket"]["current_price"] == 10
    assert stats["food_basket"]["selected_component_count"] == 2
    assert stats["food_basket"]["historical_component_count"] == 2
    assert stats["food_basket"]["latest_component_count"] == 1
    assert stats["food_basket"]["latest_component_names"] == ["Maize"]
    assert stats["food_basket"]["missing_latest_component_names"] == ["Beans"]
