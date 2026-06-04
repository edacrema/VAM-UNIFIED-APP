import os
import sys
import types
from datetime import date

import pytest

import app.services.price_cache.databridges_adapter as adapter_module
from app.services.price_cache.databridges_adapter import (
    DataBridgesAdapterConfig,
    DataBridgesAdapterError,
    DataBridgesClientAdapter,
    DataBridgesNormalizationError,
    load_databridges_adapter_config,
    normalize_monthly_price_row,
)


class ApiError(Exception):
    def __init__(self, status, message):
        super().__init__(message)
        self.status = status


class FakePagedApi:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def monthly(self, **kwargs):
        self.calls.append(kwargs)
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    def commodities(self, **kwargs):
        self.calls.append(kwargs)
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    def units(self, **kwargs):
        self.calls.append(kwargs)
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    def markets(self, **kwargs):
        self.calls.append(kwargs)
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    def currencies(self, **kwargs):
        self.calls.append(kwargs)
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class FakeMarketPricesApi:
    def __init__(self, fake):
        self.fake = fake

    def market_prices_price_monthly_get(self, **kwargs):
        return self.fake.monthly(**kwargs)


class FakeCommoditiesApi:
    def __init__(self, fake):
        self.fake = fake

    def commodities_list_get(self, **kwargs):
        return self.fake.commodities(**kwargs)


class FakeUnitsApi:
    def __init__(self, fake):
        self.fake = fake

    def commodity_units_list_get(self, **kwargs):
        return self.fake.units(**kwargs)


class FakeMarketsApi:
    def __init__(self, fake):
        self.fake = fake

    def markets_list_get(self, **kwargs):
        return self.fake.markets(**kwargs)


class FakeCurrencyApi:
    def __init__(self, fake):
        self.fake = fake

    def currency_list_get(self, **kwargs):
        return self.fake.currencies(**kwargs)


class ModelLike:
    def __init__(self, values):
        self.values = values

    def to_dict(self):
        return dict(self.values)


def _config(**overrides):
    values = {
        "api_key": "key",
        "api_secret": "secret",
        "max_retries": 2,
        "retry_backoff_seconds": 0,
    }
    values.update(overrides)
    return DataBridgesAdapterConfig(**values)


def _adapter(**apis):
    defaults = {
        "market_prices_api": FakeMarketPricesApi(FakePagedApi([])),
        "commodities_api": FakeCommoditiesApi(FakePagedApi([])),
        "commodity_units_api": FakeUnitsApi(FakePagedApi([])),
        "markets_api": FakeMarketsApi(FakePagedApi([])),
        "currency_api": FakeCurrencyApi(FakePagedApi([])),
    }
    defaults.update(apis)
    return DataBridgesClientAdapter(_config(), sleep=lambda _seconds: None, **defaults)


def _price_row(**overrides):
    row = {
        "countryIso3": "SSD",
        "commodityId": 1,
        "commodityName": "Maize",
        "marketId": 10,
        "marketName": "Juba",
        "admin1Name": "Central Equatoria",
        "commodityPriceDate": "2025-02-17",
        "commodityPrice": "12.5",
        "currencyId": 200,
        "currencyCode": "SSP",
        "currencyName": "South Sudanese Pound",
        "commodityUnitId": 100,
        "commodityUnitName": "kg",
        "priceTypeId": 1,
        "priceTypeName": "Retail",
        "commodityPriceFlag": "forecasted",
        "commodityPriceObservations": 4,
        "commodityPriceSourceName": "DataBridges",
    }
    row.update(overrides)
    return row


def test_config_defaults_and_wfp_v2_precedence():
    config = load_databridges_adapter_config(
        {
            "DATA_BRIDGES_KEY": "old-key",
            "DATA_BRIDGES_SECRET": "old-secret",
            "DATA_BRIDGES_API_BASE_URL": "https://old-base",
            "DATA_BRIDGES_ENV": "dev",
            "WFP_V2_API_KEY": "new-key",
            "WFP_V2_API_SECRET": "new-secret",
            "WFP_V2_API_BASE_URL": "https://new-base/",
            "WFP_V2_API_ENV": "prod",
            "DATA_BRIDGES_PAGE_SIZE": "123",
            "DATA_BRIDGES_MAX_WORKERS": "7",
            "DATA_BRIDGES_REQUEST_TIMEOUT_SECONDS": "88",
            "DATA_BRIDGES_MAX_RETRIES": "5",
            "DATA_BRIDGES_RETRY_BACKOFF_SECONDS": "0.5",
        }
    )

    assert config.api_key == "new-key"
    assert config.api_secret == "new-secret"
    assert config.base_url == "https://new-base"
    assert config.env == "prod"
    assert config.page_size == 123
    assert config.max_workers == 7
    assert config.request_timeout_seconds == 88
    assert config.max_retries == 5
    assert config.retry_backoff_seconds == 0.5


def test_generated_client_host_and_wfp_token_are_used(monkeypatch):
    created = {}

    class FakeConfiguration:
        def __init__(self, host, access_token):
            self.host = host
            self.access_token = access_token

    class FakeApiClient:
        def __init__(self, configuration):
            created["configuration"] = configuration

    class FakeToken:
        def __init__(self, api_key, api_secret):
            created["token_args"] = (api_key, api_secret)
            self.refresh_calls = []

        def refresh(self, scopes=None, force=False):
            self.refresh_calls.append({"scopes": scopes, "force": force})
            return "access-token"

    fake_module = types.SimpleNamespace(
        Configuration=FakeConfiguration,
        ApiClient=FakeApiClient,
        MarketPricesApi=lambda api_client: object(),
        CommoditiesApi=lambda api_client: object(),
        CommodityUnitsApi=lambda api_client: object(),
        MarketsApi=lambda api_client: object(),
        CurrencyApi=lambda api_client: object(),
    )
    fake_rest = types.SimpleNamespace(ApiException=ApiError)
    fake_token = types.SimpleNamespace(WfpApiToken=FakeToken)
    monkeypatch.setitem(sys.modules, "data_bridges_client", fake_module)
    monkeypatch.setitem(sys.modules, "data_bridges_client.rest", fake_rest)
    monkeypatch.setitem(sys.modules, "data_bridges_client.token", fake_token)

    client = DataBridgesClientAdapter(_config(base_url="https://gateway.test", scope="scope-a"))

    assert created["token_args"] == ("key", "secret")
    assert client.configuration.host == "https://gateway.test"
    assert created["configuration"].access_token == "access-token"
    assert client._token.refresh_calls == [{"scopes": ["scope-a"], "force": False}]


def test_monthly_pagination_normalizes_rows_and_omits_page_size():
    fake = FakePagedApi(
        [
            {"items": [_price_row(commodityId=1), _price_row(commodityId=2)], "totalItems": 3},
            {"items": [_price_row(commodityId=3)], "totalItems": 3},
        ]
    )
    client = _adapter(market_prices_api=FakeMarketPricesApi(fake))

    rows = client.fetch_monthly_price_rows(
        "SSD",
        commodity_id=1,
        start_date="2025-01-01",
        end_date="2025-12-31",
        latest_value_only=True,
    )

    assert len(rows) == 3
    assert rows[0]["country_iso3"] == "SSD"
    assert rows[0]["price_date"] == date(2025, 2, 1)
    assert rows[0]["price_flag"] == "forecasted"
    assert [call["page"] for call in fake.calls] == [1, 2]
    assert all("page_size" not in call for call in fake.calls)
    assert fake.calls[0]["format"] == "json"
    assert fake.calls[0]["env"] == "prod"
    assert fake.calls[0]["latest_value_only"] is True
    assert fake.calls[0]["_request_timeout"] == 60.0


def test_metadata_fetches_normalize_cache_compatible_rows():
    commodities = FakePagedApi([{"items": [{"id": "1", "name": "Maize", "commodityUnitId": 100}]}, {"items": []}])
    units = FakePagedApi([{"items": [{"commodityUnitId": 100, "commodityUnitName": "kg"}]}, {"items": []}])
    markets = FakePagedApi([{"items": [{"marketId": 10, "marketName": "Juba", "admin1Name": "Central"}]}, {"items": []}])
    currencies = FakePagedApi([{"items": [{"currencyId": 200, "currencyCode": "SSP", "currencyName": "Pound"}]}, {"items": []}])
    client = _adapter(
        commodities_api=FakeCommoditiesApi(commodities),
        commodity_units_api=FakeUnitsApi(units),
        markets_api=FakeMarketsApi(markets),
        currency_api=FakeCurrencyApi(currencies),
    )

    assert client.fetch_commodities("ssd")[0] == {
        "country_iso3": "SSD",
        "commodity_id": 1,
        "commodity_name": "Maize",
        "commodity_unit_id": 100,
        "commodity_unit_name": None,
        "category_name": None,
        "active": True,
    }
    assert client.fetch_units()[0]["commodity_unit_name"] == "kg"
    assert client.fetch_markets("ssd")[0]["admin1_name"] == "Central"
    assert client.fetch_currencies()[0]["currency_code"] == "SSP"


def test_normalization_accepts_model_like_objects_and_hash_is_deterministic():
    row_a = _price_row(commodityPrice="9.5")
    row_b = dict(reversed(list(row_a.items())))

    normalized_a = normalize_monthly_price_row(ModelLike(row_a), country_iso3="SSD")
    normalized_b = normalize_monthly_price_row(row_b, country_iso3="SSD")

    assert normalized_a["price"] == 9.5
    assert normalized_a["source_payload_hash"] == normalized_b["source_payload_hash"]


def test_missing_required_price_fields_raise_normalization_error():
    row = _price_row()
    row.pop("marketId")

    with pytest.raises(DataBridgesNormalizationError, match="market_id"):
        normalize_monthly_price_row(row, country_iso3="SSD")


def test_transient_failures_retry_and_permanent_failures_surface():
    transient = FakePagedApi([ApiError(500, "try again"), {"items": [_price_row()], "totalItems": 1}])
    client = _adapter(market_prices_api=FakeMarketPricesApi(transient))

    assert len(client.fetch_monthly_price_rows("SSD")) == 1
    assert len(transient.calls) == 2

    permanent = FakePagedApi([ApiError(400, "bad request")])
    client = _adapter(market_prices_api=FakeMarketPricesApi(permanent))
    with pytest.raises(DataBridgesAdapterError, match="bad request"):
        client.fetch_monthly_price_rows("SSD")
    assert len(permanent.calls) == 1


def test_auth_401_refreshes_token_once_before_retry():
    fake = FakePagedApi([ApiError(401, "expired"), {"items": [_price_row()], "totalItems": 1}])
    client = _adapter(market_prices_api=FakeMarketPricesApi(fake))

    class FakeToken:
        def __init__(self):
            self.calls = []

        def refresh(self, scopes=None, force=False):
            self.calls.append({"scopes": scopes, "force": force})
            return "fresh-token"

    class FakeConfiguration:
        access_token = "old-token"

    token = FakeToken()
    client._token = token
    client._configuration = FakeConfiguration()

    rows = client.fetch_monthly_price_rows("SSD")

    assert len(rows) == 1
    assert token.calls == [{"scopes": ["api://wfp-api-mediation-service/.default"], "force": True}]
    assert client._configuration.access_token == "fresh-token"


def test_whole_cache_dry_run_uses_app_countries_unbounded_dates_and_reports_failures(monkeypatch):
    monkeypatch.setattr(
        adapter_module,
        "supported_country_options",
        lambda: [{"iso3": "AAA"}, {"iso3": "BBB"}],
    )

    class DryRunAdapter(DataBridgesClientAdapter):
        def __init__(self):
            pass

        config = _config(max_workers=1)

        def _fetch_units_result(self):
            return adapter_module.PagedFetchResult(rows=[{"unit": 1}], pages=1)

        def _fetch_currencies_result(self):
            return adapter_module.PagedFetchResult(rows=[{"currency": 1}], pages=1)

        def _fetch_commodities_result(self, country_iso3):
            return adapter_module.PagedFetchResult(rows=[{"commodity": country_iso3}], pages=1)

        def _fetch_markets_result(self, country_iso3):
            return adapter_module.PagedFetchResult(rows=[{"market": country_iso3}], pages=1)

        def _fetch_monthly_price_rows_result(self, country_iso3, **kwargs):
            assert kwargs["start_date"] is None
            assert kwargs["end_date"] is None
            if country_iso3 == "BBB":
                raise RuntimeError("country failed")
            return adapter_module.PagedFetchResult(rows=[{"price": 1}, {"price": 2}], pages=2)

    summary = DryRunAdapter().dry_run_full_cache(max_workers=1)

    assert summary["countries_requested"] == 2
    assert summary["countries_completed"] == 1
    assert summary["countries_failed"] == 1
    assert summary["rows_prices"] == 2
    assert summary["rows_units"] == 1
    assert summary["rows_currencies"] == 1
    assert summary["errors"] == [{"country_iso3": "BBB", "error": "country failed"}]


@pytest.mark.skipif(
    not (
        os.getenv("RUN_DATABRIDGES_LIVE") == "1"
        and os.getenv("WFP_V2_API_KEY")
        and os.getenv("WFP_V2_API_SECRET")
    ),
    reason="Set RUN_DATABRIDGES_LIVE=1 with WFP v2 credentials to run live DataBridges smoke test.",
)
def test_live_databridges_adapter_smoke():
    client = DataBridgesClientAdapter()

    rows = client.fetch_monthly_price_rows("SSD", latest_value_only=True)

    assert rows
