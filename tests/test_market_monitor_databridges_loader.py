from pathlib import Path
from datetime import date
from types import SimpleNamespace

import pandas as pd
import pytest
from sqlalchemy import text

from app.services.market_monitor import data_loader
from app.services.price_cache.config import load_price_cache_config
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


def _patch_repo(monkeypatch, repo: SqlPriceCacheRepository) -> None:
    data_loader.reset_market_monitor_caches_for_tests()
    monkeypatch.setattr(data_loader, "_get_price_cache_repository", lambda: repo)


def _seed_loader_cache(
    repo: SqlPriceCacheRepository,
    *,
    include_latest_beans: bool = True,
    include_rice: bool = False,
    duplicate_commodities: bool = False,
    price_offset: float = 0.0,
) -> str:
    version_id = repo.create_cache_version()
    repo.insert_currencies(
        version_id,
        [{"currency_id": 200, "currency_code": "SSP", "currency_name": "South Sudanese Pound"}],
    )
    commodities = [
        {
            "commodity_id": 1,
            "commodity_name": "Maize",
            "commodity_unit_id": 100,
            "commodity_unit_name": "kg",
            "category_name": "Cereals",
        },
        {
            "commodity_id": 2,
            "commodity_name": "Beans",
            "commodity_unit_id": 100,
            "commodity_unit_name": "kg",
            "category_name": "Pulses",
        },
    ]
    if include_rice:
        commodities.append(
            {
                "commodity_id": 4,
                "commodity_name": "Rice",
                "commodity_unit_id": 100,
                "commodity_unit_name": "kg",
                "category_name": "Cereals",
            }
        )
    if duplicate_commodities:
        commodities.append(
            {
                "commodity_id": 3,
                "commodity_name": "Maize",
                "commodity_unit_id": 100,
                "commodity_unit_name": "kg",
                "category_name": "Cereals",
            }
        )
    markets = [
        {"market_id": 10, "market_name": "Juba", "admin1_name": "Central Equatoria"},
        {"market_id": 11, "market_name": "Wau", "admin1_name": "Western Bahr el Ghazal"},
    ]
    prices = [
        _price(1, "Maize", 10, "Juba", "Central Equatoria", "2024-02-01", 10 + price_offset),
        _price(2, "Beans", 11, "Wau", "Western Bahr el Ghazal", "2024-02-01", 5 + price_offset),
        _price(1, "Maize", 10, "Juba", "Central Equatoria", "2025-02-01", 12 + price_offset),
    ]
    if include_latest_beans:
        prices.append(_price(2, "Beans", 11, "Wau", "Western Bahr el Ghazal", "2025-02-01", 6 + price_offset))
    if include_rice:
        prices.append(_price(4, "Rice", 10, "Juba", "Central Equatoria", "2024-02-01", 4 + price_offset))
        prices.append(_price(4, "Rice", 10, "Juba", "Central Equatoria", "2025-02-01", 7 + price_offset))
    if duplicate_commodities:
        prices.append(_price(3, "Maize", 10, "Juba", "Central Equatoria", "2025-02-01", 30 + price_offset))
    prices.append(
        _price(
            1,
            "Maize",
            10,
            "Juba",
            "Central Equatoria",
            "2025-02-01",
            99 + price_offset,
            flag="forecasted",
        )
    )

    repo.insert_country_snapshot(
        cache_version_id=version_id,
        country_iso3="SSD",
        country_name="South Sudan",
        commodities=commodities,
        markets=markets,
        prices=prices,
        latest_price_date="2025-02-01",
        currency_code="SSP",
        currency_name="South Sudanese Pound",
    )
    repo.record_country_result(
        cache_version_id=version_id,
        country_iso3="SSD",
        status="success",
        rows_prices=len(prices),
        rows_commodities=len(commodities),
        rows_markets=len(markets),
        latest_price_date="2025-02-01",
    )
    repo.insert_units(
        version_id,
        [{"commodity_unit_id": 100, "commodity_unit_name": "kg", "conversion_to_kg_l": 1.0, "active": True}],
    )
    repo.publish_cache_version(
        version_id,
        country_iso3s=["SSD"],
        status="active",
        validation_summary={"fixture": True},
    )
    return version_id


def _price(commodity_id, commodity, market_id, market, admin1, price_date, price, flag="actual"):
    return {
        "country_iso3": "SSD",
        "commodity_id": commodity_id,
        "commodity_name": commodity,
        "market_id": market_id,
        "market_name": market,
        "admin1_name": admin1,
        "price_date": price_date,
        "price": price,
        "currency_id": 200,
        "currency_code": "SSP",
        "currency_name": "South Sudanese Pound",
        "commodity_unit_id": 100,
        "commodity_unit_name": "kg",
        "price_type_id": 1,
        "price_type_name": "Retail",
        "price_flag": flag,
        "observations": 1,
        "source_payload_hash": f"{commodity_id}-{market_id}-{price_date}-{flag}",
        "data_source": "fixture",
    }


def _basket_items():
    return [
        {
            "commodity_id": 1,
            "commodity_name_snapshot": "Maize",
            "databridges_unit": "kg",
            "weight_quantity": 2,
        },
        {
            "commodity_id": 2,
            "commodity_name_snapshot": "Beans",
            "databridges_unit": "kg",
            "weight_quantity": 3,
        },
    ]


class _FakeBackfillAdapter:
    def __init__(self, rows_by_commodity=None, *, error_by_commodity=None):
        self.rows_by_commodity = rows_by_commodity or {}
        self.error_by_commodity = error_by_commodity or {}
        self.config = type("Config", (), {"max_workers": 8})()
        self.calls = []

    def fetch_commodities(self, country_iso3):
        return [
            {
                "country_iso3": country_iso3,
                "commodity_id": 1,
                "commodity_name": "Maize",
                "commodity_unit_id": 100,
                "commodity_unit_name": "kg",
                "category_name": "Cereals",
            },
            {
                "country_iso3": country_iso3,
                "commodity_id": 2,
                "commodity_name": "Beans",
                "commodity_unit_id": 100,
                "commodity_unit_name": "kg",
                "category_name": "Pulses",
            },
            {
                "country_iso3": country_iso3,
                "commodity_id": 52,
                "commodity_name": "Rice",
                "commodity_unit_id": 100,
                "commodity_unit_name": "kg",
                "category_name": "Cereals",
            },
            {
                "country_iso3": country_iso3,
                "commodity_id": 65,
                "commodity_name": "Sorghum",
                "commodity_unit_id": 100,
                "commodity_unit_name": "kg",
                "category_name": "Cereals",
            },
        ]

    def fetch_markets(self, country_iso3):
        return [
            {
                "country_iso3": country_iso3,
                "market_id": 10,
                "market_name": "Juba",
                "admin1_name": "Central Equatoria",
                "admin2_name": "",
            },
            {
                "country_iso3": country_iso3,
                "market_id": 11,
                "market_name": "Wau",
                "admin1_name": "Western Bahr el Ghazal",
                "admin2_name": "",
            },
        ]

    def fetch_monthly_price_rows(self, country_iso3, *, commodity_id=None, start_date=None, end_date=None, **_kwargs):
        self.calls.append(
            {
                "country_iso3": country_iso3,
                "commodity_id": commodity_id,
                "start_date": start_date,
                "end_date": end_date,
            }
        )
        if commodity_id in self.error_by_commodity:
            raise RuntimeError(self.error_by_commodity[commodity_id])
        return list(self.rows_by_commodity.get(commodity_id, []))


class _FakeManualRefreshAdapter:
    def __init__(self, rows=None, *, error: str | None = None):
        self.rows = list(rows or [])
        self.error = error
        self.calls = []
        self.config = SimpleNamespace(max_workers=4, base_url="https://databridges.test", env="test")

    def fetch_commodities(self, country_iso3):
        return [
            {
                "country_iso3": country_iso3,
                "commodity_id": 1,
                "commodity_name": "Maize",
                "commodity_unit_id": 100,
                "commodity_unit_name": "kg",
                "category_name": "Cereals",
            },
            {
                "country_iso3": country_iso3,
                "commodity_id": 2,
                "commodity_name": "Beans",
                "commodity_unit_id": 100,
                "commodity_unit_name": "kg",
                "category_name": "Pulses",
            },
        ]

    def fetch_markets(self, country_iso3):
        return [
            {
                "country_iso3": country_iso3,
                "market_id": 10,
                "market_name": "Juba",
                "admin1_name": "Central Equatoria",
                "admin2_name": "",
            },
            {
                "country_iso3": country_iso3,
                "market_id": 11,
                "market_name": "Wau",
                "admin1_name": "Western Bahr el Ghazal",
                "admin2_name": "",
            },
        ]

    def fetch_monthly_price_rows(self, country_iso3, *, start_date=None, end_date=None, **_kwargs):
        self.calls.append({"country_iso3": country_iso3, "start_date": start_date, "end_date": end_date})
        if self.error:
            raise RuntimeError(self.error)
        return list(self.rows)


class _FakeFxAdapter:
    def __init__(self, rows):
        self.rows = list(rows)
        self.calls = []

    def fetch_exchange_rate_rows_result(self, country_iso3, *, currency_name=None, start_date=None, end_date=None):
        self.calls.append(
            {
                "country_iso3": country_iso3,
                "currency_name": currency_name,
                "start_date": start_date,
                "end_date": end_date,
            }
        )
        return SimpleNamespace(rows=list(self.rows), pages=1, permission_denied=False, error=None)


def _seed_contiguous_cache(
    repo: SqlPriceCacheRepository,
    *,
    start_month: str = "2020-02-01",
    periods: int = 73,
    country_iso3: str = "SSD",
    country_name: str = "South Sudan",
    currency_code: str = "SSP",
    currency_name: str = "South Sudanese Pound",
) -> str:
    version_id = repo.create_cache_version()
    repo.insert_currencies(
        version_id,
        [{"currency_id": 200, "currency_code": currency_code, "currency_name": currency_name}],
    )
    months = pd.date_range(start=start_month, periods=periods, freq="MS")
    prices = []
    for index, month in enumerate(months):
        row = _price(
            1,
            "Maize",
            10,
            "Juba",
            "Central Equatoria",
            month.strftime("%Y-%m-%d"),
            10 + index,
        )
        row["country_iso3"] = country_iso3
        row["currency_code"] = currency_code
        row["currency_name"] = currency_name
        prices.append(row)
    repo.insert_country_snapshot(
        cache_version_id=version_id,
        country_iso3=country_iso3,
        country_name=country_name,
        commodities=[
            {
                "commodity_id": 1,
                "commodity_name": "Maize",
                "commodity_unit_id": 100,
                "commodity_unit_name": "kg",
                "category_name": "Cereals",
            }
        ],
        markets=[{"market_id": 10, "market_name": "Juba", "admin1_name": "Central Equatoria"}],
        prices=prices,
        latest_price_date=months[-1].strftime("%Y-%m-%d"),
        currency_code=currency_code,
        currency_name=currency_name,
    )
    repo.record_country_result(
        cache_version_id=version_id,
        country_iso3=country_iso3,
        status="success",
        rows_prices=len(prices),
        rows_commodities=1,
        rows_markets=1,
        latest_price_date=months[-1].strftime("%Y-%m-%d"),
    )
    repo.insert_units(
        version_id,
        [{"commodity_unit_id": 100, "commodity_unit_name": "kg", "conversion_to_kg_l": 1.0, "active": True}],
    )
    repo.publish_cache_version(version_id, country_iso3s=[country_iso3], status="active")
    return version_id


def _country_price(
    country_iso3,
    *,
    commodity_id,
    commodity,
    market_id,
    market,
    admin1,
    price_date,
    price,
    currency_code,
    currency_name,
    unit_id=100,
    unit_name="kg",
    flag="actual",
):
    row = _price(commodity_id, commodity, market_id, market, admin1, price_date, price, flag=flag)
    row.update(
        {
            "country_iso3": country_iso3,
            "currency_code": currency_code,
            "currency_name": currency_name,
            "commodity_unit_id": unit_id,
            "commodity_unit_name": unit_name,
            "source_payload_hash": f"{country_iso3}-{commodity_id}-{market_id}-{price_date}-{flag}",
        }
    )
    return row


def _seed_country_fuel_cache(
    repo: SqlPriceCacheRepository,
    *,
    country_iso3: str,
    country_name: str,
    currency_code: str,
    currency_name: str,
    report_month: str = "2026-06-01",
    fuel_specs=None,
) -> str:
    version_id = repo.create_cache_version()
    repo.insert_currencies(
        version_id,
        [{"currency_id": 200, "currency_code": currency_code, "currency_name": currency_name}],
    )
    months = pd.date_range(end=report_month, periods=13, freq="MS")
    commodities = [
        {
            "commodity_id": 1,
            "commodity_name": "Maize",
            "commodity_unit_id": 100,
            "commodity_unit_name": "kg",
            "category_name": "Cereals",
        }
    ]
    prices = []
    for index, month in enumerate(months):
        prices.append(
            _country_price(
                country_iso3,
                commodity_id=1,
                commodity="Maize",
                market_id=10,
                market="Capital",
                admin1="Central",
                price_date=month.strftime("%Y-%m-%d"),
                price=10 + index,
                currency_code=currency_code,
                currency_name=currency_name,
            )
        )

    for spec in fuel_specs or []:
        commodity_id = int(spec["commodity_id"])
        commodity_name = str(spec["commodity_name"])
        commodities.append(
            {
                "commodity_id": commodity_id,
                "commodity_name": commodity_name,
                "commodity_unit_id": 200,
                "commodity_unit_name": "L",
                "category_name": "Fuel",
            }
        )
        start_offset = int(spec.get("start_offset", 0))
        end_offset = int(spec.get("end_offset", len(months) - 1))
        base = float(spec.get("base", 100.0))
        step = float(spec.get("step", 1.0))
        markets = spec.get("markets") or [("Capital", "Central", 10, 0.0)]
        for index, month in enumerate(months):
            if index < start_offset or index > end_offset:
                continue
            for market_name, admin1, market_id, offset in markets:
                prices.append(
                    _country_price(
                        country_iso3,
                        commodity_id=commodity_id,
                        commodity=commodity_name,
                        market_id=market_id,
                        market=market_name,
                        admin1=admin1,
                        price_date=month.strftime("%Y-%m-%d"),
                        price=base + (step * index) + float(offset),
                        currency_code=currency_code,
                        currency_name=currency_name,
                        unit_id=200,
                        unit_name="L",
                    )
                )

    repo.insert_country_snapshot(
        cache_version_id=version_id,
        country_iso3=country_iso3,
        country_name=country_name,
        commodities=commodities,
        markets=[
            {"market_id": 10, "market_name": "Capital", "admin1_name": "Central"},
            {"market_id": 11, "market_name": "Remote", "admin1_name": "Remote"},
        ],
        prices=prices,
        latest_price_date=report_month,
        currency_code=currency_code,
        currency_name=currency_name,
    )
    repo.record_country_result(
        cache_version_id=version_id,
        country_iso3=country_iso3,
        status="success",
        rows_prices=len(prices),
        rows_commodities=len(commodities),
        rows_markets=2,
        latest_price_date=report_month,
    )
    repo.insert_units(
        version_id,
        [
            {"commodity_unit_id": 100, "commodity_unit_name": "kg", "conversion_to_kg_l": 1.0, "active": True},
            {"commodity_unit_id": 200, "commodity_unit_name": "L", "conversion_to_kg_l": 1.0, "active": True},
        ],
    )
    repo.publish_cache_version(version_id, country_iso3s=[country_iso3], status="active")
    return version_id


def _seed_country_module_cache(
    repo: SqlPriceCacheRepository,
    *,
    country_iso3: str,
    country_name: str,
    currency_code: str,
    currency_name: str,
    report_month: str = "2026-06-01",
    extra_specs=None,
) -> str:
    version_id = repo.create_cache_version()
    repo.insert_currencies(
        version_id,
        [{"currency_id": 200, "currency_code": currency_code, "currency_name": currency_name}],
    )
    months = pd.date_range(end=report_month, periods=13, freq="MS")
    commodities = [
        {
            "commodity_id": 1,
            "commodity_name": "Maize",
            "commodity_unit_id": 100,
            "commodity_unit_name": "kg",
            "category_name": "Cereals",
        }
    ]
    prices = []
    for index, month in enumerate(months):
        prices.append(
            _country_price(
                country_iso3,
                commodity_id=1,
                commodity="Maize",
                market_id=10,
                market="Capital",
                admin1="Central",
                price_date=month.strftime("%Y-%m-%d"),
                price=10 + index,
                currency_code=currency_code,
                currency_name=currency_name,
            )
        )

    for spec in extra_specs or []:
        commodity_id = int(spec["commodity_id"])
        commodity_name = str(spec["commodity_name"])
        unit_id = int(spec.get("unit_id", 100))
        unit_name = str(spec.get("unit_name", "kg"))
        commodities.append(
            {
                "commodity_id": commodity_id,
                "commodity_name": commodity_name,
                "commodity_unit_id": unit_id,
                "commodity_unit_name": unit_name,
                "category_name": str(spec.get("category_name", "Other")),
            }
        )
        start_offset = int(spec.get("start_offset", 0))
        end_offset = int(spec.get("end_offset", len(months) - 1))
        base = float(spec.get("base", 100.0))
        step = float(spec.get("step", 1.0))
        markets = spec.get("markets") or [("Capital", "Central", 10, 0.0)]
        for index, month in enumerate(months):
            if index < start_offset or index > end_offset:
                continue
            for market_name, admin1, market_id, offset in markets:
                prices.append(
                    _country_price(
                        country_iso3,
                        commodity_id=commodity_id,
                        commodity=commodity_name,
                        market_id=market_id,
                        market=market_name,
                        admin1=admin1,
                        price_date=month.strftime("%Y-%m-%d"),
                        price=base + (step * index) + float(offset),
                        currency_code=currency_code,
                        currency_name=currency_name,
                        unit_id=unit_id,
                        unit_name=unit_name,
                    )
                )

    repo.insert_country_snapshot(
        cache_version_id=version_id,
        country_iso3=country_iso3,
        country_name=country_name,
        commodities=commodities,
        markets=[
            {"market_id": 10, "market_name": "Capital", "admin1_name": "Central"},
            {"market_id": 11, "market_name": "Remote", "admin1_name": "Remote"},
        ],
        prices=prices,
        latest_price_date=report_month,
        currency_code=currency_code,
        currency_name=currency_name,
    )
    repo.record_country_result(
        cache_version_id=version_id,
        country_iso3=country_iso3,
        status="success",
        rows_prices=len(prices),
        rows_commodities=len(commodities),
        rows_markets=2,
        latest_price_date=report_month,
    )
    repo.insert_units(
        version_id,
        [
            {"commodity_unit_id": 100, "commodity_unit_name": "kg", "conversion_to_kg_l": 1.0, "active": True},
            {"commodity_unit_id": 200, "commodity_unit_name": "L", "conversion_to_kg_l": 1.0, "active": True},
            {"commodity_unit_id": 300, "commodity_unit_name": "Head", "conversion_to_kg_l": 1.0, "active": True},
            {"commodity_unit_id": 400, "commodity_unit_name": "1 piece", "conversion_to_kg_l": 1.0, "active": True},
            {"commodity_unit_id": 500, "commodity_unit_name": "Day", "conversion_to_kg_l": 1.0, "active": True},
        ],
    )
    repo.publish_cache_version(version_id, country_iso3s=[country_iso3], status="active")
    return version_id


def _fx_rows(months, *, official=True, missing=None, base=1000, country_iso3="SSD", currency_code="SSP"):
    missing = set(missing or [])
    rows = []
    for index, month in enumerate(months):
        label = pd.Timestamp(month).strftime("%Y-%m")
        if label in missing:
            continue
        rows.append(
            {
                "country_iso3": country_iso3,
                "currency_code": currency_code,
                "date": pd.Timestamp(month).date(),
                "value": float(base + index),
                "is_official": official,
                "frequency": "Daily" if official else "Weekly",
            }
        )
    return rows


def _seed_dto_shaped_cache(repo: SqlPriceCacheRepository) -> str:
    """Seed price rows the way the real Databridges DTO delivers them: no admin fields."""
    version_id = repo.create_cache_version()
    repo.insert_currencies(
        version_id,
        [{"currency_id": 200, "currency_code": "SSP", "currency_name": "South Sudanese Pound"}],
    )
    prices = []
    for price_date, maize_price, beans_price in (("2024-02-01", 10, 5), ("2025-02-01", 12, 6)):
        for commodity_id, commodity, market_id, market, price in (
            (1, "Maize", 10, "Juba", maize_price),
            (2, "Beans", 11, "Wau", beans_price),
        ):
            row = _price(commodity_id, commodity, market_id, market, None, price_date, price)
            row["admin1_name"] = None
            row["admin2_name"] = None
            prices.append(row)
    repo.insert_country_snapshot(
        cache_version_id=version_id,
        country_iso3="SSD",
        country_name="South Sudan",
        commodities=[
            {
                "commodity_id": 1,
                "commodity_name": "Maize",
                "commodity_unit_id": 100,
                "commodity_unit_name": "kg",
                "category_name": "Cereals",
            },
            {
                "commodity_id": 2,
                "commodity_name": "Beans",
                "commodity_unit_id": 100,
                "commodity_unit_name": "kg",
                "category_name": "Pulses",
            },
        ],
        markets=[
            {"market_id": 10, "market_name": "Juba", "admin1_name": "Central Equatoria"},
            {"market_id": 11, "market_name": "Wau", "admin1_name": "Western Bahr el Ghazal"},
        ],
        prices=prices,
        latest_price_date="2025-02-01",
        currency_code="SSP",
        currency_name="South Sudanese Pound",
    )
    repo.record_country_result(
        cache_version_id=version_id,
        country_iso3="SSD",
        status="success",
        rows_prices=len(prices),
        rows_commodities=2,
        rows_markets=2,
        latest_price_date="2025-02-01",
    )
    repo.insert_units(
        version_id,
        [{"commodity_unit_id": 100, "commodity_unit_name": "kg", "conversion_to_kg_l": 1.0, "active": True}],
    )
    repo.publish_cache_version(
        version_id,
        country_iso3s=["SSD"],
        status="active",
        validation_summary={"fixture": True},
    )
    return version_id


def test_regions_resolve_from_markets_when_price_rows_lack_admin1(monkeypatch, tmp_path):
    repo = _repo(tmp_path)
    _seed_dto_shaped_cache(repo)
    _patch_repo(monkeypatch, repo)

    metadata = data_loader.get_country_metadata("South Sudan")
    national, regional = data_loader.extract_time_series_from_csv(
        "South Sudan",
        "2025-02",
        ["Maize", "Beans"],
        ["Central Equatoria", "Western Bahr el Ghazal"],
    )

    assert metadata["regions"] == ["Central Equatoria", "Western Bahr el Ghazal"]
    assert national.loc["2025-02-01", "FoodBasket"] == 18
    assert set(regional["Region"].unique()) == {"Central Equatoria", "Western Bahr el Ghazal"}


def test_extract_selects_single_currency_for_dual_currency_rows(monkeypatch, tmp_path):
    repo = _repo(tmp_path)
    version_id = repo.create_cache_version()
    base = _price(1, "Maize", 10, "Juba", "Central Equatoria", "2025-02-01", 12)
    usd = dict(
        base,
        price=0.5,
        currency_id=201,
        currency_code="USD",
        currency_name="US Dollar",
        source_payload_hash="usd-row",
    )
    repo.insert_country_snapshot(
        cache_version_id=version_id,
        country_iso3="SSD",
        country_name="South Sudan",
        commodities=[
            {
                "commodity_id": 1,
                "commodity_name": "Maize",
                "commodity_unit_id": 100,
                "commodity_unit_name": "kg",
                "category_name": "Cereals",
            }
        ],
        markets=[{"market_id": 10, "market_name": "Juba", "admin1_name": "Central Equatoria"}],
        prices=[base, usd],
        latest_price_date="2025-02-01",
        currency_code="SSP",
        currency_name="South Sudanese Pound",
    )
    repo.record_country_result(
        cache_version_id=version_id,
        country_iso3="SSD",
        status="success",
        rows_prices=2,
        rows_commodities=1,
        rows_markets=1,
        latest_price_date="2025-02-01",
    )
    repo.publish_cache_version(version_id, country_iso3s=["SSD"], status="active")
    _patch_repo(monkeypatch, repo)

    national, _regional = data_loader.extract_time_series_from_csv(
        "South Sudan",
        "2025-02",
        ["Maize"],
        [],
    )
    availability = data_loader.check_data_availability("South Sudan", "2025-02", ["Maize"])

    # The country default (SSP) wins; USD rows must not be averaged in.
    assert national.loc["2025-02-01", "Maize"] == 12
    assert any("multiple currencies" in warning for warning in availability["warnings"])


def test_composite_price_flags_survive_read_path(monkeypatch, tmp_path):
    repo = _repo(tmp_path)
    version_id = repo.create_cache_version()
    row = _price(1, "Maize", 10, "Juba", "Central Equatoria", "2025-02-01", 12, flag="actual,aggregate")
    repo.insert_country_snapshot(
        cache_version_id=version_id,
        country_iso3="SSD",
        country_name="South Sudan",
        commodities=[
            {
                "commodity_id": 1,
                "commodity_name": "Maize",
                "commodity_unit_id": 100,
                "commodity_unit_name": "kg",
                "category_name": "Cereals",
            }
        ],
        markets=[{"market_id": 10, "market_name": "Juba", "admin1_name": "Central Equatoria"}],
        prices=[row],
        latest_price_date="2025-02-01",
        currency_code="SSP",
        currency_name="South Sudanese Pound",
    )
    repo.record_country_result(
        cache_version_id=version_id,
        country_iso3="SSD",
        status="success",
        rows_prices=1,
        rows_commodities=1,
        rows_markets=1,
        latest_price_date="2025-02-01",
    )
    repo.publish_cache_version(version_id, country_iso3s=["SSD"], status="active")
    _patch_repo(monkeypatch, repo)

    national, _regional = data_loader.extract_time_series_from_csv(
        "South Sudan",
        "2025-02",
        ["Maize"],
        [],
    )

    assert national.loc["2025-02-01", "Maize"] == 12


def test_country_metadata_uses_price_cache(monkeypatch, tmp_path):
    repo = _repo(tmp_path)
    version_id = _seed_loader_cache(repo)
    _patch_repo(monkeypatch, repo)

    metadata = data_loader.get_country_metadata("South Sudan")

    assert metadata["source"] == "PriceCache"
    assert metadata["cache_version_id"] == version_id
    assert metadata["iso3"] == "SSD"
    assert [item["name"] for item in metadata["commodities"]] == ["Beans", "Maize"]
    assert metadata["regions"] == ["Central Equatoria", "Western Bahr el Ghazal"]
    assert metadata["date_range"] == {"start": "2024-02-01", "end": "2025-02-01"}
    assert metadata["latest_cached_date"] == "2025-02-01"
    assert metadata["units"] == [
        {"id": 100, "name": "kg", "conversion_to_kg_l": 1.0, "source": "cached_units"}
    ]


def test_country_metadata_derives_commodity_units_from_price_rows(monkeypatch, tmp_path):
    repo = _repo(tmp_path)
    version_id = _seed_loader_cache(repo)
    with repo.engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE cached_commodities
                SET commodity_unit_name = '',
                    commodity_unit_id = NULL
                WHERE cache_version_id = :cache_version_id
                  AND country_iso3 = 'SSD'
                  AND commodity_id = 1
                """
            ),
            {"cache_version_id": version_id},
        )
    _patch_repo(monkeypatch, repo)

    metadata = data_loader.get_country_metadata("South Sudan")
    maize = next(item for item in metadata["commodities"] if item["name"] == "Maize")

    assert maize["unit"] == "kg"
    assert maize["unit_name"] == "kg"
    assert maize["unit_id"] == 100


def test_reportable_months_ignore_forecast_only_latest_month(monkeypatch, tmp_path):
    repo = _repo(tmp_path)
    version_id = _seed_loader_cache(repo)
    repo.upsert_monthly_prices(
        cache_version_id=version_id,
        country_iso3="SSD",
        prices=[
            _price(1, "Maize", 10, "Juba", "Central Equatoria", "2025-03-01", 14, flag="forecast"),
            _price(2, "Beans", 11, "Wau", "Western Bahr el Ghazal", "2025-03-01", 8, flag="forecast"),
        ],
    )
    _patch_repo(monkeypatch, repo)
    monkeypatch.setattr(data_loader, "_get_active_food_basket", lambda _iso3: _basket_namespace())

    payload = data_loader.get_reportable_months("South Sudan")

    assert payload["latest_cached_month"] == "2025-03"
    assert payload["latest_reportable_month"] == "2025-02"
    assert "2025-03" not in payload["reportable_months"]
    assert payload["missing_by_month"]["2025-03"] == ["Maize", "Beans"]


def test_reportable_months_require_each_basket_commodity(monkeypatch, tmp_path):
    repo = _repo(tmp_path)
    _seed_loader_cache(repo, include_latest_beans=False)
    _patch_repo(monkeypatch, repo)
    monkeypatch.setattr(data_loader, "_get_active_food_basket", lambda _iso3: _basket_namespace())

    payload = data_loader.get_reportable_months("South Sudan")

    assert payload["latest_cached_month"] == "2025-02"
    assert payload["latest_reportable_month"] == "2024-02"
    assert payload["missing_by_month"]["2025-02"] == ["Beans"]


def test_reportable_months_without_basket_returns_warning(monkeypatch, tmp_path):
    repo = _repo(tmp_path)
    _seed_loader_cache(repo)
    _patch_repo(monkeypatch, repo)
    monkeypatch.setattr(data_loader, "_get_active_food_basket", lambda _iso3: None)

    payload = data_loader.get_reportable_months("South Sudan")

    assert payload["reportable_months"] == []
    assert payload["latest_reportable_month"] is None
    assert "No active food basket" in payload["warnings"][0]


def test_manual_refresh_promotes_country_cache_when_new_actual_rows_exist(monkeypatch, tmp_path):
    repo = _repo(tmp_path)
    source_version = _seed_loader_cache(repo)
    _patch_repo(monkeypatch, repo)
    monkeypatch.setenv("MARKET_MONITOR_MANUAL_REFRESH_ENABLED", "true")
    monkeypatch.setattr(data_loader, "_get_active_food_basket", lambda _iso3: _basket_namespace("basket-v1"))
    monkeypatch.setattr(data_loader, "_current_month_start", lambda: date(2025, 3, 1))
    adapter = _FakeManualRefreshAdapter(
        [
            _price(1, "Maize", 10, "Juba", "Central Equatoria", "2025-03-01", 14),
            _price(2, "Beans", 11, "Wau", "Western Bahr el Ghazal", "2025-03-01", 8),
        ]
    )

    result = data_loader.refresh_reportable_months_from_databridges(
        "South Sudan",
        basket_version_id="basket-v1",
        adapter=adapter,
    )

    assert result["status"] == "updated"
    assert result["source_cache_version_id"] == source_version
    assert result["new_cache_version_id"] != source_version
    assert result["checked_start_month"] == "2025-03"
    assert result["latest_reportable_month_before"] == "2025-02"
    assert result["latest_reportable_month_after"] == "2025-03"
    assert result["rows_saved"] == 2
    assert repo.get_active_version_id() == source_version
    assert repo.get_active_version_id_for_country("SSD") == result["new_cache_version_id"]


def test_manual_refresh_forecast_only_returns_no_update(monkeypatch, tmp_path):
    repo = _repo(tmp_path)
    source_version = _seed_loader_cache(repo)
    _patch_repo(monkeypatch, repo)
    monkeypatch.setenv("MARKET_MONITOR_MANUAL_REFRESH_ENABLED", "true")
    monkeypatch.setattr(data_loader, "_get_active_food_basket", lambda _iso3: _basket_namespace("basket-v1"))
    monkeypatch.setattr(data_loader, "_current_month_start", lambda: date(2025, 3, 1))
    adapter = _FakeManualRefreshAdapter(
        [
            _price(1, "Maize", 10, "Juba", "Central Equatoria", "2025-03-01", 14, flag="forecast"),
            _price(2, "Beans", 11, "Wau", "Western Bahr el Ghazal", "2025-03-01", 8, flag="forecast"),
        ]
    )

    result = data_loader.refresh_reportable_months_from_databridges(
        "South Sudan",
        basket_version_id="basket-v1",
        adapter=adapter,
    )

    assert result["status"] == "no_update"
    assert result["rows_saved"] == 0
    assert result["excluded_non_real_rows"] == 2
    assert repo.get_active_version_id_for_country("SSD") == source_version


def test_manual_refresh_partial_actual_saves_rows_but_month_remains_unreportable(monkeypatch, tmp_path):
    repo = _repo(tmp_path)
    source_version = _seed_loader_cache(repo)
    _patch_repo(monkeypatch, repo)
    monkeypatch.setenv("MARKET_MONITOR_MANUAL_REFRESH_ENABLED", "true")
    monkeypatch.setattr(data_loader, "_get_active_food_basket", lambda _iso3: _basket_namespace("basket-v1"))
    monkeypatch.setattr(data_loader, "_current_month_start", lambda: date(2025, 3, 1))
    adapter = _FakeManualRefreshAdapter(
        [_price(1, "Maize", 10, "Juba", "Central Equatoria", "2025-03-01", 14)]
    )

    result = data_loader.refresh_reportable_months_from_databridges(
        "South Sudan",
        basket_version_id="basket-v1",
        adapter=adapter,
    )

    assert result["status"] == "updated"
    assert result["rows_saved"] == 1
    assert result["latest_reportable_month_after"] == "2025-02"
    assert result["missing_by_month"]["2025-03"] == ["Beans"]
    assert repo.get_active_version_id_for_country("SSD") != source_version


def test_manual_refresh_databridges_error_returns_unavailable(monkeypatch, tmp_path):
    repo = _repo(tmp_path)
    source_version = _seed_loader_cache(repo)
    _patch_repo(monkeypatch, repo)
    monkeypatch.setenv("MARKET_MONITOR_MANUAL_REFRESH_ENABLED", "true")
    monkeypatch.setattr(data_loader, "_get_active_food_basket", lambda _iso3: _basket_namespace("basket-v1"))
    monkeypatch.setattr(data_loader, "_current_month_start", lambda: date(2025, 3, 1))

    result = data_loader.refresh_reportable_months_from_databridges(
        "South Sudan",
        basket_version_id="basket-v1",
        adapter=_FakeManualRefreshAdapter(error="permission denied"),
    )

    assert result["status"] == "unavailable"
    assert "permission denied" in result["warnings"][0]
    assert repo.get_active_version_id_for_country("SSD") == source_version


def test_country_metadata_caps_future_price_dates(monkeypatch, tmp_path):
    repo = _repo(tmp_path)
    version_id = repo.create_cache_version()
    repo.insert_currencies(
        version_id,
        [{"currency_id": 200, "currency_code": "LBP", "currency_name": "Lebanese Pound"}],
    )
    repo.insert_country_snapshot(
        cache_version_id=version_id,
        country_iso3="LBN",
        country_name="Lebanon",
        commodities=[
            {
                "commodity_id": 1,
                "commodity_name": "Wheat flour",
                "commodity_unit_id": 100,
                "commodity_unit_name": "kg",
                "category_name": "Cereals",
            }
        ],
        markets=[{"market_id": 10, "market_name": "Beirut", "admin1_name": "Beirut"}],
        prices=[
            {
                **_price(1, "Wheat flour", 10, "Beirut", "Beirut", "2026-05-01", 10),
                "country_iso3": "LBN",
                "currency_code": "LBP",
                "currency_name": "Lebanese Pound",
            },
            {
                **_price(1, "Wheat flour", 10, "Beirut", "Beirut", "2026-10-01", 12),
                "country_iso3": "LBN",
                "currency_code": "LBP",
                "currency_name": "Lebanese Pound",
            },
        ],
        latest_price_date="2026-10-01",
        currency_code="LBP",
        currency_name="Lebanese Pound",
    )
    repo.record_country_result(
        cache_version_id=version_id,
        country_iso3="LBN",
        status="success",
        rows_prices=2,
        rows_commodities=1,
        rows_markets=1,
        latest_price_date="2026-10-01",
    )
    repo.insert_units(
        version_id,
        [{"commodity_unit_id": 100, "commodity_unit_name": "kg", "conversion_to_kg_l": 1.0, "active": True}],
    )
    repo.publish_cache_version(version_id, country_iso3s=["LBN"], status="active")
    _patch_repo(monkeypatch, repo)
    monkeypatch.setattr(data_loader, "_current_month_start", lambda: date(2026, 6, 1))

    metadata = data_loader.get_country_metadata("Lebanon")

    assert metadata["date_range"] == {"start": "2026-05-01", "end": "2026-06-01"}
    assert metadata["latest_cached_date"] == "2026-06-01"
    assert any("future-dated monthly prices" in warning for warning in metadata["warnings"])


def test_cache_warning_summary_aggregates_global_noise():
    status = data_loader.CacheStatus(
        has_active_cache=True,
        validation_summary={
            "warnings": [
                {
                    "scope": "units",
                    "warning": "CommodityUnits/List could not be fetched; Error: (403) huge headers",
                },
                {
                    "country_iso3": "LBN",
                    "warnings": [
                        "Found 194 price metadata references not present in fetched commodities/markets.",
                        "Excluded 16335 non-real monthly price row(s) for LBN based on price_flag (forecast=16335); only actual/aggregate rows are cached.",
                        "Deduplicated 13689 duplicate monthly price row(s) across 12211 canonical key(s) for LBN; kept the row with the most complete metadata and highest observation count per key.",
                    ],
                },
                {
                    "country_iso3": "BOL",
                    "warnings": [
                        "Found 4678 price metadata references not present in fetched commodities/markets.",
                        "222 duplicate monthly price key(s) for BOL had conflicting price values; the deterministic best-ranked row was kept.",
                    ],
                },
            ]
        },
    )

    global_warnings = data_loader._cache_warnings(status)
    lebanon_warnings = data_loader._cache_warnings(status, country_iso3="LBN")

    assert any("CommodityUnits/List could not be fetched" in warning for warning in global_warnings)
    assert any("2 country/countries" in warning for warning in global_warnings)
    assert any("16335 non-real monthly price row" in warning for warning in global_warnings)
    assert any("13689 duplicate monthly price row" in warning for warning in global_warnings)
    assert any("CommodityUnits/List could not be fetched" in warning for warning in lebanon_warnings)
    assert any("16335 non-real monthly price row" in warning for warning in lebanon_warnings)
    assert any("13689 duplicate monthly price row" in warning for warning in lebanon_warnings)
    assert not any("BOL" in warning for warning in lebanon_warnings)


def test_time_series_preserves_missing_months_and_current_statistics(monkeypatch, tmp_path):
    repo = _repo(tmp_path)
    _seed_loader_cache(repo)
    _patch_repo(monkeypatch, repo)

    national, regional = data_loader.extract_time_series_from_csv(
        "South Sudan",
        "2025-02",
        ["Maize", "Beans"],
        [],
    )
    stats = data_loader.calculate_statistics_from_csv(national, ["Maize", "Beans"])

    assert len(national) == 13
    assert national.index[0].strftime("%Y-%m") == "2024-02"
    assert national.index[-1].strftime("%Y-%m") == "2025-02"
    assert national.loc["2024-03-01", "FoodBasket"] != national.loc["2024-03-01", "FoodBasket"]
    assert national.loc["2025-02-01", "FoodBasket"] == 18
    assert regional["Region"].nunique() == 2
    assert stats["food_basket"]["current_price"] == 18
    assert stats["food_basket"]["yoy_change_pct"] == 20.0
    assert stats["food_basket"]["latest_component_names"] == ["Maize", "Beans"]


def test_availability_reports_missing_commodities_without_mock(monkeypatch, tmp_path):
    repo = _repo(tmp_path)
    _seed_loader_cache(repo)
    _patch_repo(monkeypatch, repo)

    availability = data_loader.check_data_availability(
        "South Sudan",
        "2025-02",
        ["Rice"],
    )

    assert availability["available"] is True
    assert availability["cache_metadata"]["source"] == "PriceCache"
    assert availability["missing_commodities"] == ["Rice"]
    assert "Rice" in availability["warnings"][0]


def test_reversed_cache_row_order_produces_identical_time_series(monkeypatch, tmp_path):
    repo = _repo(tmp_path)
    _seed_loader_cache(repo)
    _patch_repo(monkeypatch, repo)
    normal_national, normal_regional = data_loader.extract_time_series_from_csv(
        "South Sudan",
        "2025-02",
        ["Maize", "Beans"],
        [],
    )

    data_loader.reset_market_monitor_caches_for_tests()
    reversed_rows = list(
        reversed(repo.get_price_window("SSD", "2024-02-01", "2025-02-01"))
    )
    normalised = data_loader._normalise_cached_price_rows(reversed_rows, "South Sudan", "SSD")
    data_loader._cache_set(data_loader._PRICE_CACHE, ("SSD", "2024-02-01", "2025-02-28", (1, 2), False), normalised)
    reversed_national, reversed_regional = data_loader.extract_time_series_from_csv(
        "South Sudan",
        "2025-02",
        ["Maize", "Beans"],
        [],
    )

    pd.testing.assert_frame_equal(normal_national, reversed_national)
    pd.testing.assert_frame_equal(normal_regional, reversed_regional)


def test_duplicate_commodity_name_prefers_lower_id(monkeypatch, tmp_path):
    repo = _repo(tmp_path)
    _seed_loader_cache(repo, duplicate_commodities=True)
    _patch_repo(monkeypatch, repo)

    national, _regional = data_loader.extract_time_series_from_csv(
        "South Sudan",
        "2025-02",
        ["Maize"],
        [],
    )

    assert national.loc["2025-02-01", "Maize"] == 12


def test_food_basket_coverage_only_counts_latest_month_contributors(monkeypatch, tmp_path):
    repo = _repo(tmp_path)
    _seed_loader_cache(repo, include_latest_beans=False)
    _patch_repo(monkeypatch, repo)

    national, _regional = data_loader.extract_time_series_from_csv(
        "South Sudan",
        "2025-02",
        ["Maize", "Beans"],
        [],
    )
    stats = data_loader.calculate_statistics_from_csv(national, ["Maize", "Beans"])

    assert stats["food_basket"]["current_price"] == 12
    assert stats["food_basket"]["selected_component_count"] == 2
    assert stats["food_basket"]["historical_component_count"] == 2
    assert stats["food_basket"]["latest_component_count"] == 1
    assert stats["food_basket"]["latest_component_names"] == ["Maize"]
    assert stats["food_basket"]["missing_latest_component_names"] == ["Beans"]


def test_weighted_food_basket_uses_saved_component_quantities(monkeypatch, tmp_path):
    repo = _repo(tmp_path)
    _seed_loader_cache(repo)
    _patch_repo(monkeypatch, repo)

    national, regional = data_loader.extract_time_series_from_csv(
        "South Sudan",
        "2025-02",
        ["Maize", "Beans"],
        ["Central Equatoria", "Western Bahr el Ghazal"],
        basket_items=_basket_items(),
    )
    stats = data_loader.calculate_statistics_from_csv(
        national,
        ["Maize", "Beans"],
        food_basket_components=_basket_items(),
    )

    assert national.loc["2025-02-01", "Maize"] == 12
    assert national.loc["2025-02-01", "Beans"] == 6
    assert national.loc["2025-02-01", "FoodBasket"] == 42
    latest_regions = regional[regional["Date"] == pd.Timestamp("2025-02-01")]
    # Each fixture region contains only one of the two basket components.
    # Regional basket aliases must therefore remain null rather than exposing
    # the old partial sums, even though the country-wide basket is complete.
    assert latest_regions["FoodBasket"].isna().all()
    assert stats["food_basket"]["current_price"] == 42
    assert stats["food_basket"]["selected_component_names"] == ["Maize", "Beans"]
    assert stats["food_basket"]["available_component_names"] == ["Maize", "Beans"]


def test_additional_commodities_do_not_change_weighted_food_basket(monkeypatch, tmp_path):
    repo = _repo(tmp_path)
    _seed_loader_cache(repo, include_rice=True)
    _patch_repo(monkeypatch, repo)

    national, _regional = data_loader.extract_time_series_from_csv(
        "South Sudan",
        "2025-02",
        ["Maize", "Beans", "Rice"],
        [],
        basket_items=_basket_items(),
    )
    stats = data_loader.calculate_statistics_from_csv(
        national,
        ["Maize", "Beans", "Rice"],
        food_basket_components=_basket_items(),
    )

    assert national.loc["2025-02-01", "Rice"] == 7
    assert national.loc["2025-02-01", "FoodBasket"] == 42
    assert stats["commodities"]["Rice"]["current_price"] == 7
    assert stats["food_basket"]["current_price"] == 42


def test_resolve_report_price_data_adds_databridges_fx_and_history(monkeypatch, tmp_path):
    repo = _repo(tmp_path)
    _seed_contiguous_cache(repo)
    _patch_repo(monkeypatch, repo)
    months = pd.date_range(end="2026-02-01", periods=13, freq="MS")
    adapter = _FakeFxAdapter(
        _fx_rows(months, official=True, base=1000)
        + _fx_rows(months, official=False, missing={"2026-01"}, base=1200)
    )

    result = data_loader.resolve_report_price_data(
        "South Sudan",
        "2026-02",
        ["Maize"],
        ["Central Equatoria"],
        basket_items=[{"commodity_id": 1, "commodity_name_snapshot": "Maize", "weight_quantity": 2}],
        currency_code="SSP",
        adapter=adapter,
    )
    stats = data_loader.calculate_statistics_from_csv(
        result.df_national,
        ["Maize"],
        food_basket_components=[{"commodity_id": 1, "commodity_name_snapshot": "Maize", "weight_quantity": 2}],
        currency_code="SSP",
    )

    assert adapter.calls == [
        {
            "country_iso3": "SSD",
            "currency_name": "SSP",
            "start_date": date(2025, 2, 1),
            "end_date": date(2026, 2, 28),
        }
    ]
    assert result.df_national["ExchangeRate"].notna().all()
    assert "ExchangeRateUnofficial" not in result.df_national.columns
    assert result.exchange_rate_data["source"] == "DataBridges"
    assert result.exchange_rate_data["current_rate"] == 1012.0
    assert result.exchange_rate_data["unit"] == "SSP per 1 USD"
    assert result.exchange_rate_data["higher_value_indicates"] == "local_currency_depreciation"
    assert stats["exchange_rate"]["official"]["current_rate"] == 1012.0
    assert stats["exchange_rate"]["official"]["unit"] == "SSP per 1 USD"
    assert result.cache_metadata["currency_code"] == "SSP"
    assert result.cache_metadata["fx"]["official"]["included"] is True
    assert result.cache_metadata["fx"]["unofficial"]["included"] is False
    assert any("Unofficial FX omitted" in warning for warning in result.warnings)
    assert len(result.df_history_national) == 73
    assert result.df_history_national["FoodBasket"].notna().all()


def test_exchange_rate_resampling_averages_raw_daily_and_weekly_rows():
    months = pd.date_range("2026-01-01", periods=2, freq="MS")
    rows = [
        {"date": date(2026, 1, 1), "value": 100, "is_official": True},
        {"date": date(2026, 1, 15), "value": 110, "is_official": True},
        {"date": date(2026, 2, 1), "value": 120, "is_official": True},
        {"date": date(2026, 2, 18), "value": 140, "is_official": True},
        {"date": date(2026, 1, 2), "value": 200, "is_official": False, "frequency": "Weekly"},
        {"date": date(2026, 1, 23), "value": 220, "is_official": False, "frequency": "Weekly"},
        {"date": date(2026, 2, 6), "value": 240, "is_official": False, "frequency": "Weekly"},
        {"date": date(2026, 2, 27), "value": 280, "is_official": False, "frequency": "Weekly"},
    ]

    official, official_missing = data_loader._monthly_exchange_rate_series(
        rows,
        full_date_index=months,
        official=True,
    )
    unofficial, unofficial_missing = data_loader._monthly_exchange_rate_series(
        rows,
        full_date_index=months,
        official=False,
    )

    assert official_missing == []
    assert unofficial_missing == []
    assert official.tolist() == [105.0, 130.0]
    assert unofficial.tolist() == [210.0, 260.0]


def test_resolve_exchange_rate_series_keeps_official_when_unofficial_missing_month():
    months = pd.date_range(end="2026-02-01", periods=13, freq="MS")
    rows = (
        _fx_rows(months, official=True, base=1000)
        + _fx_rows(months, official=False, missing={"2026-01"}, base=1200)
    )
    result = data_loader._resolve_exchange_rate_series(
        iso3="SSD",
        currency_code="SSP",
        currency_name="South Sudanese Pound",
        full_date_index=months,
        adapter=_FakeFxAdapter(rows),
    )

    assert "ExchangeRate" in result["series"]
    assert "ExchangeRateUnofficial" not in result["series"]
    assert result["metadata"]["official"]["included"] is True
    assert result["metadata"]["unofficial"]["included"] is False
    assert "2026-01" in result["metadata"]["unofficial"]["missing_months"]
    assert any("Unofficial FX omitted" in warning for warning in result["warnings"])


def test_resolve_exchange_rate_series_no_unofficial_is_official_only_warning():
    months = pd.date_range(end="2026-02-01", periods=13, freq="MS")
    rows = _fx_rows(months, official=True, base=1000, country_iso3="AFG", currency_code="AFN")

    result = data_loader._resolve_exchange_rate_series(
        iso3="AFG",
        currency_code="AFN",
        currency_name="Afghani",
        full_date_index=months,
        adapter=_FakeFxAdapter(rows),
    )

    assert "ExchangeRate" in result["series"]
    assert "ExchangeRateUnofficial" not in result["series"]
    assert result["metadata"]["official"]["included"] is True
    assert result["metadata"]["unofficial"]["included"] is False
    assert any("no unofficial/parallel" in warning for warning in result["warnings"])


def test_resolve_exchange_rate_series_suppresses_static_official_when_price_scale_implausible():
    months = pd.date_range(end="2026-02-01", periods=13, freq="MS")
    rows = [
        {
            "country_iso3": "SOM",
            "currency_code": "SOS",
            "date": month.date(),
            "value": 571.5,
            "is_official": True,
            "frequency": "Daily",
        }
        for month in months
    ]
    price_frame = pd.DataFrame(
        {
            "FoodBasket": [493383.0] * len(months),
            "Sugar": [32000.0] * len(months),
            "Rice": [28000.0] * len(months),
            "Oil": [40000.0] * len(months),
        },
        index=months,
    )

    result = data_loader._resolve_exchange_rate_series(
        iso3="SOM",
        currency_code="SOS",
        currency_name="Somali Shilling",
        full_date_index=months,
        adapter=_FakeFxAdapter(rows),
        price_frame=price_frame,
    )

    assert result["series"] == {}
    assert result["exchange_rate_data"] is None
    usability = result["metadata"]["usability"]
    assert usability["official_usable"] is False
    assert usability["near_static"] is True
    assert usability["price_scale_inconsistent"] is True
    assert usability["selected_series"] is None
    assert "near-static official-only rate" in usability["omitted_reason"]
    assert any("DataBridges FX omitted" in warning for warning in result["warnings"])


def test_resolve_exchange_rate_series_keeps_static_official_when_price_scale_plausible():
    months = pd.date_range(end="2026-02-01", periods=13, freq="MS")
    rows = [
        {
            "country_iso3": "BFA",
            "currency_code": "XOF",
            "date": month.date(),
            "value": 567.0,
            "is_official": True,
            "frequency": "Daily",
        }
        for month in months
    ]
    price_frame = pd.DataFrame(
        {
            "FoodBasket": [3600.0] * len(months),
            "Rice": [600.0] * len(months),
            "Millet": [300.0] * len(months),
            "Sorghum": [250.0] * len(months),
        },
        index=months,
    )

    result = data_loader._resolve_exchange_rate_series(
        iso3="BFA",
        currency_code="XOF",
        currency_name="CFA Franc BCEAO",
        full_date_index=months,
        adapter=_FakeFxAdapter(rows),
        price_frame=price_frame,
    )

    assert "ExchangeRate" in result["series"]
    assert result["exchange_rate_data"]["source_series"] == "official"
    usability = result["metadata"]["usability"]
    assert usability["official_usable"] is True
    assert usability["near_static"] is True
    assert usability["price_scale_inconsistent"] is False
    assert usability["selected_series"] == "official"


def test_resolve_exchange_rate_series_uses_unofficial_when_official_scale_is_unusable():
    months = pd.date_range(end="2026-02-01", periods=13, freq="MS")
    official_rows = [
        {
            "country_iso3": "SOM",
            "currency_code": "SOS",
            "date": month.date(),
            "value": 571.5,
            "is_official": True,
            "frequency": "Daily",
        }
        for month in months
    ]
    unofficial_rows = _fx_rows(months, official=False, base=25000, country_iso3="SOM", currency_code="SOS")
    price_frame = pd.DataFrame(
        {
            "FoodBasket": [493383.0] * len(months),
            "Sugar": [32000.0] * len(months),
            "Rice": [28000.0] * len(months),
            "Oil": [40000.0] * len(months),
        },
        index=months,
    )

    result = data_loader._resolve_exchange_rate_series(
        iso3="SOM",
        currency_code="SOS",
        currency_name="Somali Shilling",
        full_date_index=months,
        adapter=_FakeFxAdapter(official_rows + unofficial_rows),
        price_frame=price_frame,
    )

    assert "ExchangeRate" not in result["series"]
    assert "ExchangeRateUnofficial" in result["series"]
    assert result["exchange_rate_data"]["source_series"] == "unofficial"
    assert result["exchange_rate_data"]["rate_type"] == "unofficial"
    assert result["metadata"]["usability"]["selected_series"] == "unofficial"
    assert any("using unofficial/parallel FX" in warning for warning in result["warnings"])


def test_resolve_exchange_rate_series_partial_current_month_warns(monkeypatch):
    months = pd.date_range(end="2026-06-01", periods=13, freq="MS")
    prior_rows = _fx_rows(months[:-1], official=True, base=1000)
    latest_rows = [
        {
            "country_iso3": "SSD",
            "currency_code": "SSP",
            "date": date(2026, 6, 1),
            "value": 2000.0,
            "is_official": True,
            "frequency": "Daily",
        },
        {
            "country_iso3": "SSD",
            "currency_code": "SSP",
            "date": date(2026, 6, 18),
            "value": 2200.0,
            "is_official": True,
            "frequency": "Daily",
        },
    ]


def _basket_namespace(version_id="basket-v1"):
    return SimpleNamespace(
        basket_version_id=version_id,
        items=[
            SimpleNamespace(commodity_id=1, commodity_name_snapshot="Maize"),
            SimpleNamespace(commodity_id=2, commodity_name_snapshot="Beans"),
        ],
    )
    monkeypatch.setattr(data_loader, "_current_month_start", lambda: date(2026, 6, 1))

    result = data_loader._resolve_exchange_rate_series(
        iso3="SSD",
        currency_code="SSP",
        currency_name="South Sudanese Pound",
        full_date_index=months,
        adapter=_FakeFxAdapter(prior_rows + latest_rows),
    )

    assert result["series"]["ExchangeRate"].iloc[-1] == 2100.0
    assert result["exchange_rate_data"]["current_rate"] == 2100.0
    assert result["metadata"]["partial_latest_month"] == {
        "partial": True,
        "month": "2026-06",
        "through_date": "2026-06-18",
    }
    assert any("partial through 2026-06-18" in warning for warning in result["warnings"])


def test_resolve_report_price_data_fx_flag_disables_fetch(monkeypatch, tmp_path):
    repo = _repo(tmp_path)
    _seed_contiguous_cache(repo, start_month="2025-06-01", periods=13)
    _patch_repo(monkeypatch, repo)
    monkeypatch.setenv("MARKET_MONITOR_FX_ENABLED", "false")
    months = pd.date_range(end="2026-06-01", periods=13, freq="MS")
    adapter = _FakeFxAdapter(_fx_rows(months, official=True, base=1000))

    result = data_loader.resolve_report_price_data(
        "South Sudan",
        "2026-06",
        ["Maize"],
        [],
        basket_items=[{"commodity_id": 1, "commodity_name_snapshot": "Maize", "weight_quantity": 1}],
        currency_code="SSP",
        adapter=adapter,
    )

    assert adapter.calls == []
    assert result.exchange_rate_data is None
    assert result.cache_metadata["fx"]["source"] == "disabled"
    assert result.df_national["ExchangeRate"].isna().all()


def test_fuel_transport_classifier_excludes_cooking_energy_and_false_positives():
    assert data_loader._fuel_transport_kind("Fuel (diesel)") == "diesel"
    assert data_loader._fuel_transport_kind("Fuel (petrol-gasoline)") == "petrol_gasoline"
    assert data_loader._fuel_transport_kind("Fuel (Super Petrol)") == "petrol_gasoline"
    assert data_loader._fuel_transport_kind("Fuel (gas)") is None
    assert data_loader._fuel_transport_kind("Fuel (kerosene)") is None
    assert data_loader._fuel_transport_kind("Firewood") is None
    assert data_loader._fuel_transport_kind("Fish (live, pangasius)") is None


def test_resolve_report_price_data_adds_multi_fuel_without_polluting_food_basket(monkeypatch, tmp_path):
    repo = _repo(tmp_path)
    _seed_country_fuel_cache(
        repo,
        country_iso3="COD",
        country_name="Democratic Republic of the Congo",
        currency_code="CDF",
        currency_name="Congolese Franc",
        fuel_specs=[
            {"commodity_id": 284, "commodity_name": "Fuel (diesel)", "base": 100, "step": 1},
            {"commodity_id": 285, "commodity_name": "Fuel (petrol-gasoline)", "base": 200, "step": 2},
            {"commodity_id": 283, "commodity_name": "Fuel (kerosene)", "base": 50, "step": 1},
        ],
    )
    _patch_repo(monkeypatch, repo)

    result = data_loader.resolve_report_price_data(
        "Democratic Republic of the Congo",
        "2026-06",
        ["Maize"],
        [],
        basket_items=[{"commodity_id": 1, "commodity_name_snapshot": "Maize", "weight_quantity": 1}],
        currency_code="CDF",
        enabled_modules=["fuel_energy"],
    )
    stats = data_loader.calculate_statistics_from_csv(
        result.df_national,
        ["Maize"],
        food_basket_components=[{"commodity_id": 1, "commodity_name_snapshot": "Maize", "weight_quantity": 1}],
        currency_code="CDF",
    )

    fuel = result.fuel_energy_data
    assert fuel is not None
    assert [item["kind"] for item in fuel["series"]] == ["diesel", "petrol_gasoline"]
    assert result.cache_metadata["fuel_energy"]["series_count"] == 2
    assert result.df_national.loc[pd.Timestamp("2026-06-01"), "FoodBasket"] == 22
    assert result.df_national.loc[pd.Timestamp("2026-06-01"), "Fuel (diesel)"] == 112
    assert result.df_national.loc[pd.Timestamp("2026-06-01"), "Fuel (petrol/gasoline)"] == 224
    assert "Fuel (kerosene)" not in result.df_national.columns
    assert "Fuel (diesel)" in stats["auxiliary"]
    assert "Fuel (diesel)" not in stats["commodities"]
    diesel = fuel["series"][0]
    assert diesel["current_price"] == 112
    assert diesel["mom_change_pct"] == 0.9
    assert diesel["yoy_change_pct"] == 12.0
    assert diesel["latest_month"] == "2026-06"
    assert diesel["axis_unit"] == "CDF/Litre"


def test_resolve_report_price_data_diesel_only_country(monkeypatch, tmp_path):
    repo = _repo(tmp_path)
    _seed_country_fuel_cache(
        repo,
        country_iso3="SOM",
        country_name="Somalia",
        currency_code="SOS",
        currency_name="Somali Shilling",
        fuel_specs=[{"commodity_id": 284, "commodity_name": "Fuel (diesel)", "base": 1000, "step": 10}],
    )
    _patch_repo(monkeypatch, repo)

    result = data_loader.resolve_report_price_data(
        "Somalia",
        "2026-06",
        ["Maize"],
        [],
        basket_items=[{"commodity_id": 1, "commodity_name_snapshot": "Maize", "weight_quantity": 1}],
        currency_code="SOS",
        enabled_modules=["fuel_energy"],
    )

    assert result.fuel_energy_data is not None
    assert [item["kind"] for item in result.fuel_energy_data["series"]] == ["diesel"]
    assert "Fuel (diesel)" in result.df_national.columns
    assert "Fuel (petrol/gasoline)" not in result.df_national.columns


def test_resolve_report_price_data_no_fuel_country_omits_module_with_warning(monkeypatch, tmp_path):
    repo = _repo(tmp_path)
    _seed_country_fuel_cache(
        repo,
        country_iso3="BFA",
        country_name="Burkina Faso",
        currency_code="XOF",
        currency_name="CFA Franc BCEAO",
        fuel_specs=[],
    )
    _patch_repo(monkeypatch, repo)

    result = data_loader.resolve_report_price_data(
        "Burkina Faso",
        "2026-06",
        ["Maize"],
        [],
        basket_items=[{"commodity_id": 1, "commodity_name_snapshot": "Maize", "weight_quantity": 1}],
        currency_code="XOF",
        enabled_modules=["fuel_energy"],
    )

    assert result.fuel_energy_data is None
    assert result.cache_metadata["fuel_energy"]["series_count"] == 0
    assert "Fuel (diesel)" not in result.df_national.columns
    assert any("Fuel & Energy omitted: no transport fuel" in warning for warning in result.warnings)


def test_resolve_report_price_data_lagging_bangladesh_fuel_uses_latest_actual_month(monkeypatch, tmp_path):
    repo = _repo(tmp_path)
    _seed_country_fuel_cache(
        repo,
        country_iso3="BGD",
        country_name="Bangladesh",
        currency_code="BDT",
        currency_name="Bangladeshi Taka",
        fuel_specs=[
            {"commodity_id": 284, "commodity_name": "Fuel (diesel)", "base": 90, "step": 1, "end_offset": 11},
            {"commodity_id": 1041, "commodity_name": "Fuel (petrol)", "base": 110, "step": 1, "end_offset": 11},
            {"commodity_id": 341, "commodity_name": "Fuel (gas)", "base": 1000, "step": 5, "end_offset": 11},
        ],
    )
    _patch_repo(monkeypatch, repo)

    result = data_loader.resolve_report_price_data(
        "Bangladesh",
        "2026-06",
        ["Maize"],
        [],
        basket_items=[{"commodity_id": 1, "commodity_name_snapshot": "Maize", "weight_quantity": 1}],
        currency_code="BDT",
        enabled_modules=["fuel_energy"],
    )

    fuel = result.fuel_energy_data
    assert fuel is not None
    assert fuel["latest_month"] == "2026-05"
    assert pd.isna(result.df_national.loc[pd.Timestamp("2026-06-01"), "Fuel (diesel)"])
    diesel = next(item for item in fuel["series"] if item["kind"] == "diesel")
    petrol = next(item for item in fuel["series"] if item["kind"] == "petrol_gasoline")
    assert diesel["latest_month"] == "2026-05"
    assert diesel["current_price"] == 101
    assert diesel["mom_change_pct"] == 1.0
    assert diesel["yoy_change_pct"] is None
    assert petrol["current_price"] == 121
    assert "Fuel (gas)" not in result.df_national.columns


def test_livestock_and_labour_classifiers_avoid_false_positives():
    assert data_loader._animal_product_type("Livestock (Goat)", "Head") == "live_animal"
    assert data_loader._animal_product_type("Meat (beef)", "KG") == "meat"
    assert data_loader._animal_product_type("Milk (camel)", "L") == "milk"
    assert data_loader._animal_product_type("Eggs (brown)", "1 piece") == "eggs"
    assert data_loader._animal_product_type("Eggplants", "KG") is None
    assert data_loader._animal_product_type("Fish (live, pangasius)", "KG") is None
    assert data_loader._labour_kind("Wage (non-qualified labour, non-agricultural)", "Day") == "casual_unskilled"
    assert data_loader._labour_kind("Wage (qualified labour)", "Day") == "skilled_qualified"
    assert data_loader._labour_kind("Labour availability", "days/week") is None


def test_resolve_report_price_data_adds_livestock_dominant_unit_without_polluting_food(monkeypatch, tmp_path):
    repo = _repo(tmp_path)
    _seed_country_module_cache(
        repo,
        country_iso3="COD",
        country_name="Democratic Republic of the Congo",
        currency_code="CDF",
        currency_name="Congolese Franc",
        extra_specs=[
            {"commodity_id": 141, "commodity_name": "Meat (beef)", "unit_name": "KG", "base": 100, "step": 1},
            {"commodity_id": 140, "commodity_name": "Meat (pork)", "unit_name": "KG", "base": 200, "step": 2},
            {"commodity_id": 81, "commodity_name": "Milk", "unit_id": 200, "unit_name": "L", "base": 50, "step": 1},
            {"commodity_id": 434, "commodity_name": "Eggplants", "unit_name": "KG", "base": 30, "step": 1},
        ],
    )
    _patch_repo(monkeypatch, repo)

    result = data_loader.resolve_report_price_data(
        "Democratic Republic of the Congo",
        "2026-06",
        ["Maize"],
        [],
        basket_items=[{"commodity_id": 1, "commodity_name_snapshot": "Maize", "weight_quantity": 1}],
        currency_code="CDF",
        enabled_modules=["livestock_animal_products"],
    )
    stats = data_loader.calculate_statistics_from_csv(
        result.df_national,
        ["Maize"],
        food_basket_components=[{"commodity_id": 1, "commodity_name_snapshot": "Maize", "weight_quantity": 1}],
        currency_code="CDF",
    )

    animal = result.livestock_animal_products_data
    assert animal is not None
    assert result.cache_metadata["livestock_animal_products"]["series_count"] == 3
    assert animal["chart"]["mode"] == "absolute"
    assert [item["unit"] for item in animal["chart"]["series"]] == ["KG", "KG"]
    assert "Animal Products - Meat (beef)" in result.df_national.columns
    assert "Animal Products - Eggplants" not in result.df_national.columns
    assert result.df_national.loc[pd.Timestamp("2026-06-01"), "FoodBasket"] == 22
    assert "Animal Products - Meat (beef)" in stats["auxiliary"]
    assert "Animal Products - Meat (beef)" not in stats["commodities"]


def test_resolve_report_price_data_adds_livestock_mixed_unit_index_chart(monkeypatch, tmp_path):
    repo = _repo(tmp_path)
    _seed_country_module_cache(
        repo,
        country_iso3="SOM",
        country_name="Somalia",
        currency_code="SOS",
        currency_name="Somali Shilling",
        extra_specs=[
            {
                "commodity_id": 383,
                "commodity_name": "Livestock (Goat)",
                "unit_id": 300,
                "unit_name": "Head",
                "base": 1000,
                "step": 10,
            },
            {"commodity_id": 342, "commodity_name": "Milk (camel)", "unit_id": 200, "unit_name": "L", "base": 50, "step": 1},
            {"commodity_id": 451, "commodity_name": "Meat (goat)", "unit_name": "KG", "base": 200, "step": 2},
        ],
    )
    _patch_repo(monkeypatch, repo)

    result = data_loader.resolve_report_price_data(
        "Somalia",
        "2026-06",
        ["Maize"],
        [],
        basket_items=[{"commodity_id": 1, "commodity_name_snapshot": "Maize", "weight_quantity": 1}],
        currency_code="SOS",
        enabled_modules=["livestock_animal_products"],
    )

    animal = result.livestock_animal_products_data
    assert animal is not None
    assert animal["chart"]["mode"] == "indexed"
    assert animal["chart"]["axis_label"] == "Index (first month = 100)"
    assert {item["group"] for item in animal["series"]} == {"live_animal", "milk", "meat"}


def test_resolve_report_price_data_labour_market_purchasing_power(monkeypatch, tmp_path):
    repo = _repo(tmp_path)
    _seed_country_module_cache(
        repo,
        country_iso3="AFG",
        country_name="Afghanistan",
        currency_code="AFN",
        currency_name="Afghani",
        extra_specs=[
            {
                "commodity_id": 465,
                "commodity_name": "Wage (non-qualified labour, non-agricultural)",
                "unit_id": 500,
                "unit_name": "Day",
                "base": 300,
                "step": 10,
            },
            {
                "commodity_id": 274,
                "commodity_name": "Wage (qualified labour)",
                "unit_id": 500,
                "unit_name": "Day",
                "base": 500,
                "step": 5,
            },
        ],
    )
    _patch_repo(monkeypatch, repo)

    result = data_loader.resolve_report_price_data(
        "Afghanistan",
        "2026-06",
        ["Maize"],
        [],
        basket_items=[{"commodity_id": 1, "commodity_name_snapshot": "Maize", "weight_quantity": 1, "databridges_unit": "kg"}],
        currency_code="AFN",
        enabled_modules=["labour_market"],
    )
    stats = data_loader.calculate_statistics_from_csv(
        result.df_national,
        ["Maize"],
        food_basket_components=[{"commodity_id": 1, "commodity_name_snapshot": "Maize", "weight_quantity": 1}],
        currency_code="AFN",
    )

    labour = result.labour_market_data
    assert labour is not None
    assert [item["kind"] for item in labour["series"]] == ["casual_unskilled", "skilled_qualified"]
    assert labour["purchasing_power"]["staple_name"] == "Maize"
    assert labour["purchasing_power"]["current_kg"] == round(420 / 22, 2)
    assert labour["chart"]["mode"] == "purchasing_power"
    assert "Labour - Purchasing power (Maize)" in result.df_national.columns
    assert "Labour - Casual wage" in stats["auxiliary"]
    assert "Labour - Casual wage" not in stats["commodities"]


def test_resolve_report_price_data_no_livestock_or_labour_omits_with_warning(monkeypatch, tmp_path):
    repo = _repo(tmp_path)
    _seed_country_module_cache(
        repo,
        country_iso3="BFA",
        country_name="Burkina Faso",
        currency_code="XOF",
        currency_name="CFA Franc BCEAO",
        extra_specs=[],
    )
    _patch_repo(monkeypatch, repo)

    result = data_loader.resolve_report_price_data(
        "Burkina Faso",
        "2026-06",
        ["Maize"],
        [],
        basket_items=[{"commodity_id": 1, "commodity_name_snapshot": "Maize", "weight_quantity": 1}],
        currency_code="XOF",
        enabled_modules=["livestock_animal_products", "labour_market"],
    )

    assert result.livestock_animal_products_data is None
    assert result.labour_market_data is None
    assert result.cache_metadata["livestock_animal_products"]["series_count"] == 0
    assert result.cache_metadata["labour_market"]["series_count"] == 0
    assert any("Livestock & Animal Products omitted: no animal product" in warning for warning in result.warnings)
    assert any("Labour Market omitted: no wage" in warning for warning in result.warnings)


def test_weighted_food_basket_reports_missing_latest_components(monkeypatch, tmp_path):
    repo = _repo(tmp_path)
    _seed_loader_cache(repo, include_latest_beans=False)
    _patch_repo(monkeypatch, repo)

    national, _regional = data_loader.extract_time_series_from_csv(
        "South Sudan",
        "2025-02",
        ["Maize"],
        [],
        basket_items=_basket_items(),
    )
    stats = data_loader.calculate_statistics_from_csv(
        national,
        ["Maize"],
        food_basket_components=_basket_items(),
    )

    assert pd.isna(national.loc["2025-02-01", "FoodBasket"])
    assert stats["food_basket"]["current_price"] is None
    assert stats["food_basket"]["selected_component_count"] == 2
    assert stats["food_basket"]["available_component_names"] == ["Maize"]
    assert stats["food_basket"]["missing_component_names"] == ["Beans"]


def test_resolve_report_price_data_backfills_missing_reference_basket_component(monkeypatch, tmp_path):
    repo = _repo(tmp_path)
    _seed_loader_cache(repo, include_latest_beans=False)
    _patch_repo(monkeypatch, repo)
    beans_backfill = _price(2, "", 11, "", None, "2025-02-01", 6)
    beans_backfill["commodity_name"] = None
    beans_backfill["market_name"] = None
    beans_backfill["admin1_name"] = None
    adapter = _FakeBackfillAdapter(rows_by_commodity={2: [beans_backfill]})

    result = data_loader.resolve_report_price_data(
        "South Sudan",
        "2025-02",
        ["Maize"],
        ["Central Equatoria", "Western Bahr el Ghazal"],
        basket_items=_basket_items(),
        currency_code="SSP",
        adapter=adapter,
    )

    assert result.df_national.loc["2025-02-01", "Maize"] == 12
    assert result.df_national.loc["2025-02-01", "Beans"] == 6
    assert result.df_national.loc["2025-02-01", "FoodBasket"] == 42
    latest_regions = result.df_regional[result.df_regional["Date"] == pd.Timestamp("2025-02-01")]
    assert latest_regions.set_index("Region")["FoodBasket"].isna().to_dict() == {
        "Central Equatoria": True,
        "Western Bahr el Ghazal": True,
    }
    assert result.cache_metadata["targeted_backfill"]["attempted"] is True
    assert result.cache_metadata["targeted_backfill"]["rows_fetched"] == 1
    assert any(call["commodity_id"] == 2 for call in adapter.calls)


def test_resolve_report_price_data_unresolved_reference_basket_raises(monkeypatch, tmp_path):
    repo = _repo(tmp_path)
    _seed_loader_cache(repo)
    _patch_repo(monkeypatch, repo)
    invalid_basket = [
        {
            "commodity_id": 52,
            "commodity_name_snapshot": "Rice",
            "databridges_unit": "kg",
            "weight_quantity": 1,
        },
        {
            "commodity_id": 65,
            "commodity_name_snapshot": "Sorghum",
            "databridges_unit": "kg",
            "weight_quantity": 1,
        },
    ]

    with pytest.raises(data_loader.BasketReferenceMonthMissing) as exc_info:
        data_loader.resolve_report_price_data(
            "South Sudan",
            "2025-02",
            [],
            [],
            basket_items=invalid_basket,
            currency_code="SSP",
            adapter=_FakeBackfillAdapter(),
        )

    message = str(exc_info.value)
    assert "reference-month food basket is incomplete" in message
    assert "Rice (commodity_id=52): DataBridges returns no monthly price data" in message
    assert "Sorghum (commodity_id=65): DataBridges returns no monthly price data" in message
    assert exc_info.value.to_dict()["price_gap_report"]["hard_missing"]


def test_resolve_report_price_data_adapter_error_for_hard_gap_is_503(monkeypatch, tmp_path):
    repo = _repo(tmp_path)
    _seed_loader_cache(repo, include_latest_beans=False)
    _patch_repo(monkeypatch, repo)

    with pytest.raises(data_loader.ReportPriceBackfillUnavailable) as exc_info:
        data_loader.resolve_report_price_data(
            "South Sudan",
            "2025-02",
            ["Maize"],
            [],
            basket_items=_basket_items(),
            currency_code="SSP",
            adapter=_FakeBackfillAdapter(error_by_commodity={2: "adapter retries exhausted"}),
        )

    assert exc_info.value.status_code == 503
    assert "targeted DataBridges backfill could not verify or obtain" in str(exc_info.value)
    assert "Beans (commodity_id=2): adapter retries exhausted" in str(exc_info.value)


def test_resolve_report_price_data_backfill_does_not_pollute_global_price_cache(monkeypatch, tmp_path):
    repo = _repo(tmp_path)
    _seed_loader_cache(repo, include_latest_beans=False)
    _patch_repo(monkeypatch, repo)
    adapter = _FakeBackfillAdapter(rows_by_commodity={2: [_price(2, "Beans", 11, "Wau", "Western Bahr el Ghazal", "2025-02-01", 6)]})

    result = data_loader.resolve_report_price_data(
        "South Sudan",
        "2025-02",
        ["Maize"],
        [],
        basket_items=_basket_items(),
        currency_code="SSP",
        adapter=adapter,
    )
    cache_only, _regional = data_loader.extract_time_series_from_csv(
        "South Sudan",
        "2025-02",
        ["Maize"],
        [],
        basket_items=_basket_items(),
        currency_code="SSP",
    )

    assert result.df_national.loc["2025-02-01", "FoodBasket"] == 42
    assert pd.isna(cache_only.loc["2025-02-01", "FoodBasket"])
