from pathlib import Path
from datetime import date

import pandas as pd
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
    assert dict(zip(latest_regions["Region"], latest_regions["FoodBasket"])) == {
        "Central Equatoria": 24,
        "Western Bahr el Ghazal": 18,
    }
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

    assert national.loc["2025-02-01", "FoodBasket"] == 24
    assert stats["food_basket"]["current_price"] == 24
    assert stats["food_basket"]["selected_component_count"] == 2
    assert stats["food_basket"]["available_component_names"] == ["Maize"]
    assert stats["food_basket"]["missing_component_names"] == ["Beans"]
