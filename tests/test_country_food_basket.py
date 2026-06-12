from pathlib import Path

import pytest
from sqlalchemy import text

from app.services.market_monitor.food_basket import BasketValidationError, SqlCountryFoodBasketRepository
from app.services.price_cache.config import load_price_cache_config
from app.services.price_cache.fixtures import seed_cache_snapshot
from app.services.price_cache.migrations import apply_migrations
from app.services.price_cache.sql_repository import SqlPriceCacheRepository, create_price_cache_engine


def _price_repo(tmp_path: Path) -> SqlPriceCacheRepository:
    config = load_price_cache_config(
        {
            "PRICE_CACHE_BACKEND": "sqlite",
            "PRICE_CACHE_SQLITE_PATH": str(tmp_path / "price_cache.sqlite3"),
        }
    )
    engine = create_price_cache_engine(config)
    apply_migrations(engine, config.backend)
    return SqlPriceCacheRepository(engine)


def _basket_repo(price_repo: SqlPriceCacheRepository) -> SqlCountryFoodBasketRepository:
    return SqlCountryFoodBasketRepository(price_repo.engine)


def test_save_versions_publish_latest_and_cross_instance_visibility(tmp_path):
    price_repo = _price_repo(tmp_path)
    cache_version_id = seed_cache_snapshot(price_repo)
    repo_one = _basket_repo(price_repo)

    first = repo_one.save_basket(
        "SSD",
        items=[{"commodity_id": 1, "weight_quantity": 2}],
        created_by_user_id="alice",
        change_note="initial",
    )

    repo_two = _basket_repo(price_repo)
    active_from_second_instance = repo_two.get_active_basket("SSD")

    assert first.version_number == 1
    assert first.status == "active"
    assert first.created_by_user_id == "alice"
    assert first.cache_version_id_at_creation == cache_version_id
    assert active_from_second_instance is not None
    assert active_from_second_instance.basket_version_id == first.basket_version_id

    second = repo_two.save_basket(
        "SSD",
        items=[
            {"commodity_id": 1, "weight_quantity": 2},
            {"commodity_id": 2, "weight_quantity": 3, "item_note": "protein"},
        ],
        change_note="add beans",
    )
    reloaded_first = repo_one.get_basket_version("SSD", first.basket_version_id)
    history = repo_one.list_basket_history("SSD")

    assert second.version_number == 2
    assert second.created_by_user_id == "unknown"
    assert [item.commodity_name_snapshot for item in second.items] == ["Maize", "Beans"]
    assert reloaded_first is not None
    assert reloaded_first.status == "superseded"
    assert [version.version_number for version in history] == [2, 1]
    assert repo_one.get_active_basket("SSD").basket_version_id == second.basket_version_id


@pytest.mark.parametrize(
    ("items", "message"),
    [
        ([], "at least one"),
        ([{"commodity_id": 1, "weight_quantity": 1}, {"commodity_id": 1, "weight_quantity": 2}], "Duplicate"),
        ([{"commodity_id": 999, "weight_quantity": 1}], "not available"),
        ([{"commodity_id": 1, "weight_quantity": 0}], "positive"),
        ([{"commodity_id": 1, "weight_quantity": -1}], "positive"),
    ],
)
def test_invalid_baskets_are_rejected(tmp_path, items, message):
    price_repo = _price_repo(tmp_path)
    seed_cache_snapshot(price_repo)

    with pytest.raises(BasketValidationError, match=message):
        _basket_repo(price_repo).save_basket("SSD", items=items)


def test_unpriced_commodity_is_rejected(tmp_path):
    price_repo = _price_repo(tmp_path)
    cache_version_id = seed_cache_snapshot(price_repo)
    with price_repo.engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO cached_commodities (
                    cache_version_id,
                    country_iso3,
                    commodity_id,
                    commodity_name,
                    commodity_unit_id,
                    commodity_unit_name,
                    category_name,
                    active
                ) VALUES (
                    :cache_version_id,
                    'SSD',
                    3,
                    'Rice',
                    100,
                    'kg',
                    'Cereals',
                    1
                )
                """
            ),
            {"cache_version_id": cache_version_id},
        )

    with pytest.raises(BasketValidationError, match="no cached price rows"):
        _basket_repo(price_repo).save_basket("SSD", items=[{"commodity_id": 3, "weight_quantity": 1}])


def test_missing_commodity_unit_falls_back_to_price_row_unit(tmp_path):
    price_repo = _price_repo(tmp_path)
    cache_version_id = seed_cache_snapshot(price_repo)
    with price_repo.engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE cached_commodities
                SET commodity_unit_name = ''
                WHERE cache_version_id = :cache_version_id
                  AND country_iso3 = 'SSD'
                  AND commodity_id = 1
                """
            ),
            {"cache_version_id": cache_version_id},
        )

    basket = _basket_repo(price_repo).save_basket("SSD", items=[{"commodity_id": 1, "weight_quantity": 1}])

    assert basket.items[0].databridges_unit == "kg"
    assert basket.items[0].databridges_unit_id == 100


def test_missing_databridges_unit_is_rejected_when_price_rows_also_lack_unit(tmp_path):
    price_repo = _price_repo(tmp_path)
    cache_version_id = seed_cache_snapshot(price_repo)
    with price_repo.engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE cached_commodities
                SET commodity_unit_name = ''
                WHERE cache_version_id = :cache_version_id
                  AND country_iso3 = 'SSD'
                  AND commodity_id = 1
                """
            ),
            {"cache_version_id": cache_version_id},
        )
        conn.execute(
            text(
                """
                UPDATE cached_price_monthly
                SET commodity_unit_name = '',
                    commodity_unit_id = NULL
                WHERE cache_version_id = :cache_version_id
                  AND country_iso3 = 'SSD'
                  AND commodity_id = 1
                """
            ),
            {"cache_version_id": cache_version_id},
        )

    with pytest.raises(BasketValidationError, match="no Databridges unit"):
        _basket_repo(price_repo).save_basket("SSD", items=[{"commodity_id": 1, "weight_quantity": 1}])
