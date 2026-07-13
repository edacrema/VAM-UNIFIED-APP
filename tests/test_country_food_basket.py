from pathlib import Path

import pytest
from sqlalchemy import text

from app.services.market_monitor import food_basket as basket_module
from app.services.market_monitor.food_basket import (
    DEFAULT_PRIMARY_BASKET_DESCRIPTION,
    BasketRole,
    BasketScopeType,
    BasketValidationError,
    SqlCountryFoodBasketRepository,
)
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
    assert first.basket_role is BasketRole.PRIMARY
    assert first.basket_name == "MEB"
    assert first.short_description == DEFAULT_PRIMARY_BASKET_DESCRIPTION
    assert first.scope_type is BasketScopeType.NATIONAL
    assert first.regions == []
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


def test_primary_and_secondary_version_independently_with_country_wide_numbers(tmp_path):
    price_repo = _price_repo(tmp_path)
    seed_cache_snapshot(price_repo)
    repo = _basket_repo(price_repo)

    primary_v1 = repo.save_basket(
        "SSD",
        items=[{"commodity_id": 1, "weight_quantity": 2}],
    )
    secondary_v1 = repo.save_basket(
        "SSD",
        role=BasketRole.SECONDARY,
        basket_name="Pastoral Basket",
        short_description="Pastoral household affordability proxy.",
        items=[{"commodity_id": 2, "weight_quantity": 3}],
    )
    primary_v2 = repo.save_basket(
        "SSD",
        basket_name="Urban Reference Basket",
        short_description="Urban household reference basket.",
        items=[{"commodity_id": 1, "weight_quantity": 4}],
    )

    active_after_primary_save = repo.get_active_baskets("SSD")
    assert active_after_primary_save["primary"].basket_version_id == primary_v2.basket_version_id
    assert active_after_primary_save["secondary"].basket_version_id == secondary_v1.basket_version_id

    secondary_v2 = repo.save_basket(
        "SSD",
        role="secondary",
        basket_name="Pastoral Basket",
        short_description="Revised pastoral household affordability proxy.",
        items=[{"commodity_id": 2, "weight_quantity": 5}],
    )

    active = repo.get_active_baskets("SSD")
    assert [primary_v1.version_number, secondary_v1.version_number, primary_v2.version_number, secondary_v2.version_number] == [
        1,
        2,
        3,
        4,
    ]
    assert active["primary"].basket_version_id == primary_v2.basket_version_id
    assert active["secondary"].basket_version_id == secondary_v2.basket_version_id
    assert repo.get_basket_version("SSD", primary_v1.basket_version_id).status == "superseded"
    assert repo.get_basket_version("SSD", secondary_v1.basket_version_id).status == "superseded"
    assert [item.version_number for item in repo.list_basket_history("SSD", BasketRole.PRIMARY)] == [3, 1]
    assert [item.version_number for item in repo.list_basket_history("SSD", BasketRole.SECONDARY)] == [4, 2]


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"role": "tertiary"}, "Invalid basket role"),
        ({"scope_type": "local"}, "Invalid basket scope"),
        ({"basket_name": "Custom Primary"}, "requires a short description"),
        (
            {"role": BasketRole.SECONDARY, "short_description": "A description"},
            "requires a name",
        ),
        (
            {"role": BasketRole.SECONDARY, "basket_name": "Healthy Diet"},
            "requires a short description",
        ),
        (
            {"scope_type": BasketScopeType.SELECTED_REGIONS, "regions": []},
            "requires at least one region",
        ),
        (
            {"scope_type": BasketScopeType.SELECTED_REGIONS, "regions": ["Unknown Region"]},
            "not available",
        ),
    ],
)
def test_basket_metadata_and_scope_validation(tmp_path, overrides, message):
    price_repo = _price_repo(tmp_path)
    seed_cache_snapshot(price_repo)
    payload = {"items": [{"commodity_id": 1, "weight_quantity": 1}], **overrides}

    with pytest.raises(BasketValidationError, match=message):
        _basket_repo(price_repo).save_basket("SSD", **payload)


def test_scope_normalization_uses_canonical_regions_and_deduplicates_input(tmp_path):
    price_repo = _price_repo(tmp_path)
    seed_cache_snapshot(price_repo)
    repo = _basket_repo(price_repo)

    national = repo.save_basket(
        "SSD",
        regions=["Not a real region"],
        items=[{"commodity_id": 1, "weight_quantity": 1}],
    )
    secondary = repo.save_basket(
        "SSD",
        role=BasketRole.SECONDARY,
        basket_name="Regional Basket",
        short_description="Reference basket for selected regions.",
        scope_type=BasketScopeType.SELECTED_REGIONS,
        regions=[
            "western bahr el ghazal",
            " Central Equatoria ",
            "CENTRAL EQUATORIA",
        ],
        items=[{"commodity_id": 2, "weight_quantity": 2}],
    )

    assert national.regions == []
    assert secondary.scope_type is BasketScopeType.SELECTED_REGIONS
    assert secondary.regions == ["Western Bahr el Ghazal", "Central Equatoria"]
    assert secondary.to_dict()["basket_role"] == "secondary"
    assert secondary.to_dict()["scope_type"] == "selected_regions"


def test_secondary_archive_preserves_history_and_primary_cannot_be_archived(tmp_path):
    price_repo = _price_repo(tmp_path)
    seed_cache_snapshot(price_repo)
    repo = _basket_repo(price_repo)
    primary = repo.save_basket(
        "SSD",
        items=[{"commodity_id": 1, "weight_quantity": 1}],
    )
    secondary = repo.save_basket(
        "SSD",
        role=BasketRole.SECONDARY,
        basket_name="Regional Basket",
        short_description="Reference basket for Central Equatoria.",
        scope_type=BasketScopeType.SELECTED_REGIONS,
        regions=["Central Equatoria"],
        items=[{"commodity_id": 2, "weight_quantity": 2}],
    )

    with pytest.raises(BasketValidationError, match="cannot be archived"):
        repo.archive_basket("SSD", BasketRole.PRIMARY)

    archived = repo.archive_secondary_basket("SSD")

    assert archived is not None
    assert archived.basket_version_id == secondary.basket_version_id
    assert archived.status == "archived"
    assert archived.regions == ["Central Equatoria"]
    assert repo.get_active_basket("SSD", BasketRole.PRIMARY).basket_version_id == primary.basket_version_id
    assert repo.get_active_basket("SSD", BasketRole.SECONDARY) is None
    assert repo.get_active_baskets("SSD")["secondary"] is None
    assert repo.get_basket_version("SSD", secondary.basket_version_id).status == "archived"
    assert repo.list_basket_history("SSD", BasketRole.SECONDARY)[0].status == "archived"
    assert repo.archive_secondary_basket("SSD") is None

    replacement = repo.save_basket(
        "SSD",
        role=BasketRole.SECONDARY,
        basket_name="Replacement Basket",
        short_description="Replacement secondary reference basket.",
        items=[{"commodity_id": 2, "weight_quantity": 3}],
    )
    assert replacement.version_number == 3
    assert repo.get_basket_version("SSD", secondary.basket_version_id).status == "archived"


def test_primary_compatibility_helpers_default_metadata_and_reject_secondary(tmp_path, monkeypatch):
    price_repo = _price_repo(tmp_path)
    seed_cache_snapshot(price_repo)
    repo = _basket_repo(price_repo)
    monkeypatch.setattr(basket_module, "create_food_basket_repository", lambda: repo)

    response = basket_module.save_country_basket(
        "South Sudan",
        {"items": [{"commodity_id": 1, "weight_quantity": 2}]},
    )
    report_basket = basket_module.get_active_basket_for_report("South Sudan")

    assert response["active_basket"]["basket_role"] == "primary"
    assert response["active_basket"]["basket_name"] == "MEB"
    assert response["active_basket"]["short_description"] == DEFAULT_PRIMARY_BASKET_DESCRIPTION
    assert report_basket["basket_version_id"] == response["active_basket"]["basket_version_id"]
    assert report_basket["basket_role"] == "primary"

    with pytest.raises(BasketValidationError, match="primary-only"):
        basket_module.save_country_basket(
            "South Sudan",
            {
                "basket_role": "secondary",
                "basket_name": "Secondary",
                "short_description": "Secondary basket.",
                "items": [{"commodity_id": 2, "weight_quantity": 1}],
            },
        )


def test_plural_service_helpers_save_list_and_archive_both_roles(tmp_path, monkeypatch):
    price_repo = _price_repo(tmp_path)
    seed_cache_snapshot(price_repo)
    repo = _basket_repo(price_repo)
    monkeypatch.setattr(basket_module, "create_food_basket_repository", lambda: repo)

    empty = basket_module.get_country_baskets_response("South Sudan")
    assert empty["primary"] is None
    assert empty["secondary"] is None
    assert empty["needs_primary_setup"] is True
    assert empty["has_secondary"] is False

    primary_response = basket_module.save_country_basket_role(
        "South Sudan",
        BasketRole.PRIMARY,
        {"items": [{"commodity_id": 1, "weight_quantity": 2}]},
    )
    secondary_response = basket_module.save_country_basket_role(
        "South Sudan",
        BasketRole.SECONDARY,
        {
            "basket_name": "Pastoral Basket",
            "short_description": "Pastoral household affordability proxy.",
            "items": [{"commodity_id": 2, "weight_quantity": 3}],
        },
    )

    assert primary_response["primary"]["basket_role"] == "primary"
    assert secondary_response["secondary"]["basket_role"] == "secondary"
    assert secondary_response["has_secondary"] is True
    assert basket_module.list_country_basket_role_history(
        "South Sudan",
        BasketRole.PRIMARY,
    )["versions"][0]["version_number"] == 1
    assert basket_module.list_country_basket_role_history(
        "South Sudan",
        BasketRole.SECONDARY,
    )["versions"][0]["version_number"] == 2

    with pytest.raises(BasketValidationError, match="does not match"):
        basket_module.save_country_basket_role(
            "South Sudan",
            BasketRole.SECONDARY,
            {
                "basket_role": "primary",
                "basket_name": "Wrong Role",
                "short_description": "Wrong role payload.",
                "items": [{"commodity_id": 2, "weight_quantity": 1}],
            },
        )

    archived = basket_module.archive_country_secondary_basket("South Sudan")
    archived_again = basket_module.archive_country_secondary_basket("South Sudan")
    assert archived["archived_secondary"]["basket_name"] == "Pastoral Basket"
    assert archived["secondary"] is None
    assert archived["has_secondary"] is False
    assert archived_again["archived_secondary"] is None


def test_report_selection_resolves_versions_and_ignores_excluded_secondary(tmp_path, monkeypatch):
    price_repo = _price_repo(tmp_path)
    seed_cache_snapshot(price_repo)
    repo = _basket_repo(price_repo)
    monkeypatch.setattr(basket_module, "create_food_basket_repository", lambda: repo)
    primary = repo.save_basket(
        "SSD",
        items=[{"commodity_id": 1, "weight_quantity": 2}],
    )
    secondary = repo.save_basket(
        "SSD",
        role=BasketRole.SECONDARY,
        basket_name="Pastoral Basket",
        short_description="Pastoral household affordability proxy.",
        items=[{"commodity_id": 2, "weight_quantity": 3}],
    )

    included = basket_module.resolve_baskets_for_report("South Sudan")
    excluded = basket_module.resolve_baskets_for_report(
        "South Sudan",
        primary_basket_version_id=primary.basket_version_id,
        include_secondary_basket=False,
        secondary_basket_version_id="stale-secondary",
    )

    assert included.primary_basket_version_id == primary.basket_version_id
    assert included.secondary_basket_version_id == secondary.basket_version_id
    assert included.secondary_basket_included is True
    assert excluded.secondary is None
    assert excluded.secondary_basket_included is False

    with pytest.raises(basket_module.BasketVersionConflict, match="primary basket 'MEB'"):
        basket_module.resolve_baskets_for_report(
            "South Sudan",
            primary_basket_version_id="stale-primary",
        )
    with pytest.raises(basket_module.BasketVersionConflict, match="secondary basket 'Pastoral Basket'"):
        basket_module.resolve_baskets_for_report(
            "South Sudan",
            secondary_basket_version_id="stale-secondary",
        )


def test_report_selection_normalizes_absent_secondary_and_conflicts_on_explicit_id(tmp_path, monkeypatch):
    price_repo = _price_repo(tmp_path)
    seed_cache_snapshot(price_repo)
    repo = _basket_repo(price_repo)
    monkeypatch.setattr(basket_module, "create_food_basket_repository", lambda: repo)
    repo.save_basket(
        "SSD",
        items=[{"commodity_id": 1, "weight_quantity": 2}],
    )

    selection = basket_module.resolve_baskets_for_report(
        "South Sudan",
        include_secondary_basket=True,
    )
    assert selection.secondary is None
    assert selection.secondary_basket_included is False

    with pytest.raises(basket_module.BasketVersionConflict, match="secondary basket version"):
        basket_module.resolve_baskets_for_report(
            "South Sudan",
            include_secondary_basket=True,
            secondary_basket_version_id="archived-secondary",
        )


def test_result_contract_attaches_selection_and_normalizes_legacy_results(tmp_path, monkeypatch):
    price_repo = _price_repo(tmp_path)
    seed_cache_snapshot(price_repo)
    repo = _basket_repo(price_repo)
    monkeypatch.setattr(basket_module, "create_food_basket_repository", lambda: repo)
    repo.save_basket(
        "SSD",
        items=[{"commodity_id": 1, "weight_quantity": 2}],
    )
    repo.save_basket(
        "SSD",
        role=BasketRole.SECONDARY,
        basket_name="Pastoral Basket",
        short_description="Pastoral household affordability proxy.",
        items=[{"commodity_id": 2, "weight_quantity": 3}],
    )
    selection = basket_module.resolve_baskets_for_report("South Sudan")

    result = basket_module.attach_basket_selection_to_result(
        {"data_statistics": {"food_basket": {"current_price": 42}}},
        selection,
    )
    legacy = basket_module.attach_basket_selection_to_result(
        {
            "food_basket": {"basket_version_id": "legacy-primary"},
            "data_statistics": {"food_basket": {"current_price": 21}},
        },
        None,
    )
    mock = basket_module.attach_basket_selection_to_result({}, None)

    assert result["food_baskets"]["primary"]["basket_name"] == "MEB"
    assert result["food_baskets"]["secondary"]["basket_name"] == "Pastoral Basket"
    assert result["basket_statistics"] == {
        "primary": {"current_price": 42},
        "secondary": None,
    }
    assert result["secondary_basket_included"] is True
    assert legacy["food_baskets"]["primary"]["basket_version_id"] == "legacy-primary"
    assert legacy["basket_statistics"]["primary"] == {"current_price": 21}
    assert mock["food_baskets"] == {"primary": None, "secondary": None}
    assert mock["secondary_basket_included"] is False


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


def test_empty_priced_commodity_set_is_rejected(tmp_path):
    price_repo = _price_repo(tmp_path)
    cache_version_id = seed_cache_snapshot(price_repo)
    with price_repo.engine.begin() as conn:
        conn.execute(
            text(
                """
                DELETE FROM cached_price_monthly
                WHERE cache_version_id = :cache_version_id
                  AND country_iso3 = 'SSD'
                """
            ),
            {"cache_version_id": cache_version_id},
        )

    with pytest.raises(BasketValidationError, match="No commodities with cached price rows"):
        _basket_repo(price_repo).save_basket("SSD", items=[{"commodity_id": 1, "weight_quantity": 1}])


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
