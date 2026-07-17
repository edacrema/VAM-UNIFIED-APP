"""Shared country food basket storage and validation."""
from __future__ import annotations

import uuid
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import StrEnum
from typing import Any, Mapping, Optional, Sequence

from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.services.price_cache.config import load_price_cache_config
from app.services.price_cache.db_resilience import retry_disconnected_read
from app.services.price_cache.migrations import apply_migrations
from app.services.price_cache.sql_repository import SqlPriceCacheRepository, create_price_cache_engine
from app.shared.countries import resolve_country
from .features import SecondBasketFeatureDisabled, market_monitor_second_basket_enabled


class BasketValidationError(ValueError):
    """Raised when a proposed basket cannot be published."""


class BasketNotConfigured(RuntimeError):
    """Raised when report generation needs a basket but no active basket exists."""


class BasketVersionConflict(RuntimeError):
    """Raised when a report references a stale basket version."""


class BasketRole(StrEnum):
    PRIMARY = "primary"
    SECONDARY = "secondary"


class BasketScopeType(StrEnum):
    NATIONAL = "national"
    SELECTED_REGIONS = "selected_regions"


DEFAULT_PRIMARY_BASKET_NAME = "MEB"
DEFAULT_PRIMARY_BASKET_DESCRIPTION = (
    "Primary MEB reference basket configured by the Country Office."
)


_FOOD_BASKET_REPOSITORY: Optional[SqlCountryFoodBasketRepository] = None
_FOOD_BASKET_REPOSITORY_LOCK = threading.Lock()


class BasketItemInput(BaseModel):
    commodity_id: int = Field(..., description="Databridges commodity ID.")
    weight_quantity: float = Field(..., gt=0, description="Quantity in the commodity's Databridges unit.")
    item_note: Optional[str] = None


class BasketSaveInput(BaseModel):
    basket_role: BasketRole = BasketRole.PRIMARY
    basket_name: Optional[str] = None
    short_description: Optional[str] = None
    scope_type: BasketScopeType = BasketScopeType.NATIONAL
    regions: list[str] = Field(default_factory=list)
    items: list[BasketItemInput] = Field(default_factory=list)
    change_note: Optional[str] = None
    created_by_user_id: Optional[str] = None


@dataclass(frozen=True)
class CountryFoodBasketItem:
    basket_item_id: str
    basket_version_id: str
    commodity_id: int
    commodity_name_snapshot: str
    databridges_unit_id: Optional[int]
    databridges_unit: str
    weight_quantity: float
    sort_order: int
    item_note: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "basket_item_id": self.basket_item_id,
            "basket_version_id": self.basket_version_id,
            "commodity_id": self.commodity_id,
            "commodity_name_snapshot": self.commodity_name_snapshot,
            "commodity_name": self.commodity_name_snapshot,
            "databridges_unit_id": self.databridges_unit_id,
            "databridges_unit": self.databridges_unit,
            "unit": self.databridges_unit,
            "weight_quantity": self.weight_quantity,
            "sort_order": self.sort_order,
            "item_note": self.item_note,
        }


@dataclass(frozen=True)
class CountryFoodBasketVersion:
    basket_version_id: str
    country_iso3: str
    version_number: int
    basket_role: BasketRole
    basket_name: str
    short_description: Optional[str]
    scope_type: BasketScopeType
    status: str
    created_at: datetime
    created_by_user_id: str
    cache_version_id_at_creation: Optional[str] = None
    change_note: Optional[str] = None
    regions: list[str] = field(default_factory=list)
    items: list[CountryFoodBasketItem] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "basket_version_id": self.basket_version_id,
            "country_iso3": self.country_iso3,
            "version_number": self.version_number,
            "basket_role": self.basket_role.value,
            "basket_name": self.basket_name,
            "short_description": self.short_description,
            "scope_type": self.scope_type.value,
            "status": self.status,
            "created_at": _datetime_wire(self.created_at),
            "created_by_user_id": self.created_by_user_id,
            "cache_version_id_at_creation": self.cache_version_id_at_creation,
            "change_note": self.change_note,
            "regions": list(self.regions),
            "items": [item.to_dict() for item in self.items],
        }


@dataclass(frozen=True)
class ResolvedBasketSelection:
    country: str
    iso3: str
    primary: CountryFoodBasketVersion
    secondary: Optional[CountryFoodBasketVersion]
    secondary_basket_included: bool

    @property
    def primary_basket_version_id(self) -> str:
        return self.primary.basket_version_id

    @property
    def secondary_basket_version_id(self) -> Optional[str]:
        if self.secondary is None:
            return None
        return self.secondary.basket_version_id

    def food_baskets_dict(self) -> dict[str, Any]:
        return {
            BasketRole.PRIMARY.value: self.primary.to_dict(),
            BasketRole.SECONDARY.value: self.secondary.to_dict() if self.secondary else None,
        }

    def to_metadata(self) -> dict[str, Any]:
        return {
            "country": self.country,
            "iso3": self.iso3,
            "primary_basket_version_id": self.primary_basket_version_id,
            "secondary_basket_version_id": self.secondary_basket_version_id,
            "secondary_basket_included": self.secondary_basket_included,
            "food_baskets": self.food_baskets_dict(),
        }


class SqlCountryFoodBasketRepository:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        self.price_repo = SqlPriceCacheRepository(engine)

    @retry_disconnected_read
    def get_active_basket(
        self,
        country_iso3: str,
        role: BasketRole | str = BasketRole.PRIMARY,
    ) -> Optional[CountryFoodBasketVersion]:
        country = str(country_iso3 or "").upper()
        basket_role = _normalize_basket_role(role)
        with self.engine.begin() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT v.*
                    FROM country_food_basket_current c
                    JOIN country_food_basket_versions v
                      ON v.basket_version_id = c.active_basket_version_id
                    WHERE c.country_iso3 = :country_iso3
                      AND c.basket_role = :basket_role
                      AND v.basket_role = c.basket_role
                      AND v.status = 'active'
                    """
                ),
                {"country_iso3": country, "basket_role": basket_role.value},
            ).mappings().first()
        if row is None:
            return None
        version_id = str(row["basket_version_id"])
        return self._version_from_row(
            row,
            regions=self._get_regions(version_id),
            items=self._get_items(version_id),
        )

    @retry_disconnected_read
    def get_active_baskets(
        self,
        country_iso3: str,
    ) -> dict[str, Optional[CountryFoodBasketVersion]]:
        country = str(country_iso3 or "").upper()
        with self.engine.begin() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT v.*
                    FROM country_food_basket_current c
                    JOIN country_food_basket_versions v
                      ON v.basket_version_id = c.active_basket_version_id
                    WHERE c.country_iso3 = :country_iso3
                      AND v.basket_role = c.basket_role
                      AND v.status = 'active'
                    ORDER BY c.basket_role
                    """
                ),
                {"country_iso3": country},
            ).mappings().all()

        baskets: dict[str, Optional[CountryFoodBasketVersion]] = {
            BasketRole.PRIMARY.value: None,
            BasketRole.SECONDARY.value: None,
        }
        for row in rows:
            version_id = str(row["basket_version_id"])
            basket = self._version_from_row(
                row,
                regions=self._get_regions(version_id),
                items=self._get_items(version_id),
            )
            baskets[basket.basket_role.value] = basket
        return baskets

    @retry_disconnected_read
    def get_basket_version(
        self,
        country_iso3: str,
        basket_version_id: str,
    ) -> Optional[CountryFoodBasketVersion]:
        with self.engine.begin() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT *
                    FROM country_food_basket_versions
                    WHERE country_iso3 = :country_iso3
                      AND basket_version_id = :basket_version_id
                    """
                ),
                {
                    "country_iso3": str(country_iso3 or "").upper(),
                    "basket_version_id": str(basket_version_id),
                },
            ).mappings().first()
        if row is None:
            return None
        version_id = str(row["basket_version_id"])
        return self._version_from_row(
            row,
            regions=self._get_regions(version_id),
            items=self._get_items(version_id),
        )

    @retry_disconnected_read
    def list_basket_history(
        self,
        country_iso3: str,
        role: BasketRole | str = BasketRole.PRIMARY,
        *,
        limit: int = 20,
    ) -> list[CountryFoodBasketVersion]:
        safe_limit = min(max(int(limit), 1), 100)
        basket_role = _normalize_basket_role(role)
        with self.engine.begin() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT *
                    FROM country_food_basket_versions
                    WHERE country_iso3 = :country_iso3
                      AND basket_role = :basket_role
                    ORDER BY version_number DESC
                    LIMIT :limit
                    """
                ),
                {
                    "country_iso3": str(country_iso3 or "").upper(),
                    "basket_role": basket_role.value,
                    "limit": safe_limit,
                },
            ).mappings().all()
        return [
            self._version_from_row(
                row,
                regions=self._get_regions(str(row["basket_version_id"])),
                items=self._get_items(str(row["basket_version_id"])),
            )
            for row in rows
        ]

    def save_basket(
        self,
        country_iso3: str,
        *,
        role: BasketRole | str = BasketRole.PRIMARY,
        basket_name: Optional[str] = None,
        short_description: Optional[str] = None,
        scope_type: BasketScopeType | str = BasketScopeType.NATIONAL,
        regions: Sequence[str] = (),
        items: Sequence[Mapping[str, Any] | Any],
        created_by_user_id: Optional[str] = None,
        change_note: Optional[str] = None,
    ) -> CountryFoodBasketVersion:
        country = str(country_iso3 or "").upper()
        basket_role = _normalize_basket_role(role)
        basket_scope = _normalize_basket_scope(scope_type)
        normalized_name, normalized_description = _normalize_basket_metadata(
            basket_role,
            basket_name,
            short_description,
        )
        normalized_regions = self._validate_regions(country, basket_scope, regions)
        normalized, cache_version_id = self._validate_items(country, items)
        version_id = str(uuid.uuid4())
        created_at = _now()
        user_id = str(created_by_user_id or "unknown").strip() or "unknown"
        note = _optional_str(change_note)

        with self.engine.begin() as conn:
            current_row = conn.execute(
                text(
                    """
                    SELECT active_basket_version_id
                    FROM country_food_basket_current
                    WHERE country_iso3 = :country_iso3
                      AND basket_role = :basket_role
                    """
                ),
                {"country_iso3": country, "basket_role": basket_role.value},
            ).mappings().first()
            if current_row is not None:
                conn.execute(
                    text(
                        """
                        UPDATE country_food_basket_versions
                        SET status = 'superseded'
                        WHERE basket_version_id = :basket_version_id
                          AND basket_role = :basket_role
                        """
                    ),
                    {
                        "basket_version_id": str(current_row["active_basket_version_id"]),
                        "basket_role": basket_role.value,
                    },
                )

            next_version = conn.execute(
                text(
                    """
                    SELECT COALESCE(MAX(version_number), 0) + 1 AS version_number
                    FROM country_food_basket_versions
                    WHERE country_iso3 = :country_iso3
                    """
                ),
                {"country_iso3": country},
            ).mappings().first()
            version_number = int((next_version or {}).get("version_number") or 1)

            conn.execute(
                text(
                    """
                    INSERT INTO country_food_basket_versions (
                        basket_version_id,
                        country_iso3,
                        version_number,
                        basket_role,
                        basket_name,
                        short_description,
                        scope_type,
                        status,
                        created_at,
                        created_by_user_id,
                        cache_version_id_at_creation,
                        change_note
                    ) VALUES (
                        :basket_version_id,
                        :country_iso3,
                        :version_number,
                        :basket_role,
                        :basket_name,
                        :short_description,
                        :scope_type,
                        'active',
                        :created_at,
                        :created_by_user_id,
                        :cache_version_id_at_creation,
                        :change_note
                    )
                    """
                ),
                {
                    "basket_version_id": version_id,
                    "country_iso3": country,
                    "version_number": version_number,
                    "basket_role": basket_role.value,
                    "basket_name": normalized_name,
                    "short_description": normalized_description,
                    "scope_type": basket_scope.value,
                    "created_at": _datetime_wire(created_at),
                    "created_by_user_id": user_id,
                    "cache_version_id_at_creation": cache_version_id,
                    "change_note": note,
                },
            )

            rows = []
            for index, item in enumerate(normalized, start=1):
                rows.append(
                    {
                        "basket_item_id": str(uuid.uuid4()),
                        "basket_version_id": version_id,
                        "commodity_id": item["commodity_id"],
                        "commodity_name_snapshot": item["commodity_name_snapshot"],
                        "databridges_unit_id": item["databridges_unit_id"],
                        "databridges_unit": item["databridges_unit"],
                        "weight_quantity": item["weight_quantity"],
                        "sort_order": index,
                        "item_note": item["item_note"],
                    }
                )
            conn.execute(
                text(
                    """
                    INSERT INTO country_food_basket_items (
                        basket_item_id,
                        basket_version_id,
                        commodity_id,
                        commodity_name_snapshot,
                        databridges_unit_id,
                        databridges_unit,
                        weight_quantity,
                        sort_order,
                        item_note
                    ) VALUES (
                        :basket_item_id,
                        :basket_version_id,
                        :commodity_id,
                        :commodity_name_snapshot,
                        :databridges_unit_id,
                        :databridges_unit,
                        :weight_quantity,
                        :sort_order,
                        :item_note
                    )
                    """
                ),
                rows,
            )

            if normalized_regions:
                region_rows = [
                    {
                        "basket_region_id": str(uuid.uuid4()),
                        "basket_version_id": version_id,
                        "region_name": region_name,
                        "sort_order": index,
                    }
                    for index, region_name in enumerate(normalized_regions, start=1)
                ]
                conn.execute(
                    text(
                        """
                        INSERT INTO country_food_basket_regions (
                            basket_region_id,
                            basket_version_id,
                            region_name,
                            sort_order
                        ) VALUES (
                            :basket_region_id,
                            :basket_version_id,
                            :region_name,
                            :sort_order
                        )
                        """
                    ),
                    region_rows,
                )

            conn.execute(
                text(
                    """
                    DELETE FROM country_food_basket_current
                    WHERE country_iso3 = :country_iso3
                      AND basket_role = :basket_role
                    """
                ),
                {"country_iso3": country, "basket_role": basket_role.value},
            )
            conn.execute(
                text(
                    """
                    INSERT INTO country_food_basket_current (
                        country_iso3,
                        basket_role,
                        active_basket_version_id,
                        updated_at,
                        updated_by_user_id
                    ) VALUES (
                        :country_iso3,
                        :basket_role,
                        :active_basket_version_id,
                        :updated_at,
                        :updated_by_user_id
                    )
                    """
                ),
                {
                    "country_iso3": country,
                    "basket_role": basket_role.value,
                    "active_basket_version_id": version_id,
                    "updated_at": _datetime_wire(created_at),
                    "updated_by_user_id": user_id,
                },
            )

        saved = self.get_basket_version(country, version_id)
        if saved is None:
            raise RuntimeError(f"Basket save completed but version {version_id} could not be reloaded for {country}.")
        return saved

    def archive_basket(
        self,
        country_iso3: str,
        role: BasketRole | str,
    ) -> Optional[CountryFoodBasketVersion]:
        basket_role = _normalize_basket_role(role)
        if basket_role is BasketRole.PRIMARY:
            raise BasketValidationError("The primary food basket cannot be archived.")

        country = str(country_iso3 or "").upper()
        archived_version_id: Optional[str] = None
        with self.engine.begin() as conn:
            current_row = conn.execute(
                text(
                    """
                    SELECT active_basket_version_id
                    FROM country_food_basket_current
                    WHERE country_iso3 = :country_iso3
                      AND basket_role = :basket_role
                    """
                ),
                {"country_iso3": country, "basket_role": basket_role.value},
            ).mappings().first()
            if current_row is None:
                return None

            archived_version_id = str(current_row["active_basket_version_id"])
            conn.execute(
                text(
                    """
                    UPDATE country_food_basket_versions
                    SET status = 'archived'
                    WHERE basket_version_id = :basket_version_id
                      AND country_iso3 = :country_iso3
                      AND basket_role = :basket_role
                    """
                ),
                {
                    "basket_version_id": archived_version_id,
                    "country_iso3": country,
                    "basket_role": basket_role.value,
                },
            )
            conn.execute(
                text(
                    """
                    DELETE FROM country_food_basket_current
                    WHERE country_iso3 = :country_iso3
                      AND basket_role = :basket_role
                    """
                ),
                {"country_iso3": country, "basket_role": basket_role.value},
            )

        if archived_version_id is None:
            return None
        return self.get_basket_version(country, archived_version_id)

    def archive_secondary_basket(self, country_iso3: str) -> Optional[CountryFoodBasketVersion]:
        return self.archive_basket(country_iso3, BasketRole.SECONDARY)

    def _validate_regions(
        self,
        country_iso3: str,
        scope_type: BasketScopeType,
        regions: Sequence[str],
    ) -> list[str]:
        if scope_type is BasketScopeType.NATIONAL:
            return []

        metadata = self.price_repo.get_country_metadata(country_iso3)
        if metadata is None:
            raise BasketValidationError(f"No active PriceCache metadata is available for {country_iso3}.")
        availability = self.price_repo.get_country_availability(country_iso3)
        if availability is None:
            raise BasketValidationError(f"No active PriceCache availability is available for {country_iso3}.")

        available_regions = [
            str(region).strip()
            for region in availability.admin1_names or []
            if str(region).strip()
        ]
        if not available_regions:
            available_regions = [
                str(market.admin1_name).strip()
                for market in metadata.markets
                if market.admin1_name and str(market.admin1_name).strip()
            ]

        canonical_by_key: dict[str, str] = {}
        for region in available_regions:
            canonical_by_key.setdefault(region.casefold(), region)
        if not canonical_by_key:
            raise BasketValidationError(f"No regions are available for {country_iso3} in the active PriceCache.")

        normalized: list[str] = []
        seen: set[str] = set()
        unknown: list[str] = []
        for raw_region in list(regions or []):
            candidate = str(raw_region or "").strip()
            if not candidate:
                continue
            key = candidate.casefold()
            canonical = canonical_by_key.get(key)
            if canonical is None:
                unknown.append(candidate)
                continue
            if key in seen:
                continue
            seen.add(key)
            normalized.append(canonical)

        if unknown:
            raise BasketValidationError(
                f"Regions are not available for {country_iso3}: {', '.join(unknown)}."
            )
        if not normalized:
            raise BasketValidationError("Selected-regions basket scope requires at least one region.")
        return normalized

    def _validate_items(
        self,
        country_iso3: str,
        items: Sequence[Mapping[str, Any] | Any],
    ) -> tuple[list[dict[str, Any]], Optional[str]]:
        metadata = self.price_repo.get_country_metadata(country_iso3)
        if metadata is None:
            raise BasketValidationError(f"No active PriceCache metadata is available for {country_iso3}.")
        availability = self.price_repo.get_country_availability(country_iso3)
        if availability is None:
            raise BasketValidationError(f"No active PriceCache availability is available for {country_iso3}.")

        raw_items = list(items or [])
        if not raw_items:
            raise BasketValidationError("Food basket must contain at least one commodity.")

        commodities = {int(item.commodity_id): item for item in metadata.commodities}
        priced_ids = {int(item) for item in availability.priced_commodity_ids or []}
        if not priced_ids:
            raise BasketValidationError(f"No commodities with cached price rows are available for {country_iso3}.")
        seen: set[int] = set()
        normalized: list[dict[str, Any]] = []

        for raw in raw_items:
            commodity_id = _required_int(_payload_get(raw, "commodity_id"), "commodity_id")
            if commodity_id in seen:
                raise BasketValidationError(f"Duplicate basket commodity ID {commodity_id}.")
            seen.add(commodity_id)

            commodity = commodities.get(commodity_id)
            if commodity is None:
                raise BasketValidationError(f"Commodity ID {commodity_id} is not available for {country_iso3}.")
            if commodity_id not in priced_ids:
                raise BasketValidationError(f"Commodity ID {commodity_id} has no cached price rows for {country_iso3}.")

            unit_id = commodity.commodity_unit_id
            unit = str(commodity.commodity_unit_name or "").strip()
            if not unit:
                unit_id, unit = self._get_price_row_unit(country_iso3, commodity_id)
            if not unit:
                raise BasketValidationError(
                    f"Commodity {commodity.commodity_name} has no Databridges unit in the active cache."
                )

            weight = _required_float(_payload_get(raw, "weight_quantity"), "weight_quantity")
            if weight <= 0:
                raise BasketValidationError(f"Weight/quantity for {commodity.commodity_name} must be positive.")

            normalized.append(
                {
                    "commodity_id": commodity_id,
                    "commodity_name_snapshot": str(commodity.commodity_name),
                    "databridges_unit_id": unit_id,
                    "databridges_unit": unit,
                    "weight_quantity": weight,
                    "item_note": _optional_str(_payload_get(raw, "item_note")),
                }
            )

        return normalized, availability.cache_version_id or metadata.country.cache_version_id

    @retry_disconnected_read
    def _get_price_row_unit(self, country_iso3: str, commodity_id: int) -> tuple[Optional[int], str]:
        active_version_id = self.price_repo.get_active_version_id_for_country(country_iso3)
        if active_version_id is None:
            return None, ""
        with self.engine.begin() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT commodity_unit_id, commodity_unit_name
                    FROM cached_price_monthly
                    WHERE cache_version_id = :cache_version_id
                      AND country_iso3 = :country_iso3
                      AND commodity_id = :commodity_id
                      AND commodity_unit_name IS NOT NULL
                      AND commodity_unit_name <> ''
                    ORDER BY price_date DESC, commodity_unit_name
                    LIMIT 1
                    """
                ),
                {
                    "cache_version_id": active_version_id,
                    "country_iso3": str(country_iso3 or "").upper(),
                    "commodity_id": int(commodity_id),
                },
            ).mappings().first()
        if row is None:
            return None, ""
        return _optional_int(row.get("commodity_unit_id")), str(row.get("commodity_unit_name") or "").strip()

    def _get_items(self, basket_version_id: str) -> list[CountryFoodBasketItem]:
        with self.engine.begin() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT *
                    FROM country_food_basket_items
                    WHERE basket_version_id = :basket_version_id
                    ORDER BY sort_order, commodity_name_snapshot
                    """
                ),
                {"basket_version_id": basket_version_id},
            ).mappings().all()
        return [self._item_from_row(row) for row in rows]

    def _get_regions(self, basket_version_id: str) -> list[str]:
        with self.engine.begin() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT region_name
                    FROM country_food_basket_regions
                    WHERE basket_version_id = :basket_version_id
                    ORDER BY sort_order, region_name
                    """
                ),
                {"basket_version_id": basket_version_id},
            ).mappings().all()
        return [str(row["region_name"]) for row in rows]

    def _version_from_row(
        self,
        row: Mapping[str, Any],
        *,
        regions: list[str],
        items: list[CountryFoodBasketItem],
    ) -> CountryFoodBasketVersion:
        return CountryFoodBasketVersion(
            basket_version_id=str(row["basket_version_id"]),
            country_iso3=str(row["country_iso3"]),
            version_number=int(row["version_number"]),
            basket_role=_normalize_basket_role(row.get("basket_role")),
            basket_name=str(row.get("basket_name") or DEFAULT_PRIMARY_BASKET_NAME),
            short_description=_optional_str(row.get("short_description")),
            scope_type=_normalize_basket_scope(row.get("scope_type")),
            status=str(row["status"]),
            created_at=_to_datetime(row.get("created_at")) or datetime.min.replace(tzinfo=timezone.utc),
            created_by_user_id=str(row.get("created_by_user_id") or "unknown"),
            cache_version_id_at_creation=_optional_str(row.get("cache_version_id_at_creation")),
            change_note=_optional_str(row.get("change_note")),
            regions=regions,
            items=items,
        )

    def _item_from_row(self, row: Mapping[str, Any]) -> CountryFoodBasketItem:
        return CountryFoodBasketItem(
            basket_item_id=str(row["basket_item_id"]),
            basket_version_id=str(row["basket_version_id"]),
            commodity_id=int(row["commodity_id"]),
            commodity_name_snapshot=str(row["commodity_name_snapshot"]),
            databridges_unit_id=_optional_int(row.get("databridges_unit_id")),
            databridges_unit=str(row["databridges_unit"]),
            weight_quantity=float(row["weight_quantity"]),
            sort_order=int(row["sort_order"]),
            item_note=_optional_str(row.get("item_note")),
        )


def create_food_basket_repository() -> SqlCountryFoodBasketRepository:
    global _FOOD_BASKET_REPOSITORY
    if _FOOD_BASKET_REPOSITORY is not None:
        return _FOOD_BASKET_REPOSITORY
    with _FOOD_BASKET_REPOSITORY_LOCK:
        if _FOOD_BASKET_REPOSITORY is None:
            config = load_price_cache_config()
            engine = create_price_cache_engine(config)
            apply_migrations(engine, config.backend)
            _FOOD_BASKET_REPOSITORY = SqlCountryFoodBasketRepository(engine)
    return _FOOD_BASKET_REPOSITORY


def get_country_baskets_response(country: str) -> dict[str, Any]:
    canonical, iso3 = resolve_country(country)
    baskets = create_food_basket_repository().get_active_baskets(iso3)
    primary = baskets[BasketRole.PRIMARY.value]
    secondary = baskets[BasketRole.SECONDARY.value]
    return {
        "country": canonical,
        "iso3": iso3,
        "primary": primary.to_dict() if primary else None,
        "secondary": secondary.to_dict() if secondary else None,
        "needs_primary_setup": primary is None,
        "has_secondary": secondary is not None,
        "second_basket_enabled": market_monitor_second_basket_enabled(),
    }


def save_country_basket_role(
    country: str,
    role: BasketRole | str,
    payload: Mapping[str, Any] | BasketSaveInput,
) -> dict[str, Any]:
    canonical, iso3 = resolve_country(country)
    basket_role = _normalize_basket_role(role)
    if basket_role is BasketRole.SECONDARY and not market_monitor_second_basket_enabled():
        raise SecondBasketFeatureDisabled()
    data = _model_dict(payload)
    if _payload_field_is_explicit(payload, "basket_role"):
        payload_role = _normalize_basket_role(data.get("basket_role"))
        if payload_role is not basket_role:
            raise BasketValidationError(
                f"Basket role {payload_role.value!r} does not match the {basket_role.value!r} endpoint."
            )

    create_food_basket_repository().save_basket(
        iso3,
        role=basket_role,
        basket_name=data.get("basket_name"),
        short_description=data.get("short_description"),
        scope_type=data.get("scope_type", BasketScopeType.NATIONAL),
        regions=data.get("regions") or [],
        items=data.get("items") or [],
        created_by_user_id=data.get("created_by_user_id"),
        change_note=data.get("change_note"),
    )
    return get_country_baskets_response(canonical)


def list_country_basket_role_history(
    country: str,
    role: BasketRole | str,
    *,
    limit: int = 20,
) -> dict[str, Any]:
    canonical, iso3 = resolve_country(country)
    basket_role = _normalize_basket_role(role)
    versions = create_food_basket_repository().list_basket_history(
        iso3,
        basket_role,
        limit=limit,
    )
    return {
        "country": canonical,
        "iso3": iso3,
        "basket_role": basket_role.value,
        "versions": [version.to_dict() for version in versions],
    }


def archive_country_secondary_basket(country: str) -> dict[str, Any]:
    if not market_monitor_second_basket_enabled():
        raise SecondBasketFeatureDisabled()
    canonical, iso3 = resolve_country(country)
    archived = create_food_basket_repository().archive_secondary_basket(iso3)
    response = get_country_baskets_response(canonical)
    response["archived_secondary"] = archived.to_dict() if archived else None
    return response


def resolve_baskets_for_report(
    country: str,
    *,
    primary_basket_version_id: Optional[str] = None,
    include_secondary_basket: bool = True,
    secondary_basket_version_id: Optional[str] = None,
) -> ResolvedBasketSelection:
    canonical, iso3 = resolve_country(country)
    baskets = create_food_basket_repository().get_active_baskets(iso3)
    primary = baskets[BasketRole.PRIMARY.value]
    if primary is None:
        raise BasketNotConfigured(
            f"No active primary food basket is configured for {canonical}. "
            "Create a primary country basket before generating a bulletin."
        )

    requested_primary = _optional_str(primary_basket_version_id)
    if requested_primary and requested_primary != primary.basket_version_id:
        raise BasketVersionConflict(
            f"The selected primary basket {primary.basket_name!r} version is no longer active. "
            "Refresh the country baskets and run the report again."
        )

    if not include_secondary_basket:
        return ResolvedBasketSelection(
            country=canonical,
            iso3=iso3,
            primary=primary,
            secondary=None,
            secondary_basket_included=False,
        )

    requested_secondary = _optional_str(secondary_basket_version_id)
    secondary = baskets[BasketRole.SECONDARY.value]
    if secondary is None:
        if requested_secondary:
            raise BasketVersionConflict(
                "The selected secondary basket version is no longer active. "
                "Refresh the country baskets and run the report again."
            )
        return ResolvedBasketSelection(
            country=canonical,
            iso3=iso3,
            primary=primary,
            secondary=None,
            secondary_basket_included=False,
        )

    if requested_secondary and requested_secondary != secondary.basket_version_id:
        raise BasketVersionConflict(
            f"The selected secondary basket {secondary.basket_name!r} version is no longer active. "
            "Refresh the country baskets and run the report again."
        )

    return ResolvedBasketSelection(
        country=canonical,
        iso3=iso3,
        primary=primary,
        secondary=secondary,
        secondary_basket_included=True,
    )


def attach_basket_selection_to_result(
    result: Optional[Mapping[str, Any]],
    selection: Optional[ResolvedBasketSelection],
) -> dict[str, Any]:
    normalized = dict(result or {})
    data_statistics = normalized.get("data_statistics")
    if not isinstance(data_statistics, Mapping):
        data_statistics = {}
    primary_statistics = data_statistics.get("food_basket")
    if not isinstance(primary_statistics, Mapping) or not primary_statistics:
        primary_statistics = None

    existing_basket_statistics = normalized.get("basket_statistics")
    if isinstance(existing_basket_statistics, Mapping):
        basket_statistics = {
            BasketRole.PRIMARY.value: existing_basket_statistics.get(BasketRole.PRIMARY.value),
            BasketRole.SECONDARY.value: existing_basket_statistics.get(BasketRole.SECONDARY.value),
        }
    else:
        basket_statistics = {
            BasketRole.PRIMARY.value: dict(primary_statistics) if primary_statistics else None,
            BasketRole.SECONDARY.value: None,
        }

    if selection is not None:
        food_baskets = selection.food_baskets_dict()
        normalized["food_basket"] = food_baskets[BasketRole.PRIMARY.value]
        secondary_included = selection.secondary_basket_included
    else:
        existing_food_baskets = normalized.get("food_baskets")
        primary_alias = normalized.get("food_basket")
        primary_snapshot = primary_alias if isinstance(primary_alias, Mapping) and primary_alias else None
        if not isinstance(primary_alias, Mapping):
            normalized["food_basket"] = {}
        if isinstance(existing_food_baskets, Mapping):
            food_baskets = {
                BasketRole.PRIMARY.value: existing_food_baskets.get(
                    BasketRole.PRIMARY.value,
                    primary_snapshot,
                ),
                BasketRole.SECONDARY.value: existing_food_baskets.get(BasketRole.SECONDARY.value),
            }
        else:
            food_baskets = {
                BasketRole.PRIMARY.value: primary_snapshot,
                BasketRole.SECONDARY.value: None,
            }
        secondary_included = bool(normalized.get("secondary_basket_included", False))

    normalized["food_baskets"] = food_baskets
    normalized["basket_statistics"] = basket_statistics
    normalized["basket_series_national"] = list(normalized.get("basket_series_national") or [])
    normalized["basket_series_regional"] = list(normalized.get("basket_series_regional") or [])
    if basket_statistics.get(BasketRole.PRIMARY.value):
        data_statistics = dict(data_statistics)
        data_statistics["food_basket"] = basket_statistics[BasketRole.PRIMARY.value]
        normalized["data_statistics"] = data_statistics
    normalized["secondary_basket_included"] = secondary_included
    return normalized


def get_country_basket_response(country: str) -> dict[str, Any]:
    canonical, iso3 = resolve_country(country)
    basket = create_food_basket_repository().get_active_basket(iso3, BasketRole.PRIMARY)
    return {
        "country": canonical,
        "iso3": iso3,
        "needs_setup": basket is None,
        "active_basket": basket.to_dict() if basket else None,
    }


def list_country_basket_history(country: str, *, limit: int = 20) -> dict[str, Any]:
    canonical, iso3 = resolve_country(country)
    versions = create_food_basket_repository().list_basket_history(
        iso3,
        BasketRole.PRIMARY,
        limit=limit,
    )
    return {
        "country": canonical,
        "iso3": iso3,
        "versions": [version.to_dict() for version in versions],
    }


def save_country_basket(country: str, payload: Mapping[str, Any] | BasketSaveInput) -> dict[str, Any]:
    canonical, iso3 = resolve_country(country)
    data = _model_dict(payload)
    requested_role = _normalize_basket_role(data.get("basket_role", BasketRole.PRIMARY))
    if requested_role is not BasketRole.PRIMARY:
        raise BasketValidationError(
            "The singular food basket operation is primary-only; use the secondary basket operation instead."
        )
    basket = create_food_basket_repository().save_basket(
        iso3,
        role=BasketRole.PRIMARY,
        basket_name=data.get("basket_name"),
        short_description=data.get("short_description"),
        scope_type=data.get("scope_type", BasketScopeType.NATIONAL),
        regions=data.get("regions") or [],
        items=data.get("items") or [],
        created_by_user_id=data.get("created_by_user_id"),
        change_note=data.get("change_note"),
    )
    return {
        "country": canonical,
        "iso3": iso3,
        "needs_setup": False,
        "active_basket": basket.to_dict(),
    }


def get_active_basket_for_report(country: str, basket_version_id: Optional[str] = None) -> dict[str, Any]:
    canonical, iso3 = resolve_country(country)
    basket = create_food_basket_repository().get_active_basket(iso3, BasketRole.PRIMARY)
    if basket is None:
        raise BasketNotConfigured(
            f"No active food basket is configured for {canonical}. Create a country basket before generating a bulletin."
        )
    requested = str(basket_version_id or "").strip()
    if requested and requested != basket.basket_version_id:
        raise BasketVersionConflict(
            "The selected food basket version is no longer active. Refresh the country basket and run the report again."
        )
    return {
        "country": canonical,
        "iso3": iso3,
        **basket.to_dict(),
    }


def _normalize_basket_role(value: BasketRole | str | Any) -> BasketRole:
    if isinstance(value, BasketRole):
        return value
    try:
        return BasketRole(str(value or "").strip().lower())
    except ValueError as exc:
        raise BasketValidationError(
            f"Invalid basket role {value!r}; expected 'primary' or 'secondary'."
        ) from exc


def _normalize_basket_scope(value: BasketScopeType | str | Any) -> BasketScopeType:
    if isinstance(value, BasketScopeType):
        return value
    try:
        return BasketScopeType(str(value or "").strip().lower())
    except ValueError as exc:
        raise BasketValidationError(
            f"Invalid basket scope {value!r}; expected 'national' or 'selected_regions'."
        ) from exc


def _normalize_basket_metadata(
    role: BasketRole,
    basket_name: Optional[str],
    short_description: Optional[str],
) -> tuple[str, str]:
    name = _optional_str(basket_name)
    description = _optional_str(short_description)

    if role is BasketRole.PRIMARY:
        if name is None or name.casefold() == DEFAULT_PRIMARY_BASKET_NAME.casefold():
            name = DEFAULT_PRIMARY_BASKET_NAME
            description = description or DEFAULT_PRIMARY_BASKET_DESCRIPTION
        elif description is None:
            raise BasketValidationError("A custom primary basket name requires a short description.")
        return name, description

    if name is None:
        raise BasketValidationError("The secondary food basket requires a name.")
    if description is None:
        raise BasketValidationError("The secondary food basket requires a short description.")
    return name, description


def _payload_get(payload: Mapping[str, Any] | Any, key: str, default: Any = None) -> Any:
    if isinstance(payload, Mapping):
        return payload.get(key, default)
    return getattr(payload, key, default)


def _payload_field_is_explicit(payload: Mapping[str, Any] | Any, key: str) -> bool:
    if isinstance(payload, Mapping):
        return key in payload
    fields_set = getattr(payload, "model_fields_set", None)
    if fields_set is None:
        fields_set = getattr(payload, "__fields_set__", set())
    return key in set(fields_set or set())


def _model_dict(value: Mapping[str, Any] | Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if hasattr(value, "model_dump"):
        return value.model_dump()
    if hasattr(value, "dict"):
        return value.dict()
    return dict(value)


def _required_int(value: Any, name: str) -> int:
    parsed = _optional_int(value)
    if parsed is None:
        raise BasketValidationError(f"Missing required integer field {name}.")
    return parsed


def _required_float(value: Any, name: str) -> float:
    if value in (None, ""):
        raise BasketValidationError(f"Missing required numeric field {name}.")
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise BasketValidationError(f"Invalid numeric value for {name}: {value!r}.") from exc


def _optional_int(value: Any) -> Optional[int]:
    if value in (None, ""):
        return None
    return int(value)


def _optional_str(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    text_value = str(value).strip()
    return text_value or None


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _datetime_wire(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _to_datetime(value: Any) -> Optional[datetime]:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
