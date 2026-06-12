"""Shared country food basket storage and validation."""
from __future__ import annotations

import uuid
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping, Optional, Sequence

from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.services.price_cache.config import load_price_cache_config
from app.services.price_cache.migrations import apply_migrations
from app.services.price_cache.sql_repository import SqlPriceCacheRepository, create_price_cache_engine
from app.shared.countries import resolve_country


class BasketValidationError(ValueError):
    """Raised when a proposed basket cannot be published."""


class BasketNotConfigured(RuntimeError):
    """Raised when report generation needs a basket but no active basket exists."""


class BasketVersionConflict(RuntimeError):
    """Raised when a report references a stale basket version."""


_FOOD_BASKET_REPOSITORY: Optional[SqlCountryFoodBasketRepository] = None
_FOOD_BASKET_REPOSITORY_LOCK = threading.Lock()


class BasketItemInput(BaseModel):
    commodity_id: int = Field(..., description="Databridges commodity ID.")
    weight_quantity: float = Field(..., gt=0, description="Quantity in the commodity's Databridges unit.")
    item_note: Optional[str] = None


class BasketSaveInput(BaseModel):
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
    status: str
    created_at: datetime
    created_by_user_id: str
    cache_version_id_at_creation: Optional[str] = None
    change_note: Optional[str] = None
    items: list[CountryFoodBasketItem] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "basket_version_id": self.basket_version_id,
            "country_iso3": self.country_iso3,
            "version_number": self.version_number,
            "status": self.status,
            "created_at": _datetime_wire(self.created_at),
            "created_by_user_id": self.created_by_user_id,
            "cache_version_id_at_creation": self.cache_version_id_at_creation,
            "change_note": self.change_note,
            "items": [item.to_dict() for item in self.items],
        }


class SqlCountryFoodBasketRepository:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        self.price_repo = SqlPriceCacheRepository(engine)

    def get_active_basket(self, country_iso3: str) -> Optional[CountryFoodBasketVersion]:
        country = str(country_iso3 or "").upper()
        with self.engine.begin() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT v.*
                    FROM country_food_basket_current c
                    JOIN country_food_basket_versions v
                      ON v.basket_version_id = c.active_basket_version_id
                    WHERE c.country_iso3 = :country_iso3
                      AND v.status = 'active'
                    """
                ),
                {"country_iso3": country},
            ).mappings().first()
        if row is None:
            return None
        return self._version_from_row(row, items=self._get_items(str(row["basket_version_id"])))

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
        return self._version_from_row(row, items=self._get_items(str(row["basket_version_id"])))

    def list_basket_history(self, country_iso3: str, *, limit: int = 20) -> list[CountryFoodBasketVersion]:
        safe_limit = min(max(int(limit), 1), 100)
        with self.engine.begin() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT *
                    FROM country_food_basket_versions
                    WHERE country_iso3 = :country_iso3
                    ORDER BY version_number DESC
                    LIMIT :limit
                    """
                ),
                {"country_iso3": str(country_iso3 or "").upper(), "limit": safe_limit},
            ).mappings().all()
        return [
            self._version_from_row(row, items=self._get_items(str(row["basket_version_id"])))
            for row in rows
        ]

    def save_basket(
        self,
        country_iso3: str,
        *,
        items: Sequence[Mapping[str, Any] | Any],
        created_by_user_id: Optional[str] = None,
        change_note: Optional[str] = None,
    ) -> CountryFoodBasketVersion:
        country = str(country_iso3 or "").upper()
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
                    """
                ),
                {"country_iso3": country},
            ).mappings().first()
            if current_row is not None:
                conn.execute(
                    text(
                        """
                        UPDATE country_food_basket_versions
                        SET status = 'superseded'
                        WHERE basket_version_id = :basket_version_id
                        """
                    ),
                    {"basket_version_id": str(current_row["active_basket_version_id"])},
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
                        status,
                        created_at,
                        created_by_user_id,
                        cache_version_id_at_creation,
                        change_note
                    ) VALUES (
                        :basket_version_id,
                        :country_iso3,
                        :version_number,
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

            conn.execute(
                text(
                    """
                    DELETE FROM country_food_basket_current
                    WHERE country_iso3 = :country_iso3
                    """
                ),
                {"country_iso3": country},
            )
            conn.execute(
                text(
                    """
                    INSERT INTO country_food_basket_current (
                        country_iso3,
                        active_basket_version_id,
                        updated_at,
                        updated_by_user_id
                    ) VALUES (
                        :country_iso3,
                        :active_basket_version_id,
                        :updated_at,
                        :updated_by_user_id
                    )
                    """
                ),
                {
                    "country_iso3": country,
                    "active_basket_version_id": version_id,
                    "updated_at": _datetime_wire(created_at),
                    "updated_by_user_id": user_id,
                },
            )

        saved = self.get_active_basket(country)
        if saved is None:
            raise RuntimeError(f"Basket save completed but active basket could not be reloaded for {country}.")
        return saved

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
            if priced_ids and commodity_id not in priced_ids:
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

    def _version_from_row(
        self,
        row: Mapping[str, Any],
        *,
        items: list[CountryFoodBasketItem],
    ) -> CountryFoodBasketVersion:
        return CountryFoodBasketVersion(
            basket_version_id=str(row["basket_version_id"]),
            country_iso3=str(row["country_iso3"]),
            version_number=int(row["version_number"]),
            status=str(row["status"]),
            created_at=_to_datetime(row.get("created_at")) or datetime.min.replace(tzinfo=timezone.utc),
            created_by_user_id=str(row.get("created_by_user_id") or "unknown"),
            cache_version_id_at_creation=_optional_str(row.get("cache_version_id_at_creation")),
            change_note=_optional_str(row.get("change_note")),
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


def get_country_basket_response(country: str) -> dict[str, Any]:
    canonical, iso3 = resolve_country(country)
    basket = create_food_basket_repository().get_active_basket(iso3)
    return {
        "country": canonical,
        "iso3": iso3,
        "needs_setup": basket is None,
        "active_basket": basket.to_dict() if basket else None,
    }


def list_country_basket_history(country: str, *, limit: int = 20) -> dict[str, Any]:
    canonical, iso3 = resolve_country(country)
    versions = create_food_basket_repository().list_basket_history(iso3, limit=limit)
    return {
        "country": canonical,
        "iso3": iso3,
        "versions": [version.to_dict() for version in versions],
    }


def save_country_basket(country: str, payload: Mapping[str, Any] | BasketSaveInput) -> dict[str, Any]:
    canonical, iso3 = resolve_country(country)
    data = _model_dict(payload)
    basket = create_food_basket_repository().save_basket(
        iso3,
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
    basket = create_food_basket_repository().get_active_basket(iso3)
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


def _payload_get(payload: Mapping[str, Any] | Any, key: str, default: Any = None) -> Any:
    if isinstance(payload, Mapping):
        return payload.get(key, default)
    return getattr(payload, key, default)


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
