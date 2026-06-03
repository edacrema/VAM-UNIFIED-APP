from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from .config import POSTGRES_BACKEND, SQLITE_BACKEND, PriceCacheConfig
from .schemas import (
    CacheStatus,
    CommodityRecord,
    CountryMetadata,
    CountryRecord,
    CurrencyRecord,
    MarketRecord,
    MonthlyPriceRecord,
    UnitRecord,
)


def create_price_cache_engine(config: PriceCacheConfig) -> Engine:
    if config.backend == SQLITE_BACKEND:
        sqlite_path = config.sqlite_path
        if not sqlite_path.is_absolute():
            sqlite_path = Path.cwd() / sqlite_path
        sqlite_path.parent.mkdir(parents=True, exist_ok=True)
        return create_engine(f"sqlite:///{sqlite_path.as_posix()}", future=True)
    if config.backend == POSTGRES_BACKEND:
        if not config.database_url:
            raise ValueError("PRICE_CACHE_DATABASE_URL is required for the postgres price cache backend.")
        return create_engine(config.database_url, future=True)
    raise ValueError(f"Unsupported price cache backend {config.backend!r}.")


class SqlPriceCacheRepository:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def get_cache_status(self) -> CacheStatus:
        with self.engine.begin() as conn:
            active = conn.execute(
                text(
                    """
                    SELECT v.*
                    FROM price_cache_active_version av
                    JOIN price_cache_versions v
                        ON v.cache_version_id = av.cache_version_id
                    WHERE av.singleton_id = 1
                    """
                )
            ).mappings().first()
        if active is None:
            return CacheStatus(has_active_cache=False)
        return CacheStatus(
            has_active_cache=True,
            active_version_id=str(active["cache_version_id"]),
            status=_optional_str(active.get("status")),
            activated_at=_to_datetime(active.get("activated_at")),
            completed_at=_to_datetime(active.get("completed_at")),
            rows_prices=int(active.get("rows_prices") or 0),
            rows_commodities=int(active.get("rows_commodities") or 0),
            rows_markets=int(active.get("rows_markets") or 0),
            rows_countries=int(active.get("rows_countries") or 0),
            validation_summary=_json_dict(active.get("validation_summary_json")),
            error_message=_optional_str(active.get("error_message")),
        )

    def get_active_version_id(self) -> Optional[str]:
        with self.engine.begin() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT cache_version_id
                    FROM price_cache_active_version
                    WHERE singleton_id = 1
                    """
                )
            ).mappings().first()
        if row is None:
            return None
        return str(row["cache_version_id"])

    def list_countries(self) -> List[CountryRecord]:
        active_version_id = self.get_active_version_id()
        if active_version_id is None:
            return []
        with self.engine.begin() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT *
                    FROM cached_countries
                    WHERE cache_version_id = :cache_version_id
                    ORDER BY country_name
                    """
                ),
                {"cache_version_id": active_version_id},
            ).mappings().all()
        return [_country_from_row(row) for row in rows]

    def get_country_metadata(self, country_iso3: str) -> Optional[CountryMetadata]:
        active_version_id = self.get_active_version_id()
        if active_version_id is None:
            return None
        params = {
            "cache_version_id": active_version_id,
            "country_iso3": country_iso3.upper(),
        }
        with self.engine.begin() as conn:
            country_row = conn.execute(
                text(
                    """
                    SELECT *
                    FROM cached_countries
                    WHERE cache_version_id = :cache_version_id
                      AND country_iso3 = :country_iso3
                    """
                ),
                params,
            ).mappings().first()
            if country_row is None:
                return None

            commodity_rows = conn.execute(
                text(
                    """
                    SELECT *
                    FROM cached_commodities
                    WHERE cache_version_id = :cache_version_id
                      AND country_iso3 = :country_iso3
                      AND active = :active
                    ORDER BY commodity_name
                    """
                ),
                {**params, "active": _active_param(conn)},
            ).mappings().all()
            unit_rows = conn.execute(
                text(
                    """
                    SELECT *
                    FROM cached_units
                    WHERE cache_version_id = :cache_version_id
                      AND active = :active
                    ORDER BY commodity_unit_name
                    """
                ),
                {"cache_version_id": active_version_id, "active": _active_param(conn)},
            ).mappings().all()
            market_rows = conn.execute(
                text(
                    """
                    SELECT *
                    FROM cached_markets
                    WHERE cache_version_id = :cache_version_id
                      AND country_iso3 = :country_iso3
                      AND active = :active
                    ORDER BY market_name
                    """
                ),
                {**params, "active": _active_param(conn)},
            ).mappings().all()
            currency_rows = conn.execute(
                text(
                    """
                    SELECT *
                    FROM cached_currencies
                    WHERE cache_version_id = :cache_version_id
                    ORDER BY currency_name
                    """
                ),
                {"cache_version_id": active_version_id},
            ).mappings().all()

        return CountryMetadata(
            country=_country_from_row(country_row),
            commodities=[_commodity_from_row(row) for row in commodity_rows],
            units=[_unit_from_row(row) for row in unit_rows],
            markets=[_market_from_row(row) for row in market_rows],
            currencies=[_currency_from_row(row) for row in currency_rows],
        )

    def get_price_window(
        self,
        country_iso3: str,
        start_date: date | str,
        end_date: date | str,
        *,
        commodity_ids: Optional[Sequence[int]] = None,
        market_ids: Optional[Sequence[int]] = None,
        admin1_names: Optional[Sequence[str]] = None,
    ) -> List[MonthlyPriceRecord]:
        active_version_id = self.get_active_version_id()
        if active_version_id is None:
            return []

        clauses = [
            "cache_version_id = :cache_version_id",
            "country_iso3 = :country_iso3",
            "price_date >= :start_date",
            "price_date <= :end_date",
        ]
        params: Dict[str, Any] = {
            "cache_version_id": active_version_id,
            "country_iso3": country_iso3.upper(),
            "start_date": _date_wire(start_date),
            "end_date": _date_wire(end_date),
        }
        _add_in_clause(clauses, params, "commodity_id", commodity_ids, "commodity_id")
        _add_in_clause(clauses, params, "market_id", market_ids, "market_id")
        _add_in_clause(clauses, params, "admin1_name", admin1_names, "admin1_name")

        query = f"""
            SELECT *
            FROM cached_price_monthly
            WHERE {' AND '.join(clauses)}
            ORDER BY price_date, admin1_name, market_name, commodity_name
        """
        with self.engine.begin() as conn:
            rows = conn.execute(text(query), params).mappings().all()
        return [_monthly_price_from_row(row) for row in rows]


def _add_in_clause(
    clauses: list[str],
    params: Dict[str, Any],
    column: str,
    values: Optional[Sequence[Any]],
    prefix: str,
) -> None:
    cleaned = [value for value in values or [] if value not in (None, "")]
    if not cleaned:
        return
    placeholders = []
    for index, value in enumerate(cleaned):
        key = f"{prefix}_{index}"
        placeholders.append(f":{key}")
        params[key] = value
    clauses.append(f"{column} IN ({', '.join(placeholders)})")


def _active_param(conn) -> Any:
    return True if conn.dialect.name != "sqlite" else 1


def _country_from_row(row: Any) -> CountryRecord:
    return CountryRecord(
        cache_version_id=str(row["cache_version_id"]),
        country_iso3=str(row["country_iso3"]),
        country_name=str(row["country_name"]),
        currency_code=_optional_str(row.get("currency_code")),
        currency_name=_optional_str(row.get("currency_name")),
        latest_price_date=_to_date(row.get("latest_price_date")),
    )


def _commodity_from_row(row: Any) -> CommodityRecord:
    return CommodityRecord(
        cache_version_id=str(row["cache_version_id"]),
        country_iso3=str(row["country_iso3"]),
        commodity_id=int(row["commodity_id"]),
        commodity_name=str(row["commodity_name"]),
        commodity_unit_id=_optional_int(row.get("commodity_unit_id")),
        commodity_unit_name=_optional_str(row.get("commodity_unit_name")),
        category_name=_optional_str(row.get("category_name")),
        active=_to_bool(row.get("active")),
    )


def _unit_from_row(row: Any) -> UnitRecord:
    return UnitRecord(
        cache_version_id=str(row["cache_version_id"]),
        commodity_unit_id=int(row["commodity_unit_id"]),
        commodity_unit_name=str(row["commodity_unit_name"]),
        conversion_to_kg_l=_optional_float(row.get("conversion_to_kg_l")),
        active=_to_bool(row.get("active")),
    )


def _market_from_row(row: Any) -> MarketRecord:
    return MarketRecord(
        cache_version_id=str(row["cache_version_id"]),
        country_iso3=str(row["country_iso3"]),
        market_id=int(row["market_id"]),
        market_name=str(row["market_name"]),
        admin1_name=_optional_str(row.get("admin1_name")),
        admin2_name=_optional_str(row.get("admin2_name")),
        latitude=_optional_float(row.get("latitude")),
        longitude=_optional_float(row.get("longitude")),
        active=_to_bool(row.get("active")),
    )


def _currency_from_row(row: Any) -> CurrencyRecord:
    return CurrencyRecord(
        cache_version_id=str(row["cache_version_id"]),
        currency_id=int(row["currency_id"]),
        currency_code=_optional_str(row.get("currency_code")),
        currency_name=str(row["currency_name"]),
    )


def _monthly_price_from_row(row: Any) -> MonthlyPriceRecord:
    return MonthlyPriceRecord(
        cache_version_id=str(row["cache_version_id"]),
        country_iso3=str(row["country_iso3"]),
        commodity_id=int(row["commodity_id"]),
        market_id=int(row["market_id"]),
        price_date=_to_date(row["price_date"]) or date.min,
        price=float(row["price"]),
        currency_id=_optional_int(row.get("currency_id")),
        currency_code=_optional_str(row.get("currency_code")),
        currency_name=_optional_str(row.get("currency_name")),
        commodity_unit_id=_optional_int(row.get("commodity_unit_id")),
        commodity_unit_name=_optional_str(row.get("commodity_unit_name")),
        price_type_id=_optional_int(row.get("price_type_id")),
        price_type_name=str(row.get("price_type_name") or ""),
        price_flag=str(row.get("price_flag") or ""),
        original_frequency=_optional_str(row.get("original_frequency")),
        observations=_optional_int(row.get("observations")),
        source_payload_hash=_optional_str(row.get("source_payload_hash")),
        admin1_name=_optional_str(row.get("admin1_name")),
        admin2_name=_optional_str(row.get("admin2_name")),
        market_name=_optional_str(row.get("market_name")),
        commodity_name=_optional_str(row.get("commodity_name")),
        data_source=_optional_str(row.get("data_source")),
    )


def _date_wire(value: date | str) -> str:
    if isinstance(value, date):
        return value.isoformat()
    return str(value)[:10]


def _to_date(value: Any) -> Optional[date]:
    if value in (None, ""):
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    return date.fromisoformat(str(value)[:10])


def _to_datetime(value: Any) -> Optional[datetime]:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value
    text_value = str(value).replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(text_value)
    except ValueError:
        return None


def _optional_str(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    return str(value)


def _optional_int(value: Any) -> Optional[int]:
    if value in (None, ""):
        return None
    return int(value)


def _optional_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    return float(value)


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "yes"}


def _json_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if value in (None, ""):
        return {}
    try:
        parsed = json.loads(str(value))
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}
