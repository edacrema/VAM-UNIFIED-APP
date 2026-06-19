from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Optional, Sequence

from .validation import CANONICAL_PRICE_KEY_FIELDS, CANONICAL_PRICE_KEY_INT_FIELDS


REAL_PRICE_FLAG_COMPONENTS = {"actual", "aggregate", "aggregated"}


@dataclass(frozen=True)
class PriceDeduplicationResult:
    rows: list[dict[str, Any]]
    duplicate_rows: int = 0
    duplicate_keys: int = 0
    conflicting_price_keys: int = 0


@dataclass(frozen=True)
class PriceFlagFilterResult:
    rows: list[dict[str, Any]]
    excluded_rows: int = 0
    excluded_flags: tuple[tuple[str, int], ...] = ()


@dataclass(frozen=True)
class FuturePriceFilterResult:
    rows: list[dict[str, Any]]
    excluded_rows: int = 0
    max_excluded_date: Optional[str] = None


def deduplicate_monthly_price_rows(rows: Sequence[dict[str, Any]]) -> PriceDeduplicationResult:
    grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    key_order: list[tuple[Any, ...]] = []
    for row in rows:
        key = price_deduplication_key(row)
        if key not in grouped:
            grouped[key] = []
            key_order.append(key)
        grouped[key].append(dict(row))

    duplicate_keys = 0
    duplicate_rows = 0
    conflicting_price_keys = 0
    deduplicated: list[dict[str, Any]] = []

    for key in key_order:
        group = grouped[key]
        if len(group) == 1:
            deduplicated.append(group[0])
            continue

        duplicate_keys += 1
        duplicate_rows += len(group) - 1
        if len({price_value(row) for row in group}) > 1:
            conflicting_price_keys += 1
        deduplicated.append(max(group, key=price_row_rank))

    return PriceDeduplicationResult(
        rows=deduplicated,
        duplicate_rows=duplicate_rows,
        duplicate_keys=duplicate_keys,
        conflicting_price_keys=conflicting_price_keys,
    )


def is_real_price_flag(flag: Any) -> bool:
    normalized = str(flag or "").strip().lower()
    if not normalized:
        return True
    components = [component.strip() for component in normalized.split(",") if component.strip()]
    return bool(components) and all(component in REAL_PRICE_FLAG_COMPONENTS for component in components)


def filter_real_monthly_price_rows(rows: Sequence[dict[str, Any]]) -> PriceFlagFilterResult:
    kept: list[dict[str, Any]] = []
    excluded: dict[str, int] = {}
    for row in rows:
        flag = str(row.get("price_flag") or "").strip().lower()
        if is_real_price_flag(flag):
            kept.append(dict(row))
        else:
            excluded[flag or "unknown"] = excluded.get(flag or "unknown", 0) + 1
    return PriceFlagFilterResult(
        rows=kept,
        excluded_rows=sum(excluded.values()),
        excluded_flags=tuple(sorted(excluded.items())),
    )


def current_month_start_utc() -> date:
    today = datetime.now(timezone.utc).date()
    return date(today.year, today.month, 1)


def filter_future_monthly_price_rows(
    rows: Sequence[dict[str, Any]],
    *,
    current_month_start: date,
) -> FuturePriceFilterResult:
    kept: list[dict[str, Any]] = []
    excluded_rows = 0
    max_excluded: Optional[date] = None
    for row in rows:
        parsed = price_date_obj(row.get("price_date"))
        if parsed is not None and parsed > current_month_start:
            excluded_rows += 1
            if max_excluded is None or parsed > max_excluded:
                max_excluded = parsed
            continue
        kept.append(dict(row))
    return FuturePriceFilterResult(
        rows=kept,
        excluded_rows=excluded_rows,
        max_excluded_date=max_excluded.isoformat() if max_excluded else None,
    )


def enrich_price_rows_with_metadata(
    rows: Sequence[dict[str, Any]],
    *,
    markets: Sequence[dict[str, Any]],
    commodities: Sequence[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Fill admin/market/commodity fields the PriceMonthly DTO does not carry."""
    market_lookup: dict[int, dict[str, Any]] = {}
    for market in markets or []:
        market_id = maybe_int(market.get("market_id"))
        if market_id is not None:
            market_lookup[market_id] = market
    commodity_lookup: dict[int, dict[str, Any]] = {}
    for commodity in commodities or []:
        commodity_id = maybe_int(commodity.get("commodity_id"))
        if commodity_id is not None:
            commodity_lookup[commodity_id] = commodity

    enriched: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        market = market_lookup.get(maybe_int(item.get("market_id")))
        if market:
            if item.get("admin1_name") in (None, ""):
                item["admin1_name"] = market.get("admin1_name")
            if item.get("admin2_name") in (None, ""):
                item["admin2_name"] = market.get("admin2_name")
            if item.get("market_name") in (None, ""):
                item["market_name"] = market.get("market_name")
        commodity = commodity_lookup.get(maybe_int(item.get("commodity_id")))
        if commodity:
            if item.get("commodity_name") in (None, ""):
                item["commodity_name"] = commodity.get("commodity_name")
            if item.get("commodity_unit_id") in (None, ""):
                item["commodity_unit_id"] = commodity.get("commodity_unit_id")
            if item.get("commodity_unit_name") in (None, ""):
                item["commodity_unit_name"] = commodity.get("commodity_unit_name")
        enriched.append(item)
    return enriched


def price_date_obj(value: Any) -> Optional[date]:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def price_deduplication_key(row: dict[str, Any]) -> tuple[Any, ...]:
    values: list[Any] = []
    for field_name in CANONICAL_PRICE_KEY_FIELDS:
        value = row.get(field_name)
        if field_name == "country_iso3":
            value = str(value or "").upper()
        elif field_name in CANONICAL_PRICE_KEY_INT_FIELDS:
            value = maybe_int(value)
        elif field_name == "price_date":
            value = str(value)[:10] if value not in (None, "") else ""
        else:
            value = str(value or "")
        values.append(value)
    return tuple(values)


def price_row_rank(row: dict[str, Any]) -> tuple[Any, ...]:
    metadata_fields = (
        "currency_id",
        "currency_code",
        "currency_name",
        "commodity_unit_id",
        "commodity_unit_name",
        "price_type_id",
        "price_type_name",
        "price_flag",
        "original_frequency",
        "data_source",
        "commodity_name",
        "market_name",
        "admin1_name",
        "admin2_name",
    )
    completeness = sum(1 for field in metadata_fields if row.get(field) not in (None, ""))
    observations = maybe_int(row.get("observations")) or 0
    stable_payload = json.dumps(row, sort_keys=True, default=str, separators=(",", ":"))
    return (
        completeness,
        observations,
        str(row.get("source_payload_hash") or ""),
        stable_payload,
    )


def price_value(row: dict[str, Any]) -> str:
    value = row.get("price")
    if value in (None, ""):
        return ""
    try:
        return f"{float(value):.12g}"
    except (TypeError, ValueError):
        return str(value)


def maybe_int(value: Any) -> Optional[int]:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None

