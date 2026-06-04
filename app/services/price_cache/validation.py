from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Mapping, Sequence


CANONICAL_PRICE_KEY_FIELDS = (
    "country_iso3",
    "commodity_id",
    "market_id",
    "price_date",
    "price_type_name",
    "price_flag",
)


@dataclass(frozen=True)
class CountryCacheValidationResult:
    country_iso3: str
    valid: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    latest_price_date: date | None = None
    rows_prices: int = 0
    rows_commodities: int = 0
    rows_markets: int = 0

    def to_summary(self) -> dict[str, Any]:
        return {
            "valid": self.valid,
            "errors": list(self.errors),
            "warnings": list(self.warnings),
            "latest_price_date": self.latest_price_date.isoformat() if self.latest_price_date else None,
            "rows_prices": self.rows_prices,
            "rows_commodities": self.rows_commodities,
            "rows_markets": self.rows_markets,
        }


def validate_country_snapshot(
    *,
    country_iso3: str,
    prices: Sequence[Mapping[str, Any]],
    commodities: Sequence[Mapping[str, Any]],
    markets: Sequence[Mapping[str, Any]],
    previous_rows_prices: int = 0,
    max_country_drop_ratio: float = 0.25,
) -> CountryCacheValidationResult:
    country = country_iso3.upper()
    errors: list[str] = []
    warnings: list[str] = []
    latest_price_date: date | None = None

    if not prices:
        errors.append("No monthly price rows were fetched.")

    commodity_ids = {_as_int(row.get("commodity_id")) for row in commodities}
    commodity_ids.discard(None)
    market_ids = {_as_int(row.get("market_id")) for row in markets}
    market_ids.discard(None)

    seen_keys: set[tuple[Any, ...]] = set()
    duplicate_count = 0
    missing_reference_count = 0

    for index, row in enumerate(prices, start=1):
        missing = [
            field_name
            for field_name in ("country_iso3", "commodity_id", "market_id", "price_date", "price")
            if _blank(row.get(field_name))
        ]
        if missing:
            errors.append(f"Price row {index} missing required fields: {', '.join(missing)}.")
            continue

        row_country = str(row.get("country_iso3") or "").upper()
        if row_country != country:
            errors.append(f"Price row {index} has country {row_country!r}, expected {country!r}.")

        parsed_date = _as_date(row.get("price_date"))
        if parsed_date is None:
            errors.append(f"Price row {index} has invalid price_date {row.get('price_date')!r}.")
        elif latest_price_date is None or parsed_date > latest_price_date:
            latest_price_date = parsed_date

        try:
            float(row.get("price"))
        except (TypeError, ValueError):
            errors.append(f"Price row {index} has invalid price {row.get('price')!r}.")

        key = _canonical_key(row)
        if key in seen_keys:
            duplicate_count += 1
        else:
            seen_keys.add(key)

        commodity_id = _as_int(row.get("commodity_id"))
        market_id = _as_int(row.get("market_id"))
        if commodity_ids and commodity_id not in commodity_ids:
            missing_reference_count += 1
        if market_ids and market_id not in market_ids:
            missing_reference_count += 1

    if duplicate_count:
        errors.append(f"Found {duplicate_count} duplicate canonical monthly price keys.")

    if missing_reference_count:
        warnings.append(
            f"Found {missing_reference_count} price metadata references not present in fetched commodities/markets."
        )

    if previous_rows_prices > 0:
        min_expected = previous_rows_prices * (1 - max_country_drop_ratio)
        if len(prices) < min_expected:
            errors.append(
                "Price row count dropped from "
                f"{previous_rows_prices} to {len(prices)}, exceeding allowed ratio {max_country_drop_ratio}."
            )

    return CountryCacheValidationResult(
        country_iso3=country,
        valid=not errors,
        errors=errors,
        warnings=warnings,
        latest_price_date=latest_price_date,
        rows_prices=len(prices),
        rows_commodities=len(commodities),
        rows_markets=len(markets),
    )


def _canonical_key(row: Mapping[str, Any]) -> tuple[Any, ...]:
    values: list[Any] = []
    for field_name in CANONICAL_PRICE_KEY_FIELDS:
        value = row.get(field_name)
        if field_name == "country_iso3":
            value = str(value or "").upper()
        elif field_name in {"commodity_id", "market_id"}:
            value = _as_int(value)
        elif field_name == "price_date":
            parsed = _as_date(value)
            value = parsed.isoformat() if parsed else str(value)
        else:
            value = str(value or "")
        values.append(value)
    return tuple(values)


def _blank(value: Any) -> bool:
    return value is None or value == ""


def _as_int(value: Any) -> int | None:
    if _blank(value):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _as_date(value: Any) -> date | None:
    if _blank(value):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None
