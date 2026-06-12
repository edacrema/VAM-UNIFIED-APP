"""Cache-first data loading for the Price Bulletin drafter."""
from __future__ import annotations

import logging
import re
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

from app.services.price_cache.config import load_price_cache_config
from app.services.price_cache.migrations import apply_migrations
from app.services.price_cache.repository import PriceCacheRepository
from app.services.price_cache.schemas import CacheStatus, CountryMetadata, MonthlyPriceRecord
from app.services.price_cache.sql_repository import SqlPriceCacheRepository, create_price_cache_engine
from app.shared.countries import (
    COUNTRY_CURRENCIES,
    normalize_country_name as _normalize_country_name,
    resolve_country,
)
from app.shared.gcs import (
    download_gcs_to_file as _download_gcs_to_file,
    parse_gcs_uri as _parse_gcs_uri,
)

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent / "data"
_CACHE_TTL_SECONDS = 15 * 60
_RECENT_METADATA_MONTHS = 36
_REAL_PRICE_FLAG_COMPONENTS = {"actual", "aggregate", "aggregated"}

# Worker-side ETL telemetry that operators need but officers should not be
# shown as report warnings.
_OPERATOR_WARNING_MARKERS = (
    "CommodityUnits/List",
    "price metadata references",
    "non-real monthly price row",
    "future-dated monthly price row",
    "duplicate monthly price",
    "conflicting price values",
)

_COUNTRY_CACHE: dict[str, tuple[float, list[dict[str, Any]]]] = {}
_COMMODITY_CACHE: dict[str, tuple[float, list[dict[str, Any]]]] = {}
_MARKET_CACHE: dict[str, tuple[float, list[dict[str, Any]]]] = {}
_PRICE_CACHE: dict[tuple[Any, ...], tuple[float, pd.DataFrame]] = {}
_METADATA_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}


class PriceCacheUnavailableError(RuntimeError):
    """Raised when the bulletin cannot be served from the active cache."""


def normalize_country_name(country: str) -> str:
    """Normalize a UI country value to the canonical country name."""
    return _normalize_country_name(country)


def reset_market_monitor_caches_for_tests() -> None:
    _COUNTRY_CACHE.clear()
    _COMMODITY_CACHE.clear()
    _MARKET_CACHE.clear()
    _PRICE_CACHE.clear()
    _METADATA_CACHE.clear()


def load_csv_price_data(csv_path: Optional[Path] = None) -> pd.DataFrame:
    """Deprecated compatibility shim for the removed spreadsheet data path."""
    raise FileNotFoundError(
        "The provisional price_data.csv path has been removed. Price Bulletin "
        "data is now retrieved from the active PriceCache."
    )


def _upload_file_to_gcs(content: bytes, gcs_uri: str) -> None:
    """Deprecated compatibility shim for removed dataset upload endpoints."""
    raise RuntimeError(
        "Uploading processed Price Bulletin datasets is no longer supported. "
        "Use the Price Data Validator to validate raw files before DataBridges upload."
    )


def get_cache_status_snapshot() -> dict[str, Any]:
    repo = _get_price_cache_repository()
    return _cache_status_dict(repo.get_cache_status())


def get_supported_countries() -> List[Dict[str, Any]]:
    """Return only countries with an active PriceCache country snapshot."""
    cached = _cache_get(_COUNTRY_CACHE, "countries")
    if cached is not None:
        return [dict(item) for item in cached]

    repo = _get_price_cache_repository()
    status = repo.get_cache_status()
    if not status.has_active_cache:
        _cache_set(_COUNTRY_CACHE, "countries", [])
        return []

    countries: list[dict[str, Any]] = []
    for country in repo.list_countries():
        availability = repo.get_country_availability(country.country_iso3)
        has_data = bool(availability and availability.date_start and availability.date_end)
        date_range = None
        latest_cached_date = None
        cache_version_id = country.cache_version_id
        if availability is not None:
            cache_version_id = availability.cache_version_id
            date_range, latest_cached_date, _ = _bounded_cache_dates(
                availability.date_start,
                availability.date_end,
                availability.latest_price_date,
            )
        countries.append(
            {
                "name": country.country_name,
                "iso3": country.country_iso3,
                "currency_code": country.currency_code,
                "currency_name": country.currency_name,
                "has_data": has_data,
                "source": "PriceCache",
                "cache_version_id": cache_version_id,
                "latest_cached_date": latest_cached_date,
                "date_range": date_range,
            }
        )
    countries = sorted(countries, key=lambda item: str(item["name"]))
    _cache_set(_COUNTRY_CACHE, "countries", countries)
    return [dict(item) for item in countries]


def get_available_countries(df: Optional[pd.DataFrame] = None) -> List[str]:
    if df is not None and "Country" in df.columns:
        return sorted(df["Country"].dropna().astype(str).unique().tolist())
    return [str(item["name"]) for item in get_supported_countries() if item.get("has_data")]


def get_available_commodities(
    df_or_country: Optional[Any] = None,
    country: Optional[str] = None,
) -> List[str]:
    """Return commodity names available for a country."""
    if isinstance(df_or_country, pd.DataFrame):
        if not country:
            return []
        country_normalized = normalize_country_name(country)
        country_df = df_or_country[df_or_country["Country"] == country_normalized]
        return sorted(country_df["Commodity"].dropna().astype(str).unique().tolist())

    country_value = country or str(df_or_country or "")
    canonical, iso3 = resolve_country(country_value)
    start_date, end_date = _recent_price_window()
    rows = _get_country_price_df(
        canonical,
        iso3,
        start_date=start_date,
        end_date=end_date,
        latest_value_only=True,
    )
    if rows.empty:
        rows = _get_country_price_df(canonical, iso3, latest_value_only=True)
    names = sorted(rows["Commodity"].dropna().astype(str).unique().tolist()) if not rows.empty else []
    if names:
        return names
    return [item["name"] for item in _get_commodities(canonical, iso3)]


def get_all_commodities(df: Optional[pd.DataFrame] = None) -> List[str]:
    if df is not None and "Commodity" in df.columns:
        return sorted(df["Commodity"].dropna().astype(str).unique().tolist())
    names: set[str] = set()
    for country in get_supported_countries():
        try:
            names.update(get_available_commodities(str(country["name"])))
        except Exception:
            logger.debug("Could not fetch commodities for %s", country["name"], exc_info=True)
    return sorted(names)


def get_available_regions(
    df_or_country: Optional[Any] = None,
    country: Optional[str] = None,
) -> List[str]:
    if isinstance(df_or_country, pd.DataFrame):
        if not country:
            return []
        country_normalized = normalize_country_name(country)
        country_df = df_or_country[df_or_country["Country"] == country_normalized]
        return sorted(country_df["Admin 1"].dropna().astype(str).unique().tolist())

    country_value = country or str(df_or_country or "")
    _canonical, iso3 = resolve_country(country_value)
    repo = _get_price_cache_repository()
    availability = repo.get_country_availability(iso3)
    if availability is not None and availability.admin1_names:
        return list(availability.admin1_names)
    return sorted(
        {
            str(market["admin1_name"])
            for market in _get_markets(_canonical, iso3)
            if market.get("admin1_name")
        }
    )


def get_available_markets(
    df_or_country: Optional[Any] = None,
    country: Optional[str] = None,
) -> List[str]:
    if isinstance(df_or_country, pd.DataFrame):
        if not country:
            return []
        country_normalized = normalize_country_name(country)
        country_df = df_or_country[df_or_country["Country"] == country_normalized]
        return sorted(country_df["Market Name"].dropna().astype(str).unique().tolist())

    country_value = country or str(df_or_country or "")
    canonical, iso3 = resolve_country(country_value)
    return sorted(
        {
            str(market["market_name"])
            for market in _get_markets(canonical, iso3)
            if market.get("market_name")
        }
    )


def get_date_range(
    df_or_country: Optional[Any] = None,
    country: Optional[str] = None,
) -> Tuple[datetime, datetime]:
    if isinstance(df_or_country, pd.DataFrame):
        if not country:
            raise ValueError("Country is required")
        country_normalized = normalize_country_name(country)
        country_df = df_or_country[df_or_country["Country"] == country_normalized]
        return country_df["Price Date"].min(), country_df["Price Date"].max()

    country_value = country or str(df_or_country or "")
    _canonical, iso3 = resolve_country(country_value)
    repo = _get_price_cache_repository()
    availability = repo.get_country_availability(iso3)
    if availability is None or availability.date_start is None or availability.date_end is None:
        raise ValueError(f"No cached monthly price data is available for {country_value}.")
    return (
        pd.Timestamp(availability.date_start).to_pydatetime(),
        pd.Timestamp(availability.date_end).to_pydatetime(),
    )


def get_commodity_categories(source: Optional[Any] = None) -> Dict[str, List[str]]:
    """Group commodity names into broad report-friendly categories."""
    if isinstance(source, pd.DataFrame):
        commodities = get_all_commodities(source)
    elif isinstance(source, list):
        commodities = [str(item.get("name", item)) if isinstance(item, dict) else str(item) for item in source]
    else:
        commodities = []

    categories: Dict[str, List[str]] = {
        "Cereals": [],
        "Pulses": [],
        "Oil": [],
        "Sugar": [],
        "Condiments": [],
        "Vegetables": [],
        "Livestock": [],
        "Fuel": [],
        "Exchange Rate": [],
        "Milling": [],
        "Wage": [],
        "Other": [],
    }
    for commodity in sorted({c for c in commodities if c}):
        lowered = commodity.lower()
        if any(token in lowered for token in ["sorghum", "maize", "wheat", "rice", "millet", "bread"]):
            categories["Cereals"].append(commodity)
        elif any(token in lowered for token in ["beans", "lentil", "pea", "chickpea", "pulse"]):
            categories["Pulses"].append(commodity)
        elif "oil" in lowered:
            categories["Oil"].append(commodity)
        elif "sugar" in lowered:
            categories["Sugar"].append(commodity)
        elif "salt" in lowered:
            categories["Condiments"].append(commodity)
        elif any(token in lowered for token in ["cabbage", "tomato", "onion", "vegetable", "leaves", "cassava"]):
            categories["Vegetables"].append(commodity)
        elif "livestock" in lowered or any(token in lowered for token in ["goat", "sheep", "cattle", "chicken"]):
            categories["Livestock"].append(commodity)
        elif "fuel" in lowered or any(token in lowered for token in ["petrol", "diesel", "gasoline"]):
            categories["Fuel"].append(commodity)
        elif "exchange" in lowered:
            categories["Exchange Rate"].append(commodity)
        elif "milling" in lowered:
            categories["Milling"].append(commodity)
        elif "wage" in lowered or "labour" in lowered:
            categories["Wage"].append(commodity)
        else:
            categories["Other"].append(commodity)
    return {name: values for name, values in categories.items() if values}


def get_country_metadata(country: str) -> Dict[str, Any]:
    canonical, iso3 = resolve_country(country)
    cached = _cache_get(_METADATA_CACHE, iso3)
    if cached is not None:
        return dict(cached)

    repo = _get_price_cache_repository()
    status = repo.get_cache_status()
    if not status.has_active_cache:
        raise PriceCacheUnavailableError("No active PriceCache version is available. Run a cache refresh first.")

    cache_metadata = repo.get_country_metadata(iso3)
    if cache_metadata is None:
        raise PriceCacheUnavailableError(
            f"No active PriceCache metadata is available for {canonical} ({iso3})."
        )
    availability = repo.get_country_availability(iso3)
    if availability is None:
        raise PriceCacheUnavailableError(
            f"No active PriceCache availability is available for {canonical} ({iso3})."
        )

    country_record = cache_metadata.country
    country_name = country_record.country_name or canonical
    currency = _country_currency(country_record, fallback_country=canonical)
    priced_ids = set(availability.priced_commodity_ids)
    raw_commodities = cache_metadata.commodities
    if priced_ids:
        raw_commodities = [item for item in raw_commodities if item.commodity_id in priced_ids]
    commodities = [
        {
            "id": item.commodity_id,
            "name": item.commodity_name,
            "category": item.category_name or _infer_category(item.commodity_name),
            "unit_id": item.commodity_unit_id,
            "unit": item.commodity_unit_name,
            "unit_name": item.commodity_unit_name,
        }
        for item in sorted(raw_commodities, key=lambda item: (item.commodity_name.lower(), item.commodity_id))
    ]
    commodity_names = [str(item["name"]) for item in commodities if item.get("name")]

    units = _merge_units(cache_metadata, availability.units)
    markets = _markets_payload(cache_metadata, country_name=country_name, iso3=iso3)
    regions = availability.admin1_names or sorted(
        {str(item["admin1_name"]) for item in markets if item.get("admin1_name")}
    )
    date_range, latest_cached_date, has_future_dates = _bounded_cache_dates(
        availability.date_start,
        availability.date_end,
        availability.latest_price_date,
    )
    warnings, operator_warnings = _split_warning_audiences(_cache_warnings(status, country_iso3=iso3))
    if date_range is None:
        warnings.append(f"PriceCache has no monthly price date range for {country_name}.")
    if has_future_dates:
        warnings.append(
            "PriceCache contains future-dated monthly prices for this country; "
            f"time period options are capped at the current month ({_current_month_start().strftime('%Y-%m')})."
        )
    if not units:
        warnings.append(
            "No commodity unit metadata is available in the active cache for this country; report rows may show blank units."
        )

    metadata = {
        "country": country_name,
        "iso3": iso3,
        "currency": currency,
        "commodities": commodities,
        "units": units,
        "commodity_categories": get_commodity_categories(commodity_names),
        "default_commodities": _select_default_commodities(commodity_names),
        "regions": regions,
        "markets": markets,
        "date_range": date_range,
        "metadata_price_window": {
            "start": date_range.get("start") if date_range else None,
            "end": date_range.get("end") if date_range else None,
            "months": None,
            "bounded": False,
            "source": "PriceCache",
        },
        "latest_cached_date": latest_cached_date,
        "cache_version_id": availability.cache_version_id,
        "source": "PriceCache",
        "cache_status": _cache_status_dict(status),
        "warnings": warnings,
        "operator_warnings": operator_warnings,
    }
    _cache_set(_METADATA_CACHE, iso3, metadata)
    return dict(metadata)


def get_cache_metadata_for_report(country: str) -> Dict[str, Any]:
    metadata = get_country_metadata(country)
    status = metadata.get("cache_status") or get_cache_status_snapshot()
    return {
        "source": "PriceCache",
        "cache_version_id": metadata.get("cache_version_id"),
        "active_version_id": status.get("active_version_id"),
        "country_active_version_id": metadata.get("cache_version_id"),
        "status": status.get("status"),
        "active_country_count": status.get("active_country_count"),
        "latest_cached_date": metadata.get("latest_cached_date"),
        "date_range": metadata.get("date_range"),
        "activated_at": status.get("activated_at"),
        "completed_at": status.get("completed_at"),
        "warnings": metadata.get("warnings") or [],
        "operator_warnings": metadata.get("operator_warnings") or [],
    }


def extract_time_series_from_csv(
    country: str,
    time_period: str,
    commodities: List[str],
    admin1_list: List[str],
    csv_path: Optional[Path] = None,
    lookback_months: int = 13,
    return_raw_rows: bool = False,
    currency_code: Optional[str] = None,
    basket_items: Optional[List[Dict[str, Any]]] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame] | Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Compatibility wrapper that returns PriceCache price time series."""
    if csv_path is not None:
        logger.info("Ignoring csv_path=%s because PriceCache is now the price source.", csv_path)

    canonical, iso3 = resolve_country(country)
    target_date = _parse_time_period(time_period)
    start_date = target_date - pd.DateOffset(months=lookback_months - 1)
    end_date = target_date + pd.DateOffset(months=1) - pd.DateOffset(days=1)
    full_date_index = pd.date_range(start=start_date, end=target_date, freq="MS")

    valid_names, commodity_ids, missing = _resolve_commodities(canonical, iso3, commodities)
    basket_components = _normalise_basket_components(basket_items)
    for component in basket_components:
        name = component["commodity_name"]
        commodity_id = component["commodity_id"]
        if name not in valid_names:
            valid_names.append(name)
        if commodity_id not in commodity_ids:
            commodity_ids.append(commodity_id)
    if missing:
        logger.warning("Requested commodities not available in PriceCache for %s: %s", canonical, missing)
    if not valid_names:
        raise ValueError(
            f"No requested commodities are available in the active PriceCache for {canonical}. "
            f"Available commodities: {get_available_commodities(canonical)}"
        )

    df = _get_country_price_df(
        canonical,
        iso3,
        start_date=start_date.strftime("%Y-%m-%d"),
        end_date=end_date.strftime("%Y-%m-%d"),
        commodity_ids=commodity_ids,
    )
    if df.empty:
        raise ValueError(
            f"PriceCache returned no monthly price rows for {canonical} from "
            f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}."
        )

    df = df[df["Commodity"].isin(valid_names)].copy()
    if df.empty:
        raise ValueError(f"No PriceCache price rows matched the requested commodities: {valid_names}")

    df, selected_currency, excluded_currencies = _select_report_currency(
        df,
        requested_code=currency_code,
        country=canonical,
    )
    if excluded_currencies:
        logger.warning(
            "Multiple currencies present in PriceCache rows for %s; report uses %s and excludes %s.",
            canonical,
            selected_currency,
            excluded_currencies,
        )
    if df.empty:
        raise ValueError(
            f"No PriceCache price rows remained for {canonical} after selecting a single report currency."
        )

    df["Month"] = df["Price Date"].dt.to_period("M").dt.to_timestamp()

    national_pivot = df.pivot_table(
        index="Month",
        columns="Commodity",
        values="Price",
        aggfunc="mean",
    ).reindex(full_date_index)

    for name in valid_names:
        if name not in national_pivot.columns:
            national_pivot[name] = np.nan
    national_pivot = national_pivot[valid_names].round(2)
    if basket_components:
        basket_by_id = df.pivot_table(
            index="Month",
            columns="Commodity ID",
            values="Price",
            aggfunc="mean",
        ).reindex(full_date_index)
        basket_component_ids = [component["commodity_id"] for component in basket_components]
        weighted_components = pd.DataFrame(index=full_date_index)
        for component in basket_components:
            commodity_id = component["commodity_id"]
            if commodity_id not in basket_by_id.columns:
                basket_by_id[commodity_id] = np.nan
            weighted_components[commodity_id] = basket_by_id[commodity_id] * component["weight_quantity"]
        national_pivot["FoodBasket"] = weighted_components.sum(axis=1, skipna=True).round(2)
        national_pivot.loc[basket_by_id[basket_component_ids].isna().all(axis=1), "FoodBasket"] = np.nan
    else:
        national_pivot["FoodBasket"] = national_pivot[valid_names].sum(axis=1, skipna=True).round(2)
        national_pivot.loc[national_pivot[valid_names].isna().all(axis=1), "FoodBasket"] = np.nan
    national_pivot["ExchangeRate"] = np.nan
    national_pivot["FuelPrice"] = np.nan
    national_pivot.index.name = "Date"

    available_regions = sorted(df["Admin 1"].dropna().astype(str).unique().tolist())
    if admin1_list:
        valid_regions = [region for region in admin1_list if region in available_regions]
        if not valid_regions:
            raise ValueError(
                f"Requested regions are not available for {canonical}: {admin1_list}. "
                f"Available regions: {available_regions}"
            )
    else:
        valid_regions = available_regions

    df_regional_data = df[df["Admin 1"].isin(valid_regions)].copy()
    if basket_components:
        weights_by_id = {component["commodity_id"]: component["weight_quantity"] for component in basket_components}
        regional_components = (
            df_regional_data[df_regional_data["Commodity ID"].isin(weights_by_id.keys())]
            .groupby(["Month", "Admin 1", "Commodity ID"], dropna=True)["Price"]
            .mean()
            .reset_index()
        )
        if not regional_components.empty:
            regional_components["ComponentValue"] = regional_components.apply(
                lambda row: float(row["Price"]) * weights_by_id.get(int(row["Commodity ID"]), 0.0),
                axis=1,
            )
            regional_agg = (
                regional_components.groupby(["Month", "Admin 1"], dropna=True)["ComponentValue"]
                .sum()
                .reset_index()
            )
        else:
            regional_agg = pd.DataFrame(columns=["Month", "Admin 1", "ComponentValue"])
        regional_agg.columns = ["Date", "Region", "FoodBasket"]
    else:
        regional_agg = (
            df_regional_data.groupby(["Month", "Admin 1", "Commodity"], dropna=True)["Price"]
            .mean()
            .reset_index()
            .groupby(["Month", "Admin 1"], dropna=True)["Price"]
            .sum()
            .reset_index()
        )
        regional_agg.columns = ["Date", "Region", "FoodBasket"]
    regional_agg["FoodBasket"] = regional_agg["FoodBasket"].round(2)

    regional_index = pd.MultiIndex.from_product(
        [full_date_index, valid_regions],
        names=["Date", "Region"],
    )
    df_regional = pd.DataFrame(index=regional_index).reset_index()
    df_regional = df_regional.merge(regional_agg, on=["Date", "Region"], how="left")
    df_regional = df_regional.sort_values(["Date", "Region"]).reset_index(drop=True)

    if return_raw_rows:
        raw_columns = [
            "Country",
            "Country ISO3",
            "Commodity",
            "Commodity ID",
            "Price Type",
            "Price Date",
            "Price",
            "Admin 1",
            "Admin 2",
            "Market Name",
            "Market ID",
            "Unit",
            "Currency",
            "Data Type",
            "Price Flag",
            "Observations",
            "Data Source",
        ]
        raw_rows = df[raw_columns].sort_values(
            ["Price Date", "Admin 1", "Market Name", "Commodity"],
            na_position="last",
        ).reset_index(drop=True)
        return national_pivot, df_regional, raw_rows

    return national_pivot, df_regional


extract_time_series_from_databridges = extract_time_series_from_csv


def calculate_statistics_from_csv(
    df_national: pd.DataFrame,
    commodities: List[str],
    food_basket_components: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    stats: Dict[str, Any] = {
        "food_basket": {},
        "commodities": {},
        "auxiliary": {},
    }
    if df_national.empty:
        return stats

    current_idx = -1
    mom_idx = -2 if len(df_national) >= 2 else -1
    yoy_idx = 0
    current = df_national.iloc[current_idx]
    mom = df_national.iloc[mom_idx]
    yoy = df_national.iloc[yoy_idx]
    basket_components = _normalise_basket_components(food_basket_components)
    if basket_components:
        selected_components = [
            component["commodity_name"]
            for component in basket_components
            if component["commodity_name"] in df_national.columns
        ]
        configured_components = [component["commodity_name"] for component in basket_components]
    else:
        selected_components = [commodity for commodity in commodities if commodity in df_national.columns]
        configured_components = list(selected_components)
    historical_components = [
        commodity for commodity in selected_components if df_national[commodity].notna().any()
    ]
    latest_components = [
        commodity for commodity in selected_components if pd.notna(current.get(commodity))
    ]
    missing_latest_components = [
        commodity for commodity in selected_components if commodity not in latest_components
    ]

    for column in df_national.columns:
        current_val = current[column]
        if pd.isna(current_val):
            continue
        mom_val = mom[column]
        yoy_val = yoy[column]
        mom_pct = round(((current_val - mom_val) / mom_val * 100), 1) if pd.notna(mom_val) and mom_val != 0 else None
        yoy_pct = round(((current_val - yoy_val) / yoy_val * 100), 1) if pd.notna(yoy_val) and yoy_val != 0 else None
        item = {
            "current_price": round(float(current_val), 2),
            "mom_change_pct": float(mom_pct) if mom_pct is not None else None,
            "yoy_change_pct": float(yoy_pct) if yoy_pct is not None else None,
        }
        if column == "FoodBasket":
            item.update(
                {
                    "selected_component_count": len(selected_components),
                    "selected_component_names": selected_components,
                    "configured_component_count": len(configured_components),
                    "configured_component_names": configured_components,
                    "historical_component_count": len(historical_components),
                    "historical_component_names": historical_components,
                    "latest_component_count": len(latest_components),
                    "latest_component_names": latest_components,
                    "available_component_count": len(latest_components),
                    "available_component_names": latest_components,
                    "missing_latest_component_names": missing_latest_components,
                    "missing_component_names": missing_latest_components,
                }
            )
            stats["food_basket"] = item
        elif any(token in column.lower() for token in ["exchange", "fuel", "wage", "milling"]):
            stats["auxiliary"][column] = item
        elif column in commodities:
            stats["commodities"][column] = item
    return stats


def check_data_availability(
    country: str,
    time_period: str,
    commodities: List[str],
    csv_path: Optional[Path] = None,
    currency_code: Optional[str] = None,
) -> Dict[str, Any]:
    if csv_path is not None:
        logger.info("Ignoring csv_path=%s because PriceCache is now the price source.", csv_path)

    try:
        canonical, iso3 = resolve_country(country)
        metadata = get_country_metadata(canonical)
        available_commodities = [str(item["name"]) for item in metadata.get("commodities", [])]
        missing = [item for item in commodities if item not in available_commodities]
        target_date = _parse_time_period(time_period)
        start_date = target_date - pd.DateOffset(months=12)
        df = _get_country_price_df(
            canonical,
            iso3,
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=target_date.strftime("%Y-%m-%d"),
        )
        expected_months = pd.date_range(start=start_date, end=target_date, freq="MS")
        actual_months = set()
        if not df.empty:
            actual_months = set(df["Price Date"].dt.to_period("M").dt.to_timestamp())
        data_gaps = [month.strftime("%Y-%m") for month in expected_months if month not in actual_months]

        warnings = list(metadata.get("warnings") or [])
        if missing:
            warnings.append(
                f"Requested commodities not available in PriceCache for {metadata.get('country')}: {missing}."
            )
        if data_gaps:
            warnings.append(f"PriceCache price data has gaps in {len(data_gaps)} month(s): {data_gaps}.")
        if metadata.get("date_range") is None:
            warnings.append(f"PriceCache returned no monthly price date range for {metadata.get('country')}.")

        if not df.empty and "Currency" in df.columns:
            currencies_present = sorted(
                {str(value) for value in df["Currency"].dropna().astype(str) if str(value).strip()}
            )
            if len(currencies_present) > 1:
                _, selected_currency, _excluded = _select_report_currency(
                    df,
                    requested_code=currency_code,
                    country=canonical,
                )
                warnings.append(
                    f"Cached prices for {metadata.get('country')} are quoted in multiple currencies "
                    f"({', '.join(currencies_present)}). Report calculations use a single currency "
                    f"({selected_currency}); rows in other currencies are excluded."
                )

        return {
            "available": True,
            "country_normalized": metadata.get("country") or canonical,
            "iso3": iso3,
            "countries": get_available_countries(),
            "commodities": available_commodities,
            "regions": metadata.get("regions", []),
            "markets": [market.get("market_name") for market in metadata.get("markets", [])],
            "date_range": metadata.get("date_range"),
            "missing_commodities": missing,
            "data_gaps": data_gaps,
            "warnings": warnings,
            "cache_metadata": get_cache_metadata_for_report(canonical),
        }
    except Exception as exc:
        return {
            "available": False,
            "error": str(exc),
            "country_normalized": None,
            "countries": get_available_countries(),
            "commodities": [],
            "regions": [],
            "markets": [],
            "date_range": None,
            "missing_commodities": commodities,
            "data_gaps": [],
            "warnings": [str(exc)],
            "cache_metadata": _safe_cache_metadata(),
        }


def get_data_summary(csv_path: Optional[Path] = None) -> Dict[str, Any]:
    if csv_path is not None:
        logger.info("Ignoring csv_path=%s because PriceCache is now the price source.", csv_path)
    countries = get_supported_countries()
    status = get_cache_status_snapshot()
    return {
        "source": "PriceCache",
        "countries": {str(item["name"]): {"iso3": item["iso3"]} for item in countries},
        "total_records": status.get("rows_prices"),
        "date_range": None,
        "cache_status": status,
    }


def _get_country_price_df(
    canonical: str,
    iso3: str,
    *,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    commodity_ids: Optional[Iterable[int]] = None,
    latest_value_only: bool = False,
) -> pd.DataFrame:
    ids = tuple(sorted({int(item) for item in commodity_ids or [] if item is not None}))
    cache_key = (iso3, start_date, end_date, ids, latest_value_only)
    cached = _cache_get(_PRICE_CACHE, cache_key)
    if cached is not None:
        return cached.copy()

    repo = _get_price_cache_repository()
    availability = repo.get_country_availability(iso3)
    if availability is None:
        df = _empty_price_df()
        _cache_set(_PRICE_CACHE, cache_key, df)
        return df.copy()

    query_start = start_date or _date_wire(availability.date_start)
    query_end = end_date or _date_wire(availability.date_end)
    if not query_start or not query_end:
        df = _empty_price_df()
        _cache_set(_PRICE_CACHE, cache_key, df)
        return df.copy()

    rows = repo.get_price_window(
        iso3,
        query_start,
        query_end,
        commodity_ids=ids or None,
    )
    df = _normalise_cached_price_rows(rows, canonical, iso3)
    if latest_value_only and not df.empty:
        latest_date = df["Price Date"].max()
        df = df[df["Price Date"] == latest_date].copy()
    _cache_set(_PRICE_CACHE, cache_key, df)
    return df.copy()


def _normalise_cached_price_rows(
    rows: list[MonthlyPriceRecord],
    canonical: str,
    iso3: str,
) -> pd.DataFrame:
    records = []
    for row in _sort_cached_price_rows(rows):
        flag = str(row.price_flag or "").strip().lower()
        if not _is_real_price_flag(flag):
            continue

        price = pd.to_numeric(row.price, errors="coerce")
        price_date = pd.to_datetime(row.price_date, errors="coerce")
        if pd.isna(price) or pd.isna(price_date):
            continue

        records.append(
            {
                "Country": canonical,
                "Country ISO3": iso3,
                "Commodity": row.commodity_name or str(row.commodity_id),
                "Commodity ID": row.commodity_id,
                "Price Type": row.price_type_name or "",
                "Price Date": price_date.to_period("M").to_timestamp(),
                "Price": float(price),
                "Admin 1": row.admin1_name or "Unknown",
                "Admin 2": row.admin2_name or "",
                "Market Name": row.market_name or "Unknown",
                "Market ID": row.market_id,
                "Unit": row.commodity_unit_name or "",
                "Currency": row.currency_name or row.currency_code or "",
                "Currency Code": row.currency_code or "",
                "Data Type": _data_type_from_flag(flag),
                "Price Flag": flag,
                "Observations": row.observations,
                "Data Source": row.data_source or "PriceCache",
            }
        )

    columns = [
        "Country",
        "Country ISO3",
        "Commodity",
        "Commodity ID",
        "Price Type",
        "Price Date",
        "Price",
        "Admin 1",
        "Admin 2",
        "Market Name",
        "Market ID",
        "Unit",
        "Currency",
        "Currency Code",
        "Data Type",
        "Price Flag",
        "Observations",
        "Data Source",
    ]
    if not records:
        return pd.DataFrame(columns=columns)
    df = pd.DataFrame(records)
    sort_columns = [
        "Commodity ID",
        "Commodity",
        "Price Type",
        "Currency",
        "Unit",
        "Price Date",
        "Market ID",
        "Market Name",
        "Price Flag",
        "Price",
        "Observations",
        "Data Source",
    ]
    return df.sort_values(sort_columns, na_position="last").drop_duplicates().reset_index(drop=True)


def _get_commodities(canonical: str, iso3: str) -> list[dict[str, Any]]:
    cached = _cache_get(_COMMODITY_CACHE, iso3)
    if cached is not None:
        return list(cached)

    metadata = _get_repository_country_metadata(iso3)
    commodities = []
    for row in metadata.commodities:
        name = str(row.commodity_name or "").strip()
        if not name:
            continue
        commodities.append(
            {
                "id": row.commodity_id,
                "name": name,
                "category": row.category_name or _infer_category(name),
                "category_id": None,
                "unit_id": row.commodity_unit_id,
                "unit": row.commodity_unit_name,
                "unit_name": row.commodity_unit_name,
                "country": metadata.country.country_name or canonical,
                "iso3": iso3,
            }
        )
    commodities = sorted(commodities, key=_commodity_sort_key)
    _cache_set(_COMMODITY_CACHE, iso3, commodities)
    return list(commodities)


def _get_markets(canonical: str, iso3: str) -> list[dict[str, Any]]:
    cached = _cache_get(_MARKET_CACHE, iso3)
    if cached is not None:
        return list(cached)

    metadata = _get_repository_country_metadata(iso3)
    markets = _markets_payload(metadata, country_name=metadata.country.country_name or canonical, iso3=iso3)
    _cache_set(_MARKET_CACHE, iso3, markets)
    return list(markets)


def _market_lookup(canonical: str, iso3: str) -> dict[int, dict[str, Any]]:
    return {
        int(market["market_id"]): market
        for market in _get_markets(canonical, iso3)
        if market.get("market_id") is not None
    }


def _sort_cached_price_rows(rows: list[MonthlyPriceRecord]) -> list[MonthlyPriceRecord]:
    return sorted(rows, key=_cached_price_sort_key)


def _cached_price_sort_key(row: MonthlyPriceRecord) -> tuple[Any, ...]:
    return (
        _sort_int(row.commodity_id),
        _sort_text(row.commodity_name),
        _sort_text(row.price_type_name),
        _sort_text(row.currency_name or row.currency_code),
        _sort_text(row.commodity_unit_name),
        _sort_text(row.price_date),
        _sort_int(row.market_id),
        _sort_text(row.market_name),
        _sort_text(row.price_flag),
        _sort_float(row.price),
        _sort_int(row.observations),
        _sort_text(row.data_source),
    )


def _commodity_sort_key(item: dict[str, Any]) -> tuple[str, int]:
    commodity_id = _to_int(item.get("id"))
    return (str(item.get("name") or "").strip().lower(), commodity_id if commodity_id is not None else 10**12)


def _normalise_basket_components(items: Optional[List[Dict[str, Any]]]) -> list[dict[str, Any]]:
    components: list[dict[str, Any]] = []
    seen: set[int] = set()
    for item in items or []:
        if not isinstance(item, dict):
            continue
        commodity_id = _to_int(item.get("commodity_id"))
        name = str(
            item.get("commodity_name_snapshot")
            or item.get("commodity_name")
            or item.get("name")
            or ""
        ).strip()
        weight = _to_float(item.get("weight_quantity"))
        if commodity_id is None or not name or weight is None or weight <= 0:
            continue
        if commodity_id in seen:
            continue
        seen.add(commodity_id)
        components.append(
            {
                "commodity_id": commodity_id,
                "commodity_name": name,
                "weight_quantity": weight,
                "databridges_unit": item.get("databridges_unit") or item.get("unit"),
            }
        )
    return components


def _resolve_commodities(
    canonical: str,
    iso3: str,
    requested: List[str],
) -> Tuple[List[str], List[int], List[str]]:
    commodities = _get_commodities(canonical, iso3)
    by_name: dict[str, dict[str, Any]] = {}
    for item in sorted(commodities, key=_commodity_sort_key):
        by_name.setdefault(str(item["name"]).lower(), item)
    by_id = {str(item["id"]): item for item in commodities if item.get("id") is not None}

    selected = requested or _select_default_commodities([str(item["name"]) for item in commodities])
    valid_names: list[str] = []
    valid_ids: list[int] = []
    missing: list[str] = []
    for item in selected:
        key = str(item).strip().lower()
        commodity = by_name.get(key) or by_id.get(str(item).strip())
        if not commodity:
            missing.append(str(item))
            continue
        name = str(commodity["name"])
        if name not in valid_names:
            valid_names.append(name)
        if commodity.get("id") is not None:
            valid_ids.append(int(commodity["id"]))
    return valid_names, valid_ids, missing


def _select_default_commodities(available: List[str], max_items: int = 5) -> List[str]:
    defaults: list[str] = []
    priority_patterns = ["maize", "wheat", "rice", "sorghum", "millet", "beans", "lentil", "oil", "salt", "sugar"]
    for pattern in priority_patterns:
        for commodity in available:
            if pattern in commodity.lower() and commodity not in defaults:
                defaults.append(commodity)
                break
        if len(defaults) >= max_items:
            return defaults
    for commodity in available:
        if commodity not in defaults:
            defaults.append(commodity)
        if len(defaults) >= max_items:
            break
    return defaults


def _infer_category(commodity: str) -> str:
    categories = get_commodity_categories([commodity])
    return next(iter(categories.keys()), "Other")


def _parse_time_period(time_period: str) -> pd.Timestamp:
    try:
        return pd.to_datetime(f"{time_period}-01").to_period("M").to_timestamp()
    except Exception as exc:
        raise ValueError(f"Invalid time period '{time_period}'. Expected YYYY-MM.") from exc


def _recent_price_window(months: int = _RECENT_METADATA_MONTHS) -> Tuple[str, str]:
    today = pd.Timestamp.today().normalize()
    start = (today - pd.DateOffset(months=months)).replace(day=1)
    return start.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d")


def _get_price_cache_repository() -> PriceCacheRepository:
    config = load_price_cache_config()
    engine = create_price_cache_engine(config)
    apply_migrations(engine, config.backend)
    return SqlPriceCacheRepository(engine)


def _get_repository_country_metadata(iso3: str) -> CountryMetadata:
    repo = _get_price_cache_repository()
    metadata = repo.get_country_metadata(iso3)
    if metadata is None:
        raise PriceCacheUnavailableError(f"No active PriceCache metadata is available for {iso3}.")
    return metadata


def _markets_payload(metadata: CountryMetadata, *, country_name: str, iso3: str) -> list[dict[str, Any]]:
    markets = []
    for row in metadata.markets:
        markets.append(
            {
                "market_id": row.market_id,
                "market_name": row.market_name,
                "admin1_name": row.admin1_name or "",
                "admin1_code": None,
                "admin2_name": row.admin2_name or "",
                "admin2_code": None,
                "latitude": row.latitude,
                "longitude": row.longitude,
                "country": country_name,
                "iso3": iso3,
            }
        )
    return sorted(markets, key=lambda item: item["market_name"])


def _merge_units(metadata: CountryMetadata, derived_units: list[Any]) -> list[dict[str, Any]]:
    by_id: dict[int, dict[str, Any]] = {}
    for unit in metadata.units:
        by_id[unit.commodity_unit_id] = {
            "id": unit.commodity_unit_id,
            "name": unit.commodity_unit_name,
            "conversion_to_kg_l": unit.conversion_to_kg_l,
            "source": "cached_units",
        }
    for unit in derived_units:
        by_id.setdefault(
            unit.commodity_unit_id,
            {
                "id": unit.commodity_unit_id,
                "name": unit.commodity_unit_name,
                "conversion_to_kg_l": unit.conversion_to_kg_l,
                "source": "price_rows",
            },
        )
    return sorted(by_id.values(), key=lambda item: str(item["name"]).lower())


def _country_currency(country: Any, *, fallback_country: str) -> dict[str, str]:
    code = getattr(country, "currency_code", None)
    name = getattr(country, "currency_name", None)
    if code or name:
        return {"code": str(code or "USD"), "name": str(name or code or "US Dollar")}
    return COUNTRY_CURRENCIES.get(fallback_country, {"code": "USD", "name": "US Dollar"})


def _cache_status_dict(status: CacheStatus) -> dict[str, Any]:
    return {
        "source": "PriceCache",
        "has_active_cache": status.has_active_cache,
        "active_version_id": status.active_version_id,
        "status": status.status,
        "activated_at": _datetime_wire(status.activated_at),
        "completed_at": _datetime_wire(status.completed_at),
        "rows_prices": status.rows_prices,
        "rows_commodities": status.rows_commodities,
        "rows_markets": status.rows_markets,
        "rows_countries": status.rows_countries,
        "active_country_count": status.active_country_count,
        "validation_summary": status.validation_summary,
        "error_message": status.error_message,
        "warnings": _cache_warnings(status),
    }


def _cache_warnings(status: CacheStatus, *, country_iso3: str | None = None) -> list[str]:
    summary = status.validation_summary or {}
    raw_items = list(summary.get("warnings") or [])
    if country_iso3:
        return _country_cache_warnings(raw_items, country_iso3=country_iso3, error_message=status.error_message)

    warnings: list[str] = []
    unit_warning: Optional[str] = None
    metadata_reference_countries = 0
    metadata_reference_count = 0
    dedup_countries: set[str] = set()
    dedup_rows = 0
    excluded_non_real_countries: set[str] = set()
    excluded_non_real_rows = 0
    conflict_countries: set[str] = set()
    conflict_keys = 0
    other_warnings: list[str] = []

    for item in raw_items:
        if isinstance(item, str):
            other_warnings.append(_sanitize_cache_warning(item))
            continue
        if not isinstance(item, dict):
            other_warnings.append(_sanitize_cache_warning(str(item)))
            continue

        warning = item.get("warning")
        if warning:
            sanitized = _sanitize_cache_warning(str(warning))
            if "CommodityUnits/List" in sanitized:
                unit_warning = unit_warning or sanitized
            else:
                other_warnings.append(sanitized)

        nested = item.get("warnings")
        if not isinstance(nested, list):
            continue
        item_country = str(item.get("country_iso3") or "").upper()
        for nested_warning in nested:
            text = str(nested_warning)
            metadata_match = re.search(r"Found\s+(\d+)\s+price metadata references", text)
            if metadata_match:
                metadata_reference_countries += 1
                metadata_reference_count += int(metadata_match.group(1))
                continue
            dedup_match = re.search(r"Deduplicated\s+(\d+)\s+duplicate monthly price row", text)
            if dedup_match:
                dedup_rows += int(dedup_match.group(1))
                if item_country:
                    dedup_countries.add(item_country)
                continue
            excluded_match = re.search(r"Excluded\s+(\d+)\s+non-real monthly price row", text)
            if excluded_match:
                excluded_non_real_rows += int(excluded_match.group(1))
                if item_country:
                    excluded_non_real_countries.add(item_country)
                continue
            conflict_match = re.search(r"(\d+)\s+duplicate monthly price key\(s\).*conflicting price values", text)
            if conflict_match:
                conflict_keys += int(conflict_match.group(1))
                if item_country:
                    conflict_countries.add(item_country)
                continue
            other_warnings.append(_sanitize_cache_warning(text))

    if unit_warning:
        warnings.append(unit_warning)
    if metadata_reference_countries:
        warnings.append(
            "Price metadata references were missing from current commodity/market metadata for "
            f"{metadata_reference_countries} country/countries ({metadata_reference_count} reference(s)); "
            "historical price rows were still cached."
        )
    if dedup_rows:
        warnings.append(
            f"Deduplicated {dedup_rows} duplicate monthly price row(s) across "
            f"{len(dedup_countries)} country/countries before cache publication."
        )
    if excluded_non_real_rows:
        warnings.append(
            f"Excluded {excluded_non_real_rows} non-real monthly price row(s) across "
            f"{len(excluded_non_real_countries)} country/countries before cache publication; "
            "only actual/aggregate rows are cached."
        )
    if conflict_keys:
        warnings.append(
            f"{conflict_keys} duplicate monthly price key(s) across {len(conflict_countries)} "
            "country/countries had conflicting price values; deterministic best-ranked rows were kept."
        )
    warnings.extend(other_warnings[:10])
    if len(other_warnings) > 10:
        warnings.append(f"{len(other_warnings) - 10} additional cache warning(s) omitted from this summary.")
    if status.error_message:
        warnings.append(str(status.error_message))
    return _dedupe_preserve_order(warnings)


def _country_cache_warnings(
    raw_items: list[Any],
    *,
    country_iso3: str,
    error_message: str | None = None,
) -> list[str]:
    country = country_iso3.upper()
    warnings: list[str] = []
    for item in raw_items:
        if isinstance(item, str):
            warnings.append(_sanitize_cache_warning(item))
            continue
        if not isinstance(item, dict):
            warnings.append(_sanitize_cache_warning(str(item)))
            continue

        item_country = str(item.get("country_iso3") or "").upper()
        warning = item.get("warning")
        if warning and not item_country:
            warnings.append(_sanitize_cache_warning(str(warning)))
        if item_country != country:
            continue
        nested = item.get("warnings")
        if isinstance(nested, list):
            warnings.extend(_sanitize_cache_warning(str(value)) for value in nested if value)
    if error_message:
        warnings.append(str(error_message))
    return _dedupe_preserve_order(warnings)


def _sanitize_cache_warning(warning: str) -> str:
    text_value = str(warning).strip()
    if len(text_value) > 500:
        text_value = text_value[:497] + "..."
    return text_value


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        deduped.append(value)
    return deduped


def _bounded_cache_dates(start: Any, end: Any, latest: Any) -> tuple[Optional[dict[str, str]], Optional[str], bool]:
    start_d = _date_obj(start)
    end_d = _date_obj(end)
    latest_d = _date_obj(latest) or end_d
    current_month = _current_month_start()
    has_future_dates = any(value is not None and value > current_month for value in (end_d, latest_d))

    if end_d and end_d > current_month:
        end_d = current_month
    if latest_d and latest_d > current_month:
        latest_d = end_d or current_month
    if start_d and end_d and start_d > end_d:
        return None, _date_wire(latest_d), has_future_dates
    return _date_range_dict(start_d, end_d), _date_wire(latest_d), has_future_dates


def _current_month_start() -> date:
    today = date.today()
    return date(today.year, today.month, 1)


def _date_obj(value: Any) -> Optional[date]:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return datetime.fromisoformat(str(value)[:10]).date()
    except ValueError:
        return None


def _safe_cache_metadata() -> dict[str, Any]:
    try:
        return get_cache_status_snapshot()
    except Exception:
        return {"source": "PriceCache", "has_active_cache": False}


def _date_range_dict(start: Any, end: Any) -> Optional[dict[str, str]]:
    if not start or not end:
        return None
    return {"start": _date_wire(start), "end": _date_wire(end)}


def _date_wire(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)[:10]


def _datetime_wire(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _empty_price_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "Country",
            "Country ISO3",
            "Commodity",
            "Commodity ID",
            "Price Type",
            "Price Date",
            "Price",
            "Admin 1",
            "Admin 2",
            "Market Name",
            "Market ID",
            "Unit",
            "Currency",
            "Currency Code",
            "Data Type",
            "Price Flag",
            "Observations",
            "Data Source",
        ]
    )


def _is_real_price_flag(flag: Any) -> bool:
    normalized = str(flag or "").strip().lower()
    if not normalized:
        return True
    components = [component.strip() for component in normalized.split(",") if component.strip()]
    return bool(components) and all(component in _REAL_PRICE_FLAG_COMPONENTS for component in components)


def _data_type_from_flag(flag: str) -> str:
    components = [component.strip() for component in str(flag or "").split(",") if component.strip()]
    if any(component in {"aggregate", "aggregated"} for component in components):
        return "Aggregated"
    if components:
        return components[0].title()
    return ""


def _select_report_currency(
    df: pd.DataFrame,
    *,
    requested_code: Optional[str] = None,
    country: Optional[str] = None,
) -> tuple[pd.DataFrame, Optional[str], list[str]]:
    """Restrict a price dataframe to a single currency.

    Databridges can quote the same series in several currencies (e.g. LBP and
    USD in Lebanon); averaging across them would corrupt every statistic, so one
    currency is selected per report: the requested currency if present, then the
    country default, then the most frequent currency in the data.
    """
    if df.empty or "Currency" not in df.columns:
        return df, None, []

    labels = df["Currency"].fillna("").astype(str)
    codes = (
        df["Currency Code"].fillna("").astype(str)
        if "Currency Code" in df.columns
        else labels
    )
    distinct_labels = sorted({label for label in labels if label.strip()})
    if len(distinct_labels) <= 1:
        return df, (distinct_labels[0] if distinct_labels else None), []

    preferences: list[str] = []
    if requested_code:
        preferences.append(str(requested_code))
    default_currency = COUNTRY_CURRENCIES.get(country or "")
    if default_currency:
        preferences.extend([default_currency.get("code") or "", default_currency.get("name") or ""])

    selected_label: Optional[str] = None
    for preference in preferences:
        if not str(preference).strip():
            continue
        match_mask = (codes.str.lower() == str(preference).lower()) | (
            labels.str.lower() == str(preference).lower()
        )
        if match_mask.any():
            selected_label = labels[match_mask].iloc[0]
            break
    if selected_label is None:
        selected_label = labels[labels.str.strip() != ""].value_counts().idxmax()

    excluded = sorted(set(distinct_labels) - {selected_label})
    return df[labels == selected_label].copy(), selected_label, excluded


def _split_warning_audiences(warnings: list[str]) -> tuple[list[str], list[str]]:
    """Separate officer-relevant warnings from refresh-worker ETL telemetry."""
    user_warnings: list[str] = []
    operator_warnings: list[str] = []
    for warning in warnings:
        text_value = str(warning)
        if any(marker in text_value for marker in _OPERATOR_WARNING_MARKERS):
            operator_warnings.append(text_value)
        else:
            user_warnings.append(text_value)
    return user_warnings, operator_warnings


def _field(row: dict[str, Any], *names: str, default: Any = None) -> Any:
    for name in names:
        if name in row:
            return row[name]
    lowered = {key.lower(): value for key, value in row.items()}
    for name in names:
        key = name.lower()
        if key in lowered:
            return lowered[key]
    return default


def _to_int(value: Any) -> Optional[int]:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _sort_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().lower()


def _sort_int(value: Any) -> int:
    parsed = _to_int(value)
    return parsed if parsed is not None else 10**12


def _sort_float(value: Any) -> float:
    parsed = _to_float(value)
    return parsed if parsed is not None else float("inf")


def _cache_get(cache: dict[Any, tuple[float, Any]], key: Any) -> Any:
    item = cache.get(key)
    if not item:
        return None
    created_at, value = item
    if time.time() - created_at > _CACHE_TTL_SECONDS:
        cache.pop(key, None)
        return None
    return value


def _cache_set(cache: dict[Any, tuple[float, Any]], key: Any, value: Any) -> None:
    cache[key] = (time.time(), value)
