"""Databridges-backed data loading for the Price Bulletin drafter."""
from __future__ import annotations

import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

from app.shared.countries import (
    COUNTRY_CURRENCIES,
    normalize_country_name as _normalize_country_name,
    resolve_country,
    supported_country_options,
)
from app.shared.databridges import get_databridges_client
from app.shared.gcs import (
    download_gcs_to_file as _download_gcs_to_file,
    parse_gcs_uri as _parse_gcs_uri,
)

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent / "data"
_CACHE_TTL_SECONDS = 15 * 60
_RECENT_METADATA_MONTHS = 36
_ALLOWED_PRICE_FLAGS = {"actual", "aggregate", "aggregated"}

_COMMODITY_CACHE: dict[str, tuple[float, list[dict[str, Any]]]] = {}
_MARKET_CACHE: dict[str, tuple[float, list[dict[str, Any]]]] = {}
_PRICE_CACHE: dict[tuple[Any, ...], tuple[float, pd.DataFrame]] = {}
_METADATA_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}


def normalize_country_name(country: str) -> str:
    """Normalize a UI country value to the canonical country name."""
    return _normalize_country_name(country)


def reset_market_monitor_caches_for_tests() -> None:
    _COMMODITY_CACHE.clear()
    _MARKET_CACHE.clear()
    _PRICE_CACHE.clear()
    _METADATA_CACHE.clear()


def load_csv_price_data(csv_path: Optional[Path] = None) -> pd.DataFrame:
    """Deprecated compatibility shim for the removed spreadsheet data path."""
    raise FileNotFoundError(
        "The provisional price_data.csv path has been removed. Price Bulletin "
        "data is now retrieved from Databridges."
    )


def _upload_file_to_gcs(content: bytes, gcs_uri: str) -> None:
    """Deprecated compatibility shim for removed dataset upload endpoints."""
    raise RuntimeError(
        "Uploading processed Price Bulletin datasets is no longer supported. "
        "Use the Price Data Validator to validate raw files before Databridges upload."
    )


def get_supported_countries() -> List[Dict[str, Any]]:
    """Return country options backed by the existing ISO3/currency mapping."""
    return supported_country_options()


def get_available_countries(df: Optional[pd.DataFrame] = None) -> List[str]:
    if df is not None and "Country" in df.columns:
        return sorted(df["Country"].dropna().astype(str).unique().tolist())
    return [str(item["name"]) for item in get_supported_countries()]


def get_available_commodities(
    df_or_country: Optional[Any] = None,
    country: Optional[str] = None,
) -> List[str]:
    """Return commodity names available for a country.

    Accepts the old signature ``get_available_commodities(df, country)`` and the
    new Databridges signature ``get_available_commodities(country)``.
    """
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
    canonical, iso3 = resolve_country(country_value)
    return sorted(
        {
            str(market["admin1_name"])
            for market in _get_markets(canonical, iso3)
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
    canonical, iso3 = resolve_country(country_value)
    df = _get_country_price_df(canonical, iso3)
    if df.empty:
        raise ValueError(f"No monthly price data returned by Databridges for {canonical}.")
    return df["Price Date"].min().to_pydatetime(), df["Price Date"].max().to_pydatetime()


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

    commodities = [
        {"id": item["id"], "name": item["name"], "category": _infer_category(item["name"])}
        for item in _get_commodities(canonical, iso3)
    ]
    metadata_warnings: list[str] = []
    start_date, end_date = _recent_price_window()
    price_df = _get_country_price_df(canonical, iso3, start_date=start_date, end_date=end_date)
    metadata_window = {
        "start": start_date,
        "end": end_date,
        "months": _RECENT_METADATA_MONTHS,
        "bounded": True,
    }
    if price_df.empty:
        metadata_warnings.append(
            f"No Databridges price rows found in the recent {_RECENT_METADATA_MONTHS}-month metadata window; used unbounded fallback."
        )
        price_df = _get_country_price_df(canonical, iso3)
        metadata_window = {
            "start": None,
            "end": None,
            "months": None,
            "bounded": False,
        }
    if not price_df.empty:
        priced_names = sorted(price_df["Commodity"].dropna().astype(str).unique().tolist())
        if priced_names:
            commodity_by_name = {str(item["name"]).lower(): item for item in commodities}
            commodities = [
                commodity_by_name.get(name.lower(), {"id": None, "name": name, "category": _infer_category(name)})
                for name in priced_names
            ]

    regions = get_available_regions(canonical)
    markets = _get_markets(canonical, iso3)
    date_range = None
    if not price_df.empty:
        date_range = {
            "start": price_df["Price Date"].min().strftime("%Y-%m-%d"),
            "end": price_df["Price Date"].max().strftime("%Y-%m-%d"),
        }

    commodity_names = [str(item["name"]) for item in commodities if item.get("name")]
    metadata = {
        "country": canonical,
        "iso3": iso3,
        "currency": COUNTRY_CURRENCIES.get(canonical, {"code": "USD", "name": "US Dollar"}),
        "commodities": commodities,
        "commodity_categories": get_commodity_categories(commodity_names),
        "default_commodities": _select_default_commodities(commodity_names),
        "regions": regions,
        "markets": markets,
        "date_range": date_range,
        "metadata_price_window": metadata_window,
        "warnings": metadata_warnings,
        "source": "Databridges",
    }
    _cache_set(_METADATA_CACHE, iso3, metadata)
    return dict(metadata)


def extract_time_series_from_csv(
    country: str,
    time_period: str,
    commodities: List[str],
    admin1_list: List[str],
    csv_path: Optional[Path] = None,
    lookback_months: int = 13,
    return_raw_rows: bool = False,
) -> Tuple[pd.DataFrame, pd.DataFrame] | Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Compatibility wrapper that returns Databridges price time series."""
    if csv_path is not None:
        logger.info("Ignoring csv_path=%s because Databridges is now the price source.", csv_path)

    canonical, iso3 = resolve_country(country)
    target_date = _parse_time_period(time_period)
    start_date = target_date - pd.DateOffset(months=lookback_months - 1)
    end_date = target_date + pd.DateOffset(months=1) - pd.DateOffset(days=1)
    full_date_index = pd.date_range(start=start_date, end=target_date, freq="MS")

    valid_names, commodity_ids, missing = _resolve_commodities(canonical, iso3, commodities)
    if missing:
        logger.warning("Requested commodities not available in Databridges for %s: %s", canonical, missing)
    if not valid_names:
        raise ValueError(
            f"No requested commodities are available for {canonical}. "
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
            f"Databridges returned no monthly price rows for {canonical} from "
            f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}."
        )

    df = df[df["Commodity"].isin(valid_names)].copy()
    if df.empty:
        raise ValueError(f"No Databridges price rows matched the requested commodities: {valid_names}")

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
    selected_components = [commodity for commodity in commodities if commodity in df_national.columns]
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
                    "historical_component_count": len(historical_components),
                    "latest_component_count": len(latest_components),
                    "latest_component_names": latest_components,
                    "missing_latest_component_names": missing_latest_components,
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
) -> Dict[str, Any]:
    if csv_path is not None:
        logger.info("Ignoring csv_path=%s because Databridges is now the price source.", csv_path)

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

        warnings = []
        if missing:
            warnings.append(
                f"Requested commodities not available from Databridges for {canonical}: {missing}."
            )
        if data_gaps:
            warnings.append(f"Databridges price data has gaps in {len(data_gaps)} month(s): {data_gaps}.")
        if metadata.get("date_range") is None:
            warnings.append(f"Databridges returned no monthly price date range for {canonical}.")

        return {
            "available": True,
            "country_normalized": canonical,
            "iso3": iso3,
            "countries": get_available_countries(),
            "commodities": available_commodities,
            "regions": metadata.get("regions", []),
            "markets": [market.get("market_name") for market in metadata.get("markets", [])],
            "date_range": metadata.get("date_range"),
            "missing_commodities": missing,
            "data_gaps": data_gaps,
            "warnings": warnings,
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
        }


def get_data_summary(csv_path: Optional[Path] = None) -> Dict[str, Any]:
    if csv_path is not None:
        logger.info("Ignoring csv_path=%s because Databridges is now the price source.", csv_path)
    countries = get_supported_countries()
    return {
        "source": "Databridges",
        "countries": {str(item["name"]): {"iso3": item["iso3"]} for item in countries},
        "total_records": None,
        "date_range": None,
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

    client = get_databridges_client()
    raw_rows: list[dict[str, Any]] = []
    if ids:
        for commodity_id in ids:
            raw_rows.extend(
                client.list_monthly_prices(
                    iso3,
                    commodity_id=commodity_id,
                    start_date=start_date,
                    end_date=end_date,
                    latest_value_only=latest_value_only,
                )
            )
    else:
        raw_rows = client.list_monthly_prices(
            iso3,
            start_date=start_date,
            end_date=end_date,
            latest_value_only=latest_value_only,
        )

    df = _normalise_price_rows(raw_rows, canonical, iso3, _market_lookup(canonical, iso3))
    _cache_set(_PRICE_CACHE, cache_key, df)
    return df.copy()


def _normalise_price_rows(
    rows: list[dict[str, Any]],
    canonical: str,
    iso3: str,
    markets_by_id: dict[int, dict[str, Any]],
) -> pd.DataFrame:
    records = []
    for row in _sort_raw_price_rows(rows):
        flag = str(_field(row, "commodityPriceFlag", "commodity_price_flag", "priceFlag", default="") or "").strip()
        if flag and flag.lower() not in _ALLOWED_PRICE_FLAGS:
            continue

        price = pd.to_numeric(_field(row, "commodityPrice", "commodity_price", "price"), errors="coerce")
        price_date = pd.to_datetime(
            _field(row, "commodityPriceDate", "commodity_price_date", "priceDate", "date"),
            errors="coerce",
        )
        if pd.isna(price) or pd.isna(price_date):
            continue

        market_id = _to_int(_field(row, "marketId", "market_id", "marketID"))
        market = markets_by_id.get(market_id or -1, {})
        observations = _to_int(_field(row, "commodityPriceObservations", "commodity_price_observations", "observations"))
        source = str(_field(row, "commodityPriceSourceName", "commodity_price_source_name", "source", default="") or "")
        records.append(
            {
                "Country": str(_field(row, "countryName", "country_name", default=canonical) or canonical),
                "Country ISO3": str(_field(row, "countryIso3", "country_iso3", default=iso3) or iso3),
                "Commodity": str(_field(row, "commodityName", "commodity_name", default="Unknown") or "Unknown"),
                "Commodity ID": _to_int(_field(row, "commodityId", "commodity_id", "commodityID")),
                "Price Type": str(_field(row, "priceTypeName", "price_type_name", default="") or ""),
                "Price Date": price_date.to_period("M").to_timestamp(),
                "Price": float(price),
                "Admin 1": str(market.get("admin1_name") or _field(row, "admin1Name", "adm1Name", "adm1_name", default="Unknown") or "Unknown"),
                "Admin 2": str(market.get("admin2_name") or _field(row, "admin2Name", "adm2Name", "adm2_name", default="") or ""),
                "Market Name": str(_field(row, "marketName", "market_name", default=market.get("market_name", "Unknown")) or "Unknown"),
                "Market ID": market_id,
                "Unit": str(_field(row, "commodityUnitName", "commodity_unit_name", default="") or ""),
                "Currency": str(_field(row, "currencyName", "currency_name", default="") or ""),
                "Data Type": "Aggregated" if flag.lower() == "aggregate" else (flag.title() if flag else ""),
                "Price Flag": flag.lower(),
                "Observations": observations,
                "Data Source": source,
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

    rows = get_databridges_client().list_commodities(iso3)
    commodities = []
    for row in rows:
        commodity_id = _to_int(_field(row, "id", "commodityId", "commodity_id", "commodityID"))
        name = str(_field(row, "name", "commodityName", "commodity_name", default="") or "").strip()
        if not name:
            continue
        commodities.append(
            {
                "id": commodity_id,
                "name": name,
                "category_id": _to_int(_field(row, "categoryId", "category_id")),
                "country": canonical,
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

    rows = get_databridges_client().list_markets(iso3)
    markets = []
    for row in rows:
        market_id = _to_int(_field(row, "marketId", "market_id", "marketID"))
        market_name = str(_field(row, "marketName", "market_name", default="") or "").strip()
        if not market_name:
            continue
        markets.append(
            {
                "market_id": market_id,
                "market_name": market_name,
                "admin1_name": str(_field(row, "admin1Name", "admin1_name", default="") or ""),
                "admin1_code": _to_int(_field(row, "admin1Code", "admin1_code")),
                "admin2_name": str(_field(row, "admin2Name", "admin2_name", default="") or ""),
                "admin2_code": _to_int(_field(row, "admin2Code", "admin2_code")),
                "latitude": _to_float(_field(row, "marketLatitude", "market_latitude")),
                "longitude": _to_float(_field(row, "marketLongitude", "market_longitude")),
                "country": canonical,
                "iso3": iso3,
            }
        )
    markets = sorted(markets, key=lambda item: item["market_name"])
    _cache_set(_MARKET_CACHE, iso3, markets)
    return list(markets)


def _market_lookup(canonical: str, iso3: str) -> dict[int, dict[str, Any]]:
    return {
        int(market["market_id"]): market
        for market in _get_markets(canonical, iso3)
        if market.get("market_id") is not None
    }


def _sort_raw_price_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(rows, key=_raw_price_sort_key)


def _raw_price_sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        _sort_int(_field(row, "commodityId", "commodity_id", "commodityID")),
        _sort_text(_field(row, "commodityName", "commodity_name")),
        _sort_text(_field(row, "priceTypeName", "price_type_name")),
        _sort_text(_field(row, "currencyName", "currency_name")),
        _sort_text(_field(row, "commodityUnitName", "commodity_unit_name")),
        _sort_text(_field(row, "commodityPriceDate", "commodity_price_date", "priceDate", "date")),
        _sort_int(_field(row, "marketId", "market_id", "marketID")),
        _sort_text(_field(row, "marketName", "market_name")),
        _sort_text(_field(row, "commodityPriceFlag", "commodity_price_flag", "priceFlag")),
        _sort_float(_field(row, "commodityPrice", "commodity_price", "price")),
        _sort_int(_field(row, "commodityPriceObservations", "commodity_price_observations", "observations")),
        _sort_text(_field(row, "commodityPriceSourceName", "commodity_price_source_name", "source")),
    )


def _commodity_sort_key(item: dict[str, Any]) -> tuple[str, int]:
    commodity_id = _to_int(item.get("id"))
    return (str(item.get("name") or "").strip().lower(), commodity_id if commodity_id is not None else 10**12)


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
