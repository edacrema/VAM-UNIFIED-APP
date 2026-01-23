"""
Market Monitor - Data Loader
============================
Functions to load and process price data from CSV files.

This module replaces the mock data generation in graph.py with real
data loading from CSV files.

Features:
- Loads price data from CSV
- Normalizes country names
- Extracts time series with complete date index (no missing rows)
- Handles missing data gracefully (NaN values preserved)
- Calculates MoM and YoY statistics

Usage:
    from .data_loader import (
        extract_time_series_from_csv,
        calculate_statistics_from_csv,
        check_data_availability
    )
"""
from __future__ import annotations

import logging
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

# Path to the data directory (relative to this file's location)
DATA_DIR = Path(__file__).parent / "data"


# ============================================================================
# COUNTRY NAME MAPPING
# ============================================================================

# Maps various country name formats to the canonical form used in the CSV
COUNTRY_NAME_MAPPING = {
    # Syrian Arab Republic variations
    "syria": "Syrian Arab Republic",
    "syrian arab republic": "Syrian Arab Republic",
    "syr": "Syrian Arab Republic",
    "syrian": "Syrian Arab Republic",
    
    # South Sudan variations
    "south sudan": "South Sudan",
    "southsudan": "South Sudan",
    "ssd": "South Sudan",
    "s. sudan": "South Sudan",
    
    # Lebanon variations
    "lebanon": "Lebanon",
    "lbn": "Lebanon",
    
    # Additional country mappings (add as needed)
    "sudan": "Sudan",
    "yemen": "Yemen",
    "myanmar": "Myanmar",
    "burma": "Myanmar",
    "afghanistan": "Afghanistan",
    "ethiopia": "Ethiopia",
    "nigeria": "Nigeria",
    "pakistan": "Pakistan",
    "bangladesh": "Bangladesh",
    "kenya": "Kenya",
    "uganda": "Uganda",
    "tanzania": "Tanzania",
    "zambia": "Zambia",
    "malawi": "Malawi",
    "haiti": "Haiti",
    "democratic republic of congo": "Democratic Republic of Congo",
    "drc": "Democratic Republic of Congo",
    "congo": "Democratic Republic of Congo",
    "somalia": "Somalia",
}


def normalize_country_name(country: str) -> str:
    """
    Normalize country name to match CSV format.
    
    Args:
        country: Input country name (any format)
    
    Returns:
        Normalized country name matching the CSV
    
    Examples:
        >>> normalize_country_name("Syria")
        'Syrian Arab Republic'
        >>> normalize_country_name("South Sudan")
        'South Sudan'
    """
    # Try exact match first (already in canonical form)
    if country in COUNTRY_NAME_MAPPING.values():
        return country
    
    # Try case-insensitive lookup
    country_lower = country.lower().strip()
    if country_lower in COUNTRY_NAME_MAPPING:
        return COUNTRY_NAME_MAPPING[country_lower]
    
    # Return original if no mapping found
    logger.warning(f"No country mapping found for '{country}', using as-is")
    return country


# ============================================================================
# CSV DATA LOADER
# ============================================================================

def load_csv_price_data(csv_path: Optional[Path] = None) -> pd.DataFrame:
    """
    Load the price data CSV file.
    
    Args:
        csv_path: Optional path to CSV file. Defaults to data/price_data.csv
    
    Returns:
        DataFrame with price data, dates parsed
    
    Raises:
        FileNotFoundError: If CSV file doesn't exist
    """
    if csv_path is None:
        csv_path = DATA_DIR / "price_data.csv"
    
    # Convert string to Path if needed
    csv_path = Path(csv_path)
    
    if not csv_path.exists():
        raise FileNotFoundError(
            f"Price data file not found at {csv_path}. "
            f"Please ensure 'price_data.csv' exists in the 'data' folder."
        )
    
    logger.info(f"Loading price data from {csv_path}")
    
    df = pd.read_csv(csv_path)
    
    # Parse dates (format: DD/MM/YYYY)
    df['Price Date'] = pd.to_datetime(df['Price Date'], format='%d/%m/%Y', errors='coerce')
    
    # Ensure Price is numeric
    df['Price'] = pd.to_numeric(df['Price'], errors='coerce')
    
    # Remove rows with invalid dates or prices
    initial_len = len(df)
    df = df.dropna(subset=['Price Date', 'Price'])
    if len(df) < initial_len:
        logger.warning(f"Dropped {initial_len - len(df)} rows with invalid date/price")
    
    logger.info(f"Loaded {len(df)} price records")
    
    return df


def get_available_countries(df: pd.DataFrame) -> List[str]:
    """Get list of countries available in the dataset."""
    return sorted(df['Country'].unique().tolist())


def get_available_commodities(df: pd.DataFrame, country: str) -> List[str]:
    """Get list of commodities available for a specific country."""
    country_normalized = normalize_country_name(country)
    country_df = df[df['Country'] == country_normalized]
    return sorted(country_df['Commodity'].unique().tolist())


def get_available_regions(df: pd.DataFrame, country: str) -> List[str]:
    """Get list of Admin1 regions available for a specific country."""
    country_normalized = normalize_country_name(country)
    country_df = df[df['Country'] == country_normalized]
    return sorted(country_df['Admin 1'].unique().tolist())


def get_available_markets(df: pd.DataFrame, country: str) -> List[str]:
    """Get list of markets available for a specific country."""
    country_normalized = normalize_country_name(country)
    country_df = df[df['Country'] == country_normalized]
    return sorted(country_df['Market Name'].unique().tolist())


def get_all_commodities(df: pd.DataFrame) -> List[str]:
    """Get list of all unique commodities across all countries."""
    return sorted(df['Commodity'].unique().tolist())


def get_commodity_categories(df: pd.DataFrame) -> Dict[str, List[str]]:
    """
    Get commodities grouped by category (inferred from naming conventions).

    Returns:
        Dictionary mapping category names to lists of commodities
    """
    all_commodities = get_all_commodities(df)

    categories = {
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
        "Other": []
    }

    for commodity in all_commodities:
        commodity_lower = commodity.lower()

        if any(x in commodity_lower for x in ["sorghum", "maize", "wheat", "rice", "millet", "bread"]):
            categories["Cereals"].append(commodity)
        elif any(x in commodity_lower for x in ["beans", "lentil", "pea", "chickpea", "pulse"]):
            categories["Pulses"].append(commodity)
        elif "oil" in commodity_lower:
            categories["Oil"].append(commodity)
        elif "sugar" in commodity_lower:
            categories["Sugar"].append(commodity)
        elif "salt" in commodity_lower:
            categories["Condiments"].append(commodity)
        elif any(x in commodity_lower for x in ["cabbage", "tomato", "onion", "vegetable", "leaves", "sukuma", "pumpkin", "cassava"]):
            categories["Vegetables"].append(commodity)
        elif "livestock" in commodity_lower or any(x in commodity_lower for x in ["goat", "sheep", "cattle", "chicken"]):
            categories["Livestock"].append(commodity)
        elif "fuel" in commodity_lower or any(x in commodity_lower for x in ["petrol", "diesel", "gasoline"]):
            categories["Fuel"].append(commodity)
        elif "exchange" in commodity_lower:
            categories["Exchange Rate"].append(commodity)
        elif "milling" in commodity_lower:
            categories["Milling"].append(commodity)
        elif "wage" in commodity_lower or "labour" in commodity_lower:
            categories["Wage"].append(commodity)
        else:
            categories["Other"].append(commodity)

    return {k: v for k, v in categories.items() if v}


def get_date_range(df: pd.DataFrame, country: str) -> Tuple[datetime, datetime]:
    """Get the date range available for a specific country."""
    country_normalized = normalize_country_name(country)
    country_df = df[df['Country'] == country_normalized]
    return country_df['Price Date'].min(), country_df['Price Date'].max()


# ============================================================================
# TIME SERIES EXTRACTION
# ============================================================================

def extract_time_series_from_csv(
    country: str,
    time_period: str,
    commodities: List[str],
    admin1_list: List[str],
    csv_path: Optional[Path] = None,
    lookback_months: int = 13
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Extract time series data from the CSV file.
    
    This function replaces `generate_mock_time_series` in graph.py.
    It returns data in the same format expected by downstream nodes.
    
    IMPORTANT: The returned DataFrames have a COMPLETE date index covering
    all months in the lookback period. Months with no data will have NaN values.
    This ensures:
    - Correct MoM and YoY calculations (comparing the right months)
    - Visualizations show gaps where data is missing
    - No silent data alignment errors
    
    Args:
        country: Country name (will be normalized)
        time_period: Target period in YYYY-MM format (e.g., "2025-01")
        commodities: List of commodities to include
        admin1_list: List of Admin1 regions to include (empty = all regions)
        csv_path: Optional path to CSV file
        lookback_months: Number of months to include (default: 13 for YoY calculation)
    
    Returns:
        Tuple of (df_national, df_regional):
        - df_national: National-level aggregated time series (indexed by Date)
                      Columns: commodity names + FoodBasket + ExchangeRate + FuelPrice
                      Index: Complete monthly DatetimeIndex (13 months, no gaps)
        - df_regional: Regional-level time series
                      Columns: Date, Region, FoodBasket
    
    Raises:
        ValueError: If no data found for the specified parameters
        FileNotFoundError: If CSV file doesn't exist
    """
    # Load data
    df = load_csv_price_data(csv_path)
    
    # Normalize country name
    country_normalized = normalize_country_name(country)
    logger.info(f"[DataLoader] Extracting data for country: {country_normalized}")
    
    # Filter by country
    df_country = df[df['Country'] == country_normalized].copy()
    
    if df_country.empty:
        available = get_available_countries(df)
        raise ValueError(
            f"No data found for country '{country}' (normalized: '{country_normalized}'). "
            f"Available countries: {available}"
        )
    
    # Parse target period
    try:
        target_date = pd.to_datetime(time_period + "-01")
    except Exception:
        target_date = pd.to_datetime("2025-01-01")
        logger.warning(f"Invalid time_period '{time_period}', using default: 2025-01")
    
    # Calculate date range (lookback_months ending at target_date)
    start_date = target_date - pd.DateOffset(months=lookback_months - 1)
    end_date = target_date + pd.DateOffset(months=1) - pd.DateOffset(days=1)
    
    logger.info(f"[DataLoader] Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    
    # Create COMPLETE date range for the lookback period (no gaps)
    full_date_index = pd.date_range(start=start_date, end=target_date, freq='MS')
    logger.info(f"[DataLoader] Full date index: {len(full_date_index)} months")
    
    # Filter by date range
    df_filtered = df_country[
        (df_country['Price Date'] >= start_date) & 
        (df_country['Price Date'] <= end_date)
    ].copy()
    
    if df_filtered.empty:
        actual_range = get_date_range(df, country_normalized)
        raise ValueError(
            f"No data found for period {time_period} (range: {start_date} to {end_date}). "
            f"Available data range for {country_normalized}: "
            f"{actual_range[0].strftime('%Y-%m-%d')} to {actual_range[1].strftime('%Y-%m-%d')}"
        )
    
    # Filter by commodities (use available if requested not found)
    available_commodities = df_filtered['Commodity'].unique().tolist()
    valid_commodities = [c for c in commodities if c in available_commodities]
    
    if not valid_commodities:
        logger.warning(
            f"[DataLoader] None of the requested commodities {commodities} found. "
            f"Available: {available_commodities}. Using all available."
        )
        valid_commodities = available_commodities
    else:
        missing = set(commodities) - set(valid_commodities)
        if missing:
            logger.warning(f"[DataLoader] Commodities not found in data: {missing}")
    
    df_commodities = df_filtered[df_filtered['Commodity'].isin(valid_commodities)].copy()
    
    # Extract month for grouping (first day of month)
    df_commodities['Month'] = df_commodities['Price Date'].dt.to_period('M').dt.to_timestamp()
    
    # =========================================================================
    # NATIONAL AGGREGATION
    # =========================================================================
    
    # Aggregate prices at national level: mean price per commodity per month
    national_pivot = df_commodities.pivot_table(
        index='Month',
        columns='Commodity',
        values='Price',
        aggfunc='mean'
    )
    
    # CRITICAL: Reindex to complete date range to ensure no missing rows
    # Missing months will have NaN for all commodities
    national_pivot = national_pivot.reindex(full_date_index)
    
    # Round existing values
    national_pivot = national_pivot.round(2)
    
    # Calculate FoodBasket as sum of commodity prices (NaN if any commodity is NaN)
    # Use skipna=False to propagate NaN if any commodity is missing
    # Actually, let's use skipna=True so partial data still produces a food basket
    national_pivot['FoodBasket'] = national_pivot[valid_commodities].sum(axis=1, skipna=True).round(2)
    
    # Replace 0 with NaN for FoodBasket if ALL commodities are NaN for that month
    all_commodities_nan = national_pivot[valid_commodities].isna().all(axis=1)
    national_pivot.loc[all_commodities_nan, 'FoodBasket'] = np.nan
    
    # Add placeholder columns for auxiliary data
    # These would typically come from other sources (exchange rate module, etc.)
    national_pivot['ExchangeRate'] = np.nan
    national_pivot['FuelPrice'] = np.nan
    
    df_national = national_pivot.copy()
    df_national.index.name = 'Date'
    
    # Log data availability
    data_coverage = df_national[valid_commodities].notna().sum()
    logger.info(f"[DataLoader] Data coverage per commodity: {data_coverage.to_dict()}")
    
    missing_months = df_national[valid_commodities].isna().all(axis=1).sum()
    if missing_months > 0:
        logger.warning(f"[DataLoader] {missing_months} month(s) have NO data for any commodity")
    
    # =========================================================================
    # REGIONAL AGGREGATION
    # =========================================================================
    
    # Determine which regions to include
    available_regions = df_filtered['Admin 1'].unique().tolist()
    
    if admin1_list and len(admin1_list) > 0:
        valid_regions = [r for r in admin1_list if r in available_regions]
        if not valid_regions:
            logger.warning(
                f"[DataLoader] None of the requested regions {admin1_list} found. "
                f"Available: {available_regions}. Using all available."
            )
            valid_regions = available_regions
        else:
            missing = set(admin1_list) - set(valid_regions)
            if missing:
                logger.warning(f"[DataLoader] Regions not found in data: {missing}")
    else:
        valid_regions = available_regions
    
    # Filter for valid regions
    df_regional_data = df_commodities[df_commodities['Admin 1'].isin(valid_regions)].copy()
    
    # Regional aggregation: sum of mean commodity prices per region per month
    # This creates a "food basket" proxy at regional level
    regional_agg = df_regional_data.groupby(['Month', 'Admin 1']).agg({
        'Price': 'sum'
    }).reset_index()
    
    regional_agg.columns = ['Date', 'Region', 'FoodBasket']
    regional_agg['FoodBasket'] = regional_agg['FoodBasket'].round(2)
    
    # Create complete regional DataFrame with all month-region combinations
    # This ensures no missing rows in regional data
    regional_index = pd.MultiIndex.from_product(
        [full_date_index, valid_regions],
        names=['Date', 'Region']
    )
    df_regional_complete = pd.DataFrame(index=regional_index).reset_index()
    
    # Merge with actual data (left join to keep all month-region combinations)
    df_regional = df_regional_complete.merge(
        regional_agg,
        on=['Date', 'Region'],
        how='left'
    )
    
    # Sort by date and region
    df_regional = df_regional.sort_values(['Date', 'Region']).reset_index(drop=True)
    
    # =========================================================================
    # LOGGING & SUMMARY
    # =========================================================================
    
    logger.info(f"[DataLoader] National data: {len(df_national)} months, columns: {list(df_national.columns)}")
    logger.info(f"[DataLoader] Regional data: {len(df_regional)} records ({len(valid_regions)} regions × {len(full_date_index)} months)")
    logger.info(f"[DataLoader] Commodities included: {valid_commodities}")
    
    # Report on missing data
    national_missing = df_national[valid_commodities].isna().sum()
    if national_missing.any():
        logger.info(f"[DataLoader] Missing data points per commodity: {national_missing.to_dict()}")
    
    return df_national, df_regional


# ============================================================================
# STATISTICS CALCULATION
# ============================================================================

def calculate_statistics_from_csv(
    df_national: pd.DataFrame,
    commodities: List[str]
) -> Dict[str, Any]:
    """
    Calculate MoM and YoY statistics from the extracted data.
    
    This function is compatible with the original `calculate_statistics` in graph.py.
    It properly handles NaN values in the time series.
    
    Args:
        df_national: National-level time series DataFrame (from extract_time_series_from_csv)
        commodities: List of commodity names to include in stats
    
    Returns:
        Dictionary with structure:
        {
            "food_basket": {"current_price": X, "mom_change_pct": X, "yoy_change_pct": X},
            "commodities": {"Commodity1": {...}, "Commodity2": {...}},
            "auxiliary": {"ExchangeRate": {...}, "FuelPrice": {...}}
        }
        
        Values will be None or excluded if data is not available.
    """
    stats: Dict[str, Any] = {
        "food_basket": {},
        "commodities": {},
        "auxiliary": {}
    }
    
    if df_national.empty:
        logger.warning("[DataLoader] Empty DataFrame - cannot calculate statistics")
        return stats
    
    if len(df_national) < 2:
        logger.warning(f"[DataLoader] Only {len(df_national)} month(s) of data - limited statistics")
    
    # Get indices for current, previous month, and year-ago
    # Since we have a complete date index, positions are reliable:
    # - iloc[-1] = current month (target_date)
    # - iloc[-2] = previous month
    # - iloc[0]  = 12 months ago (if 13 months of data)
    
    current_idx = -1
    mom_idx = -2 if len(df_national) >= 2 else -1
    yoy_idx = 0  # First row = oldest month in the lookback
    
    current = df_national.iloc[current_idx]
    mom = df_national.iloc[mom_idx]
    yoy = df_national.iloc[yoy_idx]
    
    # Log the dates being compared
    logger.info(f"[DataLoader] Statistics comparing:")
    logger.info(f"  Current: {df_national.index[current_idx].strftime('%Y-%m')}")
    logger.info(f"  MoM vs:  {df_national.index[mom_idx].strftime('%Y-%m')}")
    logger.info(f"  YoY vs:  {df_national.index[yoy_idx].strftime('%Y-%m')}")
    
    # Calculate statistics for each column
    for col in df_national.columns:
        current_val = current[col]
        mom_val = mom[col]
        yoy_val = yoy[col]
        
        # Skip if current value is NaN
        if pd.isna(current_val):
            logger.debug(f"[DataLoader] Skipping {col}: current value is NaN")
            continue
        
        # Calculate MoM percentage change
        if pd.notna(mom_val) and mom_val != 0:
            mom_pct = round(((current_val - mom_val) / mom_val * 100), 1)
        else:
            mom_pct = None  # Cannot calculate
            logger.debug(f"[DataLoader] Cannot calculate MoM for {col}: previous month is NaN or zero")
        
        # Calculate YoY percentage change
        if pd.notna(yoy_val) and yoy_val != 0:
            yoy_pct = round(((current_val - yoy_val) / yoy_val * 100), 1)
        else:
            yoy_pct = None  # Cannot calculate
            logger.debug(f"[DataLoader] Cannot calculate YoY for {col}: year-ago value is NaN or zero")
        
        data = {
            "current_price": round(float(current_val), 2),
            "mom_change_pct": float(mom_pct) if mom_pct is not None else None,
            "yoy_change_pct": float(yoy_pct) if yoy_pct is not None else None
        }
        
        # Categorize the statistic
        if col == "FoodBasket":
            stats["food_basket"] = data
        else:
            # Auxiliary indicators (non-food items)
            auxiliary_patterns = ["exchange", "fuel", "wage", "milling"]
            is_auxiliary = any(pattern in col.lower() for pattern in auxiliary_patterns)

            if is_auxiliary:
                if pd.notna(current_val):
                    stats["auxiliary"][col] = data
            elif col != "FoodBasket":
                # This is a commodity column
                if pd.notna(current_val):
                    stats["commodities"][col] = data
    
    return stats


# ============================================================================
# DATA AVAILABILITY CHECK
# ============================================================================

def check_data_availability(
    country: str,
    time_period: str,
    commodities: List[str],
    csv_path: Optional[Path] = None
) -> Dict[str, Any]:
    """
    Check what data is available before running the full extraction.
    
    Useful for validation, error handling, and informing users about
    available data including any gaps.
    
    Args:
        country: Country name
        time_period: Target period in YYYY-MM format
        commodities: Requested commodities
        csv_path: Optional path to CSV file
    
    Returns:
        Dictionary with availability information:
        {
            "available": bool,
            "country_normalized": str,
            "countries": [list of available countries],
            "commodities": [list of available commodities for country],
            "regions": [list of available regions for country],
            "markets": [list of available markets for country],
            "date_range": {"start": "YYYY-MM-DD", "end": "YYYY-MM-DD"},
            "missing_commodities": [requested but not available],
            "data_gaps": [list of months with no data],
            "warnings": [list of warning messages]
        }
    """
    try:
        df = load_csv_price_data(csv_path)
    except FileNotFoundError as e:
        return {
            "available": False,
            "error": str(e),
            "country_normalized": None,
            "countries": [],
            "commodities": [],
            "regions": [],
            "markets": [],
            "date_range": None,
            "missing_commodities": commodities,
            "data_gaps": [],
            "warnings": [str(e)]
        }
    
    country_normalized = normalize_country_name(country)
    
    result: Dict[str, Any] = {
        "available": country_normalized in df['Country'].values,
        "country_normalized": country_normalized,
        "countries": get_available_countries(df),
        "commodities": [],
        "regions": [],
        "markets": [],
        "date_range": None,
        "missing_commodities": [],
        "data_gaps": [],
        "warnings": []
    }
    
    if result["available"]:
        result["commodities"] = get_available_commodities(df, country_normalized)
        result["regions"] = get_available_regions(df, country_normalized)
        result["markets"] = get_available_markets(df, country_normalized)
        
        date_range = get_date_range(df, country_normalized)
        result["date_range"] = {
            "start": date_range[0].strftime("%Y-%m-%d"),
            "end": date_range[1].strftime("%Y-%m-%d")
        }
        
        # Check which requested commodities are missing
        result["missing_commodities"] = [
            c for c in commodities if c not in result["commodities"]
        ]
        
        if result["missing_commodities"]:
            result["warnings"].append(
                f"Requested commodities not available: {result['missing_commodities']}. "
                f"Available: {result['commodities']}"
            )
        
        # Check for data gaps in the requested period
        try:
            target_date = pd.to_datetime(time_period + "-01")
            start_date = target_date - pd.DateOffset(months=12)
            
            # Check if time_period is within available range
            if target_date < date_range[0] or target_date > date_range[1]:
                result["warnings"].append(
                    f"Requested period {time_period} is outside available range "
                    f"({result['date_range']['start']} to {result['date_range']['end']})"
                )
            
            # Find gaps in the data
            df_country = df[df['Country'] == country_normalized].copy()
            df_country['Month'] = df_country['Price Date'].dt.to_period('M').dt.to_timestamp()
            
            # Filter to lookback period
            df_period = df_country[
                (df_country['Month'] >= start_date) & 
                (df_country['Month'] <= target_date)
            ]
            
            if not df_period.empty:
                # Generate expected months
                expected_months = pd.date_range(start=start_date, end=target_date, freq='MS')
                actual_months = set(df_period['Month'].unique())
                
                # Find missing months
                missing_months = [
                    m.strftime("%Y-%m") for m in expected_months 
                    if m not in actual_months
                ]
                
                if missing_months:
                    result["data_gaps"] = missing_months
                    result["warnings"].append(
                        f"Data gaps detected in {len(missing_months)} month(s): {missing_months}"
                    )
                    
        except Exception as e:
            result["warnings"].append(f"Could not check for data gaps: {str(e)}")
    else:
        result["warnings"].append(
            f"Country '{country}' (normalized: '{country_normalized}') not found. "
            f"Available: {result['countries']}"
        )
    
    return result


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

def get_data_summary(csv_path: Optional[Path] = None) -> Dict[str, Any]:
    """
    Get a summary of all data in the CSV file.
    
    Returns:
        Dictionary with overall data summary
    """
    try:
        df = load_csv_price_data(csv_path)
    except FileNotFoundError as e:
        return {"error": str(e)}
    
    summary = {
        "total_records": len(df),
        "countries": {},
        "date_range": {
            "start": df['Price Date'].min().strftime("%Y-%m-%d"),
            "end": df['Price Date'].max().strftime("%Y-%m-%d")
        }
    }
    
    for country in get_available_countries(df):
        country_df = df[df['Country'] == country]
        date_range = get_date_range(df, country)
        
        # Count months with data
        months_with_data = country_df['Price Date'].dt.to_period('M').nunique()
        
        summary["countries"][country] = {
            "records": len(country_df),
            "commodities": get_available_commodities(df, country),
            "regions": len(get_available_regions(df, country)),
            "markets": len(get_available_markets(df, country)),
            "months_with_data": months_with_data,
            "date_range": {
                "start": date_range[0].strftime("%Y-%m-%d"),
                "end": date_range[1].strftime("%Y-%m-%d")
            }
        }
    
    return summary