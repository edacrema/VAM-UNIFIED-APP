"""
MFI Drafter - Data Loader
=========================
Functions to load MFI data from CSV files.

This module transforms processed MFI CSV data into the format expected
by the MFI Drafter agent, replacing the need for mock data.
"""
from __future__ import annotations

import io
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, BinaryIO, Union
from datetime import datetime
import logging

from .schemas import (
    MFI_DIMENSIONS,
    DIMENSION_NAME_MAP,
    SCORE_VARIABLE_MAP,
    get_risk_level
)

logger = logging.getLogger(__name__)


def load_mfi_from_csv(
    file_content: Union[BinaryIO, bytes, str],
    country_override: Optional[str] = None,
    start_date_override: Optional[str] = None,
    end_date_override: Optional[str] = None
) -> Dict[str, Any]:
    """
    Load processed MFI data from CSV and transform to agent format.
    
    Args:
        file_content: CSV file content (file-like object, bytes, or path string)
        country_override: Override country name from CSV
        start_date_override: Override start date (YYYY-MM-DD format)
        end_date_override: Override end date (YYYY-MM-DD format)
        
    Returns:
        Dictionary with keys:
            - markets_data: List of market data dictionaries
            - dimension_scores: List of dimension aggregation dictionaries
            - survey_metadata: Survey metadata dictionary
            - country: Country name
            - data_collection_start: Start date string
            - data_collection_end: End date string
            - markets: List of market names
        
    Raises:
        ValueError: If CSV format is invalid or missing required columns
    """
    logger.info("Loading MFI data from CSV")
    
    # Load CSV
    if isinstance(file_content, str):
        df = pd.read_csv(file_content)
    elif isinstance(file_content, bytes):
        df = pd.read_csv(io.BytesIO(file_content))
    else:
        df = pd.read_csv(file_content)
    
    logger.info(f"CSV loaded: {len(df)} rows, {len(df.columns)} columns")
    
    # Validate required columns
    required_cols = {
        'MarketName', 'Adm0Name', 'Adm1Name', 'LevelID', 
        'DimensionName', 'VariableName', 'OutputValue', 'TradersSampleSize'
    }
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"CSV missing required columns: {missing}")
    
    # ==================== EXTRACT METADATA ====================
    country = country_override or df['Adm0Name'].iloc[0]
    regions = df['Adm1Name'].unique().tolist()
    markets = df['MarketName'].unique().tolist()
    
    logger.info(f"Country: {country}, Markets: {len(markets)}, Regions: {len(regions)}")
    
    # Parse dates
    if start_date_override:
        start_date_str = start_date_override
    elif 'StartDate' in df.columns:
        try:
            start_date_str = pd.to_datetime(df['StartDate'].iloc[0]).strftime("%Y-%m-%d")
        except Exception:
            start_date_str = "Unknown"
    else:
        start_date_str = "Unknown"
    
    if end_date_override:
        end_date_str = end_date_override
    elif 'EndDate' in df.columns:
        try:
            end_date_str = pd.to_datetime(df['EndDate'].iloc[0]).strftime("%Y-%m-%d")
        except Exception:
            end_date_str = "Unknown"
    else:
        end_date_str = "Unknown"
    
    collection_period = f"{start_date_str} to {end_date_str}"
    
    # ==================== EXTRACT NORMALIZED SCORES ====================
    # Filter to LevelID=1 (Normalized Score)
    normalized = df[df['LevelID'] == 1].copy()
    
    if normalized.empty:
        raise ValueError("No normalized scores found (LevelID=1). Check CSV data.")
    
    # Strip whitespace from VariableName
    normalized['VariableName'] = normalized['VariableName'].str.strip()
    
    # Filter to our target variable names
    score_rows = []
    for dim, var_name in SCORE_VARIABLE_MAP.items():
        dim_rows = normalized[
            (normalized['DimensionName'] == dim) & 
            (normalized['VariableName'] == var_name)
        ]
        if not dim_rows.empty:
            score_rows.append(dim_rows)
    
    if not score_rows:
        raise ValueError("No matching score variables found. Check VariableName values in CSV.")
    
    scores_df = pd.concat(score_rows, ignore_index=True)
    
    # ==================== BUILD MARKETS DATA ====================
    markets_data = []
    
    for market in markets:
        market_scores = scores_df[scores_df['MarketName'] == market]
        
        if market_scores.empty:
            logger.warning(f"No normalized scores found for market: {market}")
            continue
        
        # Get admin info
        admin0 = market_scores['Adm0Name'].iloc[0]
        admin1 = market_scores['Adm1Name'].iloc[0]
        admin2 = market_scores['Adm2Name'].iloc[0] if 'Adm2Name' in market_scores.columns else admin1
        region = admin1
        
        # Get coordinates if available
        lat = None
        lon = None
        if 'MarketLatitude' in market_scores.columns:
            try:
                lat = float(market_scores['MarketLatitude'].iloc[0])
            except (ValueError, TypeError):
                pass
        if 'MarketLongitude' in market_scores.columns:
            try:
                lon = float(market_scores['MarketLongitude'].iloc[0])
            except (ValueError, TypeError):
                pass
        
        # Get traders surveyed
        try:
            traders = int(market_scores['TradersSampleSize'].iloc[0])
        except (ValueError, TypeError):
            traders = 0
        
        # Build dimension_scores dict
        dimension_scores = {}
        for _, row in market_scores.iterrows():
            csv_dim = row['DimensionName']
            if csv_dim == 'MFI':
                continue  # Handle separately
            agent_dim = DIMENSION_NAME_MAP.get(csv_dim, csv_dim)
            try:
                dimension_scores[agent_dim] = round(float(row['OutputValue']), 1)
            except (ValueError, TypeError):
                dimension_scores[agent_dim] = 0.0
        
        # Get overall MFI
        mfi_row = market_scores[market_scores['DimensionName'] == 'MFI']
        if not mfi_row.empty:
            try:
                overall_mfi = round(float(mfi_row['OutputValue'].iloc[0]), 2)
            except (ValueError, TypeError):
                overall_mfi = 0.0
        else:
            # Calculate from dimension scores if not present
            overall_mfi = round(np.mean(list(dimension_scores.values())), 2) if dimension_scores else 0.0
        
        # Calculate risk level
        risk_level = get_risk_level(overall_mfi)
        
        # Extract sub-scores
        sub_scores = _extract_sub_scores(df, market)
        
        markets_data.append({
            "market_name": market,
            "admin0": admin0,
            "admin1": admin1,
            "admin2": admin2,
            "region": region,
            "overall_mfi": overall_mfi,
            "dimension_scores": dimension_scores,
            "sub_scores": sub_scores,
            "risk_level": risk_level,
            "traders_surveyed": traders,
            "latitude": lat,
            "longitude": lon
        })
    
    if not markets_data:
        raise ValueError("No valid market data found in CSV. Check LevelID=1 and VariableName values.")
    
    logger.info(f"Processed {len(markets_data)} markets successfully")
    
    # ==================== BUILD DIMENSION AGGREGATIONS ====================
    dimension_aggregations = []
    
    for csv_dim, agent_dim in DIMENSION_NAME_MAP.items():
        # National score (mean of all markets)
        market_scores_list = [
            m['dimension_scores'].get(agent_dim, 0) 
            for m in markets_data 
            if agent_dim in m['dimension_scores']
        ]
        national_score = round(np.mean(market_scores_list), 1) if market_scores_list else 0.0
        
        # Regional scores
        regional_scores = {}
        for region in regions:
            region_markets = [m for m in markets_data if m['region'] == region]
            region_scores = [
                m['dimension_scores'].get(agent_dim, 0) 
                for m in region_markets 
                if agent_dim in m['dimension_scores']
            ]
            if region_scores:
                regional_scores[region] = round(np.mean(region_scores), 1)
        
        # Market scores
        market_scores_dict = {
            m['market_name']: m['dimension_scores'].get(agent_dim, 0)
            for m in markets_data
            if agent_dim in m['dimension_scores']
        }
        
        dimension_aggregations.append({
            "dimension": agent_dim,
            "national_score": national_score,
            "regional_scores": regional_scores,
            "market_scores": market_scores_dict
        })
    
    # ==================== BUILD SURVEY METADATA ====================
    survey_metadata = {
        "country": country,
        "collection_period": collection_period,
        "total_traders": sum(m['traders_surveyed'] for m in markets_data),
        "total_markets": len(markets_data),
        "regions_covered": regions
    }
    
    return {
        "markets_data": markets_data,
        "dimension_scores": dimension_aggregations,
        "survey_metadata": survey_metadata,
        # Also return extracted values for state initialization
        "country": country,
        "data_collection_start": start_date_str,
        "data_collection_end": end_date_str,
        "markets": [m['market_name'] for m in markets_data]
    }


def _extract_sub_scores(df: pd.DataFrame, market: str) -> Dict[str, Dict[str, Any]]:
    """
    Extract sub-scores for a market from Trader Mean/Median levels.
    
    Args:
        df: Full DataFrame with all MFI data
        market: Market name to extract sub-scores for
        
    Returns:
        Dictionary mapping dimension names to sub-score dictionaries
    """
    market_df = df[df['MarketName'] == market]
    sub_scores = {}
    
    # Trader Mean (LevelID=5) and Market Mean (LevelID=6)
    trader_mean = market_df[market_df['LevelID'] == 5].copy()
    market_mean = market_df[market_df['LevelID'] == 6].copy()
    
    # Strip variable names
    if not trader_mean.empty:
        trader_mean['VariableName'] = trader_mean['VariableName'].str.strip()
    if not market_mean.empty:
        market_mean['VariableName'] = market_mean['VariableName'].str.strip()
    
    # Helper functions
    def safe_mean(series):
        if series.empty:
            return 0.0
        try:
            return float(series.mean())
        except (ValueError, TypeError):
            return 0.0
    
    def safe_first(series):
        if series.empty:
            return 0.0
        try:
            return float(series.iloc[0])
        except (ValueError, TypeError):
            return 0.0
    
    # Availability sub-scores
    avail_scarcity = trader_mean[
        trader_mean['VariableName'].str.contains('AvailabilityScarcity_FCer', na=False)
    ]['OutputValue']
    avail_runout = trader_mean[
        trader_mean['VariableName'].str.contains('AvailabilityRunout_FCer', na=False)
    ]['OutputValue']
    sub_scores["Availability"] = {
        "scarce_cereals_pct": round(max(0, 1 - safe_first(avail_scarcity)), 2),
        "runout_cereals_pct": round(max(0, 1 - safe_first(avail_runout)), 2)
    }
    
    # Price sub-scores
    price_increase = trader_mean[
        trader_mean['VariableName'].str.contains('PriceIncrease_FCer', na=False)
    ]['OutputValue']
    price_stability = trader_mean[
        trader_mean['VariableName'].str.contains('PriceStability', na=False)
    ]['OutputValue']
    sub_scores["Price"] = {
        "increase_cereals_pct": round(max(0, 1 - safe_first(price_increase)), 2),
        "unstable_cereals_pct": round(max(0, 1 - safe_mean(price_stability)), 2)
    }
    
    # Resilience sub-scores
    density = trader_mean[
        trader_mean['VariableName'].str.contains('VulnerabilityDensity', na=False)
    ]['OutputValue']
    complexity = trader_mean[
        trader_mean['VariableName'].str.contains('VulnerabilityComplexity', na=False)
    ]['OutputValue']
    criticality = trader_mean[
        trader_mean['VariableName'].str.contains('VulnerabilityCriticality', na=False)
    ]['OutputValue']
    sub_scores["Resilience"] = {
        "node_density": int(safe_mean(density) > 0.5),
        "node_complexity": int(safe_mean(complexity) > 0.5),
        "node_criticality": int(safe_mean(criticality) > 0.5)
    }
    
    # Competition sub-scores
    concentration = market_mean[
        market_mean['VariableName'].str.contains('CompetitionConcentration', na=False)
    ]['OutputValue']
    monopoly = market_mean[
        market_mean['VariableName'].str.contains('CompetitionMonopoly', na=False)
    ]['OutputValue']
    sub_scores["Competition"] = {
        "less_than_five_competitors": int(safe_first(concentration) < 3),
        "monopoly_risk": int(safe_first(monopoly) > 3)
    }
    
    # Infrastructure sub-scores
    cond_good = trader_mean[trader_mean['VariableName'] == 'InfrastructureConditionGood']['OutputValue']
    cond_med = trader_mean[trader_mean['VariableName'] == 'InfrastructureConditionMedium']['OutputValue']
    cond_poor = trader_mean[trader_mean['VariableName'] == 'InfrastructureConditionPoor']['OutputValue']
    sub_scores["Infrastructure"] = {
        "condition_good": int(safe_first(cond_good) > 0.5),
        "condition_medium": int(safe_first(cond_med) > 0.5),
        "condition_poor": int(safe_first(cond_poor) > 0.5)
    }
    
    # Service sub-scores
    checkout = trader_mean[
        trader_mean['VariableName'].str.contains('ServiceCheckout', na=False)
    ]['OutputValue']
    shopping = trader_mean[
        trader_mean['VariableName'].str.contains('ServiceShopping', na=False)
    ]['OutputValue']
    sub_scores["Service"] = {
        "checkout_score": round(safe_mean(checkout) * 10, 1),
        "shopping_experience_score": round(safe_mean(shopping) * 10, 1)
    }
    
    # Food Quality sub-scores
    # Note: Variables are named Quality* (e.g., QualityPrepackaged, QualitySeparate)
    # NOT QualityFeatures - we filter by dimension + prefix to get all quality indicators
    quality_vars = trader_mean[
        (trader_mean['DimensionName'] == 'Quality') & 
        (trader_mean['VariableName'].str.startswith('Quality'))
    ]['OutputValue']
    sub_scores["Food Quality"] = {
        "quality_features_pct": round(safe_mean(quality_vars), 2) if not quality_vars.empty else 0.75
    }
    
    # Access & Protection sub-scores
    access = market_mean[
        market_mean['VariableName'].str.contains('AccessProtectionAccess', na=False)
    ]['OutputValue']
    protection = market_mean[
        market_mean['VariableName'].str.contains('AccessProtectionProtection', na=False)
    ]['OutputValue']
    sub_scores["Access & Protection"] = {
        "access_issues_pct": round(max(0, 1 - safe_mean(access)), 2),
        "protection_issues_pct": round(max(0, 1 - safe_mean(protection)), 2)
    }
    
    # Assortment sub-scores
    assort_vars = trader_mean[trader_mean['DimensionName'] == 'Assortment']['OutputValue']
    assort_score = safe_mean(assort_vars) * 10 if not assort_vars.empty else 7.5
    sub_scores["Assortment"] = {
        "breadth": round(assort_score, 1),
        "depth": round(assort_score, 1)
    }
    
    return sub_scores


def validate_csv_structure(file_content: Union[BinaryIO, bytes]) -> Dict[str, Any]:
    """
    Validate CSV structure before full processing.
    
    Args:
        file_content: CSV file content (file-like object or bytes)
        
    Returns:
        Dictionary with validation results:
            - valid: Boolean indicating if CSV is valid
            - missing_columns: List of missing required columns
            - has_normalized_scores: Boolean if LevelID=1 exists
            - preview: Preview data (country, counts, columns)
            - errors: List of error messages
    """
    try:
        if isinstance(file_content, bytes):
            df = pd.read_csv(io.BytesIO(file_content), nrows=1000)
        else:
            # Reset file pointer if possible
            if hasattr(file_content, 'seek'):
                file_content.seek(0)
            df = pd.read_csv(file_content, nrows=1000)
    except Exception as e:
        return {
            "valid": False,
            "missing_columns": [],
            "has_normalized_scores": False,
            "preview": {},
            "errors": [f"Failed to read CSV: {str(e)}"]
        }
    
    required_cols = {
        'MarketName', 'Adm0Name', 'Adm1Name', 'LevelID', 
        'DimensionName', 'VariableName', 'OutputValue', 'TradersSampleSize'
    }
    
    present_cols = set(df.columns)
    missing_cols = required_cols - present_cols
    
    # Check for normalized scores
    has_normalized = False
    if 'LevelID' in df.columns:
        try:
            has_normalized = 1 in df['LevelID'].values
        except Exception:
            pass
    
    # Extract preview info
    errors = []
    preview = {"columns": list(df.columns)}
    
    if 'Adm0Name' in df.columns:
        preview["country"] = df['Adm0Name'].iloc[0]
    else:
        preview["country"] = "Unknown"
    
    if 'MarketName' in df.columns:
        preview["markets_count"] = df['MarketName'].nunique()
        preview["markets_sample"] = df['MarketName'].unique()[:5].tolist()
    else:
        preview["markets_count"] = 0
    
    if 'Adm1Name' in df.columns:
        preview["regions_count"] = df['Adm1Name'].nunique()
        preview["regions"] = df['Adm1Name'].unique().tolist()
    else:
        preview["regions_count"] = 0
    
    if 'DimensionName' in df.columns:
        preview["dimensions"] = df['DimensionName'].unique().tolist()
    
    # Build error messages
    if missing_cols:
        errors.append(f"Missing required columns: {', '.join(sorted(missing_cols))}")
    
    if not has_normalized:
        errors.append("No normalized scores found (LevelID=1 is required)")
    
    return {
        "valid": len(missing_cols) == 0 and has_normalized,
        "missing_columns": list(missing_cols),
        "has_normalized_scores": has_normalized,
        "preview": preview,
        "errors": errors
    }