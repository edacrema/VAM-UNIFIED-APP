"""MFI data loading and normalisation helpers."""
from __future__ import annotations

import io
import logging
from typing import Any, BinaryIO, Dict, List, Optional, Union

import numpy as np
import pandas as pd

from .schemas import DIMENSION_NAME_MAP, SCORE_VARIABLE_MAP, get_risk_level

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = {
    "MarketName",
    "Adm0Name",
    "Adm1Name",
    "LevelID",
    "DimensionName",
    "VariableName",
    "OutputValue",
    "TradersSampleSize",
}

DATABRIDGES_COLUMN_MAP = {
    "marketName": "MarketName",
    "market_name": "MarketName",
    "adm0Name": "Adm0Name",
    "adm0_name": "Adm0Name",
    "adm1Name": "Adm1Name",
    "adm1_name": "Adm1Name",
    "adm2Name": "Adm2Name",
    "adm2_name": "Adm2Name",
    "levelID": "LevelID",
    "levelId": "LevelID",
    "level_id": "LevelID",
    "dimensionName": "DimensionName",
    "dimension_name": "DimensionName",
    "variableName": "VariableName",
    "variable_name": "VariableName",
    "outputValue": "OutputValue",
    "output_value": "OutputValue",
    "tradersSampleSize": "TradersSampleSize",
    "traders_sample_size": "TradersSampleSize",
    "startDate": "StartDate",
    "start_date": "StartDate",
    "endDate": "EndDate",
    "end_date": "EndDate",
    "marketLatitude": "MarketLatitude",
    "market_latitude": "MarketLatitude",
    "marketLongitude": "MarketLongitude",
    "market_longitude": "MarketLongitude",
}


def load_mfi_from_csv(
    file_content: Union[BinaryIO, bytes, str],
    country_override: Optional[str] = None,
    start_date_override: Optional[str] = None,
    end_date_override: Optional[str] = None,
) -> Dict[str, Any]:
    """Load processed MFI CSV data and transform it to the report graph shape."""
    if isinstance(file_content, str):
        df = pd.read_csv(file_content)
    elif isinstance(file_content, bytes):
        df = pd.read_csv(io.BytesIO(file_content))
    else:
        df = pd.read_csv(file_content)

    return load_mfi_from_dataframe(
        df,
        country_override=country_override,
        start_date_override=start_date_override,
        end_date_override=end_date_override,
    )


def load_mfi_from_databridges_rows(
    rows: List[Dict[str, Any]],
    survey: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Normalise Databridges MFI processed rows to the existing MFI graph shape."""
    if not rows:
        raise ValueError("Databridges returned no processed MFI rows for the selected survey.")

    normalised_rows = []
    for row in rows:
        mapped: Dict[str, Any] = {}
        for source, target in DATABRIDGES_COLUMN_MAP.items():
            value = _field(row, source)
            if value is not None:
                mapped[target] = value
        normalised_rows.append(mapped)

    df = pd.DataFrame(normalised_rows)
    survey = survey or {}
    country = _field(survey, "countryName", "country_name") or (df["Adm0Name"].iloc[0] if "Adm0Name" in df.columns else None)
    start_date = _field(survey, "surveyStartDate", "survey_start_date")
    end_date = _field(survey, "surveyEndDate", "survey_end_date")

    result = load_mfi_from_dataframe(
        df,
        country_override=str(country) if country else None,
        start_date_override=_format_date(start_date) if start_date else None,
        end_date_override=_format_date(end_date) if end_date else None,
    )
    result["survey_metadata"]["survey_id"] = _field(survey, "surveyID", "survey_id")
    result["survey_metadata"]["survey_name"] = (
        _field(survey, "surveyName", "survey_name")
        or _field(survey, "surveyOriginalFilename", "survey_original_filename")
    )
    return result


def load_mfi_from_dataframe(
    df: pd.DataFrame,
    country_override: Optional[str] = None,
    start_date_override: Optional[str] = None,
    end_date_override: Optional[str] = None,
) -> Dict[str, Any]:
    """Transform processed MFI rows into the format expected by the MFI graph."""
    df = df.copy()
    df = _standardise_columns(df)
    _validate_required_columns(df)

    df["LevelID"] = pd.to_numeric(df["LevelID"], errors="coerce")
    df["OutputValue"] = pd.to_numeric(df["OutputValue"], errors="coerce")
    df["TradersSampleSize"] = pd.to_numeric(df["TradersSampleSize"], errors="coerce").fillna(0)
    for column in ["MarketName", "Adm0Name", "Adm1Name", "DimensionName", "VariableName"]:
        df[column] = df[column].astype(str).str.strip()
    if "Adm2Name" not in df.columns:
        df["Adm2Name"] = df["Adm1Name"]

    country = country_override or str(df["Adm0Name"].iloc[0])
    regions = sorted(df["Adm1Name"].dropna().astype(str).unique().tolist())
    markets = sorted(df["MarketName"].dropna().astype(str).unique().tolist())
    start_date_str = start_date_override or _first_date(df, "StartDate")
    end_date_str = end_date_override or _first_date(df, "EndDate")
    collection_period = f"{start_date_str} to {end_date_str}"

    normalised = df[df["LevelID"] == 1].copy()
    if normalised.empty:
        raise ValueError("No normalized scores found (LevelID=1). Check MFI data.")
    normalised["VariableName"] = normalised["VariableName"].astype(str).str.strip()

    score_frames = []
    for dimension, variable_name in SCORE_VARIABLE_MAP.items():
        rows = normalised[
            (normalised["DimensionName"] == dimension)
            & (normalised["VariableName"] == variable_name)
        ]
        if not rows.empty:
            score_frames.append(rows)
    if not score_frames:
        raise ValueError("No matching score variables found. Check DimensionName and VariableName values.")
    scores_df = pd.concat(score_frames, ignore_index=True)

    markets_data = []
    for market in markets:
        market_scores = scores_df[scores_df["MarketName"] == market]
        if market_scores.empty:
            logger.warning("No normalized scores found for market: %s", market)
            continue

        admin0 = str(market_scores["Adm0Name"].iloc[0])
        admin1 = str(market_scores["Adm1Name"].iloc[0])
        admin2 = str(market_scores["Adm2Name"].iloc[0]) if "Adm2Name" in market_scores.columns else admin1
        traders = int(_safe_float(market_scores["TradersSampleSize"].iloc[0], default=0.0) or 0)
        lat = _safe_float(market_scores["MarketLatitude"].iloc[0]) if "MarketLatitude" in market_scores.columns else None
        lon = _safe_float(market_scores["MarketLongitude"].iloc[0]) if "MarketLongitude" in market_scores.columns else None

        dimension_scores = {}
        for _, row in market_scores.iterrows():
            csv_dim = row["DimensionName"]
            if csv_dim == "MFI":
                continue
            agent_dim = DIMENSION_NAME_MAP.get(csv_dim, csv_dim)
            dimension_scores[agent_dim] = round(_safe_float(row["OutputValue"], default=0.0), 1)

        mfi_row = market_scores[market_scores["DimensionName"] == "MFI"]
        if not mfi_row.empty:
            overall_mfi = round(_safe_float(mfi_row["OutputValue"].iloc[0], default=0.0), 2)
        else:
            overall_mfi = round(float(np.mean(list(dimension_scores.values()))), 2) if dimension_scores else 0.0

        markets_data.append(
            {
                "market_name": market,
                "admin0": admin0,
                "admin1": admin1,
                "admin2": admin2,
                "region": admin1,
                "overall_mfi": overall_mfi,
                "dimension_scores": dimension_scores,
                "sub_scores": _extract_sub_scores(df, market),
                "risk_level": get_risk_level(overall_mfi),
                "traders_surveyed": traders,
                "latitude": lat,
                "longitude": lon,
            }
        )

    if not markets_data:
        raise ValueError("No valid market data found. Check LevelID=1 and score variables.")

    dimension_aggregations = []
    for _csv_dim, agent_dim in DIMENSION_NAME_MAP.items():
        national_values = [
            market["dimension_scores"].get(agent_dim)
            for market in markets_data
            if agent_dim in market["dimension_scores"]
        ]
        national_score = round(float(np.mean(national_values)), 1) if national_values else 0.0
        regional_scores = {}
        for region in regions:
            region_values = [
                market["dimension_scores"].get(agent_dim)
                for market in markets_data
                if market["region"] == region and agent_dim in market["dimension_scores"]
            ]
            if region_values:
                regional_scores[region] = round(float(np.mean(region_values)), 1)
        market_scores = {
            market["market_name"]: market["dimension_scores"].get(agent_dim, 0)
            for market in markets_data
            if agent_dim in market["dimension_scores"]
        }
        dimension_aggregations.append(
            {
                "dimension": agent_dim,
                "national_score": national_score,
                "regional_scores": regional_scores,
                "market_scores": market_scores,
            }
        )

    survey_metadata = {
        "country": country,
        "collection_period": collection_period,
        "total_traders": sum(market["traders_surveyed"] for market in markets_data),
        "total_markets": len(markets_data),
        "regions_covered": regions,
    }
    return {
        "markets_data": markets_data,
        "dimension_scores": dimension_aggregations,
        "survey_metadata": survey_metadata,
        "country": country,
        "data_collection_start": start_date_str,
        "data_collection_end": end_date_str,
        "markets": [market["market_name"] for market in markets_data],
    }


def validate_csv_structure(file_content: Union[BinaryIO, bytes]) -> Dict[str, Any]:
    """Validate processed MFI CSV shape. Kept for internal/backward compatibility."""
    try:
        if isinstance(file_content, bytes):
            df = pd.read_csv(io.BytesIO(file_content), nrows=1000)
        else:
            if hasattr(file_content, "seek"):
                file_content.seek(0)
            df = pd.read_csv(file_content, nrows=1000)
        df = _standardise_columns(df)
    except Exception as exc:
        return {
            "valid": False,
            "missing_columns": [],
            "has_normalized_scores": False,
            "preview": {},
            "errors": [f"Failed to read CSV: {str(exc)}"],
        }

    present = set(df.columns)
    missing = sorted(REQUIRED_COLUMNS - present)
    has_normalized = False
    if "LevelID" in df.columns:
        levels = pd.to_numeric(df["LevelID"], errors="coerce")
        has_normalized = bool((levels == 1).any())

    errors = []
    if missing:
        errors.append(f"Missing required columns: {', '.join(missing)}")
    if not has_normalized:
        errors.append("No normalized scores found (LevelID=1 is required)")

    preview = {"columns": list(df.columns)}
    if "Adm0Name" in df.columns and not df.empty:
        preview["country"] = df["Adm0Name"].iloc[0]
    if "MarketName" in df.columns:
        preview["markets_count"] = int(df["MarketName"].nunique())
        preview["markets_sample"] = df["MarketName"].dropna().unique()[:5].tolist()
    if "Adm1Name" in df.columns:
        preview["regions_count"] = int(df["Adm1Name"].nunique())
        preview["regions"] = df["Adm1Name"].dropna().unique().tolist()
    if "DimensionName" in df.columns:
        preview["dimensions"] = df["DimensionName"].dropna().unique().tolist()

    return {
        "valid": not missing and has_normalized,
        "missing_columns": missing,
        "has_normalized_scores": has_normalized,
        "preview": preview,
        "errors": errors,
    }


def _extract_sub_scores(df: pd.DataFrame, market: str) -> Dict[str, Dict[str, Any]]:
    market_df = df[df["MarketName"] == market]
    trader_mean = market_df[market_df["LevelID"] == 5].copy()
    market_mean = market_df[market_df["LevelID"] == 6].copy()
    if not trader_mean.empty:
        trader_mean["VariableName"] = trader_mean["VariableName"].astype(str).str.strip()
    if not market_mean.empty:
        market_mean["VariableName"] = market_mean["VariableName"].astype(str).str.strip()

    def safe_mean(series: pd.Series) -> float:
        if series.empty:
            return 0.0
        return _safe_float(series.mean(), default=0.0)

    def safe_first(series: pd.Series) -> float:
        if series.empty:
            return 0.0
        return _safe_float(series.iloc[0], default=0.0)

    avail_scarcity = trader_mean[
        trader_mean["VariableName"].str.contains("AvailabilityScarcity_FCer", na=False)
    ]["OutputValue"]
    avail_runout = trader_mean[
        trader_mean["VariableName"].str.contains("AvailabilityRunout_FCer", na=False)
    ]["OutputValue"]
    price_increase = trader_mean[
        trader_mean["VariableName"].str.contains("PriceIncrease_FCer", na=False)
    ]["OutputValue"]
    price_stability = trader_mean[
        trader_mean["VariableName"].str.contains("PriceStability", na=False)
    ]["OutputValue"]
    density = trader_mean[
        trader_mean["VariableName"].str.contains("VulnerabilityDensity", na=False)
    ]["OutputValue"]
    complexity = trader_mean[
        trader_mean["VariableName"].str.contains("VulnerabilityComplexity", na=False)
    ]["OutputValue"]
    criticality = trader_mean[
        trader_mean["VariableName"].str.contains("VulnerabilityCriticality", na=False)
    ]["OutputValue"]
    concentration = market_mean[
        market_mean["VariableName"].str.contains("CompetitionConcentration", na=False)
    ]["OutputValue"]
    monopoly = market_mean[
        market_mean["VariableName"].str.contains("CompetitionMonopoly", na=False)
    ]["OutputValue"]
    checkout = trader_mean[
        trader_mean["VariableName"].str.contains("ServiceCheckout", na=False)
    ]["OutputValue"]
    shopping = trader_mean[
        trader_mean["VariableName"].str.contains("ServiceShopping", na=False)
    ]["OutputValue"]
    quality_vars = trader_mean[
        (trader_mean["DimensionName"] == "Quality")
        & (trader_mean["VariableName"].str.startswith("Quality", na=False))
    ]["OutputValue"]
    access = market_mean[
        market_mean["VariableName"].str.contains("AccessProtectionAccess", na=False)
    ]["OutputValue"]
    protection = market_mean[
        market_mean["VariableName"].str.contains("AccessProtectionProtection", na=False)
    ]["OutputValue"]
    assort_vars = trader_mean[trader_mean["DimensionName"] == "Assortment"]["OutputValue"]
    assort_score = safe_mean(assort_vars) * 10 if not assort_vars.empty else 7.5

    return {
        "Availability": {
            "scarce_cereals_pct": round(max(0, 1 - safe_mean(avail_scarcity)), 2),
            "runout_cereals_pct": round(max(0, 1 - safe_mean(avail_runout)), 2),
        },
        "Price": {
            "increase_cereals_pct": round(max(0, 1 - safe_mean(price_increase)), 2),
            "unstable_cereals_pct": round(max(0, 1 - safe_mean(price_stability)), 2),
        },
        "Resilience": {
            "low_density_pct": round(max(0, 1 - safe_mean(density)), 2),
            "high_complexity_pct": round(max(0, 1 - safe_mean(complexity)), 2),
            "high_criticality_pct": round(max(0, 1 - safe_mean(criticality)), 2),
        },
        "Competition": {
            "less_than_five_competitors": int(safe_first(concentration) < 3),
            "monopoly_risk": int(safe_first(monopoly) > 3),
        },
        "Infrastructure": {
            "condition_good": int(safe_first(trader_mean[trader_mean["VariableName"] == "InfrastructureConditionGood"]["OutputValue"]) > 0.5),
            "condition_medium": int(safe_first(trader_mean[trader_mean["VariableName"] == "InfrastructureConditionMedium"]["OutputValue"]) > 0.5),
            "condition_poor": int(safe_first(trader_mean[trader_mean["VariableName"] == "InfrastructureConditionPoor"]["OutputValue"]) > 0.5),
        },
        "Service": {
            "checkout_score": round(safe_mean(checkout) * 10, 1),
            "shopping_experience_score": round(safe_mean(shopping) * 10, 1),
        },
        "Food Quality": {
            "quality_standards_met_pct": round(safe_mean(quality_vars), 2) if not quality_vars.empty else 0.75,
            "quality_problems_pct": round(max(0, 1 - safe_mean(quality_vars)), 2) if not quality_vars.empty else 0.25,
        },
        "Access & Protection": {
            "access_issues_pct": round(max(0, 1 - safe_mean(access)), 2),
            "protection_issues_pct": round(max(0, 1 - safe_mean(protection)), 2),
        },
        "Assortment": {
            "breadth": round(assort_score, 1),
            "depth": round(assort_score, 1),
        },
    }


def _standardise_columns(df: pd.DataFrame) -> pd.DataFrame:
    rename = {column: DATABRIDGES_COLUMN_MAP.get(str(column), str(column).strip()) for column in df.columns}
    return df.rename(columns=rename)


def _validate_required_columns(df: pd.DataFrame) -> None:
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(f"MFI data missing required columns: {', '.join(sorted(missing))}")


def _first_date(df: pd.DataFrame, column: str) -> str:
    if column not in df.columns or df[column].dropna().empty:
        return "Unknown"
    return _format_date(df[column].dropna().iloc[0])


def _format_date(value: Any) -> str:
    if value in (None, ""):
        return "Unknown"
    try:
        parsed = pd.to_datetime(value, errors="coerce")
        if pd.isna(parsed):
            return "Unknown"
        return parsed.strftime("%Y-%m-%d")
    except Exception:
        return str(value)


def _field(row: Dict[str, Any], *names: str, default: Any = None) -> Any:
    for name in names:
        if name in row:
            return row[name]
    lowered = {str(key).lower(): value for key, value in row.items()}
    for name in names:
        key = name.lower()
        if key in lowered:
            return lowered[key]
    return default


def _safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    if value in (None, ""):
        return default
    try:
        if pd.isna(value):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default
