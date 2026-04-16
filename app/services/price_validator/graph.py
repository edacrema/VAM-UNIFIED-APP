"""
Price Validator - Graph
=======================
Validation logic for Price Data XLSX with template comparison and LLM-assisted checks.

Layers:
- Layer 1: XLSX Validation (file integrity + load)
- Layer 2: Template Comparison (required, strict order + extra rules)
- Layer 3: Content Validation (LLM column detection + commodity/market/date checks)
- Layer 4: Deterministic Report (errors, locations, suggestions)
"""

from __future__ import annotations

import io
import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import TypedDict, Annotated, Literal, Dict, Any, Optional, Callable
import operator
import pandas as pd

from langgraph.graph import StateGraph, END
from langchain_core.messages import HumanMessage, SystemMessage

from app.shared.llm import get_model
from app.shared.gcs import (
    download_gcs_to_file,
    get_market_names_cache_path,
    get_market_names_gcs_uri,
)
from .schemas import ValidationError, LayerResult, PriceDataTemplate, Severity

logger = logging.getLogger(__name__)

OnStepCallback = Callable[[str, Dict[str, Any]], None]

# ============================================================================
# WFP PRODUCTS DICTIONARY
# ============================================================================

WFP_PRODUCTS = {
    "Apples": 144, "Apples (golden delicious)": 629, "Apples (red delicious)": 628,
    "Apricots": 195, "Apricots (dry)": 236, "Avocados": 157,
    "Bananas": 89, "Bananas (cooking)": 457, "Bananas (green)": 456,
    "Bananas (ripe)": 289, "Batteries (big)": 808,
    "Batteries (small)": 807, "Battery lamp": 1238, "Bean (mung dry)": 1211,
    "Beans": 50, "Beans (black gram)": 923, "Beans (black)": 85,
    "Beans (black dry)": 1259, "Beans (kidney red)": 206, "Beans (kidney white)": 389,
    "Beans (kidney)": 180, "Beans (mung)": 393, "Beans (niebe)": 120,
    "Beans (red)": 78, "Beans (white)": 66,
    "Bread": 55, "Bread (brown)": 159, "Bread (white)": 375, "Bread (wheat)": 375,
    "Butter": 372, "Cabbage": 181, "Carrots": 166,
    "Cashew nut": 167, "Cassava": 68, "Cassava (dry)": 290, "Cassava (fresh)": 291,
    "Cassava flour": 74, "Cassava meal": 217, "Cassava meal (gari)": 403,
    "Charcoal": 446, "Cheese": 414, "Chicken": 182, "Chickpeas": 244,
    "Chili": 820, "Chili (green)": 90, "Chili (red)": 91,
    "Rice": 52, "Rice (aromatic)": 894, "Rice (basmati)": 1172,
    "Rice (broken imported)": 438, "Rice (coarse)": 60, "Rice (fortified)": 1280,
    "Rice (high quality)": 247, "Rice (imported)": 64, "Rice (local)": 71,
    "Rice (long grain)": 162, "Rice (low quality)": 145, "Rice (medium grain)": 131,
    "Rice (paddy)": 203, "Rice (white)": 133, "Rice (white imported)": 400,
    "Sorghum": 65, "Sorghum (brown)": 481, "Sorghum (local)": 511,
    "Sorghum (red)": 282, "Sorghum (white)": 135, "Sorghum flour": 253,
    "Sugar": 97, "Sugar (brown)": 214, "Sugar (white)": 349, "Sugar (local)": 754,
    "Salt": 185, "Salt (iodised)": 334, "Salt (imported)": 823, "Salt (local)": 822,
    "Wheat": 84, "Wheat flour": 58, "Wheat flour (imported)": 339,
    "Wheat flour (local)": 179, "Wheat flour (fortified)": 496,
    "Oil (vegetable)": 189, "Oil (palm)": 192, "Oil (groundnut)": 190,
    "Oil (cooking)": 188, "Oil (sunflower)": 191, "Oil (imported)": 378,
    "Maize": 56, "Maize (white)": 136, "Maize (yellow)": 137, "Maize flour": 63,
    "Maize (local)": 512, "Maize (imported)": 202,
    "Lentils": 164, "Lentils (local)": 395, "Lentils (imported)": 396,
    "Eggs": 92, "Eggs (local)": 430, "Eggs (imported)": 298,
    "Milk": 93, "Milk (fresh)": 352, "Milk (powdered)": 186, "Milk (imported)": 353,
    "Meat (beef)": 94, "Meat (goat)": 140, "Meat (sheep)": 141, "Meat (camel)": 312,
    "Fish": 139, "Fish (fresh)": 401, "Fish (dry)": 171, "Fish (frozen)": 486,
    "Potatoes": 83, "Potatoes (Irish)": 148, "Potatoes (local)": 687,
    "Onions": 100, "Onions (red)": 168, "Onions (white)": 169,
    "Tomatoes": 114, "Tomatoes (local)": 346, "Tomatoes (paste)": 317,
    "Fuel (diesel)": 447, "Fuel (petrol)": 448, "Fuel (kerosene)": 495,
    "Firewood": 804,
    "Transport (public)": 304, "Exchange rate": 305, "Wage (casual labour)": 276,
    "Millet": 57, "Millet (finger)": 147, "Millet (pearl)": 146,
    "Groundnuts": 105, "Groundnuts (shelled)": 327, "Groundnuts (unshelled)": 328,
    "Cowpeas": 119, "Cowpeas (white)": 491, "Cowpeas (brown)": 879,
    "Peas": 165, "Peas (dry)": 265, "Peas (green)": 266,
    "Garlic": 170, "Ginger": 194, "Peppers": 116, "Peppers (green)": 212,
    "Spinach": 210, "Lettuce": 211, "Cucumber": 213, "Eggplant": 215,
    "Pumpkin": 216, "Okra": 218, "Watermelon": 219,
    "Oranges": 143, "Lemons": 172, "Mangoes": 158, "Papaya": 160,
    "Pineapple": 161, "Grapes": 196, "Dates": 197,
    "Tea": 187, "Coffee": 183, "Cocoa": 184,
    "Soap": 449, "Soap (bar)": 450, "Soap (washing)": 451,
    "Water (bottled)": 452, "Water (mineral)": 453,
}

# ============================================================================
# LLM PROMPTS
# ============================================================================

PROMPTS = {
    "column_detection": {
        "system": """You are a data quality analyst. Identify which columns contain:
1) commodity names
2) market names
3) date values

Return ONLY valid JSON with keys:
{
  "commodity_columns": [..],
  "market_columns": [..],
  "date_columns": [..]
}

Rules:
- Use column names exactly as provided.
- If unsure, leave the list empty.
- Do NOT include explanations or extra keys.""",
        "user": """Columns with samples (JSON):
{columns_with_samples}
""",
    },
    "commodity_suggestions": {
        "system": """You are a WFP commodity nomenclature expert.
Given a list of invalid commodity values, suggest the best matching approved commodity name.
Only suggest if similarity is high; otherwise return null.

Return ONLY valid JSON array:
[
  {"invalid": "...", "suggested": "..."|null, "confidence": 0.0-1.0},
  ...
]""",
        "user": """Invalid commodity values:
{invalid_values}

Approved commodity list:
{approved_list}
""",
    },
}

MAX_SAMPLE_VALUES = 3
MAX_SUGGESTION_BATCH = 20
MAX_SUGGESTION_VALUES = 80
SUGGESTION_CONFIDENCE_THRESHOLD = 0.8


# ============================================================================
# STATE DEFINITION
# ============================================================================

class PriceDataState(TypedDict):
    """Stato tipizzato per il grafo LangGraph."""
    # Input
    file_path: str
    template_path: str | None
    template: dict | None
    
    # Computed during execution
    file_type: str | None
    dataframe_json: str | None
    template_columns: list[str] | None
    column_roles: dict | None
    product_classifications: list
    
    # Results accumulator
    layer_results: Annotated[list[dict], operator.add]
    
    # Control flow
    can_continue: bool
    current_layer: int
    llm_calls: int
    
    # Output
    final_report: str | None
    
    # Metadata
    file_name: str
    country: str | None
    num_products: int | None
    num_markets: int | None


def create_initial_state(
    file_path: str,
    template_path: str | None = None
) -> PriceDataState:
    """Crea stato iniziale per il grafo."""
    
    template_dict = None
    if template_path:
        if template_path.endswith('.xlsx'):
            template_dict = PriceDataTemplate.from_excel(template_path).to_dict()
    
    return PriceDataState(
        file_path=file_path,
        template_path=template_path,
        template=template_dict,
        file_type=None,

        dataframe_json=None,
        template_columns=None,
        column_roles=None,
        product_classifications=[],
        layer_results=[],
        can_continue=True,
        current_layer=1,
        llm_calls=0,
        final_report=None,
        file_name=Path(file_path).name,
        country=None,
        num_products=None,
        num_markets=None
    )


# ============================================================================
# HELPERS
# ============================================================================

def _trim_value(value: Any) -> str:
    return str(value).strip()


def _parse_llm_json(payload: str) -> Any:
    cleaned = payload.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned)
        cleaned = re.sub(r"\s*```$", "", cleaned)
    return json.loads(cleaned)


def _chunk_list(values: list[str], size: int) -> list[list[str]]:
    return [values[i:i + size] for i in range(0, len(values), size)]


def _df_from_state(state: PriceDataState) -> pd.DataFrame:
    if not state.get("dataframe_json"):
        return pd.DataFrame()
    return pd.read_json(io.StringIO(state["dataframe_json"]), orient="split")


def _get_market_names_gcs_uri() -> str | None:
    return get_market_names_gcs_uri()


def _load_market_names() -> set[str]:
    gcs_uri = _get_market_names_gcs_uri()
    if not gcs_uri:
        raise FileNotFoundError("Market names GCS URI not configured")

    destination = get_market_names_cache_path()
    if not destination.exists():
        download_gcs_to_file(gcs_uri, destination)

    df = pd.read_csv(destination)
    if df.empty:
        raise ValueError("market_names.csv is empty")

    lower_map = {c.lower().strip(): c for c in df.columns}
    column = None
    for candidate in ["market_name", "market", "name"]:
        if candidate in lower_map:
            column = lower_map[candidate]
            break
    if column is None:
        column = df.columns[0]

    values = df[column].dropna().astype(str).map(_trim_value)
    return {value for value in values if value}


def _build_affected_rows(indices: list[tuple[int, str]], column: str) -> list[dict]:
    affected = []
    for idx, value in indices:
        affected.append({
            "row": int(idx) + 2,
            "column": column,
            "value": value,
        })
        if len(affected) >= 50:
            break
    return affected


def _detect_columns_with_llm(df: pd.DataFrame) -> tuple[dict, list[ValidationError], int]:
    errors: list[ValidationError] = []
    llm_calls = 0
    if df.empty:
        errors.append(ValidationError(
            code="L3.0",
            severity=Severity.CRITICAL,
            message="No data available for column detection",
        ))
        return {}, errors, llm_calls

    columns_with_samples: dict[str, list[str]] = {}
    for column in df.columns:
        samples = (
            df[column]
            .dropna()
            .astype(str)
            .map(_trim_value)
            .unique()
            .tolist()
        )
        columns_with_samples[str(column)] = [value for value in samples if value][:MAX_SAMPLE_VALUES]

    model = get_model()
    try:
        response = model.invoke([
            SystemMessage(content=PROMPTS["column_detection"]["system"]),
            HumanMessage(content=PROMPTS["column_detection"]["user"].format(
                columns_with_samples=json.dumps(columns_with_samples, ensure_ascii=True)
            )),
        ])
        llm_calls += 1
        payload = _parse_llm_json(response.content)
        roles = {
            "commodity_columns": payload.get("commodity_columns", []) or [],
            "market_columns": payload.get("market_columns", []) or [],
            "date_columns": payload.get("date_columns", []) or [],
        }
        available = set(df.columns)
        for key, cols in roles.items():
            roles[key] = [col for col in cols if col in available]
        return roles, errors, llm_calls
    except Exception as exc:
        logger.warning("Column detection failed: %s", exc)
        errors.append(ValidationError(
            code="L3.0",
            severity=Severity.CRITICAL,
            message="LLM column detection failed",
            suggestion="Ensure the dataset has clear commodity, market, and date columns",
        ))
        return {}, errors, llm_calls


def _suggest_commodities(values: list[str]) -> tuple[list[dict], int]:
    if not values:
        return [], 0

    model = get_model()
    llm_calls = 0
    suggestions: list[dict] = []
    approved_list = "\n".join(sorted(WFP_PRODUCTS.keys()))

    for batch in _chunk_list(values, MAX_SUGGESTION_BATCH):
        try:
            response = model.invoke([
                SystemMessage(content=PROMPTS["commodity_suggestions"]["system"]),
                HumanMessage(content=PROMPTS["commodity_suggestions"]["user"].format(
                    invalid_values=json.dumps(batch, ensure_ascii=True),
                    approved_list=approved_list,
                )),
            ])
            llm_calls += 1
            parsed = _parse_llm_json(response.content)
            for item in parsed:
                confidence = float(item.get("confidence", 0))
                suggested = item.get("suggested")
                if suggested and confidence >= SUGGESTION_CONFIDENCE_THRESHOLD:
                    suggestions.append({
                        "invalid": item.get("invalid"),
                        "suggested": suggested,
                        "confidence": confidence,
                    })
        except Exception as exc:
            logger.warning("Commodity suggestion failed: %s", exc)
            continue

    return suggestions, llm_calls


# ============================================================================
# LAYER 1: XLSX VALIDATION
# ============================================================================

def layer1_xlsx_validation(state: PriceDataState) -> dict:
    errors: list[ValidationError] = []
    warnings: list[ValidationError] = []
    metadata: dict[str, Any] = {}

    file_path = Path(state["file_path"])
    if not file_path.exists():
        errors.append(ValidationError(
            code="L1.0",
            severity=Severity.CRITICAL,
            message="File not found",
        ))

    if file_path.suffix.lower() != ".xlsx":
        errors.append(ValidationError(
            code="L1.1",
            severity=Severity.CRITICAL,
            message="Unsupported file format",
            suggestion="Upload an Excel .xlsx file",
        ))

    df = None
    if not any(err.severity == Severity.CRITICAL for err in errors):
        try:
            df = pd.read_excel(file_path)
            metadata["total_rows"] = len(df)
            metadata["total_columns"] = len(df.columns)
            metadata["columns"] = df.columns.tolist()
        except Exception as exc:
            errors.append(ValidationError(
                code="L1.2",
                severity=Severity.CRITICAL,
                message=f"Unable to read Excel file: {exc}",
            ))

    has_critical = any(err.severity == Severity.CRITICAL for err in errors)
    result = LayerResult(
        layer_id=1,
        layer_name="XLSX Validation",
        passed=not errors,
        can_continue=not has_critical and df is not None,
        errors=errors,
        warnings=warnings,
        metadata=metadata,
    )

    return {
        "layer_results": [result.to_dict()],
        "can_continue": not has_critical and df is not None,
        "current_layer": 1,
        "file_type": "XLSX" if not has_critical else None,
        "dataframe_json": df.to_json(orient="split", date_format="iso") if df is not None else None,
    }


# ============================================================================
# LAYER 2: TEMPLATE COMPARISON (REQUIRED)
# ============================================================================

def layer2_template_comparison(state: PriceDataState) -> dict:
    errors: list[ValidationError] = []
    warnings: list[ValidationError] = []
    metadata: dict[str, Any] = {}

    df = _df_from_state(state)
    if df.empty:
        errors.append(ValidationError(
            code="L2.0",
            severity=Severity.CRITICAL,
            message="Dataset is empty or unreadable",
        ))

    if not state.get("template") or not state["template"].get("columns"):
        errors.append(ValidationError(
            code="L2.0",
            severity=Severity.CRITICAL,
            message="Template is required for validation",
            suggestion="Upload the official template XLSX",
        ))

    template_cols = [str(col) for col in state.get("template", {}).get("columns", [])]
    template_trimmed = [_trim_value(col) for col in template_cols]
    df_cols = [str(col) for col in df.columns]
    df_trimmed = [_trim_value(col) for col in df_cols]

    metadata["template_columns"] = template_trimmed
    metadata["submitted_columns"] = df_trimmed

    template_set = set(template_trimmed)
    df_set = set(df_trimmed)

    missing_columns = [col for col in template_trimmed if col not in df_set]
    if missing_columns:
        errors.append(ValidationError(
            code="L2.1",
            severity=Severity.CRITICAL,
            message="Missing columns compared to template",
            details={"missing_columns": missing_columns},
            suggestion="Add the missing template columns in the correct order",
        ))

    if template_set and df_trimmed:
        present_required = [col for col in df_trimmed if col in template_set]
        if present_required != template_trimmed:
            errors.append(ValidationError(
                code="L2.2",
                severity=Severity.CRITICAL,
                message="Column order does not match template",
                details={"expected_order": template_trimmed},
                suggestion="Reorder columns to match the template exactly",
            ))

        extra_columns = [col for col in df_trimmed if col not in template_set]
        if extra_columns:
            required_indices = [i for i, col in enumerate(df_trimmed) if col in template_set]
            last_required_index = max(required_indices) if required_indices else -1
            inserted_extras = [
                df_trimmed[i]
                for i in range(0, last_required_index + 1)
                if df_trimmed[i] not in template_set
            ]
            appended_extras = [
                df_trimmed[i]
                for i in range(last_required_index + 1, len(df_trimmed))
                if df_trimmed[i] not in template_set
            ]
            if inserted_extras:
                errors.append(ValidationError(
                    code="L2.3",
                    severity=Severity.CRITICAL,
                    message="Extra columns inserted within template columns",
                    details={"extra_columns": inserted_extras},
                    suggestion="Move extra columns after all template columns",
                ))
            if appended_extras:
                metadata["appended_extra_columns"] = appended_extras

    has_critical = any(err.severity == Severity.CRITICAL for err in errors)
    result = LayerResult(
        layer_id=2,
        layer_name="Template Comparison",
        passed=not errors,
        can_continue=not has_critical,
        errors=errors,
        warnings=warnings,
        metadata=metadata,
    )

    return {
        "layer_results": [result.to_dict()],
        "can_continue": not has_critical,
        "current_layer": 2,
        "template_columns": template_trimmed,
    }


# ============================================================================
# LAYER 3: CONTENT VALIDATION
# ============================================================================

def layer3_content_validation(state: PriceDataState) -> dict:
    errors: list[ValidationError] = []
    warnings: list[ValidationError] = []
    metadata: dict[str, Any] = {}
    llm_calls = 0

    df = _df_from_state(state)
    if df.empty:
        errors.append(ValidationError(
            code="L3.0",
            severity=Severity.CRITICAL,
            message="Dataset is empty; cannot validate contents",
        ))
        result = LayerResult(
            layer_id=3,
            layer_name="Content Validation",
            passed=False,
            can_continue=False,
            errors=errors,
            warnings=warnings,
            metadata=metadata,
        )
        return {
            "layer_results": [result.to_dict()],
            "can_continue": False,
            "current_layer": 3,
            "llm_calls": state["llm_calls"],
        }

    column_roles, role_errors, role_llm_calls = _detect_columns_with_llm(df)
    llm_calls += role_llm_calls
    errors.extend(role_errors)

    commodity_columns = column_roles.get("commodity_columns", []) if column_roles else []
    market_columns = column_roles.get("market_columns", []) if column_roles else []
    date_columns = column_roles.get("date_columns", []) if column_roles else []

    metadata["column_roles"] = column_roles

    if not commodity_columns:
        errors.append(ValidationError(
            code="L3.1",
            severity=Severity.CRITICAL,
            message="Commodity column not detected",
            suggestion="Ensure the commodity column uses clear headers",
        ))
    if not market_columns:
        errors.append(ValidationError(
            code="L3.2",
            severity=Severity.CRITICAL,
            message="Market column not detected",
            suggestion="Ensure the market column uses clear headers",
        ))
    if not date_columns:
        errors.append(ValidationError(
            code="L3.3",
            severity=Severity.CRITICAL,
            message="Date column not detected",
            suggestion="Ensure the date column uses clear headers",
        ))

    approved_products = set(WFP_PRODUCTS.keys())
    invalid_commodities: list[tuple[int, str]] = []
    for column in commodity_columns:
        series = df[column]
        for idx, value in series.items():
            if pd.isna(value):
                continue
            trimmed = _trim_value(value)
            if trimmed not in approved_products:
                invalid_commodities.append((idx, trimmed))

    commodity_suggestions: list[dict] = []
    if invalid_commodities:
        unique_invalid = list(dict.fromkeys([val for _, val in invalid_commodities]))
        if len(unique_invalid) <= MAX_SUGGESTION_VALUES:
            commodity_suggestions, suggest_llm_calls = _suggest_commodities(unique_invalid)
            llm_calls += suggest_llm_calls

        errors.append(ValidationError(
            code="L3.4",
            severity=Severity.CRITICAL,
            message="Invalid commodity names detected",
            details={
                "invalid_values": unique_invalid[:20],
                "suggestions": commodity_suggestions,
            },
            suggestion="Replace invalid commodities with approved names",
            affected_rows=_build_affected_rows(invalid_commodities, commodity_columns[0]),
        ))

    market_names: set[str] = set()
    try:
        market_names = _load_market_names()
    except Exception as exc:
        errors.append(ValidationError(
            code="L3.5",
            severity=Severity.CRITICAL,
            message=f"Unable to load market names: {exc}",
            suggestion="Ensure market_names.csv is available in the GCS bucket",
        ))

    invalid_markets: list[tuple[int, str]] = []
    if market_names and market_columns:
        for column in market_columns:
            series = df[column]
            for idx, value in series.items():
                if pd.isna(value):
                    continue
                trimmed = _trim_value(value)
                if trimmed not in market_names:
                    invalid_markets.append((idx, trimmed))

    if invalid_markets:
        unique_invalid_markets = list(dict.fromkeys([val for _, val in invalid_markets]))
        errors.append(ValidationError(
            code="L3.6",
            severity=Severity.CRITICAL,
            message="Invalid market names detected",
            details={"invalid_values": unique_invalid_markets[:20]},
            suggestion="Update market names to match the official list",
            affected_rows=_build_affected_rows(invalid_markets, market_columns[0]),
        ))

    invalid_dates: list[tuple[int, str]] = []
    now = datetime.utcnow()
    if date_columns:
        for column in date_columns:
            parsed = pd.to_datetime(df[column], errors="coerce")
            for idx, value in parsed.items():
                if pd.isna(value):
                    continue
                if value.to_pydatetime() > now:
                    original = df.at[idx, column]
                    invalid_dates.append((idx, _trim_value(original)))

    if invalid_dates:
        errors.append(ValidationError(
            code="L3.7",
            severity=Severity.CRITICAL,
            message="Future dates detected",
            details={"examples": [val for _, val in invalid_dates[:10]]},
            suggestion="Remove or correct future dates",
            affected_rows=_build_affected_rows(invalid_dates, date_columns[0] if date_columns else "date"),
        ))

    if commodity_columns:
        metadata["num_products"] = df[commodity_columns[0]].nunique(dropna=True)
    if market_columns:
        metadata["num_markets"] = df[market_columns[0]].nunique(dropna=True)

    has_critical = any(err.severity == Severity.CRITICAL for err in errors)
    result = LayerResult(
        layer_id=3,
        layer_name="Content Validation",
        passed=not errors,
        can_continue=not has_critical,
        errors=errors,
        warnings=warnings,
        metadata=metadata,
    )

    return {
        "layer_results": [result.to_dict()],
        "can_continue": not has_critical,
        "current_layer": 3,
        "llm_calls": state["llm_calls"] + llm_calls,
        "column_roles": column_roles,
        "product_classifications": commodity_suggestions,
        "num_products": metadata.get("num_products"),
        "num_markets": metadata.get("num_markets"),
    }

# ============================================================================
# LAYER 4: DETERMINISTIC REPORT
# ============================================================================

def layer4_deterministic_report(state: PriceDataState) -> dict:
    report_lines = [
        "Price Data Validation Report",
        "",
        f"File: {state.get('file_name')}",
        f"File Type: {state.get('file_type') or 'Unknown'}",
        "",
        "Errors:",
    ]

    errors_found = False
    warnings_found = False

    for layer in state.get("layer_results", []):
        layer_label = f"Layer {layer['layer_id']} - {layer['layer_name']}"
        for err in layer.get("errors", []) or []:
            errors_found = True
            details = err.get("details") or {}
            suggestion = err.get("suggestion") or "Review the dataset and template."
            affected_rows = err.get("affected_rows") or []
            location = ""
            if affected_rows:
                rows = ", ".join(
                    [f"{item.get('column')}@{item.get('row')}" for item in affected_rows[:5]]
                )
                location = f" (locations: {rows})"
            report_lines.append(
                f"- [{err.get('code')}] {layer_label}: {err.get('message')}{location}"
            )
            if details:
                report_lines.append(f"  Details: {json.dumps(details, ensure_ascii=True)}")
            report_lines.append(f"  Suggestion: {suggestion}")

        for warn in layer.get("warnings", []) or []:
            warnings_found = True
            report_lines.append(
                f"- [WARN {warn.get('code')}] {layer_label}: {warn.get('message')}"
            )

    if not errors_found:
        report_lines.append("- No errors detected.")

    if warnings_found:
        report_lines.append("\nWarnings were detected. Review them if needed.")

    return {
        "final_report": "\n".join(report_lines),
        "current_layer": 4,
    }


# ============================================================================
# ROUTING FUNCTIONS
# ============================================================================

def route_after_layer(state: PriceDataState) -> Literal["continue", "report"]:
    """Router generico: continua o vai al report."""
    if state["can_continue"]:
        return "continue"
    return "report"


# ============================================================================
# GRAPH BUILDER
# ============================================================================

def build_graph(on_step: Optional[OnStepCallback] = None):
    """
    Costruisce il grafo LangGraph per Price Data troubleshooting.
    
    Struttura:
        L1 → [route] → L2 → [route] → L3 → report → END
    """
    def wrap_node(node_name: str, fn):
        def wrapped(state: PriceDataState):
            if on_step is not None:
                on_step(node_name, dict(state))
            return fn(state)

        return wrapped

    graph = StateGraph(PriceDataState)
    
    # Add nodes
    graph.add_node("layer1", wrap_node("layer1", layer1_xlsx_validation))
    graph.add_node("layer2", wrap_node("layer2", layer2_template_comparison))
    graph.add_node("layer3", wrap_node("layer3", layer3_content_validation))
    graph.add_node("report", wrap_node("report", layer4_deterministic_report))
    
    # Set entry point
    graph.set_entry_point("layer1")
    
    # Add conditional edges for early exit
    graph.add_conditional_edges(
        "layer1",
        route_after_layer,
        {"continue": "layer2", "report": "report"}
    )
    
    graph.add_conditional_edges(
        "layer2",
        route_after_layer,
        {"continue": "layer3", "report": "report"}
    )
    
    # Layer 3 sempre va al report
    graph.add_edge("layer3", "report")
    
    # End
    graph.add_edge("report", END)
    
    return graph.compile()


# ============================================================================
# PUBLIC API
# ============================================================================

def run_troubleshooting(
    file_path: str,
    template_path: str | None = None,
    on_step: Optional[OnStepCallback] = None
) -> dict:
    """
    Entry point per validazione Price Data.
    
    Args:
        file_path: Path al file Excel (.xlsx)
        template_path: Path al template corretto (obbligatorio)
    
    Returns:
        Stato finale con report
    """
    initial_state = create_initial_state(file_path, template_path)
    agent = build_graph(on_step=on_step)
    return agent.invoke(initial_state)
