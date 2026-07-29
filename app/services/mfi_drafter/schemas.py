"""
MFI Drafter - Schemas
=====================
Classi e modelli per la generazione di MFI Reports.
"""
from __future__ import annotations

from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any, Literal
from dataclasses import dataclass, asdict

from app.shared.report_blocks import ReportBlock
from .methodology import (
    ANALYSIS_SCHEMA_VERSION,
    CSV_DIMENSION_TO_DISPLAY,
    DISPLAY_DIMENSIONS,
    METHODOLOGY_VERSION,
    OFFICIAL_DIMENSION_SCORE_VARIABLES,
    SCORE_AUTHORITY,
)


# ============================================================================
# CONSTANTS
# ============================================================================

MFI_DIMENSIONS = list(DISPLAY_DIMENSIONS)

RISK_COLORS = {
    "Very High Risk": "#d62728",
    "High Risk": "#ff7f0e",
    "Medium Risk": "#ffbb78",
    "Low Risk": "#2ca02c"
}

DIMENSION_NAME_MAP = {
    key: value
    for key, value in CSV_DIMENSION_TO_DISPLAY.items()
    if key != "MFI"
}

SCORE_VARIABLE_MAP = {
    **dict(OFFICIAL_DIMENSION_SCORE_VARIABLES),
    "MFI": "MFIScoreMFI",
}


def get_risk_level(mfi_score: float) -> str:
    """Classifica il livello di rischio in base allo score MFI."""
    if mfi_score < 4.0:
        return "Very High Risk"
    elif mfi_score < 5.5:
        return "High Risk"
    elif mfi_score < 7.0:
        return "Medium Risk"
    else:
        return "Low Risk"


# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class Document:
    """Documento recuperato da fonti esterne."""
    doc_id: str
    title: str
    url: str
    source: str
    date: str
    content: str

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class MFIMarketData:
    """Dati MFI per un singolo mercato."""
    market_name: str
    admin0: str
    admin1: str
    admin2: str
    region: str
    overall_mfi: float
    dimension_scores: Dict[str, float]
    subsections: Dict[str, List[Dict[str, Any]]]
    drivers: Dict[str, List[Dict[str, Any]]]
    risk_level: str
    traders_surveyed: int
    latitude: Optional[float] = None
    longitude: Optional[float] = None

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class MFIDimensionScore:
    """Aggregazione score per dimensione."""
    dimension: str
    national_score: float
    regional_scores: Dict[str, float]
    market_scores: Dict[str, float]

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class DimensionFinding:
    """Findings generati per una dimensione."""
    key_findings: str
    score_interpretation: str
    recommendations: str

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class SkepticFlag:
    """Flag sollevato dal Red Team QA."""
    section: str
    claim: str
    issue_type: Literal["score_mismatch", "interpretation_error", 
                        "template_violation", "missing_content"]
    severity: Literal["high", "medium", "low"]
    details: str
    recommendation: str

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class SurveyMetadata:
    """Metadati del survey MFI."""
    country: str
    collection_period: str
    total_traders: int
    total_markets: int
    regions_covered: List[str]

    def to_dict(self) -> dict:
        return asdict(self)


# ============================================================================
# MFI 2.0 METHODOLOGY EVIDENCE
# ============================================================================

class MFIMetric(BaseModel):
    """One exact processed-DataBridge metric for one assessed market."""

    metric_id: str
    dimension: str
    display_name: str
    variable_name: str
    source_level_id: int
    source_level_name: str
    role: Literal[
        "official_score",
        "official_subsection",
        "dimension_validation_component",
        "question_driver",
        "category_driver",
        "item_driver",
    ]
    raw_value: Optional[float] = None
    raw_min: float
    raw_max: float
    normalized_value: Optional[float] = None
    orientation: Literal["higher_is_better", "higher_is_worse", "descriptive"]
    unit: Literal["score", "proportion"]
    evidence_scope: Literal["assessed_market", "surveyed_traders_in_market"]
    observed_raw_values: List[Optional[float]] = Field(default_factory=list)
    market_coverage: int = 0
    market_coverage_total: int = 0
    missing_count: int = 0
    applicability_status: Literal[
        "available",
        "missing",
        "not_applicable",
        "not_represented",
    ] = "available"
    validation_status: Literal[
        "valid",
        "missing",
        "out_of_range",
        "duplicate",
        "formula_mismatch",
    ] = "valid"
    methodology_note: str
    product_group: Optional[str] = None
    question_group: Optional[str] = None
    item_name: Optional[str] = None
    severity_weight: Optional[int] = None
    traders_sample_size: Optional[int] = None


class MFIMetricSummary(BaseModel):
    """Deterministic unweighted summary across available assessed markets."""

    metric_id: str
    dimension: str
    display_name: str
    role: str
    mean_raw_value: Optional[float] = None
    mean_normalized_value: Optional[float] = None
    aggregation_numerator: Optional[float] = None
    aggregation_denominator: int = 0
    available_market_count: int = 0
    total_assessed_market_count: int = 0
    missing_count: int = 0
    unit: str
    orientation: str
    evidence_scope: str
    contributing_metric_ids: List[str] = Field(default_factory=list)
    methodology_note: str = ""


class MFIMethodologyWarning(BaseModel):
    """Structured, user-visible methodology or coverage warning."""

    code: str
    severity: Literal["warning", "error"] = "warning"
    message: str
    market_name: Optional[str] = None
    dimension: Optional[str] = None
    metric_ids: List[str] = Field(default_factory=list)
    expected_value: Optional[float] = None
    actual_value: Optional[float] = None
    delta: Optional[float] = None
    tolerance: Optional[float] = None


class MFIExcludedMarketRecord(BaseModel):
    """A processed market record excluded from the Full MFI assessment."""

    market_name: str
    detected_record_type: Literal["mfir_only"]
    reason: str
    available_level1_variables: List[str] = Field(default_factory=list)
    missing_level1_variables: List[str] = Field(default_factory=list)


# ============================================================================
# PYDANTIC MODELS (API)
# ============================================================================

class GenerateMFIReportInput(BaseModel):
    """Input for generating MFI report."""
    country: str = Field(..., description="Country name")
    data_collection_start: str = Field(..., description="Data collection start date (YYYY-MM-DD)")
    data_collection_end: str = Field(..., description="Data collection end date (YYYY-MM-DD)")
    markets: List[str] = Field(..., description="List of surveyed markets")


class GenerateMFIReportFromCSVInput(BaseModel):
    """Input for generating MFI report from uploaded CSV."""
    country_override: Optional[str] = Field(None, description="Override country name from CSV")
    data_collection_start_override: Optional[str] = Field(None, description="Override start date")
    data_collection_end_override: Optional[str] = Field(None, description="Override end date")


class GenerateMFIReportOutput(BaseModel):
    """Output of MFI report generation."""
    run_id: str
    country: str
    data_collection_start: str
    data_collection_end: str

    analysis_schema_version: Literal["2.0"] = ANALYSIS_SCHEMA_VERSION
    methodology_version: Literal["databridge-current"] = METHODOLOGY_VERSION
    score_authority: Literal["databridge_level_1", "synthetic_mock"] = SCORE_AUTHORITY
    excluded_market_records: List[MFIExcludedMarketRecord] = Field(default_factory=list)
    methodology_warnings: List[MFIMethodologyWarning] = Field(default_factory=list)
    
    # Survey info
    survey_metadata: Dict[str, Any]
    
    # MFI Data
    national_mfi: float
    risk_distribution: Dict[str, int]
    markets_data: List[Dict[str, Any]]
    dimension_scores: List[Dict[str, Any]]
    
    # Generated content
    executive_summary: str
    dimension_findings: Dict[str, Dict[str, str]]
    market_recommendations: Dict[str, Dict[str, Any]] = Field(default_factory=dict)
    country_context: Optional[str] = None

    document_references: List[Dict[str, Any]] = Field(default_factory=list)

    report_blocks: List[ReportBlock] = Field(default_factory=list)
    
    # Visualizations (Base64)
    visualizations: Dict[str, str]
    
    # Control
    warnings: List[str] = Field(default_factory=list)
    llm_calls: int = 0
    correction_attempts: int = 0
    success: bool = True


class MFIReportStatusOutput(BaseModel):
    """Status of an in-progress report."""
    run_id: str
    status: Literal["pending", "running", "completed", "failed"]
    current_node: Optional[str] = None
    progress_pct: int = 0
    warnings: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    error: Optional[str] = None
    traceback: Optional[str] = None
