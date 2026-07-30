"""
MFI Drafter - Schemas
=====================
Classi e modelli per la generazione di MFI Reports.
"""
from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, model_validator
from typing import Optional, List, Dict, Any, Literal
from dataclasses import dataclass, asdict

from app.shared.report_blocks import ReportBlock
from .methodology import (
    ANALYSIS_SCHEMA_VERSION,
    CSV_DIMENSION_TO_DISPLAY,
    DISPLAY_DIMENSIONS,
    METHODOLOGY_VERSION,
    NARRATIVE_SCHEMA_VERSION,
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
# MFI 2.0 DETERMINISTIC ANALYSIS
# ============================================================================

class MFIAnalysisConfig(BaseModel):
    """Injectable, validated configuration for the pure Phase 2 analysis."""

    model_config = ConfigDict(frozen=True)

    priority_dimension_min: int = 3
    priority_dimension_max: int = 4
    priority_market_max: int = 15
    market_weak_dimension_count: int = 3
    item_min_market_count: int = 3
    item_min_market_ratio: float = 0.25
    item_category_contrast: float = 0.10
    item_max_per_group: int = 3
    ranking_tie_tolerance: float = 1e-6
    quartile_interpolation: Literal["linear"] = "linear"

    @model_validator(mode="after")
    def validate_analysis_config(self) -> "MFIAnalysisConfig":
        if self.priority_dimension_min < 1:
            raise ValueError("priority_dimension_min must be at least 1")
        if self.priority_dimension_max < self.priority_dimension_min:
            raise ValueError(
                "priority_dimension_max must be greater than or equal to "
                "priority_dimension_min"
            )
        if self.priority_dimension_max > len(MFI_DIMENSIONS):
            raise ValueError("priority_dimension_max cannot exceed the dimension count")
        if self.priority_market_max < 1:
            raise ValueError("priority_market_max must be at least 1")
        if not 1 <= self.market_weak_dimension_count <= len(MFI_DIMENSIONS):
            raise ValueError("market_weak_dimension_count is outside the dimension count")
        if self.item_min_market_count < 1:
            raise ValueError("item_min_market_count must be at least 1")
        if not 0.0 <= self.item_min_market_ratio <= 1.0:
            raise ValueError("item_min_market_ratio must be between 0 and 1")
        if not 0.0 <= self.item_category_contrast <= 1.0:
            raise ValueError("item_category_contrast must be between 0 and 1")
        if self.item_max_per_group < 1:
            raise ValueError("item_max_per_group must be at least 1")
        if self.ranking_tie_tolerance < 0.0:
            raise ValueError("ranking_tie_tolerance cannot be negative")
        return self


class MFICoverageSummary(BaseModel):
    """Assessment-level market coverage for one deterministic value."""

    available_market_count: int
    total_assessed_market_count: int
    missing_count: int
    coverage_ratio: float


class MFIStatisticalSummary(BaseModel):
    """Unrounded unweighted statistics over included assessed markets."""

    mean: float
    median: float
    minimum: float
    maximum: float
    q1: float
    q3: float
    iqr: float
    score_range: float
    numerator: float
    denominator: int
    coverage: MFICoverageSummary


class MFIRankedValue(BaseModel):
    """One value in a deterministic, tolerance-aware ordered population."""

    name: str
    value: float
    rank: int
    selection_order: int
    ledger_metric_id: str


class MFIAnalyzedMetric(BaseModel):
    """Assessment-level subsection or driver with deterministic ranking."""

    metric_id: str
    dimension: str
    display_name: str
    role: str
    mean_raw_value: Optional[float] = None
    mean_normalized_value: Optional[float] = None
    unit: str
    orientation: str
    evidence_scope: str
    coverage: MFICoverageSummary
    unfavorable_rate: Optional[float] = None
    weakness_rank: Optional[int] = None
    group_rank: Optional[int] = None
    product_group: Optional[str] = None
    question_group: Optional[str] = None
    item_name: Optional[str] = None
    severity_weight: Optional[int] = None
    item_relevant: bool = False
    relevance_reasons: List[str] = Field(default_factory=list)
    matching_category_metric_id: Optional[str] = None
    source_metric_ids: List[str] = Field(default_factory=list)
    ledger_metric_ids: List[str] = Field(default_factory=list)


class MFIRegionalDimensionSummary(BaseModel):
    """One dimension's unweighted summary and rank inside one region."""

    region: str
    statistics: MFIStatisticalSummary
    rank: int
    selection_order: int
    ledger_metric_ids: List[str] = Field(default_factory=list)


class MFILocalizedPatterns(BaseModel):
    """Deterministic location patterns for one dimension."""

    regions_where_bottom_one: List[str] = Field(default_factory=list)
    regions_where_bottom_two: List[str] = Field(default_factory=list)
    markets_where_lowest: List[str] = Field(default_factory=list)
    ordered_markets: List[MFIRankedValue] = Field(default_factory=list)
    score_range: float
    iqr: float


class MFIDimensionProfile(BaseModel):
    """Complete deterministic analytical profile for one official dimension."""

    dimension: str
    statistics: MFIStatisticalSummary
    profile_rank: int
    selection_order: int
    is_priority: bool
    priority_reasons: List[
        Literal["bottom_rank", "below_profile_mean"]
    ] = Field(default_factory=list)
    subsections: List[MFIAnalyzedMetric] = Field(default_factory=list)
    drivers: List[MFIAnalyzedMetric] = Field(default_factory=list)
    regional_summaries: List[MFIRegionalDimensionSummary] = Field(default_factory=list)
    localized_patterns: MFILocalizedPatterns
    ledger_metric_ids: List[str] = Field(default_factory=list)


class MFIMarketDimensionProfile(BaseModel):
    """One official dimension score and weakness status inside a market."""

    dimension: str
    score: float
    rank: int
    selection_order: int
    is_weak: bool
    ledger_metric_ids: List[str] = Field(default_factory=list)


class MFIMarketProfile(BaseModel):
    """Stored overall score, relative rank, and weak dimensions for one market."""

    market_name: str
    region: Optional[str] = None
    overall_mfi: float
    score_rank: int
    selection_order: int
    is_priority_market: bool
    selection_reasons: List[str] = Field(default_factory=list)
    weak_dimensions: List[MFIMarketDimensionProfile] = Field(default_factory=list)
    dimension_profile: List[MFIMarketDimensionProfile] = Field(default_factory=list)
    ledger_metric_ids: List[str] = Field(default_factory=list)


class MFILimitation(BaseModel):
    """Stable, explicit limitation attached to the deterministic profile."""

    code: str
    severity: Literal["info", "warning"] = "warning"
    message: str
    market_name: Optional[str] = None
    region: Optional[str] = None
    dimension: Optional[str] = None
    metric_ids: List[str] = Field(default_factory=list)


class MFIMetricLedgerEntry(BaseModel):
    """One uniquely addressable value supporting profiles and tables."""

    ledger_id: str
    label: str
    value: float
    statistic: str
    unit: str
    orientation: str
    evidence_scope: str
    dimension: Optional[str] = None
    market_name: Optional[str] = None
    region: Optional[str] = None
    coverage: Optional[MFICoverageSummary] = None
    source_metric_ids: List[str] = Field(default_factory=list)


class MFIDeterministicTableRow(BaseModel):
    """A presentation-neutral table row backed entirely by ledger entries."""

    row_id: str
    values: Dict[str, Any] = Field(default_factory=dict)
    ledger_metric_ids: List[str] = Field(default_factory=list)


class MFIDeterministicTables(BaseModel):
    """Versioned, deterministic tables ready for later presentation work."""

    dimension_rows: List[MFIDeterministicTableRow] = Field(default_factory=list)
    regional_rows: List[MFIDeterministicTableRow] = Field(default_factory=list)
    subsection_rows: List[MFIDeterministicTableRow] = Field(default_factory=list)
    driver_rows: List[MFIDeterministicTableRow] = Field(default_factory=list)
    relevant_item_rows: List[MFIDeterministicTableRow] = Field(default_factory=list)
    priority_market_rows: List[MFIDeterministicTableRow] = Field(default_factory=list)


class MFIAssessmentProfile(BaseModel):
    """Complete public Phase 2 deterministic assessment profile."""

    analysis_schema_version: Literal["2.0"] = ANALYSIS_SCHEMA_VERSION
    analysis_version: str
    methodology_version: str
    score_authority: str
    assessed_market_count: int
    excluded_market_count: int = 0
    mean_mfi_across_assessed_markets: float
    overall_statistics: MFIStatisticalSummary
    dimension_profile_mean: float
    dimensions: List[MFIDimensionProfile]
    markets: List[MFIMarketProfile]
    priority_dimension_names: List[str]
    priority_market_names: List[str]
    limitations: List[MFILimitation] = Field(default_factory=list)
    metric_ledger: Dict[str, MFIMetricLedgerEntry] = Field(default_factory=dict)
    tables: MFIDeterministicTables


# ============================================================================
# MFI 2.0 STRUCTURED NARRATIVE AND QA
# ============================================================================

class MFIContextEvidenceStatement(BaseModel):
    """One source-linked contextual statement classified before drafting."""

    statement_id: str
    text: str
    classification: Literal[
        "corroborating",
        "potentially_explanatory",
        "unrelated",
    ]
    document_ids: List[str] = Field(default_factory=list)
    validation_status: Literal["pending", "verified", "unverified"] = "pending"
    validation_flags: List[str] = Field(default_factory=list)


class MFIClaimCatalogEntry(BaseModel):
    """Closed deterministic value that narrative claims may cite."""

    metric_id: str
    label: str
    numeric_value: float
    formatted_value: str
    allowed_renderings: List[str] = Field(default_factory=list)
    statistic: str
    unit: str
    orientation: str
    scope: str
    dimension: Optional[str] = None
    market_name: Optional[str] = None
    region: Optional[str] = None
    coverage_label: Optional[str] = None
    source_metric_ids: List[str] = Field(default_factory=list)


class MFINarrativeClaim(BaseModel):
    """One independently cited and validated narrative statement."""

    claim_id: str
    text: str
    claim_kind: Literal[
        "summary",
        "finding",
        "geographic_pattern",
        "limitation",
        "recommendation",
        "context",
        "modality_consideration",
    ]
    metric_ids: List[str] = Field(default_factory=list)
    document_ids: List[str] = Field(default_factory=list)
    scope: Literal[
        "assessment",
        "region",
        "market",
        "surveyed_traders",
        "context",
    ] = "assessment"
    polarity: Literal[
        "favorable",
        "unfavorable",
        "neutral",
        "descriptive",
    ] = "neutral"
    validation_status: Literal["pending", "verified", "unverified"] = "pending"
    validation_flags: List[str] = Field(default_factory=list)


class MFISubdimensionNarrative(BaseModel):
    """Priority-dimension interpretation linked to official evidence."""

    name: str
    subsection_metric_id: Optional[str] = None
    score_0_10: Optional[float] = None
    interpretation: MFINarrativeClaim
    driver_metric_ids: List[str] = Field(default_factory=list)


class MFIDimensionNarrative(BaseModel):
    """Canonical structured narrative for one MFI dimension."""

    dimension: str
    is_priority: bool
    summary: MFINarrativeClaim
    key_findings: List[MFINarrativeClaim] = Field(default_factory=list)
    subdimension_analysis: List[MFISubdimensionNarrative] = Field(
        default_factory=list
    )
    geographic_patterns: List[MFINarrativeClaim] = Field(default_factory=list)
    data_limitations: List[MFINarrativeClaim] = Field(default_factory=list)
    recommendations: List[MFINarrativeClaim] = Field(default_factory=list)


class MFIMarketNarrative(BaseModel):
    """Canonical recommendation narrative for one selected market."""

    market_name: str
    region: Optional[str] = None
    overall_mfi: float
    score_rank: int
    weak_dimensions: List[str] = Field(default_factory=list)
    priority_issues: List[MFINarrativeClaim] = Field(default_factory=list)
    recommended_interventions: List[MFINarrativeClaim] = Field(
        default_factory=list
    )
    modality_consideration: Optional[MFINarrativeClaim] = None


class MFIExecutiveNarrative(BaseModel):
    """Canonical structured executive summary."""

    motivation: Optional[MFINarrativeClaim] = None
    key_findings: List[MFINarrativeClaim] = Field(default_factory=list)
    recommendations: List[MFINarrativeClaim] = Field(default_factory=list)
    limitations: List[MFINarrativeClaim] = Field(default_factory=list)


class MFINarrativeQAFlag(BaseModel):
    """Stable deterministic, Red-Team, or system narrative flag."""

    flag_id: str
    source: Literal["deterministic", "red_team", "system"]
    code: str
    severity: Literal["high", "medium", "low"]
    artifact_type: Literal[
        "context",
        "dimension",
        "market",
        "executive_summary",
        "global",
    ]
    artifact_id: Optional[str] = None
    field_name: Optional[str] = None
    claim_id: Optional[str] = None
    message: str
    recommendation: str = ""
    metric_ids: List[str] = Field(default_factory=list)
    document_ids: List[str] = Field(default_factory=list)
    expected_value: Optional[str] = None
    actual_value: Optional[str] = None
    repairable: bool = True


class MFIClaimValidationResult(BaseModel):
    """Deterministic validation result for all structured claims."""

    status: Literal[
        "not_recorded",
        "passed",
        "passed_with_warnings",
        "failed",
    ] = "not_recorded"
    validated_claim_count: int = 0
    verified_claim_count: int = 0
    unverified_claim_count: int = 0
    flags: List[MFINarrativeQAFlag] = Field(default_factory=list)


class MFICorrectionTarget(BaseModel):
    """Exact narrative field selected for a targeted repair."""

    artifact_type: Literal[
        "context",
        "dimension",
        "market",
        "executive_summary",
        "global",
    ]
    artifact_id: Optional[str] = None
    field_name: Optional[str] = None
    claim_ids: List[str] = Field(default_factory=list)
    flag_ids: List[str] = Field(default_factory=list)


class MFIQAReview(BaseModel):
    """Final combined deterministic and LLM QA status."""

    status: Literal[
        "not_recorded",
        "passed",
        "passed_with_advisories",
        "completed_with_warnings",
    ] = "not_recorded"
    correction_attempts: int = 0
    flags: List[MFINarrativeQAFlag] = Field(default_factory=list)


class MFIMarketScoreDistributionEntry(BaseModel):
    """Neutral ordered market-score value for public consumers and charts."""

    market_name: str
    overall_mfi: float
    score_rank: int
    selection_order: int
    is_priority_market: bool


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
    narrative_schema_version: Literal["2.0"] = NARRATIVE_SCHEMA_VERSION
    excluded_market_records: List[MFIExcludedMarketRecord] = Field(default_factory=list)
    methodology_warnings: List[MFIMethodologyWarning] = Field(default_factory=list)
    
    # Survey info
    survey_metadata: Dict[str, Any]
    
    # MFI Data
    national_mfi: float
    risk_distribution: Dict[str, int]
    markets_data: List[Dict[str, Any]]
    dimension_scores: List[Dict[str, Any]]
    mean_mfi_across_assessed_markets: float
    assessment_profile: MFIAssessmentProfile
    market_score_distribution: List[MFIMarketScoreDistributionEntry] = Field(
        default_factory=list
    )

    # Canonical structured narratives and verification.
    context_evidence: List[MFIContextEvidenceStatement] = Field(default_factory=list)
    dimension_narratives: Dict[str, MFIDimensionNarrative] = Field(
        default_factory=dict
    )
    market_narratives: Dict[str, MFIMarketNarrative] = Field(default_factory=dict)
    executive_summary_narrative: MFIExecutiveNarrative = Field(
        default_factory=MFIExecutiveNarrative
    )
    claim_validation: MFIClaimValidationResult = Field(
        default_factory=MFIClaimValidationResult
    )
    qa_review: MFIQAReview = Field(default_factory=MFIQAReview)
    
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
