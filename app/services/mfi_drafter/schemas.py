"""
MFI Drafter - Schemas
=====================
Classi e modelli per la generazione di MFI Reports.
"""
from __future__ import annotations

from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any, Literal
from dataclasses import dataclass, field, asdict

from app.shared.report_blocks import ReportBlock


# ============================================================================
# CONSTANTS
# ============================================================================

MFI_DIMENSIONS = [
    "Assortment",
    "Availability",
    "Price",
    "Resilience",
    "Competition",
    "Infrastructure",
    "Service",
    "Food Quality",
    "Access & Protection"
]

RISK_COLORS = {
    "Very High Risk": "#d62728",
    "High Risk": "#ff7f0e",
    "Medium Risk": "#ffbb78",
    "Low Risk": "#2ca02c"
}

DIMENSION_NAME_MAP = {
    "Quality": "Food Quality",
    "AccessProtection": "Access & Protection",
    "Assortment": "Assortment",
    "Availability": "Availability",
    "Price": "Price",
    "Resilience": "Resilience",
    "Competition": "Competition",
    "Infrastructure": "Infrastructure",
    "Service": "Service",
}

SCORE_VARIABLE_MAP = {
    "Assortment": "AssortmentScoreMFI",
    "Availability": "AvailabilityScoreMFI",
    "Price": "PriceScoreMFI",
    "Resilience": "ResilienceScoreMFI",
    "Competition": "CompetitionScoreMFI",
    "Infrastructure": "InfrastructureScoreMFI",
    "Service": "ServiceScoreMFI",
    "Quality": "QualityScoreMFI",
    "AccessProtection": "AccessProtectionScoreMFI",
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
    sub_scores: Dict[str, Dict[str, Any]]
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
    country_context: Optional[str] = None

    document_references: List[Dict[str, Any]] = []

    report_blocks: List[ReportBlock] = []
    
    # Visualizations (Base64)
    visualizations: Dict[str, str]
    
    # Control
    warnings: List[str] = []
    llm_calls: int = 0
    correction_attempts: int = 0
    success: bool = True


class MFIReportStatusOutput(BaseModel):
    """Status of an in-progress report."""
    run_id: str
    status: Literal["pending", "running", "completed", "failed"]
    current_node: Optional[str] = None
    progress_pct: int = 0
    warnings: List[str] = []
    metadata: Dict[str, Any] = {}
    error: Optional[str] = None
    traceback: Optional[str] = None