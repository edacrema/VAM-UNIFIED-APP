"""
Market Monitor - Schemas
========================
Classi e modelli per la generazione di Market Monitor Reports.
"""
from __future__ import annotations

from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any, Literal
from dataclasses import dataclass, field, asdict

from app.shared.report_blocks import ReportBlock


# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class ModuleOutput:
    """Output standardizzato di un modulo opzionale."""
    section_title: str
    narrative: str
    visualization: Optional[str] = None  # Base64 encoded image
    key_metrics: Dict[str, Any] = field(default_factory=dict)
    data_for_qa: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)


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
class Event:
    """Evento estratto dai documenti."""
    event_id: str
    category: Literal["political", "economic", "climate", "security", "logistics", "agriculture", "other"]
    statement: str
    source_ids: List[str]
    location: str
    date: str

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class TrendAnalysis:
    """Analisi del trend di mercato."""
    trajectory: Literal["increasing_prices", "decreasing_prices", "stable", "volatile"]
    key_market_drivers: List[str]
    commodity_analysis: Dict[str, str]
    regional_analysis: Dict[str, str]
    outlook: str

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class SkepticFlag:
    """Flag sollevato dal Red Team QA."""
    section: str
    claim: str
    issue_type: Literal["numeracy_error", "contradiction", "source_mismatch", 
                        "unsupported_speculation", "hedging", "template_violation"]
    severity: Literal["high", "medium", "low"]
    details: str
    recommendation: str

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class DataStatistics:
    """Statistiche calcolate sui dati."""
    food_basket: Dict[str, Any] = field(default_factory=dict)
    commodities: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    auxiliary: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)


# ============================================================================
# PYDANTIC MODELS (API)
# ============================================================================

class GenerateReportInput(BaseModel):
    """Input for generating a report."""
    country: str = Field(..., description="Country name (e.g., 'Sudan', 'Yemen')")
    time_period: str = Field(..., description="Period in YYYY-MM format (e.g., '2025-01')")

    news_start_date: Optional[str] = Field(
        default=None,
        description="Optional start date for news retrieval (YYYY-MM-DD). If provided, must be paired with news_end_date."
    )
    news_end_date: Optional[str] = Field(
        default=None,
        description="Optional end date for news retrieval (YYYY-MM-DD). If provided, must be paired with news_start_date."
    )

    commodity_list: List[str] = Field(
        default=[],
        description="Additional commodities to analyze. Active basket commodities are always included."
    )
    basket_version_id: Optional[str] = Field(
        default=None,
        description="Active country food basket version selected by the UI. Stale versions are rejected."
    )
    admin1_list: List[str] = Field(
        default=[],
        description="List of Admin1 regions to include"
    )
    currency_code: str = Field(
        default="USD",
        description="ISO 4217 currency code (e.g., 'SDG', 'YER')"
    )
    enabled_modules: List[str] = Field(
        default=[],
        description=(
            "Optional modules to enable "
            "(exchange_rate uses DataBridges FX first, fuel_energy uses transport fuel price series, "
            "livestock_animal_products uses animal product price series, labour_market uses wage series)"
        )
    )
    previous_report_text: str = Field(
        default="",
        description="Previous report text for context"
    )

    use_mock_data: bool = Field(
        default=False,
        description="If True, use mock numeric datasets instead of real APIs (Seerist/ReliefWeb news retrieval is always real)"
    )


class GenerateReportOutput(BaseModel):
    """Output of the report generation."""
    run_id: str
    country: str
    time_period: str
    report_sections: Dict[str, str]
    report_blocks: List[ReportBlock] = []
    visualizations: Dict[str, str]  # Base64 encoded images
    data_statistics: Dict[str, Any]
    trend_analysis: Optional[Dict[str, Any]] = None
    events: List[Dict[str, Any]] = []
    module_sections: Dict[str, str] = {}
    document_references: List[Dict[str, Any]] = []
    news_counts: Dict[str, int] = {}
    cache_metadata: Dict[str, Any] = {}
    food_basket: Dict[str, Any] = {}
    fuel_energy_data: Optional[Dict[str, Any]] = None
    livestock_animal_products_data: Optional[Dict[str, Any]] = None
    labour_market_data: Optional[Dict[str, Any]] = None
    warnings: List[str] = []
    llm_calls: int = 0
    success: bool = True


class ReportStatusOutput(BaseModel):
    """Status of an in-progress report."""
    run_id: str
    status: Literal["pending", "running", "completed", "failed"]
    current_node: Optional[str] = None
    progress_pct: int = 0
    warnings: List[str] = []
    metadata: Dict[str, Any] = {}
    error: Optional[str] = None
    traceback: Optional[str] = None
