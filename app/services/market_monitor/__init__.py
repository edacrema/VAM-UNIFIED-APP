"""
Market Monitor Service
======================
Servizio per la generazione di Market Monitor Reports WFP.
"""
from .router import router
from .graph import (
    run_report_generation,
    build_graph,
    create_initial_state,
    AVAILABLE_MODULES,
    CURRENCY_SYMBOLS
)
from .schemas import (
    ModuleOutput,
    Document,
    Event,
    TrendAnalysis,
    SkepticFlag,
    DataStatistics,
    GenerateReportInput,
    GenerateReportOutput,
    ReportStatusOutput
)

__all__ = [
    # Router
    "router",
    
    # Graph
    "run_report_generation",
    "build_graph",
    "create_initial_state",
    "AVAILABLE_MODULES",
    "CURRENCY_SYMBOLS",
    
    # Schemas
    "ModuleOutput",
    "Document",
    "Event",
    "TrendAnalysis",
    "SkepticFlag",
    "DataStatistics",
    "GenerateReportInput",
    "GenerateReportOutput",
    "ReportStatusOutput"
]