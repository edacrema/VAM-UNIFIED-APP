"""MFI Drafter service package."""

from importlib import import_module

__all__ = [
    "router",
    "run_mfi_report_generation",
    "build_graph",
    "create_initial_state",
    "DIMENSION_DESCRIPTIONS",
    "MFI_DIMENSIONS",
    "RISK_COLORS",
    "get_risk_level",
    "Document",
    "MFIMarketData",
    "MFIDimensionScore",
    "DimensionFinding",
    "SkepticFlag",
    "SurveyMetadata",
    "GenerateMFIReportInput",
    "GenerateMFIReportOutput",
    "MFIReportStatusOutput",
    "MFIMetric",
    "MFIMetricSummary",
    "MFIMethodologyWarning",
    "MFIExcludedMarketRecord",
    "MFIAnalysisConfig",
    "MFICoverageSummary",
    "MFIStatisticalSummary",
    "MFIAnalyzedMetric",
    "MFILocalizedPatterns",
    "MFIDimensionProfile",
    "MFIMarketProfile",
    "MFIMetricLedgerEntry",
    "MFIDeterministicTables",
    "MFIAssessmentProfile",
    "MFIReleaseControl",
    "MFIGenerationDiagnostics",
    "build_assessment_profile",
]


def __getattr__(name: str):
    if name == "router":
        from .router import router

        return router
    if name in {
        "run_mfi_report_generation",
        "build_graph",
        "create_initial_state",
        "DIMENSION_DESCRIPTIONS",
    }:
        return getattr(import_module(".graph", __name__), name)
    if name == "build_assessment_profile":
        return getattr(import_module(".analysis", __name__), name)
    if name in {
        "MFI_DIMENSIONS",
        "RISK_COLORS",
        "get_risk_level",
        "Document",
        "MFIMarketData",
        "MFIDimensionScore",
        "DimensionFinding",
        "SkepticFlag",
        "SurveyMetadata",
        "GenerateMFIReportInput",
        "GenerateMFIReportOutput",
        "MFIReportStatusOutput",
        "MFIMetric",
        "MFIMetricSummary",
        "MFIMethodologyWarning",
        "MFIExcludedMarketRecord",
        "MFIAnalysisConfig",
        "MFICoverageSummary",
        "MFIStatisticalSummary",
        "MFIAnalyzedMetric",
        "MFILocalizedPatterns",
        "MFIDimensionProfile",
        "MFIMarketProfile",
        "MFIMetricLedgerEntry",
        "MFIDeterministicTables",
        "MFIAssessmentProfile",
        "MFIReleaseControl",
        "MFIGenerationDiagnostics",
    }:
        return getattr(import_module(".schemas", __name__), name)
    raise AttributeError(name)
