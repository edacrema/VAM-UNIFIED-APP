"""Local Phase 4 regression, comparison, and release-readiness tooling.

This module deliberately never deploys, changes feature configuration, calls
an LLM, invokes a retriever, or imports the legacy MFI implementation. Generated
assessment evidence belongs under the repository's ignored ``.tmp`` directory.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import html
import json
import math
import subprocess
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Literal, Mapping, Optional, Sequence

import pandas as pd
from pydantic import BaseModel, Field, model_validator

from app.shared.docx_export import build_docx_bytes_from_report_blocks
from app.shared.llm_observability import LLMRunDiagnostics
from app.shared.report_blocks import (
    ReportBlock,
    build_mfi_report_blocks,
    resolve_mfi_report_blocks,
)

from .analysis import MFIAnalysisConfig, build_assessment_profile
from .context_status import not_attempted_context_status
from .data_loader import load_mfi_from_csv
from .features import MFI_DRAFTER_SERVICE_NAME
from .graph import node_mfi_graph_designer
from .methodology import OFFICIAL_SCORE_DEFINITIONS
from .narrative import (
    build_claim_catalog,
    build_qa_review,
    deduplicate_dimension_recommendations,
    validate_structured_narratives,
)
from .offline_narrative_fixtures import (
    build_offline_dimension_fixture,
    build_offline_executive_fixture,
    build_offline_market_fixture,
)


RELEASE_MANIFEST_SCHEMA_VERSION = "1.0"
LEGACY_REFERENCE_COMMIT = "524cc9a"
DEFAULT_OUTPUT_ROOT = Path(".tmp") / "mfi-phase4"
# Limitations every complete assessment is expected to carry. Since R1,
# "unavailable_explanatory_evidence" is raised only for genuinely unusable required
# evidence, so a healthy assessment must not be required to produce it.
EXPECTED_LIMITATION_CODES = {
    "assessment_scope_not_representative",
    "item_trader_denominator_unavailable",
}
HARD_METHODOLOGY_WARNING_CODES = {
    "dimension_formula_mismatch",
    "overall_formula_mismatch",
    "duplicate_level_1_score",
    "partial_full_mfi_record",
    "invalid_level_1_score",
}
FORBIDDEN_PRESENTATION_TERMS = (
    "risk distribution",
    "risk level",
    "national mfi",
    "national score",
)


class MFIReleaseValidationCheck(BaseModel):
    check_id: str
    status: Literal["passed", "failed", "advisory"]
    blocking: bool = True
    message: str
    evidence: dict[str, Any] = Field(default_factory=dict)


class MFIReleaseArtifact(BaseModel):
    artifact_id: str
    path: str
    sha256: str
    byte_count: int
    kind: str


class MFIExpectedMetricAssertion(BaseModel):
    """Optional case-specific assertion expressed without country semantics."""

    assertion_id: str = Field(pattern=r"^[a-z0-9][a-z0-9._-]*$")
    dimension: str
    metric_id: str
    statistic: str = "mean_normalized_value"
    expected_value: float
    tolerance: float = Field(default=1e-6, gt=0)


class MFIRegressionCaseConfig(BaseModel):
    """One locally configured regression fixture.

    ``source_csv`` is resolved relative to the configuration file that contains
    it. The application has no built-in country cases or dataset paths.
    """

    case_id: str = Field(pattern=r"^[a-z0-9][a-z0-9._-]*$")
    label: str
    source_csv: str
    expected_included_market_count: int = Field(ge=1)
    expected_excluded_market_count: int = Field(default=0, ge=0)
    expected_priority_dimensions: list[str]
    expected_methodology_warning_codes: list[str] = Field(default_factory=list)
    expected_limitation_codes: list[str] = Field(
        default_factory=lambda: sorted(EXPECTED_LIMITATION_CODES)
    )
    metric_assertions: list[MFIExpectedMetricAssertion] = Field(
        default_factory=list
    )
    required_for_release: bool = True
    requires_live_pilot: bool = True

    @model_validator(mode="after")
    def validate_assertion_ids(self) -> "MFIRegressionCaseConfig":
        assertion_ids = [
            assertion.assertion_id for assertion in self.metric_assertions
        ]
        if len(assertion_ids) != len(set(assertion_ids)):
            raise ValueError(
                f"Duplicate metric assertion ID in case {self.case_id!r}."
            )
        return self


class MFIReleaseValidationConfig(BaseModel):
    """Portable release-validation inputs; normally kept local and untracked."""

    schema_version: Literal["1.0"] = "1.0"
    cases: list[MFIRegressionCaseConfig] = Field(default_factory=list)
    required_real_pilot_count: int = Field(default=3, ge=0)

    @model_validator(mode="after")
    def validate_case_ids(self) -> "MFIReleaseValidationConfig":
        case_ids = [case.case_id for case in self.cases]
        if len(case_ids) != len(set(case_ids)):
            raise ValueError("Regression case IDs must be unique.")
        return self


class MFIReleaseRequirements(BaseModel):
    required_case_ids: list[str] = Field(default_factory=list)
    required_live_pilot_case_ids: list[str] = Field(default_factory=list)
    required_real_pilot_count: int = Field(default=3, ge=0)

    @model_validator(mode="after")
    def validate_case_requirements(self) -> "MFIReleaseRequirements":
        required = self.required_case_ids
        live = self.required_live_pilot_case_ids
        if len(required) != len(set(required)):
            raise ValueError("Required regression case IDs must be unique.")
        if len(live) != len(set(live)):
            raise ValueError("Required live-pilot case IDs must be unique.")
        if not set(live) <= set(required):
            raise ValueError(
                "Live-pilot case IDs must also be required regression cases."
            )
        return self


class MFIRegressionCaseEvidence(BaseModel):
    case_id: str
    label: str
    source_sha256: str
    checks: list[MFIReleaseValidationCheck] = Field(default_factory=list)
    artifacts: list[MFIReleaseArtifact] = Field(default_factory=list)
    methodology_warning_codes: list[str] = Field(default_factory=list)
    limitation_codes: list[str] = Field(default_factory=list)


class MFIHumanApproval(BaseModel):
    case_id: str
    reviewer: str
    reviewed_at: str
    analytical_status: Literal["approved", "rejected"]
    visual_status: Literal["approved", "rejected"]
    accepted_medium_flag_ids: list[str] = Field(default_factory=list)
    warning_dispositions: dict[str, str] = Field(default_factory=dict)
    artifact_sha256: dict[str, str] = Field(default_factory=dict)
    notes: Optional[str] = None


class MFIPilotRunEvidence(BaseModel):
    run_id: str
    kind: Literal["regression_case", "real"]
    case_id: Optional[str] = None
    completed_async: bool
    analysis_version: str
    llm_calls: int
    docx_exported: bool
    preview_rendered: bool
    score_integrity_passed: bool
    fallback_used: bool
    unresolved_high_count: int = 0
    unresolved_medium_flag_ids: list[str] = Field(default_factory=list)
    accepted_medium_flag_ids: list[str] = Field(default_factory=list)
    human_status: Literal["approved", "rejected", "pending"] = "pending"

    @model_validator(mode="after")
    def validate_case_context(self) -> "MFIPilotRunEvidence":
        if self.kind == "regression_case" and not self.case_id:
            raise ValueError(
                "Regression-case pilot evidence requires a case_id."
            )
        if self.kind == "real" and self.case_id is not None:
            raise ValueError("Real pilot evidence must not declare a case_id.")
        return self


class MFIReleaseEvidenceManifest(BaseModel):
    schema_version: Literal["1.0"] = RELEASE_MANIFEST_SCHEMA_VERSION
    release_id: str
    mode: Literal["regression", "compare"]
    generated_at: str
    candidate_revision: str
    baseline_revision: str = LEGACY_REFERENCE_COMMIT
    service_name: str = MFI_DRAFTER_SERVICE_NAME
    model_identifier: Optional[str] = None
    validation_config_sha256: Optional[str] = None
    requirements: MFIReleaseRequirements = Field(
        default_factory=MFIReleaseRequirements
    )
    regression_cases: list[MFIRegressionCaseEvidence] = Field(
        default_factory=list
    )
    comparison_checks: list[MFIReleaseValidationCheck] = Field(
        default_factory=list
    )
    artifacts: list[MFIReleaseArtifact] = Field(default_factory=list)
    approvals: list[MFIHumanApproval] = Field(default_factory=list)
    pilot_runs: list[MFIPilotRunEvidence] = Field(default_factory=list)
    blockers: list[str] = Field(default_factory=list)
    release_ready: bool = False


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _current_revision() -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return "unknown"
    return result.stdout.strip() or "unknown"


def _sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _artifact(path: Path, *, root: Path, artifact_id: str, kind: str) -> MFIReleaseArtifact:
    return MFIReleaseArtifact(
        artifact_id=artifact_id,
        path=path.resolve().relative_to(root.resolve()).as_posix(),
        sha256=_sha256_path(path),
        byte_count=path.stat().st_size,
        kind=kind,
    )


def _check(
    check_id: str,
    passed: bool,
    message: str,
    *,
    evidence: Optional[dict[str, Any]] = None,
    blocking: bool = True,
    advisory: bool = False,
) -> MFIReleaseValidationCheck:
    status: Literal["passed", "failed", "advisory"]
    status = "advisory" if advisory else "passed" if passed else "failed"
    return MFIReleaseValidationCheck(
        check_id=check_id,
        status=status,
        blocking=blocking,
        message=message,
        evidence=evidence or {},
    )


def _write_json(path: Path, value: Any) -> None:
    if isinstance(value, BaseModel):
        value = value.model_dump(mode="json")
    path.write_text(
        json.dumps(value, indent=2, ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )


def _iter_metric_groups(
    markets_data: Sequence[Mapping[str, Any]],
    field: Literal["subsections", "drivers"],
) -> Iterable[Mapping[str, Any]]:
    for market in markets_data:
        groups = market.get(field) or {}
        if not isinstance(groups, Mapping):
            continue
        for metrics in groups.values():
            if not isinstance(metrics, list):
                continue
            for metric in metrics:
                if isinstance(metric, Mapping):
                    yield metric


def _deterministic_result(
    csv_path: Path,
) -> tuple[dict[str, Any], list[ReportBlock], bytes]:
    loaded = load_mfi_from_csv(csv_path)
    profile_model = build_assessment_profile(
        loaded["markets_data"],
        loaded["metric_summaries"],
        loaded,
    )
    profile = profile_model.model_dump()
    catalog = build_claim_catalog(profile)
    dimension_narratives = {
        item["dimension"]: build_offline_dimension_fixture(
            item,
            assessment_profile=profile,
        )
        for item in profile["dimensions"]
    }
    dimension_narratives = deduplicate_dimension_recommendations(
        dimension_narratives,
        profile["dimensions"],
    )
    market_narratives = {
        item["market_name"]: build_offline_market_fixture(item)
        for item in profile["markets"]
        if item["is_priority_market"]
    }
    executive = build_offline_executive_fixture(profile)
    (
        validation,
        dimension_narratives,
        market_narratives,
        executive,
        context,
        flag_payload,
    ) = validate_structured_narratives(
        context_evidence=[],
        dimension_narratives=dimension_narratives,
        market_narratives=market_narratives,
        executive_narrative=executive,
        claim_catalog=catalog,
        assessment_profile=profile,
        documents=[],
    )
    flags = flag_payload.get("flags", [])
    severity_counts = Counter(
        str(flag.get("severity"))
        for flag in flags
        if isinstance(flag, Mapping)
    )
    diagnostics = {
        "fallback_policy": "offline_fixture",
        "dimensions": {
            "llm": [],
            "fallback": sorted(dimension_narratives, key=str.casefold),
        },
        "markets": {
            "llm": [],
            "fallback": sorted(market_narratives, key=str.casefold),
        },
        "context_extraction_mode": "not_applicable",
        "context_classification_status": "not_attempted",
        "executive_summary_mode": "fallback",
        "red_team_status": "not_started",
        "correction_attempts": 0,
        "unresolved_high_count": severity_counts.get("high", 0),
        "unresolved_medium_count": severity_counts.get("medium", 0),
        "unresolved_low_count": severity_counts.get("low", 0),
        "retrievers": {},
        "delivery_contract_status": "validated",
    }
    result = {
        **loaded,
        "release_control": {
            "analysis_version": "2",
            "enabled": True,
            "configuration_status": "configured",
            "service_name": "mfi-drafter-release-validation",
            "deployment_revision": _current_revision(),
        },
        "generation_diagnostics": diagnostics,
        "mean_mfi_across_assessed_markets": (
            profile["mean_mfi_across_assessed_markets"]
        ),
        "assessment_profile": profile,
        "claim_catalog": catalog,
        "market_score_distribution": [
            {
                "market_name": market["market_name"],
                "overall_mfi": market["overall_mfi"],
                "score_rank": market["score_rank"],
                "selection_order": market["selection_order"],
                "is_priority_market": market["is_priority_market"],
            }
            for market in profile["markets"]
        ],
        "context_status": not_attempted_context_status().model_dump(),
        "context_evidence": context,
        "dimension_narratives": dimension_narratives,
        "market_narratives": market_narratives,
        "executive_summary_narrative": executive,
        "claim_validation": validation,
        "qa_review": build_qa_review(
            flags,
            [],
            correction_attempts=0,
        ),
        "document_references": [],
        "warnings": [
            "Deterministic Phase 4 regression rendering; no LLM or retriever "
            "was invoked."
        ],
        "llm_calls": 0,
        "llm_diagnostics": LLMRunDiagnostics(
            service="mfi-drafter",
            run_id=str(loaded.get("run_id") or "deterministic-release-validation"),
        ).model_dump(mode="json"),
        "correction_attempts": 0,
    }
    visualizations = node_mfi_graph_designer(result).get("visualizations", {})
    result["visualizations"] = visualizations
    blocks = build_mfi_report_blocks(result)
    result["report_blocks"] = [block.model_dump() for block in blocks]
    docx = build_docx_bytes_from_report_blocks(
        blocks,
        visualizations=visualizations,
    )
    return result, blocks, docx


def _official_scores_match(
    csv_path: Path,
    markets_data: Sequence[Mapping[str, Any]],
) -> tuple[bool, list[dict[str, Any]]]:
    frame = pd.read_csv(csv_path)
    frame["VariableName"] = frame["VariableName"].astype("string").str.strip()
    frame["DimensionName"] = frame["DimensionName"].astype("string").str.strip()
    frame["MarketName"] = frame["MarketName"].astype("string").str.strip()
    frame["LevelID"] = pd.to_numeric(frame["LevelID"], errors="coerce")
    frame["OutputValue"] = pd.to_numeric(frame["OutputValue"], errors="coerce")
    source_by_market = {
        str(name): group for name, group in frame.groupby("MarketName", sort=False)
    }
    mismatches: list[dict[str, Any]] = []
    for market in markets_data:
        market_name = str(market.get("market_name"))
        source = source_by_market.get(market_name)
        if source is None:
            mismatches.append({"market": market_name, "reason": "missing_source"})
            continue
        for definition in OFFICIAL_SCORE_DEFINITIONS:
            rows = source.loc[
                (source["LevelID"] == 1)
                & (source["DimensionName"] == definition.csv_dimension)
                & (source["VariableName"] == definition.variable_name),
                "OutputValue",
            ]
            if len(rows) != 1:
                mismatches.append(
                    {
                        "market": market_name,
                        "dimension": definition.dimension,
                        "reason": f"source_row_count_{len(rows)}",
                    }
                )
                continue
            expected = float(rows.iloc[0])
            actual = (
                market.get("overall_mfi")
                if definition.dimension == "MFI"
                else (market.get("dimension_scores") or {}).get(
                    definition.dimension
                )
            )
            if actual != expected:
                mismatches.append(
                    {
                        "market": market_name,
                        "dimension": definition.dimension,
                        "expected": expected,
                        "actual": actual,
                    }
                )
    return not mismatches, mismatches


def _ledger_links_valid(profile: Mapping[str, Any]) -> tuple[bool, list[str]]:
    ledger = profile.get("metric_ledger") or {}
    missing: list[str] = []
    for rows in (profile.get("tables") or {}).values():
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, Mapping):
                continue
            for ledger_id in row.get("ledger_metric_ids", []) or []:
                if ledger_id not in ledger:
                    missing.append(str(ledger_id))
    return not missing, sorted(set(missing))


def _presentation_text(result: Mapping[str, Any]) -> str:
    parts: list[str] = []

    def visit(value: Any) -> None:
        if isinstance(value, Mapping):
            for nested in value.values():
                visit(nested)
        elif isinstance(value, list):
            for item in value:
                visit(item)
        elif isinstance(value, str):
            parts.append(value)

    visit(
        {
            "dimension_narratives": result.get("dimension_narratives", {}),
            "market_narratives": result.get("market_narratives", {}),
            "executive_summary_narrative": result.get(
                "executive_summary_narrative", {}
            ),
            "report_blocks": result.get("report_blocks", []),
        }
    )
    return "\n".join(parts).casefold()


def _regression_case_checks(
    case: MFIRegressionCaseConfig,
    csv_path: Path,
    result: Mapping[str, Any],
    blocks: Sequence[ReportBlock],
    docx: bytes,
) -> list[MFIReleaseValidationCheck]:
    case_id = case.case_id
    checks: list[MFIReleaseValidationCheck] = []
    markets_data = result.get("markets_data") or []
    profile = result.get("assessment_profile") or {}
    warnings = result.get("methodology_warnings") or []
    warning_codes = sorted(
        str(item.get("code"))
        for item in warnings
        if isinstance(item, Mapping) and item.get("code")
    )

    checks.append(
        _check(
            "included_market_count",
            len(markets_data) == case.expected_included_market_count,
            f"{case_id}: included Full MFI market count",
            evidence={
                "expected": case.expected_included_market_count,
                "actual": len(markets_data),
            },
        )
    )
    exclusions = result.get("excluded_market_records") or []
    checks.append(
        _check(
            "excluded_market_count",
            len(exclusions) == case.expected_excluded_market_count,
            f"{case_id}: MFIr-only exclusion count",
            evidence={
                "expected": case.expected_excluded_market_count,
                "actual": len(exclusions),
            },
        )
    )
    priorities = profile.get("priority_dimension_names") or []
    checks.append(
        _check(
            "priority_dimensions",
            priorities == case.expected_priority_dimensions,
            f"{case_id}: deterministic priority dimensions",
            evidence={
                "expected": case.expected_priority_dimensions,
                "actual": priorities,
            },
        )
    )
    exact_scores, score_mismatches = _official_scores_match(
        csv_path, markets_data
    )
    checks.append(
        _check(
            "official_score_identity",
            exact_scores,
            f"{case_id}: parsed DataBridge Level-1 scores are unchanged",
            evidence={"mismatches": score_mismatches[:20]},
        )
    )
    hard_warnings = sorted(
        set(warning_codes) & HARD_METHODOLOGY_WARNING_CODES
    )
    checks.append(
        _check(
            "formula_validation",
            not hard_warnings,
            f"{case_id}: formula validation produced no blocking warning",
            evidence={"blocking_warning_codes": hard_warnings},
        )
    )
    checks.append(
        _check(
            "expected_methodology_warnings",
            warning_codes == case.expected_methodology_warning_codes,
            f"{case_id}: methodology warning set is expected",
            evidence={
                "expected": case.expected_methodology_warning_codes,
                "actual": warning_codes,
            },
        )
    )

    competition = [
        metric
        for metric in _iter_metric_groups(markets_data, "drivers")
        if metric.get("dimension") == "Competition"
    ]
    checks.append(
        _check(
            "competition_polarity",
            bool(competition)
            and all(
                metric.get("orientation") == "higher_is_better"
                for metric in competition
            ),
            f"{case_id}: Competition evidence polarity is favorable",
        )
    )
    quality = [
        metric
        for metric in _iter_metric_groups(markets_data, "drivers")
        if metric.get("dimension") == "Food Quality"
    ]
    quality_null_aware = all(
        metric.get("raw_value") is None
        for metric in quality
        if metric.get("applicability_status") == "not_applicable"
    )
    checks.append(
        _check(
            "quality_applicability",
            quality_null_aware,
            f"{case_id}: Quality not-applicable evidence remains null",
        )
    )
    separation_valid = all(
        (metric.get("role") != "item" or ".item." in str(metric.get("metric_id")))
        and (
            metric.get("role") != "category"
            or ".category." in str(metric.get("metric_id"))
        )
        for metric in _iter_metric_groups(markets_data, "drivers")
        if metric.get("role") in {"item", "category"}
    )
    checks.append(
        _check(
            "category_item_separation",
            separation_valid,
            f"{case_id}: category and item populations remain separate",
        )
    )
    all_drivers = {
        metric["metric_id"]: metric
        for dimension in profile.get("dimensions", []) or []
        for metric in dimension.get("drivers", []) or []
        if isinstance(metric, Mapping) and metric.get("metric_id")
    }
    required_market_count = max(
        3,
        math.ceil(
            MFIAnalysisConfig().item_min_market_ratio
            * case.expected_included_market_count
        ),
    )
    relevant_items_valid = True
    item_failures: list[str] = []
    for metric in all_drivers.values():
        if not metric.get("item_relevant"):
            continue
        category = all_drivers.get(metric.get("matching_category_metric_id"))
        coverage = (metric.get("coverage") or {}).get(
            "available_market_count", 0
        )
        contrast = (
            float(metric["unfavorable_rate"])
            - float(category["unfavorable_rate"])
            if category
            and metric.get("unfavorable_rate") is not None
            and category.get("unfavorable_rate") is not None
            else -1.0
        )
        if (
            coverage < required_market_count
            or contrast < MFIAnalysisConfig().item_category_contrast - 1e-6
        ):
            relevant_items_valid = False
            item_failures.append(str(metric["metric_id"]))
    checks.append(
        _check(
            "relevant_item_eligibility",
            relevant_items_valid,
            f"{case_id}: relevant items meet coverage and contrast gates",
            evidence={"failures": item_failures},
        )
    )
    ledger_valid, missing_ledger_ids = _ledger_links_valid(profile)
    checks.append(
        _check(
            "ledger_referential_integrity",
            ledger_valid,
            f"{case_id}: deterministic tables reference existing ledger IDs",
            evidence={"missing_ledger_ids": missing_ledger_ids},
        )
    )
    checks.append(
        _check(
            "claim_validation",
            (result.get("claim_validation") or {}).get("status") == "passed",
            f"{case_id}: deterministic fallback claims validate",
            evidence={"validation": result.get("claim_validation")},
        )
    )
    heading_counts = Counter(
        block.text
        for block in blocks
        if block.type == "heading" and block.text
    )
    dimensions_once = all(
        heading_counts.get(dimension, 0) == 1
        for dimension in (
            "Assortment",
            "Availability",
            "Price",
            "Resilience",
            "Competition",
            "Infrastructure",
            "Service",
            "Food Quality",
            "Access & Protection",
        )
    )
    checks.append(
        _check(
            "report_hierarchy",
            dimensions_once,
            f"{case_id}: all nine dimension sections appear exactly once",
        )
    )
    visualizations = result.get("visualizations") or {}
    readable_images = True
    for value in visualizations.values():
        try:
            decoded = base64.b64decode(str(value), validate=True)
        except Exception:
            readable_images = False
            break
        if not decoded.startswith(b"\x89PNG"):
            readable_images = False
            break
    checks.append(
        _check(
            "visualizations",
            bool(visualizations.get("mfi_radar"))
            and bool(visualizations.get("market_score_ranking"))
            and "risk_distribution" not in visualizations
            and readable_images,
            f"{case_id}: required neutral PNG visualizations render",
            evidence={"figure_ids": sorted(visualizations)},
        )
    )
    presentation = _presentation_text(result)
    found_terms = [
        term for term in FORBIDDEN_PRESENTATION_TERMS if term in presentation
    ]
    checks.append(
        _check(
            "presentation_terminology",
            not found_terms,
            f"{case_id}: presentation avoids unsupported terminology",
            evidence={"found_terms": found_terms},
        )
    )
    checks.append(
        _check(
            "docx_export",
            docx.startswith(b"PK") and len(docx) > 1000,
            f"{case_id}: DOCX export is a readable package",
            evidence={"byte_count": len(docx)},
        )
    )

    limitations = profile.get("limitations") or []
    limitation_codes = {
        str(item.get("code"))
        for item in limitations
        if isinstance(item, Mapping) and item.get("code")
    }
    expected_limitations = set(case.expected_limitation_codes)
    checks.append(
        _check(
            "expected_limitations",
            expected_limitations <= limitation_codes,
            f"{case_id}: required limitations are disclosed",
            evidence={
                "expected": sorted(expected_limitations),
                "actual": sorted(limitation_codes),
            },
        )
    )
    dimensions = {
        str(item.get("dimension")): item
        for item in profile.get("dimensions", [])
        if isinstance(item, Mapping)
    }
    for assertion in case.metric_assertions:
        dimension = dimensions.get(assertion.dimension) or {}
        metrics = [
            metric
            for field in ("subsections", "drivers")
            for metric in dimension.get(field, []) or []
            if isinstance(metric, Mapping)
            and metric.get("metric_id") == assertion.metric_id
        ]
        actual = (
            metrics[0].get(assertion.statistic)
            if len(metrics) == 1
            else None
        )
        assertion_valid = (
            actual is not None
            and math.isclose(
                float(actual),
                assertion.expected_value,
                abs_tol=assertion.tolerance,
            )
        )
        checks.append(
            _check(
                f"metric_assertion:{assertion.assertion_id}",
                assertion_valid,
                (
                    f"{case_id}: configured metric assertion "
                    f"{assertion.assertion_id}"
                ),
                evidence={
                    "dimension": assertion.dimension,
                    "metric_id": assertion.metric_id,
                    "statistic": assertion.statistic,
                    "expected": assertion.expected_value,
                    "actual": actual,
                    "tolerance": assertion.tolerance,
                    "matching_metric_count": len(metrics),
                },
            )
        )
    return checks


def _blocking_failures(
    checks: Iterable[MFIReleaseValidationCheck],
) -> list[str]:
    return [
        check.check_id
        for check in checks
        if check.blocking and check.status == "failed"
    ]


def _pilot_qualifies(run: MFIPilotRunEvidence) -> bool:
    return (
        run.completed_async
        and run.analysis_version == "2"
        and run.llm_calls > 0
        and run.docx_exported
        and run.preview_rendered
        and run.score_integrity_passed
        and not run.fallback_used
        and run.unresolved_high_count == 0
        and set(run.unresolved_medium_flag_ids)
        <= set(run.accepted_medium_flag_ids)
        and run.human_status == "approved"
    )


def evaluate_release_readiness(
    manifest: MFIReleaseEvidenceManifest,
) -> MFIReleaseEvidenceManifest:
    blockers: list[str] = []
    required_case_ids = set(manifest.requirements.required_case_ids)
    all_checks = [
        check
        for case in manifest.regression_cases
        if case.case_id in required_case_ids
        for check in case.checks
    ] + list(manifest.comparison_checks)
    blockers.extend(
        f"check:{check_id}" for check_id in _blocking_failures(all_checks)
    )
    if manifest.baseline_revision != LEGACY_REFERENCE_COMMIT:
        blockers.append("baseline_revision")
    if not manifest.validation_config_sha256:
        blockers.append("validation_config")
    case_by_id = {
        case.case_id: case for case in manifest.regression_cases
    }
    if not required_case_ids:
        blockers.append("regression:no_required_cases_configured")
    for case_id in required_case_ids:
        if case_id not in case_by_id:
            blockers.append(f"regression:{case_id}")

    approved: set[str] = set()
    known_hashes = {
        artifact.sha256
        for case in manifest.regression_cases
        for artifact in case.artifacts
    } | {artifact.sha256 for artifact in manifest.artifacts}
    for approval in manifest.approvals:
        case = case_by_id.get(approval.case_id)
        expected_dispositions = (
            set(case.methodology_warning_codes)
            | set(case.limitation_codes)
            if case is not None
            else set()
        )
        hashes_valid = bool(approval.artifact_sha256) and all(
            value in known_hashes for value in approval.artifact_sha256.values()
        )
        dispositions_valid = expected_dispositions <= set(
            approval.warning_dispositions
        )
        if (
            approval.analytical_status == "approved"
            and approval.visual_status == "approved"
            and hashes_valid
            and dispositions_valid
        ):
            approved.add(approval.case_id)
        elif (
            approval.analytical_status == "approved"
            and approval.case_id in required_case_ids
        ):
            blockers.append(f"approval_evidence:{approval.case_id}")
    for case_id in required_case_ids:
        if case_id not in approved:
            blockers.append(f"approval:{case_id}")

    qualified = [run for run in manifest.pilot_runs if _pilot_qualifies(run)]
    qualified_case_ids = {
        str(run.case_id)
        for run in qualified
        if run.kind == "regression_case" and run.case_id
    }
    for case_id in manifest.requirements.required_live_pilot_case_ids:
        if case_id not in qualified_case_ids:
            blockers.append(f"pilot:regression_case:{case_id}")
    real_pilot_count = sum(run.kind == "real" for run in qualified)
    if real_pilot_count < manifest.requirements.required_real_pilot_count:
        blockers.append("pilot:real_assessments")

    manifest.blockers = sorted(set(blockers))
    manifest.release_ready = not manifest.blockers
    return manifest


def run_regression(
    *,
    validation_config: MFIReleaseValidationConfig,
    case_root: Path,
    output_directory: Path,
    release_id: str,
    candidate_revision: Optional[str] = None,
    validation_config_sha256: Optional[str] = None,
) -> MFIReleaseEvidenceManifest:
    output_directory.mkdir(parents=True, exist_ok=True)
    case_evidence: list[MFIRegressionCaseEvidence] = []
    for case in validation_config.cases:
        configured_path = Path(case.source_csv)
        csv_path = (
            configured_path
            if configured_path.is_absolute()
            else case_root / configured_path
        )
        if not csv_path.is_file():
            raise FileNotFoundError(
                f"Configured local Phase 4 regression case is absent: {csv_path}"
            )
        case_output = output_directory / case.case_id
        case_output.mkdir(parents=True, exist_ok=True)
        result, blocks, docx = _deterministic_result(csv_path)
        result_path = case_output / "v2-deterministic-result.json"
        docx_path = case_output / "v2-deterministic-report.docx"
        _write_json(result_path, result)
        docx_path.write_bytes(docx)
        checks = _regression_case_checks(
            case,
            csv_path,
            result,
            blocks,
            docx,
        )
        warning_codes = sorted(
            str(item.get("code"))
            for item in result.get("methodology_warnings", [])
            if isinstance(item, Mapping) and item.get("code")
        )
        limitation_codes = sorted(
            {
                str(item.get("code"))
                for item in (
                    result.get("assessment_profile", {}).get(
                        "limitations", []
                    )
                )
                if isinstance(item, Mapping) and item.get("code")
            }
        )
        case_evidence.append(
            MFIRegressionCaseEvidence(
                case_id=case.case_id,
                label=case.label,
                source_sha256=_sha256_path(csv_path),
                checks=checks,
                artifacts=[
                    _artifact(
                        result_path,
                        root=output_directory,
                        artifact_id=f"{case.case_id}.v2.result",
                        kind="application/json",
                    ),
                    _artifact(
                        docx_path,
                        root=output_directory,
                        artifact_id=f"{case.case_id}.v2.docx",
                        kind=(
                            "application/vnd.openxmlformats-officedocument."
                            "wordprocessingml.document"
                        ),
                    ),
                ],
                methodology_warning_codes=warning_codes,
                limitation_codes=limitation_codes,
            )
        )
    required_case_ids = [
        case.case_id
        for case in validation_config.cases
        if case.required_for_release
    ]
    required_live_pilot_case_ids = [
        case.case_id
        for case in validation_config.cases
        if case.required_for_release and case.requires_live_pilot
    ]
    config_hash = validation_config_sha256 or _sha256_bytes(
        validation_config.model_dump_json(
            exclude_none=True,
            by_alias=True,
        ).encode("utf-8")
    )
    manifest = MFIReleaseEvidenceManifest(
        release_id=release_id,
        mode="regression",
        generated_at=_utc_now(),
        candidate_revision=candidate_revision or _current_revision(),
        validation_config_sha256=config_hash,
        requirements=MFIReleaseRequirements(
            required_case_ids=required_case_ids,
            required_live_pilot_case_ids=required_live_pilot_case_ids,
            required_real_pilot_count=(
                validation_config.required_real_pilot_count
            ),
        ),
        regression_cases=case_evidence,
    )
    evaluate_release_readiness(manifest)
    manifest_path = output_directory / "manifest.json"
    _write_json(manifest_path, manifest)
    return manifest


def _load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"Expected a JSON object: {path}")
    return value


def _load_optional_models(
    path: Optional[Path],
    model_type: type[BaseModel],
) -> list[Any]:
    if path is None:
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, Mapping):
        payload = payload.get("items", [])
    if not isinstance(payload, list):
        raise ValueError(f"Expected a JSON list or items object: {path}")
    return [model_type.model_validate(item) for item in payload]


def run_compare(
    *,
    legacy_result_path: Path,
    current_result_path: Path,
    output_directory: Path,
    release_id: str,
    legacy_docx_path: Optional[Path] = None,
    current_docx_path: Optional[Path] = None,
    legacy_preview_path: Optional[Path] = None,
    current_preview_path: Optional[Path] = None,
    approvals_path: Optional[Path] = None,
    pilot_runs_path: Optional[Path] = None,
    regression_manifest_path: Optional[Path] = None,
    baseline_revision: str = LEGACY_REFERENCE_COMMIT,
    candidate_revision: Optional[str] = None,
    model_identifier: Optional[str] = None,
) -> MFIReleaseEvidenceManifest:
    output_directory.mkdir(parents=True, exist_ok=True)
    legacy = _load_json(legacy_result_path)
    current = _load_json(current_result_path)
    checks: list[MFIReleaseValidationCheck] = []
    checks.append(
        _check(
            "legacy_reference_commit",
            baseline_revision == LEGACY_REFERENCE_COMMIT,
            "Legacy comparison uses the pinned pre-2.0 reference",
            evidence={
                "expected": LEGACY_REFERENCE_COMMIT,
                "actual": baseline_revision,
            },
        )
    )
    checks.append(
        _check(
            "current_schema_versions",
            current.get("analysis_schema_version") == "2.0"
            and current.get("narrative_schema_version") == "2.0",
            "Current result exposes both 2.0 schema versions",
        )
    )
    presentation = _presentation_text(current)
    found_terms = [
        term for term in FORBIDDEN_PRESENTATION_TERMS if term in presentation
    ]
    checks.append(
        _check(
            "current_presentation_terminology",
            not found_terms,
            "Current report presentation contains no unsupported risk terminology",
            evidence={"found_terms": found_terms},
        )
    )
    checks.append(
        _check(
            "legacy_alias_compatibility",
            all(
                key in current
                for key in (
                    "national_mfi",
                    "risk_distribution",
                    "dimension_scores",
                )
            )
            and all(
                "sub_scores" in market
                for market in current.get("markets_data", [])
                if isinstance(market, Mapping)
            ),
            "Current external result retains the Phase 4 compatibility aliases",
        )
    )
    artifacts = [
        _artifact(
            legacy_result_path,
            root=legacy_result_path.parent,
            artifact_id="legacy.result",
            kind="application/json",
        ),
        _artifact(
            current_result_path,
            root=current_result_path.parent,
            artifact_id="current.result",
            kind="application/json",
        ),
    ]
    for path, artifact_id in (
        (legacy_docx_path, "legacy.docx"),
        (current_docx_path, "current.docx"),
    ):
        if path is not None:
            artifacts.append(
                MFIReleaseArtifact(
                    artifact_id=artifact_id,
                    path=str(path.resolve()),
                    sha256=_sha256_path(path),
                    byte_count=path.stat().st_size,
                    kind=(
                        "application/vnd.openxmlformats-officedocument."
                        "wordprocessingml.document"
                    ),
                )
            )
    checks.append(
        _check(
            "docx_pair_supplied",
            legacy_docx_path is not None
            and current_docx_path is not None
            and legacy_docx_path.read_bytes().startswith(b"PK")
            and current_docx_path.read_bytes().startswith(b"PK"),
            "Both legacy and current DOCX packages are available for review",
        )
    )
    for path, artifact_id in (
        (legacy_preview_path, "legacy.preview"),
        (current_preview_path, "current.preview"),
    ):
        if path is not None:
            artifacts.append(
                MFIReleaseArtifact(
                    artifact_id=artifact_id,
                    path=str(path.resolve()),
                    sha256=_sha256_path(path),
                    byte_count=path.stat().st_size,
                    kind="image/png",
                )
            )
    checks.append(
        _check(
            "preview_pair_supplied",
            legacy_preview_path is not None
            and current_preview_path is not None
            and legacy_preview_path.read_bytes().startswith(b"\x89PNG")
            and current_preview_path.read_bytes().startswith(b"\x89PNG"),
            "Both legacy and current preview screenshots are available for review",
        )
    )
    comparison_path = output_directory / "comparison.html"
    legacy_blocks = legacy.get("report_blocks") or []
    current_blocks = current.get("report_blocks") or []
    comparison_path.write_text(
        """<!doctype html><html><head><meta charset="utf-8">
<title>MFI Drafter Phase 4 comparison</title>
<style>body{font-family:Arial,sans-serif;margin:2rem}table{border-collapse:collapse}
td,th{border:1px solid #bbb;padding:.5rem;text-align:left}</style></head><body>
<h1>MFI Drafter Phase 4 old/new comparison</h1>
<p>This artifact is for human analytical and visual approval. Textual equality
is not expected because the legacy and 2.0 narrative contracts differ.</p>
<table><tr><th>Measure</th><th>Legacy</th><th>2.0</th></tr>
<tr><td>Report blocks</td><td>"""
        + str(len(legacy_blocks))
        + "</td><td>"
        + str(len(current_blocks))
        + """</td></tr><tr><td>Dimension narratives</td><td>"""
        + str(len(legacy.get("dimension_findings") or {}))
        + "</td><td>"
        + str(len(current.get("dimension_narratives") or {}))
        + """</td></tr><tr><td>Priority dimensions</td><td>Not authoritative</td><td>"""
        + html.escape(
            ", ".join(
                (current.get("assessment_profile") or {}).get(
                    "priority_dimension_names", []
                )
            )
        )
        + """</td></tr></table></body></html>""",
        encoding="utf-8",
    )
    artifacts.append(
        _artifact(
            comparison_path,
            root=output_directory,
            artifact_id="comparison.html",
            kind="text/html",
        )
    )
    regression_cases: list[MFIRegressionCaseEvidence] = []
    requirements = MFIReleaseRequirements()
    validation_config_sha256: Optional[str] = None
    if regression_manifest_path is not None:
        regression_manifest = MFIReleaseEvidenceManifest.model_validate(
            _load_json(regression_manifest_path)
        )
        regression_cases = regression_manifest.regression_cases
        requirements = regression_manifest.requirements
        validation_config_sha256 = (
            regression_manifest.validation_config_sha256
        )
        required_case_ids = set(requirements.required_case_ids)
        checks.append(
            _check(
                "regression_manifest",
                regression_manifest.baseline_revision
                == LEGACY_REFERENCE_COMMIT
                and not _blocking_failures(
                    [
                        check
                        for case in regression_cases
                        if case.case_id in required_case_ids
                        for check in case.checks
                    ]
                )
                and bool(requirements.required_case_ids),
                "Configured deterministic regression manifest is attached",
            )
        )
    else:
        checks.append(
            _check(
                "regression_manifest",
                False,
                "A configured deterministic regression manifest is required",
            )
        )
    manifest = MFIReleaseEvidenceManifest(
        release_id=release_id,
        mode="compare",
        generated_at=_utc_now(),
        candidate_revision=candidate_revision or _current_revision(),
        baseline_revision=baseline_revision,
        model_identifier=model_identifier,
        validation_config_sha256=validation_config_sha256,
        requirements=requirements,
        regression_cases=regression_cases,
        comparison_checks=checks,
        artifacts=artifacts,
        approvals=_load_optional_models(
            approvals_path, MFIHumanApproval
        ),
        pilot_runs=_load_optional_models(
            pilot_runs_path, MFIPilotRunEvidence
        ),
    )
    evaluate_release_readiness(manifest)
    _write_json(output_directory / "manifest.json", manifest)
    return manifest


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build local MFI Drafter 2.0 Phase 4 release evidence."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    regression = subparsers.add_parser(
        "regression",
        help="Run configured deterministic local regression cases.",
    )
    regression.add_argument(
        "--case-config",
        type=Path,
        required=True,
        help=(
            "Local JSON configuration. Relative source_csv paths are resolved "
            "from this file's directory."
        ),
    )
    regression.add_argument("--release-id", required=True)
    regression.add_argument("--output", type=Path)
    regression.add_argument("--candidate-revision")

    compare = subparsers.add_parser(
        "compare",
        help="Compare exported legacy and 2.0 artifacts without executing v1.",
    )
    compare.add_argument("--legacy-result", type=Path, required=True)
    compare.add_argument("--current-result", type=Path, required=True)
    compare.add_argument("--legacy-docx", type=Path)
    compare.add_argument("--current-docx", type=Path)
    compare.add_argument("--legacy-preview", type=Path)
    compare.add_argument("--current-preview", type=Path)
    compare.add_argument("--approvals", type=Path)
    compare.add_argument("--pilot-runs", type=Path)
    compare.add_argument("--regression-manifest", type=Path)
    compare.add_argument("--release-id", required=True)
    compare.add_argument("--output", type=Path)
    compare.add_argument(
        "--baseline-revision",
        default=LEGACY_REFERENCE_COMMIT,
    )
    compare.add_argument("--candidate-revision")
    compare.add_argument("--model-identifier")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parser().parse_args(argv)
    output = args.output or DEFAULT_OUTPUT_ROOT / args.release_id
    try:
        output.resolve().relative_to(DEFAULT_OUTPUT_ROOT.resolve())
    except ValueError as exc:
        raise ValueError(
            "Phase 4 release evidence must be written below "
            f"{DEFAULT_OUTPUT_ROOT.as_posix()}."
        ) from exc
    if args.command == "regression":
        case_config_path = args.case_config.resolve()
        validation_config = MFIReleaseValidationConfig.model_validate(
            _load_json(case_config_path)
        )
        manifest = run_regression(
            validation_config=validation_config,
            case_root=case_config_path.parent,
            output_directory=output,
            release_id=args.release_id,
            candidate_revision=args.candidate_revision,
            validation_config_sha256=_sha256_path(case_config_path),
        )
    else:
        manifest = run_compare(
            legacy_result_path=args.legacy_result,
            current_result_path=args.current_result,
            legacy_docx_path=args.legacy_docx,
            current_docx_path=args.current_docx,
            legacy_preview_path=args.legacy_preview,
            current_preview_path=args.current_preview,
            approvals_path=args.approvals,
            pilot_runs_path=args.pilot_runs,
            regression_manifest_path=args.regression_manifest,
            output_directory=output,
            release_id=args.release_id,
            baseline_revision=args.baseline_revision,
            candidate_revision=args.candidate_revision,
            model_identifier=args.model_identifier,
        )
    print(
        json.dumps(
            {
                "manifest": str((output / "manifest.json").resolve()),
                "release_ready": manifest.release_ready,
                "blockers": manifest.blockers,
            },
            sort_keys=True,
        )
    )
    required_case_ids = set(manifest.requirements.required_case_ids)
    return 0 if not _blocking_failures(
        [
            check
            for case in manifest.regression_cases
            if case.case_id in required_case_ids
            for check in case.checks
        ]
        + manifest.comparison_checks
    ) else 1


if __name__ == "__main__":
    raise SystemExit(main())
