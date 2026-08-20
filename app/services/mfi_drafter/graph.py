"""
MFI Drafter - Graph
===================
LangGraph workflow for Market Functionality Index reports.

Graph: data → analysis → context → visuals → structured drafting →
deterministic validation → Red-Team → targeted repair or final QA.
"""
from __future__ import annotations

import ast
import io
import re
import json
import uuid
import html as html_lib
import base64
import random
import logging
from copy import deepcopy
from math import pi
from typing import TypedDict, Annotated, Literal, List, Dict, Any, Optional, Callable

import operator
from collections import Counter

import numpy as np

from langgraph.graph import StateGraph, END
from langchain_core.messages import HumanMessage

from app.shared.llm import get_model
from app.shared.llm_observability import (
    TraceSink,
    get_trace_session,
    llm_trace_session,
    log_llm_run_summary,
)
from app.shared.retrievers import ReliefWebRetriever, SeeristRetriever
from .analysis import build_assessment_profile
from .claim_identity import (
    CLAIM_IDENTITY_AUTHORITY,
    CLAIM_IDENTITY_VERSION,
    MFIClaimIdentityError,
    assert_claim_identity_contract,
    canonicalize_narrative_identities,
    count_model_identifiers,
)
from .context_status import (
    not_attempted_context_status,
    reconcile_context_status,
    resolve_context_status,
)
from .features import require_mfi_analysis_v2
from .schemas import (
    MFI_DIMENSIONS,
    MFICorrectionAttemptRecord,
    MFIMetric,
    MFIReleaseControl,
)
from .methodology import (
    ANALYSIS_SCHEMA_VERSION,
    DIMENSION_DESCRIPTIONS,
    DRIVERS_BY_DIMENSION,
    METHODOLOGY_VERSION,
    NARRATIVE_PROHIBITIONS,
    NARRATIVE_PROMPT_CONSTRAINTS,
    NARRATIVE_SCHEMA_VERSION,
    SUBSECTIONS_BY_DIMENSION,
)
from .narrative import (
    apply_unresolved_claim_policy,
    build_claim_catalog,
    build_correction_targets,
    build_qa_review,
    compact_catalog,
    deduplicate_dimension_recommendations,
    dimension_catalog_ids,
    executive_catalog_ids,
    fallback_dimension_narrative,
    fallback_executive_narrative,
    fallback_market_narrative,
    market_catalog_ids,
    material_repairable_flags,
    NARRATIVE_DENSITY_POLICY,
    normalize_red_team_flags,
    parse_context_evidence,
    parse_dimension_narrative,
    parse_executive_narrative,
    parse_market_narrative,
    unmatched_high_claim_ids,
    validate_structured_narratives,
)
from .visualization import (
    MAP_LABEL_MAX,
    MFIMapLabelInput,
    MFIVisualizationContractError,
    format_market_coverage,
    place_map_callouts,
    validate_dimension_chart_coverage,
)

logger = logging.getLogger(__name__)

OnStepCallback = Callable[[str, Dict[str, Any]], None]

WFP_BLUE = "#0072BC"


# ============================================================================
# STATE DEFINITION
# ============================================================================

class MFIReportState(TypedDict):
    """Stato principale del grafo MFI."""
    
    # ===== INPUTS =====
    country: str
    data_collection_start: str
    data_collection_end: str
    markets: List[str]

    csv_data: Optional[Dict[str, Any]]
    use_csv_data: bool
    
    # ===== BRANCH 1: MFI DATA =====
    raw_survey_data: Optional[str]
    markets_data: List[Dict[str, Any]]
    metric_summaries: Dict[str, List[Dict[str, Any]]]
    survey_metadata: Optional[Dict[str, Any]]
    analysis_schema_version: str
    methodology_version: str
    score_authority: str
    narrative_schema_version: str
    release_control: Dict[str, Any]
    generation_diagnostics: Dict[str, Any]
    excluded_market_records: List[Dict[str, Any]]
    methodology_warnings: List[Dict[str, Any]]
    mean_mfi_across_assessed_markets: Optional[float]
    assessment_profile: Optional[Dict[str, Any]]
    claim_catalog: Dict[str, Dict[str, Any]]
    market_score_distribution: List[Dict[str, Any]]
    
    # ===== BRANCH 2: CONTEXT =====
    contextual_documents: List[Dict[str, Any]]
    document_references: List[Dict[str, Any]]
    seerist_documents: List[Dict[str, Any]]
    reliefweb_documents: List[Dict[str, Any]]
    context_evidence: List[Dict[str, Any]]
    context_status: Dict[str, Any]
    context_counts: Dict[str, int]
    retriever_traces: List[Dict[str, Any]]
    
    # ===== VISUALIZATIONS =====
    visualizations: Dict[str, str]  # Base64
    
    # ===== STRUCTURED NARRATIVES =====
    executive_summary_narrative: Dict[str, Any]
    dimension_narratives: Dict[str, Dict[str, Any]]
    market_narratives: Dict[str, Dict[str, Any]]
    
    # ===== QA & CONTROL =====
    claim_validation: Dict[str, Any]
    deterministic_flags: List[Dict[str, Any]]
    red_team_flags: List[Dict[str, Any]]
    qa_review: Dict[str, Any]
    correction_targets: List[Dict[str, Any]]
    correction_history: List[Dict[str, Any]]
    claim_substitutions: List[Dict[str, Any]]
    report_blocks: List[Dict[str, Any]]
    warnings: Annotated[List[str], operator.add]
    run_id: str
    correction_attempts: int
    llm_calls: int
    llm_diagnostics: Dict[str, Any]
    current_node: str


def create_initial_state(
    country: str,
    data_collection_start: str,
    data_collection_end: str,
    markets: List[str],
    csv_data: Optional[Dict[str, Any]] = None,
    release_control: Optional[MFIReleaseControl] = None,
    run_id: Optional[str] = None,
) -> MFIReportState:
    """Crea stato iniziale per il grafo."""
    return MFIReportState(
        country=country,
        data_collection_start=data_collection_start,
        data_collection_end=data_collection_end,
        markets=markets,
        csv_data=csv_data,
        use_csv_data=csv_data is not None,
        raw_survey_data=None,
        markets_data=[],
        metric_summaries=(csv_data or {}).get("metric_summaries", {}),
        survey_metadata=None,
        analysis_schema_version=(csv_data or {}).get(
            "analysis_schema_version", ANALYSIS_SCHEMA_VERSION
        ),
        methodology_version=(csv_data or {}).get(
            "methodology_version", METHODOLOGY_VERSION
        ),
        score_authority=(csv_data or {}).get("score_authority", "synthetic_mock"),
        narrative_schema_version=NARRATIVE_SCHEMA_VERSION,
        release_control=(
            release_control.model_dump()
            if release_control is not None
            else {}
        ),
        generation_diagnostics={
            "dimensions": {"llm": [], "fallback": []},
            "markets": {"llm": [], "fallback": []},
            "context_extraction_mode": "not_started",
            "context_classification_status": "not_started",
            "executive_summary_mode": "not_started",
            "red_team_status": "not_started",
            "correction_attempts": 0,
            "unresolved_high_count": 0,
            "unresolved_medium_count": 0,
            "unresolved_low_count": 0,
            "retrievers": {},
            "claim_identity_authority": CLAIM_IDENTITY_AUTHORITY,
            "claim_identity_version": CLAIM_IDENTITY_VERSION,
            "ignored_model_identifier_count": 0,
            "identity_fallback_artifacts": [],
            "delivery_contract_status": "not_validated",
        },
        excluded_market_records=(csv_data or {}).get("excluded_market_records", []),
        methodology_warnings=(csv_data or {}).get("methodology_warnings", []),
        mean_mfi_across_assessed_markets=None,
        assessment_profile=None,
        claim_catalog={},
        market_score_distribution=[],
        contextual_documents=[],
        document_references=[],
        seerist_documents=[],
        reliefweb_documents=[],
        context_evidence=[],
        context_status=not_attempted_context_status().model_dump(),
        context_counts={"Seerist": 0, "ReliefWeb": 0, "total": 0},
        retriever_traces=[],
        visualizations={},
        executive_summary_narrative={},
        dimension_narratives={},
        market_narratives={},
        claim_validation={"status": "not_recorded", "flags": []},
        deterministic_flags=[],
        red_team_flags=[],
        qa_review={
            "status": "not_recorded",
            "correction_attempts": 0,
            "correction_history": [],
            "flags": [],
        },
        correction_targets=[],
        correction_history=[],
        claim_substitutions=[],
        report_blocks=[],
        warnings=[],
        run_id=run_id or f"mfi_{uuid.uuid4().hex[:8]}",
        correction_attempts=0,
        llm_calls=0,
        llm_diagnostics={},
        current_node="init"
    )


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def _generation_diagnostics(state: MFIReportState) -> Dict[str, Any]:
    diagnostics = deepcopy(state.get("generation_diagnostics") or {})
    diagnostics.setdefault("dimensions", {"llm": [], "fallback": []})
    diagnostics.setdefault("markets", {"llm": [], "fallback": []})
    diagnostics.setdefault("context_extraction_mode", "not_started")
    diagnostics.setdefault("context_classification_status", "not_started")
    diagnostics.setdefault("executive_summary_mode", "not_started")
    diagnostics.setdefault("red_team_status", "not_started")
    diagnostics.setdefault("correction_attempts", 0)
    diagnostics.setdefault("unresolved_high_count", 0)
    diagnostics.setdefault("unresolved_medium_count", 0)
    diagnostics.setdefault("unresolved_low_count", 0)
    diagnostics.setdefault("retrievers", {})
    diagnostics.setdefault("claim_substitutions", [])
    diagnostics.setdefault("unmatched_high_claim_ids", [])
    diagnostics.setdefault("claim_identity_authority", CLAIM_IDENTITY_AUTHORITY)
    diagnostics.setdefault("claim_identity_version", CLAIM_IDENTITY_VERSION)
    diagnostics.setdefault("ignored_model_identifier_count", 0)
    diagnostics.setdefault("identity_fallback_artifacts", [])
    diagnostics.setdefault("delivery_contract_status", "not_validated")
    return diagnostics


def _record_artifact_mode(
    diagnostics: Dict[str, Any],
    collection: Literal["dimensions", "markets"],
    artifact_id: str,
    mode: Literal["llm", "fallback"],
) -> None:
    bucket = diagnostics.setdefault(collection, {"llm": [], "fallback": []})
    values = bucket.setdefault(mode, [])
    if artifact_id not in values:
        values.append(artifact_id)
    values.sort(key=lambda value: str(value).casefold())


def _record_ignored_model_identifiers(
    diagnostics: Dict[str, Any], payload: Any
) -> None:
    diagnostics["ignored_model_identifier_count"] = int(
        diagnostics.get("ignored_model_identifier_count", 0) or 0
    ) + count_model_identifiers(payload)


def _metric_prompt_view(metric: Dict[str, Any]) -> Dict[str, Any]:
    """Keep the typed semantics a drafter needs without audit-only metadata."""
    return {
        key: metric.get(key)
        for key in (
            "metric_id",
            "display_name",
            "role",
            "raw_value",
            "mean_raw_value",
            "normalized_value",
            "mean_normalized_value",
            "unit",
            "orientation",
            "evidence_scope",
            "applicability_status",
            "validation_status",
            "available_market_count",
            "total_assessed_market_count",
            "market_coverage",
            "market_coverage_total",
            "missing_count",
            "product_group",
            "question_group",
            "item_name",
        )
        if key in metric
    }


def _normalize_llm_text(value: Any, *, bulletify: bool = False) -> str:
    if value is None:
        return ""

    if isinstance(value, str):
        s = value.strip()

        if "<" in s and ">" in s:
            s = html_lib.unescape(s)

            def _strip_tags(text: str) -> str:
                return re.sub(r"<[^>]+>", "", text).strip()

            def _render_list(inner_html: str, *, ordered: bool) -> str:
                items = re.findall(r"<li[^>]*>(.*?)</li>", inner_html, flags=re.IGNORECASE | re.DOTALL)
                cleaned = [_strip_tags(item) for item in items]
                cleaned = [c for c in cleaned if c]
                if not cleaned:
                    return ""
                if ordered:
                    return "\n".join([f"{i + 1}. {c}" for i, c in enumerate(cleaned)])
                return "\n".join([f"- {c}" for c in cleaned])

            s = re.sub(
                r"<ol[^>]*>(.*?)</ol>",
                lambda m: _render_list(m.group(1), ordered=True),
                s,
                flags=re.IGNORECASE | re.DOTALL,
            )
            s = re.sub(
                r"<ul[^>]*>(.*?)</ul>",
                lambda m: _render_list(m.group(1), ordered=False),
                s,
                flags=re.IGNORECASE | re.DOTALL,
            )

            s = re.sub(r"<br\s*/?>", "\n", s, flags=re.IGNORECASE)
            s = re.sub(r"</p\s*>", "\n", s, flags=re.IGNORECASE)
            s = re.sub(r"<p\b[^>]*>", "", s, flags=re.IGNORECASE)

            s = re.sub(r"</li\s*>", "\n", s, flags=re.IGNORECASE)
            s = re.sub(r"<li\b[^>]*>", "- ", s, flags=re.IGNORECASE)

            s = re.sub(r"<[^>]+>", "", s)
            s = re.sub(r"\n\s*\n+", "\n\n", s).strip()

        if s.startswith("[") and s.endswith("]"):
            try:
                parsed = json.loads(s)
                if isinstance(parsed, list):
                    return _normalize_llm_text(parsed, bulletify=bulletify)
            except Exception:
                try:
                    parsed = ast.literal_eval(s)
                    if isinstance(parsed, list):
                        return _normalize_llm_text(parsed, bulletify=bulletify)
                except Exception:
                    pass
        return s

    if isinstance(value, (list, tuple)):
        parts: list[str] = []
        for item in value:
            item_str = _normalize_llm_text(item, bulletify=bulletify).strip()
            if not item_str:
                continue
            if bulletify:
                stripped = item_str.lstrip()
                if not (
                    stripped.startswith("-")
                    or stripped.startswith("*")
                    or re.match(r"^\d+\.\s+", stripped)
                ):
                    item_str = f"- {item_str}"
            parts.append(item_str)
        return "\n".join(parts)

    return str(value).strip()


def save_plot_to_base64() -> str:
    """Salva il plot corrente come base64."""
    import matplotlib.pyplot as plt
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=150, bbox_inches='tight', 
                facecolor='white', edgecolor='none')
    plt.close()
    buf.seek(0)
    return base64.b64encode(buf.read()).decode('utf-8')


def _generate_simple_geographic_map(
    state: MFIReportState,
    markets_with_coords: List[Dict[str, Any]],
    visualizations: Dict[str, str],
) -> None:
    import matplotlib.pyplot as plt
    from matplotlib.lines import Line2D

    lats = [float(m["latitude"]) for m in markets_with_coords]
    lons = [float(m["longitude"]) for m in markets_with_coords]
    mfi_scores = [float(m["overall_mfi"]) for m in markets_with_coords]
    names = [str(m.get("market_name", "")).strip() for m in markets_with_coords]
    priority_names = {
        str(name).strip()
        for name in (state.get("assessment_profile") or {}).get(
            "priority_market_names", []
        )
        if str(name).strip()
    }
    point_sizes = [150 if name in priority_names else 80 for name in names]
    line_widths = [1.5 if name in priority_names else 0.5 for name in names]

    fig, ax = plt.subplots(figsize=(14, 10))
    fig.subplots_adjust(left=0.08, right=0.70, bottom=0.09, top=0.90)
    scatter = ax.scatter(
        lons,
        lats,
        c=mfi_scores,
        cmap="Blues",
        s=point_sizes,
        vmin=0,
        vmax=10,
        edgecolors="black",
        linewidths=line_widths,
    )

    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    country = str(state.get("country", "")).strip()
    suffix = f" ({country})" if country else ""
    ax.set_title(
        f"Assessed-market MFI scores{suffix}",
        fontsize=12,
        fontweight="bold",
    )

    cbar = plt.colorbar(scatter, ax=ax, shrink=0.6, pad=0.02)
    cbar.set_label("Stored MFI score (0-10)")

    profile = state.get("assessment_profile") or {}
    market_profiles = {
        str(item.get("market_name", "")).strip(): item
        for item in profile.get("markets", [])
        if isinstance(item, dict) and str(item.get("market_name", "")).strip()
    }
    callout_inputs: list[MFIMapLabelInput] = []
    for lon, lat, name in zip(lons, lats, names):
        if name not in priority_names:
            continue
        market_profile = market_profiles.get(name)
        if not isinstance(market_profile, dict):
            raise MFIVisualizationContractError(
                f"Selected market {name!r} has coordinates but no market profile"
            )
        try:
            selection_order = int(market_profile["selection_order"])
            score_rank = int(market_profile["score_rank"])
        except (KeyError, TypeError, ValueError) as exc:
            raise MFIVisualizationContractError(
                f"Selected market {name!r} lacks typed rank metadata"
            ) from exc
        callout_inputs.append(
            MFIMapLabelInput(
                market_name=name,
                longitude=lon,
                latitude=lat,
                selection_order=selection_order,
                score_rank=score_rank,
            )
        )

    placements = place_map_callouts(ax, callout_inputs, maximum=MAP_LABEL_MAX)
    for placement in placements:
        ax.annotate(
            str(placement.number),
            (placement.longitude, placement.latitude),
            fontsize=7,
            fontweight="bold",
            ha="center",
            va="center",
            xytext=placement.offset_points,
            textcoords="offset points",
            bbox={
                "boxstyle": "circle,pad=0.25",
                "facecolor": "white",
                "edgecolor": "black",
                "linewidth": 0.7,
            },
            arrowprops={
                "arrowstyle": "-",
                "color": "#555555",
                "linewidth": 0.6,
                "shrinkA": 4,
                "shrinkB": 3,
            },
            zorder=5,
        )

    if placements:
        handles = [
            Line2D(
                [],
                [],
                linestyle="none",
                marker="o",
                markerfacecolor="white",
                markeredgecolor="black",
                markersize=6,
                label=(
                    f"{placement.number}. {placement.market_name} "
                    f"(score rank {placement.score_rank})"
                ),
            )
            for placement in placements
        ]
        fig.legend(
            handles=handles,
            title="Selected markets",
            loc="upper left",
            bbox_to_anchor=(0.73, 0.88),
            borderaxespad=0.0,
            fontsize=7,
            title_fontsize=8,
            frameon=True,
        )

    visualizations["geographic_map"] = save_plot_to_base64()


# ============================================================================
# NODE: MFI DATA AGENT
# ============================================================================

def generate_mock_mfi_data(
    country: str, 
    markets: List[str],
    data_collection_start: str,
    data_collection_end: str
) -> Dict[str, Any]:
    """Generate explicitly synthetic data in the canonical typed evidence shape."""
    logger.info(f"[MOCK] Generating MFI data for {country} ({len(markets)} markets)")
    
    # Mock admin mapping (API-like enrichment)
    admin0 = country
    admin1_pool = [f"{country} - Admin1 {i + 1}" for i in range(min(3, max(1, len(markets))))]
    market_admin_map: Dict[str, Dict[str, str]] = {}
    for i, market in enumerate(markets):
        admin1 = admin1_pool[i % len(admin1_pool)]
        admin2 = f"{admin1} - Admin2 {(i % 2) + 1}"
        market_admin_map[market] = {"admin0": admin0, "admin1": admin1, "admin2": admin2}
    
    markets_data = []
    for market in markets:
        admin_info = market_admin_map.get(market, {"admin0": country, "admin1": "Unknown", "admin2": "Unknown"})
        region = admin_info["admin1"]
        dimension_scores = {}
        subsections: Dict[str, List[Dict[str, Any]]] = {
            dimension: [] for dimension in MFI_DIMENSIONS
        }
        drivers: Dict[str, List[Dict[str, Any]]] = {
            dimension: [] for dimension in MFI_DIMENSIONS
        }

        for dim in MFI_DIMENSIONS:
            base_score = random.uniform(4.5, 9.5)
            dimension_scores[dim] = round(base_score, 1)

            quality_maximum = 8.0
            quality_measure = random.uniform(0.0, quality_maximum)
            for definition in SUBSECTIONS_BY_DIMENSION[dim]:
                if definition.metric_id == "quality.maximum":
                    raw_value = quality_maximum
                elif definition.metric_id == "quality.measure":
                    raw_value = quality_measure
                else:
                    raw_value = random.uniform(definition.raw_min, definition.raw_max)
                normalized = definition.normalize(
                    raw_value,
                    dynamic_max=quality_maximum
                    if definition.metric_id == "quality.measure"
                    else None,
                )
                subsections[dim].append(
                    MFIMetric(
                        metric_id=definition.metric_id,
                        dimension=definition.dimension,
                        display_name=definition.display_name,
                        variable_name=definition.variable_name,
                        source_level_id=definition.source_level_id,
                        source_level_name=definition.source_level_name,
                        role=definition.role,
                        raw_value=raw_value,
                        raw_min=definition.raw_min,
                        raw_max=definition.raw_max,
                        normalized_value=normalized,
                        orientation=definition.orientation,
                        unit=definition.unit,
                        evidence_scope=definition.evidence_scope,
                        observed_raw_values=[raw_value],
                        market_coverage=len(markets),
                        market_coverage_total=len(markets),
                        missing_count=0,
                        applicability_status="available",
                        validation_status="valid",
                        methodology_note=(
                            "Synthetic mock evidence for workflow demonstration only; "
                            "not a DataBridge observation."
                        ),
                        product_group=definition.product_group,
                        question_group=definition.question_group,
                        item_name=definition.item_name,
                        severity_weight=definition.severity_weight,
                    ).model_dump()
                )
            for definition in DRIVERS_BY_DIMENSION[dim]:
                raw_value = random.uniform(definition.raw_min, definition.raw_max)
                normalized = definition.normalize(raw_value)
                drivers[dim].append(
                    MFIMetric(
                        metric_id=definition.metric_id,
                        dimension=definition.dimension,
                        display_name=definition.display_name,
                        variable_name=definition.variable_name,
                        source_level_id=definition.source_level_id,
                        source_level_name=definition.source_level_name,
                        role=definition.role,
                        raw_value=raw_value,
                        raw_min=definition.raw_min,
                        raw_max=definition.raw_max,
                        normalized_value=normalized,
                        orientation=definition.orientation,
                        unit=definition.unit,
                        evidence_scope=definition.evidence_scope,
                        observed_raw_values=[raw_value],
                        market_coverage=len(markets),
                        market_coverage_total=len(markets),
                        missing_count=0,
                        applicability_status="available",
                        validation_status="valid",
                        methodology_note=(
                            "Synthetic mock evidence for workflow demonstration only; "
                            "not a DataBridge observation."
                        ),
                        product_group=definition.product_group,
                        question_group=definition.question_group,
                        item_name=definition.item_name,
                        severity_weight=definition.severity_weight,
                    ).model_dump()
                )

        overall_mfi = round(np.mean(list(dimension_scores.values())), 1)
        markets_data.append({
            "market_name": market,
            "admin0": admin_info["admin0"],
            "admin1": admin_info["admin1"],
            "admin2": admin_info["admin2"],
            "region": region,
            "overall_mfi": overall_mfi,
            "dimension_scores": dimension_scores,
            "subsections": subsections,
            "drivers": drivers,
            "traders_surveyed": random.randint(15, 30)
        })
    
    regions = sorted({m["region"] for m in markets_data})
    
    survey_metadata = {
        "country": country,
        "collection_period": f"{data_collection_start} to {data_collection_end}",
        "total_traders": sum(m["traders_surveyed"] for m in markets_data),
        "total_markets": len(markets_data),
        "regions_covered": regions
    }
    
    return {
        "analysis_schema_version": ANALYSIS_SCHEMA_VERSION,
        "methodology_version": METHODOLOGY_VERSION,
        "score_authority": "synthetic_mock",
        "excluded_market_records": [],
        "methodology_warnings": [],
        "warnings": [],
        "markets_data": markets_data,
        "metric_summaries": _summarize_mock_evidence(markets_data),
        "survey_metadata": survey_metadata
    }


def _summarize_mock_evidence(
    markets_data: List[Dict[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    """Summarize synthetic typed metrics with the same deterministic market mean."""
    summaries: Dict[str, List[Dict[str, Any]]] = {
        dimension: [] for dimension in MFI_DIMENSIONS
    }
    for dimension in MFI_DIMENSIONS:
        by_metric: Dict[str, List[Dict[str, Any]]] = {}
        for market in markets_data:
            for group_name in ("subsections", "drivers"):
                for metric in market[group_name].get(dimension, []):
                    by_metric.setdefault(metric["metric_id"], []).append(metric)
        for metric_id, metrics in sorted(by_metric.items()):
            raw_values = [float(metric["raw_value"]) for metric in metrics]
            normalized = [
                float(metric["normalized_value"])
                for metric in metrics
                if metric.get("normalized_value") is not None
            ]
            reference = metrics[0]
            summaries[dimension].append(
                {
                    "metric_id": metric_id,
                    "dimension": dimension,
                    "display_name": reference["display_name"],
                    "role": reference["role"],
                    "mean_raw_value": sum(raw_values) / len(raw_values),
                    "mean_normalized_value": (
                        sum(normalized) / len(normalized) if normalized else None
                    ),
                    "aggregation_numerator": sum(raw_values),
                    "aggregation_denominator": len(raw_values),
                    "available_market_count": len(raw_values),
                    "total_assessed_market_count": len(markets_data),
                    "missing_count": len(markets_data) - len(raw_values),
                    "unit": reference["unit"],
                    "orientation": reference["orientation"],
                    "evidence_scope": reference["evidence_scope"],
                    "contributing_metric_ids": [metric_id],
                    "methodology_note": reference["methodology_note"],
                }
            )
    return summaries


def node_mfi_data_agent(state: MFIReportState) -> dict:
    """Nodo: Recupera/genera dati MFI."""
    logger.info(f"[MFIDataAgent] Processing for {state['country']}")

    if state.get("use_csv_data") and state.get("csv_data"):
        logger.info("[MFIDataAgent] Using CSV data")
        data = state["csv_data"]
    else:
        logger.info("[MFIDataAgent] Using mock data (no CSV provided)")
        data = generate_mock_mfi_data(
            state["country"],
            state["markets"],
            state["data_collection_start"],
            state["data_collection_end"],
        )
    
    return {
        "analysis_schema_version": data.get("analysis_schema_version", ANALYSIS_SCHEMA_VERSION),
        "methodology_version": data.get("methodology_version", METHODOLOGY_VERSION),
        "score_authority": data.get("score_authority", "synthetic_mock"),
        "excluded_market_records": data.get("excluded_market_records", []),
        "methodology_warnings": data.get("methodology_warnings", []),
        "markets_data": data["markets_data"],
        "metric_summaries": data.get("metric_summaries", {}),
        "survey_metadata": data["survey_metadata"],
        "warnings": data.get("warnings", []),
        "current_node": "mfi_data_agent"
    }


def node_mfi_analysis(state: MFIReportState) -> dict:
    """Build the pure, deterministic Phase 2 assessment profile."""
    logger.info("[MFIAnalysis] Building deterministic assessment profile")
    profile = build_assessment_profile(
        state.get("markets_data", []),
        state.get("metric_summaries", {}),
        {
            "methodology_version": state.get(
                "methodology_version", METHODOLOGY_VERSION
            ),
            "score_authority": state.get("score_authority", "synthetic_mock"),
            "survey_metadata": state.get("survey_metadata") or {},
            "excluded_market_records": state.get("excluded_market_records", []),
        },
    )
    profile_payload = profile.model_dump()
    market_score_distribution = [
        {
            "market_name": market.market_name,
            "overall_mfi": market.overall_mfi,
            "score_rank": market.score_rank,
            "selection_order": market.selection_order,
            "is_priority_market": market.is_priority_market,
        }
        for market in profile.markets
    ]
    return {
        "mean_mfi_across_assessed_markets": (
            profile.mean_mfi_across_assessed_markets
        ),
        "assessment_profile": profile_payload,
        "claim_catalog": build_claim_catalog(profile_payload),
        "market_score_distribution": market_score_distribution,
        "current_node": "mfi_analysis",
    }


# NODE: CONTEXT RETRIEVAL (Mock)
# ============================================================================

def node_context_retrieval(state: MFIReportState) -> dict:
    """Nodo: Recupera notizie contestuali (mock)."""
    logger.info(f"[ContextRetrieval] Fetching context for {state['country']}")

    docs: List[Dict[str, Any]] = []
    retriever_traces: List[Dict[str, Any]] = []

    country = state.get("country", "")
    start_date = state.get("data_collection_start", "")
    end_date = state.get("data_collection_end", "")

    rw = ReliefWebRetriever(verbose=False)
    rw_query = ReliefWebRetriever.build_economy_query(
        extra_terms=["market functionality", "food security", "supply", "availability", "access"]
    )
    rw_docs = rw.fetch(country=country, start_date=start_date, end_date=end_date, max_records=8, query=rw_query)
    if getattr(rw, "last_trace", None):
        retriever_traces.append(rw.last_trace)

    seerist = SeeristRetriever(verbose=False)
    seerist_queries = [
        SeeristRetriever.build_lucene_or_query(
            list(SeeristRetriever.DEFAULT_ECON_TERMS)
            + ["market functionality", "food security", "supply", "availability", "access"]
        ),
        SeeristRetriever.build_lucene_or_query(
            ["market functionality", "market", "availability", "access", "food security"]
        ),
        "",
    ]
    seerist_docs = seerist.fetch_batch(
        queries=seerist_queries,
        start_date=start_date,
        end_date=end_date,
        country=country,
        max_per_query=8,
    )
    if len(seerist_docs) > 8:
        seerist_docs = seerist_docs[:8]
    if getattr(seerist, "last_trace", None):
        retriever_traces.append(seerist.last_trace)

    combined = list(rw_docs) + list(seerist_docs)
    seen_keys = set()
    deduped: List[Dict[str, Any]] = []
    for d in combined:
        url = (d.get("url") or "").strip()
        key = url or d.get("doc_id")
        if not key or key in seen_keys:
            continue
        seen_keys.add(key)
        if not d.get("content"):
            d["content"] = d.get("title", "")
        deduped.append(d)
    docs = deduped

    counts = Counter([d.get("source", "Unknown") for d in docs])
    context_counts = {
        "Seerist": int(counts.get("Seerist", 0)),
        "ReliefWeb": int(counts.get("ReliefWeb", 0)),
        "total": int(len(docs)),
    }

    refs = [
        {
            "doc_id": d.get("doc_id"),
            "source": d.get("source"),
            "title": d.get("title"),
            "url": d.get("url"),
            "date": d.get("date"),
        }
        for d in docs
    ]
    
    diagnostics = _generation_diagnostics(state)
    retriever_status = diagnostics.setdefault("retrievers", {})
    for name, documents, retriever in (
        ("ReliefWeb", rw_docs, rw),
        ("Seerist", seerist_docs, seerist),
    ):
        trace = getattr(retriever, "last_trace", None) or {}
        if trace.get("error"):
            retriever_status[name] = "failed"
        elif documents:
            retriever_status[name] = "completed"
        else:
            retriever_status[name] = "no_results"
    context_status = resolve_context_status(
        retriever_statuses=retriever_status,
        documents=docs,
        statements=[],
        extraction_mode="not_started",
    )

    updates = {
        "contextual_documents": docs,
        "document_references": refs,
        "seerist_documents": list(seerist_docs),
        "reliefweb_documents": list(rw_docs),
        "context_counts": context_counts,
        "context_status": context_status.model_dump(),
        "retriever_traces": retriever_traces,
        "generation_diagnostics": diagnostics,
        "current_node": "context_retrieval",
    }
    return updates

# NODE: CONTEXT EXTRACTOR
# ============================================================================

def node_context_extractor(state: MFIReportState) -> dict:
    """Classify source-linked context before it can enter MFI narratives."""
    logger.info("[ContextExtractor] Classifying contextual evidence")
    docs = state.get("contextual_documents", [])
    trace = get_trace_session(
        service="mfi-drafter",
        run_id=str(state.get("run_id") or "mfi-direct"),
        initial=state.get("llm_diagnostics"),
    )
    if not docs:
        trace.record_skip(
            node="context_extractor",
            operation="mfi.context_classification.v1",
            reason="no_contextual_documents",
        )
        diagnostics = _generation_diagnostics(state)
        diagnostics["context_extraction_mode"] = "not_applicable"
        diagnostics["context_classification_status"] = "not_attempted"
        existing_status = state.get("context_status") or {}
        context_status = (
            not_attempted_context_status()
            if existing_status.get("status") == "not_attempted"
            else resolve_context_status(
                retriever_statuses=diagnostics.get("retrievers", {}),
                documents=[],
                statements=[],
                extraction_mode="not_applicable",
            )
        )
        return {
            "context_evidence": [],
            "context_status": context_status.model_dump(),
            "generation_diagnostics": diagnostics,
            "llm_diagnostics": trace.snapshot(),
            "current_node": "context_extractor",
        }

    source_payload = [
        {
            "document_id": document.get("doc_id"),
            "source": document.get("source"),
            "date": document.get("date"),
            "title": document.get("title"),
            "content": str(document.get("content") or "")[:800],
        }
        for document in docs[:8]
        if isinstance(document, dict) and document.get("doc_id")
    ]
    prompt = f"""Classify source-linked context for the {state['country']} MFI report.

Return only statements directly supported by the supplied documents.
Classifications:
- corroborating: independently supports an observed MFI pattern;
- potentially_explanatory: may help interpret a pattern but does not establish causality;
- unrelated: not useful for interpreting the MFI evidence.

Rules:
- English only.
- Cite only supplied document_id values.
- Never say that contextual events caused an MFI result.
- Do not invent MFI values or use undocumented risk categories.

DOCUMENTS:
{json.dumps(source_payload)}

Output JSON:
{{"statements": [
  {{"text": "...", "classification": "corroborating|potentially_explanatory|unrelated",
    "document_ids": ["supplied-id"]}}
]}}"""
    def _validate_context_response(result: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(result, dict) or not isinstance(
            result.get("statements"), list
        ):
            raise ValueError("Context classifier returned an invalid response schema")
        for index, statement in enumerate(result["statements"]):
            if not isinstance(statement, dict):
                raise ValueError(f"statements[{index}] must be an object")
            if not str(statement.get("text") or "").strip():
                raise ValueError(f"statements[{index}].text is required")
            if statement.get("classification") not in {
                "corroborating",
                "potentially_explanatory",
                "unrelated",
            }:
                raise ValueError(f"statements[{index}].classification is invalid")
            if not isinstance(statement.get("document_ids"), list):
                raise ValueError(f"statements[{index}].document_ids must be a list")
        return result

    traced = trace.invoke_json(
        model=get_model(),
        messages=[HumanMessage(content=prompt)],
        node="context_extractor",
        operation="mfi.context_classification.v1",
        artifact_type="context",
        validator=_validate_context_response,
    )
    result = traced.value
    ignored_model_identifiers = count_model_identifiers(result)
    context_evidence, parse_flags = parse_context_evidence(
        result,
        documents=docs,
    )
    llm_calls = 1
    diagnostics = _generation_diagnostics(state)
    diagnostics["ignored_model_identifier_count"] = int(
        diagnostics.get("ignored_model_identifier_count", 0) or 0
    ) + ignored_model_identifiers
    diagnostics["context_extraction_mode"] = "llm"
    diagnostics["context_classification_status"] = "completed"
    context_status = resolve_context_status(
        retriever_statuses=diagnostics.get("retrievers", {}),
        documents=docs,
        statements=context_evidence,
        extraction_mode=diagnostics["context_extraction_mode"],
        classification_failed=False,
    )
    updates = {
        "context_evidence": context_evidence,
        "context_status": context_status.model_dump(),
        "llm_calls": state.get("llm_calls", 0) + llm_calls,
        "generation_diagnostics": diagnostics,
        "llm_diagnostics": trace.snapshot(),
        "current_node": "context_extractor",
    }
    if parse_flags:
        updates["deterministic_flags"] = parse_flags
    return updates


def node_mfi_graph_designer(state: MFIReportState) -> dict:
    logger.info("[GraphDesigner] Generating visualizations")

    visualizations: Dict[str, str] = {}

    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        profile = state.get("assessment_profile") or {}
        dimensions = [
            item for item in profile.get("dimensions", []) if isinstance(item, dict)
        ]
        score_map = {
            str(item["dimension"]): float(item["statistics"]["mean"])
            for item in dimensions
            if item.get("dimension") and item.get("statistics", {}).get("mean") is not None
        }

        dims = list(MFI_DIMENSIONS)
        values = [score_map[dim] for dim in dims if dim in score_map]
        radar_dims = [dim for dim in dims if dim in score_map]
        if len(values) == len(dims):
            angles = [n / float(len(dims)) * 2 * pi for n in range(len(dims))]
            values_loop = values + values[:1]
            angles_loop = angles + angles[:1]

            fig, ax = plt.subplots(figsize=(8, 8), subplot_kw={"polar": True})
            ax.set_theta_offset(pi / 2)
            ax.set_theta_direction(-1)
            plt.xticks(angles, radar_dims, size=8)
            ax.set_ylim(0, 10)
            ax.plot(angles_loop, values_loop, color=WFP_BLUE, linewidth=2)
            ax.fill(angles_loop, values_loop, color=WFP_BLUE, alpha=0.25)
            ax.set_title(
                "Average MFI dimension profile across assessed markets",
                pad=24,
                fontsize=11,
                fontweight="bold",
            )
            plt.tight_layout()
            visualizations["mfi_radar"] = save_plot_to_base64()

        market_profiles = [
            item for item in profile.get("markets", []) if isinstance(item, dict)
        ]
        selected_markets = set(profile.get("priority_market_names", []))
        ordered_markets = sorted(
            market_profiles,
            key=lambda item: (
                float(item["overall_mfi"]),
                str(item.get("market_name", "")).casefold(),
            ),
        )
        if ordered_markets:
            names = [str(item["market_name"]) for item in ordered_markets]
            scores = [float(item["overall_mfi"]) for item in ordered_markets]
            colors = [
                "#F68B1F" if name in selected_markets else WFP_BLUE for name in names
            ]
            sizes = [70 if name in selected_markets else 38 for name in names]
            fig_height = max(5, len(names) * 0.25)
            fig, ax = plt.subplots(figsize=(10, fig_height))
            y_pos = np.arange(len(names))
            ax.scatter(scores, y_pos, c=colors, s=sizes, edgecolors="white")
            ax.set_yticks(y_pos)
            ax.set_yticklabels(names, fontsize=8)
            ax.set_xlim(0, 10)
            ax.set_xlabel("Stored MFI score (0-10)")
            ax.set_title(
                "Ordered assessed-market MFI scores",
                fontsize=11,
                fontweight="bold",
            )
            ax.grid(axis="x", alpha=0.2)
            plt.tight_layout()
            visualizations["market_score_ranking"] = save_plot_to_base64()

        markets_data = [
            m for m in (state.get("markets_data", []) or [])
            if isinstance(m, dict)
        ]

        if markets_data:
            market_names = [str(m.get("market_name", "")).strip() for m in markets_data]
            dims = list(MFI_DIMENSIONS)

            data_matrix: list[list[float]] = []
            for m in markets_data:
                dim_scores = m.get("dimension_scores") or {}
                row = []
                for dim in dims:
                    try:
                        value = dim_scores.get(dim)
                        row.append(float(value) if value is not None else np.nan)
                    except Exception:
                        row.append(np.nan)
                data_matrix.append(row)

            data_matrix_np = np.array(data_matrix, dtype=float)

            fig_height = max(8, len(market_names) * 0.35)
            fig_width = max(12, len(dims) * 1.2)
            fig, ax = plt.subplots(figsize=(fig_width, fig_height))

            im = ax.imshow(
                data_matrix_np, cmap="Blues", aspect="auto", vmin=0, vmax=10
            )

            ax.set_xticks(np.arange(len(dims)))
            ax.set_yticks(np.arange(len(market_names)))
            ax.set_xticklabels(dims, rotation=45, ha="right", fontsize=9)
            ax.set_yticklabels(market_names, fontsize=8)

            for i in range(len(market_names)):
                for j in range(len(dims)):
                    score = float(data_matrix_np[i, j])
                    if np.isnan(score):
                        continue
                    text_color = "white" if score > 7 else "black"
                    ax.text(
                        j,
                        i,
                        f"{score:.2f}",
                        ha="center",
                        va="center",
                        color=text_color,
                        fontsize=7,
                        fontweight="bold",
                    )

            cbar = ax.figure.colorbar(im, ax=ax, shrink=0.5)
            cbar.set_label("MFI Score (0-10)", rotation=270, labelpad=15)

            ax.set_title(
                "Assessed-market MFI profile by dimension",
                fontsize=12,
                fontweight="bold",
                pad=10,
            )

            plt.tight_layout()
            visualizations["overview_table"] = save_plot_to_base64()

        market_regions = {
            str(market.get("market_name", "")): str(
                market.get("region") or market.get("admin1") or ""
            )
            for market in markets_data
        }
        for dim_data in dimensions:
            dim_name = str(dim_data.get("dimension", ""))
            ordered = (
                dim_data.get("localized_patterns", {}).get("ordered_markets", [])
            )
            sorted_markets = [
                (str(item["name"]), float(item["value"]))
                for item in ordered
                if isinstance(item, dict)
                and item.get("name")
                and item.get("value") is not None
            ]
            if not dim_name or not sorted_markets:
                continue
            markets = [item[0] for item in sorted_markets]
            scores = [item[1] for item in sorted_markets]
            regional_means = {
                str(item["region"]): float(item["statistics"]["mean"])
                for item in dim_data.get("regional_summaries", [])
                if isinstance(item, dict)
                and item.get("region")
                and item.get("statistics", {}).get("mean") is not None
            }
            coverage = validate_dimension_chart_coverage(
                dim_data.get("statistics", {}).get("coverage"),
                plotted_market_count=len(sorted_markets),
                dimension=dim_name,
            )
            fig_height = max(6, len(markets) * 0.3)
            fig, ax = plt.subplots(figsize=(10, fig_height))

            y_pos = np.arange(len(markets))
            ax.barh(
                y_pos,
                scores,
                color=WFP_BLUE,
                alpha=0.75,
                edgecolor="white",
                linewidth=0.5,
            )
            marker_added = False
            for index, market_name in enumerate(markets):
                region = market_regions.get(market_name)
                region_mean = regional_means.get(region or "")
                if region_mean is None:
                    continue
                ax.scatter(
                    region_mean,
                    index,
                    marker="D",
                    s=28,
                    color="#F68B1F",
                    edgecolor="black",
                    linewidth=0.3,
                    label="Regional mean" if not marker_added else None,
                    zorder=4,
                )
                marker_added = True

            ax.set_yticks(y_pos)
            ax.set_yticklabels(markets, fontsize=8)
            ax.set_xlabel("Stored dimension score (0-10)")
            ax.set_xlim(0, 10)
            ax.set_title(
                f"{dim_name} by assessed market\n"
                f"Coverage: {format_market_coverage(coverage)}",
                fontsize=11,
                fontweight="bold",
            )
            if marker_added:
                ax.legend(loc="lower right", fontsize=8)

            for i, score in enumerate(scores):
                ax.text(score + 0.1, i, f"{score:.2f}", va="center", fontsize=7)

            plt.tight_layout()

            safe_dim_name = str(dim_name).lower().replace(" ", "_").replace("&", "and")
            safe_dim_name = re.sub(r"[^a-z0-9_]+", "_", safe_dim_name).strip("_")
            visualizations[f"dim_{safe_dim_name}_bars"] = save_plot_to_base64()

        priority_dimensions = {
            str(item) for item in profile.get("priority_dimension_names", [])
        }
        for dim_data in dimensions:
            dim_name = str(dim_data.get("dimension", ""))
            if dim_name not in priority_dimensions:
                continue
            safe_dim_name = re.sub(
                r"[^a-z0-9_]+",
                "_",
                dim_name.lower().replace(" ", "_").replace("&", "and"),
            ).strip("_")

            if dim_name != "Food Quality":
                subsection_values = [
                    (
                        str(metric.get("display_name", metric.get("metric_id", ""))),
                        float(metric["mean_normalized_value"]),
                    )
                    for metric in dim_data.get("subsections", [])
                    if isinstance(metric, dict)
                    and metric.get("mean_normalized_value") is not None
                ]
                subsection_values.sort(key=lambda item: (item[1], item[0].casefold()))
                if subsection_values:
                    labels = [item[0] for item in subsection_values]
                    values = [item[1] for item in subsection_values]
                    fig, ax = plt.subplots(figsize=(9, max(3, len(labels) * 0.55)))
                    y_pos = np.arange(len(labels))
                    ax.barh(y_pos, values, color=WFP_BLUE)
                    ax.set_yticks(y_pos)
                    ax.set_yticklabels(labels, fontsize=8)
                    ax.set_xlim(0, 10)
                    ax.set_xlabel("Normalized subsection score (0-10)")
                    ax.set_title(
                        f"{dim_name}: official subsection evidence",
                        fontweight="bold",
                    )
                    for index, value in enumerate(values):
                        ax.text(value + 0.08, index, f"{value:.2f}", va="center")
                    plt.tight_layout()
                    visualizations[
                        f"priority_{safe_dim_name}_subsections"
                    ] = save_plot_to_base64()

            ranked_drivers = [
                metric
                for metric in dim_data.get("drivers", [])
                if isinstance(metric, dict)
                and metric.get("item_name") is None
                and metric.get("unfavorable_rate") is not None
                and metric.get("weakness_rank") is not None
            ]
            ranked_drivers.sort(
                key=lambda metric: (
                    int(metric["weakness_rank"]),
                    str(metric.get("metric_id", "")),
                )
            )
            ranked_drivers = ranked_drivers[:8]
            if ranked_drivers:
                labels = [
                    str(metric.get("display_name", metric.get("metric_id", "")))
                    for metric in ranked_drivers
                ]
                values = [
                    float(metric["unfavorable_rate"]) * 100
                    for metric in ranked_drivers
                ]
                fig, ax = plt.subplots(figsize=(10, max(4, len(labels) * 0.5)))
                y_pos = np.arange(len(labels))
                ax.barh(y_pos, values, color="#F68B1F")
                ax.set_yticks(y_pos)
                ax.set_yticklabels(labels, fontsize=8)
                ax.invert_yaxis()
                ax.set_xlim(0, 100)
                ax.set_xlabel("Unfavorable rate (%)")
                ax.set_title(
                    f"{dim_name}: ranked explanatory evidence",
                    fontweight="bold",
                )
                for index, value in enumerate(values):
                    ax.text(value + 0.8, index, f"{value:.1f}%", va="center")
                plt.tight_layout()
                visualizations[
                    f"priority_{safe_dim_name}_drivers"
                ] = save_plot_to_base64()

            relevant_items = [
                metric
                for metric in dim_data.get("drivers", [])
                if isinstance(metric, dict)
                and metric.get("item_relevant") is True
                and metric.get("unfavorable_rate") is not None
            ]
            relevant_items.sort(
                key=lambda metric: (
                    -float(metric["unfavorable_rate"]),
                    str(metric.get("metric_id", "")),
                )
            )
            if relevant_items:
                labels = [
                    str(metric.get("display_name", metric.get("metric_id", "")))
                    for metric in relevant_items
                ]
                values = [
                    float(metric["unfavorable_rate"]) * 100
                    for metric in relevant_items
                ]
                fig, ax = plt.subplots(figsize=(10, max(4, len(labels) * 0.5)))
                y_pos = np.arange(len(labels))
                ax.barh(y_pos, values, color="#8A2BE2")
                ax.set_yticks(y_pos)
                ax.set_yticklabels(labels, fontsize=8)
                ax.invert_yaxis()
                ax.set_xlim(0, 100)
                ax.set_xlabel("Unfavorable rate (%)")
                ax.set_title(
                    f"{dim_name}: relevant item evidence",
                    fontweight="bold",
                )
                plt.tight_layout()
                visualizations[
                    f"priority_{safe_dim_name}_items"
                ] = save_plot_to_base64()

        markets_with_coords = [
            m
            for m in markets_data
            if m.get("latitude") is not None
            and m.get("longitude") is not None
            and m.get("overall_mfi") is not None
        ]

        if markets_with_coords:
            _generate_simple_geographic_map(
                state, markets_with_coords, visualizations
            )
    except MFIVisualizationContractError:
        logger.exception("MFI visualization contract violation")
        raise
    except Exception as e:
        logger.error(f"Error generating visualizations: {e}")

    return {
        "visualizations": visualizations,
        "current_node": "mfi_graph_designer",
    }


# ============================================================================
# NODE: DIMENSION DRAFTER
# ============================================================================

def node_dimension_drafter(state: MFIReportState) -> dict:
    """Draft or repair metric-cited narratives for all nine dimensions."""
    logger.info("[DimensionDrafter] Generating structured dimension narratives")
    profile = state.get("assessment_profile") or {}
    dimensions = [
        item for item in profile.get("dimensions", []) if isinstance(item, dict)
    ]
    if not dimensions:
        return {"current_node": "dimension_drafter"}

    llm = get_model()
    narratives = dict(state.get("dimension_narratives") or {})
    catalog = state.get("claim_catalog") or {}
    targets = state.get("correction_targets") or []
    repairing = bool(targets)
    llm_calls = 0
    diagnostics = _generation_diagnostics(state)
    trace = get_trace_session(
        service="mfi-drafter",
        run_id=str(state.get("run_id") or "mfi-direct"),
        initial=state.get("llm_diagnostics"),
    )
    fallback_dimensions: List[str] = []
    correction_history = deepcopy(state.get("correction_history", []) or [])
    attempt_number = int(state.get("correction_attempts", 0) or 0)

    for dimension_profile in dimensions:
        dimension = str(dimension_profile["dimension"])
        relevant_targets = [
            target
            for target in targets
            if target.get("artifact_type") in {"dimension", "global"}
            and (
                target.get("artifact_type") == "global"
                or target.get("artifact_id") in {None, dimension}
            )
        ]
        if repairing and not relevant_targets:
            continue
        allowed_ids = dimension_catalog_ids(dimension_profile)
        prompt_catalog = compact_catalog(catalog, allowed_ids)
        previous = narratives.get(dimension)
        flagged_fields = sorted(
            {
                str(target["field_name"])
                for target in relevant_targets
                if target.get("field_name")
            }
        )
        logger.info("Processing dimension: %s", dimension)
        prompt = f"""Draft the {dimension} section of an MFI assessment report.

Use English only. Return valid JSON only. Every numeric statement must use an
exact `formatted_value` from CLAIM_CATALOG and cite its `metric_id`. Do not
calculate, round, invert, or combine values. Every claim object must contain:
`text`, `metric_ids`, `document_ids`, `scope`, and `polarity`.

METHODOLOGY DESCRIPTION:
{DIMENSION_DESCRIPTIONS.get(dimension, '')}

CONSTRAINTS:
{json.dumps(list(NARRATIVE_PROMPT_CONSTRAINTS))}

DIMENSION_PROFILE:
{json.dumps(dimension_profile)}

CLAIM_CATALOG:
{json.dumps(prompt_catalog)}

This dimension is priority={bool(dimension_profile.get('is_priority'))}.
Every dimension must cover its mean, profile rank, variation, findings, and
recommendations. A priority dimension must also include weakest official
subsections (Food Quality: applicable question drivers), 2-4 explanatory
drivers, relevant items when supplied, localized patterns, and limitations.
Recommendations must cite evidence used by a finding.

R8 CLAIM CEILINGS (maximums, not quotas; order by analytical importance):
- non-priority: 1 key finding, 0 geographic patterns, 1 limitation, and
  1 recommendation; do not return subdimension analysis;
- priority: {NARRATIVE_DENSITY_POLICY.priority_findings} key findings,
  {NARRATIVE_DENSITY_POLICY.priority_subdimensions} subdimension interpretations,
  {NARRATIVE_DENSITY_POLICY.priority_geographic_patterns} geographic patterns,
  {NARRATIVE_DENSITY_POLICY.priority_limitations} limitation, and
  {NARRATIVE_DENSITY_POLICY.priority_recommendations} recommendations.

PROHIBITIONS:
{json.dumps(list(NARRATIVE_PROHIBITIONS))}

Return:
{{
  "summary": CLAIM,
  "key_findings": [CLAIM],
  "subdimension_analysis": [
    {{
      "name": "...",
      "subsection_metric_id": "ledger id or null",
      "score_0_10": 0.0,
      "interpretation": CLAIM,
      "driver_metric_ids": ["ledger ids"]
    }}
  ],
  "geographic_patterns": [CLAIM],
  "data_limitations": [CLAIM],
  "recommendations": [CLAIM]
}}
where CLAIM is:
{{
  "text": "...",
  "claim_kind": "summary|finding|geographic_pattern|limitation|recommendation",
  "metric_ids": ["ledger ids"],
  "document_ids": [],
  "scope": "assessment|region|market|surveyed_traders|context",
  "polarity": "favorable|unfavorable|neutral|descriptive"
}}
"""
        if repairing:
            prompt += f"""
TARGETED REPAIR:
Regenerate only these fields: {json.dumps(flagged_fields or ['all affected fields'])}.
Preserve the meaning and content of every unlisted field. Previous artifact:
{json.dumps(previous)}
Relevant targets:
{json.dumps(relevant_targets)}
"""
        deterministic_fallback = fallback_dimension_narrative(
            dimension_profile,
            assessment_profile=profile,
        )
        traced = trace.invoke_json(
            model=llm,
            messages=[HumanMessage(content=prompt)],
            node="dimension_drafter",
            operation=(
                "mfi.dimension_correction.v1"
                if repairing
                else "mfi.dimension_drafting.v2"
            ),
            artifact_type="dimension",
            artifact_id=dimension,
            correction_attempt=attempt_number,
            validator=lambda result, dimension_profile=dimension_profile: parse_dimension_narrative(
                result,
                dimension_profile=dimension_profile,
                assessment_profile=profile,
                strict=True,
            ),
        )
        result = traced.payload
        drafted = traced.value
        _record_ignored_model_identifiers(diagnostics, result)
        llm_calls += 1
        mode = "fallback" if drafted == deterministic_fallback else "llm"
        _record_artifact_mode(diagnostics, "dimensions", dimension, mode)
        if mode == "fallback":
            fallback_dimensions.append(dimension)
        correction_outcome = (
            "deterministic_fallback" if mode == "fallback" else "llm_completed"
        )
        if repairing:
            correction_history = _record_correction_execution(
                correction_history,
                attempt_number=attempt_number,
                targets=relevant_targets,
                outcome=correction_outcome,
            )
        if repairing and previous and flagged_fields:
            merged = dict(previous)
            for field_name in flagged_fields:
                if field_name in drafted:
                    merged[field_name] = drafted[field_name]
            narratives[dimension] = merged
        else:
            narratives[dimension] = drafted

    narratives = deduplicate_dimension_recommendations(narratives, dimensions)

    updates = {
        "dimension_narratives": narratives,
        "llm_calls": state.get("llm_calls", 0) + llm_calls,
        "generation_diagnostics": diagnostics,
        "llm_diagnostics": trace.snapshot(),
        "correction_history": correction_history,
        "current_node": "dimension_drafter",
    }
    if fallback_dimensions:
        updates["warnings"] = [
            "Deterministic narrative fallback used for dimension(s): "
            + ", ".join(sorted(fallback_dimensions, key=str.casefold))
            + "."
        ]
    return updates


def node_market_recommendations_drafter(state: MFIReportState) -> dict:
    """Draft targeted narratives from only each market's scoped evidence."""
    logger.info("[MarketRecDrafter] Generating structured market narratives")
    profile = state.get("assessment_profile") or {}
    priority_names = set(profile.get("priority_market_names", []) or [])
    priority_profiles = [
        item
        for item in profile.get("markets", [])
        if isinstance(item, dict) and item.get("market_name") in priority_names
    ]
    if not priority_profiles:
        return {"market_narratives": {}, "current_node": "market_recommendations_drafter"}
    llm = get_model()
    narratives = dict(state.get("market_narratives") or {})
    catalog = state.get("claim_catalog") or {}
    targets = state.get("correction_targets") or []
    repairing = bool(targets)
    llm_calls = 0
    diagnostics = _generation_diagnostics(state)
    trace = get_trace_session(
        service="mfi-drafter",
        run_id=str(state.get("run_id") or "mfi-direct"),
        initial=state.get("llm_diagnostics"),
    )
    fallback_markets: List[str] = []
    correction_history = deepcopy(state.get("correction_history", []) or [])
    attempt_number = int(state.get("correction_attempts", 0) or 0)

    for market_profile in priority_profiles:
        market_name = str(market_profile["market_name"])
        relevant_targets = [
            target
            for target in targets
            if target.get("artifact_type") in {"market", "global"}
            and (
                target.get("artifact_type") == "global"
                or target.get("artifact_id") in {None, market_name}
            )
        ]
        if repairing and not relevant_targets:
            continue
        allowed_ids = market_catalog_ids(market_profile, catalog)
        prompt_catalog = compact_catalog(catalog, allowed_ids)
        previous = narratives.get(market_name)
        flagged_fields = sorted(
            {
                str(target["field_name"])
                for target in relevant_targets
                if target.get("field_name")
            }
        )
        prompt = f"""Draft a targeted MFI narrative for {market_name}.

Use English only and valid JSON only. The prompt contains only this market's
weak dimensions and matching market-scoped evidence. Every number must exactly
match a `formatted_value` in CLAIM_CATALOG and cite the associated `metric_id`.
Do not calculate or infer values. Every claim must declare metric_ids,
document_ids, scope, and polarity. Recommendations must cite evidence used by a
priority issue. A limitation is optional and may be included only when it is
specific to this market and cites market-scoped evidence. Do not repeat a
generic assessment limitation.

PROHIBITIONS:
{json.dumps(list(NARRATIVE_PROHIBITIONS))}

MARKET_PROFILE:
{json.dumps(market_profile)}

CLAIM_CATALOG:
{json.dumps(prompt_catalog)}

CONSTRAINTS:
{json.dumps(list(NARRATIVE_PROMPT_CONSTRAINTS))}

R8 CLAIM CEILINGS (maximums, not quotas; order by analytical importance):
- {NARRATIVE_DENSITY_POLICY.market_priority_issues} priority issues;
- {NARRATIVE_DENSITY_POLICY.market_recommendations} linked recommendations;
- {NARRATIVE_DENSITY_POLICY.market_limitations} market-specific limitation.

Return:
{{
  "priority_issues": [CLAIM],
  "recommended_interventions": [CLAIM],
  "limitations": [CLAIM]
}}
where CLAIM is:
{{
  "text": "...",
  "claim_kind": "finding|recommendation|limitation",
  "metric_ids": ["ledger ids"],
  "document_ids": [],
  "scope": "market",
  "polarity": "favorable|unfavorable|neutral|descriptive"
}}
"""
        if repairing:
            prompt += f"""
TARGETED REPAIR:
Regenerate only these fields: {json.dumps(flagged_fields or ['all affected fields'])}.
Preserve all unlisted fields. Previous artifact:
{json.dumps(previous)}
Relevant targets:
{json.dumps(relevant_targets)}
"""
        deterministic_fallback = fallback_market_narrative(market_profile)
        traced = trace.invoke_json(
            model=llm,
            messages=[HumanMessage(content=prompt)],
            node="market_recommendations_drafter",
            operation=(
                "mfi.market_correction.v1"
                if repairing
                else "mfi.market_drafting.v2"
            ),
            artifact_type="market",
            artifact_id=market_name,
            correction_attempt=attempt_number,
            validator=lambda result, market_profile=market_profile: parse_market_narrative(
                result,
                market_profile=market_profile,
                strict=True,
            ),
        )
        result = traced.payload
        drafted = traced.value
        _record_ignored_model_identifiers(diagnostics, result)
        llm_calls += 1
        mode = "fallback" if drafted == deterministic_fallback else "llm"
        _record_artifact_mode(diagnostics, "markets", market_name, mode)
        if mode == "fallback":
            fallback_markets.append(market_name)
        correction_outcome = (
            "deterministic_fallback" if mode == "fallback" else "llm_completed"
        )
        if repairing:
            correction_history = _record_correction_execution(
                correction_history,
                attempt_number=attempt_number,
                targets=relevant_targets,
                outcome=correction_outcome,
            )
        if repairing and previous and flagged_fields:
            merged = dict(previous)
            for field_name in flagged_fields:
                if field_name in drafted:
                    merged[field_name] = drafted[field_name]
            narratives[market_name] = merged
        else:
            narratives[market_name] = drafted

    updates = {
        "market_narratives": narratives,
        "llm_calls": state.get("llm_calls", 0) + llm_calls,
        "generation_diagnostics": diagnostics,
        "llm_diagnostics": trace.snapshot(),
        "correction_history": correction_history,
        "current_node": "market_recommendations_drafter",
    }
    if fallback_markets:
        updates["warnings"] = [
            "Deterministic narrative fallback used for market(s): "
            + ", ".join(sorted(fallback_markets, key=str.casefold))
            + "."
        ]
    return updates


# ============================================================================
# NODE: EXECUTIVE SUMMARY DRAFTER
# ============================================================================

def node_executive_summary_drafter(state: MFIReportState) -> dict:
    """Draft or repair the structured assessment executive summary."""
    logger.info("[ExecSummaryDrafter] Generating structured executive summary")
    profile = state.get("assessment_profile") or {}
    if not profile:
        return {"current_node": "executive_summary_drafter"}
    targets = state.get("correction_targets") or []
    repairing = bool(targets)
    relevant_targets = [
        target
        for target in targets
        if target.get("artifact_type") in {"executive_summary", "global"}
    ]
    if repairing and not relevant_targets:
        return {"current_node": "executive_summary_drafter"}
    llm = get_model()
    catalog = state.get("claim_catalog") or {}
    allowed_ids = executive_catalog_ids(profile)
    prompt_catalog = compact_catalog(catalog, allowed_ids)
    context = [
        statement
        for statement in state.get("context_evidence", [])
        if isinstance(statement, dict)
        and statement.get("classification")
        in {"corroborating", "potentially_explanatory"}
    ]
    priority_dimensions = [
        narrative
        for name, narrative in (state.get("dimension_narratives") or {}).items()
        if name in set(profile.get("priority_dimension_names", []))
    ]
    previous = state.get("executive_summary_narrative") or None
    flagged_fields = sorted(
        {
            str(target["field_name"])
            for target in relevant_targets
            if target.get("field_name")
        }
    )
    prompt = f"""Draft the structured executive summary for an MFI assessment.

Use English only and return valid JSON only. Every quantitative statement must
use an exact `formatted_value` from CLAIM_CATALOG and cite its `metric_id`.
Context claims must cite document IDs, retain their supplied classification,
and must not assert causality. Do not calculate or infer values. Do not use
national-score or risk-class terminology. Recommendations must cite a finding's
evidence.

PROHIBITIONS:
{json.dumps(list(NARRATIVE_PROHIBITIONS))}

ASSESSMENT_PROFILE:
{json.dumps({
    'assessed_market_count': profile.get('assessed_market_count'),
    'priority_dimension_names': profile.get('priority_dimension_names'),
    'priority_market_names': profile.get('priority_market_names'),
    'limitations': profile.get('limitations'),
})}

PRIORITY_DIMENSION_NARRATIVES:
{json.dumps(priority_dimensions)}

CLASSIFIED_CONTEXT:
{json.dumps(context)}

CLAIM_CATALOG:
{json.dumps(prompt_catalog)}

CONSTRAINTS:
{json.dumps(list(NARRATIVE_PROMPT_CONSTRAINTS))}

R8 CLAIM CEILINGS (maximums, not quotas; order by analytical importance):
- one key finding for each supplied priority dimension;
- {NARRATIVE_DENSITY_POLICY.executive_recommendations} recommendations;
- {NARRATIVE_DENSITY_POLICY.executive_limitations} limitations.

Return:
{{
  "motivation": CLAIM_OR_NULL,
  "key_findings": [CLAIM],
  "recommendations": [CLAIM],
  "limitations": [CLAIM]
}}
where CLAIM contains `text`, `claim_kind`, `metric_ids`, `document_ids`,
`scope`, and `polarity`. Claim identities are assigned by the application.
"""
    if repairing:
        prompt += f"""
TARGETED REPAIR:
Regenerate only these fields: {json.dumps(flagged_fields or ['all affected fields'])}.
Preserve all unlisted fields. Previous artifact:
{json.dumps(previous)}
Relevant targets:
{json.dumps(relevant_targets)}
"""
    trace = get_trace_session(
        service="mfi-drafter",
        run_id=str(state.get("run_id") or "mfi-direct"),
        initial=state.get("llm_diagnostics"),
    )
    deterministic_fallback = fallback_executive_narrative(profile)
    traced = trace.invoke_json(
        model=llm,
        messages=[HumanMessage(content=prompt)],
        node="executive_summary_drafter",
        operation=(
            "mfi.executive_correction.v1"
            if repairing
            else "mfi.executive_drafting.v2"
        ),
        artifact_type="executive_summary",
        artifact_id="executive_summary",
        correction_attempt=int(state.get("correction_attempts", 0) or 0),
        validator=lambda result: parse_executive_narrative(
            result,
            assessment_profile=profile,
            strict=True,
        ),
    )
    result = traced.payload
    drafted = traced.value
    model_identifier_count = count_model_identifiers(result)
    llm_calls = 1
    executive_mode = "fallback" if drafted == deterministic_fallback else "llm"
    correction_outcome = (
        "deterministic_fallback"
        if executive_mode == "fallback"
        else "llm_completed"
    )
    if repairing and previous and flagged_fields:
        merged = dict(previous)
        for field_name in flagged_fields:
            if field_name in drafted:
                merged[field_name] = drafted[field_name]
        drafted = merged
    diagnostics = _generation_diagnostics(state)
    diagnostics["ignored_model_identifier_count"] = int(
        diagnostics.get("ignored_model_identifier_count", 0) or 0
    ) + model_identifier_count
    if (
        diagnostics.get("executive_summary_mode") != "fallback"
        or executive_mode == "fallback"
    ):
        diagnostics["executive_summary_mode"] = executive_mode
    updates = {
        "executive_summary_narrative": drafted,
        "llm_calls": state.get("llm_calls", 0) + llm_calls,
        "generation_diagnostics": diagnostics,
        "llm_diagnostics": trace.snapshot(),
        "current_node": "executive_summary_drafter",
    }
    if repairing:
        updates["correction_history"] = _record_correction_execution(
            list(state.get("correction_history", []) or []),
            attempt_number=int(state.get("correction_attempts", 0) or 0),
            targets=relevant_targets,
            outcome=correction_outcome,
        )
    if executive_mode == "fallback":
        updates["warnings"] = [
            "Deterministic narrative fallback used for the executive summary."
        ]
    return updates


# ============================================================================
# NODE: RED TEAM (QA)
# ============================================================================

def _identity_failure_flag(error: MFIClaimIdentityError) -> Dict[str, Any]:
    artifacts = ", ".join(error.artifacts) or "unknown artifact"
    return {
        "flag_id": "system-claim-identity-contract-failure",
        "source": "system",
        "code": "claim_identity_contract_failure",
        "severity": "high",
        "artifact_type": "global",
        "artifact_id": None,
        "field_name": None,
        "claim_id": None,
        "message": (
            "Narrative identity validation failed for an internal artifact; "
            "deterministic fallback content was used."
        ),
        "recommendation": (
            "Review the deterministic fallback and technical identity diagnostics."
        ),
        "metric_ids": [],
        "document_ids": [],
        "expected_value": CLAIM_IDENTITY_VERSION,
        "actual_value": artifacts,
        "repairable": False,
    }


def _deterministic_identity_fallbacks(
    state: MFIReportState,
) -> tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any], List[Dict[str, Any]], List[str]]:
    """Build a fully application-owned narrative set after an internal ID breach."""
    profile = state.get("assessment_profile") or {}
    dimensions = {
        str(item["dimension"]): fallback_dimension_narrative(
            item,
            assessment_profile=profile,
        )
        for item in profile.get("dimensions", []) or []
        if isinstance(item, dict) and item.get("dimension")
    }
    priority_names = set(profile.get("priority_market_names", []) or [])
    markets = {
        str(item["market_name"]): fallback_market_narrative(item)
        for item in profile.get("markets", []) or []
        if isinstance(item, dict) and item.get("market_name") in priority_names
    }
    executive = fallback_executive_narrative(profile)
    # Context is omitted under an internal identity breach because a high-severity
    # statement may itself be the malformed artifact. Raw statements remain in the
    # technical graph trace; none is allowed back into reader-facing fallback content.
    had_context = bool(state.get("context_evidence", []) or [])
    context: List[Dict[str, Any]] = []
    dimensions, markets, executive, context = canonicalize_narrative_identities(
        dimension_narratives=dimensions,
        market_narratives=markets,
        executive_narrative=executive,
        context_evidence=context,
        preserve_context_ids=False,
    )
    artifacts = [
        *[f"dimension:{name}" for name in dimensions],
        *[f"market:{name}" for name in markets],
        "executive_summary",
    ]
    if had_context:
        artifacts.append("context")
    return dimensions, markets, executive, context, artifacts

def node_deterministic_claim_validator(state: MFIReportState) -> dict:
    """Validate every narrative claim against the closed evidence catalog."""
    logger.info("[ClaimValidator] Validating structured claims")
    identity_flag: Optional[Dict[str, Any]] = None
    diagnostics = _generation_diagnostics(state)
    try:
        (
            validation,
            dimensions,
            markets,
            executive,
            context,
            flag_payload,
        ) = validate_structured_narratives(
            context_evidence=state.get("context_evidence", []),
            dimension_narratives=state.get("dimension_narratives", {}),
            market_narratives=state.get("market_narratives", {}),
            executive_narrative=state.get("executive_summary_narrative", {}),
            claim_catalog=state.get("claim_catalog", {}),
            assessment_profile=state.get("assessment_profile") or {},
            documents=state.get("contextual_documents", []),
        )
    except MFIClaimIdentityError as exc:
        identity_flag = _identity_failure_flag(exc)
        dimensions, markets, executive, context, artifacts = (
            _deterministic_identity_fallbacks(state)
        )
        diagnostics["identity_fallback_artifacts"] = sorted(
            set(diagnostics.get("identity_fallback_artifacts", [])) | set(artifacts)
        )
        (
            validation,
            dimensions,
            markets,
            executive,
            context,
            flag_payload,
        ) = validate_structured_narratives(
            context_evidence=context,
            dimension_narratives=dimensions,
            market_narratives=markets,
            executive_narrative=executive,
            claim_catalog=state.get("claim_catalog", {}),
            assessment_profile=state.get("assessment_profile") or {},
            documents=state.get("contextual_documents", []),
        )
    flags = list(flag_payload.get("flags", []))
    if identity_flag is not None:
        flags.append(identity_flag)
        validation["flags"] = [*validation.get("flags", []), identity_flag]
        validation["status"] = "failed"
    return {
        "claim_validation": validation,
        "dimension_narratives": dimensions,
        "market_narratives": markets,
        "executive_summary_narrative": executive,
        "context_evidence": context,
        "deterministic_flags": flags,
        "generation_diagnostics": diagnostics,
        "current_node": "deterministic_claim_validator",
    }


def _cited_catalog(
    state: MFIReportState,
) -> Dict[str, Dict[str, Any]]:
    cited: set[str] = set()

    def visit(value: Any) -> None:
        if isinstance(value, dict):
            metric_ids = value.get("metric_ids")
            if isinstance(metric_ids, list):
                cited.update(str(item) for item in metric_ids if item)
            for nested in value.values():
                visit(nested)
        elif isinstance(value, list):
            for nested in value:
                visit(nested)

    visit(state.get("dimension_narratives", {}))
    visit(state.get("market_narratives", {}))
    visit(state.get("executive_summary_narrative", {}))
    return compact_catalog(state.get("claim_catalog", {}), sorted(cited))


def node_red_team(state: MFIReportState) -> dict:
    """Run semantic LLM QA after deterministic claim validation."""
    logger.info("[RedTeam] Reviewing structured MFI narratives")
    if not state.get("executive_summary_narrative"):
        return {"red_team_flags": [], "current_node": "red_team"}
    trace = get_trace_session(
        service="mfi-drafter",
        run_id=str(state.get("run_id") or "mfi-direct"),
        initial=state.get("llm_diagnostics"),
    )
    prompt = f"""Red-Team this structured MFI assessment narrative.

Use English only. Review the structured artifacts against the cited catalog,
deterministic flags, context classifications, and limitations. Check semantic
interpretation, polarity, scope, coverage qualification, recommendation
linkage, unsupported causal or affordability claims, priority drill-downs,
and any conclusion about transfer modality. Do not invent new facts or
recalculate values. Return valid JSON only.

PROHIBITIONS:
{json.dumps(list(NARRATIVE_PROHIBITIONS))}

DIMENSION_NARRATIVES:
{json.dumps(state.get('dimension_narratives', {}))}

MARKET_NARRATIVES:
{json.dumps(state.get('market_narratives', {}))}

EXECUTIVE_SUMMARY:
{json.dumps(state.get('executive_summary_narrative', {}))}

CITED_CATALOG:
{json.dumps(_cited_catalog(state))}

CONTEXT_EVIDENCE:
{json.dumps(state.get('context_evidence', []))}

LIMITATIONS:
{json.dumps((state.get('assessment_profile') or {}).get('limitations', []))}

DETERMINISTIC_FLAGS:
{json.dumps(state.get('deterministic_flags', []))}

Return:
{{"flags": [{{
  "flag_id": "stable id",
  "code": "...",
  "severity": "high|medium|low",
  "artifact_type": "context|dimension|market|executive_summary|global",
  "artifact_id": "dimension or market name, or null",
  "field_name": "exact output field, or null",
  "claim_id": "exact claim id, or null",
  "message": "...",
  "recommendation": "...",
  "metric_ids": [],
  "document_ids": [],
  "repairable": true
}}]}}
"""
    def _validate_red_team(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        if not isinstance(payload, dict) or not isinstance(
            payload.get("flags"), list
        ):
            raise ValueError("Red-Team response does not satisfy its schema")
        required = {
            "flag_id",
            "code",
            "severity",
            "artifact_type",
            "artifact_id",
            "field_name",
            "claim_id",
            "message",
            "recommendation",
            "metric_ids",
            "document_ids",
            "repairable",
        }
        for index, flag in enumerate(payload["flags"]):
            if not isinstance(flag, dict):
                raise ValueError(f"flags[{index}] must be an object")
            missing = sorted(required - set(flag))
            if missing:
                raise ValueError(f"flags[{index}] is missing: {', '.join(missing)}")
            if flag.get("severity") not in {"high", "medium", "low"}:
                raise ValueError(f"flags[{index}].severity is invalid")
            if flag.get("artifact_type") not in {
                "context",
                "dimension",
                "market",
                "executive_summary",
                "global",
            }:
                raise ValueError(f"flags[{index}].artifact_type is invalid")
            if not str(flag.get("code") or "").strip() or not str(
                flag.get("message") or ""
            ).strip():
                raise ValueError(f"flags[{index}] requires code and message")
            if not isinstance(flag.get("metric_ids"), list) or not isinstance(
                flag.get("document_ids"), list
            ):
                raise ValueError(f"flags[{index}] citations must be lists")
            if not isinstance(flag.get("repairable"), bool):
                raise ValueError(f"flags[{index}].repairable must be boolean")
        return normalize_red_team_flags(payload)

    traced = trace.invoke_json(
        model=get_model(),
        messages=[HumanMessage(content=prompt)],
        node="red_team",
        operation="mfi.red_team_review.v2",
        artifact_type="global",
        artifact_id="qa_review",
        correction_attempt=int(state.get("correction_attempts", 0) or 0),
        validator=_validate_red_team,
    )
    flags = traced.value
    llm_calls = 1
    red_team_status = "completed"
    qa_review = build_qa_review(
        state.get("deterministic_flags", []),
        flags,
        correction_attempts=state.get("correction_attempts", 0),
        correction_history=state.get("correction_history", []),
    )
    diagnostics = _generation_diagnostics(state)
    diagnostics["red_team_status"] = red_team_status
    return {
        "red_team_flags": flags,
        "qa_review": qa_review,
        "llm_calls": state.get("llm_calls", 0) + llm_calls,
        "generation_diagnostics": diagnostics,
        "llm_diagnostics": trace.snapshot(),
        "current_node": "red_team",
    }


# ============================================================================
# ROUTING & GRAPH BUILDER
# ============================================================================

MAX_CORRECTION_ATTEMPTS = 3

_CORRECTION_EXECUTION_PRECEDENCE = {
    "pending": 0,
    "not_executed": 1,
    "llm_completed": 2,
    "deterministic_fallback": 3,
    "llm_or_schema_failed": 4,
}


def _flags_for_correction_target(
    flags: List[Dict[str, Any]],
    target: Dict[str, Any],
    claim_id: Optional[str],
) -> List[Dict[str, Any]]:
    target_flag_ids = {str(value) for value in target.get("flag_ids", []) if value}
    matched = [
        flag
        for flag in flags
        if str(flag.get("flag_id") or "") in target_flag_ids
        and (
            str(flag.get("claim_id") or "") == str(claim_id or "")
            if claim_id is not None
            else not flag.get("claim_id")
        )
    ]
    return matched


def _start_correction_attempt(
    *,
    history: List[Dict[str, Any]],
    flags: List[Dict[str, Any]],
    targets: List[Dict[str, Any]],
    attempt_number: int,
) -> List[Dict[str, Any]]:
    """Append one auditable record per targeted claim (or unscoped target)."""
    updated = deepcopy(history)
    for target in targets:
        claim_ids: List[Optional[str]] = [
            str(value) for value in target.get("claim_ids", []) if value
        ]
        target_flags = [
            flag
            for flag in flags
            if str(flag.get("flag_id") or "")
            in {str(value) for value in target.get("flag_ids", []) if value}
        ]
        if any(not flag.get("claim_id") for flag in target_flags) or not claim_ids:
            claim_ids.append(None)
        for claim_id in claim_ids:
            matched = _flags_for_correction_target(flags, target, claim_id)
            record = MFICorrectionAttemptRecord(
                attempt_number=attempt_number,
                artifact_type=str(target.get("artifact_type") or "global"),
                artifact_id=target.get("artifact_id"),
                field_name=target.get("field_name"),
                claim_id=claim_id,
                flag_ids=sorted(
                    {str(flag.get("flag_id")) for flag in matched if flag.get("flag_id")}
                ),
                flag_codes=sorted(
                    {str(flag.get("code")) for flag in matched if flag.get("code")}
                ),
            ).model_dump()
            if record not in updated:
                updated.append(record)
    return updated


def _record_correction_execution(
    history: List[Dict[str, Any]],
    *,
    attempt_number: int,
    targets: List[Dict[str, Any]],
    outcome: Literal[
        "llm_completed",
        "deterministic_fallback",
        "llm_or_schema_failed",
        "not_executed",
    ],
) -> List[Dict[str, Any]]:
    """Record the worst execution outcome without losing an earlier failure."""
    updated = deepcopy(history)
    target_keys = {
        (
            str(target.get("artifact_type") or "global"),
            target.get("artifact_id"),
            target.get("field_name"),
        )
        for target in targets
    }
    for record in updated:
        key = (
            str(record.get("artifact_type") or "global"),
            record.get("artifact_id"),
            record.get("field_name"),
        )
        if int(record.get("attempt_number") or 0) != attempt_number or key not in target_keys:
            continue
        previous = str(record.get("execution_outcome") or "pending")
        if _CORRECTION_EXECUTION_PRECEDENCE[outcome] >= _CORRECTION_EXECUTION_PRECEDENCE.get(
            previous, 0
        ):
            record["execution_outcome"] = outcome
    return updated


def _correction_flag_matches_record(
    flag: Dict[str, Any], record: Dict[str, Any]
) -> bool:
    codes = {str(value) for value in record.get("flag_codes", []) if value}
    if codes and str(flag.get("code") or "") not in codes:
        return False
    claim_id = record.get("claim_id")
    if claim_id is not None:
        return str(flag.get("claim_id") or "") == str(claim_id)
    if str(flag.get("artifact_type") or "global") != str(
        record.get("artifact_type") or "global"
    ):
        return False
    if record.get("artifact_id") is not None and str(flag.get("artifact_id") or "") != str(
        record.get("artifact_id")
    ):
        return False
    if record.get("field_name") is not None and str(flag.get("field_name") or "") != str(
        record.get("field_name")
    ):
        return False
    return True


def _reconcile_correction_history(
    history: List[Dict[str, Any]],
    flags: List[Dict[str, Any]],
    *,
    close_pending_execution: bool = False,
) -> List[Dict[str, Any]]:
    """Freeze each attempt's validation outcome at the next QA boundary."""
    updated = deepcopy(history)
    for record in updated:
        if close_pending_execution and record.get("execution_outcome") == "pending":
            record["execution_outcome"] = "not_executed"
        if record.get("validation_outcome") != "pending":
            continue
        expected_codes = {
            str(value) for value in record.get("flag_codes", []) if value
        }
        remaining_codes = {
            str(flag.get("code"))
            for flag in flags
            if _correction_flag_matches_record(flag, record) and flag.get("code")
        }
        if not remaining_codes:
            record["validation_outcome"] = "resolved"
        elif expected_codes and remaining_codes < expected_codes:
            record["validation_outcome"] = "partially_resolved"
        else:
            record["validation_outcome"] = "unresolved"
    return [
        MFICorrectionAttemptRecord.model_validate(record).model_dump()
        for record in updated
    ]

def should_correct(state: MFIReportState) -> Literal["correct", "finish"]:
    """Route material repairable issues through at most three targeted repairs."""
    flags = [
        *state.get("deterministic_flags", []),
        *state.get("red_team_flags", []),
    ]
    attempts = state.get("correction_attempts", 0)
    material = material_repairable_flags(flags)
    if material and attempts < MAX_CORRECTION_ATTEMPTS:
        logger.info(
            "Corrections needed: %s material flags, attempt %s",
            len(material),
            attempts + 1,
        )
        return "correct"
    return "finish"


def node_prepare_correction(state: MFIReportState) -> dict:
    """Build exact artifact/field targets without changing unaffected content."""
    flags = [
        *state.get("deterministic_flags", []),
        *state.get("red_team_flags", []),
    ]
    targets = build_correction_targets(flags)
    attempt_number = state.get("correction_attempts", 0) + 1
    history = _reconcile_correction_history(
        list(state.get("correction_history", []) or []),
        flags,
    )
    history = _start_correction_attempt(
        history=history,
        flags=flags,
        targets=targets,
        attempt_number=attempt_number,
    )
    updates: Dict[str, Any] = {
        "correction_targets": targets,
        "correction_attempts": attempt_number,
        "correction_history": history,
        "current_node": "targeted_correction",
    }
    trace = get_trace_session(
        service="mfi-drafter",
        run_id=str(state.get("run_id") or "mfi-direct"),
        initial=state.get("llm_diagnostics"),
    )
    context_targets = [
        target
        for target in targets
        if target.get("artifact_type") in {"context", "global"}
    ]
    documents = [
        item
        for item in state.get("contextual_documents", [])
        if isinstance(item, dict) and item.get("doc_id")
    ]
    if not context_targets or not documents:
        if context_targets:
            trace.record_skip(
                node="targeted_correction",
                operation="mfi.context_correction.v1",
                reason="no_contextual_documents",
                artifact_type="context",
            )
            updates["llm_diagnostics"] = trace.snapshot()
        unavailable_context_targets = [
            target
            for target in context_targets
            if target.get("artifact_type") == "context"
        ]
        if unavailable_context_targets:
            updates["correction_history"] = _record_correction_execution(
                history,
                attempt_number=attempt_number,
                targets=unavailable_context_targets,
                outcome="not_executed",
            )
        return updates
    prompt = f"""Repair only the targeted contextual evidence statements.

Use English only and return valid JSON only. Each statement must cite only a
supplied document_id and use one classification: corroborating,
potentially_explanatory, or unrelated. Do not assert causality or introduce MFI
values. Preserve every statement that is not targeted.

DOCUMENTS:
{json.dumps(documents)}

CURRENT_CONTEXT:
{json.dumps(state.get('context_evidence', []))}

TARGETS:
{json.dumps(context_targets)}

Return {{"statements": [{{"statement_id": "...", "text": "...",
"classification": "...", "document_ids": ["..."]}}]}}.
Each returned statement_id must exactly match a canonical statement_id in the
targeted CURRENT_CONTEXT. Do not create or rename identifiers.
"""
    targeted_ids = {
        str(target.get("artifact_id"))
        for target in context_targets
        if target.get("artifact_type") == "context"
        and target.get("artifact_id")
    }
    has_global = any(
        target.get("artifact_type") == "global" for target in context_targets
    )
    current_ids = [
        str(statement.get("statement_id"))
        for statement in state.get("context_evidence", [])
        if isinstance(statement, dict) and statement.get("statement_id")
    ]
    expected_ids = (
        current_ids
        if has_global or not targeted_ids
        else [value for value in current_ids if value in targeted_ids]
    )

    def _validate_context_correction(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        raw_statements = payload.get("statements")
        if not isinstance(raw_statements, list):
            raise ValueError("Context correction statements must be a list")
        returned_ids = [
            str(statement.get("statement_id") or "")
            for statement in raw_statements
            if isinstance(statement, dict)
        ]
        if returned_ids != expected_ids:
            raise ValueError(
                "Context correction must preserve the supplied canonical statement IDs"
            )
        repaired, parse_flags = parse_context_evidence(
            payload,
            documents=documents,
            expected_statement_ids=expected_ids,
        )
        if parse_flags:
            raise ValueError("Context correction contains invalid document citations")
        return repaired

    traced = trace.invoke_json(
        model=get_model(),
        messages=[HumanMessage(content=prompt)],
        node="targeted_correction",
        operation="mfi.context_correction.v1",
        artifact_type="context",
        artifact_id="context_evidence",
        correction_attempt=attempt_number,
        validator=_validate_context_correction,
    )
    repaired = traced.value
    if has_global or not targeted_ids:
        updates["context_evidence"] = repaired
    else:
        replacements = {
            str(statement.get("statement_id")): statement
            for statement in repaired
            if statement.get("statement_id") in targeted_ids
        }
        updates["context_evidence"] = [
            replacements.get(str(statement.get("statement_id")), statement)
            for statement in state.get("context_evidence", [])
            if isinstance(statement, dict)
        ]
    updates["llm_calls"] = state.get("llm_calls", 0) + 1
    updates["llm_diagnostics"] = trace.snapshot()
    updates["correction_history"] = _record_correction_execution(
        history,
        attempt_number=attempt_number,
        targets=context_targets,
        outcome="llm_completed",
    )
    return updates


def _mark_unresolved_claims(
    value: Any,
    flag_ids_by_claim: Dict[str, List[str]],
    flag_codes_by_claim: Optional[Dict[str, List[str]]] = None,
) -> Any:
    if isinstance(value, dict):
        result = {
            key: _mark_unresolved_claims(
                nested, flag_ids_by_claim, flag_codes_by_claim
            )
            for key, nested in value.items()
        }
        claim_id = result.get("claim_id")
        if claim_id in flag_ids_by_claim:
            result["validation_status"] = "unverified"
            # Identifiers go in their own field. They hash the flag message, so they
            # change whenever wording does; the codes beside them stay stable and are what
            # a reader or a downstream renderer should key on.
            result["validation_flag_ids"] = sorted(
                set(result.get("validation_flag_ids", []))
                | set(flag_ids_by_claim[claim_id])
            )
            if flag_codes_by_claim:
                result["validation_flags"] = sorted(
                    set(result.get("validation_flags", []))
                    | set(flag_codes_by_claim.get(claim_id, []))
                )
        return result
    if isinstance(value, list):
        return [
            _mark_unresolved_claims(
                item, flag_ids_by_claim, flag_codes_by_claim
            )
            for item in value
        ]
    return value


def _apply_context_qa_policy(
    context_evidence: List[Dict[str, Any]],
    flags: List[Dict[str, Any]],
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Mark context findings and withdraw causal statements deterministically."""
    statements = deepcopy(context_evidence)
    substitutions: List[Dict[str, Any]] = []
    for statement in statements:
        if not isinstance(statement, dict):
            continue
        statement_id = str(statement.get("statement_id") or "")
        matched = [
            flag
            for flag in flags
            if str(flag.get("artifact_type") or "") == "context"
            and str(flag.get("artifact_id") or "") == statement_id
            and str(flag.get("severity") or "") in {"high", "medium"}
        ]
        if not matched:
            continue
        statement["validation_status"] = "unverified"
        statement["validation_flags"] = sorted(
            {str(flag.get("code")) for flag in matched if flag.get("code")}
        )
        statement["validation_flag_ids"] = sorted(
            {str(flag.get("flag_id")) for flag in matched if flag.get("flag_id")}
        )
        high = [flag for flag in matched if flag.get("severity") == "high"]
        if not high:
            continue
        rejected_text = str(statement.get("text") or "")
        rejected_document_ids = list(statement.get("document_ids", []) or [])
        replacement = (
            "A contextual statement was withdrawn because it could not be "
            "validated for use in this report."
        )
        statement["text"] = replacement
        statement["document_ids"] = []
        statement["substituted"] = True
        substitutions.append(
            {
                "claim_id": statement_id,
                "artifact_type": "context",
                "artifact_id": statement_id,
                "field_name": "text",
                "claim_kind": "context",
                "rejected_text": rejected_text,
                "replacement_text": replacement,
                "rejected_metric_ids": [],
                "rejected_document_ids": rejected_document_ids,
                "codes": sorted(
                    {str(flag.get("code")) for flag in high if flag.get("code")}
                ),
                "flag_ids": sorted(
                    {str(flag.get("flag_id")) for flag in high if flag.get("flag_id")}
                ),
                "severity": "high",
                "disposition": "replaced_by_deterministic_fallback",
            }
        )
    return statements, substitutions


def _visible_qa_claim_ids(*values: Any) -> set[str]:
    visible: set[str] = set()

    def visit(value: Any) -> None:
        if isinstance(value, dict):
            if value.get("claim_id"):
                visible.add(str(value["claim_id"]))
            if value.get("statement_id") and value.get("classification") != "unrelated":
                visible.add(str(value["statement_id"]))
            for nested in value.values():
                visit(nested)
        elif isinstance(value, list):
            for nested in value:
                visit(nested)

    for value in values:
        visit(value)
    return visible


def node_finalize_qa(state: MFIReportState) -> dict:
    """Finalize delivery, retaining visible warnings for unresolved material QA."""
    deterministic_flags = list(state.get("deterministic_flags", []) or [])
    diagnostics = _generation_diagnostics(state)
    try:
        (
            canonical_dimensions,
            canonical_markets,
            canonical_executive,
            canonical_context,
        ) = canonicalize_narrative_identities(
            dimension_narratives=state.get("dimension_narratives", {}),
            market_narratives=state.get("market_narratives", {}),
            executive_narrative=state.get("executive_summary_narrative", {}),
            context_evidence=state.get("context_evidence", []),
        )
    except MFIClaimIdentityError as exc:
        (
            canonical_dimensions,
            canonical_markets,
            canonical_executive,
            canonical_context,
            artifacts,
        ) = _deterministic_identity_fallbacks(state)
        identity_flag = _identity_failure_flag(exc)
        if not any(
            flag.get("code") == identity_flag["code"]
            for flag in deterministic_flags
            if isinstance(flag, dict)
        ):
            deterministic_flags.append(identity_flag)
        diagnostics["identity_fallback_artifacts"] = sorted(
            set(diagnostics.get("identity_fallback_artifacts", [])) | set(artifacts)
        )
    assert_claim_identity_contract(
        canonical_dimensions,
        canonical_markets,
        canonical_executive,
        canonical_context,
    )
    combined = [
        *deterministic_flags,
        *state.get("red_team_flags", []),
    ]
    material = [
        flag
        for flag in combined
        if str(flag.get("severity")) in {"high", "medium"}
    ]
    flag_ids_by_claim: Dict[str, List[str]] = {}
    flag_codes_by_claim: Dict[str, List[str]] = {}
    for flag in material:
        if flag.get("claim_id"):
            flag_ids_by_claim.setdefault(str(flag["claim_id"]), []).append(
                str(flag.get("flag_id"))
            )
            flag_codes_by_claim.setdefault(str(flag["claim_id"]), []).append(
                str(flag.get("code"))
            )
    # Repair attempts are spent by the time this runs, so a surviving high-severity
    # finding means the statement is unsupported rather than merely unpolished. Withdraw
    # first, then mark, so a withdrawn claim still carries its unverified status.
    (
        dimension_narratives,
        market_narratives,
        executive_narrative,
        substitutions,
    ) = apply_unresolved_claim_policy(
        dimension_narratives=canonical_dimensions,
        market_narratives=canonical_markets,
        executive_narrative=canonical_executive,
        flags=combined,
    )
    context_evidence, context_substitutions = _apply_context_qa_policy(
        canonical_context,
        material,
    )
    context_status = reconcile_context_status(
        state.get("context_status") or not_attempted_context_status().model_dump(),
        documents=list(state.get("contextual_documents", []) or []),
        statements=context_evidence,
    )
    substitutions.extend(context_substitutions)
    unmatched = unmatched_high_claim_ids(combined, substitutions)

    correction_history = _reconcile_correction_history(
        list(state.get("correction_history", []) or []),
        combined,
        close_pending_execution=True,
    )
    review = build_qa_review(
        deterministic_flags,
        state.get("red_team_flags", []),
        correction_attempts=state.get("correction_attempts", 0),
        correction_history=correction_history,
    )

    warnings: list[str] = []
    visible_claim_ids = _visible_qa_claim_ids(
        dimension_narratives,
        market_narratives,
        executive_narrative,
        context_evidence,
    )
    marked_claim_findings = [
        flag
        for flag in material
        if str(
            flag.get("claim_id")
            or (
                flag.get("artifact_id")
                if flag.get("artifact_type") == "context"
                else ""
            )
            or ""
        )
        in visible_claim_ids
    ]
    global_findings = [
        flag for flag in material if flag not in marked_claim_findings
    ]
    if marked_claim_findings:
        warnings.append(
            "Narrative QA completed with unresolved material issues. "
            "Review the claim-level notices and final QA findings table."
        )
    if global_findings or unmatched:
        warnings.append(
            "Material process-level QA issues remain; consult the final QA "
            "findings table."
        )
    if substitutions:
        warnings.append(
            "Statements that could not be validated against the assessment evidence "
            "were withdrawn. The rejected drafts are retained in the technical QA "
            "details."
        )
    severity_counts = Counter(
        str(flag.get("severity"))
        for flag in review.get("flags", [])
        if isinstance(flag, dict)
    )
    diagnostics.update(
        {
            "correction_attempts": state.get("correction_attempts", 0),
            "unresolved_high_count": int(severity_counts.get("high", 0)),
            "unresolved_medium_count": int(severity_counts.get("medium", 0)),
            "unresolved_low_count": int(severity_counts.get("low", 0)),
            "claim_substitutions": substitutions,
            "unmatched_high_claim_ids": unmatched,
        }
    )
    return {
        "dimension_narratives": _mark_unresolved_claims(
            dimension_narratives, flag_ids_by_claim, flag_codes_by_claim
        ),
        "market_narratives": _mark_unresolved_claims(
            market_narratives, flag_ids_by_claim, flag_codes_by_claim
        ),
        "executive_summary_narrative": _mark_unresolved_claims(
            executive_narrative, flag_ids_by_claim, flag_codes_by_claim
        ),
        "context_evidence": context_evidence,
        "context_status": context_status.model_dump(),
        "qa_review": review,
        "correction_history": correction_history,
        "generation_diagnostics": diagnostics,
        "claim_substitutions": substitutions,
        "deterministic_flags": deterministic_flags,
        "correction_targets": [],
        "warnings": warnings,
        "current_node": "finalize_qa",
    }


def _globalize_material_delivery_flags(
    flags: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    globalized: List[Dict[str, Any]] = []
    for raw in flags:
        flag = deepcopy(raw)
        if str(flag.get("severity")) in {"high", "medium"}:
            original = "/".join(
                str(value)
                for value in (
                    flag.get("artifact_type"),
                    flag.get("artifact_id"),
                    flag.get("field_name"),
                    flag.get("claim_id"),
                )
                if value
            )
            flag["artifact_type"] = "global"
            flag["artifact_id"] = None
            flag["field_name"] = None
            flag["claim_id"] = None
            if original and not flag.get("actual_value"):
                flag["actual_value"] = original
        globalized.append(flag)
    return globalized


def node_finalize_delivery(state: MFIReportState) -> dict:
    """Build and validate the canonical reader payload before run completion."""
    from app.shared.report_blocks import build_mfi_report_blocks

    diagnostics = _generation_diagnostics(state)
    payload = dict(state)
    assert_claim_identity_contract(
        payload.get("dimension_narratives", {}),
        payload.get("market_narratives", {}),
        payload.get("executive_summary_narrative", {}),
        payload.get("context_evidence", []),
    )
    fallback_used = bool(diagnostics.get("identity_fallback_artifacts"))
    try:
        blocks = build_mfi_report_blocks(payload)
    except ValueError as exc:
        if "must map to one visible marker or global notice" not in str(exc):
            raise
        # A delivery mapping defect is itself material, but it must never turn a
        # completed run into a result-retrieval 500. Withdraw all drafted artifacts,
        # preserve every original flag as a global finding, and validate once more.
        dimensions, markets, executive, context, artifacts = (
            _deterministic_identity_fallbacks(state)
        )
        deterministic_flags = _globalize_material_delivery_flags(
            list(state.get("deterministic_flags", []) or [])
        )
        red_team_flags = _globalize_material_delivery_flags(
            list(state.get("red_team_flags", []) or [])
        )
        identity_flag = _identity_failure_flag(
            MFIClaimIdentityError(
                "Material QA mapping failed during delivery validation.",
                artifacts=artifacts,
            )
        )
        if not any(
            flag.get("code") == identity_flag["code"]
            for flag in deterministic_flags
            if isinstance(flag, dict)
        ):
            deterministic_flags.append(identity_flag)
        review = build_qa_review(
            deterministic_flags,
            red_team_flags,
            correction_attempts=state.get("correction_attempts", 0),
            correction_history=state.get("correction_history", []),
        )
        diagnostics["identity_fallback_artifacts"] = sorted(
            set(diagnostics.get("identity_fallback_artifacts", [])) | set(artifacts)
        )
        payload.update(
            {
                "dimension_narratives": dimensions,
                "market_narratives": markets,
                "executive_summary_narrative": executive,
                "context_evidence": context,
                "deterministic_flags": deterministic_flags,
                "red_team_flags": red_team_flags,
                "qa_review": review,
                "claim_substitutions": [],
                "context_status": reconcile_context_status(
                    state.get("context_status")
                    or not_attempted_context_status().model_dump(),
                    documents=list(state.get("contextual_documents", []) or []),
                    statements=context,
                ).model_dump(),
            }
        )
        blocks = build_mfi_report_blocks(payload)
        fallback_used = True
    diagnostics["delivery_contract_status"] = (
        "fallback_validated" if fallback_used else "validated"
    )
    updates = {
        key: payload[key]
        for key in (
            "dimension_narratives",
            "market_narratives",
            "executive_summary_narrative",
            "context_evidence",
            "context_status",
            "deterministic_flags",
            "red_team_flags",
            "qa_review",
            "claim_substitutions",
        )
        if key in payload and payload.get(key) != state.get(key)
    }
    updates.update(
        {
            "generation_diagnostics": diagnostics,
            "report_blocks": [block.model_dump(mode="json") for block in blocks],
            "current_node": "finalize_delivery",
        }
    )
    if fallback_used and not state.get("generation_diagnostics", {}).get(
        "identity_fallback_artifacts"
    ):
        updates["warnings"] = [
            "An internal narrative identity issue was replaced with deterministic "
            "fallback content; consult the final QA findings table."
        ]
    return updates


def build_graph(on_step: Optional[OnStepCallback] = None):
    """Costruisce il grafo LangGraph per MFI Report."""

    def wrap_node(node_name: str, fn):
        def wrapped(state: MFIReportState):
            state_dict = dict(state)
            if on_step is not None:
                on_step(node_name, state_dict)

            updates = fn(state)

            if on_step is not None:
                merged = dict(state_dict)
                if isinstance(updates, dict):
                    merged.update(updates)
                on_step(node_name, merged)

            return updates

        return wrapped

    graph = StateGraph(MFIReportState)
    
    # Add nodes
    graph.add_node("mfi_data_agent", wrap_node("mfi_data_agent", node_mfi_data_agent))
    graph.add_node("mfi_analysis", wrap_node("mfi_analysis", node_mfi_analysis))
    graph.add_node("context_retrieval", wrap_node("context_retrieval", node_context_retrieval))
    graph.add_node("context_extractor", wrap_node("context_extractor", node_context_extractor))
    graph.add_node("mfi_graph_designer", wrap_node("mfi_graph_designer", node_mfi_graph_designer))
    graph.add_node("dimension_drafter", wrap_node("dimension_drafter", node_dimension_drafter))
    graph.add_node(
        "market_recommendations_drafter",
        wrap_node("market_recommendations_drafter", node_market_recommendations_drafter),
    )
    graph.add_node(
        "executive_summary_drafter",
        wrap_node("executive_summary_drafter", node_executive_summary_drafter),
    )
    graph.add_node(
        "deterministic_claim_validator",
        wrap_node(
            "deterministic_claim_validator",
            node_deterministic_claim_validator,
        ),
    )
    graph.add_node("red_team", wrap_node("red_team", node_red_team))
    graph.add_node(
        "targeted_correction",
        wrap_node("targeted_correction", node_prepare_correction),
    )
    graph.add_node("finalize_qa", wrap_node("finalize_qa", node_finalize_qa))
    graph.add_node(
        "finalize_delivery",
        wrap_node("finalize_delivery", node_finalize_delivery),
    )
    
    # Set entry point
    graph.set_entry_point("mfi_data_agent")
    
    # Linear flow
    graph.add_edge("mfi_data_agent", "mfi_analysis")
    graph.add_edge("mfi_analysis", "context_retrieval")
    graph.add_edge("context_retrieval", "context_extractor")
    graph.add_edge("context_extractor", "mfi_graph_designer")
    graph.add_edge("mfi_graph_designer", "dimension_drafter")
    graph.add_edge("dimension_drafter", "market_recommendations_drafter")
    graph.add_edge("market_recommendations_drafter", "executive_summary_drafter")
    graph.add_edge("executive_summary_drafter", "deterministic_claim_validator")
    graph.add_edge("deterministic_claim_validator", "red_team")
    
    # QA Loop
    graph.add_conditional_edges(
        "red_team",
        should_correct,
        {
            "correct": "targeted_correction",
            "finish": "finalize_qa",
        },
    )
    graph.add_edge("targeted_correction", "dimension_drafter")
    graph.add_edge("finalize_qa", "finalize_delivery")
    graph.add_edge("finalize_delivery", END)
    
    return graph.compile()


# ============================================================================
# PUBLIC API
# ============================================================================

def run_mfi_report_generation(
    country: str,
    data_collection_start: str,
    data_collection_end: str,
    markets: List[str],
    csv_data: Optional[Dict[str, Any]] = None,
    on_step: Optional[OnStepCallback] = None,
    release_control: Optional[MFIReleaseControl] = None,
    run_id: Optional[str] = None,
    llm_trace_sink: Optional[TraceSink] = None,
) -> dict:
    """
    Entry point per la generazione del MFI Report.
    
    Returns:
        Stato finale con report completo
    """
    control = require_mfi_analysis_v2(release_control)
    logger.info(
        "MFI Drafter 2.0 generation started",
        extra={
            "mfi_event": "generation_started",
            "mfi_analysis_version": control.analysis_version,
            "mfi_deployment_revision": control.deployment_revision,
            "mfi_country": country,
        },
    )
    initial_state = create_initial_state(
        country=country,
        data_collection_start=data_collection_start,
        data_collection_end=data_collection_end,
        markets=markets,
        csv_data=csv_data,
        release_control=control,
        run_id=run_id,
    )
    
    agent = build_graph(on_step=on_step)
    try:
        # Three bounded correction cycles plus final delivery exceed LangGraph's
        # conservative default of 25 supersteps in the worst case.  The graph's own
        # MAX_CORRECTION_ATTEMPTS remains the controlling limit.
        with llm_trace_session(
            service="mfi-drafter",
            run_id=initial_state["run_id"],
            initial=initial_state.get("llm_diagnostics"),
            sink=llm_trace_sink,
        ) as trace:
            try:
                result = agent.invoke(initial_state, config={"recursion_limit": 100})
            except Exception:
                log_llm_run_summary(trace.snapshot())
                raise
            result["llm_diagnostics"] = trace.snapshot()
            log_llm_run_summary(result["llm_diagnostics"])
    except Exception:
        logger.exception(
            "MFI Drafter 2.0 generation failed",
            extra={
                "mfi_event": "generation_failed",
                "mfi_analysis_version": control.analysis_version,
                "mfi_deployment_revision": control.deployment_revision,
                "mfi_country": country,
            },
        )
        raise
    for key in ("seerist_documents", "reliefweb_documents"):
        result.pop(key, None)
    diagnostics = result.get("generation_diagnostics") or {}
    logger.info(
        "MFI Drafter 2.0 generation completed",
        extra={
            "mfi_event": "generation_completed",
            "mfi_analysis_version": control.analysis_version,
            "mfi_deployment_revision": control.deployment_revision,
            "mfi_country": country,
            "mfi_qa_status": (result.get("qa_review") or {}).get("status"),
            "mfi_llm_calls": result.get("llm_calls", 0),
            "mfi_correction_attempts": result.get("correction_attempts", 0),
            "mfi_fallback_dimensions": len(
                (diagnostics.get("dimensions") or {}).get("fallback", [])
            ),
            "mfi_fallback_markets": len(
                (diagnostics.get("markets") or {}).get("fallback", [])
            ),
            "mfi_methodology_warning_codes": sorted(
                str(item.get("code"))
                for item in result.get("methodology_warnings", [])
                if isinstance(item, dict) and item.get("code")
            ),
            "mfi_retriever_status": diagnostics.get("retrievers", {}),
            "mfi_context_status": (result.get("context_status") or {}).get("status"),
            "mfi_context_limitation_code": (
                result.get("context_status") or {}
            ).get("limitation_code"),
            "mfi_claim_identity_authority": diagnostics.get(
                "claim_identity_authority"
            ),
            "mfi_claim_identity_version": diagnostics.get(
                "claim_identity_version"
            ),
            "mfi_ignored_model_identifier_count": diagnostics.get(
                "ignored_model_identifier_count", 0
            ),
            "mfi_identity_fallback_artifacts": diagnostics.get(
                "identity_fallback_artifacts", []
            ),
            "mfi_delivery_contract_status": diagnostics.get(
                "delivery_contract_status"
            ),
        },
    )
    return result
