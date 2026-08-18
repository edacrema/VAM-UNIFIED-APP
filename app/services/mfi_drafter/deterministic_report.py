"""Deterministic, LLM-free execution of the full MFI report path.

This module generalises ``release_validation._deterministic_result`` so the same pipeline
can be driven from a CSV path, a DataFrame, or an already-loaded payload, and so chart
titles can be captured while figures are rendered.

Capturing chart titles is the only way to observe them: titles are rasterised into the
matplotlib PNGs, so they never appear as text in the exported document.

Like ``release_validation``, this module never calls an LLM, invokes a retriever, or
deploys anything — ``forbid_llm`` turns that convention into an enforced guarantee. The
``graph`` module is imported lazily inside functions because importing it pulls in the
LangChain and LangGraph stack, which costs several seconds and is unnecessary for callers
that only need the loader or the inspector.
"""

from __future__ import annotations

import subprocess
from collections import Counter
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from time import perf_counter
from typing import TYPE_CHECKING, Any, Iterator, Mapping, Optional

from .context_status import not_attempted_context_status
from .report_inspector import ChartTitle, InspectorConfig, _parse_chart_title

if TYPE_CHECKING:  # pragma: no cover - typing only
    import pandas as pd

    from app.shared.report_blocks import ReportBlock


@dataclass
class DeterministicReportRun:
    """Every artifact produced by one deterministic pipeline execution."""

    loaded: dict[str, Any]
    profile: dict[str, Any]
    result: dict[str, Any]
    blocks: list[Any]
    docx: bytes
    visualizations: dict[str, str] = field(default_factory=dict)
    chart_titles: tuple[ChartTitle, ...] = ()
    timings: dict[str, float] = field(default_factory=dict)

    @property
    def loader_warnings(self) -> list[str]:
        """Warnings emitted by the loader itself.

        ``result["warnings"]`` is overwritten downstream with a deterministic-rendering
        note, so the loader's own warnings must be read from the pre-pipeline payload.
        """
        return list(self.loaded.get("warnings", []) or [])

    @property
    def methodology_warnings(self) -> list[Any]:
        return list(self.loaded.get("methodology_warnings", []) or [])


class LLMInvocationError(RuntimeError):
    """Raised when the deterministic pipeline attempts to reach a model or retriever."""


@contextmanager
def capture_chart_titles() -> Iterator[list[list[str]]]:
    """Capture axis titles of every figure rendered while the context is active.

    ``graph.save_plot_to_base64`` closes the current figure immediately after saving, so
    titles must be read before delegating to it. The original callable is still invoked,
    which keeps real PNGs in the export and leaves figure-lifecycle behaviour unchanged.
    """
    import matplotlib

    matplotlib.use("Agg", force=True)
    import matplotlib.pyplot as plt

    from . import graph

    captured: list[list[str]] = []
    original = graph.save_plot_to_base64

    def _capture_then_save() -> str:
        titles = [
            axis.get_title() for axis in plt.gcf().axes if axis.get_title().strip()
        ]
        captured.append(titles)
        return original()

    graph.save_plot_to_base64 = _capture_then_save
    try:
        yield captured
    finally:
        graph.save_plot_to_base64 = original
        plt.close("all")


@contextmanager
def forbid_llm() -> Iterator[None]:
    """Make any model or retriever construction raise for the duration of the block."""
    from . import graph

    def _raise(*args: Any, **kwargs: Any) -> Any:
        raise LLMInvocationError(
            "The deterministic report path must not construct a model or retriever."
        )

    originals = {
        name: getattr(graph, name)
        for name in ("get_model", "ReliefWebRetriever", "SeeristRetriever")
        if hasattr(graph, name)
    }
    for name in originals:
        setattr(graph, name, _raise)
    try:
        yield
    finally:
        for name, value in originals.items():
            setattr(graph, name, value)


def _current_revision() -> str:
    try:
        completed = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return "unknown"
    return completed.stdout.strip() or "unknown"


def _build_result(loaded: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """Run analysis, fallback narratives, and validation; return (result, profile)."""
    from .analysis import build_assessment_profile
    from .narrative import (
        build_claim_catalog,
        build_qa_review,
        deduplicate_dimension_recommendations,
        fallback_dimension_narrative,
        fallback_executive_narrative,
        fallback_market_narrative,
        validate_structured_narratives,
    )

    profile = build_assessment_profile(
        loaded["markets_data"],
        loaded["metric_summaries"],
        loaded,
    ).model_dump()
    catalog = build_claim_catalog(profile)
    dimension_narratives = {
        item["dimension"]: fallback_dimension_narrative(item, assessment_profile=profile)
        for item in profile["dimensions"]
    }
    dimension_narratives = deduplicate_dimension_recommendations(
        dimension_narratives,
        profile["dimensions"],
    )
    market_narratives = {
        item["market_name"]: fallback_market_narrative(item)
        for item in profile["markets"]
        if item["is_priority_market"]
    }
    executive = fallback_executive_narrative(profile)
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
        str(flag.get("severity")) for flag in flags if isinstance(flag, Mapping)
    )
    diagnostics = {
        "dimensions": {"llm": [], "fallback": sorted(dimension_narratives, key=str.casefold)},
        "markets": {"llm": [], "fallback": sorted(market_narratives, key=str.casefold)},
        "context_extraction_mode": "not_applicable",
        "context_classification_status": "not_attempted",
        "executive_summary_mode": "fallback",
        "red_team_status": "not_started",
        "correction_attempts": 0,
        "unresolved_high_count": severity_counts.get("high", 0),
        "unresolved_medium_count": severity_counts.get("medium", 0),
        "unresolved_low_count": severity_counts.get("low", 0),
        "retrievers": {},
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
        "mean_mfi_across_assessed_markets": profile["mean_mfi_across_assessed_markets"],
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
        "qa_review": build_qa_review(flags, [], correction_attempts=0),
        "document_references": [],
        "warnings": [
            "Deterministic Phase 4 regression rendering; no LLM or retriever was invoked."
        ],
        "llm_calls": 0,
        "correction_attempts": 0,
    }
    return result, profile


def run_deterministic_report(
    loaded: Mapping[str, Any],
    *,
    render_figures: bool = True,
    capture_titles: bool = True,
    forbid_llm_calls: bool = True,
) -> DeterministicReportRun:
    """Execute analysis, narratives, figures, blocks, and export from a loaded payload."""
    from app.shared.docx_export import build_docx_bytes_from_report_blocks
    from app.shared.report_blocks import build_mfi_report_blocks

    from . import graph

    timings: dict[str, float] = {}

    started = perf_counter()
    result, profile = _build_result(loaded)
    timings["analysis_and_narratives"] = perf_counter() - started

    visualizations: dict[str, str] = {}
    chart_titles: tuple[ChartTitle, ...] = ()
    if render_figures:
        started = perf_counter()
        config = InspectorConfig()
        if capture_titles:
            with capture_chart_titles() as captured:
                with _optional_forbid_llm(forbid_llm_calls):
                    visualizations = graph.node_mfi_graph_designer(result).get(
                        "visualizations", {}
                    )
            chart_titles = _zip_titles(captured, visualizations, config)
        else:
            with _optional_forbid_llm(forbid_llm_calls):
                visualizations = graph.node_mfi_graph_designer(result).get(
                    "visualizations", {}
                )
        timings["figures"] = perf_counter() - started

    result["visualizations"] = visualizations

    started = perf_counter()
    blocks = build_mfi_report_blocks(result)
    result["report_blocks"] = [block.model_dump() for block in blocks]
    docx = build_docx_bytes_from_report_blocks(blocks, visualizations=visualizations)
    timings["render"] = perf_counter() - started

    return DeterministicReportRun(
        loaded=dict(loaded),
        profile=profile,
        result=result,
        blocks=blocks,
        docx=docx,
        visualizations=visualizations,
        chart_titles=chart_titles,
        timings=timings,
    )


@contextmanager
def _optional_forbid_llm(enabled: bool) -> Iterator[None]:
    if not enabled:
        yield
        return
    with forbid_llm():
        yield


def _zip_titles(
    captured: list[list[str]],
    visualizations: Mapping[str, str],
    config: InspectorConfig,
) -> tuple[ChartTitle, ...]:
    """Associate captured titles with figure ids by insertion order.

    Every call site in ``graph`` assigns ``visualizations[key] = save_plot_to_base64()``,
    so the n-th capture belongs to the n-th inserted key. When the counts disagree the
    association is dropped rather than guessed; coverage is parsed from the title text
    itself, so the measurement survives a missing figure id.
    """
    keys = list(visualizations)
    aligned = len(captured) == len(keys)
    titles: list[ChartTitle] = []
    for index, group in enumerate(captured):
        figure_id = keys[index] if aligned else None
        for title in group:
            titles.append(_parse_chart_title(title, figure_id, config))
    return tuple(titles)


def run_deterministic_report_from_dataframe(
    frame: "pd.DataFrame", **kwargs: Any
) -> DeterministicReportRun:
    """Run the deterministic pipeline from an in-memory assessment DataFrame."""
    from .data_loader import load_mfi_from_dataframe

    loader_keys = ("country_override", "start_date_override", "end_date_override")
    loader_kwargs = {key: kwargs.pop(key) for key in loader_keys if key in kwargs}
    return run_deterministic_report(
        load_mfi_from_dataframe(frame, **loader_kwargs), **kwargs
    )


def run_deterministic_report_from_csv(
    path: Path | str, **kwargs: Any
) -> DeterministicReportRun:
    """Run the deterministic pipeline from an assessment CSV on disk."""
    from .data_loader import load_mfi_from_csv

    loader_keys = ("country_override", "start_date_override", "end_date_override")
    loader_kwargs = {key: kwargs.pop(key) for key in loader_keys if key in kwargs}
    return run_deterministic_report(
        load_mfi_from_csv(Path(path), **loader_kwargs), **kwargs
    )
