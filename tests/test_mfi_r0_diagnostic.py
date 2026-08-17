"""Phase R0 baseline lock for the local diagnostic assessment.

These tests need a real Full MFI assessment, which is never committed. When the file is
absent they skip with an explicit reason; set ``MFI_R0_REQUIRE_DIAGNOSTIC=1`` to turn a
skip into a failure so a phase checklist cannot pass vacuously.

The invariants asserted here are Section 3 of the remediation specification: the values
that must be identical before and after every phase.
"""

from __future__ import annotations

import copy
from functools import lru_cache
from pathlib import Path

import pytest

from app.services.mfi_drafter import r0_diagnostic as diagnostic
from app.services.mfi_drafter.report_inspector import (
    inspect_docx_bytes,
    inspect_report_blocks,
    merge_reports,
)

# The verified analytical baseline of the diagnostic upload.
EXPECTED_MARKET_COUNT = 27
EXPECTED_MEAN_MFI = 5.981638237282082
EXPECTED_PRIORITY_DIMENSIONS = ("Service", "Price", "Infrastructure")


def _require_diagnostic() -> Path:
    path = diagnostic.resolve_diagnostic_csv()
    if path is None:
        message = (
            "The local diagnostic assessment is absent; assessment data is "
            f"intentionally not committed. Place it in "
            f"'{diagnostic.BENCHMARK_DIRECTORY_NAME}' or set "
            f"{diagnostic.ENV_DIAGNOSTIC_CSV}."
        )
        if diagnostic.diagnostic_required():
            pytest.fail(message)
        pytest.skip(message)
    return path


@lru_cache(maxsize=1)
def _run_cache(path_key: str, digest: str):
    """Run the deterministic pipeline once and reuse it across this module.

    The cache key includes the file digest so swapping the source mid-session cannot serve
    a stale run.
    """
    from app.services.mfi_drafter.deterministic_report import (
        run_deterministic_report_from_csv,
    )

    return run_deterministic_report_from_csv(Path(path_key))


@pytest.fixture(scope="module")
def diagnostic_run():
    path = _require_diagnostic()
    digest = diagnostic._sha256(path)
    run = _run_cache(str(path), digest)
    # Cached artifacts are shared mutable state; hand each test its own copy.
    return copy.deepcopy(run)


@pytest.fixture(scope="module")
def diagnostic_report(diagnostic_run):
    return merge_reports(
        inspect_report_blocks(
            diagnostic_run.blocks,
            chart_titles=diagnostic_run.chart_titles,
            profile=diagnostic_run.profile,
        ),
        inspect_docx_bytes(diagnostic_run.docx),
    )


# ---------------------------------------------------------------------------
# Analytical baseline — must never change
# ---------------------------------------------------------------------------


def test_diagnostic_baseline_invariants_hold(diagnostic_run) -> None:
    problems = diagnostic.assert_diagnostic_invariants(
        diagnostic_run.loaded,
        diagnostic_run.profile,
        expected_market_count=EXPECTED_MARKET_COUNT,
        expected_mean_mfi=EXPECTED_MEAN_MFI,
        expected_priority_dimensions=EXPECTED_PRIORITY_DIMENSIONS,
    )

    assert problems == [], "\n".join(problems)


def test_stored_scores_survive_numeric_parsing(diagnostic_run) -> None:
    """Every authoritative Level-1 value must equal the source file exactly."""
    path = _require_diagnostic()

    problems = diagnostic.stored_scores_match_source(path, diagnostic_run.loaded)

    assert problems == [], "\n".join(problems[:10])


def test_deterministic_run_invokes_no_model(diagnostic_run) -> None:
    assert diagnostic_run.result["llm_calls"] == 0
    assert diagnostic_run.result["generation_diagnostics"]["retrievers"] == {}
    assert diagnostic_run.loader_warnings == []


def test_baseline_snapshot_is_recorded_and_matches(diagnostic_run) -> None:
    """Create the snapshot on first run; afterwards compare against it.

    A source change never re-baselines silently — that requires an explicit environment
    flag, so an accidentally swapped input cannot erase the recorded identity.
    """
    path = _require_diagnostic()
    target = diagnostic.baseline_path()
    current = diagnostic.build_baseline(
        loaded=diagnostic_run.loaded,
        profile=diagnostic_run.profile,
        report=None,
        source=path,
    )

    recorded = diagnostic.load_baseline(target)
    if recorded is None or diagnostic.update_requested():
        diagnostic.write_baseline(current, target)
        pytest.skip(f"Recorded a new R0 baseline at {target}; re-run to compare.")

    comparison = diagnostic.compare_baseline(recorded, current)
    assert comparison.is_clean, comparison.describe()


# ---------------------------------------------------------------------------
# Live-mode defect ratchet — remove the marker when the owning phase lands
# ---------------------------------------------------------------------------


def test_dimension_charts_report_real_coverage(
    diagnostic_report, diagnostic_run
) -> None:
    assert diagnostic_report.zero_coverage_chart_count == 0
    expected_by_dimension = {
        str(item["dimension"]): item["statistics"]["coverage"]
        for item in diagnostic_run.profile["dimensions"]
    }
    dimension_titles = [
        item
        for item in diagnostic_report.chart_titles
        if " by assessed market" in item.title and item.has_coverage
    ]
    assert len(dimension_titles) == 9
    for item in dimension_titles:
        dimension = item.title.split(" by assessed market", 1)[0]
        expected = expected_by_dimension[dimension]
        assert item.coverage_available == expected["available_market_count"]
        assert item.coverage_total == expected["total_assessed_market_count"]
        assert (
            f"({float(expected['coverage_ratio']) * 100.0:.1f}%)" in item.title
        )


def test_every_dimension_chart_is_titled_with_coverage(diagnostic_report) -> None:
    """Coverage must be stated on all nine dimension charts, right or wrong.

    This is structural rather than data-dependent, and it guards the capture hook itself:
    if title interception silently broke, the coverage ratchet above would xfail for the
    wrong reason.
    """
    assert diagnostic_report.coverage_titled_chart_count == 9


def test_no_false_evidence_limitation_on_the_diagnostic_sample(diagnostic_report) -> None:
    """Fixed in R1: no limitation may rest solely on optional metrics."""
    assert diagnostic_report.optional_only_limitation_count == 0
    assert diagnostic_report.optional_only_limitation_dimensions == ()


def test_partial_optional_coverage_remains_visible(diagnostic_run) -> None:
    """Removing the warning must not remove the evidence.

    Availability and Price are the dimensions whose optional items are partially
    represented in the diagnostic sample. After R1 that is a coverage disclosure rather
    than a warning, but it must still be queryable per metric and citable from the ledger.
    """
    classified: dict[str, int] = {}
    for dimension in diagnostic_run.profile["dimensions"]:
        partial = [
            metric
            for metric in dimension["drivers"]
            if (metric.get("availability") or {}).get("classification")
            == "partial_optional"
        ]
        if partial:
            classified[dimension["dimension"]] = len(partial)

    assert {"Availability", "Price"} <= set(classified)
    ledger_ids = {
        key
        for key in diagnostic_run.profile["metric_ledger"]
        if "partial_optional_items" in key
    }
    assert {
        "assessment.coverage.partial_optional_items.availability.count",
        "assessment.coverage.partial_optional_items.price.count",
    } <= ledger_ids


def test_required_evidence_is_complete_on_the_diagnostic_sample(diagnostic_run) -> None:
    """The sample has no genuine evidence failure, so nothing may warrant a warning."""
    warning_metrics = [
        metric["metric_id"]
        for dimension in diagnostic_run.profile["dimensions"]
        for metric in list(dimension["subsections"]) + list(dimension["drivers"])
        if (metric.get("availability") or {}).get("warrants_warning")
    ]

    assert warning_metrics == []


@pytest.mark.xfail(
    strict=True,
    reason="R0 ledger: deterministic tables render every key as a column "
    "(FIX-05, fixed in R6)",
)
def test_diagnostic_tables_stay_within_the_readable_column_budget(
    diagnostic_report,
) -> None:
    assert diagnostic_report.max_table_column_count <= 8


@pytest.mark.xfail(
    strict=True,
    reason="R0 ledger: the context heading is emitted with no content and no status "
    "disclosure (FIX-07, fixed in R7)",
)
def test_diagnostic_context_section_is_not_empty(diagnostic_report) -> None:
    assert diagnostic_report.empty_section_titles == ()
