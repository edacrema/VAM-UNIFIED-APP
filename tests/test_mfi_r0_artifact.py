"""Phase R0 artifact-mode checks against a report produced by a real generation run.

Some defects only exist in output written by a model. The deterministic pipeline uses
fallback narratives and therefore produces neither Markdown leakage nor unresolved QA
findings, so those defects can only be observed in a stored artifact.

The artifact is untracked. When it is absent these tests skip; set
``MFI_R0_REQUIRE_DIAGNOSTIC=1`` to make a missing artifact a failure instead.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import pytest

from app.services.mfi_drafter import r0_diagnostic as diagnostic
from app.services.mfi_drafter.report_inspector import (
    StructuralReport,
    inspect_docx_bytes,
    inspect_docx_path,
)
from app.shared.docx_export import build_docx_bytes_from_report_blocks
from app.shared.report_blocks import ReportBlock


def _require_artifact() -> Path:
    path = diagnostic.resolve_artifact_docx()
    if path is None:
        message = (
            "No generated MFI report is available for artifact inspection; reports are "
            f"intentionally not committed. Set {diagnostic.ENV_ARTIFACT_DOCX} or place a "
            "DOCX under '.tmp/mfi-review-*/'."
        )
        if diagnostic.diagnostic_required():
            pytest.fail(message)
        pytest.skip(message)
    return path


@lru_cache(maxsize=1)
def _inspect(path_key: str) -> StructuralReport:
    return inspect_docx_path(Path(path_key))


@pytest.fixture(scope="module")
def artifact_report() -> StructuralReport:
    return _inspect(str(_require_artifact()))


@pytest.fixture(scope="module")
def r4_traceability_report() -> StructuralReport:
    """Exercise the current renderer; the stored diagnostic may predate R4."""
    blocks = [
        ReportBlock(
            type="claim_warning",
            text=(
                "[UNVERIFIED] Unverified — review required. Claim ID: "
                "market.alpha.issue.1. QA codes: scope_mismatch."
            ),
            meta={
                "claim_id": "market.alpha.issue.1",
                "severity": "medium",
                "flag_ids": ["flag-1"],
                "flag_codes": ["scope_mismatch"],
                "disposition": "retained_unverified_for_delivery",
            },
        ),
        ReportBlock(type="heading", text="QA findings", level=3),
        ReportBlock(
            type="table",
            meta={
                "table_kind": "mfi_deterministic",
                "title": "QA findings",
                "columns": ["severity", "claim_id", "code"],
                "rows": [
                    {
                        "row_id": "flag-1",
                        "values": {
                            "severity": "MEDIUM",
                            "claim_id": "market.alpha.issue.1",
                            "code": "scope_mismatch",
                        },
                    }
                ],
            },
        ),
    ]
    return inspect_docx_bytes(
        build_docx_bytes_from_report_blocks(blocks, visualizations={})
    )


def test_artifact_is_a_real_report(artifact_report) -> None:
    """Guard against a vacuous pass: every later zero must mean something."""
    assert artifact_report.paragraph_char_count > 10_000
    assert artifact_report.data_table_count >= 1
    assert len(artifact_report.headings) >= 10


@pytest.mark.xfail(
    strict=True,
    reason="R0 ledger: narrative prose reaches the export with Markdown delimiters "
    "intact (FIX-08, fixed in R3)",
)
def test_generated_report_contains_no_markdown_delimiters(artifact_report) -> None:
    assert artifact_report.backtick_count == 0
    assert artifact_report.code_fence_count == 0
    assert artifact_report.markdown_link_count == 0


def test_unverified_language_is_backed_by_visible_markers(
    r4_traceability_report,
) -> None:
    """The report must not promise traceability it does not deliver."""
    assert r4_traceability_report.unverified_word_count >= 1
    assert r4_traceability_report.claim_status_marker_count >= 1


def test_material_qa_findings_are_tabulated(r4_traceability_report) -> None:
    assert r4_traceability_report.qa_table_row_count >= 1


@pytest.mark.xfail(
    strict=True,
    reason="R0 ledger: market narratives reach operational verdicts about transfer "
    "modalities from MFI evidence alone (FIX-02, fixed in R3)",
)
def test_no_modality_conclusion_is_drawn_from_mfi_evidence(artifact_report) -> None:
    assert artifact_report.modality_conclusion_count == 0, "\n".join(
        artifact_report.modality_conclusion_samples
    )


@pytest.mark.xfail(
    strict=True,
    reason="R0 ledger: unweighted means of market rates are worded as pooled respondent "
    "shares (FIX-03, fixed in R3)",
)
def test_no_assessment_statistic_is_worded_as_a_respondent_share(artifact_report) -> None:
    assert artifact_report.pooled_population_phrase_count == 0, ", ".join(
        artifact_report.pooled_population_samples
    )


@pytest.mark.xfail(
    strict=True,
    reason="R0 ledger: internal deterministic rows are exported without a presentation "
    "projection (FIX-05, fixed in R6)",
)
def test_exported_tables_stay_within_the_readable_column_budget(artifact_report) -> None:
    assert artifact_report.max_table_column_count <= 8


@pytest.mark.xfail(
    strict=True,
    reason="R0 ledger: the context heading is exported with no content and no status "
    "disclosure (FIX-07, fixed in R7)",
)
def test_no_exported_section_is_empty(artifact_report) -> None:
    assert artifact_report.empty_section_titles == ()


@pytest.mark.xfail(
    strict=True,
    reason="R0 ledger: complete fixed-metric coverage is restated on every citation "
    "(FIX-09, fixed in R7)",
)
def test_coverage_is_not_restated_on_every_citation(artifact_report) -> None:
    """A coverage label repeated once per citation is noise, not traceability."""
    repetition = artifact_report.coverage_note_repetition
    assert max(repetition.values(), default=0) <= artifact_report.evidence_note_count


@pytest.mark.xfail(
    strict=True,
    reason="R0 ledger: the same follow-up sentence is repeated across dimensions "
    "(FIX-11, fixed in R8)",
)
def test_boilerplate_is_not_repeated_verbatim(artifact_report) -> None:
    assert artifact_report.max_boilerplate_repetition <= 1
