"""Phase R0 inspector unit tests and the synthetic defect ratchet.

Two kinds of test live here.

Unit tests pin the inspector's own behaviour on hand-built inputs, so a measurement that
silently stops working is caught immediately.

Ratchet tests assert the *fixed* state of each known defect and are marked
``xfail(strict=True)``. They xfail today, which records the defect as detected; when the
owning phase fixes it the test starts passing, strict mode turns that into a failure, and
the implementer must delete the marker. The defect ledger is therefore self-maintaining,
and ``pytest -rx`` prints it on every run.
"""

from __future__ import annotations

import subprocess
import sys
from functools import lru_cache
from pathlib import Path

import pytest

from app.services.mfi_drafter import report_inspector as inspector
from app.services.mfi_drafter.report_inspector import (
    ChartTitle,
    InspectorConfig,
    StructuralReport,
    inspect_docx_bytes,
    inspect_profile,
    inspect_report_blocks,
    merge_reports,
)
from app.services.mfi_drafter.synthetic_fixtures import (
    SyntheticSpec,
    build_blocks_with_claim_status,
    build_profile,
    build_report_run,
)
from app.shared.docx_export import build_docx_bytes_from_report_blocks
from app.shared.report_blocks import ReportBlock

REPO_ROOT = Path(__file__).resolve().parents[1]

# Partial optional coverage is what makes the current evidence classifier misfire, so the
# ratchet fixture uses it deliberately.
RATCHET_SPEC = SyntheticSpec(market_count=5, item_market_ratio=0.5)


@lru_cache(maxsize=1)
def _ratchet_report() -> StructuralReport:
    """Inspect one synthetic deterministic run, reused by every ratchet test."""
    run = build_report_run(RATCHET_SPEC, render_figures=False)
    return merge_reports(
        inspect_report_blocks(run.blocks, profile=run.profile),
        inspect_docx_bytes(run.docx),
    )


@pytest.fixture(scope="module")
def ratchet_report() -> StructuralReport:
    return _ratchet_report()


# ---------------------------------------------------------------------------
# Inspector unit behaviour
# ---------------------------------------------------------------------------


def test_markdown_leakage_is_counted_per_container() -> None:
    """Backticks in prose and in notice boxes must be distinguishable."""
    blocks = [
        ReportBlock(type="paragraph", text="A rate of `35.1%` was reported."),
        ReportBlock(type="limitation_box", text="Coverage was `partial`."),
    ]

    report = inspect_report_blocks(blocks)

    assert report.backtick_count == 4
    assert report.backtick_count_paragraphs == 2
    assert report.backtick_count_table_cells == 2


def test_clean_text_reports_zero_markdown() -> None:
    blocks = [ReportBlock(type="paragraph", text="A rate of 35.1% was reported.")]

    report = inspect_report_blocks(blocks)

    assert report.backtick_count == 0
    assert report.code_fence_count == 0
    assert report.paragraph_char_count > 0, "a zero must not come from empty input"


def test_empty_section_requires_equal_or_higher_next_heading() -> None:
    """Parent-to-child nesting is legitimate structure, not an empty section."""
    blocks = [
        ReportBlock(type="heading", text="Context and sources", level=2),
        ReportBlock(type="heading", text="Assessed-market profile", level=2),
        ReportBlock(type="paragraph", text="Body text."),
        ReportBlock(type="heading", text="Dimensions", level=2),
        ReportBlock(type="heading", text="Service", level=3),
        ReportBlock(type="paragraph", text="More body text."),
    ]

    report = inspect_report_blocks(blocks)

    assert report.empty_section_titles == ("Context and sources",)
    assert len(report.heading_level_jumps) == 0


def test_heading_level_jump_is_advisory_only() -> None:
    blocks = [
        ReportBlock(type="heading", text="Markets", level=2),
        ReportBlock(type="heading", text="Selected markets", level=4),
        ReportBlock(type="paragraph", text="Body text."),
    ]

    report = inspect_report_blocks(blocks)

    assert report.empty_section_titles == ()
    assert len(report.heading_level_jumps) == 1


def test_chart_coverage_is_parsed_from_the_title() -> None:
    titles = [
        ChartTitle(figure_id="dim_service_bars", title="Service\nCoverage: 0/0 markets"),
        ChartTitle(figure_id="dim_price_bars", title="Price\nCoverage: 27/27 markets"),
        ChartTitle(figure_id="mfi_radar", title="Average profile"),
    ]

    report = inspect_report_blocks([], chart_titles=titles)

    assert report.coverage_titled_chart_count == 2
    assert report.zero_coverage_chart_count == 1


def test_notice_boxes_are_excluded_from_table_width() -> None:
    """Single-cell notice boxes must not be measured as data tables."""
    blocks = [
        ReportBlock(type="limitation_box", text="Data limitation: partial coverage."),
        ReportBlock(
            type="table",
            meta={
                "table_kind": "mfi_presentation",
                "spec_id": "mfi.inspector_width_probe.v1",
                "title": "Dimension summary",
                "columns": ["dimension", "mean"],
                "column_specs": [
                    {
                        "key": "dimension",
                        "label": "Dimension",
                        "format": "text",
                        "alignment": "left",
                        "width_hint": 2.0,
                        "ledger_linkage_policy": "not_applicable",
                    },
                    {
                        "key": "mean",
                        "label": "Mean",
                        "format": "score_statistic",
                        "alignment": "right",
                        "width_hint": 1.0,
                        "ledger_linkage_policy": "not_applicable",
                    },
                ],
                "rows": [{"values": {"dimension": "Service", "mean": 3.33}}],
            },
        ),
    ]

    docx = build_docx_bytes_from_report_blocks(blocks, visualizations={})
    report = inspect_docx_bytes(docx)

    assert report.notice_box_count == 1
    assert report.data_table_count == 1
    assert report.max_table_column_count == 2


def test_undeclared_columns_are_reported_for_block_tables() -> None:
    """A table with no explicit projection is what produces the 22-column export."""
    blocks = [
        ReportBlock(
            type="table",
            meta={
                "table_kind": "mfi_deterministic",
                "title": "Ranked drivers",
                "columns": [],
                "rows": [
                    {"values": {f"column_{index}": index for index in range(22)}}
                ],
            },
        )
    ]

    report = inspect_report_blocks(blocks)

    assert report.undeclared_column_table_count == 1
    assert report.max_table_column_count == 22


def test_coverage_notes_are_matched_by_pattern_not_literal_text() -> None:
    blocks = [
        ReportBlock(type="evidence_note", text="Price: 5.0; coverage: 27/27 assessed markets"),
        ReportBlock(type="evidence_note", text="Barley: 0.3; coverage: 12/27 assessed markets"),
        ReportBlock(type="evidence_note", text="Wheat: 0.4; coverage: 27/27 assessed markets"),
    ]

    report = inspect_report_blocks(blocks)

    assert report.coverage_note_total == 3
    assert report.coverage_note_repetition == {"27/27": 2, "12/27": 1}
    assert report.evidence_note_word_count > report.evidence_note_count
    assert report.evidence_note_count == 3


def test_boilerplate_family_is_detected_across_dimension_slots() -> None:
    template = (
        "Use the cited {} evidence to target further market assessment and "
        "proportionate operational follow-up."
    )
    blocks = [
        ReportBlock(type="paragraph", text=template.format(name))
        for name in ("Price", "Service", "Assortment")
    ]

    report = inspect_report_blocks(blocks)

    assert report.max_boilerplate_repetition == 3


def test_modality_conclusions_need_both_a_subject_and_a_verdict() -> None:
    """Naming a modality is not a conclusion; reaching a verdict about one is."""
    blocks = [
        ReportBlock(
            type="paragraph",
            text="The effectiveness of an exclusively cash-based response could be compromised.",
        ),
        ReportBlock(
            type="paragraph",
            text="Both cash and voucher assistance could be viable in this market.",
        ),
        ReportBlock(
            type="paragraph",
            text="Traders in the market accept multiple payment types.",
        ),
    ]

    report = inspect_report_blocks(blocks)

    assert report.modality_conclusion_count == 2
    assert len(report.modality_conclusion_samples) == 2


def test_methodological_negation_is_not_a_modality_conclusion() -> None:
    """The neutral replacement wording must never register as the defect it replaces."""
    blocks = [
        ReportBlock(
            type="paragraph",
            text=(
                "MFI findings can inform further feasibility analysis but do not "
                "determine transfer modality, affordability, or household purchasing power."
            ),
        )
    ]

    report = inspect_report_blocks(blocks)

    assert report.modality_conclusion_count == 0


def test_pooled_population_phrases_are_detected() -> None:
    """An assessment mean of market rates must not be worded as a respondent share."""
    blocks = [
        ReportBlock(type="paragraph", text="Here 35.1% of traders reported scarcity."),
        ReportBlock(type="paragraph", text="Instability affected 72.8% of responses."),
        ReportBlock(
            type="paragraph",
            text="The unweighted mean market-level unfavorable rate was 35.1%.",
        ),
    ]

    report = inspect_report_blocks(blocks)

    assert report.pooled_population_phrase_count == 2
    assert "35.1% of traders" in report.pooled_population_samples


def test_limitations_are_classified_by_applicability() -> None:
    """The optional-only signature is dataset-independent; the raw count is not."""
    profile = {
        "limitations": [
            {
                "code": "unavailable_explanatory_evidence",
                "dimension": "Price",
                "message": "Price has 30 metric(s) without complete evidence.",
                "metric_ids": [
                    "price.increase.item.cereal_food.barley",
                    "price.stability.item.cereal_food.bread",
                ],
            },
            {
                "code": "unavailable_explanatory_evidence",
                "dimension": "Service",
                "message": "Service has 1 metric without complete evidence.",
                "metric_ids": ["service.shopping"],
            },
        ]
    }

    report = inspect_profile(profile)

    assert report.optional_only_limitation_count == 1
    assert report.optional_only_limitation_dimensions == ("Price",)


def test_claim_status_is_read_from_block_metadata() -> None:
    report = inspect_report_blocks(build_blocks_with_claim_status("unverified"))

    assert report.claim_paragraph_count == 1
    assert report.claim_status_counts == {"unverified": 1}


def test_merge_reports_combines_modes_without_losing_measurements() -> None:
    blocks_report = inspect_report_blocks(
        [ReportBlock(type="paragraph", text="Body text.")],
        chart_titles=[ChartTitle(figure_id="dim_x", title="X\nCoverage: 0/0 markets")],
    )
    docx_report = inspect_docx_bytes(
        build_docx_bytes_from_report_blocks(
            [ReportBlock(type="paragraph", text="Body `text`.")], visualizations={}
        )
    )

    merged = merge_reports(blocks_report, docx_report)

    assert merged.source == "pipeline"
    assert merged.zero_coverage_chart_count == 1
    assert merged.backtick_count == 2


def test_diff_reports_lists_changed_fields_only() -> None:
    first = inspect_report_blocks([ReportBlock(type="paragraph", text="a b")])
    second = inspect_report_blocks([ReportBlock(type="paragraph", text="a `b`")])

    differences = inspector.diff_reports(first, second)

    assert "backtick_count" in differences
    assert differences["backtick_count"] == (0, 2)
    assert "zero_coverage_chart_count" not in differences


def test_inspector_imports_no_heavy_dependencies() -> None:
    """The inspector must stay cheap enough to use anywhere, including a CLI.

    Importing the graph stack costs seconds and drags LangChain into the process, so the
    import boundary is a contract rather than a convention.
    """
    code = (
        "import sys;"
        "import app.services.mfi_drafter.report_inspector as module;"
        "banned=[name for name in ('langgraph','langchain_core','streamlit',"
        "'matplotlib.pyplot','pandas') if name in sys.modules];"
        "print(','.join(banned))"
    )
    completed = subprocess.run(
        [sys.executable, "-c", code],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )

    assert completed.stdout.strip() == ""


# ---------------------------------------------------------------------------
# Defect ratchet — remove the marker when the owning phase lands the fix
# ---------------------------------------------------------------------------


def test_optional_coverage_does_not_raise_an_evidence_limitation(ratchet_report) -> None:
    """Fixed in R1: optional non-representation is coverage, not missing evidence."""
    assert ratchet_report.optional_only_limitation_count == 0


def test_report_tables_stay_within_the_readable_column_budget(ratchet_report) -> None:
    assert ratchet_report.max_table_column_count <= 8


def test_every_report_table_declares_its_columns(ratchet_report) -> None:
    assert ratchet_report.undeclared_column_table_count == 0


def test_no_section_heading_is_left_empty(ratchet_report) -> None:
    assert ratchet_report.empty_section_titles == ()


def test_boilerplate_is_not_repeated_verbatim(ratchet_report) -> None:
    assert ratchet_report.max_boilerplate_repetition <= 1


def test_coverage_notes_are_not_restated_on_every_citation(ratchet_report) -> None:
    repetition = ratchet_report.coverage_note_repetition
    assert max(repetition.values(), default=0) <= RATCHET_SPEC.market_count
    assert ratchet_report.evidence_note_word_count > 0
    assert ratchet_report.coverage_note_total < ratchet_report.evidence_note_count


def test_unverified_claims_are_visibly_marked() -> None:
    """Probe the renderers directly, independent of whether a run produces a flag.

    A deterministic run validates cleanly, so the absence of markers can only be shown by
    handing the renderers a claim that is explicitly unverified.
    """
    blocks = build_blocks_with_claim_status("unverified")
    report = inspect_docx_bytes(
        build_docx_bytes_from_report_blocks(blocks, visualizations={})
    )

    assert report.claim_status_marker_count >= 1


def test_material_qa_findings_are_tabulated() -> None:
    blocks = [
        ReportBlock(type="heading", text="QA findings", level=3),
        ReportBlock(
            type="qa_warning",
            text="Narrative QA completed with unresolved material issues.",
        ),
        ReportBlock(
            type="table",
            meta={
                "table_kind": "mfi_presentation",
                "spec_id": "mfi.qa_probe.v1",
                "title": "QA findings",
                "columns": ["severity", "claim_id", "code", "message"],
                "column_specs": [
                    {
                        "key": key,
                        "label": label,
                        "format": "text",
                        "alignment": "left",
                        "width_hint": 1.0,
                        "ledger_linkage_policy": "not_applicable",
                    }
                    for key, label in (
                        ("severity", "Severity"),
                        ("claim_id", "Claim ID"),
                        ("code", "Code"),
                        ("message", "Message"),
                    )
                ],
                "rows": [
                    {
                        "row_id": "flag-1",
                        "values": {
                            "severity": "HIGH",
                            "claim_id": "market.alpha.issue.1",
                            "code": "unsupported_modality_conclusion",
                            "message": "MFI evidence cannot determine transfer modality.",
                        },
                    }
                ],
            },
        )
    ]

    report = inspect_docx_bytes(
        build_docx_bytes_from_report_blocks(blocks, visualizations={})
    )

    assert report.qa_table_row_count >= 1
