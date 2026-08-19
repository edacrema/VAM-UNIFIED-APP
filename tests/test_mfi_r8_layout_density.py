from __future__ import annotations

import base64
import inspect
import io
import re
import zipfile

import pytest
from docx import Document
from pydantic import ValidationError

from app.services.mfi_drafter import graph
from app.services.mfi_drafter.claim_identity import context_token
from app.services.mfi_drafter.methodology import (
    DIMENSION_REVIEW_GUIDANCE,
    DISPLAY_DIMENSIONS,
)
from app.services.mfi_drafter.narrative import (
    NARRATIVE_DENSITY_POLICY,
    apply_narrative_density_policy,
    deduplicate_dimension_recommendations,
    parse_dimension_narrative,
    parse_executive_narrative,
    parse_market_narrative,
)
from app.services.mfi_drafter.report_inspector import inspect_docx_bytes
from app.services.mfi_drafter.schemas import MFIMarketNarrative
from app.shared.docx_export import build_docx_bytes_from_report_blocks
from app.shared.report_blocks import ReportBlock, build_mfi_report_blocks
import streamlit_shared as shared


def _claim(index: int, kind: str = "finding", scope: str = "assessment") -> dict:
    return {
        "claim_id": f"claim.{kind}.{index}",
        "text": f"Content-bearing {kind} {index}.",
        "claim_kind": kind,
        "metric_ids": [],
        "document_ids": [],
        "scope": scope,
        "polarity": "neutral",
        "validation_status": "verified",
        "validation_flags": [],
        "validation_flag_ids": [],
        "substituted": False,
    }


def _dimension_profile(dimension: str, *, priority: bool) -> dict:
    return {
        "dimension": dimension,
        "is_priority": priority,
        "ledger_metric_ids": [],
        "statistics": {},
        "subsections": [],
        "drivers": [],
        "localized_patterns": {"markets_where_lowest": [], "ordered_markets": []},
    }


def _dimension_payload() -> dict:
    return {
        "summary": _claim(1, "summary"),
        "key_findings": [_claim(index) for index in range(1, 7)],
        "subdimension_analysis": [
            {
                "name": f"Evidence {index}",
                "subsection_metric_id": None,
                "score_0_10": None,
                "interpretation": _claim(index, "finding"),
                "driver_metric_ids": [],
            }
            for index in range(1, 6)
        ],
        "geographic_patterns": [
            _claim(index, "geographic_pattern", "region")
            for index in range(1, 6)
        ],
        "data_limitations": [
            _claim(index, "limitation") for index in range(1, 5)
        ],
        "recommendations": [
            _claim(index, "recommendation") for index in range(1, 6)
        ],
    }


def _market_profile() -> dict:
    return {
        "market_name": "Alpha",
        "region": "North",
        "overall_mfi": 4.5,
        "score_rank": 1,
        "weak_dimensions": [
            {"dimension": "Price", "ledger_metric_ids": []},
            {"dimension": "Service", "ledger_metric_ids": []},
        ],
        "ledger_metric_ids": [],
    }


def _summary_claim(dimension: str) -> dict:
    claim = _claim(1, "summary")
    claim["claim_id"] = f"dimension.{dimension.casefold().replace(' ', '-')}.summary"
    claim["text"] = f"{dimension} evidence is summarized below."
    return claim


def _report_result() -> dict:
    dimensions = {
        dimension: {
            "dimension": dimension,
            "is_priority": dimension == "Price",
            "summary": _summary_claim(dimension),
            "key_findings": [],
            "subdimension_analysis": [],
            "geographic_patterns": [],
            "data_limitations": [],
            "recommendations": [],
        }
        for dimension in DISPLAY_DIMENSIONS
    }
    market_limitation = _claim(1, "limitation", "market")
    market_limitation["claim_id"] = "market.alpha.limitation.1"
    market_limitation["text"] = "Market-scoped explanatory evidence is incomplete."
    return {
        "country": "Testland",
        "data_collection_start": "2026-01-01",
        "data_collection_end": "2026-01-31",
        "methodology_version": "databridge-current",
        "score_authority": "synthetic_mock",
        "assessment_profile": {
            "priority_dimension_names": ["Price"],
            "priority_market_names": ["Alpha"],
            "markets": [
                {
                    "market_name": "Alpha",
                    "selection_order": 1,
                    "is_priority_market": True,
                }
            ],
            "limitations": [],
            "metric_ledger": {},
            "tables": {
                "dimension_rows": [],
                "regional_rows": [],
                "subsection_rows": [],
                "driver_rows": [],
                "relevant_item_rows": [],
                "priority_market_rows": [],
            },
        },
        "claim_catalog": {},
        "context_status": {
            "status": "not_attempted",
            "limitation_code": None,
        },
        "context_evidence": [],
        "dimension_narratives": dimensions,
        "market_narratives": {
            "Alpha": {
                "market_name": "Alpha",
                "region": "North",
                "overall_mfi": 4.5,
                "score_rank": 1,
                "weak_dimensions": ["Price", "Service"],
                "priority_issues": [],
                "recommended_interventions": [],
                "limitations": [market_limitation],
                "modality_consideration": None,
            }
        },
        "executive_summary_narrative": {},
        "qa_review": {},
        "visualizations": {"dim_price_bars": "present"},
        "document_references": [],
    }


def test_dimension_parser_enforces_priority_and_non_priority_ceilings() -> None:
    assessment = {"limitations": [], "metric_ledger": {}}
    non_priority = parse_dimension_narrative(
        _dimension_payload(),
        dimension_profile=_dimension_profile("Availability", priority=False),
        assessment_profile=assessment,
    )
    assert len(non_priority["key_findings"]) == 1
    assert non_priority["subdimension_analysis"] == []
    assert non_priority["geographic_patterns"] == []
    assert len(non_priority["data_limitations"]) == 1
    assert len(non_priority["recommendations"]) == 1

    priority = parse_dimension_narrative(
        _dimension_payload(),
        dimension_profile=_dimension_profile("Price", priority=True),
        assessment_profile=assessment,
    )
    assert len(priority["key_findings"]) == 3
    assert [claim["claim_id"] for claim in priority["key_findings"]] == [
        "dimension.price.finding.1",
        "dimension.price.finding.2",
        "dimension.price.finding.3",
    ]
    assert len(priority["subdimension_analysis"]) == 2
    assert len(priority["geographic_patterns"]) == 2
    assert len(priority["data_limitations"]) == 1
    assert len(priority["recommendations"]) == 3


def test_market_and_executive_parsers_enforce_canonical_ceilings() -> None:
    payload = {
        "priority_issues": [_claim(index, scope="market") for index in range(1, 6)],
        "recommended_interventions": [
            _claim(index, "recommendation", "market") for index in range(1, 6)
        ],
        "limitations": [
            {
                **_claim(index, "limitation", "market"),
                "metric_ids": [f"market.alpha.coverage.{index}"],
            }
            for index in range(1, 4)
        ],
        "modality_consideration": _claim(1, "modality_consideration", "market"),
    }
    market = parse_market_narrative(payload, market_profile=_market_profile())
    assert len(market["priority_issues"]) == 3
    assert [claim["claim_id"] for claim in market["priority_issues"]] == [
        f"market.{context_token('Alpha')}.issue.1",
        f"market.{context_token('Alpha')}.issue.2",
        f"market.{context_token('Alpha')}.issue.3",
    ]
    assert len(market["recommended_interventions"]) == 3
    assert len(market["limitations"]) == 1
    assert market["modality_consideration"] is None

    executive = parse_executive_narrative(
        {
            "key_findings": [_claim(index) for index in range(1, 7)],
            "recommendations": [
                _claim(index, "recommendation") for index in range(1, 7)
            ],
            "limitations": [
                _claim(index, "limitation") for index in range(1, 7)
            ],
        },
        assessment_profile={
            "priority_dimension_names": ["Price", "Service"],
            "limitations": [],
            "metric_ledger": {},
        },
    )
    assert len(executive["key_findings"]) == 2
    assert len(executive["recommendations"]) == 3
    assert len(executive["limitations"]) == 3


def test_market_limitation_model_is_additive_and_bounded() -> None:
    base = {
        "market_name": "Alpha",
        "overall_mfi": 4.5,
        "score_rank": 1,
    }
    assert MFIMarketNarrative(**base).limitations == []
    with pytest.raises(ValidationError):
        MFIMarketNarrative(
            **base,
            limitations=[_claim(1, "limitation", "market"), _claim(2, "limitation", "market")],
        )


def test_rehydrated_or_correction_state_is_rebounded_before_validation() -> None:
    market = {
        **_market_profile(),
        "priority_issues": [_claim(index, scope="market") for index in range(1, 6)],
        "recommended_interventions": [
            _claim(index, "recommendation", "market") for index in range(1, 6)
        ],
        "limitations": [
            {
                **_claim(index, "limitation", "market"),
                "metric_ids": [f"market.alpha.coverage.{index}"],
            }
            for index in range(1, 4)
        ],
        "modality_consideration": _claim(1, "modality_consideration", "market"),
    }
    dimensions, markets, executive = apply_narrative_density_policy(
        dimension_narratives={},
        market_narratives={"Alpha": market},
        executive_narrative={
            "key_findings": [_claim(index) for index in range(1, 5)],
            "recommendations": [],
            "limitations": [],
        },
        assessment_profile={"priority_dimension_names": ["Price", "Service"]},
    )
    assert dimensions == {}
    assert len(markets["Alpha"]["priority_issues"]) == 3
    assert len(markets["Alpha"]["limitations"]) == 1
    assert markets["Alpha"]["modality_consideration"] is None
    assert len(executive["key_findings"]) == 2


def test_repeated_recommendations_receive_dimension_specific_guidance() -> None:
    repeated = _claim(1, "recommendation")
    repeated["text"] = "Use the cited evidence to target further assessment."
    narratives = {
        dimension: {"recommendations": [dict(repeated)]}
        for dimension in ("Price", "Service")
    }
    profiles = [
        _dimension_profile("Price", priority=True),
        _dimension_profile("Service", priority=True),
    ]
    result = deduplicate_dimension_recommendations(narratives, profiles)
    assert result["Price"]["recommendations"][0]["text"] == repeated["text"]
    assert result["Service"]["recommendations"][0]["text"] == (
        DIMENSION_REVIEW_GUIDANCE["Service"]
    )


def test_prompt_contracts_declare_limits_and_never_request_modality() -> None:
    dimension_source = inspect.getsource(graph.node_dimension_drafter)
    market_source = inspect.getsource(graph.node_market_recommendations_drafter)
    executive_source = inspect.getsource(graph.node_executive_summary_drafter)
    assert "R8 CLAIM CEILINGS" in dimension_source
    assert "R8 CLAIM CEILINGS" in market_source
    assert "R8 CLAIM CEILINGS" in executive_source
    assert '"limitations": [CLAIM]' in market_source
    assert '"modality_consideration"' not in market_source


def test_report_layout_is_typed_ordered_and_dataset_independent() -> None:
    blocks = build_mfi_report_blocks(_report_result())
    title = blocks[0]
    assert title.meta["mfi_layout"]["report_family"] == "mfi"
    assert title.meta["mfi_layout"]["country"] == "Testland"
    page_breaks = {
        block.text
        for block in blocks
        if (block.meta or {}).get("mfi_layout", {}).get("page_break_before")
    }
    assert page_breaks == {
        "Executive summary",
        "MFI dimensions",
        "Expanded priority-dimension evidence",
        "Lowest-scoring assessed markets selected for review",
        "Methodology, limitations, and QA notices",
    }
    for dimension in DISPLAY_DIMENSIONS:
        assert sum(
            block.type == "heading" and block.text == dimension for block in blocks
        ) == 1
    summary_index = next(
        index
        for index, block in enumerate(blocks)
        if (block.meta or {}).get("claim_id") == "dimension.price.summary"
    )
    figure_index = next(
        index for index, block in enumerate(blocks) if block.figure_id == "dim_price_bars"
    )
    assert summary_index < figure_index
    assert any(
        (block.meta or {}).get("claim_id") == "market.alpha.limitation.1"
        for block in blocks
    )
    # The single neutral methodology boundary remains allowed; no market narrative
    # renders a modality conclusion or compatibility-field payload.
    assert not any(
        (block.meta or {}).get("claim_id", "").endswith("modality_consideration")
        for block in blocks
    )


def test_docx_contains_r8_styles_geometry_fields_and_grouping() -> None:
    blocks = build_mfi_report_blocks(_report_result())
    # A valid 1x1 PNG is enough to exercise figure/caption grouping without coupling the
    # structural test to matplotlib.
    png = base64.b64decode(
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9Y9Zl1sAAAAASUVORK5CYII="
    )
    docx = build_docx_bytes_from_report_blocks(
        blocks,
        visualizations={"dim_price_bars": base64.b64encode(png).decode("ascii")},
    )
    with zipfile.ZipFile(io.BytesIO(docx)) as archive:
        document_xml = archive.read("word/document.xml").decode("utf-8")
        styles_xml = archive.read("word/styles.xml").decode("utf-8")
        footer_xml = "\n".join(
            archive.read(name).decode("utf-8")
            for name in archive.namelist()
            if name.startswith("word/footer") and name.endswith(".xml")
        )
        header_xml = "\n".join(
            archive.read(name).decode("utf-8")
            for name in archive.namelist()
            if name.startswith("word/header") and name.endswith(".xml")
        )
    for style_name in (
        "MFI Title",
        "MFI Major Heading",
        "MFI Claim",
        "MFI Evidence Note",
        "MFI Notice",
        "MFI Caption",
    ):
        assert style_name in styles_xml
    assert 'w:w="11906"' in document_xml and 'w:h="16838"' in document_xml
    assert document_xml.count('w:top="1008"') >= 1
    assert "w:pageBreakBefore" in document_xml
    assert "w:keepNext" in document_xml
    assert "w:keepLines" in document_xml
    assert "MFI Drafter 2.0 - Testland" in header_xml
    assert "PAGE" in footer_xml and "NUMPAGES" in footer_xml
    assert not re.search(r"</w:tbl>\s*<w:p\s*/>", document_xml)
    inspected = inspect_docx_bytes(docx)
    assert inspected.max_boilerplate_repetition <= 1
    document = Document(io.BytesIO(docx))
    caption = next(
        paragraph
        for paragraph in document.paragraphs
        if paragraph.text == "Price by assessed market with regional means"
    )
    assert caption.style.name == "MFI Caption"


def test_streamlit_uses_mfi_major_section_dividers_without_reordering(monkeypatch) -> None:
    events: list[tuple[str, str]] = []
    monkeypatch.setattr(shared.st, "title", lambda text: events.append(("title", text)))
    monkeypatch.setattr(shared.st, "header", lambda text: events.append(("header", text)))
    monkeypatch.setattr(shared.st, "divider", lambda: events.append(("divider", "")))
    monkeypatch.setattr(shared.st, "markdown", lambda text: events.append(("text", text)))
    blocks = [
        ReportBlock(
            type="heading",
            text="MFI Report - Testland",
            level=1,
            meta={"mfi_layout": {"role": "title"}},
        ).model_dump(),
        ReportBlock(
            type="heading",
            text="Assessment metadata and coverage",
            level=2,
            meta={"mfi_layout": {"role": "major_section"}},
        ).model_dump(),
        ReportBlock(
            type="heading",
            text="Executive summary",
            level=2,
            meta={"mfi_layout": {"role": "major_section"}},
        ).model_dump(),
        ReportBlock(
            type="paragraph",
            text="Concise claim.",
            meta={"mfi_layout": {"role": "claim"}},
        ).model_dump(),
    ]
    shared.render_report_blocks(blocks, {})
    assert events == [
        ("title", "MFI Report - Testland"),
        ("header", "Assessment metadata and coverage"),
        ("divider", ""),
        ("header", "Executive summary"),
        ("text", "Concise claim."),
    ]


def test_density_policy_values_are_the_approved_targeted_ceilings() -> None:
    assert NARRATIVE_DENSITY_POLICY.non_priority_findings == 1
    assert NARRATIVE_DENSITY_POLICY.priority_findings == 3
    assert NARRATIVE_DENSITY_POLICY.priority_subdimensions == 2
    assert NARRATIVE_DENSITY_POLICY.market_priority_issues == 3
    assert NARRATIVE_DENSITY_POLICY.market_recommendations == 3
    assert NARRATIVE_DENSITY_POLICY.market_limitations == 1
