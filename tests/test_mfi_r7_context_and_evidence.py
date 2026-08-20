from __future__ import annotations

from io import BytesIO

import pytest
from docx import Document
from pydantic import ValidationError

from app.services.mfi_drafter import graph
from app.services.mfi_drafter.context_status import (
    not_attempted_context_status,
    reconcile_context_status,
    resolve_context_status,
)
from app.services.mfi_drafter.evidence_notes import compose_evidence_note
from app.services.mfi_drafter.methodology import METRIC_DEFINITIONS_BY_ID
from app.services.mfi_drafter.narrative import build_claim_catalog, evidence_note
from app.services.mfi_drafter.report_inspector import inspect_report_blocks
from app.services.mfi_drafter.router import (
    _analysis_run_metadata,
    _build_mfi_output,
)
from app.services.mfi_drafter.schemas import (
    MFIContextRetrieverStatus,
    MFIContextStatus,
)
from app.services.mfi_drafter.synthetic_fixtures import build_report_run
from app.shared.docx_export import build_docx_bytes_from_report_blocks
from app.shared.llm_observability import LLMCallError
from app.shared.report_blocks import ReportBlock, build_mfi_report_blocks
from app.streamlit_backend.dispatcher import (
    _build_mfi_report_output,
    _mfi_analysis_run_metadata,
)


def _document(doc_id: str = "doc-1", source: str = "ReliefWeb") -> dict:
    return {
        "doc_id": doc_id,
        "source": source,
        "title": f"Document {doc_id}",
        "url": f"https://example.test/{doc_id}",
        "date": "2026-01-15",
        "content": "Contextual source text.",
    }


def _statement(
    statement_id: str = "context.statement.1",
    *,
    document_ids: list[str] | None = None,
    classification: str = "corroborating",
    substituted: bool = False,
) -> dict:
    return {
        "statement_id": statement_id,
        "text": "The cited document reports a relevant market condition.",
        "classification": classification,
        "document_ids": document_ids if document_ids is not None else ["doc-1"],
        "validation_status": "verified",
        "validation_flags": [],
        "substituted": substituted,
    }


def _empty_profile() -> dict:
    return {
        "tables": {
            "dimension_rows": [],
            "regional_rows": [],
            "subsection_rows": [],
            "driver_rows": [],
            "relevant_item_rows": [],
            "priority_market_rows": [],
        },
        "metric_ledger": {},
        "priority_dimension_names": [],
        "markets": [],
        "limitations": [],
    }


def _report_result(*, context_status: dict, evidence: list[dict], docs: list[dict]) -> dict:
    return {
        "country": "Testland",
        "data_collection_start": "2026-01-01",
        "data_collection_end": "2026-01-31",
        "assessment_profile": _empty_profile(),
        "claim_catalog": {},
        "context_status": context_status,
        "context_evidence": evidence,
        "contextual_documents": docs,
        "document_references": [
            {key: document.get(key) for key in ("doc_id", "source", "title", "url", "date")}
            for document in docs
        ],
        "dimension_narratives": {},
        "market_narratives": {},
        "executive_summary_narrative": {},
        "visualizations": {},
        "qa_review": {},
    }


def test_context_status_models_are_frozen_sorted_and_count_consistent() -> None:
    status = MFIContextStatus(
        status="available",
        retrievers={
            "Seerist": MFIContextRetrieverStatus(
                status="no_results", retrieved_document_count=0
            ),
            "ReliefWeb": MFIContextRetrieverStatus(
                status="completed", retrieved_document_count=1
            ),
        },
        total_deduplicated_documents_retrieved=1,
        statements_classified=1,
        final_accepted_statements=1,
        extraction_mode="llm",
    )
    assert list(status.retrievers) == ["ReliefWeb", "Seerist"]
    with pytest.raises(ValidationError):
        status.status = "no_results"
    with pytest.raises(ValidationError):
        MFIContextStatus(
            status="no_results",
            retrievers={
                "ReliefWeb": MFIContextRetrieverStatus(
                    status="completed", retrieved_document_count=1
                )
            },
            total_deduplicated_documents_retrieved=0,
            statements_classified=0,
            final_accepted_statements=0,
        )


@pytest.mark.parametrize(
    ("retrievers", "documents", "statements", "failed", "expected", "code"),
    [
        (
            {"ReliefWeb": "completed", "Seerist": "no_results"},
            [_document()],
            [_statement()],
            False,
            "available",
            None,
        ),
        (
            {"ReliefWeb": "no_results", "Seerist": "no_results"},
            [],
            [],
            False,
            "no_results",
            None,
        ),
        (
            {"ReliefWeb": "failed", "Seerist": "no_results"},
            [],
            [],
            False,
            "retrieval_failed",
            "context_retrieval_unavailable",
        ),
        (
            {"ReliefWeb": "completed", "Seerist": "no_results"},
            [_document()],
            [],
            True,
            "classification_failed",
            "context_classification_unavailable",
        ),
        (
            {"ReliefWeb": "completed", "Seerist": "no_results"},
            [_document()],
            [_statement(classification="unrelated")],
            False,
            "no_accepted_statements",
            None,
        ),
        (
            {"ReliefWeb": "completed", "Seerist": "failed"},
            [_document()],
            [_statement()],
            False,
            "available",
            "context_partial_retrieval_unavailable",
        ),
    ],
)
def test_context_state_resolution(
    retrievers, documents, statements, failed, expected, code
) -> None:
    status = resolve_context_status(
        retriever_statuses=retrievers,
        documents=documents,
        statements=statements,
        extraction_mode="fallback" if failed else "llm",
        classification_failed=failed,
    )
    assert status.status == expected
    assert status.limitation_code == code


def test_offline_context_is_not_attempted_and_qa_withdrawal_removes_acceptance() -> None:
    offline = not_attempted_context_status()
    assert offline.status == "not_attempted"
    assert {item.status for item in offline.retrievers.values()} == {"not_attempted"}

    available = resolve_context_status(
        retriever_statuses={"ReliefWeb": "completed", "Seerist": "no_results"},
        documents=[_document()],
        statements=[_statement()],
        extraction_mode="llm",
    )
    reconciled = reconcile_context_status(
        available,
        documents=[_document()],
        statements=[_statement(substituted=True, document_ids=[])],
    )
    assert reconciled.status == "no_accepted_statements"
    assert reconciled.final_accepted_statements == 0


def test_finalize_qa_reconciles_withdrawn_context_to_no_accepted_statements() -> None:
    document = _document()
    statement = _statement()
    available = resolve_context_status(
        retriever_statuses={"ReliefWeb": "completed", "Seerist": "no_results"},
        documents=[document],
        statements=[statement],
        extraction_mode="llm",
    )
    flag = {
        "flag_id": "context-high-1",
        "source": "deterministic",
        "code": "unsupported_causal_claim",
        "severity": "high",
        "artifact_type": "context",
        "artifact_id": "context.statement.1",
        "field_name": "text",
        "claim_id": None,
        "message": "The contextual statement asserted causality.",
        "recommendation": "Withdraw the assertion.",
        "metric_ids": [],
        "document_ids": ["doc-1"],
        "repairable": True,
    }
    final = graph.node_finalize_qa(
        {
            "deterministic_flags": [flag],
            "red_team_flags": [],
            "correction_attempts": 0,
            "correction_history": [],
            "dimension_narratives": {},
            "market_narratives": {},
            "executive_summary_narrative": {},
            "context_evidence": [statement],
            "contextual_documents": [document],
            "context_status": available.model_dump(),
        }
    )
    assert final["context_evidence"][0]["substituted"] is True
    assert final["context_status"]["status"] == "no_accepted_statements"
    report_result = _report_result(
        context_status=final["context_status"],
        evidence=final["context_evidence"],
        docs=[document],
    )
    report_result.update(
        {
            "qa_review": final["qa_review"],
            "generation_diagnostics": final["generation_diagnostics"],
            "claim_substitutions": final["claim_substitutions"],
        }
    )
    blocks = build_mfi_report_blocks(report_result)
    rejected_text = statement["text"]
    assert rejected_text not in "\n".join(block.text or "" for block in blocks)
    context_claim_index = next(
        index
        for index, block in enumerate(blocks)
        if block.type == "paragraph"
        and isinstance(block.meta, dict)
        and block.meta.get("claim_id") == "context.statement.1"
    )
    assert blocks[context_claim_index + 1].type == "claim_warning"


class _Model:
    def __init__(self, content: str):
        self.content = content

    def invoke(self, _messages):
        return type("Response", (), {"content": self.content})()


def test_context_extractor_distinguishes_schema_failure_and_no_accepted(monkeypatch) -> None:
    state = {
        "country": "Testland",
        "contextual_documents": [_document()],
        "generation_diagnostics": {
            "retrievers": {"ReliefWeb": "completed", "Seerist": "no_results"}
        },
        "llm_calls": 0,
    }
    monkeypatch.setattr(graph, "get_model", lambda: _Model("{}"))
    with pytest.raises(LLMCallError) as caught:
        graph.node_context_extractor(state)
    assert caught.value.failure_code == "llm_response_contract_error"
    assert caught.value.node == "context_extractor"

    monkeypatch.setattr(
        graph,
        "get_model",
        lambda: _Model(
            '{"statements":[{"statement_id":"context-1","text":"Not relevant",'
            '"classification":"unrelated","document_ids":["doc-1"]}]}'
        ),
    )
    unrelated = graph.node_context_extractor(state)
    assert unrelated["context_status"]["status"] == "no_accepted_statements"


def test_partial_live_retrieval_keeps_raw_error_only_in_trace(monkeypatch) -> None:
    class FailedReliefWeb:
        last_trace = {"error": "provider secret detail"}

        def __init__(self, *args, **kwargs):
            pass

        @staticmethod
        def build_economy_query(**_kwargs):
            return "query"

        def fetch(self, **_kwargs):
            return []

    class WorkingSeerist:
        DEFAULT_ECON_TERMS = ("market",)
        last_trace = {"error": None}

        def __init__(self, *args, **kwargs):
            pass

        @staticmethod
        def build_lucene_or_query(_terms):
            return "query"

        def fetch_batch(self, **_kwargs):
            return [_document(source="Seerist")]

    monkeypatch.setattr(graph, "ReliefWebRetriever", FailedReliefWeb)
    monkeypatch.setattr(graph, "SeeristRetriever", WorkingSeerist)
    update = graph.node_context_retrieval(
        {
            "country": "Testland",
            "data_collection_start": "2026-01-01",
            "data_collection_end": "2026-01-31",
            "score_authority": "databridge_level_1",
        }
    )
    assert "warnings" not in update
    assert update["context_status"]["status"] == "no_accepted_statements"
    assert (
        update["context_status"]["limitation_code"]
        == "context_partial_retrieval_unavailable"
    )
    assert update["retriever_traces"][0]["error"] == "provider secret detail"


@pytest.mark.parametrize(
    ("status", "message"),
    [
        (
            "no_results",
            "No contextual documents were retrieved for the selected country and period.",
        ),
        (
            "retrieval_failed",
            "Context retrieval was unavailable; interpretation relies only on the MFI assessment.",
        ),
        (
            "classification_failed",
            "Context classification was unavailable after documents were retrieved; interpretation relies only on the MFI assessment.",
        ),
        (
            "no_accepted_statements",
            "Documents were retrieved, but none met the evidence-classification requirements.",
        ),
        (
            "not_attempted",
            "Context retrieval was not run for this report; interpretation relies only on the MFI assessment.",
        ),
    ],
)
def test_context_section_always_discloses_nonavailable_state(status, message) -> None:
    limitation = {
        "retrieval_failed": "context_retrieval_unavailable",
        "classification_failed": "context_classification_unavailable",
    }.get(status)
    blocks = build_mfi_report_blocks(
        _report_result(
            context_status={"status": status, "limitation_code": limitation},
            evidence=[],
            docs=[_document()] if status in {"classification_failed", "no_accepted_statements"} else [],
        )
    )
    context_index = next(
        index
        for index, block in enumerate(blocks)
        if block.type == "heading" and block.text == "Context and sources"
    )
    assert blocks[context_index + 1].text == message
    if limitation:
        assert blocks[context_index + 2].type == "limitation_box"
        assert blocks[context_index + 2].meta["code"] == limitation


def test_available_context_renders_only_accepted_statements_and_cited_references() -> None:
    docs = [_document("doc-1"), _document("doc-2", "Seerist")]
    evidence = [
        _statement("accepted", document_ids=["doc-1"]),
        _statement("unrelated", document_ids=["doc-2"], classification="unrelated"),
        _statement("uncited", document_ids=[]),
    ]
    status = resolve_context_status(
        retriever_statuses={"ReliefWeb": "completed", "Seerist": "completed"},
        documents=docs,
        statements=evidence,
        extraction_mode="llm",
    )
    blocks = build_mfi_report_blocks(
        _report_result(
            context_status=status.model_dump(), evidence=evidence, docs=docs
        )
    )
    visible_claim_ids = {
        block.meta.get("claim_id")
        for block in blocks
        if block.type == "paragraph" and isinstance(block.meta, dict)
    }
    assert "accepted" in visible_claim_ids
    assert "unrelated" not in visible_claim_ids
    assert "uncited" not in visible_claim_ids
    reference_block = next(block for block in blocks if block.type == "references")
    assert [item["doc_id"] for item in reference_block.references] == ["doc-1"]


def test_partial_retrieval_keeps_valid_context_and_adds_stable_limitation() -> None:
    document = _document()
    evidence = [_statement()]
    status = resolve_context_status(
        retriever_statuses={"ReliefWeb": "completed", "Seerist": "failed"},
        documents=[document],
        statements=evidence,
        extraction_mode="llm",
    )
    blocks = build_mfi_report_blocks(
        _report_result(
            context_status=status.model_dump(), evidence=evidence, docs=[document]
        )
    )
    assert any(
        block.type == "limitation_box"
        and block.meta.get("code") == "context_partial_retrieval_unavailable"
        for block in blocks
    )
    assert any(
        block.type == "paragraph"
        and isinstance(block.meta, dict)
        and block.meta.get("claim_id") == "context.statement.1"
        for block in blocks
    )


def _coverage(available: int, total: int) -> dict:
    return {
        "available_market_count": available,
        "total_assessed_market_count": total,
        "missing_count": total - available,
        "coverage_ratio": available / total,
    }


def _ledger_entry(
    *,
    label: str,
    statistic: str,
    source_metric_id: str,
    coverage: dict,
    representation_basis: str = "all_assessed_markets",
) -> dict:
    return {
        "label": label,
        "value": 0.5 if statistic == "coverage_ratio" else 5.0,
        "statistic": statistic,
        "unit": "proportion" if statistic == "coverage_ratio" else "score",
        "orientation": "higher_is_better",
        "evidence_scope": "included_assessed_markets",
        "coverage": coverage,
        "source_metric_ids": [source_metric_id],
        "representation_basis": representation_basis,
    }


def test_catalog_derives_fixed_item_applicability_and_explicit_coverage_representation() -> None:
    official_id = next(
        metric_id
        for metric_id, definition in METRIC_DEFINITIONS_BY_ID.items()
        if definition.role == "official_score"
    )
    item_id = next(
        metric_id
        for metric_id, definition in METRIC_DEFINITIONS_BY_ID.items()
        if definition.role == "item_driver"
    )
    applicability_id = next(
        metric_id
        for metric_id, definition in METRIC_DEFINITIONS_BY_ID.items()
        if definition.applicability_rule == "quality_applicability"
    )
    catalog = build_claim_catalog(
        {
            "metric_ledger": {
                "fixed.complete": _ledger_entry(
                    label="Price mean", statistic="mean", source_metric_id=official_id,
                    coverage=_coverage(27, 27),
                ),
                "fixed.partial": _ledger_entry(
                    label="Price evidence", statistic="mean", source_metric_id=official_id,
                    coverage=_coverage(12, 27),
                    representation_basis="incomplete_assessed_markets",
                ),
                "item": _ledger_entry(
                    label="Barley: unfavorable rate", statistic="mean", source_metric_id=item_id,
                    coverage=_coverage(12, 27),
                    representation_basis="represented_assessed_markets",
                ),
                "applicability": _ledger_entry(
                    label="Food quality condition", statistic="mean",
                    source_metric_id=applicability_id, coverage=_coverage(8, 27),
                    representation_basis="applicable_assessed_markets",
                ),
                "coverage": _ledger_entry(
                    label="Price coverage", statistic="coverage_ratio",
                    source_metric_id=official_id, coverage=_coverage(27, 27),
                ),
            }
        }
    )
    assert catalog["fixed.complete"]["representation_kind"] == "fixed_metric"
    assert catalog["fixed.complete"]["representation_complete"] is True
    assert catalog["fixed.complete"]["representation_required"] is False
    assert catalog["fixed.partial"]["representation_required"] is True
    assert catalog["item"]["representation_kind"] == "item"
    assert catalog["item"]["representation_required"] is True
    assert catalog["applicability"]["representation_kind"] == "applicability"
    assert catalog["coverage"]["representation_required"] is True


def test_evidence_note_is_scope_correct_grouped_concise_and_id_free() -> None:
    catalog = {
        "metric.secret.1": {
            "label": "Price mean",
            "formatted_value": "5.00/10",
            "scope": "market",
            "market_name": "An Nasser",
            "representation_kind": "fixed_metric",
            "representation_complete": True,
            "representation_required": False,
            "represented_market_count": 27,
            "assessed_market_count": 27,
        },
        "metric.secret.2": {
            "label": "Price stability",
            "formatted_value": "2.50/10",
            "scope": "market",
            "market_name": "An Nasser",
            "representation_kind": "fixed_metric",
            "representation_complete": False,
            "representation_required": True,
            "represented_market_count": 12,
            "assessed_market_count": 27,
        },
        "metric.secret.3": {
            "label": "Price increase",
            "formatted_value": "8.00/10",
            "scope": "market",
            "market_name": "An Nasser",
            "representation_kind": "fixed_metric",
            "representation_complete": False,
            "representation_required": True,
            "represented_market_count": 12,
            "assessed_market_count": 27,
        },
    }
    claim = {
        "scope": "market",
        "metric_ids": ["metric.secret.1", "metric.secret.2", "metric.secret.3"],
        "document_ids": ["doc-1"],
    }
    note = compose_evidence_note(claim, catalog, {"doc-1": _document()})
    assert note.startswith("Claim scope: market — An Nasser;")
    assert note.index("Price mean") < note.index("Price stability") < note.index("Price increase")
    assert "27/27" not in note
    assert note.count("Assessment representation:") == 1
    assert "12/27 assessed markets" in note
    assert "Context sources: Document doc-1 (2026-01-15)" in note
    assert "metric.secret" not in note
    assert evidence_note(claim, catalog, {"doc-1": _document()}) == note


@pytest.mark.parametrize(
    ("scope", "entry", "expected"),
    [
        ("assessment", {}, "Claim scope: assessed-market profile"),
        ("region", {"region": "North"}, "Claim scope: region — North"),
        ("market", {"market_name": "Alpha"}, "Claim scope: market — Alpha"),
        ("surveyed_traders", {}, "Claim scope: surveyed traders"),
    ],
)
def test_evidence_note_scope_labels(scope, entry, expected) -> None:
    catalog_entry = {
        "label": "Metric",
        "formatted_value": "5.00/10",
        "scope": scope,
        "representation_kind": "fixed_metric",
        "representation_complete": True,
        "representation_required": False,
        **entry,
    }
    note = compose_evidence_note(
        {"scope": scope, "metric_ids": ["metric"], "document_ids": []},
        {"metric": catalog_entry},
        {},
    )
    assert note.startswith(expected)


def test_item_applicability_and_document_only_notes_are_always_disclosed() -> None:
    catalog = {
        "item": {
            "label": "Barley: unfavorable rate",
            "formatted_value": "40.0%",
            "scope": "assessment",
            "representation_kind": "item",
            "representation_complete": False,
            "representation_required": True,
            "represented_market_count": 12,
            "assessed_market_count": 27,
        },
        "quality": {
            "label": "Acceptable food quality",
            "formatted_value": "75.0%",
            "scope": "assessment",
            "representation_kind": "applicability",
            "representation_complete": False,
            "representation_required": True,
            "represented_market_count": 8,
            "assessed_market_count": 27,
        },
    }
    note = compose_evidence_note(
        {"metric_ids": ["item", "quality"], "document_ids": []}, catalog, {}
    )
    assert "Item representation: Barley observed in 12/27 assessed markets" in note
    assert "Applicability representation:" in note
    document_only = compose_evidence_note(
        {"metric_ids": [], "document_ids": ["doc-1"]}, {}, {"doc-1": _document()}
    )
    assert document_only.startswith("Claim scope: context;")


def test_canonical_evidence_block_is_identical_in_blocks_and_docx() -> None:
    note = "Claim scope: assessed-market profile; Service mean: 4.20/10."
    blocks = [ReportBlock(type="evidence_note", text=note)]
    report = inspect_report_blocks(blocks)
    assert report.evidence_note_count == 1
    assert report.evidence_note_word_count > 0
    document = Document(
        BytesIO(build_docx_bytes_from_report_blocks(blocks, visualizations={}))
    )
    assert any(paragraph.text == f"Evidence: {note}" for paragraph in document.paragraphs)


@pytest.fixture(scope="module")
def deterministic_run():
    return build_report_run(render_figures=False)


def test_context_status_propagates_through_outputs_metadata_and_offline_render(
    deterministic_run,
) -> None:
    status = deterministic_run.result["context_status"]
    assert status["status"] == "not_attempted"
    assert "Context and sources" not in deterministic_run.result.get("warnings", [])
    assert _analysis_run_metadata({"context_status": status})["context_status"] == status
    assert _mfi_analysis_run_metadata({"context_status": status})["context_status"] == status

    result = dict(deterministic_run.result)
    api_output = _build_mfi_output(
        result=result,
        country=result["country"],
        data_collection_start=result["data_collection_start"],
        data_collection_end=result["data_collection_end"],
    )
    assert api_output.context_status.status == "not_attempted"
    dispatcher_output = _build_mfi_report_output(
        result=result,
        run_id="r7-run",
        country=result["country"],
        data_collection_start=result["data_collection_start"],
        data_collection_end=result["data_collection_end"],
    )
    assert dispatcher_output["context_status"]["status"] == "not_attempted"
    assert any(
        block.type == "paragraph"
        and block.text
        == "Context retrieval was not run for this report; interpretation relies only on the MFI assessment."
        for block in deterministic_run.blocks
    )


def test_report_evidence_notes_keep_ids_in_metadata_and_adjacent_to_claims(
    deterministic_run,
) -> None:
    blocks = deterministic_run.blocks
    evidence_count = 0
    for index, block in enumerate(blocks):
        if block.type != "evidence_note":
            continue
        evidence_count += 1
        assert index > 0 and blocks[index - 1].type == "paragraph"
        metric_ids = list((block.meta or {}).get("metric_ids", []) or [])
        document_ids = list((block.meta or {}).get("document_ids", []) or [])
        for raw_id in [*metric_ids, *document_ids]:
            assert str(raw_id) not in str(block.text)
    assert evidence_count > 0
