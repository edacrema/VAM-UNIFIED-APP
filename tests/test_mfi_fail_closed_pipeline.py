from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.services.mfi_drafter import graph
from app.services.mfi_drafter.claim_identity import dimension_claim_id
from app.services.mfi_drafter.claim_identity import (
    canonical_claim_index,
    canonicalize_narrative_identities,
)
from app.services.mfi_drafter.errors import MFIGenerationBlockedError
from app.services.mfi_drafter.narrative import (
    _duplicate_dimension_recommendation_flags,
)
from app.services.mfi_drafter.qa_pipeline import (
    apply_field_patch,
    build_red_team_batches,
    build_sequential_correction_tasks,
    validate_field_patch_payload,
)
from app.services.mfi_drafter.schemas import MFIReleaseControl
from app.services.mfi_drafter.synthetic_fixtures import SyntheticSpec, build_loaded
from app.shared.docx_export import build_docx_bytes_from_report_blocks
from app.shared.llm_observability import LLMCallError
from app.shared.report_blocks import resolve_mfi_report_blocks


def _claim(text: str, *, scope: str = "assessment") -> dict:
    return {
        "text": text,
        "claim_kind": "finding",
        "metric_ids": [],
        "document_ids": [],
        "scope": scope,
        "polarity": "neutral",
    }


def _dimension_narrative(name: str = "Infrastructure") -> dict:
    return {
        "dimension": name,
        "is_priority": True,
        "summary": {
            **_claim("Original summary."),
            "claim_id": dimension_claim_id(name, "summary", 1),
            "claim_kind": "summary",
        },
        "key_findings": [],
        "subdimension_analysis": [],
        "geographic_patterns": [],
        "data_limitations": [],
        "recommendations": [],
    }


def _flag(
    flag_id: str,
    *,
    artifact_type: str = "dimension",
    artifact_id: str = "Infrastructure",
    field_name: str = "geographic_patterns",
    severity: str = "medium",
    claim_id: str | None = None,
    repairable: bool = True,
) -> dict:
    return {
        "flag_id": flag_id,
        "source": "deterministic",
        "code": "test_flag",
        "severity": severity,
        "artifact_type": artifact_type,
        "artifact_id": artifact_id,
        "field_name": field_name,
        "claim_id": claim_id,
        "message": "Repair this field.",
        "repairable": repairable,
    }


def test_correction_tasks_group_exact_field_and_follow_report_order() -> None:
    flags = [
        _flag("price-rec", artifact_id="Price", field_name="recommendations"),
        _flag("infra-geo-2"),
        _flag("infra-geo-1"),
        _flag("market", artifact_type="market", artifact_id="B", field_name="priority_issues"),
    ]
    tasks = build_sequential_correction_tasks(
        flags,
        attempt_number=1,
        assessment_profile={"priority_market_names": ["B"]},
    )
    assert [
        (item["artifact_type"], item["artifact_id"], item["field_name"])
        for item in tasks
    ] == [
        ("dimension", "Price", "recommendations"),
        ("dimension", "Infrastructure", "geographic_patterns"),
        ("market", "B", "priority_issues"),
    ]
    infrastructure = tasks[1]
    assert infrastructure["flag_ids"] == ["infra-geo-1", "infra-geo-2"]


def test_field_only_geographic_patch_merges_without_requiring_summary() -> None:
    original = _dimension_narrative()
    task = build_sequential_correction_tasks(
        [_flag("geo")],
        attempt_number=1,
        assessment_profile={"priority_market_names": []},
    )[0]
    payload = {
        "replacement": [
            {
                **_claim("The lowest observed markets warrant local review.", scope="market"),
                "claim_kind": "geographic_pattern",
            }
        ]
    }
    replacement = validate_field_patch_payload(payload, task=task)
    merged = apply_field_patch(
        task=task,
        replacement=replacement,
        dimension_narratives={"Infrastructure": original},
        market_narratives={},
        executive_narrative={},
        context_evidence=[],
        assessment_profile={
            "dimensions": [{"dimension": "Infrastructure", "is_priority": True}],
            "priority_dimension_names": ["Infrastructure"],
        },
    )
    updated = merged["dimension_narratives"]["Infrastructure"]
    assert updated["summary"]["text"] == "Original summary."
    assert updated["geographic_patterns"][0]["text"].startswith("The lowest")
    assert updated["geographic_patterns"][0]["claim_id"] == (
        dimension_claim_id("Infrastructure", "geography", 1)
    )


def test_patch_contract_rejects_complete_artifact_and_model_identity() -> None:
    task = build_sequential_correction_tasks(
        [_flag("geo")],
        attempt_number=1,
        assessment_profile={"priority_market_names": []},
    )[0]
    with pytest.raises(Exception):
        validate_field_patch_payload(
            {
                "replacement": [{**_claim("Pattern.", scope="market"), "claim_id": "model-id"}],
                "summary": _claim("Unrequested field."),
            },
            task=task,
        )


def test_global_or_nonrepairable_material_flag_blocks_before_llm() -> None:
    with pytest.raises(MFIGenerationBlockedError) as caught:
        build_sequential_correction_tasks(
            [
                _flag(
                    "global",
                    artifact_type="global",
                    artifact_id="global",
                    field_name="",
                    severity="high",
                    repairable=False,
                )
            ],
            attempt_number=1,
            assessment_profile={},
        )
    assert caught.value.code == "mfi_narrative_qa_unresolved"
    assert caught.value.status_code == 502


def test_duplicate_recommendation_is_medium_finding_without_text_replacement() -> None:
    narratives = {
        "Assortment": {
            "recommendations": [
                {**_claim("Review the same evidence."), "claim_id": "a"}
            ]
        },
        "Price": {
            "recommendations": [
                {**_claim("Review the same evidence."), "claim_id": "p"}
            ]
        },
    }
    before = narratives["Price"]["recommendations"][0]["text"]
    flags = _duplicate_dimension_recommendation_flags(narratives)
    assert narratives["Price"]["recommendations"][0]["text"] == before
    assert len(flags) == 1
    assert flags[0]["severity"] == "medium"
    assert flags[0]["field_name"] == "recommendations"
    assert flags[0]["repairable"] is True


def _review_source(*, long_text: str = "") -> dict:
    claims = [
        {
            "a": "dimension",
            "aid": "Price",
            "f": "summary",
            "p": 1,
            "id": "dimension.price.summary.1",
            "text": long_text or "Price summary.",
            "s": "assessment",
            "o": "neutral",
            "m": ["price.mean"],
            "d": [],
        },
        {
            "a": "dimension",
            "aid": "Price",
            "f": "recommendations",
            "p": 1,
            "id": "dimension.price.recommendation.1",
            "text": "Price recommendation.",
            "s": "assessment",
            "o": "neutral",
            "m": ["price.mean"],
            "d": [],
        },
        {
            "a": "executive_summary",
            "aid": "executive_summary",
            "f": "key_findings",
            "p": 1,
            "id": "executive.finding.1",
            "text": "Executive finding.",
            "s": "assessment",
            "o": "neutral",
            "m": ["assessment.mean"],
            "d": ["doc-1"],
        },
    ]
    return {
        "claims": claims,
        "context_statements": [],
        "priority_context": {
            "dimensions": [{"id": "Price", "rank": 1}],
            "markets": [],
        },
        "evidence_by_metric_id": {
            "price.mean": {"formatted_value": "4.00"},
            "assessment.mean": {"formatted_value": "5.00"},
            "unused": {"formatted_value": "9.99"},
        },
        "cited_documents": [
            {"id": "doc-1", "title": "Used"},
            {"id": "doc-unused", "title": "Unused"},
        ],
        "limitations": [],
        "deterministic_flags": [],
        "prohibitions": [],
    }


def test_red_team_batches_are_bounded_deterministic_and_locally_exhaustive() -> None:
    first = build_red_team_batches(_review_source(), target_characters=1_400)
    second = build_red_team_batches(_review_source(), target_characters=1_400)
    assert [item.model_dump() for item in first] == [item.model_dump() for item in second]
    assert all(item.character_count <= 1_400 for item in first)
    local_claims = [
        claim_id
        for batch in first
        if batch.batch_kind == "local"
        for claim_id in batch.claim_ids
    ]
    assert local_claims == [
        "dimension.price.summary.1",
        "dimension.price.recommendation.1",
        "executive.finding.1",
    ]
    assert len(local_claims) == len(set(local_claims))
    for batch in first:
        cited = {
            metric_id
            for row in batch.package["claims"]
            for metric_id in row.get("m", [])
        }
        assert set(batch.package["evidence_by_metric_id"]) == cited
        assert "unused" not in batch.package["evidence_by_metric_id"]
        assert all(item["id"] != "doc-unused" for item in batch.package["cited_documents"])


def test_changed_field_invalidates_only_its_local_and_related_coherence_batches() -> None:
    first = build_red_team_batches(_review_source())
    changed_source = _review_source()
    changed_source["claims"][0]["text"] = "Corrected Price summary."
    second = build_red_team_batches(changed_source)
    first_by_id = {item.batch_id: item for item in first}
    second_by_id = {item.batch_id: item for item in second}
    common = set(first_by_id) & set(second_by_id)
    changed = {
        batch_id
        for batch_id in common
        if first_by_id[batch_id].signature != second_by_id[batch_id].signature
    }
    changed_kinds = {second_by_id[item].batch_kind for item in changed}
    assert changed_kinds == {"local", "dimension_coherence"}
    assert all(
        second_by_id[item].batch_kind != "market_coherence" for item in changed
    )


def test_oversized_red_team_atomic_field_fails_without_truncation() -> None:
    with pytest.raises(MFIGenerationBlockedError) as caught:
        build_red_team_batches(
            _review_source(long_text="x" * 2_000),
            target_characters=1_000,
        )
    assert caught.value.code == "mfi_red_team_batch_contract_failed"


class _NormalizationTrace:
    def __init__(self, first: LLMCallError, repaired_value: object = None):
        self.first = first
        self.repaired_value = repaired_value
        self.operations: list[str] = []
        self.recovered: list[str] = []

    def invoke_json(self, **kwargs):
        self.operations.append(kwargs["operation"])
        if len(self.operations) == 1:
            raise self.first
        return SimpleNamespace(call_id="repair-call", value=self.repaired_value)

    def mark_recovered(self, call_id: str) -> None:
        self.recovered.append(call_id)


def test_invalid_json_gets_one_distinct_syntax_normalization_call() -> None:
    trace = _NormalizationTrace(
        LLMCallError(
            failure_code="llm_invalid_json",
            call_id="original-call",
            node="dimension_drafter",
            operation="mfi.dimension_drafting.v2",
            stage="json_parse",
            raw_text='{ "summary": ',
        ),
        repaired_value={"validated": True},
    )
    result, calls = graph._invoke_json_with_one_normalization(
        trace=trace,
        model=object(),
        messages=[],
        node="dimension_drafter",
        operation="mfi.dimension_drafting.v2",
        artifact_type="dimension",
        artifact_id="Price",
        correction_attempt=0,
        validator=lambda payload: payload,
    )
    assert result.value == {"validated": True}
    assert calls == 2
    assert trace.operations == [
        "mfi.dimension_drafting.v2",
        "mfi.dimension_drafting.v2.json_normalization.v1",
    ]
    assert trace.recovered == ["original-call"]


def test_contract_incomplete_response_is_not_sent_to_normalization() -> None:
    failure = LLMCallError(
        failure_code="llm_response_contract_error",
        call_id="contract-call",
        node="dimension_drafter",
        operation="mfi.dimension_drafting.v2",
        stage="contract_validation",
    )
    trace = _NormalizationTrace(failure)
    with pytest.raises(LLMCallError) as caught:
        graph._invoke_json_with_one_normalization(
            trace=trace,
            model=object(),
            messages=[],
            node="dimension_drafter",
            operation="mfi.dimension_drafting.v2",
            artifact_type="dimension",
            artifact_id="Price",
            correction_attempt=0,
            validator=lambda payload: payload,
        )
    assert caught.value is failure
    assert trace.operations == ["mfi.dimension_drafting.v2"]


def test_live_graph_has_no_offline_narrative_fixture_dependency() -> None:
    root = Path(__file__).resolve().parents[1]
    live_paths = [
        root / "app/services/mfi_drafter/graph.py",
        root / "app/services/mfi_drafter/router.py",
        root / "app/streamlit_backend/dispatcher.py",
        root / "app/shared/report_blocks.py",
    ]
    for path in live_paths:
        source = path.read_text(encoding="utf-8")
        assert "offline_narrative_fixtures" not in source
        assert "fallback_dimension_narrative(" not in source
        assert "fallback_market_narrative(" not in source
        assert "fallback_executive_narrative(" not in source


def test_material_final_qa_and_delivery_are_fail_closed() -> None:
    state = {
        "deterministic_flags": [_flag("material")],
        "red_team_flags": [],
        "correction_attempts": 3,
    }
    with pytest.raises(MFIGenerationBlockedError) as qa_error:
        graph.node_finalize_qa(state)
    assert qa_error.value.code == "mfi_narrative_qa_unresolved"
    assert qa_error.value.status_code == 502

    with pytest.raises(MFIGenerationBlockedError) as delivery_error:
        graph.node_finalize_delivery(state)
    assert delivery_error.value.code == "mfi_narrative_qa_unresolved"


def test_report_block_contract_failure_uses_stable_internal_error(
    monkeypatch,
) -> None:
    monkeypatch.setattr(graph, "assert_claim_identity_contract", lambda *_args: None)
    monkeypatch.setattr(
        "app.shared.report_blocks.build_mfi_report_blocks",
        lambda _state: (_ for _ in ()).throw(AssertionError("invalid mapping")),
    )
    with pytest.raises(MFIGenerationBlockedError) as caught:
        graph.node_finalize_delivery(
            {
                "dimension_narratives": {},
                "market_narratives": {},
                "executive_summary_narrative": {},
                "context_evidence": [],
                "deterministic_flags": [],
                "red_team_flags": [],
                "generation_diagnostics": {},
            }
        )
    assert caught.value.code == "mfi_report_delivery_contract_failed"
    assert caught.value.status_code == 500


def test_llm_error_public_contract_includes_active_task_or_batch() -> None:
    error = LLMCallError(
        failure_code="llm_transport_error",
        call_id="llm-1",
        node="red_team",
        operation="mfi.red_team_review.v5",
        stage="transport",
        batch_id="batch-1",
    )
    assert error.to_public_dict()["batch_id"] == "batch-1"
    assert error.to_public_dict()["call_id"] == "llm-1"


class _GranularWorkflowModel:
    def __init__(self) -> None:
        self.market_flag_emitted = False

    def bind(self, **_kwargs):
        return self

    @staticmethod
    def _claim(text: str, *, scope: str = "assessment") -> dict:
        return {
            "claim_id": "ignored-model-id",
            "text": text,
            "claim_kind": "finding",
            "metric_ids": [],
            "document_ids": [],
            "scope": scope,
            "polarity": "neutral",
        }

    def invoke(self, messages):
        prompt = str(messages[0].content)
        if "Repair exactly one field" in prompt:
            task = json.loads(
                prompt.split("TASK:\n", 1)[1].split("\n\nQA_FINDINGS:", 1)[0]
            )
            field = task["field_name"]
            scope = "market" if task["artifact_type"] == "market" else "assessment"
            claim = self._claim(f"Corrected {task['artifact_id']} {field}.", scope=scope)
            claim.pop("claim_id", None)
            claim["claim_kind"] = (
                "geographic_pattern"
                if field == "geographic_patterns"
                else "recommendation"
                if field in {"recommendations", "recommended_interventions"}
                else "finding"
            )
            return SimpleNamespace(content=json.dumps({"replacement": [claim]}))
        if "Red-Team this bounded" in prompt:
            package = json.loads(prompt.split("REVIEW_BATCH:\n", 1)[1])
            flag = None
            if not self.market_flag_emitted:
                row = next(
                    (
                        item
                        for item in package["claims"]
                        if item.get("a") == "market"
                        and item.get("f") == "priority_issues"
                    ),
                    None,
                )
                if row is not None:
                    self.market_flag_emitted = True
                    flag = {
                        "code": "market_wording_review",
                        "severity": "medium",
                        "artifact_type": "market",
                        "artifact_id": row["aid"],
                        "field_name": "priority_issues",
                        "claim_id": row["id"],
                        "message": "Make the market issue more specific.",
                        "recommendation": "Rewrite the cited field.",
                        "metric_ids": row.get("m", []),
                        "document_ids": row.get("d", []),
                        "repairable": True,
                    }
            return SimpleNamespace(content=json.dumps({"flags": [flag] if flag else []}))
        if "targeted MFI narrative" in prompt:
            return SimpleNamespace(
                content=json.dumps(
                    {
                        "priority_issues": [self._claim("LLM market issue.", scope="market")],
                        "recommended_interventions": [
                            self._claim("LLM market recommendation.", scope="market")
                        ],
                        "limitations": [],
                    }
                )
            )
        if "structured executive summary" in prompt:
            return SimpleNamespace(
                content=json.dumps(
                    {
                        "motivation": self._claim("LLM executive motivation."),
                        "key_findings": [self._claim("LLM executive finding.")],
                        "recommendations": [self._claim("LLM executive recommendation.")],
                        "limitations": [],
                    }
                )
            )
        return SimpleNamespace(
            content=json.dumps(
                {
                    "summary": self._claim("LLM dimension summary."),
                    "key_findings": [self._claim("LLM dimension finding.")],
                    "subdimension_analysis": [],
                    "geographic_patterns": [
                        self._claim("LLM geographic pattern.", scope="market")
                    ],
                    "data_limitations": [],
                    "recommendations": [self._claim("LLM dimension recommendation.")],
                }
            )
        )


def test_full_fake_graph_uses_granular_repairs_and_distributed_red_team(
    monkeypatch,
) -> None:
    loaded = build_loaded(SyntheticSpec(market_count=15, region_count=3))
    model = _GranularWorkflowModel()
    monkeypatch.setattr(graph, "get_model", lambda **_kwargs: model)
    monkeypatch.setattr(
        graph,
        "node_context_retrieval",
        lambda state: {
            "contextual_documents": [],
            "document_references": [],
            "seerist_documents": [],
            "reliefweb_documents": [],
            "context_status": state["context_status"],
            "current_node": "context_retrieval",
        },
    )
    monkeypatch.setattr(
        graph,
        "node_mfi_graph_designer",
        lambda _state: {"visualizations": {}, "current_node": "mfi_graph_designer"},
    )

    def controlled_validation(**kwargs):
        dimensions, markets, executive, context = canonicalize_narrative_identities(
            dimension_narratives=kwargs["dimension_narratives"],
            market_narratives=kwargs["market_narratives"],
            executive_narrative=kwargs["executive_narrative"],
            context_evidence=kwargs["context_evidence"],
        )
        flags = []
        for dimension, field in (
            ("Price", "recommendations"),
            ("Infrastructure", "geographic_patterns"),
        ):
            values = dimensions[dimension][field]
            if not values or not str(values[0]["text"]).startswith("Corrected"):
                flags.append(
                    {
                        "flag_id": f"repair-{dimension}-{field}",
                        "source": "deterministic",
                        "code": "controlled_repair",
                        "severity": "medium",
                        "artifact_type": "dimension",
                        "artifact_id": dimension,
                        "field_name": field,
                        "claim_id": values[0]["claim_id"] if values else None,
                        "message": "Controlled test repair.",
                        "repairable": True,
                    }
                )
        count = len(canonical_claim_index(dimensions, markets, executive, context))
        validation = {
            "status": "passed_with_warnings" if flags else "passed",
            "validated_claim_count": count,
            "verified_claim_count": count - len(flags),
            "unverified_claim_count": len(flags),
            "flags": flags,
        }
        return validation, dimensions, markets, executive, context, {"flags": flags}

    monkeypatch.setattr(graph, "validate_structured_narratives", controlled_validation)
    result = graph.run_mfi_report_generation(
        country=loaded["country"],
        data_collection_start=loaded["data_collection_start"],
        data_collection_end=loaded["data_collection_end"],
        markets=loaded["markets"],
        csv_data=loaded,
        release_control=MFIReleaseControl(
            analysis_version="2",
            enabled=True,
            configuration_status="configured",
        ),
    )
    diagnostics = result["generation_diagnostics"]
    assert result["correction_attempts"] == 2
    assert diagnostics["correction_tasks_total"] == 3
    assert diagnostics["correction_tasks_completed"] == 3
    assert diagnostics["red_team_batches_total"] > 1
    assert diagnostics["red_team_batches_completed"] == diagnostics["red_team_batches_total"]
    assert diagnostics["red_team_status"] == "completed"
    assert diagnostics["fallback_policy"] == "disabled_live"
    assert diagnostics["identity_fallback_artifacts"] == []
    assert diagnostics["claim_substitutions"] == []
    assert diagnostics["delivery_contract_status"] == "validated"
    assert result["qa_review"]["status"] == "passed"
    visible_text = "\n".join(block.get("text") or "" for block in result["report_blocks"])
    assert "LLM dimension summary." in visible_text
    blocks = resolve_mfi_report_blocks(result)
    assert build_docx_bytes_from_report_blocks(blocks, visualizations={}).startswith(b"PK")
