from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.services.mfi_drafter import graph
from app.services.mfi_drafter.claim_identity import context_token, dimension_claim_id
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
    correction_field_patch_contract,
    project_correction_transport,
    validate_field_patch_payload,
)
from app.services.mfi_drafter.schemas import MFIGenerationDiagnostics, MFIReleaseControl
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


def _semantic_contract_reviews() -> list[dict]:
    return [
        {
            "review_id": f"semantic-review-{section}",
            "section": section,
            "sequence": sequence,
            "claim_ids": [],
            "character_count": 17,
            "package": {
                "claims": [],
                "evidence_by_metric_id": [],
                "accepted_context": [],
                "cited_documents": [],
            },
        }
        for sequence, section in enumerate(
            ("overview", "dimensions", "markets"), start=1
        )
    ]


def _semantic_runtime() -> SimpleNamespace:
    return SimpleNamespace(mfi_red_team_timeout_seconds=600.0, max_retries=2)


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


def test_semantic_review_preflight_measures_exact_final_prompt_boundary(
    monkeypatch,
) -> None:
    reviews = _semantic_contract_reviews()
    sizes = {"overview": 400_000, "dimensions": 64, "markets": 128}
    monkeypatch.setattr(
        graph,
        "_semantic_review_prompt",
        lambda review: "x" * sizes[str(review["section"])],
    )
    prepared, rows = graph._prepare_semantic_review_prompts(
        reviews,
        timeout_seconds=600.0,
    )
    assert [row["prompt_character_count"] for row in rows] == [
        400_000,
        64,
        128,
    ]
    assert rows[0]["package_character_count"] == 17
    assert rows[0]["character_count"] == 400_000
    assert rows[0]["target_characters"] == 400_000
    assert prepared[0]["package"] == reviews[0]["package"]
    assert len(prepared[0]["prompt"]) == 400_000
    monkeypatch.setattr(
        graph, "build_semantic_review_packages", lambda **_kwargs: reviews
    )
    monkeypatch.setattr(graph, "llm_runtime_config", _semantic_runtime)
    monkeypatch.setattr(graph, "get_model", lambda **_kwargs: object())
    monkeypatch.setattr(
        graph,
        "get_trace_session",
        lambda **_kwargs: SimpleNamespace(snapshot=lambda: {"calls": []}),
    )
    invocation_count = 0

    def invoke(**_kwargs):
        nonlocal invocation_count
        invocation_count += 1
        return SimpleNamespace(call_id=f"review-{invocation_count}", value=[]), 1

    monkeypatch.setattr(graph, "_invoke_json_with_one_normalization", invoke)
    result = graph.node_semantic_review(
        {
            "run_id": "mfi-boundary",
            "generation_diagnostics": {},
            "llm_diagnostics": {},
            "deterministic_flags": [],
            "correction_history": [],
        }
    )
    assert invocation_count == 3
    assert result["generation_diagnostics"]["semantic_reviews_completed"] == 3
    assert result["generation_diagnostics"]["red_team_package_within_target"] is True


def test_semantic_review_preflight_rejects_400001_before_any_llm_call(
    monkeypatch,
) -> None:
    reviews = _semantic_contract_reviews()
    sizes = {"overview": 100, "dimensions": 200, "markets": 400_001}
    monkeypatch.setattr(
        graph, "build_semantic_review_packages", lambda **_kwargs: reviews
    )
    monkeypatch.setattr(
        graph,
        "_semantic_review_prompt",
        lambda review: "x" * sizes[str(review["section"])],
    )
    monkeypatch.setattr(graph, "llm_runtime_config", _semantic_runtime)
    monkeypatch.setattr(
        graph,
        "get_model",
        lambda **_kwargs: pytest.fail("preflight must run before model creation"),
    )

    with pytest.raises(MFIGenerationBlockedError) as caught:
        graph.node_semantic_review(
            {
                "run_id": "mfi-preflight",
                "generation_diagnostics": {},
                "llm_diagnostics": {},
            }
        )

    error = caught.value
    assert error.code == "mfi_semantic_review_contract_failed"
    assert error.batch_kind == "markets"
    assert error.character_count == 400_001
    assert error.target_characters == 400_000
    assert [row["status"] for row in error.batch_diagnostics] == [
        "pending",
        "pending",
        "failed",
    ]
    diagnostics = graph.reconcile_generation_diagnostics_for_blocked_failure(
        {"generation_diagnostics": {}, "llm_diagnostics": {"calls": []}}, error
    )
    assert diagnostics["semantic_reviews_total"] == 3
    assert diagnostics["semantic_reviews_completed"] == 0
    assert diagnostics["semantic_reviews_failed"] == 1
    assert diagnostics["red_team_status"] == "failed"
    assert diagnostics["red_team_package_target_characters"] == 400_000
    assert diagnostics["failed_red_team_batch_kind"] == "markets"
    assert diagnostics["failed_red_team_character_count"] == 400_001


def test_semantic_review_partial_llm_failure_preserves_completed_rows(
    monkeypatch,
) -> None:
    reviews = _semantic_contract_reviews()
    monkeypatch.setattr(
        graph, "build_semantic_review_packages", lambda **_kwargs: reviews
    )
    monkeypatch.setattr(graph, "_semantic_review_prompt", lambda _review: "ok")
    monkeypatch.setattr(graph, "llm_runtime_config", _semantic_runtime)
    monkeypatch.setattr(graph, "get_model", lambda **_kwargs: object())
    monkeypatch.setattr(
        graph,
        "get_trace_session",
        lambda **_kwargs: SimpleNamespace(snapshot=lambda: {"calls": []}),
    )
    invocations = 0

    def invoke(**kwargs):
        nonlocal invocations
        invocations += 1
        if invocations == 1:
            return SimpleNamespace(call_id="llm-overview", value=[]), 1
        raise LLMCallError(
            failure_code="llm_transport_error",
            call_id="llm-dimensions",
            node="semantic_review",
            operation=str(kwargs["operation"]),
            stage="transport",
            batch_id=str(kwargs["batch_id"]),
        )

    monkeypatch.setattr(graph, "_invoke_json_with_one_normalization", invoke)

    with pytest.raises(LLMCallError) as caught:
        graph.node_semantic_review(
            {
                "run_id": "mfi-partial-review",
                "generation_diagnostics": {},
                "llm_diagnostics": {},
            }
        )

    diagnostics = graph.reconcile_generation_diagnostics_for_llm_failure(
        {"generation_diagnostics": {}, "llm_diagnostics": {"calls": []}},
        caught.value,
    )
    assert diagnostics["semantic_reviews_total"] == 3
    assert diagnostics["semantic_reviews_completed"] == 1
    assert diagnostics["semantic_reviews_failed"] == 1
    assert [row["status"] for row in diagnostics["semantic_reviews"]] == [
        "completed",
        "failed",
        "pending",
    ]
    assert diagnostics["semantic_reviews"][0]["call_id"] == "llm-overview"
    assert diagnostics["semantic_reviews"][1]["call_id"] == "llm-dimensions"


@pytest.mark.parametrize(
    ("prompt_size", "should_fail"),
    [(400_000, False), (400_001, True)],
)
def test_corrected_claim_verification_uses_exact_prompt_boundary(
    monkeypatch, prompt_size: int, should_fail: bool
) -> None:
    review = {
        "review_id": "corrected-claims-verification",
        "section": "corrected_claims",
        "claim_ids": [],
        "character_count": 23,
        "package": {
            "claims": [],
            "evidence_by_metric_id": [],
            "accepted_context": [],
            "cited_documents": [],
        },
    }
    monkeypatch.setattr(
        graph, "build_corrected_claim_verification_package", lambda **_kwargs: review
    )
    monkeypatch.setattr(
        graph,
        "_corrected_claim_verification_prompt",
        lambda _review: "x" * prompt_size,
    )
    monkeypatch.setattr(graph, "llm_runtime_config", _semantic_runtime)
    monkeypatch.setattr(graph, "get_model", lambda **_kwargs: object())
    monkeypatch.setattr(
        graph,
        "get_trace_session",
        lambda **_kwargs: SimpleNamespace(snapshot=lambda: {"calls": []}),
    )
    monkeypatch.setattr(
        graph,
        "_invoke_json_with_one_normalization",
        lambda **_kwargs: (SimpleNamespace(call_id="verification", value=[]), 1),
    )
    state = {
        "run_id": "mfi-corrected-boundary",
        "correction_targets": [
            {
                "task_id": "dimension-price-summary",
                "artifact_type": "dimension",
                "artifact_id": "Price",
                "field_name": "summary",
            }
        ],
        "generation_diagnostics": {},
        "llm_diagnostics": {},
        "red_team_flags": [],
        "deterministic_flags": [],
        "correction_history": [],
    }
    if should_fail:
        with pytest.raises(MFIGenerationBlockedError) as caught:
            graph.node_corrected_claim_verification(state)
        assert caught.value.character_count == 400_001
        assert caught.value.target_characters == 400_000
        assert caught.value.batch_diagnostics[0][
            "package_character_count"
        ] == 23
    else:
        result = graph.node_corrected_claim_verification(state)
        diagnostics = result["generation_diagnostics"]
        assert diagnostics[
            "corrected_claim_verification_prompt_character_count"
        ] == 400_000
        assert diagnostics["corrected_claim_verification_max_characters"] == 400_000


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


def test_patch_contract_rejects_complete_artifact() -> None:
    task = build_sequential_correction_tasks(
        [_flag("geo")],
        attempt_number=1,
        assessment_profile={"priority_market_names": []},
    )[0]
    with pytest.raises(Exception):
        validate_field_patch_payload(
            {
                "replacement": [{**_claim("Pattern.", scope="market")}],
                "summary": _claim("Unrequested field."),
            },
            task=task,
        )


def test_patch_contract_ignores_only_application_owned_claim_metadata() -> None:
    original = _dimension_narrative("Infrastructure")
    task = build_sequential_correction_tasks(
        [
            _flag(
                "recommendation",
                artifact_id="Infrastructure",
                field_name="recommendations",
                claim_id="dimension.infrastructure.recommendation.1",
            )
        ],
        attempt_number=1,
        assessment_profile={"priority_market_names": []},
    )[0]
    ignored: list[str] = []
    payload = {
        "replacement": [
            {
                **_claim("Review the cited Infrastructure evidence."),
                "claim_kind": "recommendation",
                "claim_id": "model-owned-id",
                "validation_status": "verified",
                "validation_flags": [],
                "validation_flag_ids": [],
                "substituted": False,
            }
        ]
    }

    replacement = validate_field_patch_payload(
        payload,
        task=task,
        ignored_metadata_fields=ignored,
    )

    assert len(ignored) == 5
    assert {item.rsplit(".", 1)[-1] for item in ignored} == {
        "claim_id",
        "validation_status",
        "validation_flags",
        "validation_flag_ids",
        "substituted",
    }
    assert not set(payload["replacement"][0]) <= set(replacement[0])
    assert all(
        key not in replacement[0]
        for key in (
            "claim_id",
            "validation_status",
            "validation_flags",
            "validation_flag_ids",
            "substituted",
        )
    )
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
    claim = merged["dimension_narratives"]["Infrastructure"]["recommendations"][0]
    assert claim["claim_id"] == dimension_claim_id(
        "Infrastructure", "recommendation", 1
    )
    assert "validation_status" not in claim


def test_patch_contract_keeps_unknown_extras_fail_closed() -> None:
    task = build_sequential_correction_tasks(
        [_flag("geo")],
        attempt_number=1,
        assessment_profile={"priority_market_names": []},
    )[0]
    with pytest.raises(Exception):
        validate_field_patch_payload(
            {
                "replacement": [
                    {
                        **_claim("Pattern.", scope="market"),
                        "unknown_model_field": "must fail",
                    }
                ]
            },
            task=task,
        )


def test_patch_transport_projection_and_contract_are_closed_and_bounded() -> None:
    artifact = _dimension_narrative("Infrastructure")
    artifact["recommendations"] = [
        {
            **_claim("Current recommendation."),
            "claim_id": "canonical-id",
            "validation_status": "unverified",
            "validation_flags": ["scope_mismatch"],
            "validation_flag_ids": ["flag-1"],
            "substituted": False,
        }
    ]
    projected = project_correction_transport(artifact)
    serialized = json.dumps(projected)
    assert "claim_id" not in serialized
    assert "validation_status" not in serialized
    assert "validation_flags" not in serialized
    assert "validation_flag_ids" not in serialized
    assert "substituted" not in serialized

    task = build_sequential_correction_tasks(
        [
            _flag(
                "recommendation",
                artifact_id="Infrastructure",
                field_name="recommendations",
            )
        ],
        attempt_number=1,
        assessment_profile={"priority_market_names": []},
    )[0]
    contract = correction_field_patch_contract(
        task=task,
        artifact=artifact,
        assessment_profile={"priority_dimension_names": ["Infrastructure"]},
    )
    assert contract["replacement_shape"] == "array of claim objects"
    assert contract["maximum_items"] == 3
    assert list(contract["example"]) == ["replacement"]
    assert contract["allowed_claim_fields"] == [
        "text",
        "claim_kind",
        "metric_ids",
        "document_ids",
        "scope",
        "polarity",
    ]


def test_subdimension_patch_strips_nested_application_metadata_only() -> None:
    task = build_sequential_correction_tasks(
        [
            _flag(
                "subdimension",
                artifact_id="Infrastructure",
                field_name="subdimension_analysis",
                claim_id=None,
            )
        ],
        attempt_number=1,
        assessment_profile={"priority_market_names": []},
    )[0]
    ignored: list[str] = []
    replacement = validate_field_patch_payload(
        {
            "replacement": [
                {
                    "name": "Condition",
                    "subsection_metric_id": "infrastructure.condition",
                    "score_0_10": 6.5,
                    "interpretation": {
                        **_claim("Condition evidence should guide review."),
                        "claim_id": "model-subdimension-id",
                        "validation_status": "verified",
                    },
                    "driver_metric_ids": [],
                }
            ]
        },
        task=task,
        ignored_metadata_fields=ignored,
    )

    assert len(ignored) == 2
    assert replacement[0]["interpretation"]["claim_kind"] == "finding"
    assert "claim_id" not in replacement[0]["interpretation"]
    assert "validation_status" not in replacement[0]["interpretation"]


@pytest.mark.parametrize(
    ("artifact_type", "artifact_id", "field_name", "is_list"),
    [
        ("dimension", "Infrastructure", "summary", False),
        ("market", "Market A", "priority_issues", True),
        ("executive_summary", "executive_summary", "motivation", False),
    ],
)
def test_known_metadata_is_ignored_for_each_claim_patch_shape(
    artifact_type: str,
    artifact_id: str,
    field_name: str,
    is_list: bool,
) -> None:
    task = build_sequential_correction_tasks(
        [
            _flag(
                "field",
                artifact_type=artifact_type,
                artifact_id=artifact_id,
                field_name=field_name,
            )
        ],
        attempt_number=1,
        assessment_profile={"priority_market_names": ["Market A"]},
    )[0]
    claim = {
        **_claim("Corrected field."),
        "claim_id": "model-id",
        "validation_status": "verified",
    }
    ignored: list[str] = []
    replacement = validate_field_patch_payload(
        {"replacement": [claim] if is_list else claim},
        task=task,
        ignored_metadata_fields=ignored,
    )
    parsed_claim = replacement[0] if is_list else replacement
    assert len(ignored) == 2
    assert "claim_id" not in parsed_claim
    assert "validation_status" not in parsed_claim


def test_patch_contract_rejects_missing_required_claim_fields() -> None:
    task = build_sequential_correction_tasks(
        [_flag("geo")],
        attempt_number=1,
        assessment_profile={"priority_market_names": []},
    )[0]
    with pytest.raises(Exception):
        validate_field_patch_payload(
            {
                "replacement": [
                    {
                        "text": "Missing citations and semantic fields.",
                        "metric_ids": [],
                    }
                ]
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


def _gaza_sized_review_source(*, text_size: int = 400) -> dict:
    """Return 174 realistic review rows with the maximum selected-market set."""
    rows: list[dict] = []
    evidence: dict[str, dict] = {}

    def add_rows(
        artifact_type: str,
        artifact_id: str,
        field_name: str,
        count: int,
    ) -> None:
        for position in range(1, count + 1):
            claim_id = (
                f"{artifact_type}.{context_token(artifact_id)}."
                f"{field_name}.{position}"
            )
            metric_id = f"metric.{len(evidence) + 1:03d}"
            rows.append(
                {
                    "a": artifact_type,
                    "aid": artifact_id,
                    "f": field_name,
                    "p": position,
                    "id": claim_id,
                    "text": f"{artifact_id} {field_name} {position} "
                    + ("x" * text_size),
                    "s": "market" if artifact_type == "market" else "assessment",
                    "o": "neutral",
                    "m": [metric_id],
                    "d": [],
                }
            )
            evidence[metric_id] = {
                "label": f"Evidence {metric_id}",
                "formatted_value": f"{len(evidence) + 1}.00",
                "unit": "score",
                "scope": "assessed markets",
            }

    priority_dimensions = ["Service", "Price", "Infrastructure", "Food Quality"]
    other_dimensions = [
        "Assortment",
        "Availability",
        "Resilience",
        "Competition",
        "Access and Protection",
    ]
    for dimension in priority_dimensions:
        for field_name, count in (
            ("summary", 1),
            ("key_findings", 3),
            ("subdimension_analysis", 2),
            ("geographic_patterns", 2),
            ("data_limitations", 1),
            ("recommendations", 3),
        ):
            add_rows("dimension", dimension, field_name, count)
    for dimension in other_dimensions:
        add_rows("dimension", dimension, "summary", 1)

    market_names = [
        "Gaza City",
        "Rafah",
        "Khan Younis",
        "Deir al-Balah",
        "Jabalia",
        "Beit Lahia",
        "Beit Hanoun",
        "Nuseirat",
        "Bureij",
        "Maghazi",
        "Al Qarara",
        "Abasan al-Kabira",
        "Az Zawayda",
        "Shuja'iyya",
        "Al Mawasi",
    ]
    for market_name in market_names:
        add_rows("market", market_name, "priority_issues", 3)
        add_rows("market", market_name, "recommended_interventions", 3)
        add_rows("market", market_name, "limitations", 1)

    for field_name, count in (
        ("motivation", 1),
        ("key_findings", 4),
        ("recommendations", 3),
        ("limitations", 3),
        ("scope_statement", 1),
    ):
        add_rows("executive_summary", "executive_summary", field_name, count)

    context_statements = [
        {
            "id": f"context.statement.{index}",
            "text": f"Context statement {index} " + ("c" * 80),
            "class": "corroborating",
            "docs": [f"doc-{index}"],
        }
        for index in range(1, 5)
    ]
    assert len(rows) + len(context_statements) == 174
    return {
        "contract_version": "mfi-red-team-input-v5",
        "claims": rows,
        "context_statements": context_statements,
        "priority_context": {
            "dimensions": [
                {"id": name, "token": name.casefold().replace(" ", "_"), "rank": rank}
                for rank, name in enumerate(priority_dimensions, start=1)
            ],
            "markets": [
                {
                    "id": name,
                    "token": context_token(name),
                    "rank": rank,
                    "order": rank,
                    "weak": priority_dimensions[:3],
                }
                for rank, name in enumerate(market_names, start=1)
            ],
        },
        "evidence_by_metric_id": evidence,
        "cited_documents": [
            {"id": f"doc-{index}", "title": f"Document {index}"}
            for index in range(1, 5)
        ],
        "limitations": [],
        "deterministic_flags": [],
        "prohibitions": ["Do not infer causality."],
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


def test_gaza_sized_review_is_sharded_by_priority_dimension_and_market() -> None:
    source = _gaza_sized_review_source()
    executive_ids = {
        str(row["id"])
        for row in source["claims"]
        if row["a"] == "executive_summary"
    }
    old_market_text_size = sum(
        len(str(row["text"]))
        for row in source["claims"]
        if row["a"] in {"market", "executive_summary"}
    )
    assert old_market_text_size > 45_000

    batches = build_red_team_batches(source)
    dimensions = [item for item in batches if item.batch_kind == "dimension_coherence"]
    markets = [item for item in batches if item.batch_kind == "market_coherence"]
    local = [item for item in batches if item.batch_kind == "local"]

    assert len(dimensions) == 4
    assert len(markets) == 15
    assert all(item.character_count <= 45_000 for item in batches)
    assert [item.shard_key for item in dimensions] == [
        "dimension:service",
        "dimension:price",
        "dimension:infrastructure",
        "dimension:food_quality",
    ]
    assert [item.shard_key for item in markets] == [
        f"market:{item['token']}" for item in source["priority_context"]["markets"]
    ]
    local_claim_ids = [claim_id for item in local for claim_id in item.claim_ids]
    expected_local_ids = [
        *[str(item["id"]) for item in source["context_statements"]],
        *[str(item["id"]) for item in source["claims"]],
    ]
    assert local_claim_ids == expected_local_ids
    assert len(local_claim_ids) == len(set(local_claim_ids)) == 174
    for batch in [*dimensions, *markets]:
        assert executive_ids <= set(batch.claim_ids)
        cited = {
            metric_id
            for row in batch.package["claims"]
            for metric_id in row.get("m", [])
        }
        assert set(batch.package["evidence_by_metric_id"]) == cited
    for batch in dimensions:
        assert len(batch.package["priority_context"]["dimensions"]) == 4
        assert batch.package["priority_context"]["markets"] == []
        scoped_rows = [row for row in batch.package["claims"] if row["a"] == "dimension"]
        assert {row["aid"] for row in scoped_rows} == {
            batch.package["coherence_scope"]["artifact_id"]
        }
        assert {row["f"] for row in scoped_rows} == {
            "summary",
            "key_findings",
            "subdimension_analysis",
            "geographic_patterns",
            "data_limitations",
            "recommendations",
        }
    for batch in markets:
        market_context = batch.package["priority_context"]["markets"]
        assert len(market_context) == 15
        assert sum("weak" in item for item in market_context) == 1
        assert len(batch.package["priority_context"]["dimensions"]) == 3
        scoped_rows = [row for row in batch.package["claims"] if row["a"] == "market"]
        assert {row["aid"] for row in scoped_rows} == {
            batch.package["coherence_scope"]["artifact_id"]
        }
        assert {row["f"] for row in scoped_rows} == {
            "priority_issues",
            "recommended_interventions",
            "limitations",
        }


def test_coherence_batch_ids_are_stable_and_changes_are_artifact_scoped() -> None:
    source = _gaza_sized_review_source(text_size=80)
    first = build_red_team_batches(source)
    first_coherence = {
        item.shard_key: item
        for item in first
        if item.batch_kind != "local"
    }

    changed_market = deepcopy(source)
    changed_market_row = next(
        row
        for row in changed_market["claims"]
        if row["a"] == "market" and row["aid"] == "Rafah"
    )
    changed_market_row["text"] = "Corrected Rafah issue."
    second = build_red_team_batches(changed_market)
    second_coherence = {
        item.shard_key: item
        for item in second
        if item.batch_kind != "local"
    }
    assert set(first_coherence) == set(second_coherence)
    changed_shards = {
        key
        for key in first_coherence
        if first_coherence[key].signature != second_coherence[key].signature
    }
    assert changed_shards == {f"market:{context_token('Rafah')}"}
    assert all(
        first_coherence[key].batch_id == second_coherence[key].batch_id
        for key in first_coherence
    )

    changed_executive = deepcopy(source)
    executive_row = next(
        row for row in changed_executive["claims"] if row["a"] == "executive_summary"
    )
    executive_row["text"] = "Corrected executive summary."
    third = build_red_team_batches(changed_executive)
    third_coherence = {
        item.shard_key: item
        for item in third
        if item.batch_kind != "local"
    }
    assert all(
        first_coherence[key].signature != third_coherence[key].signature
        for key in first_coherence
    )
    assert all(
        first_coherence[key].batch_id == third_coherence[key].batch_id
        for key in first_coherence
    )


def test_coherence_ids_do_not_depend_on_preceding_local_batches() -> None:
    source = _gaza_sized_review_source(text_size=80)
    first = build_red_team_batches(source, target_characters=20_000)
    changed = deepcopy(source)
    changed["context_statements"].insert(
        0,
        {
            "id": "context.statement.0",
            "text": "Earlier context.",
            "class": "corroborating",
            "docs": [],
        },
    )
    second = build_red_team_batches(changed, target_characters=20_000)
    first_coherence = {
        item.shard_key: (item.batch_id, item.signature)
        for item in first
        if item.batch_kind != "local"
    }
    second_coherence = {
        item.shard_key: (item.batch_id, item.signature)
        for item in second
        if item.batch_kind != "local"
    }
    assert first_coherence == second_coherence


def test_oversized_coherence_shard_exposes_precise_contract_diagnostics() -> None:
    source = _review_source()
    source["priority_context"]["markets"] = [
        {
            "id": "Dense Market",
            "token": context_token("Dense Market"),
            "rank": 1,
            "order": 1,
            "weak": ["Price"],
        }
    ]
    source["claims"].extend(
        [
            {
                "a": "market",
                "aid": "Dense Market",
                "f": field_name,
                "p": 1,
                "id": f"market.dense.{field_name}.1",
                "text": "m" * 1_100,
                "s": "market",
                "o": "neutral",
                "m": [],
                "d": [],
            }
            for field_name in ("priority_issues", "recommended_interventions")
        ]
    )
    source["claims"][2]["text"] = "e" * 1_100
    with pytest.raises(MFIGenerationBlockedError) as caught:
        build_red_team_batches(source, target_characters=3_000)
    error = caught.value
    assert error.code == "mfi_red_team_batch_contract_failed"
    assert error.batch_kind == "market_coherence"
    assert error.shard_key == f"market:{context_token('Dense Market')}"
    assert error.character_count > 3_000
    assert error.target_characters == 3_000
    assert error.to_public_dict()["shard_key"] == error.shard_key
    assert error.to_public_dict()["character_count"] == error.character_count
    assert error.to_public_dict()["target_characters"] == 3_000
    assert error.batch_diagnostics
    failed = [item for item in error.batch_diagnostics if item["status"] == "failed"]
    assert len(failed) == 1
    assert failed[0]["failure_code"] == error.code
    diagnostics = graph.reconcile_generation_diagnostics_for_blocked_failure(
        {"generation_diagnostics": {}}, error
    )
    assert diagnostics["red_team_status"] == "failed"
    assert diagnostics["red_team_contract_version"] == "mfi-red-team-batches-v2"
    assert diagnostics["red_team_review_operation"] == "mfi.red_team_review.v6"
    assert diagnostics["failed_red_team_batch"] == error.batch_id
    assert diagnostics["failed_red_team_shard_key"] == error.shard_key
    assert diagnostics["failed_red_team_character_count"] == error.character_count
    assert diagnostics["red_team_package_target_characters"] == 3_000
    assert diagnostics["red_team_batches_failed"] == 1
    assert diagnostics["red_team_package_within_target"] is False
    MFIGenerationDiagnostics.model_validate(diagnostics)


def test_coherence_character_limit_accepts_45000_and_rejects_45001() -> None:
    source = _gaza_sized_review_source(text_size=20)
    market_name = source["priority_context"]["markets"][0]["id"]
    shard_key = f"market:{context_token(market_name)}"
    initial = build_red_team_batches(source)
    initial_batch = next(item for item in initial if item.shard_key == shard_key)
    padding = 45_000 - initial_batch.character_count
    assert padding > 0
    target_row = next(
        row
        for row in source["claims"]
        if row["a"] == "market" and row["aid"] == market_name
    )
    target_row["text"] += "x" * padding
    boundary = build_red_team_batches(source)
    boundary_batch = next(item for item in boundary if item.shard_key == shard_key)
    assert boundary_batch.character_count == 45_000

    target_row["text"] += "x"
    with pytest.raises(MFIGenerationBlockedError) as caught:
        build_red_team_batches(source)
    assert caught.value.shard_key == shard_key
    assert caught.value.character_count == 45_001


def test_repeated_coherence_findings_merge_by_semantic_identity() -> None:
    first = graph._validate_red_team_response(
        {
            "flags": [
                {
                    "code": "executive_mismatch",
                    "severity": "medium",
                    "artifact_type": "executive_summary",
                    "artifact_id": "executive_summary",
                    "field_name": "key_findings",
                    "claim_id": "executive.key_findings.1",
                    "message": "First canonical message.",
                    "recommendation": "Repair it.",
                    "metric_ids": ["metric.1"],
                    "document_ids": [],
                    "repairable": True,
                }
            ]
        }
    )[0]
    second = graph._validate_red_team_response(
        {
            "flags": [
                {
                    "code": "executive_mismatch",
                    "severity": "high",
                    "artifact_type": "executive_summary",
                    "artifact_id": "executive_summary",
                    "field_name": "key_findings",
                    "claim_id": "executive.key_findings.1",
                    "message": "Later wording from another shard.",
                    "recommendation": "Repair it.",
                    "metric_ids": ["metric.2"],
                    "document_ids": ["doc-1"],
                    "repairable": True,
                }
            ]
        }
    )[0]
    first.update(
        review_batch_id="dimension-batch",
        review_batch_ids=["dimension-batch"],
    )
    second.update(
        review_batch_id="market-batch",
        review_batch_ids=["market-batch"],
    )
    result = graph.node_finalize_red_team(
        {
            "generation_diagnostics": {
                "red_team_batches": [
                    {
                        "batch_id": "dimension-batch",
                        "batch_kind": "dimension_coherence",
                        "sequence": 1,
                        "character_count": 100,
                        "claim_count": 1,
                        "status": "completed",
                        "flag_count": 1,
                    },
                    {
                        "batch_id": "market-batch",
                        "batch_kind": "market_coherence",
                        "sequence": 2,
                        "character_count": 100,
                        "claim_count": 1,
                        "status": "completed",
                        "flag_count": 1,
                    },
                ]
            },
            "red_team_batch_flags": {
                "dimension-batch": [first],
                "market-batch": [second],
            },
            "deterministic_flags": [],
            "correction_attempts": 0,
            "correction_history": [],
        }
    )
    assert len(result["red_team_flags"]) == 1
    merged = result["red_team_flags"][0]
    assert merged["severity"] == "high"
    assert merged["message"] == "First canonical message."
    assert merged["metric_ids"] == ["metric.1", "metric.2"]
    assert merged["document_ids"] == ["doc-1"]
    assert merged["review_batch_ids"] == ["dimension-batch", "market-batch"]


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


def test_only_high_final_qa_and_delivery_are_fail_closed() -> None:
    state = {
        "deterministic_flags": [_flag("material", severity="high")],
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
        operation="mfi.red_team_review.v6",
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
        if "Correct all supplied MFI narrative fields" in prompt:
            package = json.loads(
                prompt.split("CORRECTION_PACKAGE:\n", 1)[1].split(
                    "\n\nReturn exactly:", 1
                )[0]
            )
            patches = []
            for target in package["targets"]:
                field = target["field_name"]
                scope = "market" if target["artifact_type"] == "market" else "assessment"
                claim = self._claim(
                    f"Corrected {target['artifact_id']} {field}.", scope=scope
                )
                claim.pop("claim_id", None)
                claim["claim_kind"] = (
                    "geographic_pattern"
                    if field == "geographic_patterns"
                    else "recommendation"
                    if field in {"recommendations", "recommended_interventions"}
                    else "finding"
                )
                patches.append(
                    {"target_id": target["target_id"], "replacement": [claim]}
                )
            return SimpleNamespace(content=json.dumps({"patches": patches}))
        if "Check only the corrected MFI claims" in prompt:
            return SimpleNamespace(content='{"flags": []}')
        if "Check one section of an MFI report" in prompt:
            package = json.loads(
                prompt.split("REVIEW_PACKAGE:\n", 1)[1].split(
                    "\n\nReturn exactly:", 1
                )[0]
            )
            flag = None
            if not self.market_flag_emitted:
                row = next(
                    (
                        item
                        for item in package["claims"]
                        if item.get("artifact_type") == "market"
                        and item.get("field_name") == "priority_issues"
                    ),
                    None,
                )
                if row is not None:
                    self.market_flag_emitted = True
                    flag = {
                        "issue_type": "context_interpretation_problem",
                        "severity": "medium",
                        "claim_id": row["claim_id"],
                        "message": "Make the market issue more specific.",
                        "recommendation": "Rewrite the cited field.",
                    }
            return SimpleNamespace(content=json.dumps({"flags": [flag] if flag else []}))
        if "targeted MFI narrative" in prompt:
            requested = json.loads(
                prompt.split("REQUESTED_MARKETS_IN_REQUIRED_ORDER:\n", 1)[1].split(
                    "\n\nMARKET_PROFILES:", 1
                )[0]
            )
            narrative = {
                "priority_issues": [self._claim("LLM market issue.", scope="market")],
                "recommended_interventions": [
                    self._claim("LLM market recommendation.", scope="market")
                ],
                "limitations": [],
            }
            return SimpleNamespace(
                content=json.dumps(
                    {
                        "markets": [
                            {"market_name": name, "narrative": narrative}
                            for name in requested
                        ]
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
        requested = json.loads(
            prompt.split("REQUESTED_DIMENSIONS_IN_REQUIRED_ORDER:\n", 1)[1].split(
                "\n\nMETHODOLOGY_DESCRIPTIONS:", 1
            )[0]
        )
        narrative = {
            "summary": self._claim("LLM dimension summary."),
            "key_findings": [self._claim("LLM dimension finding.")],
            "subdimension_analysis": [],
            "geographic_patterns": [
                self._claim("LLM geographic pattern.", scope="market")
            ],
            "data_limitations": [],
            "recommendations": [self._claim("LLM dimension recommendation.")],
        }
        return SimpleNamespace(
            content=json.dumps(
                {
                    "dimensions": [
                        {"dimension": name, "narrative": narrative}
                        for name in requested
                    ]
                }
            )
        )


def test_full_fake_graph_uses_one_correction_and_three_semantic_reviews(
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

    monkeypatch.setattr(
        graph, "validate_evidence_bound_narratives", controlled_validation
    )
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
    assert result["correction_attempts"] == 1
    assert diagnostics["correction_tasks_total"] == 3
    assert diagnostics["correction_tasks_completed"] == 3
    assert diagnostics["correction_attempts"] == 1
    assert diagnostics["consolidated_correction_llm_calls"] == 1
    assert diagnostics["consolidated_correction_prompt_character_count"] <= (
        diagnostics["consolidated_correction_prompt_max_characters"]
    )
    assert diagnostics["semantic_reviews_total"] == 3
    assert diagnostics["semantic_reviews_completed"] == 3
    assert diagnostics["semantic_reviews_failed"] == 0
    assert diagnostics["corrected_claim_verification_status"] == "completed"
    operations = {
        str(item.get("operation"))
        for item in result["llm_diagnostics"]["calls"]
    }
    assert {
        "mfi.semantic_review.overview.v2",
        "mfi.semantic_review.dimensions.v2",
        "mfi.semantic_review.markets.v2",
        "mfi.corrected_claim_verification.v2",
    } <= operations
    assert result["llm_calls"] <= 15
    assert diagnostics["red_team_status"] == "completed"
    assert diagnostics["fallback_policy"] == "disabled_live"
    assert diagnostics["identity_fallback_artifacts"] == []
    assert diagnostics["claim_substitutions"] == []
    assert diagnostics["delivery_contract_status"] == "validated"
    assert result["qa_review"]["status"] == "passed"
    market_calls = [
        item
        for item in result["llm_diagnostics"]["calls"]
        if item.get("operation") == "mfi.market_batch_drafting.v2"
    ]
    assert len(market_calls) == 3
    assert all(
        item["configured_timeout_seconds"] == 180.0 for item in market_calls
    )
    market_batch_rows = [
        item
        for item in diagnostics["draft_batches"]
        if item.get("batch_kind") == "selected_markets"
    ]
    assert len(market_batch_rows) == 3
    assert all(
        item["prompt_character_count"]
        <= diagnostics["market_draft_prompt_max_characters"]
        for item in market_batch_rows
    )
    visible_text = "\n".join(block.get("text") or "" for block in result["report_blocks"])
    assert "LLM dimension summary." in visible_text
    blocks = resolve_mfi_report_blocks(result)
    assert build_docx_bytes_from_report_blocks(blocks, visualizations={}).startswith(b"PK")


def test_oversized_consolidated_correction_fails_before_model_call(
    monkeypatch,
) -> None:
    target = {
        "task_id": "oversized-target",
        "artifact_type": "dimension",
        "artifact_id": "Price",
        "field_name": "recommendations",
        "flag_ids": ["flag-one"],
    }
    monkeypatch.setattr(
        graph,
        "build_consolidated_correction_targets",
        lambda *_args, **_kwargs: [target],
    )
    monkeypatch.setattr(
        graph,
        "consolidated_correction_prompt_payload",
        lambda **_kwargs: {"oversized": "x" * 600_000},
    )
    monkeypatch.setattr(
        graph,
        "get_model",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("Vertex must not be called for an oversized prompt")
        ),
    )
    state = {
        "deterministic_flags": [_flag("flag-one")],
        "red_team_flags": [],
        "generation_diagnostics": {},
        "assessment_profile": {},
        "dimension_narratives": {},
        "market_narratives": {},
        "executive_summary_narrative": {},
        "context_evidence": [],
        "claim_catalog": {},
        "contextual_documents": [],
    }

    with pytest.raises(MFIGenerationBlockedError) as caught:
        graph.node_consolidated_correction(state)
    assert caught.value.code == (
        "mfi_consolidated_correction_prompt_contract_failed"
    )
    assert caught.value.stage == "consolidated_correction"
    assert caught.value.character_count > caught.value.target_characters
    assert caught.value.target_characters == 600_000
    assert caught.value.target_count == 1
    diagnostics = graph.reconcile_generation_diagnostics_for_blocked_failure(
        state, caught.value
    )
    assert diagnostics["consolidated_correction_field_count"] == 1
    assert diagnostics["correction_tasks_total"] == 1
    assert diagnostics["consolidated_correction_llm_calls"] == 0
    assert diagnostics["consolidated_correction_prompt_max_characters"] == 600_000
    assert (
        MFIGenerationDiagnostics.model_validate(
            diagnostics
        ).consolidated_correction_prompt_max_characters
        == 600_000
    )
