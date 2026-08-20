from __future__ import annotations

import asyncio
import json
import re

import pytest

from app.services.mfi_drafter import graph, router
from app.services.mfi_drafter.claim_identity import (
    CLAIM_IDENTITY_AUTHORITY,
    CLAIM_IDENTITY_VERSION,
    canonical_claim_index,
    canonicalize_narrative_identities,
    context_statement_id,
    context_token,
    count_model_identifiers,
    dimension_claim_id,
    executive_claim_id,
    market_claim_id,
    subdimension_claim_id,
)
from app.services.mfi_drafter.narrative import (
    NARRATIVE_DENSITY_POLICY,
    apply_narrative_density_policy,
    apply_unresolved_claim_policy,
    fallback_executive_narrative,
    fallback_market_narrative,
    parse_context_evidence,
    parse_dimension_narrative,
    parse_executive_narrative,
    parse_market_narrative,
)
from app.services.mfi_drafter.schemas import MFIReleaseControl
from app.services.mfi_drafter.synthetic_fixtures import SyntheticSpec, build_loaded
from app.shared.async_runs import create_run, set_run_completed
from app.shared.docx_export import build_docx_bytes_from_report_blocks
from app.shared.report_blocks import ReportBlock, resolve_mfi_report_blocks


def _model_claim(text: str, *, scope: str = "assessment") -> dict:
    return {
        "claim_id": "stable id",
        "text": text,
        "claim_kind": "finding",
        "metric_ids": [],
        "document_ids": [],
        "scope": scope,
        "polarity": "neutral",
    }


def _dimension_profile(dimension: str = "Price", *, priority: bool = True) -> dict:
    return {
        "dimension": dimension,
        "is_priority": priority,
        "ledger_metric_ids": [],
        "statistics": {},
        "subsections": [],
        "drivers": [],
        "localized_patterns": {"markets_where_lowest": [], "ordered_markets": []},
    }


def _market_profile(name: str = "Café") -> dict:
    return {
        "market_name": name,
        "region": "Region A",
        "overall_mfi": 4.5,
        "score_rank": 1,
        "weak_dimensions": [{"dimension": "Price"}],
        "ledger_metric_ids": [],
    }


def test_identity_builders_cover_every_canonical_artifact_location() -> None:
    assert CLAIM_IDENTITY_AUTHORITY == "application"
    assert CLAIM_IDENTITY_VERSION == "mfi-claim-id-v1"
    assert dimension_claim_id("Food Quality", "summary", 1) == (
        "dimension.food_quality.summary.1"
    )
    assert subdimension_claim_id("Food Quality", 2) == (
        "dimension.food_quality.subdimension.2.interpretation"
    )
    assert executive_claim_id("scope_statement", 1) == (
        "executive.scope_statement.1"
    )
    assert context_statement_id(3) == "context.statement.3"
    assert market_claim_id("Café", "issue", 1).startswith(
        f"market.{context_token('Café')}.issue."
    )


def test_market_context_tokens_prevent_slug_case_and_unicode_collisions() -> None:
    names = ["Cafe", "Café", "CAFE", "cafe"]
    tokens = [context_token(name) for name in names]
    assert len(tokens) == len(set(tokens))
    assert all(token.startswith("cafe_") for token in tokens)


@pytest.mark.parametrize("market_name", ["Dangbo", "Café"])
def test_market_limitations_use_the_canonical_singular_identity_token(
    market_name: str,
) -> None:
    market = {
        "market_name": market_name,
        "region": "Region A",
        "overall_mfi": 4.5,
        "score_rank": 1,
        "weak_dimensions": ["Price"],
        "priority_issues": [_model_claim("Issue.", scope="market")],
        "recommended_interventions": [
            _model_claim("Intervention.", scope="market")
        ],
        "limitations": [
            {
                **_model_claim("Market-specific limitation.", scope="market"),
                "metric_ids": ["market.coverage"],
            }
        ],
        "modality_consideration": None,
    }
    _dimensions, markets, _executive, _context = (
        canonicalize_narrative_identities(
            dimension_narratives={},
            market_narratives={market_name: market},
            executive_narrative={},
        )
    )
    limitation_id = markets[market_name]["limitations"][0]["claim_id"]
    assert limitation_id == market_claim_id(market_name, "limitation", 1)
    assert ".limitations." not in limitation_id
    index = canonical_claim_index({}, markets, {}, [])
    assert limitation_id in index


def test_localized_market_identity_failure_preserves_other_llm_artifacts() -> None:
    profile = {
        "dimensions": [],
        "markets": [_market_profile("Dangbo"), _market_profile("Café")],
        "priority_market_names": ["Dangbo", "Café"],
        "priority_dimension_names": [],
        "limitations": [],
        "metric_ledger": {},
    }
    valid_market = {
        **fallback_market_narrative(_market_profile("Café")),
    }
    valid_market["priority_issues"][0]["text"] = "Preserve this LLM-authored issue."
    malformed_market = fallback_market_narrative(_market_profile("Dangbo"))
    malformed_market["priority_issues"] = ["malformed claim"]
    result = graph.node_deterministic_claim_validator(
        {
            "assessment_profile": profile,
            "claim_catalog": {},
            "contextual_documents": [],
            "context_evidence": [],
            "dimension_narratives": {},
            "market_narratives": {
                "Dangbo": malformed_market,
                "Café": valid_market,
            },
            "executive_summary_narrative": fallback_executive_narrative(profile),
            "generation_diagnostics": {},
        }
    )
    assert result["market_narratives"]["Café"]["priority_issues"][0][
        "text"
    ] == "Preserve this LLM-authored issue."
    assert result["generation_diagnostics"]["identity_fallback_artifacts"] == [
        "market:Dangbo"
    ]
    assert any(
        flag.get("code") == "claim_identity_contract_failure"
        for flag in result["deterministic_flags"]
    )


def test_all_initial_model_identifiers_are_ignored_and_globally_unique() -> None:
    dimension_payload = {
        "summary": _model_claim("Summary."),
        "key_findings": [_model_claim("Finding A."), _model_claim("Finding B.")],
        "subdimension_analysis": [
            {
                "name": "Component",
                "interpretation": _model_claim("Interpretation."),
                "driver_metric_ids": [],
            }
        ],
        "geographic_patterns": [_model_claim("Pattern.", scope="market")],
        "data_limitations": [_model_claim("Limitation.")],
        "recommendations": [_model_claim("Recommendation.")],
    }
    dimension = parse_dimension_narrative(
        dimension_payload,
        dimension_profile=_dimension_profile(),
        assessment_profile={"limitations": [], "metric_ledger": {}},
    )
    market = parse_market_narrative(
        {
            "priority_issues": [_model_claim("Issue.", scope="market")],
            "recommended_interventions": [
                _model_claim("Intervention.", scope="market")
            ],
            "limitations": [],
        },
        market_profile=_market_profile(),
    )
    executive = parse_executive_narrative(
        {
            "motivation": _model_claim("Motivation."),
            "key_findings": [_model_claim("Executive finding.")],
            "recommendations": [_model_claim("Executive recommendation.")],
            "limitations": [_model_claim("Executive limitation.")],
        },
        assessment_profile={
            "priority_dimension_names": ["Price"],
            "limitations": [],
            "metric_ledger": {},
        },
    )
    context, _warnings = parse_context_evidence(
        {
            "statements": [
                {
                    "statement_id": "stable id",
                    "text": "Document-backed statement.",
                    "classification": "corroborating",
                    "document_ids": ["doc-1"],
                }
            ]
        },
        documents=[{"doc_id": "doc-1"}],
    )

    dimensions, markets, executive, context = canonicalize_narrative_identities(
        dimension_narratives={"Price": dimension},
        market_narratives={"Café": market},
        executive_narrative=executive,
        context_evidence=context,
    )
    index = canonical_claim_index(dimensions, markets, executive, context)
    assert len(index) == len(set(index))
    assert "stable id" not in index
    assert context[0]["statement_id"] == "context.statement.1"
    assert count_model_identifiers(dimension_payload) > 1


def test_missing_empty_malformed_and_cross_artifact_model_ids_are_non_authoritative() -> None:
    observed: list[str] = []
    for supplied in (None, "", "stable id", {"bad": "shape"}, 42):
        raw = _model_claim("Summary.")
        if supplied is None:
            raw.pop("claim_id", None)
        else:
            raw["claim_id"] = supplied
        parsed = parse_dimension_narrative(
            {"summary": raw, "key_findings": [], "recommendations": []},
            dimension_profile=_dimension_profile(priority=False),
            assessment_profile={"limitations": [], "metric_ledger": {}},
        )
        observed.append(parsed["summary"]["claim_id"])
    assert observed == ["dimension.price.summary.1"] * len(observed)

    executive = parse_executive_narrative(
        {"motivation": _model_claim("Motivation.")},
        assessment_profile={
            "priority_dimension_names": [],
            "limitations": [],
            "metric_ledger": {},
        },
    )
    assert executive["motivation"]["claim_id"] == "executive.motivation.1"


def test_internal_identity_failure_becomes_nonrepairable_global_high_fallback() -> None:
    profile = {
        "dimensions": [_dimension_profile(priority=False)],
        "markets": [],
        "priority_market_names": [],
        "priority_dimension_names": [],
        "limitations": [],
        "metric_ledger": {},
    }
    result = graph.node_deterministic_claim_validator(
        {
            "assessment_profile": profile,
            "claim_catalog": {},
            "contextual_documents": [],
            "context_evidence": [],
            "dimension_narratives": {"Price": {"summary": "malformed"}},
            "market_narratives": {},
            "executive_summary_narrative": {},
            "generation_diagnostics": {},
        }
    )
    identity_flags = [
        flag
        for flag in result["deterministic_flags"]
        if flag.get("code") == "claim_identity_contract_failure"
    ]
    assert len(identity_flags) == 1
    assert identity_flags[0]["artifact_type"] == "global"
    assert identity_flags[0]["severity"] == "high"
    assert identity_flags[0]["repairable"] is False
    assert result["generation_diagnostics"]["identity_fallback_artifacts"]


def test_ids_stay_stable_after_density_and_recommendation_deduplication() -> None:
    payload = {
        "summary": _model_claim("Summary."),
        "key_findings": [_model_claim(f"Finding {index}.") for index in range(6)],
        "subdimension_analysis": [],
        "geographic_patterns": [],
        "data_limitations": [],
        "recommendations": [
            _model_claim("Review this evidence.") for _index in range(5)
        ],
    }
    parsed = parse_dimension_narrative(
        payload,
        dimension_profile=_dimension_profile(),
        assessment_profile={"limitations": [], "metric_ledger": {}},
    )
    first = apply_narrative_density_policy(
        dimension_narratives={"Price": parsed},
        market_narratives={},
        executive_narrative={},
        assessment_profile={"dimensions": [_dimension_profile()]},
    )
    canonical_first = canonicalize_narrative_identities(
        dimension_narratives=first[0],
        market_narratives=first[1],
        executive_narrative=first[2],
    )
    canonical_second = canonicalize_narrative_identities(
        dimension_narratives=canonical_first[0],
        market_narratives=canonical_first[1],
        executive_narrative=canonical_first[2],
    )
    assert canonical_first == canonical_second
    assert len(canonical_first[0]["Price"]["key_findings"]) == (
        NARRATIVE_DENSITY_POLICY.priority_findings
    )


def test_one_high_flag_substitutes_exactly_one_canonical_claim() -> None:
    parsed = parse_dimension_narrative(
        {
            "summary": _model_claim("Summary."),
            "key_findings": [_model_claim("First."), _model_claim("Second.")],
            "recommendations": [],
        },
        dimension_profile=_dimension_profile(priority=False),
        assessment_profile={"limitations": [], "metric_ledger": {}},
    )
    target = parsed["key_findings"][0]["claim_id"]
    dimensions, _markets, _executive, substitutions = apply_unresolved_claim_policy(
        dimension_narratives={"Price": parsed},
        market_narratives={},
        executive_narrative={},
        flags=[
            {
                "flag_id": "flag-1",
                "code": "unsupported_claim",
                "severity": "high",
                "claim_id": target,
            }
        ],
    )
    assert len(substitutions) == 1
    assert substitutions[0]["claim_id"] == target
    assert dimensions["Price"]["key_findings"][0]["substituted"] is True


def test_resolver_prefers_persisted_validated_blocks(monkeypatch) -> None:
    stored = [ReportBlock(type="heading", text="Validated", level=1).model_dump()]

    def fail_if_rebuilt(_result):
        raise AssertionError("persisted blocks were rebuilt")

    monkeypatch.setattr(
        "app.shared.report_blocks.build_mfi_report_blocks", fail_if_rebuilt
    )
    resolved = resolve_mfi_report_blocks({"report_blocks": stored})
    assert [block.text for block in resolved] == ["Validated"]


class _RepeatedIdModel:
    def invoke(self, messages):
        prompt = str(messages[0].content)
        if "Red-Team this structured" in prompt:
            return type("Response", (), {"content": '{"flags": []}'})()
        metric_match = re.search(r'"metric_id":\s*"([^"]+)"', prompt)
        metric_ids = [metric_match.group(1)] if metric_match else []

        def claim(text: str, scope: str = "assessment") -> dict:
            return {
                "claim_id": "stable id",
                "text": text,
                "claim_kind": "finding",
                "metric_ids": metric_ids,
                "document_ids": [],
                "scope": scope,
                "polarity": "neutral",
            }

        if "targeted MFI narrative" in prompt:
            payload = {
                "priority_issues": [claim("Review this market evidence.", "market")],
                "recommended_interventions": [
                    claim("Triangulate this market evidence.", "market")
                ],
                "limitations": [
                    claim(
                        "This market interpretation is limited to the cited evidence.",
                        "market",
                    )
                ],
            }
        elif "structured executive summary" in prompt:
            payload = {
                "motivation": claim("This report summarizes assessed markets."),
                "key_findings": [claim("A priority dimension warrants review.")],
                "recommendations": [claim("Review the cited assessment evidence.")],
                "limitations": [],
            }
        else:
            payload = {
                "summary": claim("This dimension is summarized by cited evidence."),
                "key_findings": [claim("The cited evidence warrants review.")],
                "subdimension_analysis": [],
                "geographic_patterns": [],
                "data_limitations": [],
                "recommendations": [claim("Review the cited dimension evidence.")],
            }
        return type("Response", (), {"content": json.dumps(payload)})()


def test_full_graph_duplicate_model_ids_complete_retrieve_and_export(
    monkeypatch,
) -> None:
    loaded = build_loaded(SyntheticSpec(market_count=1, region_count=1))
    monkeypatch.setattr(graph, "get_model", lambda **_kwargs: _RepeatedIdModel())
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
        lambda _state: {
            "visualizations": {},
            "current_node": "mfi_graph_designer",
        },
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

    index = canonical_claim_index(
        result["dimension_narratives"],
        result["market_narratives"],
        result["executive_summary_narrative"],
        result["context_evidence"],
    )
    assert index
    assert "stable id" not in index
    assert result["generation_diagnostics"]["ignored_model_identifier_count"] > 0
    assert result["generation_diagnostics"]["identity_fallback_artifacts"] == []
    assert result["generation_diagnostics"]["red_team_status"] == "completed"
    assert result["llm_diagnostics"]["calls"][-1]["operation"] == (
        "mfi.red_team_review.v3"
    )
    assert result["correction_attempts"] == 3
    assert result["generation_diagnostics"]["delivery_contract_status"] in {
        "validated",
        "fallback_validated",
    }
    assert result["report_blocks"]

    run_id = "mfi_claim_identity_delivery_test"
    create_run(run_id)
    set_run_completed(run_id, result=result)
    output = asyncio.run(router.get_report_result(run_id))
    assert output.report_blocks
    blocks = resolve_mfi_report_blocks(result)
    docx = build_docx_bytes_from_report_blocks(
        blocks,
        visualizations=result.get("visualizations", {}),
    )
    assert docx.startswith(b"PK")
