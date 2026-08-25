from __future__ import annotations

import copy

import pytest

from app.services.mfi_drafter.claim_identity import (
    dimension_claim_id,
    executive_claim_id,
    market_claim_id,
)
from app.services.mfi_drafter.methodology import DISPLAY_DIMENSIONS
from app.services.mfi_drafter.narrative import (
    apply_final_qa_annotations,
    validate_evidence_bound_narratives,
)
from app.services.mfi_drafter.simple_orchestration import (
    MAX_MARKETS_PER_DRAFT_BATCH,
    SEMANTIC_REVIEW_MAX_CHARACTERS,
    build_dimension_draft_batches,
    build_market_draft_batches,
    build_semantic_review_packages,
    validate_consolidated_correction_response,
    validate_dimension_draft_batch,
    validate_semantic_review_response,
)


def _claim(claim_id: str, *, metric_id: str = "metric.one") -> dict:
    return {
        "claim_id": claim_id,
        "text": "The cited evidence warrants review.",
        "claim_kind": "finding",
        "metric_ids": [metric_id],
        "document_ids": [],
        "scope": "assessment",
        "polarity": "neutral",
        "validation_status": "pending",
        "validation_flags": [],
        "validation_flag_ids": [],
        "substituted": False,
    }


def _profile(*, priority_count: int = 4, market_count: int = 15) -> dict:
    dimensions = []
    for index, dimension in enumerate(DISPLAY_DIMENSIONS, start=1):
        dimensions.append(
            {
                "dimension": dimension,
                "is_priority": index <= priority_count,
                "profile_rank": index,
                "statistics": {"mean": 4.0 + index / 10},
                "coverage": {
                    "available_market_count": market_count,
                    "total_assessed_market_count": market_count,
                    "missing_count": 0,
                    "coverage_ratio": 1.0,
                },
                "ledger_metric_ids": [f"metric.dimension.{index}"],
                "ranked_subsections": [],
                "ranked_drivers": [],
                "relevant_items": [],
                "localized_patterns": {},
            }
        )
    markets = [
        {
            "market_name": f"Market {index:02d}",
            "overall_mfi": 3.0 + index / 10,
            "score_rank": index,
            "selection_order": index,
            "is_priority_market": True,
            "weak_dimensions": [{"dimension": DISPLAY_DIMENSIONS[0]}],
            "ledger_metric_ids": [f"metric.market.{index}"],
        }
        for index in range(1, market_count + 1)
    ]
    return {
        "assessed_market_count": market_count,
        "mean_mfi_across_assessed_markets": 5.0,
        "dimensions": dimensions,
        "markets": markets,
        "priority_dimension_names": [
            item["dimension"] for item in dimensions if item["is_priority"]
        ],
        "priority_market_names": [item["market_name"] for item in markets],
        "limitations": [],
    }


def _narratives(profile: dict) -> tuple[dict, dict, dict]:
    dimensions = {
        item["dimension"]: {
            "dimension": item["dimension"],
            "is_priority": item["is_priority"],
            "summary": _claim(dimension_claim_id(item["dimension"], "summary")),
            "key_findings": [],
            "subdimension_analysis": [],
            "geographic_patterns": [],
            "data_limitations": [],
            "recommendations": [],
        }
        for item in profile["dimensions"]
    }
    markets = {
        item["market_name"]: {
            "market_name": item["market_name"],
            "region": None,
            "overall_mfi": item["overall_mfi"],
            "score_rank": item["score_rank"],
            "weak_dimensions": [DISPLAY_DIMENSIONS[0]],
            "priority_issues": [
                {
                    **_claim(market_claim_id(item["market_name"], "issue")),
                    "scope": "market",
                }
            ],
            "recommended_interventions": [],
            "limitations": [],
            "modality_consideration": None,
        }
        for item in profile["markets"]
    }
    executive = {
        "motivation": _claim(executive_claim_id("motivation")),
        "key_findings": [],
        "recommendations": [],
        "limitations": [],
        "scope_statement": None,
    }
    return dimensions, markets, executive


def test_draft_batches_reduce_nine_dimensions_and_fifteen_markets() -> None:
    profile = _profile()
    dimension_batches = build_dimension_draft_batches(profile)
    market_batches = build_market_draft_batches(profile)
    assert len(dimension_batches) == 5
    assert [item["batch_kind"] for item in dimension_batches].count(
        "priority_dimension"
    ) == 4
    assert dimension_batches[-1]["artifact_ids"] == list(DISPLAY_DIMENSIONS[4:])
    assert len(market_batches) == 3
    assert all(
        len(item["artifact_ids"]) <= MAX_MARKETS_PER_DRAFT_BATCH
        for item in market_batches
    )
    assert [name for item in market_batches for name in item["artifact_ids"]] == (
        profile["priority_market_names"]
    )


def test_dimension_batch_requires_exact_order_and_complete_rows() -> None:
    profile = _profile(priority_count=1, market_count=1)
    batch = build_dimension_draft_batches(profile)[-1]
    claim = {
        "text": "A supported finding.",
        "claim_kind": "finding",
        "metric_ids": [],
        "document_ids": [],
        "scope": "assessment",
        "polarity": "neutral",
    }
    narrative = {
        "summary": claim,
        "key_findings": [],
        "subdimension_analysis": [],
        "geographic_patterns": [],
        "data_limitations": [],
        "recommendations": [],
    }
    payload = {
        "dimensions": [
            {"dimension": name, "narrative": copy.deepcopy(narrative)}
            for name in batch["artifact_ids"]
        ]
    }
    parsed = validate_dimension_draft_batch(
        payload, batch=batch, assessment_profile=profile
    )
    assert list(parsed) == batch["artifact_ids"]
    payload["dimensions"].reverse()
    with pytest.raises(ValueError, match="ordering"):
        validate_dimension_draft_batch(
            payload, batch=batch, assessment_profile=profile
        )


def test_three_review_packages_have_exclusive_claim_ownership() -> None:
    profile = _profile()
    dimensions, markets, executive = _narratives(profile)
    packages = build_semantic_review_packages(
        dimension_narratives=dimensions,
        market_narratives=markets,
        executive_narrative=executive,
        context_evidence=[],
        assessment_profile=profile,
        claim_catalog={
            "metric.one": {
                "metric_id": "metric.one",
                "label": "Metric one",
                "formatted_value": "5.00/10",
                "unit": "score_0_10",
                "scope": "assessment",
            }
        },
        documents=[],
        deterministic_flags=[],
    )
    assert [item["section"] for item in packages] == [
        "overview",
        "dimensions",
        "markets",
    ]
    all_ids = [claim_id for package in packages for claim_id in package["claim_ids"]]
    assert len(all_ids) == len(set(all_ids))
    assert all(
        item["character_count"] <= SEMANTIC_REVIEW_MAX_CHARACTERS
        for item in packages
    )
    assert packages[0]["claim_ids"] == [executive_claim_id("motivation")]


def test_semantic_review_routes_by_claim_id_and_ignores_repeated_metadata() -> None:
    profile = _profile(market_count=1)
    dimensions, markets, executive = _narratives(profile)
    review = build_semantic_review_packages(
        dimension_narratives=dimensions,
        market_narratives=markets,
        executive_narrative=executive,
        context_evidence=[],
        assessment_profile=profile,
        claim_catalog={},
        documents=[],
        deterministic_flags=[],
    )[1]
    claim_id = review["claim_ids"][0]
    flags = validate_semantic_review_response(
        {
            "flags": [
                {
                    "claim_id": claim_id,
                    "code": "ambiguous_scope",
                    "severity": "medium",
                    "message": "Clarify the assessed-market scope.",
                    "recommendation": "Qualify the wording.",
                    "artifact_type": "market",
                    "artifact_id": "wrong",
                    "field_name": "wrong",
                    "unexpected_metadata": True,
                }
            ]
        },
        review=review,
    )
    assert len(flags) == 1
    assert flags[0]["claim_id"] == claim_id
    assert flags[0]["artifact_type"] == "dimension"
    assert flags[0]["artifact_id"] == DISPLAY_DIMENSIONS[0]
    assert flags[0]["field_name"] == "summary"


def test_semantic_review_rejects_claim_from_another_section() -> None:
    profile = _profile(market_count=1)
    dimensions, markets, executive = _narratives(profile)
    reviews = build_semantic_review_packages(
        dimension_narratives=dimensions,
        market_narratives=markets,
        executive_narrative=executive,
        context_evidence=[],
        assessment_profile=profile,
        claim_catalog={},
        documents=[],
        deterministic_flags=[],
    )
    with pytest.raises(ValueError, match="outside its section"):
        validate_semantic_review_response(
            {
                "flags": [
                    {
                        "claim_id": reviews[2]["claim_ids"][0],
                        "code": "wrong_section",
                        "severity": "high",
                        "message": "Wrong owner.",
                    }
                ]
            },
            review=reviews[1],
        )


def test_consolidated_response_requires_every_target_exactly_once() -> None:
    targets = [
        {
            "task_id": "target-one",
            "artifact_type": "dimension",
            "artifact_id": "Price",
            "field_name": "recommendations",
        },
        {
            "task_id": "target-two",
            "artifact_type": "market",
            "artifact_id": "Market 01",
            "field_name": "limitations",
        },
    ]
    claim = {
        "text": "Review the cited evidence.",
        "claim_kind": "recommendation",
        "metric_ids": ["metric.one"],
        "document_ids": [],
        "scope": "assessment",
        "polarity": "neutral",
        "claim_id": "ignored-model-id",
        "validation_status": "verified",
    }
    values = validate_consolidated_correction_response(
        {
            "patches": [
                {"target_id": "target-one", "replacement": [claim]},
                {"target_id": "target-two", "replacement": []},
            ]
        },
        targets=targets,
    )
    assert set(values) == {"target-one", "target-two"}
    assert "claim_id" not in values["target-one"][0]
    with pytest.raises(ValueError, match="exactly once"):
        validate_consolidated_correction_response(
            {
                "patches": [
                    {"target_id": "target-one", "replacement": [claim]}
                ]
            },
            targets=targets,
        )


def test_medium_finding_marks_canonical_claim_unverified_without_replacing_text() -> None:
    profile = _profile(market_count=1)
    dimensions, markets, executive = _narratives(profile)
    claim_id = dimensions[DISPLAY_DIMENSIONS[0]]["summary"]["claim_id"]
    original_text = dimensions[DISPLAY_DIMENSIONS[0]]["summary"]["text"]
    annotated_dimensions, _, _, _ = apply_final_qa_annotations(
        dimension_narratives=dimensions,
        market_narratives=markets,
        executive_narrative=executive,
        context_evidence=[],
        flags=[
            {
                "flag_id": "semantic-medium-1",
                "code": "interpretation_needs_qualification",
                "severity": "medium",
                "claim_id": claim_id,
            }
        ],
    )
    claim = annotated_dimensions[DISPLAY_DIMENSIONS[0]]["summary"]
    assert claim["text"] == original_text
    assert claim["validation_status"] == "unverified"
    assert claim["validation_flags"] == ["interpretation_needs_qualification"]
    assert claim["validation_flag_ids"] == ["semantic-medium-1"]


@pytest.mark.parametrize(
    ("text", "metric_ids", "expected_codes"),
    [
        ("The assessed-market mean was 5.00/10.", ["metric.one"], set()),
        ("The assessed-market mean was 7.00/10.", ["metric.one"], {"numeric_value_mismatch"}),
        ("The assessed-market mean was 5.00/10.", [], {"uncited_numeric_value", "numeric_value_mismatch"}),
        ("The assessment warrants review.", ["missing.metric"], {"invalid_metric_id"}),
    ],
)
def test_reduced_validator_checks_only_numbers_and_citation_existence(
    text: str, metric_ids: list[str], expected_codes: set[str]
) -> None:
    profile = _profile(market_count=1)
    dimensions, markets, executive = _narratives(profile)
    summary = dimensions[DISPLAY_DIMENSIONS[0]]["summary"]
    summary["text"] = text
    summary["metric_ids"] = metric_ids
    validation, *_rest = validate_evidence_bound_narratives(
        context_evidence=[],
        dimension_narratives=dimensions,
        market_narratives=markets,
        executive_narrative=executive,
        claim_catalog={
            "metric.one": {
                "metric_id": "metric.one",
                "formatted_value": "5.00/10",
                "allowed_renderings": ["5.00/10"],
                "unit": "score_0_10",
                "dimension": DISPLAY_DIMENSIONS[0],
            }
        },
        assessment_profile=profile,
        documents=[],
    )
    codes = {
        flag["code"]
        for flag in validation["flags"]
        if flag.get("claim_id") == summary["claim_id"]
    }
    assert codes == expected_codes
