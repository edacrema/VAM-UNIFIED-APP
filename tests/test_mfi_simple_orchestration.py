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
    build_claim_catalog,
    validate_evidence_bound_narratives,
)
from app.services.mfi_drafter.simple_orchestration import (
    MARKET_DRAFT_MAX_PROMPT_CHARACTERS,
    MAX_MARKETS_PER_DRAFT_BATCH,
    MarketDraftPromptContractError,
    SEMANTIC_REVIEW_MAX_CHARACTERS,
    SEMANTIC_REVIEW_CONTRACT_VERSION,
    build_budgeted_market_draft_batches,
    build_dimension_draft_batches,
    build_market_draft_batches,
    build_market_prompt_projection,
    build_semantic_review_packages,
    consolidated_correction_prompt_payload,
    validate_consolidated_correction_response,
    validate_dimension_draft_batch,
    validate_semantic_review_response,
)
from app.services.mfi_drafter.synthetic_fixtures import SyntheticSpec, build_profile


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


@pytest.fixture(scope="module")
def compact_market_bundle() -> tuple[dict, dict]:
    profile = build_profile(SyntheticSpec(market_count=15)).model_dump()
    return profile, build_claim_catalog(profile)


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


def test_compact_market_batches_obey_exact_prompt_budget_and_are_deterministic(
    compact_market_bundle: tuple[dict, dict],
) -> None:
    profile, catalog = compact_market_bundle
    first = build_budgeted_market_draft_batches(profile, catalog)
    second = build_budgeted_market_draft_batches(profile, catalog)

    assert first == second
    assert [name for batch in first for name in batch["artifact_ids"]] == (
        profile["priority_market_names"]
    )
    assert all(len(batch["artifact_ids"]) <= 5 for batch in first)
    assert all(
        batch["prompt_character_count"] == len(batch["prompt"])
        <= MARKET_DRAFT_MAX_PROMPT_CHARACTERS
        for batch in first
    )
    assert all(batch["projection_version"] == "mfi-market-prompt-v1" for batch in first)

    projected_ids = {
        metric_id
        for market in profile["markets"]
        if market["market_name"] in set(profile["priority_market_names"])
        for metric_id in build_market_prompt_projection(
            profile, market
        )["selected_ledger_metric_ids"]
    }
    prompt_ids = {
        entry["metric_id"]
        for batch in first
        for entry in batch["claim_catalog"]
    }
    assert prompt_ids == projected_ids.intersection(catalog)


def test_market_prompt_budget_accepts_exact_boundary_and_rejects_one_over(
    compact_market_bundle: tuple[dict, dict],
) -> None:
    profile, catalog = compact_market_bundle
    one_market = copy.deepcopy(profile)
    market_name = profile["priority_market_names"][0]
    one_market["priority_market_names"] = [market_name]
    exact = build_budgeted_market_draft_batches(
        one_market,
        catalog,
        maximum_prompt_characters=MARKET_DRAFT_MAX_PROMPT_CHARACTERS,
    )[0]["prompt_character_count"]

    accepted = build_budgeted_market_draft_batches(
        one_market,
        catalog,
        maximum_prompt_characters=exact,
    )
    assert accepted[0]["prompt_character_count"] == exact
    with pytest.raises(MarketDraftPromptContractError) as caught:
        build_budgeted_market_draft_batches(
            one_market,
            catalog,
            maximum_prompt_characters=exact - 1,
        )
    assert caught.value.market_name == market_name
    assert caught.value.character_count == exact


def test_market_projection_is_bounded_and_does_not_mutate_analysis(
    compact_market_bundle: tuple[dict, dict],
) -> None:
    profile, _catalog = compact_market_bundle
    before = copy.deepcopy(profile)
    market = next(
        item
        for item in profile["markets"]
        if item["market_name"] == profile["priority_market_names"][0]
    )
    projection = build_market_prompt_projection(profile, market)

    assert len(projection["weak_dimensions"]) == len(market["weak_dimensions"])
    for weak in projection["weak_dimensions"]:
        assert len(weak["official_subsections"]) <= 2
        assert len(weak["explanatory_drivers"]) <= 4
        assert len(weak["relevant_items"]) <= 3
    assert profile == before


def test_market_projection_preserves_food_quality_measure_and_maximum(
) -> None:
    severity = {dimension: 0.8 for dimension in DISPLAY_DIMENSIONS}
    severity.update({"Food Quality": 0.1, "Service": 0.2, "Price": 0.3})
    profile = build_profile(
        SyntheticSpec(market_count=1, dimension_severity=severity)
    ).model_dump()
    market = copy.deepcopy(profile["markets"][0])
    assert market["weak_dimensions"][0]["dimension"] == "Food Quality"

    projection = build_market_prompt_projection(profile, market)
    quality = next(
        item
        for item in projection["weak_dimensions"]
        if item["dimension"] == "Food Quality"
    )
    assert [
        item["source_metric_id"] for item in quality["official_subsections"]
    ] == ["quality.measure", "quality.maximum"]
    assert 1 <= len(quality["explanatory_drivers"]) <= 4
    assert all(
        item["role"] == "question_driver"
        for item in quality["explanatory_drivers"]
    )


def test_consolidated_correction_deduplicates_shared_authorized_evidence(
    compact_market_bundle: tuple[dict, dict],
) -> None:
    profile, catalog = compact_market_bundle
    dimensions, markets, executive = _narratives(profile)
    market_name = profile["priority_market_names"][0]
    targets = [
        {
            "task_id": f"target-{index}",
            "artifact_type": "market",
            "artifact_id": market_name,
            "field_name": field_name,
            "flag_ids": [f"flag-{index}"],
            "claim_ids": [],
        }
        for index, field_name in enumerate(
            ("priority_issues", "recommended_interventions"), start=1
        )
    ]
    flags = [
        {
            "flag_id": f"flag-{index}",
            "code": "needs_revision",
            "severity": "medium",
            "claim_id": None,
            "message": "Revise this field.",
            "recommendation": "Use authorized evidence.",
            "metric_ids": [],
            "document_ids": [],
        }
        for index in (1, 2)
    ]
    payload = consolidated_correction_prompt_payload(
        targets=targets,
        flags=flags,
        dimension_narratives=dimensions,
        market_narratives=markets,
        executive_narrative=executive,
        context_evidence=[],
        assessment_profile=profile,
        claim_catalog=catalog,
        documents=[],
    )

    assert payload["contract_version"] == "mfi-consolidated-correction-v2"
    assert len(payload["targets"]) == 2
    assert all(
        "authorized_claim_catalog" not in target for target in payload["targets"]
    )
    assert len(payload["artifact_contexts"]) == 1
    assert len(payload["artifact_authorizations"]) == 1
    assert payload["targets"][0]["artifact_context_id"] == (
        payload["targets"][1]["artifact_context_id"]
    )
    metric_ids = [
        item["metric_id"] for item in payload["authorized_claim_catalog"]
    ]
    assert metric_ids
    assert len(metric_ids) == len(set(metric_ids))
    authorization = next(iter(payload["artifact_authorizations"].values()))
    assert authorization["metric_ids"]
    assert len(authorization["metric_ids"]) == len(set(authorization["metric_ids"]))


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
        report_context={
            "country": "Testland",
            "data_collection_start": "2026-01-01",
            "data_collection_end": "2026-01-31",
        },
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
    assert all(
        item["package"]["contract_version"] == SEMANTIC_REVIEW_CONTRACT_VERSION
        for item in packages
    )
    assert all(
        item["package"]["report_context"]["country"] == "Testland"
        for item in packages
    )
    assert all("prohibitions" not in item["package"] for item in packages)
    assert all("severity_policy" not in item["package"] for item in packages)


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
                    "issue_type": "data_mismatch",
                    "severity": "medium",
                    "message": "The cited figure differs from the report data.",
                    "recommendation": "Use the cited figure.",
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
    assert flags[0]["code"] == "semantic_data_mismatch"


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
                        "issue_type": "data_mismatch",
                        "severity": "high",
                        "message": "Wrong owner.",
                    }
                ]
            },
            review=reviews[1],
        )


def test_semantic_review_ignores_out_of_scope_phraseology_findings() -> None:
    profile = _profile(market_count=15)
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
    )[2]
    claim_ids = review["claim_ids"]
    payload = {
        "flags": [
            {
                "claim_id": claim_ids[index % len(claim_ids)],
                "code": "prohibited_phraseology",
                "severity": "medium",
                "message": "Prefer different wording.",
                "recommendation": "Rewrite for style.",
            }
            for index in range(40)
        ]
    }
    assert validate_semantic_review_response(payload, review=review) == []


def test_semantic_review_receives_actual_data_and_source_excerpts() -> None:
    profile = _profile(market_count=1)
    dimensions, markets, executive = _narratives(profile)
    dimensions[DISPLAY_DIMENSIONS[0]]["summary"]["document_ids"] = ["doc-a"]
    context = [
        {
            "statement_id": "context-001",
            "text": "The source describes a possible market disruption.",
            "classification": "potentially_explanatory",
            "document_ids": ["doc-a"],
        }
    ]
    excerpt = "A" * 900
    documents = [
        {
            "doc_id": "doc-a",
            "source": "ReliefWeb",
            "title": "Relevant source",
            "date": "2026-01-10",
            "content": excerpt,
        },
        {
            "doc_id": "doc-unused",
            "source": "ReliefWeb",
            "title": "Unused source",
            "content": "This must not be projected.",
        },
    ]
    catalog = {
        "metric.one": {
            "metric_id": "metric.one",
            "label": "Metric one",
            "numeric_value": 0.42,
            "formatted_value": "42.0%",
            "allowed_renderings": ["42.0%", "42%"],
            "statistic": "market_value",
            "unit": "proportion",
            "orientation": "unfavorable_high",
            "scope": "market",
            "coverage_label": "1/1 markets (100.0%)",
            "aggregation_method": "market_value",
            "population_basis": "trader_level_within_market",
            "pooled_denominator_available": False,
            "representation_basis": "single_assessed_market",
        }
    }
    packages = build_semantic_review_packages(
        dimension_narratives=dimensions,
        market_narratives=markets,
        executive_narrative=executive,
        context_evidence=context,
        assessment_profile=profile,
        claim_catalog=catalog,
        documents=documents,
        deterministic_flags=[],
        report_context={"country": "Testland"},
    )

    for package in packages:
        assert package["package"]["accepted_context"] == [
            {
                "statement_id": "context-001",
                "text": "The source describes a possible market disruption.",
                "classification": "potentially_explanatory",
                "document_ids": ["doc-a"],
            }
        ]
        projected_documents = package["package"]["cited_documents"]
        assert [item["document_id"] for item in projected_documents] == ["doc-a"]
        assert projected_documents[0]["content_excerpt"] == excerpt[:800]
        assert "doc-unused" not in str(projected_documents)

    evidence = packages[1]["package"]["evidence_by_metric_id"]["metric.one"]
    assert evidence["numeric_value"] == 0.42
    assert evidence["formatted_value"] == "42.0%"
    assert evidence["aggregation_method"] == "market_value"
    assert evidence["population_basis"] == "trader_level_within_market"
    assert evidence["pooled_denominator_available"] is False


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
