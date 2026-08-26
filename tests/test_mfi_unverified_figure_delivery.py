from __future__ import annotations

from copy import deepcopy

import pytest

from app.services.mfi_drafter import graph
from app.services.mfi_drafter.claim_identity import dimension_claim_id
from app.services.mfi_drafter.context_status import not_attempted_context_status
from app.services.mfi_drafter.errors import MFIGenerationBlockedError
from app.services.mfi_drafter.narrative import classify_final_qa_flags
from app.services.mfi_drafter.schemas import (
    MFIGenerationDiagnostics,
    MFINarrativeQAFlag,
    MFIQAReview,
)


def _claim(claim_id: str, text: str, *, kind: str = "finding") -> dict:
    return {
        "claim_id": claim_id,
        "text": text,
        "claim_kind": kind,
        "metric_ids": ["assessment.dimension.price.mean"],
        "document_ids": [],
        "scope": "assessment",
        "polarity": "descriptive",
        "validation_status": "unverified",
        "validation_flags": [],
        "validation_flag_ids": [],
        "substituted": False,
    }


def _narratives() -> tuple[dict, dict, dict]:
    dimension = {
        "dimension": "Price",
        "is_priority": True,
        "summary": _claim(
            dimension_claim_id("Price", "summary", 1),
            "The assessed-market Price mean was 5.00/10.",
            kind="summary",
        ),
        "key_findings": [],
        "subdimension_analysis": [],
        "geographic_patterns": [
            _claim(
                dimension_claim_id("Price", "geography", 1),
                "Price varied across assessed markets.",
                kind="geographic_pattern",
            ),
            _claim(
                dimension_claim_id("Price", "geography", 2),
                "Sixteen markets were reviewed, reported here as 16.",
                kind="geographic_pattern",
            ),
        ],
        "data_limitations": [],
        "recommendations": [],
    }
    return {"Price": dimension}, {}, {}


def _flag(
    code: str,
    *,
    source: str = "deterministic",
    actual_value: str | None = "16",
    claim_id: str | None = None,
) -> dict:
    claim_id = claim_id or dimension_claim_id("Price", "geography", 2)
    return MFINarrativeQAFlag(
        flag_id=f"{source}-{code}",
        source=source,
        code=code,
        severity="high",
        artifact_type="dimension",
        artifact_id="Price",
        field_name="geographic_patterns",
        claim_id=claim_id,
        message=f"Test finding for {code}.",
        actual_value=actual_value,
    ).model_dump()


def test_public_qa_and_diagnostic_contracts_expose_figure_delivery_state() -> None:
    review = MFIQAReview(
        status="delivered_with_unverified_figures",
        unverified_figure_flag_ids=["numeric-16"],
        unverified_figure_claim_ids=["dimension.price.geography.2"],
        unverified_figure_values=["16"],
    )
    diagnostics = MFIGenerationDiagnostics(
        delivery_qa_status="delivered_with_unverified_figures",
        unresolved_high_count=1,
        blocking_high_count=0,
        unverified_figure_flag_count=1,
        unverified_figure_claim_count=1,
        unverified_figure_flag_ids=["numeric-16"],
        unverified_figure_values=["16"],
    )

    assert review.status == "delivered_with_unverified_figures"
    assert diagnostics.unresolved_high_count == 1
    assert diagnostics.blocking_high_count == 0


@pytest.mark.parametrize(
    ("code", "actual_value", "claim_text"),
    [
        ("numeric_value_mismatch", "16", "The reported count is 16."),
        ("uncited_numeric_value", "16, 27", "The values are 16 and 27."),
        ("unit_mismatch", "16%", "The reported rate is 16%."),
    ],
)
def test_final_policy_retains_only_exact_claim_scoped_figures(
    code: str, actual_value: str, claim_text: str
) -> None:
    dimensions, markets, executive = _narratives()
    dimensions["Price"]["geographic_patterns"][1]["text"] = claim_text
    partition = classify_final_qa_flags(
        [_flag(code, actual_value=actual_value)],
        dimension_narratives=dimensions,
        market_narratives=markets,
        executive_narrative=executive,
        correction_completed=True,
        corrected_verification_completed=True,
    )

    assert partition["blocking_high_flags"] == []
    assert len(partition["deliverable_figure_flags"]) == 1
    assert partition["deliverable_figure_flags"][0]["delivery_disposition"] == (
        "retained_unverified_figure_for_delivery"
    )
    assert partition["unverified_figure_values"] == actual_value.split(", ")


@pytest.mark.parametrize(
    "mutate",
    [
        lambda flag: {**flag, "source": "red_team"},
        lambda flag: {**flag, "code": "invalid_metric_id"},
        lambda flag: {**flag, "claim_id": "missing.claim"},
        lambda flag: {**flag, "actual_value": "99"},
    ],
)
def test_final_policy_keeps_noneligible_high_findings_blocking(mutate) -> None:
    dimensions, markets, executive = _narratives()
    partition = classify_final_qa_flags(
        [mutate(_flag("numeric_value_mismatch"))],
        dimension_narratives=dimensions,
        market_narratives=markets,
        executive_narrative=executive,
        correction_completed=True,
        corrected_verification_completed=True,
    )

    assert partition["deliverable_figure_flags"] == []
    assert len(partition["blocking_high_flags"]) == 1


def test_final_policy_requires_completed_correction_and_verification() -> None:
    dimensions, markets, executive = _narratives()
    for correction_completed, verification_completed in ((False, True), (True, False)):
        partition = classify_final_qa_flags(
            [_flag("numeric_value_mismatch")],
            dimension_narratives=dimensions,
            market_narratives=markets,
            executive_narrative=executive,
            correction_completed=correction_completed,
            corrected_verification_completed=verification_completed,
        )
        assert len(partition["blocking_high_flags"]) == 1


def test_blocking_high_on_same_claim_prevents_quantitative_delivery() -> None:
    dimensions, markets, executive = _narratives()
    partition = classify_final_qa_flags(
        [
            _flag("numeric_value_mismatch"),
            _flag("citation_context_mismatch", actual_value=None),
        ],
        dimension_narratives=dimensions,
        market_narratives=markets,
        executive_narrative=executive,
        correction_completed=True,
        corrected_verification_completed=True,
    )

    assert partition["deliverable_figure_flags"] == []
    assert len(partition["blocking_high_flags"]) == 2


def _finalize_state(flags: list[dict]) -> dict:
    dimensions, markets, executive = _narratives()
    return {
        "dimension_narratives": deepcopy(dimensions),
        "market_narratives": markets,
        "executive_summary_narrative": executive,
        "context_evidence": [],
        "contextual_documents": [],
        "context_status": not_attempted_context_status().model_dump(),
        "deterministic_flags": flags,
        "red_team_flags": [],
        "correction_attempts": 1,
        "correction_history": [
            {
                "attempt_number": 1,
                "task_id": "dimension-price-geography",
                "artifact_type": "dimension",
                "artifact_id": "Price",
                "field_name": "geographic_patterns",
                "claim_id": dimension_claim_id("Price", "geography", 2),
                "flag_ids": [flag["flag_id"] for flag in flags],
                "flag_codes": [flag["code"] for flag in flags],
                "execution_outcome": "llm_completed",
                "validation_outcome": "pending",
            }
        ],
        "llm_diagnostics": {"calls": []},
        "generation_diagnostics": {
            "consolidated_correction_status": "completed",
            "corrected_claim_verification_status": "completed",
        },
    }


def test_finalize_qa_delivers_gaza_style_residual_figure_without_changing_text() -> None:
    flag = _flag("numeric_value_mismatch")
    state = _finalize_state([flag])
    original_text = state["dimension_narratives"]["Price"][
        "geographic_patterns"
    ][1]["text"]

    result = graph.node_finalize_qa(state)

    claim = result["dimension_narratives"]["Price"]["geographic_patterns"][1]
    assert claim["text"] == original_text
    assert claim["validation_status"] == "unverified"
    assert result["qa_review"]["status"] == "delivered_with_unverified_figures"
    assert result["qa_review"]["unverified_figure_values"] == ["16"]
    diagnostics = result["generation_diagnostics"]
    assert diagnostics["unresolved_high_count"] == 1
    assert diagnostics["blocking_high_count"] == 0
    assert diagnostics["unverified_figure_flag_count"] == 1
    assert diagnostics["unverified_figure_claim_count"] == 1
    assert diagnostics["delivery_qa_status"] == (
        "delivered_with_unverified_figures"
    )
    assert result["correction_history"][0]["validation_outcome"] == "unresolved"


def test_finalize_qa_still_blocks_invalid_citation() -> None:
    state = _finalize_state([_flag("invalid_metric_id", actual_value=None)])
    with pytest.raises(MFIGenerationBlockedError) as caught:
        graph.node_finalize_qa(state)
    assert caught.value.code == "mfi_narrative_qa_unresolved"
    assert caught.value.target_count == 1
