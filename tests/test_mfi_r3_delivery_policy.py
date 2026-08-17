"""Phase R3 tests for the unresolved-claim delivery policy and prompt contracts.

Repair is attempted first and bounded at three passes. These tests cover what happens
after that: a statement the assessment cannot support is withdrawn rather than delivered
with a caveat, and the rejected draft survives only in technical QA details.
"""

from __future__ import annotations

import pytest

from app.services.mfi_drafter import graph
from app.services.mfi_drafter.narrative import (
    apply_unresolved_claim_policy,
    unmatched_high_claim_ids,
)
from app.services.mfi_drafter.wording import WITHDRAWN_CLAIM_TEXT, withdrawn_text


def _claim(claim_id: str, text: str, *, kind: str = "finding") -> dict:
    return {
        "claim_id": claim_id,
        "text": text,
        "claim_kind": kind,
        "metric_ids": ["assessment.mfi.mean"],
        "document_ids": [],
        "scope": "assessment",
        "polarity": "neutral",
        "validation_status": "pending",
        "validation_flags": [],
    }


def _flag(claim_id: str, severity: str, code: str = "unsupported_modality_conclusion") -> dict:
    return {
        "flag_id": f"deterministic-{code}-{claim_id}",
        "source": "deterministic",
        "code": code,
        "severity": severity,
        "artifact_type": "dimension",
        "artifact_id": "Price",
        "field_name": "key_findings",
        "claim_id": claim_id,
        "message": "test",
        "repairable": True,
    }


@pytest.fixture
def narratives() -> tuple[dict, dict, dict]:
    dimensions = {
        "Price": {
            "dimension": "Price",
            "key_findings": [
                _claim("dim.price.1", "Cash assistance could be viable here."),
                _claim("dim.price.2", "Price scored 6.14 out of ten."),
            ],
        }
    }
    markets = {
        "Alpha": {
            "market_name": "Alpha",
            "priority_issues": [_claim("market.alpha.1", "Service is weak.")],
        }
    }
    executive = {
        "key_findings": [_claim("exec.1", "Service is the weakest dimension.")],
    }
    return dimensions, markets, executive


# ---------------------------------------------------------------------------
# Substitution
# ---------------------------------------------------------------------------


def test_high_severity_claim_text_is_withdrawn(narratives) -> None:
    dimensions, markets, executive = narratives

    result_dims, _, _, records = apply_unresolved_claim_policy(
        dimension_narratives=dimensions,
        market_narratives=markets,
        executive_narrative=executive,
        flags=[_flag("dim.price.1", "high")],
    )

    withdrawn = result_dims["Price"]["key_findings"][0]
    assert withdrawn["text"] == withdrawn_text("finding")
    assert withdrawn["substituted"] is True
    assert len(records) == 1
    assert records[0]["disposition"] == "replaced_by_deterministic_fallback"


def test_the_rejected_draft_leaves_the_report_but_not_the_record(narratives) -> None:
    """A withdrawn statement must not remain readable in the delivered narrative."""
    dimensions, markets, executive = narratives
    original = dimensions["Price"]["key_findings"][0]["text"]

    result_dims, _, _, records = apply_unresolved_claim_policy(
        dimension_narratives=dimensions,
        market_narratives=markets,
        executive_narrative=executive,
        flags=[_flag("dim.price.1", "high")],
    )

    assert original not in result_dims["Price"]["key_findings"][0]["text"]
    assert records[0]["rejected_text"] == original


def test_medium_severity_claims_are_left_in_place(narratives) -> None:
    """Medium findings are wording problems, shown with a marker rather than withdrawn."""
    dimensions, markets, executive = narratives
    original = dimensions["Price"]["key_findings"][0]["text"]

    result_dims, _, _, records = apply_unresolved_claim_policy(
        dimension_narratives=dimensions,
        market_narratives=markets,
        executive_narrative=executive,
        flags=[_flag("dim.price.1", "medium", code="unsupported_markup")],
    )

    assert result_dims["Price"]["key_findings"][0]["text"] == original
    assert records == []


def test_unaffected_claims_are_untouched(narratives) -> None:
    dimensions, markets, executive = narratives
    before_second = dict(dimensions["Price"]["key_findings"][1])
    before_market = dict(markets["Alpha"]["priority_issues"][0])

    result_dims, result_markets, result_exec, _ = apply_unresolved_claim_policy(
        dimension_narratives=dimensions,
        market_narratives=markets,
        executive_narrative=executive,
        flags=[_flag("dim.price.1", "high")],
    )

    assert result_dims["Price"]["key_findings"][1] == before_second
    assert result_markets["Alpha"]["priority_issues"][0] == before_market
    assert result_exec["key_findings"][0]["text"] == "Service is the weakest dimension."


def test_citations_are_cleared_on_the_claim_and_kept_in_the_record(narratives) -> None:
    """Evidence notes would otherwise print values beneath text that states nothing."""
    dimensions, markets, executive = narratives

    result_dims, _, _, records = apply_unresolved_claim_policy(
        dimension_narratives=dimensions,
        market_narratives=markets,
        executive_narrative=executive,
        flags=[_flag("dim.price.1", "high")],
    )

    assert result_dims["Price"]["key_findings"][0]["metric_ids"] == []
    assert records[0]["rejected_metric_ids"] == ["assessment.mfi.mean"]


def test_the_inputs_are_not_mutated(narratives) -> None:
    dimensions, markets, executive = narratives

    apply_unresolved_claim_policy(
        dimension_narratives=dimensions,
        market_narratives=markets,
        executive_narrative=executive,
        flags=[_flag("dim.price.1", "high")],
    )

    assert dimensions["Price"]["key_findings"][0]["text"] == (
        "Cash assistance could be viable here."
    )


def test_replacement_text_varies_by_claim_kind() -> None:
    kinds = {kind: withdrawn_text(kind) for kind in WITHDRAWN_CLAIM_TEXT}

    assert len(set(kinds.values())) > 1
    assert "recommendation" in withdrawn_text("recommendation")


def test_a_finding_pointing_at_no_claim_is_reported(narratives) -> None:
    """Otherwise the report would promise traceability it cannot deliver."""
    dimensions, markets, executive = narratives
    flags = [_flag("dim.price.1", "high"), _flag("does.not.exist", "high")]

    _, _, _, records = apply_unresolved_claim_policy(
        dimension_narratives=dimensions,
        market_narratives=markets,
        executive_narrative=executive,
        flags=flags,
    )

    assert unmatched_high_claim_ids(flags, records) == ["does.not.exist"]


# ---------------------------------------------------------------------------
# Wiring into finalize
# ---------------------------------------------------------------------------


def test_finalize_withdraws_and_records_without_calling_a_model(
    narratives, monkeypatch
) -> None:
    """The delivery policy is deterministic; it must never reach for the model."""
    dimensions, markets, executive = narratives

    def _fail(*args, **kwargs):
        raise AssertionError("finalize must not invoke a model")

    monkeypatch.setattr(graph, "get_model", _fail)

    update = graph.node_finalize_qa(
        {
            "dimension_narratives": dimensions,
            "market_narratives": markets,
            "executive_summary_narrative": executive,
            "deterministic_flags": [_flag("dim.price.1", "high")],
            "red_team_flags": [],
            "correction_attempts": 3,
        }
    )

    finding = update["dimension_narratives"]["Price"]["key_findings"][0]
    assert finding["substituted"] is True
    assert finding["validation_status"] == "unverified"
    assert update["qa_review"]["status"] == "completed_with_warnings"

    diagnostics = update["generation_diagnostics"]
    assert len(diagnostics["claim_substitutions"]) == 1
    assert diagnostics["claim_substitutions"][0]["claim_id"] == "dim.price.1"
    assert diagnostics["unmatched_high_claim_ids"] == []
    assert any("withdrawn" in warning for warning in update["warnings"])


def test_finalize_leaves_a_clean_run_alone(narratives) -> None:
    dimensions, markets, executive = narratives

    update = graph.node_finalize_qa(
        {
            "dimension_narratives": dimensions,
            "market_narratives": markets,
            "executive_summary_narrative": executive,
            "deterministic_flags": [],
            "red_team_flags": [],
            "correction_attempts": 0,
        }
    )

    assert update["dimension_narratives"] == dimensions
    assert update["generation_diagnostics"]["claim_substitutions"] == []
    assert update["warnings"] == []


def test_flag_identifiers_are_kept_apart_from_flag_codes(narratives) -> None:
    """Identifiers hash the message and churn; codes are stable and displayable."""
    dimensions, markets, executive = narratives
    dimensions["Price"]["key_findings"][0]["validation_flags"] = [
        "unsupported_modality_conclusion"
    ]

    update = graph.node_finalize_qa(
        {
            "dimension_narratives": dimensions,
            "market_narratives": markets,
            "executive_summary_narrative": executive,
            "deterministic_flags": [_flag("dim.price.1", "high")],
            "red_team_flags": [],
            "correction_attempts": 3,
        }
    )

    finding = update["dimension_narratives"]["Price"]["key_findings"][0]
    assert finding["validation_flags"] == ["unsupported_modality_conclusion"]
    assert finding["validation_flag_ids"] == [
        "deterministic-unsupported_modality_conclusion-dim.price.1"
    ]


# ---------------------------------------------------------------------------
# Prompt contracts
# ---------------------------------------------------------------------------


def test_the_market_prompt_no_longer_requests_a_modality_conclusion() -> None:
    """Asking for a verdict the evidence cannot support is where the defect started."""
    import inspect

    source = inspect.getsource(graph.node_market_recommendations_drafter)

    assert '"modality_consideration": CLAIM_OR_NULL' not in source
    assert "finding|recommendation|modality_consideration" not in source


@pytest.mark.parametrize(
    "node_name",
    [
        "node_dimension_drafter",
        "node_market_recommendations_drafter",
        "node_executive_summary_drafter",
        "node_red_team",
    ],
)
def test_every_prompt_carries_the_prohibitions(node_name) -> None:
    import inspect

    source = inspect.getsource(getattr(graph, node_name))

    assert "NARRATIVE_PROHIBITIONS" in source


@pytest.mark.parametrize(
    "node_name",
    ["node_dimension_drafter", "node_market_recommendations_drafter"],
)
def test_prompts_no_longer_licence_conditional_modality_language(node_name) -> None:
    """"Conditional" and "unilateral" both told the model a hedged verdict was fine."""
    import inspect

    source = inspect.getsource(getattr(graph, node_name))

    assert "must remain conditional" not in source
    assert "unilateral modality" not in source
