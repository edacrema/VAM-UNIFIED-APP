"""Phase R1 tests for typed evidence-availability classification.

R1 replaces a single "fewer markets than assessed" rule with a classification that
distinguishes a genuine evidence failure from normal optional non-representation. These
tests cover each classification directly, so the warning policy cannot regress into
counting optional items again.
"""

from __future__ import annotations

import pytest

from app.services.mfi_drafter.analysis import (
    _classify_evidence_availability,
    build_assessment_profile,
)
from app.services.mfi_drafter.methodology import METRIC_DEFINITIONS_BY_ID
from app.services.mfi_drafter.schemas import MFIAnalysisConfig
from app.services.mfi_drafter.synthetic_fixtures import (
    DEFAULT_SPEC,
    SyntheticSpec,
    build_loaded,
    build_profile,
)

ASSESSED = 10

REQUIRED_METRIC = "service.shopping"
OPTIONAL_ITEM_METRIC = "price.increase.item.cereal_food.barley"
OPTIONAL_GROUP_METRIC = "competition.concentration.cereal_food"
QUALITY_METRIC = "quality.condition.refrigeration"


def _classify(metric_id: str, *, available: int, invalid: int = 0, raw=0.5):
    definition = METRIC_DEFINITIONS_BY_ID[metric_id]
    return _classify_evidence_availability(
        definition=definition,
        role=definition.role,
        available=available,
        assessed_count=ASSESSED,
        invalid=invalid,
        raw=raw,
        config=MFIAnalysisConfig(),
    )


# ---------------------------------------------------------------------------
# The six coverage cases
# ---------------------------------------------------------------------------


def test_complete_required_evidence_is_complete() -> None:
    availability = _classify(REQUIRED_METRIC, available=ASSESSED)

    assert availability.classification == "complete"
    assert availability.warrants_warning is False


def test_missing_required_evidence_is_unusable() -> None:
    availability = _classify(REQUIRED_METRIC, available=0, raw=None)

    assert availability.classification == "unusable_required"
    assert availability.warrants_warning is True


def test_invalid_required_evidence_is_unusable() -> None:
    """A validation failure must remain a warning even when other markets are fine."""
    availability = _classify(REQUIRED_METRIC, available=ASSESSED - 2, invalid=2)

    assert availability.classification == "unusable_required"
    assert availability.warrants_warning is True


def test_partial_required_evidence_warns() -> None:
    availability = _classify(REQUIRED_METRIC, available=ASSESSED - 3)

    assert availability.classification == "partial_required"
    assert availability.warrants_warning is True


def test_partial_optional_item_does_not_warn() -> None:
    availability = _classify(OPTIONAL_ITEM_METRIC, available=4)

    assert availability.classification == "partial_optional"
    assert availability.warrants_warning is False
    assert availability.represented_market_count == 4
    assert availability.total_assessed_market_count == ASSESSED


def test_absent_optional_item_does_not_warn() -> None:
    """An item traded nowhere is not evidence that failed; it is simply not sold."""
    availability = _classify(OPTIONAL_ITEM_METRIC, available=0, raw=None)

    assert availability.classification == "partial_optional"
    assert availability.warrants_warning is False


def test_optional_product_group_follows_the_optional_policy() -> None:
    availability = _classify(OPTIONAL_GROUP_METRIC, available=3)

    assert availability.classification == "partial_optional"
    assert availability.warrants_warning is False


def test_quality_applicability_is_not_applicable_rather_than_missing() -> None:
    """Food Quality applicability is decided per market by the assessment itself."""
    availability = _classify(QUALITY_METRIC, available=6)

    assert availability.classification == "not_applicable"
    assert availability.warrants_warning is False


# ---------------------------------------------------------------------------
# Warning policy end to end
# ---------------------------------------------------------------------------


def test_partial_required_warning_ratio_is_configurable() -> None:
    """A deployment may accept partial required coverage without warning."""
    definition = METRIC_DEFINITIONS_BY_ID[REQUIRED_METRIC]
    tolerant = MFIAnalysisConfig(partial_required_warning_ratio=0.5)

    availability = _classify_evidence_availability(
        definition=definition,
        role=definition.role,
        available=8,
        assessed_count=ASSESSED,
        invalid=0,
        raw=0.5,
        config=tolerant,
    )

    assert availability.classification == "partial_required"
    assert availability.warrants_warning is False


def test_complete_assessment_produces_no_evidence_limitation() -> None:
    profile = build_profile(DEFAULT_SPEC).model_dump()

    codes = {limitation["code"] for limitation in profile["limitations"]}
    assert "unavailable_explanatory_evidence" not in codes


def test_missing_required_subsection_still_warns() -> None:
    """The fix must not silence genuine required-evidence failures."""
    from app.services.mfi_drafter.synthetic_fixtures import SyntheticDefect

    spec = SyntheticSpec(
        defects=(SyntheticDefect(kind="missing_fixed_subsection", metric_id=REQUIRED_METRIC),)
    )
    loaded = build_loaded(spec)
    profile = build_assessment_profile(
        loaded["markets_data"], loaded["metric_summaries"], loaded
    ).model_dump()

    limitations = [
        limitation
        for limitation in profile["limitations"]
        if limitation["code"] == "unavailable_explanatory_evidence"
    ]
    assert limitations, "a missing required subsection must still be reported"
    assert REQUIRED_METRIC in set(limitations[0]["metric_ids"])
    assert "required" in limitations[0]["message"]


def test_partial_optional_coverage_is_disclosed_in_the_ledger() -> None:
    """Removing the warning must not remove the evidence from the ledger."""
    profile = build_profile(SyntheticSpec(item_market_ratio=0.5)).model_dump()

    disclosures = {
        key for key in profile["metric_ledger"] if "partial_optional_items" in key
    }
    assert disclosures
    entry = profile["metric_ledger"][sorted(disclosures)[0]]
    assert entry["value"] >= 1
    assert entry["source_metric_ids"]


def test_item_denominator_limitation_is_retained() -> None:
    """The item-specific trader-denominator caveat survives the reclassification."""
    profile = build_profile(DEFAULT_SPEC).model_dump()

    codes = {limitation["code"] for limitation in profile["limitations"]}
    assert "item_trader_denominator_unavailable" in codes


@pytest.mark.parametrize("ratio", [0.25, 0.5, 0.75])
def test_optional_coverage_never_warns_at_any_representation(ratio: float) -> None:
    profile = build_profile(SyntheticSpec(item_market_ratio=ratio)).model_dump()

    warned = [
        metric["metric_id"]
        for dimension in profile["dimensions"]
        for metric in list(dimension["subsections"]) + list(dimension["drivers"])
        if (metric.get("availability") or {}).get("warrants_warning")
    ]
    assert warned == []
