"""Phase R2 tests for aggregation and representation semantics.

R2 records, for every citable number, what population it describes and a deterministic
phrase describing it correctly. It enforces nothing yet — R3 does that — so these tests
verify the metadata is complete and correct, and that nothing downstream moved.

The sharpest risk is the phrase text itself: it is sent to the drafting prompts, and
numeric authorization only accepts numbers matching a cited value's rendering. A digit in
a phrase could therefore be quoted into a claim and rejected as unauthorized, failing
validation. Several tests below exist solely to prevent that.
"""

from __future__ import annotations

import json
import re

import pytest

from app.services.mfi_drafter.analysis import _ledger_semantics
from app.services.mfi_drafter.narrative import (
    _allowed_renderings,
    _format_catalog_value,
    build_claim_catalog,
    compact_catalog,
    dimension_catalog_ids,
)
from app.services.mfi_drafter.synthetic_fixtures import (
    DEFAULT_SPEC,
    SyntheticDefect,
    SyntheticSpec,
    build_profile,
)

# Mirrors report_inspector.pooled_population_pattern: nouns that imply a respondent
# denominator the processed data cannot supply.
RESPONDENT_NOUNS = re.compile(r"traders|respondents|responses|vendors", re.IGNORECASE)

PARTIAL_SPEC = SyntheticSpec(item_market_ratio=0.5)


@pytest.fixture(scope="module")
def complete_profile() -> dict:
    return build_profile(DEFAULT_SPEC).model_dump()


@pytest.fixture(scope="module")
def partial_profile() -> dict:
    return build_profile(PARTIAL_SPEC).model_dump()


def _ledgers(profile: dict) -> dict:
    return profile["metric_ledger"]


def _phrases(profile: dict) -> set[str]:
    return {entry["permitted_subject_phrase"] for entry in _ledgers(profile).values()}


# ---------------------------------------------------------------------------
# Derivation
# ---------------------------------------------------------------------------


def test_unmapped_statistic_raises() -> None:
    """A new statistic must fail loudly rather than default to something plausible."""
    with pytest.raises(ValueError, match="Unmapped MFI ledger statistic"):
        _ledger_semantics(
            statistic="totally_new_statistic",
            unit="score",
            evidence_scope="included_assessed_markets",
        )


def test_derivation_table_matches_the_ledger_vocabulary(complete_profile) -> None:
    """Every statistic in a real ledger must derive, and every entry must be populated."""
    for entry in _ledgers(complete_profile).values():
        assert entry["permitted_subject_phrase"], entry["ledger_id"]
        assert entry["aggregation_method"]
        assert entry["population_basis"]
        assert entry["representation_basis"]


def test_statistic_takes_precedence_over_inherited_evidence_scope() -> None:
    """A metric's scope is inherited by all its entries, including market-denominated ones.

    A coverage ratio counts markets whatever the underlying metric surveys, so resolving
    by evidence scope first would mislabel it as trader-level.
    """
    coverage = _ledger_semantics(
        statistic="coverage_ratio",
        unit="proportion",
        evidence_scope="surveyed_traders_in_market",
    )
    rank = _ledger_semantics(
        statistic="rank",
        unit="rank",
        evidence_scope="surveyed_traders_in_market",
    )

    assert coverage.population_basis == "market_level"
    assert coverage.aggregation_method == "coverage"
    assert rank.population_basis == "descriptive"
    assert rank.aggregation_method == "rank"


def test_market_name_never_implies_a_single_market_value() -> None:
    """A market's rank among all markets carries a market name but ranks over markets."""
    semantics = _ledger_semantics(
        statistic="rank_lowest_first",
        unit="rank",
        evidence_scope="included_assessed_markets",
        market_name="Market 01",
    )

    assert semantics.aggregation_method == "rank"


def test_trader_level_values_never_claim_a_pooled_denominator() -> None:
    """This is the data-layer statement of FIX-03."""
    assessment = _ledger_semantics(
        statistic="derived_unfavorable_rate",
        unit="proportion",
        evidence_scope="surveyed_traders_in_market",
    )

    assert assessment.aggregation_method == "unweighted_market_mean"
    assert assessment.population_basis == "trader_level_within_market"
    assert assessment.pooled_denominator_available is False
    assert "unweighted" in assessment.permitted_subject_phrase
    assert "market-level" in assessment.permitted_subject_phrase


def test_binary_market_conditions_are_market_level() -> None:
    """A per-market yes/no condition averaged over markets is a share of markets."""
    semantics = _ledger_semantics(
        statistic="derived_unfavorable_rate",
        unit="proportion",
        evidence_scope="assessed_market",
    )

    assert semantics.population_basis == "market_level"
    assert semantics.pooled_denominator_available is True
    assert "assessed markets" in semantics.permitted_subject_phrase
    assert not RESPONDENT_NOUNS.search(semantics.permitted_subject_phrase)


def test_single_market_trader_values_are_trader_level() -> None:
    """The one case where naming surveyed traders is correct."""
    semantics = _ledger_semantics(
        statistic="market_explanatory_raw_value",
        unit="proportion",
        evidence_scope="surveyed_traders_in_market",
        market_name="Market 01",
    )

    assert semantics.aggregation_method == "market_value"
    assert semantics.population_basis == "trader_level_within_market"
    assert semantics.pooled_denominator_available is False
    assert "surveyed traders" in semantics.permitted_subject_phrase


# ---------------------------------------------------------------------------
# Phrase safety
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("spec_name", ["complete", "partial"])
def test_no_permitted_phrase_contains_a_digit(
    spec_name, complete_profile, partial_profile
) -> None:
    """The single most important invariant in R2.

    Phrases reach the drafting prompts. A digit quoted from one would not match any cited
    value's rendering, so numeric authorization would reject it and claim validation would
    fail. Market and item names are therefore never interpolated.
    """
    profile = complete_profile if spec_name == "complete" else partial_profile

    offenders = sorted(phrase for phrase in _phrases(profile) if re.search(r"\d", phrase))

    assert offenders == []


def test_only_trader_level_phrases_name_a_respondent_population(
    complete_profile, partial_profile
) -> None:
    """Saying "traders" about a market-level statistic is exactly the FIX-03 defect."""
    for profile in (complete_profile, partial_profile):
        offenders = [
            (entry["ledger_id"], entry["permitted_subject_phrase"])
            for entry in _ledgers(profile).values()
            if entry["population_basis"] != "trader_level_within_market"
            and RESPONDENT_NOUNS.search(entry["permitted_subject_phrase"])
        ]
        assert offenders == []


def test_no_permitted_phrase_trips_the_terminology_validator(complete_profile) -> None:
    """A supplied phrase must never be one the validator would reject."""
    from app.services.mfi_drafter.narrative import _terminology_flags

    location = {"artifact_type": "dimension", "artifact_id": "Price", "field_name": "text"}
    for phrase in _phrases(complete_profile):
        assert _terminology_flags(phrase, location, "claim.test", []) == [], phrase


def test_phrases_are_lowercase_noun_phrases(complete_profile) -> None:
    """They are dropped into sentences, so they must not carry their own punctuation."""
    for phrase in _phrases(complete_profile):
        assert not phrase.endswith(".")
        assert phrase == phrase.strip()
        assert phrase[0].islower()


# ---------------------------------------------------------------------------
# Representation basis
# ---------------------------------------------------------------------------


def test_complete_coverage_reports_all_assessed_markets(complete_profile) -> None:
    bases = {entry["representation_basis"] for entry in _ledgers(complete_profile).values()}

    assert "represented_assessed_markets" not in bases
    assert "incomplete_assessed_markets" not in bases
    assert "all_assessed_markets" in bases


def test_partial_optional_items_are_marked_as_represented(partial_profile) -> None:
    """The wording must qualify which markets stand behind a partially represented item."""
    entries = [
        entry
        for entry in _ledgers(partial_profile).values()
        if entry["representation_basis"] == "incomplete_assessed_markets"
    ]

    assert entries
    assert any("usable evidence" in entry["permitted_subject_phrase"] for entry in entries)


def test_food_quality_applicability_is_distinguished(partial_profile) -> None:
    """Inapplicable is not the same as missing, and needs different wording."""
    entries = [
        entry
        for entry in _ledgers(partial_profile).values()
        if entry["representation_basis"] == "incomplete_assessed_markets"
    ]

    assert entries
    assert any("usable evidence" in entry["permitted_subject_phrase"] for entry in entries)


def test_required_evidence_failure_is_distinguished() -> None:
    """A required metric that failed to load must not read as optional non-representation."""
    spec = SyntheticSpec(
        defects=(
            SyntheticDefect(kind="missing_fixed_subsection", metric_id="service.shopping"),
        )
    )
    ledger = _ledgers(build_profile(spec).model_dump())

    incomplete = [
        entry
        for entry in ledger.values()
        if entry["representation_basis"] == "incomplete_assessed_markets"
    ]
    assert incomplete
    assert any("usable evidence" in entry["permitted_subject_phrase"] for entry in incomplete)


def test_count_entries_name_what_they_count(partial_profile) -> None:
    """A bare "count" phrase would be useless; each must say what was counted."""
    phrases = {
        entry["ledger_id"]: entry["permitted_subject_phrase"]
        for entry in _ledgers(partial_profile).values()
        if entry["statistic"] == "count"
    }

    assert phrases
    partial_optional = [
        phrase for key, phrase in phrases.items() if "partial_optional_items" in key
    ]
    assert partial_optional
    assert all("optional items" in phrase for phrase in partial_optional)


# ---------------------------------------------------------------------------
# Catalog and prompt serialization
# ---------------------------------------------------------------------------


def test_catalog_carries_the_ledger_metadata_unchanged(complete_profile) -> None:
    catalog = build_claim_catalog(complete_profile)
    ledger = _ledgers(complete_profile)

    assert set(catalog) == set(ledger)
    for metric_id, entry in catalog.items():
        source = ledger[metric_id]
        for field in (
            "aggregation_method",
            "population_basis",
            "pooled_denominator_available",
            "representation_basis",
            "permitted_subject_phrase",
        ):
            assert entry[field] == source[field], f"{metric_id}/{field}"


def test_catalog_formatting_is_unchanged_by_r2(complete_profile) -> None:
    """Numeric authorization re-parses these strings, so R2 must not perturb them."""
    catalog = build_claim_catalog(complete_profile)

    for entry in catalog.values():
        expected = _format_catalog_value(
            entry["numeric_value"], entry["unit"], entry["statistic"]
        )
        assert entry["formatted_value"] == expected
        assert entry["allowed_renderings"] == _allowed_renderings(
            entry["numeric_value"], entry["unit"], entry["statistic"]
        ) + ([entry["fact"]["rendered_text"]] if entry.get("fact") else [])


def test_representation_counts_are_available_as_integers(partial_profile) -> None:
    """R3 needs an authorized denominator without parsing the coverage label."""
    catalog = build_claim_catalog(partial_profile)

    partial = [
        entry
        for entry in catalog.values()
        if entry["representation_basis"] == "incomplete_assessed_markets"
        and entry["represented_market_count"] is not None
    ]
    assert partial
    for entry in partial:
        assert entry["represented_market_count"] <= entry["assessed_market_count"]


def test_permitted_claim_scopes_equals_scope_in_r2(complete_profile) -> None:
    """Deliberate pin: R2 must not change claim-scope behaviour.

    Later phases widen this list so one value can back more than one kind of claim, and
    move the scope validator onto it. This test is expected to change then.
    """
    catalog = build_claim_catalog(complete_profile)

    for entry in catalog.values():
        assert entry["permitted_claim_scopes"] == [entry["scope"]]


def test_prompt_catalog_exposes_the_permitted_phrase(complete_profile) -> None:
    catalog = build_claim_catalog(complete_profile)
    ids = dimension_catalog_ids(complete_profile["dimensions"][0])

    projected = compact_catalog(catalog, ids)

    assert projected
    assert all(entry.get("permitted_subject_phrase") for entry in projected)


def test_prompt_catalog_hides_the_classification_enums(complete_profile) -> None:
    """Only the wording reaches the model; validation reads the full catalog itself.

    R3 may deliberately change this if a validator instruction needs an enum.
    """
    catalog = build_claim_catalog(complete_profile)
    ids = dimension_catalog_ids(complete_profile["dimensions"][0])

    projected = compact_catalog(catalog, ids)

    for entry in projected:
        for field in (
            "aggregation_method",
            "population_basis",
            "pooled_denominator_available",
            "representation_basis",
            "permitted_claim_scopes",
        ):
            assert field not in entry


def test_prompt_catalog_stays_within_its_size_budget(complete_profile) -> None:
    """An oversized prompt makes dimension drafting fall back, which is a narrative change."""
    catalog = build_claim_catalog(complete_profile)

    for dimension in complete_profile["dimensions"]:
        projected = compact_catalog(catalog, dimension_catalog_ids(dimension))
        assert len(json.dumps(projected)) < 120_000, dimension["dimension"]
