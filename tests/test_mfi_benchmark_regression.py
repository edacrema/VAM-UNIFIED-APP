from __future__ import annotations

import json
from functools import lru_cache
from math import ceil
from pathlib import Path

import pandas as pd
import pytest

from app.services.mfi_drafter.analysis import build_assessment_profile
from app.services.mfi_drafter.data_loader import load_mfi_from_csv
from app.services.mfi_drafter.methodology import OFFICIAL_SCORE_DEFINITIONS
from app.services.mfi_drafter.narrative import (
    build_claim_catalog,
    fallback_dimension_narrative,
    fallback_executive_narrative,
    fallback_market_narrative,
    validate_structured_narratives,
)
from app.services.mfi_drafter.release_validation import (
    MFIExpectedMetricAssertion,
    MFIRegressionCaseConfig,
    MFIReleaseValidationConfig,
    run_regression,
)


BENCHMARK_DIRECTORY = Path(__file__).resolve().parents[1] / "MFI Test Databases"
BENCHMARKS = [
    (
        BENCHMARK_DIRECTORY / "MFI_Full_Benin_surveyid5896.csv",
        53,
        0,
    ),
    (
        BENCHMARK_DIRECTORY / "MFI_Full_Haiti_surveyid5899.csv",
        68,
        6,
    ),
]


def _skip_if_benchmark_absent(path: Path) -> None:
    if not path.exists():
        pytest.skip(
            "Local MFI Test Databases benchmark files are absent; benchmark data "
            "is intentionally not committed."
        )


@lru_cache(maxsize=2)
def _loaded(path_string: str):
    path = Path(path_string)
    _skip_if_benchmark_absent(path)
    frame = pd.read_csv(path)
    frame["VariableName"] = frame["VariableName"].astype("string").str.strip()
    frame["DimensionName"] = frame["DimensionName"].astype("string").str.strip()
    frame["MarketName"] = frame["MarketName"].astype("string").str.strip()
    frame["LevelID"] = pd.to_numeric(frame["LevelID"], errors="coerce")
    frame["OutputValue"] = pd.to_numeric(frame["OutputValue"], errors="coerce")
    return frame, load_mfi_from_csv(path)


@lru_cache(maxsize=2)
def _profile(path_string: str):
    _frame, result = _loaded(path_string)
    return build_assessment_profile(
        result["markets_data"],
        result["metric_summaries"],
        result,
    )


@lru_cache(maxsize=2)
def _phase3_fallback(path_string: str):
    profile = _profile(path_string).model_dump()
    catalog = build_claim_catalog(profile)
    dimensions = {
        item["dimension"]: fallback_dimension_narrative(
            item, assessment_profile=profile
        )
        for item in profile["dimensions"]
    }
    markets = {
        item["market_name"]: fallback_market_narrative(item)
        for item in profile["markets"]
        if item["is_priority_market"]
    }
    executive = fallback_executive_narrative(profile)
    validation = validate_structured_narratives(
        context_evidence=[],
        dimension_narratives=dimensions,
        market_narratives=markets,
        executive_narrative=executive,
        claim_catalog=catalog,
        assessment_profile=profile,
        documents=[],
    )[0]
    return profile, dimensions, validation


@pytest.mark.parametrize(("path", "expected_full", "expected_mfir"), BENCHMARKS)
def test_local_databridge_benchmark_regression(path, expected_full, expected_mfir):
    frame, result = _loaded(str(path))

    assert len(result["markets_data"]) == expected_full
    assert len(result["excluded_market_records"]) == expected_mfir
    assert sum(
        warning["code"] == "mfir_records_excluded"
        for warning in result["methodology_warnings"]
    ) == (1 if expected_mfir else 0)
    assert not {
        "dimension_formula_mismatch",
        "overall_formula_mismatch",
    } & {warning["code"] for warning in result["methodology_warnings"]}

    source_by_market = {
        market: market_frame
        for market, market_frame in frame.groupby("MarketName", sort=False)
    }
    for market in result["markets_data"]:
        source = source_by_market[market["market_name"]]
        for definition in OFFICIAL_SCORE_DEFINITIONS:
            stored = source.loc[
                (source["LevelID"] == 1)
                & (source["DimensionName"] == definition.csv_dimension)
                & (source["VariableName"] == definition.variable_name),
                "OutputValue",
            ].iloc[0]
            canonical = (
                market["overall_mfi"]
                if definition.dimension == "MFI"
                else market["dimension_scores"][definition.dimension]
            )
            assert canonical == float(stored)


@pytest.mark.parametrize(("path", "_expected_full", "_expected_mfir"), BENCHMARKS)
def test_local_benchmark_exact_category_quality_and_competition_semantics(
    path, _expected_full, _expected_mfir
):
    frame, result = _loaded(str(path))
    source_by_market = {
        market: market_frame
        for market, market_frame in frame.groupby("MarketName", sort=False)
    }

    for market in result["markets_data"]:
        source = source_by_market[market["market_name"]]
        availability = {
            metric["metric_id"]: metric
            for metric in market["drivers"]["Availability"]
        }
        price = {
            metric["metric_id"]: metric for metric in market["drivers"]["Price"]
        }
        competition = {
            metric["metric_id"]: metric
            for metric in market["drivers"]["Competition"]
        }
        quality = {
            metric["metric_id"]: metric
            for metric in market["drivers"]["Food Quality"]
        }

        for variable, metric in (
            (
                "AvailabilityScarcity_FCer",
                availability["availability.scarcity.category.cereal_food"],
            ),
            (
                "PriceIncrease_FCer",
                price["price.increase.category.cereal_food"],
            ),
        ):
            rows = source.loc[
                (source["LevelID"] == 5)
                & (source["VariableName"] == variable),
                "OutputValue",
            ]
            if rows.empty or pd.isna(rows.iloc[0]):
                assert metric["raw_value"] is None
            else:
                assert metric["raw_value"] == float(rows.iloc[0])

        quality_metric = quality["quality.condition.food_protection"]
        quality_rows = source.loc[
            (source["LevelID"] == 6)
            & (source["DimensionName"] == "Quality")
            & (source["VariableName"] == "QualityFood"),
            "OutputValue",
        ]
        if quality_rows.empty or pd.isna(quality_rows.iloc[0]):
            assert quality_metric["raw_value"] is None
            assert quality_metric["applicability_status"] == "not_applicable"
        else:
            assert quality_metric["raw_value"] == float(quality_rows.iloc[0])
            assert quality_metric["applicability_status"] == "available"

        assert all(
            metric["orientation"] == "higher_is_better"
            for metric in competition.values()
        )


@pytest.mark.parametrize(
    ("path", "expected_priorities"),
    [
        (
            BENCHMARKS[0][0],
            ["Service", "Infrastructure", "Food Quality"],
        ),
        (
            BENCHMARKS[1][0],
            ["Food Quality", "Infrastructure", "Service", "Price"],
        ),
    ],
)
def test_local_benchmark_phase2_priority_dimensions(path, expected_priorities):
    profile = _profile(str(path))

    assert profile.priority_dimension_names == expected_priorities
    assert len(profile.priority_market_names) == 15
    assert profile.mean_mfi_across_assessed_markets == (
        sum(market.overall_mfi for market in profile.markets)
        / profile.assessed_market_count
    )


def test_local_haiti_phase2_price_subsection_regression():
    path = BENCHMARKS[1][0]
    profile = _profile(str(path))
    price = next(
        dimension for dimension in profile.dimensions if dimension.dimension == "Price"
    )
    subsections = {metric.metric_id: metric for metric in price.subsections}

    assert subsections["price.increase"].mean_normalized_value == pytest.approx(
        9.240196 / 10 * 10,
        abs=1e-6,
    )
    assert subsections["price.stability"].mean_normalized_value == pytest.approx(
        1.691176 / 10 * 10,
        abs=1e-6,
    )


@pytest.mark.parametrize(("path", "expected_full", "_expected_mfir"), BENCHMARKS)
def test_local_benchmark_relevant_items_meet_coverage_and_contrast(
    path, expected_full, _expected_mfir
):
    profile = _profile(str(path))
    all_drivers = {
        metric.metric_id: metric
        for dimension in profile.dimensions
        for metric in dimension.drivers
    }
    required_markets = max(3, ceil(0.25 * expected_full))

    for metric in all_drivers.values():
        if not metric.item_relevant:
            continue
        category = all_drivers[metric.matching_category_metric_id]
        assert metric.coverage.available_market_count >= required_markets
        assert metric.unfavorable_rate is not None
        assert category.unfavorable_rate is not None
        assert metric.unfavorable_rate - category.unfavorable_rate >= (
            0.10 - 1e-6
        )


@pytest.mark.parametrize(
    ("path", "expected_priorities"),
    [
        (BENCHMARKS[0][0], ["Service", "Infrastructure", "Food Quality"]),
        (
            BENCHMARKS[1][0],
            ["Food Quality", "Infrastructure", "Service", "Price"],
        ),
    ],
)
def test_local_benchmark_phase3_fallback_is_evidence_backed(
    path, expected_priorities
):
    profile, narratives, validation = _phase3_fallback(str(path))

    assert profile["priority_dimension_names"] == expected_priorities
    assert validation["status"] == "passed"
    for dimension in expected_priorities:
        narrative = narratives[dimension]
        assert narrative["subdimension_analysis"]
        driver_ids = {
            metric_id
            for item in narrative["subdimension_analysis"]
            for metric_id in item["driver_metric_ids"]
        }
        assert 2 <= len(driver_ids) <= 4
        assert narrative["geographic_patterns"]
        assert narrative["recommendations"]


def test_local_benchmarks_build_phase4_release_evidence(tmp_path):
    for path, _expected_full, _expected_mfir in BENCHMARKS:
        _skip_if_benchmark_absent(path)

    validation_config = MFIReleaseValidationConfig(
        cases=[
            MFIRegressionCaseConfig(
                case_id="benin-example",
                label="Benin local regression example",
                source_csv=BENCHMARKS[0][0].name,
                expected_included_market_count=53,
                expected_excluded_market_count=0,
                expected_priority_dimensions=[
                    "Service",
                    "Infrastructure",
                    "Food Quality",
                ],
            ),
            MFIRegressionCaseConfig(
                case_id="haiti-example",
                label="Haiti local regression example",
                source_csv=BENCHMARKS[1][0].name,
                expected_included_market_count=68,
                expected_excluded_market_count=6,
                expected_priority_dimensions=[
                    "Food Quality",
                    "Infrastructure",
                    "Service",
                    "Price",
                ],
                expected_methodology_warning_codes=[
                    "mfir_records_excluded"
                ],
                # Since R1, optional item non-representation is disclosed as coverage
                # rather than reported as unavailable evidence, so this benchmark no
                # longer expects that limitation.
                expected_limitation_codes=[
                    "assessment_scope_not_representative",
                    "item_trader_denominator_unavailable",
                    "mfir_records_excluded",
                ],
                metric_assertions=[
                    MFIExpectedMetricAssertion(
                        assertion_id="price-increase-mean",
                        dimension="Price",
                        metric_id="price.increase",
                        expected_value=9.240196,
                    ),
                    MFIExpectedMetricAssertion(
                        assertion_id="price-stability-mean",
                        dimension="Price",
                        metric_id="price.stability",
                        expected_value=1.691176,
                    ),
                ],
            ),
        ]
    )
    manifest = run_regression(
        validation_config=validation_config,
        case_root=BENCHMARK_DIRECTORY,
        output_directory=tmp_path / "phase4",
        release_id="pytest-phase4",
        candidate_revision="pytest-candidate",
    )

    assert manifest.release_ready is False
    assert manifest.blockers == [
        "approval:benin-example",
        "approval:haiti-example",
        "pilot:real_assessments",
        "pilot:regression_case:benin-example",
        "pilot:regression_case:haiti-example",
    ]
    assert {
        case.case_id for case in manifest.regression_cases
    } == {"benin-example", "haiti-example"}
    for case in manifest.regression_cases:
        assert not [
            check
            for check in case.checks
            if check.blocking and check.status == "failed"
        ]
        assert all(
            len(artifact.sha256) == 64 for artifact in case.artifacts
        )
        result_artifact = next(
            artifact
            for artifact in case.artifacts
            if artifact.artifact_id.endswith(".result")
        )
        rendered = json.loads(
            (tmp_path / "phase4" / result_artifact.path).read_text(
                encoding="utf-8"
            )
        )
        assert rendered["llm_calls"] == 0
        assert rendered["generation_diagnostics"]["dimensions"]["llm"] == []
        assert rendered["generation_diagnostics"]["retrievers"] == {}
