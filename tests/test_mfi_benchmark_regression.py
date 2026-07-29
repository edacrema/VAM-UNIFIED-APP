from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import pandas as pd
import pytest

from app.services.mfi_drafter.data_loader import load_mfi_from_csv
from app.services.mfi_drafter.methodology import OFFICIAL_SCORE_DEFINITIONS


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
