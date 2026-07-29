from __future__ import annotations

import pandas as pd
import pytest

from app.services.mfi_drafter.data_loader import (
    load_mfi_from_dataframe,
    validate_csv_structure,
)
from app.services.mfi_drafter.methodology import (
    OFFICIAL_SCORE_DEFINITIONS,
    SUBSECTION_DEFINITIONS,
    calculate_current_databridge_mfi,
    calculate_dimension_score,
)


COMPONENT_VALUES = {
    "assortment.breadth": 5.0,
    "assortment.depth": 3.0,
    "availability.scarcity": 3.0,
    "availability.runout": 3.0,
    "price.increase": 4.0,
    "price.stability": 4.0,
    "resilience.responsiveness": 2.0,
    "resilience.vulnerability": 4.0,
    "competition.concentration": 3.0,
    "competition.monopoly": 3.0,
    "infrastructure.condition": 4.0,
    "infrastructure.features": 6.0,
    "service.shopping": 2.0,
    "service.checkout": 2.0,
    "quality.measure": 6.0,
    "quality.maximum": 8.0,
    "access_protection.access": 4.0,
    "access_protection.protection": 4.0,
}


def _row(
    *,
    market: str,
    level: int,
    dimension: str,
    variable: str,
    value,
    **overrides,
):
    row = {
        "MarketName": market,
        "Adm0Name": "South Sudan",
        "Adm1Name": "Central Equatoria",
        "LevelID": level,
        "DimensionName": dimension,
        "VariableName": variable,
        "OutputValue": value,
        "TradersSampleSize": 12,
        "StartDate": "2026-01-01",
        "EndDate": "2026-01-31",
    }
    row.update(overrides)
    return row


def _complete_market(market: str = "Juba") -> pd.DataFrame:
    dimension_values = {
        dimension: calculate_dimension_score(dimension, COMPONENT_VALUES)
        for dimension in (
            "Assortment",
            "Availability",
            "Price",
            "Resilience",
            "Competition",
            "Infrastructure",
            "Service",
            "Food Quality",
            "Access & Protection",
        )
    }
    overall = calculate_current_databridge_mfi(dimension_values.values())
    rows = []
    for definition in OFFICIAL_SCORE_DEFINITIONS:
        value = (
            overall
            if definition.dimension == "MFI"
            else dimension_values[definition.dimension]
        )
        rows.append(
            _row(
                market=market,
                level=definition.source_level_id,
                dimension=definition.csv_dimension,
                variable=definition.variable_name,
                value=value,
            )
        )
    for definition in SUBSECTION_DEFINITIONS:
        rows.append(
            _row(
                market=market,
                level=definition.source_level_id,
                dimension=definition.csv_dimension,
                variable=definition.variable_name,
                value=COMPONENT_VALUES[definition.metric_id],
            )
        )
    return pd.DataFrame(rows)


def _csv_bytes(frame: pd.DataFrame) -> bytes:
    return frame.to_csv(index=False).encode("utf-8")


def _metric(result, market_index: int, group: str, dimension: str, metric_id: str):
    metrics = result["markets_data"][market_index][group][dimension]
    return next(metric for metric in metrics if metric["metric_id"] == metric_id)


def test_csv_validation_requires_valid_collection_dates():
    missing = _complete_market().drop(columns=["StartDate", "EndDate"])
    blank = _complete_market()
    blank["StartDate"] = ""
    blank["EndDate"] = None
    invalid = _complete_market()
    invalid["StartDate"] = "not-a-date"
    invalid["EndDate"] = "also-not-a-date"

    for frame in (missing, blank, invalid):
        result = validate_csv_structure(_csv_bytes(frame))
        assert result["valid"] is False
        assert result["missing_metadata_fields"] == ["StartDate", "EndDate"]
        assert "Missing or invalid required collection metadata" in result["errors"][0]


def test_csv_validation_accepts_complete_full_mfi():
    result = validate_csv_structure(_csv_bytes(_complete_market()))

    assert result["valid"] is True
    assert result["missing_metadata_fields"] == []
    assert result["errors"] == []


def test_csv_validation_reads_beyond_the_old_one_thousand_row_limit():
    padding = pd.DataFrame(
        [
            _row(
                market="Juba",
                level=5,
                dimension="Unregistered",
                variable=f"UnregisteredVariable{index}",
                value=0.5,
            )
            for index in range(1001)
        ]
    )
    frame = pd.concat([padding, _complete_market()], ignore_index=True)

    result = validate_csv_structure(_csv_bytes(frame))

    assert result["valid"] is True
    assert result["has_normalized_scores"] is True


def test_loader_preserves_authoritative_scores_and_has_no_internal_alias():
    frame = _complete_market()
    official = frame.loc[frame["VariableName"] == "MFIScoreMFI", "OutputValue"].iloc[0]

    result = load_mfi_from_dataframe(frame)

    market = result["markets_data"][0]
    assert market["overall_mfi"] == official
    assert "sub_scores" not in market
    assert result["analysis_schema_version"] == "2.0"
    assert result["methodology_version"] == "databridge-current"
    assert result["score_authority"] == "databridge_level_1"


def test_loader_rejects_missing_dates_without_overrides():
    frame = _complete_market().drop(columns=["StartDate", "EndDate"])

    with pytest.raises(ValueError, match="requires a valid StartDate"):
        load_mfi_from_dataframe(frame)


def test_loader_accepts_api_date_overrides_when_csv_dates_are_missing():
    frame = _complete_market().drop(columns=["StartDate", "EndDate"])

    result = load_mfi_from_dataframe(
        frame,
        start_date_override="2026-02-01",
        end_date_override="2026-02-28",
    )

    assert result["data_collection_start"] == "2026-02-01"
    assert result["data_collection_end"] == "2026-02-28"


def test_loader_rejects_invalid_api_date_override():
    with pytest.raises(ValueError, match="requires a valid StartDate"):
        load_mfi_from_dataframe(
            _complete_market(),
            start_date_override="not-a-date",
        )


@pytest.mark.parametrize("mutation", ["partial", "duplicate", "non_numeric", "out_of_range"])
def test_loader_rejects_invalid_authoritative_level_one(mutation):
    frame = _complete_market()
    target = frame["VariableName"] == "AvailabilityScoreMFI"
    if mutation == "partial":
        frame = frame.loc[~target]
    elif mutation == "duplicate":
        frame = pd.concat([frame, frame.loc[target]], ignore_index=True)
    elif mutation == "non_numeric":
        frame["OutputValue"] = frame["OutputValue"].astype(object)
        frame.loc[target, "OutputValue"] = "not-numeric"
    else:
        frame.loc[target, "OutputValue"] = 11

    with pytest.raises(ValueError, match="Invalid or incomplete Full MFI"):
        load_mfi_from_dataframe(frame)


def test_mfir_only_record_is_excluded_with_structured_summary_warning():
    full = _complete_market()
    mfir = pd.DataFrame(
        [
            _row(
                market="MFIr Market",
                level=1,
                dimension="MFIr",
                variable="MFIScoreMFIr",
                value=6.1,
            )
        ]
    )

    result = load_mfi_from_dataframe(pd.concat([full, mfir], ignore_index=True))

    assert result["markets"] == ["Juba"]
    assert result["excluded_market_records"][0]["market_name"] == "MFIr Market"
    assert (
        result["excluded_market_records"][0]["detected_record_type"] == "mfir_only"
    )
    assert [warning["code"] for warning in result["methodology_warnings"]] == [
        "mfir_records_excluded"
    ]


def test_missing_required_subsection_degrades_without_replacing_official_score():
    frame = _complete_market()
    official = frame.loc[
        frame["VariableName"] == "AvailabilityScoreMFI", "OutputValue"
    ].iloc[0]
    frame = frame.loc[frame["VariableName"] != "AvailabilityScarcity"]

    result = load_mfi_from_dataframe(frame)
    metric = _metric(
        result, 0, "subsections", "Availability", "availability.scarcity"
    )

    assert result["markets_data"][0]["dimension_scores"]["Availability"] == official
    assert metric["raw_value"] is None
    assert metric["normalized_value"] is None
    assert metric["applicability_status"] == "missing"
    assert metric["validation_status"] == "missing"
    assert any(
        warning["code"] == "evidence_missing"
        for warning in result["methodology_warnings"]
    )


def test_out_of_range_and_duplicate_subsections_preserve_raw_audit_values():
    frame = _complete_market()
    target = frame["VariableName"] == "AvailabilityScarcity"
    frame.loc[target, "OutputValue"] = 7.0
    duplicate = frame.loc[target].copy()
    duplicate["OutputValue"] = 2.0
    frame = pd.concat([frame, duplicate], ignore_index=True)

    result = load_mfi_from_dataframe(frame)
    metric = _metric(
        result, 0, "subsections", "Availability", "availability.scarcity"
    )

    assert metric["validation_status"] == "duplicate"
    assert metric["normalized_value"] is None
    assert metric["observed_raw_values"] == [7.0, 2.0]


def test_formula_mismatch_marks_evidence_unusable_but_retains_official_score():
    frame = _complete_market()
    official = frame.loc[
        frame["VariableName"] == "AvailabilityScoreMFI", "OutputValue"
    ].iloc[0]
    frame.loc[
        frame["VariableName"] == "AvailabilityScarcity", "OutputValue"
    ] = 2.0

    result = load_mfi_from_dataframe(frame)
    scarcity = _metric(
        result, 0, "subsections", "Availability", "availability.scarcity"
    )
    runout = _metric(
        result, 0, "subsections", "Availability", "availability.runout"
    )

    assert result["markets_data"][0]["dimension_scores"]["Availability"] == official
    assert scarcity["raw_value"] == 2.0
    assert scarcity["normalized_value"] is None
    assert scarcity["validation_status"] == "formula_mismatch"
    assert runout["validation_status"] == "formula_mismatch"
    warning = next(
        warning
        for warning in result["methodology_warnings"]
        if warning["code"] == "dimension_formula_mismatch"
    )
    assert warning["actual_value"] == official
    assert warning["expected_value"] != official
    assert warning["tolerance"] == 1e-6


def test_overall_formula_mismatch_never_replaces_stored_mfi():
    frame = _complete_market()
    frame.loc[frame["VariableName"] == "MFIScoreMFI", "OutputValue"] = 4.25

    result = load_mfi_from_dataframe(frame)

    assert result["markets_data"][0]["overall_mfi"] == 4.25
    warning = next(
        warning
        for warning in result["methodology_warnings"]
        if warning["code"] == "overall_formula_mismatch"
    )
    assert warning["actual_value"] == 4.25
    assert warning["expected_value"] != 4.25


def test_missing_driver_applicability_is_null_aware_and_never_defaulted():
    result = load_mfi_from_dataframe(_complete_market())
    quality = _metric(
        result, 0, "drivers", "Food Quality", "quality.condition.separation"
    )
    optional_item = _metric(
        result,
        0,
        "drivers",
        "Availability",
        "availability.scarcity.item.cereal_food.barley",
    )
    fixed = _metric(
        result,
        0,
        "drivers",
        "Resilience",
        "resilience.responsiveness.current_stock",
    )

    assert (quality["raw_value"], quality["normalized_value"]) == (None, None)
    assert quality["applicability_status"] == "not_applicable"
    assert optional_item["applicability_status"] == "not_represented"
    assert fixed["applicability_status"] == "missing"


def test_exact_category_and_item_metrics_do_not_collide():
    frame = _complete_market()
    frame = pd.concat(
        [
            frame,
            pd.DataFrame(
                [
                    _row(
                        market="Juba",
                        level=5,
                        dimension="Availability",
                        variable="AvailabilityScarcity_FCer",
                        value=0.8,
                    ),
                    _row(
                        market="Juba",
                        level=5,
                        dimension="Availability",
                        variable="AvailabilityScarcity_FCerBarley",
                        value=0.2,
                    ),
                ]
            ),
        ],
        ignore_index=True,
    )

    result = load_mfi_from_dataframe(frame)
    category = _metric(
        result,
        0,
        "drivers",
        "Availability",
        "availability.scarcity.category.cereal_food",
    )
    item = _metric(
        result,
        0,
        "drivers",
        "Availability",
        "availability.scarcity.item.cereal_food.barley",
    )

    assert category["raw_value"] == 0.8
    assert item["raw_value"] == 0.2
