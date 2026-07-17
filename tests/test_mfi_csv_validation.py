from __future__ import annotations

import pandas as pd
import pytest

from app.services.mfi_drafter.data_loader import (
    load_mfi_from_dataframe,
    validate_csv_structure,
)


def _mfi_frame(**overrides):
    row = {
        "MarketName": "Juba",
        "Adm0Name": "South Sudan",
        "Adm1Name": "Central Equatoria",
        "LevelID": 1,
        "DimensionName": "MFI",
        "VariableName": "MFIScoreMFI",
        "OutputValue": 6.2,
        "TradersSampleSize": 12,
        "StartDate": "2026-01-01",
        "EndDate": "2026-01-31",
    }
    row.update(overrides)
    return pd.DataFrame([row])


def _csv_bytes(frame):
    return frame.to_csv(index=False).encode("utf-8")


def test_csv_validation_requires_valid_collection_dates():
    missing = _mfi_frame().drop(columns=["StartDate", "EndDate"])
    blank = _mfi_frame(StartDate="", EndDate=None)
    invalid = _mfi_frame(StartDate="not-a-date", EndDate="also-not-a-date")

    for frame in (missing, blank, invalid):
        result = validate_csv_structure(_csv_bytes(frame))
        assert result["valid"] is False
        assert result["missing_metadata_fields"] == ["StartDate", "EndDate"]
        assert "Missing or invalid required collection metadata" in result["errors"][0]


def test_csv_validation_accepts_valid_collection_dates():
    result = validate_csv_structure(_csv_bytes(_mfi_frame()))

    assert result["valid"] is True
    assert result["missing_metadata_fields"] == []


def test_loader_rejects_missing_dates_without_overrides():
    frame = _mfi_frame().drop(columns=["StartDate", "EndDate"])

    with pytest.raises(ValueError, match="requires a valid StartDate"):
        load_mfi_from_dataframe(frame)


def test_loader_accepts_api_date_overrides_when_csv_dates_are_missing():
    frame = _mfi_frame().drop(columns=["StartDate", "EndDate"])

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
            _mfi_frame(),
            start_date_override="not-a-date",
        )
