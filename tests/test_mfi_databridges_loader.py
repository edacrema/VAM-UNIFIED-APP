from app.services.mfi_drafter import databridges_loader
from app.services.mfi_drafter.data_loader import load_mfi_from_databridges_rows
from app.services.mfi_drafter.schemas import SCORE_VARIABLE_MAP


class FakeMFIClient:
    def list_mfi_surveys(self, adm0_code=None, start_date=None, end_date=None):
        return [
            {
                "surveyID": 100,
                "countryName": "South Sudan",
                "iso3Alpha3": "SSD",
                "adm0Code": 74,
                "surveyStartDate": "2025-01-01",
                "surveyEndDate": "2025-01-15",
                "surveyOriginalFilename": "ssd.xlsx",
            },
            {
                "surveyID": 200,
                "countryName": "Kenya",
                "iso3Alpha3": "KEN",
                "adm0Code": 123,
                "surveyStartDate": "2024-01-01",
                "surveyEndDate": "2024-01-15",
            },
        ]

    def list_mfi_processed_data(self, survey_id, page_size=1000):
        return _processed_rows()


def _processed_rows():
    rows = []
    for dimension, variable in SCORE_VARIABLE_MAP.items():
        rows.append(
            {
                "market_name": "Juba",
                "adm0_name": "South Sudan",
                "adm1_name": "Central Equatoria",
                "adm2_name": "Juba",
                "level_id": 1,
                "dimension_name": dimension,
                "variable_name": variable,
                "output_value": 7.5 if dimension == "MFI" else 8.0,
                "traders_sample_size": 12,
                "start_date": "2025-01-01",
                "end_date": "2025-01-15",
                "market_latitude": 4.85,
                "market_longitude": 31.58,
            }
        )
    return rows


def test_databridges_processed_rows_match_existing_graph_shape():
    result = load_mfi_from_databridges_rows(
        _processed_rows(),
        survey={
            "surveyID": 100,
            "countryName": "South Sudan",
            "surveyStartDate": "2025-01-01",
            "surveyEndDate": "2025-01-15",
        },
    )

    assert result["country"] == "South Sudan"
    assert result["data_collection_start"] == "2025-01-01"
    assert result["data_collection_end"] == "2025-01-15"
    assert result["markets"] == ["Juba"]
    assert result["markets_data"][0]["overall_mfi"] == 7.5
    assert result["markets_data"][0]["dimension_scores"]["Availability"] == 8.0
    assert result["dimension_scores"][0]["national_score"] == 8.0
    assert result["survey_metadata"]["survey_id"] == 100


def test_mfi_country_and_survey_discovery(monkeypatch):
    databridges_loader.reset_mfi_databridges_caches_for_tests()
    monkeypatch.setattr(databridges_loader, "get_databridges_client", lambda: FakeMFIClient())

    countries = databridges_loader.list_mfi_countries()
    surveys = databridges_loader.list_mfi_surveys_for_country("South Sudan")
    csv_data = databridges_loader.load_mfi_survey_from_databridges(100)

    assert [country["name"] for country in countries] == ["Kenya", "South Sudan"]
    assert surveys[0]["survey_id"] == 100
    assert csv_data["markets"] == ["Juba"]

