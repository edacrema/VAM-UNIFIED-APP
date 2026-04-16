"""Databridges discovery and loading helpers for the MFI drafter."""
from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

from app.shared.countries import resolve_country
from app.shared.databridges import get_databridges_client

from .data_loader import load_mfi_from_databridges_rows

_CACHE_TTL_SECONDS = 15 * 60
_SURVEYS_CACHE: tuple[float, list[dict[str, Any]]] | None = None


def reset_mfi_databridges_caches_for_tests() -> None:
    global _SURVEYS_CACHE
    _SURVEYS_CACHE = None


def list_mfi_countries() -> List[Dict[str, Any]]:
    surveys = _all_surveys()
    grouped: dict[str, dict[str, Any]] = {}
    for survey in surveys:
        country_name = _field(survey, "countryName", "country_name")
        iso3 = _field(survey, "iso3Alpha3", "iso3_alpha3")
        adm0_code = _field(survey, "adm0Code", "adm0_code")
        if not country_name:
            continue
        key = str(iso3 or country_name).upper()
        item = grouped.setdefault(
            key,
            {
                "name": str(country_name),
                "iso3": iso3,
                "adm0_code": adm0_code,
                "survey_count": 0,
                "latest_survey_end_date": None,
                "has_data": True,
            },
        )
        item["survey_count"] += 1
        end_date = _field(survey, "surveyEndDate", "survey_end_date")
        if end_date and (item["latest_survey_end_date"] is None or str(end_date) > str(item["latest_survey_end_date"])):
            item["latest_survey_end_date"] = end_date
    return sorted(grouped.values(), key=lambda item: str(item["name"]))


def list_mfi_surveys_for_country(
    country: str,
    *,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> List[Dict[str, Any]]:
    try:
        canonical, iso3 = resolve_country(country)
    except ValueError:
        canonical, iso3 = str(country).strip(), ""
    surveys = []
    for survey in _all_surveys():
        survey_country = str(_field(survey, "countryName", "country_name", default="")).lower()
        survey_iso3 = str(_field(survey, "iso3Alpha3", "iso3_alpha3", default="")).upper()
        if (iso3 and survey_iso3 == iso3) or survey_country == canonical.lower():
            pass
        else:
            continue
        survey_start = _field(survey, "surveyStartDate", "survey_start_date")
        survey_end = _field(survey, "surveyEndDate", "survey_end_date")
        if start_date and survey_end and str(survey_end)[:10] < start_date:
            continue
        if end_date and survey_start and str(survey_start)[:10] > end_date:
            continue
        surveys.append(normalize_survey(survey))
    return sorted(surveys, key=lambda item: str(item.get("survey_end_date") or ""), reverse=True)


def load_mfi_survey_from_databridges(survey_id: int) -> Dict[str, Any]:
    survey = find_survey(survey_id)
    rows = get_databridges_client().list_mfi_processed_data(int(survey_id), page_size=1000)
    return load_mfi_from_databridges_rows(rows, survey=survey)


def find_survey(survey_id: int) -> Optional[Dict[str, Any]]:
    target = int(survey_id)
    for survey in _all_surveys():
        current = _field(survey, "surveyID", "survey_id")
        try:
            if int(current) == target:
                return normalize_survey(survey)
        except (TypeError, ValueError):
            continue
    return None


def normalize_survey(survey: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "survey_id": _field(survey, "surveyID", "survey_id"),
        "survey_start_date": _date_part(_field(survey, "surveyStartDate", "survey_start_date")),
        "survey_end_date": _date_part(_field(survey, "surveyEndDate", "survey_end_date")),
        "survey_original_filename": _field(survey, "surveyOriginalFilename", "survey_original_filename"),
        "survey_name": _field(survey, "surveyName", "survey_name"),
        "xls_form_name": _field(survey, "xlsFormName", "xls_form_name"),
        "base_xls_form_name": _field(survey, "baseXlsFormName", "base_xls_form_name"),
        "country_name": _field(survey, "countryName", "country_name"),
        "adm0_code": _field(survey, "adm0Code", "adm0_code"),
        "iso3_alpha3": _field(survey, "iso3Alpha3", "iso3_alpha3"),
    }


def _all_surveys() -> List[Dict[str, Any]]:
    global _SURVEYS_CACHE
    if _SURVEYS_CACHE is not None:
        created_at, surveys = _SURVEYS_CACHE
        if time.time() - created_at <= _CACHE_TTL_SECONDS:
            return list(surveys)
    surveys = get_databridges_client().list_mfi_surveys()
    _SURVEYS_CACHE = (time.time(), surveys)
    return list(surveys)


def _field(row: Optional[Dict[str, Any]], *names: str, default: Any = None) -> Any:
    if not row:
        return default
    for name in names:
        if name in row:
            return row[name]
    lowered = {str(key).lower(): value for key, value in row.items()}
    for name in names:
        key = name.lower()
        if key in lowered:
            return lowered[key]
    return default


def _date_part(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    return str(value)[:10]
