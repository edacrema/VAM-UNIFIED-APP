from __future__ import annotations

from copy import deepcopy

from app.services.mfi_drafter import graph, router
from app.streamlit_backend import dispatcher


def _typed_market():
    metric = {
        "metric_id": "availability.scarcity",
        "dimension": "Availability",
        "display_name": "Availability scarcity component",
        "variable_name": "AvailabilityScarcity",
        "source_level_id": 4,
        "source_level_name": "Trader Median",
        "role": "official_subsection",
        "raw_value": 3.0,
        "raw_min": 0.0,
        "raw_max": 6.0,
        "normalized_value": 5.0,
        "orientation": "higher_is_better",
        "unit": "score",
        "evidence_scope": "assessed_market",
        "observed_raw_values": [3.0],
        "market_coverage": 1,
        "market_coverage_total": 1,
        "missing_count": 0,
        "applicability_status": "available",
        "validation_status": "valid",
        "methodology_note": "Synthetic typed test evidence.",
    }
    dimensions = {dimension: 5.0 for dimension in graph.MFI_DIMENSIONS}
    return {
        "market_name": "Juba",
        "admin0": "South Sudan",
        "admin1": "Central Equatoria",
        "admin2": "Juba",
        "region": "Central Equatoria",
        "overall_mfi": 5.0,
        "dimension_scores": dimensions,
        "subsections": {
            dimension: [deepcopy(metric)] if dimension == "Availability" else []
            for dimension in graph.MFI_DIMENSIONS
        },
        "drivers": {dimension: [] for dimension in graph.MFI_DIMENSIONS},
        "risk_level": "High Risk",
        "traders_surveyed": 12,
        "latitude": None,
        "longitude": None,
    }


def _canonical_result():
    warning = {
        "code": "mfir_records_excluded",
        "severity": "warning",
        "message": "Excluded one MFIr-only record.",
        "market_name": None,
        "dimension": None,
        "metric_ids": [],
        "expected_value": None,
        "actual_value": None,
        "delta": None,
        "tolerance": None,
    }
    market = _typed_market()
    return {
        "run_id": "mfi-test",
        "country": "South Sudan",
        "data_collection_start": "2026-01-01",
        "data_collection_end": "2026-01-31",
        "analysis_schema_version": "2.0",
        "methodology_version": "databridge-current",
        "score_authority": "databridge_level_1",
        "excluded_market_records": [
            {
                "market_name": "MFIr Market",
                "detected_record_type": "mfir_only",
                "reason": "MFIr only",
                "available_level1_variables": ["MFIScoreMFIr"],
                "missing_level1_variables": ["MFIScoreMFI"],
            }
        ],
        "methodology_warnings": [warning],
        "warnings": [warning["message"]],
        "markets_data": [market],
        "dimension_scores": [
            {
                "dimension": dimension,
                "national_score": 5.0,
                "regional_scores": {"Central Equatoria": 5.0},
                "market_scores": {"Juba": 5.0},
            }
            for dimension in graph.MFI_DIMENSIONS
        ],
        "metric_summaries": {
            dimension: [] for dimension in graph.MFI_DIMENSIONS
        },
        "survey_metadata": {
            "country": "South Sudan",
            "collection_period": "2026-01-01 to 2026-01-31",
            "total_traders": 12,
            "total_markets": 1,
            "regions_covered": ["Central Equatoria"],
        },
        "executive_summary": "",
        "dimension_findings": {},
        "market_recommendations": {},
        "visualizations": {},
    }


def test_graph_data_node_propagates_typed_methodology_fields_and_warnings():
    data = _canonical_result()
    state = graph.create_initial_state(
        country=data["country"],
        data_collection_start=data["data_collection_start"],
        data_collection_end=data["data_collection_end"],
        markets=["Juba"],
        csv_data=data,
    )

    update = graph.node_mfi_data_agent(state)

    assert update["score_authority"] == "databridge_level_1"
    assert update["analysis_schema_version"] == "2.0"
    assert update["methodology_warnings"][0]["code"] == "mfir_records_excluded"
    assert update["excluded_market_records"][0]["market_name"] == "MFIr Market"
    assert update["warnings"] == ["Excluded one MFIr-only record."]
    assert "sub_scores" not in update["markets_data"][0]


def test_mock_data_is_typed_and_cannot_masquerade_as_databridge():
    result = graph.generate_mock_mfi_data(
        "Testland", ["Market A"], "2026-01-01", "2026-01-31"
    )

    assert result["score_authority"] == "synthetic_mock"
    assert result["analysis_schema_version"] == "2.0"
    assert result["markets_data"][0]["subsections"]
    assert result["markets_data"][0]["drivers"]
    assert "sub_scores" not in result["markets_data"][0]


def test_router_adds_deprecated_alias_only_to_serialized_copy(monkeypatch):
    result = _canonical_result()
    monkeypatch.setattr(router, "resolve_mfi_report_blocks", lambda *_args, **_kwargs: [])

    output = router._build_mfi_output(
        result=result,
        country=result["country"],
        data_collection_start=result["data_collection_start"],
        data_collection_end=result["data_collection_end"],
    )

    assert output.score_authority == "databridge_level_1"
    assert output.methodology_warnings[0].code == "mfir_records_excluded"
    assert output.mean_mfi_across_assessed_markets == 5.0
    assert output.assessment_profile.assessed_market_count == 1
    assert output.assessment_profile.excluded_market_count == 1
    assert output.assessment_profile.priority_market_names == ["Juba"]
    assert "sub_scores" in output.markets_data[0]
    assert "sub_scores" not in result["markets_data"][0]


def test_dispatcher_adds_deprecated_alias_only_to_serialized_copy(monkeypatch):
    result = _canonical_result()
    monkeypatch.setattr(
        dispatcher,
        "resolve_mfi_report_blocks",
        lambda *_args, **_kwargs: [],
    )

    output = dispatcher._build_mfi_report_output(
        result=result,
        run_id=result["run_id"],
        country=result["country"],
        data_collection_start=result["data_collection_start"],
        data_collection_end=result["data_collection_end"],
    )

    assert output["score_authority"] == "databridge_level_1"
    assert output["methodology_warnings"][0]["code"] == "mfir_records_excluded"
    assert output["mean_mfi_across_assessed_markets"] == 5.0
    assert output["assessment_profile"]["assessed_market_count"] == 1
    assert output["assessment_profile"]["excluded_market_count"] == 1
    assert output["assessment_profile"]["priority_market_names"] == ["Juba"]
    assert "sub_scores" in output["markets_data"][0]
    assert "sub_scores" not in result["markets_data"][0]


def test_analysis_metadata_is_exposed_by_router_and_dispatcher_helpers():
    profile = {
        "analysis_version": "mfi-analysis-phase2-v1",
        "analysis_schema_version": "2.0",
        "priority_dimension_names": ["Service"],
        "priority_market_names": ["Juba"],
        "limitations": [{"code": "assessment_scope_not_representative"}],
    }
    state = {
        "assessment_profile": profile,
        "methodology_warnings": [{"code": "mfir_records_excluded"}],
        "narrative_schema_version": "2.0",
        "claim_validation": {"status": "passed"},
        "qa_review": {"status": "passed"},
    }

    router_metadata = router._analysis_run_metadata(state)
    dispatcher_metadata = dispatcher._mfi_analysis_run_metadata(state)

    assert router_metadata == dispatcher_metadata
    assert router_metadata["analysis_version"] == "mfi-analysis-phase2-v1"
    assert router_metadata["priority_dimension_names"] == ["Service"]
    assert router_metadata["priority_market_names"] == ["Juba"]
    assert router_metadata["analysis_limitations"][0]["code"] == (
        "assessment_scope_not_representative"
    )
    assert router_metadata["methodology_warnings"][0]["code"] == (
        "mfir_records_excluded"
    )
    assert router_metadata["narrative_schema_version"] == "2.0"
    assert router_metadata["claim_validation"]["status"] == "passed"
    assert router_metadata["qa_review"]["status"] == "passed"
