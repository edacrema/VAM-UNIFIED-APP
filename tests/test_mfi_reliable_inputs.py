from pathlib import Path
import json

import pandas as pd
import pytest

from app.services.mfi_drafter.data_loader import load_mfi_from_csv, load_mfi_from_dataframe
from app.services.mfi_drafter.analysis import build_assessment_profile
from app.services.mfi_drafter.input_validation import MFIInputError

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def market_frame():
    path = ROOT / "MFI Test Databases/MFI_Full_Benin_surveyid5896.csv"
    if not path.exists():
        pytest.skip("Local benchmark absent")
    frame = pd.read_csv(path, dtype=str)
    return frame[frame.MarketID == "124"].copy()


@pytest.mark.parametrize("country,survey", [("Benin",5896),("Haiti",5899)])
def test_authoritative_benchmark_unchanged(country, survey, monkeypatch):
    path = ROOT / f"MFI Test Databases/MFI_Full_{country}_surveyid{survey}.csv"
    if not path.exists():
        pytest.skip("Local benchmark absent")
    expected = json.loads((ROOT / "tests/fixtures/mfi_reliable_baseline.json").read_text(encoding="utf-8"))[country]
    data = load_mfi_from_csv(path)
    profile = build_assessment_profile(data["markets_data"], data["metric_summaries"], data).model_dump()
    assert len(data["markets_data"]) == expected["count"]
    assert len(data["excluded_market_records"]) == expected["excluded"]
    assert profile["mean_mfi_across_assessed_markets"] == expected["mean"]
    assert profile["priority_dimension_names"] == expected["priorities"]
    assert profile["priority_market_names"] == expected["selected"]
    if country == "Benin":
        facts = profile["analytical_facts"]
        assert facts["fact.assessment.service.at_or_below_median"]["numerator"] == 37
        assert facts["fact.assessment.service.at_or_below_median"]["denominator"] == 53
        assert facts["fact.assessment.infrastructure.below_3"]["numerator"] == 7
        assert facts["fact.region.food_quality.minimum"]["subject"] == "Oueme"
        assert facts["fact.region.food_quality.minimum"]["value"] == pytest.approx(4.7916666667)
    for market in data["markets_data"]:
        assert {k:market[k] for k in ["overall_mfi","dimension_scores","traders_surveyed"]} == expected["markets"][market["market_name"]]
    for metrics in data["metric_summaries"].values():
        for metric in metrics:
            assert {k:metric[k] for k in ["mean_raw_value","mean_normalized_value","aggregation_denominator"]} == expected["metrics"][metric["metric_id"]]

    # Exercise the actual outgoing dimension/review builders against both full inputs.
    from app.services.mfi_drafter import graph
    from app.services.mfi_drafter.narrative import build_claim_catalog
    from app.services.mfi_drafter.reliable_nodes import complete_dimension_node
    from app.services.mfi_drafter.packages import ensure_message_budget
    from app.services.mfi_drafter.review import review_packages
    from types import SimpleNamespace
    catalog = build_claim_catalog(profile)
    captured = []
    def invoke(**kwargs):
        ensure_message_budget(kwargs["messages"])
        packet = json.loads(kwargs["messages"][0].content.rsplit("\n",1)[-1])
        assert len(packet["dimension"]["market_comparators"]) == expected["count"]
        captured.append(packet["dimension"]["dimension"])
        return SimpleNamespace(value={"summary":{"text":"Observed dimension evidence.", "metric_ids":packet["dimension"]["ledger_metric_ids"],
            "document_ids":[],"scope":"assessment","polarity":"neutral"}, "key_findings":[],"subdimension_analysis":[],
            "geographic_patterns":[],"data_limitations":[],"recommendations":[]}, payload={}, call_id="dry-run"), 0
    monkeypatch.setattr(graph,"get_model",lambda **kwargs: object())
    monkeypatch.setattr(graph,"_invoke_json_with_one_normalization",invoke)
    state = {"run_id":"dry-"+country,"assessment_profile":profile,"claim_catalog":catalog}
    update = complete_dimension_node(state)
    assert len(set(captured)) == 9
    reviews = review_packages(assessment_profile=profile, claim_catalog=catalog, dimension_narratives=update["dimension_narratives"],
        market_narratives={}, executive_narrative={}, context_evidence=[], documents=[])
    for review in reviews:
        score_rows = [entry for entry in review["package"]["evidence_by_metric_id"].values() if entry.get("statistic") == "stored_level_1_score"]
        assert len(score_rows) == expected["count"]
        assert review["character_count"] <= 125000

    from app.services.mfi_drafter import reliable_nodes
    from app.services.mfi_drafter.packages import bounded_groups
    from app.services.mfi_drafter.correction import correction_request
    from app.shared.llm_observability import serialize_messages
    from app.services.mfi_drafter.simple_orchestration import build_budgeted_market_draft_batches
    market_batches = build_budgeted_market_draft_batches(profile, catalog)
    assert all(batch["prompt_character_count"] <= 140000 for batch in market_batches)

    class BudgetsChecked(Exception):
        pass

    def check_correction_packages(**kwargs):
        fingerprints = {row["task_id"]: "0" * 64 for row in kwargs["targets"]}
        def package(rows):
            messages, schema, _ = correction_request("claim", rows, kwargs["build_payload"], fingerprints)
            return {"messages": serialize_messages(messages), "response_schema": schema}
        groups = bounded_groups(kwargs["targets"], package, maximum_items=4, maximum_characters=155000)
        assert sum(map(len, groups)) == 9
        for group in groups:
            messages, schema, _ = correction_request("claim", group, kwargs["build_payload"], fingerprints)
            ensure_message_budget(messages, schema)
        raise BudgetsChecked

    monkeypatch.setattr(reliable_nodes, "correct_targets", check_correction_packages)
    flags = [{"flag_id": "check-"+name, "severity":"high", "repairable":True,
              "artifact_type":"dimension", "artifact_id":name, "field_name":"summary",
              "claim_id":"summary", "message":"Verify against the full evidence", "code":"numeric_value_mismatch"}
             for name in update["dimension_narratives"]]
    with pytest.raises(BudgetsChecked):
        reliable_nodes.typed_correction_node({**state, **update, "deterministic_flags":flags})


def test_aliases_do_not_duplicate_traders(market_frame):
    alias = market_frame.assign(MarketName="Dantokpa alternate")
    data = load_mfi_from_dataframe(pd.concat([market_frame, alias], ignore_index=True))
    assert len(data["markets_data"]) == 1
    assert data["survey_metadata"]["total_traders"] == 24
    assert len(data["markets_data"][0]["identity"]["aliases"]) == 2


def test_shared_names_have_separate_identifiers(market_frame):
    other = market_frame.assign(MarketID="999999")
    data = load_mfi_from_dataframe(pd.concat([market_frame, other], ignore_index=True))
    assert len(data["markets_data"]) == 2
    assert len({m["market_name"] for m in data["markets_data"]}) == 2
    assert len({m["market_key"] for m in data["markets_data"]}) == 2


def test_identity_is_independent_of_row_order(market_frame):
    a = load_mfi_from_dataframe(market_frame)
    b = load_mfi_from_dataframe(market_frame.iloc[::-1])
    for key in ("market_key", "overall_mfi", "dimension_scores", "traders_surveyed", "identity"):
        assert a["markets_data"][0][key] == b["markets_data"][0][key]
    assert a["survey_metadata"] == b["survey_metadata"]


@pytest.mark.parametrize("column,value", [("SurveyID","wrong"),("Adm0Name","Haiti"),("Adm0Code","999")])
def test_mixed_assessment_rejected(market_frame,column,value):
    mutated=market_frame.copy()
    mutated.loc[mutated.index[0],column]=value
    with pytest.raises(MFIInputError,match="multiple assessment"):
        load_mfi_from_dataframe(mutated)


@pytest.mark.parametrize("value",["-5","2.9","broken",None])
def test_invalid_trader_counts_are_unknown(market_frame,value):
    data=load_mfi_from_dataframe(market_frame.assign(TradersSampleSize=value))
    assert data["survey_metadata"]["total_traders"] is None
    assert data["markets_data"][0]["traders_surveyed"] is None
    assert data["input_findings"]


def test_bad_coordinates_do_not_remove_market(market_frame):
    data=load_mfi_from_dataframe(market_frame.assign(MarketLatitude="500",MarketLongitude="-999"))
    assert data["markets_data"][0]["latitude"] is None
    assert data["markets_data"][0]["longitude"] is None
    assert data["markets_data"][0]["overall_mfi"] > 0


def test_bad_optional_value_is_not_non_applicability(market_frame):
    data=market_frame.copy()
    mask=data.VariableName.str.strip() == "QualityRefrigerate"
    assert mask.any()
    data.loc[mask,"OutputValue"]="broken"
    loaded=load_mfi_from_dataframe(data)
    metric=next(m for m in loaded["markets_data"][0]["drivers"]["Food Quality"] if m["variable_name"] == "QualityRefrigerate")
    assert metric["applicability_status"] == "missing"
    assert metric["parsing_status"] == "nonnumeric"
    assert metric["applicability_basis"] == "unknown"
    assert metric["source_values"] == ["broken"]
    assert metric["source_rows"]
    finding = next(item for item in loaded["input_findings"] if "QualityRefrigerate" in item["fields"])
    assert finding["row_references"] == metric["source_rows"]
    assert finding["market_key"] == loaded["markets_data"][0]["market_key"]


def test_conflicting_official_score_has_structured_source_findings(market_frame):
    score = market_frame[market_frame.LevelID == "1"].iloc[[0]].copy()
    score["OutputValue"] = "99"
    with pytest.raises(MFIInputError) as failure:
        load_mfi_from_dataframe(pd.concat([market_frame, score], ignore_index=True))
    assert failure.value.findings[0]["code"] == "invalid_official_scores"
    assert failure.value.findings[0]["market_key"]
    assert failure.value.findings[0]["row_references"]


def test_explicit_non_applicability_remains_distinct_from_empty_value(market_frame):
    frame = market_frame.copy()
    frame["MetricApplicability"] = ""
    selected = frame.VariableName.str.strip() == "QualityRefrigerate"
    frame.loc[selected, "MetricApplicability"] = "not_applicable"
    frame.loc[selected, "OutputValue"] = ""
    loaded = load_mfi_from_dataframe(frame)
    metric = next(m for m in loaded["markets_data"][0]["drivers"]["Food Quality"] if m["variable_name"] == "QualityRefrigerate")
    assert metric["parsing_status"] == "empty"
    assert metric["applicability"] == "not_applicable"
    assert metric["applicability_basis"] == "explicit_source_column"
    profile = build_assessment_profile(loaded["markets_data"], loaded["metric_summaries"], loaded).model_dump()
    quality = next(d for d in profile["dimensions"] if d["dimension"] == "Food Quality")
    summary = next(m for m in quality["drivers"] if m["metric_id"] == metric["metric_id"])
    assert summary["availability"]["classification"] == "not_applicable"
    assert summary["coverage"]["available_market_count"] == 0
