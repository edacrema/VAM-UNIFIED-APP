"""Real lightweight contracts and analytical engine with malformed provider stubs."""
import json
import threading
from copy import deepcopy
from types import SimpleNamespace
from collections import Counter
import pytest

from app.services.mfi_drafter import light_graph, light_service
from app.services.mfi_drafter.execution import MemoryRecoveryStore, execution_status, reserve_execution
from app.services.mfi_drafter.synthetic_fixtures import SyntheticSpec, build_loaded
from app.services.mfi_drafter.schemas import MFIReleaseControl


@pytest.fixture(scope="module")
def loaded():
    return build_loaded(SyntheticSpec(market_count=2, region_count=1))


class Client:
    def __init__(self, changes=(), fail=None):
        self.changes, self.fail = set(changes), fail
        self.calls, self.packages = Counter(), []
        self.lock = threading.Lock()

    def count(self, messages, schema, timeout):
        return sum(len(str(m.content)) for m in messages)//3

    def generate(self, messages, schema, timeout):
        p = json.loads(messages[0].content.split("\nREQUEST:\n",1)[1])
        specs = p["requested_sections"]
        ids = [s["section_id"] if isinstance(s,dict) else s for s in specs]
        family = "summary" if "executive_summary" in ids else "dimensions" if ids[0] in {"Assortment","Availability","Price","Resilience","Competition","Infrastructure","Service","Food Quality","Access & Protection"} else "markets"
        kind = "review" if "needs_revision" in schema["properties"] else "correct" if "REVIEW_REPORT" in p else "draft"
        key = kind+"_"+family
        with self.lock:
            self.calls[key] += 1
            self.packages.append((key, p))
        if self.fail == key:
            raise TimeoutError("injected transient provider failure")
        if kind == "review":
            response = {"needs_revision":family in self.changes,"review_markdown":"Check the evidence and correct the affected passage."}
        else:
            response = {"sections":[{"section_id":sid,"text_markdown":f"{kind}: supported analysis for {sid}."} for sid in ids],"notes":[]}
        return SimpleNamespace(content=json.dumps(response), response_metadata={"finish_reason":"STOP"},usage_metadata={"input_tokens":100,"output_tokens":50,"total_tokens":150})


@pytest.fixture
def setup(monkeypatch, loaded):
    store = MemoryRecoveryStore()
    monkeypatch.setattr(light_service,"recovery_store",lambda:store)
    monkeypatch.setattr(light_graph,"retrieve_context",lambda base:{"sources":{},"document_references":[],"contextual_documents":[],"context_status":{},"context_limitation":"No context"})
    monkeypatch.setattr(light_graph,"render_figures",lambda base,execution:{"visualizations":{},"figure_metadata":{}})
    args = dict(country=loaded["country"],data_collection_start=loaded["data_collection_start"],data_collection_end=loaded["data_collection_end"],
        markets=loaded["markets"],csv_data=deepcopy(loaded),run_id="light-test",
        release_control=MFIReleaseControl(analysis_version="2",enabled=True,configuration_status="configured"))
    return store,args


@pytest.mark.parametrize("changes,expected",[((),5),(("dimensions",),6),(("markets",),6),(("dimensions","markets"),7)])
def test_five_six_seven_calls_and_complete_report(setup,changes,expected):
    store,args=setup
    client=Client(changes)
    result=light_service.run_mfi_report_generation(**args,client=client)
    assert sum(client.calls.values()) == result["llm_calls"] == expected
    assert len(result["generation_diagnostics"]["phases"]) == 11
    assert result["coverage"]["complete"]
    assert len(result["light_narrative"]["dimensions"]) == 9
    assert len(result["light_narrative"]["markets"]) == 2
    assert result["workflow_revision"] == "mfi-light-v1"
    assert result["narrative_schema_version"] == "3.0"
    assert store.read(args["run_id"])["execution_state"] == "completed"
    for kind,p in client.packages:
        if kind.startswith("correct"):
            assert p["ORIGINAL_DRAFT"] and p["REVIEW_REPORT"]["needs_revision"] and p["EVIDENCE"]
    assert not execution_status(args["run_id"],store,runtime=light_service.effective_contract())["draft_available"]


def test_resume_reuses_successful_work(setup):
    store,args=setup
    client=Client(fail="review_dimensions")
    with pytest.raises(TimeoutError):
        light_service.run_mfi_report_generation(**args,client=client)
    before=dict(client.calls)
    assert before["review_dimensions"] == 2
    status=execution_status(args["run_id"],store,runtime=light_service.effective_contract())
    assert status["resumable"] and not status["draft_available"]
    reservation=reserve_execution(store,args["run_id"],expected_revision=status["run_revision"],idempotency_key="resume-1",runtime=light_service.effective_contract())
    client.fail=None
    result=light_service.run_mfi_report_generation(**args,client=client,execution_reservation=reservation)
    assert client.calls["draft_dimensions"] == before["draft_dimensions"]
    assert client.calls["draft_markets"] == before["draft_markets"]
    assert client.calls["review_dimensions"] == 3
    assert result["success"]


def test_corrections_start_without_waiting_for_other_review_or_charts(setup, monkeypatch):
    _, args = setup
    corrected = threading.Event()
    drafts = threading.Barrier(2)
    reviews = threading.Barrier(2)
    class Concurrent(Client):
        def generate(self, messages, schema, timeout):
            response = super().generate(messages, schema, timeout)
            packet = json.loads(messages[0].content.split("\nREQUEST:\n", 1)[1])
            family = "dimensions" if isinstance(packet["requested_sections"][0], dict) and packet["requested_sections"][0]["section_id"] == "Assortment" else "markets"
            if "needs_revision" in schema["properties"]:
                reviews.wait(timeout=20)
                if family == "markets": assert corrected.wait(20), "Correction unnecessarily waited for other review"
            elif "REVIEW_REPORT" in packet:
                if family == "dimensions": corrected.set()
            elif "FINAL_DIMENSIONS" not in packet:
                drafts.wait(timeout=20)
            return response
    def charts(base, execution):
        assert corrected.wait(40), "Drafting unnecessarily waited for charts"
        return {"visualizations": {}, "figure_metadata": {}}
    monkeypatch.setattr(light_graph, "render_figures", charts)
    assert light_service.run_mfi_report_generation(**args, client=Concurrent(["dimensions"]))["success"]


def test_http_and_streamlit_share_results_and_draft_lock(setup, monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from app.services.mfi_drafter import router, execution, execution_service, drafts
    from app.streamlit_backend import dispatcher
    from app.shared import async_runs
    from app.shared.docx_export import build_docx_bytes_from_report_blocks
    from docx import Document
    from io import BytesIO
    store, args = setup
    monkeypatch.setattr(async_runs, "_BACKEND", "memory")
    for module in (execution, execution_service, drafts): monkeypatch.setattr(module, "recovery_store", lambda: store)
    result = light_service.run_mfi_report_generation(**args, client=Client())
    app = FastAPI(); app.include_router(router.router, prefix="/mfi-drafter")
    http = TestClient(app)
    reply = http.get("/mfi-drafter/result/"+args["run_id"])
    assert reply.status_code == 200, reply.text
    assert reply.json()["narrative_schema_version"] == "3.0"
    local = dispatcher._mfi_drafter_result(args["run_id"])
    assert json.loads(local.content)["light_narrative"] == reply.json()["light_narrative"]
    for suffix, method in (("draft", http.get), ("export-draft-docx", http.post)):
        assert method(f"/mfi-drafter/{suffix}/{args['run_id']}").status_code == 409
    response = http.post("/mfi-drafter/export-docx/"+args["run_id"], json={})
    assert response.status_code == 200, response.text[:300]
    doc = Document(BytesIO(response.content))
    text = "\n".join(p.text for p in doc.paragraphs)
    assert "Analytical annex" in text and "INCOMPLETE" not in text
    assert all(d in text for d in result["light_narrative"]["dimensions"])


def test_light_resume_endpoint_idempotent_and_failed_report_locked(setup, monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from app.services.mfi_drafter import router, execution, execution_service, drafts
    from app.shared import async_runs
    store, args = setup
    monkeypatch.setattr(async_runs, "_BACKEND", "memory")
    for module in (execution, execution_service, drafts): monkeypatch.setattr(module, "recovery_store", lambda: store)
    with pytest.raises(TimeoutError): light_service.run_mfi_report_generation(**args, client=Client(fail="review_dimensions"))
    scheduled=[]
    monkeypatch.setattr(light_service, "execute_resumed", lambda *args: scheduled.append(args))
    app=FastAPI();app.include_router(router.router,prefix="/mfi-drafter");http=TestClient(app)
    status=http.get("/mfi-drafter/status/"+args["run_id"]).json()
    assert status["resumable"] and not status["draft_available"]
    for prefix in ("draft", "analysis", "result"):
        assert http.get(f"/mfi-drafter/{prefix}/{args['run_id']}").status_code == (400 if prefix == "result" else 409)
    body={"expected_revision":status["run_revision"],"idempotency_key":"repeat"}
    first=http.post("/mfi-drafter/resume/"+args["run_id"],json=body)
    second=http.post("/mfi-drafter/resume/"+args["run_id"],json=body)
    assert first.status_code == second.status_code == 202
    assert first.json() == second.json() and len(scheduled) == 1


@pytest.mark.parametrize("country,survey", [("Benin",5896), ("Haiti",5899)])
def test_full_country_packages_and_unchanged_analysis(setup, country, survey):
    from pathlib import Path
    from app.services.mfi_drafter.data_loader import load_mfi_from_csv
    root=Path(__file__).resolve().parents[1]
    path=root/f"MFI Test Databases/MFI_Full_{country}_surveyid{survey}.csv"
    if not path.exists(): pytest.skip("Local confidential benchmark absent")
    store,args=setup
    data=load_mfi_from_csv(path)
    args.update(csv_data=data,country=country,markets=data["markets"],data_collection_start=data["data_collection_start"],data_collection_end=data["data_collection_end"])
    client=Client(["dimensions", "markets"])
    result=light_service.run_mfi_report_generation(**args,client=client)
    expected=json.loads((root/"tests/fixtures/mfi_reliable_baseline.json").read_text(encoding="utf-8"))[country]
    profile=result["assessment_profile"]
    assert profile["assessed_market_count"] == expected["count"]
    assert len(result["excluded_market_records"]) == expected["excluded"]
    assert profile["mean_mfi_across_assessed_markets"] == expected["mean"]
    assert profile["priority_market_names"] == expected["selected"]
    assert profile["priority_dimension_names"] == expected["priorities"]
    assert result["llm_calls"] == 7
    for kind, packet in client.packages:
        ev=packet["EVIDENCE"]
        assert len(ev["market_scores"]["rows"]) == expected["count"]
        if kind.endswith("markets"):
            assert ev["local_evidence"]["rows"] and ev["indicator_definitions"]["rows"]
            assert len({tuple(r[:2]) for r in ev["local_evidence"]["rows"]}) == len(ev["local_evidence"]["rows"])
        if kind.endswith("dimensions") and country == "Benin":
            dims={d["dimension"]:d for d in ev["dimensions"]}
            service=dims["Service"]["facts"]["fact.assessment.service.at_or_below_median"]
            assert (service["numerator"],service["denominator"]) == (37,53)
            assert dims["Infrastructure"]["facts"]["fact.assessment.infrastructure.below_3"]["numerator"] == 7
            assert dims["Food Quality"]["facts"]["fact.region.food_quality.minimum"]["value"] == pytest.approx(4.791666667)
    assert result["coverage"]["complete"]


def test_oversized_groups_split_without_repeating_successful_sections(setup):
    _,args=setup
    class Limited(Client):
        def count(self,messages,schema,timeout):
            packet=json.loads(messages[0].content.split("\nREQUEST:\n",1)[1])
            return 250001 if len(packet["requested_sections"]) > 4 else 100
    client=Limited()
    result=light_service.run_mfi_report_generation(**args,client=client)
    assert len(result["light_narrative"]["dimensions"]) == 9
    ids=[s["section_id"] for kind, p in client.packages if kind=="draft_dimensions" for s in p["requested_sections"]]
    assert len(ids)==len(set(ids))==9


@pytest.mark.parametrize("variant", ["renamed", "unicode", "sparse"])
def test_country_identity_and_sparse_variants_preserve_official_scores(setup, loaded, variant):
    from app.services.mfi_drafter.synthetic_fixtures import build_dataframe
    from app.services.mfi_drafter.data_loader import load_mfi_from_dataframe
    _,args=setup
    frame=build_dataframe(SyntheticSpec(country="Alternative Country",market_count=2,region_count=1,
        include_item_drivers=variant != "sparse", include_category_drivers=variant != "sparse"))
    if variant == "unicode":
        frame["MarketID"] = frame.MarketName.map({"Market 01":"01","Market 02":"02"})
        frame["SurveyID"] = "100"
        frame["MarketName"] = "São José — Marché"
    data=load_mfi_from_dataframe(frame)
    args.update(country=data["country"],csv_data=data,markets=data["markets"])
    client=Client()
    result=light_service.run_mfi_report_generation(**args,client=client)
    assert sorted(m["overall_mfi"] for m in data["markets_data"]) == sorted(m["overall_mfi"] for m in loaded["markets_data"])
    assert result["llm_calls"] == 5 and result["coverage"]["complete"]
    assert len(result["light_narrative"]["markets"]) == 2
    assert all(p["EVIDENCE"]["country"] == "Alternative Country" for _,p in client.packages)
