"""Real graph-node validators and journals, with controlled model responses."""
import copy
import json
from collections import Counter

from app.services.mfi_drafter import graph, execution_service
from app.services.mfi_drafter.execution import (MemoryRecoveryStore, Execution, create_checkpoint, reserve_execution, current_execution)
from app.services.mfi_drafter.analysis import build_assessment_profile
from app.services.mfi_drafter.narrative import build_claim_catalog
from app.services.mfi_drafter.synthetic_fixtures import build_loaded, SyntheticSpec
from app.shared.llm_observability import LLMTraceSession
from test_mfi_claim_identity_delivery import _RepeatedIdModel
from test_mfi_response_contracts import repair_fields, ResponseModel, dimension, claim

import pytest


def test_recovered_context_reuses_dimensions_and_refreshes_executive_and_review(monkeypatch):
    loaded = build_loaded(SyntheticSpec(market_count=1, region_count=1))
    profile = build_assessment_profile(loaded["markets_data"],loaded["metric_summaries"],loaded).model_dump()
    base = {"run_id":"run","country":loaded["country"],"data_collection_start":loaded["data_collection_start"],
            "data_collection_end":loaded["data_collection_end"],"assessment_profile":profile,"claim_catalog":build_claim_catalog(profile),
            "contextual_documents":[{"doc_id":"doc","source":"ReliefWeb","content":"Exact passage.","title":"Source"}],
            "generation_diagnostics":{"retrievers":{"ReliefWeb":"completed","Seerist":"no_results"}}}
    statement = {"text":"Exact passage.","classification":"potentially_explanatory", "document_ids":["doc"],
                 "source_passages":[{"document_id":"doc","text":"Exact passage."}]}
    class Model(_RepeatedIdModel):
        def __init__(self):
            self.counts = Counter()
            self.recovered = False
        def invoke(self,messages):
            prompt = str(messages[0].content)
            kind = "repair" if prompt.startswith("Repair only") else "context" if "Classify source-linked context" in prompt else "dimension" if "Analyse this MFI dimension" in prompt else "executive" if "structured executive summary" in prompt else "review"
            self.counts[kind] += 1
            if kind == "context":
                payload = {"statements":[statement, {**statement,"document_ids":"doc"}]}
            elif kind == "repair":
                payload = repair_fields(messages,["doc"] if self.recovered else "doc")
            else:
                return super().invoke(messages)
            return type("Response",(),{"content":json.dumps(payload)})()
    model = Model()
    monkeypatch.setattr(graph,"get_model",lambda **kw:model)
    trace = LLMTraceSession(service="mfi-drafter",run_id="run",sink=lambda data:current_execution().record_trace(data))
    monkeypatch.setattr(graph,"get_trace_session",lambda **kw:trace)
    store = MemoryRecoveryStore()
    create_checkpoint(store,"run",{}, {})
    first = Execution(store,"run",reserve_execution(store,"run"))
    def stages(worker):
        state = copy.deepcopy(base)
        with worker.activate():
            for name, function in (("context_extractor",graph.node_context_extractor),
                                   ("dimension_drafter",graph.node_dimension_drafter),
                                   ("executive_summary_drafter",graph.node_executive_summary_drafter),
                                   ("semantic_review",graph.node_semantic_review)):
                state.update(execution_service.execute_node(name,state,function))
        return state
    initial = stages(first)
    before = dict(model.counts)
    assert initial["context_status"]["classification_outcome"] == "degraded"
    assert len(initial["context_evidence"]) == 1
    first.finish(ValueError("later failure"))
    model.recovered = True
    second = Execution(store,"run",reserve_execution(store,"run",idempotency_key="resume"))
    recovered = stages(second)
    assert len(recovered["context_evidence"]) == 2
    assert recovered["context_status"]["classification_outcome"] == "completed"
    assert recovered["context_evidence"][0]["statement_id"] == initial["context_evidence"][0]["statement_id"]
    assert model.counts["context"] == 1
    assert model.counts["dimension"] == before["dimension"]
    assert model.counts["executive"] == before["executive"] + 1
    assert model.counts["review"] > before["review"]
    assert recovered["dimension_narratives"] == initial["dimension_narratives"]
    diagnostics = recovered["generation_diagnostics"]
    assert len(diagnostics["dimensions"]["llm"]) == 9
    assert diagnostics["degraded_work_count"] == 0
    assert not diagnostics["structural_validation_issues"]


def test_failed_availability_is_visible_in_status_and_partial_draft(monkeypatch):
    from app.services.mfi_drafter.response_runtime import plan_drafts
    from app.services.mfi_drafter.drafts import draft_payload
    from app.services.mfi_drafter.execution import execution_status
    from app.shared.llm_observability import LLMCallError
    loaded = build_loaded(SyntheticSpec(market_count=1,region_count=1))
    profile = build_assessment_profile(loaded["markets_data"],loaded["metric_summaries"],loaded).model_dump()
    state = {"run_id":"run","country":loaded["country"],"assessment_profile":profile,"claim_catalog":build_claim_catalog(profile)}
    bad = dimension()
    bad["summary"]["polarity"] = "positive"
    bad["key_findings"] = [claim()]
    model = ResponseModel([dimension(),bad,lambda m:repair_fields(m,"positive")])
    monkeypatch.setattr(graph,"get_model",lambda **kw:model)
    store = MemoryRecoveryStore()
    monkeypatch.setattr(execution_service,"recovery_store",lambda:store)
    create_checkpoint(store,"run",{}, {})
    worker = Execution(store,"run",reserve_execution(store,"run"))
    trace = LLMTraceSession(service="mfi-drafter",run_id="run")
    monkeypatch.setattr(graph,"get_trace_session",lambda **kw:trace)
    with worker.activate():
        worker.snapshot(state)
        plan_drafts(worker,profile,state["claim_catalog"])
        with pytest.raises(LLMCallError):
            graph.node_dimension_drafter(state)
        # The generation boundary commits the latest consistent state on failure.
        from app.services.mfi_drafter.execution import load_snapshot
        worker.snapshot(load_snapshot(store,store.read("run")["snapshot_ref"]))
        worker.finish(ValueError("structural validation failed"))
    metadata = execution_service.get_mfi_run("run").metadata["generation_diagnostics"]
    batches = {r["batch_id"]:r for r in metadata["draft_batches"]}
    assert batches["dimension:Assortment:1"]["status"] == "completed"
    assert batches["dimension:Availability:1"]["status"] == "failed"
    assert batches["dimension:Price:1"]["status"] == "pending"
    assert metadata["dimensions"]["llm"] == ["Assortment"]
    assert metadata["draft_batches_completed"] == 1
    assert len(batches) == len(metadata["draft_batches"])
    assert execution_status("run",store)["draft_available"]
    draft = draft_payload("run",store=store)
    assert any("Availability: partially generated" in str(b.get("text")) for b in draft["report_blocks"])
    assert any(f.get("path") == ["summary","polarity"] for f in draft["findings"])
    assert store.read("run")["execution_state"] == "failed"


def test_country_renaming_changes_identity_but_not_equivalent_numerical_analysis():
    profiles, inputs = [], []
    for country in ("Test Republic", "République Ω"):
        loaded = build_loaded(SyntheticSpec(country=country,market_count=3,region_count=2,item_market_ratio=.5))
        inputs.append(loaded)
        profiles.append(build_assessment_profile(loaded["markets_data"],loaded["metric_summaries"],loaded).model_dump())
    assert {m["market_key"] for m in inputs[0]["markets_data"]}.isdisjoint({m["market_key"] for m in inputs[1]["markets_data"]})
    assert profiles[0]["mean_mfi_across_assessed_markets"] == profiles[1]["mean_mfi_across_assessed_markets"]
    assert profiles[0]["priority_market_names"] == profiles[1]["priority_market_names"]
    assert [d["statistics"] for d in profiles[0]["dimensions"]] == [d["statistics"] for d in profiles[1]["dimensions"]]
