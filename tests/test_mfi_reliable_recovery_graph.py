from collections import Counter
import uuid
import pytest

from app.services.mfi_drafter import graph, execution_service
from app.services.mfi_drafter.execution import MemoryRecoveryStore, Execution, reserve_execution, execution_status
from app.services.mfi_drafter.claim_identity import canonicalize_narrative_identities, canonical_claim_index
from app.services.mfi_drafter.schemas import MFIReleaseControl
from app.services.mfi_drafter.synthetic_fixtures import build_loaded, SyntheticSpec
from app.shared.llm_observability import LLMCallError
from test_mfi_claim_identity_delivery import _RepeatedIdModel


def test_interrupted_dimension_node_resumes_only_unfinished_batches(monkeypatch):
    store = MemoryRecoveryStore()
    monkeypatch.setattr(execution_service, "recovery_store", lambda: store)
    loaded = build_loaded(SyntheticSpec(market_count=1, region_count=1))
    class Model(_RepeatedIdModel):
        def __init__(self):
            self.calls = Counter()
            self.failed = False
        def invoke(self, messages):
            prompt = str(messages[0].content)
            self.calls[prompt] += 1
            if "Analyse this MFI dimension" in prompt and len(self.calls) == 5 and not self.failed:
                self.failed = True
                raise RuntimeError("Injected worker failure during the fifth dimension")
            return super().invoke(messages)
    model = Model()
    monkeypatch.setattr(graph, "get_model", lambda **kwargs: model)
    def validation(**kwargs):
        dimensions, markets, executive, context = canonicalize_narrative_identities(
            dimension_narratives=kwargs["dimension_narratives"], market_narratives=kwargs["market_narratives"],
            executive_narrative=kwargs["executive_narrative"], context_evidence=kwargs["context_evidence"])
        count = len(canonical_claim_index(dimensions, markets, executive, context))
        return ({"status":"passed", "validated_claim_count":count, "verified_claim_count":count,
                 "unverified_claim_count":0, "flags":[]}, dimensions, markets, executive, context, {"flags":[]})
    monkeypatch.setattr(graph, "validate_evidence_bound_narratives", validation)
    monkeypatch.setattr(graph, "node_context_retrieval", lambda state: {
        "contextual_documents":[], "document_references":[], "seerist_documents":[], "reliefweb_documents":[],
        "context_status":state["context_status"], "current_node":"context_retrieval"})
    monkeypatch.setattr(graph, "node_mfi_graph_designer", lambda state: {"visualizations":{}, "current_node":"mfi_graph_designer"})
    run_id = "recovery-" + uuid.uuid4().hex
    arguments = dict(country=loaded["country"], data_collection_start=loaded["data_collection_start"],
        data_collection_end=loaded["data_collection_end"], markets=loaded["markets"], csv_data=loaded, run_id=run_id,
        release_control=MFIReleaseControl(analysis_version="2", enabled=True, configuration_status="configured"))
    reservation = execution_service.prepare_submission(run_id, loaded)
    assert store.read(run_id)["input_ref"]
    with pytest.raises(LLMCallError):
        graph.run_mfi_report_generation(**arguments, execution_reservation=reservation)
    status = execution_status(run_id, store)
    assert status["resumable"] and status["draft_available"] and status["analysis_available"]
    first_prompts = list(model.calls)[:4]
    reservation = reserve_execution(store, run_id, expected_revision=status["run_revision"],
        idempotency_key="manual-resume", runtime=execution_service.effective_contract())
    result = graph.run_mfi_report_generation(**arguments, execution_reservation=reservation)
    assert all(model.calls[prompt] == 1 for prompt in first_prompts)
    assert len(result["dimension_narratives"]) == 9
    assert result["generation_diagnostics"]["delivery_contract_status"] == "validated"
    assert store.read(run_id)["execution_state"] == "completed"
    assert result["llm_diagnostics"]["total_calls"] == sum(model.calls.values())
