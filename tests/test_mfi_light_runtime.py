import json
from types import SimpleNamespace
import pytest
from langchain_core.messages import HumanMessage
from app.services.mfi_drafter import execution, light_service, light_runtime
from app.services.mfi_drafter.light_contracts import response_schema, inspect_sections, instructions


class Responses:
    def __init__(self, values):
        self.values, self.calls, self.counts = iter(values), [], 0

    def count(self, messages, schema, timeout):
        self.counts += 1
        return 100

    def generate(self, messages, schema, timeout):
        self.calls.append(json.loads(messages[0].content.split("\nREQUEST:\n")[1]))
        value = next(self.values)
        if isinstance(value, Exception):
            raise value
        if isinstance(value, tuple):
            value, finish = value
        else:
            finish = "STOP"
        return SimpleNamespace(content=value if isinstance(value, str) else json.dumps(value), response_metadata={"finish_reason": finish})


def answer(*ids, text="Supported prose."):
    return {"sections": [{"section_id": sid, "text_markdown": text} for sid in ids], "notes": []}


def test_active_prompts_are_isolated_from_legacy_recommendation_policy(monkeypatch):
    from app.services.mfi_drafter import light_contracts, methodology
    nodes = [n for n in light_contracts.NODES
             if n.startswith(("draft_", "review_", "correct_")) or n == "executive_summary"]
    before = {node: instructions(node) for node in nodes}
    monkeypatch.setattr(methodology, "NARRATIVE_PROHIBITIONS", ("Legacy-only policy change.",))
    assert {node: instructions(node) for node in nodes} == before


@pytest.fixture
def runtime(monkeypatch):
    store = execution.MemoryRecoveryStore()
    contract = light_service.effective_contract()
    execution.create_checkpoint(store, "response", {}, contract)
    worker = execution.Execution(store, "response", execution.reserve_execution(store, "response", runtime=contract))
    monkeypatch.setattr(light_runtime.time, "sleep", lambda _: None)
    return store, worker


@pytest.mark.parametrize("policy_name", ["RECOMMENDATION_POLICY", "ANALYSIS_POLICY", "STYLE_POLICY"])
def test_shared_policy_change_blocks_resume_without_changing_checkpoint(runtime, monkeypatch, policy_name):
    from app.services.mfi_drafter import light_contracts
    store, worker = runtime
    worker.finish(ValueError("Interrupted before completion"))
    before = store.read("response")
    old_contract = light_service.effective_contract()
    monkeypatch.setattr(light_contracts, policy_name,
                        getattr(light_contracts, policy_name) + "\nRevised shared guidance.")
    new_contract = light_service.effective_contract()
    assert new_contract["schema_hashes"] == old_contract["schema_hashes"]
    assert len(new_contract["prompt_hashes"]) == 7
    assert all(value != old_contract["prompt_hashes"][node]
               for node, value in new_contract["prompt_hashes"].items())
    status = execution.execution_status("response", store, runtime=new_contract)
    assert not status["resumable"] and "incompatible" in status["resume_block_reason"]
    with pytest.raises(execution.RecoveryError, match="incompatible"):
        execution.reserve_execution(store, "response", runtime=new_contract)
    assert store.read("response") == before


def invoke(worker, client):
    return light_runtime.ModelRuntime(worker, client).invoke("draft_dimensions", "dimensions", {"EVIDENCE": {"sources": {}}, "requested_sections": ["A", "B"]}, ["A", "B"])


@pytest.mark.parametrize("defect", ["missing", "blank", "duplicate", "bad_citation", "metadata"])
def test_repairs_only_invalid_sections(runtime, defect):
    store, worker = runtime
    first = answer("A", "B")
    first["sections"][0]["text_markdown"] = "Original valid prose stays."
    if defect == "missing": first["sections"].pop()
    if defect == "blank": first["sections"][1]["text_markdown"] = " "
    if defect == "duplicate": first["sections"].append(first["sections"][1])
    if defect == "bad_citation": first["sections"][1]["text_markdown"] = "Unsupported [S99]."
    if defect == "metadata": first["sections"][1]["claim_id"] = "forbidden"
    client = Responses([first, answer("B", text="Repaired prose.")])
    output = invoke(worker, client)
    assert client.calls[1]["requested_sections"] == ["B"]
    assert output["sections"][0]["text_markdown"] == "Original valid prose stays."
    assert len(client.calls) == 2
    assert len(next(iter(store.read("response")["light_work"].values()))["attempts"]) == 2


def test_syntax_then_schema_error_has_no_third_attempt(runtime):
    store, worker = runtime
    client = Responses(["{", answer("A")])
    with pytest.raises(light_runtime.InvalidResponse): invoke(worker, client)
    with pytest.raises(execution.RecoveryError, match="exhausted"): invoke(worker, client)
    assert len(client.calls) == 2


@pytest.mark.parametrize("first", [(answer("A", "B"), "MAX_TOKENS"), "{", [1, 2]])
def test_truncation_and_unreadable_response_bounded(runtime, first):
    _, worker = runtime
    client = Responses([first, answer("B") if isinstance(first, tuple) else answer("A", "B")])
    assert len(invoke(worker, client)["sections"]) == 2
    assert len(client.calls) == 2


def test_access_error_is_not_a_format_retry(runtime):
    from google.api_core.exceptions import PermissionDenied
    _, worker = runtime
    client = Responses([PermissionDenied("test model not enabled")])
    with pytest.raises(PermissionDenied): invoke(worker, client)
    assert len(client.calls) == 1


def test_saved_response_recovers_after_commit_failure_without_another_call(runtime, monkeypatch):
    store, worker = runtime
    client = Responses([answer("A", "B")])
    original = light_runtime.ModelRuntime.complete
    monkeypatch.setattr(light_runtime.ModelRuntime, "complete", lambda *args: (_ for _ in ()).throw(execution.RecoveryError("injected output commit failure")))
    with pytest.raises(execution.RecoveryError): invoke(worker, client)
    monkeypatch.setattr(light_runtime.ModelRuntime, "complete", original)
    assert len(invoke(worker, client)["sections"]) == 2
    assert len(client.calls) == 1
    diagnostics = light_runtime.public_diagnostics(store.read("response"))
    assert diagnostics["llm_diagnostics"]["calls"][0]["status"] == "succeeded"
    assert "raw_text" not in json.dumps(diagnostics)


def test_ownership_loss_cannot_commit_late_response(runtime):
    store, worker = runtime
    class Late(Responses):
        def generate(self, *args):
            response = super().generate(*args)
            store.transaction("response", lambda m: {**m, "fence": m["fence"]+1})
            return response
    with pytest.raises(execution.RecoveryError): invoke(worker, Late([answer("A", "B")]))
    assert not next(iter(store.read("response")["light_work"].values())).get("output_ref")


def test_provider_configuration_schema_count_and_cached_client_are_isolated(monkeypatch):
    from google.auth.credentials import AnonymousCredentials
    from langchain_google_vertexai import ChatVertexAI
    from app.services.mfi_drafter.light_contracts import MODEL
    model = ChatVertexAI(model_name=MODEL, project="offline-test", location="global", temperature=1.0,
        timeout=600, max_retries=0, max_output_tokens=65536, credentials=AnonymousCredentials())
    messages = [HumanMessage(content="Offline configuration test")]
    for review in (False, True):
        schema = response_schema(review)
        request = model._prepare_request_gemini(messages, response_mime_type="application/json", response_schema=schema)
        assert request.model.endswith("/"+MODEL)
        assert request.generation_config.max_output_tokens == 65536
        assert request.generation_config.temperature == 1.0
        assert request.generation_config.response_mime_type == "application/json"
        assert request.generation_config.response_schema.properties
        from google.cloud.aiplatform_v1beta1.types import CountTokensRequest
        count = CountTokensRequest(model=request.model, endpoint=request.model, contents=request.contents,
            system_instruction=request.system_instruction, generation_config=request.generation_config)
        assert count.generation_config.response_schema == request.generation_config.response_schema
    assert model.response_schema is None
    assert "evidence overrides" in instructions("correct_dimensions")


def test_unknown_identifiers_and_scalar_notes_are_not_silently_accepted():
    payload = answer("A", "unknown")
    payload["notes"] = None
    valid, issues = inspect_sections(payload, ["A", "B"], {})
    assert set(valid) == {"A"} and len(issues) == 3
