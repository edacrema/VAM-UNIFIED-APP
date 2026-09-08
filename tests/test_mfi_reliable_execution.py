import time
import pytest
from app.services.mfi_drafter.execution import (
    MemoryRecoveryStore, Execution, RecoveryError, create_checkpoint, reserve_execution, execution_status)
from app.services.mfi_drafter.correction import validate_independent_patches, response_schema
from app.services.mfi_drafter.packages import bounded_groups, PackageTooLarge


def ready():
    store = MemoryRecoveryStore()
    create_checkpoint(store, "run", {"input": "validated"}, {"contract": "1"})
    return store, Execution(store, "run", reserve_execution(store, "run"))


def test_completed_work_is_reused_and_failures_resume():
    store, first = ready()
    calls = []
    assert first.execute_once("draft", [1], lambda: calls.append(1) or {"text": "saved"}) == {"text": "saved"}
    with pytest.raises(ValueError):
        first.execute_once("review", [1], lambda: (_ for _ in ()).throw(ValueError("bad shape")))
    first.finish(ValueError("bad shape"))
    previous = store.read("run")
    reservation = reserve_execution(store, "run", expected_revision=previous["run_revision"], idempotency_key="resume-1")
    second = Execution(store, "run", reservation)
    assert second.execute_once("draft", [1], lambda: calls.append(2)) == {"text": "saved"}
    assert second.execute_once("review", [1], lambda: "fixed") == "fixed"
    assert calls == [1]
    duplicate = reserve_execution(store, "run", expected_revision=previous["run_revision"], idempotency_key="resume-1")
    assert duplicate["epoch"] == reservation["epoch"] and not duplicate["scheduled"]


def test_stale_worker_cannot_commit_after_resume():
    store, worker = ready()
    store.manifests["run"]["lease_until"] = time.time() - 1
    assert execution_status("run", store)["execution_state"] == "interrupted"
    reserve_execution(store, "run", idempotency_key="next")
    with pytest.raises(RecoveryError, match="ownership"):
        worker.snapshot({"dimension_narratives": {"Service": {"text": "late"}}})
    assert store.read("run")["snapshot_ref"] is None


def test_failed_object_upload_never_commits_success(monkeypatch):
    store, worker = ready()
    monkeypatch.setattr(store, "put", lambda *args: (_ for _ in ()).throw(RecoveryError("unavailable")))
    with pytest.raises(RecoveryError):
        worker.execute_once("draft", {}, lambda: {"text": "generated"})
    assert list(store.read("run")["tasks"].values())[0]["status"] == "failed"


def test_checksum_corruption_is_detected():
    store, worker = ready()
    ref = store.put("run", {"value": 1})
    store.objects[ref] = b'{"value":2}'
    with pytest.raises(RecoveryError, match="checksum"):
        store.get(ref)


def test_status_explains_incompatible_resume_without_changing_checkpoint():
    store, worker = ready()
    worker.finish(ValueError("interrupted"))
    before = store.read("run")
    status = execution_status("run", store, runtime={"contract":"different"})
    assert not status["resumable"] and "incompatible" in status["resume_block_reason"]
    assert store.read("run") == before


def test_context_shape_recovery_and_partial_validation():
    targets = [{"task_id": key, "artifact_type": "context", "field_name": "text"} for key in ("a", "b")]
    valid, errors, normalized = validate_independent_patches({"patches": [
        {"target_id": "a", "replacement": {"text": "A source-supported sentence."}},
        {"target_id": "b", "replacement": {"text": "No", "extra": "not allowed"}}]}, targets)
    assert valid == {"a": "A source-supported sentence."}
    assert set(errors) == {"b"}
    assert normalized == [{"target_id": "a", "normalization": "exact_text_object_to_string"}]
    assert response_schema("context_text", ["a"])["properties"]["patches"]["items"]["properties"]["replacement"]["type"] == "string"


@pytest.mark.parametrize("ids", [["a", "a"], ["a", "unknown"], []])
def test_duplicate_unknown_missing_targets_are_rejected(ids):
    valid, errors, _ = validate_independent_patches({"patches": [{"target_id": key, "replacement": "text"} for key in ids]},
        [{"task_id": "a", "artifact_type": "context", "field_name": "text"}])
    assert not valid and errors


def test_bounded_packages_split_without_truncating():
    rows = list(range(81))
    groups = bounded_groups(rows, lambda value: value, maximum_items=40)
    assert list(map(len, groups)) == [40, 40, 1]
    assert sum(groups, []) == rows
    with pytest.raises(PackageTooLarge):
        bounded_groups(["huge" * 100], lambda value: value, maximum_items=1, maximum_characters=10)


def test_typed_correction_repairs_only_failed_targets_and_recovers_saved_response(monkeypatch):
    import json
    from types import SimpleNamespace
    from app.services.mfi_drafter.correction import correct_targets
    store, execution = ready()
    targets = [{"task_id": key, "artifact_type":"context", "artifact_id":key, "field_name":"text"} for key in ("a","b")]
    def payload(rows):
        return {"targets":[{"target_id":row["task_id"], "current_field":"original"} for row in rows],
                "authorized_claim_catalog":[], "authorized_documents":[]}
    class Model:
        def bind(self, **kwargs):
            return SimpleNamespace(ids=kwargs["response_schema"]["properties"]["patches"]["items"]["properties"]["target_id"]["enum"])
    class Trace:
        def __init__(self): self.calls = []
        def invoke_json(self, **kwargs):
            ids = kwargs["model"].ids
            self.calls.append(ids)
            patches = [{"target_id":key, "replacement": {"text":"Corrected text."}} for key in ids]
            if len(self.calls) == 1:
                patches[-1]["replacement"]["unexpected"] = True
            return SimpleNamespace(payload={"patches":patches}, call_id=f"call-{len(self.calls)}")
    trace = Trace()
    with execution.activate():
        result = correct_targets(targets=targets, build_payload=payload, model=Model(), trace=trace, timeout_seconds=600, max_retries=2)
    assert trace.calls == [["a","b"],["b"]]
    assert not result["failures"] and len(result["replacements"]) == 2
    # Simulate a stop after response commit but before independent patch commit.
    store.manifests["run"]["tasks"] = {k:v for k,v in store.manifests["run"]["tasks"].items() if v["kind"] != "staged_patch"}
    execution.finish(ValueError("worker stopped"))
    resumed = Execution(store, "run", reserve_execution(store,"run",idempotency_key="resume"))
    with resumed.activate():
        result = correct_targets(targets=targets, build_payload=payload, model=Model(), trace=trace, timeout_seconds=600, max_retries=2)
    assert trace.calls == [["a","b"],["b"]]
    assert result["replacements"] == {"a":"Corrected text.","b":"Corrected text."}


def test_provider_patch_schemas_have_concrete_object_properties():
    from app.services.mfi_drafter.correction import PATCH_CONTRACTS
    def check(value):
        if isinstance(value, dict):
            if value.get("type") == "object":
                assert value.get("properties"), value
            for child in value.values(): check(child)
        elif isinstance(value,list):
            for child in value: check(child)
    for kind in PATCH_CONTRACTS:
        check(response_schema(kind,["target"]))


def test_context_shape_recovery_through_complete_correction_node(monkeypatch):
    import json
    from types import SimpleNamespace
    from app.services.mfi_drafter import graph
    from app.services.mfi_drafter.reliable_nodes import typed_correction_node

    class Model:
        def bind(self, **kwargs):
            self.target_ids = kwargs["response_schema"]["properties"]["patches"]["items"]["properties"]["target_id"]["enum"]
            return self

        def invoke(self, messages):
            return SimpleNamespace(content=json.dumps({"patches": [
                {"target_id": key, "replacement": {"text": "The source describes the broader region."}}
                for key in self.target_ids]}))

    monkeypatch.setattr(graph, "get_model", lambda **kwargs: Model())
    store, execution = ready()
    state = {
        "run_id": "run", "assessment_profile": {}, "claim_catalog": {},
        "context_evidence": [{"statement_id": "context.statement.1", "text": "Original context.",
                              "classification": "potentially_explanatory", "document_ids": ["source"]}],
        "contextual_documents": [{"doc_id": "source", "title": "Regional report",
                                  "text": "The source describes the broader region."}],
        "deterministic_flags": [{"flag_id": "context-scope", "source": "deterministic",
            "code": "context_scope", "severity": "medium", "artifact_type": "context",
            "artifact_id": "context.statement.1", "field_name": "text", "claim_id": "context.statement.1",
            "message": "Clarify regional scope.", "repairable": True, "document_ids": ["source"]}],
    }
    with execution.activate():
        result = typed_correction_node(state)
    assert result["context_evidence"][0]["text"] == "The source describes the broader region."
    assert result["generation_diagnostics"]["correction_tasks_completed"] == 1
    assert result["correction_history"]
    assert result["correction_targets"][0]["expected_field_hash"]
    assert store.read("run")["snapshot_ref"]
