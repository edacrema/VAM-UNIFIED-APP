import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from app.services.mfi_drafter import router, execution, execution_service, drafts
from app.shared import async_runs


def test_resume_routes_preserve_idempotency_and_final_export_lock(monkeypatch):
    store = execution.MemoryRecoveryStore()
    monkeypatch.setattr(async_runs, "_BACKEND", "memory")
    monkeypatch.setattr(execution, "recovery_store", lambda: store)
    monkeypatch.setattr(execution_service, "recovery_store", lambda: store)
    monkeypatch.setattr(drafts, "recovery_store", lambda: store)
    monkeypatch.setattr(execution_service, "effective_contract", lambda: {})
    scheduled = []
    monkeypatch.setattr(execution_service, "execute_resumed", lambda *args: scheduled.append(args))
    execution.create_checkpoint(store, "api-recovery", {}, {})
    worker = execution.Execution(store, "api-recovery", execution.reserve_execution(store,"api-recovery"))
    worker.snapshot({"dimension_narratives":{"Service":{"summary":{"claim_id":"dimension.service.summary.1","text":"Saved analysis."}}}})
    worker.finish(ValueError("injected"))
    # Recovery uses its own durable manifest even if the legacy progress cache is absent.
    app = FastAPI()
    app.include_router(router.router, prefix="/mfi-drafter")
    client = TestClient(app)
    status = client.get("/mfi-drafter/status/api-recovery").json()
    assert status["status"] == "failed" and status["resumable"] and status["draft_available"]
    assert status["unresolved_counts"] is None
    assert client.get("/mfi-drafter/draft/api-recovery").status_code == 200
    assert client.post("/mfi-drafter/export-docx/api-recovery", json={}).status_code == 409
    body = {"expected_revision":status["run_revision"],"idempotency_key":"same"}
    first = client.post("/mfi-drafter/resume/api-recovery", json=body)
    repeated = client.post("/mfi-drafter/resume/api-recovery", json=body)
    assert first.status_code == repeated.status_code == 202
    assert first.json() == repeated.json() and len(scheduled) == 1
    assert client.post("/mfi-drafter/resume/api-recovery", json={**body,"idempotency_key":"different"}).status_code == 409
    assert client.post("/mfi-drafter/resume/unknown", json=body).status_code == 404


def test_resume_storage_failure_returns_503(monkeypatch):
    def unavailable(): raise execution.RecoveryError("storage unavailable")
    monkeypatch.setattr(execution_service,"recovery_store",unavailable)
    app = FastAPI()
    app.include_router(router.router,prefix="/mfi-drafter")
    response = TestClient(app).post("/mfi-drafter/resume/unavailable",json={"expected_revision":0,"idempotency_key":"request"})
    assert response.status_code == 503
