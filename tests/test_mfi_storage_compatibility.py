"""Deployments using the pre-reliability run backend require no new services."""
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import BackgroundTasks, FastAPI
from fastapi.testclient import TestClient

from app.services.mfi_drafter import execution, execution_service, router
from app.services.mfi_drafter.schemas import MFIReleaseControl
from app.services.mfi_drafter.synthetic_fixtures import SyntheticSpec, build_csv_bytes
from app.shared import async_runs
from app.streamlit_backend import dispatcher


@pytest.fixture
def legacy_cloud(monkeypatch):
    monkeypatch.setenv("K_SERVICE", "vam-unified-app-phase4")
    for key in ("RUNS_BACKEND", "RUNS_GCS_URI", "RUNS_FIRESTORE_DATABASE", "RUNS_FIRESTORE_COLLECTION"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setattr(async_runs, "_BACKEND", None)
    monkeypatch.setattr(async_runs, "_RUNS", {})
    store = execution.MemoryRecoveryStore()
    monkeypatch.setattr(execution, "_memory", store)
    return store


@pytest.mark.parametrize("backend", [None, "memory", " MEMORY "])
def test_existing_cloud_memory_configuration_is_accepted(legacy_cloud, monkeypatch, backend):
    if backend is not None:
        monkeypatch.setenv("RUNS_BACKEND", backend)
        # An explicit memory backend must take precedence over a leftover URI.
        monkeypatch.setenv("RUNS_GCS_URI", "gs://existing-runs/runs")
    def unexpected_cloud():
        pytest.fail("An existing memory deployment must not initialize cloud storage")
    monkeypatch.setattr(execution, "CloudRecoveryStore", unexpected_cloud)
    assert execution.recovery_store() is legacy_cloud
    assert not async_runs._use_durable_store()


@pytest.mark.parametrize("backend", [None, "firestore_gcs", "firestore", "gcs", " FIRESTORE_GCS "])
def test_existing_durable_backend_is_preserved(legacy_cloud, monkeypatch, backend):
    if backend is not None:
        monkeypatch.setenv("RUNS_BACKEND", backend)
    monkeypatch.setenv("RUNS_GCS_URI", " gs://existing-runs/runs ")
    monkeypatch.setattr(async_runs, "_has_gcp_deps", lambda: True)
    cloud = SimpleNamespace(durable=True)
    monkeypatch.setattr(execution, "CloudRecoveryStore", lambda: cloud)
    assert execution.recovery_store() is cloud
    assert async_runs._use_durable_store()


def test_selected_durable_backend_failure_does_not_change_to_memory(legacy_cloud, monkeypatch):
    monkeypatch.setenv("RUNS_BACKEND", "firestore_gcs")
    monkeypatch.setenv("RUNS_GCS_URI", "gs://existing-runs/runs")
    monkeypatch.setattr(async_runs, "_has_gcp_deps", lambda: True)
    def unavailable():
        raise execution.RecoveryError("Existing recovery storage unavailable")
    monkeypatch.setattr(execution, "CloudRecoveryStore", unavailable)
    with pytest.raises(execution.RecoveryError, match="unavailable"):
        execution.recovery_store()
    assert not legacy_cloud.manifests


@pytest.mark.parametrize("entrypoint", ["api", "streamlit"])
@pytest.mark.parametrize("dataset", ["synthetic", "Benin"])
def test_csv_submission_with_previous_cloud_configuration(legacy_cloud, monkeypatch, entrypoint, dataset):
    if dataset == "Benin":
        path = Path(__file__).resolve().parents[1] / "MFI Test Databases/MFI_Full_Benin_surveyid5896.csv"
        if not path.exists():
            pytest.skip("Local Benin benchmark absent")
        content, count = path.read_bytes(), 53
    else:
        content, count = build_csv_bytes(SyntheticSpec(market_count=1, region_count=1)), 1
    control = MFIReleaseControl(analysis_version="2", enabled=True, configuration_status="configured")
    monkeypatch.setattr(router, "_require_enabled_release_control", lambda: control)
    monkeypatch.setattr(dispatcher, "_require_enabled_mfi_release_control", lambda: control)
    monkeypatch.setattr(execution_service, "effective_contract", lambda: {})
    scheduled = []
    if entrypoint == "api":
        monkeypatch.setattr(BackgroundTasks, "add_task", lambda self, target: scheduled.append(target))
        app = FastAPI()
        app.include_router(router.router, prefix="/mfi-drafter")
        client = TestClient(app)
        submit = client.post("/mfi-drafter/generate-from-csv-async", files={"file": ("mfi.csv", content, "text/csv")})
        get_status = lambda run_id: client.get(f"/mfi-drafter/status/{run_id}")
    else:
        monkeypatch.setattr(dispatcher, "threading", SimpleNamespace(
            Thread=lambda target, **kwargs: SimpleNamespace(start=lambda: scheduled.append(target))))
        submit = dispatcher.dispatch_request("POST", "/mfi-drafter/generate-from-csv-async",
                                             files={"file": ("mfi.csv", content, "text/csv")})
        get_status = lambda run_id: dispatcher.dispatch_request("GET", f"/mfi-drafter/status/{run_id}")
    assert submit.status_code == 200, submit.json()
    assert len(scheduled) == 1
    run_id = submit.json()["run_id"]
    saved = legacy_cloud.read(run_id)
    assert saved and saved["owner"] and not saved["durable"]
    assert len(legacy_cloud.get(saved["input_ref"])["markets"]) == count
    status = get_status(run_id)
    assert status.status_code == 200
    assert status.json()["recovery_storage"] == "process_local"
    assert "same server process" in status.json()["recovery_limitation"]
    assert "restart" in status.json()["recovery_limitation"]


def test_process_local_checkpoint_is_not_reported_resumable_after_restart(legacy_cloud):
    execution.create_checkpoint(legacy_cloud, "old-process", {}, {})
    worker = execution.Execution(legacy_cloud, "old-process", execution.reserve_execution(legacy_cloud, "old-process"))
    worker.finish(ValueError("Interrupted call"))
    assert execution.execution_status("old-process", legacy_cloud)["resumable"]
    fresh_process = execution.MemoryRecoveryStore()
    assert not execution.execution_status("old-process", fresh_process)["resumable"]
    with pytest.raises(execution.RecoveryError, match="no recovery checkpoint"):
        execution.reserve_execution(fresh_process, "old-process", idempotency_key="resume")
