"""MFI execution journal on the application's existing run storage backend.

Objects are committed before their references. Firestore transactions contain
only manifests; no model, network upload or report calculation runs in them.
"""
from __future__ import annotations

import copy
import contextvars
import hashlib
import json
import threading
import time
import uuid
from collections import Counter
from contextlib import contextmanager
from typing import Any, Callable

from .reliable_contracts import CONTRACT_BUNDLE, WORKFLOW_REVISION, fingerprint


class RecoveryError(RuntimeError):
    def __init__(self, message: str, status_code: int = 503):
        super().__init__(message)
        self.status_code = status_code


class MemoryRecoveryStore:
    """Process-local backend for existing memory deployments and local tests."""
    durable = False

    def __init__(self):
        self.manifests: dict[str, dict] = {}
        self.objects: dict[str, bytes] = {}
        self.lock = threading.RLock()

    def read(self, run_id):
        with self.lock:
            return copy.deepcopy(self.manifests.get(run_id))

    def transaction(self, run_id, change):
        with self.lock:
            value = change(copy.deepcopy(self.manifests.get(run_id)))
            self.manifests[run_id] = copy.deepcopy(value)
            return copy.deepcopy(value)

    def put(self, run_id, value):
        raw = encode(value)
        ref = f"{run_id}/{hashlib.sha256(raw).hexdigest()}"
        with self.lock:
            self.objects.setdefault(ref, raw)
        return ref

    def get(self, ref):
        with self.lock:
            raw = self.objects[ref]
        return decode_checked(ref, raw)


def encode(value):
    from app.shared.async_runs import _json_default
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
                      default=_json_default, allow_nan=False).encode("utf-8")


def decode_checked(ref, raw):
    if hashlib.sha256(raw).hexdigest() != ref.rsplit("/", 1)[-1]:
        raise RecoveryError("Recovery object checksum mismatch")
    return json.loads(raw)


def load_snapshot(store, ref):
    value = store.get(ref)
    if value.get("snapshot_format") != "mfi-field-references-v1":
        return value
    fields = {key: store.get(reference) for key, reference in value["field_references"].items()}
    if value.get("figure_references") is not None:
        fields["visualizations"] = {key: store.get(reference) for key, reference in value["figure_references"].items()}
    return fields


class CloudRecoveryStore:
    durable = True

    def __init__(self):
        from app.shared import async_runs
        self.client = async_runs._get_firestore_client()
        self.storage = async_runs._get_storage_client()
        self.bucket, prefix = async_runs._get_runs_gcs_bucket_prefix()
        self.prefix = f"{prefix}/mfi-recovery"
        self.collection = async_runs._get_firestore_collection()
        if self.client is None or self.storage is None or not self.bucket:
            raise RecoveryError("MFI requires accessible Firestore and GCS recovery storage")

    def doc(self, run_id):
        return self.client.collection(self.collection).document(run_id).collection("mfi").document("checkpoint")

    def read(self, run_id):
        try:
            snap = self.doc(run_id).get()
            return snap.to_dict() if snap.exists else None
        except Exception as exc:
            raise RecoveryError("Unable to read MFI recovery storage") from exc

    def transaction(self, run_id, change):
        from google.cloud import firestore
        @firestore.transactional
        def commit(transaction):
            doc = self.doc(run_id)
            snap = doc.get(transaction=transaction)
            value = change(snap.to_dict() if snap.exists else None)
            transaction.set(doc, value)
            return value
        try:
            return commit(self.client.transaction())
        except RecoveryError:
            raise
        except Exception as exc:
            raise RecoveryError("Unable to commit MFI recovery storage") from exc

    def put(self, run_id, value):
        raw = encode(value)
        ref = f"{self.prefix}/{run_id}/{hashlib.sha256(raw).hexdigest()}"
        try:
            from google.api_core.exceptions import PreconditionFailed
            blob = self.storage.bucket(self.bucket).blob(ref)
            try:
                blob.upload_from_string(raw, content_type="application/json", if_generation_match=0)
            except PreconditionFailed:
                decode_checked(ref, blob.download_as_bytes())
            return ref
        except Exception as exc:
            raise RecoveryError("Unable to persist MFI recovery object") from exc

    def get(self, ref):
        if not ref.startswith(self.prefix + "/"):
            raise RecoveryError("Recovery object is outside the MFI namespace")
        try:
            return decode_checked(ref, self.storage.bucket(self.bucket).blob(ref).download_as_bytes())
        except Exception as exc:
            raise RecoveryError("Unable to read verified MFI recovery object") from exc


_memory = MemoryRecoveryStore()
_current: contextvars.ContextVar[Any] = contextvars.ContextVar("mfi_execution", default=None)


def recovery_store():
    """Reuse the existing backend selection without requiring cloud resources.

    Memory checkpoints live only in this process, including on Cloud Run.
    Once durable storage is selected, its errors propagate instead of creating
    a separate memory journal that other workers could not recover.
    """
    from app.shared.async_runs import _use_durable_store
    if _use_durable_store():
        return CloudRecoveryStore()
    return _memory


def current_execution():
    return _current.get()


def create_checkpoint(store, run_id, inputs, runtime):
    input_ref = store.put(run_id, inputs)
    def create(old):
        if old:
            if old["input_fingerprint"] != fingerprint(inputs):
                raise RecoveryError("Input changes require a new run", 409)
            return old
        return dict(workflow_revision=runtime.get("workflow", WORKFLOW_REVISION), contract_bundle=runtime.get("bundle", CONTRACT_BUNDLE),
                    runtime_fingerprint=fingerprint(runtime), runtime=runtime,
                    input_fingerprint=fingerprint(inputs), input_ref=input_ref, run_id=run_id,
                    run_revision=0, epoch=0, fence=0, owner=None, lease_until=0,
                    execution_state="pending", resumable=False, resume_block_reason=None,
                    tasks={}, requests={}, snapshot_ref=None, draft_revision=None,
                    analysis_available=False, durable=store.durable)
    return store.transaction(run_id, create)


def reconcile_expiry(store, run_id):
    value = store.read(run_id)
    if value and value.get("owner") and value.get("lease_until", 0) <= time.time():
        def expire(current):
            if current.get("owner") and current.get("lease_until", 0) <= time.time():
                current.update(owner=None, execution_state="interrupted", resumable=True,
                               run_revision=current["run_revision"] + 1)
            return current
        value = store.transaction(run_id, expire)
    return value


def reserve_execution(store, run_id, *, expected_revision=None, idempotency_key=None, runtime=None):
    reconcile_expiry(store, run_id)
    request_key = fingerprint(idempotency_key) if idempotency_key else None
    reservation = {"owner": uuid.uuid4().hex, "scheduled": True}
    def reserve(value):
        if value is None:
            raise RecoveryError("Run has no recovery checkpoint", 409)
        if request_key and request_key in value["requests"]:
            reservation.update(value["requests"][request_key], scheduled=False)
            return value
        if value["contract_bundle"] != (runtime or {}).get("bundle", CONTRACT_BUNDLE) or (runtime is not None and value["runtime_fingerprint"] != fingerprint(runtime)):
            raise RecoveryError("Recorded workflow or effective configuration is incompatible with this worker", 409)
        if expected_revision is not None and expected_revision != value["run_revision"]:
            raise RecoveryError("Run revision conflict", 409)
        if value.get("owner") or value["execution_state"] == "completed":
            raise RecoveryError("Run is already executing or completed", 409)
        if value["epoch"] and not value.get("resumable"):
            raise RecoveryError(value.get("resume_block_reason") or "Run cannot be resumed", 409)
        value.update(owner=reservation["owner"], fence=value["fence"] + 1,
                     epoch=value["epoch"] + 1, run_revision=value["run_revision"] + 1,
                     lease_until=time.time() + 120, execution_state="running", resumable=False)
        reservation.update(epoch=value["epoch"], fence=value["fence"], run_revision=value["run_revision"], run_id=run_id)
        if request_key:
            value["requests"][request_key] = dict(reservation)
        return value
    store.transaction(run_id, reserve)
    return reservation


class Execution:
    def __init__(self, store, run_id, reservation):
        self.store, self.run_id, self.reservation = store, run_id, reservation
        self.stopped = threading.Event()
        self.lost = threading.Event()
        previous = store.read(run_id) or {}
        self.resume_candidate = None
        self.previous_context_fingerprint = None
        self.object_references = {}
        self.force_new_corrections = "finalize" in str(previous.get("active_task", ""))
        if reservation.get("epoch", 1) > 1 and previous.get("snapshot_ref"):
            saved = load_snapshot(store, previous["snapshot_ref"])
            self.previous_context_fingerprint = previous.get("context_dependency_fingerprint") or fingerprint([saved.get("context_evidence", []), saved.get("context_status", {})])
            if saved.get("correction_attempts"):
                self.resume_candidate = {key: saved[key] for key in (
                    "dimension_narratives", "market_narratives", "executive_summary_narrative", "context_evidence") if key in saved}

    def change(self, mutation):
        def owned(value):
            if self.lost.is_set() or not value or value.get("owner") != self.reservation["owner"] or value["fence"] != self.reservation["fence"] or value["lease_until"] <= time.time():
                raise RecoveryError("Execution ownership was lost; late results cannot be committed", 409)
            mutation(value)
            value["run_revision"] += 1
            return value
        return self.store.transaction(self.run_id, owned)

    def heartbeat(self):
        while not self.stopped.wait(30):
            try:
                self.change(lambda value: value.update(lease_until=time.time() + 120))
            except Exception:
                self.lost.set()
                return

    def record_trace(self, trace):
        ref = self.store.put(self.run_id, trace)
        def commit(value):
            value["trace_ref"] = ref
            active = value.get("active_task")
            response = value.get("response_work", {}).get(active)
            if response and response.get("attempts") and response["status"] in {"running", "repairing"} and not response["attempts"][-1].get("call_id"):
                current = next((call for call in reversed(trace.get("calls", [])) if call.get("status") == "started"), None)
                if current:
                    response["call_id"] = current["call_id"]
                    response["attempts"][-1]["call_id"] = current["call_id"]
            for record in value["tasks"].values():
                if record["task_id"] == active and record["status"] == "running":
                    call_ids = [call["call_id"] for call in trace.get("calls", []) if call.get("status") == "started"]
                    record["call_ids"] = list(dict.fromkeys([*record.get("call_ids", []), *call_ids]))
        self.change(commit)

    @contextmanager
    def activate(self):
        token = _current.set(self)
        thread = threading.Thread(target=self.heartbeat, daemon=True, name="mfi-lease")
        thread.start()
        try:
            yield self
        finally:
            self.stopped.set()
            thread.join(timeout=2)
            _current.reset(token)

    def execute_once(self, task_id, dependencies, action, *, kind="work", epoch_scoped=False):
        manifest = self.store.read(self.run_id)
        dep = fingerprint([manifest["input_fingerprint"], manifest["contract_bundle"], dependencies,
                           self.reservation["epoch"] if epoch_scoped else None])
        key = fingerprint([task_id, dep])
        record = manifest["tasks"].get(key)
        if record and record["status"] == "succeeded":
            # Even cache reads cannot advance the graph after ownership is lost.
            self.change(lambda _: None)
            return self.store.get(record["output_ref"])
        def start(value):
            previous = value["tasks"].get(key, {})
            value["tasks"][key] = dict(task_id=task_id, kind=kind, input_fingerprint=dep,
                epoch=self.reservation["epoch"], status="running", attempt=previous.get("attempt", 0) + 1,
                started_at=time.time(), call_ids=previous.get("call_ids", []),
                previous_attempts=[*previous.get("previous_attempts", []), *([{k:v for k,v in previous.items() if k != "previous_attempts"}] if previous else [])])
            value["active_task"] = task_id
        self.change(start)
        try:
            output = action()
            ref = self.store.put(self.run_id, output)
            self.change(lambda value: value["tasks"][key].update(status="succeeded", output_ref=ref, completed_at=time.time()))
            return output
        except Exception as exc:
            self.change(lambda value: value["tasks"][key].update(status="failed", completed_at=time.time(),
                        error={"type": type(exc).__name__, "message": str(exc), "call_id": getattr(exc, "call_id", None)}))
            raise

    def snapshot(self, state):
        from .response_runtime import public_journal
        state = dict(state)
        manifest = self.store.read(self.run_id)
        if manifest.get("response_work"):
            state["response_validation"] = public_journal(manifest)
            state["llm_calls"] = manifest.get("model_attempt_total", state.get("llm_calls", 0))
            state["staged_response_fragments"] = [{"work_id":r["work_id"], "artifact_id":r.get("artifact_id"), "kind":r["kind"],
                "fragments":self.store.get(r["fragments_ref"])} for r in manifest["response_work"].values()
                if r.get("fragments_ref") and r["status"] != "succeeded" and r["kind"] in {"dimension","market","executive"}]
        def reference(value):
            key = fingerprint(value)
            if key not in self.object_references:
                self.object_references[key] = self.store.put(self.run_id, value)
            return self.object_references[key]
        fields = {key: reference(value) for key, value in state.items() if key != "visualizations"}
        figures = {key: reference(value) for key, value in state.get("visualizations", {}).items()}
        ref = self.store.put(self.run_id, {"snapshot_format":"mfi-field-references-v1",
            "field_references":fields, "figure_references":figures})
        def commit(value):
            value["snapshot_ref"] = ref
            value["analysis_available"] = bool(state.get("assessment_profile"))
            reviewed = (state.get("generation_diagnostics") or {}).get("red_team_status") == "completed"
            value["qa_evaluation_status"] = "evaluated" if reviewed else "not_evaluated"
            flags = {flag["flag_id"]: flag for flag in [*state.get("deterministic_flags", []), *state.get("red_team_flags", [])]}
            value["unresolved_counts"] = dict(Counter(flag["severity"] for flag in flags.values())) if reviewed else None
            if state.get("dimension_narratives") or state.get("market_narratives") or state.get("executive_summary_narrative") or any(r["fragments"] for r in state.get("staged_response_fragments", [])):
                value["draft_revision"] = value["run_revision"] + 1
                value.setdefault("snapshots", {})[str(value["draft_revision"])] = ref
        self.change(commit)

    def finish(self, error=None):
        def finalize(value):
            nonrepairable = getattr(error, "status_code", None) == 422 or getattr(error, "groups", None)
            value.update(owner=None, lease_until=0,
                         execution_state="failed" if error else "completed",
                         resumable=bool(error) and not bool(nonrepairable),
                         resume_block_reason=str(error) if nonrepairable else None,
                         last_error=str(error) if error else None)
        return self.change(finalize)


def execution_status(run_id, store=None, *, runtime=None):
    store = store or recovery_store()
    value = reconcile_expiry(store, run_id)
    if not value:
        return dict(workflow_revision=None, run_revision=0, resumable=False,
                    resume_block_reason="This historical run has no recovery checkpoint", draft_available=False,
                    draft_revision=None, analysis_available=False, execution_state="legacy")
    if value["contract_bundle"] != (runtime or {}).get("bundle", CONTRACT_BUNDLE) or (
        runtime is not None and value["runtime_fingerprint"] != fingerprint(runtime)
    ):
        value["resumable"] = False
        value["resume_block_reason"] = "The recorded workflow or effective configuration is incompatible with this deployment"
    latest = {}
    for row in value["tasks"].values():
        if row.get("started_at", 0) >= latest.get(row["task_id"], {}).get("started_at", 0):
            latest[row["task_id"]] = row
    totals = Counter(row["status"] for row in latest.values())
    reviewed = value.get("qa_evaluation_status") == "evaluated"
    qa_counts = value.get("unresolved_counts")
    from .response_runtime import public_journal
    response = public_journal(value) if value.get("response_work") else {}
    if value.get("workflow_revision") == "mfi-light-v1":
        from .light_runtime import public_diagnostics
        light = public_diagnostics(value)
        return {**{key: value.get(key) for key in ("workflow_revision", "run_revision", "execution_state", "resumable", "resume_block_reason", "analysis_available", "active_task", "last_error")},
            "draft_available": False, "draft_revision": None, "response_contract_bundle": value["contract_bundle"],
            "recovery_storage": "durable" if store.durable else "process_local",
            "recovery_limitation": None if store.durable else "Resume requires the same server process; restart recovery is unavailable.",
            "qa_evaluation_status": "reviewed" if value["execution_state"] == "completed" else "not_completed",
            "unresolved_counts": None, "work_totals": light["work_totals"], "light_progress": light}
    return {**{key: value.get(key) for key in ("workflow_revision", "run_revision", "execution_state",
            "resumable", "resume_block_reason", "draft_revision", "analysis_available", "active_task", "last_error")},
            "draft_available": value.get("draft_revision") is not None,
            "recovery_storage": "durable" if store.durable else "process_local",
            "recovery_limitation": None if store.durable else (
                "Saved progress and Resume are available only in the same server process. "
                "A server restart or a request routed to another instance cannot recover this run."),
            "qa_evaluation_status": "evaluated" if reviewed else "not_evaluated", "unresolved_counts": qa_counts,
            **{key:response.get(key) for key in ("structural_validation_issues", "structural_repair_summary", "degraded_work_count", "context_classification_outcome", "unresolved_context_statement_count")},
            "response_contract_bundle":value["contract_bundle"],
            "work_totals": response.get("response_work_totals") or {"planned": len(latest), "pending": totals["planned"], **{key: totals[key] for key in ("running", "succeeded", "failed")}}}
