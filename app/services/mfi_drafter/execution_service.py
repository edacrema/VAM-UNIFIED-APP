"""Shared entry point used by API and in-process MFI dispatch."""
from __future__ import annotations

import functools
import inspect
import uuid
from importlib.metadata import version

from .execution import (Execution, create_checkpoint, current_execution, recovery_store,
                        reserve_execution, load_snapshot)
from .reliable_contracts import CONTRACT_BUNDLE, WORKFLOW_REVISION


def effective_contract():
    from .response_contracts import contract_manifest
    from app.shared.llm import llm_runtime_config
    runtime = llm_runtime_config()
    config = runtime.model_dump() if hasattr(runtime, "model_dump") else vars(runtime)
    # Runtime config is a typed configuration object, not the environment or credentials.
    return {"bundle": CONTRACT_BUNDLE, "workflow": WORKFLOW_REVISION,
            "methodology": "databridge-current", "analysis_schema": "2.1", "narrative_schema": "2.1",
            "claim_identity": "mfi-claim-id-v2", "runtime": config, "response_contracts":contract_manifest(),
            "dependencies": {name: version(name) for name in (
                "pydantic", "langgraph", "langchain-core", "langchain-google-vertexai", "matplotlib")}}


def prepare_submission(run_id, csv_data):
    """Persist validated inputs and reserve ownership before acknowledging a run."""
    if csv_data.get("workflow_revision") != WORKFLOW_REVISION:
        return None
    from .graph import run_mfi_report_generation
    arguments = inspect.signature(run_mfi_report_generation).bind(
        country=csv_data["country"], data_collection_start=csv_data["data_collection_start"],
        data_collection_end=csv_data["data_collection_end"], markets=csv_data["markets"],
        csv_data=csv_data, run_id=run_id)
    arguments.apply_defaults()
    inputs = {key: value for key, value in arguments.arguments.items()
              if key not in {"on_step", "llm_trace_sink", "release_control"}}
    store = recovery_store()
    create_checkpoint(store, run_id, inputs, effective_contract())
    return reserve_execution(store, run_id, runtime=effective_contract())


def reliable_generation(function):
    @functools.wraps(function)
    def run(*args, **kwargs):
        reservation = kwargs.pop("execution_reservation", None)
        arguments = inspect.signature(function).bind(*args, **kwargs)
        arguments.apply_defaults()
        inputs = dict(arguments.arguments)
        csv = inputs.get("csv_data") or {}
        if csv.get("workflow_revision") != WORKFLOW_REVISION or current_execution():
            return function(*args, **kwargs)
        inputs["run_id"] = inputs.get("run_id") or f"mfi_{uuid.uuid4().hex[:8]}"
        callbacks = {key: inputs.pop(key) for key in ("on_step", "llm_trace_sink")}
        control = inputs.pop("release_control")
        store = recovery_store()
        create_checkpoint(store, inputs["run_id"], inputs, effective_contract())
        reservation = reservation or reserve_execution(store, inputs["run_id"], runtime=effective_contract())
        execution = Execution(store, inputs["run_id"], reservation)
        trace_sink = callbacks["llm_trace_sink"]
        def journal_trace(trace):
            execution.record_trace(trace)
            if trace_sink:
                trace_sink(trace)
        journal_trace.requires_persistence = True
        callbacks["llm_trace_sink"] = journal_trace
        with execution.activate():
            try:
                result = function(**inputs, **callbacks, release_control=control)
                execution.snapshot(result)
                execution.finish()
                return result
            except Exception as exc:
                manifest = store.read(inputs["run_id"])
                if manifest and manifest.get("snapshot_ref") and not execution.lost.is_set():
                    execution.snapshot(load_snapshot(store, manifest["snapshot_ref"]))
                execution.finish(exc)
                raise
    return run


def node_dependencies(name, state):
    """Explicit evidence projections; diagnostics never determine analytical reuse."""
    common = ("country", "data_collection_start", "data_collection_end")
    analysis = ("markets_data", "metric_summaries", "survey_metadata", "methodology_version", "score_authority", "excluded_market_records", "csv_data")
    narratives = ("dimension_narratives", "market_narratives", "executive_summary_narrative", "context_evidence", "context_status")
    projections = {
        "mfi_data_agent": (*common, "use_csv_data", "csv_data", "markets"),
        "mfi_analysis": analysis,
        "context_retrieval": common,
        "context_extractor": (*common, "contextual_documents", "retriever_traces"),
        "mfi_graph_designer": (*common, "assessment_profile", "markets_data", "metric_summaries", "survey_metadata"),
        "dimension_drafter": ("assessment_profile", "claim_catalog"),
        "market_recommendations_drafter": ("assessment_profile", "claim_catalog"),
        "executive_summary_drafter": (*common, "assessment_profile", "claim_catalog", *narratives),
    }
    keys = projections.get(name, (*common, "assessment_profile", "claim_catalog", *narratives,
        "contextual_documents", "deterministic_flags", "red_team_flags", "correction_history", "correction_attempts", "qa_review"))
    return {key:state.get(key) for key in keys}


def execute_node(name, state, function):
    execution = current_execution()
    if execution is None:
        return function(state)
    if name == "deterministic_claim_validator" and execution.resume_candidate:
        state = {**state, **execution.resume_candidate}
    # Model nodes replay their own response journals, not entire graph-state caches.
    # This also retries degraded context in a new epoch and recomputes disclosure.
    cached_nodes = {"mfi_data_agent", "mfi_analysis", "context_retrieval", "mfi_graph_designer"}
    if name in cached_nodes:
        def owned():
            result = function(state)
            diagnostics = result.pop("generation_diagnostics", {})
            before = state.get("generation_diagnostics", {})
            result["_diagnostic_delta"] = {k:v for k,v in diagnostics.items() if before.get(k) != v}
            for key in ("llm_diagnostics", "llm_calls", "current_node"):
                result.pop(key, None)
            return result
        updates = execution.execute_once(f"node:{name}", node_dependencies(name,state), owned, kind="node")
        delta = updates.pop("_diagnostic_delta", {})
        updates["generation_diagnostics"] = {**state.get("generation_diagnostics", {}), **delta}
    else:
        updates = function(state)
    if name == "context_extractor":
        from .reliable_contracts import fingerprint
        new_context = fingerprint([updates.get("context_evidence", []), updates.get("context_status", {})])
        if execution.previous_context_fingerprint and execution.previous_context_fingerprint != new_context:
            # Previously corrected candidates may contain claims about old context.
            # Rebuild from independently cached drafts and freshly drafted context.
            execution.resume_candidate = None
            def invalidate(value):
                for row in value.get("response_work", {}).values():
                    if row["kind"] in {"review", "correction"}:
                        row["status"] = "superseded"
            execution.change(invalidate)
        def context_outcome(value):
            value["context_dependency_fingerprint"] = new_context
            rows = value.setdefault("response_work", {})
            if not any(r["kind"] == "context" for r in rows.values()):
                rows["context:context_evidence"] = {"work_id":"context:context_evidence", "kind":"context",
                    "operation":"mfi.context_classification.v1", "status":"succeeded", "issues":[], "attempts":[],
                    "artifact_id":"context_evidence", "skip_reason":"no_documents"}
            if (updates.get("generation_diagnostics") or {}).get("context_classification_status") == "failed":
                for row in rows.values():
                    if row["kind"] == "context" and row["status"] == "failed":
                        row["status"] = "degraded"
        execution.change(context_outcome)
    from .response_runtime import public_journal, plan_drafts
    profile = updates.get("assessment_profile") or state.get("assessment_profile")
    if name == "mfi_analysis":
        plan_drafts(execution, profile, updates.get("claim_catalog", state.get("claim_catalog", {})))
    manifest = execution.store.read(execution.run_id)
    if manifest.get("response_work"):
        updates["generation_diagnostics"] = {**state.get("generation_diagnostics", {}), **updates.get("generation_diagnostics", {}), **public_journal(manifest)}
        updates["llm_calls"] = manifest.get("model_attempt_total", state.get("llm_calls", 0))
    updates["current_node"] = name
    snapshot = dict(state)
    snapshot.update(updates)
    execution.snapshot(snapshot)
    return updates


def save_partial(state, **updates):
    execution = current_execution()
    if execution:
        from .response_runtime import public_journal
        diagnostics = {**state.get("generation_diagnostics", {}), **updates.get("generation_diagnostics", {}), **public_journal(execution.store.read(execution.run_id))}
        execution.snapshot({**state, **updates, "generation_diagnostics":diagnostics})


def get_mfi_run(run_id):
    """The committed MFI manifest is authoritative over legacy progress caches."""
    from app.shared.async_runs import get_run, RunRecord
    from .execution import reconcile_expiry
    from copy import deepcopy
    run = get_run(run_id)
    if run is not None and run.status == "completed" and not (run.metadata or {}).get("workflow_revision"):
        return run
    store = recovery_store()
    manifest = reconcile_expiry(store, run_id)
    if not manifest:
        return run
    run = deepcopy(run) if run is not None else RunRecord(metadata={"workflow_revision": WORKFLOW_REVISION})
    run.metadata = dict(run.metadata or {})
    diagnostics = dict(run.metadata.get("generation_diagnostics") or {})
    if manifest.get("response_work"):
        from .response_runtime import public_journal
        diagnostics.update(public_journal(manifest))
    if manifest.get("trace_ref"):
        run.metadata["llm_diagnostics"] = store.get(manifest["trace_ref"])
    diagnostics["qa_evaluation_status"] = manifest.get("qa_evaluation_status", "not_evaluated")
    if manifest.get("qa_evaluation_status") != "evaluated":
        for field in ("unresolved_high_count", "unresolved_medium_count", "unresolved_low_count", "blocking_high_count"):
            diagnostics.pop(field, None)
    else:
        counts = manifest.get("unresolved_counts") or {}
        for severity in ("high", "medium", "low"):
            diagnostics[f"unresolved_{severity}_count"] = counts.get(severity, 0)
        if manifest["execution_state"] != "completed":
            diagnostics.pop("blocking_high_count", None)
    run.metadata["generation_diagnostics"] = diagnostics
    state = manifest["execution_state"]
    run.status = "failed" if state == "interrupted" else state if state in {"pending", "running", "completed", "failed"} else "failed"
    if state == "completed":
        run.result = load_snapshot(store, manifest["snapshot_ref"])
        run.error = None
    elif state in {"failed", "interrupted"}:
        run.current_node = manifest.get("active_task") or run.current_node
        run.error = manifest.get("last_error") or "Execution lease expired; Resume is available."
    return run


def schedule_resume(run_id, expected_revision, idempotency_key, schedule):
    from .execution import RecoveryError
    if get_mfi_run(run_id) is None:
        raise RecoveryError("Run ID not found", 404)
    store = recovery_store()
    reservation = reserve_execution(store, run_id, expected_revision=expected_revision,
                                    idempotency_key=idempotency_key, runtime=effective_contract())
    if reservation["scheduled"]:
        schedule(execute_resumed, run_id, reservation)
    return {key: value for key, value in reservation.items() if key not in {"owner", "fence", "scheduled"}} | {"status": "running"}


def execute_resumed(run_id, reservation):
    from app.shared.async_runs import update_run, set_run_completed, set_run_failed
    from .graph import run_mfi_report_generation
    store = recovery_store()
    manifest = store.read(run_id)
    inputs = store.get(manifest["input_ref"])
    def on_step(name, state):
        update_run(run_id, current_node=name, metadata={"generation_diagnostics": state.get("generation_diagnostics", {}),
                                                      "qa_review": state.get("qa_review", {})})
    try:
        update_run(run_id, status="running", error=None, traceback=None)
        result = run_mfi_report_generation(**inputs, on_step=on_step, execution_reservation=reservation)
        set_run_completed(run_id, result=result)
    except Exception as exc:
        # Never let a stale worker change a new execution's progress cache.
        current = store.read(run_id)
        if current and current["epoch"] == reservation["epoch"]:
            set_run_failed(run_id, error=str(exc))
