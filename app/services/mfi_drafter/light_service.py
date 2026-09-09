"""Public lightweight MFI execution, shared by HTTP and in-process dispatch."""
from __future__ import annotations
import inspect
import uuid
from copy import deepcopy
from importlib.metadata import version
from .execution import (Execution, RecoveryError, recovery_store, create_checkpoint,
                        reserve_execution, load_snapshot)
from .light_contracts import WORKFLOW, BUNDLE, MODEL, MAX_CHARACTERS, MAX_INPUT_TOKENS, MAX_OUTPUT_TOKENS, NODES
from .light_runtime import public_diagnostics


def effective_contract():
    from .light_contracts import instructions, response_schema
    from .reliable_contracts import fingerprint
    return {"workflow": WORKFLOW, "bundle": BUNDLE, "model": MODEL, "location": "global", "temperature": 1.0,
        "analysis_schema": "2.1", "narrative_schema": "3.0", "methodology": "databridge-current",
        "timeout": 600, "summary_timeout": 180, "max_attempts": 2, "sdk_retries": 0,
        "max_characters": MAX_CHARACTERS, "max_input_tokens": MAX_INPUT_TOKENS, "max_output_tokens": MAX_OUTPUT_TOKENS,
        "schema_hashes": {"sections": fingerprint(response_schema()), "review": fingerprint(response_schema(True))},
        "prompt_hashes": {node: fingerprint(instructions(node)) for node in NODES if node.startswith(("draft_", "review_", "correct_")) or node == "executive_summary"},
        "dependencies": {name:version(name) for name in ("pydantic", "langgraph", "langchain-core", "langchain-google-vertexai")}}


def runtime_status():
    return {"status": "configured", "provider": "vertex_ai", **effective_contract(),
            "access_validation": "performed_on_invocation"}


def inputs_for(**kwargs):
    bound = inspect.signature(run_mfi_report_generation).bind(**kwargs)
    bound.apply_defaults()
    return {k:v for k,v in bound.arguments.items() if k not in {"on_step", "llm_trace_sink", "release_control", "execution_reservation", "client"}}


def prepare_submission(run_id, csv_data):
    inputs = inputs_for(country=csv_data["country"], data_collection_start=csv_data["data_collection_start"],
        data_collection_end=csv_data["data_collection_end"], markets=csv_data["markets"], csv_data=csv_data, run_id=run_id)
    store = recovery_store()
    create_checkpoint(store, run_id, inputs, effective_contract())
    return reserve_execution(store, run_id, runtime=effective_contract())


def run_mfi_report_generation(country, data_collection_start, data_collection_end, markets, csv_data=None,
        on_step=None, release_control=None, run_id=None, llm_trace_sink=None, execution_reservation=None, *, client=None):
    from .features import require_mfi_analysis_v2
    from .light_graph import build_graph
    control = require_mfi_analysis_v2(release_control)
    run_id = run_id or "mfi_"+uuid.uuid4().hex[:8]
    inputs = inputs_for(country=country, data_collection_start=data_collection_start, data_collection_end=data_collection_end,
        markets=markets, csv_data=csv_data, run_id=run_id)
    store = recovery_store()
    create_checkpoint(store, run_id, inputs, effective_contract())
    reservation = execution_reservation or reserve_execution(store, run_id, runtime=effective_contract())
    execution = Execution(store, run_id, reservation)
    with execution.activate():
        execution.change(lambda v: v.setdefault("light_phases", {name:{"status":"pending"} for name in NODES}))
        try:
            graph = build_graph(execution, client=client, on_step=on_step, trace_sink=llm_trace_sink)
            result = graph.invoke({"base": {**inputs, "release_control": control.model_dump()}}, config={"recursion_limit": 30})["report"]
            diagnostics = public_diagnostics(store.read(run_id))
            result["llm_diagnostics"] = {**diagnostics.pop("llm_diagnostics"), "status": "completed"}
            result["generation_diagnostics"] = diagnostics
            result["response_contract_bundle"] = BUNDLE
            result["effective_contract"] = effective_contract()
            result["llm_calls"] = diagnostics["model_attempt_total"]
            execution.snapshot(result)
            execution.finish()
            return result
        except Exception as exc:
            # A late worker must never change the execution now owned by Resume.
            current = store.read(run_id)
            if current and current.get("owner") == reservation["owner"] and current.get("fence") == reservation["fence"]:
                execution.finish(exc)
            raise


def get_light_run(run_id, manifest, store, existing=None):
    from app.shared.async_runs import RunRecord
    run = deepcopy(existing) if existing else RunRecord()
    diagnostics = public_diagnostics(manifest)
    run.metadata = {**{k:v for k,v in (run.metadata or {}).items() if k in {
        "release_control", "context_status", "context_counts", "retriever_traces", "live_outputs"}},
        "workflow_revision": WORKFLOW, "response_contract_bundle": BUNDLE,
        "generation_diagnostics": {k:v for k,v in diagnostics.items() if k != "llm_diagnostics"},
        "llm_diagnostics": diagnostics["llm_diagnostics"]}
    run.status = "failed" if manifest["execution_state"] == "interrupted" else manifest["execution_state"]
    active = [p["node"] for p in diagnostics["phases"] if p["status"] == "running"]
    failed = [p["node"] for p in diagnostics["phases"] if p["status"] == "failed"]
    run.current_node = ", ".join(active or failed) or manifest.get("active_task")
    run.progress_pct = diagnostics["progress_pct"]
    run.error = manifest.get("last_error")
    run.result = load_snapshot(store, manifest["snapshot_ref"]) if run.status == "completed" and manifest.get("snapshot_ref") else None
    return run


def schedule_resume(run_id, expected_revision, idempotency_key, schedule):
    store = recovery_store()
    if not store.read(run_id):
        from app.shared.async_runs import get_run
        raise RecoveryError("Run has no compatible lightweight checkpoint", 409 if get_run(run_id) else 404)
    reservation = reserve_execution(store, run_id, expected_revision=expected_revision,
        idempotency_key=idempotency_key, runtime=effective_contract())
    if reservation["scheduled"]:
        schedule(execute_resumed, run_id, reservation)
    return {k:v for k,v in reservation.items() if k not in {"owner", "fence", "scheduled"}} | {"status":"running"}


def execute_resumed(run_id, reservation):
    from app.shared.async_runs import update_run, set_run_completed, set_run_failed
    store = recovery_store()
    inputs = store.get(store.read(run_id)["input_ref"])
    try:
        update_run(run_id, status="running", error=None, traceback=None)
        result = run_mfi_report_generation(**inputs, execution_reservation=reservation)
        set_run_completed(run_id, result=result)
    except Exception as exc:
        current = store.read(run_id)
        if current and current["epoch"] == reservation["epoch"]:
            set_run_failed(run_id, error=str(exc))
