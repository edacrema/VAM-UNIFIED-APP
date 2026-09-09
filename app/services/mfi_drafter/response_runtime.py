"""MFI-only response journal and bounded, path-authorized structural repair.

Raw candidates live in the configured recovery store, never public diagnostics.
Analytical validation remains downstream of this structural boundary.
"""
from __future__ import annotations

import copy
import json
import time
import re
from collections import Counter

from langchain_core.messages import HumanMessage
from app.shared.llm_observability import LLMCallError, TracedLLMResult
from .execution import current_execution, RecoveryError
from .packages import ensure_message_budget, serialized, bounded_groups
from .reliable_contracts import fingerprint
from .response_contracts import (CONTRACTS, Claim, ContextStatement, expanded_schema,
                                 compile_provider_schema, inspect_response, bound_request)


def parse_response(raw):
    # Preserve the established code-fence adapter, but reject trailing objects,
    # explanatory prose and non-finite JSON constants at this MFI-only boundary.
    cleaned = re.sub(r"\A\s*```(?:json)?\s*([\s\S]*?)\s*```\s*\Z", r"\1", raw, flags=re.IGNORECASE)
    def invalid_constant(value):
        raise json.JSONDecodeError(f"Non-finite JSON constant: {value}", cleaned, 0)
    return json.loads(cleaned, parse_constant=invalid_constant)


def syntax_tokens(raw):
    """Content tokens must survive a syntax-only repair; punctuation may change."""
    pattern = r'"(?:\\.|[^"\\])*"|(?<![\w.])-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?|\b(?:true|false|null)\b'
    return re.findall(pattern, raw)


def read_path(value, path):
    try:
        for part in path:
            value = value[part]
        return value
    except (KeyError, IndexError, TypeError):
        return {"__missing_field__": True}


def write_path(value, path, replacement, *, remove=False):
    if not path:
        return copy.deepcopy(replacement)
    parent = value
    for part in path[:-1]:
        parent = parent[part]
    if remove:
        del parent[path[-1]]
    elif isinstance(parent,list) and path[-1] == len(parent):
        parent.append(copy.deepcopy(replacement))
    else:
        parent[path[-1]] = copy.deepcopy(replacement)
    return value


def path_schema(schema, path):
    for part in path:
        if "anyOf" in schema:
            schema = next(item for item in schema["anyOf"] if item.get("type") != "null")
        schema = schema["items"] if isinstance(part, int) else schema["properties"][part]
    return schema


def valid_fragments(payload, issues):
    """Accepted, typed fragments only; paths make missing components visible."""
    fragments = []
    def visit(value, path):
        blocked = any(i["path"][:len(path)] == path or path[:len(i["path"])] == i["path"] for i in issues)
        if isinstance(value, dict):
            if "text" in value and not blocked:
                model = ContextStatement if "classification" in value else Claim
                try:
                    model.model_validate(value, strict=True)
                except ValueError:
                    pass
                else:
                    fragments.append({"path": path, "value": copy.deepcopy(value)})
                    return
            for key, item in value.items():
                visit(item, [*path, key])
        elif isinstance(value, list):
            for index, item in enumerate(value):
                visit(item, [*path, index])
    visit(payload, [])
    return fragments


def apply_field_patches(payload, patched, group, contract, context, journal):
    entries = patched.get("patches") if isinstance(patched, dict) and set(patched) == {"patches"} else None
    if not isinstance(entries, list):
        return payload
    ids = [t["target_id"] for t in group]
    supplied = [p.get("target_id") for p in entries if isinstance(p,dict)]
    if len(supplied) != len(entries) or any(not isinstance(i,str) for i in supplied) or len(set(supplied)) != len(supplied) or set(supplied) - set(ids):
        return payload
    for target in group:
        entry = next((e for e in entries if e["target_id"] == target["target_id"]), None)
        if entry is None:
            continue
        if set(entry) != {"target_id","expected_hash","replacement"} or entry["expected_hash"] != target["expected_hash"]:
            continue
        if fingerprint(read_path(payload,target["path"])) != target["expected_hash"]:
            continue
        if target["remove"] and entry["replacement"] is not True:
            continue
        candidate = write_path(copy.deepcopy(payload), target["path"], entry["replacement"], remove=target["remove"])
        new_issues = inspect_response(contract, candidate, context)
        if any(i["path"][:len(target["path"])] == target["path"] for i in new_issues):
            continue
        payload = candidate
        journal.stage(payload, new_issues)
    return payload


def validate_repair_response(patched, payload, group, contract, context):
    """Pure call-level validation; independently accepted fields are staged later."""
    entries = patched.get("patches") if isinstance(patched, dict) and set(patched) == {"patches"} else None
    expected = {t["target_id"]:t for t in group}
    if not isinstance(entries, list) or any(not isinstance(p,dict) for p in entries):
        raise ValueError("Repair response requires a patches array of objects")
    ids = [p.get("target_id") for p in entries]
    if any(not isinstance(i,str) for i in ids) or len(set(ids)) != len(ids) or set(ids) != set(expected):
        raise ValueError("Every authorized repair target must occur exactly once")
    candidate = copy.deepcopy(payload)
    for target in group:
        entry = next(e for e in entries if e["target_id"] == target["target_id"])
        if set(entry) != {"target_id","expected_hash","replacement"} or entry["expected_hash"] != target["expected_hash"]:
            raise ValueError(f"Repair target {target['target_id']} has an invalid envelope or stale hash")
        if target["remove"] and entry["replacement"] is not True:
            raise ValueError("A field-removal target requires true")
        candidate = write_path(candidate, target["path"], entry["replacement"], remove=target["remove"])
        errors = [i for i in inspect_response(contract,candidate,context) if i["path"][:len(target["path"])] == target["path"]]
        if errors:
            raise ValueError(serialized(errors))
    return patched


class Journal:
    def __init__(self, work_id, kind, dependencies, metadata):
        self.execution = current_execution()
        self.epoch = self.execution.reservation["epoch"] if self.execution else 1
        self.work_id, self.kind = work_id, kind
        self.dep = fingerprint(dependencies)
        self.local_objects = {}
        existing = ((self.execution.store.read(self.execution.run_id) or {}).get("response_work", {}).get(work_id, {})
                    if self.execution else {})
        self.record = copy.deepcopy(existing) if existing.get("dependency_fingerprint") == self.dep else {
            **metadata, "work_id":work_id, "kind":kind, "dependency_fingerprint":self.dep,
            "status":"planned", "attempts":[], "issues":[], "created_at":time.time()}
        if existing and existing.get("dependency_fingerprint") != self.dep:
            self.record["superseded"] = [*existing.get("superseded", []), self.put({k:v for k,v in existing.items() if k != "superseded"})]
        self.save()

    def put(self, value):
        if self.execution:
            return self.execution.store.put(self.execution.run_id, value)
        key = fingerprint(value)
        self.local_objects[key] = copy.deepcopy(value)
        return key

    def get(self, ref):
        return self.execution.store.get(ref) if self.execution else copy.deepcopy(self.local_objects[ref])

    def save(self, **changes):
        self.record.update(changes)
        if self.execution:
            saved = copy.deepcopy(self.record)
            def commit(value):
                rows = value.setdefault("response_work", {})
                old = rows.get(self.work_id, {})
                previous = len(old.get("attempts", [])) if old.get("dependency_fingerprint") == saved.get("dependency_fingerprint") else 0
                value["model_attempt_total"] = value.get("model_attempt_total", 0) + max(0, len(saved["attempts"]) - previous)
                rows[self.work_id] = saved
                if saved["status"] in {"running", "repairing", "failed"}:
                    value["active_task"] = self.work_id
            self.execution.change(commit)

    def stage(self, payload, issues):
        self.save(candidate_ref=self.put(payload), issues=issues,
                  fragments_ref=self.put(valid_fragments(payload, issues)))


def public_journal(manifest):
    rows = [row for row in manifest.get("response_work", {}).values() if row["status"] != "superseded"]
    issues = [{**issue, "work_id":r["work_id"], "artifact_id":r.get("artifact_id"), "artifact_type":r.get("artifact_type"),
               "severity":"warning" if r["kind"] == "context" else "high", "required":r["kind"] != "context",
               "flag_id":fingerprint([r["work_id"], r["dependency_fingerprint"], issue["path"], issue["code"]])}
              for r in rows for issue in r.get("issues", [])]
    counts = Counter(r["status"] for r in rows)
    drafts = [{"batch_id":r["work_id"], "batch_kind": "complete_dimension" if r["kind"] == "dimension" else "selected_markets",
               "artifact_ids":r.get("artifact_ids", [r.get("artifact_id")]), "status":{"succeeded":"completed","planned":"pending"}.get(r["status"],r["status"]),
               "operation":r["operation"], "call_id":r.get("call_id"), "prompt_character_count":r.get("prompt_character_count"),
               "response_contract_version":r.get("contract_version")}
              for r in rows if r["kind"] in {"dimension","market"}]
    dimensions = {}
    for r in rows:
        if r["kind"] == "dimension":
            dimensions.setdefault(r["artifact_id"], []).append(r["status"])
    context = next((r for r in rows if r["kind"] == "context"), {})
    return {"structural_validation_issues":issues, "degraded_work_count":counts["degraded"],
            "response_work_totals":{"planned":len(rows), "pending":counts["planned"], **{s:counts[s] for s in ("running","repairing","succeeded","failed","degraded")}},
            "model_attempt_count":manifest.get("model_attempt_total", sum(len(r["attempts"]) for r in rows)),
            "structural_repair_summary":{"attempted":sum(a["kind"] != "initial" for r in rows for a in r["attempts"]),
                "unresolved_target_count":len(issues)},
            "context_classification_outcome":context.get("status", "not_started"),
            "unresolved_context_statement_count":len({tuple(i["path"][:2]) for i in context.get("issues", [])}),
            "draft_batches":drafts, "draft_batches_total":len(drafts),
            "draft_batches_completed":sum(r["status"] == "completed" for r in drafts),
            "draft_batches_failed":sum(r["status"] == "failed" for r in drafts),
            "dimensions":{"llm":[name for name,states in dimensions.items() if all(s == "succeeded" for s in states)], "fallback":[]},
            "narrative_orchestration_version":"mfi-reliable-v1"}


def plan_drafts(execution, profile, catalog):
    from .simple_orchestration import ordered_dimension_profiles, build_budgeted_market_draft_batches
    planned = [(f"dimension:{d['dimension']}:1", "dimension", d["dimension"], [d["dimension"]], "mfi.complete_dimension.v1") for d in ordered_dimension_profiles(profile)]
    planned += [(b["batch_id"], "market", b["batch_id"], b["artifact_ids"], "mfi.market_batch_drafting.v2") for b in build_budgeted_market_draft_batches(profile,catalog)]
    def commit(value):
        rows = value.setdefault("response_work", {})
        for work_id,kind,artifact,ids,operation in planned:
            rows.setdefault(work_id, {"work_id":work_id, "kind":kind, "artifact_id":artifact,
                "artifact_ids":ids, "operation":operation, "status":"planned", "issues":[], "attempts":[]})
    execution.change(commit)


def capture_correction_attempt(*, trace, target_ids, dependencies, attempt_index, **kwargs):
    """Use the response journal without adding a repair loop to typed correction."""
    work_id = "correction:" + fingerprint([kwargs["operation"], target_ids])[:20]
    journal = Journal(work_id, "correction", dependencies,
        {"operation":kwargs["operation"], "artifact_id":kwargs["artifact_id"], "artifact_type":"narrative_fields",
         "target_ids":target_ids, "contract_version":CONTRACTS["patch_" + dependencies["patch_kind"]].version})
    key = fingerprint([journal.epoch, attempt_index])
    attempts = journal.record["attempts"]
    attempt = next((a for a in attempts if a["attempt_key"] == key), None)
    if attempt and attempt.get("response_ref"):
        try:
            payload = parse_response(journal.get(attempt["response_ref"])["raw_text"])
        except json.JSONDecodeError:
            return {"error":"Saved correction response is not valid JSON", "call_id":attempt["call_id"], "journal_work_id":work_id}
        return {"payload":payload, "call_id":attempt["call_id"], "journal_work_id":work_id}
    if attempt:
        raise RecoveryError("Interrupted correction attempt has no saved response; use Resume", 409)
    attempt = {"attempt_key":key, "kind":"initial" if not attempt_index else "fields", "epoch":journal.epoch,
               "target_ids":target_ids, "status":"started", "started_at":time.time()}
    attempts.append(attempt)
    journal.save(status="running", epoch=journal.epoch)
    def capture(value):
        attempt.update(response_ref=journal.put({**value, "work_id":work_id, "epoch":journal.epoch,
            "contract_version":journal.record["contract_version"], "dependency_fingerprint":journal.dep}), call_id=value["call_id"])
        journal.save(call_id=value["call_id"])
    try:
        result = trace.invoke_json(**kwargs, response_sink=capture, response_parser=parse_response)
    except LLMCallError as exc:
        attempt.update(status="failed", failure_code=exc.failure_code, call_id=exc.call_id)
        journal.save(status="failed", call_id=exc.call_id)
        if exc.failure_code not in {"llm_invalid_json", "llm_response_contract_error"}:
            raise
        return {"error":str(exc), "call_id":exc.call_id, "journal_work_id":work_id}
    attempt.update(status="succeeded", call_id=result.call_id)
    journal.save(candidate_ref=journal.put(result.payload), call_id=result.call_id)
    return {"payload":result.payload, "call_id":result.call_id, "journal_work_id":work_id}


def correction_validation(work_id, errors, normalized, valid_targets=()):
    execution = current_execution()
    if not execution or not work_id:
        return
    def commit(value):
        for old in value["response_work"].values():
            if old["kind"] == "correction" and old.get("issues"):
                old["issues"] = [issue for issue in old["issues"] if issue["path"][0] not in valid_targets]
                if not old["issues"]:
                    old["status"] = "succeeded"
        row = value["response_work"][work_id]
        row.update(status="failed" if errors else "succeeded", normalizations=normalized,
            issues=[{"path":[target], "code":"invalid_correction_patch", "message":str(error), "repairable":True} for target,error in errors.items()])
    execution.change(commit)


def invoke_response(*, response_contract, contract_context=None, **kwargs):
    contract = CONTRACTS[response_contract]
    context = contract_context or {}
    validator = kwargs.pop("validator")
    trace = kwargs.pop("trace")
    model = kwargs.pop("model")
    messages = kwargs.pop("messages")
    original_messages = messages
    messages, schema = bound_request(contract.kind, messages)
    count = ensure_message_budget(messages, schema)
    work_id = str(kwargs.get("batch_id") or kwargs.get("task_id") or f"{contract.kind}:{kwargs.get('artifact_id')}")
    if contract.kind == "review":
        work_id = kwargs["node"] + ":" + work_id
    journal = Journal(work_id, contract.kind, [kwargs["operation"], contract.version, contract.schema_hash, [m.content for m in messages], context],
                      {"operation":kwargs["operation"], "artifact_id":kwargs.get("artifact_id"), "artifact_type":kwargs.get("artifact_type"),
                       "artifact_ids":context.get("market_names") or [kwargs.get("artifact_id")],
                       "contract_version":contract.version, "schema_hash":contract.schema_hash, "prompt_character_count":count})
    record = journal.record
    calls_before = len(record["attempts"])

    def structural(payload):
        issues = inspect_response(contract, payload, context)
        if issues:
            raise ValueError(serialized(issues))
        return payload

    def call(kind, outgoing, response_schema, validate, target_ids=(), targets=None):
        # Reserve the allowance before invocation. Captured replies can be consumed
        # again after interruption without another paid call.
        outgoing_size = ensure_message_budget(outgoing, response_schema)
        attempt_key = fingerprint([kind, list(target_ids), journal.epoch])
        attempt = next((a for a in record["attempts"] if a["attempt_key"] == attempt_key), None)
        if attempt and attempt.get("response_ref"):
            captured = journal.get(attempt["response_ref"])
            return parse_response(captured["raw_text"]), attempt["call_id"]
        if attempt:
            raise RecoveryError("The reserved response attempt has no recoverable reply; use Resume for a new execution epoch", 409)
        attempt = {"attempt_key":attempt_key, "kind":kind, "epoch":journal.epoch, "target_ids":list(target_ids),
                   "status":"started", "started_at":time.time(), "prompt_character_count":outgoing_size}
        if targets is not None:
            attempt["targets_ref"] = journal.put(targets)
        record["attempts"].append(attempt)
        journal.save(status="running" if kind == "initial" else "repairing", epoch=journal.epoch)
        def capture(value):
            attempt.update(response_ref=journal.put({**value, "operation":kwargs["operation"], "work_id":work_id,
                "contract_version":contract.version, "dependency_fingerprint":journal.dep, "epoch":journal.epoch}), call_id=value["call_id"])
            journal.save(call_id=value["call_id"])
            if kind == "initial":
                try:
                    candidate = parse_response(value["raw_text"])
                except json.JSONDecodeError:
                    pass
                else:
                    journal.stage(candidate, inspect_response(contract, candidate, context))
        invocation = {**kwargs, "model":model.bind(response_mime_type="application/json", response_schema=response_schema),
                      "messages":outgoing, "validator":validate, "response_sink":capture, "response_parser":parse_response}
        if kind != "initial":
            invocation["operation"] += ".structural_repair.v1"
        try:
            result = trace.invoke_json(**invocation)
        except LLMCallError as exc:
            attempt.update(status="failed", failure_code=exc.failure_code, call_id=exc.call_id)
            journal.save(call_id=exc.call_id)
            if exc.failure_code not in {"llm_invalid_json", "llm_response_contract_error"}:
                raise
            if not attempt.get("response_ref"):
                raise
            captured = journal.get(attempt["response_ref"])
            return parse_response(captured["raw_text"]), exc.call_id
        except Exception as exc:
            attempt.update(status="interrupted", error_type=type(exc).__name__)
            journal.save()
            raise
        attempt.update(status="succeeded")
        journal.save(call_id=result.call_id)
        return result.payload, result.call_id

    def finish(payload, issues):
        source_indices = None
        if issues and contract.kind == "context":
            statements = payload.get("statements", []) if isinstance(payload, dict) else []
            if not isinstance(statements, list):
                statements = []
            source_indices = [i for i in range(len(statements)) if not any(not x["path"] or x["path"] == ["statements"] or x["path"][:2] == ["statements",i] for x in issues)]
            payload = {"statements":[statements[i] for i in source_indices]}
        elif issues:
            journal.save(status="failed", epoch=journal.epoch)
            raise LLMCallError(failure_code="llm_response_contract_error", call_id=record.get("call_id") or "saved-response",
                node=kwargs["node"], operation=kwargs["operation"], stage="contract_validation",
                artifact_type=kwargs.get("artifact_type"), artifact_id=kwargs.get("artifact_id"), batch_id=kwargs.get("batch_id"))
        value = validator(payload)
        if not issues:
            failed_calls = {c["call_id"] for c in trace.snapshot().get("calls", []) if c["status"] == "failed"}
            for attempt in record["attempts"]:
                if attempt.get("failure_code") in {"llm_invalid_json", "llm_response_contract_error"} and attempt.get("call_id") in failed_calls:
                    trace.mark_recovered(attempt["call_id"])
        output = {"call_id":record.get("call_id") or "saved-response", "payload":payload, "value":value}
        if contract.kind == "context":
            output["value"] = {**value, "_structural_issues":issues,
                               "_source_indices":source_indices if source_indices is not None else list(range(len(payload["statements"])))}
        journal.save(status="degraded" if issues else "succeeded", epoch=journal.epoch, output_ref=journal.put(output))
        return TracedLLMResult(raw_text=serialized(payload), **output), len(record["attempts"]) - calls_before

    if record.get("output_ref") and (record["status"] == "succeeded" or record["status"] == "degraded" and record.get("epoch") == journal.epoch):
        output = journal.get(record["output_ref"])
        return TracedLLMResult(raw_text=serialized(output["payload"]), **output), 0
    try:
        syntax_failure = any(i["code"] in {"invalid_json", "syntax_repair_changed_content"} for i in record.get("issues", []))
        if record.get("candidate_ref") and not syntax_failure:
            payload = journal.get(record["candidate_ref"])
        else:
            # Resume consumes a captured original response even if the worker stopped
            # before parsing. A transport failure without a response retries next epoch.
            original = next((a for a in reversed(record["attempts"]) if a["kind"] == "initial" and a.get("response_ref")), None)
            try:
                if original:
                    payload = parse_response(journal.get(original["response_ref"])["raw_text"])
                else:
                    payload, _ = call("initial", messages, schema, structural)
            except json.JSONDecodeError:
                original = next(a for a in reversed(record["attempts"]) if a["kind"] == "initial" and a.get("response_ref"))
                syntax = [HumanMessage(content="Repair JSON syntax only. Preserve every original value and prose. This consumes the structural repair allowance.\n" + journal.get(original["response_ref"])["raw_text"])]
                try:
                    saved_syntax = next((a for a in reversed(record["attempts"]) if a["kind"] == "syntax" and a.get("response_ref")), None)
                    reusable_syntax = False
                    if saved_syntax:
                        try:
                            payload = parse_response(journal.get(saved_syntax["response_ref"])["raw_text"])
                            reusable_syntax = syntax_tokens(journal.get(original["response_ref"])["raw_text"]) == syntax_tokens(journal.get(saved_syntax["response_ref"])["raw_text"])
                        except json.JSONDecodeError:
                            pass
                    if not reusable_syntax:
                        payload, _ = call("syntax", syntax, schema, structural, ["whole-response"])
                except json.JSONDecodeError:
                    journal.stage(None, [{"path":[], "code":"invalid_json", "message":"JSON syntax repair failed", "repairable":True}])
                    return finish(None, record["issues"])
                repaired = next(a for a in reversed(record["attempts"]) if a["kind"] == "syntax" and a.get("response_ref"))
                if syntax_tokens(journal.get(original["response_ref"])["raw_text"]) != syntax_tokens(journal.get(repaired["response_ref"])["raw_text"]):
                    journal.stage(None, [{"path":[], "code":"syntax_repair_changed_content", "message":"Syntax repair changed original content tokens", "repairable":True}])
                    return finish(None, record["issues"])
        for attempt in record["attempts"]:
            if attempt["kind"] != "fields" or not attempt.get("response_ref") or not attempt.get("targets_ref"):
                continue
            try:
                patched = parse_response(journal.get(attempt["response_ref"])["raw_text"])
            except json.JSONDecodeError:
                continue
            payload = apply_field_patches(payload, patched, journal.get(attempt["targets_ref"]), contract, context, journal)
        issues = inspect_response(contract, payload, context)
        journal.stage(payload, issues)
        syntax_used = any(a["kind"] == "syntax" and a["epoch"] == journal.epoch for a in record["attempts"])
        if issues and not syntax_used:
            root_schema = expanded_schema(contract.model)
            targets = []
            for issue in issues:
                path = issue["path"]
                if any(t["path"] == path for t in targets):
                    continue
                remove = issue["code"] in {"extra_forbidden", "unexpected_artifact"}
                target_schema = {"type":"boolean", "const":True} if remove else path_schema(root_schema, path)
                target_id = fingerprint([work_id, path])
                targets.append({"target_id":target_id, "path":path, "expected_hash":fingerprint(read_path(payload,path)),
                                "original":read_path(payload,path), "schema":target_schema, "remove":remove,
                                "errors":[i for i in issues if i["path"] == path]})
            targets.sort(key=lambda t: (0, -t["path"][-1]) if t["remove"] and t["path"] and isinstance(t["path"][-1],int) else (1,0))
            def repair_package(group):
                # All supplied evidence is retained. The budget check happens before
                # each invocation; oversized groups split without truncation.
                return "Repair only the listed structural fields; preserve factual content and all other fields. Return patches with target_id, expected_hash and replacement.\n" + serialized(group) + "\nAUTHORIZED ORIGINAL REQUEST:\n" + serialized([m.content for m in original_messages])
            def repair_schema(group):
                return compile_provider_schema({"type":"object", "required":["patches"], "properties":{"patches":{"type":"array", "items":{
                    "type":"object", "required":["target_id","expected_hash","replacement"], "properties":{
                        "target_id":{"type":"string", "enum":[t["target_id"] for t in group]},
                        "expected_hash":{"type":"string"}, "replacement":group[0]["schema"]}}}}})
            def repair_budget(group):
                from app.shared.llm_observability import serialize_messages
                return {"messages":serialize_messages([HumanMessage(content=repair_package(group))]), "response_schema":repair_schema(group)}
            groups = {}
            for target in targets:
                groups.setdefault(fingerprint([target["schema"], target["remove"]]), []).append(target)
            for compatible in groups.values():
                for group in bounded_groups(compatible, repair_budget, maximum_items=4, maximum_characters=160_000):
                    ids = [t["target_id"] for t in group]
                    if any(a["epoch"] == journal.epoch and set(a["target_ids"]) & set(ids) for a in record["attempts"]):
                        # A reserved group is replayed below when its reply was saved.
                        previous = next(a for a in record["attempts"] if a["epoch"] == journal.epoch and set(a["target_ids"]) & set(ids))
                        if previous["target_ids"] != ids or not previous.get("response_ref"):
                            continue
                    patch_schema = repair_schema(group)
                    try:
                        patched, _ = call("fields", [HumanMessage(content=repair_package(group))], patch_schema,
                            lambda value:validate_repair_response(value,payload,group,contract,context), ids, group)
                    except json.JSONDecodeError:
                        continue
                    payload = apply_field_patches(payload, patched, group, contract, context, journal)
            issues = inspect_response(contract, payload, context)
            journal.stage(payload, issues)
        return finish(payload, issues)
    except Exception:
        if record["status"] not in {"failed", "degraded"}:
            journal.save(status="failed", epoch=journal.epoch)
        raise
