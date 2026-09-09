"""Country-independent response-contract and bounded-recovery regressions."""
import copy
import json
from types import SimpleNamespace

import pytest

from app.services.mfi_drafter import graph
from app.services.mfi_drafter.narrative import parse_dimension_narrative


def claim(**changes):
    return dict(text="Observed evidence.", metric_ids=[], document_ids=[],
                scope="assessment", polarity="neutral", **changes)


def dimension():
    return dict(summary=claim(), key_findings=[], subdimension_analysis=[],
                geographic_patterns=[], data_limitations=[], recommendations=[])


@pytest.mark.parametrize("field", ["key_findings", "geographic_patterns", "data_limitations", "recommendations"])
def test_strict_parser_never_discards_invalid_enum(field):
    payload = dimension()
    bad = claim()
    bad["polarity"] = "positive"
    payload[field] = [bad]
    with pytest.raises(ValueError):
        parse_dimension_narrative(payload, dimension_profile={"dimension":"Availability", "is_priority":True},
                                  assessment_profile={"workflow_revision":"mfi-reliable-v1"}, strict=True)


def test_shared_contracts_convert_through_installed_vertex_sdk():
    from app.services.mfi_drafter.response_contracts import CONTRACTS, provider_schema, examples
    from langchain_google_vertexai.chat_models import _convert_schema_dict_to_gapic
    for kind, contract in CONTRACTS.items():
        schema = provider_schema(contract.model)
        assert _convert_schema_dict_to_gapic(schema)
        contract.model.model_validate(examples(kind), strict=True)
    schema_text = json.dumps(provider_schema(CONTRACTS["dimension"].model))
    assert all(value in schema_text for value in ("favorable", "unfavorable", "neutral", "descriptive", "surveyed_traders"))


def test_contract_validation_reports_exact_field_without_reinterpreting_polarity():
    from app.services.mfi_drafter.response_contracts import inspect_response, CONTRACTS
    payload = dimension()
    payload["key_findings"] = [{**claim(), "polarity":"positive"}]
    issues = inspect_response(CONTRACTS["dimension"], payload)
    assert issues[0]["path"] == ["key_findings", 0, "polarity"]
    assert payload["key_findings"][0]["polarity"] == "positive"


class ResponseModel:
    def __init__(self, replies):
        self.replies = list(replies)
        self.messages, self.schemas = [], []
    def bind(self, **kwargs):
        self.schemas.append(kwargs)
        return self
    def invoke(self, messages):
        self.messages.append(messages)
        reply = self.replies.pop(0)
        value = reply(messages) if callable(reply) else reply
        return SimpleNamespace(content=value if isinstance(value,str) else json.dumps(value),
                               response_metadata={}, usage_metadata={})


def repair_fields(messages, replacement="favorable", **changes):
    targets = json.loads(str(messages[0].content).split("\n",1)[1].split("\nAUTHORIZED ORIGINAL REQUEST:")[0])
    return {"patches":[{"target_id":t["target_id"], "expected_hash":t["expected_hash"],
                        "replacement":replacement, **changes} for t in targets]}


def invoke(model, kind="dimension", context=None, trace=None):
    from app.shared.llm_observability import LLMTraceSession
    from langchain_core.messages import HumanMessage
    return graph._invoke_json_with_one_normalization(trace=trace or LLMTraceSession(service="mfi-drafter",run_id="run"),
        model=model, messages=[HumanMessage(content="Authorized evidence for an arbitrary country.")],
        node="context_extractor" if kind == "context" else "dimension_drafter", operation=f"test.{kind}",
        artifact_type=kind, artifact_id="Availability", correction_attempt=0, validator=lambda x:x,
        response_contract=kind, contract_context=context)


def test_invalid_field_repaired_without_rewriting_valid_prose():
    original = dimension()
    original["key_findings"] = [claim(), {**claim(),"polarity":"positive"}]
    model = ResponseModel([original, repair_fields])
    result, count = invoke(model)
    assert count == 2 and len(model.messages) == 2
    expected = copy.deepcopy(original)
    expected["key_findings"][1]["polarity"] = "favorable"
    assert result.payload == expected
    assert all(schema["response_mime_type"] == "application/json" for schema in model.schemas)
    assert "positive" in model.messages[1][0].content


@pytest.mark.parametrize("replacement", ["positive", None, 5, []])
def test_invalid_repair_stops_after_one_attempt(replacement):
    from app.shared.llm_observability import LLMCallError
    original = dimension()
    original["summary"]["polarity"] = "positive"
    model = ResponseModel([original, lambda m:repair_fields(m,replacement)])
    with pytest.raises(LLMCallError):
        invoke(model)
    assert len(model.messages) == 2


def test_syntax_repair_cannot_start_a_second_schema_repair():
    from app.shared.llm_observability import LLMCallError
    bad = dimension()
    bad["summary"]["scope"] = "national"
    model = ResponseModel(['{"summary":', bad])
    with pytest.raises(LLMCallError):
        invoke(model)
    assert len(model.messages) == 2


def test_partial_context_keeps_valid_statements_and_retries_only_invalid_fields_on_resume():
    from app.services.mfi_drafter.execution import MemoryRecoveryStore, Execution, create_checkpoint, reserve_execution
    from app.services.mfi_drafter.response_runtime import public_journal
    store = MemoryRecoveryStore()
    create_checkpoint(store,"run",{}, {})
    first = Execution(store,"run",reserve_execution(store,"run"))
    statement = {"text":"Exact passage.","classification":"potentially_explanatory", "document_ids":["doc"],
                 "source_passages":[{"document_id":"doc","text":"Exact passage."}]}
    bad = {**statement,"document_ids":"doc"}
    model = ResponseModel([{"statements":[statement,bad]}, lambda m:repair_fields(m,"doc"), lambda m:repair_fields(m,["doc"])])
    with first.activate():
        result, count = invoke(model,"context",{"documents":{"doc":"Exact passage."}})
        assert count == 2 and result.payload["statements"] == [statement]
        assert result.value["_source_indices"] == [0]
        assert public_journal(store.read("run"))["degraded_work_count"] == 1
        first.finish(ValueError("later failure"))
    second = Execution(store,"run",reserve_execution(store,"run",idempotency_key="resume"))
    with second.activate():
        result, count = invoke(model,"context",{"documents":{"doc":"Exact passage."}})
        assert count == 1 and result.payload["statements"] == [statement,statement]
        assert not result.value["_structural_issues"]
    assert len(model.messages) == 3


def test_raw_response_and_valid_fragments_survive_required_failure():
    from app.services.mfi_drafter.execution import MemoryRecoveryStore, Execution, create_checkpoint, reserve_execution
    from app.shared.llm_observability import LLMCallError
    store = MemoryRecoveryStore()
    create_checkpoint(store,"run",{}, {})
    worker = Execution(store,"run",reserve_execution(store,"run"))
    bad = dimension()
    bad["summary"]["polarity"] = "positive"
    bad["key_findings"] = [claim()]
    model = ResponseModel([bad,lambda m:repair_fields(m,"positive")])
    with worker.activate(), pytest.raises(LLMCallError):
        invoke(model)
    row = next(iter(store.read("run")["response_work"].values()))
    raw = store.get(row["attempts"][0]["response_ref"])
    assert json.loads(raw["raw_text"]) == bad
    assert store.get(row["candidate_ref"]) == bad
    assert store.get(row["fragments_ref"])[0]["path"] == ["key_findings",0]
    assert row["issues"][0]["path"] == ["summary","polarity"]


@pytest.mark.parametrize("field,value", [("document_ids",None),("document_ids","doc"),("metric_ids",None),("scope","national"),("polarity","positive")])
@pytest.mark.parametrize("kind", ["dimension","market","executive"])
def test_all_claim_locations_share_strict_types(kind,field,value):
    from app.services.mfi_drafter.response_contracts import CONTRACTS, inspect_response
    malformed = {**claim(),field:value}
    payload = dimension() if kind == "dimension" else {"markets":[{"market_name":"港湾","narrative":{"priority_issues":[malformed],"recommended_interventions":[],"limitations":[]}}]} if kind == "market" else {"motivation":malformed,"key_findings":[],"recommendations":[],"limitations":[]}
    if kind == "dimension":
        payload["subdimension_analysis"] = [{"name":"Component","interpretation":malformed,"driver_metric_ids":[]}]
    issues = inspect_response(CONTRACTS[kind],payload)
    assert issues and issues[0]["path"][-1] == field


def test_interruption_after_repair_capture_reuses_saved_reply(monkeypatch):
    from app.services.mfi_drafter import response_runtime as runtime
    from app.services.mfi_drafter.execution import MemoryRecoveryStore, Execution, create_checkpoint, reserve_execution
    store = MemoryRecoveryStore()
    create_checkpoint(store,"run",{}, {})
    first = Execution(store,"run",reserve_execution(store,"run"))
    bad = dimension()
    bad["summary"]["polarity"] = "positive"
    model = ResponseModel([bad,repair_fields])
    real_apply = runtime.apply_field_patches
    monkeypatch.setattr(runtime,"apply_field_patches",lambda *a: (_ for _ in ()).throw(RuntimeError("worker stopped after response")))
    with first.activate(), pytest.raises(RuntimeError,match="worker stopped"):
        invoke(model)
    first.finish(ValueError("interrupted"))
    monkeypatch.setattr(runtime,"apply_field_patches",real_apply)
    second = Execution(store,"run",reserve_execution(store,"run",idempotency_key="resume"))
    with second.activate():
        result,calls = invoke(model)
    assert result.payload["summary"]["polarity"] == "favorable"
    assert calls == 0 and len(model.messages) == 2


def test_storage_failure_does_not_trigger_format_repair(monkeypatch):
    from app.services.mfi_drafter.execution import MemoryRecoveryStore, Execution, RecoveryError, create_checkpoint, reserve_execution
    from app.shared.llm_observability import LLMTraceSession
    store = MemoryRecoveryStore()
    create_checkpoint(store,"run",{}, {})
    worker = Execution(store,"run",reserve_execution(store,"run"))
    original_put = store.put
    def fail_raw(run,value):
        if isinstance(value,dict) and "raw_text" in value:
            raise RecoveryError("Response storage unavailable")
        return original_put(run,value)
    monkeypatch.setattr(store,"put",fail_raw)
    model = ResponseModel([dimension()])
    trace = LLMTraceSession(service="mfi-drafter",run_id="run")
    with worker.activate(), pytest.raises(RecoveryError,match="storage"):
        invoke(model,trace=trace)
    assert len(model.messages) == 1
    assert trace.snapshot()["calls"][0]["failure_stage"] == "response_persistence"
    assert trace.snapshot()["response_persistence_failed_calls"] == 1
    assert trace.snapshot()["contract_failed_calls"] == 0


def test_repair_cannot_change_an_unauthorized_path_or_stale_revision():
    from app.shared.llm_observability import LLMCallError
    bad = dimension()
    bad["summary"]["polarity"] = "positive"
    model = ResponseModel([bad,lambda m:repair_fields(m,"favorable",expected_hash="stale")])
    with pytest.raises(LLMCallError):
        invoke(model)
    assert len(model.messages) == 2


def test_schema_compilation_never_returns_shared_mutable_dictionary():
    from app.services.mfi_drafter.response_contracts import CONTRACTS, provider_schema
    first = provider_schema(CONTRACTS["context"].model)
    first["properties"]["statements"].clear()
    assert provider_schema(CONTRACTS["context"].model)["properties"]["statements"]["type"] == "array"


def test_partial_context_disclosure_contract_and_renderer_agree():
    from app.services.mfi_drafter.context_status import resolve_context_status, reconcile_context_status
    from app.shared.report_blocks import _mfi_context_limitation_text
    docs = [{"doc_id":"d","source":"ReliefWeb"}]
    statements = [{"statement_id":"context.statement.1","text":"Context.","document_ids":["d"],"classification":"corroborating"}]
    status = resolve_context_status(retriever_statuses={"ReliefWeb":"completed"},documents=docs,statements=statements,
                                    extraction_mode="llm",unresolved_statement_count=1)
    assert status.status == "available" and status.classification_outcome == "degraded"
    assert status.limitation_code == "context_partial_classification_unavailable"
    assert "Only accepted" in _mfi_context_limitation_text(status.limitation_code)
    assert reconcile_context_status(status,documents=docs,statements=statements).unresolved_statement_count == 1


def test_required_structural_failure_blocks_delivery_even_with_numeric_exception():
    from app.services.mfi_drafter.errors import MFIGenerationBlockedError
    with pytest.raises(MFIGenerationBlockedError) as failure:
        graph.node_finalize_delivery({"response_validation":{"structural_validation_issues":[{"required":True}]},
            "qa_review":{"status":"delivered_with_unverified_figures"}})
    assert failure.value.code == "mfi_structural_validation_failed"


def test_unknown_provider_schema_construct_is_configuration_error():
    from app.services.mfi_drafter.response_contracts import compile_provider_schema, ContractConfigurationError
    with pytest.raises(ContractConfigurationError):
        compile_provider_schema({"oneOf":[{"type":"string"},{"type":"number"}]})


def test_valid_json_syntax_repair_preserves_content():
    original = json.dumps(dimension())
    model = ResponseModel([original[:-1] + ",}",original])
    result,count = invoke(model)
    assert result.payload == dimension() and count == 2


def test_extra_application_metadata_is_rejected_not_ignored():
    from app.services.mfi_drafter.response_contracts import inspect_response, CONTRACTS
    payload = dimension()
    payload["summary"]["claim_id"] = "model-owned"
    assert inspect_response(CONTRACTS["dimension"],payload)[0]["path"] == ["summary","claim_id"]


def test_empty_successful_context_is_reusable_without_new_calls():
    from app.services.mfi_drafter.execution import MemoryRecoveryStore, Execution, create_checkpoint, reserve_execution
    store = MemoryRecoveryStore()
    create_checkpoint(store,"run",{}, {})
    model = ResponseModel([{"statements":[]}])
    first = Execution(store,"run",reserve_execution(store,"run"))
    with first.activate():
        result,_ = invoke(model,"context",{"documents":{"doc":"No relevant passage."}})
        first.finish(ValueError("later failure"))
    second = Execution(store,"run",reserve_execution(store,"run",idempotency_key="resume"))
    with second.activate():
        repeated,calls = invoke(model,"context",{"documents":{"doc":"No relevant passage."}})
    assert repeated.payload == result.payload and calls == 0


def test_missing_market_row_preserves_other_market_narratives():
    row = {"market_name":"Port α","narrative":{"priority_issues":[{**claim(),"scope":"market"}],"recommended_interventions":[],"limitations":[]}}
    missing = {**copy.deepcopy(row),"market_name":"Port β"}
    model = ResponseModel([{"markets":[row]},lambda m:repair_fields(m,missing)])
    result,count = invoke(model,"market",{"market_names":["Port α","Port β"]})
    assert count == 2 and result.payload["markets"] == [row,missing]


def test_multiple_extra_market_rows_are_removed_without_index_drift():
    row = {"market_name":"Port","narrative":{"priority_issues":[],"recommended_interventions":[],"limitations":[]}}
    model = ResponseModel([{"markets":[row,{**row,"market_name":"extra1"},{**row,"market_name":"extra2"}]},lambda m:repair_fields(m,True)])
    result,count = invoke(model,"market",{"market_names":["Port"]})
    assert result.payload == {"markets":[row]} and count == 2
