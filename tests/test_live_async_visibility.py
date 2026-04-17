import sys
import types

sys.modules.setdefault("app.shared.llm", types.SimpleNamespace(get_model=lambda: None))

from app.shared import async_runs
from app.streamlit_backend import dispatcher
from streamlit_shared import ordered_live_output_sections


class ImmediateThread:
    def __init__(self, target=None, name=None, daemon=None, *args, **kwargs):
        self._target = target

    def start(self):
        if self._target is not None:
            self._target()


class FakeDatabridgesClient:
    def __init__(self, rows=None, error=None):
        self._rows = list(rows or [])
        self._error = error

    def list_mfi_processed_data(self, survey_id, page_size=1000):
        if self._error:
            raise self._error
        return list(self._rows)


def _reset_run_store(monkeypatch):
    monkeypatch.setattr(async_runs, "_BACKEND", "memory")
    async_runs._RUNS.clear()
    async_runs._RUN_ARTIFACTS.clear()


def test_market_monitor_async_status_exposes_live_outputs_and_artifacts(monkeypatch):
    _reset_run_store(monkeypatch)
    monkeypatch.setattr(dispatcher.threading, "Thread", ImmediateThread)

    def fake_run_report_generation(*, country, time_period, on_step=None, **kwargs):
        if on_step is not None:
            on_step(
                "data_agent",
                {
                    "databridges_rows": [
                        {
                            "Country": country,
                            "Commodity": "Maize",
                            "Price Date": "2025-01-01T00:00:00.000Z",
                            "Price": 10.5,
                        }
                    ]
                },
            )
            on_step(
                "news_retrieval",
                {
                    "news_counts": {"Seerist": 1, "ReliefWeb": 1, "total": 2},
                    "retriever_traces": [
                        {"retriever": "ReliefWeb", "error": None},
                        {"retriever": "Seerist", "error": None},
                    ],
                    "seerist_documents": [
                        {
                            "doc_id": "seer-1",
                            "title": "Seerist title",
                            "url": "",
                            "source": "Seerist",
                            "date": "2025-01-10",
                            "content": "Seerist content",
                        }
                    ],
                    "reliefweb_documents": [
                        {
                            "doc_id": "rw-1",
                            "title": "ReliefWeb title",
                            "url": "https://reliefweb.test/report",
                            "source": "ReliefWeb",
                            "date": "2025-01-11",
                            "content": "ReliefWeb content",
                        }
                    ],
                },
            )
        return {
            "run_id": "final-market-run",
            "country": country,
            "time_period": time_period,
            "report_draft_sections": {},
            "visualizations": {},
            "data_statistics": {},
            "document_references": [],
            "news_counts": {"Seerist": 1, "ReliefWeb": 1, "total": 2},
            "warnings": [],
        }

    monkeypatch.setattr(dispatcher, "run_report_generation", fake_run_report_generation)

    response = dispatcher._market_monitor_generate_async(
        json_body={
            "country": "South Sudan",
            "time_period": "2025-01",
            "commodity_list": ["Maize"],
            "admin1_list": [],
            "currency_code": "SSP",
            "enabled_modules": [],
            "use_mock_data": False,
        }
    )
    run_id = response.json()["run_id"]

    run = async_runs.get_run(run_id)
    assert run is not None
    assert run.status == "completed"
    live_outputs = run.metadata["live_outputs"]
    assert live_outputs["databridges"]["rows_preview"][0]["Commodity"] == "Maize"
    assert live_outputs["seerist"]["documents"][0]["title"] == "Seerist title"
    assert live_outputs["reliefweb"]["documents"][0]["url"] == "https://reliefweb.test/report"

    artifact_path = live_outputs["databridges"]["download_artifacts"][0]["download_path"]
    artifact_response = dispatcher.dispatch_request("GET", artifact_path)
    assert artifact_response.status_code == 200
    assert artifact_response.headers["Content-Type"] == "application/json"


def test_mfi_async_survey_status_shows_databridges_and_context_live_outputs(monkeypatch):
    _reset_run_store(monkeypatch)
    monkeypatch.setattr(dispatcher.threading, "Thread", ImmediateThread)
    monkeypatch.setattr(
        dispatcher,
        "find_survey",
        lambda survey_id: {
            "survey_id": survey_id,
            "survey_start_date": "2025-01-01",
            "survey_end_date": "2025-01-31",
            "country_name": "South Sudan",
        },
    )
    raw_rows = [
        {"marketName": "Juba", "dimensionName": "MFI", "outputValue": 6.5},
        {"marketName": "Juba", "dimensionName": "Availability", "outputValue": 7.0},
    ]
    monkeypatch.setattr(dispatcher, "get_databridges_client", lambda: FakeDatabridgesClient(rows=raw_rows))
    monkeypatch.setattr(
        dispatcher,
        "load_mfi_from_databridges_rows",
        lambda rows, survey=None: {
            "country": "South Sudan",
            "data_collection_start": "2025-01-01",
            "data_collection_end": "2025-01-31",
            "markets": ["Juba"],
            "survey_metadata": {"collection_period": "2025-01-01 to 2025-01-31"},
        },
    )

    def fake_run_mfi_report_generation(*, country, data_collection_start, data_collection_end, markets, on_step=None, **kwargs):
        if on_step is not None:
            on_step(
                "context_retrieval",
                {
                    "context_counts": {"Seerist": 1, "ReliefWeb": 1, "total": 2},
                    "retriever_traces": [
                        {"retriever": "ReliefWeb", "error": None},
                        {"retriever": "Seerist", "error": None},
                    ],
                    "seerist_documents": [
                        {
                            "doc_id": "seer-1",
                            "title": "Seerist context",
                            "url": "",
                            "source": "Seerist",
                            "date": "2025-01-15",
                            "content": "Seerist contextual content",
                        }
                    ],
                    "reliefweb_documents": [
                        {
                            "doc_id": "rw-1",
                            "title": "ReliefWeb context",
                            "url": "https://reliefweb.test/context",
                            "source": "ReliefWeb",
                            "date": "2025-01-16",
                            "content": "ReliefWeb contextual content",
                        }
                    ],
                },
            )
        return {
            "markets_data": [{"overall_mfi": 6.5, "risk_level": "Medium Risk"}],
            "dimension_scores": [],
            "survey_metadata": {"collection_period": "2025-01-01 to 2025-01-31"},
            "dimension_findings": {},
            "market_recommendations": {},
            "country_context": "",
            "document_references": [],
            "visualizations": {},
            "warnings": [],
        }

    monkeypatch.setattr(dispatcher, "run_mfi_report_generation", fake_run_mfi_report_generation)

    response = dispatcher._mfi_drafter_generate_from_survey_async(json_body={"survey_id": 123})
    run_id = response.json()["run_id"]

    run = async_runs.get_run(run_id)
    assert run is not None
    assert run.status == "completed"
    live_outputs = run.metadata["live_outputs"]
    assert live_outputs["databridges"]["count"] == 2
    assert live_outputs["seerist"]["documents"][0]["title"] == "Seerist context"
    assert live_outputs["reliefweb"]["documents"][0]["title"] == "ReliefWeb context"

    artifact_path = live_outputs["seerist"]["documents"][0]["download_json_path"]
    artifact_response = dispatcher.dispatch_request("GET", artifact_path)
    assert artifact_response.status_code == 200
    assert artifact_response.headers["Content-Type"] == "application/json"


def test_mfi_async_survey_fetch_failures_mark_run_failed(monkeypatch):
    _reset_run_store(monkeypatch)
    monkeypatch.setattr(dispatcher.threading, "Thread", ImmediateThread)
    monkeypatch.setattr(dispatcher, "find_survey", lambda survey_id: {"survey_id": survey_id})
    monkeypatch.setattr(
        dispatcher,
        "get_databridges_client",
        lambda: FakeDatabridgesClient(error=RuntimeError("Databridges unavailable")),
    )

    response = dispatcher._mfi_drafter_generate_from_survey_async(json_body={"survey_id": 999})
    run_id = response.json()["run_id"]

    run = async_runs.get_run(run_id)
    assert run is not None
    assert run.status == "failed"
    assert run.current_node == "databridges_fetch"
    assert "Databridges unavailable" in str(run.error)
    assert run.metadata["live_outputs"]["databridges"]["status"] == "failed"


def test_mfi_dispatcher_routes_csv_endpoints(monkeypatch):
    calls = []

    monkeypatch.setattr(
        dispatcher,
        "_mfi_drafter_generate_from_csv",
        lambda **kwargs: calls.append(("generate", kwargs)) or dispatcher._json_response({"route": "generate"}),
    )
    monkeypatch.setattr(
        dispatcher,
        "_mfi_drafter_validate_csv",
        lambda **kwargs: calls.append(("validate", kwargs)) or dispatcher._json_response({"route": "validate"}),
    )
    monkeypatch.setattr(
        dispatcher,
        "_mfi_drafter_generate_from_csv_async",
        lambda **kwargs: calls.append(("generate_async", kwargs)) or dispatcher._json_response({"route": "generate_async"}),
    )

    files = {"file": object()}
    data = {"country_override": "Sudan"}

    generate_response = dispatcher.dispatch_request("POST", "/mfi-drafter/generate-from-csv", data=data, files=files)
    validate_response = dispatcher.dispatch_request("POST", "/mfi-drafter/validate-csv", files=files)
    async_response = dispatcher.dispatch_request("POST", "/mfi-drafter/generate-from-csv-async", data=data, files=files)

    assert generate_response.json()["route"] == "generate"
    assert validate_response.json()["route"] == "validate"
    assert async_response.json()["route"] == "generate_async"
    assert [name for name, _kwargs in calls] == ["generate", "validate", "generate_async"]


def test_mfi_dispatcher_info_advertises_csv_upload_support():
    info = dispatcher._mfi_drafter_info()

    assert info["supports_csv_upload"] is True
    assert info["data_source"] == "Databridges or uploaded processed CSV"
    assert info["csv_upload"]["endpoint"] == "/generate-from-csv"
    assert info["databridges"]["endpoint"] == "/generate-from-survey"


def test_ordered_live_output_sections_prioritizes_standard_order():
    ordered = ordered_live_output_sections(
        {
            "reliefweb": {"kind": "documents"},
            "extra": {"kind": "unknown"},
            "seerist": {"kind": "documents"},
            "databridges": {"kind": "table"},
        }
    )

    assert [name for name, _payload in ordered] == ["databridges", "seerist", "reliefweb", "extra"]
