import sys
import types
from contextlib import nullcontext

sys.modules.setdefault("app.shared.llm", types.SimpleNamespace(get_model=lambda: None))

from app.shared import async_runs
from app.streamlit_backend import dispatcher
import streamlit_shared
from streamlit_shared import ordered_live_output_sections


class ImmediateThread:
    def __init__(self, target=None, name=None, daemon=None, *args, **kwargs):
        self._target = target

    def start(self):
        if self._target is not None:
            self._target()


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


def test_mfi_dispatcher_removed_survey_endpoints_return_404():
    survey_response = dispatcher.dispatch_request("POST", "/mfi-drafter/generate-from-survey", json_body={"survey_id": 123})
    surveys_response = dispatcher.dispatch_request("GET", "/mfi-drafter/countries/South%20Sudan/surveys")

    assert survey_response.status_code == 404
    assert surveys_response.status_code == 404
    assert "Unknown MFI drafter endpoint" in survey_response.json()["detail"]
    assert "Unknown MFI drafter endpoint" in surveys_response.json()["detail"]


def test_mfi_dispatcher_info_advertises_csv_upload_support():
    info = dispatcher._mfi_drafter_info()

    assert info["supports_csv_upload"] is True
    assert info["data_source"] == "Uploaded processed MFI CSV"
    assert info["csv_upload"]["endpoint"] == "/generate-from-csv"
    assert "inputs" not in info
    assert "databridges" not in info


def test_run_async_and_poll_enables_downloads_only_for_final_status(monkeypatch):
    events = []

    class DummyPlaceholder:
        def container(self):
            return nullcontext()

    responses = iter(
        [
            {"run_id": "mfi_test"},
            {"run_id": "mfi_test", "status": "running", "progress_pct": 25},
            {"run_id": "mfi_test", "status": "completed", "progress_pct": 100},
            {"success": True},
        ]
    )

    monkeypatch.setattr(streamlit_shared.st, "empty", lambda: DummyPlaceholder())
    monkeypatch.setattr(streamlit_shared, "request_json", lambda *args, **kwargs: next(responses))
    monkeypatch.setattr(streamlit_shared.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(
        streamlit_shared,
        "render_run_status",
        lambda status, **kwargs: events.append((status.get("status"), kwargs.get("render_instance_id"), kwargs.get("enable_downloads"))),
    )

    run_id, final_status, result = streamlit_shared.run_async_and_poll(
        start_method="POST",
        start_path="/start",
        status_path_template="/status/{run_id}",
        result_path_template="/result/{run_id}",
        poll_interval_seconds=0.0,
        timeout_seconds=1,
    )

    assert run_id == "mfi_test"
    assert final_status["status"] == "completed"
    assert result == {"success": True}
    assert events == [
        ("running", "poll-0", False),
        ("completed", None, True),
    ]


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
