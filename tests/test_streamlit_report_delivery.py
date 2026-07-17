from __future__ import annotations

from contextlib import nullcontext

import streamlit_shared as shared


class FakeStreamlit:
    def __init__(self, *, button_values=None):
        self.session_state = {}
        self.button_values = dict(button_values or {})
        self.events = []

    def spinner(self, label):
        self.events.append(("spinner", label))
        return nullcontext()

    def container(self, **kwargs):
        self.events.append(("container", kwargs.get("key")))
        return nullcontext()

    def expander(self, label, **_kwargs):
        self.events.append(("expander", label))
        return nullcontext()

    def subheader(self, label):
        self.events.append(("subheader", label))

    def caption(self, label):
        self.events.append(("caption", label))

    def error(self, label):
        self.events.append(("error", label))

    def download_button(self, label, **kwargs):
        self.events.append(("download", label, kwargs))
        return False

    def button(self, label, *, key, **kwargs):
        self.events.append(("button", label, key, kwargs))
        return bool(self.button_values.get(key, False))

    def rerun(self):
        self.events.append(("rerun",))


def test_delivery_download_precedes_preview_and_export_is_cached(monkeypatch):
    fake_st = FakeStreamlit(button_values={"mm_toggle_preview_run-1": True})
    export_calls = []
    preview_calls = []
    technical_calls = []
    monkeypatch.setattr(shared, "st", fake_st)
    monkeypatch.setattr(
        shared,
        "request_bytes",
        lambda *args, **kwargs: export_calls.append((args, kwargs)) or b"docx",
    )

    shared.render_report_delivery(
        run_id="run-1",
        key_prefix="mm",
        export_path="/export/run-1",
        file_name="report.docx",
        render_preview=lambda: preview_calls.append("run-1"),
        render_technical_details=lambda: technical_calls.append("run-1"),
    )

    event_names = [event[0] for event in fake_st.events]
    assert event_names.index("download") < event_names.index("button")
    download = next(event for event in fake_st.events if event[0] == "download")
    assert download[2]["type"] == "primary"
    assert download[2]["width"] == "stretch"
    assert download[2]["on_click"] == "ignore"
    assert preview_calls == ["run-1"]
    assert technical_calls == ["run-1"]
    assert len(export_calls) == 1

    fake_st.button_values.clear()
    shared.render_report_delivery(
        run_id="run-1",
        key_prefix="mm",
        export_path="/export/run-1",
        file_name="report.docx",
        render_preview=lambda: preview_calls.append("run-1"),
    )
    assert len(export_calls) == 1
    assert preview_calls == ["run-1", "run-1"]

    shared.render_report_delivery(
        run_id="run-2",
        key_prefix="mm",
        export_path="/export/run-2",
        file_name="report-2.docx",
        render_preview=lambda: preview_calls.append("run-2"),
    )
    assert len(export_calls) == 2
    assert "run-2" not in preview_calls


def test_delivery_keeps_preview_available_after_export_failure(monkeypatch):
    fake_st = FakeStreamlit(button_values={"mfi_toggle_preview_failed-run": True})
    preview_calls = []
    monkeypatch.setattr(shared, "st", fake_st)
    monkeypatch.setattr(
        shared,
        "request_bytes",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("export failed")),
    )

    shared.render_report_delivery(
        run_id="failed-run",
        key_prefix="mfi",
        export_path="/export/failed-run",
        file_name="report.docx",
        render_preview=lambda: preview_calls.append("failed-run"),
    )

    assert any(event[0] == "error" for event in fake_st.events)
    assert any(event[0] == "button" and event[1] == "View report on this page" for event in fake_st.events)
    assert preview_calls == ["failed-run"]
