from __future__ import annotations

import threading
from types import SimpleNamespace

import pytest

import streamlit_shared as shared


def _response(status_code: int = 200, content: bytes = b'{"ok": true}'):
    return SimpleNamespace(
        status_code=status_code,
        content=content,
        text=content.decode("utf-8"),
        json=lambda: {"ok": True},
    )


def test_request_json_returns_payload_with_patched_dispatch(monkeypatch) -> None:
    calls: list[tuple[str, str]] = []

    def fake_dispatch(method, path, **kwargs):
        calls.append((method, path))
        return _response()

    monkeypatch.setattr(shared, "dispatch_request", fake_dispatch)

    assert shared.request_json("GET", "/x") == {"ok": True}
    assert calls == [("GET", "/x")]


def test_request_json_times_out_when_dispatch_blocks(monkeypatch) -> None:
    release = threading.Event()

    def blocking_dispatch(method, path, **kwargs):
        release.wait(5)
        return _response()

    monkeypatch.setattr(shared, "dispatch_request", blocking_dispatch)
    try:
        with pytest.raises(RuntimeError, match="timed out after 0.05s"):
            shared.request_json("GET", "/slow", timeout=0.05)
    finally:
        release.set()


def test_request_json_propagates_dispatch_exceptions(monkeypatch) -> None:
    def failing_dispatch(method, path, **kwargs):
        raise ValueError("boom")

    monkeypatch.setattr(shared, "dispatch_request", failing_dispatch)
    with pytest.raises(ValueError, match="boom"):
        shared.request_json("GET", "/x")


def test_request_json_raises_on_http_error_status(monkeypatch) -> None:
    def error_dispatch(method, path, **kwargs):
        return SimpleNamespace(
            status_code=500,
            content=b"internal error",
            text="internal error",
            json=lambda: (_ for _ in ()).throw(ValueError("not json")),
        )

    monkeypatch.setattr(shared, "dispatch_request", error_dispatch)
    with pytest.raises(RuntimeError, match=r"failed \(500\)"):
        shared.request_json("GET", "/x")


def test_request_bytes_returns_content_and_times_out(monkeypatch) -> None:
    monkeypatch.setattr(
        shared,
        "dispatch_request",
        lambda method, path, **kwargs: _response(content=b"raw-bytes"),
    )
    assert shared.request_bytes("GET", "/x") == b"raw-bytes"

    release = threading.Event()

    def blocking_dispatch(method, path, **kwargs):
        release.wait(5)
        return _response()

    monkeypatch.setattr(shared, "dispatch_request", blocking_dispatch)
    try:
        with pytest.raises(RuntimeError, match="timed out"):
            shared.request_bytes("GET", "/slow", timeout=0.05)
    finally:
        release.set()
