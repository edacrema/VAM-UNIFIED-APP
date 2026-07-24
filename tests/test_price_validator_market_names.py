from __future__ import annotations

import inspect

import pytest

import app.services.price_validator.graph as graph


class _StubClient:
    def __init__(self, markets=None, error=None):
        self.markets = markets or []
        self.error = error
        self.calls = []

    def list_markets(self, iso3):
        self.calls.append(iso3)
        if self.error:
            raise self.error
        return self.markets


@pytest.fixture(autouse=True)
def _isolate(monkeypatch):
    monkeypatch.setattr(graph, "_MARKET_NAMES_CACHE", {})
    monkeypatch.delenv("MARKET_NAMES_GCS_URI", raising=False)


def test_market_names_fetched_from_databridges_for_country(monkeypatch):
    stub = _StubClient(
        markets=[
            {"marketName": "Addis Ababa", "marketLocalName": None},
            {"marketName": "Abi Adi", "marketLocalName": "Abiy Addi"},
        ]
    )
    monkeypatch.setattr(graph, "get_databridges_client", lambda: stub)

    names = graph._load_market_names("Ethiopia")

    assert stub.calls == ["ETH"]
    assert names == {"Addis Ababa", "Abi Adi", "Abiy Addi"}


def test_market_names_accepts_iso3_code_directly(monkeypatch):
    stub = _StubClient(markets=[{"marketName": "Afgooye"}])
    monkeypatch.setattr(graph, "get_databridges_client", lambda: stub)

    assert graph._load_market_names("SOM") == {"Afgooye"}
    assert stub.calls == ["SOM"]


def test_market_names_cached_per_country(monkeypatch):
    stub = _StubClient(markets=[{"marketName": "Afgooye"}])
    monkeypatch.setattr(graph, "get_databridges_client", lambda: stub)

    graph._load_market_names("Somalia")
    graph._load_market_names("Somalia")

    assert stub.calls == ["SOM"]


def test_market_names_fall_back_to_gcs_when_databridges_fails(monkeypatch):
    stub = _StubClient(error=RuntimeError("api down"))
    monkeypatch.setattr(graph, "get_databridges_client", lambda: stub)
    monkeypatch.setattr(graph, "_load_market_names_from_gcs", lambda: {"Fallback Market"})

    assert graph._load_market_names("Ethiopia") == {"Fallback Market"}


def test_market_names_error_mentions_both_failed_sources(monkeypatch):
    stub = _StubClient(error=RuntimeError("api down"))
    monkeypatch.setattr(graph, "get_databridges_client", lambda: stub)

    with pytest.raises(RuntimeError, match="api down"):
        graph._load_market_names("Ethiopia")


def test_empty_market_list_is_an_error(monkeypatch):
    stub = _StubClient(markets=[])
    monkeypatch.setattr(graph, "get_databridges_client", lambda: stub)

    with pytest.raises(RuntimeError, match="no markets"):
        graph._load_market_names("Ethiopia")


def test_country_flows_through_public_entrypoints():
    assert "country" in inspect.signature(graph.run_troubleshooting).parameters
    assert "country" in inspect.signature(graph.create_initial_state).parameters
    state = graph.create_initial_state("dummy.xlsx", None, country="  Ethiopia  ")
    assert state["country"] == "Ethiopia"
