from __future__ import annotations

import logging
import os
import uuid
from pathlib import Path

import pytest
from sqlalchemy import create_engine as create_sqlalchemy_engine
from sqlalchemy import event, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import OperationalError

from app.services.market_monitor.food_basket import SqlCountryFoodBasketRepository
from app.services.price_cache import db_resilience, sql_repository
from app.services.price_cache.config import load_price_cache_config
from app.services.price_cache.db_resilience import retry_disconnected_read
from app.services.price_cache.fixtures import seed_cache_snapshot
from app.services.price_cache.migrations import apply_migrations
from app.services.price_cache.sql_repository import (
    SqlPriceCacheRepository,
    create_price_cache_engine,
)


class _FakeEngine:
    def __init__(self) -> None:
        self.dispose_calls = 0

    def dispose(self) -> None:
        self.dispose_calls += 1


def _disconnect_error(message: str = "server closed the connection") -> OperationalError:
    return OperationalError(
        "SELECT 1",
        {},
        RuntimeError(message),
        connection_invalidated=True,
    )


def _sqlite_repository(tmp_path: Path) -> SqlPriceCacheRepository:
    config = load_price_cache_config(
        {
            "PRICE_CACHE_BACKEND": "sqlite",
            "PRICE_CACHE_SQLITE_PATH": str(tmp_path / "resilience.sqlite3"),
        }
    )
    engine = create_price_cache_engine(config)
    apply_migrations(engine, "sqlite")
    repository = SqlPriceCacheRepository(engine)
    seed_cache_snapshot(repository)
    return repository


def test_pool_configuration_defaults_and_overrides() -> None:
    default_config = load_price_cache_config({"PRICE_CACHE_BACKEND": "sqlite"})
    override_config = load_price_cache_config(
        {
            "PRICE_CACHE_BACKEND": "sqlite",
            "PRICE_CACHE_POOL_RECYCLE_SECONDS": "900",
            "PRICE_CACHE_POOL_TIMEOUT_SECONDS": "12",
            "PRICE_CACHE_CONNECT_TIMEOUT_SECONDS": "7",
            "PRICE_CACHE_TCP_KEEPALIVES_IDLE_SECONDS": "21",
            "PRICE_CACHE_TCP_KEEPALIVES_INTERVAL_SECONDS": "6",
            "PRICE_CACHE_TCP_KEEPALIVES_COUNT": "5",
            "PRICE_CACHE_TCP_USER_TIMEOUT_MS": "15000",
            "PRICE_CACHE_STATEMENT_TIMEOUT_MS": "45000",
        }
    )

    assert default_config.pool_recycle_seconds == 240
    assert default_config.pool_timeout_seconds == 30
    assert default_config.connect_timeout_seconds == 10
    assert default_config.tcp_keepalives_idle_seconds == 30
    assert default_config.tcp_keepalives_interval_seconds == 10
    assert default_config.tcp_keepalives_count == 3
    assert default_config.tcp_user_timeout_ms == 30000
    assert default_config.statement_timeout_ms == 120000
    assert override_config.pool_recycle_seconds == 900
    assert override_config.pool_timeout_seconds == 12
    assert override_config.connect_timeout_seconds == 7
    assert override_config.tcp_keepalives_idle_seconds == 21
    assert override_config.tcp_keepalives_interval_seconds == 6
    assert override_config.tcp_keepalives_count == 5
    assert override_config.tcp_user_timeout_ms == 15000
    assert override_config.statement_timeout_ms == 45000


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("PRICE_CACHE_POOL_RECYCLE_SECONDS", "0"),
        ("PRICE_CACHE_POOL_TIMEOUT_SECONDS", "0"),
        ("PRICE_CACHE_POOL_RECYCLE_SECONDS", "not-an-integer"),
        ("PRICE_CACHE_POOL_TIMEOUT_SECONDS", "not-an-integer"),
        ("PRICE_CACHE_CONNECT_TIMEOUT_SECONDS", "0"),
        ("PRICE_CACHE_CONNECT_TIMEOUT_SECONDS", "not-an-integer"),
        ("PRICE_CACHE_TCP_KEEPALIVES_IDLE_SECONDS", "0"),
        ("PRICE_CACHE_TCP_KEEPALIVES_INTERVAL_SECONDS", "0"),
        ("PRICE_CACHE_TCP_KEEPALIVES_COUNT", "0"),
        ("PRICE_CACHE_TCP_USER_TIMEOUT_MS", "0"),
        ("PRICE_CACHE_STATEMENT_TIMEOUT_MS", "0"),
        ("PRICE_CACHE_STATEMENT_TIMEOUT_MS", "not-an-integer"),
    ],
)
def test_pool_configuration_rejects_invalid_values(name: str, value: str) -> None:
    with pytest.raises(ValueError):
        load_price_cache_config(
            {
                "PRICE_CACHE_BACKEND": "sqlite",
                name: value,
            }
        )


def test_postgres_engine_receives_resilience_pool_arguments(monkeypatch) -> None:
    captured: dict[str, object] = {}
    sentinel = object()

    def fake_create_engine(url: str, **kwargs):
        captured["url"] = url
        captured["kwargs"] = kwargs
        return sentinel

    monkeypatch.setattr(sql_repository, "create_engine", fake_create_engine)
    config = load_price_cache_config(
        {
            "PRICE_CACHE_BACKEND": "postgres",
            "PRICE_CACHE_DATABASE_URL": "postgresql://example.invalid/cache",
            "PRICE_CACHE_POOL_RECYCLE_SECONDS": "901",
            "PRICE_CACHE_POOL_TIMEOUT_SECONDS": "13",
        }
    )

    engine = create_price_cache_engine(config)

    assert engine is sentinel
    assert captured == {
        "url": "postgresql://example.invalid/cache",
        "kwargs": {
            "future": True,
            "pool_pre_ping": True,
            "pool_recycle": 901,
            "pool_timeout": 13,
            "connect_args": {
                "connect_timeout": 10,
                "keepalives": 1,
                "keepalives_idle": 30,
                "keepalives_interval": 10,
                "keepalives_count": 3,
                "tcp_user_timeout": 30000,
                "options": "-c statement_timeout=120000",
            },
        },
    }


def test_worker_engine_skips_statement_timeout(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_create_engine(url: str, **kwargs):
        captured["kwargs"] = kwargs
        return object()

    monkeypatch.setattr(sql_repository, "create_engine", fake_create_engine)
    config = load_price_cache_config(
        {
            "PRICE_CACHE_BACKEND": "postgres",
            "PRICE_CACHE_DATABASE_URL": "postgresql://example.invalid/cache",
        }
    )

    create_price_cache_engine(config, apply_statement_timeout=False)

    connect_args = captured["kwargs"]["connect_args"]
    assert "options" not in connect_args
    assert connect_args["keepalives"] == 1
    assert connect_args["connect_timeout"] == 10
    assert connect_args["tcp_user_timeout"] == 30000


def test_statement_timeout_merges_with_url_options(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_create_engine(url: str, **kwargs):
        captured["kwargs"] = kwargs
        return object()

    monkeypatch.setattr(sql_repository, "create_engine", fake_create_engine)
    config = load_price_cache_config(
        {
            "PRICE_CACHE_BACKEND": "postgres",
            "PRICE_CACHE_DATABASE_URL": (
                "postgresql://example.invalid/cache?options=-csearch_path%3Dfoo"
            ),
        }
    )

    create_price_cache_engine(config)

    connect_args = captured["kwargs"]["connect_args"]
    assert connect_args["options"] == "-csearch_path=foo -c statement_timeout=120000"


def test_disable_statement_timeout_helper_targets_postgres_only() -> None:
    from types import SimpleNamespace

    executed: list[str] = []
    postgres_conn = SimpleNamespace(
        dialect=SimpleNamespace(name="postgresql"),
        execute=lambda clause: executed.append(str(clause)),
    )
    sqlite_conn = SimpleNamespace(
        dialect=SimpleNamespace(name="sqlite"),
        execute=lambda clause: executed.append("unexpected"),
    )

    sql_repository._disable_statement_timeout_for_transaction(postgres_conn)
    sql_repository._disable_statement_timeout_for_transaction(sqlite_conn)

    assert executed == ["SET LOCAL statement_timeout = 0"]


def test_sqlite_copy_country_snapshot_emits_no_set_local(tmp_path: Path) -> None:
    repository = _sqlite_repository(tmp_path)
    statements: list[str] = []

    def record_statement(
        _conn,
        _cursor,
        statement,
        _parameters,
        _context,
        _executemany,
    ) -> None:
        statements.append(" ".join(str(statement).split()))

    event.listen(repository.engine, "before_cursor_execute", record_statement)

    repository.copy_country_snapshot(
        source_cache_version_id=repository.get_active_version_id() or "",
        target_cache_version_id=str(uuid.uuid4()),
        country_iso3="SSD",
    )

    assert statements
    assert not any("SET LOCAL" in statement for statement in statements)


def test_sqlite_engine_arguments_are_unchanged(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}
    sentinel = object()

    def fake_create_engine(url: str, **kwargs):
        captured["url"] = url
        captured["kwargs"] = kwargs
        return sentinel

    monkeypatch.setattr(sql_repository, "create_engine", fake_create_engine)
    config = load_price_cache_config(
        {
            "PRICE_CACHE_BACKEND": "sqlite",
            "PRICE_CACHE_SQLITE_PATH": str(tmp_path / "cache.sqlite3"),
            "PRICE_CACHE_POOL_RECYCLE_SECONDS": "901",
            "PRICE_CACHE_POOL_TIMEOUT_SECONDS": "13",
        }
    )

    engine = create_price_cache_engine(config)

    assert engine is sentinel
    assert captured["kwargs"] == {"future": True}


def test_disconnected_read_disposes_waits_once_and_recovers(monkeypatch, caplog) -> None:
    sleeps: list[float] = []

    class Repository:
        def __init__(self) -> None:
            self.engine = _FakeEngine()
            self.calls = 0

        @retry_disconnected_read
        def read(self) -> str:
            self.calls += 1
            if self.calls == 1:
                raise _disconnect_error()
            return "recovered"

    monkeypatch.setattr(db_resilience.time, "sleep", sleeps.append)
    repository = Repository()

    with caplog.at_level(logging.INFO, logger=db_resilience.__name__):
        result = repository.read()

    assert result == "recovered"
    assert repository.calls == 2
    assert repository.engine.dispose_calls == 1
    assert len(sleeps) == 1
    assert 0.1 <= sleeps[0] <= 0.25
    assert "Repository.read" in caplog.text
    assert "recovered" in caplog.text


def test_persistent_disconnect_stops_after_one_retry(monkeypatch) -> None:
    first_error = _disconnect_error("first")
    final_error = _disconnect_error("second")
    sleeps: list[float] = []

    class Repository:
        def __init__(self) -> None:
            self.engine = _FakeEngine()
            self.calls = 0

        @retry_disconnected_read
        def read(self) -> None:
            self.calls += 1
            raise first_error if self.calls == 1 else final_error

    monkeypatch.setattr(db_resilience.time, "sleep", sleeps.append)
    repository = Repository()

    with pytest.raises(OperationalError) as caught:
        repository.read()

    assert caught.value is final_error
    assert repository.calls == 2
    assert repository.engine.dispose_calls == 1
    assert len(sleeps) == 1


def test_non_disconnect_dbapi_error_is_not_retried(monkeypatch) -> None:
    error = OperationalError(
        "SELECT missing_column",
        {},
        RuntimeError("schema error"),
        connection_invalidated=False,
    )
    sleeps: list[float] = []

    class Repository:
        def __init__(self) -> None:
            self.engine = _FakeEngine()
            self.calls = 0

        @retry_disconnected_read
        def read(self) -> None:
            self.calls += 1
            raise error

    monkeypatch.setattr(db_resilience.time, "sleep", sleeps.append)
    repository = Repository()

    with pytest.raises(OperationalError) as caught:
        repository.read()

    assert caught.value is error
    assert repository.calls == 1
    assert repository.engine.dispose_calls == 0
    assert sleeps == []


def test_arbitrary_exception_is_not_retried(monkeypatch) -> None:
    sleeps: list[float] = []

    class Repository:
        def __init__(self) -> None:
            self.engine = _FakeEngine()
            self.calls = 0

        @retry_disconnected_read
        def read(self) -> None:
            self.calls += 1
            raise ValueError("validation failed")

    monkeypatch.setattr(db_resilience.time, "sleep", sleeps.append)
    repository = Repository()

    with pytest.raises(ValueError, match="validation failed"):
        repository.read()

    assert repository.calls == 1
    assert repository.engine.dispose_calls == 0
    assert sleeps == []


def test_multi_query_logical_read_restarts_from_the_beginning(monkeypatch) -> None:
    events: list[str] = []
    returned_attempts: list[list[str]] = []

    class Repository:
        def __init__(self) -> None:
            self.engine = _FakeEngine()
            self.second_query_calls = 0

        @retry_disconnected_read
        def read_snapshot(self) -> list[str]:
            attempt = ["basket-row"]
            events.append("basket-row")
            self.second_query_calls += 1
            events.append("basket-items")
            if self.second_query_calls == 1:
                raise _disconnect_error()
            attempt.append("basket-items")
            returned_attempts.append(attempt)
            return attempt

    monkeypatch.setattr(db_resilience.time, "sleep", lambda _delay: None)
    repository = Repository()

    result = repository.read_snapshot()

    assert result == ["basket-row", "basket-items"]
    assert returned_attempts == [["basket-row", "basket-items"]]
    assert events == ["basket-row", "basket-items", "basket-row", "basket-items"]


def test_nested_read_uses_one_outer_retry(monkeypatch) -> None:
    events: list[str] = []

    class Repository:
        def __init__(self) -> None:
            self.engine = _FakeEngine()
            self.inner_calls = 0

        @retry_disconnected_read
        def outer_read(self) -> str:
            events.append("outer")
            return self.inner_read()

        @retry_disconnected_read
        def inner_read(self) -> str:
            events.append("inner")
            self.inner_calls += 1
            if self.inner_calls == 1:
                raise _disconnect_error()
            return "complete"

    monkeypatch.setattr(db_resilience.time, "sleep", lambda _delay: None)
    repository = Repository()

    assert repository.outer_read() == "complete"
    assert events == ["outer", "inner", "outer", "inner"]
    assert repository.engine.dispose_calls == 1


def test_price_cache_multi_query_read_restarts_completely(tmp_path: Path) -> None:
    repository = _sqlite_repository(tmp_path)
    country_query_calls = 0
    disconnect_pending = True

    def fail_once_after_country_query(
        _conn,
        _cursor,
        statement,
        _parameters,
        _context,
        _executemany,
    ) -> None:
        nonlocal country_query_calls, disconnect_pending
        normalized = " ".join(str(statement).split())
        if "FROM cached_countries" in normalized:
            country_query_calls += 1
        if disconnect_pending and "FROM cached_commodities" in normalized:
            disconnect_pending = False
            raise _disconnect_error()

    event.listen(repository.engine, "before_cursor_execute", fail_once_after_country_query)

    metadata = repository.get_country_metadata("SSD")

    assert metadata is not None
    assert metadata.country.country_iso3 == "SSD"
    assert country_query_calls == 2


def test_basket_multi_query_read_restarts_completely(monkeypatch, tmp_path: Path) -> None:
    price_repository = _sqlite_repository(tmp_path)
    repository = SqlCountryFoodBasketRepository(price_repository.engine)
    saved = repository.save_basket(
        "SSD",
        items=[{"commodity_id": 1, "weight_quantity": 1}],
    )
    basket_query_calls = 0
    original_get_regions = repository._get_regions
    regions_calls = 0

    def count_basket_query(
        _conn,
        _cursor,
        statement,
        _parameters,
        _context,
        _executemany,
    ) -> None:
        nonlocal basket_query_calls
        if "FROM country_food_basket_current" in " ".join(str(statement).split()):
            basket_query_calls += 1

    def fail_regions_once(basket_version_id: str) -> list[str]:
        nonlocal regions_calls
        regions_calls += 1
        if regions_calls == 1:
            raise _disconnect_error()
        return original_get_regions(basket_version_id)

    event.listen(price_repository.engine, "before_cursor_execute", count_basket_query)
    monkeypatch.setattr(repository, "_get_regions", fail_regions_once)

    reloaded = repository.get_active_basket("SSD")

    assert reloaded is not None
    assert reloaded.basket_version_id == saved.basket_version_id
    assert basket_query_calls == 2


def test_read_methods_are_protected_and_mutations_are_not() -> None:
    price_reads = (
        "get_cache_status",
        "get_active_version_id",
        "get_active_version_id_for_country",
        "list_countries",
        "get_country_metadata",
        "get_country_availability",
        "get_price_window",
        "get_commodity_price_units",
        "get_country_price_keys",
        "count_country_price_rows",
        "count_active_country_price_rows",
        "list_cache_refreshes",
        "get_cache_refresh",
    )
    basket_reads = (
        "get_active_basket",
        "get_active_baskets",
        "get_basket_version",
        "list_basket_history",
        "_get_price_row_unit",
    )
    mutations = (
        (SqlPriceCacheRepository, "create_cache_version"),
        (SqlPriceCacheRepository, "publish_cache_version"),
        (SqlPriceCacheRepository, "acquire_refresh_lock"),
        (SqlPriceCacheRepository, "release_refresh_lock"),
        (SqlCountryFoodBasketRepository, "save_basket"),
        (SqlCountryFoodBasketRepository, "archive_basket"),
        (SqlCountryFoodBasketRepository, "archive_secondary_basket"),
    )

    assert all(
        hasattr(getattr(SqlPriceCacheRepository, name), "__wrapped__")
        for name in price_reads
    )
    assert all(
        hasattr(getattr(SqlCountryFoodBasketRepository, name), "__wrapped__")
        for name in basket_reads
    )
    assert all(
        not hasattr(getattr(repository, name), "__wrapped__")
        for repository, name in mutations
    )


def test_postgres_repository_read_recovers_after_idle_backend_termination() -> None:
    database_url = os.getenv("TEST_POSTGRES_DATABASE_URL")
    if not database_url:
        if str(os.getenv("CI", "")).strip().lower() in {"1", "true", "yes", "on"}:
            pytest.fail(
                "TEST_POSTGRES_DATABASE_URL is required for PostgreSQL resilience tests in CI"
            )
        pytest.skip("TEST_POSTGRES_DATABASE_URL is not configured")

    schema = f"cache_resilience_{uuid.uuid4().hex}"
    admin_engine = create_sqlalchemy_engine(database_url, future=True)
    with admin_engine.begin() as conn:
        conn.exec_driver_sql(f'CREATE SCHEMA "{schema}"')

    scoped_url = make_url(database_url).update_query_dict(
        {"options": f"-csearch_path={schema}"},
    ).render_as_string(
        hide_password=False,
    )
    config = load_price_cache_config(
        {
            "PRICE_CACHE_BACKEND": "postgres",
            "PRICE_CACHE_DATABASE_URL": scoped_url,
        }
    )
    engine = create_price_cache_engine(config)
    try:
        apply_migrations(engine, "postgres")
        repository = SqlPriceCacheRepository(engine)
        seed_cache_snapshot(repository)
        assert repository.get_cache_status().has_active_cache is True

        with engine.connect() as conn:
            backend_pid = int(conn.scalar(text("SELECT pg_backend_pid()")))
        with admin_engine.begin() as conn:
            terminated = conn.scalar(
                text("SELECT pg_terminate_backend(:backend_pid)"),
                {"backend_pid": backend_pid},
            )
        assert terminated is True

        recovered = repository.get_cache_status()

        assert recovered.has_active_cache is True
        assert recovered.active_version_id is not None
    finally:
        engine.dispose()
        with admin_engine.begin() as conn:
            conn.exec_driver_sql(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        admin_engine.dispose()
