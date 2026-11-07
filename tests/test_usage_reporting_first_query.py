"""The install's first successful query date: which queries set it, how long it lasts, where it goes."""

from __future__ import annotations

import datetime
import json
import uuid
from contextlib import contextmanager

import pytest
from fastapi.testclient import TestClient
from mcp.shared.memory import create_connected_server_and_client_session

from slayer import usage_reporting
from slayer.api.server import create_app
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.mcp.server import create_mcp_server
from slayer.storage.yaml_storage import YAMLStorage
from slayer.usage_reporting import settings
from tests._usage_reporting_helpers import (
    build_storage,
    cli,
    end_process,
    quiet_config,
    raw_config,
    show,
    stored_first_query,
    write_config,
    write_raw_config,
)
from tests.flight.test_handlers import _make_handlers
from tests.pg_facade.test_connection import _query, _run, _startup, _terminate

_QUERY = {"source_model": "orders", "measures": ["sum(amount)"]}
_FAILING_QUERY = {"source_model": "no_such_model", "measures": ["count(*)"]}
_T0 = datetime.datetime(2026, 5, 4, 12, 0, tzinfo=datetime.timezone.utc)
_TODAY = "2026-05-04"


@pytest.fixture
def at_t0(usage_reporting_env):
    quiet_config(last_sent=_T0)
    usage_reporting_env.set_now(_T0)
    return usage_reporting_env


@pytest.fixture
def store(at_t0) -> str:
    return build_storage(at_t0.root)


@pytest.fixture
def started(at_t0) -> None:
    usage_reporting.start()


async def _mcp_query(store: str, **arguments) -> bool:
    """One MCP ``query`` call; whether it succeeded."""
    server = create_mcp_server(storage=YAMLStorage(base_dir=store), _seed_help=False)
    async with create_connected_server_and_client_session(server) as session:
        result = await session.call_tool("query", {"query": _QUERY, **arguments})
    return not result.isError


# --- which queries set it -------------------------------------------------------------------------


def test_cli_query_sets_the_date(store: str) -> None:
    assert cli(["query", json.dumps(_QUERY), "--storage", store]).returncode == 0
    assert stored_first_query() == _TODAY
    assert show()["first_query_ok"] == _TODAY


async def test_mcp_query_sets_the_date(store: str, started) -> None:
    assert await _mcp_query(store)
    end_process()
    assert stored_first_query() == _TODAY


async def test_mcp_explain_sets_the_date(store: str, started) -> None:
    assert await _mcp_query(store, explain=True)
    end_process()
    assert stored_first_query() == _TODAY


def test_rest_query_sets_the_date(store: str, started) -> None:
    assert TestClient(create_app(storage=YAMLStorage(base_dir=store))).post("/query", json=_QUERY).status_code == 200
    end_process()
    assert stored_first_query() == _TODAY


def test_flight_query_sets_the_date(started) -> None:
    _make_handlers().do_get_for_sql("SELECT revenue_sum FROM jaffle.orders")
    end_process()
    assert stored_first_query() == _TODAY


async def test_pg_query_sets_the_date(started) -> None:
    await _run(_startup(user="u", database="jaffle") + _query("SELECT revenue_sum FROM orders") + _terminate())
    end_process()
    assert stored_first_query() == _TODAY


def _assert_no_date() -> None:
    assert raw_config().get("first_query_ok") is None
    assert show()["first_query_ok"] is None


def test_failed_cli_query_does_not_set_it(store: str) -> None:
    cli(["query", json.dumps(_FAILING_QUERY), "--storage", store])
    _assert_no_date()


def test_cli_dry_run_does_not_set_it(store: str) -> None:
    assert cli(["query", json.dumps(_QUERY), "--storage", store, "--dry-run"]).returncode == 0
    _assert_no_date()


async def test_mcp_failure_and_dry_run_do_not_set_it(store: str, started) -> None:
    server = create_mcp_server(storage=YAMLStorage(base_dir=store), _seed_help=False)
    async with create_connected_server_and_client_session(server) as session:
        assert (await session.call_tool("query", {"query": _FAILING_QUERY})).isError
        assert not (await session.call_tool("query", {"query": _QUERY, "dry_run": True})).isError
    end_process()
    _assert_no_date()


def test_rest_failure_and_dry_run_do_not_set_it(store: str, started) -> None:
    client = TestClient(create_app(storage=YAMLStorage(base_dir=store)))
    assert client.post("/query", json=_FAILING_QUERY).status_code >= 400
    assert client.post("/query", json={**_QUERY, "dry_run": True}).status_code == 200
    end_process()
    _assert_no_date()


async def test_probes_do_not_set_it(started) -> None:
    _make_handlers().do_get_for_sql("SELECT 1")
    await _run(_startup(user="u", database="jaffle") + _query("SELECT 1") + _terminate())
    end_process()
    _assert_no_date()


def test_library_use_stores_nothing(at_t0) -> None:
    store = build_storage(at_t0.root)
    settings.config_path().unlink()
    SlayerQueryEngine(YAMLStorage(base_dir=store)).execute_sync(SlayerQuery.model_validate(_QUERY))
    assert not settings.config_path().exists()


# --- how long it lasts --------------------------------------------------------------------------------


def test_later_query_keeps_the_date(store: str, at_t0) -> None:
    write_config(install_id=uuid.uuid4(), install_id_created=_T0.date())
    write_raw_config(first_query_ok="2026-01-02")
    at_t0.set_now(_T0 + datetime.timedelta(days=3))
    assert cli(["query", json.dumps(_QUERY), "--storage", store]).returncode == 0
    assert stored_first_query() == "2026-01-02"
    assert show()["first_query_ok"] == "2026-01-02"


def _write_spies(monkeypatch: pytest.MonkeyPatch) -> tuple[list[object], list[object]]:
    writes: list[object] = []
    locks: list[object] = []
    real_write, real_lock = settings.write_private, settings.exclusive_lock

    def write(path, data):
        if path == settings.config_path():
            writes.append(path)
        return real_write(path, data)

    @contextmanager
    def lock(path, *, wait):
        locks.append(path)
        with real_lock(path, wait=wait) as held:
            yield held

    monkeypatch.setattr(settings, "write_private", write)
    monkeypatch.setattr(settings, "exclusive_lock", lock)
    return writes, locks


def test_one_config_write_per_install_id(started, monkeypatch: pytest.MonkeyPatch) -> None:
    writes, locks = _write_spies(monkeypatch)
    for _ in range(4):
        usage_reporting.record(surface="mcp", token="query")
    end_process()
    assert stored_first_query() == _TODAY
    assert (len(writes), len(locks)) == (1, 1)


def test_rotation_by_another_process_is_seen(started, at_t0) -> None:
    usage_reporting.record(surface="mcp", token="query")
    assert stored_first_query() == _TODAY
    rotated = uuid.uuid4()
    write_raw_config(install_id=str(rotated), install_id_created=_TODAY, first_query_ok=None)
    at_t0.set_now(_T0 + datetime.timedelta(days=1))
    usage_reporting.record(surface="mcp", token="query")
    end_process()
    assert raw_config()["install_id"] == str(rotated)
    assert stored_first_query() == "2026-05-05"


# --- rotation and disable clear it ---------------------------------------------------------------------


@pytest.mark.parametrize(("age_days", "kept"), [(10 * 30, True), (11 * 31, False)])
def test_with_install_id_clears_the_date_on_a_new_id(age_days: int, kept: bool) -> None:
    created = _T0.date() - datetime.timedelta(days=age_days)
    config = settings.UsageReportingConfig.model_validate({
        "install_id": str(uuid.uuid4()), "install_id_created": created.isoformat(), "first_query_ok": "2026-01-02",
    })
    rotated = settings.with_install_id(config, today=_T0.date())
    assert (rotated.install_id == config.install_id) is kept
    assert rotated.first_query_ok == (datetime.date(2026, 1, 2) if kept else None)


def test_with_install_id_creating_the_first_id_has_no_date() -> None:
    config = settings.UsageReportingConfig.model_validate({"first_query_ok": "2026-01-02"})
    assert settings.with_install_id(config, today=_T0.date()).first_query_ok is None


def test_rotation_at_start_clears_the_date(at_t0) -> None:
    old = uuid.uuid4()
    write_config(install_id=old, install_id_created=(_T0 - datetime.timedelta(days=400)).date())
    write_raw_config(first_query_ok="2026-01-02")
    usage_reporting.start()
    usage_reporting.record(surface="mcp", token="search")
    end_process()
    assert raw_config()["install_id"] != str(old)
    assert stored_first_query() is None
    assert show()["first_query_ok"] is None


def test_rotation_at_send_clears_the_date(at_t0) -> None:
    old = uuid.uuid4()
    write_config(install_id=old, install_id_created=(_T0 - datetime.timedelta(days=300)).date())
    write_raw_config(first_query_ok="2026-01-02")
    at_t0.set_now(_T0 + datetime.timedelta(hours=1))
    usage_reporting.start()
    at_t0.set_now(_T0 + datetime.timedelta(days=60))
    usage_reporting.record(surface="mcp", token="search")
    (request,) = at_t0.capture.wait_for(count=1)
    end_process()
    properties = request.json()["properties"]
    assert properties["install_id"] != str(old)
    assert properties["first_query_ok"] is None
    assert stored_first_query() is None


def test_disable_clears_the_date(store: str) -> None:
    assert cli(["query", json.dumps(_QUERY), "--storage", store]).returncode == 0
    assert stored_first_query() == _TODAY
    assert cli(["usage-reporting", "disable"]).returncode == 0
    assert stored_first_query() is None


# --- where it goes -------------------------------------------------------------------------------------


def test_sent_report_carries_the_date(store: str, at_t0) -> None:
    assert cli(["query", json.dumps(_QUERY), "--storage", store]).returncode == 0
    at_t0.set_now(_T0 + datetime.timedelta(days=2))
    usage_reporting.start()
    (request,) = at_t0.capture.wait_for(count=1)
    end_process()
    assert request.json()["properties"]["first_query_ok"] == _TODAY
