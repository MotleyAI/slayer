"""What one query or ingest sends: the span tree, OTel standard attributes, and the metrics."""

from __future__ import annotations

import asyncio
import inspect
from typing import Any

import pytest
from opentelemetry.trace import SpanKind, StatusCode

from slayer import otel
from slayer.core import probes
from slayer.core.models import DatasourceConfig
from slayer.database import client as client_module
from slayer.storage.sqlite_conn import transaction
from slayer.otel import adapter
from tests._otel_helpers import OtelCapture
from tests._saved_query_refinement_fixtures import refine_engine

_REVENUE = {"population": "orders", "metrics": ["sum(amount)"], "dimensions": ["region"]}
_MISSING = {"population": "no_such_dataset", "metrics": ["count(*)"]}


def _run(query: dict[str, Any], **kwargs: Any) -> None:
    async def main() -> None:
        async with refine_engine("sqlite") as (engine, _):
            await engine.execute(query, **kwargs)

    asyncio.run(main())


def _run_twice_cached() -> None:
    async def main() -> None:
        async with refine_engine("sqlite") as (engine, _):
            await engine.execute(_REVENUE, cache=True)
            await engine.execute(_REVENUE, cache=True)

    asyncio.run(main())


def _ingest() -> None:
    async def main() -> None:
        async with refine_engine("sqlite") as (engine, _):
            datasource = await engine.storage.get_datasource("test")
            assert datasource is not None
            await engine.ingest_idempotent(datasource)

    asyncio.run(main())


def _failing() -> None:
    with pytest.raises(Exception, match="no_such_dataset"):
        _run(_MISSING)


# --- traces --------------------------------------------------------------------------


_PARENTS = [
    ("slayer.query.normalize", "slayer.query"),
    ("slayer.query.prepare", "slayer.query"),
    ("slayer.query.run", "slayer.query"),
    ("slayer.db.execute", "slayer.query.run"),
]


@pytest.mark.parametrize(("child", "parent"), _PARENTS)
def test_a_query_is_one_span_tree(otel_env: OtelCapture, child: str, parent: str) -> None:
    _run(_REVENUE)
    assert otel_env.parent_name(child) == parent


def test_the_query_span_carries_its_datasource_and_rows(otel_env: OtelCapture) -> None:
    _run(_REVENUE)
    attributes = otel_env.attributes("slayer.query")
    assert (attributes["slayer.datasource.name"], attributes["slayer.query.rows"], attributes["slayer.query.dry_run"]) == (
        "test", 2, False,
    )


def test_the_database_span_follows_the_otel_database_conventions(otel_env: OtelCapture) -> None:
    _run(_REVENUE)
    span = otel_env.span("slayer.db.execute")
    attributes = dict(span.attributes or {})
    assert (span.kind, attributes["db.system.name"], attributes["db.response.returned_rows"]) == (
        SpanKind.CLIENT, "sqlite", 2,
    )


def test_a_dry_run_reaches_no_database(otel_env: OtelCapture) -> None:
    _run(_REVENUE, dry_run=True)
    names = otel_env.span_names()
    assert ("slayer.query" in names, [name for name in names if name.startswith("slayer.db.")]) == (True, [])


def test_a_failing_query_marks_its_span_as_an_error(otel_env: OtelCapture) -> None:
    _failing()
    span = otel_env.span("slayer.query")
    assert (span.status.status_code, dict(span.attributes or {}).get("error.type")) == (
        StatusCode.ERROR, "DatasetNotFoundError",
    )


def test_ingest_is_one_span_with_its_outcome(otel_env: OtelCapture) -> None:
    _ingest()
    attributes = otel_env.attributes("slayer.ingest")
    assert (attributes["slayer.datasource.name"], attributes["slayer.ingest.datasets.updated"]) == ("test", 1)


_INGEST_CHILDREN = ["slayer.ingest.scan.schema", "slayer.ingest.merge", "slayer.ingest.drift", "slayer.ingest.embeddings"]


@pytest.mark.parametrize("child", _INGEST_CHILDREN)
def test_ingest_steps_nest_under_the_ingest_span(otel_env: OtelCapture, child: str) -> None:
    _ingest()
    assert otel_env.parent_name(child) == "slayer.ingest"


# --- metrics -------------------------------------------------------------------------


def test_query_duration_is_a_histogram_in_seconds(otel_env: OtelCapture) -> None:
    _run(_REVENUE)
    (point,) = otel_env.metric("slayer.query.duration")
    assert (point.unit, point.count, point.buckets, point.attributes) == (
        "s", 1, adapter.QUERY_BUCKETS, {"slayer.datasource.name": "test", "slayer.datasource.type": "sqlite"},
    )


def test_database_duration_uses_the_standard_metric(otel_env: OtelCapture) -> None:
    _run(_REVENUE)
    (point,) = otel_env.metric("db.client.operation.duration")
    assert (point.buckets, point.attributes["db.system.name"], point.attributes["slayer.datasource.name"]) == (
        adapter.DB_BUCKETS, "sqlite", "test",
    )


def test_a_failing_query_is_labelled_with_its_error_type(otel_env: OtelCapture) -> None:
    _failing()
    (point,) = otel_env.metric("slayer.query.duration")
    assert point.attributes == {"error.type": "DatasetNotFoundError"}


def test_cache_lookups_count_hits_and_misses(otel_env: OtelCapture) -> None:
    _run_twice_cached()
    lookups = {point.attributes["slayer.cache.hit"]: point.value for point in otel_env.metric("slayer.cache.lookups")}
    assert lookups == {False: 1, True: 1}


def test_ingest_counts_models_by_change(otel_env: OtelCapture) -> None:
    _ingest()
    counts = {point.attributes["slayer.ingest.change"]: point.value for point in otel_env.metric("slayer.ingest.datasets")}
    assert counts == {"updated": 1}


# --- adapter rules -------------------------------------------------------------------


_NAMES = [
    ("query.rows", "slayer.query.rows"),
    ("datasource.name", "slayer.datasource.name"),
    ("error.type", "error.type"),
    ("db.system.name", "db.system.name"),
    ("gen_ai.tool.name", "gen_ai.tool.name"),
]


@pytest.mark.parametrize(("key", "exported"), _NAMES)
def test_only_non_standard_keys_gain_the_namespace(key: str, exported: str) -> None:
    assert adapter.exported_name(key) == exported


_SYSTEMS = [
    ("postgres", "postgresql"),
    ("sqlite", "sqlite"),
    ("oracle", "oracle.db"),
    ("redshift", "aws.redshift"),
    ("mssql", "microsoft.sql_server"),
    ("duckdb", "other_sql"),
    ("snowflake", "other_sql"),
]


@pytest.mark.parametrize(("datasource_type", "system"), _SYSTEMS)
def test_db_system_name_uses_the_standard_values(otel_env: OtelCapture, datasource_type: str, system: str) -> None:
    with probes.scope(name="db.execute", attributes={"datasource.type": datasource_type}):
        pass
    assert otel_env.attributes("slayer.db.execute")["db.system.name"] == system


def test_every_mapped_system_is_an_otel_standard_value() -> None:
    known = pytest.importorskip("opentelemetry.semconv._incubating.attributes.db_attributes").DbSystemNameValues
    assert set(adapter.DB_SYSTEMS.values()) - {value.value for value in known} == set()


_KINDS = [
    ("db.execute", {}, SpanKind.CLIENT),
    ("request", {"surface": "pg"}, SpanKind.SERVER),
    ("request", {"surface": "flight"}, SpanKind.SERVER),
    ("request", {"surface": "cli"}, SpanKind.INTERNAL),
    ("query", {}, SpanKind.INTERNAL),
]


@pytest.mark.parametrize(("name", "attributes", "kind"), _KINDS)
def test_span_kind_follows_the_probe(
    otel_env: OtelCapture, name: str, attributes: dict[str, str], kind: SpanKind,
) -> None:
    with probes.scope(name=name, attributes=attributes):
        pass
    assert otel_env.span(f"slayer.{name}").kind == kind


def test_a_retry_is_a_span_event_and_a_counter(otel_env: OtelCapture) -> None:
    with probes.scope(name="db.execute", attributes={"datasource.name": "shop"}):
        probes.current().add_event(name="db.retry", attributes={"db.attempt": 2, "error.type": "OperationalError"})
    events = [(event.name, dict(event.attributes or {})) for event in otel_env.span("slayer.db.execute").events]
    (point,) = otel_env.metric("slayer.db.retries")
    assert (events, point.value) == (
        [("slayer.db.retry", {"slayer.db.attempt": 2, "error.type": "OperationalError"})], 1,
    )


def test_drift_entries_are_counted(otel_env: OtelCapture) -> None:
    with probes.scope(name="ingest.drift", attributes={"datasource.name": "shop"}):
        probes.current().set({"ingest.drift.entries": 3})
    (point,) = otel_env.metric("slayer.ingest.drift.entries")
    assert (point.value, point.attributes) == (3, {"slayer.datasource.name": "shop"})


def test_spans_nest_under_the_callers_span(otel_env: OtelCapture) -> None:
    tracer = otel_env.tracer_provider.get_tracer("host")
    with tracer.start_as_current_span("host.handler"), probes.scope(name="query"):
        pass
    assert otel_env.parent_name("slayer.query") == "host.handler"


def test_the_request_decorator_names_the_surface(otel_env: OtelCapture) -> None:
    @otel.request(surface="cli", token="query")
    def handler() -> int:
        return 1

    handler()
    attributes = otel_env.attributes("slayer.request")
    assert (attributes["slayer.surface"], attributes["slayer.operation"]) == ("cli", "query")


@pytest.mark.parametrize(("datasource", "expected"), [
    (DatasourceConfig(name="shop", type="postgres", host="old-db", connection_string="postgresql://u:pw@new-db:5433/sales"),
     ("new-db", 5433, "sales")),
    (DatasourceConfig(name="shop", type="postgres", host="db", port=5434, database="sales", username="u"),
     ("db", 5434, "sales")),
])
def test_database_server_attributes_follow_the_connection_used(
    datasource: DatasourceConfig, expected: tuple[str, int, str],
) -> None:
    client = client_module.SlayerSQLClient(datasource)
    bound = inspect.signature(client_module.SlayerSQLClient._typed_execute).bind(
        client, sql="SELECT 1", timeout_seconds=1, writes=False,
    )
    attributes = client_module._db_probe_attributes(bound)
    actual = (attributes["server.address"], attributes["server.port"], attributes["db.namespace"])
    assert actual == expected


def test_direct_synchronous_execution_is_a_database_span(
    otel_env: OtelCapture, tmp_path: Any, monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = tmp_path / "x.db"
    with transaction(database):  # creates the empty database file
        pass
    client = client_module.SlayerSQLClient(DatasourceConfig(name="shop", type="sqlite", database=str(database)))
    monkeypatch.setattr(client, "_async_url", lambda: None)  # a driver with no async twin
    try:
        client.execute_sync("SELECT 1")
    finally:
        client.close()
    assert otel_env.attributes("slayer.db.execute")["db.response.returned_rows"] == 1


def test_a_probe_with_an_error_type_is_a_failed_span(otel_env: OtelCapture) -> None:
    with probes.scope(name="request", attributes={"surface": "rest"}) as request:
        request.set({"error.type": "500"})
    (point,) = otel_env.metric("slayer.request.duration")
    assert (otel_env.span("slayer.request").status.status_code, point.attributes["error.type"]) == (
        StatusCode.ERROR, "500",
    )


def _responding(status: int) -> Any:
    async def app(scope: Any, receive: Any, send: Any) -> None:
        await send({"type": "http.response.start", "status": status, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    return app


@pytest.mark.parametrize(("status", "failed"), [(500, True), (503, True), (404, False), (200, False)])
async def test_a_rest_request_span_records_its_status_and_fails_on_5xx(
    otel_env: OtelCapture, status: int, failed: bool,
) -> None:
    async def receive() -> dict[str, Any]:
        return {"type": "http.request"}

    async def send(_message: Any) -> None:
        return None

    await otel.RequestMiddleware(_responding(status), mounted="/mcp")({"type": "http", "path": "/query"}, receive, send)
    span = otel_env.span("slayer.request")
    attributes = otel_env.attributes("slayer.request")
    assert (span.status.status_code is StatusCode.ERROR, attributes["http.response.status_code"],
            attributes.get("error.type")) == (failed, status, str(status) if failed else None)
