"""The integrator's query set renders as dry runs through its 1.x read-only backend, and the SQL returns its rows.

Spec: compatibility/integrator-1x-calls › Dry-run rendering through a 1.x read-only backend; › Queries build from
column-reference objects. Expected rows were recorded by executing the 1.8.4 statements on the same data.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Callable, Iterator
from decimal import Decimal
from typing import Any, LiteralString, cast

import duckdb
import psycopg
import pytest
from pytest_postgresql import factories

from slayer.core.errors import ListTypeError
from slayer.engine.query_engine import SlayerQueryEngine
from tests._integrator_1x import DATA_SQL, ReadOnlyStorage, legacy, one_deprecation, quiet, v15_models

postgresql_proc = factories.postgresql_proc(port=None)

DIALECTS = ("duckdb", "bigquery", "snowflake", "postgres", "redshift")
GRAINS = ("second", "minute", "hour", "day", "week", "week_sunday", "month", "quarter", "year")
CLOCK = dt.datetime(2025, 1, 15, 12, 0, 0)


def _column(name: str) -> Any:
    with quiet():
        return legacy("slayer.core.query", "ColumnRef")(name=name)


def _order(name: str, direction: str) -> Any:
    return legacy("slayer.core.query", "OrderItem")(column=_column(name), direction=direction)


def _time(name: str, granularity: str) -> Any:
    return legacy("slayer.core.query", "TimeDimension")(dimension=_column(name), granularity=granularity)


def _query(**fields: Any) -> Any:
    return legacy("slayer.core.query", "SlayerQuery")(**fields)


def _query_set() -> dict[str, Any]:
    """The integrator's queries, built as it builds them."""
    with quiet():
        queries = {
            "by_status": _query(
                source_model="orders", measures=["revenue", "order_count", "aov"], dimensions=[_column("status")],
                filters=["customers.country IN ('DE', 'FR')"], order=[_order("revenue", "desc")], limit=10),
            "filtered_measures": _query(
                source_model="orders", measures=["paid_revenue", "amount_spread"], dimensions=[_column("status")],
                filters=["sum(amount) > 10"], order=[_order("status", "asc")]),
            "status_upper": _query(
                source_model="orders", measures=["order_count"], dimensions=[_column("status_upper")],
                order=[_order("status_upper", "asc")]),
            "values_status": _query(source_model="orders", distinct_dimension_values=True,
                                    dimensions=[_column("status")]),
            "values_month": _query(source_model="orders", distinct_dimension_values=True,
                                   time_dimensions=[_time("created_at", "month")]),
            "tags": _query(source_model="orders", measures=["order_count"], dimensions=[_column("tags.value")],
                           to_many_handling="associate", order=[_order("tags.value", "asc")]),
            "customer_count": _query(source_model="orders", measures=["customer_count"],
                                     dimensions=[_column("status")], order=[_order("status", "asc")]),
        }
        for grain in GRAINS:
            queries[f"grain_{grain}"] = _query(
                source_model="orders", measures=["revenue"], time_dimensions=[_time("created_at", grain)],
                order=[_order("revenue", "desc")])
    return queries


GRAIN_COLUMNS = ["orders.created_at", "orders.revenue"]
COLUMNS_1_8_4 = {
    "by_status": ["orders.status", "orders.revenue", "orders.order_count", "orders.aov"],
    "filtered_measures": ["orders.status", "orders.paid_revenue", "orders.amount_spread"],
    "status_upper": ["orders.status_upper", "orders.order_count"],
    "values_status": ["orders.status"],
    "values_month": ["orders.created_at"],
    "tags": ["orders.tags.value", "orders.order_count"],
    "customer_count": ["orders.status", "orders.customer_count"],
    **{f"grain_{grain}": GRAIN_COLUMNS for grain in GRAINS},
}


def _grain_rows(*buckets: tuple[str, float]) -> list[list[Any]]:
    return [[bucket, amount] for bucket, amount in buckets]


ROWS_1_8_4: dict[str, list[list[Any]]] = {
    "by_status": [["paid", 45.0, 3, 15.0], ["open", 20.0, 1, 20.0]],
    "filtered_measures": [["open", None, 20.0], ["paid", 45.0, 25.0]],
    "status_upper": [["OPEN", 2], ["PAID", 3]],
    "values_status": [["open"], ["paid"]],
    "values_month": [["2024-01-01T00:00:00"], ["2024-02-01T00:00:00"], ["2024-04-01T00:00:00"],
                     ["2024-07-01T00:00:00"]],
    "tags": [["dbt", 3], ["py", 2], ["sql", 2]],
    # 2.0 value (joined aggregates no longer associate); 1.8.4 returned 3 for both.
    "customer_count": [["open", 2], ["paid", 2]],
    "grain_second": _grain_rows(("2024-07-14T12:30:00", 40.0), ("2024-02-11T23:59:59", 30.0),
                                ("2024-01-20T08:00:00", 20.0), ("2024-01-03T10:15:30", 10.0),
                                ("2024-04-01T00:00:00", 5.0)),
    "grain_minute": _grain_rows(("2024-07-14T12:30:00", 40.0), ("2024-02-11T23:59:00", 30.0),
                                ("2024-01-20T08:00:00", 20.0), ("2024-01-03T10:15:00", 10.0),
                                ("2024-04-01T00:00:00", 5.0)),
    "grain_hour": _grain_rows(("2024-07-14T12:00:00", 40.0), ("2024-02-11T23:00:00", 30.0),
                              ("2024-01-20T08:00:00", 20.0), ("2024-01-03T10:00:00", 10.0),
                              ("2024-04-01T00:00:00", 5.0)),
    "grain_day": _grain_rows(("2024-07-14T00:00:00", 40.0), ("2024-02-11T00:00:00", 30.0),
                             ("2024-01-20T00:00:00", 20.0), ("2024-01-03T00:00:00", 10.0),
                             ("2024-04-01T00:00:00", 5.0)),
    "grain_week": _grain_rows(("2024-07-08T00:00:00", 40.0), ("2024-02-05T00:00:00", 30.0),
                              ("2024-01-15T00:00:00", 20.0), ("2024-01-01T00:00:00", 10.0),
                              ("2024-04-01T00:00:00", 5.0)),
    "grain_week_sunday": _grain_rows(("2024-07-14T00:00:00", 40.0), ("2024-02-11T00:00:00", 30.0),
                                     ("2024-01-14T00:00:00", 20.0), ("2023-12-31T00:00:00", 10.0),
                                     ("2024-03-31T00:00:00", 5.0)),
    "grain_month": _grain_rows(("2024-07-01T00:00:00", 40.0), ("2024-02-01T00:00:00", 30.0),
                               ("2024-01-01T00:00:00", 30.0), ("2024-04-01T00:00:00", 5.0)),
    "grain_quarter": _grain_rows(("2024-01-01T00:00:00", 60.0), ("2024-07-01T00:00:00", 40.0),
                                 ("2024-04-01T00:00:00", 5.0)),
    "grain_year": _grain_rows(("2024-01-01T00:00:00", 105.0)),
}

LIST_ELEMENT_QUERY = "tags"


def _engine(ds_type: str) -> SlayerQueryEngine:
    with quiet():
        storage = ReadOnlyStorage(v15_models(), ds_type=ds_type)  # pyright: ignore[reportAbstractUsage]
    return SlayerQueryEngine(storage, policy=None, clock=lambda: CLOCK)


def _render(ds_type: str) -> dict[str, Any]:
    """Each query's dry-run response, or the error rendering raised (the integrator silences SLayer's warnings)."""
    engine = _engine(ds_type)
    out: dict[str, Any] = {}
    try:
        for name, query in _query_set().items():
            with quiet():
                try:
                    out[name] = engine.execute_sync(query, dry_run=True)
                except Exception as exc:  # noqa: BLE001 — the refusal is the result under test
                    out[name] = exc
    finally:
        engine.close()
    return out


@pytest.fixture(scope="module")
def rendered() -> dict[str, dict[str, Any]]:
    return {ds_type: _render(ds_type) for ds_type in DIALECTS}


def test_a_time_dimension_builds_from_a_column_reference() -> None:
    column = _column("created_at")
    with one_deprecation(naming="field"):
        time_dimension = legacy("slayer.core.query", "TimeDimension")(dimension=column, granularity="month")
    assert time_dimension.field == "created_at"


def test_an_order_item_builds_from_a_column_reference() -> None:
    column = _column("revenue")
    with one_deprecation(naming="field"):
        item = legacy("slayer.core.query", "OrderItem")(column=column, direction="desc")
    assert item.field == "revenue"


def test_every_granularity_renders(rendered: dict[str, dict[str, Any]]) -> None:
    for grain in GRAINS:
        response = rendered["duckdb"][f"grain_{grain}"]
        assert not isinstance(response, Exception), f"{grain}: {response}"
        assert response.sql
        assert list(response.columns) == GRAIN_COLUMNS


@pytest.mark.parametrize("ds_type", DIALECTS)
def test_the_query_set_renders_on_every_supported_dialect(ds_type: str, rendered: dict[str, dict[str, Any]]) -> None:
    for name, response in rendered[ds_type].items():
        if ds_type == "redshift" and name == LIST_ELEMENT_QUERY:
            continue
        assert not isinstance(response, Exception), f"{name}: {response!r}"
        assert response.sql, name
        assert list(response.columns) == COLUMNS_1_8_4[name], name


def test_the_list_element_query_is_refused_on_redshift(rendered: dict[str, dict[str, Any]]) -> None:
    refusal = rendered["redshift"][LIST_ELEMENT_QUERY]
    assert isinstance(refusal, ListTypeError)
    assert "tags.value" in str(refusal)


def test_bigquery_spells_result_columns_with_triple_underscores(rendered: dict[str, dict[str, Any]]) -> None:
    dialect = legacy("slayer.sql.dialects", "dialect_for_ds_type")("bigquery")
    aliases = {name: [dialect.emit_alias(key) for key in r.columns] for name, r in rendered["bigquery"].items()}
    assert aliases["by_status"] == ["orders___status", "orders___revenue", "orders___order_count", "orders___aov"]
    assert aliases["tags"] == ["orders___tags___value", "orders___order_count"]


def test_the_read_only_backend_refuses_writes() -> None:
    with quiet():
        storage = ReadOnlyStorage(v15_models(), ds_type="duckdb")  # pyright: ignore[reportAbstractUsage]
        orders = v15_models()["orders"]
    engine = SlayerQueryEngine(storage, policy=None, clock=lambda: CLOCK)
    try:
        with pytest.raises(RuntimeError, match="read-only"):
            engine.run_sync(storage.save_dataset(orders))
    finally:
        engine.close()


def _normalized(value: Any) -> Any:
    if isinstance(value, dt.datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        value = float(value)
    if isinstance(value, float):
        return round(value, 6)
    return value


def _rows(rows: list[tuple[Any, ...]]) -> list[list[Any]]:
    return sorted(([_normalized(v) for v in row] for row in rows), key=repr)


def _statements() -> list[str]:
    return [s.strip() for s in DATA_SQL.read_text().split(";") if s.strip()]


@pytest.fixture
def duckdb_run() -> Iterator[Callable[[str], list[tuple[Any, ...]]]]:
    con = duckdb.connect()
    for statement in _statements():
        con.execute(statement)
    try:
        yield lambda sql: con.execute(sql).fetchall()
    finally:
        con.close()


@pytest.fixture
def postgres_run(postgresql_proc: Any) -> Iterator[Callable[[str], list[tuple[Any, ...]]]]:
    info = postgresql_proc
    with psycopg.connect(host=info.host, port=info.port, user=info.user, dbname="postgres", autocommit=True) as conn:
        conn.execute("DROP SCHEMA IF EXISTS public CASCADE")
        conn.execute("CREATE SCHEMA public")
        for statement in _statements():
            conn.execute(cast(LiteralString, statement))
        yield lambda sql: list(conn.execute(cast(LiteralString, sql)).fetchall())


def test_rendered_duckdb_statements_return_the_expected_rows(
    duckdb_run: Callable[[str], list[tuple[Any, ...]]],
) -> None:
    for name, response in _render("duckdb").items():
        assert not isinstance(response, Exception), f"{name}: {response!r}"
        assert _rows(duckdb_run(response.sql)) == _rows([tuple(r) for r in ROWS_1_8_4[name]]), name


def test_rendered_postgres_statements_return_the_expected_rows(
    postgres_run: Callable[[str], list[tuple[Any, ...]]],
) -> None:
    for name, response in _render("postgres").items():
        assert not isinstance(response, Exception), f"{name}: {response!r}"
        assert _rows(postgres_run(response.sql)) == _rows([tuple(r) for r in ROWS_1_8_4[name]]), name
