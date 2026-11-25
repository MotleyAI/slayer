"""The Ossie compliance gaps outside Layer 1: the semantic-model format (DEV-2244) cases.

Each probe asserts Ossie's behaviour; the gap's case id and fixing issue are the compliance page's row. Layer 1 gaps
are the gap rows of ``tests/_dev2446_required_manifest.py``.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from decimal import Decimal
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict

from slayer.core.errors import (
    AggregationNotAllowedError, DimensionTypeError, DuplicateMetricNameError, OssieQueryError, RelationshipKeyError,
    TimeLiteralError, UnknownReferenceError,
)
from slayer.facade.catalog import build_local_view
from slayer.inspect.service import InspectService
from tests._dev2217_fixtures import DS, sql_engine, strict
from tests._dev2243_fixtures import dataset, document_text, field, metric, relationship

#: The vendored Ossie revision the compliance page and these cases are measured against.
REVISION = "d4dca4e9"
FORMAT = "DEV-2244"

Probe = Callable[[Path], Awaitable[None]]


class Gap(BaseModel):
    """One probe of a gap; several probes may share a case."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    case: str
    issue: str
    label: str
    database: str
    raises: type[BaseException]
    probe: Probe

    @property
    def id(self) -> str:
        return f"{self.case}|{self.label}|{self.database}"


# --------------------------------------------------------------------------- #
# DEV-2244: semantic-model format
# --------------------------------------------------------------------------- #
ORDERS_SQL = ("CREATE TABLE orders (id VARCHAR, amount DOUBLE, unit_price INTEGER, \"Order Amount\" DOUBLE);"
              "INSERT INTO orders VALUES ('O1', 10, 1, 1.5), ('O2', 20, 2, 2.5);")
SALES_SQL = ("CREATE TABLE sales (id VARCHAR, amount DOUBLE, order_date DATE, created_at TIMESTAMP, d_year INTEGER);"
             "INSERT INTO sales VALUES ('S1', 10, '2026-01-05', '2026-03-09 10:00:00', 2026),"
             "('S2', 20, '2026-01-20', '2026-03-01 11:00:00', 2026), ('S3', 5, '2026-02-03', '2026-03-02 10:00:00', 2026);")
TYPES_SQL = ("CREATE TABLE t (id VARCHAR, tod TIME, ts TIMESTAMPTZ);"
             "INSERT INTO t VALUES ('A', '09:30:00', '2026-01-01 23:30:00+00'), ('B', '09:30:00', '2026-01-02 01:30:00+05'),"
             "('C', '17:00:00', '2026-01-02 12:00:00+00');")
TYPES_DOC = document_text(name="m", datasets=[dataset("t", primary_key=["id"], fields=[
    field("id"), field("tod", datatype="Time"), field("ts", datatype="DateTimeTz")])])


def orders_doc(*fields: dict[str, Any], metrics: list[dict[str, Any]] | None = None, name: str = "orders") -> str:
    return document_text(name="m", datasets=[dataset(name, source="orders", primary_key=["id"],
                                                     fields=[field("id"), *fields], metrics=metrics)])


async def relationship_ai_context(base: Path) -> None:
    sql = ("CREATE TABLE customers (id VARCHAR, region VARCHAR); INSERT INTO customers VALUES ('C1', 'EAST');"
           "CREATE TABLE orders (id VARCHAR, customer_id VARCHAR); INSERT INTO orders VALUES ('O1', 'C1');")
    rel = {**relationship("orders_to_customers", src="orders", to="customers", from_columns=["customer_id"],
                          to_columns=["id"]), "ai_context": {"synonyms": ["buyer"]}}
    doc = document_text(name="m", relationships=[rel], datasets=[
        dataset("customers", primary_key=["id"], fields=[field("id"), field("region")]),
        dataset("orders", primary_key=["id"], fields=[field("id"), field("customer_id")])])
    async with sql_engine(base, sql=sql, documents=[doc]) as engine:
        text = await InspectService(storage=engine.storage, engine=engine).inspect(
            reference="orders", entity_type="dataset", compact=False)
    assert "buyer" in text


async def is_time_role(base: Path) -> None:
    doc = document_text(name="m", datasets=[dataset("sales", primary_key=["id"], fields=[
        field("id"), field("order_date", datatype="Date"),
        field("created_at", datatype="DateTime", dimension={"is_time": False}),
        field("d_year", datatype="Integer", dimension={"is_time": True})])])
    async with sql_engine(base, sql=SALES_SQL, documents=[doc]) as engine:
        sales = await engine.storage.get_dataset("sales", data_source=DS)
    assert sales is not None
    dims, _ = build_local_view(sales)
    assert {d.name: d.is_time for d in dims} == {"id": False, "order_date": True, "created_at": False, "d_year": True}


async def is_time_false_axis(base: Path) -> None:
    doc = document_text(name="m", datasets=[dataset("sales", primary_key=["id"], fields=[
        field("id"), field("amount", datatype="Float"), field("order_date", datatype="Date"),
        field("created_at", datatype="DateTime", dimension={"is_time": False})])])
    async with sql_engine(base, sql=SALES_SQL, documents=[doc]) as engine:
        response = await engine.execute({"population": "sales", "metrics": [{"metric": "last(sales.amount)", "alias": "v"}]})
    assert response.data == [{"v": 5.0}]


async def metric_dialect_entry(base: Path) -> None:
    doc = orders_doc(field("amount", datatype="Float"), metrics=[{"name": "m", "expression": {
        "sql": "SUM(amount)", "dialects": [{"dialect": "DUCKDB", "expression": "SUM(amount) * 2"}]}}])
    async with sql_engine(base, sql=ORDERS_SQL, documents=[doc]) as engine:
        response = await engine.execute({"population": "orders", "metrics": ["orders.m"]})
    assert response.data == [{"orders.m": 60.0}]


async def field_name_dot(base: Path) -> None:
    doc = orders_doc(field("unit.price", "unit_price", datatype="Integer"))
    async with sql_engine(base, sql=ORDERS_SQL, documents=[doc]) as engine:
        response = await strict(engine, {"dimensions": ['orders."unit.price"']})
    assert response.columns == ["unit.price"]
    assert sorted(r["unit.price"] for r in response.data) == [1, 2]


async def dataset_name_dot(base: Path) -> None:
    doc = orders_doc(field("unit_price", datatype="Integer"), name="sales.orders")
    async with sql_engine(base, sql=ORDERS_SQL, documents=[doc]) as engine:
        response = await engine.execute({"dimensions": ['"sales.orders".unit_price']})
    assert sorted(r["sales.orders.unit_price"] for r in response.data) == [1, 2]


async def metric_name_space(base: Path) -> None:
    doc = orders_doc(field("unit_price", datatype="Integer"), metrics=[metric("total price", "SUM(unit_price)")])
    async with sql_engine(base, sql=ORDERS_SQL, documents=[doc]) as engine:
        response = await strict(engine, {"metrics": ['orders."total price"']})
    assert response.data == [{"total price": 3}]


async def quoted_names_distinct(base: Path) -> None:
    doc = orders_doc(field("unit price", "unit_price * 2", datatype="Integer"), field("unit_price", datatype="Integer"))
    async with sql_engine(base, sql=ORDERS_SQL, documents=[doc]) as engine:
        response = await engine.execute({"dimensions": ['orders."unit price"', "orders.unit_price"]})
    assert sorted((r["orders.unit price"], r["orders.unit_price"]) for r in response.data) == [(2, 1), (4, 2)]


async def delimited_dataset_name(base: Path) -> None:
    doc = orders_doc(field("unit_price", datatype="Integer"), name='"Order Lines"')
    async with sql_engine(base, sql=ORDERS_SQL, documents=[doc]) as engine:
        response = await engine.execute({"dimensions": ['"Order Lines".unit_price']})
    assert sorted(r["Order Lines.unit_price"] for r in response.data) == [1, 2]


async def quoted_column_expression(base: Path) -> None:
    doc = orders_doc(field("order_amount", '"Order Amount"', datatype="Float"))
    async with sql_engine(base, sql=ORDERS_SQL, documents=[doc]) as engine:
        response = await engine.execute({"metrics": [{"metric": "sum(orders.order_amount)", "alias": "s"}]})
    assert response.data == [{"s": 4.0}]


async def decimal_exact(base: Path) -> None:
    sql = ("CREATE TABLE big (id VARCHAR, amount DECIMAL(38,2));"
           "INSERT INTO big VALUES ('A', 12345678901234567.89), ('B', 0.01);")
    doc = document_text(name="m", datasets=[dataset("big", primary_key=["id"], fields=[
        field("id"), field("amount", datatype="Decimal")], metrics=[metric("total", "SUM(amount)")])])
    async with sql_engine(base, sql=sql, documents=[doc]) as engine:
        response = await engine.execute({"population": "big", "metrics": ["big.total"]})
    assert response.data == [{"big.total": Decimal("12345678901234567.90")}]


async def time_dimension(base: Path) -> None:
    async with sql_engine(base, sql=TYPES_SQL, documents=[TYPES_DOC]) as engine:
        response = await engine.execute({"population": "t", "dimensions": ["t.tod"],
                                         "metrics": [{"metric": "count(t.*)", "alias": "n"}]})
    assert sorted((str(r["t.tod"]), r["n"]) for r in response.data) == [("09:30:00", 2), ("17:00:00", 1)]


async def time_aggregate(base: Path) -> None:
    async with sql_engine(base, sql=TYPES_SQL, documents=[TYPES_DOC]) as engine:
        response = await engine.execute({"population": "t", "metrics": [{"metric": "min(t.tod)", "alias": "v"}]})
    assert str(response.data[0]["v"]) == "09:30:00"


async def datetimetz_truncation(base: Path) -> None:
    async with sql_engine(base, sql=TYPES_SQL, documents=[TYPES_DOC]) as engine:
        response = await engine.execute({"population": "t", "time_dimensions": [{"field": "t.ts", "granularity": "day"}],
                                         "metrics": [{"metric": "count(t.*)", "alias": "n"}]})
    assert response.data
    assert all(r["t.ts"].tzinfo is not None for r in response.data)


async def datetimetz_offset_literal(base: Path) -> None:
    async with sql_engine(base, sql=TYPES_SQL, documents=[TYPES_DOC]) as engine:
        response = await engine.execute({"population": "t", "metrics": [{"metric": "count(t.*)", "alias": "n"}],
                                         "filter": "t.ts < '2026-01-02 00:00:00+00'"})
    assert response.data == [{"n": 2}]


def _format(case: str, probe: Probe, raises: type[BaseException]) -> Gap:
    return Gap(case=case, issue=FORMAT, label=probe.__name__, database="duckdb", raises=raises, probe=probe)


FORMAT_GAPS: tuple[Gap, ...] = (
    _format("format-relationship-ai-context", relationship_ai_context, AssertionError),
    _format("format-is-time-role", is_time_role, AssertionError),
    _format("format-is-time-axis", is_time_false_axis, ValueError),
    _format("format-metric-dialect", metric_dialect_entry, AssertionError),
    _format("format-field-name-dot", field_name_dot, OssieQueryError),
    _format("format-dataset-name-dot", dataset_name_dot, UnknownReferenceError),
    _format("format-metric-name-punctuation", metric_name_space, OssieQueryError),
    _format("format-quoted-names-distinct", quoted_names_distinct, DuplicateMetricNameError),
    _format("format-delimited-dataset-name", delimited_dataset_name, UnknownReferenceError),
    _format("format-quoted-column-expression", quoted_column_expression, RelationshipKeyError),
    _format("format-decimal-exact", decimal_exact, AssertionError),
    _format("format-time-dimension", time_dimension, DimensionTypeError),
    _format("format-time-aggregate", time_aggregate, AggregationNotAllowedError),
    _format("format-datetimetz-truncation", datetimetz_truncation, AssertionError),
    _format("format-datetimetz-offset-literal", datetimetz_offset_literal, TimeLiteralError),
)

GAPS: tuple[Gap, ...] = FORMAT_GAPS
