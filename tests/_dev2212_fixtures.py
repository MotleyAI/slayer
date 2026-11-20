"""Seeds, engines and helpers for the root-less row-supply tests (spec: queries/population).

Ossie Layer 3 Appendix B seed, datasource ``test``::

    customers (customer_id, region, segment, tenant):
        C1 EAST retail t1 | C2 EAST corporate t1 | C3 WEST retail t2 | C4 NORTH retail t1
    orders (order_id, customer_id, amount, status):
        O1 C1 100 completed | O2 C1 200 open | O3 C2 50 completed | O4 C3 300 open
    returns (return_id, customer_id, amount): R1 C1 25 | R2 C4 10
    visits (visit_id, customer_id): V1 C4 | V2 C4 | V3 C3
    tickets (ticket_id, priority): T1 high | T2 low | T3 low   (no join path to anything)

``orders``, ``returns`` and ``visits`` join many-to-one to ``customers``. ``orphan`` adds O5 C9 70 open.
``null_returns`` adds C5 SOUTH retail, O6 C5 40 completed and R3 C3 NULL. ``tenant`` is physical only
(no dataset field), for a row-level security policy hiding C3.
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncGenerator
from typing import Any, Optional

import pytest

from slayer.core.enums import DataType, RelationshipCardinality
from slayer.core.models import DatasetField, DatasetMetric, DatasetRelationship, SlayerDataset
from slayer.core.policy import ColumnFilterRuleset, SessionPolicy
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse
from slayer.storage.sqlite_conn import transaction

from tests._dev1737_fixtures import TableSpec, seed_backend
from tests._engine_helpers import seeded_exec_engine

BACKENDS = ("sqlite", "duckdb")
DS = "test"

_CUSTOMER_ROWS = [("C1", "EAST", "retail", "t1"), ("C2", "EAST", "corporate", "t1"),
                  ("C3", "WEST", "retail", "t2"), ("C4", "NORTH", "retail", "t1")]
_ORDER_ROWS = [("O1", "C1", 100.0, "completed", "2025-01-10"), ("O2", "C1", 200.0, "open", "2025-02-10"),
               ("O3", "C2", 50.0, "completed", "2025-01-20"), ("O4", "C3", 300.0, "open", "2025-02-20")]
_RETURN_ROWS = [("R1", "C1", 25.0), ("R2", "C4", 10.0)]


def seed_tables(*, orphan: bool = False, null_returns: bool = False) -> list[TableSpec]:
    customers: list[tuple] = list(_CUSTOMER_ROWS)
    orders: list[tuple] = list(_ORDER_ROWS)
    returns: list[tuple] = list(_RETURN_ROWS)
    if orphan:
        orders.append(("O5", "C9", 70.0, "open", "2025-01-15"))
    if null_returns:
        customers.append(("C5", "SOUTH", "retail", "t1"))
        orders.append(("O6", "C5", 40.0, "completed", "2025-03-05"))
        returns.append(("R3", "C3", None))
    return [
        TableSpec(name="customers", columns=[("customer_id", "TEXT"), ("region", "TEXT"), ("segment", "TEXT"),
                                             ("tenant", "TEXT")], rows=customers),
        TableSpec(name="orders", columns=[("order_id", "TEXT"), ("customer_id", "TEXT"), ("amount", "DOUBLE"),
                                          ("status", "TEXT"), ("ordered_at", "DATE")], rows=orders),
        TableSpec(name="returns", columns=[("return_id", "TEXT"), ("customer_id", "TEXT"), ("amount", "DOUBLE")],
                  rows=returns),
        TableSpec(name="visits", columns=[("visit_id", "TEXT"), ("customer_id", "TEXT")],
                  rows=[("V1", "C4"), ("V2", "C4"), ("V3", "C3")]),
        TableSpec(name="tickets", columns=[("ticket_id", "TEXT"), ("priority", "TEXT")],
                  rows=[("T1", "high"), ("T2", "low"), ("T3", "low")]),
    ]


def _field(name: str, datatype: DataType, *, pk: bool = False) -> DatasetField:
    return DatasetField(name=name, datatype=datatype, primary_key=pk)


def to_customers(*, cardinality: Optional[RelationshipCardinality] = RelationshipCardinality.MANY_TO_ONE,
                 ) -> DatasetRelationship:
    return DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["customer_id"],
                               cardinality=cardinality)


def customers_dataset(*, keyed: bool = True, **extra: Any) -> SlayerDataset:
    """``keyed=False`` declares no key on ``customer_id``."""
    return SlayerDataset(
        name="customers", sql_table="customers", data_source=DS,
        fields=[_field("customer_id", DataType.STRING, pk=keyed), _field("region", DataType.STRING),
                _field("segment", DataType.STRING)],
        metrics=[DatasetMetric(name="customer_count", expression="count(*)")],
        **extra,
    )


def orders_dataset(*, relationship: Optional[DatasetRelationship] = None, **extra: Any) -> SlayerDataset:
    return SlayerDataset(
        name="orders", sql_table="orders", data_source=DS,
        fields=[_field("order_id", DataType.STRING, pk=True), _field("customer_id", DataType.STRING),
                _field("amount", DataType.FLOAT), _field("status", DataType.STRING),
                _field("ordered_at", DataType.DATE)],
        relationships=[relationship or to_customers()],
        metrics=[DatasetMetric(name="total_revenue", expression="sum(amount)")],
        **extra,
    )


def returns_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="returns", sql_table="returns", data_source=DS,
        fields=[_field("return_id", DataType.STRING, pk=True), _field("customer_id", DataType.STRING),
                _field("amount", DataType.FLOAT)],
        relationships=[to_customers()],
        metrics=[DatasetMetric(name="total_returns", expression="sum(amount)")],
    )


def visits_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="visits", sql_table="visits", data_source=DS,
        fields=[_field("visit_id", DataType.STRING, pk=True), _field("customer_id", DataType.STRING)],
        relationships=[to_customers()],
        metrics=[DatasetMetric(name="visit_count", expression="count(*)")],
    )


def tickets_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="tickets", sql_table="tickets", data_source=DS,
        fields=[_field("ticket_id", DataType.STRING, pk=True), _field("priority", DataType.STRING)],
        metrics=[DatasetMetric(name="ticket_count", expression="count(*)")],
    )


def seed_datasets(*, customers: Optional[SlayerDataset] = None, orders: Optional[SlayerDataset] = None,
                  extra: tuple[SlayerDataset, ...] = ()) -> list[SlayerDataset]:
    return [customers or customers_dataset(), orders or orders_dataset(), returns_dataset(), visits_dataset(),
            tickets_dataset(), *extra]


#: Keyless ``notes`` (no primary or unique key): C1 5 | C3 6 | C9 7 (orphan).
NOTES = TableSpec(name="notes", columns=[("customer_id", "TEXT"), ("amount", "DOUBLE")],
                  rows=[("C1", 5.0), ("C3", 6.0), ("C9", 7.0)])


def notes_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="notes", sql_table="notes", data_source=DS,
        fields=[_field("customer_id", DataType.STRING), _field("amount", DataType.FLOAT)],
        relationships=[to_customers()],
    )


#: DuckDB only: ``baskets`` with a list column, B1 C1 ['a', 'b'] | B2 C9 ['c'] (orphan).
BASKETS_STATEMENTS = (
    "CREATE TABLE baskets (basket_id VARCHAR, customer_id VARCHAR, items VARCHAR[])",
    "INSERT INTO baskets VALUES ('B1', 'C1', ['a', 'b']), ('B2', 'C9', ['c'])",
)


def baskets_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="baskets", sql_table="baskets", data_source=DS,
        fields=[_field("basket_id", DataType.STRING, pk=True), _field("customer_id", DataType.STRING),
                DatasetField(name="items", datatype=DataType.ARRAY, element_type=DataType.STRING)],
        relationships=[to_customers()],
    )


def _run_statements(backend: str, db_path: str, statements: tuple[str, ...]) -> None:
    if not statements:
        return
    if backend == "duckdb":
        with contextlib.closing(pytest.importorskip("duckdb").connect(db_path)) as con:
            for stmt in statements:
                con.execute(stmt)
        return
    with transaction(db_path) as con:
        for stmt in statements:
            con.execute(stmt)


@contextlib.asynccontextmanager
async def seeded(backend: str, *, orphan: bool = False, null_returns: bool = False,
                 datasets: Optional[list[SlayerDataset]] = None, extra_tables: tuple[TableSpec, ...] = (),
                 extra_statements: tuple[str, ...] = ()) -> AsyncGenerator[tuple[SlayerQueryEngine, str]]:
    """``(engine, db_path)`` over the Appendix B seed (variants as in the module docstring)."""
    tables = [*seed_tables(orphan=orphan, null_returns=null_returns), *extra_tables]

    def seed(db_path: str) -> None:
        seed_backend(backend, db_path, tables)
        _run_statements(backend, db_path, extra_statements)

    async with seeded_exec_engine(
        dialect=backend, seed=seed, datasets=datasets if datasets is not None else seed_datasets(),
    ) as (engine, db_path):
        yield engine, db_path


@contextlib.asynccontextmanager
async def seed_engine(backend: str, **kw: Any) -> AsyncGenerator[SlayerQueryEngine]:
    """Executing engine over the Appendix B seed (``seeded``'s keywords)."""
    async with seeded(backend, **kw) as (engine, _):
        yield engine


#: A policy hiding C3 (``tenant = 't2'``); tables without ``tenant`` pass unfiltered.
HIDE_C3 = SessionPolicy(ruleset=ColumnFilterRuleset(column="tenant", value="t1", on_unapplicable="pass"))


@contextlib.asynccontextmanager
async def with_policy(engine: SlayerQueryEngine, policy: SessionPolicy) -> AsyncGenerator[SlayerQueryEngine]:
    """``engine``'s store under ``policy``."""
    eng = SlayerQueryEngine(storage=engine.storage, policy=policy)
    try:
        yield eng
    finally:
        await eng.aclose()
        eng.close()


def q(**kw: Any) -> SlayerQuery:
    return SlayerQuery.model_validate(kw)


async def run(engine: SlayerQueryEngine, **kw: Any) -> SlayerResponse:
    return await engine.execute(q(**kw))


# --------------------------------------------------------------------------- #
# Results and warnings
# --------------------------------------------------------------------------- #
def _norm(v: Any) -> Any:
    return float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else v


def table(resp: SlayerResponse, *keys: str) -> list[tuple]:
    """The rows of ``resp`` projected on exact result ``keys`` (numbers as floats), sorted; duplicates kept."""
    return sorted((tuple(_norm(r[k]) for k in keys) for r in resp.data), key=repr)


def rows(*tuples: tuple) -> list[tuple]:
    """Expected rows in ``table``'s order."""
    return sorted((tuple(_norm(v) for v in t) for t in tuples), key=repr)


def kinds(resp: SlayerResponse, kind: str) -> list:
    return [w for w in resp.warnings or [] if getattr(w, "kind", None) == kind]


def warned_metrics(resp: SlayerResponse, kind: str) -> set[str]:
    """Every metric text a ``kind`` warning names (its ``metrics`` / ``metric`` payload)."""
    out: set[str] = set()
    for w in kinds(resp, kind):
        for attr in ("metrics", "metric", "measures", "measure"):
            v = getattr(w, attr, None)
            if isinstance(v, str):
                out.add(v)
            elif isinstance(v, (list, tuple)):
                out.update(str(x) for x in v)
    return out
