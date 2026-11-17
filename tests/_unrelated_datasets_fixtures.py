"""Unrelated-dataset seeds, engines, row helpers and the emitted-SQL product-hop checker.

Main seed (datasource ``test``), with no join path between ``customers`` and ``events``::

    customers (id, city, tenant): 1 A t1 | 2 A t1 | 3 B t2
    events    (id, amount, created_at, kind, user_id, tenant):
        1 10 2024-01-01 x 1 t1 | 2 20 2024-01-02 y 1 t1 | 3 30 2024-02-01 x 2 t1
        4 40 2024-02-02 x 2 t2 | 5 50 2024-03-01 y 3 t2
    users     (id, name, org_id, tenant): 1 u1 1 t1 | 2 u2 1 t1 | 3 u3 2 t2
    orgs      (id, name, tenant): 1 o1 t1 | 2 o2 t2

``events → users → orgs`` many-to-one; ``events.amount_x2`` is ``amount * 2``.

Wide seed (datasource ``wide``): ``customers`` (200 rows over 7 cities) and the unrelated
``big`` (1,000 rows), ``small`` (3 rows) and ``empty`` (0 rows), each ``(id, v)``.
"""

from __future__ import annotations

import contextlib
import warnings
from collections.abc import AsyncGenerator, Iterable
from typing import Any, Optional

import sqlglot
from sqlglot import exp
from sqlglot.optimizer.scope import Scope, build_scope

from slayer.core.enums import DataType, RelationshipCardinality
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.core.warnings import UnrelatedDatasetWarningPayload
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse

from tests._dev1737_fixtures import TableSpec, seed_backend
from tests._engine_helpers import seeded_exec_engine

BACKENDS = ("sqlite", "duckdb")

CUSTOMERS = TableSpec(
    name="customers",
    columns=[("id", "INT"), ("city", "TEXT"), ("tenant", "TEXT")],
    rows=[(1, "A", "t1"), (2, "A", "t1"), (3, "B", "t2")],
)
EVENT_ROWS: list[tuple] = [
    (1, 10.0, "2024-01-01", "x", 1, "t1"), (2, 20.0, "2024-01-02", "y", 1, "t1"),
    (3, 30.0, "2024-02-01", "x", 2, "t1"), (4, 40.0, "2024-02-02", "x", 2, "t2"),
    (5, 50.0, "2024-03-01", "y", 3, "t2"),
]
USERS = TableSpec(
    name="users",
    columns=[("id", "INT"), ("name", "TEXT"), ("org_id", "INT"), ("tenant", "TEXT")],
    rows=[(1, "u1", 1, "t1"), (2, "u2", 1, "t1"), (3, "u3", 2, "t2")],
)
ORGS = TableSpec(
    name="orgs", columns=[("id", "INT"), ("name", "TEXT"), ("tenant", "TEXT")],
    rows=[(1, "o1", "t1"), (2, "o2", "t2")],
)


def events_table(rows: Optional[list[tuple]] = None) -> TableSpec:
    return TableSpec(
        name="events",
        columns=[
            ("id", "INT"), ("amount", "DOUBLE"), ("created_at", "DATE"), ("kind", "TEXT"),
            ("user_id", "INT"), ("tenant", "TEXT"),
        ],
        rows=EVENT_ROWS if rows is None else rows,
    )


def main_tables(*, event_rows: Optional[list[tuple]] = None) -> list[TableSpec]:
    return [CUSTOMERS, events_table(event_rows), USERS, ORGS]


def _many_to_one(target: str, column: str) -> DatasetRelationship:
    return DatasetRelationship(to=target, join_pairs=[[column, "id"]], cardinality=RelationshipCardinality.MANY_TO_ONE)


def customers_dataset(**extra: Any) -> SlayerDataset:
    columns = [
        DatasetField(name="id", datatype=DataType.INT, primary_key=True),
        DatasetField(name="city", datatype=DataType.TEXT),
        DatasetField(name="tenant", datatype=DataType.TEXT),
        *extra.pop("columns", []),
    ]
    return SlayerDataset(name="customers", sql_table="customers", data_source="test", fields=columns, **extra)


def events_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="events", sql_table="events", data_source="test", default_time_dimension="created_at",
        fields=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="amount", datatype=DataType.DOUBLE),
            DatasetField(name="amount_x2", datatype=DataType.DOUBLE, sql="amount * 2"),
            DatasetField(name="created_at", datatype=DataType.DATE),
            DatasetField(name="kind", datatype=DataType.TEXT),
            DatasetField(name="user_id", datatype=DataType.INT),
            DatasetField(name="tenant", datatype=DataType.TEXT),
        ],
        relationships=[_many_to_one("users", "user_id")],
    )


def users_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="users", sql_table="users", data_source="test",
        fields=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="name", datatype=DataType.TEXT),
            DatasetField(name="org_id", datatype=DataType.INT),
            DatasetField(name="tenant", datatype=DataType.TEXT),
        ],
        relationships=[_many_to_one("orgs", "org_id")],
    )


def orgs_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="orgs", sql_table="orgs", data_source="test",
        fields=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="name", datatype=DataType.TEXT),
            DatasetField(name="tenant", datatype=DataType.TEXT),
        ],
    )


def main_datasets() -> list[SlayerDataset]:
    return [customers_dataset(), events_dataset(), users_dataset(), orgs_dataset()]


@contextlib.asynccontextmanager
async def main_engine(
    backend: str, *, datasets: Optional[list[SlayerDataset]] = None, event_rows: Optional[list[tuple]] = None,
) -> AsyncGenerator[SlayerQueryEngine]:
    """Executing engine over the main seed."""
    tables = main_tables(event_rows=event_rows)
    async with seeded_exec_engine(
        dialect=backend, seed=lambda p: seed_backend(backend, p, tables),
        datasets=main_datasets() if datasets is None else datasets,
    ) as (engine, _db):
        yield engine


# ---------------------------------------------------------------------------
# Wide seed: three unrelated datasets of very different sizes
# ---------------------------------------------------------------------------

WIDE_CITIES = ("c0", "c1", "c2", "c3", "c4", "c5", "c6")
WIDE_SIZES = {"big": 1000, "small": 3, "empty": 0}
WIDE_DATASETS = tuple(WIDE_SIZES)


def _wide_tables() -> list[TableSpec]:
    customers = TableSpec(
        name="customers", columns=[("id", "INT"), ("city", "TEXT")],
        rows=[(i, WIDE_CITIES[i % len(WIDE_CITIES)]) for i in range(1, 201)],
    )
    return [customers, *(
        TableSpec(name=name, columns=[("id", "INT"), ("v", "DOUBLE")],
                  rows=[(i, float(i * i % 997 + 1)) for i in range(1, size + 1)])
        for name, size in WIDE_SIZES.items()
    )]


def _wide_datasets() -> list[SlayerDataset]:
    def flat(name: str, *columns: DatasetField) -> SlayerDataset:
        return SlayerDataset(
            name=name, sql_table=name, data_source="wide",
            fields=[DatasetField(name="id", datatype=DataType.INT, primary_key=True), *columns],
        )

    return [
        flat("customers", DatasetField(name="city", datatype=DataType.TEXT)),
        *(flat(name, DatasetField(name="v", datatype=DataType.DOUBLE)) for name in WIDE_DATASETS),
    ]


@contextlib.asynccontextmanager
async def wide_engine(backend: str) -> AsyncGenerator[SlayerQueryEngine]:
    tables = _wide_tables()
    async with seeded_exec_engine(
        dialect=backend, seed=lambda p: seed_backend(backend, p, tables), datasets=_wide_datasets(), datasource="wide",
    ) as (engine, _db):
        yield engine


def wide_metrics() -> list[dict[str, str]]:
    """Two aggregates per unrelated dataset, three cross-dataset expressions, and ``count(*)``."""
    out = [m("count(*)", "n_customers")]
    for name in WIDE_DATASETS:
        out += [m(f"sum({name}.v)", f"{name}_sum"), m(f"count({name}.id)", f"{name}_n")]
    out += [
        m("sum(big.v) + sum(small.v)", "big_plus_small"),
        m("count(big.id) + count(small.id) + count(empty.id)", "all_rows"),
        m("count(small.id) / count(*)", "small_per_customer"),
    ]
    return out


# ---------------------------------------------------------------------------
# Queries, rows, warnings
# ---------------------------------------------------------------------------

def m(expression: str, name: str) -> dict[str, str]:
    return {"formula": expression, "name": name}


RATIO = m("count(events.id) / count(*)", "events_per_customer")
GROUPED = [m("count(*)", "n_customers"), m("count(events.id)", "n_events")]


def query(**fields: Any) -> SlayerQuery:
    return SlayerQuery.model_validate({"population": "customers", **fields})


def ratio_query(**extra: Any) -> SlayerQuery:
    return query(measures=[RATIO], **extra)


def grouped_query(**extra: Any) -> SlayerQuery:
    return query(**{"dimensions": ["city"], "metrics": GROUPED, **extra})


def stages(*raw: dict[str, Any]) -> list[SlayerQuery]:
    return [SlayerQuery.model_validate(s) for s in raw]


def key_of(row: dict[str, Any], name: str) -> str:
    """The unique result key naming ``name`` (a bare name or its dotted tail)."""
    hits = [k for k in row if k == name or k.endswith(f".{name}")]
    assert len(hits) == 1, (name, list(row))
    return hits[0]


def value(row: dict[str, Any], name: str) -> Any:
    v = row[key_of(row, name)]
    return float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else v


def rows(resp: SlayerResponse, *names: str, by: str = "city") -> dict[Any, tuple]:
    """``{by value: (values of names...)}``; asserts the ``by`` values are unique."""
    out: dict[Any, tuple] = {}
    for row in resp.data:
        k = value(row, by)
        assert k not in out, k
        out[k] = tuple(value(row, n) for n in names)
    return out


def only_row(resp: SlayerResponse, *names: str) -> tuple:
    assert len(resp.data) == 1, resp.data
    return tuple(value(resp.data[0], n) for n in names)


def warnings_of(resp: SlayerResponse, kind: str) -> list:
    return [w for w in (resp.warnings or []) if getattr(w, "kind", None) == kind]


def unrelated_warnings(resp: SlayerResponse) -> list[UnrelatedDatasetWarningPayload]:
    return [w for w in resp.warnings or [] if isinstance(w, UnrelatedDatasetWarningPayload)]


async def execute_capturing(engine: SlayerQueryEngine, q: Any) -> tuple[SlayerResponse, list[warnings.WarningMessage]]:
    """Execute ``q``; the response and every Python warning raised meanwhile."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        resp = await engine.execute(q)
    return resp, list(caught)


# ---------------------------------------------------------------------------
# Emitted-SQL checker: unrelated tables only in their own producers and uncorrelated EXISTS
# ---------------------------------------------------------------------------

def _aggregates(scope: Scope) -> bool:
    select = scope.expression
    if not isinstance(select, exp.Select):
        return False
    if select.args.get("group"):
        return True
    return any(
        agg.find_ancestor(exp.Window) is None
        for projection in select.expressions for agg in projection.find_all(exp.AggFunc)
    )


def _row_inputs(scope: Scope, physical: frozenset[str], memo: dict[int, frozenset[str]]) -> frozenset[str]:
    """The physical tables whose rows reach ``scope``'s rows unaggregated."""
    if id(scope) in memo:
        return memo[id(scope)]
    found: set[str] = set()
    for source in scope.sources.values():
        if isinstance(source, Scope):
            if not _aggregates(source):
                found |= _row_inputs(source, physical, memo)
        elif isinstance(source, exp.Table) and source.name in physical:
            found.add(source.name)
    for union_branch in scope.union_scopes:
        found |= _row_inputs(union_branch, physical, memo)
    memo[id(scope)] = frozenset(found)
    return memo[id(scope)]


def _correlated(scope: Scope) -> bool:
    """Some qualified column names a table outside ``scope`` (unqualified ones are its own)."""
    return any(col.table and col.table not in scope.sources for col in scope.columns)


def _nested_scopes(scope: Scope) -> Iterable[Scope]:
    for child in (*scope.subquery_scopes, *scope.derived_table_scopes, *scope.cte_scopes, *scope.union_scopes):
        yield child
        yield from _nested_scopes(child)


def product_hop_violations(
    sql: Optional[str], *, dialect: str, unrelated: Iterable[str], physical: Iterable[str],
) -> list[str]:
    """Every place ``sql`` pairs an unrelated table's rows with other rows or correlates across it."""
    assert sql is not None
    unrelated, physical = frozenset(unrelated), frozenset(physical) | frozenset(unrelated)
    root = build_scope(sqlglot.parse_one(sql, dialect=dialect))
    assert root is not None, sql
    scopes = [root, *_nested_scopes(root)]
    memo: dict[int, frozenset[str]] = {}
    out: list[str] = []
    seen: set[str] = set()
    for scope in scopes:
        inputs = _row_inputs(scope, physical, memo)
        seen |= inputs & unrelated
        for table in inputs & unrelated:
            if inputs != {table}:
                out.append(f"{table} rows meet {sorted(inputs - {table})}: {scope.expression.sql(dialect=dialect)}")
        if inputs & unrelated:
            correlated = [s for s in (scope, *_nested_scopes(scope)) if _correlated(s)]
            out += [f"correlated beside {sorted(inputs & unrelated)}: {s.expression.sql(dialect=dialect)}"
                    for s in correlated]
        reads_unrelated_inside = any(_row_inputs(s, physical, memo) & unrelated for s in _nested_scopes(scope))
        if reads_unrelated_inside and scope.is_subquery and _correlated(scope):
            out.append(f"correlated subquery reads an unrelated table: {scope.expression.sql(dialect=dialect)}")
    missing = unrelated - seen
    assert not missing, f"{sorted(missing)} never read:\n{sql}"
    return out
