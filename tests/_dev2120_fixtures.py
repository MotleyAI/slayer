"""Shared fixtures for list columns: the ``contractors`` table, its list-function case table, seeding.

Expected values are the Postgres meaning (spec: queries/list-functions).
"""

from __future__ import annotations

import contextlib
import json
import math
from collections.abc import AsyncGenerator, Callable, Iterable
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Optional

import pytest
from pydantic import BaseModel, ConfigDict

from slayer.core.enums import DataType
from slayer.core.errors import SlayerError
from slayer.core.models import Column, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse
from slayer.storage.sqlite_conn import transaction
from tests._dev1737_fixtures import as_temporal
from tests._engine_helpers import _engine_generate, seeded_exec_engine

TIER1 = ("sqlite", "postgres", "duckdb", "mysql", "clickhouse", "tsql", "trino", "snowflake", "bigquery")
EXEC_BACKENDS = ("sqlite", "duckdb")


async def emit(query: SlayerQuery, *, dialect: str, models: Iterable[SlayerModel],
               datasource_fields: dict | None = None) -> str:
    """Dry-run SQL for ``query`` over ``models`` on ``dialect`` (asserts it parses there)."""
    first, *rest = list(models)
    return await _engine_generate(query=query, model=first, extra_models=rest, dialect=dialect,
                                  datasource_fields=datasource_fields)


# --------------------------------------------------------------------------- #
# The case table
# --------------------------------------------------------------------------- #
class Case(BaseModel):
    """One executed case: a row value per id, or the ids a filter keeps."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)
    case_id: str
    covers: tuple[str, ...]
    expr: str
    values: Optional[dict[int, Any]] = None
    keeps: Optional[frozenset[int]] = None
    variables: Optional[dict[str, Any]] = None
    model: str = "contractors"

    def query(self) -> SlayerQuery:
        body: dict[str, Any] = {"source_model": self.model, "distinct_dimension_values": False}
        if self.keeps is not None:
            body |= {"dimensions": ["id"], "filters": [self.expr]}
        else:
            body |= {"dimensions": ["id", {"expression": self.expr, "name": "v"}]}
        if self.variables:
            body["variables"] = self.variables
        return SlayerQuery.model_validate(body)


def assert_cell(actual: Any, expected: Any, *, where: str) -> None:
    if expected is None:
        assert actual is None, f"{where}: expected NULL, got {actual!r}"
        return
    assert actual is not None, f"{where}: expected {expected!r}, got NULL"
    if isinstance(expected, bool):
        assert actual in (True, False, 0, 1), f"{where}: expected a boolean, got {actual!r}"
        assert bool(actual) is expected, f"{where}: expected {expected}, got {actual!r}"
    elif isinstance(expected, (date, datetime)):
        got = as_temporal(actual)
        got_day = got.date() if isinstance(got, datetime) and not isinstance(expected, datetime) else got
        assert got_day == expected, f"{where}: expected {expected}, got {actual!r}"
    elif isinstance(expected, str):
        assert actual == expected, f"{where}: expected {expected!r}, got {actual!r}"
    elif isinstance(expected, int):
        assert not isinstance(actual, (str, bool)), f"{where}: expected a number, got {actual!r}"
        assert Decimal(str(actual)) == Decimal(expected), f"{where}: expected {expected}, got {actual!r}"
    else:
        assert not isinstance(actual, (str, bool)), f"{where}: expected a number, got {actual!r}"
        assert math.isclose(float(actual), float(expected), rel_tol=1e-9, abs_tol=1e-12), (
            f"{where}: expected {expected}, got {actual!r}")


def assert_case(data: list[dict[str, Any]], case: Case) -> None:
    if case.keeps is not None:
        got = {int(r[f"{case.model}.id"]) for r in data}
        assert got == set(case.keeps), f"{case.case_id}: kept {sorted(got)}, expected {sorted(case.keeps)}"
        return
    assert case.values is not None
    got = {int(r[f"{case.model}.id"]): r[f"{case.model}.v"] for r in data}
    assert set(got) == set(case.values), f"{case.case_id}: row ids {sorted(got)}"
    for rid, expected in case.values.items():
        assert_cell(got[rid], expected, where=f"{case.case_id} id={rid}")


async def run_case(engine: SlayerQueryEngine, case: Case) -> None:
    resp = await engine.execute(case.query())
    try:
        assert_case(resp.data, case)
    except AssertionError as exc:
        raise AssertionError(f"{exc}\nSQL:\n{resp.sql}") from None


async def check_library(engine: SlayerQueryEngine, *, cases: list[Case], gated: frozenset[str] = frozenset(),
                        refusal: str = "") -> None:
    """Every case on a live engine; a case covering a ``gated`` name must be refused naming ``refusal``.

    Failures are collected so one run reports them all.
    """
    failures: list[str] = []
    for case in cases:
        try:
            if gated & set(case.covers):
                problem = await _refusal_problem(engine, case, refusal=refusal)
            else:
                await run_case(engine, case)
                problem = None
        except Exception as exc:  # noqa: BLE001 — report every failing case, not the first
            problem = f"{type(exc).__name__}: {exc}"
        if problem:
            failures.append(f"{case.case_id}: {problem}")
    assert not failures, "\n\n".join(failures)


async def _refusal_problem(engine: SlayerQueryEngine, case: Case, *, refusal: str) -> str | None:
    try:
        await engine.execute(case.query())
    except SlayerError as exc:
        return None if refusal in str(exc) else f"refusal without {refusal!r}: {exc}"
    return f"expected a plan-time refusal naming {refusal}"


# --------------------------------------------------------------------------- #
# Server literals
# --------------------------------------------------------------------------- #
_TEMPORAL_PREFIX = {"trino": "TIMESTAMP", "bigquery": "DATETIME"}
_SUFFIX = {"clickhouse": " ENGINE = MergeTree ORDER BY tuple()"}


def server_literal(value: Any, *, kind: str, dialect: str) -> str:
    """``value`` as a SQL literal on ``dialect`` (backslash-escaping engines and N'' strings handled)."""
    if value is None:
        return "NULL"
    if isinstance(value, (int, float)):
        return repr(value)
    text = str(value)
    if dialect in ("mysql", "clickhouse", "snowflake", "bigquery"):
        text = text.replace("\\", "\\\\")
    if dialect == "bigquery":
        text = text.replace("'", "\\'").replace("\n", "\\n")
    else:
        text = text.replace("'", "''")
    if kind == "TIMESTAMP" and dialect in _TEMPORAL_PREFIX:
        return f"{_TEMPORAL_PREFIX[dialect]} '{text}'"
    return f"N'{text}'" if dialect == "tsql" else f"'{text}'"


# --------------------------------------------------------------------------- #
# List columns: the ``contractors`` table and its list-function cases
# --------------------------------------------------------------------------- #
LIST_FUNCTIONS = frozenset({"contains", "contains_any", "contains_all", "length"})
NATIVE_LIST_DIALECTS = ("postgres", "duckdb", "clickhouse", "trino", "snowflake", "bigquery")
#: Dialects without native lists → the database name their refusal carries.
NO_LIST_DATABASES = {"sqlite": "SQLite", "mysql": "MySQL", "tsql": "SQL Server"}

#: ``(name, kind)``; ``T[]`` is a list of ``T``.
CONTRACTOR_COLUMNS: tuple[tuple[str, str], ...] = (
    ("id", "INT"), ("name", "TEXT"), ("team", "TEXT"), ("rate", "INT"), ("city", "TEXT"), ("pick", "TEXT"),
    ("skills", "TEXT[]"), ("required", "TEXT[]"), ("scores", "INT[]"), ("codes", "TEXT[]"),
)
CONTRACTORS: tuple[dict[str, Any], ...] = (
    {"id": 1, "name": "Ann", "team": "A", "rate": 10, "city": "Oslo", "pick": "sql",
     "skills": ["go", "sql", "go"], "required": ["rust", "sql"], "scores": [3, 9], "codes": ["3", "x"]},
    {"id": 2, "name": "Bob", "team": "A", "rate": 20, "city": None, "pick": None,
     "skills": ["sql", None], "required": ["go"], "scores": [1], "codes": ["1", "4"]},
    {"id": 3, "name": "Cid", "team": "B", "rate": 30, "city": "Rome", "pick": "go",
     "skills": [], "required": [], "scores": [], "codes": []},
    {"id": 4, "name": "Dee", "team": "B", "rate": 40, "city": "Oslo", "pick": "go",
     "skills": None, "required": ["go"], "scores": None, "codes": ["2"]},
    {"id": 5, "name": "Eve", "team": "B", "rate": 50, "city": "Oslo", "pick": "rust",
     "skills": ["go"], "required": None, "scores": [5, None], "codes": ["6", None]},
)
LIST_COLUMNS = tuple(c for c, k in CONTRACTOR_COLUMNS if k.endswith("[]"))


class ListStorage(BaseModel):
    """What a backend can store: some keep a NULL list as ``[]``, some keep no NULL element."""

    model_config = ConfigDict(frozen=True)
    null_list_is_empty: bool = False
    drops_null_elements: bool = False


LIST_STORAGE: dict[str, ListStorage] = {
    "bigquery": ListStorage(null_list_is_empty=True, drops_null_elements=True),
    "clickhouse": ListStorage(null_list_is_empty=True),
    "snowflake": ListStorage(drops_null_elements=True),
}


def stored_contractors(storage: ListStorage = ListStorage()) -> list[dict[str, Any]]:
    """``CONTRACTORS`` as ``storage`` holds them."""
    def store(value: Optional[list[Any]]) -> Optional[list[Any]]:
        if value is None:
            return [] if storage.null_list_is_empty else None
        return [e for e in value if e is not None] if storage.drops_null_elements else list(value)
    return [{k: store(v) if k in LIST_COLUMNS else v for k, v in row.items()} for row in CONTRACTORS]


def contractors_model(*, data_source: str = "test", sql_table: str = "contractors", keyed: bool = True) -> SlayerModel:
    scalars = {"INT": DataType.INT, "TEXT": DataType.TEXT}
    columns = [
        Column(name=c, type=DataType.ARRAY, element_type=scalars[k[:-2]]) if k.endswith("[]")
        else Column(name=c, type=scalars[k], primary_key=keyed and c == "id")
        for c, k in CONTRACTOR_COLUMNS
    ]
    return SlayerModel(name="contractors", sql_table=sql_table, data_source=data_source, columns=columns)


# The list functions' meaning: a list's elements are its non-NULL elements.
def elements(value: list[Any]) -> list[Any]:
    return [e for e in value if e is not None]


def list_contains(value: Optional[list[Any]], x: Any) -> Optional[bool]:
    return None if value is None or x is None else x in elements(value)


def list_contains_any(value: Optional[list[Any]], other: Optional[list[Any]]) -> Optional[bool]:
    if value is None or other is None:
        return None
    return bool(set(elements(value)) & set(elements(other)))


def list_contains_all(value: Optional[list[Any]], other: Optional[list[Any]]) -> Optional[bool]:
    if value is None or other is None:
        return None
    return set(elements(other)) <= set(elements(value))


def list_length(value: Optional[list[Any]]) -> Optional[int]:
    return None if value is None else len(elements(value))


def list_cases(storage: ListStorage = ListStorage()) -> list[Case]:
    """The list-function cases over ``contractors`` as ``storage`` holds it."""
    rows = {r["id"]: r for r in stored_contractors(storage)}

    def v(case_id: str, covers: Iterable[str], expr: str, fn: Callable[[dict[str, Any]], Any], **kw: Any) -> Case:
        return Case(case_id=f"list|{case_id}", covers=tuple(covers), expr=expr,
                    values={rid: fn(r) for rid, r in rows.items()}, **kw)

    def f(case_id: str, covers: Iterable[str], expr: str, fn: Callable[[dict[str, Any]], Any], **kw: Any) -> Case:
        return Case(case_id=f"list|{case_id}", covers=tuple(covers), expr=expr,
                    keeps=frozenset(rid for rid, r in rows.items() if fn(r) is True), **kw)

    def length_plus_one(r: dict[str, Any]) -> Optional[int]:
        n = list_length(r["skills"])
        return None if n is None else n + 1

    return [
        v("contains", ["contains"], "contains(skills, 'go')", lambda r: list_contains(r["skills"], "go")),
        v("contains-column-value", ["contains"], "contains(skills, pick)",
          lambda r: list_contains(r["skills"], r["pick"])),
        v("contains-numbers", ["contains"], "contains(scores, 9)", lambda r: list_contains(r["scores"], 9)),
        v("contains-text-literal-on-numbers", ["contains"], "contains(scores, '5')",
          lambda r: list_contains(r["scores"], 5)),
        v("contains-mixed-numbers", ["contains"], "contains(scores, 9.0)",
          lambda r: list_contains(r["scores"], 9)),
        f("contains-filter", ["contains"], "contains(skills, 'go')", lambda r: list_contains(r["skills"], "go")),
        f("not-contains", ["contains"], "NOT contains(skills, 'go')",
          lambda r: list_contains(r["skills"], "go") is False),
        v("contains_any", ["contains_any"], "contains_any(skills, ['go', 'sql'])",
          lambda r: list_contains_any(r["skills"], ["go", "sql"])),
        v("contains_any-numbers", ["contains_any"], "contains_any(scores, [1, 9])",
          lambda r: list_contains_any(r["scores"], [1, 9])),
        v("contains_any-list-column", ["contains_any"], "contains_any(skills, required)",
          lambda r: list_contains_any(r["skills"], r["required"])),
        v("contains_any-mixed-numbers", ["contains_any"], "contains_any(scores, [2.5, 9])",
          lambda r: list_contains_any(r["scores"], [Decimal("2.5"), 9])),
        v("contains_any-numeric-text-literals", ["contains_any"], "contains_any(scores, ['1', '9'])",
          lambda r: list_contains_any(r["scores"], [1, 9])),
        v("contains_any-empty", ["contains_any"], "contains_any(skills, [])",
          lambda r: list_contains_any(r["skills"], [])),
        f("contains_any-variable", ["contains_any"], "contains_any(skills, {wanted})",
          lambda r: list_contains_any(r["skills"], ["rust", "go"]), variables={"wanted": ["rust", "go"]}),
        f("contains_any-empty-variable", ["contains_any"], "contains_any(skills, {wanted})",
          lambda r: list_contains_any(r["skills"], []), variables={"wanted": []}),
        v("contains_all", ["contains_all"], "contains_all(skills, ['go', 'sql'])",
          lambda r: list_contains_all(r["skills"], ["go", "sql"])),
        v("contains_all-null-element", ["contains_all"], "contains_all(skills, ['sql'])",
          lambda r: list_contains_all(r["skills"], ["sql"])),
        v("contains_all-list-column", ["contains_all"], "contains_all(skills, required)",
          lambda r: list_contains_all(r["skills"], r["required"])),
        v("contains_all-mixed-numbers", ["contains_all"], "contains_all(scores, [3, 2.5])",
          lambda r: list_contains_all(r["scores"], [3, Decimal("2.5")])),
        v("contains_all-empty", ["contains_all"], "contains_all(skills, [])",
          lambda r: list_contains_all(r["skills"], [])),
        f("contains_all-variable", ["contains_all"], "contains_all(skills, {wanted})",
          lambda r: list_contains_all(r["skills"], ["go", "sql"]), variables={"wanted": ["go", "sql"]}),
        f("contains_all-empty-variable", ["contains_all"], "contains_all(skills, {wanted})",
          lambda r: list_contains_all(r["skills"], []), variables={"wanted": []}),
        v("length", ["length"], "length(skills)", lambda r: list_length(r["skills"])),
        v("length-null-elements", ["length"], "length(scores)", lambda r: list_length(r["scores"])),
        v("length-arithmetic", ["length", "+"], "length(skills) + 1", length_plus_one),
        v("length-text-unchanged", ["length"], "length(name)", lambda r: len(r["name"])),
        f("is-null", ["IS NULL"], "skills IS NULL", lambda r: r["skills"] is None),
        f("is-not-null", ["IS NULL"], "skills IS NOT NULL", lambda r: r["skills"] is not None),
    ]


def seed_contractors(backend: str, db_path: str, *, extra: Iterable[str] = ()) -> None:
    """``contractors`` on an in-process backend (SQLite holds lists as JSON text), then the ``extra`` statements."""
    names = ", ".join(c for c, _ in CONTRACTOR_COLUMNS)
    marks = ", ".join("?" for _ in CONTRACTOR_COLUMNS)
    if backend == "sqlite":
        cols = ", ".join(f"{c} {'INTEGER' if k == 'INT' else 'TEXT'}" for c, k in CONTRACTOR_COLUMNS)
        rows = [tuple(json.dumps(r[c]) if c in LIST_COLUMNS and r[c] is not None else r[c] for c, _ in
                      CONTRACTOR_COLUMNS) for r in CONTRACTORS]
        with transaction(db_path) as con:
            con.execute(f"CREATE TABLE contractors ({cols})")
            con.executemany(f"INSERT INTO contractors ({names}) VALUES ({marks})", rows)
            for stmt in extra:
                con.execute(stmt)
        return
    duckdb = pytest.importorskip("duckdb")
    types = {"INT": "INTEGER", "TEXT": "VARCHAR", "TEXT[]": "VARCHAR[]", "INT[]": "INTEGER[]"}
    cols = ", ".join(f"{c} {types[k]}" for c, k in CONTRACTOR_COLUMNS)
    with contextlib.closing(duckdb.connect(db_path)) as con:
        con.execute(f"CREATE TABLE contractors ({cols})")
        con.executemany(f"INSERT INTO contractors ({names}) VALUES ({marks})",
                        [tuple(r[c] for c, _ in CONTRACTOR_COLUMNS) for r in CONTRACTORS])
        for stmt in extra:
            con.execute(stmt)


@contextlib.asynccontextmanager
async def contractors_engine(backend: str = "duckdb", *, models: Optional[list[SlayerModel]] = None,
                             extra: Iterable[str] = ()) -> AsyncGenerator[SlayerQueryEngine]:
    async with seeded_exec_engine(
        dialect=backend, seed=lambda p: seed_contractors(backend, p, extra=extra),
        models=models or [contractors_model()],
    ) as (engine, _db):
        yield engine


async def emit_contractors(query: SlayerQuery, *, dialect: str, models: Optional[list[SlayerModel]] = None) -> str:
    """Dry-run SQL for a ``contractors`` query on ``dialect`` (asserts it parses there)."""
    return await emit(query, dialect=dialect, models=models or [contractors_model()])


def contractor_values(resp: SlayerResponse, name: str = "v") -> dict[int, Any]:
    return {int(r["contractors.id"]): r[f"contractors.{name}"] for r in resp.data}


_SERVER_LIST_TYPES: dict[str, dict[str, str]] = {
    "postgres": {"INT": "INTEGER", "TEXT": "TEXT", "TEXT[]": "TEXT[]", "INT[]": "INTEGER[]"},
    "trino": {"INT": "INTEGER", "TEXT": "VARCHAR", "TEXT[]": "ARRAY(VARCHAR)", "INT[]": "ARRAY(INTEGER)"},
    "clickhouse": {"INT": "Int32", "TEXT": "Nullable(String)", "TEXT[]": "Array(Nullable(String))",
                   "INT[]": "Array(Nullable(Int32))"},
    "bigquery": {"INT": "INT64", "TEXT": "STRING", "TEXT[]": "ARRAY<STRING>", "INT[]": "ARRAY<INT64>"},
    # Untyped ARRAY: the element type is inferred at ingest.
    "snowflake": {"INT": "NUMBER(38,0)", "TEXT": "TEXT", "TEXT[]": "ARRAY", "INT[]": "ARRAY"},
}


def _server_list_literal(value: Optional[list[Any]], *, kind: str, dialect: str) -> str:
    if value is None:
        return "NULL"
    element = kind[:-2]
    items = ", ".join(server_literal(e, kind=element, dialect=dialect) for e in value)
    if dialect == "postgres":
        return f"ARRAY[{items}]::{_SERVER_LIST_TYPES['postgres'][kind]}"
    if dialect == "trino":
        return f"CAST(ARRAY[{items}] AS {_SERVER_LIST_TYPES['trino'][kind]})"
    if dialect == "bigquery":
        return f"{_SERVER_LIST_TYPES['bigquery'][kind]}[{items}]"
    if dialect == "snowflake":
        return f"ARRAY_CONSTRUCT({items})"
    return f"[{items}]"


def contractors_statements(*, dialect: str, prefix: str = "") -> list[str]:
    """CREATE + INSERT of ``contractors`` (as ``dialect`` stores it) on a server backend."""
    types = _SERVER_LIST_TYPES[dialect]
    cols = ", ".join(f"{c} {types[k]}" for c, k in CONTRACTOR_COLUMNS)

    def lit(value: Any, kind: str) -> str:
        if kind.endswith("[]"):
            return _server_list_literal(value, kind=kind, dialect=dialect)
        return server_literal(value, kind=kind, dialect=dialect)

    rows = [[lit(r[c], k) for c, k in CONTRACTOR_COLUMNS]
            for r in stored_contractors(LIST_STORAGE.get(dialect, ListStorage()))]
    name = f"{prefix}contractors"
    if dialect == "snowflake":  # ARRAY_CONSTRUCT is not allowed in VALUES
        body = " UNION ALL ".join("SELECT " + ", ".join(row) for row in rows)
        insert = f"INSERT INTO {name} {body}"
    else:
        insert = f"INSERT INTO {name} VALUES " + ", ".join("(" + ", ".join(row) + ")" for row in rows)
    return [f"CREATE TABLE {name} ({cols}){_SUFFIX.get(dialect, '')}", insert]
