"""The gate's fixtures: one canonical form of every seeded table, emitted per engine, and the query engines over it.

Upstream's wide-table SQL is parsed into tables, column kinds and rows (any statement it cannot classify raises);
the Layer 1 seed and the mixed-case fixture are written in the same form. Each engine gets its own create / insert
statements; a wide-table model's ``source`` names the engine's table for it.
"""

from __future__ import annotations

import contextlib
import datetime
import re
import sqlite3
import uuid
from collections.abc import AsyncGenerator, Generator, Sequence
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal, Optional, Protocol

import duckdb
import sqlglot
from pydantic import BaseModel, ConfigDict
from sqlglot import exp

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, DatasetRelationship, DatasourceConfig, SlayerDataset
from slayer.database import engine_factory
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage
from tests._dev1737_fixtures import TableSpec
from tests._dev2215_fixtures import DS_FIELDS, NOW, TABLES, dev2215_datasets
from tests._dev2217_fixtures import FIXTURES, fixture_texts
from tests._dev2243_fixtures import store_document
from tests._engine_helpers import owned_query_engine
from tests._time_points_fixtures import PinnedClock

Kind = Literal["INT", "DOUBLE", "TEXT", "BOOLEAN", "DATE", "TIMESTAMP", "DECIMAL"]
LAYER1_DS = "test"
WIDE_TABLE_DS = "wide_table"
MIXED_CASE_DS = "mixed"


class Table(BaseModel):
    """A table in canonical form: logical column kinds and Python values."""

    model_config = ConfigDict(frozen=True)

    schema_name: Optional[str] = None
    name: str
    columns: tuple[tuple[str, Kind], ...]
    rows: tuple[tuple[Any, ...], ...] = ()


# --------------------------------------------------------------------------- #
# Canonical form
# --------------------------------------------------------------------------- #
_T = exp.DataType.Type
_KINDS: dict[Any, Kind] = {
    _T.VARCHAR: "TEXT", _T.TEXT: "TEXT", _T.CHAR: "TEXT", _T.NVARCHAR: "TEXT",
    _T.INT: "INT", _T.BIGINT: "INT", _T.SMALLINT: "INT",
    _T.DOUBLE: "DOUBLE", _T.FLOAT: "DOUBLE", _T.DECIMAL: "DECIMAL",
    _T.DATE: "DATE", _T.TIMESTAMP: "TIMESTAMP", _T.DATETIME: "TIMESTAMP", _T.BOOLEAN: "BOOLEAN",
}
_CONSTRAINTS = (exp.NotNullColumnConstraint, exp.PrimaryKeyColumnConstraint)


def typed(value: Any, kind: Kind) -> Any:
    """``value`` as its kind's Python value (dates and timestamps from ISO text)."""
    if value is None or not isinstance(value, str):
        return Decimal(str(value)) if kind == "DECIMAL" and value is not None else value
    if kind == "DATE":
        return datetime.date.fromisoformat(value)
    if kind == "TIMESTAMP":
        return datetime.datetime.fromisoformat(value)
    return value


def _literal_value(node: exp.Expr, kind: Kind, where: str) -> Any:
    if isinstance(node, exp.Null):
        return None
    if isinstance(node, exp.Neg):
        return -_literal_value(node.this, kind, where)
    if isinstance(node, exp.Boolean) and kind == "BOOLEAN":
        return bool(node.this)
    if isinstance(node, exp.Literal):
        text = node.this
        if node.is_string and kind in ("TEXT", "DATE", "TIMESTAMP"):
            return typed(text, kind)
        if not node.is_string and kind in ("INT", "DOUBLE", "DECIMAL"):
            return {"INT": int, "DOUBLE": float, "DECIMAL": Decimal}[kind](text)
    raise ValueError(f"unclassified fixture value {node.sql()} for a {kind} column in {where}")


def _table_name(node: exp.Expr, statement: exp.Expr) -> tuple[Optional[str], str]:
    if not isinstance(node, exp.Table):
        raise ValueError(f"unclassified fixture statement: {statement.sql()}")
    return node.db or None, node.name


def parse_fixture_sql(text: str) -> tuple[Table, ...]:
    """Upstream fixture SQL in canonical form; any statement other than a schema reset, a table and its rows raises."""
    columns: dict[tuple[Optional[str], str], tuple[tuple[str, Kind], ...]] = {}
    rows: dict[tuple[Optional[str], str], list[tuple[Any, ...]]] = {}
    for statement in sqlglot.parse(text, read="postgres"):
        if statement is None:
            continue
        if isinstance(statement, (exp.Drop, exp.Create)) and statement.args.get("kind") == "SCHEMA":
            continue
        if isinstance(statement, exp.Create) and statement.args.get("kind") == "TABLE":
            schema = statement.this
            if not isinstance(schema, exp.Schema) or statement.expression is not None:
                raise ValueError(f"unclassified fixture statement: {statement.sql()}")
            key = _table_name(schema.this, statement)
            defs: list[tuple[str, Kind]] = []
            for column in schema.expressions:
                if not isinstance(column, exp.ColumnDef) or column.args.get("kind") is None:
                    raise ValueError(f"unclassified column {column.sql()} in {statement.sql()}")
                kind = _KINDS.get(column.args["kind"].this)
                if kind is None:
                    raise ValueError(f"unclassified column type {column.sql()} in {statement.sql()}")
                if any(not isinstance(c.kind, _CONSTRAINTS) for c in column.args.get("constraints") or []):
                    raise ValueError(f"unclassified column constraint {column.sql()} in {statement.sql()}")
                defs.append((column.name, kind))
            columns[key] = tuple(defs)
            rows[key] = []
            continue
        if isinstance(statement, exp.Insert):
            key = _table_name(statement.this, statement)
            values = statement.expression
            if key not in columns or not isinstance(values, exp.Values):
                raise ValueError(f"unclassified fixture statement: {statement.sql()}")
            kinds: list[Kind] = [k for _, k in columns[key]]
            for row in values.expressions:
                cells = row.expressions if isinstance(row, exp.Tuple) else []
                if len(cells) != len(kinds):
                    raise ValueError(f"row {row.sql()} does not match {key} in {statement.sql()}")
                rows[key].append(tuple(_literal_value(c, k, statement.sql()) for c, k in zip(cells, kinds)))
            continue
        raise ValueError(f"unclassified fixture statement: {statement.sql()}")
    return tuple(Table(schema_name=s, name=n, columns=columns[(s, n)], rows=tuple(rows[(s, n)])) for s, n in columns)


def from_table_spec(spec: TableSpec) -> Table:
    table = Table.model_validate({"name": spec.name, "columns": spec.columns})
    return table.model_copy(update={"rows": tuple(tuple(typed(v, k) for v, (_, k) in zip(row, table.columns))
                                                   for row in spec.rows)})


#: A mixed-case table and columns, read through a relationship (synthetic data).
CUSTOMER = Table(name="Customer", columns=(("id", "TEXT"), ("externalId", "TEXT")),
                 rows=(("c1", "EXT-1"), ("c2", "EXT-2")))
INVOICE = Table(name="Invoice",
                columns=(("id", "TEXT"), ("invoiceNumber", "TEXT"), ("customerId", "TEXT"), ("totalAmount", "INT")),
                rows=(("i1", "INV-1", "c1", 10), ("i2", "INV-2", "c1", 5), ("i3", "INV-3", "c2", 7)))


def wide_table_fixture(name: str) -> tuple[Table, ...]:
    return parse_fixture_sql(fixture_texts(name)[0])


def gate_tables() -> dict[str, tuple[Table, ...]]:
    """Every seeded fixture: Layer 1, upstream's wide-table SQL (``stores_ambiguous`` shares ``stores``), mixed case."""
    return {"layer1": tuple(from_table_spec(t) for t in TABLES), "retail": wide_table_fixture("retail"),
            "stores": wide_table_fixture("stores"), "mixed_case": (CUSTOMER, INVOICE)}


# --------------------------------------------------------------------------- #
# Emission per engine
# --------------------------------------------------------------------------- #
TYPES: dict[str, dict[Kind, str]] = {
    "sqlite": {"INT": "INTEGER", "DOUBLE": "REAL", "TEXT": "TEXT", "BOOLEAN": "INTEGER", "DATE": "TEXT",
               "TIMESTAMP": "TEXT", "DECIMAL": "DECIMAL(18,4)"},
    "duckdb": {"INT": "INTEGER", "DOUBLE": "DOUBLE", "TEXT": "VARCHAR", "BOOLEAN": "BOOLEAN", "DATE": "DATE",
               "TIMESTAMP": "TIMESTAMP", "DECIMAL": "DECIMAL(18,4)"},
    "postgres": {"INT": "INTEGER", "DOUBLE": "DOUBLE PRECISION", "TEXT": "TEXT", "BOOLEAN": "BOOLEAN",
                 "DATE": "DATE", "TIMESTAMP": "TIMESTAMP", "DECIMAL": "NUMERIC(18,4)"},
    "mysql": {"INT": "INT", "DOUBLE": "DOUBLE", "TEXT": "VARCHAR(255)", "BOOLEAN": "BOOLEAN", "DATE": "DATE",
              "TIMESTAMP": "DATETIME", "DECIMAL": "DECIMAL(18,4)"},
    "clickhouse": {"INT": "Nullable(Int64)", "DOUBLE": "Nullable(Float64)", "TEXT": "Nullable(String)",
                   "BOOLEAN": "Nullable(Bool)", "DATE": "Nullable(Date32)", "TIMESTAMP": "Nullable(DateTime64(0))",
                   "DECIMAL": "Nullable(Decimal(18,4))"},
    "tsql": {"INT": "INT", "DOUBLE": "FLOAT", "TEXT": "NVARCHAR(255)", "BOOLEAN": "BIT", "DATE": "DATE",
             "TIMESTAMP": "DATETIME2(0)", "DECIMAL": "DECIMAL(18,4)"},
    "trino": {"INT": "INTEGER", "DOUBLE": "DOUBLE", "TEXT": "VARCHAR", "BOOLEAN": "BOOLEAN", "DATE": "DATE",
              "TIMESTAMP": "TIMESTAMP(0)", "DECIMAL": "DECIMAL(18,4)"},
    "snowflake": {"INT": "INTEGER", "DOUBLE": "DOUBLE", "TEXT": "VARCHAR", "BOOLEAN": "BOOLEAN", "DATE": "DATE",
                  "TIMESTAMP": "TIMESTAMP_NTZ", "DECIMAL": "NUMBER(18,4)"},
    "bigquery": {"INT": "INT64", "DOUBLE": "FLOAT64", "TEXT": "STRING", "BOOLEAN": "BOOL", "DATE": "DATE",
                 "TIMESTAMP": "DATETIME", "DECIMAL": "NUMERIC(18,4)"},
}
_PLAIN = re.compile(r"^[a-z_][a-z0-9_]*$")
_BACKSLASH_ESCAPES = frozenset({"mysql", "clickhouse", "snowflake", "bigquery"})
#: Engines reading a temporal column's ISO text without a typed literal.
_TEXT_TEMPORALS = frozenset({"sqlite", "mysql", "clickhouse", "snowflake"})
_TEMPORAL_LITERAL = {"bigquery": {"DATE": "DATE", "TIMESTAMP": "DATETIME"}}


class Placement(BaseModel):
    """Where an engine keeps the gate's tables; ``token`` keeps concurrent runs apart on shared servers."""

    model_config = ConfigDict(frozen=True)

    engine: str
    token: str = ""

    def schema(self, name: str) -> str:
        return f"{name}_{self.token}" if self.token else name

    def ident(self, name: str) -> str:
        """A plain lower-case name bare (each engine folds it alike), any other quoted."""
        return name if _PLAIN.match(name) else exp.to_identifier(name, quoted=True).sql(dialect=self.engine)

    def ref(self, table: Table) -> str:
        if table.schema_name is None:
            return self.ident(table.name)
        if self.engine == "sqlite":
            return self.ident(f"{table.schema_name}__{table.name}")
        return f"{self.ident(self.schema(table.schema_name))}.{self.ident(table.name)}"

    def source(self, schema: str, name: str) -> str:
        """A model's ``source`` for upstream's ``schema.name``."""
        return f"{schema}__{name}" if self.engine == "sqlite" else f"{self.schema(schema)}.{name}"


def literal(value: Any, kind: Kind, engine: str) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return ("1" if value else "0") if engine in ("sqlite", "tsql") else ("TRUE" if value else "FALSE")
    if isinstance(value, (int, Decimal)):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    if isinstance(value, datetime.datetime):
        text = value.isoformat(sep=" ")
    elif isinstance(value, datetime.date):
        text = value.isoformat()
    else:
        text = str(value)
    if engine in _BACKSLASH_ESCAPES:
        text = text.replace("\\", "\\\\")
    quoted = "'" + (text.replace("'", "\\'") if engine == "bigquery" else text.replace("'", "''")) + "'"
    if kind in ("DATE", "TIMESTAMP") and engine not in _TEXT_TEMPORALS:
        if engine == "tsql":
            return f"CAST({quoted} AS {TYPES['tsql'][kind]})"
        return f"{_TEMPORAL_LITERAL.get(engine, {}).get(kind, kind)} {quoted}"
    return f"N{quoted}" if engine == "tsql" else quoted


def schema_statements(placement: Placement, name: str) -> list[str]:
    ident = placement.ident(placement.schema(name))
    if placement.engine == "sqlite":
        return []
    if placement.engine in ("mysql", "clickhouse"):
        return [f"CREATE DATABASE {ident}"]
    if placement.engine == "trino":
        return [f"CREATE SCHEMA memory.{ident}"]
    return [f"CREATE SCHEMA {ident}"]


def table_statements(placement: Placement, table: Table) -> list[str]:
    engine = placement.engine
    columns = ", ".join(f"{placement.ident(c)} {TYPES[engine][k]}" for c, k in table.columns)
    suffix = " ENGINE = MergeTree ORDER BY tuple()" if engine == "clickhouse" else ""
    out = [f"CREATE TABLE {placement.ref(table)} ({columns}){suffix}"]
    if table.rows:
        values = ", ".join("(" + ", ".join(literal(v, k, engine) for v, (_, k) in zip(row, table.columns)) + ")"
                           for row in table.rows)
        out.append(f"INSERT INTO {placement.ref(table)} VALUES {values}")
    return out


def _all_tables() -> list[Table]:
    return [t for group in gate_tables().values() for t in group]


def gate_schemas() -> list[str]:
    """The schemas the fixtures' tables live in, besides the gate's own."""
    return sorted({t.schema_name for t in _all_tables() if t.schema_name})


def seed_statements(placement: Placement) -> list[str]:
    """Every gate fixture: wide-table schemas first, then each table."""
    out: list[str] = []
    for schema in gate_schemas():
        out += schema_statements(placement, schema)
    for table in _all_tables():
        out += table_statements(placement, table)
    return out


def select_statement(placement: Placement, table: Table) -> str:
    return f"SELECT {', '.join(placement.ident(c) for c, _ in table.columns)} FROM {placement.ref(table)}"


# --------------------------------------------------------------------------- #
# Providers: a seeded database per engine
# --------------------------------------------------------------------------- #
class Provider(Protocol):
    @property
    def placement(self) -> Placement: ...

    def run(self, statements: Sequence[str]) -> None: ...

    def fetch(self, sql: str) -> list[tuple[Any, ...]]: ...

    def datasource(self, name: str) -> DatasourceConfig: ...


class FileProvider(BaseModel):
    """An in-process SQLite or DuckDB database file."""

    model_config = ConfigDict(frozen=True)

    engine: Literal["sqlite", "duckdb"]
    path: Path

    @property
    def placement(self) -> Placement:
        return Placement(engine=self.engine)

    @contextlib.contextmanager
    def _connection(self) -> Generator[Any]:
        connection = sqlite3.connect(self.path) if self.engine == "sqlite" else duckdb.connect(str(self.path))
        try:
            yield connection
            connection.commit()
        finally:
            connection.close()

    def run(self, statements: Sequence[str]) -> None:
        with self._connection() as connection:
            for statement in statements:
                connection.execute(statement)

    def fetch(self, sql: str) -> list[tuple[Any, ...]]:
        """Read through SLayer's own engine factory: DuckDB refuses a second connection configured differently."""
        datasource = self.datasource("readback")
        try:
            with engine_factory.get_engine(datasource).connect() as connection:
                return [tuple(row) for row in connection.exec_driver_sql(sql).fetchall()]
        finally:
            engine_factory.invalidate_engine(datasource)

    def datasource(self, name: str) -> DatasourceConfig:
        return DatasourceConfig(name=name, type=self.engine, database=str(self.path))


def file_provider(engine: Literal["sqlite", "duckdb"], base: Path) -> FileProvider:
    """A seeded in-process database under ``base``."""
    base.mkdir(parents=True, exist_ok=True)
    provider = FileProvider(engine=engine, path=base / f"gate.{'duckdb' if engine == 'duckdb' else 'db'}")
    provider.run(seed_statements(provider.placement))
    return provider


def run_token() -> str:
    return uuid.uuid4().hex[:8]


# --------------------------------------------------------------------------- #
# Query engines over a seeded database
# --------------------------------------------------------------------------- #
_SOURCE = re.compile(r"^([ \t]*source:[ \t]*)([A-Za-z_]\w*)\.([A-Za-z_]\w*)[ \t]*$", re.MULTILINE)


def placed_model(text: str, placement: Placement) -> str:
    """A wide-table model whose sources name ``placement``'s tables."""
    return _SOURCE.sub(lambda m: m.group(1) + placement.source(m.group(2), m.group(3)), text)


@contextlib.asynccontextmanager
async def gate_engine(provider: Provider, base: Path, *, data_source: str, datasets: Sequence[SlayerDataset] = (),
                      documents: Sequence[str] = (), clock: Optional[PinnedClock] = None,
                      datasource_fields: Optional[dict[str, Any]] = None) -> AsyncGenerator[SlayerQueryEngine]:
    """An engine over ``provider``'s database with ``datasets`` (saved unvalidated) and ``documents`` stored."""
    datasource = provider.datasource(data_source)
    if datasource_fields:
        datasource = DatasourceConfig.model_validate({**datasource.model_dump(), **datasource_fields})
    storage = YAMLStorage(base_dir=str(base / "store"))
    async with owned_query_engine(storage=storage, datasources=[datasource], clock=clock) as engine:
        for dataset in datasets:
            await storage.save_dataset(dataset, _validate=False)
        for text in documents:
            await store_document(storage, text, data_source=data_source)
        yield engine


@contextlib.asynccontextmanager
async def layer1_engine(provider: Provider, base: Path) -> AsyncGenerator[SlayerQueryEngine]:
    async with gate_engine(provider, base, data_source=LAYER1_DS, datasets=dev2215_datasets(data_source=LAYER1_DS),
                           clock=PinnedClock(NOW), datasource_fields=DS_FIELDS) as engine:
        yield engine


@contextlib.asynccontextmanager
async def wide_table_engines(provider: Provider, base: Path) -> AsyncGenerator[dict[str, SlayerQueryEngine]]:
    """One engine per upstream wide-table fixture model."""
    async with contextlib.AsyncExitStack() as stack:
        engines = {}
        for name in FIXTURES:
            model = placed_model(fixture_texts(name)[1], provider.placement)
            engines[name] = await stack.enter_async_context(
                gate_engine(provider, base / name, data_source=WIDE_TABLE_DS, documents=[model]))
        yield engines


def mixed_case_datasets(*, data_source: str = MIXED_CASE_DS) -> list[SlayerDataset]:
    def text(name: str, **extra: Any) -> DatasetField:
        return DatasetField.model_validate({"name": name, "datatype": DataType.STRING, **extra})

    customer = SlayerDataset(name="customer", sql_table="Customer", data_source=data_source,
                             fields=[text("id", primary_key=True), text("externalId")])
    invoice = SlayerDataset(
        name="invoice", sql_table="Invoice", data_source=data_source,
        fields=[text("id", primary_key=True), text("invoiceNumber"), text("customerId"),
                DatasetField(name="totalAmount", datatype=DataType.INTEGER)],
        relationships=[DatasetRelationship(to="customer", from_columns=["customerId"], to_columns=["id"])])
    return [customer, invoice]


@contextlib.asynccontextmanager
async def mixed_case_engine(provider: Provider, base: Path) -> AsyncGenerator[SlayerQueryEngine]:
    async with gate_engine(provider, base, data_source=MIXED_CASE_DS, datasets=mixed_case_datasets()) as engine:
        yield engine
