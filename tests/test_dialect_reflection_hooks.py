"""``SqlDialect.reflect_columns`` / ``reflect_table_keys``: DuckDB catalog reads, base hooks, catalog-query emission."""

from __future__ import annotations

import re
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Any, NoReturn

import duckdb
import pytest
import sqlalchemy as sa
import sqlglot
from sqlalchemy import event
from sqlglot import exp
from sqlglot.expressions.core import Expression

from slayer.core.enums import DataType
from slayer.sql.dialects import (
    BigqueryDialect,
    ClickhouseDialect,
    DuckdbDialect,
    MariadbDialect,
    MysqlDialect,
    PostgresDialect,
    ReflectedColumn,
    ReflectedForeignKey,
    SnowflakeDialect,
    SqlDialect,
    SqliteDialect,
    TableKeys,
)
from slayer.storage.sqlite_conn import transaction
from tests._engine_helpers import disposable_engine

# ---------------------------------------------------------------------------
# Doubles
# ---------------------------------------------------------------------------


class _ExplodingInspector:
    """Any use fails: DuckDB reflection must never consult the SQLAlchemy Inspector."""

    def __getattr__(self, name: str) -> NoReturn:
        raise AssertionError(f"Inspector.{name} consulted")


class _ColumnsRaisingInspector:
    default_schema_name: str | None = None

    def get_columns(self, *_args: Any, **_kwargs: Any) -> NoReturn:
        raise RuntimeError("Catalog Error: Type with name REGCLASS does not exist!")


class _Rows:
    def __init__(self, rows: list[tuple]) -> None:
        self._rows = rows

    def fetchall(self) -> list[tuple]:
        return list(self._rows)

    def all(self) -> list[tuple]:
        return list(self._rows)

    def __iter__(self) -> Iterator[tuple]:
        return iter(self._rows)


class _RecordingConnection:
    def __init__(self, *, log: list[str], respond: Callable[[str], list[tuple]]) -> None:
        self._log = log
        self._respond = respond

    def __enter__(self) -> _RecordingConnection:
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def exec_driver_sql(self, statement: str, *args: Any, **kwargs: Any) -> _Rows:
        assert not args, "catalog queries carry no bind parameters"
        assert not kwargs, "catalog queries carry no bind parameters"
        self._log.append(statement)
        return _Rows(self._respond(statement))


class _RecordingEngine:
    def __init__(self, *, backend: str) -> None:
        self.dialect = SimpleNamespace(name=backend)
        self.statements: list[str] = []

    def connect(self) -> _RecordingConnection:
        return _RecordingConnection(log=self.statements, respond=_respond)


def _respond(statement: str) -> list[tuple]:
    low = statement.lower()
    if "comment" in low or "col_description" in low:
        return [("id", "  the id  ")]
    if "data_type" in low:
        return [("id", "INTEGER")]
    return []


# ---------------------------------------------------------------------------
# Catalog-query emission (via the public hook, Inspector failing)
# ---------------------------------------------------------------------------

_BIND = re.compile(r"(?<![:\w]):[A-Za-z_]\w*")


def _emit(
    *, dialect: SqlDialect, backend: str, table: str = "orders",
    schema: str | None, catalog: str | None = None,
) -> tuple[list[ReflectedColumn], list[str]]:
    engine = _RecordingEngine(backend=backend)
    cols = dialect.reflect_columns(
        sa_engine=engine, inspector=_ColumnsRaisingInspector(),
        table=table, schema=schema, catalog=catalog,
    )
    return cols, engine.statements


def _parse(sql: str, read: str) -> exp.Select:
    tree = sqlglot.parse_one(sql, read=read)
    assert isinstance(tree, exp.Select), sql
    return tree


def _eq_literals(tree: Expression) -> dict[str, str]:
    out: dict[str, str] = {}
    for eq in tree.find_all(exp.EQ):
        col, lit = eq.this, eq.expression
        if isinstance(lit, exp.Column) and isinstance(col, exp.Literal):
            col, lit = lit, col
        if isinstance(col, exp.Column) and isinstance(lit, exp.Literal) and lit.is_string:
            out[col.name.lower()] = lit.this
    return out


def _tables(tree: Expression) -> set[str]:
    return {
        ".".join(p.name.lower() for p in t.parts) for t in tree.find_all(exp.Table)
    }


def _selected(tree: exp.Select) -> list[str]:
    return [e.alias_or_name.lower() for e in tree.expressions]


def _one(statements: list[str], *, read: str, pick: Callable[[exp.Select], bool]) -> exp.Select:
    hits = [t for t in (_parse(s, read) for s in statements) if pick(t)]
    assert len(hits) == 1, statements
    return hits[0]


def _is_info_schema_columns(tree: exp.Select) -> bool:
    return "data_type" in _selected(tree)


class TestCatalogQueryEmission:
    def test_base_columns_query_is_scoped_by_schema_and_catalog(self) -> None:
        cols, stmts = _emit(dialect=SqlDialect(), backend="firebird", schema="sales", catalog="cat")
        tree = _one(stmts, read="postgres", pick=_is_info_schema_columns)
        assert "information_schema.columns" in _tables(tree)
        assert _selected(tree)[:2] == ["column_name", "data_type"]
        assert _eq_literals(tree) == {
            "table_name": "orders", "table_schema": "sales", "table_catalog": "cat",
        }
        order = tree.args.get("order")
        assert order is not None
        assert "ordinal_position" in order.sql().lower()
        assert cols == [ReflectedColumn(
            name="id", type=DataType.INT, is_float=False, db_type=None, comment=None,
        )]

    def test_base_columns_query_without_catalog_has_no_catalog_predicate(self) -> None:
        _, stmts = _emit(dialect=SqlDialect(), backend="firebird", schema="sales")
        tree = _one(stmts, read="postgres", pick=_is_info_schema_columns)
        assert "table_catalog" not in _eq_literals(tree)

    @pytest.mark.parametrize(
        ("dialect", "backend"),
        [(MysqlDialect(), "mysql"), (MariadbDialect(), "mariadb")],
        ids=["mysql", "mariadb"],
    )
    def test_mysql_comment_query(self, dialect: SqlDialect, backend: str) -> None:
        cols, stmts = _emit(dialect=dialect, backend=backend, schema="shop")
        tree = _one(stmts, read="mysql", pick=lambda t: "column_comment" in _selected(t))
        assert "information_schema.columns" in _tables(tree)
        assert _eq_literals(tree) == {"table_name": "orders", "table_schema": "shop"}
        assert [c.comment for c in cols] == ["the id"]

    def test_snowflake_comment_query(self) -> None:
        cols, stmts = _emit(
            dialect=SnowflakeDialect(), backend="snowflake", schema="S", catalog="DB",
        )
        tree = _one(stmts, read="snowflake", pick=lambda t: "comment" in _selected(t))
        assert "information_schema.columns" in _tables(tree)
        assert _eq_literals(tree) == {
            "table_name": "orders", "table_schema": "S", "table_catalog": "DB",
        }
        assert [c.comment for c in cols] == ["the id"]

    def test_clickhouse_comment_query(self) -> None:
        cols, stmts = _emit(dialect=ClickhouseDialect(), backend="clickhouse", schema="analytics")
        tree = _one(stmts, read="clickhouse", pick=lambda t: "comment" in _selected(t))
        assert "system.columns" in _tables(tree)
        assert _eq_literals(tree) == {"table": "orders", "database": "analytics"}
        assert [c.comment for c in cols] == ["the id"]

    def test_postgres_comment_query(self) -> None:
        cols, stmts = _emit(dialect=PostgresDialect(), backend="postgresql", schema="sales")
        tree = _one(
            stmts, read="postgres",
            pick=lambda t: "col_description" in t.sql(dialect="postgres").lower(),
        )
        assert {"pg_catalog.pg_class", "pg_catalog.pg_attribute"} <= _tables(tree)
        literals = _eq_literals(tree)
        assert literals["relname"] == "orders"
        assert literals["nspname"] == "sales"
        assert [c.comment for c in cols] == ["the id"]

    def test_bigquery_reads_the_dataset_scoped_information_schema(self) -> None:
        _, stmts = _emit(dialect=BigqueryDialect(), backend="bigquery", schema="my_ds")
        tree = _one(stmts, read="bigquery", pick=_is_info_schema_columns)
        assert "my_ds.information_schema.columns" in _tables(tree)
        assert _eq_literals(tree) == {"table_name": "orders"}

    @pytest.mark.parametrize(
        ("dialect", "backend", "read"),
        [
            (SqlDialect(), "firebird", "postgres"),
            (PostgresDialect(), "postgresql", "postgres"),
            (MysqlDialect(), "mysql", "mysql"),
            (SnowflakeDialect(), "snowflake", "snowflake"),
            (ClickhouseDialect(), "clickhouse", "clickhouse"),
            (BigqueryDialect(), "bigquery", "bigquery"),
        ],
        ids=["base", "postgres", "mysql", "snowflake", "clickhouse", "bigquery"],
    )
    def test_names_are_escaped_literals_never_bind_params(
        self, dialect: SqlDialect, backend: str, read: str,
    ) -> None:
        _, stmts = _emit(dialect=dialect, backend=backend, table="o'brien", schema="s'x")
        assert stmts
        for sql in stmts:
            assert not _BIND.search(sql), sql
            values = set(_eq_literals(_parse(sql, read)).values())
            assert "o'brien" in values, sql

    def test_postgres_quote_doubles(self) -> None:
        _, stmts = _emit(dialect=PostgresDialect(), backend="postgresql", table="o'brien", schema="s")
        assert all("'o''brien'" in s for s in stmts)


# ---------------------------------------------------------------------------
# DuckDB: keys and columns from its own catalog
# ---------------------------------------------------------------------------


def _duck(path: Path, statements: list[str]) -> None:
    con = duckdb.connect(str(path))
    try:
        for stmt in statements:
            con.execute(stmt)
    finally:
        con.close()


@pytest.fixture(scope="module")
def duck_paths(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, Path]:
    d = tmp_path_factory.mktemp("duck_hooks")
    main, other = d / "keys.duckdb", d / "aaa.duckdb"
    _duck(main, [
        'CREATE TABLE customers(id INTEGER PRIMARY KEY, code VARCHAR UNIQUE, a INTEGER, b INTEGER, '
        '"Mixed Case" INTEGER, name VARCHAR, UNIQUE (a, b))',
        'CREATE UNIQUE INDEX ux_mixed ON customers ("Mixed Case")',
        "CREATE UNIQUE INDEX ux_lower ON customers (lower(name))",
        "CREATE TABLE lines(order_id INTEGER, n INTEGER, PRIMARY KEY (order_id, n))",
        "CREATE SCHEMA ofr",
        "CREATE TABLE ofr.parent(a INTEGER, b INTEGER, PRIMARY KEY (a, b))",
        "CREATE TABLE ofr.child(id INTEGER PRIMARY KEY, pa INTEGER, pb INTEGER, "
        "FOREIGN KEY (pa, pb) REFERENCES ofr.parent(a, b))",
        "CREATE SCHEMA s2",
        "CREATE TABLE s2.customers(zz INTEGER PRIMARY KEY, code INTEGER)",
        "CREATE TABLE typed(id INTEGER, amount DECIMAL(12,2), label VARCHAR, ts TIMESTAMP, "
        "d DOUBLE, day DATE, flag BOOLEAN, big HUGEINT)",
        "COMMENT ON COLUMN typed.label IS 'main label'",
    ])
    _duck(other, [
        "CREATE TABLE customers(code VARCHAR PRIMARY KEY, id INTEGER UNIQUE)",
        "CREATE TABLE typed(other_col VARCHAR, label INTEGER)",
        "COMMENT ON COLUMN typed.label IS 'aaa label'",
    ])
    return main, other


@contextmanager
def _attached(paths: tuple[Path, Path]) -> Iterator[sa.Engine]:
    main, other = paths
    with disposable_engine(f"duckdb:///{main}") as engine:

        @event.listens_for(engine, "connect")
        def _attach(dbapi_conn, _rec):  # noqa: ANN001
            dbapi_conn.execute(f"ATTACH IF NOT EXISTS '{other}' AS aaa")

        yield engine


def _keys(engine: sa.Engine, *, table: str, schema: str = "main", catalog: str = "keys") -> TableKeys:
    return DuckdbDialect().reflect_table_keys(
        sa_engine=engine, inspector=_ExplodingInspector(),
        table=table, schema=schema, catalog=catalog,
    )


# Every unique index reports SQL sqlglot cannot parse.
_GARBLED_INDEXES = (
    "(SELECT * REPLACE ('CREATE UNIQUE INDEX broken ON customers (((' AS sql) FROM duckdb_indexes())"
)


def _key_sets(keys: TableKeys) -> set[tuple[str, ...]]:
    return {tuple(s) for s in keys.unique_key_sets}


class TestDuckdbTableKeys:
    def test_single_pk_unique_constraints_and_plain_unique_index(self, duck_paths) -> None:
        with _attached(duck_paths) as engine:
            keys = _keys(engine, table="customers")
        assert keys.primary_key == ["id"]
        assert _key_sets(keys) == {("code",), ("a", "b"), ("Mixed Case",)}
        assert keys.foreign_keys == []

    def test_composite_pk_keeps_declared_order(self, duck_paths) -> None:
        with _attached(duck_paths) as engine:
            assert _keys(engine, table="lines").primary_key == ["order_id", "n"]

    def test_composite_fk_in_non_default_schema(self, duck_paths) -> None:
        with _attached(duck_paths) as engine:
            child = _keys(engine, table="child", schema="ofr")
            parent = _keys(engine, table="parent", schema="ofr")
        assert child.foreign_keys == [ReflectedForeignKey(
            columns=["pa", "pb"], referred_schema="ofr",
            referred_table="parent", referred_columns=["a", "b"],
        )]
        assert parent.primary_key == ["a", "b"]

    def test_same_named_table_in_other_schema_is_isolated(self, duck_paths) -> None:
        with _attached(duck_paths) as engine:
            twin = _keys(engine, table="customers", schema="s2")
        assert twin.primary_key == ["zz"]
        assert twin.unique_key_sets == []

    def test_unparseable_unique_index_is_ignored(self, duck_paths) -> None:
        with _attached(duck_paths) as engine:

            @event.listens_for(engine, "before_cursor_execute", retval=True)
            def _garble(_conn, _cursor, statement, parameters, *_rest):  # noqa: ANN001
                return statement.replace("duckdb_indexes()", _GARBLED_INDEXES), parameters

            keys = _keys(engine, table="customers")
        assert keys.primary_key == ["id"]
        assert _key_sets(keys) == {("code",), ("a", "b")}

    def test_attached_catalog_twin_contributes_nothing(self, duck_paths) -> None:
        with _attached(duck_paths) as engine:
            main = _keys(engine, table="customers")
            other = _keys(engine, table="customers", catalog="aaa")
        assert main.primary_key == ["id"]
        assert ("id",) not in _key_sets(main)
        assert other.primary_key == ["code"]
        assert _key_sets(other) == {("id",)}


class TestDuckdbColumns:
    def test_types_comments_and_order_from_duckdb_columns(self, duck_paths) -> None:
        with _attached(duck_paths) as engine:
            cols = DuckdbDialect().reflect_columns(
                sa_engine=engine, inspector=_ExplodingInspector(),
                table="typed", schema="main", catalog="keys",
            )
        assert [(c.name, c.type, c.is_float, c.db_type, c.comment) for c in cols] == [
            ("id", DataType.INT, False, None, None),
            ("amount", DataType.DOUBLE, True, "DECIMAL(12,2)", None),
            ("label", DataType.TEXT, False, None, "main label"),
            ("ts", DataType.TIMESTAMP, False, None, None),
            ("d", DataType.DOUBLE, True, None, None),
            ("day", DataType.DATE, False, None, None),
            ("flag", DataType.BOOLEAN, False, None, None),
            ("big", DataType.INT, False, None, None),
        ]

    def test_attached_twin_columns_are_scoped_out(self, duck_paths) -> None:
        with _attached(duck_paths) as engine:
            cols = DuckdbDialect().reflect_columns(
                sa_engine=engine, inspector=_ExplodingInspector(),
                table="typed", schema="main", catalog="aaa",
            )
        assert [(c.name, c.comment) for c in cols] == [
            ("other_col", None), ("label", "aaa label"),
        ]


# ---------------------------------------------------------------------------
# Base hook on SQLite (today's behaviour, Inspector-authoritative PK)
# ---------------------------------------------------------------------------


@pytest.fixture
def sqlite_engine(tmp_path: Path) -> Iterator[sa.Engine]:
    db = str(tmp_path / "base.db")
    with transaction(db) as conn:
        conn.executescript(
            """
            CREATE TABLE customers (
                id INTEGER PRIMARY KEY, email TEXT UNIQUE, a INTEGER, b INTEGER,
                code TEXT, flag INTEGER, UNIQUE (a, b)
            );
            CREATE UNIQUE INDEX ux_code ON customers (code);
            CREATE UNIQUE INDEX ux_partial ON customers (flag) WHERE flag IS NOT NULL;
            CREATE TABLE orders (
                id INTEGER PRIMARY KEY, customer_id INTEGER REFERENCES customers(id),
                a INTEGER, b INTEGER, FOREIGN KEY (a, b) REFERENCES customers(a, b)
            );
            """
        )
    with disposable_engine(f"sqlite:///{db}") as engine:
        yield engine


@contextmanager
def _statements(engine: sa.Engine) -> Iterator[list[str]]:
    seen: list[str] = []

    def _record(_conn, _cursor, statement, *_rest) -> None:  # noqa: ANN001
        seen.append(statement)

    event.listen(engine, "before_cursor_execute", _record)
    try:
        yield seen
    finally:
        event.remove(engine, "before_cursor_execute", _record)


class TestSqliteBaseKeys:
    def test_pk_unique_and_whole_table_unique_index(self, sqlite_engine) -> None:
        keys = SqliteDialect().reflect_table_keys(
            sa_engine=sqlite_engine, inspector=sa.inspect(sqlite_engine),
            table="customers", schema=None, catalog=None,
        )
        assert keys.primary_key == ["id"]
        assert _key_sets(keys) == {("email",), ("a", "b"), ("code",)}

    def test_foreign_keys(self, sqlite_engine) -> None:
        keys = SqliteDialect().reflect_table_keys(
            sa_engine=sqlite_engine, inspector=sa.inspect(sqlite_engine),
            table="orders", schema=None, catalog=None,
        )
        assert {
            (tuple(fk.columns), fk.referred_table, tuple(fk.referred_columns), fk.referred_schema)
            for fk in keys.foreign_keys
        } == {
            (("customer_id",), "customers", ("id",), None),
            (("a", "b"), "customers", ("a", "b"), None),
        }

    def test_pk_never_falls_back_to_information_schema(self, sqlite_engine) -> None:
        with _statements(sqlite_engine) as seen:
            SqliteDialect().reflect_table_keys(
                sa_engine=sqlite_engine, inspector=sa.inspect(sqlite_engine),
                table="orders", schema=None, catalog=None,
            )
        assert not [s for s in seen if "information_schema" in s.lower()]

    def test_columns_through_the_inspector(self, sqlite_engine) -> None:
        cols = SqliteDialect().reflect_columns(
            sa_engine=sqlite_engine, inspector=sa.inspect(sqlite_engine),
            table="orders", schema=None, catalog=None,
        )
        assert [c.name for c in cols] == ["id", "customer_id", "a", "b"]
