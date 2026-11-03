"""One database door: user-datasource engines, inspectors and driver connections live only in the client.

Import-binding-aware AST check over ``slayer/**``. Reachability of the door at runtime is proven
by the routing tests (``tests/test_gateway_features.py``); this law proves no code bypasses it.
"""

from __future__ import annotations

import ast
import textwrap
from pathlib import Path

from tests.test_law_resource_ownership import _canonical, _import_bindings

REPO = Path(__file__).resolve().parent.parent
SLAYER = REPO / "slayer"

#: The trusted boundary: the default client and the connection / dialect code it owns.
BOUNDARY = (
    "slayer/sql/client.py",
    "slayer/sql/engine_factory.py",
    "slayer/sql/sqlite_introspect.py",
    "slayer/sql/dialects/",
)
#: SLayer's own databases: storage's SQLite, the facade-local catalog DuckDB, demo data generation.
EXEMPT = (
    "slayer/storage/sqlite_conn.py",
    "slayer/facade/catalog_sql.py",
    "slayer/demo/",
)

_FACTORY_DOORS = {"get_engine", "build_in_memory_sqlite_engine"}
_ENGINE_BUILDERS = {"create_engine", "create_async_engine"}
_CONNECTION_DOORS = {"exec_driver_sql", "raw_connection"}
_DRIVERS = {"sqlite3", "duckdb", "psycopg", "psycopg2", "pymysql", "pyodbc", "snowflake", "clickhouse_connect"}


def _door(func: ast.AST, bindings: dict[str, str]) -> str | None:
    """The database door a call target opens, or None."""
    canonical = _canonical(func, bindings)
    if not canonical:
        return None
    parts = canonical.split(".")
    leaf = parts[-1]
    if leaf in _FACTORY_DOORS and "engine_factory" in parts:
        return f"engine factory `{canonical}`"
    if leaf in _ENGINE_BUILDERS and "sqlalchemy" in parts:
        return f"engine builder `{canonical}`"
    if leaf == "inspect" and parts[0] == "sqlalchemy":
        return f"inspector `{canonical}`"
    if leaf in _CONNECTION_DOORS:
        return f"connection `{canonical}`"
    if leaf == "connect" and parts[0] in _DRIVERS:
        return f"driver connect `{canonical}`"
    if leaf == "connect" and isinstance(func, ast.Attribute):
        return f"connection `{canonical}`"
    return None


def _sites(tree: ast.AST) -> list[tuple[int, str]]:
    bindings = _import_bindings(tree)
    return [
        (node.lineno, door)
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and (door := _door(node.func, bindings)) is not None
    ]


def _allowed(relpath: str) -> bool:
    return any(relpath == p or (p.endswith("/") and relpath.startswith(p)) for p in (*BOUNDARY, *EXEMPT))


def _violations() -> list[str]:
    out: list[str] = []
    for path in sorted(SLAYER.rglob("*.py")):
        rel = path.relative_to(REPO).as_posix()
        if _allowed(rel):
            continue
        out.extend(f"{rel}:{line}: {door}" for line, door in _sites(ast.parse(path.read_text())))
    return out


def test_only_the_default_client_touches_the_database_driver() -> None:
    violations = _violations()
    assert not violations, (
        f"{len(violations)} live database access(es) outside the client boundary:\n" + "\n".join(violations)
    )


def test_every_boundary_and_exempt_path_exists() -> None:
    for rel in (*BOUNDARY, *EXEMPT):
        assert (REPO / rel).exists(), rel


def test_the_scanner_sees_every_kind_of_door() -> None:
    source = textwrap.dedent("""
        import duckdb
        import sqlalchemy as sa
        from sqlalchemy import inspect
        from sqlalchemy.ext.asyncio import create_async_engine
        from slayer.sql import engine_factory

        def f(ds, sa_engine, eng, conn):
            engine_factory.get_engine(ds)
            engine_factory.build_in_memory_sqlite_engine("sqlite://")
            sa.create_engine("sqlite://")
            create_async_engine("sqlite+aiosqlite://")
            sa.inspect(sa_engine)
            inspect(sa_engine)
            sa_engine.connect()
            eng.connect()
            sa_engine.raw_connection()
            conn.exec_driver_sql("SELECT 1")
            duckdb.connect("x.duckdb")
    """)
    assert len(_sites(ast.parse(source))) == 11


def test_unrelated_calls_are_not_doors() -> None:
    source = textwrap.dedent("""
        import inspect
        import asyncio

        def f(obj):
            inspect.signature(obj)
            asyncio.open_connection("h", 1)
    """)
    assert _sites(ast.parse(source)) == []
