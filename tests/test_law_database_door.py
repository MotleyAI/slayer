"""One database door: user-datasource engines, inspectors and driver connections live only in the client.

Import-binding-aware AST check over ``slayer/**``. Reachability of the door at runtime is proven
by the routing tests (``tests/test_gateway_features.py``); this law proves no code bypasses it.
"""

from __future__ import annotations

import ast
import contextlib
import textwrap
from pathlib import Path
from typing import Any

import duckdb

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.engine.ingestion import ingest_datasource_idempotent
from slayer.engine.profiling import ensure_samples_fresh
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql.client import ExecutionResult, ObjectListing
from slayer.sql.dialects.base import ServerProfile
from slayer.sql.dialects.reflection import ReflectedColumn, TableKeys, TableLocation
from slayer.storage.yaml_storage import YAMLStorage
from tests._gateway_harness import GatewayFactory, backend_of, no_direct_access, shop_storage
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


# --------------------------------------------------------------------------- #
# List columns reach the database only through the engine-owned client.
# --------------------------------------------------------------------------- #
LIST_DS = "lists"


def _seed_lists(db_path: str) -> None:
    with contextlib.closing(duckdb.connect(db_path)) as con:
        con.execute("CREATE TABLE contractors (id INTEGER, skills VARCHAR[])")
        con.execute("INSERT INTO contractors VALUES (1, ['go', 'sql', 'go']), (2, ['sql', NULL]), (3, []), (4, NULL)")


def _lists_datasource(tmp_path: Path) -> DatasourceConfig:
    db = str(tmp_path / "lists.duckdb")
    _seed_lists(db)
    return DatasourceConfig(name=LIST_DS, type="duckdb", database=db)


async def _gateway_engine(tmp_path: Path, *, datasource: DatasourceConfig,
                          models: list[SlayerModel]) -> tuple[SlayerQueryEngine, GatewayFactory]:
    factory = GatewayFactory()
    storage = await shop_storage(str(tmp_path / "store"), datasource=datasource, models=models)
    return SlayerQueryEngine(storage=storage, client_factory=factory), factory


class TestListPathsUseTheEngineClient:
    async def test_re_type_on_re_ingest(self, tmp_path: Path) -> None:
        ds = _lists_datasource(tmp_path)
        stale = SlayerModel(name="contractors", sql_table="contractors", data_source=LIST_DS, columns=[
            Column(name="id", type=DataType.INT), Column(name="skills", type=DataType.TEXT, description="Skills")])
        engine, _factory = await _gateway_engine(tmp_path, datasource=ds, models=[stale])
        try:
            with no_direct_access(backends=[backend_of(ds)]) as guard:
                result = await ingest_datasource_idempotent(datasource=ds, storage=engine.storage,
                                                            client=engine.client_for(ds))
            model = await engine.storage.get_model("contractors", data_source=LIST_DS)
        finally:
            await engine.aclose()
            engine.close()
        assert guard.attempts == []
        assert result.errors == []
        assert model is not None
        skills = model.get_column("skills")
        assert skills is not None
        assert (skills.type, skills.element_type, skills.description) == (DataType.ARRAY, DataType.TEXT, "Skills")

    async def test_list_profiling(self, tmp_path: Path) -> None:
        ds = _lists_datasource(tmp_path)
        model = SlayerModel(name="contractors", sql_table="contractors", data_source=LIST_DS, columns=[
            Column(name="id", type=DataType.INT),
            Column(name="skills", type=DataType.ARRAY, element_type=DataType.TEXT)])
        engine, factory = await _gateway_engine(tmp_path, datasource=ds, models=[model])
        try:
            with no_direct_access(backends=[backend_of(ds)]) as guard:
                skills = model.get_column("skills")
                assert skills is not None
                outcome = await ensure_samples_fresh(model=model, columns=[skills],
                                                     engine=engine, storage=engine.storage, force=True)
        finally:
            await engine.aclose()
            engine.close()
        assert guard.attempts == []
        assert outcome.errors == []
        assert outcome.columns[0].sampled_values == ["go", "sql"]
        assert any("skills" in sql for sql in factory.all_statements())

    async def test_sql_mode_cursor_typing(self, tmp_path: Path) -> None:
        ds = _lists_datasource(tmp_path)
        model = SlayerModel(name="picked", sql="SELECT id, skills FROM contractors", data_source=LIST_DS, columns=[
            Column(name="id", type=DataType.INT),
            Column(name="skills", type=DataType.ARRAY, element_type=DataType.TEXT)])
        engine, factory = await _gateway_engine(tmp_path, datasource=ds, models=[model])
        try:
            with no_direct_access(backends=[backend_of(ds)]) as guard:
                entries = await engine.validate_models(LIST_DS)
        finally:
            await engine.aclose()
            engine.close()
        assert guard.attempts == []
        assert "get_result_column_types" in [call.op for call in factory.all_calls()]
        assert [e for e in entries if "skills" in str(e.model_dump())] == []


class SnowflakeListClient:
    """A Snowflake client stub: one table with an untyped ``ARRAY`` whose elements are integers."""

    def __init__(self, datasource: DatasourceConfig) -> None:
        self.datasource = datasource
        self.executed: list[str] = []

    async def execute(self, sql: str) -> ExecutionResult:
        self.executed.append(sql)
        rows = [{"kind": "INTEGER"}, {"kind": "NULL_VALUE"}] if "TYPEOF" in sql.upper() else []
        return ExecutionResult(rows=rows)

    async def get_column_types(self, sql: str) -> dict[str, str]:
        return {}

    async def server_profile(self) -> ServerProfile:
        return ServerProfile()

    async def ping(self) -> None:
        return None

    async def list_schemas(self) -> list[str]:
        return ["public"]

    async def default_schema(self) -> str | None:
        return "public"

    async def current_catalog(self) -> str | None:
        return "db"

    async def list_objects(self, schema: str | None) -> ObjectListing:
        return ObjectListing(tables=["contractors"])

    async def reflect_columns(self, location: TableLocation) -> list[ReflectedColumn]:
        return [ReflectedColumn(name="id", type=DataType.INT),
                ReflectedColumn(name="scores", type=DataType.ARRAY, element_type=DataType.UNKNOWN, db_type="ARRAY")]

    async def reflect_table_keys(self, location: TableLocation) -> TableKeys:
        return TableKeys()

    async def table_comment(self, location: TableLocation) -> str | None:
        return None

    async def schema_description(self, schema: str | None, configured_schema: str | None) -> str | None:
        return None

    async def aclose(self) -> None:
        return None

    def close(self) -> None:
        return None


async def test_snowflake_element_inference_uses_the_engine_client(tmp_path: Path) -> None:
    """The inference query is one ``execute`` on the engine's client; no engine or connection is opened."""
    ds = DatasourceConfig(name="sf", type="snowflake", host="xy12345", username="u", password="p",
                          database="db", schema_name="public")
    clients: list[SnowflakeListClient] = []

    def factory(datasource: DatasourceConfig) -> Any:
        clients.append(SnowflakeListClient(datasource))
        return clients[-1]

    storage = YAMLStorage(base_dir=str(tmp_path / "store"))
    await storage.save_datasource(ds)
    engine = SlayerQueryEngine(storage=storage, client_factory=factory)
    try:
        with no_direct_access() as guard:
            await ingest_datasource_idempotent(datasource=ds, storage=storage, client=engine.client_for(ds))
        model = await storage.get_model("contractors", data_source="sf")
    finally:
        await engine.aclose()
        engine.close()
    assert guard.attempts == []
    assert any("TYPEOF" in sql.upper() for c in clients for sql in c.executed)
    assert model is not None
    scores = model.get_column("scores")
    assert scores is not None
    assert (scores.type, scores.element_type) == (DataType.ARRAY, DataType.INT)
