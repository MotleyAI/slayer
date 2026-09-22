"""Rendered SQL reaches the driver verbatim (DEV-1933).

The client used to run rendered SQL through ``sa.text()``, whose bind-parameter
regex reads any ``:word`` (e.g. a regex ``(?:too ...)`` non-capturing group) as a
bind parameter with no value, so the query failed. The fix routes every statement
through one verbatim door (``exec_driver_sql`` with ``no_parameters=True``); these
tests pin that door and the invariant that keeps it single.

The literal ``'(?i)(?:too complicated|too complex)'`` carries ``:too``, which
``text()`` misreads; ``strftime('%Y-%m', ...)`` carries the ``%`` format string.
"""

import ast
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, ModelMeasure, SlayerModel
from slayer.core.query import ColumnRef, ModelExtension, SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql import client as sql_client
from slayer.sql import engine_factory
from slayer.sql.client import SlayerSQLClient
from slayer.storage.yaml_storage import YAMLStorage
from slayer.storage.sqlite_conn import transaction

# The production shape: ``:too`` is what text() misreads; ``%Y-%m`` is the format
# hazard. Both live inside string literals, so the driver must see them unchanged.
_REGEX_LITERAL = "(?i)(?:too complicated|too complex)"
_DOOR_SQL = (
    "SELECT '(?i)(?:too complicated|too complex)' AS pat, "
    "strftime('%Y-%m', '2020-03-15') AS ym"
)
_DOOR_ROW = {"pat": _REGEX_LITERAL, "ym": "2020-03"}
_DOOR_TYPES = {"pat": "string", "ym": "string"}

# The two verbatim helpers the fix introduces; every driver-level execute lives
# here and nowhere else.
_HELPER_NAMES = {"_exec_verbatim", "_exec_verbatim_async"}


def _in_memory_client(name: str = "dev1933") -> SlayerSQLClient:
    return SlayerSQLClient(
        datasource=DatasourceConfig(
            name=name, type="sqlite", connection_string="sqlite:///:memory:",
        ),
    )


# ---------------------------------------------------------------------------
# 3.1 Module guard — one door, text() gone
# ---------------------------------------------------------------------------


def _is_text_call(func: ast.expr) -> bool:
    """A SQLAlchemy text-clause construction: ``text(...)`` or ``sa.text(...)``."""
    if isinstance(func, ast.Name):
        return func.id == "text"
    if isinstance(func, ast.Attribute):
        return func.attr == "text" and isinstance(func.value, ast.Name) and func.value.id in {"sa", "sqlalchemy"}
    return False


def _helper_spans(tree: ast.Module) -> list[tuple[int, int]]:
    spans = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in _HELPER_NAMES:
            spans.append((node.lineno, node.end_lineno or node.lineno))
    return spans


class TestClientModuleGuard:
    """arc42 sql §3.13: the client constructs no text clause and issues no
    driver-level execute outside the single verbatim helper pair."""

    def _tree(self) -> ast.Module:
        return ast.parse(Path(sql_client.__file__).read_text())

    def test_no_text_clause_constructed(self) -> None:
        tree = self._tree()
        offenders = [n.lineno for n in ast.walk(tree) if isinstance(n, ast.Call) and _is_text_call(n.func)]
        assert not offenders, f"sa.text(...) still constructed at lines {offenders}"

    def test_no_execute_outside_helper_pair(self) -> None:
        tree = self._tree()
        spans = _helper_spans(tree)
        offenders = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            if isinstance(func, ast.Attribute) and func.attr in {"execute", "exec_driver_sql"}:
                if not any(lo <= node.lineno <= hi for lo, hi in spans):
                    offenders.append((func.attr, node.lineno))
        assert not offenders, f".execute/.exec_driver_sql outside the helper pair at {offenders}"

    def test_verbatim_helper_pair_exists(self) -> None:
        """The door is a real pair — 'one door' can't be satisfied by deleting
        execution entirely; both helpers must be defined for the guards above to
        mean anything."""
        defined = {
            n.name for n in ast.walk(self._tree())
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
        }
        assert _HELPER_NAMES <= defined, f"missing verbatim helper(s): {_HELPER_NAMES - defined}"


# ---------------------------------------------------------------------------
# 3.2 Every rendered-SQL door on in-memory SQLite
# ---------------------------------------------------------------------------


class TestSqliteDoorsPassRegexLiteralVerbatim:
    """Each public and private rendered-SQL path executes the ``:too`` literal
    unchanged. Through text() every one of these raised
    ``A value is required for bind parameter 'too'``."""

    async def test_execute_async(self) -> None:
        rows = await _in_memory_client().execute(_DOOR_SQL)
        assert rows == [_DOOR_ROW]

    def test_execute_sync(self) -> None:
        rows = _in_memory_client().execute_sync(_DOOR_SQL)
        assert rows == [_DOOR_ROW]

    async def test_get_column_types(self) -> None:
        types = await _in_memory_client().get_column_types(_DOOR_SQL)
        assert types == _DOOR_TYPES

    def test_execute_sql_sync_direct(self) -> None:
        engine = engine_factory.build_in_memory_sqlite_engine("sqlite:///:memory:")
        try:
            rows = sql_client._execute_sql_sync(
                sql=_DOOR_SQL, db_type="sqlite", engine=engine,
            )
        finally:
            engine.dispose()
        assert rows == [_DOOR_ROW]

    def test_get_column_types_sync_direct(self) -> None:
        engine = engine_factory.build_in_memory_sqlite_engine("sqlite:///:memory:")
        try:
            types = sql_client._get_column_types_sync(
                sql=_DOOR_SQL, db_type="sqlite", engine=engine,
            )
        finally:
            engine.dispose()
        assert types == _DOOR_TYPES


# ---------------------------------------------------------------------------
# 3.3 Async paths route every statement through the verbatim door
# ---------------------------------------------------------------------------


def _fake_async_conn() -> MagicMock:
    """A mock async connection whose ``exec_driver_sql``/``execute`` both return a
    one-row result; the test asserts which one the code actually awaits."""
    result = MagicMock()
    result.keys.return_value = ["pat", "ym"]
    result.fetchall.return_value = [(_REGEX_LITERAL, "2020-03")]
    result.cursor.description = None  # forces value-based type inference
    conn = MagicMock()
    conn.exec_driver_sql = AsyncMock(return_value=result)
    conn.execute = AsyncMock(return_value=result)
    conn.rollback = AsyncMock()
    return conn


def _fake_async_engine(conn: MagicMock) -> MagicMock:
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=conn)
    ctx.__aexit__ = AsyncMock(return_value=False)
    engine = MagicMock()
    engine.connect.return_value = ctx
    return engine


def _assert_all_verbatim(conn: MagicMock) -> None:
    """Every awaited driver call used the verbatim door with no parameter set,
    and the text-compiling ``execute`` was never touched."""
    assert conn.exec_driver_sql.await_count >= 1
    for call in conn.exec_driver_sql.await_args_list:
        assert call.kwargs.get("execution_options") == {"no_parameters": True}
    assert conn.execute.await_count == 0


class TestAsyncPathsUseVerbatimDoor:

    async def test_execute_sql_async(self) -> None:
        conn = _fake_async_conn()
        rows = await sql_client._execute_sql_async(
            sql=_DOOR_SQL, engine=_fake_async_engine(conn), db_type="postgres", timeout_seconds=30,
        )
        assert rows == [_DOOR_ROW]
        _assert_all_verbatim(conn)
        # The timeout SET *and* the query both went through the door.
        statements = [call.args[0] for call in conn.exec_driver_sql.await_args_list]
        assert _DOOR_SQL in statements
        assert any(stmt.startswith("SET statement_timeout") for stmt in statements)

    async def test_get_column_types_async(self) -> None:
        conn = _fake_async_conn()
        types = await sql_client._get_column_types_async(
            sql=_DOOR_SQL, engine=_fake_async_engine(conn), db_type="postgres",
        )
        assert types == _DOOR_TYPES
        _assert_all_verbatim(conn)
        # The read-only-transaction SET went through the door too.
        statements = [call.args[0] for call in conn.exec_driver_sql.await_args_list]
        assert "SET TRANSACTION READ ONLY" in statements


def _fake_sync_conn() -> MagicMock:
    result = MagicMock()
    result.keys.return_value = ["pat", "ym"]
    result.fetchall.return_value = [(_REGEX_LITERAL, "2020-03")]
    result.cursor.description = None
    conn = MagicMock()
    conn.exec_driver_sql.return_value = result
    conn.execute.return_value = result
    return conn


def _fake_sync_engine(conn: MagicMock) -> MagicMock:
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value = conn
    return engine


def _assert_all_verbatim_sync(conn: MagicMock) -> None:
    """The ``no_parameters`` option (the crux of the ``%`` fix) is pinned on the
    sync door too, not only the async one and the CI-only Postgres path."""
    assert conn.exec_driver_sql.call_count >= 1
    for call in conn.exec_driver_sql.call_args_list:
        assert call.kwargs.get("execution_options") == {"no_parameters": True}
    assert conn.execute.call_count == 0


class TestSyncPathsUseVerbatimDoor:

    def test_execute_sql_sync(self) -> None:
        conn = _fake_sync_conn()
        rows = sql_client._execute_sql_sync(
            sql=_DOOR_SQL, db_type="postgres", timeout_seconds=30,
            engine=_fake_sync_engine(conn),
        )
        assert rows == [_DOOR_ROW]
        _assert_all_verbatim_sync(conn)
        statements = [call.args[0] for call in conn.exec_driver_sql.call_args_list]
        assert _DOOR_SQL in statements
        assert any(stmt.startswith("SET statement_timeout") for stmt in statements)

    def test_get_column_types_sync(self) -> None:
        conn = _fake_sync_conn()
        types = sql_client._get_column_types_sync(
            sql=_DOOR_SQL, db_type="postgres",
            engine=_fake_sync_engine(conn),
        )
        assert types == _DOOR_TYPES
        _assert_all_verbatim_sync(conn)
        statements = [call.args[0] for call in conn.exec_driver_sql.call_args_list]
        assert "SET TRANSACTION READ ONLY" in statements


# ---------------------------------------------------------------------------
# 3.4 End to end: an ad-hoc ModelExtension regex column
# ---------------------------------------------------------------------------


class TestModelExtensionRegexColumnEndToEnd:
    """The reported path: a query extends its model with a column whose SQL holds
    the regex literal, then runs through the engine. The rendered SQL carries
    ``:too``; text() failed it, the verbatim door runs it."""

    async def _seed_engine(self, tmp_path) -> SlayerQueryEngine:
        db_path = tmp_path / "dev1933.sqlite"
        with transaction(db_path) as conn:
            conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, status TEXT NOT NULL)")
            conn.executemany(
                "INSERT INTO orders VALUES (?, ?)",
                [(1, "completed"), (2, "pending"), (3, "pending"), (4, "cancelled")],
            )

        storage = YAMLStorage(base_dir=str(tmp_path / "storage"))
        await storage.save_datasource(
            DatasourceConfig(name="ds", type="sqlite", database=str(db_path))
        )
        await storage.save_model(SlayerModel(
            name="orders", sql_table="orders", data_source="ds",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
            ],
        ))
        return SlayerQueryEngine(storage=storage)

    async def test_regex_extension_column_as_dimension(self, tmp_path) -> None:
        engine = await self._seed_engine(tmp_path)
        query = SlayerQuery(
            source_model=ModelExtension(
                source_name="orders",
                columns=[Column(
                    name="rx",
                    sql=f"CASE WHEN status = '{_REGEX_LITERAL}' THEN 1 ELSE 0 END",
                    type=DataType.DOUBLE,
                )],
            ),
            dimensions=[ColumnRef(name="rx")],
            measures=[ModelMeasure(formula="*:count")],
        )
        result = await engine.execute(query=query)
        by_rx = {int(row["orders.rx"]): row["orders._count"] for row in result.data}
        assert by_rx == {0: 4}
