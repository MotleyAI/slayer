"""The executable client contract (spec: database/client-contract — the client contract is executable)."""

from __future__ import annotations

import ast
import uuid
from collections.abc import AsyncIterator, Callable
from pathlib import Path
from typing import Any, LiteralString, cast

import duckdb
import psycopg
import pytest
from psycopg import sql as pg_sql
from pytest_postgresql import factories

import slayer.database.conformance as conformance
from slayer.core.models import DatasourceConfig
from slayer.database import engine_factory
from slayer.core.enums import DataType
from slayer.database.client import (
    DelegatingSqlClient, ExecutionResult, ObjectNotFound, ResultColumnType, SlayerSQLClient, SqlClientError,
)
from slayer.sql.dialects.base import ExplainStatements
from slayer.database.conformance import ConformanceReport, capabilities_for, fixture_statements, run_conformance
from slayer.storage.sqlite_conn import transaction

postgresql_proc = factories.postgresql_proc(port=None)


def _sqlite(tmp_path: Path) -> DatasourceConfig:
    path = str(tmp_path / "conformance.db")
    with transaction(path) as cur:
        for statement in fixture_statements(dialect="sqlite"):
            cur.execute(statement)
    return DatasourceConfig(name="conf_sqlite", type="sqlite", database=path)


def _duckdb(tmp_path: Path) -> DatasourceConfig:
    path = str(tmp_path / "conformance.duckdb")
    con = duckdb.connect(path)
    try:
        for statement in fixture_statements(dialect="duckdb"):
            con.execute(statement)
    finally:
        con.close()
    return DatasourceConfig(name="conf_duckdb", type="duckdb", database=path)


@pytest.fixture
def postgres_datasource(postgresql_proc: Any) -> Any:
    info = postgresql_proc
    db_name = f"conf_{uuid.uuid4().hex[:12]}"
    with psycopg.connect(host=info.host, port=info.port, user=info.user, dbname="postgres", autocommit=True) as admin:
        admin.execute(pg_sql.SQL("CREATE DATABASE {}").format(pg_sql.Identifier(db_name)))
    with psycopg.connect(host=info.host, port=info.port, user=info.user, dbname=db_name) as conn:
        for statement in fixture_statements(dialect="postgres"):
            conn.execute(cast(LiteralString, statement))
        conn.commit()
    ds = DatasourceConfig(
        name="conf_pg", type="postgres", host=info.host, port=info.port,
        database=db_name, username=info.user, password="",
    )
    try:
        yield ds
    finally:
        engine_factory.invalidate_engine(ds)
        with psycopg.connect(host=info.host, port=info.port, user=info.user, dbname="postgres", autocommit=True) as admin:
            admin.execute(pg_sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(pg_sql.Identifier(db_name)))


async def _run(client: Any, *, dialect: str) -> ConformanceReport:
    try:
        return await run_conformance(client, capabilities=capabilities_for(dialect=dialect))
    finally:
        await client.aclose()
        client.close()


@pytest.fixture
async def sqlite_ds(tmp_path: Path) -> AsyncIterator[DatasourceConfig]:
    ds = _sqlite(tmp_path)
    yield ds
    engine_factory.invalidate_engine(ds)


class TestDefaultClientConforms:
    @pytest.mark.parametrize(("dialect", "build"), [("sqlite", _sqlite), ("duckdb", _duckdb)])
    async def test_on_local_backends(
        self, tmp_path: Path, dialect: str, build: Callable[[Path], DatasourceConfig],
    ) -> None:
        ds = build(tmp_path)
        try:
            report = await _run(SlayerSQLClient(datasource=ds), dialect=dialect)
        finally:
            engine_factory.invalidate_engine(ds)
        assert report.failed == []
        assert report.passed

    async def test_unrepresentable_checks_are_not_applicable_on_sqlite(self, sqlite_ds: DatasourceConfig) -> None:
        report = await _run(SlayerSQLClient(datasource=sqlite_ds), dialect="sqlite")
        assert report.not_applicable
        assert not set(report.not_applicable) & set(report.failed)

    async def test_on_postgres(self, postgres_datasource: DatasourceConfig) -> None:
        report = await _run(SlayerSQLClient(datasource=postgres_datasource), dialect="postgres")
        assert report.failed == []
        assert report.passed


class _EmptyForMissing(DelegatingSqlClient):
    """Broken: a missing table reads as having no columns."""

    async def reflect_columns(self, *args: Any, **kwargs: Any) -> Any:
        try:
            return await self.inner.reflect_columns(*args, **kwargs)
        except ObjectNotFound:
            return []


class _DeadAfterAclose(DelegatingSqlClient):
    """Broken: statements fail once ``aclose()`` has run."""

    closed = False

    async def aclose(self) -> None:
        self.closed = True
        await self.inner.aclose()

    async def execute(self, *args: Any, **kwargs: Any) -> Any:
        if self.closed:
            raise RuntimeError("client is closed")
        return await self.inner.execute(*args, **kwargs)


class _StatementsOnly(DelegatingSqlClient):
    async def execute(self, *args: Any, **kwargs: Any) -> Any:
        return await self.inner.execute(*args, **kwargs)

    async def get_result_column_types(self, *args: Any, **kwargs: Any) -> Any:
        return await self.inner.get_result_column_types(*args, **kwargs)


class _IntegersAsDouble(DelegatingSqlClient):
    """Broken: every integer result column is reported DOUBLE."""

    async def get_result_column_types(self, sql: str) -> dict[str, ResultColumnType]:
        types = await self.inner.get_result_column_types(sql)
        return {
            name: column.model_copy(update={"type": DataType.DOUBLE}) if column.type == DataType.INT else column
            for name, column in types.items()
        }


class _GenericErrors(DelegatingSqlClient):
    """Broken: a database error reaches the caller as a generic one."""

    async def execute(self, sql: str, **kwargs: Any) -> ExecutionResult:
        try:
            return await self.inner.execute(sql, **kwargs)
        except SqlClientError:
            raise
        except Exception as exc:  # noqa: BLE001 — the broken behaviour under test
            raise RuntimeError("query failed") from exc


class TestBrokenClientsAreCaught:
    async def test_a_rewritten_database_error_fails_its_check(self, sqlite_ds: DatasourceConfig) -> None:
        report = await _run(_GenericErrors(inner=SlayerSQLClient(datasource=sqlite_ds)), dialect="sqlite")
        assert "execute_error_unchanged" in report.failed, report.failures

    async def test_each_failed_check_is_named(self, sqlite_ds: DatasourceConfig) -> None:
        good = await _run(SlayerSQLClient(datasource=sqlite_ds), dialect="sqlite")
        empty = await _run(_EmptyForMissing(inner=SlayerSQLClient(datasource=sqlite_ds)), dialect="sqlite")
        dead = await _run(_DeadAfterAclose(inner=SlayerSQLClient(datasource=sqlite_ds)), dialect="sqlite")
        assert any("not_found" in check for check in empty.failed), empty.failed
        assert any("aclose" in check for check in dead.failed), dead.failed
        assert "get_result_column_types" in good.passed
        assert set(empty.failed) <= set(good.passed)
        assert set(dead.failed) <= set(good.passed)


class TestTheTypedProbeIsChecked:
    async def test_integers_reported_double_fail_the_check(self, sqlite_ds: DatasourceConfig) -> None:
        report = await _run(_IntegersAsDouble(inner=SlayerSQLClient(datasource=sqlite_ds)), dialect="sqlite")
        assert report.failed == ["get_result_column_types"]


class _Probe160Only:
    """A 1.6.0 client: every current member but the typed probe, which it serves as ``get_column_types``."""

    def __init__(self, inner: SlayerSQLClient) -> None:
        self._inner = inner

    def __getattr__(self, name: str) -> Any:
        if name == "get_result_column_types":
            raise AttributeError(name)
        return getattr(self._inner, name)

    async def get_column_types(self, sql: str) -> dict[str, str]:  # noqa: ARG002 — 1.6.0 signature
        return {"id": "number", "name": "string"}


class TestA160ClientIsCheckedAgainstTheCurrentContract:
    async def test_its_typed_probe_check_fails(self, sqlite_ds: DatasourceConfig) -> None:
        report = await _run(_Probe160Only(inner=SlayerSQLClient(datasource=sqlite_ds)), dialect="sqlite")
        assert report.failed == ["get_result_column_types"], report.failures


class TestStatementsOnlyDelegatingClientConforms:
    async def test_conforms(self, sqlite_ds: DatasourceConfig) -> None:
        report = await _run(_StatementsOnly(inner=SlayerSQLClient(datasource=sqlite_ds)), dialect="sqlite")
        assert report.failed == []



class _NoExplain:
    """Broken: every current member but ``explain``."""

    def __init__(self, inner: SlayerSQLClient) -> None:
        self._inner = inner

    def __getattr__(self, name: str) -> Any:
        if name == "explain":
            raise AttributeError(name)
        return getattr(self._inner, name)


class _PlanModeLeftOn(DelegatingSqlClient):
    """Broken: after an explain, statements return plan rows."""

    plan_mode = False

    async def explain(self, statements: ExplainStatements) -> ExecutionResult:
        result = await self.inner.explain(statements)
        self.plan_mode = True
        return result

    async def execute(self, sql: str) -> ExecutionResult:
        if self.plan_mode:
            return ExecutionResult(rows=[{"detail": f"SCAN {sql}"}])
        return await self.inner.execute(sql)


class TestExplainIsChecked:
    @pytest.mark.parametrize(("dialect", "build"), [("sqlite", _sqlite), ("duckdb", _duckdb)])
    async def test_the_default_client_passes_on_local_backends(
        self, tmp_path: Path, dialect: str, build: Callable[[Path], DatasourceConfig],
    ) -> None:
        ds = build(tmp_path)
        try:
            report = await _run(SlayerSQLClient(datasource=ds), dialect=dialect)
        finally:
            engine_factory.invalidate_engine(ds)
        assert "explain" in report.passed, report.failures

    async def test_the_default_client_passes_on_postgres(self, postgres_datasource: DatasourceConfig) -> None:
        report = await _run(SlayerSQLClient(datasource=postgres_datasource), dialect="postgres")
        assert "explain" in report.passed, report.failures

    async def test_a_statements_only_delegating_client_passes(self, sqlite_ds: DatasourceConfig) -> None:
        report = await _run(_StatementsOnly(inner=SlayerSQLClient(datasource=sqlite_ds)), dialect="sqlite")
        assert "explain" in report.passed, report.failures

    async def test_a_client_without_explain_fails_it(self, sqlite_ds: DatasourceConfig) -> None:
        report = await _run(_NoExplain(inner=SlayerSQLClient(datasource=sqlite_ds)), dialect="sqlite")
        assert "explain" in report.failed

    async def test_an_explain_leaving_plan_mode_on_fails_it(self, sqlite_ds: DatasourceConfig) -> None:
        report = await _run(_PlanModeLeftOn(inner=SlayerSQLClient(datasource=sqlite_ds)), dialect="sqlite")
        assert "explain" in report.failed


def test_the_suite_needs_no_test_framework() -> None:
    assert conformance.__file__ is not None
    tree = ast.parse(Path(conformance.__file__).read_text(encoding="utf-8"))
    imported = {
        alias.name.split(".")[0]
        for node in ast.walk(tree) if isinstance(node, ast.Import)
        for alias in node.names
    } | {
        (node.module or "").split(".")[0]
        for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)
    }
    assert not imported & {"pytest", "unittest", "hypothesis"}
