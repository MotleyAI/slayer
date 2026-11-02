"""The executable client contract (spec: sql/client-contract — the client contract is executable)."""

from __future__ import annotations

import ast
import uuid
from collections.abc import AsyncIterator, Callable
from pathlib import Path
from typing import Any

import duckdb
import psycopg
import pytest
from psycopg import sql as pg_sql
from pytest_postgresql import factories

import slayer.sql.conformance as conformance
from slayer.core.models import DatasourceConfig
from slayer.sql import engine_factory
from slayer.sql.client import DelegatingSqlClient, ObjectNotFound, SlayerSQLClient
from slayer.sql.conformance import ConformanceReport, capabilities_for, fixture_statements, run_conformance
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
            conn.execute(statement)
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

    async def get_column_types(self, *args: Any, **kwargs: Any) -> Any:
        return await self.inner.get_column_types(*args, **kwargs)


class TestBrokenClientsAreCaught:
    async def test_each_failed_check_is_named(self, sqlite_ds: DatasourceConfig) -> None:
        good = await _run(SlayerSQLClient(datasource=sqlite_ds), dialect="sqlite")
        empty = await _run(_EmptyForMissing(inner=SlayerSQLClient(datasource=sqlite_ds)), dialect="sqlite")
        dead = await _run(_DeadAfterAclose(inner=SlayerSQLClient(datasource=sqlite_ds)), dialect="sqlite")
        assert any("not_found" in check for check in empty.failed), empty.failed
        assert any("aclose" in check for check in dead.failed), dead.failed
        assert set(empty.failed) <= set(good.passed)
        assert set(dead.failed) <= set(good.passed)


class TestStatementsOnlyDelegatingClientConforms:
    async def test_conforms(self, sqlite_ds: DatasourceConfig) -> None:
        report = await _run(_StatementsOnly(inner=SlayerSQLClient(datasource=sqlite_ds)), dialect="sqlite")
        assert report.failed == []


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
