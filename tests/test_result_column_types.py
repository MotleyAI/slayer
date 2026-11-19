"""The typed column-type probe on SQLite, DuckDB and Postgres (spec: database/client-contract)."""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any

import duckdb
import pytest
from pytest_postgresql import factories

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, DatasourceConfig, SlayerDataset
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.database import engine_factory
from slayer.database.client import (
    ResultColumnType,
    SlayerSQLClient,
    get_result_column_types_sync,
)
from slayer.sql.dialects import reflection
from slayer.sql.dialects.reflection import info_schema_type
from slayer.storage.sqlite_conn import transaction
from slayer.storage.yaml_storage import YAMLStorage
from tests._pg_helpers import pg_database

postgresql_proc = factories.postgresql_proc(port=None)

_SQLITE_DDL = [
    "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, d REAL, qty INTEGER)",
    "INSERT INTO t VALUES (1, 'ab', 1.5, 3), (2, 'cde', 2.5, 4)",
]
_DUCKDB_DDL = [
    "CREATE TABLE t (id INTEGER PRIMARY KEY, name VARCHAR, d DOUBLE, a DECIMAL(10,0), b DECIMAL(10,2))",
    "INSERT INTO t VALUES (1, 'ab', 1.5, 7, 7.25), (2, 'cde', 2.5, 8, 8.5)",
]
_PG_DDL = [
    "CREATE TYPE mood AS ENUM ('calm', 'busy')",
    "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, d DOUBLE PRECISION, a NUMERIC(10,0), b NUMERIC(10,2))",
    "INSERT INTO t VALUES (1, 'ab', 1.5, 7, 7.25), (2, 'cde', 2.5, 8, 8.5)",
]


@pytest.fixture
def sqlite_ds(tmp_path: Path) -> Iterator[DatasourceConfig]:
    path = str(tmp_path / "probe.db")
    with transaction(path) as cur:
        for statement in _SQLITE_DDL:
            cur.execute(statement)
    ds = DatasourceConfig(name="probe_sqlite", type="sqlite", database=path)
    yield ds
    engine_factory.invalidate_engine(ds)


@pytest.fixture
def duckdb_ds(tmp_path: Path) -> Iterator[DatasourceConfig]:
    path = str(tmp_path / "probe.duckdb")
    con = duckdb.connect(path)
    try:
        for statement in _DUCKDB_DDL:
            con.execute(statement)
    finally:
        con.close()
    ds = DatasourceConfig(name="probe_duckdb", type="duckdb", database=path)
    yield ds
    engine_factory.invalidate_engine(ds)


@pytest.fixture
def postgres_ds(postgresql_proc: Any) -> Iterator[DatasourceConfig]:
    with pg_database(postgresql_proc, statements=_PG_DDL, name="probe_pg") as ds:
        yield ds


@pytest.fixture
def backend(request: pytest.FixtureRequest) -> DatasourceConfig:
    return request.getfixturevalue(f"{request.param}_ds")


async def _probe(ds: DatasourceConfig, sql: str) -> dict[str, ResultColumnType]:
    client = SlayerSQLClient(datasource=ds)
    try:
        return await client.get_result_column_types(sql)
    finally:
        await client.aclose()
        client.close()


def _types(result: dict[str, ResultColumnType]) -> dict[str, DataType]:
    return {name: column.type for name, column in result.items()}


def _db_types(result: dict[str, ResultColumnType]) -> dict[str, str | None]:
    return {name: column.db_type for name, column in result.items()}


def test_result_column_type_is_reexported_from_the_client() -> None:
    assert ResultColumnType is reflection.ResultColumnType
    assert ResultColumnType(type=DataType.INTEGER).db_type is None


class TestIntegerResultsAreInt:
    _SQL = "SELECT id, LENGTH(name) AS len, COUNT(*) OVER () AS cnt FROM t"
    _DB_TYPES = {
        "sqlite": {"id": None, "len": None, "cnt": None},
        "duckdb": {"id": "INTEGER", "len": "BIGINT", "cnt": "BIGINT"},
        "postgres": {"id": "integer", "len": "integer", "cnt": "bigint"},
    }

    @pytest.mark.parametrize("backend", ["sqlite", "duckdb", "postgres"], indirect=True)
    async def test_integers(self, backend: DatasourceConfig) -> None:
        result = await _probe(backend, self._SQL)
        assert _types(result) == {"id": DataType.INTEGER, "len": DataType.INTEGER, "cnt": DataType.INTEGER}
        assert _db_types(result) == self._DB_TYPES[backend.type or ""]


class TestExactNumericsAreTypedByScale:
    @pytest.mark.parametrize(("backend", "names"), [
        ("duckdb", ("DECIMAL(10,0)", "DECIMAL(10,2)")),
        ("postgres", ("numeric(10,0)", "numeric(10,2)")),
    ], indirect=["backend"])
    async def test_scale(self, backend: DatasourceConfig, names: tuple[str, str]) -> None:
        result = await _probe(backend, "SELECT a, b FROM t")
        assert result == {
            "a": ResultColumnType(type=DataType.INTEGER, db_type=names[0]),
            "b": ResultColumnType(type=DataType.FLOAT, db_type=names[1]),
        }


class TestFloatsStayDouble:
    @pytest.mark.parametrize("backend", ["sqlite", "duckdb", "postgres"], indirect=True)
    async def test_double_and_average(self, backend: DatasourceConfig) -> None:
        result = await _probe(backend, "SELECT d, AVG(id) OVER () AS av FROM t")
        assert _types(result) == {"d": DataType.FLOAT, "av": DataType.FLOAT}


class TestPostgresAnswersTheSameFromEitherPath:
    _SQL = "SELECT id, CAST(id AS NUMERIC(12,0)) AS n0, CAST(id AS NUMERIC(12,3)) AS n3 FROM t"

    async def test_async_and_sync_agree(self, postgres_ds: DatasourceConfig) -> None:
        from_async = await _probe(postgres_ds, self._SQL)
        from_sync = get_result_column_types_sync(
            self._SQL, engine=engine_factory.get_engine(postgres_ds), db_type="postgres",
        )
        assert from_async == from_sync
        assert from_async == {
            "id": ResultColumnType(type=DataType.INTEGER, db_type="integer"),
            "n0": ResultColumnType(type=DataType.INTEGER, db_type="numeric(12,0)"),
            "n3": ResultColumnType(type=DataType.FLOAT, db_type="numeric(12,3)"),
        }


class TestSqliteCertifiesIntegersFromTheData:
    @pytest.mark.parametrize(("sql", "expected"), [
        pytest.param("SELECT 1 AS n UNION ALL SELECT 2", DataType.INTEGER, id="integers"),
        pytest.param("SELECT 1 AS n UNION ALL SELECT NULL", DataType.INTEGER, id="integers-and-null"),
        pytest.param("SELECT 1 AS n UNION ALL SELECT 2.5", DataType.FLOAT, id="mixed"),
        pytest.param("SELECT 1 AS n UNION ALL SELECT 2.0", DataType.FLOAT, id="integral-real"),
        pytest.param(
            "WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM s WHERE n < 100000) SELECT n FROM s",
            DataType.INTEGER, id="at-the-cap",
        ),
        pytest.param(
            "WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM s WHERE n < 100001) SELECT n FROM s",
            DataType.FLOAT, id="saturated",
        ),
        pytest.param("SELECT '42' AS n", DataType.STRING, id="integer-looking-text"),
        pytest.param("SELECT 1 AS n UNION ALL SELECT 'x'", DataType.STRING, id="integer-and-text"),
        pytest.param("SELECT 1 AS n WHERE 0", DataType.STRING, id="no-rows"),
        pytest.param("SELECT NULL AS n", DataType.STRING, id="only-null"),
    ])
    async def test_verdict(self, sqlite_ds: DatasourceConfig, sql: str, expected: DataType) -> None:
        assert await _probe(sqlite_ds, sql) == {"n": ResultColumnType(type=expected)}


class TestContainersAreNotIntegers:
    async def test_list_and_struct(self, duckdb_ds: DatasourceConfig) -> None:
        result = await _probe(duckdb_ds, "SELECT [1, 2]::INTEGER[] AS l, {'a': 1} AS s")
        assert _db_types(result) == {"l": "INTEGER[]", "s": "STRUCT(a INTEGER)"}
        for column in result.values():
            assert column.type != DataType.INTEGER
            assert column.db_type is not None
            assert column.type == info_schema_type(column.db_type)[0]


class TestOpaqueAndUnrecognisedTypes:
    async def test_postgres(self, postgres_ds: DatasourceConfig) -> None:
        result = await _probe(
            postgres_ds,
            "SELECT '{}'::json AS j, '{}'::jsonb AS jb, "
            "'00000000-0000-0000-0000-000000000000'::uuid AS u, 'calm'::mood AS e",
        )
        assert result == {
            "j": ResultColumnType(type=DataType.OPAQUE, db_type="json"),
            "jb": ResultColumnType(type=DataType.STRING, db_type="jsonb"),
            "u": ResultColumnType(type=DataType.STRING, db_type="uuid"),
            "e": ResultColumnType(type=DataType.STRING),
        }


@pytest.fixture
async def engine(tmp_path: Path, sqlite_ds: DatasourceConfig) -> AsyncIterator[SlayerQueryEngine]:
    storage = YAMLStorage(base_dir=str(tmp_path / "models"))
    await storage.save_datasource(sqlite_ds)
    await storage.save_dataset(SlayerDataset(
        name="t", sql_table="t", data_source=sqlite_ds.name,
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="name", datatype=DataType.STRING),
            DatasetField(name="d", datatype=DataType.FLOAT),
            DatasetField(name="qty", datatype=DataType.INTEGER),
        ],
    ))
    engine = SlayerQueryEngine(storage=storage)
    try:
        yield engine
    finally:
        engine.close()


class TestTheEngineProbe:
    async def test_typed(self, engine: SlayerQueryEngine) -> None:
        result = await engine.get_result_column_types("t")
        assert _types(result) == {"name": DataType.STRING, "d": DataType.FLOAT, "qty": DataType.INTEGER}
