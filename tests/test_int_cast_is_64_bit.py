"""A cast to INT is 64-bit (spec: sql/statement-assembly)."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import duckdb
import pytest
from pytest_postgresql import factories

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql import engine_factory
from slayer.storage.yaml_storage import YAMLStorage
from tests._engine_helpers import _engine_generate
from tests._pg_helpers import pg_database

postgresql_proc = factories.postgresql_proc(port=None)

_BIG = "id * 3000000000"
_QUERY = SlayerQuery.model_validate({"source_model": "t", "dimensions": ["big"], "order": [{"column": "big"}]})


def _model(sql: str, *, data_source: str = "ds") -> SlayerModel:
    return SlayerModel(name="t", sql_table="t", data_source=data_source, columns=[
        Column(name="id", type=DataType.INT, primary_key=True),
        Column(name="big", sql=sql, type=DataType.INT),
    ])


@pytest.mark.parametrize(("dialect", "target"), [
    ("postgres", "BIGINT"),
    ("duckdb", "BIGINT"),
    ("sqlite", "INTEGER"),
    ("mysql", "SIGNED"),
    ("tsql", "BIGINT"),
    ("snowflake", "BIGINT"),
    ("bigquery", "INT64"),
    ("clickhouse", "Nullable(Int64)"),
    ("trino", "BIGINT"),
])
async def test_derived_int_column_casts_to_the_64_bit_type(dialect: str, target: str) -> None:
    sql = await _engine_generate(query=_QUERY, model=_model(_BIG), dialect=dialect)
    assert f"3000000000 AS {target})" in sql, sql


async def test_no_double_cast() -> None:
    sql = await _engine_generate(query=_QUERY, model=_model(f"CAST({_BIG} AS BIGINT)"), dialect="postgres")
    projection = next(line for line in sql.splitlines() if '"t.big"' in line and "CAST(" in line)
    assert projection.count("CAST(") == 1, sql


async def _values(tmp_path: Path, ds: DatasourceConfig) -> list[Any]:
    storage = YAMLStorage(base_dir=str(tmp_path / "models"))
    await storage.save_datasource(ds)
    await storage.save_model(_model(_BIG, data_source=ds.name))
    engine = SlayerQueryEngine(storage=storage)
    try:
        return [row["t.big"] for row in (await engine.execute(_QUERY)).data]
    finally:
        engine.close()


@pytest.fixture
def duckdb_ds(tmp_path: Path) -> Iterator[DatasourceConfig]:
    path = str(tmp_path / "big.duckdb")
    con = duckdb.connect(path)
    try:
        con.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        con.execute("INSERT INTO t VALUES (1), (2)")
    finally:
        con.close()
    ds = DatasourceConfig(name="big_duckdb", type="duckdb", database=path)
    yield ds
    engine_factory.invalidate_engine(ds)


@pytest.fixture
def postgres_ds(postgresql_proc: Any) -> Iterator[DatasourceConfig]:
    with pg_database(postgresql_proc, statements=[
        "CREATE TABLE t (id INTEGER PRIMARY KEY)", "INSERT INTO t VALUES (1), (2)",
    ], name="big_pg") as ds:
        yield ds


@pytest.mark.parametrize("backend", ["duckdb_ds", "postgres_ds"])
async def test_values_above_2_31_survive(tmp_path: Path, request: pytest.FixtureRequest, backend: str) -> None:
    assert await _values(tmp_path, request.getfixturevalue(backend)) == [3_000_000_000, 6_000_000_000]
