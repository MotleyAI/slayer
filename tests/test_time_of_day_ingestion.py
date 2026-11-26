"""Database time-of-day columns ingest as ``Time``; a stored field of another type over one is a type conflict a
re-ingest fixes (spec: queries/time-of-day › Database time-of-day columns ingest as Time)."""

from __future__ import annotations

import contextlib
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import mssql, mysql, postgresql

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, DatasourceConfig, SlayerDataset
from slayer.database.client import SlayerSQLClient
from slayer.live_schema.ingestion import ingest_datasource_idempotent
from slayer.sql.dialects import base as dialects_base
from slayer.sql.dialects import dialect_for_ds_type
from slayer.sql.dialects.reflection import (
    TableLocation,
    column_from_sa_type,
    column_from_type_string,
    time_of_day_type,
)
from slayer.storage.sqlite_conn import transaction
from slayer.storage.yaml_storage import YAMLStorage
from tests._engine_helpers import owned_query_engine

TIME, OPAQUE = DataType.TIME, DataType.OPAQUE
#: A database's name for a column type and the type it ingests as.
TYPE_NAMES: list[tuple[str, DataType]] = [
    ("TIME", TIME), ("time", TIME), ("TIME(3)", TIME), ("TIME(6)", TIME), ("time(0)", TIME),
    ("TIME WITHOUT TIME ZONE", TIME), ("time without time zone", TIME), ("time(3) without time zone", TIME),
    ("Time", TIME), ("Time64(6)", TIME), ("Time64(3)", TIME), ("Nullable(Time)", TIME), ("Nullable(Time64(6))", TIME),
    ("TIME WITH TIME ZONE", OPAQUE), ("time with time zone", OPAQUE), ("time(3) with time zone", OPAQUE),
    ("TIMETZ", OPAQUE), ("timetz", OPAQUE),
    ("TIMESTAMP", DataType.DATETIME), ("DATETIME", DataType.DATETIME), ("DATE", DataType.DATE),
]
DDL = "CREATE TABLE clocks (id INTEGER PRIMARY KEY, opens TIME, zoned TIME WITH TIME ZONE, label VARCHAR)"


def _duckdb(tmp_path: Path, *statements: str) -> DatasourceConfig:
    duckdb = pytest.importorskip("duckdb")
    path = str(tmp_path / "live.duckdb")
    with contextlib.closing(duckdb.connect(path)) as con:
        for statement in statements:
            con.execute(statement)
    return DatasourceConfig(name="d", type="duckdb", database=path)


def _authored(name: str, datatype: DataType) -> DatasetField:
    return DatasetField(name=name, datatype=datatype, description=f"{name} description", label=f"{name} label",
                        meta={"owner": name}, hidden=True)


def _attrs(field: DatasetField) -> dict[str, Any]:
    return field.model_dump(exclude={"datatype"})


class TestTypeNames:
    @pytest.mark.parametrize(("name", "expected"), TYPE_NAMES, ids=[n for n, _ in TYPE_NAMES])
    def test_a_catalog_type_name(self, name: str, expected: DataType) -> None:
        assert column_from_type_string(name="c", data_type=name).narrowed_type is expected

    @pytest.mark.parametrize("name", [
        "TIMESTAMP", "TIME(x)", "TIME(3", "TIME ZONE", "TIMETZ WITH TIME ZONE", "TIME WITH ZONE", "INTERVAL",
    ])
    def test_a_name_that_is_no_time_of_day(self, name: str) -> None:
        assert time_of_day_type(name) is None

    @pytest.mark.parametrize(("name", "expected"), [
        ("  time ( 3 )  WITHOUT  time zone ", TIME), ("Time64 WITH TIME ZONE", OPAQUE), ("timetz(6)", OPAQUE),
    ])
    def test_spacing_and_case_do_not_matter(self, name: str, expected: DataType) -> None:
        assert time_of_day_type(name) is expected

    @pytest.mark.parametrize(("sa_type", "sa_dialect", "expected"), [
        pytest.param(sa.Time(), postgresql.dialect(), TIME, id="generic"),
        pytest.param(postgresql.TIME(), postgresql.dialect(), TIME, id="postgres"),
        pytest.param(postgresql.TIME(precision=3), postgresql.dialect(), TIME, id="postgres-precision"),
        pytest.param(postgresql.TIME(timezone=True), postgresql.dialect(), OPAQUE, id="postgres-zoned"),
        pytest.param(mysql.TIME(fsp=6), mysql.dialect(), TIME, id="mysql"),
        pytest.param(mssql.TIME(), mssql.dialect(), TIME, id="sql-server"),
    ])
    def test_an_inspector_type(self, sa_type: Any, sa_dialect: Any, expected: DataType) -> None:
        column = column_from_sa_type(name="c", sa_type=sa_type, sa_dialect=sa_dialect)
        assert column.narrowed_type is expected


class TestClickhouseCatalogTypes:
    def test_a_column_the_driver_cannot_type_reads_its_catalog_type(self) -> None:
        """The driver knows no ``Time``; a ``Nullable`` around it is still untyped."""
        clickhouse_types = pytest.importorskip("clickhouse_sqlalchemy.types")
        raw = [{"name": "t", "type": sa.types.NullType()},
               {"name": "n", "type": clickhouse_types.Nullable(sa.types.NullType())},
               {"name": "s", "type": clickhouse_types.Nullable(clickhouse_types.String())}]
        declared = [("t", "Time"), ("n", "Nullable(Time)"), ("s", "Nullable(String)")]
        with patch.object(dialects_base, "fetch", return_value=declared):
            columns = dialect_for_ds_type("clickhouse").columns_from_inspector(
                raw, connection=None, inspector=None, location=TableLocation(name="clocks"))
        assert {c.name: c.narrowed_type for c in columns} == {"t": TIME, "n": TIME, "s": DataType.STRING}


class TestStatementProbe:
    @pytest.mark.parametrize(("ds_type", "row", "expected"), [
        pytest.param("postgres", ("c", 1083, None, None, None, None, True), TIME, id="postgres-time"),
        pytest.param("postgres", ("c", 1266, None, None, None, None, True), OPAQUE, id="postgres-timetz"),
        pytest.param("mysql", ("c", 11, None, 10, 10, 0, True), TIME, id="mysql-time"),
        pytest.param("mariadb", ("c", 11, None, 10, 10, 0, True), TIME, id="mariadb-time"),
        pytest.param("snowflake", ("c", 12, None, None, None, None, True), TIME, id="snowflake-time"),
    ])
    def test_a_driver_description(self, ds_type: str, row: tuple[Any, ...], expected: DataType) -> None:
        described = dialect_for_ds_type(ds_type).describe_cursor_column(row)
        assert described is not None
        assert described.type is expected

    async def test_duckdb_statement(self, tmp_path: Path) -> None:
        client = SlayerSQLClient(datasource=_duckdb(tmp_path, DDL))
        try:
            types = await client.get_result_column_types(
                "SELECT opens, zoned, CAST('10:30:00' AS TIME) AS made, label FROM clocks")
        finally:
            await client.aclose()
            client.close()
        assert {name: column.type for name, column in types.items()} == {
            "opens": TIME, "zoned": OPAQUE, "made": TIME, "label": DataType.STRING}

    async def test_sqlite_declared_type(self, tmp_path: Path) -> None:
        path = str(tmp_path / "live.db")
        with transaction(path) as con:
            con.execute("CREATE TABLE clocks (id INTEGER PRIMARY KEY, opens TIME, label TEXT)")
            con.execute("INSERT INTO clocks VALUES (1, '09:30:00', 'a')")
        client = SlayerSQLClient(datasource=DatasourceConfig(name="d", type="sqlite", database=path))
        try:
            types = await client.get_result_column_types("SELECT opens, label FROM clocks")
        finally:
            await client.aclose()
            client.close()
        assert types["opens"].type is TIME


class TestIngestion:
    async def test_duckdb(self, tmp_path: Path) -> None:
        datasource = _duckdb(tmp_path, "CREATE TABLE clocks (id INTEGER PRIMARY KEY, opens TIME, zoned TIME WITH TIME "
                                       "ZONE, ts TIMESTAMP, label VARCHAR)")
        async with owned_query_engine(storage=YAMLStorage(base_dir=str(tmp_path / "store"))) as engine:
            [dataset] = await engine.ingest(datasource)
        assert {f.name: str(f.datatype) for f in dataset.fields} == {
            "id": "Integer", "opens": "Time", "zoned": "Opaque", "ts": "DateTime", "label": "String"}

    async def test_sqlite(self, tmp_path: Path) -> None:
        path = str(tmp_path / "live.db")
        with transaction(path) as con:
            con.execute("CREATE TABLE clocks (id INTEGER PRIMARY KEY, opens TIME, closes TIME(3), ts TIMESTAMP)")
        async with owned_query_engine(storage=YAMLStorage(base_dir=str(tmp_path / "store"))) as engine:
            [dataset] = await engine.ingest(DatasourceConfig(name="d", type="sqlite", database=path))
        assert {f.name: str(f.datatype) for f in dataset.fields} == {
            "id": "Integer", "opens": "Time", "closes": "Time", "ts": "DateTime"}

    async def test_an_ingested_time_field_is_queryable(self, tmp_path: Path) -> None:
        datasource = _duckdb(tmp_path, DDL, "INSERT INTO clocks VALUES (1, '09:30:00', NULL, 'a'), "
                                            "(2, '17:00:00', NULL, 'b')")
        storage = YAMLStorage(base_dir=str(tmp_path / "store"))
        async with owned_query_engine(storage=storage, datasources=[datasource]) as engine:
            await ingest_datasource_idempotent(datasource=datasource, storage=storage,
                                               client=engine.client_for(datasource))
            resp = await engine.execute({"population": "clocks", "metrics": [
                {"metric": "min(clocks.opens)", "alias": "lo"}], "filter": "clocks.opens < '12:00'"})
        assert [str(r["lo"]) for r in resp.data] == ["09:30:00"]


class TestDrift:
    async def _stored(self, tmp_path: Path, *, opens: DataType) -> tuple[DatasourceConfig, YAMLStorage]:
        datasource = _duckdb(tmp_path, DDL, "INSERT INTO clocks VALUES (1, '09:30:00', NULL, 'a')")
        storage = YAMLStorage(base_dir=str(tmp_path / "store"))
        await storage.save_datasource(datasource)
        fields = [DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True), _authored("opens", opens),
                  _authored("zoned", DataType.STRING), DatasetField(name="label", datatype=DataType.STRING)]
        await storage.save_dataset(SlayerDataset(name="clocks", sql_table="clocks", data_source="d", fields=fields))
        await storage.save_dataset(SlayerDataset(name="clocks_sql", sql="SELECT id, opens FROM clocks",
                                                 data_source="d", fields=fields[:2]))
        return datasource, storage

    @pytest.mark.parametrize("stored", [DataType.DATETIME, DataType.STRING])
    async def test_validation_reports_the_conflict_and_changes_nothing(self, tmp_path: Path, stored: DataType) -> None:
        datasource, storage = await self._stored(tmp_path, opens=stored)
        before = await storage.get_dataset("clocks", data_source="d")
        async with owned_query_engine(storage=storage, datasources=[datasource]) as engine:
            entries = await engine.validate_datasets(data_source="d")
        found = {(e["dataset_name"], e["field"]): (e["stored_type"], e["live_type"])
                 for entry in entries if (e := entry.model_dump(mode="json")).get("finding") == "retype"}
        assert found.get(("clocks", "opens")) == (stored.value, "Time")
        assert found.get(("clocks_sql", "opens")) == (stored.value, "Time")
        assert await storage.get_dataset("clocks", data_source="d") == before

    async def test_a_time_field_over_a_time_column_is_no_conflict(self, tmp_path: Path) -> None:
        datasource, storage = await self._stored(tmp_path, opens=TIME)
        async with owned_query_engine(storage=storage, datasources=[datasource]) as engine:
            entries = await engine.validate_datasets(data_source="d")
        assert [e for entry in entries if (e := entry.model_dump(mode="json")).get("field") == "opens"] == []

    @pytest.mark.parametrize("stored", [DataType.DATETIME, DataType.STRING])
    async def test_a_reingest_retypes_the_field_and_nothing_else(self, tmp_path: Path, stored: DataType) -> None:
        datasource, storage = await self._stored(tmp_path, opens=stored)
        before = await storage.get_dataset("clocks", data_source="d")
        assert before is not None
        async with owned_query_engine(storage=storage, datasources=[datasource]) as engine:
            await ingest_datasource_idempotent(datasource=datasource, storage=storage,
                                               client=engine.client_for(datasource), include_tables=["clocks"])
        after = await storage.get_dataset("clocks", data_source="d")
        assert after is not None
        got = {f.name: f for f in after.fields}
        was = {f.name: f for f in before.fields}
        assert got["opens"].datatype is TIME
        assert got["zoned"].datatype is DataType.STRING
        for name in ("opens", "zoned", "label"):
            assert _attrs(got[name]) == _attrs(was[name]), name
