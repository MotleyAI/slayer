"""The default client keeps each dialect's connection behaviour (spec: sql/client-contract)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb
import pytest
from sqlalchemy import event

from slayer.core.models import DatasourceConfig
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql import engine_factory
from slayer.sql.client import SlayerSQLClient
from slayer.sql.dialects.reflection import TableLocation
from slayer.storage.yaml_storage import YAMLStorage
from tests._gateway_harness import COUNT_ORDERS_MEM, mem_count_model


def _duck(path: Path, statements: list[str]) -> None:
    con = duckdb.connect(str(path))
    try:
        for statement in statements:
            con.execute(statement)
    finally:
        con.close()


async def test_duckdb_attached_catalogs_stay_distinct(tmp_path: Path) -> None:
    main, other = tmp_path / "main_cat.duckdb", tmp_path / "aaa.duckdb"
    _duck(main, ["CREATE SCHEMA s", "CREATE TABLE s.t(id INTEGER PRIMARY KEY, u VARCHAR UNIQUE)"])
    _duck(other, ["CREATE SCHEMA s", "CREATE TABLE s.t(k DOUBLE PRIMARY KEY, w INTEGER, z VARCHAR UNIQUE)"])
    ds = DatasourceConfig(name="attached", type="duckdb", database=str(main))

    def _attach(dbapi_conn: Any, _record: Any) -> None:
        dbapi_conn.execute(f"ATTACH IF NOT EXISTS '{other}' AS aaa")

    event.listen(engine_factory.get_engine(ds), "connect", _attach)
    client = SlayerSQLClient(datasource=ds)
    here = TableLocation(name="t", schema_name="s")
    there = TableLocation(name="t", schema_name="s", catalog="aaa")
    try:
        here_cols = [c.name for c in await client.reflect_columns(here)]
        there_cols = [c.name for c in await client.reflect_columns(there)]
        here_keys = await client.reflect_table_keys(here)
        there_keys = await client.reflect_table_keys(there)
    finally:
        client.close()
        engine_factory.invalidate_engine(ds)
    assert here_cols == ["id", "u"]
    assert there_cols == ["k", "w", "z"]
    assert (here_keys.primary_key, here_keys.unique_key_sets) == (["id"], [["u"]])
    assert (there_keys.primary_key, there_keys.unique_key_sets) == (["k"], [["z"]])


async def test_in_memory_sqlite_is_one_database_per_owning_engine(tmp_path: Path) -> None:
    ds = DatasourceConfig(name="mem", type="sqlite", database=":memory:")
    storage = YAMLStorage(base_dir=str(tmp_path / "store"))
    await storage.save_datasource(ds)
    await storage.save_model(mem_count_model(), _validate=False)
    owner, other = SlayerQueryEngine(storage=storage), SlayerQueryEngine(storage=storage)
    try:
        await owner._client_for(ds).execute("CREATE TABLE t (v INTEGER)")
        assert "t" in (await owner._client_for(ds).list_objects(None)).tables
        assert "t" not in (await other._client_for(ds).list_objects(None)).tables
        assert (await owner.execute(COUNT_ORDERS_MEM)).data[0]["t.n"] == 0
        with pytest.raises(Exception, match="no such table"):  # NOSONAR(S5958) — the driver's message is the contract
            await other.execute(COUNT_ORDERS_MEM)
    finally:
        owner.close()
        other.close()
