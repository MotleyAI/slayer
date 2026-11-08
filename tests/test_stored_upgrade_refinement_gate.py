"""Live type refinement on load applies only below model v8, through the loading engine (spec: models/stored-upgrade)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb
import pytest
from sqlalchemy import event

from slayer.core.enums import DataType
from slayer.core.models import DatasourceConfig
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql import engine_factory
from slayer.sql.client import SlayerSQLClient
from slayer.storage import base as storage_base
from slayer.storage import migrations as mig
from slayer.storage.sqlite_conn import transaction
from tests._gateway_harness import RecordingClient
from tests._stored_upgrade_fixtures import BACKENDS, DS, column_of, raw_doc, raw_store


def _items(*, version: int, amount_type: str = "DOUBLE", sql_table: str = "items", columns: list[dict] | None = None) -> dict:
    return {
        "version": version, "name": "items", "sql_table": sql_table, "data_source": DS,
        "columns": columns or [
            {"name": "id", "type": "INT", "primary_key": True},
            {"name": "amount", "type": amount_type},
        ],
    }


def _live_items_sqlite(db: str) -> None:
    with transaction(db) as cur:
        cur.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, amount INTEGER)")
        cur.executemany("INSERT INTO items VALUES (?, ?)", [(1, 10), (2, 20)])


def _recording_factory(clients: list[RecordingClient]) -> Any:
    def factory(ds: DatasourceConfig) -> RecordingClient:
        clients.append(RecordingClient(inner=SlayerSQLClient(datasource=ds), datasource=ds))
        return clients[-1]

    return factory


@pytest.fixture
def no_live_access(monkeypatch) -> list[str]:
    """Record (and refuse) every live refinement or engine creation."""
    calls: list[str] = []

    def _refine(*_a, **_k):
        calls.append("refine")
        raise AssertionError("live refinement attempted")

    def _engine(*_a, **_k):
        calls.append("get_engine")
        raise AssertionError("engine creation attempted")

    monkeypatch.setattr(storage_base, "refine_dict_with_live_schema", _refine)
    monkeypatch.setattr(engine_factory, "get_engine", _engine)
    return calls


@pytest.mark.parametrize("backend", BACKENDS)
async def test_v10_model_loads_without_datasource_entry(tmp_path, backend):
    storage = await raw_store(backend=backend, base=str(tmp_path), datasource=None, models=[_items(version=10)])
    model = await storage.get_model("items", data_source=DS)
    assert model is not None
    assert column_of(model, "amount").type is DataType.DOUBLE
    assert (await raw_doc(storage, "items"))["version"] == mig.CURRENT_VERSIONS["SlayerModel"]


@pytest.mark.parametrize("backend", BACKENDS)
async def test_v10_model_loads_with_unreachable_datasource(tmp_path, backend, no_live_access):
    ds = DatasourceConfig(name=DS, type="postgres", host="127.0.0.1", port=1,
                          database="shop", username="u", password="p")
    storage = await raw_store(backend=backend, base=str(tmp_path), datasource=ds, models=[_items(version=10)])
    model = await storage.get_model("items", data_source=DS)
    assert model is not None
    assert column_of(model, "amount").type is DataType.DOUBLE
    assert no_live_access == []


@pytest.mark.parametrize("backend", BACKENDS)
async def test_pre_v8_model_still_requires_its_datasource(tmp_path, backend):
    storage = await raw_store(backend=backend, base=str(tmp_path), datasource=None, models=[_items(version=5)])
    with pytest.raises(ValueError, match="unavailable for type refinement"):
        await storage.get_model("items", data_source=DS)
    engine = SlayerQueryEngine(storage=storage)
    with pytest.raises(ValueError, match="unavailable for type refinement"):
        await engine.storage.get_model("items", data_source=DS)


@pytest.mark.parametrize("backend", BACKENDS)
async def test_v7_model_is_still_refined(tmp_path, backend):
    db = str(tmp_path / "live.db")
    _live_items_sqlite(db)
    ds = DatasourceConfig(name=DS, type="sqlite", database=db)
    storage = await raw_store(backend=backend, base=str(tmp_path / "store"), datasource=ds,
                              models=[_items(version=7)])
    engine = SlayerQueryEngine(storage=storage)
    try:
        model = await engine.storage.get_model("items", data_source=DS)
    finally:
        engine.close()
        engine_factory.invalidate_engine(ds)
    assert model is not None
    assert column_of(model, "amount").type is DataType.INT


@pytest.mark.parametrize("backend", BACKENDS)
async def test_refinement_without_a_query_engine_is_refused(tmp_path, backend):
    db = str(tmp_path / "live.db")
    _live_items_sqlite(db)
    ds = DatasourceConfig(name=DS, type="sqlite", database=db)
    storage = await raw_store(backend=backend, base=str(tmp_path / "store"), datasource=ds,
                              models=[_items(version=5)])
    with pytest.raises(ValueError, match="through a query engine"):
        await storage.get_model("items", data_source=DS)


async def test_refinement_reads_only_the_models_table(tmp_path: Path) -> None:
    db = str(tmp_path / "live.duckdb")
    con = duckdb.connect(db)
    try:
        con.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, amount INTEGER)")
        con.execute("CREATE TABLE other_a (x INTEGER)")
        con.execute("CREATE TABLE other_b (y DOUBLE)")
        con.execute("CREATE VIEW items_v AS SELECT * FROM items")
    finally:
        con.close()
    ds = DatasourceConfig(name=DS, type="duckdb", database=db)
    storage = await raw_store(backend="yaml", base=str(tmp_path / "store"), datasource=ds, models=[_items(version=7)])
    clients: list[RecordingClient] = []
    engine = SlayerQueryEngine(storage=storage, client_factory=_recording_factory(clients))
    try:
        model = await engine.storage.get_model("items", data_source=DS)
    finally:
        engine.close()
        engine_factory.invalidate_engine(ds)
    assert model is not None
    assert column_of(model, "amount").type is DataType.INT
    reads = [c for c in clients[0].calls if c.op == "reflect_columns"]
    assert [(c.args[0] if c.args else c.kwargs["location"]).name for c in reads] == ["items"]
    assert "list_objects" not in clients[0].ops()


def _attach(path: str) -> Any:
    def _on_connect(dbapi_conn: Any, _record: Any) -> None:
        dbapi_conn.execute(f"ATTACH DATABASE '{path}' AS aux")

    return _on_connect


async def test_sqlite_refinement_keeps_its_sampling_rules(tmp_path: Path) -> None:
    db, aux = str(tmp_path / "live.db"), str(tmp_path / "aux.db")
    with transaction(db) as cur:
        cur.execute(
            "CREATE TABLE samples (id INTEGER PRIMARY KEY, ints, reals, mixed_real, blobs, "
            "coercible, noncoercible, nonfinite)"
        )
        cur.executemany(
            "INSERT INTO samples VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [(1, 1, 1.5, 1, 1, 1, 1, 1), (2, 2, 2.5, 2.5, b"\x00", "12.5", "abc", "inf")],
        )
        cur.execute("CREATE TABLE big (v)")
        cur.execute(
            "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 100001) "
            "INSERT INTO big SELECT x FROM c"
        )
    with transaction(aux) as cur:
        cur.execute('CREATE TABLE events ("group")')
        cur.executemany("INSERT INTO events VALUES (?)", [(1,), (2,)])
        cur.execute('CREATE TABLE "odd table" (v)')
        cur.executemany('INSERT INTO "odd table" VALUES (?)', [(1,), (2,)])
    ds = DatasourceConfig(name=DS, type="sqlite", database=db)
    event.listen(engine_factory.get_engine(ds), "connect", _attach(aux))
    stored = {
        "ints": "DOUBLE", "reals": "DOUBLE", "mixed_real": "INT", "blobs": "INT",
        "coercible": "INT", "noncoercible": "INT", "nonfinite": "INT",
    }
    samples = {
        "version": 7, "name": "samples", "sql_table": "samples", "data_source": DS,
        "columns": [{"name": "id", "type": "INT", "primary_key": True},
                    *({"name": n, "type": t} for n, t in stored.items())],
    }
    big = {"version": 7, "name": "big", "sql_table": "big", "data_source": DS,
           "columns": [{"name": "v", "type": "DOUBLE"}]}
    events = {"version": 7, "name": "events", "sql_table": "aux.events", "data_source": DS,
              "columns": [{"name": "group", "type": "DOUBLE"}]}
    odd = {"version": 7, "name": "odd", "sql_table": 'aux."odd table"', "data_source": DS,
           "columns": [{"name": "v", "type": "DOUBLE"}]}
    storage = await raw_store(
        backend="yaml", base=str(tmp_path / "store"), datasource=ds, models=[samples, big, events, odd],
    )
    clients: list[RecordingClient] = []
    engine = SlayerQueryEngine(storage=storage, client_factory=_recording_factory(clients))
    try:
        loaded = {
            name: await engine.storage.get_model(name, data_source=DS)
            for name in ("samples", "big", "events", "odd")
        }
    finally:
        engine.close()
        engine_factory.invalidate_engine(ds)
    assert all(m is not None for m in loaded.values())
    models = {name: m for name, m in loaded.items() if m is not None}
    samples_model = models["samples"]
    types = {c.name: c.type for c in samples_model.columns}
    assert types == {
        "id": DataType.INT, "ints": DataType.INT, "reals": DataType.DOUBLE, "mixed_real": DataType.DOUBLE,
        "blobs": DataType.TEXT, "coercible": DataType.DOUBLE, "noncoercible": DataType.TEXT,
        "nonfinite": DataType.TEXT,
    }
    assert column_of(models["big"], "v").type is DataType.DOUBLE
    assert column_of(models["events"], "group").type is DataType.INT
    assert column_of(models["odd"], "v").type is DataType.INT
    assert clients
    assert clients[0].statements()
