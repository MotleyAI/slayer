"""Instant columns ingest as ``DateTimeTz``, re-ingest re-types them, stored BigQuery ``TIMESTAMP`` fields load as
``DateTimeTz``, and zones round-trip (specs: compatibility/ossie-vocabulary, models/stored-upgrade, queries/time-zones)."""

from __future__ import annotations

import contextlib
import os
from pathlib import Path
from types import ModuleType
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

from slayer.core.enums import DataType
from slayer.core import slayer_extension as ext
from slayer.core.models import DatasetField, DatasourceConfig, ExtensionNote, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.live_schema.ingestion import (
    _fields_to_dataset,
    _introspected_field,
    _merge_one_field,
    _retyped_list,
    ingest_datasource_idempotent,
)
from slayer.sql.dialects import base as dialects_base
from slayer.sql.dialects import dialect_for_sa_backend, get_dialect
from slayer.sql.dialects.reflection import TableLocation
from slayer.storage.base import StorageBackend
from slayer.storage.document_extensions import Entry, apply_field_entry
from slayer.storage.migrations import CURRENT_VERSIONS
from slayer.storage.sqlite_storage import SQLiteStorage
from slayer.storage.upgrade import upgrade_store
from slayer.storage.yaml_storage import YAMLStorage
from tests._engine_helpers import owned_query_engine
from tests._osi_export_helpers import carries, dataset, export_datasets, make_env, reimport, stored, validate
from tests._osi_export_helpers import field as doc_field
from tests._stored_upgrade_fixtures import BACKENDS, DS, raw_store, stored_bytes

DT, TZ = DataType.DATETIME, DataType.DATETIME_TZ


# --------------------------------------------------------------------------- #
# Ingestion types instant columns DateTimeTz
# --------------------------------------------------------------------------- #
def _inspected(backend: str, raw: list[dict[str, Any]]) -> dict[str, DataType]:
    cols = dialect_for_sa_backend(backend).columns_from_inspector(
        raw, connection=None, inspector=None, location=TableLocation(name="t"))
    return {c.name: _introspected_field(c, name=c.name, primary_key=False).type for c in cols}


def _from_information_schema(dialect: str, rows: list[tuple[str, str]]) -> dict[str, DataType]:
    inspector = MagicMock()
    inspector.get_columns.side_effect = RuntimeError("no inspector")
    with patch.object(dialects_base, "fetch", return_value=rows):
        cols = get_dialect(dialect).reflect_columns(connection=None, inspector=inspector, table="t", schema="s",
                                                    catalog=None)
    return {c.name: c.narrowed_type for c in cols}


class TestInstantColumnsIngestAsDateTimeTz:
    def test_bigquery_inspector(self) -> None:
        assert _inspected("bigquery", [{"name": "ts", "type": sa.TIMESTAMP()}, {"name": "dt", "type": sa.DATETIME()}]) \
            == {"ts": TZ, "dt": DT}

    def test_mysql_inspector(self) -> None:
        assert _inspected("mysql", [{"name": "ts", "type": mysql.TIMESTAMP()}, {"name": "dt", "type": mysql.DATETIME()}]) \
            == {"ts": TZ, "dt": DT}

    @pytest.mark.parametrize(("dialect", "rows", "expected"), [
        ("bigquery", [("ts", "TIMESTAMP"), ("dt", "DATETIME")], {"ts": TZ, "dt": DT}),
        ("mysql", [("ts", "timestamp"), ("dt", "datetime")], {"ts": TZ, "dt": DT}),
        ("clickhouse", [("a", "DateTime('UTC')"), ("b", "DateTime64(3, 'Asia/Tokyo')"), ("c", "DateTime"),
                        ("d", "Nullable(DateTime('UTC'))")], {"a": TZ, "b": TZ, "c": DT, "d": TZ}),
    ])
    def test_information_schema_fallback(self, dialect: str, rows: list[tuple[str, str]],
                                         expected: dict[str, DataType]) -> None:
        assert _from_information_schema(dialect, rows) == expected

    def test_postgres_timestamp_stays_naive(self) -> None:
        assert _from_information_schema("postgres", [("ts", "timestamp without time zone")]) == {"ts": DT}


@pytest.fixture
def ch_types() -> ModuleType:
    return pytest.importorskip("clickhouse_sqlalchemy.types")


def test_clickhouse_inspector(ch_types: ModuleType) -> None:
    raw = [
        {"name": "a", "type": ch_types.DateTime("UTC")},
        {"name": "b", "type": ch_types.DateTime64(3, "Asia/Tokyo")},
        {"name": "c", "type": ch_types.DateTime()},
        {"name": "d", "type": ch_types.Nullable(ch_types.DateTime("UTC"))},
        {"name": "e", "type": ch_types.DateTime64(3)},
    ]
    assert _inspected("clickhouse", raw) == {"a": TZ, "b": TZ, "c": DT, "d": TZ, "e": DT}


# --------------------------------------------------------------------------- #
# Re-ingest re-types a DateTime field whose live column is an instant
# --------------------------------------------------------------------------- #
def _authored(name: str) -> DatasetField:
    return DatasetField(name=name, datatype=DT, description=f"{name} description", label=f"{name} label",
                        meta={"owner": name}, granularity="second", hidden=True)


def _attrs(f: DatasetField) -> dict[str, Any]:
    return f.model_dump(exclude={"datatype"})


async def _reingest_duckdb(tmp_path: Path, stored_dataset: SlayerDataset) -> SlayerDataset:
    duckdb = pytest.importorskip("duckdb")
    db = str(tmp_path / "live.duckdb")
    with contextlib.closing(duckdb.connect(db)) as con:
        con.execute("CREATE TABLE events (id INTEGER, inst TIMESTAMPTZ, wall TIMESTAMP)")
        con.execute("INSERT INTO events VALUES (1, '2025-01-01 03:30:00+00', '2025-01-01 03:30:00')")
    ds = DatasourceConfig(name="test", type="duckdb", database=db)
    storage = YAMLStorage(base_dir=str(tmp_path / "store"))
    async with owned_query_engine(storage=storage, datasources=[ds]) as engine:
        await storage.save_dataset(stored_dataset)
        await ingest_datasource_idempotent(datasource=ds, storage=storage, client=engine.client_for(ds),
                                           include_tables=["events"])
    loaded = await storage.get_dataset("events", data_source="test")
    assert loaded is not None
    return loaded


async def test_reingest_retypes_an_instant_column_and_nothing_else(tmp_path: Path) -> None:
    before = SlayerDataset(name="events", sql_table="events", data_source="test", fields=[
        DatasetField(name="id", datatype=DataType.INTEGER), _authored("inst"), _authored("wall")])
    after = await _reingest_duckdb(tmp_path, before)
    got = {f.name: f for f in after.fields}
    assert got["inst"].datatype is TZ
    assert got["wall"].datatype is DT
    for name in ("inst", "wall"):
        authored = before.get_field(name)
        assert authored is not None
        assert _attrs(got[name]) == _attrs(authored), name


def test_reingest_merge_retypes_a_mysql_timestamp_column() -> None:
    reflected = dialect_for_sa_backend("mysql").columns_from_inspector(
        [{"name": "ts", "type": mysql.TIMESTAMP()}, {"name": "dt", "type": mysql.DATETIME()}],
        connection=None, inspector=None, location=TableLocation(name="events"))
    fresh = _fields_to_dataset(name="events", data_source="my", sql_table="events",
                               fields=[_introspected_field(c, name=c.name, primary_key=False) for c in reflected])
    for name, expected in (("ts", TZ), ("dt", DT)):
        persisted = _authored(name)
        merged, *_ = _merge_one_field(persisted_col=persisted, fresh_col=fresh.get_field(name), dataset_name="events",
                                      sqlite_widen_enabled=False)
        assert merged.datatype is expected, name
        assert _attrs(merged) == _attrs(persisted), name


# --------------------------------------------------------------------------- #
# Stored BigQuery TIMESTAMP fields load as DateTimeTz (SlayerModel 31)
# --------------------------------------------------------------------------- #
PREVIOUS = 30
BIGQUERY = DatasourceConfig(name=DS, type="bigquery", database="proj")
POSTGRES = DatasourceConfig(name=DS, type="postgres", host="localhost", database="db")
MYSQL = DatasourceConfig(name=DS, type="mysql", host="localhost", database="db")
NO_ZONE_QUERY = SlayerQuery.model_validate({
    "population": "events", "metrics": [{"metric": "count(events.*)", "alias": "n"}],
    "time_dimensions": [{"field": "events.created_at", "granularity": "month"}],
    "filter": ["events.created_at >= '2025-01-01 00:00:00'", "events.created_at < now()"],
})
# The SQL of ``NO_ZONE_QUERY`` over ``created_at`` typed DateTime with db_type TIMESTAMP, before the upgrade existed.
NO_ZONE_SQL = (
    "SELECT\n  DATE_TRUNC(CAST(events.created_at AS DATETIME), MONTH) AS `events___created_at`,\n  COUNT(*) AS `n`\n"
    "FROM events AS events\nWHERE\n  events.created_at >= '2025-01-01 00:00:00'\n"
    "  AND events.created_at < CAST(CURRENT_DATETIME() AS TIMESTAMP)\nGROUP BY\n  `events___created_at`\n"
    "ORDER BY\n  `events___created_at` ASC NULLS LAST"
)


def _events_v30() -> dict[str, Any]:
    return {"version": PREVIOUS, "name": "events", "sql_table": "events", "data_source": DS, "fields": [
        {"name": "id", "datatype": "Integer", "primary_key": True},
        {"name": "created_at", "datatype": "DateTime", "db_type": "TIMESTAMP"},
        {"name": "wall_at", "datatype": "DateTime"},
    ]}


async def _v30_store(tmp_path: Path, *, backend: str, datasource: DatasourceConfig) -> tuple[StorageBackend, str]:
    base = str(tmp_path / f"store-{backend}")
    return await raw_store(backend=backend, base=base, datasource=datasource, models=[_events_v30()]), base


def test_the_dataset_version_is_bumped() -> None:
    assert CURRENT_VERSIONS["SlayerModel"] == 32


@pytest.mark.parametrize("backend", BACKENDS)
async def test_bigquery_timestamp_field_loads_as_datetimetz(tmp_path: Path, backend: str) -> None:
    storage, base = await _v30_store(tmp_path, backend=backend, datasource=BIGQUERY)
    raw = stored_bytes(backend=backend, base=base, name="events")
    model = await storage.get_dataset("events", data_source=DS)
    assert model is not None
    assert {f.name: f.datatype for f in model.fields} == {"id": DataType.INTEGER, "created_at": TZ, "wall_at": DT}
    assert stored_bytes(backend=backend, base=base, name="events") == raw


@pytest.mark.parametrize("backend", BACKENDS)
async def test_upgraded_field_emits_the_same_sql_without_a_zone(tmp_path: Path, backend: str) -> None:
    storage, _ = await _v30_store(tmp_path, backend=backend, datasource=BIGQUERY)
    async with owned_query_engine(storage=storage) as engine:
        sql = (await engine.execute(NO_ZONE_QUERY, dry_run=True)).sql
    assert sql == NO_ZONE_SQL


@pytest.mark.parametrize("datasource", [POSTGRES, MYSQL], ids=["postgres", "mysql"])
@pytest.mark.parametrize("backend", BACKENDS)
async def test_other_datasources_are_untouched(tmp_path: Path, backend: str, datasource: DatasourceConfig) -> None:
    storage, _ = await _v30_store(tmp_path, backend=backend, datasource=datasource)
    model = await storage.get_dataset("events", data_source=DS)
    assert model is not None
    created = model.get_field("created_at")
    assert created is not None
    assert created.datatype is DT


async def test_upgrade_waits_for_the_datasource_type(tmp_path: Path) -> None:
    storage = await raw_store(backend="yaml", base=str(tmp_path / "store"), datasource=None, models=[_events_v30()])
    report = await upgrade_store(storage)
    [refused] = [o for o in report.outcomes if o.name == f"{DS}.events"]
    assert refused.status == "not upgraded", refused
    assert refused.detail is not None
    assert "instant typing waits for datasource" in refused.detail
    typed = await upgrade_store(storage, datasource_types={DS: "bigquery"})
    assert typed.complete, typed.outcomes
    model = await storage.get_dataset("events", data_source=DS)
    assert model is not None
    assert {f.name: f.datatype for f in model.fields} == {"id": DataType.INTEGER, "created_at": TZ, "wall_at": DT}


# --------------------------------------------------------------------------- #
# Zones round-trip through storage and the Ossie export
# --------------------------------------------------------------------------- #
def _zoned_orders(data_source: str) -> SlayerDataset:
    return SlayerDataset(name="orders", sql_table="orders", data_source=data_source, fields=[
        DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
        DatasetField.model_validate({"name": "ordered_at", "datatype": "DateTime", "time_zone": "Asia/Tokyo"}),
        DatasetField.model_validate({"name": "local_slot", "datatype": "DateTime", "time_zone": "civil"}),
    ])


def _zones(model: SlayerDataset) -> dict[str, Any]:
    return {f.name: getattr(f, "time_zone", None) for f in model.fields}


ZONES = {"id": None, "ordered_at": "Asia/Tokyo", "local_slot": "civil"}


def _storage(backend: str, tmp_path: Path) -> StorageBackend:
    if backend == "yaml":
        return YAMLStorage(base_dir=str(tmp_path / "yaml"))
    return SQLiteStorage(db_path=os.path.join(str(tmp_path), "store.db"))


@pytest.mark.parametrize("backend", BACKENDS)
async def test_field_and_datasource_zones_round_trip_through_storage(tmp_path: Path, backend: str) -> None:
    storage = _storage(backend, tmp_path)
    await storage.save_datasource(DatasourceConfig.model_validate(
        {"name": "zds", "type": "sqlite", "database": ":memory:", "time_zone": "UTC"}))
    await storage.save_dataset(_zoned_orders("zds"))
    loaded_ds = await storage.get_datasource("zds")
    loaded = await storage.get_dataset("orders", data_source="zds")
    assert loaded_ds is not None
    assert loaded is not None
    assert getattr(loaded_ds, "time_zone", None) == "UTC"
    assert _zones(loaded) == ZONES


async def test_field_zones_round_trip_through_the_ossie_export(tmp_path: Path) -> None:
    env = await make_env(tmp_path / "env", statements=[
        "CREATE TABLE orders (id INTEGER, ordered_at TIMESTAMP, local_slot TIMESTAMP)"])
    result = await export_datasets(env, _zoned_orders(env.datasource))
    validate(result.document)
    orders = dataset(result.document, "orders")
    for name in ("ordered_at", "local_slot"):
        entity = doc_field(orders, name)
        assert entity is not None
        assert carries(entity, ZONES[name]), entity
    target, _ = await reimport(env, result)
    assert _zones(await stored(target, "orders")) == ZONES


# --------------------------------------------------------------------------- #
# Without a zone, DateTimeTz behaves as the timestamp type
# --------------------------------------------------------------------------- #
async def test_datetimetz_range_filter_matches_datetime_without_a_zone(tmp_path: Path) -> None:
    duckdb = pytest.importorskip("duckdb")
    db = str(tmp_path / "t.duckdb")
    with contextlib.closing(duckdb.connect(db)) as con:
        con.execute("CREATE TABLE ev (id INTEGER, ts TIMESTAMP)")
        con.executemany("INSERT INTO ev VALUES (?, ?)", [
            (1, "2024-12-31 23:59:59"), (2, "2025-01-01 00:00:00"), (3, "2025-01-31 23:30:00"),
            (4, "2025-02-01 00:00:00")])
    ds = DatasourceConfig(name="test", type="duckdb", database=db)

    def ev(name: str, datatype: DataType) -> SlayerDataset:
        return SlayerDataset(name=name, sql_table="ev", data_source="test", fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="ts", datatype=datatype)])

    async with owned_query_engine(storage=YAMLStorage(base_dir=str(tmp_path / "store")), datasources=[ds]) as engine:
        await engine.save_dataset(ev("naive", DT))
        await engine.save_dataset(ev("instant", TZ))
        got = {name: await _ids_in_january(engine, name) for name in ("naive", "instant")}
    assert got["instant"] == got["naive"] == {2, 3}


async def _ids_in_january(engine: SlayerQueryEngine, name: str) -> set[int]:
    resp = await engine.execute(SlayerQuery.model_validate({
        "population": name, "dimensions": [f"{name}.id"],
        "time_dimensions": [{"field": f"{name}.ts", "granularity": "day", "date_range": ["2025-01-01", "2025-01-31"]}],
    }))
    return {int(r[f"{name}.id"]) for r in resp.data}


@pytest.mark.parametrize("datatype, zone", [(DataType.DATETIME, "Mars/Olympus"), (DataType.STRING, "Asia/Tokyo")])
def test_a_slayer_entry_zone_the_field_cannot_take_is_dropped_with_a_note(datatype: DataType, zone: str) -> None:
    notes: list[ExtensionNote] = []
    entry = Entry(ext.FieldPayload(entity="slot", field=ext.FieldContent(datatype=datatype, time_zone=zone)),
                  matched=True)
    out = apply_field_entry(DatasetField(name="slot", datatype=DataType.DATETIME), entry, notes=notes, label="slot",
                            target="duckdb")
    assert out.time_zone is None
    assert out.datatype is datatype
    [note] = notes
    assert zone in note.message


def test_a_zoned_field_retyped_to_a_list_drops_its_zone() -> None:
    stored = DatasetField.model_validate({"name": "ts", "datatype": "DateTime", "time_zone": "UTC"})
    live = DatasetField(name="ts", datatype=DataType.ARRAY, element_type=DataType.DATETIME, db_type="TIMESTAMP[]")
    merged = _retyped_list(persisted=stored, fresh=live)
    assert merged is not None
    assert merged.time_zone is None
    DatasetField.model_validate(merged.model_dump())
