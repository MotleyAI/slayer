"""Extraction values, ``first``/``last`` NULL keys and case-only-distinct ingestion on a throwaway Postgres
(specs: queries/date-functions, aggregations/functional-form, queries/identifier-case)."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import Any

import pytest

from slayer.core.models import DatasourceConfig, SlayerModel
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.live_schema import ingestion
from slayer.storage.yaml_storage import YAMLStorage
from tests._dev2215_fixtures import (
    DS_FIELDS,
    EXTRACTIONS,
    ROW3_PARTS,
    check_events,
    dev2215_models,
    ids,
    m,
    postgres_statements,
    q,
    sorted_rows,
)

pytest.importorskip("pytest_postgresql")

from pytest_postgresql import factories  # ALLOW(import-not-top): optional DB driver, gated by pytest.importorskip above

postgresql_proc = factories.postgresql_proc(port=None)
postgresql = factories.postgresql("postgresql_proc")

CASE_TABLES = [
    'CREATE TABLE casey ("Foo" INTEGER, foo INTEGER)',
    'INSERT INTO casey VALUES (1, 10), (2, 20)',
    'CREATE TABLE casey3 ("Foo" INTEGER, foo INTEGER, foo_2 INTEGER)',
    'INSERT INTO casey3 VALUES (1, 10, 100)',
]


def _datasource(postgresql: Any) -> DatasourceConfig:
    info = postgresql.info
    return DatasourceConfig.model_validate({"name": "pg", "type": "postgres", "host": info.host, "port": info.port,
                                            "database": info.dbname, "username": info.user, "password": "",
                                            **DS_FIELDS})


@pytest.fixture
async def pg(postgresql, tmp_path) -> AsyncIterator[tuple[SlayerQueryEngine, DatasourceConfig]]:
    cur = postgresql.cursor()
    for stmt in [*postgres_statements(), *CASE_TABLES]:
        cur.execute(stmt)
    postgresql.commit()
    storage = YAMLStorage(base_dir=str(tmp_path))
    ds = _datasource(postgresql)
    await storage.save_datasource(ds)
    for model in dev2215_models(data_source="pg"):
        await storage.save_model(model, _validate=False)
    engine = SlayerQueryEngine(storage=storage)
    try:
        yield engine, ds
    finally:
        engine.close()


@pytest.mark.parametrize("part", EXTRACTIONS)
async def test_extraction_equals_its_date_part_twin(pg, part: str) -> None:
    engine, _ = pg
    alias = await engine.execute(q(dimensions=["id", {"expression": f"{part}(created_at)", "name": "x"}]))
    twin = await engine.execute(q(dimensions=["id", {"expression": f"date_part('{part}', created_at)", "name": "x"}]))
    assert alias.columns == twin.columns
    assert alias.sql == twin.sql
    assert sorted_rows(alias) == sorted_rows(twin)
    assert int(next(r["orders.x"] for r in alias.data if int(r["orders.id"]) == 3)) == ROW3_PARTS[part]


async def test_week_is_the_iso_week_number(pg) -> None:
    engine, _ = pg
    assert ids(await engine.execute(q(dimensions=["id"], filters=["week(created_at) = 1"]))) == {3}


async def test_first_last_never_pick_a_null_key(pg) -> None:
    engine, _ = pg
    await check_events(engine)


async def test_first_last_partitioned_cross_model(pg) -> None:
    engine, _ = pg
    resp = await engine.execute(q(dimensions=["customers.region", "customers.name"], measures=[
        m("last(customers.status, customers.updated_at, partition_by=customers.region)", "l"),
        m("first(customers.status, customers.updated_at, partition_by=customers.region)", "f")]))
    got = {r["orders.customers.name"]: (r["orders.f"], r["orders.l"]) for r in resp.data}
    assert got == {"A": ("gold", "gold"), "B": ("gold", "gold"), "C": ("bronze", "bronze"), "D": ("bronze", "bronze")}


# --------------------------------------------------------------------------- #
# Ingestion keeps case-only-distinct columns apart
# --------------------------------------------------------------------------- #
async def _ingest(engine: SlayerQueryEngine, ds: DatasourceConfig, table: str) -> tuple[SlayerModel, str]:
    """The ingested model and the report's structured warnings (everything but the models) as text."""
    report = await ingestion.ingest_datasource_report_async(ds, client=engine.client_for(ds), include_tables=[table])
    (model,) = [mdl for mdl in report.models if mdl.name == table]
    return model, json.dumps(report.model_dump(mode="json", exclude={"models"}), default=str)


def _columns(model: SlayerModel) -> list[tuple[str, str | None]]:
    return [(c.name, c.sql) for c in model.columns]


async def test_two_case_variants(pg) -> None:
    engine, ds = pg
    model, notes = await _ingest(engine, ds, "casey")
    named = dict(_columns(model))
    assert set(named) == {"Foo", "foo_2"}
    assert named["foo_2"] is not None
    assert "foo" in named["foo_2"]
    assert "foo_2" in notes
    assert "foo" in notes
    await engine.storage.save_model(model)
    resp = await engine.execute(q(source_model="casey", dimensions=["Foo", "foo_2"]))
    assert sorted((r["casey.Foo"], r["casey.foo_2"]) for r in resp.data) == [(1, 10), (2, 20)]


async def test_suffix_already_taken_and_reingestion(pg) -> None:
    engine, ds = pg
    first, _ = await _ingest(engine, ds, "casey3")
    again, _ = await _ingest(engine, ds, "casey3")
    named = dict(_columns(first))
    assert set(named) == {"Foo", "foo_3", "foo_2"}
    assert named["foo_3"] is not None
    assert named["foo_3"] != named["foo_2"]
    assert _columns(first) == _columns(again)
    await engine.storage.save_model(first)
    resp = await engine.execute(q(source_model="casey3", dimensions=["Foo", "foo_3", "foo_2"]))
    assert [(r["casey3.Foo"], r["casey3.foo_3"], r["casey3.foo_2"]) for r in resp.data] == [(1, 10, 100)]
