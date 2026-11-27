"""Casts SLayer emits itself keep their values (spec: queries/casts › CAST has one meaning on every dialect)."""

from __future__ import annotations

from typing import Any

import pytest

from slayer.core.enums import DataType
from slayer.core.models import DatasetField
from slayer.engine.query_engine import SlayerQueryEngine
from tests._cast_fixtures import check_array_cast, duckdb_tags_engine
from tests._dev2101_fixtures import by_key, dev2101_datasets, engine_for, ids, m, q, single
from tests._dev2120_fixtures import emit_lib, lib_dataset, lib_query

#: Declared result types: the database's own conversion, per backend.
DECLARED = {
    "sqlite": {"big": {1: 0, 2: 1, 3: 1, 4: 1, 5: None}, "half": {1: 1, 2: 1, 3: 2, 4: 2, 5: 0}},
    "duckdb": {"big": {1: False, 2: True, 3: True, 4: True, 5: None}, "half": {1: 1, 2: 2, 3: 2, 4: 2, 5: 0}},
}
RESTORED = {"sqlite": (1, 0), "duckdb": (True, False)}


@pytest.fixture(params=sorted(DECLARED))
async def backend(request):
    async for engine in engine_for(request.param):
        orders = dev2101_datasets()[0]
        await engine.storage.save_dataset(orders.model_copy(update={"fields": [
            *orders.fields,
            DatasetField(name="big", expression="amount > 15", datatype=DataType.BOOLEAN),
            DatasetField(name="half", expression="price / 2", datatype=DataType.INTEGER),
        ]}))
        yield request.param, engine


@pytest.mark.parametrize("field", ["big", "half"])
async def test_declared_result_type(backend: tuple[str, SlayerQueryEngine], field: str) -> None:
    name, engine = backend
    resp = await engine.execute(q(dimensions=["orders.id", {"field": f"orders.{field}", "alias": "x"}]))
    got: dict[int, Any] = {int(k): v for k, v in by_key(resp, "id", "x").items()}
    assert got == DECLARED[name][field], resp.sql
    assert all(type(v) is type(DECLARED[name][field][k]) for k, v in got.items() if v is not None), got


async def test_aggregate_boolean_restoration(backend: tuple[str, SlayerQueryEngine]) -> None:
    name, engine = backend
    hi = single(await engine.execute(q(metrics=[m("max(orders.amount > 15)")])))
    lo = single(await engine.execute(q(metrics=[m("min(orders.amount > 15)")])))
    got = (hi, lo)
    assert got == RESTORED[name]
    assert type(hi) is type(RESTORED[name][0])


@pytest.mark.parametrize(("expr", "expected"), [
    ("json_value('{{\"v\": 1.5}}', '$.v' RETURNING INT)", 2),
    ("json_value('{{\"v\": 5}}', '$.v' RETURNING BOOLEAN)", None),
    ("json_value('{{\"v\": \"7\"}}', '$.v' RETURNING INT)", 7),
])
async def test_json_returning(backend: tuple[str, SlayerQueryEngine], expr: str, expected: Any) -> None:
    _, engine = backend
    resp = await engine.execute(q(dimensions=["orders.id", {"field": expr, "alias": "x"}]))
    assert set(by_key(resp, "id", "x").values()) == {expected}, resp.sql


@pytest.mark.parametrize(("pred", "kept"), [
    ("orders.order_date >= '2024-02-25'", {4, 5}), ("orders.created_at < '2024-01-20 11:00:00'", {1}),
])
async def test_temporal_comparison(backend: tuple[str, SlayerQueryEngine], pred: str, kept: set[int]) -> None:
    _, engine = backend
    assert ids(await engine.execute(q(dimensions=["orders.id"], filter=[pred]))) == kept


async def test_list_element_coercion() -> None:
    async for engine in duckdb_tags_engine():
        await check_array_cast(engine)


async def test_bigquery_zoned_column_read() -> None:
    lib = lib_dataset()
    zoned = lib.model_copy(update={"fields": [
        f.model_copy(update={"db_type": "TIMESTAMP", "datatype": DataType.DATETIME_TZ}) if f.name == "ts" else f
        for f in lib.fields]})
    sql = await emit_lib(lib_query("lib.ts"), dialect="bigquery", datasets=[zoned])
    assert "CAST(lib.ts AS DATETIME)" in sql, sql
