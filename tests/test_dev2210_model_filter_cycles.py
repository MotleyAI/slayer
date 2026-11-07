"""Model filters whose dataset reaches itself are refused, at both save doors and at query time."""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import ModelFilterCycleError, SlayerError
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from tests._dev2210_fixtures import ids, seed
from tests._engine_helpers import seeded_exec_engine

DDL = (
    "CREATE TABLE a (id INTEGER PRIMARY KEY, flag INTEGER, b_id INTEGER)",
    "INSERT INTO a VALUES (1, 1, 1), (2, 0, 2)",
    "CREATE TABLE b (id INTEGER PRIMARY KEY, flag INTEGER, a_id INTEGER, c_id INTEGER)",
    "INSERT INTO b VALUES (1, 1, 1, 1), (2, 0, 2, 2)",
    "CREATE TABLE c (id INTEGER PRIMARY KEY, flag INTEGER, a_id INTEGER)",
    "INSERT INTO c VALUES (1, 1, 1), (2, 0, 2)",
)


def _model(name: str, *others: str, filters: Sequence[str] = (), extra: Sequence[Column] = ()) -> SlayerModel:
    return SlayerModel(
        name=name, data_source="test", sql_table=name,
        columns=[Column(name="id", type=DataType.INT, primary_key=True), Column(name="flag", type=DataType.INT),
                 *(Column(name=f"{o}_id", type=DataType.INT) for o in others), *extra],
        joins=[ModelJoin(target_model=o, name=f"{name}_to_{o}", join_pairs=[[f"{o}_id", "id"]]) for o in others],
        filters=list(filters),
    )


@pytest.fixture
async def engine() -> AsyncIterator[SlayerQueryEngine]:
    async with seeded_exec_engine(
        dialect="sqlite", seed=lambda p: seed("sqlite", p, DDL), models=[_model("a", "b"), _model("b", "a", "c"), _model("c", "a")],
    ) as (eng, _db):
        yield eng


async def _save(engine: SlayerQueryEngine, model: SlayerModel, *, door: str) -> None:
    if door == "engine":
        await engine.save_model(model)
    else:
        await engine.storage.save_model(model)


async def _stored_filters(engine: SlayerQueryEngine, name: str) -> list[str]:
    stored = await engine.storage.get_model(name, data_source="test")
    assert stored is not None
    return list(stored.filters)


async def _ids(engine: SlayerQueryEngine, source: str) -> set[int]:
    resp = await engine.execute(SlayerQuery.model_validate({"source_model": source, "dimensions": ["id"]}))
    return ids(resp, source)


def test_error_family() -> None:
    assert issubclass(ModelFilterCycleError, SlayerError)
    assert issubclass(ModelFilterCycleError, ValueError)


@pytest.mark.parametrize("door", ["engine", "storage"])
class TestSaveRefusesCycles:
    async def test_two_filters_reading_each_other(self, engine, door):
        await _save(engine, _model("a", "b", filters=["a_to_b.flag = 1"]), door=door)
        b = _model("b", "a", "c", filters=["b_to_a.flag = 1"])
        with pytest.raises(ModelFilterCycleError) as exc:
            await _save(engine, b, door=door)
        assert exc.value.cycle == ["b", "a", "b"]
        assert "b → a → b" in str(exc.value)
        assert await _stored_filters(engine, "b") == []

    async def test_cycle_through_a_derived_column(self, engine, door):
        await _save(engine, _model("a", "b", filters=["a_to_b.flag = 1"]), door=door)
        b = _model("b", "a", "c", filters=["a_flag = 1"], extra=[Column(name="a_flag", type=DataType.INT, sql="b_to_a.flag")])
        with pytest.raises(ModelFilterCycleError):
            await _save(engine, b, door=door)
        assert await _stored_filters(engine, "b") == []

    @pytest.mark.parametrize("b_filters", [[], ["flag = 1"]], ids=["no-filter", "own-columns"])
    async def test_reverse_edges_without_filter_recursion(self, engine, door, b_filters):
        await _save(engine, _model("a", "b", filters=["a_to_b.flag = 1"]), door=door)
        await _save(engine, _model("b", "a", "c", filters=b_filters), door=door)
        assert await _ids(engine, "a") == {1}
        assert await _ids(engine, "b") == ({1} if b_filters else {1, 2})


    async def test_cycle_through_an_intermediate_hop(self, engine, door):
        await _save(engine, _model("a", "b", filters=["a_to_b.b_to_c.flag = 1"]), door=door)
        b = _model("b", "a", "c", filters=["b_to_a.flag = 1"])
        with pytest.raises(ModelFilterCycleError):
            await _save(engine, b, door=door)

    async def test_stored_cycle_never_blocks_an_unrelated_save(self, engine, door):
        await engine.storage.save_model(_model("a", "b", filters=["a_to_b.flag = 1"]), _validate=False)
        await engine.storage.save_model(_model("b", "a", "c", filters=["b_to_a.flag = 1"]), _validate=False)
        await _save(engine, _model("c", "a", filters=["flag = 1"]), door=door)
        assert await _stored_filters(engine, "c") == ["flag = 1"]


@pytest.mark.parametrize("source", ["a", "b"])
async def test_stored_cycle_fails_the_query(engine, source):
    await engine.storage.save_model(_model("a", "b", filters=["a_to_b.flag = 1"]), _validate=False)
    await engine.storage.save_model(_model("b", "a", "c", filters=["b_to_a.flag = 1"]), _validate=False)
    with pytest.raises(ModelFilterCycleError):
        await _ids(engine, source)
