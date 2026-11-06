"""Model filters are validated per reference against the owning model, at both save doors and for every model a query reads."""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import ModelFilterError
from slayer.core.models import Column, ModelJoin, ModelMeasure, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from tests._dev2210_fixtures import seed, table_models
from tests._engine_helpers import seeded_exec_engine

RANKED = Column(name="ranked", type=DataType.INT, sql="rank() over (order by amount)")


def _orders(name: str, *, columns: Sequence[Column] = (), **kw) -> SlayerModel:
    return SlayerModel(
        name=name, data_source="test", sql_table="orders",
        columns=[Column(name="id", type=DataType.INT, primary_key=True), Column(name="status", type=DataType.TEXT),
                 Column(name="amount", type=DataType.INT), *columns],
        **kw,
    )


def _returns(name: str, *, target: str, **kw) -> SlayerModel:
    return SlayerModel(
        name=name, data_source="test", sql_table="returns",
        columns=[Column(name="id", type=DataType.INT, primary_key=True), Column(name="order_id", type=DataType.INT)],
        joins=[ModelJoin(target_model=target, join_pairs=[["order_id", "id"]])], **kw,
    )


def _with_total(name: str) -> SlayerModel:
    return _orders(name, measures=[ModelMeasure(formula="sum(amount)", name="tot")], filters=["tot > 100"])


@pytest.fixture
async def engine() -> AsyncIterator[SlayerQueryEngine]:
    async with seeded_exec_engine(
        dialect="sqlite", seed=lambda p: seed("sqlite", p), models=table_models(),
    ) as (eng, _db):
        yield eng


async def _save(engine: SlayerQueryEngine, model: SlayerModel, *, door: str) -> None:
    if door == "engine":
        await engine.save_model(model)
    else:
        await engine.storage.save_model(model)


@pytest.mark.parametrize("door", ["engine", "storage"])
class TestSaveRefusesInvalidFilters:
    async def test_filter_on_own_measure(self, engine, door):
        with pytest.raises(ModelFilterError) as exc:
            await _save(engine, _with_total("orders_tot"), door=door)
        assert "model filter 'tot > 100'" in str(exc.value)
        assert await engine.storage.get_model("orders_tot", data_source="test") is None

    async def test_filter_on_joined_windowed_column(self, engine, door):
        await engine.storage.save_model(_orders("ranked_orders", columns=[RANKED]), _validate=False)
        with pytest.raises(ModelFilterError) as exc:
            await _save(engine, _returns("returns_ranked", target="ranked_orders",
                                         filters=["ranked_orders.ranked > 1"]), door=door)
        assert "window" in str(exc.value)
        assert await engine.storage.get_model("returns_ranked", data_source="test") is None

    async def test_filter_on_windowed_column_through_derived_column(self, engine, door):
        model = _orders("orders_rank_plus", columns=[RANKED, Column(name="rank_plus", type=DataType.INT, sql="ranked + 0")],
                        filters=["rank_plus > 1"])
        with pytest.raises(ModelFilterError):
            await _save(engine, model, door=door)


class TestQueryReadsInvalidStoredFilter:
    @pytest.fixture
    async def stored(self, engine) -> SlayerQueryEngine:
        await engine.storage.save_model(_with_total("bad_done"), _validate=False)
        await engine.storage.save_model(_returns("returns_bad", target="bad_done"), _validate=False)
        await engine.storage.save_model(
            _returns("returns_bad_filtered", target="bad_done", filters=["bad_done.id is not null"]), _validate=False,
        )
        derived = _returns("returns_bad_derived", target="bad_done")
        derived.columns.append(Column(name="bad_status", type=DataType.TEXT, sql="bad_done.status"))
        await engine.storage.save_model(derived, _validate=False)
        return engine

    @pytest.mark.parametrize("body", [
        {"source_model": "bad_done", "dimensions": ["id"]},
        {"source_model": "returns_bad", "dimensions": ["id", "bad_done.status"]},
        {"source_model": "returns_bad_filtered", "dimensions": ["id"]},
        {"source_model": "returns_bad_derived", "dimensions": ["id", "bad_status"]},
    ], ids=["source", "join-target", "another-models-filter", "derived-column"])
    async def test_fails_with_the_targets_filter(self, stored, body):
        with pytest.raises(ModelFilterError) as exc:
            await stored.execute(SlayerQuery.model_validate(body))
        assert "model filter 'tot > 100'" in str(exc.value)
