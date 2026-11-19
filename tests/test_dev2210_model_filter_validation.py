"""Model filters are validated per reference against the owning model, at both save doors and for every model a query reads."""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import DatasetFilterError
from slayer.core.models import DatasetField, DatasetRelationship, DatasetMetric, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from tests._dev2210_fixtures import seed, table_datasets
from tests._engine_helpers import seeded_exec_engine

RANKED = DatasetField(name="ranked", datatype=DataType.INTEGER, sql="rank() over (order by amount)")


def _orders(name: str, *, columns: Sequence[DatasetField] = (), **kw) -> SlayerDataset:
    return SlayerDataset(
        name=name, data_source="test", sql_table="orders",
        fields=[DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True), DatasetField(name="status", datatype=DataType.STRING),
                 DatasetField(name="amount", datatype=DataType.INTEGER), *columns],
        **kw,
    )


def _returns(name: str, *, target: str, **kw) -> SlayerDataset:
    return SlayerDataset(
        name=name, data_source="test", sql_table="returns",
        fields=[DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True), DatasetField(name="order_id", datatype=DataType.INTEGER)],
        relationships=[DatasetRelationship(to=target, from_columns=["order_id"], to_columns=["id"])], **kw,
    )


def _with_total(name: str) -> SlayerDataset:
    return _orders(name, metrics=[DatasetMetric(expression="sum(amount)", name="tot")], filters=["tot > 100"])


@pytest.fixture
async def engine() -> AsyncIterator[SlayerQueryEngine]:
    async with seeded_exec_engine(
        dialect="sqlite", seed=lambda p: seed("sqlite", p), datasets=table_datasets(),
    ) as (eng, _db):
        yield eng


async def _save(engine: SlayerQueryEngine, dataset: SlayerDataset, *, door: str) -> None:
    if door == "engine":
        await engine.save_dataset(dataset)
    else:
        await engine.storage.save_dataset(dataset)


@pytest.mark.parametrize("door", ["engine", "storage"])
class TestSaveRefusesInvalidFilters:
    async def test_filter_on_own_measure(self, engine: SlayerQueryEngine, door):
        model = _with_total("orders_tot")
        with pytest.raises(DatasetFilterError) as exc:
            await _save(engine, model, door=door)
        assert "dataset filter 'tot > 100'" in str(exc.value)
        assert await engine.storage.get_dataset("orders_tot", data_source="test") is None

    async def test_filter_on_joined_windowed_column(self, engine: SlayerQueryEngine, door):
        await engine.storage.save_dataset(_orders("ranked_orders", columns=[RANKED]), _validate=False)
        model = _returns("returns_ranked", target="ranked_orders", filters=["ranked_orders.ranked > 1"])
        with pytest.raises(DatasetFilterError) as exc:
            await _save(engine, model, door=door)
        assert "window" in str(exc.value)
        assert await engine.storage.get_dataset("returns_ranked", data_source="test") is None

    async def test_filter_on_windowed_column_through_derived_column(self, engine, door):
        model = _orders("orders_rank_plus", columns=[RANKED, DatasetField(name="rank_plus", datatype=DataType.INTEGER, sql="ranked + 0")],
                        filters=["rank_plus > 1"])
        with pytest.raises(DatasetFilterError):
            await _save(engine, model, door=door)


class TestQueryReadsInvalidStoredFilter:
    @pytest.fixture
    async def stored(self, engine: SlayerQueryEngine) -> SlayerQueryEngine:
        await engine.storage.save_dataset(_with_total("bad_done"), _validate=False)
        await engine.storage.save_dataset(_returns("returns_bad", target="bad_done"), _validate=False)
        await engine.storage.save_dataset(
            _returns("returns_bad_filtered", target="bad_done", filters=["bad_done.id is not null"]), _validate=False,
        )
        derived = _returns("returns_bad_derived", target="bad_done")
        derived.fields.append(DatasetField(name="bad_status", datatype=DataType.STRING, sql="bad_done.status"))
        await engine.storage.save_dataset(derived, _validate=False)
        return engine

    @pytest.mark.parametrize("body", [
        {"population": "bad_done", "dimensions": ["bad_done.id"]},
        {"population": "returns_bad", "dimensions": ["returns_bad.id", "bad_done.status"]},
        {"population": "returns_bad_filtered", "dimensions": ["returns_bad_filtered.id"]},
        {"population": "returns_bad_derived", "dimensions": ["returns_bad_derived.id", "returns_bad_derived.bad_status"]},
    ], ids=["source", "join-target", "another-models-filter", "derived-column"])
    async def test_fails_with_the_targets_filter(self, stored, body):
        query = SlayerQuery.model_validate(body)
        with pytest.raises(DatasetFilterError) as exc:
            await stored.execute(query)
        assert "dataset filter 'tot > 100'" in str(exc.value)
