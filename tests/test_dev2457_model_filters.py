"""A window written in a model's own filters reads the dataset the filters define: a filter cycle.

Spec: models/model-filters › Filters whose dataset reaches itself are refused (scenario "A window written in a
model's filters is refused"). orders (id, status, amount): ``tests/_dev2210_fixtures.py``.
"""

from __future__ import annotations

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import DatasetFilterCycleError
from slayer.core.models import SlayerDataset
from tests.test_dev2432_windowed_columns import _col, _run, _save, filter_engine

__all__ = ["filter_engine"]

WINDOW_FILTER = "rank() over (order by amount) = 1"


def _orders_top(*filters: str) -> SlayerDataset:
    return SlayerDataset(name="orders_top", data_source="test", sql_table="orders", filters=list(filters), fields=[
        _col("id", primary_key=True), _col("status", DataType.STRING), _col("amount")])


def _is_the_own_cycle(error: DatasetFilterCycleError) -> None:
    assert error.cycle == ["orders_top", "orders_top"]
    assert "orders_top → orders_top" in str(error)


@pytest.mark.parametrize("door", ["engine", "storage"])
async def test_a_window_written_in_a_models_filters_is_refused(filter_engine, door):
    dataset = _orders_top(WINDOW_FILTER)
    with pytest.raises(DatasetFilterCycleError) as exc:
        await _save(filter_engine, dataset, door=door)
    _is_the_own_cycle(exc.value)
    assert await filter_engine.storage.get_dataset("orders_top", data_source="test") is None


@pytest.mark.parametrize("door", ["engine", "storage"])
async def test_a_refused_window_filter_leaves_the_stored_model_unchanged(filter_engine, door):
    await _save(filter_engine, _orders_top("amount >= 200"), door=door)
    dataset = _orders_top(WINDOW_FILTER)
    with pytest.raises(DatasetFilterCycleError) as exc:
        await _save(filter_engine, dataset, door=door)
    _is_the_own_cycle(exc.value)
    stored = await filter_engine.storage.get_dataset("orders_top", data_source="test")
    assert stored is not None
    assert stored.filters == ["amount >= 200"]


async def test_a_stored_window_filter_fails_the_query(filter_engine):
    await filter_engine.storage.save_dataset(_orders_top(WINDOW_FILTER), _validate=False)
    query = {"population": "orders_top", "dimensions": ["orders_top.id"]}
    with pytest.raises(DatasetFilterCycleError) as exc:
        await _run(filter_engine, query)
    _is_the_own_cycle(exc.value)
