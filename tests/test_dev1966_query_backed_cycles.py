"""A self- or transitively-referencing query-backed model raises one cycle error everywhere.

Spec: openspec …/specs/queries/query-backed-inline — "Query-backed cycles are rejected";
…/specs/models/save-validation — "Self-referencing query-backed models are rejected at save time".
"""

from __future__ import annotations

import os
import tempfile
from typing import Awaitable, Callable, List, Tuple

import pytest

from slayer.core.errors import SlayerError
from slayer.core.models import DatasourceConfig, SlayerModel
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage

from tests._dev1966_fixtures import customers_model, m, orders_model, query, seed_sqlite


def _qb(name: str, source: str) -> SlayerModel:
    return SlayerModel(name=name, data_source="test", source_queries=[
        query(source_model=source, measures=[m("amount:sum", "amount")])])


#: shape → (stored models, the model the surface starts from, the expected ordered path).
_SHAPES = {
    "direct": ([_qb("cyc_s", "cyc_s")], "cyc_s", "cyc_s -> cyc_s"),
    "transitive": ([_qb("cyc_a", "cyc_b"), _qb("cyc_b", "cyc_a")], "cyc_a", "cyc_a -> cyc_b -> cyc_a"),
}


async def _raise_via(surface: str, engine: SlayerQueryEngine, models: List[SlayerModel],
                     start: str) -> BaseException:
    calls: dict[str, Callable[[], Awaitable[object]]] = {
        "consumer": lambda: engine.execute(query(source_model=start, measures=[m("amount:sum", "t")])),
        "run_by_name": lambda: engine.execute(start),
        "save_model": lambda: engine.save_model(next(x for x in models if x.name == start)),
        "get_column_types": lambda: engine.get_column_types(start),
    }
    with pytest.raises(SlayerError) as exc:
        await calls[surface]()
    return exc.value


async def _engine(tmp: str, models: List[SlayerModel], *, omit: str = "") -> Tuple[
        SlayerQueryEngine, YAMLStorage]:
    db = os.path.join(tmp, "data.db")
    seed_sqlite(db)
    storage = YAMLStorage(base_dir=os.path.join(tmp, "store"))
    await storage.save_datasource(DatasourceConfig(name="test", type="sqlite", database=db))
    for model in [orders_model(), customers_model(), *models]:
        if model.name != omit:
            await storage.save_model(model, _validate=False)
    return SlayerQueryEngine(storage=storage), storage


@pytest.mark.parametrize("shape", list(_SHAPES))
@pytest.mark.parametrize("surface", ["consumer", "run_by_name", "save_model", "get_column_types"])
async def test_cycle_raises_naming_the_ordered_path(surface, shape) -> None:
    models, start, path = _SHAPES[shape]
    with tempfile.TemporaryDirectory() as tmp:
        # save_model starts from an unsaved model; the rest find it in storage.
        engine, storage = await _engine(tmp, models, omit=start if surface == "save_model" else "")
        exc = await _raise_via(surface, engine, models, start)
        assert type(exc) is not SlayerError
        assert path in str(exc), str(exc)
        if surface == "save_model":
            assert await storage.get_model(start, data_source="test") is None


async def test_every_surface_raises_the_same_type() -> None:
    types = set()
    for models, start, _ in _SHAPES.values():
        for surface in ["consumer", "run_by_name", "save_model", "get_column_types"]:
            with tempfile.TemporaryDirectory() as tmp:
                engine, _ = await _engine(tmp, models, omit=start if surface == "save_model" else "")
                types.add(type(await _raise_via(surface, engine, models, start)))
    assert len(types) == 1, types
