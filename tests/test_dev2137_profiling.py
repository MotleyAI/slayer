"""List columns are profiled over their elements (spec: models/sample-profiling)."""

from __future__ import annotations

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.profiling import ensure_samples_fresh
from tests._dev2120_fixtures import contractors_engine, contractors_model


def _record(*, engine, monkeypatch) -> list[SlayerQuery]:
    log: list[SlayerQuery] = []
    real = engine.execute

    async def recording(*args, **kwargs):
        q = kwargs.get("query", args[0] if args else None)
        log.append(q if isinstance(q, SlayerQuery) else SlayerQuery.model_validate(q))
        return await real(*args, **kwargs)

    monkeypatch.setattr(engine, "execute", recording)
    return log


async def _profile(engine, name: str, monkeypatch, *, model: SlayerModel | None = None):
    stored = await engine.storage.get_model((model or contractors_model()).name, data_source="test")
    column = stored.get_column(name)
    log = _record(engine=engine, monkeypatch=monkeypatch)
    outcome = await ensure_samples_fresh(model=stored, columns=[column], engine=engine, storage=engine.storage)
    (profiled,) = outcome.columns
    return profiled, outcome, log


@pytest.fixture
async def engine():
    async with contractors_engine() as e:
        yield e


class TestElements:
    async def test_text_elements(self, engine, monkeypatch) -> None:
        skills, outcome, log = await _profile(engine, "skills", monkeypatch)
        assert outcome.errors == []
        assert skills.sampled_values == ["go", "sql"]
        assert skills.distinct_count == 2
        assert log, "profiling ran no query"

    async def test_whole_list_never_sampled(self, engine, monkeypatch) -> None:
        _, _, log = await _profile(engine, "skills", monkeypatch)
        for q in log:
            assert "skills" not in [d if isinstance(d, str) else getattr(d, "name", None) for d in q.dimensions or []]

    async def test_numeric_elements(self, engine, monkeypatch) -> None:
        scores, outcome, _ = await _profile(engine, "scores", monkeypatch)
        assert outcome.errors == []
        assert scores.sampled == "1 .. 9"


GRIDS = ("CREATE TABLE grids (id INTEGER, grid INTEGER[][])", "INSERT INTO grids VALUES (1, [[1, 2], [3]])")


class TestOpaqueElements:
    async def test_never_profiled(self, monkeypatch) -> None:
        grids = SlayerModel(name="grids", sql_table="grids", data_source="test", columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="grid", type=DataType.ARRAY, element_type=DataType.UNKNOWN)])
        async with contractors_engine(models=[contractors_model(), grids], extra=GRIDS) as e:
            grid, _, log = await _profile(e, "grid", monkeypatch, model=grids)
        assert log == []
        assert grid.sampled_values is None
