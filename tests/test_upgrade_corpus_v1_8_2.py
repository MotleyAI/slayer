"""A store written by ``motley-slayer==1.8.2`` over list columns loads, re-saves, re-ingests and queries with 1.8.2's
results."""

from __future__ import annotations

import pytest

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, SlayerDataset
from slayer.core.query import SlayerQuery
from tests._upgrade_corpus import Corpus, OfflineChecks, OnlineChecks, offline

CORPUS = Corpus.load(release="v1_8_2", datasources=("shop",), data_files=("data.duckdb",))


@pytest.fixture(params=sorted(CORPUS.expected))
def key(request) -> str:
    return request.param


@pytest.fixture(params=["shop"])
def data_source(request) -> str:
    return request.param


class TestOnline(OnlineChecks):
    corpus = CORPUS


class TestOffline(OfflineChecks):
    corpus = CORPUS


def _column(model: SlayerDataset | None, name: str) -> DatasetField:
    assert model is not None
    column = model.get_field(name)
    assert column is not None, name
    return column


@pytest.mark.parametrize("backend", ["yaml", "sqlite"])
async def test_the_store_holds_the_list_column_shapes(backend: str, tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    storage = offline(CORPUS, backend=backend, tmp=tmp_path, monkeypatch=monkeypatch)
    contractors = await storage.get_dataset("contractors", data_source="shop")
    pk = _column(contractors, "id")
    assert (pk.primary_key, pk.allowed_aggregations) == (True, ["count", "collect"])
    assert (_column(contractors, "skills").datatype, _column(contractors, "skills").element_type) == (
        DataType.ARRAY, DataType.STRING)
    assert (_column(contractors, "scores").datatype, _column(contractors, "scores").element_type) == (
        DataType.ARRAY, DataType.INTEGER)
    assert contractors is not None
    assert {m.expression for m in contractors.metrics} >= {"collect(id)", "collect(skills.value)", "count(skills)"}
    dev_skills = await storage.get_dataset("dev_skills", data_source="shop")
    assert dev_skills is not None
    assert dev_skills.sql
    assert (_column(dev_skills, "skills").datatype, _column(dev_skills, "skills").element_type) == (
        DataType.ARRAY, DataType.OPAQUE)
    go_teams = await storage.get_dataset("go_teams", data_source="shop")
    assert go_teams is not None
    assert go_teams.source_queries
    stages: list[SlayerQuery] = go_teams.source_queries
    filters = " ".join(str(f) for q in stages for f in (q.filter or []))
    assert "contains(" in filters, filters
    assert "contains_any(" in filters, filters
    memory = await storage.get_memory("m_assignments")
    assert memory.query is not None
    assert "collect(assignments.id)" in [m.metric for m in memory.query.metrics or []]
