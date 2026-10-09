"""The model-access fixture store builds on both backends and both dialects."""

from __future__ import annotations

import pytest

from slayer.embeddings import client as embedding_client
from tests._model_access_fixtures import (
    ACCESS_PARAMS,
    ALL_LOADABLE,
    DS,
    EMBEDDED,
    UNLOADABLE,
    VISIBLE_TO_FIN,
    access_store,
    reference_store,
)


@pytest.mark.parametrize("params", ACCESS_PARAMS)
async def test_full_store(params: tuple[str, str]) -> None:
    backend, dialect = params
    async with access_store(backend=backend, dialect=dialect) as s:
        assert s.backend == backend and s.dialect == dialect
        loaded, unloaded = await s.storage.load_models(data_source=DS)
        assert {m.name for m in loaded} == ALL_LOADABLE
        assert [e.name for e in unloaded] == [UNLOADABLE]
        assert all(m.columns for m in loaded if m.source_queries)
        rows = await s.storage.list_embeddings(embedding_model_name=embedding_client.current_model())
        assert {r.canonical_id for r in rows} == set(EMBEDDED)


@pytest.mark.parametrize("params", ACCESS_PARAMS)
async def test_reference_store(params: tuple[str, str]) -> None:
    backend, dialect = params
    async with reference_store(backend=backend, dialect=dialect) as s:
        loaded, unloaded = await s.storage.load_models(data_source=DS)
        assert {m.name for m in loaded} == VISIBLE_TO_FIN
        assert unloaded == []
        assert all(not m.joins for m in loaded if m.name in {"fin", "pub_link"})
