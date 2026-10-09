"""Memories and embeddings through a TagFilteredStorage: hidden, stripped, or untouched."""

from __future__ import annotations

from collections.abc import AsyncGenerator

import pytest

from slayer.core.errors import MemoryNotFoundError
from slayer.embeddings import client as embedding_client
from slayer.inspect.service import InspectService
from slayer.search.service import SearchService
from slayer.storage.tag_filtered import TagFilteredStorage
from tests._model_access_fixtures import (
    ACCESS_PARAMS,
    DATASOURCE_MEMORY,
    DS,
    EMBEDDED,
    FIN_MEMORY,
    GHOST_HR_MEMORY,
    GHOST_MEMORY,
    HELP_MEMORY,
    HIDDEN_EMBEDDINGS_FOR_FIN,
    HIDDEN_TOKEN,
    HR_ONLY_MEMORY,
    MEMORY_REF_MEMORY,
    MIXED_MEMORY,
    QUERY_HOP_MEMORY,
    AccessStore,
    access_store,
    outcome,
)

UNTAGGED_MEMORIES = {MEMORY_REF_MEMORY, DATASOURCE_MEMORY, HELP_MEMORY, GHOST_MEMORY, GHOST_HR_MEMORY}
FIN_MEMORIES = UNTAGGED_MEMORIES | {MIXED_MEMORY, FIN_MEMORY}


@pytest.fixture(params=ACCESS_PARAMS)
async def store(request) -> AsyncGenerator[AccessStore]:
    backend, dialect = request.param
    async with access_store(backend=backend, dialect=dialect) as s:
        yield s


def view(store: AccessStore, *tags: str, bypass: bool = False) -> TagFilteredStorage:
    return TagFilteredStorage(store.storage, tags=set(tags), bypass=bypass)


class TestMemoryVisibility:
    async def test_fin_caller_memories(self, store: AccessStore) -> None:
        assert {m.id for m in await view(store, "fin").list_memories()} == FIN_MEMORIES

    async def test_untagged_caller_sees_unlinked_memories(self, store: AccessStore) -> None:
        assert {m.id for m in await view(store).list_memories()} == UNTAGGED_MEMORIES

    @pytest.mark.parametrize("memory_id", [HR_ONLY_MEMORY, QUERY_HOP_MEMORY])
    async def test_hidden_memory_is_not_found(self, store: AccessStore, memory_id: str) -> None:
        fin = view(store, "fin")
        assert await fin.get_memory_row(memory_id) is None
        with pytest.raises(MemoryNotFoundError):
            await fin.get_memory(memory_id)
        loaded, unloaded = await fin.load_memories()
        assert memory_id not in {m.id for m in loaded} | {e.name for e in unloaded}

    async def test_hidden_memory_inspects_like_a_missing_one(self, store: AccessStore) -> None:
        def inspect(memory_id: str):
            return InspectService(storage=view(store, "fin")).inspect(reference=f"memory:{memory_id}", entity_type="memory")

        hidden = await outcome(inspect(HR_ONLY_MEMORY))
        missing = await outcome(inspect("nope"))
        assert tuple(s.replace(HR_ONLY_MEMORY, "X") for s in hidden) == tuple(s.replace("nope", "X") for s in missing)

    async def test_bypass_sees_every_memory_whole(self, store: AccessStore) -> None:
        assert await view(store, bypass=True).list_memories() == await store.storage.list_memories()


class TestMemoryStripping:
    async def test_mixed_memory_loses_hidden_entities_and_query(self, store: AccessStore) -> None:
        memory = await view(store, "fin").get_memory(MIXED_MEMORY)
        assert memory.entities == [f"{DS}.fin"]
        assert memory.query is None

    async def test_listed_copy_is_stripped_too(self, store: AccessStore) -> None:
        listed = {m.id: m for m in await view(store, "fin").list_memories()}
        assert listed[MIXED_MEMORY].entities == [f"{DS}.fin"]
        assert listed[MIXED_MEMORY].query is None

    async def test_link_to_nonexistent_model_is_kept(self, store: AccessStore) -> None:
        assert (await view(store, "fin").get_memory(GHOST_MEMORY)).entities == [f"{DS}.ghost"]

    async def test_nonexistent_link_keeps_memory_visible_without_hidden_entities(self, store: AccessStore) -> None:
        assert (await view(store, "fin").get_memory(GHOST_HR_MEMORY)).entities == [f"{DS}.ghost"]

    async def test_reference_to_hidden_memory_is_stripped(self, store: AccessStore) -> None:
        memory = await view(store, "fin").get_memory(MEMORY_REF_MEMORY)
        assert memory.entities == []

    async def test_stripping_does_not_change_the_stored_memory(self, store: AccessStore) -> None:
        await view(store, "fin").get_memory(MIXED_MEMORY)
        stored = await store.storage.get_memory(MIXED_MEMORY)
        assert stored.entities == [f"{DS}.hr", f"{DS}.fin"]
        assert stored.query is not None

    async def test_visible_memory_with_visible_query_keeps_it(self, store: AccessStore) -> None:
        memory = await view(store, "hr", "fin").get_memory(MIXED_MEMORY)
        assert memory.query is not None
        assert memory.entities == [f"{DS}.hr", f"{DS}.fin"]

    async def test_withheld_query_raises_no_stale_warning(self, store: AccessStore) -> None:
        response = await SearchService(storage=view(store, "fin")).search(
            question="Ledger by department", entities=[f"{DS}.fin"], compact=False, max_results=50,
        )
        hits = {h.id: h for h in response.results if h.kind == "memory"}
        assert MIXED_MEMORY in hits
        assert hits[MIXED_MEMORY].query is None
        assert not any("stale" in w for w in response.warnings), response.warnings
        assert HIDDEN_TOKEN.findall(" ".join(response.warnings)) == []

    async def test_named_memory_ref_raises_no_stale_warning(self, store: AccessStore) -> None:
        response = await SearchService(storage=view(store, "fin")).search(entities=[f"memory:{MIXED_MEMORY}"], max_results=50)
        assert not any("stale" in w for w in response.warnings), response.warnings


class TestEmbeddings:
    @pytest.fixture
    def model_name(self) -> str:
        return embedding_client.current_model()

    async def test_listing_omits_hidden_rows(self, store: AccessStore, model_name: str) -> None:
        rows = await view(store, "fin").list_embeddings(embedding_model_name=model_name)
        assert {r.canonical_id for r in rows} == set(EMBEDDED) - HIDDEN_EMBEDDINGS_FOR_FIN

    @pytest.mark.parametrize("canonical_id", sorted(HIDDEN_EMBEDDINGS_FOR_FIN))
    async def test_hidden_row_is_absent(self, store: AccessStore, model_name: str, canonical_id: str) -> None:
        fin = view(store, "fin")
        assert await fin.get_embedding(canonical_id=canonical_id, embedding_model_name=model_name) is None

    async def test_visible_row_is_present(self, store: AccessStore, model_name: str) -> None:
        row = await view(store, "fin").get_embedding(canonical_id=f"{DS}.fin.amount", embedding_model_name=model_name)
        assert row is not None

    async def test_batch_read_omits_hidden_rows(self, store: AccessStore, model_name: str) -> None:
        rows = await view(store, "fin").get_embeddings_for_canonical_ids(
            canonical_ids=sorted(EMBEDDED), embedding_model_name=model_name,
        )
        assert set(rows) == set(EMBEDDED) - HIDDEN_EMBEDDINGS_FOR_FIN

    async def test_bypass_reads_every_row(self, store: AccessStore, model_name: str) -> None:
        rows = await view(store, bypass=True).list_embeddings(embedding_model_name=model_name)
        assert {r.canonical_id for r in rows} == set(EMBEDDED)
