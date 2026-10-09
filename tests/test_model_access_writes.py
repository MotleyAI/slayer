"""Non-bypass writes through a TagFilteredStorage merge onto the full documents and never reveal hidden parts."""

from __future__ import annotations

from collections.abc import AsyncGenerator, Callable

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import AccessTagsEditError, HiddenContentConflictError, MemoryNotFoundError, SlayerError
from slayer.core.models import Column, DatasourceConfig, ModelJoin, SlayerModel
from slayer.embeddings import client as embedding_client
from slayer.embeddings.models import Embedding, EntityKind
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.tag_filtered import TagFilteredStorage
from tests._model_access_fixtures import (
    ACCESS_PARAMS,
    DS,
    FIN_MEMORY,
    HIDDEN_TOKEN,
    HR_ONLY_MEMORY,
    MIXED_MEMORY,
    PRUNED_EDGE,
    AccessStore,
    access_store,
    mcp_text,
    must_get,
    outcome,
)

SAVERS = ["storage", "engine"]


@pytest.fixture(params=ACCESS_PARAMS)
async def store(request) -> AsyncGenerator[AccessStore]:
    backend, dialect = request.param
    async with access_store(backend=backend, dialect=dialect) as s:
        yield s


def view(store: AccessStore, *tags: str, bypass: bool = False) -> TagFilteredStorage:
    return TagFilteredStorage(store.storage, tags=set(tags), bypass=bypass)


async def save(storage: TagFilteredStorage, model: SlayerModel, *, saver: str) -> None:
    if saver == "storage":
        await storage.save_model(model)
        return
    engine = SlayerQueryEngine(storage=storage)
    try:
        await engine.save_model(model)
    finally:
        engine.close()


def foreign_leaks(text: str, *, own: str | None = None) -> list[str]:
    """Hidden names in ``text`` other than the caller's own ``own`` token."""
    return [t for t in HIDDEN_TOKEN.findall(text) if t != own]


def test_errors_are_typed() -> None:
    assert issubclass(HiddenContentConflictError, SlayerError)
    assert issubclass(AccessTagsEditError, SlayerError)


@pytest.mark.parametrize("saver", SAVERS)
class TestMergedModelWrites:
    async def test_edit_keeps_pruned_join(self, store: AccessStore, saver: str) -> None:
        fin = view(store, "fin")
        model = await must_get(fin, "fin")
        assert model.joins == []
        doubled = Column(name="doubled", type=DataType.DOUBLE, sql="amount * 2")
        await save(fin, model.model_copy(update={"columns": [*model.columns, doubled]}), saver=saver)
        stored = await must_get(store.storage, "fin")
        assert stored.get_column("doubled") is not None
        assert [j.target_model for j in stored.joins] == ["hr"]
        assert (await must_get(fin, "fin")).joins == []

    async def test_edit_keeps_pruned_named_edge(self, store: AccessStore, saver: str) -> None:
        fin = view(store, "fin")
        model = await must_get(fin, "pub_link")
        await save(fin, model.model_copy(update={"description": "linked items"}), saver=saver)
        stored = await must_get(store.storage, "pub_link")
        assert stored.description == "linked items"
        assert [j.name for j in stored.joins] == [PRUNED_EDGE]


def _new_model_named_hr(_: SlayerModel, __: SlayerModel) -> SlayerModel:
    return SlayerModel(name="hr", sql_table="pub_items", data_source=DS,
                       columns=[Column(name="id", type=DataType.INT, primary_key=True)])


def _edge_named_like_hidden_model(fin: SlayerModel, _: SlayerModel) -> SlayerModel:
    return fin.model_copy(update={"joins": [ModelJoin(target_model="pub", name="hr", join_pairs=[["staff_id", "id"]])]})


def _edge_named_like_pruned_edge(_: SlayerModel, pub_link: SlayerModel) -> SlayerModel:
    return pub_link.model_copy(update={"joins": [ModelJoin(target_model="fin", name=PRUNED_EDGE, join_pairs=[["id", "id"]])]})


def _without_key_column(fin: SlayerModel, _: SlayerModel) -> SlayerModel:
    return fin.model_copy(update={"columns": [c for c in fin.columns if c.name != "staff_id"]})


def _key_column(fin: SlayerModel, **update) -> SlayerModel:
    columns = [c.model_copy(update=update) if c.name == "staff_id" else c for c in fin.columns]
    return fin.model_copy(update={"columns": columns})


CONFLICTS: dict[str, tuple[Callable[[SlayerModel, SlayerModel], SlayerModel], str | None, str]] = {
    "hidden-model-name": (_new_model_named_hr, "hr", "hr"),
    "edge-named-like-hidden-model": (_edge_named_like_hidden_model, "hr", "fin"),
    "edge-named-like-pruned-edge": (_edge_named_like_pruned_edge, None, "pub_link"),
    "removed-key-column": (_without_key_column, None, "fin"),
    "expression-key-column": (lambda fin, _: _key_column(fin, sql="id + 0"), None, "fin"),
    "filtered-key-column": (lambda fin, _: _key_column(fin, filter="staff_id > 0"), None, "fin"),
}


@pytest.mark.parametrize("saver", SAVERS)
@pytest.mark.parametrize("case", sorted(CONFLICTS))
async def test_conflict_with_hidden_content_is_refused(store: AccessStore, saver: str, case: str) -> None:
    build, own, stored_name = CONFLICTS[case]
    fin = view(store, "fin")
    before = await store.storage.get_model(stored_name, data_source=DS)
    edited = build(await must_get(fin, "fin"), await must_get(fin, "pub_link"))
    with pytest.raises(HiddenContentConflictError) as excinfo:
        await save(fin, edited, saver=saver)
    assert foreign_leaks(str(excinfo.value), own=own) == []
    assert await store.storage.get_model(stored_name, data_source=DS) == before


@pytest.mark.parametrize("saver", SAVERS)
async def test_join_to_hidden_model_fails_like_join_to_missing_one(store: AccessStore, saver: str) -> None:
    fin = view(store, "fin")
    pub = await must_get(fin, "pub")
    before = await store.storage.get_model("pub", data_source=DS)

    def joined_to(target: str) -> SlayerModel:
        return pub.model_copy(update={"joins": [ModelJoin(target_model=target, join_pairs=[["id", "id"]])]})

    hidden = await outcome(save(fin, joined_to("hr"), saver=saver))
    missing = await outcome(save(fin, joined_to("nosuch"), saver=saver))
    assert hidden[0] != "ok"
    assert hidden[0] != HiddenContentConflictError.__name__
    assert (hidden[0], hidden[1].replace("hr", "X")) == (missing[0], missing[1].replace("nosuch", "X"))
    assert await store.storage.get_model("pub", data_source=DS) == before


@pytest.mark.parametrize("saver", SAVERS)
@pytest.mark.parametrize("unwrapped", [True, False], ids=["unwrapped", "bypass"])
async def test_editors_still_save_dangling_joins(store: AccessStore, saver: str, unwrapped: bool) -> None:
    pub = await must_get(store.storage, "pub")
    dangling = pub.model_copy(update={"joins": [ModelJoin(target_model="nosuch", join_pairs=[["id", "id"]])]})
    target = store.storage if unwrapped else view(store, bypass=True)
    if saver == "storage":
        await target.save_model(dangling)
    else:
        engine = SlayerQueryEngine(storage=target)
        try:
            await engine.save_model(dangling)
        finally:
            engine.close()
    assert [j.target_model for j in (await must_get(store.storage, "pub")).joins] == ["nosuch"]


@pytest.mark.parametrize("saver", SAVERS)
class TestAccessTags:
    async def test_retag_is_refused(self, store: AccessStore, saver: str) -> None:
        fin = view(store, "fin")
        model = await must_get(fin, "fin")
        for tags in (["fin", "hr"], []):
            retagged = model.model_copy(update={"access_tags": tags})
            with pytest.raises(AccessTagsEditError):
                await save(fin, retagged, saver=saver)
        assert (await must_get(store.storage, "fin")).access_tags == ["fin"]

    async def test_access_tags_error_comes_first(self, store: AccessStore, saver: str) -> None:
        fin = view(store, "fin")
        model = await must_get(fin, "fin")
        conflicting = _without_key_column(model, model).model_copy(update={"access_tags": ["hr"]})
        with pytest.raises(AccessTagsEditError):
            await save(fin, conflicting, saver=saver)

    async def test_tagged_create_is_refused(self, store: AccessStore, saver: str) -> None:
        fresh = SlayerModel(name="fresh", sql_table="pub_items", data_source=DS, access_tags=["fin"],
                            columns=[Column(name="id", type=DataType.INT, primary_key=True)])
        fin = view(store, "fin")
        with pytest.raises(AccessTagsEditError):
            await save(fin, fresh, saver=saver)
        assert await store.storage.get_model("fresh", data_source=DS) is None

    async def test_bypass_may_retag(self, store: AccessStore, saver: str) -> None:
        everything = view(store, bypass=True)
        model = await must_get(everything, "pub")
        await save(everything, model.model_copy(update={"access_tags": ["ops"]}), saver=saver)
        assert (await must_get(store.storage, "pub")).access_tags == ["ops"]


class TestDeletes:
    async def test_deleting_hidden_model_is_a_miss(self, store: AccessStore) -> None:
        assert await view(store, "fin").delete_model("hr", data_source=DS) is False
        assert await view(store, "fin").delete_model("hr") is False
        assert await store.storage.get_model("hr", data_source=DS) is not None

    async def test_deleting_hidden_memory_is_not_found(self, store: AccessStore) -> None:
        fin = view(store, "fin")
        with pytest.raises(MemoryNotFoundError):
            await fin.delete_memory(HR_ONLY_MEMORY)
        assert await store.storage.get_memory_row(HR_ONLY_MEMORY) is not None

    async def test_deleting_visible_model_cascades_over_the_full_store(self, store: AccessStore) -> None:
        assert await view(store, "fin").delete_model("fin", data_source=DS) is True
        mixed = await store.storage.get_memory(MIXED_MEMORY)
        assert mixed.entities == [f"{DS}.hr"]
        assert mixed.query is not None
        rows = await store.storage.list_embeddings(embedding_model_name=embedding_client.current_model())
        assert not any(r.canonical_id.startswith(f"{DS}.fin") for r in rows)

    async def test_deleting_datasource_cascades_over_the_full_store(self, store: AccessStore) -> None:
        assert await view(store, "fin").delete_datasource(DS) is True
        assert (await store.storage.get_memory(HR_ONLY_MEMORY)).entities == []
        rows = await store.storage.list_embeddings(embedding_model_name=embedding_client.current_model())
        assert not any(r.canonical_id == DS or r.canonical_id.startswith(f"{DS}.") for r in rows)

    async def test_deleting_visible_memory_strips_it_from_hidden_memories(self, store: AccessStore) -> None:
        await store.storage.save_memory(id="hrref", learning="see the ledger note",
                                        entities=[f"{DS}.hr", f"memory:{FIN_MEMORY}"])
        await view(store, "fin").delete_memory(FIN_MEMORY)
        assert (await store.storage.get_memory("hrref")).entities == [f"{DS}.hr"]


class TestMergedMemoryWrites:
    async def test_upsert_keeps_withheld_entities_and_query(self, store: AccessStore) -> None:
        fin = view(store, "fin")
        seen = await fin.get_memory(MIXED_MEMORY)
        await fin.save_memory(id=MIXED_MEMORY, learning="Updated ledger note", entities=seen.entities, query=seen.query)
        stored = await store.storage.get_memory(MIXED_MEMORY)
        assert stored.learning == "Updated ledger note"
        assert set(stored.entities) == {f"{DS}.hr", f"{DS}.fin"}
        assert stored.query is not None

    async def test_entity_cleanup_keeps_withheld_parts(self, store: AccessStore) -> None:
        assert await view(store, "fin").strip_dangling_entities_from_memories(canonical_id=f"{DS}.fin") >= 1
        stored = await store.storage.get_memory(MIXED_MEMORY)
        assert stored.entities == [f"{DS}.hr"]
        assert stored.query is not None

    async def test_upsert_on_hidden_id_is_refused(self, store: AccessStore) -> None:
        fin = view(store, "fin")
        with pytest.raises(HiddenContentConflictError) as excinfo:
            await fin.save_memory(id=HR_ONLY_MEMORY, learning="overwrite", entities=[])
        assert foreign_leaks(str(excinfo.value), own="hr") == []
        assert (await store.storage.get_memory(HR_ONLY_MEMORY)).learning != "overwrite"


class TestEmbeddingWrites:
    def row(self, canonical_id: str, kind: EntityKind = "model") -> Embedding:
        return Embedding(canonical_id=canonical_id, embedding_model_name=embedding_client.current_model(),
                         entity_kind=kind, content_hash="new", embedding=[0.0, 1.0])

    @pytest.mark.parametrize(("canonical_id", "kind"), [
        (f"{DS}.hr", "model"), (f"{DS}.hr.dept", "column"), (f"memory:{HR_ONLY_MEMORY}", "memory"),
    ])
    async def test_write_for_hidden_id_is_refused(self, store: AccessStore, canonical_id: str, kind: EntityKind) -> None:
        fin = view(store, "fin")
        row = self.row(canonical_id, kind)
        rows = [self.row(f"{DS}.fin"), self.row(canonical_id, kind)]
        with pytest.raises(HiddenContentConflictError):
            await fin.save_embedding(row)
        with pytest.raises(HiddenContentConflictError):
            await fin.save_embeddings(rows)
        stored = await store.storage.get_embedding(canonical_id=canonical_id, embedding_model_name=embedding_client.current_model())
        assert stored is not None
        assert stored.content_hash != "new"

    async def test_write_for_visible_id_lands(self, store: AccessStore) -> None:
        await view(store, "fin").save_embedding(self.row(f"{DS}.fin"))
        stored = await store.storage.get_embedding(canonical_id=f"{DS}.fin", embedding_model_name=embedding_client.current_model())
        assert stored is not None
        assert stored.content_hash == "new"


class TestMcpEdits:
    async def call(self, storage: TagFilteredStorage, **arguments) -> str:
        return await mcp_text(storage, "edit_model", **arguments)

    async def test_editing_hidden_model_reads_like_missing(self, store: AccessStore) -> None:
        fin = view(store, "fin")
        hidden = await self.call(fin, model_name="hr", description="x")
        missing = await self.call(fin, model_name="nosuch", description="x")
        assert hidden.replace("hr", "X") == missing.replace("nosuch", "X")
        assert (await must_get(store.storage, "hr")).description != "x"

    async def test_moving_model_with_pruned_join_is_refused(self, store: AccessStore) -> None:
        await store.storage.save_datasource(DatasourceConfig(name="acme2", type=store.dialect, database=store.db_path))
        before = await store.storage.get_model("fin", data_source=DS)
        text = await self.call(view(store, "fin"), model_name="fin", data_source=DS, new_data_source="acme2")
        assert "conflict" in text.lower()
        assert foreign_leaks(text) == []
        assert await store.storage.get_model("fin", data_source=DS) == before
        assert await store.storage.get_model("fin", data_source="acme2") is None
