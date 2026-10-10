"""TagFilteredStorage visibility: tag intersection, dependents, unloadable documents, bypass."""

from __future__ import annotations

import warnings
from collections.abc import AsyncGenerator

import pytest

from slayer.core.models import Column, ModelJoin, ModelMeasure, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.embeddings import client as embedding_client
from slayer.engine.model_reads import models_read_by
from slayer.storage.tag_filtered import TagFilteredStorage
from tests._model_access_fixtures import (
    ACCESS_PARAMS,
    ALL_LOADABLE,
    DS,
    HIDDEN_FROM_FIN,
    MIXED_QUERY,
    UNLOADABLE,
    UNTAGGED_PUBLIC,
    VISIBLE_TO_FIN,
    AccessStore,
    access_store,
    model_ids,
    must_get,
)


@pytest.fixture(params=ACCESS_PARAMS)
async def store(request) -> AsyncGenerator[AccessStore]:
    backend, dialect = request.param
    async with access_store(backend=backend, dialect=dialect) as s:
        yield s


def view(store: AccessStore, *tags: str, bypass: bool = False) -> TagFilteredStorage:
    return TagFilteredStorage(store.storage, tags=set(tags), bypass=bypass)


class TestVisibility:
    async def test_tag_intersection_grants_access(self, store: AccessStore) -> None:
        fin = view(store, "fin")
        assert set(await fin.list_models(DS)) == VISIBLE_TO_FIN
        assert model_ids(await fin._list_all_model_identities()) == VISIBLE_TO_FIN

    async def test_caller_without_tags_sees_only_untagged(self, store: AccessStore) -> None:
        assert set(await view(store).list_models(DS)) == UNTAGGED_PUBLIC

    async def test_hr_caller_sees_hr_and_its_readers(self, store: AccessStore) -> None:
        expected = (ALL_LOADABLE - {"fin", "fin_hop", "fin_ordered", "fin_report"})
        assert set(await view(store, "hr").list_models(DS)) == expected

    async def test_several_tags_union(self, store: AccessStore) -> None:
        assert set(await view(store, "hr", "fin").list_models(DS)) == ALL_LOADABLE

    async def test_hidden_model_lookups_are_absent(self, store: AccessStore) -> None:
        fin = view(store, "fin")
        assert await fin.get_model("hr", data_source=DS) is None
        assert await fin.get_model("hr") is None
        assert await fin.resolve_model_identity("hr") is None
        assert await fin.get_model_or_builtin("hr") is None
        loaded, _ = await fin.load_models(data_source=DS)
        assert {m.name for m in loaded} == VISIBLE_TO_FIN

    async def test_visible_model_loads_through_the_view(self, store: AccessStore) -> None:
        model = await view(store, "fin").get_model("fin")
        assert model is not None
        assert model.access_tags == ["fin"]

    async def test_unloadable_model_is_invisible_without_warning(self, store: AccessStore) -> None:
        fin = view(store, "fin")
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            loaded, unloaded = await fin.load_models(data_source=DS)
            await fin.list_models(DS)
            await fin.list_memories()
        assert not any(UNLOADABLE in str(w.message) for w in caught)
        assert unloaded == []
        assert UNLOADABLE not in {m.name for m in loaded}
        assert UNLOADABLE not in await fin.list_models(DS)
        assert await fin.get_model(UNLOADABLE, data_source=DS) is None

    async def test_unloadable_model_still_reported_to_bypass(self, store: AccessStore) -> None:
        _, unloaded = await view(store, bypass=True).load_models(data_source=DS)
        assert [e.name for e in unloaded] == [UNLOADABLE]

    @pytest.mark.parametrize("kwargs", [{}, {"tags": {"fin"}}, {"bypass": False}], ids=["neither", "no-bypass", "no-tags"])
    async def test_construction_requires_tags_and_bypass(self, store: AccessStore, kwargs: dict) -> None:
        with pytest.raises(TypeError):
            TagFilteredStorage(store.storage, **kwargs)


class TestWrapperPlumbing:
    async def test_filename_collision_flag_is_copied(self, store: AccessStore) -> None:
        assert view(store, "fin")._ids_collide_as_filenames is store.storage._ids_collide_as_filenames

    async def test_aclose_is_forwarded(self, store: AccessStore, monkeypatch: pytest.MonkeyPatch) -> None:
        closed: list[bool] = []

        async def aclose() -> None:  # NOSONAR(S7503) — stub matches the awaited hook
            closed.append(True)

        monkeypatch.setattr(store.storage, "aclose", aclose, raising=False)
        await view(store, "fin").aclose()
        assert closed == [True]


class TestBypass:
    async def test_bypass_reads_equal_the_unwrapped_store(self, store: AccessStore) -> None:
        inner, everything = store.storage, view(store, bypass=True)
        assert await everything._list_all_model_identities() == await inner._list_all_model_identities()
        assert await everything.list_models(DS) == await inner.list_models(DS)
        assert await everything.get_model("fin") == await inner.get_model("fin")
        assert (await must_get(everything, "fin")).joins  # the join to hr is kept
        assert await everything.list_memories() == await inner.list_memories()
        model_name = embedding_client.current_model()
        assert (
            await everything.list_embeddings(embedding_model_name=model_name)
            == await inner.list_embeddings(embedding_model_name=model_name)
        )

    async def test_bypass_ignores_tags(self, store: AccessStore) -> None:
        assert set(await view(store, "nobody", bypass=True).list_models(DS)) == ALL_LOADABLE | {UNLOADABLE}


class TestDependents:
    @pytest.mark.parametrize("name", sorted(HIDDEN_FROM_FIN - {"hr"}))
    async def test_model_reading_hr_is_hidden(self, store: AccessStore, name: str) -> None:
        fin = view(store, "fin")
        assert name not in await fin.list_models(DS)
        assert await fin.get_model(name, data_source=DS) is None

    @pytest.mark.parametrize("name", ["fin", "pub_link"])
    async def test_declared_unused_join_does_not_hide(self, store: AccessStore, name: str) -> None:
        assert await view(store, "fin").get_model(name, data_source=DS) is not None

    async def test_query_backed_model_on_visible_model_is_visible(self, store: AccessStore) -> None:
        assert await view(store, "fin").get_model("fin_report", data_source=DS) is not None

    async def test_untagged_reader_of_tagged_model_is_hidden_from_untagged_caller(self, store: AccessStore) -> None:
        assert "fin_report" not in await view(store).list_models(DS)


class TestModelsReadBy:
    @pytest.fixture
    async def models(self, store: AccessStore) -> dict[str, SlayerModel]:
        loaded, _ = await store.storage.load_models(data_source=DS)
        return {m.name: m for m in loaded}

    def reads(self, subject, models: dict[str, SlayerModel]) -> set[str]:
        return model_ids(models_read_by(subject, models=list(models.values())))

    async def test_declared_join_is_not_a_read(self, models: dict[str, SlayerModel]) -> None:
        assert self.reads(models["fin"], models) == set()
        assert self.reads(models["pub_link"], models) == set()

    @pytest.mark.parametrize("name", ["pay", "pay_col", "pay_colfilter", "pay_filter", "pay_measure", "pay_agg"])
    async def test_definition_paths_read_their_hops(self, models: dict[str, SlayerModel], name: str) -> None:
        reads = self.reads(models[name], models)
        assert "hr" in reads
        assert "pub" not in reads

    @pytest.mark.parametrize(("name", "expected"), [
        ("hr_summary", {"hr"}), ("hr_rollup", {"hr_summary"}), ("fin_hop", {"fin", "hr"}),
        ("fin_ordered", {"fin", "hr"}), ("fin_report", {"fin"}),
    ])
    async def test_stage_reads(self, models: dict[str, SlayerModel], name: str, expected: set[str]) -> None:
        assert expected <= self.reads(models[name], models)

    async def test_query_reads_its_source_and_hops(self, models: dict[str, SlayerModel]) -> None:
        assert {"fin", "hr"} <= self.reads(SlayerQuery.model_validate(MIXED_QUERY), models)

    @pytest.mark.parametrize("stage", [
        {"source_model": {"source_name": "hr", "columns": [{"name": "x", "sql": "salary * 2"}]}},
        {"source_model": {"source_name": "pub", "joins": [{"target_model": "hr", "join_pairs": [["id", "id"]]}]}},
        {"source_model": {"name": "inline", "sql_table": "pub_items", "data_source": DS,
                          "columns": [{"name": "id", "type": "number", "primary_key": True}],
                          "joins": [{"target_model": "hr", "join_pairs": [["id", "id"]]}]}},
        {"source_model": "fin", "time_dimensions": [{"dimension": "hr.hired_at", "granularity": "month"}]},
        {"source_model": "fin", "measures": [{"formula": "sum(amount)"}], "filters": ["hr.dept = 'eng'"]},
    ], ids=["extension-base", "extension-join", "inline-model-join", "time-dimension-hop", "filter-hop"])
    async def test_query_fields_read_hr(self, models: dict[str, SlayerModel], stage: dict) -> None:
        assert "hr" in self.reads(SlayerQuery.model_validate(stage), models)

    async def test_sibling_stage_chain_reads_the_first_source(self, models: dict[str, SlayerModel]) -> None:
        chained = SlayerModel(name="chained", source_queries=[
            SlayerQuery.model_validate({"name": "s1", "source_model": "hr", "measures": [{"formula": "sum(salary)", "name": "t"}]}),
            SlayerQuery.model_validate({"source_model": "s1", "measures": [{"formula": "sum(t)"}]}),
        ])
        reads = self.reads(chained, models)
        assert "hr" in reads
        assert "s1" not in reads

    async def test_saved_measure_reads_are_followed_recursively(self, models: dict[str, SlayerModel]) -> None:
        relay = models["pub"].model_copy(update={
            "name": "relay",
            "joins": [ModelJoin(target_model="pay_measure", join_pairs=[["id", "id"]])],
            "measures": [ModelMeasure(name="relayed", formula="pay_measure.hr_salary_total")],
        })
        assert {"pay_measure", "hr"} <= self.reads(relay, models)

    async def test_unresolvable_path_adds_nothing(self, models: dict[str, SlayerModel]) -> None:
        stray = models["pub"].model_copy(update={
            "name": "stray", "columns": [*models["pub"].columns, Column(name="far", sql="nowhere.x")],
        })
        assert self.reads(stray, models) == set()
