"""AccessFilteredStorage visibility: tag intersection, dependents, unloadable documents, bypass."""

from __future__ import annotations

import warnings
from collections.abc import AsyncGenerator

import pytest

from slayer.core.enums import DataType, RelationshipCardinality
from slayer.core.models import DatasetField, DatasetRelationship, DatasetMetric, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.embeddings import client as embedding_client
from slayer.engine.dataset_reads import datasets_read_by, placeholder
from slayer.storage.access_filtered import AccessFilteredStorage
from tests._dataset_access_fixtures import (
    ACCESS_PARAMS,
    ALL_LOADABLE,
    DS,
    HIDDEN_FROM_FIN,
    MIXED_QUERY,
    PRUNED_EDGE,
    UNLOADABLE,
    UNTAGGED_PUBLIC,
    VISIBLE_TO_FIN,
    AccessStore,
    access_store,
    dataset_ids,
    must_get,
)


@pytest.fixture(params=ACCESS_PARAMS)
async def store(request) -> AsyncGenerator[AccessStore]:
    backend, dialect = request.param
    async with access_store(backend=backend, dialect=dialect) as s:
        yield s


def view(store: AccessStore, *tags: str, bypass: bool = False) -> AccessFilteredStorage:
    return AccessFilteredStorage(store.storage, tags=set(tags), bypass=bypass)


def _unloadable_reader(expression: str) -> SlayerDataset:
    """``reader``, related to the unloadable dataset, with a field ``sql``."""
    return SlayerDataset(
        name="reader", sql_table="pub_items", data_source=DS,
        fields=[DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True), DatasetField(name="leak", datatype=DataType.INTEGER, expression=expression)],
        relationships=[DatasetRelationship(to=UNLOADABLE, from_columns=["id"], to_columns=["id"], cardinality=RelationshipCardinality.ONE_TO_ONE)],
    )


READS_OF_UNLOADABLE = pytest.mark.parametrize(
    "sql", [f"{UNLOADABLE}.id", f"{UNLOADABLE}.onward.id"], ids=["reaching", "passing"],
)


class TestVisibility:
    async def test_tag_intersection_grants_access(self, store: AccessStore) -> None:
        fin = view(store, "fin")
        assert set(await fin.list_datasets(DS)) == VISIBLE_TO_FIN
        assert dataset_ids(await fin._list_all_dataset_identities()) == VISIBLE_TO_FIN

    async def test_caller_without_tags_sees_only_untagged(self, store: AccessStore) -> None:
        assert set(await view(store).list_datasets(DS)) == UNTAGGED_PUBLIC

    async def test_hr_caller_sees_hr_and_its_readers(self, store: AccessStore) -> None:
        expected = (ALL_LOADABLE - {"fin", "fin_hop", "fin_ordered", "fin_report"})
        assert set(await view(store, "hr").list_datasets(DS)) == expected

    async def test_several_tags_union(self, store: AccessStore) -> None:
        assert set(await view(store, "hr", "fin").list_datasets(DS)) == ALL_LOADABLE

    async def test_hidden_model_lookups_are_absent(self, store: AccessStore) -> None:
        fin = view(store, "fin")
        assert await fin.get_dataset("hr", data_source=DS) is None
        assert await fin.get_dataset("hr") is None
        assert await fin.resolve_dataset_identity("hr") is None
        assert await fin.get_dataset_or_builtin("hr") is None
        loaded, _ = await fin.load_datasets(data_source=DS)
        assert {m.name for m in loaded} == VISIBLE_TO_FIN

    async def test_visible_model_loads_through_the_view(self, store: AccessStore) -> None:
        model = await view(store, "fin").get_dataset("fin")
        assert model is not None
        assert model.access_tags == ["fin"]

    async def test_unloadable_model_is_invisible_without_warning(self, store: AccessStore) -> None:
        fin = view(store, "fin")
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            loaded, unloaded = await fin.load_datasets(data_source=DS)
            await fin.list_datasets(DS)
            await fin.list_memories()
        assert not any(UNLOADABLE in str(w.message) for w in caught)
        assert unloaded == []
        assert UNLOADABLE not in {m.name for m in loaded}
        assert UNLOADABLE not in await fin.list_datasets(DS)
        assert await fin.get_dataset(UNLOADABLE, data_source=DS) is None

    async def test_unloadable_model_still_reported_to_bypass(self, store: AccessStore) -> None:
        _, unloaded = await view(store, bypass=True).load_datasets(data_source=DS)
        assert [e.name for e in unloaded] == [UNLOADABLE]

    @pytest.mark.parametrize("kwargs", [{}, {"tags": {"fin"}}, {"bypass": False}], ids=["neither", "no-bypass", "no-tags"])
    async def test_construction_requires_tags_and_bypass(self, store: AccessStore, kwargs: dict) -> None:
        with pytest.raises(TypeError):
            AccessFilteredStorage(store.storage, **kwargs)


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
        assert await everything._list_all_dataset_identities() == await inner._list_all_dataset_identities()
        assert await everything.list_datasets(DS) == await inner.list_datasets(DS)
        assert await everything.get_dataset("fin") == await inner.get_dataset("fin")
        assert (await must_get(everything, "fin")).relationships  # the relationship to hr is kept
        assert await everything.list_memories() == await inner.list_memories()
        model_name = embedding_client.current_model()
        assert (
            await everything.list_embeddings(embedding_model_name=model_name)
            == await inner.list_embeddings(embedding_model_name=model_name)
        )

    async def test_bypass_ignores_tags(self, store: AccessStore) -> None:
        assert set(await view(store, "nobody", bypass=True).list_datasets(DS)) == ALL_LOADABLE | {UNLOADABLE}


class TestDependents:
    @pytest.mark.parametrize("name", sorted(HIDDEN_FROM_FIN - {"hr"}))
    async def test_model_reading_hr_is_hidden(self, store: AccessStore, name: str) -> None:
        fin = view(store, "fin")
        assert name not in await fin.list_datasets(DS)
        assert await fin.get_dataset(name, data_source=DS) is None

    @pytest.mark.parametrize("name", ["fin", "pub_link"])
    async def test_declared_unused_join_does_not_hide(self, store: AccessStore, name: str) -> None:
        assert await view(store, "fin").get_dataset(name, data_source=DS) is not None

    async def test_query_backed_model_on_visible_model_is_visible(self, store: AccessStore) -> None:
        assert await view(store, "fin").get_dataset("fin_report", data_source=DS) is not None

    async def test_untagged_reader_of_tagged_model_is_hidden_from_untagged_caller(self, store: AccessStore) -> None:
        assert "fin_report" not in await view(store).list_datasets(DS)

    @READS_OF_UNLOADABLE
    async def test_reader_of_unloadable_model_is_hidden(self, store: AccessStore, sql: str) -> None:
        await store.storage.save_dataset(_unloadable_reader(sql), _validate=False)
        assert "reader" not in await view(store, "fin").list_datasets(DS)
        assert "reader" in await view(store, bypass=True).list_datasets(DS)


class TestModelsReadBy:
    @pytest.fixture
    async def datasets(self, store: AccessStore) -> dict[str, SlayerDataset]:
        loaded, _ = await store.storage.load_datasets(data_source=DS)
        return {m.name: m for m in loaded}

    def reads(self, subject, datasets: dict[str, SlayerDataset]) -> set[str]:
        return dataset_ids(datasets_read_by(subject, datasets=list(datasets.values())))

    async def test_declared_join_is_not_a_read(self, datasets: dict[str, SlayerDataset]) -> None:
        assert self.reads(datasets["fin"], datasets) == set()
        assert self.reads(datasets["pub_link"], datasets) == set()

    @pytest.mark.parametrize("name", ["pay", "pay_col", "pay_colfilter", "pay_filter", "pay_metric", "pay_agg"])
    async def test_definition_paths_read_their_hops(self, datasets: dict[str, SlayerDataset], name: str) -> None:
        reads = self.reads(datasets[name], datasets)
        assert "hr" in reads
        assert "pub" not in reads

    @pytest.mark.parametrize(("name", "expected"), [
        ("hr_summary", {"hr"}), ("hr_rollup", {"hr_summary"}), ("fin_hop", {"fin", "hr"}),
        ("fin_ordered", {"fin", "hr"}), ("fin_report", {"fin"}),
    ])
    async def test_stage_reads(self, datasets: dict[str, SlayerDataset], name: str, expected: set[str]) -> None:
        assert expected <= self.reads(datasets[name], datasets)

    async def test_query_reads_its_source_and_hops(self, datasets: dict[str, SlayerDataset]) -> None:
        assert {"fin", "hr"} <= self.reads(SlayerQuery.model_validate(MIXED_QUERY), datasets)

    @pytest.mark.parametrize("stage", [
        {"population": "hr", "extensions": [{"dataset": "hr", "metrics": [{"name": "x", "expression": "sum(salary * 2)"}]}]},
        {"population": "pub", "extensions": [
            {"dataset": "pub", "relationships": [{"to": "hr", "from_columns": ["id"], "to_columns": ["id"]}]}]},
        {"population": "pub_link", "dimensions": [f"{PRUNED_EDGE}.dept"]},
        {"population": "fin", "time_dimensions": [{"field": "hr.hired_at", "granularity": "month"}]},
        {"population": "fin", "metrics": [{"metric": "sum(fin.amount)"}], "filter": ["hr.dept = 'eng'"]},
    ], ids=["extension-base", "extension-join", "model-join", "time-dimension-hop", "filter-hop"])
    async def test_query_fields_read_hr(self, datasets: dict[str, SlayerDataset], stage: dict) -> None:
        assert "hr" in self.reads(SlayerQuery.model_validate(stage), datasets)

    async def test_sibling_stage_chain_reads_the_first_source(self, datasets: dict[str, SlayerDataset]) -> None:
        chained = SlayerDataset(name="chained", source_queries=[
            SlayerQuery.model_validate({"name": "s1", "population": "hr", "metrics": [{"metric": "sum(hr.salary)", "alias": "t"}]}),
            SlayerQuery.model_validate({"population": "s1", "metrics": [{"metric": "sum(s1.t)"}]}),
        ])
        reads = self.reads(chained, datasets)
        assert "hr" in reads
        assert "s1" not in reads

    async def test_saved_measure_reads_are_followed_recursively(self, datasets: dict[str, SlayerDataset]) -> None:
        relay = datasets["pub"].model_copy(update={
            "name": "relay",
            "relationships": [DatasetRelationship(to="pay_metric", from_columns=["id"], to_columns=["id"])],
            "metrics": [DatasetMetric(name="relayed", expression="pay_metric.hr_salary_total")],
        })
        assert {"pay_metric", "hr"} <= self.reads(relay, datasets)

    async def test_unresolvable_path_adds_nothing(self, datasets: dict[str, SlayerDataset]) -> None:
        stray = datasets["pub"].model_copy(update={
            "name": "stray", "fields": [*datasets["pub"].fields, DatasetField(name="far", expression="nowhere.x")],
        })
        assert self.reads(stray, datasets) == set()

    @READS_OF_UNLOADABLE
    async def test_path_reaching_a_placeholder_reads_it(self, datasets: dict[str, SlayerDataset], sql: str) -> None:
        stand_in = placeholder((DS, UNLOADABLE))
        assert dataset_ids(datasets_read_by(_unloadable_reader(sql), datasets=[*datasets.values(), stand_in])) == {UNLOADABLE}


def _linked(name: str, *, to: str, fields: list[DatasetField]) -> SlayerDataset:
    return SlayerDataset(
        name=name, sql_table=name, data_source=DS,
        fields=[DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True), *fields],
        relationships=[DatasetRelationship(to=to, from_columns=["id"], to_columns=["id"],
                                           cardinality=RelationshipCardinality.ONE_TO_ONE)],
    )


@pytest.mark.parametrize("leaf", ['"TOTAL"', "TOTAL", "Total"])
def test_a_derived_field_reached_in_another_spelling_reads_what_it_reads(leaf: str) -> None:
    hr = SlayerDataset(name="hr", sql_table="hr", data_source=DS,
                       fields=[DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                               DatasetField(name="salary", datatype=DataType.INTEGER)])
    mid = _linked("mid", to="hr", fields=[DatasetField(name="total", datatype=DataType.INTEGER, expression="hr.salary")])
    reader = _linked("reader", to="mid", fields=[DatasetField(name="leak", datatype=DataType.INTEGER,
                                                              expression=f"mid.{leaf}")])
    assert dataset_ids(datasets_read_by(reader, datasets=[hr, mid, reader])) == {"mid", "hr"}


def test_a_leaf_reaching_several_case_variants_reads_what_each_reads() -> None:
    hr = SlayerDataset(name="hr", sql_table="hr", data_source=DS,
                       fields=[DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                               DatasetField(name="salary", datatype=DataType.INTEGER)])
    mid = _linked("mid", to="hr", fields=[DatasetField(name="total", datatype=DataType.INTEGER, expression="id"),
                                          DatasetField(name="Total", datatype=DataType.INTEGER,
                                                       expression="hr.salary")])
    reader = _linked("reader", to="mid", fields=[DatasetField(name="leak", datatype=DataType.INTEGER,
                                                              expression="mid.TOTAL")])
    assert dataset_ids(datasets_read_by(reader, datasets=[hr, mid, reader])) == {"mid", "hr"}
