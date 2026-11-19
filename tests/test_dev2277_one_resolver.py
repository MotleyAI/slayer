"""One anchor resolver: binding, inference, recommendation, schema-drift attribution and the stored migration
agree on the directed cycle and on two to-one paths (spec: queries/references › The anchor is reached by its
only to-one path)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from slayer.core.errors import (
    PopulationErrorReason,
    PopulationInferenceError,
    UnresolvableDimensionRelationshipError,
)
from slayer.core.models import DatasourceConfig, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerResponse
from slayer.live_schema.schema_drift import WholeDatasetDelete, field_dependents
from slayer.storage.base import StorageBackend
from slayer.storage.migrations import CURRENT_VERSIONS

from tests._dev2277_fixtures import (
    BACKENDS,
    cycle_datasets,
    cycle_engine,
    execute_recording,
    response_conversions,
    seed_datasets,
    seed_engine,
    table,
)
from tests._stored_upgrade_fixtures import DS as STORE_DS
from tests._stored_upgrade_fixtures import raw_store

#: The pre-change stored versions (this change adds one step to each).
PRE_MODEL, PRE_QUERY = CURRENT_VERSIONS["SlayerModel"] - 1, CURRENT_VERSIONS["SlayerQuery"] - 1


def column(resp: SlayerResponse, suffix: str) -> str:
    """The unique result key equal to ``suffix`` or ending in ``.suffix``."""
    hits = [c for c in resp.columns if c == suffix or c.endswith(f".{suffix}")]
    assert len(hits) == 1, (suffix, resp.columns)
    return hits[0]


def qb(population: str, **stage: Any) -> SlayerDataset:
    query = SlayerQuery.model_validate({"population": population, **stage})
    return SlayerDataset(name="qb", data_source="test", source_queries=[query])


def drops_qb(*, datasets: list[SlayerDataset], dataset: str, field: str) -> bool:
    """Whether dropping ``dataset.field`` cascades to the whole query-backed ``qb``."""
    owner = next(d for d in datasets if d.name == dataset)
    entries = field_dependents(datasets=datasets, dataset=owner, field=field)
    return any(isinstance(e, WholeDatasetDelete) and e.dataset_name == "qb" for e in entries)


async def stored_stage(tmp_path: Path, *, datasets: list[SlayerDataset], stage: dict) -> SlayerQuery:
    """``stage``, stored before this change in saved query ``sq``, as the store loads it."""
    docs = [{"version": PRE_MODEL, **d.model_copy(update={"data_source": STORE_DS}).model_dump(
        mode="json", exclude_none=True, exclude_defaults=True)} for d in datasets]
    saved = {"version": PRE_MODEL, "name": "sq", "data_source": STORE_DS,
             "source_queries": [{"version": PRE_QUERY, **stage}]}
    storage: StorageBackend = await raw_store(
        backend="yaml", base=str(tmp_path / "store"), models=[*docs, saved],
        datasource=DatasourceConfig(name=STORE_DS, type="sqlite", database=":memory:"),
    )
    model = await storage.get_dataset("sq", data_source=STORE_DS)
    assert model is not None and model.source_queries
    return model.source_queries[0]


def metric_texts(stage: SlayerQuery) -> list[str]:
    return [m.metric for m in stage.metrics or []]


def dimension_texts(stage: SlayerQuery) -> list[str]:
    return [d.field for d in stage.dimensions or []]


# --------------------------------------------------------------------------- #
# Directed cycle x → y → z → x: the to-one path x → y → z
# --------------------------------------------------------------------------- #
class TestDirectedCycle:
    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_binding_takes_the_to_one_path(self, backend: str) -> None:
        async with cycle_engine(backend) as eng:
            resp = await eng.execute({"population": "x", "dimensions": ["x.v"], "metrics": ["sum(z.amount)"]})
        assert table(resp, "x.v", column(resp, "amount_sum")) == {("x1", 10.0), ("x2", 20.0)}

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_binding_reads_a_dimension_through_the_to_one_path(self, backend: str) -> None:
        async with cycle_engine(backend) as eng:
            resp = await eng.execute({"population": "x", "dimensions": ["x.v", "z.v"]})
        assert table(resp, "x.v", column(resp, "z.v")) == {("x1", "z1"), ("x2", "z2")}

    async def test_inference_fails_closed_on_a_tie(self) -> None:
        async with cycle_engine("sqlite") as eng:
            with pytest.raises(PopulationInferenceError) as info:
                await eng.execute({"dimensions": ["x.v", "y.v", "z.v"]})
        assert info.value.reason is PopulationErrorReason.TIE
        assert set(info.value.candidates) == {"x", "y", "z"}

    async def test_recommendation_agrees_with_inference(self) -> None:
        async with cycle_engine("sqlite") as eng:
            rec = await eng.recommend_population(["x.v", "y.v", "z.v"])
        assert rec.reachable is False
        assert rec.population is None
        assert {c.dataset_name: set(c.reachable_items) for c in rec.coverage} == {
            name: {"x.v", "y.v", "z.v"} for name in ("x", "y", "z")
        }

    @pytest.mark.parametrize(("dataset", "field", "dropped"), [
        ("y", "z_id", True), ("z", "x_id", False), ("z", "amount", True),
    ], ids=["to-one-path-edge", "direct-edge", "leaf"])
    def test_drift_attributes_along_the_to_one_path(self, dataset: str, field: str, dropped: bool) -> None:
        datasets = [*cycle_datasets(), qb("x", metrics=[{"metric": "sum(z.amount)", "alias": "s"}])]
        assert drops_qb(datasets=datasets, dataset=dataset, field=field) is dropped

    async def test_migration_keeps_the_old_direct_path(self, tmp_path: Path) -> None:
        stage = await stored_stage(tmp_path, datasets=cycle_datasets(), stage={
            "population": "x", "dimensions": ["y.v"], "metrics": ["sum(z.amount)"],
        })
        assert metric_texts(stage) == ["sum(x.z.amount)"]
        assert dimension_texts(stage) == ["y.v"]


# --------------------------------------------------------------------------- #
# Two to-one paths: orders → customers and orders → stores → customers
# --------------------------------------------------------------------------- #
class TestTwoToOnePaths:
    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_binding_refuses_the_dataset_first_reference(self, backend: str) -> None:
        async with seed_engine(backend, stores=True) as eng:
            with pytest.raises(UnresolvableDimensionRelationshipError) as info:
                await eng.execute({"population": "orders", "dimensions": ["customers.region"]})
        assert "orders.customers.region" in str(info.value)
        assert "orders.stores.customers.region" in str(info.value)

    async def test_inference_refuses_the_same_reference(self) -> None:
        async with seed_engine("sqlite", stores=True) as eng:
            with pytest.raises(PopulationInferenceError) as info:
                await eng.execute({"dimensions": ["orders.status", "customers.region"]})
        assert info.value.reason is PopulationErrorReason.AMBIGUOUS_PATH
        assert "orders" in info.value.candidates
        assert "customers.region" in str(info.value)

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_the_explicit_route_agrees_everywhere(self, backend: str) -> None:
        async with seed_engine(backend, stores=True) as eng:
            inferred = await eng.execute({"dimensions": ["orders.status", "orders.customers.region"]})
            named = await eng.execute(
                {"population": "orders", "dimensions": ["orders.status", "orders.customers.region"]})
        assert inferred.population == "orders"
        assert inferred.population_inferred is True
        expected = {("completed", "EAST"), ("open", "EAST"), ("open", "WEST")}
        assert table(inferred, "orders.status", "orders.customers.region") == expected
        assert table(named, "orders.status", "orders.customers.region") == expected

    async def test_recommendation_emits_an_executable_path(self) -> None:
        async with seed_engine("sqlite", stores=True) as eng:
            rec = await eng.recommend_population(["orders.status", "customers.region"])
            assert rec.population == "orders"
            paths = {p.input_item: p.path for p in rec.item_paths}
            assert paths == {"orders.status": "orders.status", "customers.region": "orders.customers.region"}
            resp, _ = await execute_recording(
                eng, {"population": "orders", "dimensions": [paths["orders.status"], paths["customers.region"]]})
            assert response_conversions(resp) == []
            with pytest.raises(UnresolvableDimensionRelationshipError):
                await eng.execute({"population": "orders", "dimensions": ["customers.region"]})
        assert {tuple(r.values()) for r in resp.data} == {("completed", "EAST"), ("open", "EAST"), ("open", "WEST")}

    def test_drift_attributes_the_ambiguous_reference_to_nothing(self) -> None:
        datasets = [*seed_datasets(stores=True), qb("orders", dimensions=["customers.region"])]
        assert drops_qb(datasets=datasets, dataset="customers", field="region") is False

    @pytest.mark.parametrize(("dataset", "field", "dropped"), [
        ("orders", "customer_id", True), ("stores", "customer_id", False), ("customers", "region", True),
    ], ids=["route-edge", "other-path-edge", "leaf"])
    def test_drift_follows_the_explicit_route(self, dataset: str, field: str, dropped: bool) -> None:
        datasets = [*seed_datasets(stores=True), qb("orders", dimensions=["orders.customers.region"])]
        assert drops_qb(datasets=datasets, dataset=dataset, field=field) is dropped

    async def test_migration_spells_the_old_direct_path(self, tmp_path: Path) -> None:
        stage = await stored_stage(tmp_path, datasets=seed_datasets(stores=True), stage={
            "population": "orders", "dimensions": ["customers.region", "stores.store_id"],
        })
        assert dimension_texts(stage) == ["orders.customers.region", "stores.store_id"]
