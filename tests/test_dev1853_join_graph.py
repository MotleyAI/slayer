"""Edge-aware route search (``slayer.core.relationship_search``) and executable root recommendation.

The graph spans the bidirectional edge set: parallel edges are distinct
routes, recommendations emit edge-name tokens where a hop needs one, and an
ambiguous unnamed hop makes the item unreachable rather than recommended
with a path that would fail.
"""

from __future__ import annotations

import asyncio
import tempfile
from collections.abc import AsyncIterator
from typing import cast

import pytest

from slayer.core.enums import DataType
from slayer.core.relationship_search import count_routes, reachable, shortest_route
from slayer.core.models import DatasetField, DatasourceConfig, DatasetRelationship, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.base import StorageBackend
from slayer.storage.document_loading import DocumentLoadFailures
from slayer.storage.yaml_storage import YAMLStorage

from tests._dev1853_fixtures import chain_datasets, parallel_engine, parallel_datasets
from tests._edge_graph_fixtures import edge_graph
import slayer.engine.bundle_builder



def _by(datasets: list[SlayerDataset]) -> dict[str, SlayerDataset]:
    return {m.name: m for m in datasets}


class TestBidirectionalReachability:
    def test_reverse_neighbors_are_reachable(self) -> None:
        models = _by(chain_datasets())
        assert "orders" in reachable(root=models["customers"], datasets_by_name=models)
        assert "orders" in reachable(root=models["regions"], datasets_by_name=models)

    def test_reverse_route_is_unique_on_a_single_edge(self) -> None:
        models = _by(chain_datasets())
        assert count_routes(root=models["customers"], target="orders", datasets_by_name=models) == 1


class TestParallelEdgesAreDistinctRoutes:
    def test_route_counting_reports_ambiguous(self) -> None:
        models = _by(parallel_datasets(named=False))
        assert count_routes(root=models["orders"], target="customers", datasets_by_name=models, cap=5) == 2
        assert count_routes(root=models["customers"], target="orders", datasets_by_name=models, cap=5) == 2

    def test_named_parallel_edges_also_count_as_two_routes(self) -> None:
        models = _by(parallel_datasets(named=True))
        assert count_routes(root=models["orders"], target="customers", datasets_by_name=models, cap=5) == 2


class TestEdgeNameShadowsModelToken:
    def test_shadowed_bare_token_is_not_executable(self) -> None:
        # Edge a↔b named "c" shadows the bare dataset token "c" (names resolve
        # first), so the a↔c hop has no executable token — unroutable, never
        # a path that would land on the wrong endpoint.
        models = edge_graph(nodes={"a", "b", "c"}, edges=[("a", "b", "c"), ("a", "c", None)])
        assert shortest_route(root=models["a"], target="c", datasets_by_name=models) is None
        route = shortest_route(root=models["a"], target="b", datasets_by_name=models)
        assert route is not None
        assert list(route.tokens) == ["c"]


async def _engine_for(datasets, tmpdir: str) -> SlayerQueryEngine:
    storage = YAMLStorage(base_dir=tmpdir)
    await storage.save_datasource(
        DatasourceConfig(name="test", type="sqlite", database=":memory:"))
    for model in datasets:
        await storage.save_dataset(model)
    return SlayerQueryEngine(storage=storage)


@pytest.fixture
async def named_rec_engine() -> AsyncIterator[SlayerQueryEngine]:
    with tempfile.TemporaryDirectory() as d:
        yield await parallel_engine(d, named=True)


@pytest.fixture
async def unnamed_rec_engine() -> AsyncIterator[SlayerQueryEngine]:
    with tempfile.TemporaryDirectory() as d:
        yield await parallel_engine(d, named=False)


class TestRecommendRootModel:
    async def test_named_pair_recommendation_is_executable(
        self, named_rec_engine: SlayerQueryEngine,
    ) -> None:
        rec = await named_rec_engine.recommend_population(
            ["orders.status", "customers.name"])
        assert rec.reachable is True
        assert rec.population is not None
        paths = {ip.input_item: ip.path for ip in rec.item_paths}
        crossing = next(p for p in paths.values() if "." in p)
        assert crossing.split(".")[0] in {"billing_customer",
                                          "shipping_customer"}, crossing
        # Executable means executable: the emitted paths bind and run.
        resp = await named_rec_engine.execute(SlayerQuery.model_validate(
            {"population": rec.population, "dimensions": list(paths.values())}))
        assert resp.data

    async def test_unnamed_ambiguous_pair_reports_unreachable(
        self, unnamed_rec_engine: SlayerQueryEngine,
    ) -> None:
        rec = await unnamed_rec_engine.recommend_population(
            ["orders.status", "customers.name"])
        assert rec.reachable is False
        assert rec.item_paths == []

    async def test_reverse_reachability_growth(self) -> None:
        # a → b and c → b share no directed common root today; the
        # bidirectional graph makes every dataset a valid root.
        def leaf(name: str, extra: str, joins=None) -> SlayerDataset:
            return SlayerDataset(
                name=name, data_source="test", sql_table=name,
                fields=[DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                         DatasetField(name=extra, datatype=DataType.STRING)],
                relationships=joins or [],
            )
        models = [
            leaf("a", "x", [DatasetRelationship(to="b",
                                      from_columns=["id"], to_columns=["id"])]),
            leaf("b", "y"),
            leaf("c", "z", [DatasetRelationship(to="b",
                                      from_columns=["id"], to_columns=["id"])]),
        ]
        with tempfile.TemporaryDirectory() as d:
            engine = await _engine_for(models, d)
            rec = await engine.recommend_population(["a.x", "c.z"])
            assert rec.reachable is True
            assert {ip.input_item for ip in rec.item_paths} == {"a.x", "c.z"}


class TestPeerLoadsAreConcurrent:
    async def test_peer_models_load_concurrently_not_sequentially(self) -> None:
        """The datasource peer scan gathers reads (bounded), not one-by-one."""
        col = [DatasetField(name="id", datatype=DataType.FLOAT)]
        peers = {
            f"m{i}": SlayerDataset(
                name=f"m{i}", sql_table=f"m{i}", data_source="db", fields=col,
            )
            for i in range(12)
        }
        root = SlayerDataset(
            name="root", sql_table="root", data_source="db", fields=col,
        )

        class _SlowStorage:
            def __init__(self) -> None:
                self.in_flight = 0
                self.max_in_flight = 0

            async def list_datasets(self, ds: str) -> list[str]:
                return list(peers)

            async def _list_all_dataset_identities(self) -> list[tuple[str, str]]:
                return [("db", name) for name in peers]

            load_datasets = StorageBackend.load_datasets

            async def get_dataset(self, name: str, data_source=None) -> SlayerDataset:
                self.in_flight += 1
                self.max_in_flight = max(self.max_in_flight, self.in_flight)
                await asyncio.sleep(0.01)
                self.in_flight -= 1
                return peers[name]

        storage = _SlowStorage()
        out = await slayer.engine.bundle_builder._collect_referenced_datasets(
            source_dataset=root, named_queries={},
            storage=cast("StorageBackend", storage), data_source="db",
            failures=DocumentLoadFailures(),
        )
        assert out[0] is root
        assert storage.max_in_flight > 1, "peer loads ran sequentially"
        assert storage.max_in_flight <= 8, "peer loads are not bounded"
