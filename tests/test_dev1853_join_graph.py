"""DEV-1853 — edge-aware ``JoinGraph`` and executable root recommendation.

The graph spans the bidirectional edge set: parallel edges are distinct
routes, recommendations emit edge-name tokens where a hop needs one, and an
ambiguous unnamed hop makes the item unreachable rather than recommended
with a path that would fail.
"""

from __future__ import annotations

import tempfile
from collections.abc import AsyncIterator

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, ModelJoin, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.join_graph import JoinGraph
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage

from tests._dev1853_fixtures import chain_models, parallel_engine, parallel_models


class TestBidirectionalReachability:
    def test_reverse_neighbors_are_reachable(self) -> None:
        graph = JoinGraph.build_from_models(chain_models())
        assert "orders" in graph.reachable_from("customers")
        assert "orders" in graph.reachable_from("regions")

    def test_reverse_route_is_unique_on_a_single_edge(self) -> None:
        graph = JoinGraph.build_from_models(chain_models())
        assert graph.count_simple_paths("customers", "orders") == 1


class TestParallelEdgesAreDistinctRoutes:
    def test_route_counting_reports_ambiguous(self) -> None:
        graph = JoinGraph.build_from_models(parallel_models(named=False))
        assert graph.count_simple_paths("orders", "customers", cap=5) == 2
        assert graph.count_simple_paths("customers", "orders", cap=5) == 2

    def test_named_parallel_edges_also_count_as_two_routes(self) -> None:
        graph = JoinGraph.build_from_models(parallel_models(named=True))
        assert graph.count_simple_paths("orders", "customers", cap=5) == 2


class TestEdgeNameShadowsModelToken:
    def test_shadowed_bare_token_is_not_executable(self) -> None:
        # Edge a↔b named "c" shadows the bare model token "c" (names resolve
        # first), so the a↔c hop has no executable token — unroutable, never
        # a path that would land on the wrong endpoint.
        graph = JoinGraph(nodes={"a", "b", "c"},
                          edges=[("a", "b", "c"), ("a", "c", None)])
        assert graph.shortest_path("a", "c") is None
        assert graph.shortest_path("a", "b") == ["b"]


async def _engine_for(models, tmpdir: str) -> SlayerQueryEngine:
    storage = YAMLStorage(base_dir=tmpdir)
    await storage.save_datasource(
        DatasourceConfig(name="test", type="sqlite", database=":memory:"))
    for model in models:
        await storage.save_model(model)
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
        self, named_rec_engine,
    ) -> None:
        rec = await named_rec_engine.recommend_root_model(
            ["orders.status", "customers.name"])
        assert rec.reachable is True
        assert rec.root_model is not None
        paths = {ip.input_item: ip.path for ip in rec.item_paths}
        crossing = next(p for p in paths.values() if "." in p)
        assert crossing.split(".")[0] in {"billing_customer",
                                          "shipping_customer"}, crossing
        # Executable means executable: the emitted paths bind and run.
        resp = await named_rec_engine.execute(SlayerQuery(
            source_model=rec.root_model, dimensions=list(paths.values())))
        assert resp.data

    async def test_unnamed_ambiguous_pair_reports_unreachable(
        self, unnamed_rec_engine,
    ) -> None:
        rec = await unnamed_rec_engine.recommend_root_model(
            ["orders.status", "customers.name"])
        assert rec.reachable is False
        assert rec.item_paths == []

    async def test_reverse_reachability_growth(self) -> None:
        # a → b and c → b share no directed common root today; the
        # bidirectional graph makes every model a valid root.
        def leaf(name: str, extra: str, joins=None) -> SlayerModel:
            return SlayerModel(
                name=name, data_source="test", sql_table=name,
                columns=[Column(name="id", type=DataType.INT, primary_key=True),
                         Column(name=extra, type=DataType.TEXT)],
                joins=joins or [],
            )
        models = [
            leaf("a", "x", [ModelJoin(target_model="b",
                                      join_pairs=[["id", "id"]])]),
            leaf("b", "y"),
            leaf("c", "z", [ModelJoin(target_model="b",
                                      join_pairs=[["id", "id"]])]),
        ]
        with tempfile.TemporaryDirectory() as d:
            engine = await _engine_for(models, d)
            rec = await engine.recommend_root_model(["a.x", "c.z"])
            assert rec.reachable is True
            assert {ip.input_item for ip in rec.item_paths} == {"a.x", "c.z"}
