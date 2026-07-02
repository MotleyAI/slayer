"""Engine-level tests for ``recommend_root_model`` (DEV-1626).

Given a set of ``model.column`` / ``model.metric`` items an agent wants in
one query, the engine recommends which model to use as ``source_model``
(the "root") and returns each item's join-qualified reference path from
that root.

Fixture graph (datasource ``mydb``)::

    orders ──LEFT──> customers ──LEFT──> regions
      │  │  └──LEFT──> warehouses ──LEFT──> regions   (diamond onto regions)
      │  └──LEFT──> products
      └──INNER──> order_items  (symmetric: order_items ──INNER──> orders)

    tickets ──LEFT──> agents            (disconnected region)

A second datasource ``otherdb`` also has a model named ``orders`` (for
data_source disambiguation) plus ``widgets`` (for the cross-datasource
guard).
"""

from __future__ import annotations

import tempfile
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio

from slayer.core.enums import DataType, JoinType
from slayer.core.errors import AmbiguousModelError, EntityResolutionError
from slayer.core.models import (
    Aggregation,
    Column,
    DatasourceConfig,
    ModelJoin,
    ModelMeasure,
    SlayerModel,
)
from slayer.core.recommend import (
    CandidateCoverage,
    ItemPath,
    RootModelRecommendation,
)
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.base import StorageBackend
from slayer.storage.yaml_storage import YAMLStorage


def _col(name: str, type_: DataType = DataType.TEXT, pk: bool = False) -> Column:
    return Column(name=name, sql=name, type=type_, primary_key=pk)


def _left(target: str, pairs: list[list[str]]) -> ModelJoin:
    return ModelJoin(target_model=target, join_pairs=pairs, join_type=JoinType.LEFT)


def _inner(target: str, pairs: list[list[str]]) -> ModelJoin:
    return ModelJoin(target_model=target, join_pairs=pairs, join_type=JoinType.INNER)


@pytest_asyncio.fixture
async def storage() -> AsyncIterator[StorageBackend]:
    with tempfile.TemporaryDirectory() as tmpdir:
        s = YAMLStorage(base_dir=tmpdir)
        await s.save_datasource(DatasourceConfig(name="mydb", type="postgres", host="x"))
        await s.save_datasource(DatasourceConfig(name="otherdb", type="postgres", host="x"))

        await s.save_model(SlayerModel(
            name="orders", data_source="mydb", sql_table="orders",
            columns=[
                _col("id", DataType.INT, pk=True),
                _col("status"),
                _col("revenue", DataType.DOUBLE),
                _col("amount", DataType.DOUBLE),
                _col("qty", DataType.INT),
            ],
            measures=[ModelMeasure(formula="revenue:sum / *:count", name="aov")],
            aggregations=[Aggregation(
                name="trimmed_mean",
                formula="avg(CASE WHEN {expr} BETWEEN {low} AND {high} THEN {expr} END)",
            )],
            joins=[
                _left("customers", [["customer_id", "id"]]),
                _left("products", [["product_id", "id"]]),
                _left("warehouses", [["warehouse_id", "id"]]),
                _inner("order_items", [["id", "order_id"]]),
            ],
        ))
        await s.save_model(SlayerModel(
            name="customers", data_source="mydb", sql_table="customers",
            columns=[_col("id", DataType.INT, pk=True), _col("name"), _col("region_id", DataType.INT)],
            joins=[_left("regions", [["region_id", "id"]])],
        ))
        await s.save_model(SlayerModel(
            name="products", data_source="mydb", sql_table="products",
            columns=[_col("id", DataType.INT, pk=True), _col("category"), _col("price", DataType.DOUBLE)],
        ))
        await s.save_model(SlayerModel(
            name="warehouses", data_source="mydb", sql_table="warehouses",
            columns=[_col("id", DataType.INT, pk=True), _col("name"), _col("region_id", DataType.INT)],
            joins=[_left("regions", [["region_id", "id"]])],
        ))
        await s.save_model(SlayerModel(
            name="regions", data_source="mydb", sql_table="regions",
            columns=[_col("id", DataType.INT, pk=True), _col("name"), _col("population", DataType.INT)],
        ))
        await s.save_model(SlayerModel(
            name="order_items", data_source="mydb", sql_table="order_items",
            columns=[_col("id", DataType.INT, pk=True), _col("order_id", DataType.INT), _col("sku"), _col("quantity", DataType.INT)],
            joins=[_inner("orders", [["order_id", "id"]])],
        ))
        await s.save_model(SlayerModel(
            name="tickets", data_source="mydb", sql_table="tickets",
            columns=[_col("id", DataType.INT, pk=True), _col("subject"), _col("agent_id", DataType.INT)],
            joins=[_left("agents", [["agent_id", "id"]])],
        ))
        await s.save_model(SlayerModel(
            name="agents", data_source="mydb", sql_table="agents",
            columns=[_col("id", DataType.INT, pk=True), _col("name")],
        ))

        # otherdb — a uniquely-named model, for the cross-datasource guard.
        await s.save_model(SlayerModel(
            name="widgets", data_source="otherdb", sql_table="widgets",
            columns=[_col("id", DataType.INT, pk=True), _col("sku")],
        ))
        yield s


@pytest_asyncio.fixture
async def collision_storage() -> AsyncIterator[StorageBackend]:
    """Two datasources sharing the model name 'orders' — for data_source
    disambiguation and the ambiguity-raises path."""
    with tempfile.TemporaryDirectory() as tmpdir:
        s = YAMLStorage(base_dir=tmpdir)
        await s.save_datasource(DatasourceConfig(name="mydb", type="postgres", host="x"))
        await s.save_datasource(DatasourceConfig(name="otherdb", type="postgres", host="x"))
        for ds in ("mydb", "otherdb"):
            await s.save_model(SlayerModel(
                name="orders", data_source=ds, sql_table="orders",
                columns=[_col("id", DataType.INT, pk=True), _col("status")],
            ))
        yield s


@pytest_asyncio.fixture
async def engine(storage: StorageBackend) -> AsyncIterator[SlayerQueryEngine]:
    eng = SlayerQueryEngine(storage=storage)
    yield eng
    await eng.aclose()


def _paths(rec: RootModelRecommendation) -> dict[str, str]:
    return {ip.input_item: ip.path for ip in rec.item_paths}


# --------------------------------------------------------------------------
# Resolution & validation
# --------------------------------------------------------------------------
class TestResolutionValidation:
    async def test_returns_pydantic_result(self, engine) -> None:
        rec = await engine.recommend_root_model(["regions.name"])
        assert isinstance(rec, RootModelRecommendation)
        assert rec.data_source == "mydb"
        assert rec.reachable is True
        assert rec.warnings == []

    async def test_measure_leaf_accepted(self, engine) -> None:
        rec = await engine.recommend_root_model(["orders.aov"], data_source="mydb")
        assert rec.root_model == "orders"
        assert _paths(rec) == {"orders.aov": "aov"}

    async def test_unresolvable_item_raises(self, engine) -> None:
        with pytest.raises(EntityResolutionError):
            await engine.recommend_root_model(["regions.does_not_exist"])

    async def test_bare_model_rejected(self, engine) -> None:
        # "regions" resolves to a model (no leaf) → not a column/metric.
        with pytest.raises(ValueError):
            await engine.recommend_root_model(["regions"])

    async def test_bare_datasource_rejected(self, engine) -> None:
        # "mydb" resolves to a datasource (no model/leaf) → not a column/metric.
        with pytest.raises(ValueError):
            await engine.recommend_root_model(["mydb"])

    async def test_custom_aggregation_leaf_rejected(self, engine) -> None:
        with pytest.raises(ValueError):
            await engine.recommend_root_model(["orders.trimmed_mean"], data_source="mydb")

    async def test_cross_datasource_raises(self, engine) -> None:
        with pytest.raises(ValueError):
            await engine.recommend_root_model(["regions.name", "widgets.sku"])

    async def test_ambiguous_model_without_data_source_raises(self, collision_storage) -> None:
        # "orders" exists in both mydb and otherdb → resolver ambiguity.
        eng = SlayerQueryEngine(storage=collision_storage)
        try:
            with pytest.raises(AmbiguousModelError):
                await eng.recommend_root_model(["orders.status"])
        finally:
            await eng.aclose()


# --------------------------------------------------------------------------
# Root selection & path emission
# --------------------------------------------------------------------------
class TestRootSelection:
    async def test_single_model_bare_paths(self, engine) -> None:
        rec = await engine.recommend_root_model(
            ["orders.status", "orders.revenue"], data_source="mydb"
        )
        assert rec.root_model == "orders"
        assert _paths(rec) == {"orders.status": "status", "orders.revenue": "revenue"}

    async def test_fan_out_picks_unmentioned_bridge(self, engine) -> None:
        # customers & products don't reach each other; orders bridges both.
        rec = await engine.recommend_root_model(["customers.name", "products.category"])
        assert rec.root_model == "orders"
        assert _paths(rec) == {
            "customers.name": "customers.name",
            "products.category": "products.category",
        }

    async def test_min_hops_prefers_closer_mentioned_root(self, engine) -> None:
        # {customers, regions}: customers reaches both in fewer total hops
        # than orders, and is itself mentioned.
        rec = await engine.recommend_root_model(["customers.name", "regions.name"])
        assert rec.root_model == "customers"
        assert _paths(rec) == {"customers.name": "name", "regions.name": "regions.name"}

    async def test_multi_hop_diamond_path_lexicographic(self, engine) -> None:
        # regions reachable via customers (lex) and warehouses; customers wins.
        rec = await engine.recommend_root_model(["orders.status", "regions.population"])
        assert rec.root_model == "orders"
        assert _paths(rec) == {
            "orders.status": "status",
            "regions.population": "customers.regions.population",
        }

    async def test_sole_unmentioned_bridge_root(self, engine) -> None:
        # Only orders reaches {customers, products}; it is unmentioned but
        # is the sole valid root.
        rec = await engine.recommend_root_model(["customers.name", "products.price"])
        assert rec.root_model == "orders"

    async def test_symmetric_tie_broken_lexicographically(self, engine) -> None:
        # orders <-> order_items symmetric INNER: root=orders costs 1 hop,
        # root=order_items costs 1 hop. Both are mentioned owning models, so
        # the mentioned-preference can't break it → lexicographically
        # smallest name wins: "order_items" < "orders".
        rec = await engine.recommend_root_model(["orders.status", "order_items.sku"])
        assert rec.root_model == "order_items"
        assert _paths(rec) == {"orders.status": "orders.status", "order_items.sku": "sku"}


# --------------------------------------------------------------------------
# Aggregation-suffix preservation
# --------------------------------------------------------------------------
class TestAggSuffix:
    async def test_simple_suffix_local(self, engine) -> None:
        rec = await engine.recommend_root_model(["orders.revenue:sum"], data_source="mydb")
        assert rec.root_model == "orders"
        assert _paths(rec) == {"orders.revenue:sum": "revenue:sum"}

    async def test_suffix_cross_model(self, engine) -> None:
        rec = await engine.recommend_root_model(["products.price:sum", "orders.status"])
        assert rec.root_model == "orders"
        assert _paths(rec)["products.price:sum"] == "products.price:sum"

    async def test_kwarg_suffix_local(self, engine) -> None:
        rec = await engine.recommend_root_model(
            ["orders.amount:weighted_avg(weight=qty)"], data_source="mydb"
        )
        assert _paths(rec) == {
            "orders.amount:weighted_avg(weight=qty)": "amount:weighted_avg(weight=qty)"
        }

    async def test_kwarg_suffix_cross_model_preserved_verbatim(self, engine) -> None:
        rec = await engine.recommend_root_model(
            ["products.price:weighted_avg(weight=price)", "orders.status"]
        )
        assert rec.root_model == "orders"
        assert (
            _paths(rec)["products.price:weighted_avg(weight=price)"]
            == "products.price:weighted_avg(weight=price)"
        )

    async def test_dotted_kwarg_arg_preserved_verbatim(self, engine) -> None:
        # A dotted column arg is preserved as-is (engine resolves it in the
        # aggregated column's owning-model frame, invariant under root).
        item = "products.price:weighted_avg(weight=products.price)"
        rec = await engine.recommend_root_model([item, "orders.status"])
        assert rec.root_model == "orders"
        assert _paths(rec)[item] == item

    async def test_suffix_through_multi_hop_path(self, engine) -> None:
        # regions is 2 hops from orders (via customers); suffix rides along.
        rec = await engine.recommend_root_model(
            ["orders.status", "regions.population:sum"]
        )
        assert rec.root_model == "orders"
        assert _paths(rec)["regions.population:sum"] == "customers.regions.population:sum"


# --------------------------------------------------------------------------
# INNER symmetry + real resolvability
#
# INNER joins are kept symmetric by the storage layer (join_sync materializes
# BOTH directions), and JoinGraph reads stored *outgoing* joins only — there
# is deliberately no "reverse-only" traversal (a reverse-only fixture would be
# unreachable by design). The fixture declares the symmetric pair explicitly
# (orders<->order_items), so these tests verify routing works in both
# directions and that the emitted path is walkable by the query engine.
# --------------------------------------------------------------------------
class TestInnerJoins:
    async def test_inner_pair_routes_from_either_side(self, engine) -> None:
        # {orders, order_items, products}: orders reaches all in 2 hops
        # (order_items via the stored INNER edge, products via LEFT);
        # order_items would need 3 (order_items->orders->products). So the
        # chosen root is orders and it reaches order_items via the stored
        # orders->order_items INNER edge. The other direction
        # (order_items->orders) is exercised by
        # TestRootSelection.test_symmetric_tie_broken_lexicographically.
        rec = await engine.recommend_root_model(
            ["orders.status", "order_items.sku", "products.category"]
        )
        assert rec.root_model == "orders"
        assert _paths(rec)["order_items.sku"] == "order_items.sku"

    async def test_inner_path_resolvable_by_engine_walker(self, engine, storage) -> None:
        # The emitted INNER hop must be walkable by the engine's own
        # _walk_join_chain (the query-time resolver), proving the recommended
        # path is query-resolvable given the storage symmetry invariant.
        orders = await storage.get_model("orders", data_source="mydb")
        terminal, _first = await engine._walk_join_chain(
            source_model=orders, hop_names=["order_items"]
        )
        assert terminal.name == "order_items"


# --------------------------------------------------------------------------
# No common root — structured coverage diagnostics
# --------------------------------------------------------------------------
class TestNoCommonRoot:
    async def test_disconnected_returns_structured_no_root(self, engine) -> None:
        rec = await engine.recommend_root_model(["customers.name", "agents.name"])
        assert rec.root_model is None
        assert rec.reachable is False
        assert rec.item_paths == []
        assert rec.message
        covered = {c.model_name for c in rec.coverage}
        # Pareto frontier: customers (hops 0) dominates orders (hops 1) for
        # {customers}; agents (hops 0) dominates tickets for {agents}.
        assert covered == {"customers", "agents"}

    async def test_coverage_surfaces_unmentioned_bridge(self, engine) -> None:
        rec = await engine.recommend_root_model(
            ["customers.name", "products.category", "agents.name"]
        )
        assert rec.reachable is False
        # orders (unmentioned) is the best partial root: covers the two
        # order-world items; it should sort first (len desc), and its item
        # lists preserve original input order.
        assert rec.coverage[0].model_name == "orders"
        assert rec.coverage[0].reachable_items == ["customers.name", "products.category"]
        assert rec.coverage[0].unreachable_items == ["agents.name"]
        # Sorted by len(reachable_items) desc, then total hops asc, then name.
        lengths = [len(c.reachable_items) for c in rec.coverage]
        assert lengths == sorted(lengths, reverse=True)

    async def test_pareto_incomparable_candidates_both_kept(self, engine) -> None:
        # Disconnected regions → customers (reach {customers}) and agents
        # (reach {agents}) are incomparable (neither reach set contains the
        # other), so BOTH survive the Pareto frontier.
        rec = await engine.recommend_root_model(["customers.name", "agents.name"])
        covered = {c.model_name for c in rec.coverage}
        assert {"customers", "agents"} <= covered

    async def test_dominated_candidates_dropped(self, engine) -> None:
        rec = await engine.recommend_root_model(
            ["customers.name", "products.category", "agents.name"]
        )
        # Neither a bare owning model dominated by orders (customers/products)
        # nor tickets (dominated by agents) survives the frontier.
        names = {c.model_name for c in rec.coverage}
        assert "orders" in names and "agents" in names
        assert "tickets" not in names
        assert "products" not in names and "customers" not in names


# --------------------------------------------------------------------------
# data_source scoping & dedup
# --------------------------------------------------------------------------
class TestDataSourceAndDedup:
    async def test_data_source_disambiguates_same_named_model(self, collision_storage) -> None:
        eng = SlayerQueryEngine(storage=collision_storage)
        try:
            rec_my = await eng.recommend_root_model(["orders.status"], data_source="mydb")
            rec_other = await eng.recommend_root_model(["orders.status"], data_source="otherdb")
            assert rec_my.data_source == "mydb"
            assert rec_other.data_source == "otherdb"
            assert rec_my.root_model == "orders" and rec_other.root_model == "orders"
        finally:
            await eng.aclose()

    async def test_already_datasource_qualified_item_passthrough(self, collision_storage) -> None:
        # Item already starting with a known datasource is NOT double-prefixed.
        eng = SlayerQueryEngine(storage=collision_storage)
        try:
            rec = await eng.recommend_root_model(
                ["mydb.orders.status"], data_source="mydb"
            )
            assert rec.data_source == "mydb"
            assert rec.root_model == "orders"
            assert _paths(rec) == {"mydb.orders.status": "status"}
        finally:
            await eng.aclose()

    async def test_dotless_item_wrong_data_source_raises(self, engine) -> None:
        # "revenue" only exists in mydb; asserting data_source=otherdb must fail.
        with pytest.raises(ValueError):
            await engine.recommend_root_model(["revenue"], data_source="otherdb")

    async def test_qualified_item_wrong_data_source_raises(self, engine) -> None:
        # An already-datasource-qualified item naming a DIFFERENT datasource
        # than the requested scope must be rejected — not silently resolved
        # to the qualified datasource. (widgets lives in otherdb.) Under the
        # requested scope it re-roots to 'mydb.otherdb...' which has no such
        # model → EntityResolutionError.
        with pytest.raises((ValueError, EntityResolutionError)):
            await engine.recommend_root_model(
                ["otherdb.widgets.sku"], data_source="mydb"
            )

    async def test_dotted_model_colliding_with_datasource_name_scopes_to_model(self) -> None:
        # A model whose name equals a *datasource* name must still resolve to
        # the model (not the datasource) when data_source is explicit.
        with tempfile.TemporaryDirectory() as tmpdir:
            s = YAMLStorage(base_dir=tmpdir)
            await s.save_datasource(DatasourceConfig(name="shared", type="postgres", host="x"))
            await s.save_datasource(DatasourceConfig(name="mydb", type="postgres", host="x"))
            await s.save_model(SlayerModel(
                name="shared", data_source="mydb", sql_table="shared",
                columns=[_col("id", DataType.INT, pk=True), _col("status")],
            ))
            eng = SlayerQueryEngine(storage=s)
            try:
                rec = await eng.recommend_root_model(["shared.status"], data_source="mydb")
                assert rec.data_source == "mydb"
                assert rec.root_model == "shared"
                assert _paths(rec) == {"shared.status": "status"}
            finally:
                await eng.aclose()

    async def test_bare_scope_ignores_aggregation_named_leaf(self) -> None:
        # A custom aggregation sharing a bare column's name must NOT make the
        # valid column ambiguous when scoping a bare item to the datasource.
        with tempfile.TemporaryDirectory() as tmpdir:
            s = YAMLStorage(base_dir=tmpdir)
            await s.save_datasource(DatasourceConfig(name="d", type="postgres", host="x"))
            await s.save_model(SlayerModel(
                name="sales", data_source="d", sql_table="sales",
                columns=[_col("id", DataType.INT, pk=True), _col("revenue", DataType.DOUBLE)],
            ))
            await s.save_model(SlayerModel(
                name="calc", data_source="d", sql_table="calc",
                columns=[_col("id", DataType.INT, pk=True)],
                aggregations=[Aggregation(name="revenue", formula="SUM({expr})")],
            ))
            eng = SlayerQueryEngine(storage=s)
            try:
                rec = await eng.recommend_root_model(["revenue"], data_source="d")
                assert rec.root_model == "sales"
                assert _paths(rec) == {"revenue": "revenue"}
            finally:
                await eng.aclose()

    async def test_bare_item_scoped_to_requested_datasource(self, collision_storage) -> None:
        # 'status' is a column on 'orders' in BOTH mydb and otherdb. A bare
        # item with data_source must resolve WITHIN that datasource, not via
        # the global priority list (which could pick the other one and then
        # spuriously reject the scoped request).
        eng = SlayerQueryEngine(storage=collision_storage)
        try:
            rec_my = await eng.recommend_root_model(["status"], data_source="mydb")
            rec_other = await eng.recommend_root_model(["status"], data_source="otherdb")
            assert rec_my.data_source == "mydb" and rec_my.root_model == "orders"
            assert rec_other.data_source == "otherdb" and rec_other.root_model == "orders"
            assert _paths(rec_my) == {"status": "status"}
        finally:
            await eng.aclose()

    async def test_exact_duplicate_input_deduped(self, engine) -> None:
        rec = await engine.recommend_root_model(
            ["orders.status", "orders.status"], data_source="mydb"
        )
        assert len(rec.item_paths) == 1

    async def test_distinct_strings_same_entity_kept_separate(self, engine) -> None:
        rec = await engine.recommend_root_model(
            ["orders.revenue", "revenue"], data_source="mydb"
        )
        assert len(rec.item_paths) == 2
        assert all(ip.path == "revenue" for ip in rec.item_paths)


# --------------------------------------------------------------------------
# Sync wrapper
# --------------------------------------------------------------------------
class TestSyncWrapper:
    def test_sync_wrapper(self, storage) -> None:
        eng = SlayerQueryEngine(storage=storage)
        rec = eng.recommend_root_model_sync(["regions.name"])
        assert isinstance(rec, RootModelRecommendation)
        assert rec.root_model == "regions"
        assert _paths(rec) == {"regions.name": "name"}


class TestDistinctOwningModelHopSum:
    async def test_hop_total_summed_over_distinct_owning_models(self) -> None:
        # Graph (ds "iso"): B --LEFT--> A (1 hop); A --LEFT--> M1 --> M2 --> B
        # (3 hops). Mentioned owning models = {A, B}.
        #   root A: A(0) + B(3) = 3   |   root B: B(0) + A(1) = 1  → B wins.
        # Input has FIVE A-columns + one B-column. Under a per-INPUT-ITEM hop
        # sum, root A would total 3 and root B would total 5 → A would win.
        # Summing over DISTINCT owning models keeps B the winner.
        with tempfile.TemporaryDirectory() as tmpdir:
            s = YAMLStorage(base_dir=tmpdir)
            await s.save_datasource(DatasourceConfig(name="iso", type="postgres", host="x"))
            await s.save_model(SlayerModel(
                name="A", data_source="iso", sql_table="A",
                columns=[_col("id", DataType.INT, pk=True)]
                + [_col(f"a{i}") for i in range(1, 6)],
                joins=[_left("M1", [["m1_id", "id"]])],
            ))
            await s.save_model(SlayerModel(
                name="M1", data_source="iso", sql_table="M1",
                columns=[_col("id", DataType.INT, pk=True), _col("m2_id", DataType.INT)],
                joins=[_left("M2", [["m2_id", "id"]])],
            ))
            await s.save_model(SlayerModel(
                name="M2", data_source="iso", sql_table="M2",
                columns=[_col("id", DataType.INT, pk=True), _col("b_id", DataType.INT)],
                joins=[_left("B", [["b_id", "id"]])],
            ))
            await s.save_model(SlayerModel(
                name="B", data_source="iso", sql_table="B",
                columns=[_col("id", DataType.INT, pk=True), _col("b1"), _col("a_id", DataType.INT)],
                joins=[_left("A", [["a_id", "id"]])],
            ))
            eng = SlayerQueryEngine(storage=s)
            try:
                rec = await eng.recommend_root_model(
                    ["A.a1", "A.a2", "A.a3", "A.a4", "A.a5", "B.b1"]
                )
                assert rec.root_model == "B"
            finally:
                await eng.aclose()


class TestResultModels:
    def test_item_path_fields(self) -> None:
        ip = ItemPath(input_item="orders.revenue:sum", path="customers.revenue:sum")
        assert ip.input_item == "orders.revenue:sum"
        assert ip.path == "customers.revenue:sum"

    def test_candidate_coverage_fields(self) -> None:
        cc = CandidateCoverage(
            model_name="orders",
            reachable_items=["customers.name"],
            unreachable_items=["agents.name"],
        )
        assert cc.model_name == "orders"
        assert cc.reachable_items == ["customers.name"]
        assert cc.unreachable_items == ["agents.name"]

    def test_recommendation_defaults(self) -> None:
        rec = RootModelRecommendation(
            data_source="mydb", root_model="orders", reachable=True, item_paths=[]
        )
        assert rec.coverage == []
        assert rec.message == ""
        assert rec.warnings == []


class TestWarningsSurfaced:
    async def test_resolver_warnings_are_surfaced(self) -> None:
        # "Case D": a datasource whose name is also a model name in another
        # datasource. Resolving "shared.leafmodel.status" accepts the
        # 3-segment column form but emits a datasource-vs-model warning,
        # which recommend_root_model must surface.
        with tempfile.TemporaryDirectory() as tmpdir:
            s = YAMLStorage(base_dir=tmpdir)
            await s.save_datasource(DatasourceConfig(name="shared", type="postgres", host="x"))
            await s.save_datasource(DatasourceConfig(name="mydb", type="postgres", host="x"))
            # model named "shared" lives in mydb → collides with ds "shared".
            await s.save_model(SlayerModel(
                name="shared", data_source="mydb", sql_table="shared",
                columns=[_col("id", DataType.INT, pk=True)],
            ))
            await s.save_model(SlayerModel(
                name="leafmodel", data_source="shared", sql_table="leafmodel",
                columns=[_col("id", DataType.INT, pk=True), _col("status")],
            ))
            eng = SlayerQueryEngine(storage=s)
            try:
                rec = await eng.recommend_root_model(["shared.leafmodel.status"])
                assert rec.root_model == "leafmodel"
                assert rec.data_source == "shared"
                assert rec.warnings, "expected the datasource-vs-model warning to surface"
            finally:
                await eng.aclose()
