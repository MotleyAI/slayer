"""Engine-level tests for ``recommend_root_model``.

Given a set of ``model.column`` / ``model.metric`` items an agent wants in
one query, the engine recommends which model to use as ``source_model``
(the "root") and returns each item's join-qualified reference path from
that root.

Fixture graph (datasource ``mydb``)::

    orders ──LEFT──> customers ──LEFT──> regions
      │  │  └──LEFT──> warehouses ──LEFT──> regions   (diamond onto regions)
      │  └──LEFT──> products
      └──INNER──> order_items   (declared once; reverse traversal is automatic)

    tickets ──LEFT──> agents            (disconnected region)
    logs                                (isolated — reaches nothing)

Every declared edge traverses in both directions, so each
datasource splits into connected components: {orders-world}, {tickets,
agents}, {logs}. A second datasource ``otherdb`` also has a model named
``orders`` (for data_source disambiguation) plus ``widgets`` (for the
cross-datasource guard).
"""

from __future__ import annotations

import tempfile
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio

from slayer.core.enums import DataType, JoinType
from slayer.core.errors import AmbiguousDatasetError, EntityResolutionError
from slayer.core.models import (
    Aggregation,
    DatasetField,
    DatasourceConfig,
    DatasetRelationship,
    DatasetMetric,
    SlayerDataset,
)
from slayer.core.recommend import (
    CandidateCoverage,
    ItemPath,
    PopulationRecommendation,
)
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.base import StorageBackend
from slayer.storage.yaml_storage import YAMLStorage


def _col(name: str, type_: DataType = DataType.STRING, pk: bool = False) -> DatasetField:
    return DatasetField(name=name, sql=name, datatype=type_, primary_key=pk)


def _left(target: str, pairs: list[list[str]]) -> DatasetRelationship:
    return DatasetRelationship(to=target, from_columns=[p[0] for p in pairs], to_columns=[p[1] for p in pairs], join_type=JoinType.LEFT)


def _inner(target: str, pairs: list[list[str]]) -> DatasetRelationship:
    return DatasetRelationship(to=target, from_columns=[p[0] for p in pairs], to_columns=[p[1] for p in pairs], join_type=JoinType.INNER)


@pytest_asyncio.fixture
async def storage() -> AsyncIterator[StorageBackend]:
    with tempfile.TemporaryDirectory() as tmpdir:
        s = YAMLStorage(base_dir=tmpdir)
        await s.save_datasource(DatasourceConfig(name="mydb", type="postgres", host="x"))
        await s.save_datasource(DatasourceConfig(name="otherdb", type="postgres", host="x"))

        await s.save_dataset(SlayerDataset(
            name="orders", data_source="mydb", sql_table="orders",
            fields=[
                _col("id", DataType.INTEGER, pk=True),
                _col("status"),
                _col("revenue", DataType.FLOAT),
                _col("amount", DataType.FLOAT),
                _col("qty", DataType.INTEGER),
                _col("ordered_at", DataType.DATETIME),
                DatasetField(name="customer_id", datatype=DataType.INTEGER, hidden=True),
                DatasetField(name="product_id", datatype=DataType.INTEGER, hidden=True),
                DatasetField(name="warehouse_id", datatype=DataType.INTEGER, hidden=True),
            ],
            metrics=[DatasetMetric(expression="sum(revenue) / count(*)", name="aov")],
            aggregations=[Aggregation(
                name="trimmed_mean",
                expression="avg(CASE WHEN {expr} BETWEEN {low} AND {high} THEN {expr} END)",
            )],
            relationships=[
                _left("customers", [["customer_id", "id"]]),
                _left("products", [["product_id", "id"]]),
                _left("warehouses", [["warehouse_id", "id"]]),
                _inner("order_items", [["id", "order_id"]]),
            ],
        ))
        await s.save_dataset(SlayerDataset(
            name="customers", data_source="mydb", sql_table="customers",
            fields=[_col("id", DataType.INTEGER, pk=True), _col("name"), _col("region_id", DataType.INTEGER)],
            relationships=[_left("regions", [["region_id", "id"]])],
        ))
        await s.save_dataset(SlayerDataset(
            name="products", data_source="mydb", sql_table="products",
            fields=[_col("id", DataType.INTEGER, pk=True), _col("category"), _col("price", DataType.FLOAT)],
        ))
        await s.save_dataset(SlayerDataset(
            name="warehouses", data_source="mydb", sql_table="warehouses",
            fields=[_col("id", DataType.INTEGER, pk=True), _col("name"), _col("region_id", DataType.INTEGER)],
            relationships=[_left("regions", [["region_id", "id"]])],
        ))
        await s.save_dataset(SlayerDataset(
            name="regions", data_source="mydb", sql_table="regions",
            fields=[_col("id", DataType.INTEGER, pk=True), _col("name"), _col("population", DataType.INTEGER)],
        ))
        # No reverse declaration on order_items — the orders→order_items INNER
        # edge traverses both ways; a mirror would be rejected.
        await s.save_dataset(SlayerDataset(
            name="order_items", data_source="mydb", sql_table="order_items",
            fields=[_col("id", DataType.INTEGER, pk=True), _col("order_id", DataType.INTEGER), _col("sku"), _col("quantity", DataType.INTEGER)],
        ))
        await s.save_dataset(SlayerDataset(
            name="tickets", data_source="mydb", sql_table="tickets",
            fields=[_col("id", DataType.INTEGER, pk=True), _col("subject"), _col("agent_id", DataType.INTEGER)],
            relationships=[_left("agents", [["agent_id", "id"]])],
        ))
        await s.save_dataset(SlayerDataset(
            name="agents", data_source="mydb", sql_table="agents",
            fields=[_col("id", DataType.INTEGER, pk=True), _col("name")],
        ))
        await s.save_dataset(SlayerDataset(
            name="logs", data_source="mydb", sql_table="logs",
            fields=[_col("id", DataType.INTEGER, pk=True), _col("line")],
        ))

        # otherdb — a uniquely-named model, for the cross-datasource guard.
        await s.save_dataset(SlayerDataset(
            name="widgets", data_source="otherdb", sql_table="widgets",
            fields=[_col("id", DataType.INTEGER, pk=True), _col("sku")],
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
            await s.save_dataset(SlayerDataset(
                name="orders", data_source=ds, sql_table="orders",
                fields=[_col("id", DataType.INTEGER, pk=True), _col("status")],
            ))
        yield s


@pytest_asyncio.fixture
async def engine(storage: StorageBackend) -> AsyncIterator[SlayerQueryEngine]:
    eng = SlayerQueryEngine(storage=storage)
    yield eng
    await eng.aclose()


def _paths(rec: PopulationRecommendation) -> dict[str, str]:
    return {ip.input_item: ip.path for ip in rec.item_paths}


# --------------------------------------------------------------------------
# Resolution & validation
# --------------------------------------------------------------------------
class TestResolutionValidation:
    async def test_returns_pydantic_result(self, engine: SlayerQueryEngine) -> None:
        rec = await engine.recommend_population(["regions.name"])
        assert isinstance(rec, PopulationRecommendation)
        assert rec.data_source == "mydb"
        assert rec.reachable is True
        assert rec.warnings == []

    async def test_measure_leaf_accepted(self, engine: SlayerQueryEngine) -> None:
        rec = await engine.recommend_population(["orders.aov"], data_source="mydb")
        assert rec.population == "orders"
        assert _paths(rec) == {"orders.aov": "aov"}

    async def test_unresolvable_item_raises(self, engine: SlayerQueryEngine) -> None:
        with pytest.raises(EntityResolutionError):
            await engine.recommend_population(["regions.does_not_exist"])

    async def test_bare_model_rejected(self, engine: SlayerQueryEngine) -> None:
        # "regions" resolves to a model (no leaf) → not a column/metric.
        with pytest.raises(ValueError):
            await engine.recommend_population(["regions"])

    async def test_bare_datasource_rejected(self, engine: SlayerQueryEngine) -> None:
        # "mydb" resolves to a datasource (no model/leaf) → not a column/metric.
        with pytest.raises(ValueError):
            await engine.recommend_population(["mydb"])

    async def test_custom_aggregation_leaf_rejected(self, engine: SlayerQueryEngine) -> None:
        with pytest.raises(ValueError):
            await engine.recommend_population(["orders.trimmed_mean"], data_source="mydb")

    async def test_cross_datasource_raises(self, engine: SlayerQueryEngine) -> None:
        with pytest.raises(ValueError):
            await engine.recommend_population(["regions.name", "widgets.sku"])

    async def test_ambiguous_model_without_data_source_raises(self, collision_storage) -> None:
        # "orders" exists in both mydb and otherdb → resolver ambiguity.
        eng = SlayerQueryEngine(storage=collision_storage)
        try:
            with pytest.raises(AmbiguousDatasetError):
                await eng.recommend_population(["orders.status"])
        finally:
            await eng.aclose()


# --------------------------------------------------------------------------
# Root selection & path emission
# --------------------------------------------------------------------------
class TestRootSelection:
    async def test_single_model_bare_paths(self, engine: SlayerQueryEngine) -> None:
        rec = await engine.recommend_population(
            ["orders.status", "orders.revenue"], data_source="mydb"
        )
        assert rec.population == "orders"
        assert _paths(rec) == {"orders.status": "status", "orders.revenue": "revenue"}

    async def test_fan_out_picks_bridge_that_determines_both(self, engine: SlayerQueryEngine) -> None:
        # Determination is to-one. Neither customers nor products
        # determines the other's column (the reverse hop through orders fans
        # out), so only the shared child orders determines both — the bridge
        # wins outright, no tiebreak needed.
        rec = await engine.recommend_population(["customers.name", "products.category"])
        assert rec.population == "orders"
        assert _paths(rec) == {
            "customers.name": "customers.name",
            "products.category": "products.category",
        }

    async def test_min_hops_prefers_closer_mentioned_root(self, engine: SlayerQueryEngine) -> None:
        # {customers, regions}: customers reaches both in fewer total hops
        # than orders, and is itself mentioned.
        rec = await engine.recommend_population(["customers.name", "regions.name"])
        assert rec.population == "customers"
        assert _paths(rec) == {"customers.name": "name", "regions.name": "regions.name"}

    async def test_multi_hop_diamond_path_lexicographic(self, engine: SlayerQueryEngine) -> None:
        # regions reachable via customers (lex) and warehouses; customers wins.
        rec = await engine.recommend_population(["orders.status", "regions.population"])
        assert rec.population == "orders"
        assert _paths(rec) == {
            "orders.status": "status",
            "regions.population": "customers.regions.population",
        }

    async def test_shared_child_is_sole_determiner(self, engine: SlayerQueryEngine) -> None:
        # Only orders determines both columns along to-one paths;
        # customers/products each fan out to the other, so orders is the sole
        # valid root.
        rec = await engine.recommend_population(["customers.name", "products.price"])
        assert rec.population == "orders"

    async def test_symmetric_tie_broken_lexicographically(self, engine: SlayerQueryEngine) -> None:
        # The single orders→order_items INNER edge routes both ways:
        # root=orders costs 1 hop, root=order_items costs 1 hop. Both are
        # mentioned owning models, so the mentioned-preference can't break it
        # → lexicographically smallest name wins: "order_items" < "orders".
        rec = await engine.recommend_population(["orders.status", "order_items.sku"])
        assert rec.population == "order_items"
        assert _paths(rec) == {"orders.status": "orders.status", "order_items.sku": "sku"}


# --------------------------------------------------------------------------
# Aggregation-suffix preservation
# --------------------------------------------------------------------------
class TestAggSuffix:
    async def test_simple_suffix_local(self, engine: SlayerQueryEngine) -> None:
        rec = await engine.recommend_population(["sum(orders.revenue)"], data_source="mydb")
        assert rec.population == "orders"
        assert _paths(rec) == {"sum(orders.revenue)": "sum(revenue)"}

    async def test_suffix_cross_model(self, engine: SlayerQueryEngine) -> None:
        rec = await engine.recommend_population(["sum(products.price)", "orders.status"])
        assert rec.population == "orders"
        assert _paths(rec)["sum(products.price)"] == "sum(products.price)"

    async def test_kwarg_suffix_local(self, engine: SlayerQueryEngine) -> None:
        rec = await engine.recommend_population(
            ["weighted_avg(orders.amount, weight=qty)"], data_source="mydb"
        )
        assert _paths(rec) == {
            "weighted_avg(orders.amount, weight=qty)": "weighted_avg(amount, weight=qty)"
        }

    async def test_kwarg_suffix_cross_model_preserved_verbatim(self, engine: SlayerQueryEngine) -> None:
        rec = await engine.recommend_population(
            ["weighted_avg(products.price, weight=price)", "orders.status"]
        )
        assert rec.population == "orders"
        assert (
            _paths(rec)["weighted_avg(products.price, weight=price)"]
            == "weighted_avg(products.price, weight=price)"
        )

    async def test_dotted_kwarg_arg_preserved_verbatim(self, engine: SlayerQueryEngine) -> None:
        # A dotted column arg is preserved as-is (engine resolves it in the
        # aggregated column's owning-model frame, invariant under root).
        item = "weighted_avg(products.price, weight=products.price)"
        rec = await engine.recommend_population([item, "orders.status"])
        assert rec.population == "orders"
        assert _paths(rec)[item] == "weighted_avg(products.price, weight=products.price)"

    async def test_suffix_through_multi_hop_path(self, engine: SlayerQueryEngine) -> None:
        # regions is 2 hops from orders (via customers); suffix rides along.
        rec = await engine.recommend_population(
            ["orders.status", "sum(regions.population)"]
        )
        assert rec.population == "orders"
        assert _paths(rec)["sum(regions.population)"] == "sum(customers.regions.population)"

    @pytest.mark.parametrize(
        ("item", "expected"),
        [
            ("sum(orders.amount)", "sum(amount)"),
            ("percentile(orders.amount, p=0.9)", "percentile(amount, p=0.9)"),
            ("last(orders.amount, ordered_at)", "last(amount, ordered_at)"),
            ("weighted_avg(orders.amount, weight=qty)", "weighted_avg(amount, weight=qty)"),
        ],
    )
    async def test_local_paths_are_functional(self, engine: SlayerQueryEngine, item: str, expected: str) -> None:
        rec = await engine.recommend_population([item], data_source="mydb")
        assert rec.population == "orders"
        assert _paths(rec) == {item: expected}

    async def test_cross_model_path_is_functional(self, engine: SlayerQueryEngine) -> None:
        rec = await engine.recommend_population(["orders.status", "sum(regions.population)"])
        assert rec.population == "orders"
        assert _paths(rec)["sum(regions.population)"] == "sum(customers.regions.population)"


# --------------------------------------------------------------------------
# INNER routing + real resolvability
#
# The orders→order_items INNER edge is declared ONCE; every declared edge
# is traversable in both directions. Determination is
# to-one: orders→order_items fans out (order_items.order_id is not unique), so
# only order_items determines the trio — and it reaches orders/products over
# the to-one order_items→orders→(products) chain.
# --------------------------------------------------------------------------
class TestInnerJoins:
    async def test_inner_pair_routes_from_child(self, engine: SlayerQueryEngine) -> None:
        # {orders, order_items, products}: orders cannot determine order_items
        # (the INNER hop fans out), so order_items is the sole to-one determiner
        # — reaching orders in 1 hop and products in 2 (order_items→orders→products).
        rec = await engine.recommend_population(
            ["orders.status", "order_items.sku", "products.category"]
        )
        assert rec.population == "order_items"
        assert _paths(rec)["order_items.sku"] == "sku"
        assert _paths(rec)["orders.status"] == "orders.status"

    async def test_inner_path_resolvable_by_engine_walker(self, engine: SlayerQueryEngine) -> None:
        """The recommended to-one hop must be resolvable by the code that
        actually runs a query — executing the recommended path is the honest,
        strictly stronger check (it fails if any layer cannot traverse the hop).
        """
        rec = await engine.recommend_population(
            ["orders.status", "order_items.sku", "products.category"]
        )
        assert rec.population == "order_items"
        path = _paths(rec)["orders.status"]
        assert path == "orders.status"

        # Feed the recommended order_items→orders hop back in as a query.
        resp = await engine.execute(
            SlayerQuery(population=rec.population, dimensions=[path]),
            dry_run=True,
        )
        sql = resp.sql or ""
        assert "orders" in sql, sql
        assert "JOIN" in sql.upper(), sql
        # Resolved as a joined dimension under the dotted result key.
        assert "order_items.orders.status" in sql, sql


# --------------------------------------------------------------------------
# No common root — structured coverage diagnostics
# --------------------------------------------------------------------------
class TestNoCommonRoot:
    async def test_disconnected_returns_structured_no_root(self, engine: SlayerQueryEngine) -> None:
        rec = await engine.recommend_population(["customers.name", "agents.name"])
        assert rec.population is None
        assert rec.reachable is False
        assert rec.item_paths == []
        assert rec.message
        covered = {c.dataset_name for c in rec.coverage}
        # Pareto frontier: customers (hops 0) dominates orders (hops 1) for
        # {customers}; agents (hops 0) dominates tickets for {agents}.
        assert covered == {"customers", "agents"}

    async def test_attachment_only_disconnected_still_lists_coverage(self, engine: SlayerQueryEngine) -> None:
        # Attachment-only items on disconnected models: coverage falls back to
        # attachment reachability, so it is non-empty (orders.aov / count(logs.line)).
        rec = await engine.recommend_population(
            ["orders.aov", "count(logs.line)"], data_source="mydb"
        )
        assert rec.reachable is False
        assert rec.item_paths == []
        assert rec.coverage  # not empty — the regression this guards
        covered = {c.dataset_name for c in rec.coverage}
        assert "orders" in covered
        assert "logs" in covered

    async def test_coverage_surfaces_partial_roots(self, engine: SlayerQueryEngine) -> None:
        rec = await engine.recommend_population(
            ["customers.name", "products.category", "agents.name"]
        )
        assert rec.reachable is False
        # To-one determination: only orders determines both order-world
        # columns (customers/products fan out to the other), so it dominates them
        # on the frontier; agents covers the disconnected item. Item lists preserve
        # original input order.
        assert rec.coverage[0].dataset_name == "orders"
        assert rec.coverage[0].reachable_items == ["customers.name", "products.category"]
        assert rec.coverage[0].unreachable_items == ["agents.name"]
        assert {c.dataset_name for c in rec.coverage} == {"orders", "agents"}
        # Sorted by len(reachable_items) desc, then total hops asc, then name.
        lengths = [len(c.reachable_items) for c in rec.coverage]
        assert lengths == sorted(lengths, reverse=True)

    async def test_pareto_incomparable_candidates_both_kept(self, engine: SlayerQueryEngine) -> None:
        # Disconnected regions → customers (reach {customers}) and agents
        # (reach {agents}) are incomparable (neither reach set contains the
        # other), so BOTH survive the Pareto frontier.
        rec = await engine.recommend_population(["customers.name", "agents.name"])
        covered = {c.dataset_name for c in rec.coverage}
        assert {"customers", "agents"} <= covered

    async def test_dominated_candidates_dropped(self, engine: SlayerQueryEngine) -> None:
        rec = await engine.recommend_population(
            ["customers.name", "products.category", "agents.name"]
        )
        # To-one determination: orders is the sole determiner of both
        # order-world columns, dominating customers/products (each reaches only its
        # own, a subset); tickets is dominated by agents. Only orders + agents survive.
        names = {c.dataset_name for c in rec.coverage}
        assert "orders" in names
        assert "agents" in names
        assert "tickets" not in names
        assert "products" not in names
        assert "customers" not in names


# --------------------------------------------------------------------------
# Attachments on unrelated datasets (no join path from the root)
# --------------------------------------------------------------------------
class TestUnrelatedAttachments:
    async def test_unrelated_attachment_is_reported_through_the_product_hop(self, engine: SlayerQueryEngine) -> None:
        rec = await engine.recommend_population(["customers.name", "count(logs.id)"])
        assert rec.population == "customers"
        assert rec.reachable is True
        flags = {ip.input_item: (ip.path, ip.unrelated) for ip in rec.item_paths}
        assert flags == {"customers.name": ("name", False), "count(logs.id)": ("count(logs.id)", True)}
        resp = await engine.execute(
            SlayerQuery.model_validate({"population": "customers", "metrics": [flags["count(logs.id)"][0]]}),
            dry_run=True,
        )
        assert resp.sql

    async def test_unrelated_attachment_never_steers_the_choice(self, engine: SlayerQueryEngine) -> None:
        items = ["customers.name", "regions.name"]
        without = await engine.recommend_population(items)
        with_unrelated = await engine.recommend_population([*items, "count(logs.id)", "count(agents.id)"])
        assert with_unrelated.population == without.population == "customers"
        assert {ip.input_item for ip in with_unrelated.item_paths if ip.unrelated} == {
            "count(logs.id)", "count(agents.id)",
        }

    async def test_feasible_root_hint_reaches_an_unrelated_attachment(self, engine: SlayerQueryEngine) -> None:
        rec = await engine.recommend_population(["customers.name", "count(logs.id)"], root_hint="orders")
        assert rec.population == "orders"
        assert rec.warnings == []
        assert {ip.input_item: ip.unrelated for ip in rec.item_paths} == {
            "customers.name": False, "count(logs.id)": True,
        }

    async def test_unrelated_column_stays_unreachable(self, engine: SlayerQueryEngine) -> None:
        rec = await engine.recommend_population(["customers.name", "logs.line"])
        assert rec.population is None
        assert rec.reachable is False
        assert rec.item_paths == []

    async def test_attachments_alone_keep_join_path_selection(self, engine: SlayerQueryEngine) -> None:
        rec = await engine.recommend_population(["count(customers.id)", "count(logs.id)"])
        assert rec.population is None
        assert rec.reachable is False
        assert rec.item_paths == []

    async def test_connected_items_are_never_flagged(self, engine: SlayerQueryEngine) -> None:
        rec = await engine.recommend_population(["orders.status", "sum(products.price)", "orders.aov"])
        assert [ip.unrelated for ip in rec.item_paths] == [False, False, False]


# --------------------------------------------------------------------------
# data_source scoping & dedup
# --------------------------------------------------------------------------
class TestDataSourceAndDedup:
    async def test_data_source_disambiguates_same_named_model(self, collision_storage) -> None:
        eng = SlayerQueryEngine(storage=collision_storage)
        try:
            rec_my = await eng.recommend_population(["orders.status"], data_source="mydb")
            rec_other = await eng.recommend_population(["orders.status"], data_source="otherdb")
            assert rec_my.data_source == "mydb"
            assert rec_other.data_source == "otherdb"
            assert rec_my.population == "orders"
            assert rec_other.population == "orders"
        finally:
            await eng.aclose()

    async def test_already_datasource_qualified_item_passthrough(self, collision_storage) -> None:
        # Item already starting with a known datasource is NOT double-prefixed.
        eng = SlayerQueryEngine(storage=collision_storage)
        try:
            rec = await eng.recommend_population(
                ["mydb.orders.status"], data_source="mydb"
            )
            assert rec.data_source == "mydb"
            assert rec.population == "orders"
            assert _paths(rec) == {"mydb.orders.status": "status"}
        finally:
            await eng.aclose()

    async def test_dotless_item_wrong_data_source_raises(self, engine: SlayerQueryEngine) -> None:
        # "revenue" only exists in mydb; asserting data_source=otherdb must fail.
        with pytest.raises(ValueError):
            await engine.recommend_population(["revenue"], data_source="otherdb")

    async def test_qualified_item_wrong_data_source_raises(self, engine: SlayerQueryEngine) -> None:
        # An already-datasource-qualified item naming a DIFFERENT datasource
        # than the requested scope must be rejected — not silently resolved
        # to the qualified datasource. (widgets lives in otherdb.) Under the
        # requested scope it re-roots to 'mydb.otherdb...' which has no such
        # model → EntityResolutionError.
        with pytest.raises((ValueError, EntityResolutionError)):
            await engine.recommend_population(
                ["otherdb.widgets.sku"], data_source="mydb"
            )

    async def test_dotted_model_colliding_with_datasource_name_scopes_to_model(self) -> None:
        # A model whose name equals a *datasource* name must still resolve to
        # the model (not the datasource) when data_source is explicit.
        with tempfile.TemporaryDirectory() as tmpdir:
            s = YAMLStorage(base_dir=tmpdir)
            await s.save_datasource(DatasourceConfig(name="shared", type="postgres", host="x"))
            await s.save_datasource(DatasourceConfig(name="mydb", type="postgres", host="x"))
            await s.save_dataset(SlayerDataset(
                name="shared", data_source="mydb", sql_table="shared",
                fields=[_col("id", DataType.INTEGER, pk=True), _col("status")],
            ))
            eng = SlayerQueryEngine(storage=s)
            try:
                rec = await eng.recommend_population(["shared.status"], data_source="mydb")
                assert rec.data_source == "mydb"
                assert rec.population == "shared"
                assert _paths(rec) == {"shared.status": "status"}
            finally:
                await eng.aclose()

    async def test_bare_scope_ignores_aggregation_named_leaf(self) -> None:
        # A custom aggregation sharing a bare column's name must NOT make the
        # valid column ambiguous when scoping a bare item to the datasource.
        with tempfile.TemporaryDirectory() as tmpdir:
            s = YAMLStorage(base_dir=tmpdir)
            await s.save_datasource(DatasourceConfig(name="d", type="postgres", host="x"))
            await s.save_dataset(SlayerDataset(
                name="sales", data_source="d", sql_table="sales",
                fields=[_col("id", DataType.INTEGER, pk=True), _col("revenue", DataType.FLOAT)],
            ))
            await s.save_dataset(SlayerDataset(
                name="calc", data_source="d", sql_table="calc",
                fields=[_col("id", DataType.INTEGER, pk=True)],
                aggregations=[Aggregation(name="revenue", expression="SUM({expr})")],
            ))
            eng = SlayerQueryEngine(storage=s)
            try:
                rec = await eng.recommend_population(["revenue"], data_source="d")
                assert rec.population == "sales"
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
            rec_my = await eng.recommend_population(["status"], data_source="mydb")
            rec_other = await eng.recommend_population(["status"], data_source="otherdb")
            assert rec_my.data_source == "mydb"
            assert rec_my.population == "orders"
            assert rec_other.data_source == "otherdb"
            assert rec_other.population == "orders"
            assert _paths(rec_my) == {"status": "status"}
        finally:
            await eng.aclose()

    async def test_exact_duplicate_input_deduped(self, engine: SlayerQueryEngine) -> None:
        rec = await engine.recommend_population(
            ["orders.status", "orders.status"], data_source="mydb"
        )
        assert len(rec.item_paths) == 1

    async def test_distinct_strings_same_entity_kept_separate(self, engine: SlayerQueryEngine) -> None:
        rec = await engine.recommend_population(
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
        rec = eng.recommend_population_sync(["regions.name"])
        assert isinstance(rec, PopulationRecommendation)
        assert rec.population == "regions"
        assert _paths(rec) == {"regions.name": "name"}


class TestDistinctOwningModelHopSum:
    async def test_only_to_one_determiner_wins(self) -> None:
        # Chain (ds "iso"): A →to-one→ B →to-one→ C (FK→PK forward; reverse hops
        # fan out). Determination is to-one, so only A determines every
        # column: A reaches A(0), B(1), C(2). B cannot determine A.a* (B→A fans
        # out), and neither can C — so A is the sole viable root regardless of how
        # the per-item hops sum.
        with tempfile.TemporaryDirectory() as tmpdir:
            s = YAMLStorage(base_dir=tmpdir)
            await s.save_datasource(DatasourceConfig(name="iso", type="postgres", host="x"))
            await s.save_dataset(SlayerDataset(
                name="A", data_source="iso", sql_table="A",
                fields=[_col("id", DataType.INTEGER, pk=True), _col("b_id", DataType.INTEGER)]
                + [_col(f"a{i}") for i in range(1, 6)],
                relationships=[_left("B", [["b_id", "id"]])],
            ))
            await s.save_dataset(SlayerDataset(
                name="B", data_source="iso", sql_table="B",
                fields=[_col("id", DataType.INTEGER, pk=True), _col("b1"), _col("c_id", DataType.INTEGER)],
                relationships=[_left("C", [["c_id", "id"]])],
            ))
            await s.save_dataset(SlayerDataset(
                name="C", data_source="iso", sql_table="C",
                fields=[_col("id", DataType.INTEGER, pk=True), _col("c1")],
            ))
            eng = SlayerQueryEngine(storage=s)
            try:
                rec = await eng.recommend_population(
                    ["A.a1", "A.a2", "A.a3", "A.a4", "A.a5", "B.b1", "C.c1"]
                )
                assert rec.population == "A"
            finally:
                await eng.aclose()


class TestRootHint:
    """``root_hint`` (follow-up): force a caller-chosen root when feasible,
    else fall back to the auto-pick with an explanatory warning. A
    non-existent / wrong-kind / cross-datasource hint raises loudly."""

    # -- feasible hint honored ------------------------------------------
    async def test_feasible_bridge_hint_honored_over_cheaper_autopick(self, engine: SlayerQueryEngine) -> None:
        # {customers, regions}: auto-pick is 'customers' (0+1 hops, mentioned).
        # 'orders' is a feasible bridge that owns NEITHER item but reaches
        # both — the descriptions-first case. The hint must win outright.
        rec = await engine.recommend_population(
            ["customers.name", "regions.name"], root_hint="orders"
        )
        assert rec.reachable is True
        assert rec.population == "orders"
        assert rec.warnings == []
        assert "root_hint" in rec.message
        assert _paths(rec) == {
            "customers.name": "customers.name",
            "regions.name": "customers.regions.name",
        }

    async def test_feasible_hint_equal_to_autopick_honored(self, engine: SlayerQueryEngine) -> None:
        rec = await engine.recommend_population(
            ["customers.name", "regions.name"], root_hint="customers"
        )
        assert rec.population == "customers"
        assert rec.warnings == []
        assert _paths(rec) == {"customers.name": "name", "regions.name": "regions.name"}

    async def test_bare_and_ds_qualified_hint_equivalent(self, engine: SlayerQueryEngine) -> None:
        bare = await engine.recommend_population(
            ["customers.name", "regions.name"], root_hint="orders"
        )
        qualified = await engine.recommend_population(
            ["customers.name", "regions.name"], root_hint="mydb.orders"
        )
        assert bare.population == qualified.population == "orders"

    def test_sync_wrapper_forwards_root_hint(self, storage) -> None:
        eng = SlayerQueryEngine(storage=storage)
        rec = eng.recommend_population_sync(
            ["customers.name", "regions.name"], root_hint="orders"
        )
        assert rec.population == "orders"

    # -- empty / whitespace hint is a no-op -----------------------------
    async def test_empty_hint_is_noop(self, engine: SlayerQueryEngine) -> None:
        rec = await engine.recommend_population(
            ["customers.name", "regions.name"], root_hint=""
        )
        assert rec.population == "customers"  # auto-pick, unchanged
        assert rec.warnings == []
        assert "root_hint" not in rec.message

    async def test_whitespace_hint_is_noop(self, engine: SlayerQueryEngine) -> None:
        rec = await engine.recommend_population(
            ["customers.name", "regions.name"], root_hint="   "
        )
        assert rec.population == "customers"
        assert rec.warnings == []
        assert "root_hint" not in rec.message

    # -- infeasible hint (real model) → fallback + warning --------------
    async def test_infeasible_hint_falls_back_with_warning(self, engine: SlayerQueryEngine) -> None:
        # 'agents' determines neither order-world column (to-one), so
        # it is infeasible and we fall back to the auto-pick (orders, the sole
        # to-one determiner of both).
        rec = await engine.recommend_population(
            ["customers.name", "products.category"], root_hint="agents"
        )
        assert rec.reachable is True
        assert rec.population == "orders"  # fell back to the auto-pick
        joined = " ".join(rec.warnings)
        assert "root_hint" in joined
        assert "cannot reach" in joined
        assert "agents" in joined     # the rejected hint
        assert "products" in joined   # an undetermined owning model
        assert "customers" in joined  # the other undetermined owning model

    async def test_infeasible_warning_echoes_original_ds_qualified_hint(self, engine: SlayerQueryEngine) -> None:
        # The diagnostic must quote what the caller typed ('mydb.agents'),
        # not the resolved bare model name ('agents').
        rec = await engine.recommend_population(
            ["customers.name", "products.category"], root_hint="mydb.agents"
        )
        assert rec.population == "orders"
        assert any("mydb.agents" in w for w in rec.warnings)

    # -- bad hint → ValueError ------------------------------------------
    async def test_nonexistent_hint_raises(self, engine: SlayerQueryEngine) -> None:
        with pytest.raises(ValueError, match="not a dataset in datasource"):
            await engine.recommend_population(["orders.status"], root_hint="ordrs")

    async def test_wrong_kind_bare_hint_raises_not_a_model(self, engine: SlayerQueryEngine) -> None:
        # 'status' is a column, not a model → "not a model".
        with pytest.raises(ValueError, match="not a dataset in datasource"):
            await engine.recommend_population(["orders.status"], root_hint="status")

    async def test_cross_datasource_hint_raises(self, engine: SlayerQueryEngine) -> None:
        # Items resolve to 'mydb'; a hint qualified with a different ds is rejected.
        with pytest.raises(ValueError):
            await engine.recommend_population(
                ["customers.name", "products.category"], root_hint="otherdb.widgets"
            )

    async def test_three_part_hint_raises(self, engine: SlayerQueryEngine) -> None:
        with pytest.raises(ValueError):
            await engine.recommend_population(
                ["orders.status"], data_source="mydb", root_hint="mydb.orders.status"
            )

    # -- reachable=False: hint coverage row is force-surfaced -----------
    async def test_no_root_dominated_hint_row_surfaced(self, engine: SlayerQueryEngine) -> None:
        # {customers, agents} disconnected → reachable=False. 'orders' reaches
        # only {customers} and is dominated by 'customers' (0 hops) — normally
        # dropped from the frontier. With the hint it must appear, exactly once.
        rec = await engine.recommend_population(
            ["customers.name", "agents.name"], root_hint="orders"
        )
        assert rec.reachable is False
        assert rec.population is None
        orders_rows = [c for c in rec.coverage if c.dataset_name == "orders"]
        assert len(orders_rows) == 1
        assert orders_rows[0].reachable_items == ["customers.name"]
        assert orders_rows[0].unreachable_items == ["agents.name"]
        assert any("root_hint" in w and "cannot reach" in w for w in rec.warnings)
        # Force-inclusion keeps the existing sort key (reachable count desc).
        lengths = [len(c.reachable_items) for c in rec.coverage]
        assert lengths == sorted(lengths, reverse=True)

    async def test_no_root_zero_reach_hint_row_surfaced(self, engine: SlayerQueryEngine) -> None:
        # 'logs' is isolated and reaches neither {customers, agents}
        # ('products' reaches customers, so only a disconnected model has
        # zero reach). Force-included row has an empty reachable list and both
        # items unreachable.
        rec = await engine.recommend_population(
            ["customers.name", "agents.name"], root_hint="logs"
        )
        assert rec.reachable is False
        log_rows = [c for c in rec.coverage if c.dataset_name == "logs"]
        assert len(log_rows) == 1
        assert log_rows[0].reachable_items == []
        assert log_rows[0].unreachable_items == ["customers.name", "agents.name"]
        assert any("root_hint" in w and "cannot reach" in w for w in rec.warnings)

    async def test_no_root_frontier_hint_not_duplicated(self, engine: SlayerQueryEngine) -> None:
        # 'customers' is already on the frontier; forcing it in must not
        # produce a duplicate row.
        rec = await engine.recommend_population(
            ["customers.name", "agents.name"], root_hint="customers"
        )
        assert rec.reachable is False
        cust_rows = [c for c in rec.coverage if c.dataset_name == "customers"]
        assert len(cust_rows) == 1
        assert any("root_hint" in w and "cannot reach" in w for w in rec.warnings)


class TestResultModels:
    def test_item_path_fields(self) -> None:
        ip = ItemPath(input_item="sum(orders.revenue)", path="sum(customers.revenue)")
        assert ip.input_item == "sum(orders.revenue)"
        assert ip.path == "sum(customers.revenue)"

    def test_item_path_unrelated_defaults_false(self) -> None:
        assert ItemPath(input_item="orders.status", path="status").unrelated is False
        assert ItemPath(input_item="count(logs.id)", path="count(logs.id)", unrelated=True).unrelated is True

    def test_candidate_coverage_fields(self) -> None:
        cc = CandidateCoverage(
            dataset_name="orders",
            reachable_items=["customers.name"],
            unreachable_items=["agents.name"],
        )
        assert cc.dataset_name == "orders"
        assert cc.reachable_items == ["customers.name"]
        assert cc.unreachable_items == ["agents.name"]

    def test_recommendation_defaults(self) -> None:
        rec = PopulationRecommendation(
            data_source="mydb", population="orders", reachable=True, item_paths=[]
        )
        assert rec.coverage == []
        assert rec.message == ""
        assert rec.warnings == []

    def test_no_new_result_fields_for_root_hint(self) -> None:
        # root_hint status is conveyed via message + warnings only — the
        # result schema must not grow a structured hint field.
        assert set(PopulationRecommendation.model_fields) == {
            "data_source", "population", "reachable",
            "item_paths", "coverage", "message", "warnings",
        }


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
            await s.save_dataset(SlayerDataset(
                name="shared", data_source="mydb", sql_table="shared",
                fields=[_col("id", DataType.INTEGER, pk=True)],
            ))
            await s.save_dataset(SlayerDataset(
                name="leafmodel", data_source="shared", sql_table="leafmodel",
                fields=[_col("id", DataType.INTEGER, pk=True), _col("status")],
            ))
            eng = SlayerQueryEngine(storage=s)
            try:
                rec = await eng.recommend_population(["shared.leafmodel.status"])
                assert rec.population == "leafmodel"
                assert rec.data_source == "shared"
                assert rec.warnings, "expected the datasource-vs-model warning to surface"
            finally:
                await eng.aclose()
