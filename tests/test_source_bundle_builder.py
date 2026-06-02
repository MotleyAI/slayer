"""Stage 7b.15a (DEV-1450) — ``build_resolved_source_bundle`` tests.

The engine cutover (7b.15d) builds a :class:`ResolvedSourceBundle` once at the
top of execution and the typed binder/planner read from it purely (P11). This
builder is the storage-facing front door: it resolves the query's source model
(every input shape), walks the join graph to collect every referenced model the
binder may hop through, threads the named-query sibling map for multi-stage
DAGs, merges variable layers, and records the datasource hint.

These tests pin the builder's contract before it exists (TDD).
"""

from __future__ import annotations

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.core.query import ModelExtension, SlayerQuery
from slayer.engine.source_bundle import (
    ResolvedSourceBundle,
    build_resolved_source_bundle,
)
from slayer.storage.yaml_storage import YAMLStorage


# ---------------------------------------------------------------------------
# Model fixtures — orders → customers → regions, plus a diamond via warehouses.
# ---------------------------------------------------------------------------


def _regions() -> SlayerModel:
    return SlayerModel(
        name="regions",
        data_source="prod",
        sql_table="regions",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="name", type=DataType.TEXT),
            Column(name="population", type=DataType.INT),
        ],
    )


def _customers(*, query_variables: dict | None = None) -> SlayerModel:
    return SlayerModel(
        name="customers",
        data_source="prod",
        sql_table="customers",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="name", type=DataType.TEXT),
            Column(name="region_id", type=DataType.INT),
            Column(name="revenue", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])],
        query_variables=query_variables or {},
    )


def _warehouses() -> SlayerModel:
    return SlayerModel(
        name="warehouses",
        data_source="prod",
        sql_table="warehouses",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="region_id", type=DataType.INT),
        ],
        joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])],
    )


def _orders(*, query_variables: dict | None = None) -> SlayerModel:
    return SlayerModel(
        name="orders",
        data_source="prod",
        sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="warehouse_id", type=DataType.INT),
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="status", type=DataType.TEXT),
        ],
        joins=[
            ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]]),
            ModelJoin(target_model="warehouses", join_pairs=[["warehouse_id", "id"]]),
        ],
        query_variables=query_variables or {},
    )


def _audit() -> SlayerModel:
    return SlayerModel(
        name="audit",
        data_source="prod",
        sql_table="audit",
        columns=[Column(name="id", type=DataType.INT, primary_key=True)],
    )


def _bare() -> SlayerModel:
    # No joins — a sibling ModelExtension adds the audit join at query time.
    return SlayerModel(
        name="bare",
        data_source="prod",
        sql_table="bare",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="audit_id", type=DataType.INT),
            Column(name="status", type=DataType.TEXT),
        ],
    )


async def _storage(tmp_path, *models: SlayerModel) -> YAMLStorage:
    storage = YAMLStorage(base_dir=str(tmp_path))
    for m in models:
        await storage.save_model(m)
    return storage


def _names(bundle: ResolvedSourceBundle) -> set[str]:
    return {m.name for m in bundle.referenced_models}


# ---------------------------------------------------------------------------
# Source-model resolution — every input shape.
# ---------------------------------------------------------------------------


class TestSourceModelShapes:
    async def test_str_source_resolves_from_storage(self, tmp_path):
        storage = await _storage(tmp_path, _regions(), _customers(), _orders())
        query = SlayerQuery(source_model="orders")
        bundle = await build_resolved_source_bundle(query=query, storage=storage)
        assert isinstance(bundle, ResolvedSourceBundle)
        assert bundle.source_model is not None
        assert bundle.source_model.name == "orders"
        # Convention: the host is also in referenced_models so the binder
        # doesn't special-case it.
        assert bundle.get_referenced_model("orders") is not None

    async def test_inline_slayer_model_source(self, tmp_path):
        storage = await _storage(tmp_path, _regions(), _customers())
        inline = _orders()
        query = SlayerQuery(source_model=inline)
        bundle = await build_resolved_source_bundle(query=query, storage=storage)
        assert bundle.source_model is not None
        assert bundle.source_model.name == "orders"
        # Join targets still resolve from storage.
        assert bundle.get_referenced_model("customers") is not None

    async def test_model_extension_overlay(self, tmp_path):
        storage = await _storage(tmp_path, _regions(), _customers(), _orders())
        ext = ModelExtension(
            source_name="orders",
            columns=[Column(name="discount", type=DataType.DOUBLE)],
            joins=[],
        )
        query = SlayerQuery(source_model=ext)
        bundle = await build_resolved_source_bundle(query=query, storage=storage)
        assert bundle.source_model is not None
        assert bundle.source_model.name == "orders"
        col_names = {c.name for c in bundle.source_model.columns}
        assert "discount" in col_names  # overlay applied
        assert "amount" in col_names  # base columns retained

    async def test_dict_slayer_model_source(self, tmp_path):
        storage = await _storage(tmp_path, _regions(), _customers())
        inline = _orders().model_dump()
        query = SlayerQuery(source_model=inline)
        bundle = await build_resolved_source_bundle(query=query, storage=storage)
        assert bundle.source_model is not None
        assert bundle.source_model.name == "orders"

    async def test_dict_model_extension_source(self, tmp_path):
        storage = await _storage(tmp_path, _regions(), _customers(), _orders())
        query = SlayerQuery(
            source_model={
                "source_name": "orders",
                "columns": [{"name": "discount", "type": "DOUBLE"}],
            }
        )
        bundle = await build_resolved_source_bundle(query=query, storage=storage)
        assert bundle.source_model is not None
        assert bundle.source_model.name == "orders"
        assert "discount" in {c.name for c in bundle.source_model.columns}

    async def test_missing_model_raises(self, tmp_path):
        storage = await _storage(tmp_path, _regions())
        query = SlayerQuery(source_model="nope")
        with pytest.raises(ValueError, match="nope"):
            await build_resolved_source_bundle(query=query, storage=storage)


# ---------------------------------------------------------------------------
# referenced_models — transitive join-graph walk.
# ---------------------------------------------------------------------------


class TestJoinGraphCollection:
    async def test_multi_hop_collects_all(self, tmp_path):
        storage = await _storage(tmp_path, _regions(), _customers(), _orders())
        query = SlayerQuery(source_model="orders")
        bundle = await build_resolved_source_bundle(query=query, storage=storage)
        # orders → customers → regions, plus orders → warehouses (absent here).
        names = _names(bundle)
        assert {"orders", "customers", "regions"}.issubset(names)

    async def test_diamond_dedup(self, tmp_path):
        storage = await _storage(
            tmp_path, _regions(), _customers(), _warehouses(), _orders()
        )
        query = SlayerQuery(source_model="orders")
        bundle = await build_resolved_source_bundle(query=query, storage=storage)
        names = [m.name for m in bundle.referenced_models]
        # regions is reachable via customers AND warehouses — collected once.
        assert names.count("regions") == 1
        assert {"orders", "customers", "warehouses", "regions"}.issubset(set(names))

    async def test_missing_join_target_is_skipped(self, tmp_path):
        # orders joins customers/warehouses but only orders is stored —
        # the walk is best-effort and must not raise on absent targets.
        storage = await _storage(tmp_path, _orders())
        query = SlayerQuery(source_model="orders")
        bundle = await build_resolved_source_bundle(query=query, storage=storage)
        assert bundle.get_referenced_model("orders") is not None
        assert bundle.get_referenced_model("customers") is None


# ---------------------------------------------------------------------------
# Datasource hint.
# ---------------------------------------------------------------------------


class TestDatasourceHint:
    async def test_hint_recorded(self, tmp_path):
        storage = await _storage(tmp_path, _regions(), _customers(), _orders())
        query = SlayerQuery(source_model="orders")
        bundle = await build_resolved_source_bundle(
            query=query, storage=storage, data_source="prod"
        )
        assert bundle.datasource_hint == "prod"
        assert bundle.source_model.name == "orders"

    async def test_no_hint_is_none(self, tmp_path):
        storage = await _storage(tmp_path, _regions(), _customers(), _orders())
        query = SlayerQuery(source_model="orders")
        bundle = await build_resolved_source_bundle(query=query, storage=storage)
        assert bundle.datasource_hint is None


# ---------------------------------------------------------------------------
# Variable precedence: model defaults < query vars < runtime.
# ---------------------------------------------------------------------------


class TestVariablePrecedence:
    async def test_model_defaults_lowest(self, tmp_path):
        storage = await _storage(
            tmp_path,
            _regions(),
            _customers(),
            _orders(query_variables={"region": "model_default", "limit": 10}),
        )
        query = SlayerQuery(source_model="orders", variables={"region": "query_val"})
        bundle = await build_resolved_source_bundle(
            query=query, storage=storage, runtime_variables={"limit": 99}
        )
        # query var overrides model default; runtime overrides everything.
        assert bundle.query_variables["region"] == "query_val"
        assert bundle.query_variables["limit"] == 99

    async def test_runtime_wins(self, tmp_path):
        storage = await _storage(tmp_path, _regions(), _customers(), _orders())
        query = SlayerQuery(source_model="orders", variables={"k": "stage"})
        bundle = await build_resolved_source_bundle(
            query=query, storage=storage, runtime_variables={"k": "runtime"}
        )
        assert bundle.query_variables["k"] == "runtime"

    async def test_outer_layer_below_stage(self, tmp_path):
        # outer_variables sits below the query (stage) layer and above model
        # defaults: model_defaults < outer < stage < runtime.
        storage = await _storage(
            tmp_path,
            _regions(),
            _customers(),
            _orders(query_variables={"a": "model", "b": "model", "c": "model"}),
        )
        query = SlayerQuery(source_model="orders", variables={"c": "stage"})
        bundle = await build_resolved_source_bundle(
            query=query,
            storage=storage,
            outer_variables={"b": "outer", "c": "outer"},
        )
        assert bundle.query_variables["a"] == "model"   # only model has it
        assert bundle.query_variables["b"] == "outer"   # outer beats model
        assert bundle.query_variables["c"] == "stage"   # stage beats outer


# ---------------------------------------------------------------------------
# Multi-stage: named-query siblings + sibling-chain source resolution.
# ---------------------------------------------------------------------------


class TestMultiStage:
    async def test_named_queries_passthrough(self, tmp_path):
        storage = await _storage(tmp_path, _regions(), _customers(), _orders())
        stage1 = SlayerQuery(
            name="stage1",
            source_model="orders",
            dimensions=["status"],
            measures=[{"formula": "amount:sum"}],
        )
        root = SlayerQuery(source_model="stage1", dimensions=["status"])
        bundle = await build_resolved_source_bundle(
            query=root, storage=storage, named_queries={"stage1": stage1}
        )
        assert "stage1" in bundle.named_queries
        assert bundle.named_queries["stage1"] is stage1

    async def test_root_over_sibling_resolves_real_base_model(self, tmp_path):
        # Root stage's source_model is a sibling name; bundle.source_model
        # must be the REAL base model the non-sibling stages bind against
        # (plan_query uses bundle.source_model for every non-sibling stage).
        storage = await _storage(tmp_path, _regions(), _customers(), _orders())
        stage1 = SlayerQuery(
            name="stage1",
            source_model="orders",
            dimensions=["status"],
            measures=[{"formula": "amount:sum"}],
        )
        root = SlayerQuery(source_model="stage1", dimensions=["status"])
        bundle = await build_resolved_source_bundle(
            query=root, storage=storage, named_queries={"stage1": stage1}
        )
        assert bundle.source_model is not None
        assert bundle.source_model.name == "orders"

    async def test_sibling_join_targets_collected(self, tmp_path):
        # A sibling stage over orders joins customers → those targets must
        # be in referenced_models so the sibling's plan_query can hop them.
        storage = await _storage(tmp_path, _regions(), _customers(), _orders())
        stage1 = SlayerQuery(
            name="stage1",
            source_model="orders",
            dimensions=["status"],
            measures=[{"formula": "customers.revenue:sum"}],
        )
        root = SlayerQuery(source_model="stage1", dimensions=["status"])
        bundle = await build_resolved_source_bundle(
            query=root, storage=storage, named_queries={"stage1": stage1}
        )
        assert bundle.get_referenced_model("customers") is not None
        assert bundle.get_referenced_model("regions") is not None

    async def test_sibling_extension_join_walked(self, tmp_path):
        # A sibling stage whose source is a ModelExtension that ADDS a join
        # must have that overlay join walked — `audit` is reachable ONLY via
        # the sibling's extension join, never from the root's `orders`.
        storage = await _storage(
            tmp_path, _regions(), _customers(), _orders(), _bare(), _audit()
        )
        sibling = SlayerQuery(
            name="aux",
            source_model=ModelExtension(
                source_name="bare",
                joins=[ModelJoin(target_model="audit", join_pairs=[["audit_id", "id"]])],
            ),
            dimensions=["status"],
        )
        root = SlayerQuery(source_model="orders", dimensions=["status"])
        bundle = await build_resolved_source_bundle(
            query=root, storage=storage, named_queries={"aux": sibling}
        )
        assert bundle.source_model.name == "orders"
        assert bundle.get_referenced_model("audit") is not None

    async def test_sibling_chain_to_inline_model_collects_joins(self, tmp_path):
        # The chain bottoms out at an inline SlayerModel with its own joins.
        storage = await _storage(tmp_path, _regions(), _customers())
        stage1 = SlayerQuery(
            name="stage1",
            source_model=_orders(),  # inline, carries the customers join
            dimensions=["status"],
            measures=[{"formula": "amount:sum"}],
        )
        root = SlayerQuery(source_model="stage1", dimensions=["status"])
        bundle = await build_resolved_source_bundle(
            query=root, storage=storage, named_queries={"stage1": stage1}
        )
        assert bundle.source_model.name == "orders"
        assert bundle.get_referenced_model("customers") is not None

    async def test_sibling_chain_cycle_raises(self, tmp_path):
        storage = await _storage(tmp_path, _orders())
        a = SlayerQuery(name="a", source_model="b")
        b = SlayerQuery(name="b", source_model="a")
        root = SlayerQuery(source_model="a")
        with pytest.raises(ValueError, match="[Cc]ircular"):
            await build_resolved_source_bundle(
                query=root, storage=storage, named_queries={"a": a, "b": b}
            )
