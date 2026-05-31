"""Stage 2 (DEV-1450) — ResolvedSourceBundle: eagerly resolved query inputs (P11).

The orchestrator builds this once at the top of execute; the binder reads
from it purely. No ContextVar machinery, no callback re-resolution.

Per I2, ``source_model`` is ``Optional`` from day one so a future
anchor-less mode is a type-additive change. DEV-1450 binder asserts
``source_model is not None`` — the type-level optionality is the
extension point.
"""

from __future__ import annotations

from slayer.core.enums import DataType
from slayer.core.models import Aggregation, Column, ModelJoin, SlayerModel
from slayer.core.query import ModelExtension, SlayerQuery
from slayer.engine.source_bundle import ResolvedSourceBundle


def _model(name: str, ds: str = "prod") -> SlayerModel:
    return SlayerModel(
        name=name,
        data_source=ds,
        sql_table=name,
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="value", type=DataType.DOUBLE),
        ],
    )


def _model_with(
    name: str,
    *,
    aggs: list[str] | None = None,
    joins: list[str] | None = None,
    ds: str = "prod",
) -> SlayerModel:
    return SlayerModel(
        name=name,
        data_source=ds,
        sql_table=name,
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="value", type=DataType.DOUBLE),
        ],
        aggregations=[
            Aggregation(name=a, formula="AVG({value})") for a in (aggs or [])
        ],
        joins=[
            ModelJoin(target_model=t, join_pairs=[["id", "id"]])
            for t in (joins or [])
        ],
    )


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


class TestConstruction:
    def test_minimal(self):
        m = _model("orders")
        b = ResolvedSourceBundle(source_model=m, referenced_models=[m])
        assert b.source_model is m
        assert b.referenced_models == [m]
        assert b.inline_extensions == []
        assert b.named_queries == {}
        assert b.query_variables == {}
        assert b.datasource_hint is None

    def test_with_referenced_models(self):
        m = _model("orders")
        c = _model("customers")
        r = _model("regions")
        b = ResolvedSourceBundle(source_model=m, referenced_models=[m, c, r])
        assert b.referenced_models == [m, c, r]

    def test_with_extensions(self):
        m = _model("orders")
        ext = ModelExtension(source_name="orders")
        b = ResolvedSourceBundle(
            source_model=m, referenced_models=[m], inline_extensions=[ext]
        )
        assert b.inline_extensions == [ext]

    def test_with_named_queries(self):
        m = _model("orders")
        q = SlayerQuery(source_model="orders")
        b = ResolvedSourceBundle(
            source_model=m,
            referenced_models=[m],
            named_queries={"stage_a": q},
        )
        assert b.named_queries["stage_a"] is q

    def test_with_query_variables(self):
        m = _model("orders")
        b = ResolvedSourceBundle(
            source_model=m,
            referenced_models=[m],
            query_variables={"region": "NA", "threshold": 100},
        )
        assert b.query_variables == {"region": "NA", "threshold": 100}

    def test_with_datasource_hint(self):
        m = _model("orders", ds="warehouse")
        b = ResolvedSourceBundle(
            source_model=m,
            referenced_models=[m],
            datasource_hint="warehouse",
        )
        assert b.datasource_hint == "warehouse"


# ---------------------------------------------------------------------------
# Lookup helpers
# ---------------------------------------------------------------------------


class TestGetReferencedModel:
    def test_returns_match(self):
        m = _model("orders")
        c = _model("customers")
        b = ResolvedSourceBundle(source_model=m, referenced_models=[m, c])
        assert b.get_referenced_model("customers") is c

    def test_returns_none_for_missing(self):
        m = _model("orders")
        b = ResolvedSourceBundle(source_model=m, referenced_models=[m])
        assert b.get_referenced_model("absent") is None

    def test_source_model_is_in_referenced(self):
        # Convention: source_model is also in referenced_models so the
        # binder doesn't have to special-case the host.
        m = _model("orders")
        b = ResolvedSourceBundle(source_model=m, referenced_models=[m])
        assert b.get_referenced_model("orders") is m


# ---------------------------------------------------------------------------
# I2 — source_model is Optional from day one
# ---------------------------------------------------------------------------


class TestAnchorlessReadiness:
    def test_source_model_none_is_constructible(self):
        # I2: future anchor-less mode reserves source_model=None.
        b = ResolvedSourceBundle(
            source_model=None,
            referenced_models=[_model("orders"), _model("customers")],
        )
        assert b.source_model is None
        # The bundle still holds the set of referenced models that the
        # future global-join planner will operate over.
        assert len(b.referenced_models) == 2

    def test_default_source_model_is_none(self):
        # Defaulting to None keeps both modes type-compatible without
        # callers having to pass an explicit None.
        b = ResolvedSourceBundle()
        assert b.source_model is None
        assert b.referenced_models == []


# ---------------------------------------------------------------------------
# DEV-1500 — reachable_aggregation_names (sync join-graph BFS)
# ---------------------------------------------------------------------------


class TestReachableAggregationNames:
    """Sync mirror of enrichment._collect_reachable_agg_names over a bundle.

    Powers the FUNC_STYLE_AGG slack rewrite so a custom aggregation defined on
    a *joined* model (``rolling_avg(customers.score)``) is recognised and
    rewritten to colon form. Scoping is per start model: a stage only sees
    aggregations reachable from its own source model's join graph.
    """

    def test_own_aggs_only(self):
        m = _model_with("orders", aggs=["rolling_avg"])
        b = ResolvedSourceBundle(source_model=m, referenced_models=[m])
        assert b.reachable_aggregation_names(start=m) == frozenset({"rolling_avg"})

    def test_no_own_but_joined_has(self):
        orders = _model_with("orders", joins=["customers"])
        customers = _model_with("customers", aggs=["rolling_avg"])
        b = ResolvedSourceBundle(
            source_model=orders, referenced_models=[orders, customers]
        )
        assert b.reachable_aggregation_names(start=orders) == frozenset(
            {"rolling_avg"}
        )

    def test_four_hops(self):
        a = _model_with("a", joins=["b"])
        b_ = _model_with("b", joins=["c"])
        c = _model_with("c", joins=["d"])
        d = _model_with("d", joins=["e"])
        e = _model_with("e", aggs=["deep_agg"])
        bundle = ResolvedSourceBundle(
            source_model=a, referenced_models=[a, b_, c, d, e]
        )
        assert bundle.reachable_aggregation_names(start=a) == frozenset(
            {"deep_agg"}
        )

    def test_cycle_terminates(self):
        a = _model_with("a", aggs=["agg_a"], joins=["b"])
        b_ = _model_with("b", joins=["a"])
        bundle = ResolvedSourceBundle(
            source_model=a, referenced_models=[a, b_]
        )
        # Must terminate (visited guard) and collect a's own aggregation.
        assert bundle.reachable_aggregation_names(start=a) == frozenset(
            {"agg_a"}
        )

    def test_none_when_no_aggs_anywhere(self):
        orders = _model_with("orders", joins=["customers"])
        customers = _model_with("customers")
        b = ResolvedSourceBundle(
            source_model=orders, referenced_models=[orders, customers]
        )
        assert b.reachable_aggregation_names(start=orders) is None

    def test_scoping_excludes_unreachable_model(self):
        # Two models that do NOT join each other both live in
        # referenced_models. A scoped walk from M1 must NOT pick up M2's
        # aggregation (this pins scoped-per-stage vs union-of-all).
        m1 = _model_with("m1", aggs=["agg_one"])
        m2 = _model_with("m2", aggs=["agg_two"])
        b = ResolvedSourceBundle(
            source_model=m1, referenced_models=[m1, m2]
        )
        assert b.reachable_aggregation_names(start=m1) == frozenset(
            {"agg_one"}
        )

    def test_absent_join_target_skipped(self):
        # Join points at a target not present in referenced_models — the
        # walk is best-effort and skips it without error, returning the
        # source model's own aggregations.
        orders = _model_with("orders", aggs=["rolling_avg"], joins=["missing"])
        b = ResolvedSourceBundle(source_model=orders, referenced_models=[orders])
        assert b.reachable_aggregation_names(start=orders) == frozenset(
            {"rolling_avg"}
        )

    def test_absent_join_target_with_no_own_aggs_returns_none(self):
        # Skip-absent + empty-collection: a missing join target must not
        # synthesise an empty frozenset() — the contract is `None` when
        # nothing is reachable.
        orders = _model_with("orders", joins=["missing"])
        b = ResolvedSourceBundle(source_model=orders, referenced_models=[orders])
        assert b.reachable_aggregation_names(start=orders) is None

    def test_aggs_from_multiple_hops_unioned(self):
        orders = _model_with("orders", aggs=["a0"], joins=["customers"])
        customers = _model_with("customers", aggs=["a1"], joins=["regions"])
        regions = _model_with("regions", aggs=["a2"])
        b = ResolvedSourceBundle(
            source_model=orders,
            referenced_models=[orders, customers, regions],
        )
        assert b.reachable_aggregation_names(start=orders) == frozenset(
            {"a0", "a1", "a2"}
        )
