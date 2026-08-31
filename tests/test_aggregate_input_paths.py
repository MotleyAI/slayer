"""DEV-1709 (Stage 5) — unit tests for the plan-time crossing-input scan.

``compute_aggregate_input_join_paths`` is the helper behind the widened
Law-3 trigger: given an ``AggregateKey`` it reports every join path crossed
by the aggregate's EXPLICIT inputs —

* source: structural ``source.path`` plus, for a derived ``ColumnSqlKey``
  with ``path == ()``, the expanded ``Column.sql`` scanned with the shared
  Law-1 scanner (``collect_root_scope_joined_paths``);
* positional args (covers the explicit first/last time arg): structural
  path + derived-sql scan;
* kwargs: same for column-valued kwargs; template-fragment STRING kwargs
  (user-supplied values and the model-default ``AggregationParam`` sql
  fragments of the aggregation named by ``key.agg``) are parsed with the
  dialect-fallback chain and scanned — unparseable fragments contribute
  nothing (parity with the ``Column.filter`` scan's defensive fallback);
* ``column_filter_key`` is NOT re-scanned here — the trigger reads its
  bind-time ``referenced_join_paths`` directly.

Structure mirrors ``tests/test_filtered_local_isolation.py``'s fixtures.
"""

from __future__ import annotations


from slayer.core.enums import DataType
from slayer.core.keys import AggregateKey
from slayer.core.models import (
    Aggregation,
    AggregationParam,
    Column,
    ModelJoin,
    SlayerModel,
)
from slayer.core.query import SlayerQuery
from slayer.engine.aggregate_input_paths import compute_aggregate_input_join_paths
from slayer.engine.source_bundle import ResolvedSourceBundle
from slayer.engine.stage_planner import plan_query


def _regions() -> SlayerModel:
    return SlayerModel(
        name="regions", data_source="test", sql_table="regions",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="payment_amount", type=DataType.DOUBLE),
            Column(name="weight", type=DataType.DOUBLE),
        ],
    )


def _customers() -> SlayerModel:
    return SlayerModel(
        name="customers", data_source="test", sql_table="customers",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="region_id", type=DataType.INT),
            Column(name="weight", type=DataType.DOUBLE),
            Column(name="signup_at", type=DataType.TIMESTAMP),
        ],
        joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])],
    )


def _orders() -> SlayerModel:
    return SlayerModel(
        name="orders", data_source="test", sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="created_at", type=DataType.TIMESTAMP),
            Column(name="region_pay", sql="customers.regions.payment_amount",
                   type=DataType.DOUBLE),
            Column(name="cust_weight_d", sql="customers.weight",
                   type=DataType.DOUBLE),
            Column(name="pop_helper", sql="customers.regions.payment_amount",
                   type=DataType.DOUBLE),
            Column(name="doubled_pop", sql="pop_helper * 2",
                   type=DataType.DOUBLE),
            Column(name="cross_time", sql="customers.signup_at",
                   type=DataType.TIMESTAMP),
            Column(name="local_double", sql="amount * 2", type=DataType.DOUBLE),
            # Crossing ONLY via Column.filter — the helper must NOT report it
            # (that half of the trigger reads column_filter_key directly).
            Column(name="eu_amount", sql="amount",
                   filter="customers.weight > 0", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(
            target_model="customers", join_pairs=[["customer_id", "id"]],
        )],
        aggregations=[
            Aggregation(
                name="scaled_sum", formula="SUM({value}) / {scale}",
                params=[AggregationParam(name="scale", sql="1")],
            ),
            Aggregation(
                name="wscaled_sum", formula="SUM({value} * {w})",
                params=[AggregationParam(
                    name="w", sql="customers.regions.weight",
                )],
            ),
        ],
    )


def _bundle() -> ResolvedSourceBundle:
    return ResolvedSourceBundle(
        source_model=_orders(), referenced_models=[_customers(), _regions()],
    )


def _key_for(formula: str) -> AggregateKey:
    q = SlayerQuery(
        source_model="orders",
        measures=[{"formula": formula, "name": "m0"}],
    )
    planned = plan_query(query=q, bundle=_bundle())
    # DEV-1835: a local first/last desugars into a regroup producer, so its
    # AggregateKey slot can live inside a producer_plan, not at top level.
    candidates = list(planned.aggregate_slots)
    for attach in planned.regroup_attach_plans:
        candidates.extend(attach.producer_plan.aggregate_slots)
    for slot in candidates:
        if isinstance(slot.key, AggregateKey):
            return slot.key
    raise AssertionError(f"no AggregateKey slot for formula {formula!r}")


def _paths(formula: str):
    bundle = _bundle()
    return compute_aggregate_input_join_paths(
        key=_key_for(formula),
        anchor_model=bundle.source_model,
        anchor_relation="orders",
        bundle=bundle,
    )


class TestCrossingSources:
    def test_derived_source_multi_hop(self):
        paths = _paths("region_pay:sum")
        assert ("customers", "regions") in paths, paths
        assert ("customers",) in paths, paths

    def test_derived_source_single_dot(self):
        paths = _paths("cust_weight_d:sum")
        assert ("customers",) in paths, paths

    def test_sibling_derived_chain(self):
        paths = _paths("doubled_pop:sum")
        assert ("customers", "regions") in paths, paths

    def test_bare_local_source_empty(self):
        assert _paths("amount:sum") == ()

    def test_local_derived_source_empty(self):
        assert _paths("local_double:sum") == ()

    def test_star_source_empty(self):
        assert _paths("*:count") == ()


class TestCrossingArgsAndKwargs:
    def test_structural_time_arg(self):
        paths = _paths("amount:last(customers.signup_at)")
        assert ("customers",) in paths, paths

    def test_derived_time_arg(self):
        paths = _paths("amount:last(cross_time)")
        assert ("customers",) in paths, paths

    def test_local_time_arg_empty(self):
        assert _paths("amount:last(orders.created_at)") == ()

    def test_structural_kwarg(self):
        paths = _paths("amount:weighted_avg(weight=customers.weight)")
        assert ("customers",) in paths, paths

    def test_derived_kwarg(self):
        paths = _paths("amount:weighted_avg(weight=region_pay)")
        assert ("customers", "regions") in paths, paths

    def test_literal_kwarg_empty(self):
        assert _paths("amount:percentile(p=0.5)") == ()


class TestTemplateFragmentKwargs:
    def test_user_fragment_crossing(self):
        paths = _paths("amount:scaled_sum(scale='customers.regions.weight')")
        assert ("customers", "regions") in paths, paths

    def test_model_default_fragment_crossing(self):
        # No user kwarg — the crossing input is the AggregationParam default.
        paths = _paths("amount:wscaled_sum")
        assert ("customers", "regions") in paths, paths

    def test_local_fragment_empty(self):
        assert _paths("amount:scaled_sum(scale='amount * 2')") == ()

    def test_fragment_dialect_fallback_backticks(self):
        # Codex test-review #5: a fragment the primary (postgres) parser
        # rejects but a fallback dialect (mysql backticks) accepts must
        # still surface its crossed path — same fallback chain as
        # compute_column_filter_join_paths.
        paths = _paths(
            "amount:scaled_sum(scale='`customers__regions`.`weight`')",
        )
        assert ("customers", "regions") in paths, paths

    def test_unparseable_fragment_empty(self):
        # Parity with the Column.filter scan's defensive fallback — an
        # unparseable fragment contributes nothing (pre-Stage-5 behavior
        # preserved; NOT an endorsement). DEV-1709 Codex plan-review F1.
        assert _paths("amount:scaled_sum(scale='%% !! ((')") == ()


class TestColumnFilterExcluded:
    def test_filter_only_crossing_not_reported(self):
        # eu_amount's ONLY crossing input is its Column.filter — carried by
        # column_filter_key.referenced_join_paths, not by this helper.
        assert _paths("eu_amount:sum") == ()
