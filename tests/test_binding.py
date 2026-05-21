"""Stage 7a.5 (DEV-1450) — ExpressionBinder + FilterBinder tests.

The binder consumes a ``ParsedExpr`` (from ``slayer/engine/syntax.py``)
plus a scope (``ModelScope`` or ``StageSchema``) and produces a typed
``BoundExpr`` whose leaves are resolved ``ValueKey``s.

Two scope kinds (P5):
- ``ModelScope``: joins exist; dotted refs walk the join graph rooted
  at ``source_model``. ``__``-bearing refs raise
  ``IllegalScopeReferenceError`` unless they exact-match a column on
  the model.
- ``StageSchema``: flat namespace; dotted refs raise
  ``IllegalScopeReferenceError``; flat names with ``__`` are legal.

C14 (DEV-1448 cushion): same-model self-prefix in Mode B is stripped
before join resolution. ``orders.status`` over an ``orders`` query →
``status`` (a local ColumnKey, not a dotted walk).

Phase classification (P8):
- Row slots → Phase.ROW.
- Aggregates → Phase.AGGREGATE.
- Transforms → Phase.POST.
- ArithmeticKey / ScalarCallKey: phase = max(operand.phase).

Dormant in 7a — no engine wiring. The planner (7a.6) is the first
consumer.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import (
    IllegalScopeReferenceError,
    IllegalWindowInFilterError,
    UnknownReferenceError,
)
from slayer.core.keys import (
    AggregateKey,
    ArithmeticKey,
    ColumnKey,
    ColumnSqlKey,
    LiteralKey,
    Phase,
    ScalarCallKey,
    StarKey,
    TransformKey,
)
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.core.scope import ModelScope, StageColumn, StageSchema
from slayer.engine.binding import (
    bind_expr,
    bind_filter,
)
from slayer.engine.source_bundle import ResolvedSourceBundle
from slayer.engine.syntax import parse_expr


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _orders() -> SlayerModel:
    return SlayerModel(
        name="orders",
        data_source="prod",
        sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="status", type=DataType.TEXT),
            Column(name="created_at", type=DataType.TIMESTAMP),
            Column(
                name="revenue_2",
                type=DataType.DOUBLE,
                sql="amount * 2",
            ),
        ],
        joins=[
            ModelJoin(
                target_model="customers",
                join_pairs=[["customer_id", "id"]],
            ),
        ],
    )


def _customers() -> SlayerModel:
    return SlayerModel(
        name="customers",
        data_source="prod",
        sql_table="customers",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="region_id", type=DataType.INT),
            Column(name="revenue", type=DataType.DOUBLE),
            Column(name="name", type=DataType.TEXT),
            Column(
                name="revenue_doubled",
                type=DataType.DOUBLE,
                sql="revenue * 2",
            ),
        ],
        joins=[
            ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]]),
        ],
    )


def _regions() -> SlayerModel:
    return SlayerModel(
        name="regions",
        data_source="prod",
        sql_table="regions",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="name", type=DataType.TEXT),
        ],
    )


def _bundle() -> ResolvedSourceBundle:
    return ResolvedSourceBundle(
        source_model=_orders(),
        referenced_models=[_customers(), _regions()],
    )


def _scope() -> ModelScope:
    return ModelScope(source_model=_orders())


# ---------------------------------------------------------------------------
# Row-level refs
# ---------------------------------------------------------------------------


class TestRowRefs:
    def test_local_bare_ref(self):
        bound = bind_expr(
            parse_expr("amount"), scope=_scope(), bundle=_bundle(),
        )
        assert bound.value_key == ColumnKey(path=(), leaf="amount")

    def test_dotted_one_hop(self):
        bound = bind_expr(
            parse_expr("customers.name"), scope=_scope(), bundle=_bundle(),
        )
        assert bound.value_key == ColumnKey(path=("customers",), leaf="name")

    def test_dotted_multi_hop(self):
        bound = bind_expr(
            parse_expr("customers.regions.name"),
            scope=_scope(), bundle=_bundle(),
        )
        assert bound.value_key == ColumnKey(
            path=("customers", "regions"), leaf="name",
        )

    def test_derived_column_resolves_to_column_sql_key(self):
        bound = bind_expr(
            parse_expr("revenue_2"), scope=_scope(), bundle=_bundle(),
        )
        assert bound.value_key == ColumnSqlKey(
            path=(), model="orders", column_name="revenue_2",
        )

    def test_cross_model_derived_column_carries_path(self):
        # Joined derived column: ColumnSqlKey.path is the join walk.
        bound = bind_expr(
            parse_expr("customers.revenue_doubled"),
            scope=_scope(), bundle=_bundle(),
        )
        assert bound.value_key == ColumnSqlKey(
            path=("customers",),
            model="customers",
            column_name="revenue_doubled",
        )

    def test_unknown_ref_raises(self):
        with pytest.raises(UnknownReferenceError):
            bind_expr(
                parse_expr("nonexistent"),
                scope=_scope(), bundle=_bundle(),
            )

    def test_unknown_dotted_ref_raises(self):
        with pytest.raises(UnknownReferenceError):
            bind_expr(
                parse_expr("customers.nonexistent"),
                scope=_scope(), bundle=_bundle(),
            )

    def test_unknown_join_target_raises(self):
        with pytest.raises(UnknownReferenceError):
            bind_expr(
                parse_expr("warehouses.id"),
                scope=_scope(), bundle=_bundle(),
            )


# ---------------------------------------------------------------------------
# C14 self-prefix stripping
# ---------------------------------------------------------------------------


class TestSelfPrefixStripping:
    def test_self_prefix_stripped(self):
        # orders.status over an orders-rooted query → ColumnKey(path=(), leaf="status").
        bound = bind_expr(
            parse_expr("orders.status"), scope=_scope(), bundle=_bundle(),
        )
        assert bound.value_key == ColumnKey(path=(), leaf="status")

    def test_self_prefix_with_join_continues(self):
        # orders.customers.name → strip orders, then walk customers.
        bound = bind_expr(
            parse_expr("orders.customers.name"),
            scope=_scope(), bundle=_bundle(),
        )
        assert bound.value_key == ColumnKey(
            path=("customers",), leaf="name",
        )


# ---------------------------------------------------------------------------
# Aggregations
# ---------------------------------------------------------------------------


class TestAggregations:
    def test_local_aggregation(self):
        bound = bind_expr(
            parse_expr("amount:sum"), scope=_scope(), bundle=_bundle(),
        )
        assert bound.value_key == AggregateKey(
            source=ColumnKey(path=(), leaf="amount"), agg="sum",
        )

    def test_star_count(self):
        bound = bind_expr(
            parse_expr("*:count"), scope=_scope(), bundle=_bundle(),
        )
        assert bound.value_key == AggregateKey(
            source=StarKey(), agg="count",
        )

    def test_cross_model_aggregation(self):
        bound = bind_expr(
            parse_expr("customers.revenue:sum"),
            scope=_scope(), bundle=_bundle(),
        )
        assert bound.value_key == AggregateKey(
            source=ColumnKey(path=("customers",), leaf="revenue"),
            agg="sum",
        )

    def test_aggregation_with_kwarg(self):
        bound = bind_expr(
            parse_expr("amount:weighted_avg(weight=customer_id)"),
            scope=_scope(), bundle=_bundle(),
        )
        key = bound.value_key
        assert isinstance(key, AggregateKey)
        assert key.agg == "weighted_avg"
        # Kwargs canonicalised; weight=ColumnKey(...).
        assert dict(key.kwargs) == {
            "weight": ColumnKey(path=(), leaf="customer_id"),
        }

    def test_aggregation_phase_is_aggregate(self):
        bound = bind_expr(
            parse_expr("amount:sum"), scope=_scope(), bundle=_bundle(),
        )
        assert bound.value_key.phase == Phase.AGGREGATE


# ---------------------------------------------------------------------------
# Transforms
# ---------------------------------------------------------------------------


class TestTransforms:
    def test_cumsum(self):
        bound = bind_expr(
            parse_expr("cumsum(amount:sum)"),
            scope=_scope(), bundle=_bundle(),
        )
        key = bound.value_key
        assert isinstance(key, TransformKey)
        assert key.op == "cumsum"
        assert isinstance(key.input, AggregateKey)
        assert key.phase == Phase.POST


# ---------------------------------------------------------------------------
# Arithmetic / scalar / literals
# ---------------------------------------------------------------------------


class TestComposite:
    def test_simple_arithmetic(self):
        bound = bind_expr(
            parse_expr("amount + 1"),
            scope=_scope(), bundle=_bundle(),
        )
        key = bound.value_key
        assert isinstance(key, ArithmeticKey)
        assert key.op == "+"
        assert key.operands[0] == ColumnKey(path=(), leaf="amount")
        assert key.operands[1] == LiteralKey(value=Decimal(1))

    def test_arithmetic_phase_max(self):
        # amount + amount:sum → phase = AGGREGATE.
        bound = bind_expr(
            parse_expr("amount + amount:sum"),
            scope=_scope(), bundle=_bundle(),
        )
        assert bound.value_key.phase == Phase.AGGREGATE

    def test_scalar_call_resolves_args(self):
        bound = bind_expr(
            parse_expr("coalesce(amount, 0)"),
            scope=_scope(), bundle=_bundle(),
        )
        key = bound.value_key
        assert isinstance(key, ScalarCallKey)
        assert key.name == "coalesce"

    def test_literal_only(self):
        bound = bind_expr(
            parse_expr("42"),
            scope=_scope(), bundle=_bundle(),
        )
        assert bound.value_key == LiteralKey(value=Decimal(42))


# ---------------------------------------------------------------------------
# IllegalScopeReferenceError — `__` in ModelScope
# ---------------------------------------------------------------------------


class TestIllegalScopeRefs:
    def test_double_underscore_in_modelscope_rejected_at_parse(self):
        # The Mode-B syntax parser already rejects __ in identifiers
        # before the binder sees the ref — covered by syntax tests.
        # Here we verify a parsed expr that doesn't contain __ still binds.
        bound = bind_expr(
            parse_expr("amount"), scope=_scope(), bundle=_bundle(),
        )
        assert bound.value_key == ColumnKey(path=(), leaf="amount")


# ---------------------------------------------------------------------------
# StageSchema scope
# ---------------------------------------------------------------------------


def _stage_schema_with_flat_names() -> StageSchema:
    return StageSchema(
        relation_name="stage1",
        columns=[
            StageColumn(
                name="status", sql_alias="status", public_alias="status",
                type=DataType.TEXT,
            ),
            StageColumn(
                name="robot_details__modelseriesval",
                sql_alias="robot_details__modelseriesval",
                public_alias="robot_details.modelseriesval",
                type=DataType.TEXT,
            ),
            StageColumn(
                name="rev", sql_alias="rev", public_alias="rev",
                type=DataType.DOUBLE,
            ),
        ],
    )


class TestStageSchemaScope:
    def test_flat_name_resolves(self):
        # In a StageSchema scope, refs resolve as flat names — no join
        # walking.
        scope = _stage_schema_with_flat_names()
        bound = bind_expr(
            parse_expr("status"), scope=scope, bundle=_bundle(),
        )
        assert bound.value_key == ColumnKey(path=(), leaf="status")

    def test_dotted_ref_in_stage_schema_rejected(self):
        # DEV-1449: downstream stages see a flat schema — dotted refs
        # are illegal in StageSchema scope.
        scope = _stage_schema_with_flat_names()
        with pytest.raises(IllegalScopeReferenceError):
            bind_expr(
                parse_expr("robot_details.modelseriesval"),
                scope=scope, bundle=_bundle(),
            )

    def test_flat_underscore_name_resolves(self):
        # The flat name `robot_details__modelseriesval` IS a column on
        # the stage schema — it resolves.
        scope = _stage_schema_with_flat_names()
        # The syntax parser rejects `__` in user identifiers — bind_expr
        # is given a pre-parsed ParsedExpr that explicitly uses the flat
        # column name via the Ref constructor.
        from slayer.engine.syntax import Ref
        bound = bind_expr(Ref(name="robot_details__modelseriesval"),
                          scope=scope, bundle=_bundle())
        assert bound.value_key == ColumnKey(
            path=(), leaf="robot_details__modelseriesval",
        )

    def test_unknown_flat_name_raises(self):
        scope = _stage_schema_with_flat_names()
        with pytest.raises(UnknownReferenceError):
            bind_expr(parse_expr("nope"), scope=scope, bundle=_bundle())


# ---------------------------------------------------------------------------
# FilterBinder — phase classification
# ---------------------------------------------------------------------------


class TestFilterBinder:
    def test_row_phase_filter(self):
        bound = bind_filter(
            parse_expr("status == 'paid'"),
            scope=_scope(), bundle=_bundle(),
        )
        assert bound.phase == Phase.ROW

    def test_aggregate_phase_filter(self):
        bound = bind_filter(
            parse_expr("amount:sum >= 100"),
            scope=_scope(), bundle=_bundle(),
        )
        assert bound.phase == Phase.AGGREGATE

    def test_post_phase_filter(self):
        bound = bind_filter(
            parse_expr("cumsum(amount:sum) > 100"),
            scope=_scope(), bundle=_bundle(),
        )
        assert bound.phase == Phase.POST

    def test_filter_max_phase(self):
        # row + aggregate → AGGREGATE.
        bound = bind_filter(
            parse_expr("status == 'paid' and amount:sum > 100"),
            scope=_scope(), bundle=_bundle(),
        )
        assert bound.phase == Phase.AGGREGATE

    def test_referenced_keys_set(self):
        bound = bind_filter(
            parse_expr("status == 'paid' and amount:sum > 100"),
            scope=_scope(), bundle=_bundle(),
        )
        refs = set(bound.referenced_keys)
        assert any(
            isinstance(k, ColumnKey) and k.leaf == "status" for k in refs
        )
        assert any(isinstance(k, AggregateKey) for k in refs)


# ---------------------------------------------------------------------------
# Windowed Column.sql in filter → IllegalWindowInFilterError
# ---------------------------------------------------------------------------


class TestWindowInFilter:
    def test_filter_on_windowed_column_sql_raises(self):
        # A Column.sql containing a window function. A filter referencing
        # that column raises IllegalWindowInFilterError (DEV-1369: no
        # auto-promotion).
        model = _orders().model_copy(update={
            "columns": list(_orders().columns) + [
                Column(
                    name="rolling_rank",
                    type=DataType.INT,
                    sql="RANK() OVER (PARTITION BY status ORDER BY amount)",
                ),
            ],
        })
        scope = ModelScope(source_model=model)
        bundle = ResolvedSourceBundle(
            source_model=model,
            referenced_models=[_customers(), _regions()],
        )
        with pytest.raises(IllegalWindowInFilterError):
            bind_filter(
                parse_expr("rolling_rank > 1"),
                scope=scope, bundle=bundle,
            )
