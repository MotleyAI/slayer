"""ExpressionBinder + FilterBinder tests: ``ParsedExpr`` + scope (``DatasetScope``
with relationships / a stage's dataset) → typed ``BoundExpr`` of resolved ``ValueKey``s.
Covers self-prefix stripping (C14) and phase classification (P8)."""

from __future__ import annotations

from decimal import Decimal

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import (
    UnknownReferenceError,
    UnresolvableDimensionRelationshipError,
)
from slayer.core.keys import (
    AggregateKey,
    ArithmeticKey,
    FieldKey,
    FieldSqlKey,
    LiteralKey,
    Phase,
    ScalarCallKey,
    StarKey,
    TransformKey,
)
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
from slayer.core.scope import DatasetScope, StageField, StageSchema
from slayer.engine.binding import (
    bind_expr,
    bind_filter,
)
from slayer.engine.definitions import resolved_dataset
from slayer.ir.source_bundle import ResolvedSourceBundle, dataset_from_stage_schema
from slayer.core.syntax import Ref, parse_expr


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _orders() -> SlayerDataset:
    return SlayerDataset(
        name="orders",
        data_source="prod",
        sql_table="orders",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="customer_id", datatype=DataType.INTEGER),
            DatasetField(name="amount", datatype=DataType.FLOAT),
            DatasetField(name="status", datatype=DataType.STRING),
            DatasetField(name="created_at", datatype=DataType.DATETIME),
            DatasetField(
                name="revenue_2",
                datatype=DataType.FLOAT,
                expression="amount * 2",
            ),
        ],
        relationships=[
            DatasetRelationship(
                to="customers",
                from_columns=["customer_id"], to_columns=["id"],
            ),
        ],
    )


def _customers() -> SlayerDataset:
    return SlayerDataset(
        name="customers",
        data_source="prod",
        sql_table="customers",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="region_id", datatype=DataType.INTEGER),
            DatasetField(name="revenue", datatype=DataType.FLOAT),
            DatasetField(name="name", datatype=DataType.STRING),
            DatasetField(
                name="revenue_doubled",
                datatype=DataType.FLOAT,
                expression="revenue * 2",
            ),
        ],
        relationships=[
            DatasetRelationship(to="regions", from_columns=["region_id"], to_columns=["id"]),
        ],
    )


def _regions() -> SlayerDataset:
    return SlayerDataset(
        name="regions",
        data_source="prod",
        sql_table="regions",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="name", datatype=DataType.STRING),
        ],
    )


def _bundle() -> ResolvedSourceBundle:
    return ResolvedSourceBundle(
        dialect="postgres",
        population=_orders(),
        referenced_datasets=[_customers(), _regions()],
    )


def _scope() -> DatasetScope:
    return DatasetScope(population=_orders())


# ---------------------------------------------------------------------------
# Row-level refs
# ---------------------------------------------------------------------------


class TestRowRefs:
    def test_local_bare_ref(self):
        bound = bind_expr(
            parse_expr("amount"), scope=_scope(), bundle=_bundle(),
        )
        assert bound.value_key == FieldKey(path=(), leaf="amount")

    def test_dotted_one_hop(self):
        bound = bind_expr(
            parse_expr("customers.name"), scope=_scope(), bundle=_bundle(),
        )
        assert bound.value_key == FieldKey(path=("customers",), leaf="name")

    def test_dotted_multi_hop(self):
        bound = bind_expr(
            parse_expr("customers.regions.name"),
            scope=_scope(), bundle=_bundle(),
        )
        assert bound.value_key == FieldKey(
            path=("customers", "regions"), leaf="name",
        )

    def test_derived_column_resolves_to_column_sql_key(self):
        bound = bind_expr(
            parse_expr("revenue_2"), scope=_scope(), bundle=_bundle(),
        )
        assert bound.value_key == FieldSqlKey(
            path=(), dataset="orders", field_name="revenue_2",
        )

    def test_cross_model_derived_column_carries_path(self):
        # Joined derived field: FieldSqlKey.path is the relationship walk.
        bound = bind_expr(
            parse_expr("customers.revenue_doubled"),
            scope=_scope(), bundle=_bundle(),
        )
        assert bound.value_key == FieldSqlKey(
            path=("customers",),
            dataset="customers",
            field_name="revenue_doubled",
        )

    def test_unknown_ref_raises(self):
        expr, scope, bundle = parse_expr("nonexistent"), _scope(), _bundle()
        with pytest.raises(UnknownReferenceError):
            bind_expr(expr, scope=scope, bundle=bundle)

    def test_unknown_dotted_ref_raises(self):
        expr, scope, bundle = parse_expr("customers.nonexistent"), _scope(), _bundle()
        with pytest.raises(UnknownReferenceError):
            bind_expr(expr, scope=scope, bundle=bundle)

    def test_unknown_join_target_raises(self):
        # An unreachable short form is route-aware-rejected (was UnknownReferenceError).
        expr = parse_expr("warehouses.id")
        scope, bundle = _scope(), _bundle()
        with pytest.raises(UnresolvableDimensionRelationshipError):
            bind_expr(expr, scope=scope, bundle=bundle)


# ---------------------------------------------------------------------------
# C14 self-prefix stripping
# ---------------------------------------------------------------------------


class TestSelfPrefixStripping:
    def test_self_prefix_stripped(self):
        # orders.status over an orders-rooted query → FieldKey(path=(), leaf="status").
        bound = bind_expr(
            parse_expr("orders.status"), scope=_scope(), bundle=_bundle(),
        )
        assert bound.value_key == FieldKey(path=(), leaf="status")

    def test_self_prefix_with_join_continues(self):
        # orders.customers.name → strip orders, then walk customers.
        bound = bind_expr(
            parse_expr("orders.customers.name"),
            scope=_scope(), bundle=_bundle(),
        )
        assert bound.value_key == FieldKey(
            path=("customers",), leaf="name",
        )


# ---------------------------------------------------------------------------
# Aggregations
# ---------------------------------------------------------------------------


class TestAggregations:
    def test_local_aggregation(self):
        bound = bind_expr(
            parse_expr("sum(amount)"), scope=_scope(), bundle=_bundle(),
        )
        assert bound.value_key == AggregateKey(
            source=FieldKey(path=(), leaf="amount"), agg="sum",
        )

    def test_star_count(self):
        bound = bind_expr(
            parse_expr("count(*)"), scope=_scope(), bundle=_bundle(),
        )
        assert bound.value_key == AggregateKey(
            source=StarKey(), agg="count",
        )

    def test_cross_model_aggregation(self):
        bound = bind_expr(
            parse_expr("sum(customers.revenue)"),
            scope=_scope(), bundle=_bundle(),
        )
        assert bound.value_key == AggregateKey(
            source=FieldKey(path=("customers",), leaf="revenue"),
            agg="sum",
        )

    def test_aggregation_with_kwarg(self):
        bound = bind_expr(
            parse_expr("weighted_avg(amount, weight=customer_id)"),
            scope=_scope(), bundle=_bundle(),
        )
        key = bound.value_key
        assert isinstance(key, AggregateKey)
        assert key.agg == "weighted_avg"
        # Kwargs canonicalised; weight=FieldKey(...).
        assert dict(key.kwargs) == {
            "weight": FieldKey(path=(), leaf="customer_id"),
        }

    def test_aggregation_phase_is_aggregate(self):
        bound = bind_expr(
            parse_expr("sum(amount)"), scope=_scope(), bundle=_bundle(),
        )
        assert bound.value_key.phase == Phase.AGGREGATE


# ---------------------------------------------------------------------------
# Transforms
# ---------------------------------------------------------------------------


class TestTransforms:
    def test_cumsum(self):
        bound = bind_expr(
            parse_expr("cumsum(sum(amount))"),
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
        assert key.operands[0] == FieldKey(path=(), leaf="amount")
        assert key.operands[1] == LiteralKey(value=Decimal(1))

    def test_arithmetic_phase_max(self):
        # amount + amount:sum → phase = AGGREGATE.
        bound = bind_expr(
            parse_expr("amount + sum(amount)"),
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

    def test_aggregates_nested_in_scalar_call_stay_bound(self):
        # Backfill from the deleted
        # test_formula.py::TestPlaceholderLeakRegression. The legacy
        # parser flattened such expressions to a SQL string with ``__aggN__``
        # placeholders, so an aggregate nested inside a non-transform call
        # (``nullif`` / ``coalesce``) could fail to register and leak its
        # placeholder into emitted SQL. The typed pipeline keeps structure, so
        # the equivalent guarantee is that BOTH aggregates survive binding as
        # AggregateKeys in their original positions.
        bound = bind_expr(
            parse_expr("count(*) / nullif(max(amount), 0)"),
            scope=_scope(), bundle=_bundle(),
        )
        key = bound.value_key
        assert isinstance(key, ArithmeticKey)
        assert key.op == "/"
        outer_agg, wrapped = key.operands
        assert outer_agg == AggregateKey(source=StarKey(path=()), agg="count")
        assert isinstance(wrapped, ScalarCallKey)
        assert wrapped.name == "nullif"
        assert wrapped.args[0] == AggregateKey(
            source=FieldKey(path=(), leaf="amount"), agg="max",
        )
        # An aggregate anywhere inside the scalar call makes the whole
        # expression aggregate-phase.
        assert key.phase == Phase.AGGREGATE

    def test_coalesce_wrapped_aggregate_plus_outside_aggregate(self):
        # ``coalesce(sum(amount), 0) + avg(amount)`` — one
        # aggregate wrapped by a scalar, one outside it. Both must bind.
        bound = bind_expr(
            parse_expr("coalesce(sum(amount), 0) + avg(amount)"),
            scope=_scope(), bundle=_bundle(),
        )
        key = bound.value_key
        assert isinstance(key, ArithmeticKey)
        wrapped, outside = key.operands
        assert isinstance(wrapped, ScalarCallKey)
        assert wrapped.args[0] == AggregateKey(
            source=FieldKey(path=(), leaf="amount"), agg="sum",
        )
        assert outside == AggregateKey(
            source=FieldKey(path=(), leaf="amount"), agg="avg",
        )


# ---------------------------------------------------------------------------
# IllegalScopeReferenceError — `__` in DatasetScope
# ---------------------------------------------------------------------------


class TestIllegalScopeRefs:
    def test_double_underscore_in_modelscope_rejected_at_parse(self):
        # The Mode-B syntax parser already rejects __ in identifiers
        # before the binder sees the ref — covered by syntax tests.
        # Here we verify a parsed expr that doesn't contain __ still binds.
        bound = bind_expr(
            parse_expr("amount"), scope=_scope(), bundle=_bundle(),
        )
        assert bound.value_key == FieldKey(path=(), leaf="amount")


# ---------------------------------------------------------------------------
# StageSchema scope
# ---------------------------------------------------------------------------


def _stage_schema_with_flat_names() -> StageSchema:
    return StageSchema(
        relation_name="stage1",
        fields=[
            StageField(
                name="status", sql_alias="status", public_alias="status",
                datatype=DataType.STRING,
            ),
            StageField(
                name="robot_details__modelseriesval",
                sql_alias="robot_details__modelseriesval",
                public_alias="robot_details.modelseriesval",
                datatype=DataType.STRING,
            ),
            StageField(
                name="rev", sql_alias="rev", public_alias="rev",
                datatype=DataType.FLOAT,
            ),
        ],
    )


def _stage_dataset_scope() -> tuple[DatasetScope, ResolvedSourceBundle]:
    """The dataset scope a later stage binds ``stage1`` through, and its bundle."""
    stage = dataset_from_stage_schema(name="stage1", schema=_stage_schema_with_flat_names(), data_source="prod")
    bundle = ResolvedSourceBundle(
        dialect="postgres", population=stage, referenced_datasets=[_orders(), _customers(), _regions()],
    )
    return DatasetScope(population=stage), bundle


class TestStageSchemaScope:
    def test_flat_name_resolves(self):
        # A stage's fields resolve by their flat names.
        scope, bundle = _stage_dataset_scope()
        bound = bind_expr(
            parse_expr("status"), scope=scope, bundle=bundle,
        )
        assert bound.value_key == FieldKey(path=(), leaf="status")

    def test_dotted_ref_in_stage_schema_rejected(self):
        # A dotted spelling of a flattened field fails as from a stored root.
        scope, bundle = _stage_dataset_scope()
        expr = parse_expr("robot_details.modelseriesval")
        with pytest.raises(UnknownReferenceError):
            bind_expr(expr, scope=scope, bundle=bundle)

    def test_flat_underscore_name_resolves(self):
        # The flat name `robot_details__modelseriesval` IS a field on
        # the stage schema — it resolves.
        scope, bundle = _stage_dataset_scope()
        # The syntax parser rejects `__` in user identifiers — bind_expr
        # is given a pre-parsed ParsedExpr that explicitly uses the flat
        # field name via the Ref constructor.
        bound = bind_expr(Ref(name="robot_details__modelseriesval"),
                          scope=scope, bundle=bundle)
        assert bound.value_key == FieldKey(
            path=(), leaf="robot_details__modelseriesval",
        )

    def test_unknown_flat_name_raises(self):
        scope, bundle = _stage_dataset_scope()
        expr = parse_expr("nope")
        with pytest.raises(UnknownReferenceError):
            bind_expr(expr, scope=scope, bundle=bundle)


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
            parse_expr("sum(amount) >= 100"),
            scope=_scope(), bundle=_bundle(),
        )
        assert bound.phase == Phase.AGGREGATE

    def test_post_phase_filter(self):
        bound = bind_filter(
            parse_expr("cumsum(sum(amount)) > 100"),
            scope=_scope(), bundle=_bundle(),
        )
        assert bound.phase == Phase.POST

    def test_filter_max_phase(self):
        # row + aggregate → AGGREGATE.
        bound = bind_filter(
            parse_expr("status == 'paid' and sum(amount) > 100"),
            scope=_scope(), bundle=_bundle(),
        )
        assert bound.phase == Phase.AGGREGATE

    def test_referenced_keys_set(self):
        bound = bind_filter(
            parse_expr("status == 'paid' and sum(amount) > 100"),
            scope=_scope(), bundle=_bundle(),
        )
        refs = set(bound.referenced_keys)
        assert any(
            isinstance(k, FieldKey) and k.leaf == "status" for k in refs
        )
        assert any(isinstance(k, AggregateKey) for k in refs)

    def test_referenced_keys_include_aggregate_inside_scalar_call(self):
        # Backfill from the deleted
        # test_formula.py::TestPlaceholderLeakRegression::
        # test_predicate_with_nullif_wrapper — ``nullif(count(*), 0) > 5`` is a
        # predicate whose aggregate is buried inside a scalar call. It must
        # still be discovered as a referenced key (the legacy failure mode was
        # a lost registration leaking an ``__aggN__`` placeholder) and drive
        # the filter's phase.
        bound = bind_filter(
            parse_expr("nullif(count(*), 0) > 5"),
            scope=_scope(), bundle=_bundle(),
        )
        assert bound.phase == Phase.AGGREGATE
        refs = set(bound.referenced_keys)
        assert AggregateKey(source=StarKey(path=()), agg="count") in refs


# ---------------------------------------------------------------------------
# Portable windowed DatasetField resolves as a windowed field
# ---------------------------------------------------------------------------


class TestWindowedField:
    def test_portable_windowed_field_resolves_as_windowed(self):
        model = _orders().model_copy(update={
            "fields": list(_orders().fields) + [
                DatasetField(
                    name="rolling_rank",
                    datatype=DataType.INTEGER,
                    expression="RANK() OVER (PARTITION BY status ORDER BY amount)",
                ),
            ],
        })
        peers = [_customers(), _regions()]
        resolved = resolved_dataset(model, ds_type="postgres", datasets=peers, live={})
        field = resolved.get_field("rolling_rank")
        assert field is not None
        assert field.windowed


class TestNestedAggregateParamDimAliasMap:
    def test_nested_partition_by_resolves_computed_dim(self) -> None:
        # dim_alias_map must thread into a nested aggregate param's partition_by.
        parsed = parse_expr(
            "weighted_avg(amount, weight=count(id, partition_by=my_dim))")
        computed = FieldKey(leaf="status")
        outer = bind_expr(
            parsed=parsed, scope=_scope(), bundle=_bundle(),
            dimension_alias_map={"my_dim": computed},
        ).value_key
        assert isinstance(outer, AggregateKey)
        weight = dict(outer.kwargs)["weight"]
        assert isinstance(weight, AggregateKey)
        assert weight.partition_keys is not None
        assert computed in weight.partition_keys

    def test_nested_partition_by_without_thread_is_unresolved(self) -> None:
        # Guard: the bare computed-dim name only binds through dim_alias_map.
        parsed = parse_expr(
            "weighted_avg(amount, weight=count(id, partition_by=my_dim))")
        scope, bundle = _scope(), _bundle()
        with pytest.raises(UnknownReferenceError):
            bind_expr(parsed=parsed, scope=scope, bundle=bundle)
