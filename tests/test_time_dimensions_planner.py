"""Stage 7b.3b (DEV-1450) — bind_time_dimension + planner integration tests.

Wires ``TimeDimension`` entries through to typed ``TimeTruncKey`` slots
in the new pipeline. ``TimeTruncKey`` already exists
(``slayer/core/keys.py``); this stage adds the binder hook, the planner
slot-iteration / canonical-naming branches, and the stage_planner pass
that turns ``query.time_dimensions`` into ``DeclaredMeasure`` rows that
``ProjectionPlanner`` materialises.

Public-alias contract (matches legacy ``EnrichedTimeDimension.alias =
f"{model}.{td.dimension.full_name}"``): the granularity affects only the
SQL ``DATE_TRUNC``, not the alias. Local TD on column ``created_at``
emits ``created_at``; joined TD ``customers.signed_up_at`` flattens to
``customers__signed_up_at`` in the stage schema so downstream stages can
bind it as a flat name.

These tests fail until 7b.3b lands:

* ``bind_time_dimension`` does not exist in ``slayer.engine.binding``.
* ``_iter_slot_deps`` does not yield from ``TimeTruncKey``.
* ``_canonical_name`` does not handle ``TimeTruncKey``.
* ``stage_planner.plan_query`` ignores ``query.time_dimensions``.
"""

from __future__ import annotations

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.errors import IllegalScopeReferenceError, UnknownReferenceError
from slayer.core.keys import AggregateKey, ColumnKey, Phase, TimeTruncKey
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.core.query import ColumnRef, SlayerQuery, TimeDimension
from slayer.core.scope import ModelScope, StageColumn, StageSchema
from slayer.engine.binding import bind_time_dimension
from slayer.engine.planning import (
    DeclaredMeasure,
    ProjectionPlanner,
    ValueRegistry,
    _canonical_name,
    _iter_slot_deps,
)
from slayer.engine.source_bundle import ResolvedSourceBundle
from slayer.engine.stage_planner import plan_query


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _orders_model() -> SlayerModel:
    return SlayerModel(
        name="orders",
        data_source="prod",
        sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="created_at", type=DataType.TIMESTAMP),
            Column(name="reviewed_at", type=DataType.DATE),
            Column(name="status", type=DataType.TEXT),  # non-temporal
        ],
    )


def _customers_model() -> SlayerModel:
    return SlayerModel(
        name="customers",
        data_source="prod",
        sql_table="customers",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="signed_up_at", type=DataType.TIMESTAMP),
            Column(name="name", type=DataType.TEXT),
        ],
    )


def _orders_with_customers_join() -> SlayerModel:
    return SlayerModel(
        name="orders",
        data_source="prod",
        sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="created_at", type=DataType.TIMESTAMP),
        ],
        joins=[
            ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]]),
        ],
    )


def _customers_with_regions_join() -> SlayerModel:
    return SlayerModel(
        name="customers",
        data_source="prod",
        sql_table="customers",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="region_id", type=DataType.INT),
            Column(name="signed_up_at", type=DataType.TIMESTAMP),
            Column(name="name", type=DataType.TEXT),
        ],
        joins=[
            ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]]),
        ],
    )


def _regions_model() -> SlayerModel:
    return SlayerModel(
        name="regions",
        data_source="prod",
        sql_table="regions",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="name", type=DataType.TEXT),
            Column(name="opened_at", type=DataType.TIMESTAMP),
        ],
    )


def _orders_with_derived_temporal() -> SlayerModel:
    """Host model with a derived (Column.sql) temporal column.

    Used to pin the 7b.3b rejection contract: derived TD columns route
    through ``ColumnSqlKey`` and ``TimeTruncKey.column`` is typed as
    ``ColumnKey``, so the binder must raise rather than silently produce
    an ill-typed key. A later issue can widen TimeTruncKey if/when this
    case is needed.
    """
    return SlayerModel(
        name="orders",
        data_source="prod",
        sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="created_at", type=DataType.TIMESTAMP),
            Column(
                name="created_day",
                sql="date_trunc('day', created_at)",
                type=DataType.TIMESTAMP,
            ),
        ],
    )


def _bundle_local() -> ResolvedSourceBundle:
    return ResolvedSourceBundle(
        source_model=_orders_model(),
        referenced_models=[],
    )


def _bundle_joined() -> ResolvedSourceBundle:
    return ResolvedSourceBundle(
        source_model=_orders_with_customers_join(),
        referenced_models=[_customers_model()],
    )


def _bundle_multi_hop() -> ResolvedSourceBundle:
    return ResolvedSourceBundle(
        source_model=_orders_with_customers_join(),
        referenced_models=[_customers_with_regions_join(), _regions_model()],
    )


def _bundle_derived() -> ResolvedSourceBundle:
    return ResolvedSourceBundle(
        source_model=_orders_with_derived_temporal(),
        referenced_models=[],
    )


# ---------------------------------------------------------------------------
# bind_time_dimension
# ---------------------------------------------------------------------------


class TestBindTimeDimension:
    def test_local_td_yields_timetrunckey(self) -> None:
        model = _orders_model()
        td = TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
        )
        bound = bind_time_dimension(
            td, scope=ModelScope(source_model=model), bundle=_bundle_local(),
        )
        assert isinstance(bound.value_key, TimeTruncKey)
        assert bound.value_key.column == ColumnKey(path=(), leaf="created_at")
        assert bound.value_key.granularity == "month"
        assert bound.phase == Phase.ROW

    def test_joined_td_path_is_populated(self) -> None:
        host = _orders_with_customers_join()
        td = TimeDimension(
            dimension=ColumnRef(name="customers.signed_up_at"),
            granularity=TimeGranularity.DAY,
        )
        bound = bind_time_dimension(
            td, scope=ModelScope(source_model=host), bundle=_bundle_joined(),
        )
        assert isinstance(bound.value_key, TimeTruncKey)
        assert bound.value_key.column == ColumnKey(
            path=("customers",), leaf="signed_up_at",
        )
        assert bound.value_key.granularity == "day"

    def test_non_temporal_column_rejected(self) -> None:
        model = _orders_model()
        td = TimeDimension(
            dimension=ColumnRef(name="status"),
            granularity=TimeGranularity.MONTH,
        )
        with pytest.raises(ValueError, match="temporal"):
            bind_time_dimension(
                td,
                scope=ModelScope(source_model=model),
                bundle=_bundle_local(),
            )

    def test_unknown_column_raises_unknown_reference(self) -> None:
        model = _orders_model()
        td = TimeDimension(
            dimension=ColumnRef(name="not_a_column"),
            granularity=TimeGranularity.MONTH,
        )
        with pytest.raises(UnknownReferenceError):
            bind_time_dimension(
                td,
                scope=ModelScope(source_model=model),
                bundle=_bundle_local(),
            )

    def test_stage_schema_scope_rejected(self) -> None:
        # TimeDimensions only bind against ModelScope — downstream stages
        # don't introduce new TDs through a flat stage schema (the upstream
        # stage already truncated; the downstream stage just refers to
        # the resulting column by flat name).
        td = TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
        )
        stage = StageSchema(
            relation_name="prev",
            columns=[StageColumn(name="created_at", sql_alias="created_at")],
        )
        with pytest.raises(IllegalScopeReferenceError):
            bind_time_dimension(td, scope=stage, bundle=_bundle_local())

    def test_model_scope_without_source_model_rejected(self) -> None:
        td = TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
        )
        with pytest.raises(UnknownReferenceError):
            bind_time_dimension(
                td,
                scope=ModelScope(source_model=None),
                bundle=_bundle_local(),
            )

    def test_date_column_accepted(self) -> None:
        # DATE is in the same temporal bucket as TIMESTAMP — should bind.
        model = _orders_model()
        td = TimeDimension(
            dimension=ColumnRef(name="reviewed_at"),
            granularity=TimeGranularity.WEEK,
        )
        bound = bind_time_dimension(
            td,
            scope=ModelScope(source_model=model),
            bundle=_bundle_local(),
        )
        assert isinstance(bound.value_key, TimeTruncKey)
        assert bound.value_key.column.leaf == "reviewed_at"
        assert bound.value_key.granularity == "week"

    def test_multi_hop_joined_td(self) -> None:
        # orders → customers → regions. TD on regions.opened_at via the
        # multi-hop path. The bound key carries the full path.
        host = _orders_with_customers_join()
        td = TimeDimension(
            dimension=ColumnRef(name="customers.regions.opened_at"),
            granularity=TimeGranularity.MONTH,
        )
        bound = bind_time_dimension(
            td,
            scope=ModelScope(source_model=host),
            bundle=_bundle_multi_hop(),
        )
        assert isinstance(bound.value_key, TimeTruncKey)
        assert bound.value_key.column == ColumnKey(
            path=("customers", "regions"), leaf="opened_at",
        )
        assert bound.value_key.granularity == "month"

    def test_derived_column_sql_td_rejected(self) -> None:
        # 7b.3b limitation: TimeTruncKey.column is typed as ColumnKey, so
        # a derived (Column.sql) temporal column would produce an ill-
        # typed key. Reject explicitly with a clear message; a future
        # follow-up can widen TimeTruncKey if needed.
        host = _orders_with_derived_temporal()
        td = TimeDimension(
            dimension=ColumnRef(name="created_day"),
            granularity=TimeGranularity.DAY,
        )
        with pytest.raises(NotImplementedError, match="Column.sql"):
            bind_time_dimension(
                td,
                scope=ModelScope(source_model=host),
                bundle=_bundle_derived(),
            )


# ---------------------------------------------------------------------------
# _iter_slot_deps + _canonical_name for TimeTruncKey
# ---------------------------------------------------------------------------


class TestPlanningPrimitivesForTimeTruncKey:
    def test_iter_slot_deps_yields_timetrunckey(self) -> None:
        key = TimeTruncKey(
            column=ColumnKey(path=(), leaf="created_at"),
            granularity="month",
        )
        deps = list(_iter_slot_deps(key))
        # TimeTruncKey is its own materialised slot — the generator will
        # render the DATE_TRUNC at the SELECT level.
        assert key in deps

    def test_iter_slot_deps_does_not_force_inner_column_slot(self) -> None:
        # The TimeTruncKey itself is the materialised slot. The inner
        # ColumnKey does not need to be a separate slot just because a
        # TD references it — the generator picks the column expression
        # out of the TimeTruncKey at render time. (This matches legacy:
        # adding a time dimension does not auto-add the raw column as a
        # separate output column.)
        col = ColumnKey(path=(), leaf="created_at")
        key = TimeTruncKey(column=col, granularity="month")
        deps = list(_iter_slot_deps(key))
        assert deps == [key]

    def test_canonical_name_local_no_grain_suffix(self) -> None:
        # Legacy result-key contract: alias is the column name only —
        # the granularity goes into the SQL DATE_TRUNC, not the alias.
        key = TimeTruncKey(
            column=ColumnKey(path=(), leaf="created_at"),
            granularity="month",
        )
        assert _canonical_name(key) == "created_at"

    def test_canonical_name_joined_uses_dunder_path(self) -> None:
        key = TimeTruncKey(
            column=ColumnKey(path=("customers",), leaf="signed_up_at"),
            granularity="day",
        )
        assert _canonical_name(key) == "customers__signed_up_at"

    def test_canonical_name_multi_hop_path(self) -> None:
        key = TimeTruncKey(
            column=ColumnKey(path=("customers", "regions"), leaf="opened_at"),
            granularity="month",
        )
        assert _canonical_name(key) == "customers__regions__opened_at"


# ---------------------------------------------------------------------------
# ValueRegistry interning for TimeTruncKey
# ---------------------------------------------------------------------------


class TestValueRegistryTimeTrunc:
    def test_same_column_same_grain_interns_once(self) -> None:
        reg = ValueRegistry()
        k1 = TimeTruncKey(
            column=ColumnKey(path=(), leaf="created_at"),
            granularity="month",
        )
        k2 = TimeTruncKey(
            column=ColumnKey(path=(), leaf="created_at"),
            granularity="month",
        )
        sid1 = reg.intern(key=k1, declared_name="created_at", phase=Phase.ROW)
        sid2 = reg.intern(key=k2, declared_name="created_at", phase=Phase.ROW)
        assert sid1 == sid2

    def test_different_grain_distinct_slots(self) -> None:
        # TimeTruncKey identity is (column, granularity); different
        # grains on the same column intern to distinct slots even though
        # they share an underlying column.
        reg = ValueRegistry()
        k_month = TimeTruncKey(
            column=ColumnKey(path=(), leaf="created_at"),
            granularity="month",
        )
        k_day = TimeTruncKey(
            column=ColumnKey(path=(), leaf="created_at"),
            granularity="day",
        )
        sid_m = reg.intern(
            key=k_month, declared_name="created_at_month", phase=Phase.ROW,
        )
        sid_d = reg.intern(
            key=k_day, declared_name="created_at_day", phase=Phase.ROW,
        )
        assert sid_m != sid_d

    def test_different_column_distinct_slots(self) -> None:
        reg = ValueRegistry()
        k_a = TimeTruncKey(
            column=ColumnKey(path=(), leaf="created_at"),
            granularity="month",
        )
        k_b = TimeTruncKey(
            column=ColumnKey(path=(), leaf="reviewed_at"),
            granularity="month",
        )
        sid_a = reg.intern(key=k_a, declared_name="created_at", phase=Phase.ROW)
        sid_b = reg.intern(key=k_b, declared_name="reviewed_at", phase=Phase.ROW)
        assert sid_a != sid_b


# ---------------------------------------------------------------------------
# ProjectionPlanner — TDs are public
# ---------------------------------------------------------------------------


class TestProjectionPlannerTimeDimensions:
    def test_td_declared_measure_is_public_not_hidden(self) -> None:
        model = _orders_model()
        td = TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
        )
        bound = bind_time_dimension(
            td,
            scope=ModelScope(source_model=model),
            bundle=_bundle_local(),
        )
        dm = DeclaredMeasure(
            bound=bound,
            declared_name="created_at",
            public_name="created_at",
            label=None,
        )
        plan = ProjectionPlanner().plan(
            measures=[dm],
            filters=[],
            order=[],
            source_column_names=frozenset(c.name for c in model.columns),
            host_model_name=model.name,
        )
        assert len(plan.public_projection) == 1
        sid = plan.public_projection[0]
        slot = plan.registry.get(sid)
        assert slot.hidden is False
        assert slot.public_name == "created_at"
        assert isinstance(slot.key, TimeTruncKey)
        assert slot.phase == Phase.ROW


# ---------------------------------------------------------------------------
# stage_planner.plan_query — TimeDimensions in projection + StageSchema
# ---------------------------------------------------------------------------


class TestStagePlannerTimeDimensions:
    def test_local_td_appears_in_projection(self) -> None:
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[
                TimeDimension(
                    dimension=ColumnRef(name="created_at"),
                    granularity=TimeGranularity.MONTH,
                ),
            ],
        )
        planned = plan_query(query=query, bundle=_bundle_local())
        assert len(planned.projection) == 1
        all_slots = (
            planned.row_slots
            + planned.aggregate_slots
            + planned.combined_expression_slots
        )
        td_slot = next(
            s for s in all_slots if isinstance(s.key, TimeTruncKey)
        )
        assert td_slot.phase == Phase.ROW
        assert td_slot.public_name == "created_at"

    def test_label_propagates_to_stage_column(self) -> None:
        query = SlayerQuery(
            source_model="orders",
            name="s1",
            time_dimensions=[
                TimeDimension(
                    dimension=ColumnRef(name="created_at"),
                    granularity=TimeGranularity.MONTH,
                    label="Order date",
                ),
            ],
        )
        planned = plan_query(query=query, bundle=_bundle_local())
        assert planned.stage_schema is not None
        td_col = next(
            (
                c for c in planned.stage_schema.columns
                if c.name == "created_at"
            ),
            None,
        )
        assert td_col is not None, planned.stage_schema.columns
        assert td_col.label == "Order date"

    def test_joined_td_stage_column_uses_flat_dunder_form(self) -> None:
        query = SlayerQuery(
            source_model="orders",
            name="s1",
            time_dimensions=[
                TimeDimension(
                    dimension=ColumnRef(name="customers.signed_up_at"),
                    granularity=TimeGranularity.DAY,
                ),
            ],
        )
        planned = plan_query(query=query, bundle=_bundle_joined())
        assert planned.stage_schema is not None
        # Tight invariant: exactly one column, with the flat dunder name.
        names = [c.name for c in planned.stage_schema.columns]
        assert names == ["customers__signed_up_at"], names

    def test_joined_td_label_propagates(self) -> None:
        query = SlayerQuery(
            source_model="orders",
            name="s1",
            time_dimensions=[
                TimeDimension(
                    dimension=ColumnRef(name="customers.signed_up_at"),
                    granularity=TimeGranularity.DAY,
                    label="Customer signup",
                ),
            ],
        )
        planned = plan_query(query=query, bundle=_bundle_joined())
        assert planned.stage_schema is not None
        col = next(
            c for c in planned.stage_schema.columns
            if c.name == "customers__signed_up_at"
        )
        assert col.label == "Customer signup"

    def test_multi_hop_td_in_stage_schema(self) -> None:
        query = SlayerQuery(
            source_model="orders",
            name="s1",
            time_dimensions=[
                TimeDimension(
                    dimension=ColumnRef(name="customers.regions.opened_at"),
                    granularity=TimeGranularity.MONTH,
                ),
            ],
        )
        planned = plan_query(query=query, bundle=_bundle_multi_hop())
        assert planned.stage_schema is not None
        assert [c.name for c in planned.stage_schema.columns] == [
            "customers__regions__opened_at",
        ]

    def test_td_buckets_into_row_slots(self) -> None:
        # TDs are row-phase, so they end up in row_slots (not
        # aggregate_slots / combined_expression_slots).
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[
                TimeDimension(
                    dimension=ColumnRef(name="created_at"),
                    granularity=TimeGranularity.MONTH,
                ),
            ],
        )
        planned = plan_query(query=query, bundle=_bundle_local())
        assert any(isinstance(s.key, TimeTruncKey) for s in planned.row_slots)
        assert not any(
            isinstance(s.key, TimeTruncKey) for s in planned.aggregate_slots
        )
        assert not any(
            isinstance(s.key, TimeTruncKey)
            for s in planned.combined_expression_slots
        )

    def test_td_coexists_with_aggregate_measure(self) -> None:
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[
                TimeDimension(
                    dimension=ColumnRef(name="created_at"),
                    granularity=TimeGranularity.MONTH,
                ),
            ],
            measures=[{"formula": "amount:sum"}],
        )
        planned = plan_query(query=query, bundle=_bundle_local())
        assert len(planned.projection) == 2
        assert any(isinstance(s.key, TimeTruncKey) for s in planned.row_slots)
        assert any(
            isinstance(s.key, AggregateKey) for s in planned.aggregate_slots
        )

    def test_stage_schema_emits_one_td_column(self) -> None:
        query = SlayerQuery(
            source_model="orders",
            name="s1",
            time_dimensions=[
                TimeDimension(
                    dimension=ColumnRef(name="created_at"),
                    granularity=TimeGranularity.MONTH,
                ),
            ],
            measures=[{"formula": "amount:sum"}],
        )
        planned = plan_query(query=query, bundle=_bundle_local())
        assert planned.stage_schema is not None
        cols = planned.stage_schema.columns
        assert [c.name for c in cols] == ["created_at", "amount_sum"]
