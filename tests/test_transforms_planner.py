"""Stage 7b.4 (DEV-1450) — planner-side transform support.

Three slots:

1. **Per-op binder validation**: ``_bind_transform`` rejects malformed
   transform calls at bind time. ``ntile(x)`` without ``n=`` raises;
   ``n`` must be a positive int. ``time_shift(x)`` without ``periods=``
   raises. Other per-op kwarg whitelists mirror legacy
   ``_ALLOWED_TRANSFORM_KWARGS``.

2. **ProjectionPlanner auxiliary slots**: when a TransformKey carries
   ``partition_keys`` or ``time_key``, those underlying ``ValueKey``s
   must materialise as hidden slots so the generator (slice 7b.10) can
   render the PARTITION BY / ORDER BY against named SELECT projections
   rather than re-walking the model graph.

3. **stage_planner populates transform_layers**: each TransformKey slot
   in the registry produces a ``TransformLayer`` entry in the
   ``PlannedQuery`` so the generator slices group window emission by
   op.

Out of scope (deferred to later stages):

* Time-key auto-inference from ``main_time_dimension`` for transforms
  that need one — handled at SQL emission in slice 7b.10 / 7b.11.
* TransformLayer shape extension to carry ``partition_keys`` /
  ``time_key`` / ``args`` / ``kwargs``: the TransformKey on the slot
  already carries this data; redundancy on TransformLayer is unneeded.
* Aggregation-vs-transform overload for ``first`` / ``last``: parser
  registers both names as transforms today; the planner stage emits
  the typed shape and the generator decides aggregation vs
  window-function emission.
"""

from __future__ import annotations

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.keys import (
    AggregateKey,
    ColumnKey,
    TimeTruncKey,
    TransformKey,
)
from slayer.core.models import Column, SlayerModel
from slayer.core.query import ColumnRef, SlayerQuery, TimeDimension
from slayer.core.scope import ModelScope
from slayer.engine.binding import bind_expr
from slayer.engine.planning import _iter_slot_deps
from slayer.engine.source_bundle import ResolvedSourceBundle
from slayer.engine.stage_planner import plan_query
from slayer.engine.syntax import parse_expr


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
            Column(name="region", type=DataType.TEXT),
            Column(name="created_at", type=DataType.TIMESTAMP),
        ],
    )


def _bundle() -> ResolvedSourceBundle:
    return ResolvedSourceBundle(
        source_model=_orders_model(),
        referenced_models=[],
    )


def _scope() -> ModelScope:
    return ModelScope(source_model=_orders_model())


# ---------------------------------------------------------------------------
# Per-op binder validation
# ---------------------------------------------------------------------------


class TestBindTransformValidation:
    def test_ntile_without_n_raises(self) -> None:
        # `ntile(amount:sum)` — missing `n=` kwarg.
        with pytest.raises(ValueError, match="ntile.*n"):
            bind_expr(
                parse_expr("ntile(amount:sum)"),
                scope=_scope(), bundle=_bundle(),
            )

    def test_ntile_with_n_zero_raises(self) -> None:
        with pytest.raises(ValueError, match="ntile.*positive"):
            bind_expr(
                parse_expr("ntile(amount:sum, n=0)"),
                scope=_scope(), bundle=_bundle(),
            )

    def test_ntile_with_n_negative_raises(self) -> None:
        with pytest.raises(ValueError, match="ntile.*positive"):
            bind_expr(
                parse_expr("ntile(amount:sum, n=-3)"),
                scope=_scope(), bundle=_bundle(),
            )

    def test_ntile_with_n_string_raises(self) -> None:
        with pytest.raises(ValueError, match="ntile.*integer"):
            bind_expr(
                parse_expr("ntile(amount:sum, n='four')"),
                scope=_scope(), bundle=_bundle(),
            )

    def test_ntile_with_n_bool_raises(self) -> None:
        # bool is an int subclass in Python — explicit rejection so
        # ``n=True`` doesn't silently become n=1.
        with pytest.raises(ValueError, match="ntile.*integer"):
            bind_expr(
                parse_expr("ntile(amount:sum, n=True)"),
                scope=_scope(), bundle=_bundle(),
            )

    def test_ntile_with_valid_n_binds(self) -> None:
        bound = bind_expr(
            parse_expr("ntile(amount:sum, n=4)"),
            scope=_scope(), bundle=_bundle(),
        )
        assert isinstance(bound.value_key, TransformKey)
        assert bound.value_key.op == "ntile"
        # n=4 ends up in kwargs.
        kw_dict = dict(bound.value_key.kwargs)
        assert kw_dict["n"] == 4

    def test_time_shift_without_periods_raises(self) -> None:
        with pytest.raises(ValueError, match="time_shift.*periods"):
            bind_expr(
                parse_expr("time_shift(amount:sum)"),
                scope=_scope(), bundle=_bundle(),
            )

    def test_time_shift_with_periods_binds(self) -> None:
        bound = bind_expr(
            parse_expr("time_shift(amount:sum, periods=-1)"),
            scope=_scope(), bundle=_bundle(),
        )
        assert isinstance(bound.value_key, TransformKey)
        assert bound.value_key.op == "time_shift"
        kw_dict = dict(bound.value_key.kwargs)
        assert kw_dict["periods"] == -1

    def test_lag_without_periods_defaults_to_one(self) -> None:
        # lag(x) — binder normalizes a missing periods= to periods=1
        # so the typed TransformKey carries the resolved kwarg list
        # downstream. Without binder normalization the generator would
        # need its own default logic; pinning here keeps one source of
        # truth.
        bound = bind_expr(
            parse_expr("lag(amount:sum)"),
            scope=_scope(), bundle=_bundle(),
        )
        assert isinstance(bound.value_key, TransformKey)
        assert bound.value_key.op == "lag"
        kw_dict = dict(bound.value_key.kwargs)
        assert kw_dict["periods"] == 1

    def test_lead_with_explicit_periods(self) -> None:
        bound = bind_expr(
            parse_expr("lead(amount:sum, periods=2)"),
            scope=_scope(), bundle=_bundle(),
        )
        assert isinstance(bound.value_key, TransformKey)
        kw_dict = dict(bound.value_key.kwargs)
        assert kw_dict["periods"] == 2

    def test_unknown_kwarg_on_rank_raises(self) -> None:
        # rank's allowed kwargs: {partition_by}. Anything else → error.
        with pytest.raises(ValueError, match="rank.*not.*accept"):
            bind_expr(
                parse_expr("rank(amount:sum, foo='bar')"),
                scope=_scope(), bundle=_bundle(),
            )

    def test_unknown_kwarg_on_percent_rank_raises(self) -> None:
        with pytest.raises(ValueError, match="percent_rank.*not.*accept"):
            bind_expr(
                parse_expr("percent_rank(amount:sum, foo='bar')"),
                scope=_scope(), bundle=_bundle(),
            )

    def test_unknown_kwarg_on_dense_rank_raises(self) -> None:
        with pytest.raises(ValueError, match="dense_rank.*not.*accept"):
            bind_expr(
                parse_expr("dense_rank(amount:sum, foo='bar')"),
                scope=_scope(), bundle=_bundle(),
            )

    def test_unknown_kwarg_on_consecutive_periods_raises(self) -> None:
        with pytest.raises(ValueError, match="consecutive_periods.*not.*accept"):
            bind_expr(
                parse_expr("consecutive_periods(amount:sum, foo='bar')"),
                scope=_scope(), bundle=_bundle(),
            )

    def test_rank_with_partition_by_binds(self) -> None:
        bound = bind_expr(
            parse_expr("rank(amount:sum, partition_by=region)"),
            scope=_scope(), bundle=_bundle(),
        )
        assert isinstance(bound.value_key, TransformKey)
        assert bound.value_key.op == "rank"
        assert ColumnKey(path=(), leaf="region") in bound.value_key.partition_keys


# ---------------------------------------------------------------------------
# ProjectionPlanner — hidden slots for transform aux dependencies
# ---------------------------------------------------------------------------


class TestIterSlotDepsTransformAux:
    def test_iter_slot_deps_partition_keys(self) -> None:
        # cumsum(amount:sum, partition_by=region) — the region ColumnKey
        # must surface as a slot dep so the planner materialises it as
        # a hidden slot (for the generator's PARTITION BY).
        inner = AggregateKey(source=ColumnKey(path=(), leaf="amount"), agg="sum")
        region = ColumnKey(path=(), leaf="region")
        tk = TransformKey(
            op="cumsum",
            input=inner,
            partition_keys=frozenset({region}),
        )
        deps = list(_iter_slot_deps(tk))
        # The transform itself, the inner aggregate, AND the partition
        # column should all be slot-worthy deps.
        assert tk in deps
        assert inner in deps
        assert region in deps

    def test_iter_slot_deps_time_key(self) -> None:
        # cumsum-like transform with an explicit time_key column.
        inner = AggregateKey(source=ColumnKey(path=(), leaf="amount"), agg="sum")
        ts = ColumnKey(path=(), leaf="created_at")
        tk = TransformKey(op="cumsum", input=inner, time_key=ts)
        deps = list(_iter_slot_deps(tk))
        assert tk in deps
        assert inner in deps
        assert ts in deps

    def test_iter_slot_deps_time_truncated_time_key(self) -> None:
        # time_key can be a TimeTruncKey too — same dep yield rule.
        inner = AggregateKey(source=ColumnKey(path=(), leaf="amount"), agg="sum")
        tt = TimeTruncKey(
            column=ColumnKey(path=(), leaf="created_at"),
            granularity="month",
        )
        tk = TransformKey(op="cumsum", input=inner, time_key=tt)
        deps = list(_iter_slot_deps(tk))
        assert tk in deps
        assert tt in deps

    def test_iter_slot_deps_multiple_partition_keys(self) -> None:
        # partition_keys is a frozenset; ordering is insertion-agnostic.
        # All keys in the set must surface as slot deps.
        inner = AggregateKey(source=ColumnKey(path=(), leaf="amount"), agg="sum")
        region = ColumnKey(path=(), leaf="region")
        customer = ColumnKey(path=(), leaf="customer_id")
        tk = TransformKey(
            op="cumsum",
            input=inner,
            partition_keys=frozenset({region, customer}),
        )
        deps = list(_iter_slot_deps(tk))
        assert region in deps
        assert customer in deps

    def test_partition_keys_materialize_as_hidden_slots(self) -> None:
        # End-to-end through plan_query: a cumsum measure with
        # partition_by=region produces a hidden ColumnKey slot for
        # region that the public projection does NOT include.
        q = SlayerQuery(
            source_model="orders",
            measures=[
                {"formula": "cumsum(amount:sum, partition_by=region)"},
            ],
            time_dimensions=[
                TimeDimension(
                    dimension=ColumnRef(name="created_at"),
                    granularity=TimeGranularity.MONTH,
                ),
            ],
        )
        planned = plan_query(query=q, bundle=_bundle())
        # All slots in the registry.
        all_slots = (
            planned.row_slots
            + planned.aggregate_slots
            + planned.combined_expression_slots
        )
        region_slots = [
            s for s in all_slots
            if isinstance(s.key, ColumnKey) and s.key.leaf == "region"
        ]
        assert len(region_slots) == 1, region_slots
        assert region_slots[0].hidden is True, region_slots[0]


# ---------------------------------------------------------------------------
# stage_planner populates transform_layers
# ---------------------------------------------------------------------------


class TestTransformLayersPopulation:
    def test_cumsum_emits_transform_layer(self) -> None:
        q = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "cumsum(amount:sum)"}],
            time_dimensions=[
                TimeDimension(
                    dimension=ColumnRef(name="created_at"),
                    granularity=TimeGranularity.MONTH,
                ),
            ],
        )
        planned = plan_query(query=q, bundle=_bundle())
        assert len(planned.transform_layers) == 1
        layer = planned.transform_layers[0]
        assert layer.op == "cumsum"
        assert len(layer.slot_ids) == 1

    def test_rank_emits_transform_layer(self) -> None:
        q = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "rank(amount:sum)"}],
        )
        planned = plan_query(query=q, bundle=_bundle())
        assert any(
            layer.op == "rank" for layer in planned.transform_layers
        )

    def test_time_shift_emits_transform_layer(self) -> None:
        q = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "time_shift(amount:sum, periods=-1)"}],
            time_dimensions=[
                TimeDimension(
                    dimension=ColumnRef(name="created_at"),
                    granularity=TimeGranularity.MONTH,
                ),
            ],
        )
        planned = plan_query(query=q, bundle=_bundle())
        ops = [layer.op for layer in planned.transform_layers]
        assert "time_shift" in ops

    def test_change_desugars_to_time_shift_layer(self) -> None:
        # ``change(amount:sum)`` lowers via desugar_change to
        # ``amount - time_shift(amount)``. After desugar, the typed
        # plan contains a time_shift TransformKey (not a change one);
        # transform_layers should show the materialised op only.
        q = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "change(amount:sum)"}],
            time_dimensions=[
                TimeDimension(
                    dimension=ColumnRef(name="created_at"),
                    granularity=TimeGranularity.MONTH,
                ),
            ],
        )
        planned = plan_query(query=q, bundle=_bundle())
        ops = [layer.op for layer in planned.transform_layers]
        assert "time_shift" in ops, ops
        assert "change" not in ops, ops

    def test_nested_transforms_emit_in_dependency_order(self) -> None:
        # cumsum(change(amount:sum)) — change desugars to a time_shift
        # TransformKey, which the outer cumsum then wraps. The inner
        # time_shift layer must appear before the outer cumsum layer.
        q = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "cumsum(change(amount:sum))"}],
            time_dimensions=[
                TimeDimension(
                    dimension=ColumnRef(name="created_at"),
                    granularity=TimeGranularity.MONTH,
                ),
            ],
        )
        planned = plan_query(query=q, bundle=_bundle())
        ops = [layer.op for layer in planned.transform_layers]
        # time_shift (inner, from change desugar) appears before cumsum
        # (outer).
        assert "time_shift" in ops
        assert "cumsum" in ops
        assert ops.index("time_shift") < ops.index("cumsum")
        assert "change" not in ops

    def test_each_transform_slot_emits_its_own_layer(self) -> None:
        # Two distinct transform slots (rank + cumsum) → two layers,
        # one per slot. No op-grouping collapse.
        q = SlayerQuery(
            source_model="orders",
            measures=[
                {"formula": "rank(amount:sum)"},
                {"formula": "cumsum(amount:sum)"},
            ],
            time_dimensions=[
                TimeDimension(
                    dimension=ColumnRef(name="created_at"),
                    granularity=TimeGranularity.MONTH,
                ),
            ],
        )
        planned = plan_query(query=q, bundle=_bundle())
        assert len(planned.transform_layers) == 2
        ops = {layer.op for layer in planned.transform_layers}
        assert ops == {"rank", "cumsum"}
        # Each layer references exactly one slot.
        for layer in planned.transform_layers:
            assert len(layer.slot_ids) == 1

    def test_no_transform_no_layers(self) -> None:
        q = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "amount:sum"}],
        )
        planned = plan_query(query=q, bundle=_bundle())
        assert planned.transform_layers == []


# ---------------------------------------------------------------------------
# Identity preservation (DEV-1446 territory): nested transforms reuse the
# inner aggregate slot.
# ---------------------------------------------------------------------------


class TestTransformInnerIdentity:
    def test_change_preserves_inner_aggregate_identity(self) -> None:
        # change(amount:sum) lowers to ``amount - time_shift(amount)``;
        # both occurrences of amount:sum must intern to the SAME slot
        # (DEV-1446).
        q = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "change(amount:sum)"}],
            time_dimensions=[
                TimeDimension(
                    dimension=ColumnRef(name="created_at"),
                    granularity=TimeGranularity.MONTH,
                ),
            ],
        )
        planned = plan_query(query=q, bundle=_bundle())
        agg_slots = [
            s for s in planned.aggregate_slots
            if isinstance(s.key, AggregateKey)
            and getattr(s.key.source, "leaf", None) == "amount"
            and s.key.agg == "sum"
        ]
        assert len(agg_slots) == 1, (
            f"expected one AggregateKey(amount, sum) slot, got {agg_slots}"
        )

    def test_change_and_explicit_amount_sum_share_one_aggregate_slot(self) -> None:
        # change(amount:sum) + amount:sum as a separate measure must
        # still intern the amount aggregate exactly once.
        q = SlayerQuery(
            source_model="orders",
            measures=[
                {"formula": "amount:sum"},
                {"formula": "change(amount:sum)"},
            ],
            time_dimensions=[
                TimeDimension(
                    dimension=ColumnRef(name="created_at"),
                    granularity=TimeGranularity.MONTH,
                ),
            ],
        )
        planned = plan_query(query=q, bundle=_bundle())
        agg_slots = [
            s for s in planned.aggregate_slots
            if isinstance(s.key, AggregateKey)
            and getattr(s.key.source, "leaf", None) == "amount"
            and s.key.agg == "sum"
        ]
        assert len(agg_slots) == 1, agg_slots
