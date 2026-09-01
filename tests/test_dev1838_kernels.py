"""DEV-1838 stage 2 — typed producer kernels on ``RegroupAttachPlan`` (D4).

Unit pins for kernel synthesis: which attach carries which kernel, the
trailing-window field mapping (ex-``WindowedAggregatePlan`` homes per the
task-2.0 inventory), kernel participation in the D3 interning identity, and
the ranking-key precedence pins ported from the retired
``test_dev1748_ranked_plan`` (task 0.2 dispositions).
"""

from __future__ import annotations

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.keys import ColumnKey
from slayer.core.models import Column, SlayerModel
from slayer.core.query import ColumnRef, SlayerQuery, TimeDimension
from slayer.engine import stage_planner
from slayer.engine.planned import (
    PlainProducerKernel,
    RankedProducerKernel,
    RegroupAttachPlan,
    TrailingWindowProducerKernel,
)
from slayer.engine.source_bundle import ResolvedSourceBundle

from tests._dev1748_fixtures import dev1748_bundle
from tests._dev1838_fixtures import (
    BAND,
    ModelMeasure,
    dev1838_models,
    month_td,
    q,
)

M = ModelMeasure(formula="amount:sum", name="m")


def _bundle() -> ResolvedSourceBundle:
    models = dev1838_models()
    return ResolvedSourceBundle(
        source_model=models[0], referenced_models=models[1:],
    )


def _plan(query):
    return stage_planner.plan_query(query=query, bundle=_bundle())


def _combined_attaches(planned):
    return [
        a for a in planned.regroup_attach_plans if a.attach_phase == "combined"
    ]


class TestKernelSynthesis:
    def test_bare_windowed_attach_carries_trailing_window_kernel(self) -> None:
        planned = _plan(q(
            dimensions=["region"], time_dimensions=month_td(),
            measures=[ModelMeasure(formula="amount:sum(window='90d')", name="w")],
        ))
        (attach,) = _combined_attaches(planned)
        kern = attach.kernel
        assert isinstance(kern, TrailingWindowProducerKernel)
        assert kern.window_raw == "90d"
        assert kern.window_parts == [(90, "d")]
        assert kern.window_granularity == "month"
        producer = attach.producer_plan
        assert kern.bucket_slot_id == producer.active_time_dimension_slot_id
        bucket = next(s for s in producer.row_slots if s.id == kern.bucket_slot_id)
        assert bucket.key.granularity == "month"
        assert kern.src_where_filter_ids == []
        assert kern.src_filter_rewrites == []

    def test_plain_partitioned_attaches_default_to_plain_kernel(self) -> None:
        planned = _plan(q(
            dimensions=["status", BAND],
            measures=[M, ModelMeasure(
                formula="amount:sum(partition_by=status)", name="rm")],
        ))
        assert planned.regroup_attach_plans
        for attach in planned.regroup_attach_plans:
            assert isinstance(attach.kernel, PlainProducerKernel), attach.alias_hint

    def test_windowed_kernel_inherits_src_filters_minus_frame_bounds(self) -> None:
        """A wholly-frame-bound filter is excluded from the ``_src`` ids; a
        partly-frame-bound one keeps its id and gains the population residual."""
        planned = _plan(q(
            dimensions=["region"], time_dimensions=month_td(),
            measures=[ModelMeasure(formula="amount:sum(window='90d')", name="w")],
            filters=["ordered_at >= '2024-02-01' and status = 'ok'"],
        ))
        (attach,) = _combined_attaches(planned)
        kern = attach.kernel
        assert isinstance(kern, TrailingWindowProducerKernel)
        assert len(kern.src_where_filter_ids) == 1
        (rewrite,) = kern.src_filter_rewrites
        assert rewrite.filter_id in kern.src_where_filter_ids

    def test_wholly_frame_bound_filter_is_excluded_from_src(self) -> None:
        planned = _plan(q(
            dimensions=["region"], time_dimensions=month_td(),
            measures=[ModelMeasure(formula="amount:sum(window='90d')", name="w")],
            filters=["ordered_at >= '2024-02-01'"],
        ))
        (attach,) = _combined_attaches(planned)
        assert attach.kernel.src_where_filter_ids == []
        assert attach.kernel.src_filter_rewrites == []

    def test_cross_model_windowed_attach_carries_trailing_window_kernel(self) -> None:
        planned = _plan(q(
            dimensions=["customers.tier"],
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="customers.signup_at"),
                granularity=TimeGranularity.MONTH,
            )],
            measures=[ModelMeasure(
                formula="customers.spend:sum(window='1y')", name="w")],
        ))
        (attach,) = _combined_attaches(planned)
        kern = attach.kernel
        assert isinstance(kern, TrailingWindowProducerKernel)
        assert attach.producer_root_model == "customers"
        assert kern.window_parts == [(1, "y")]
        assert kern.bucket_slot_id == (
            attach.producer_plan.active_time_dimension_slot_id
        )


class TestRankedKernelSynthesis:
    def test_bare_last_attach_carries_ranked_kernel(self) -> None:
        planned = _plan(q(
            dimensions=["status"],
            measures=[ModelMeasure(formula="amount:last", name="lb")],
        ))
        (attach,) = _combined_attaches(planned)
        kern = attach.kernel
        assert isinstance(kern, RankedProducerKernel)
        assert kern.agg == "last"
        # No temporal dimension in the grain — the model default wins.
        assert kern.ranking_time_key == ColumnKey(path=(), leaf="ordered_at")

    def test_explicit_ranking_arg_lands_on_the_kernel(self) -> None:
        planned = _plan(q(
            dimensions=["status"],
            measures=[ModelMeasure(
                formula="amount:last(customers.signup_at)", name="ls")],
        ))
        (attach,) = _combined_attaches(planned)
        kern = attach.kernel
        assert isinstance(kern, RankedProducerKernel)
        assert kern.ranking_time_key == ColumnKey(
            path=("customers",), leaf="signup_at",
        )

    def test_cross_model_last_attach_carries_ranked_kernel(self) -> None:
        planned = _plan(q(
            dimensions=["customers.tier"],
            measures=[ModelMeasure(formula="customers.spend:last", name="sl")],
        ))
        (attach,) = _combined_attaches(planned)
        kern = attach.kernel
        assert isinstance(kern, RankedProducerKernel)
        assert attach.producer_root_model == "customers"
        # Producer-scope coordinates: the target's own default TD, path-free.
        assert kern.ranking_time_key == ColumnKey(path=(), leaf="signup_at")


def _plan48(query):
    return stage_planner.plan_query(query=query, bundle=dev1748_bundle())


def _q48(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "orders")
    return SlayerQuery(**kw)


def _ranked_kernel_of(planned) -> RankedProducerKernel:
    (attach,) = [
        a for a in planned.regroup_attach_plans
        if isinstance(a.kernel, RankedProducerKernel)
    ]
    return attach.kernel


class TestRankingKeyPrecedence:
    """Ported from ``test_dev1748_ranked_plan`` (retired): the per-scope
    ranking-column precedence, now pinned on the attach kernel."""

    def test_the_kernel_carries_an_explicit_ranking_time_key(self) -> None:
        planned = _plan48(_q48(
            dimensions=["status"],
            measures=[{"formula": "amount:last(shipped_at)", "name": "l"}],
        ))
        assert _ranked_kernel_of(planned).ranking_time_key.leaf == "shipped_at"

    def test_a_temporal_dimension_outranks_the_model_default(self) -> None:
        planned = _plan48(_q48(
            dimensions=["shipped_at"],
            measures=[{"formula": "amount:last", "name": "l"}],
        ))
        assert _ranked_kernel_of(planned).ranking_time_key.leaf == "shipped_at"

    def test_a_time_dimension_supplies_its_raw_column(self) -> None:
        """Never the truncated bucket — ranking within a month by the month
        ties every row in it."""
        planned = _plan48(_q48(
            time_dimensions=[{"dimension": "shipped_at", "granularity": "month"}],
            measures=[{"formula": "amount:last", "name": "l"}],
        ))
        assert _ranked_kernel_of(planned).ranking_time_key.leaf == "shipped_at"

    def test_the_model_default_is_the_last_resort(self) -> None:
        planned = _plan48(_q48(
            dimensions=["status"],
            measures=[{"formula": "amount:last", "name": "l"}],
        ))
        assert _ranked_kernel_of(planned).ranking_time_key.leaf == "created_at"

    def test_no_resolvable_ranking_column_raises_the_host_message(self) -> None:
        model = SlayerModel(
            name="untimed", sql_table="untimed", data_source="test",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="status", type=DataType.TEXT),
                Column(name="amount", type=DataType.DOUBLE),
            ],
        )
        bundle = ResolvedSourceBundle(source_model=model, referenced_models=[])
        query = SlayerQuery(
            source_model="untimed", dimensions=["status"],
            measures=[{"formula": "amount:last", "name": "l"}],
        )
        with pytest.raises(ValueError) as excinfo:
            stage_planner.plan_query(query=query, bundle=bundle)
        assert str(excinfo.value) == (
            "first/last aggregation requires a ranking time column "
            "(a time_dimension, a DATE/TIMESTAMP dimension, or the "
            "model's default_time_dimension); none is resolvable for "
            "model 'untimed'."
        )

    def test_a_target_rooted_kernel_ignores_the_host_time_dimension(self) -> None:
        """Precedence is per-scope: a cross-model ranked kernel ranks TARGET
        rows, so a host time dimension is never a candidate."""
        planned = _plan48(_q48(
            time_dimensions=[{"dimension": "created_at", "granularity": "month"}],
            measures=[{"formula": "customers.spend:last", "name": "l"}],
        ))
        (attach,) = [
            a for a in planned.regroup_attach_plans
            if a.producer_root_model == "customers"
        ]
        assert isinstance(attach.kernel, RankedProducerKernel)
        assert attach.kernel.ranking_time_key.leaf == "signup_at"

    def test_a_host_column_as_a_target_ranking_key_is_rejected(self) -> None:
        query = _q48(measures=[
            {"formula": "customers.spend:last(created_at)", "name": "l"},
        ])
        with pytest.raises(ValueError) as excinfo:
            _plan48(query)
        message = str(excinfo.value)
        assert "created_at" in message
        assert "customers" in message
        assert "not attributable from" in message


class TestRankedKernelGrain:
    """Ported from ``test_dev1748_ranked_plan``: the attach join grain IS the
    partition, structurally."""

    def test_one_join_pair_per_query_dimension(self) -> None:
        planned = _plan48(_q48(
            dimensions=["status"],
            time_dimensions=[{"dimension": "created_at", "granularity": "month"}],
            measures=[{"formula": "amount:last", "name": "l"}],
        ))
        (attach,) = planned.regroup_attach_plans
        assert isinstance(attach.kernel, RankedProducerKernel)
        assert len(attach.join_pairs) == 2
        producer_row_ids = {s.id for s in attach.producer_plan.row_slots}
        assert {sid for _, sid in attach.join_pairs} <= producer_row_ids

    def test_a_scalar_ranked_kernel_has_an_empty_grain(self) -> None:
        planned = _plan48(_q48(
            measures=[{"formula": "amount:last", "name": "l"}],
        ))
        (attach,) = planned.regroup_attach_plans
        assert isinstance(attach.kernel, RankedProducerKernel)
        assert attach.join_pairs == []

    def test_a_measure_filter_is_carried_on_the_producer_slot(self) -> None:
        planned = _plan48(_q48(
            dimensions=["status"],
            measures=[{"formula": "big_amount:last", "name": "l"}],
        ))
        (attach,) = planned.regroup_attach_plans
        (agg_slot,) = attach.producer_plan.aggregate_slots
        assert agg_slot.key.column_filter_key is not None

    def test_an_order_only_ranked_measure_stays_hidden(self) -> None:
        planned = _plan48(_q48(
            dimensions=["status"],
            measures=[{"formula": "amount:sum", "name": "s"}],
            order=[{"column": "amount:last", "direction": "desc"}],
        ))
        (attach,) = planned.regroup_attach_plans
        assert isinstance(attach.kernel, RankedProducerKernel)
        (sub,) = attach.substitutions
        ph = next(
            s for s in planned.row_slots if s.key == sub.placeholder
        )
        assert ph.hidden is True
        assert ph.public_name is None
        assert ph.id not in planned.projection
        assert any(e.slot_id == ph.id for e in planned.order)

    def test_a_rerooted_cross_model_last_is_a_target_rooted_kernel(self) -> None:
        planned = _plan48(_q48(
            dimensions=["customers.regions.name"],
            measures=[{"formula": "customers.spend:last", "name": "l"}],
        ))
        (attach,) = [
            a for a in planned.regroup_attach_plans
            if a.producer_root_model == "customers"
        ]
        assert isinstance(attach.kernel, RankedProducerKernel)


class TestKernelIdentity:
    def test_kernel_differences_separate_interning_identities(self) -> None:
        def windowed_attach(window: str):
            planned = _plan(q(
                dimensions=["region"], time_dimensions=month_td(),
                measures=[ModelMeasure(
                    formula=f"amount:sum(window='{window}')", name="w")],
            ))
            (attach,) = _combined_attaches(planned)
            return attach

        a, b = windowed_attach("90d"), windowed_attach("45d")
        assert (
            stage_planner.regroup_producer_identity(a)
            != stage_planner.regroup_producer_identity(b)
        )


class TestKernelModel:
    def test_default_kernel_is_plain(self) -> None:
        planned = _plan(q(dimensions=["status"], measures=[M, ModelMeasure(
            formula="amount:sum(partition_by=status)", name="rm")]))
        (attach,) = _combined_attaches(planned)
        assert isinstance(attach.kernel, PlainProducerKernel)

    def test_trailing_window_kernel_round_trips(self) -> None:
        kern = TrailingWindowProducerKernel(
            window_raw="90d", window_parts=[(90, "d")],
            window_granularity="month", bucket_slot_id="s1",
        )
        payload = {"producer_plan": {"source_relation": "orders"},
                   "alias_hint": "w", "kernel": kern.model_dump()}
        clone = RegroupAttachPlan.model_validate(payload)
        assert isinstance(clone.kernel, TrailingWindowProducerKernel)
        assert clone.kernel.window_parts == [(90, "d")]


class TestCrossingInputPathsUnionFilterAndStructural:
    """DEV-1783 item 6, re-homed from the retired isolation classifier —
    ``_local_crossing_input_paths`` must UNION a local aggregate's
    ``Column.filter`` crossings with its structural input crossings (source
    ``Column.sql`` / args / kwargs). Reporting the filter paths alone hides a
    crossing kwarg from the desugar and lets a fan-multiplying aggregate
    inline."""

    def test_filter_and_kwarg_crossings_are_both_reported(self) -> None:
        from slayer.core.keys import AggregateKey, SqlExprKey

        key = AggregateKey(
            agg="sum",
            source=ColumnKey(path=(), leaf="amount"),
            kwargs=(("weight", ColumnKey(path=("customers",), leaf="spend")),),
            column_filter_key=SqlExprKey(
                canonical_sql="customers__regions.name = 'X'",
                referenced_join_paths=(("customers", "regions"),),
            ),
        )
        bundle = _bundle()
        paths = stage_planner._local_crossing_input_paths(
            key=key, bundle=bundle, host_model=bundle.source_model,
        )
        assert ("customers", "regions") in paths, paths  # column_filter_key
        assert ("customers",) in paths, paths            # kwarg — dropped pre-fix
        # Order-stable + de-duplicated: filter paths first, then structural.
        assert paths == [("customers", "regions"), ("customers",)], paths
