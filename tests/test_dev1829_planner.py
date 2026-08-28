"""DEV-1829 — the local partitioned-measure join-back migrates onto the regroup
primitive's ``attach_phase="combined"`` arm.

Plan-level contracts (fail on the current tree, which still routes a local
partitioned measure through ``CrossModelAggregatePlan``):

* a local partitioned MEASURE yields ``RegroupAttachPlan(attach_phase="combined")``
  and NO ``CrossModelAggregatePlan``; its producer is a host-rooted single SELECT;
  the consumer keeps no partitioned aggregate slot (substituted to a placeholder);
* same-partition-set interning — N distinct aggregates share ONE producer;
* dispatch deferrals (DEV-1824): combined attach + a cross-model measure, and
  row+combined coexistence, both raise ``NotImplementedError``.
"""

from __future__ import annotations

import re

import pytest

from slayer.core.keys import REGROUP_LEAF_PREFIX, AggregateKey, ColumnKey
from slayer.core.query import ModelMeasure, SlayerQuery
from slayer.engine.source_bundle import ResolvedSourceBundle
from slayer.engine.stage_planner import plan_query

from tests._dev1739_fixtures import dev1739_models, gen, month_td


def _bundle() -> ResolvedSourceBundle:
    models = dev1739_models()
    return ResolvedSourceBundle(source_model=models[0], referenced_models=models[1:])


def _q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "orders")
    return SlayerQuery(**kw)


def _count_isolated_ctes(sql: str) -> int:
    return len({m.group(1) for m in re.finditer(r"(_cm_\w+)\s+AS\s*\(", sql)})


def _consumer_partition_agg_slots(planned) -> list:
    """Consumer aggregate slots that still carry a partition_by — must be empty
    once the measure is desugared into a combined regroup producer."""
    return [
        s for s in planned.aggregate_slots
        if isinstance(s.key, AggregateKey) and s.key.partition_keys is not None
    ]


class TestCombinedAttachReplacesCrossModelPlan:
    def test_local_partitioned_measure_yields_combined_attach(self) -> None:
        planned = plan_query(
            query=_q(
                dimensions=["region", "city"],
                measures=[ModelMeasure(
                    formula="amount:sum(partition_by=region)", name="region_rev",
                )],
            ),
            bundle=_bundle(),
        )
        assert planned.cross_model_aggregate_plans == []
        assert len(planned.regroup_attach_plans) == 1
        assert planned.regroup_attach_plans[0].attach_phase == "combined"
        # The partitioned aggregate no longer reaches the consumer aggregate loop.
        assert _consumer_partition_agg_slots(planned) == []

    def test_producer_is_host_rooted_single_select(self) -> None:
        planned = plan_query(
            query=_q(
                dimensions=["region", "city"],
                measures=[ModelMeasure(
                    formula="amount:sum(partition_by=region)", name="region_rev",
                )],
            ),
            bundle=_bundle(),
        )
        producer = planned.regroup_attach_plans[0].producer_plan
        # Rooted at the host, a single grouped SELECT — no nested isolation.
        assert producer.source_relation == "orders"
        assert producer.render_source_model is not None
        assert producer.render_source_model.name == "orders"
        assert producer.cross_model_aggregate_plans == []
        assert producer.regroup_attach_plans == []
        assert producer.windowed_aggregate_plans == []
        assert producer.ranked_aggregate_plans == []
        assert producer.transform_layers == []
        # One grouped aggregate output at the partition grain.
        assert len(producer.aggregate_slots) == 1

    def test_measure_slot_maps_to_placeholder_column_key(self) -> None:
        planned = plan_query(
            query=_q(
                dimensions=["region", "city"],
                measures=[ModelMeasure(
                    formula="amount:sum(partition_by=region)", name="region_rev",
                )],
            ),
            bundle=_bundle(),
        )
        rap = planned.regroup_attach_plans[0]
        assert len(rap.substitutions) == 1
        sub = rap.substitutions[0]
        # original_key is the partitioned aggregate...
        assert isinstance(sub.original_key, AggregateKey)
        assert sub.original_key.partition_keys == frozenset(
            {ColumnKey(path=(), leaf="region")}
        )
        # ...swapped for a reserved-leaf placeholder ColumnKey...
        assert isinstance(sub.placeholder, ColumnKey)
        assert sub.placeholder.path == ()
        assert sub.placeholder.leaf.startswith(REGROUP_LEAF_PREFIX)
        # ...that resolves to an aggregate slot inside the producer.
        producer_agg_ids = {s.id for s in rap.producer_plan.aggregate_slots}
        assert sub.producer_slot_id in producer_agg_ids

    def test_grand_total_producer_has_empty_join_pairs(self) -> None:
        planned = plan_query(
            query=_q(
                dimensions=["region", "city"],
                measures=[ModelMeasure(
                    formula="amount:sum(partition_by=[])", name="g",
                )],
            ),
            bundle=_bundle(),
        )
        assert planned.cross_model_aggregate_plans == []
        assert len(planned.regroup_attach_plans) == 1
        rap = planned.regroup_attach_plans[0]
        assert rap.attach_phase == "combined"
        assert rap.join_pairs == []  # grand total → single-row CROSS JOIN


class TestSamePartitionSetInterning:
    def test_two_distinct_aggregates_share_one_producer(self) -> None:
        planned = plan_query(
            query=_q(
                dimensions=["region", "city"],
                measures=[
                    ModelMeasure(formula="amount:sum(partition_by=region)", name="rsum"),
                    ModelMeasure(formula="amount:avg(partition_by=region)", name="ravg"),
                ],
            ),
            bundle=_bundle(),
        )
        assert planned.cross_model_aggregate_plans == []
        assert len(planned.regroup_attach_plans) == 1
        rap = planned.regroup_attach_plans[0]
        # One producer, two consumed aggregates, exactly two producer outputs.
        assert len(rap.substitutions) == 2
        assert len({s.producer_slot_id for s in rap.substitutions}) == 2
        assert len(rap.producer_plan.aggregate_slots) == 2

    async def test_two_distinct_aggregates_emit_one_cte(self) -> None:
        sql = await gen(_q(
            dimensions=["region", "city"],
            measures=[
                ModelMeasure(formula="amount:sum(partition_by=region)", name="rsum"),
                ModelMeasure(formula="amount:avg(partition_by=region)", name="ravg"),
            ],
        ))
        assert _count_isolated_ctes(sql) == 1

    def test_same_aggregate_two_names_shares_one_output(self) -> None:
        planned = plan_query(
            query=_q(
                dimensions=["region", "city"],
                measures=[
                    ModelMeasure(formula="amount:sum(partition_by=region)", name="a"),
                    ModelMeasure(formula="amount:sum(partition_by=region)", name="b"),
                ],
            ),
            bundle=_bundle(),
        )
        assert planned.cross_model_aggregate_plans == []
        assert len(planned.regroup_attach_plans) == 1
        rap = planned.regroup_attach_plans[0]
        # One structural aggregate → one producer output, one substitution.
        assert len(rap.substitutions) == 1
        assert len(rap.producer_plan.aggregate_slots) == 1

    async def test_same_aggregate_two_names_projects_both(self) -> None:
        sql = await gen(_q(
            dimensions=["region", "city"],
            measures=[
                ModelMeasure(formula="amount:sum(partition_by=region)", name="a"),
                ModelMeasure(formula="amount:sum(partition_by=region)", name="b"),
            ],
        ))
        assert _count_isolated_ctes(sql) == 1
        assert '"orders.a"' in sql
        assert '"orders.b"' in sql

    def test_distinct_partition_sets_produce_separate_attaches(self) -> None:
        # D3 grouping is by partition set: two DIFFERENT sets must NOT collapse
        # into one producer (a global-intern bug would pass the same-set tests).
        planned = plan_query(
            query=_q(
                dimensions=["region", "channel", "city"],
                measures=[
                    ModelMeasure(formula="amount:sum(partition_by=region)", name="rr"),
                    ModelMeasure(
                        formula="amount:sum(partition_by=[region, channel])", name="rc",
                    ),
                ],
            ),
            bundle=_bundle(),
        )
        assert planned.cross_model_aggregate_plans == []
        assert len(planned.regroup_attach_plans) == 2
        displays = {tuple(rap.partition_display) for rap in planned.regroup_attach_plans}
        assert len(displays) == 2  # distinct grains, distinct producers


class TestDispatchDeferrals:
    async def test_combined_attach_plus_cross_model_measure_raises(self) -> None:
        # Combined regroup attach (local partition) + a genuine cross-model
        # measure (its own _cm_ CTE) — composition deferred to DEV-1824.
        q = _q(
            dimensions=["region", "city"],
            measures=[
                ModelMeasure(formula="amount:sum(partition_by=region)", name="region_rev"),
                ModelMeasure(formula="customers.spend:sum", name="cm"),
            ],
        )
        with pytest.raises(NotImplementedError, match=r"DEV-1824"):
            await gen(q)

    async def test_combined_attach_plus_windowed_measure_raises(self) -> None:
        q = _q(
            dimensions=["region"], time_dimensions=month_td(),
            measures=[
                ModelMeasure(formula="amount:sum(partition_by=region)", name="region_rev"),
                ModelMeasure(formula="amount:sum(window='90d')", name="w"),
            ],
        )
        with pytest.raises(NotImplementedError, match=r"DEV-1824"):
            await gen(q)

    async def test_combined_attach_plus_ranked_measure_raises(self) -> None:
        q = _q(
            dimensions=["region", "city"], time_dimensions=month_td(),
            measures=[
                ModelMeasure(formula="amount:sum(partition_by=region)", name="region_rev"),
                ModelMeasure(formula="amount:last", name="lst"),
            ],
        )
        with pytest.raises(NotImplementedError, match=r"DEV-1824"):
            await gen(q)

    async def test_combined_attach_plus_transform_measure_raises(self) -> None:
        q = _q(
            dimensions=["region"], time_dimensions=month_td(),
            measures=[
                ModelMeasure(formula="amount:sum(partition_by=region)", name="region_rev"),
                ModelMeasure(formula="cumsum(amount:sum)", name="cs"),
            ],
        )
        with pytest.raises(NotImplementedError, match=r"DEV-1824"):
            await gen(q)

    async def test_row_and_combined_attach_coexistence_raises(self) -> None:
        # A computed dimension over one partition set (row attach) plus a
        # partitioned MEASURE over another (combined attach) — coexistence
        # of both phases in one query is deferred to DEV-1824.
        q = _q(
            dimensions=[
                "region", "city",
                {"expression": "CASE WHEN amount:sum(partition_by=city) > 5000 "
                               "THEN 1 ELSE 0 END", "name": "band"},
            ],
            measures=[ModelMeasure(
                formula="amount:sum(partition_by=region)", name="region_rev",
            )],
        )
        with pytest.raises(NotImplementedError, match=r"DEV-1824"):
            await gen(q)
