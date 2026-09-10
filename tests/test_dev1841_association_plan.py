"""DEV-1841 task 3.1 / 1.4 — the association ``ProducerKernel`` variant and its
planning: implicit grain, explicit ``partition_by=``, filter pushdown, and mode
threading into a nested (computed-dimension) producer. Spec: attribution-modes ›
Distinct-entity association; cross-model-aggregates › Producer filter routing.
"""

from __future__ import annotations

from slayer.engine.stage_planner import plan_query

from tests._dev1840_fixtures import bundle, dev1840_models
from tests._dev1841_fixtures import ModelMeasure, assoc_q

CM = ModelMeasure(formula="customers.spend:sum", name="cm")


def _association_attaches(planned) -> list:
    return [a for a in planned.regroup_attach_plans
            if getattr(a.kernel, "kind", None) == "association"]


class TestAssociationKernel:
    def test_implicit_grain_builds_association_kernel(self) -> None:
        """An unattributable slice under associate synthesizes an association
        producer, not the default plain/broadcast one."""
        planned = plan_query(
            query=assoc_q(dimensions=["status"], measures=[CM]),
            bundle=bundle())
        assert len(_association_attaches(planned)) == 1

    def test_explicit_partition_by_builds_association_kernel(self) -> None:
        """An unattributable explicit partition key attributes under associate."""
        planned = plan_query(
            query=assoc_q(dimensions=["status"], measures=[ModelMeasure(
                formula="customers.spend:sum(partition_by=status)", name="cm")]),
            bundle=bundle())
        assert len(_association_attaches(planned)) == 1

    def test_pushdown_reaches_the_association_producer(self) -> None:
        """An unsafe-but-reachable conjunct pushes into the association
        producer's plan as a semi-join."""
        planned = plan_query(
            query=assoc_q(dimensions=["status"], measures=[CM],
                          filters=["customers.plans.level = 'basic'"]),
            bundle=bundle(dev1840_models(strong_plans=False)))
        (att,) = _association_attaches(planned)
        assert list(att.producer_plan.semi_join_filters)


class TestModeThreadsIntoNestedProducer:
    def test_nested_computed_dimension_resolves_under_associate(self) -> None:
        """Task 1.4 — the mode reaches a recursively-planned nested producer:
        an unattributable computed-dimension aggregate resolves via the kernel."""
        band = {
            "expression": (
                "CASE WHEN customers.spend:sum(partition_by=status) > 400 "
                "THEN 'hi' ELSE 'lo' END"),
            "name": "band",
        }
        planned = plan_query(
            query=assoc_q(dimensions=[band]), bundle=bundle())
        assert _association_attaches(planned)
