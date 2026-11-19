"""DEV-1871 G15 — the D5 broadcast-coherence assert: a grain-differing combine
compiled without the environment's Broadcast insertions is an elaboration bug."""

from __future__ import annotations

import pytest

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, SlayerDataset
from slayer.core.query import QueryDimension, QueryMetric, SlayerQuery
from slayer.engine import elaborate, plan
from slayer.ir.planned import PlannedQuery
from slayer.ir.source_bundle import ResolvedSourceBundle


def _bundle() -> ResolvedSourceBundle:
    return ResolvedSourceBundle(dialect="postgres", population=SlayerDataset(
        name="orders", data_source="coh_ds", sql_table="orders",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="status", datatype=DataType.STRING),
            DatasetField(name="tier", datatype=DataType.STRING),
            DatasetField(name="amount", datatype=DataType.FLOAT),
        ],
    ))


def _query(expression: str) -> SlayerQuery:
    return SlayerQuery(
        population="orders",
        dimensions=[QueryDimension(field="orders.status"), QueryDimension(field="orders.tier")],
        metrics=[QueryMetric(metric=expression, alias="mix")],
    )


_TWO_ATTACHED = "sum(amount, partition_by=status) + sum(amount, partition_by=tier)"
_INLINE_PLUS_ATTACHED = "sum(amount) + sum(amount, partition_by=status)"


@pytest.mark.parametrize("formula", [_TWO_ATTACHED, _INLINE_PLUS_ATTACHED])
def test_mixed_grain_combine_plans_under_the_assert(formula) -> None:
    planned = plan.plan_query(query=_query(formula), bundle=_bundle())
    assert isinstance(planned, PlannedQuery)


@pytest.mark.parametrize("formula", [_TWO_ATTACHED, _INLINE_PLUS_ATTACHED])
def test_assert_fires_when_elaboration_drops_broadcasts(monkeypatch: pytest.MonkeyPatch, formula) -> None:
    real = elaborate.build_environment

    def stripped(**kwargs):
        env = real(**kwargs)
        return env.model_copy(update={"metrics": tuple(
            e.model_copy(update={"broadcasts": ()}) for e in env.metrics
        )})

    monkeypatch.setattr(elaborate, "build_environment", stripped)
    query = _query(formula)
    bundle = _bundle()
    with pytest.raises(AssertionError, match="broadcast-coherence"):
        plan.plan_query(query=query, bundle=bundle)
