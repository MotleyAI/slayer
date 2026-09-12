"""DEV-1871 G15 — the D5 broadcast-coherence assert: a grain-differing combine
compiled without the environment's Broadcast insertions is an elaboration bug."""

from __future__ import annotations

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, ModelMeasure, SlayerModel
from slayer.core.query import ColumnRef, SlayerQuery
from slayer.engine import elaborate, plan
from slayer.ir.planned import PlannedQuery
from slayer.ir.source_bundle import ResolvedSourceBundle


def _bundle() -> ResolvedSourceBundle:
    return ResolvedSourceBundle(source_model=SlayerModel(
        name="orders", data_source="coh_ds", sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="status", type=DataType.TEXT),
            Column(name="tier", type=DataType.TEXT),
            Column(name="amount", type=DataType.DOUBLE),
        ],
    ))


def _query(formula: str) -> SlayerQuery:
    return SlayerQuery(
        source_model="orders",
        dimensions=[ColumnRef(name="status"), ColumnRef(name="tier")],
        measures=[ModelMeasure(formula=formula, name="mix")],
    )


_TWO_ATTACHED = "amount:sum(partition_by=status) + amount:sum(partition_by=tier)"
_INLINE_PLUS_ATTACHED = "amount:sum + amount:sum(partition_by=status)"


@pytest.mark.parametrize("formula", [_TWO_ATTACHED, _INLINE_PLUS_ATTACHED])
def test_mixed_grain_combine_plans_under_the_assert(formula) -> None:
    planned = plan.plan_query(query=_query(formula), bundle=_bundle())
    assert isinstance(planned, PlannedQuery)


@pytest.mark.parametrize("formula", [_TWO_ATTACHED, _INLINE_PLUS_ATTACHED])
def test_assert_fires_when_elaboration_drops_broadcasts(monkeypatch, formula) -> None:
    real = elaborate.build_environment

    def stripped(**kwargs):
        env = real(**kwargs)
        return env.model_copy(update={"measures": tuple(
            e.model_copy(update={"broadcasts": ()}) for e in env.measures
        )})

    monkeypatch.setattr(elaborate, "build_environment", stripped)
    with pytest.raises(AssertionError, match="broadcast-coherence"):
        plan.plan_query(query=_query(formula), bundle=_bundle())
