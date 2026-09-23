"""DEV-1903 D2 — attached inputs are opaque to discovery: a local aggregate whose
attached input's inner crosses a join compiles inline, the input row-attached
through its own producer. Value-identical to the host-rooted plan it replaces.
Executed on SQLite + DuckDB (DEV-1840 dataset)."""

from __future__ import annotations

from typing import List

import pytest

from slayer.core.enums import DataType
from slayer.core.keys import AggregateKey, TransformKey
from slayer.core.models import Column, SlayerModel
from slayer.engine.plan import plan_query

from tests._dev1840_fixtures import bundle, dev1840_models, make_exec_engine
from tests._dev1841_fixtures import ModelMeasure
from tests._dev1900_fixtures import unparseable_derived_models
from tests._dev1919_fixtures import (
    HEADLINE,
    UNPARSE_PARAM,
    assert_cells,
    mode_q,
    status_vals,
)

# Region spend totals (distinct customers): North 280, South 195, NULL-region 40;
# the orphan order's NULL name meets the NULL cell. rank desc: North 1, South 2, NULL 3.
_REGION_SPEND = "sum(customers.spend, partition_by=customers.regions.name)"
SOURCE = f"sum(amount * {_REGION_SPEND})"
KWARG = f"amount:weighted_avg(weight={_REGION_SPEND})"
TRANSFORM = f"amount:weighted_avg(weight=rank({_REGION_SPEND}))"
#: host column masked to North customers: its own closure crosses to regions.
NORTH_OUTER = f"sum(north_amount * {_REGION_SPEND})"
NORTH_INNER = "amount:weighted_avg(weight=sum(north_amount, partition_by=channel))"

CASES = [
    # ok: 10·280 + 30·280 + 5·195 + 15·195 + 7·40 + 12·280 + 3·280 = 19580
    # new: 20·280 + 25·280 + 40·40 = 14200
    pytest.param(SOURCE, "sum", AggregateKey, {"ok": 19580.0, "new": 14200.0},
                 id="source"),
    # weights ok 1550, new 600 over the same numerators
    pytest.param(KWARG, "weighted_avg", AggregateKey,
                 {"ok": 19580.0 / 1550.0, "new": 14200.0 / 600.0}, id="kwarg"),
    # ok: (10+30+5·2+15·2+7·3+12+3) / (1+1+2+2+3+1+1) = 116/11; new: (20+25+40·3)/5
    pytest.param(TRANSFORM, "weighted_avg", TransformKey,
                 {"ok": 116.0 / 11.0, "new": 33.0}, id="transform"),
]


def _north_models() -> List[SlayerModel]:
    models = dev1840_models()
    models[0].columns.append(Column(
        name="north_amount", type=DataType.DOUBLE, sql="amount",
        filter="customers.regions.name = 'North'"))
    return models


@pytest.fixture(params=["sqlite", "duckdb"])
async def orders_engine(request):
    async for engine in make_exec_engine(request):
        yield engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def north_engine(request):
    async for engine in make_exec_engine(request, models=_north_models()):
        yield engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def unparse_engine(request):
    async for engine in make_exec_engine(request, models=unparseable_derived_models()):
        yield engine


def _q(formula: str, mode=None):
    return mode_q(mode, dimensions=["status"],
                  measures=[ModelMeasure(formula=formula, name="w")])


def _plan(formula: str, models=None, mode=None):
    return plan_query(query=_q(formula, mode), bundle=bundle(models))


def _host_rooted_wrapping(planned, agg: str) -> list:
    return [
        a for a in planned.regroup_attach_plans
        if a.attach_phase == "combined" and a.producer_root_model is None
        and any(isinstance(s.original_key, AggregateKey) and s.original_key.agg == agg
                for s in a.substitutions)
    ]


def _assert_inline_with_row_attach(planned, *, agg: str, input_kind: type) -> None:
    assert [s for s in planned.aggregate_slots
            if isinstance(s.key, AggregateKey) and s.key.agg == agg], (
        "the outer aggregate must be an inline slot of the top plan")
    assert not _host_rooted_wrapping(planned, agg)
    row = [a for a in planned.regroup_attach_plans if a.attach_phase == "row"
           and any(isinstance(s.original_key, input_kind) for s in a.substitutions)]
    assert len(row) == 1, planned.regroup_attach_plans


class TestCrossingAttachedInputInline:
    @pytest.mark.parametrize("formula,agg,input_kind,_expected", CASES)
    def test_plan_shape(self, formula, agg, input_kind, _expected):
        _assert_inline_with_row_attach(_plan(formula), agg=agg, input_kind=input_kind)

    def test_cross_model_input_attaches_at_its_home(self):
        [row] = [a for a in _plan(KWARG).regroup_attach_plans if a.attach_phase == "row"]
        assert row.producer_root_model == "customers"

    @pytest.mark.parametrize("formula,_agg,_kind,expected", CASES)
    async def test_values(self, orders_engine, formula, _agg, _kind, expected):
        assert_cells(status_vals(await orders_engine.execute(_q(formula))), expected)


class TestOwnClosureStillJudged:
    def test_outer_own_filter_crossing_keeps_host_rooted_producer(self):
        """A ``Column.filter`` riding the outer's own ``ColumnSqlKey`` source still
        crosses: opacity covers attached inputs only."""
        planned = _plan(NORTH_OUTER, models=_north_models())
        assert len(_host_rooted_wrapping(planned, "sum")) == 1

    async def test_outer_own_filter_crossing_values(self, north_engine):
        # North orders only — ok: 10·280 + 30·280 + 12·280 + 3·280; new: 20·280 + 25·280
        resp = await north_engine.execute(_q(NORTH_OUTER))
        assert_cells(status_vals(resp), {"ok": 15400.0, "new": 12600.0})

    def test_input_own_filter_crossing_is_the_inputs_business(self):
        _assert_inline_with_row_attach(
            _plan(NORTH_INNER, models=_north_models()),
            agg="weighted_avg", input_kind=AggregateKey)

    async def test_input_own_filter_crossing_values(self, north_engine):
        # weight = North amount by channel: web 55, app 45
        # ok: (10·55 + 30·55 + 5·45 + 15·45 + 7·55 + 12·55 + 3·55) / 365
        # new: (20·45 + 25·45 + 40·55) / 145
        resp = await north_engine.execute(_q(NORTH_INNER))
        assert_cells(status_vals(resp), {"ok": 4310.0 / 365.0, "new": 4225.0 / 145.0})

    @pytest.mark.parametrize("formula", [
        "amount:weighted_avg(weight=sum(customers.flagged_spend, "
        "partition_by=customers.regions.name))",
        "sum(amount * sum(customers.flagged_spend, partition_by=customers.regions.name))",
    ], ids=["kwarg", "source"])
    async def test_unanalysable_filter_inside_input_fails_closed(
        self, unparse_engine, formula,
    ):
        with pytest.raises(ValueError, match="no supported dialect can analyse"):
            await unparse_engine.execute(_q(formula))


class TestHostLocusWrap:
    def test_association_wrap_has_no_second_producer(self):
        """The host-locus aggregate compiles inline in the association producer;
        only its parameter nests."""
        [attach] = _plan(HEADLINE, mode="associate").regroup_attach_plans
        assert attach.kernel.kind == "association"
        sub = attach.producer_plan
        assert [s for s in sub.aggregate_slots
                if isinstance(s.key, AggregateKey) and s.key.locus == "host"]
        assert [a.attach_phase for a in sub.regroup_attach_plans] == ["row"]

    def test_association_wrap_safety_check_fires(self):
        with pytest.raises(ValueError, match="no supported dialect can analyse"):
            _plan(UNPARSE_PARAM, models=unparseable_derived_models(), mode="associate")
