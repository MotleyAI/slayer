"""Axiom 11.2: a rank-family transform's own ``partition_by=`` keys must be
members of its operand grain, in every position; repeated keyword arguments die
in the parser (spec: queries/transforms › Rank-family partition keys are
operand-grain members; aggregations/functional-form › Repeated keyword arguments
are rejected)."""

from __future__ import annotations

from typing import Dict, Tuple

import pytest

from slayer.engine.bind_inputs import bind_query_inputs
from slayer.engine.syntax import parse_expr
from slayer.ir.source_bundle import ResolvedSourceBundle

from tests._dev1840_fixtures import bundle as orders_bundle, q as orders_q
from tests._dev1847_fixtures import (
    ModelMeasure,
    _SALES_ROWS_WIDE,
    dev1847_models,
    make_exec_engine,
    rows_by,
    sales_q,
)
from tests._dev1953_fixtures import (
    AMOUNT,
    BAND,
    UREG,
    band_of,
    cell_totals,
    rank_within,
)
from tests._dev1919_fixtures import ordered_month_td

CRP = ["city", "region", "product"]
NONMEMBER = "rank(sum(amount, partition_by=[city, region]), partition_by=product)"
MEMBER = "rank(sum(amount, partition_by=[city, region]), partition_by=region)"
UNGRAINED = "rank(sum(amount), partition_by=region)"
LAST_INNER = "last(sum(amount, partition_by=[customers.regions.name, ordered_at]))"

MEMBER_RANKS = {
    ("East", "Zeta"): 1, ("East", "Delta"): 2, ("East", "Epsilon"): 2,
    ("North", "Beta"): 1, ("North", "Alpha"): 2,
    ("South", "Gamma"): 1, ("South", "Alpha"): 2,
    ("Gap", None): 1, ("Gap", "Kappa"): 2,
    ("Void", "Xi"): 1,
}
CITY_NAME_RANKS = {
    ("East", "Zeta"): 1, ("East", "Epsilon"): 2, ("East", "Delta"): 3,
    ("North", "Beta"): 1, ("North", "Alpha"): 2,
    ("South", "Gamma"): 1, ("South", "Alpha"): 2,
    ("Gap", "Kappa"): 1, ("Gap", None): 2,
    ("Void", "Xi"): 1,
}
REGION_BAND_RANKS = {
    ("North", "hi"): 1, ("North", "lo"): 2, ("South", "hi"): 1, ("South", "lo"): 2,
    ("East", "hi"): 1, ("Gap", "lo"): 1, ("Void", "lo"): 1,
}


def _city_name_ranks() -> Dict[Tuple, int]:
    """rank(city, partition_by=region): city values descending, NULL last."""
    cells = {(r[1], r[2]) for r in _SALES_ROWS_WIDE}
    out = {}
    for region, city in cells:
        peers = {c for rg, c in cells if rg == region and c is not None}
        out[(region, city)] = (1 + len(peers) if city is None
                               else 1 + sum(1 for c in peers if c > city))
    return out


def _region_band_ranks() -> Dict[Tuple, int]:
    band = band_of()
    return rank_within(cell_totals(lambda r: (r[1], band[(r[2], r[1])])))


@pytest.fixture(params=["sqlite", "duckdb"])
async def engine(request):
    async for e in make_exec_engine(request):
        yield e


def _assert_membership_error(msg: str, *, key: str, grain: str, op: str = "rank") -> None:
    assert f"'{op}'" in msg, msg
    assert key in msg, msg
    assert "operand grain" in msg, msg
    assert f"({grain})" in msg, msg
    assert "partition_by=" in msg, msg


def _sales_bundle() -> ResolvedSourceBundle:
    models = dev1847_models()
    return ResolvedSourceBundle(source_model=models[0], referenced_models=models[1:])


def _measure(formula: str) -> ModelMeasure:
    return ModelMeasure(formula=formula, name="r")


class TestOracleSelfCheck:
    def test_oracles_reproduce_spec_constants(self):
        assert rank_within(cell_totals(lambda r: (r[1], r[2]))) == MEMBER_RANKS
        assert _region_band_ranks() == REGION_BAND_RANKS
        assert _city_name_ranks() == CITY_NAME_RANKS


class TestNonMemberRejected:
    async def test_measure(self, engine):
        query = sales_q(dimensions=CRP, measures=[_measure(NONMEMBER)])
        with pytest.raises(ValueError) as ei:
            await engine.execute(query)
        _assert_membership_error(str(ei.value), key="'product'", grain="city, region")

    async def test_filter(self, engine):
        query = sales_q(
            dimensions=CRP, measures=[AMOUNT], filters=[f"{NONMEMBER} <= 2"])
        with pytest.raises(ValueError) as ei:
            await engine.execute(query)
        _assert_membership_error(str(ei.value), key="'product'", grain="city, region")

    async def test_dimension_position_plain_column(self, engine):
        dim = {"expression": "rank(sum(amount, partition_by=[city, product]), "
                             "partition_by=region)", "name": "r"}
        query = sales_q(dimensions=["region", dim], measures=[AMOUNT])
        with pytest.raises(ValueError) as ei:
            await engine.execute(query)
        _assert_membership_error(str(ei.value), key="'region'", grain="city, product")

    async def test_dimension_position_computed_dimension(self, engine):
        dim = {"expression": "rank(sum(amount, partition_by=[city, region]), "
                             "partition_by=ureg)", "name": "r"}
        query = sales_q(dimensions=[UREG, dim], measures=[AMOUNT])
        with pytest.raises(ValueError) as ei:
            await engine.execute(query)
        msg = str(ei.value)
        _assert_membership_error(msg, key="upper" if "upper" in msg else "ureg",
                                 grain="city, region")

    async def test_aggregation_parameter(self, engine):
        query = sales_q(dimensions=CRP, measures=[
            ModelMeasure(formula=f"weighted_avg(amount, weight={NONMEMBER})", name="w")])
        with pytest.raises(ValueError) as ei:
            await engine.execute(query)
        _assert_membership_error(str(ei.value), key="'product'", grain="city, region")

    async def test_order(self, engine):
        query = sales_q(
            dimensions=CRP, measures=[AMOUNT],
            order=[{"column": NONMEMBER, "direction": "asc"}])
        with pytest.raises(ValueError) as ei:
            await engine.execute(query)
        _assert_membership_error(str(ei.value), key="'product'", grain="city, region")

    @pytest.mark.parametrize("op,extra", [
        ("dense_rank", ""), ("percent_rank", ""), ("ntile", ", n=2"),
    ])
    async def test_every_rank_family_op(self, engine, op, extra):
        formula = f"{op}(sum(amount, partition_by=[city, region]){extra}, partition_by=product)"
        query = sales_q(dimensions=CRP, measures=[_measure(formula)])
        with pytest.raises(ValueError) as ei:
            await engine.execute(query)
        _assert_membership_error(str(ei.value), key="'product'", grain="city, region", op=op)

    async def test_composite_union_without_key(self, engine):
        formula = ("rank(sum(amount, partition_by=[city, region]) "
                   "+ sum(amount, partition_by=city), partition_by=product)")
        query = sales_q(dimensions=CRP, measures=[_measure(formula)])
        with pytest.raises(ValueError) as ei:
            await engine.execute(query)
        _assert_membership_error(str(ei.value), key="'product'", grain="city, region")


class TestMemberAccepted:
    async def test_member_key_executes(self, engine):
        resp = await engine.execute(sales_q(dimensions=CRP, measures=[_measure(MEMBER)]))
        for (city, region, _), row in rows_by(resp, *(f"sales.{d}" for d in CRP)).items():
            assert row["sales.r"] == MEMBER_RANKS[(region, city)], (city, region)

    async def test_composite_union_member(self, engine):
        formula = ("rank(sum(amount, partition_by=[city, region]) "
                   "+ sum(amount, partition_by=[city, product]), partition_by=product)")
        resp = await engine.execute(sales_q(dimensions=CRP, measures=[_measure(formula)]))
        assert resp.data

    async def test_ntile_member_executes(self, engine):
        formula = "ntile(sum(amount, partition_by=[city, region]), n=2, partition_by=region)"
        resp = await engine.execute(sales_q(dimensions=CRP, measures=[_measure(formula)]))
        assert resp.data

    async def test_ungrained_inner_over_region_and_band(self, engine):
        resp = await engine.execute(sales_q(
            dimensions=["region", BAND], measures=[_measure(UNGRAINED)]))
        got = {k: v["sales.r"]
               for k, v in rows_by(resp, "sales.region", "sales.spend_band").items()}
        assert got == REGION_BAND_RANKS


class TestAggregateFreeInput:
    async def test_leaf_input_takes_query_grain(self, engine):
        resp = await engine.execute(sales_q(
            dimensions=CRP, measures=[_measure("rank(city, partition_by=region)")]))
        for (city, region, _), row in rows_by(resp, *(f"sales.{d}" for d in CRP)).items():
            assert row["sales.r"] == CITY_NAME_RANKS[(region, city)], (city, region)

    # Bind level only: execution is DEV-1962.
    def test_literal_input_takes_query_grain(self):
        query = sales_q(dimensions=["region"],
                        measures=[_measure("rank(1, partition_by=region)")])
        bind_query_inputs(query=query, bundle=_sales_bundle())


class TestPrecedence:
    async def test_ungrained_inner_keeps_query_dimension_error(self, engine):
        query = sales_q(dimensions=[BAND], measures=[_measure(UNGRAINED)])
        with pytest.raises(ValueError) as ei:
            await engine.execute(query)
        msg = str(ei.value)
        assert "partition_by column 'region' is not a query dimension" in msg, msg
        assert "spend_band" in msg, msg
        assert "operand grain" not in msg, msg

    async def test_residue_error_precedes_membership(self, engine):
        dim = {"expression": UNGRAINED, "name": "r"}
        query = sales_q(dimensions=["region", dim], measures=[AMOUNT])
        with pytest.raises(ValueError) as ei:
            await engine.execute(query)
        msg = str(ei.value)
        assert "must declare partition_by= explicitly" in msg, msg
        assert "operand grain" not in msg, msg


def _bind_monthly(formula: str):
    query = orders_q(dimensions=["customers.regions.name"],
                     time_dimensions=ordered_month_td(), measures=[_measure(formula)])
    return bind_query_inputs(query=query, bundle=orders_bundle())


class TestOperandGrainTimeAxis:
    def test_windowed_inner_admits_active_bucket(self):
        _bind_monthly("rank(sum(amount, window='1y', partition_by=customers.regions.name), "
                      "partition_by=ordered_at)")

    def test_nested_last_drops_its_axis(self):
        with pytest.raises(ValueError) as ei:
            _bind_monthly(f"rank({LAST_INNER}, partition_by=ordered_at)")
        _assert_membership_error(str(ei.value), key="ordered_at",
                                 grain="customers.regions.name")

    def test_nested_last_keeps_remaining_keys(self):
        _bind_monthly(f"rank({LAST_INNER}, partition_by=customers.regions.name)")


class TestRepeatedKeyword:
    @pytest.mark.parametrize("formula,call", [
        ("rank(sum(amount), partition_by=region, partition_by=city)", "rank"),
        ("sum(amount, partition_by=region, partition_by=city)", "sum"),
        ("amount:sum(partition_by=region, partition_by=city)", "sum"),
    ])
    def test_rejected_at_parse(self, formula, call):
        self._assert_rejected(formula, call=call, kwarg="partition_by")

    @pytest.mark.parametrize("formula,call,kwarg", [
        ("sum(amount, window='1y', window='2y')", "sum", "window"),
        ("ntile(sum(amount), n=2, n=3)", "ntile", "n"),
    ])
    def test_any_keyword(self, formula, call, kwarg):
        self._assert_rejected(formula, call=call, kwarg=kwarg)

    @staticmethod
    def _assert_rejected(formula: str, *, call: str, kwarg: str) -> None:
        with pytest.raises(ValueError) as ei:
            parse_expr(formula)
        msg = str(ei.value)
        assert call in msg, msg
        assert f"'{kwarg}'" in msg or f"{kwarg}=" in msg, msg
