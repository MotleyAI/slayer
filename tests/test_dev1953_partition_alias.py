"""A rank-family transform's own ``partition_by=`` binds like an aggregation's:
computed-dimension aliases resolve in every position (spec:
queries/partitioned-aggregates › Transform partition keys bind like aggregate
partition keys; queries/computed-dimensions › Used as a transform partition)."""

from __future__ import annotations

import re
from collections import defaultdict
from typing import Dict, Optional, Tuple

import pytest

from slayer.core.errors import UnknownReferenceError
from slayer.core.keys import AggregateKey, ColumnKey, Grain, TransformKey
from slayer.core.scope import ModelScope
from slayer.engine.binding import bind_expr
from slayer.engine.syntax import parse_expr
from slayer.ir.source_bundle import ResolvedSourceBundle

from tests._dev1847_fixtures import (
    SPEND_BAND_EXPR,
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
    approx_map,
    band_of,
    cell_totals,
    rank_within,
)

RANK_UREG = "rank(sum(amount), partition_by=ureg)"
RANK_BAND = "rank(sum(amount), partition_by=spend_band)"
PARAM_UREG = ("weighted_avg(amount, weight=rank(sum(amount, partition_by=[ureg, city]), "
              "partition_by=ureg))")
DIM_R = {"expression": "rank(sum(amount, partition_by=[city, ureg]), partition_by=ureg)",
         "name": "r"}

# Spec constants.
MEASURE_RANKS = {
    ("EAST", "Zeta"): 1, ("EAST", "Delta"): 2, ("EAST", "Epsilon"): 2,
    ("NORTH", "Beta"): 1, ("NORTH", "Alpha"): 2,
    ("SOUTH", "Gamma"): 1, ("SOUTH", "Alpha"): 2,
    ("GAP", None): 1, ("GAP", "Kappa"): 2,
    ("VOID", "Xi"): 1,
}
PARAM_BY_UREG = {"EAST": 56.0, "NORTH": 120 / 7, "SOUTH": 36.0, "GAP": 7.0, "VOID": None}
FILTER_RANK1 = {("EAST", "Zeta"), ("NORTH", "Beta"), ("SOUTH", "Gamma"), ("GAP", None),
                ("VOID", "Xi")}
DIM_CELLS = {
    ("EAST", 1): 80.0, ("EAST", 2): 100.0, ("NORTH", 1): 60.0, ("NORTH", 2): 30.0,
    ("SOUTH", 1): 100.0, ("SOUTH", 2): 40.0, ("GAP", 1): 12.0, ("GAP", 2): 8.0,
    ("VOID", 1): None,
}
BAND_RANK1 = {("hi", "Gamma"): 100.0, ("lo", "Alpha"): 70.0}


# --------------------------------------------------------------------------- #
# Raw-row oracles.
# --------------------------------------------------------------------------- #
def _ureg_city_ranks() -> Dict[Tuple, int]:
    return rank_within(cell_totals(lambda r: (r[1].upper(), r[2])))


def _param_by_ureg() -> Dict[str, Optional[float]]:
    rank = _ureg_city_ranks()
    per: Dict[str, list] = defaultdict(list)
    for row in _SALES_ROWS_WIDE:
        k = (row[1].upper(), row[2])
        per[k[0]].append((row[4], rank[k]))
    out = {}
    for reg, pairs in per.items():
        num = [v * w for v, w in pairs if v is not None]
        out[reg] = sum(num) / sum(w for _v, w in pairs) if num else None
    return out


def _dim_cells() -> Dict[Tuple, Optional[float]]:
    rank = _ureg_city_ranks()
    return cell_totals(lambda r: (r[1].upper(), rank[(r[1].upper(), r[2])]))


def _band_rank1() -> Dict[Tuple, Optional[float]]:
    band = band_of()
    totals = cell_totals(lambda r: (band[(r[2], r[1])], r[2]))
    rank = rank_within(totals)
    return {k: totals[k] for k, rk in rank.items() if rk == 1}


# --------------------------------------------------------------------------- #
# Fixtures.
# --------------------------------------------------------------------------- #
@pytest.fixture(params=["sqlite", "duckdb"])
async def engine(request):
    async for e in make_exec_engine(request):
        yield e


def _scope_bundle():
    models = dev1847_models()
    return (ModelScope(source_model=models[0]),
            ResolvedSourceBundle(source_model=models[0], referenced_models=models[1:]))


def _bind(formula: str, alias_map: Optional[Dict] = None):
    scope, bundle = _scope_bundle()
    return bind_expr(parse_expr(formula), scope=scope, bundle=bundle,
                     dimension_alias_map=alias_map).value_key


def _alias(name: str, expr: str) -> Dict:
    return {name: _bind(expr)}


class TestOracleSelfCheck:
    def test_oracles_reproduce_spec_constants(self):
        assert _ureg_city_ranks() == MEASURE_RANKS
        approx_map(_param_by_ureg(), PARAM_BY_UREG)
        assert {k for k, v in _ureg_city_ranks().items() if v == 1} == FILTER_RANK1
        approx_map(_dim_cells(), DIM_CELLS)
        approx_map(_band_rank1(), BAND_RANK1)


# --------------------------------------------------------------------------- #
# Binding.
# --------------------------------------------------------------------------- #
class TestBindingSymmetry:
    def test_alias_resolves_identically(self):
        amap = _alias("ureg", "upper(region)")
        agg = _bind("sum(amount, partition_by=ureg)", amap)
        rank = _bind(RANK_UREG, amap)
        assert isinstance(rank, TransformKey)
        assert isinstance(agg, AggregateKey)
        assert rank.partition_keys == agg.partition_keys == Grain.of([amap["ureg"]])

    def test_mixed_list(self):
        amap = _alias("ureg", "upper(region)")
        agg = _bind("sum(amount, partition_by=[ureg, product])", amap)
        rank = _bind("rank(sum(amount), partition_by=[ureg, product])", amap)
        want = Grain.of([amap["ureg"], ColumnKey(path=(), leaf="product")])
        assert isinstance(rank, TransformKey)
        assert isinstance(agg, AggregateKey)
        assert rank.partition_keys == agg.partition_keys == want

    def test_attach_carrying_alias_binds_to_dimension_value(self):
        amap = _alias("spend_band", SPEND_BAND_EXPR)
        rank = _bind(RANK_BAND, amap)
        assert isinstance(rank, TransformKey)
        assert rank.partition_keys == Grain.of([amap["spend_band"]])

    @pytest.mark.parametrize("formula,message", [
        ("rank(sum(amount), partition_by=sum(amount))",
         "transform 'rank' partition_by must resolve to a column reference; got AggregateKey."),
        ("sum(amount, partition_by=sum(amount))",
         "aggregation partition_by must resolve to a column reference; got AggregateKey."),
    ])
    def test_non_column_element_names_construct(self, formula, message):
        amap = _alias("ureg", "upper(region)")
        with pytest.raises(ValueError, match=re.escape(message)):
            _bind(formula, amap)


# --------------------------------------------------------------------------- #
# Executed positions (ureg = upper(region)).
# --------------------------------------------------------------------------- #
class TestExecutedPositions:
    async def test_measure(self, engine):
        resp = await engine.execute(sales_q(
            dimensions=[UREG, "city"], measures=[ModelMeasure(formula=RANK_UREG, name="r")]))
        got = {k: v["sales.r"] for k, v in rows_by(resp, "sales.ureg", "sales.city").items()}
        assert got == MEASURE_RANKS

    async def test_aggregation_parameter(self, engine):
        resp = await engine.execute(sales_q(
            dimensions=[UREG], measures=[ModelMeasure(formula=PARAM_UREG, name="w")]))
        got = {k[0]: v["sales.w"] for k, v in rows_by(resp, "sales.ureg").items()}
        approx_map(got, PARAM_BY_UREG)

    async def test_filter(self, engine):
        resp = await engine.execute(sales_q(
            dimensions=[UREG, "city"], measures=[AMOUNT],
            filters=[f"{RANK_UREG} <= 1"]))
        assert set(rows_by(resp, "sales.ureg", "sales.city")) == FILTER_RANK1

    async def test_order(self, engine):
        resp = await engine.execute(sales_q(
            dimensions=[UREG, "city"], measures=[AMOUNT],
            order=[{"column": RANK_UREG, "direction": "asc"}]))
        keys = [(r["sales.ureg"], r["sales.city"]) for r in resp.data]
        n = len(FILTER_RANK1)
        assert set(keys[:n]) == FILTER_RANK1
        assert all(MEASURE_RANKS[k] == 2 for k in keys[n:])

    async def test_dimension_position_member_key(self, engine):
        resp = await engine.execute(sales_q(dimensions=[UREG, DIM_R], measures=[AMOUNT]))
        got = {k: v["sales.a"] for k, v in rows_by(resp, "sales.ureg", "sales.r").items()}
        approx_map(got, DIM_CELLS)

    async def test_undeclared_name_stays_unknown(self, engine):
        query = sales_q(
            dimensions=["city"], measures=[ModelMeasure(formula=RANK_UREG, name="r")])
        with pytest.raises(UnknownReferenceError, match="ureg"):
            await engine.execute(query)


# --------------------------------------------------------------------------- #
# Attach-carrying computed dimension (spend_band).
# --------------------------------------------------------------------------- #
class TestAttachCarryingKey:
    async def test_filter(self, engine):
        resp = await engine.execute(sales_q(
            dimensions=[BAND, "city"], measures=[AMOUNT], filters=[f"{RANK_BAND} <= 1"]))
        got = {k: v["sales.a"] for k, v in rows_by(resp, "sales.spend_band", "sales.city").items()}
        approx_map(got, BAND_RANK1)

    async def test_order(self, engine):
        resp = await engine.execute(sales_q(
            dimensions=[BAND, "city"], measures=[AMOUNT],
            order=[{"column": RANK_BAND, "direction": "asc"}]))
        keys = [(r["sales.spend_band"], r["sales.city"]) for r in resp.data]
        assert set(keys[:2]) == set(BAND_RANK1)

    # Pins the current planner failure; DEV-1960 flips both to executed values.
    async def test_measure_fails_closed_in_planner(self, engine):
        query = sales_q(
            dimensions=[BAND, "city"], measures=[ModelMeasure(formula=RANK_BAND, name="r")])
        with pytest.raises(ValueError, match="no routing disposition") as ei:
            await engine.execute(query)
        assert not isinstance(ei.value, UnknownReferenceError)

    async def test_parameter_fails_closed_in_planner(self, engine):
        query = sales_q(
            dimensions=[BAND],
            measures=[ModelMeasure(
                formula=f"weighted_avg(amount, weight={RANK_BAND})", name="w")])
        with pytest.raises(RuntimeError, match="missing a host / producer grain slot"):
            await engine.execute(query)
