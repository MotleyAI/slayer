"""A rank-family transform's own ``partition_by=`` binds like an aggregation's:
computed-dimension aliases resolve in every position (spec:
queries/partitioned-aggregates › Transform partition keys bind like aggregate
partition keys; queries/computed-dimensions › Used as a transform partition)."""

from __future__ import annotations

from collections import defaultdict
from typing import Callable, Dict, Optional, Tuple

import pytest

from slayer.core.errors import PartitionKeyError, UnknownReferenceError
from slayer.core.keys import AggregateKey, FieldKey, Grain, TransformKey
from slayer.core.scope import DatasetScope
from slayer.engine.binding import bind_expr
from slayer.core.syntax import parse_expr
from slayer.ir.source_bundle import ResolvedSourceBundle

from slayer.core.query import QueryMetric

from tests._dev1847_fixtures import (
    SPEND_BAND_EXPR,
    _SALES_ROWS_WIDE,
    dev1847_datasets,
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

RANK_UREG = "rank(sum(sales.amount), partition_by=ureg, direction='desc')"
RANK_BAND = "rank(sum(sales.amount), partition_by=spend_band, direction='desc')"
PARAM_BAND = "rank(sum(sales.amount, partition_by=[spend_band, sales.city]), partition_by=spend_band, direction='desc')"
PARAM_UREG = ("weighted_avg(sales.amount, weight=rank(sum(sales.amount, partition_by=[ureg, sales.city]), "
              "partition_by=ureg, direction='desc'))")
DIM_R = {"field": "rank(sum(sales.amount, partition_by=[sales.city, ureg]), partition_by=ureg, direction='desc')",
         "alias": "r"}

# Spec constants.
METRIC_RANKS = {
    ("EAST", "Zeta"): 1, ("EAST", "Delta"): 2, ("EAST", "Epsilon"): 2,
    ("NORTH", "Beta"): 1, ("NORTH", "Alpha"): 2,
    ("SOUTH", "Gamma"): 1, ("SOUTH", "Alpha"): 2,
    ("GAP", None): 1, ("GAP", "Kappa"): 2,
    ("VOID", "Xi"): None,
}
PARAM_BY_UREG = {"EAST": 56.0, "NORTH": 120 / 7, "SOUTH": 36.0, "GAP": 7.0, "VOID": None}
FILTER_RANK1 = {("EAST", "Zeta"), ("NORTH", "Beta"), ("SOUTH", "Gamma"), ("GAP", None)}
DIM_CELLS = {
    ("EAST", 1): 80.0, ("EAST", 2): 100.0, ("NORTH", 1): 60.0, ("NORTH", 2): 30.0,
    ("SOUTH", 1): 100.0, ("SOUTH", 2): 40.0, ("GAP", 1): 12.0, ("GAP", 2): 8.0,
    ("VOID", None): None,
}
BAND_RANK1 = {("hi", "Gamma"): 100.0, ("lo", "Alpha"): 70.0}


# --------------------------------------------------------------------------- #
# Raw-row oracles.
# --------------------------------------------------------------------------- #
def _ureg_city_ranks() -> Dict[Tuple, Optional[int]]:
    return rank_within(cell_totals(lambda r: (r[1].upper(), r[2])))


def _weighted_avg(group: Callable[[tuple], str],
                  weight: Callable[[tuple], Optional[int]]) -> Dict[str, Optional[float]]:
    """weighted_avg(amount, weight=…) per ``group(row)``."""
    per: Dict[str, list] = defaultdict(list)
    for row in _SALES_ROWS_WIDE:
        per[group(row)].append((row[4], weight(row)))
    out = {}
    for g, pairs in per.items():
        num = [v * w for v, w in pairs if v is not None and w is not None]
        out[g] = sum(num) / sum(w for _v, w in pairs if w is not None) if num else None
    return out


def _param_by_ureg() -> Dict[str, Optional[float]]:
    rank = _ureg_city_ranks()
    return _weighted_avg(lambda r: r[1].upper(), lambda r: rank[(r[1].upper(), r[2])])


def _band_city_ranks() -> Dict[Tuple, Optional[int]]:
    band = band_of()
    return rank_within(cell_totals(lambda r: (band[(r[2], r[1])], r[2])))


def _param_by_band() -> Dict[str, Optional[float]]:
    band = band_of()
    rank = _band_city_ranks()
    return _weighted_avg(lambda r: band[(r[2], r[1])],
                         lambda r: rank[(band[(r[2], r[1])], r[2])])


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
    async for e in make_exec_engine(request=request):
        yield e


def _scope_bundle():
    models = dev1847_datasets()
    return (DatasetScope(population=models[0]),
            ResolvedSourceBundle(dialect="postgres", population=models[0], referenced_datasets=models[1:]))


def _bind(expression: str, alias_map: Optional[Dict] = None):
    scope, bundle = _scope_bundle()
    return bind_expr(parsed=parse_expr(expression), scope=scope, bundle=bundle,
                     dimension_alias_map=alias_map).value_key


def _alias(name: str, expr: str) -> Dict:
    return {name: _bind(expr)}


class TestOracleSelfCheck:
    def test_oracles_reproduce_spec_constants(self):
        assert _ureg_city_ranks() == METRIC_RANKS
        approx_map(got=_param_by_ureg(), want=PARAM_BY_UREG)
        assert {k for k, v in _ureg_city_ranks().items() if v == 1} == FILTER_RANK1
        approx_map(got=_dim_cells(), want=DIM_CELLS)
        approx_map(got=_band_rank1(), want=BAND_RANK1)


# --------------------------------------------------------------------------- #
# Binding.
# --------------------------------------------------------------------------- #
class TestBindingSymmetry:
    def test_alias_resolves_identically(self):
        amap = _alias(name="ureg", expr="upper(sales.region)")
        agg = _bind(expression="sum(sales.amount, partition_by=ureg)", alias_map=amap)
        rank = _bind(expression=RANK_UREG, alias_map=amap)
        assert isinstance(rank, TransformKey)
        assert isinstance(agg, AggregateKey)
        assert rank.partition_keys == agg.partition_keys == Grain.of([amap["ureg"]])

    def test_mixed_list(self):
        amap = _alias(name="ureg", expr="upper(sales.region)")
        agg = _bind(expression="sum(amount, partition_by=[ureg, product])", alias_map=amap)
        rank = _bind(expression="rank(sum(amount), partition_by=[ureg, product], direction='desc')", alias_map=amap)
        want = Grain.of([amap["ureg"], FieldKey(path=(), leaf="product")])
        assert isinstance(rank, TransformKey)
        assert isinstance(agg, AggregateKey)
        assert rank.partition_keys == agg.partition_keys == want

    def test_attach_carrying_alias_binds_to_dimension_value(self):
        amap = _alias(name="spend_band", expr=SPEND_BAND_EXPR)
        rank = _bind(expression=RANK_BAND, alias_map=amap)
        assert isinstance(rank, TransformKey)
        assert rank.partition_keys == Grain.of([amap["spend_band"]])

    @pytest.mark.parametrize(argnames="formula", argvalues=[
        "rank(sum(sales.amount), partition_by=sum(sales.amount), direction='desc')",
        "sum(sales.amount, partition_by=sum(sales.amount))",
    ])
    def test_aggregate_element_names_the_computed_dimension_remedy(self, formula):
        amap = _alias(name="ureg", expr="upper(sales.region)")
        with pytest.raises(PartitionKeyError, match=r"(?s)'sum\(amount\)'.*computed dimension"):
            _bind(expression=formula, alias_map=amap)

    @pytest.mark.parametrize(argnames="formula", argvalues=[
        "sum(sales.amount, partition_by=upper(sales.region))",
        "rank(sum(sales.amount), partition_by=[sales.city, upper(sales.region)], direction='desc')",
    ])
    def test_expression_element_equal_to_an_alias_binds_to_it(self, formula):
        amap = _alias(name="ureg", expr="upper(sales.region)")
        expression = _bind(expression=formula, alias_map=amap)
        named = _bind(expression=formula.replace("upper(sales.region)", "ureg"), alias_map=amap)
        assert isinstance(expression, (AggregateKey, TransformKey))
        assert isinstance(named, (AggregateKey, TransformKey))
        assert expression.partition_keys == named.partition_keys


# --------------------------------------------------------------------------- #
# Executed positions (ureg = upper(region)).
# --------------------------------------------------------------------------- #
class TestExecutedPositions:
    async def test_measure(self, engine):
        resp = await engine.execute(sales_q(
            dimensions=[UREG, "sales.city"], metrics=[QueryMetric(metric=RANK_UREG, alias="r")]))
        got = {k: v["r"] for k, v in rows_by(resp, "ureg", "sales.city").items()}
        assert got == METRIC_RANKS

    async def test_aggregation_parameter(self, engine):
        resp = await engine.execute(sales_q(
            dimensions=[UREG], metrics=[QueryMetric(metric=PARAM_UREG, alias="w")]))
        got = {k[0]: v["w"] for k, v in rows_by(resp, "ureg").items()}
        approx_map(got=got, want=PARAM_BY_UREG)

    async def test_filter(self, engine):
        resp = await engine.execute(sales_q(
            dimensions=[UREG, "sales.city"], metrics=[AMOUNT],
            filter=[f"{RANK_UREG} <= 1"]))
        assert set(rows_by(resp, "ureg", "sales.city")) == FILTER_RANK1

    async def test_order(self, engine):
        resp = await engine.execute(sales_q(
            dimensions=[UREG, "sales.city"], metrics=[AMOUNT],
            order=[{"field": RANK_UREG, "direction": "asc"}]))
        keys = [(r["ureg"], r["sales.city"]) for r in resp.data]
        pos = {METRIC_RANKS[k]: [] for k in keys}
        for i, k in enumerate(keys):
            pos[METRIC_RANKS[k]].append(i)
        assert {k for k in keys if METRIC_RANKS[k] == 1} == FILTER_RANK1
        assert max(pos[1]) < min(pos[2])

    async def test_dimension_position_member_key(self, engine):
        resp = await engine.execute(sales_q(dimensions=[UREG, DIM_R], metrics=[AMOUNT]))
        got = {k: v["a"] for k, v in rows_by(resp, "ureg", "r").items()}
        approx_map(got=got, want=DIM_CELLS)

    async def test_undeclared_name_stays_unknown(self, engine):
        query = sales_q(
            dimensions=["sales.city"],
            metrics=[QueryMetric(metric="rank(sum(sales.amount), partition_by=sales.ureg, direction='desc')", alias="r")])
        with pytest.raises(UnknownReferenceError, match="ureg"):
            await engine.execute(query)


# --------------------------------------------------------------------------- #
# Attach-carrying computed dimension (spend_band).
# --------------------------------------------------------------------------- #
class TestAttachCarryingKey:
    async def test_filter(self, engine):
        resp = await engine.execute(sales_q(
            dimensions=[BAND, "sales.city"], metrics=[AMOUNT], filter=[f"{RANK_BAND} <= 1"]))
        got = {k: v["a"] for k, v in rows_by(resp, "spend_band", "sales.city").items()}
        approx_map(got=got, want=BAND_RANK1)

    async def test_order(self, engine):
        resp = await engine.execute(sales_q(
            dimensions=[BAND, "sales.city"], metrics=[AMOUNT],
            order=[{"field": RANK_BAND, "direction": "asc"}]))
        keys = [(r["spend_band"], r["sales.city"]) for r in resp.data]
        assert set(keys[:2]) == set(BAND_RANK1)

    async def test_measure(self, engine):
        resp = await engine.execute(sales_q(
            dimensions=[BAND, "sales.city"], metrics=[QueryMetric(metric=RANK_BAND, alias="r")]))
        got = {k: v["r"]
               for k, v in rows_by(resp, "spend_band", "sales.city").items()}
        assert got == _band_city_ranks()
        assert {k for k, r in got.items() if r is not None and r <= 1} == set(BAND_RANK1)

    async def test_parameter(self, engine):
        resp = await engine.execute(sales_q(
            dimensions=[BAND],
            metrics=[QueryMetric(
                metric=f"weighted_avg(sales.amount, weight={PARAM_BAND})", alias="w")]))
        approx_map(got={r["spend_band"]: r["w"] for r in resp.data},
                   want=_param_by_band())
