"""One aggregate expression consumed by a computed dimension AND another position
(spec: queries/computed-dimensions, queries/partitioned-aggregates)."""

from __future__ import annotations

import re
from typing import Callable, List

import pytest

from slayer.core.errors import PartitionKeyError
from slayer.core.keys import (
    REGROUP_LEAF_PREFIX,
    AggregateKey,
    ArithmeticKey,
    ColumnKey,
    Grain,
    LiteralKey,
    ScalarCallKey,
    TransformKey,
    ValueKey,
)
from slayer.engine.compile.discovery import RootDisposition
from slayer.engine.compile.stages import _regroup_producer_prebound
from slayer.engine.plan import plan_query
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.sql.scope_check import assert_scope_closed

from tests._dev1847_fixtures import (
    ModelMeasure,
    chain_q,
    dev1847_models,
    gen,
    make_exec_engine,
    sales_q,
)
from tests._engine_helpers import _extract_cte_body
from tests.test_dev1903_discovery import _discover

R = "avg(sum(amount, partition_by=[city, region]), partition_by=region)"
RLEVEL = {"expression": f"CASE WHEN {R} > 50 THEN 'hi' ELSE 'lo' END", "name": "rlevel"}
TLEVEL = {"expression": f"CASE WHEN rank({R}) > 1 THEN 'top' ELSE 'rest' END",
          "name": "tlevel"}
TOT = ModelMeasure(formula="amount:sum", name="tot")

R_BY_REGION = {"North": 45.0, "South": 70.0, "East": 60.0, "Gap": 10.0, "Void": None}
TOT_BY_REGION = {"North": 90.0, "South": 140.0, "East": 180.0, "Gap": 20.0, "Void": None}
RLEVEL_BY_REGION = {"North": "lo", "South": "hi", "East": "hi", "Gap": "lo", "Void": "lo"}
RANK_BY_REGION = {"South": 1, "East": 2, "North": 3, "Gap": 4, "Void": 5}
TLEVEL_BY_REGION = {"South": "rest", "East": "top", "North": "top", "Gap": "top",
                    "Void": "top"}

PLAIN_RANK = "rank(amount:sum(partition_by=region))"
PBAND = {"expression": f"CASE WHEN {PLAIN_RANK} > 1 THEN 'top' ELSE 'rest' END",
         "name": "pband"}

# Finer-grained re-aggregation: outer grain [city, region] under dimensions [region].
FINE = "avg(sum(amount, partition_by=[city, region, product]), partition_by=[city, region])"
FINE_DIM = {"expression": f"CASE WHEN {FINE} > 30 THEN 'hi' ELSE 'lo' END", "name": "x"}
FINE_TOT = {("North", "lo"): 30.0, ("North", "hi"): 60.0, ("South", "hi"): 140.0,
            ("East", "hi"): 180.0, ("Gap", "lo"): 20.0, ("Void", "lo"): None}

CITY_BAND = {"expression": "CASE WHEN amount:sum(partition_by=[city, region]) > 45 "
                           "THEN 'hi' ELSE 'lo' END", "name": "band"}

CHAIN_R = "avg(sum(amount, partition_by=customer_id), partition_by=customers.regions.name)"
CL = {"expression": f"CASE WHEN {CHAIN_R} > 40 THEN 'hi' ELSE 'lo' END", "name": "cl"}


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    async for engine in make_exec_engine(request):
        yield engine


def _bundle(source: str = "sales") -> ResolvedSourceBundle:
    models = dev1847_models()
    [host] = [m for m in models if m.name == source]
    return ResolvedSourceBundle(dialect="duckdb", source_model=host,
                                referenced_models=[m for m in models if m is not host])


def _col(resp, name: str) -> dict:
    """``{region: value}`` for result column ``sales.<name>``."""
    return {r["sales.region"]: r[f"sales.{name}"] for r in resp.data}


def _regions(resp) -> List[str]:
    return [r["sales.region"] for r in resp.data]


def _approx(d: dict) -> dict:
    return {k: (None if v is None else pytest.approx(v)) for k, v in d.items()}


# --------------------------------------------------------------------------- #
# queries/computed-dimensions — per-position evaluation, executed.
# --------------------------------------------------------------------------- #
class TestReaggregationInDimensionAndElsewhere:
    async def test_measure_typed_filter(self, exec_engine):
        resp = await exec_engine.execute(sales_q(
            dimensions=["region", RLEVEL], measures=[TOT], filters=[f"{R} < amount:sum"]))
        got = {(r["sales.region"], r["sales.rlevel"]): r["sales.tot"] for r in resp.data}
        assert got == _approx({("East", "hi"): 180.0, ("Gap", "lo"): 20.0,
                               ("North", "lo"): 90.0, ("South", "hi"): 140.0})

    async def test_measure_typed_filter_keeping_nothing(self, exec_engine):
        resp = await exec_engine.execute(sales_q(
            dimensions=["region", RLEVEL], measures=[TOT], filters=[f"{R} > amount:sum"]))
        assert resp.data == []

    async def test_order_target(self, exec_engine):
        resp = await exec_engine.execute(sales_q(
            dimensions=["region", RLEVEL], measures=[TOT],
            order=[{"column": R, "direction": "desc"}]))
        assert _regions(resp) == ["South", "East", "North", "Gap", "Void"]

    async def test_measure(self, exec_engine):
        resp = await exec_engine.execute(sales_q(
            dimensions=["region", RLEVEL], measures=[TOT, ModelMeasure(formula=R, name="r")]))
        assert _col(resp, "r") == _approx(R_BY_REGION)
        assert _col(resp, "rlevel") == RLEVEL_BY_REGION

    async def test_filter_by_dimension_name_uses_band(self, exec_engine):
        resp = await exec_engine.execute(sales_q(
            dimensions=["region", RLEVEL], measures=[TOT, ModelMeasure(formula=R, name="r")],
            filters=["rlevel = 'hi'"]))
        assert _col(resp, "r") == _approx({"South": 70.0, "East": 60.0})


class TestTransformOverReaggregationInDimension:
    async def test_dimension_only(self, exec_engine):
        resp = await exec_engine.execute(sales_q(dimensions=["region", TLEVEL], measures=[TOT]))
        assert _col(resp, "tlevel") == TLEVEL_BY_REGION
        assert _col(resp, "tot") == _approx(TOT_BY_REGION)

    async def test_rank_as_measure(self, exec_engine):
        resp = await exec_engine.execute(sales_q(
            dimensions=["region", TLEVEL],
            measures=[TOT, ModelMeasure(formula=f"rank({R})", name="rk")]))
        assert _col(resp, "rk") == RANK_BY_REGION
        assert _col(resp, "tlevel") == TLEVEL_BY_REGION

    @pytest.mark.parametrize(("flt", "kept"), [
        pytest.param(f"rank({R}) > 1", {"East", "North", "Gap", "Void"}, id="gt"),
        pytest.param(f"rank({R}) < 4", {"South", "East", "North"}, id="lt"),
    ])
    async def test_rank_in_filter(self, exec_engine, flt, kept):
        resp = await exec_engine.execute(sales_q(
            dimensions=["region", TLEVEL], measures=[TOT], filters=[flt]))
        assert set(_regions(resp)) == kept
        assert _col(resp, "tot") == _approx({k: TOT_BY_REGION[k] for k in kept})

    async def test_rank_as_order_target(self, exec_engine):
        resp = await exec_engine.execute(sales_q(
            dimensions=["region", TLEVEL], measures=[TOT],
            order=[{"column": f"rank({R})", "direction": "asc"}]))
        assert _regions(resp) == ["South", "East", "North", "Gap", "Void"]

    async def test_two_dimensions_share_the_reaggregation(self, exec_engine):
        resp = await exec_engine.execute(sales_q(
            dimensions=["region", TLEVEL, RLEVEL],
            measures=[TOT, ModelMeasure(formula=R, name="r")]))
        got = {r["sales.region"]: (r["sales.tlevel"], r["sales.rlevel"]) for r in resp.data}
        assert got == {"South": ("rest", "hi"), "East": ("top", "hi"),
                       "North": ("top", "lo"), "Gap": ("top", "lo"), "Void": ("top", "lo")}
        assert _col(resp, "tot") == _approx(TOT_BY_REGION)
        assert _col(resp, "r") == _approx(R_BY_REGION)
        assert not any(REGROUP_LEAF_PREFIX in c or c.endswith(".grain") for c in resp.columns)


class TestOrderByTransformSharedWithDimension:
    async def test_order_by_expression(self, exec_engine):
        resp = await exec_engine.execute(sales_q(
            dimensions=["region", PBAND], measures=[TOT],
            order=[{"column": PLAIN_RANK, "direction": "asc"}]))
        assert _regions(resp) == ["East", "South", "North", "Gap", "Void"]

    async def test_order_by_dimension_name_sorts_band(self, exec_engine):
        resp = await exec_engine.execute(sales_q(
            dimensions=["region", PBAND], measures=[TOT],
            order=[{"column": "pband", "direction": "asc"}]))
        bands = [r["sales.pband"] for r in resp.data]
        assert bands == ["rest", "top", "top", "top", "top"]
        assert _regions(resp)[0] == "East"


class TestCrossModelReaggregationInDimension:
    async def test_measure_typed_filter(self, exec_engine):
        resp = await exec_engine.execute(chain_q(
            dimensions=["customers.regions.name", CL],
            measures=[ModelMeasure(formula="amount:sum", name="tot")],
            filters=[f"{CHAIN_R} < amount:sum"]))
        got = [(r["corders.customers.regions.name"], r["corders.cl"], r["corders.tot"])
               for r in resp.data]
        assert got == [("North", "lo", pytest.approx(70.0))]


# --------------------------------------------------------------------------- #
# queries/partitioned-aggregates — combined-consumer partition keys.
# --------------------------------------------------------------------------- #
class TestFinerGrainedReaggregationDimension:
    async def test_executes_at_its_declared_grain(self, exec_engine):
        resp = await exec_engine.execute(sales_q(dimensions=["region", FINE_DIM], measures=[TOT]))
        got = {(r["sales.region"], r["sales.x"]): r["sales.tot"] for r in resp.data}
        assert got == _approx(FINE_TOT)


class TestPlainMeasureTypedFilterOverDimensionAggregate:
    def test_same_partition_key_error_as_without_dimension(self):
        flt = "amount:sum(partition_by=[city, region]) < amount:sum"
        with pytest.raises(PartitionKeyError) as bare:
            plan_query(query=sales_q(dimensions=["region"], measures=[TOT], filters=[flt]),
                       bundle=_bundle())
        with pytest.raises(PartitionKeyError) as banded:
            plan_query(query=sales_q(dimensions=["region", CITY_BAND], measures=[TOT],
                                     filters=[flt]),
                       bundle=_bundle())
        assert "'city'" in banded.value.summary
        assert (banded.value.summary, banded.value.location) == (
            bare.value.summary, bare.value.location)


def _fine_cases():
    """(id, query kwargs, expected location pattern) for combined consumers of FINE."""
    fm = ModelMeasure(formula=FINE, name="f")
    return [
        ("measure", {"measures": [fm]}, r"measure 'f'"),
        ("order", {"measures": [TOT], "order": [{"column": FINE, "direction": "asc"}]},
         r"order item\b"),
        ("arithmetic", {"measures": [ModelMeasure(formula=f"{FINE} + 1", name="f")]},
         r"measure 'f'"),
        ("transform", {"measures": [ModelMeasure(formula=f"rank({FINE})", name="f")]},
         r"measure 'f'"),
        ("filter", {"measures": [TOT], "filters": [f"{FINE} < amount:sum"]}, r"filter\b"),
        ("split-filter",
         {"measures": [TOT], "filters": [f"{FINE} < amount:sum and region <> 'Gap'"]},
         r"filter\b"),
    ]


def _assert_consumer_error(exc: PartitionKeyError, *, key: str, location: str) -> None:
    assert re.search(rf"\b{re.escape(key)}\b", exc.summary), exc.summary
    assert exc.location is not None and re.match(location, exc.location), str(exc)
    assert "dimension" not in exc.location
    assert REGROUP_LEAF_PREFIX not in str(exc)


class TestReaggregationOuterKeyCarriesTheRule:
    @pytest.mark.parametrize("with_dim", [False, True], ids=["no-dim", "with-dim"])
    @pytest.mark.parametrize(("kw", "location"),
                             [pytest.param(kw, loc, id=i) for i, kw, loc in _fine_cases()])
    def test_combined_consumer_rejected(self, kw, location, with_dim):
        dims = ["region", FINE_DIM] if with_dim else ["region"]
        with pytest.raises(PartitionKeyError) as ei:
            plan_query(query=sales_q(dimensions=dims, **kw), bundle=_bundle())
        _assert_consumer_error(ei.value, key="city", location=location)

    def test_cross_model_outer_key(self):
        formula = "avg(sum(amount, partition_by=customer_id), partition_by=customers.region_id)"
        with pytest.raises(PartitionKeyError) as ei:
            plan_query(query=chain_q(dimensions=["customers.regions.name"],
                                     measures=[ModelMeasure(formula=formula, name="c")]),
                       bundle=_bundle("corders"))
        _assert_consumer_error(ei.value, key="customers.region_id", location=r"measure 'c'")


# --------------------------------------------------------------------------- #
# Plan structure — one producer, one attach per (root, phase).
# --------------------------------------------------------------------------- #
def _is_r(key: ValueKey) -> bool:
    return isinstance(key, AggregateKey) and key.agg == "avg"


_R_CONSUMERS = [
    pytest.param({"filters": [f"{R} < amount:sum"]}, id="filter"),
    pytest.param({"order": [{"column": R, "direction": "desc"}]}, id="order"),
    pytest.param({"measures": [TOT, ModelMeasure(formula=R, name="r")]}, id="measure"),
]


def _r_query(kw: dict):
    return sales_q(**{"dimensions": ["region", RLEVEL], "measures": [TOT], **kw})


class TestDualPhasePlan:
    @pytest.mark.parametrize("kw", _R_CONSUMERS)
    def test_one_attach_per_phase_sharing_one_placeholder(self, kw):
        plan = plan_query(query=_r_query(kw), bundle=_bundle())
        attaches = [a for a in plan.regroup_attach_plans
                    if any(_is_r(s.original_key) for s in a.substitutions)]
        assert sorted(a.attach_phase for a in attaches) == ["combined", "row"]
        assert len({s.placeholder for a in attaches for s in a.substitutions}) == 1

    @pytest.mark.parametrize("kw", _R_CONSUMERS)
    async def test_one_producer_cte_joined_once_per_phase(self, kw):
        sql = await gen(_r_query(kw), dialect="duckdb")
        assert REGROUP_LEAF_PREFIX not in sql
        [cte] = [c for c in re.findall(r"(_cm_\w+) AS \(", sql)
                 if "AVG(" in _extract_cte_body(sql, re.escape(c))]
        assert len(re.findall(rf"JOIN {cte}\b", sql)) == 2, sql
        assert_scope_closed(sql, dialect="duckdb")


def _roots(ds: List[RootDisposition], match: Callable[[ValueKey], bool]) -> set:
    return {(d.phase, d.routing) for d in ds if match(d.root)}


def _is_rank(key: ValueKey) -> bool:
    return isinstance(key, TransformKey) and key.op == "rank"


def _is_inner_cr(key: ValueKey) -> bool:
    return (isinstance(key, AggregateKey) and key.agg == "sum"
            and key.partition_keys is not None and len(key.partition_keys) == 2)


class TestDimensionTransformOwnsReaggregation:
    def test_rank_dimension_is_the_only_root(self):
        ds = _discover(sales_q(dimensions=["region", TLEVEL], measures=[TOT]), _bundle())
        assert {d.phase for d in ds if _is_rank(d.root)} == {"row"}
        assert not _roots(ds, _is_r)
        assert not _roots(ds, _is_inner_cr)

    def test_measure_occurrence_is_combined_only(self):
        ds = _discover(sales_q(dimensions=["region", TLEVEL],
                               measures=[TOT, ModelMeasure(formula=R, name="r")]),
                       _bundle())
        assert {d.phase for d in ds if _is_rank(d.root)} == {"row"}
        assert {d.phase for d in ds if _is_r(d.root)} == {"combined"}
        assert not _roots(ds, _is_inner_cr)


# --------------------------------------------------------------------------- #
# Synthesized producer grain names — injective, key-derived.
# --------------------------------------------------------------------------- #
def _flag(column: str, value: str) -> ScalarCallKey:
    cond = ArithmeticKey(op="=", operands=(ColumnKey(leaf=column), LiteralKey(value=value)))
    return ScalarCallKey(name="iif", args=(cond, LiteralKey(value=1), LiteralKey(value=0)))


def _grain_names(keys) -> dict:
    """``{key: declared grain name}`` of a producer grouped by ``keys``."""
    models = dev1847_models()
    prebound, _ = _regroup_producer_prebound(
        pks=Grain(keys=frozenset(keys)),
        aggs=[AggregateKey(source=ColumnKey(leaf="amount"), agg="sum")],
        model=models[0], bundle=_bundle(), inherited=[], n_date_range=0,
    )
    dims = [dm for dm in prebound.declared_measures if dm.is_dimension]
    return {dm.bound.value_key: dm.public_name for dm in dims}


class TestSynthesizedGrainNames:
    def test_expression_keys_get_distinct_names(self):
        a, b = _flag("product", "P"), _flag("region", "North")
        names = _grain_names([a, b, ColumnKey(leaf="city")])
        assert len(set(names.values())) == 3
        assert names[ColumnKey(leaf="city")] == "city"

    def test_name_is_key_derived_not_encounter_order(self):
        a = _flag("product", "P")
        with_later = _grain_names([a, _flag("region", "North")])
        with_earlier = _grain_names([_flag("city", "Alpha"), a])
        assert len(set(with_later.values())) == len(set(with_earlier.values())) == 2
        assert with_later[a] == with_earlier[a]
