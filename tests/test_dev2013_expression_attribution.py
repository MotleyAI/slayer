"""Determination closed under row-level combination: re-aggregation outer
dimensions, parameter typing, and query-name diagnostics (SQLite + DuckDB).

Spec: openspec …/specs/queries/semantics — "Second-order aggregation over attached
values", "Aggregation parameters are typed by the home dataset's grain", "Loud
degradation".
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

import pytest

import slayer.core.refs as refs
from slayer.core.enums import DataType, TimeGranularity
from slayer.core.errors import ParameterGrainError, SlayerError
from slayer.core.keys import (
    KIND_POLICY,
    AggregateKey,
    ArithmeticKey,
    ColumnKey,
    ColumnSqlKey,
    Grain,
    InKey,
    LiteralKey,
    ScalarCallKey,
    SqlFragmentKey,
    StarKey,
    TimePointCmpKey,
    TimeTruncKey,
    TransformKey,
    ValueKey,
)
from slayer.core.models import Aggregation, AggregationParam, Column, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.join_safety import grain_determines

from tests import _dev1840_fixtures as f40
from tests._dev1847_fixtures import (
    AVG_CITY_TOTAL_BY_REGION,
    INNER_CR,
    ModelMeasure,
    associated_warnings,
    broadcast_warnings,
    chain_q,
    corders_model,
    customers_model,
    degenerate_warnings,
    dev1847_models,
    make_exec_engine,
    reagg,
    regions_model,
    rows_by,
    sales_model,
    sales_q,
)
from tests._engine_helpers import models_bundle, seeded_exec_engine

INNER_CUST = "sum(amount, partition_by=customer_id)"

#: avg(sum(amount, partition_by=[city, region])) by (region, city == 'Alpha').
ALPHA_CELLS = {
    ("North", True): 30.0, ("North", False): 60.0,
    ("South", True): 40.0, ("South", False): 100.0,
    ("East", False): 60.0, ("Gap", None): 12.0, ("Gap", False): 8.0,
    ("Void", False): None,
}
IIF_CELLS = {
    ("North", 1): 30.0, ("North", 0): 60.0, ("South", 1): 40.0, ("South", 0): 100.0,
    ("East", 0): 60.0, ("Gap", 0): 10.0, ("Void", 0): None,
}
UPPER_CELLS = {
    ("North", "ALPHA"): 30.0, ("North", "BETA"): 60.0,
    ("South", "ALPHA"): 40.0, ("South", "GAMMA"): 100.0,
    ("East", "DELTA"): 50.0, ("East", "EPSILON"): 50.0, ("East", "ZETA"): 80.0,
    ("Gap", None): 12.0, ("Gap", "KAPPA"): 8.0, ("Void", "XI"): None,
}
REGION_NORTH_CELLS = {
    ("North", True): 45.0, ("South", False): 70.0, ("East", False): 60.0,
    ("Gap", False): 10.0, ("Void", False): None,
}
IN_CELLS = {
    ("North", True): 45.0, ("South", True): 40.0, ("South", False): 100.0,
    ("East", False): 60.0, ("Gap", None): 12.0, ("Gap", False): 8.0,
    ("Void", False): None,
}
CASE_CELLS = {
    ("North", "a"): 30.0, ("North", "o"): 60.0, ("South", "a"): 40.0,
    ("South", "o"): 100.0, ("East", "o"): 60.0, ("Gap", "o"): 10.0, ("Void", "o"): None,
}
SINGLE_ALPHA = {(False,): 58.0, (True,): 35.0, (None,): 12.0}
P_REGION = "amount:sum(partition_by=region)"
IIF_P_CELLS = {
    ("North", 90.0): 30.0, ("North", 0.0): 60.0, ("South", 140.0): 40.0,
    ("South", 0.0): 100.0, ("East", 0.0): 60.0, ("Gap", 0.0): 10.0, ("Void", 0.0): None,
}
IIF_P1_CELLS = {
    ("North", 1.0): 30.0, ("North", 0.0): 60.0, ("South", 1.0): 40.0,
    ("South", 0.0): 100.0, ("East", 0.0): 60.0, ("Gap", 0.0): 10.0, ("Void", 0.0): None,
}
#: wsum(region_id, weight=region total) per rid10 cell: 1*70*2, 2*100.
RID10_WSUM = {(10,): 140.0, (20,): 200.0}
#: weighted_avg(city cells, weight=LENGTH(city)).
CITY_LEN_WAVG = {"North": 390.0 / 9.0, "South": 70.0, "East": 57.5, "Gap": 8.0,
                 "Void": None}


def _dim(expr: str, name: str) -> Dict[str, str]:
    return {"expression": expr, "name": name}


def _bool(v: Any) -> Optional[bool]:
    return None if v is None else bool(v)


def _num(v: Any) -> Optional[float]:
    return None if v is None else float(v)


def _cells(resp, keys: List[str], measure: str, norm=None) -> Dict[Tuple, Any]:
    norms = norm or [lambda v: v] * len(keys)
    return {
        tuple(n(k) for n, k in zip(norms, key)): row[measure]
        for key, row in rows_by(resp, *keys).items()
    }


def _assert_cells(actual: Dict[Tuple, Any], expected: Dict[Tuple, Any]) -> None:
    assert set(actual) == set(expected)
    for k, v in expected.items():
        if v is None:
            assert actual[k] is None, k
        else:
            assert float(actual[k]) == pytest.approx(v), k


def _no_grain_warnings(resp) -> None:
    assert broadcast_warnings(resp) == []
    assert associated_warnings(resp) == []


def _sales_variant() -> SlayerModel:
    s = sales_model()
    extra_cols = [
        Column(name="city_upper", type=DataType.TEXT, sql="UPPER(city)"),
        Column(name="city_len", type=DataType.INT, sql="LENGTH(city)"),
        Column(name="prod_flag", type=DataType.INT,
               sql="CASE WHEN product = 'P' THEN 1 ELSE 0 END"),
        Column(name="bad", type=DataType.INT, sql="amount +* )"),
        Column(name="city_q", type=DataType.TEXT, sql="city", filter="product = 'Q'"),
        Column(name="city_n", type=DataType.TEXT, sql="city", filter="region = 'North'"),
    ]
    wavg = "SUM({value} * {weight}) / SUM({weight})"
    extra_aggs = [
        Aggregation(name="wmix", formula=wavg,
                    params=[AggregationParam(name="weight", sql="LENGTH(city) + quantity")]),
        Aggregation(name="wok", formula=wavg,
                    params=[AggregationParam(name="weight",
                                             sql="LENGTH(city) + LENGTH(region)")]),
        Aggregation(name="wlit", formula=wavg,
                    params=[AggregationParam(name="weight", sql="2")]),
        Aggregation(name="wbad", formula=wavg,
                    params=[AggregationParam(name="weight", sql="bad + LENGTH(city)")]),
    ]
    return s.model_copy(update={
        "columns": [*s.columns, *extra_cols],
        "aggregations": [*s.aggregations, *extra_aggs],
    })


def _variant_models() -> List[SlayerModel]:
    c = customers_model()
    c = c.model_copy(update={"aggregations": [
        Aggregation(name="wsum", formula="SUM({value} * {weight})",
                    params=[AggregationParam(name="weight", sql="region_id")]),
    ]})
    return [_sales_variant(), regions_model(), c, corders_model()]


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    async for engine in make_exec_engine(request):
        yield engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def variant_engine(request):
    async for engine in make_exec_engine(request, models=_variant_models()):
        yield engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def fanning_engine(request):
    if request.param == "duckdb":
        pytest.importorskip("duckdb")
    seed = f40._seed_duckdb if request.param == "duckdb" else f40._seed_sqlite
    models = [f40.regions_model(), f40.plans_model(), f40.stores_model(),
              f40.customers_model(declare_reverse=True),
              f40.orders_model(customers_edge=False)]
    async with seeded_exec_engine(dialect=request.param, seed=seed, models=models) as (
        engine, _db,
    ):
        yield engine


def _acr(**kw) -> SlayerQuery:
    return sales_q(measures=[reagg("avg", INNER_CR, name="acr")], **kw)


def _acc(**kw) -> SlayerQuery:
    return chain_q(measures=[reagg("avg", INNER_CUST, name="acc")], **kw)


class TestRowLevelExpressionPartitions:
    """Scenario: Row-level expression over grain members partitions exactly."""

    @pytest.mark.parametrize("mode", ["broadcast", "error", "associate"])
    async def test_comparison_over_grain_member(self, exec_engine, mode):
        resp = await exec_engine.execute(_acr(
            dimensions=["region", _dim("city == 'Alpha'", "is_alpha")],
            to_many_handling=mode))
        _assert_cells(
            _cells(resp, ["sales.region", "sales.is_alpha"], "sales.acr",
                   norm=[str, _bool]),
            ALPHA_CELLS)
        _no_grain_warnings(resp)

    @pytest.mark.parametrize("expr,norm,expected", [
        ("iif(city == 'Alpha', 1, 0)", int, IIF_CELLS),
        ("upper(city)", lambda v: v, UPPER_CELLS),
        ("region == 'North'", _bool, REGION_NORTH_CELLS),
        ("city in ('Alpha', 'Beta')", _bool, IN_CELLS),
        ("CASE WHEN city = 'Alpha' THEN 'a' ELSE 'o' END", lambda v: v, CASE_CELLS),
    ])
    async def test_scalar_shapes(self, exec_engine, expr, norm, expected):
        resp = await exec_engine.execute(_acr(dimensions=["region", _dim(expr, "d")]))
        _assert_cells(
            _cells(resp, ["sales.region", "sales.d"], "sales.acr", norm=[str, norm]),
            expected)
        _no_grain_warnings(resp)

    async def test_expression_as_only_dimension(self, exec_engine):
        resp = await exec_engine.execute(
            _acr(dimensions=[_dim("city == 'Alpha'", "is_alpha")]))
        _assert_cells(
            _cells(resp, ["sales.is_alpha"], "sales.acr", norm=[_bool]), SINGLE_ALPHA)
        _no_grain_warnings(resp)

    @pytest.mark.parametrize("then,expected", [
        (P_REGION, IIF_P_CELLS),
        (f"{P_REGION} * 0 + 1", IIF_P1_CELLS),
    ])
    async def test_aggregate_carrying_controls(self, exec_engine, then, expected):
        resp = await exec_engine.execute(_acr(
            dimensions=["region", _dim(f"iif(city == 'Alpha', {then}, 0)", "ip")]))
        _assert_cells(
            _cells(resp, ["sales.region", "sales.ip"], "sales.acr", norm=[str, _num]),
            expected)
        _no_grain_warnings(resp)


class TestChainShapes:
    async def test_comparison_over_to_one_dimension(self, exec_engine):
        """Scenario: Expression over a to-one-determined dimension matches its plain spelling."""
        resp = await exec_engine.execute(_acc(
            dimensions=[_dim("customers.regions.name == 'North'", "is_north")]))
        _assert_cells(
            _cells(resp, ["corders.is_north"], "corders.acc", norm=[_bool]),
            {(True,): 35.0, (False,): 100.0})
        _no_grain_warnings(resp)

    async def test_arithmetic_over_fk(self, exec_engine):
        resp = await exec_engine.execute(
            _acc(dimensions=[_dim("customers.region_id * 10", "rid10")]))
        _assert_cells(
            _cells(resp, ["corders.rid10"], "corders.acc", norm=[int]),
            {(10,): 35.0, (20,): 100.0})
        _no_grain_warnings(resp)

    async def test_aggregate_carrying_dimension_grained_by_determined_key(self, exec_engine):
        """Scenario: Aggregate-carrying expression dimension grained by a determined key."""
        resp = await exec_engine.execute(_acc(dimensions=[
            _dim("sum(amount, partition_by=customers.regions.name) > 80", "big")]))
        _assert_cells(
            _cells(resp, ["corders.big"], "corders.acc", norm=[_bool]),
            {(False,): 35.0, (True,): 100.0})
        _no_grain_warnings(resp)


class TestAttachedParameterGrain:
    """Scenario: Attached parameter grained by an expression over home-determined columns."""

    @pytest.mark.parametrize("grain", ["rid10", "customers.region_id"])
    async def test_expression_grained_parameter(self, variant_engine, grain):
        resp = await variant_engine.execute(chain_q(
            dimensions=[_dim("customers.region_id * 10", "rid10")],
            measures=[ModelMeasure(
                formula=f"customers.region_id:wsum(weight=sum(amount, partition_by={grain}))",
                name="w")]))
        _assert_cells(
            _cells(resp, ["corders.rid10"], "corders.w", norm=[int]), RID10_WSUM)


class TestNegativeControls:
    def _is_p(self, mode: str = "broadcast") -> SlayerQuery:
        return _acr(dimensions=["region", _dim("product == 'P'", "is_p")],
                    to_many_handling=mode)

    async def test_undetermined_expression_broadcasts(self, exec_engine):
        """Scenario: Expression over an undetermined column still resolves per mode."""
        resp = await exec_engine.execute(self._is_p())
        cells = _cells(resp, ["sales.region", "sales.is_p"], "sales.acr", norm=[str, _bool])
        assert set(cells) == {
            ("North", True), ("North", False), ("South", True), ("South", False),
            ("East", True), ("East", False), ("Gap", True), ("Void", True)}
        for (region, _), value in cells.items():
            expected = AVG_CITY_TOTAL_BY_REGION.get(region)
            assert _num(value) == (None if expected is None else pytest.approx(expected))
        (w,) = broadcast_warnings(resp)
        (d,) = w.dimensions
        assert d.dimension == "is_p"
        assert "not determined by the operand grain" in d.reason
        assert "product" in d.reason
        assert "partition_by" in d.reason

    async def test_undetermined_expression_refuses_in_error_mode(self, exec_engine):
        query = self._is_p("error")
        with pytest.raises(SlayerError) as ei:
            await exec_engine.execute(query)
        assert "is_p" in str(ei.value)
        assert "grain_" not in str(ei.value)

    async def test_explicit_outer_partition_does_not_warn(self, exec_engine):
        resp = await exec_engine.execute(sales_q(
            dimensions=["region", _dim("city == 'Alpha'", "is_alpha")],
            measures=[reagg("avg", INNER_CR, name="acr", partition_by="region")]))
        cells = _cells(resp, ["sales.region", "sales.is_alpha"], "sales.acr",
                       norm=[str, _bool])
        assert set(cells) == set(ALPHA_CELLS)
        for (region, _), value in cells.items():
            expected = AVG_CITY_TOTAL_BY_REGION.get(region)
            assert _num(value) == (None if expected is None else pytest.approx(expected))
        _no_grain_warnings(resp)

    async def test_adding_the_reaggregation_is_cardinality_neutral(self, exec_engine):
        dims = ["region", _dim("city == 'Alpha'", "is_alpha")]
        tot = ModelMeasure(formula="amount:sum", name="tot")
        base = await exec_engine.execute(sales_q(dimensions=dims, measures=[tot]))
        both = await exec_engine.execute(sales_q(
            dimensions=dims, measures=[tot, reagg("avg", INNER_CR, name="acr")]))
        keys = ["sales.region", "sales.is_alpha"]
        assert _cells(base, keys, "sales.tot") == _cells(both, keys, "sales.tot")


class TestDiagnosticNaming:
    """Scenario: Diagnostics name dimensions by their query name."""

    def _joined(self, mode: str = "broadcast") -> SlayerQuery:
        return chain_q(
            dimensions=["customers.regions.name"], to_many_handling=mode,
            measures=[ModelMeasure(formula="avg(sum(amount, partition_by=amount))",
                                   name="a")])

    async def test_joined_dimension_broadcast_named_dotted(self, exec_engine):
        resp = await exec_engine.execute(self._joined())
        (w,) = broadcast_warnings(resp)
        assert [d.dimension for d in w.dimensions] == ["customers.regions.name"]
        assert "customers__regions__name" not in w.human_message()

    async def test_joined_dimension_error_named_dotted(self, exec_engine):
        query = self._joined("error")
        with pytest.raises(SlayerError) as ei:
            await exec_engine.execute(query)
        msg = str(ei.value)
        assert "customers.regions.name" in msg
        assert "customers__regions__name" not in msg

    def _dup(self, mode: str = "broadcast") -> SlayerQuery:
        return _acr(dimensions=["region", _dim("product == 'P'", "is_p"),
                                _dim("product == 'P'", "is_p2")], to_many_handling=mode)

    async def test_interned_aliases_named_by_first_declared(self, exec_engine):
        resp = await exec_engine.execute(self._dup())
        (w,) = broadcast_warnings(resp)
        assert [d.dimension for d in w.dimensions] == ["is_p"]

    async def test_interned_aliases_error_named_by_first_declared(self, exec_engine):
        query = self._dup("error")
        with pytest.raises(SlayerError) as ei:
            await exec_engine.execute(query)
        msg = str(ei.value)
        assert "is_p" in msg
        assert "grain_" not in msg

    async def test_unnamed_expression_never_internal_alias(self, exec_engine):
        resp = await exec_engine.execute(
            _acr(dimensions=["region", {"expression": "product == 'P'"}]))
        (w,) = broadcast_warnings(resp)
        (d,) = w.dimensions
        assert not d.dimension.startswith("grain_")
        assert "__" not in d.dimension

    async def test_degenerate_warning_grains_dotted(self, exec_engine):
        resp = await exec_engine.execute(chain_q(
            dimensions=["customers.regions.name"],
            measures=[ModelMeasure(formula="avg(sum(amount))", name="a")]))
        (w,) = degenerate_warnings(resp)
        msg = w.human_message()
        assert "customers.regions.name" in msg
        assert "__" not in msg

    async def test_parameter_grain_listing_dotted(self, exec_engine):
        query = chain_q(
            dimensions=["customers.regions.name"],
            measures=[ModelMeasure(
                formula="weighted_avg(sum(amount, partition_by=customers.regions.name), "
                        "weight=id)", name="w")])
        with pytest.raises(ParameterGrainError) as ei:
            await exec_engine.execute(query)
        msg = str(ei.value)
        assert "customers.regions.name" in msg
        assert "customers__regions__name" not in msg


class TestBroadcastReasons:
    async def test_undetermined_to_one_column_names_it(self, exec_engine):
        resp = await exec_engine.execute(chain_q(
            dimensions=["customers.regions.name"],
            measures=[ModelMeasure(formula="avg(sum(amount, partition_by=amount))",
                                   name="a")]))
        (w,) = broadcast_warnings(resp)
        (d,) = w.dimensions
        assert "not determined by the operand grain" in d.reason
        assert "customers.regions.name" in d.reason

    async def test_fanning_witness_names_the_hop(self, fanning_engine):
        resp = await fanning_engine.execute(SlayerQuery.model_validate({
            "source_model": "customers",
            "dimensions": ["tier", _dim("last_status == 'ok'", "lok")],
            "measures": [{"formula": "avg(sum(spend, partition_by=[tier, region_id]))",
                          "name": "a"}]}))
        (w,) = broadcast_warnings(resp)
        (d,) = w.dimensions
        assert d.dimension == "lok"
        assert "orders" in d.reason
        assert "fanning" in d.reason

    async def test_unsupported_kind_cannot_be_analysed(self, exec_engine):
        resp = await exec_engine.execute(_acr(dimensions=[
            "region", _dim("rank(sum(amount, partition_by=[city, region]), direction='desc')", "rk")]))
        (w,) = broadcast_warnings(resp)
        (d,) = w.dimensions
        assert d.dimension == "rk"
        assert "cannot be analysed" in d.reason

    async def test_unanalysable_derived_dimension_refuses_in_error_mode(self, variant_engine):
        query = _acr(dimensions=["region", "bad"], to_many_handling="error")
        with pytest.raises(SlayerError) as ei:
            await variant_engine.execute(query)
        assert "bad" in str(ei.value)


class TestDefinitionDefaultParameters:
    async def test_one_undetermined_ref_refuses(self, variant_engine):
        query = sales_q(dimensions=["region"], measures=[reagg("wmix", INNER_CR, name="w")])
        with pytest.raises(ParameterGrainError) as ei:
            await variant_engine.execute(query)
        assert "weight" in str(ei.value)

    async def test_unanalysable_ref_refuses(self, variant_engine):
        query = sales_q(dimensions=["region"], measures=[reagg("wbad", INNER_CR, name="w")])
        with pytest.raises(ParameterGrainError):
            await variant_engine.execute(query)

    async def test_all_refs_determined_executes(self, variant_engine):
        resp = await variant_engine.execute(sales_q(
            dimensions=["region"], measures=[reagg("wok", INNER_CR, name="w")]))
        vals = _cells(resp, ["sales.region"], "sales.w")
        assert float(vals[("North",)]) == pytest.approx(840.0 / 19.0)
        assert float(vals[("South",)]) == pytest.approx(70.0)

    async def test_literal_default_is_determined(self, variant_engine):
        resp = await variant_engine.execute(sales_q(
            dimensions=["region"], measures=[reagg("wlit", INNER_CR, name="w")]))
        vals = _cells(resp, ["sales.region"], "sales.w")
        for region, expected in AVG_CITY_TOTAL_BY_REGION.items():
            assert float(vals[(region,)]) == pytest.approx(expected)


class TestDerivedColumnSpelling:
    """Scenarios: Derived column over grain members partitions like its inline
    spelling; Derived parameter over grain members is determined."""

    async def test_derived_dimension_matches_inline(self, variant_engine):
        derived = await variant_engine.execute(
            _acr(dimensions=["region", "city_upper"]))
        inline = await variant_engine.execute(
            _acr(dimensions=["region", _dim("upper(city)", "cu")]))
        derived_cells = _cells(derived, ["sales.region", "sales.city_upper"], "sales.acr")
        _assert_cells(derived_cells, UPPER_CELLS)
        assert derived_cells == _cells(inline, ["sales.region", "sales.cu"], "sales.acr")
        _no_grain_warnings(derived)

    async def test_derived_parameter_executes(self, variant_engine):
        resp = await variant_engine.execute(sales_q(
            dimensions=["region"],
            measures=[ModelMeasure(formula=f"weighted_avg({INNER_CR}, weight=city_len)",
                                   name="w")]))
        _assert_cells(
            _cells(resp, ["sales.region"], "sales.w"),
            {(r,): v for r, v in CITY_LEN_WAVG.items()})

    async def test_undetermined_derived_dimension_broadcasts(self, variant_engine):
        resp = await variant_engine.execute(_acr(dimensions=["region", "prod_flag"]))
        (w,) = broadcast_warnings(resp)
        assert [d.dimension for d in w.dimensions] == ["prod_flag"]

    async def test_undetermined_derived_parameter_refuses(self, variant_engine):
        query = sales_q(
            dimensions=["region"],
            measures=[ModelMeasure(
                formula=f"weighted_avg({INNER_CR}, weight=prod_flag)", name="w")])
        with pytest.raises(ParameterGrainError):
            await variant_engine.execute(query)


# --- combinator units ------------------------------------------------------

CITY, REGION, PRODUCT = ColumnKey(leaf="city"), ColumnKey(leaf="region"), ColumnKey(leaf="product")
CR = Grain.of([CITY, REGION])


def _lit(v) -> LiteralKey:
    return LiteralKey(value=Decimal(v) if isinstance(v, int) else v)


def _eq(a: ValueKey, b: ValueKey) -> ArithmeticKey:
    return ArithmeticKey(op="==", operands=(a, b))


def _sum(by: Optional[List[ValueKey]]) -> AggregateKey:
    return AggregateKey(source=ColumnKey(leaf="amount"), agg="sum",
                        partition_keys=None if by is None else Grain.of(by))


def _determines(key: ValueKey, grain: Grain = CR, models=None, host: str = "sales") -> bool:
    mbn = {m.name: m for m in (models or [_sales_variant(), *dev1847_models()[1:]])}
    return grain_determines(key=key, grain=grain, host_model=mbn[host],
                            models_by_name=mbn, bundle=models_bundle(mbn))


def _derived(name: str) -> ColumnSqlKey:
    return ColumnSqlKey(model="sales", column_name=name)


class TestCombinator:
    @pytest.mark.parametrize("key,expected", [
        (_lit("x"), True),
        (TimeTruncKey(column=CITY, granularity=TimeGranularity.MONTH), True),
        (TimeTruncKey(column=PRODUCT, granularity=TimeGranularity.MONTH), False),
        (_eq(CITY, _lit("Alpha")), True),
        (_eq(PRODUCT, _lit("P")), False),
        (ArithmeticKey(op="+", operands=(CITY, PRODUCT)), False),
        (ScalarCallKey(name="upper", args=(CITY,)), True),
        (ScalarCallKey(name="iif", args=(_eq(CITY, _lit("Alpha")), REGION, PRODUCT)), False),
        (InKey(column=CITY, values=(_lit("Alpha"),)), True),
        (InKey(column=PRODUCT, values=(_lit("P"),)), False),
        (SqlFragmentKey(template="LENGTH({r0}) + LENGTH({r1})", refs=(CITY, REGION)), True),
        (SqlFragmentKey(template="LENGTH({r0}) + {r1}",
                        refs=(CITY, ColumnKey(leaf="quantity"))), False),
        (SqlFragmentKey(template="2"), True),
        (ScalarCallKey(name="iif", args=(
            _eq(ScalarCallKey(name="upper", args=(CITY,)), _lit("ALPHA")), REGION, _lit("x"))),
         True),
        (_sum([CITY]), True),
        (_sum([PRODUCT]), False),
        (_sum(None), False),
        (ArithmeticKey(op=">", operands=(_sum([REGION]), _lit(80))), True),
        (ArithmeticKey(op=">", operands=(_sum([PRODUCT]), _lit(80))), False),
        (ArithmeticKey(op="+", operands=(CITY, StarKey())), False),
        (StarKey(), False),
        (TransformKey(op="rank", input=_sum([CITY, REGION])), False),
        (ArithmeticKey(op="+", operands=(
            CITY, TransformKey(op="rank", input=_sum([CITY, REGION])))), False),
    ])
    def test_kind(self, key, expected):
        assert _determines(key) is expected

    def test_mixed_fanning_child(self):
        models = [f40.regions_model(), f40.plans_model(), f40.stores_model(),
                  f40.customers_model(declare_reverse=True),
                  f40.orders_model(customers_edge=False)]
        tier_gold = _eq(ColumnKey(leaf="tier"), _lit("gold"))
        last_ok = _eq(ColumnSqlKey(model="customers", column_name="last_status"), _lit("ok"))
        grain = Grain.of([ColumnKey(leaf="id")])
        assert _determines(tier_gold, grain, models, "customers") is True
        assert _determines(ArithmeticKey(op="and", operands=(tier_gold, last_ok)),
                           grain, models, "customers") is False

    def test_expression_member_determined_as_itself(self):
        alpha = _eq(CITY, _lit("Alpha"))
        grain = Grain.of([alpha])
        assert _determines(alpha, grain) is True
        assert _determines(ScalarCallKey(name="iif", args=(alpha, _lit(1), _lit(0))), grain) is True
        assert _determines(CITY, grain) is False

    @pytest.mark.parametrize("member", [
        TransformKey(op="rank", input=_sum([CITY, REGION])), StarKey(),
    ])
    def test_fail_closed_kind_member_determined_at_every_node(self, member):
        grain = Grain.of([member])
        assert _determines(member, grain) is True
        assert _determines(ArithmeticKey(op="+", operands=(member, _lit(1))), grain) is True

    def test_finer_bucket_does_not_determine_coarser(self):
        day = TimeTruncKey(column=CITY, granularity=TimeGranularity.DAY)
        month = TimeTruncKey(column=CITY, granularity=TimeGranularity.MONTH)
        assert _determines(month, Grain.of([day])) is False

    @pytest.mark.parametrize("name,expected", [
        ("city_upper", True), ("city_len", True),
        ("prod_flag", False), ("q_amount", False), ("bad", False),
        ("city_q", False), ("city_n", True),
    ])
    def test_derived_column(self, name, expected):
        assert _determines(_derived(name)) is expected

    def test_unanalysable_definition_inside_composite(self):
        assert _determines(ArithmeticKey(op="+", operands=(CITY, _derived("bad")))) is False


# --- key_display ------------------------------------------------------------

NAME = ColumnKey(path=("customers", "regions"), leaf="name")
DISPLAY_CASES: List[Tuple[ValueKey, List[str]]] = [
    (NAME, ["customers.regions.name"]),
    (ColumnSqlKey(path=("customers",), model="customers", column_name="rid10"),
     ["customers.rid10"]),
    (TimeTruncKey(column=ColumnKey(leaf="ordered_at"), granularity=TimeGranularity.MONTH),
     ["ordered_at", "month"]),
    (StarKey(), ["*"]),
    (_lit("Alpha"), ["'Alpha'"]),
    (_sum([CITY, REGION]), ["sum", "amount", "partition_by", "city", "region"]),
    (TransformKey(op="rank", input=_sum([CITY])), ["rank", "sum", "amount"]),
    (_eq(CITY, _lit("Alpha")), ["city == 'Alpha'"]),
    (ScalarCallKey(name="upper", args=(CITY,)), ["upper(city)"]),
    (TimePointCmpKey(op=">=", operand=ColumnKey(leaf="ordered_at"), point="last month"),
     ["ordered_at >= 'last month'"]),
    (InKey(column=CITY, values=(_lit("Alpha"), _lit("Beta"))),
     ["city", " in ", "'Alpha'", "'Beta'"]),
    (SqlFragmentKey(template="LENGTH({r0}) + {r1}", refs=(CITY, ColumnKey(leaf="quantity"))),
     ["LENGTH(city) + quantity"]),
]
_REPR_MARKERS = ("Key(", "path=", "leaf=", "operands=", "op=", "refs=", "template=")


class TestKeyDisplay:
    def test_cases_cover_every_kind(self):
        assert {type(k) for k, _ in DISPLAY_CASES} == set(KIND_POLICY)

    @pytest.mark.parametrize("key,parts", DISPLAY_CASES)
    def test_formula_text(self, key, parts):
        text = refs.key_display(key)
        for p in parts:
            assert p.lower() in text.lower()
        assert not any(m in text for m in _REPR_MARKERS)
        assert "{r" not in text

    def test_negated_in(self):
        text = refs.key_display(InKey(column=CITY, values=(_lit("A"),), negated=True))
        assert "not in" in text.lower()

    @pytest.mark.parametrize("key,_parts", DISPLAY_CASES)
    def test_dotted_key_display_never_repr(self, key, _parts):
        text = refs.dotted_key_display(key)
        assert not any(m in text for m in _REPR_MARKERS)
