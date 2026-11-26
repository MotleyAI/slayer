"""An aggregate with no ``partition_by=`` has the query grain wherever it stands (spec: queries/transforms).

City seed, one dataset ``orders``: North/A 10 (Jan) and 25 (Feb), North/B 30 and 40, South/C 50 (Jan),
South/D 20 and 40 (2025). City totals A 35, B 70, C 50, D 60; region totals North 105, South 110.
Cross-dataset cases run on the Appendix B seed of ``_dev2212_fixtures``.
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncGenerator, Callable
from typing import Any, Optional

import pytest
from pydantic import ValidationError

from slayer.core.enums import DataType
from slayer.core.errors import ComputedDimensionError, PartitionKeyError, ReaggregationError, SlayerError
from slayer.core.keys import (
    NON_IDENTITY_KEY_FIELDS,
    AggregateKey,
    ArithmeticKey,
    FieldKey,
    Grain,
    TransformKey,
    ValueKey,
    effective_root_grain,
    reroot_value_key,
    substitute_value_keys,
    transform_operand_grain,
    walk_value_keys,
)
from slayer.core.models import DatasetField, DatasetMetric, SlayerDataset
from slayer.core.query import QueryMetric, SlayerQuery
from slayer.core.refs import key_display
from slayer.engine.compile.stages import _strip_redundant_partitions, _substitute_prebound
from slayer.engine.elaborate import elaborate_query
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse
from slayer.ir.elaborated import ElaboratedStage
from slayer.ir.source_bundle import ResolvedSourceBundle

from tests._dev1737_fixtures import TableSpec, seed_backend
from tests._dev1832_fixtures import TimeDimension, TimeGranularity, monthly_q
from tests._dev1832_fixtures import make_exec_engine as make_monthly_engine
from tests._dev1832_fixtures import month_td as monthly_month_td
from tests._dev2212_fixtures import BACKENDS, DS, kinds, run, seed_engine, warned_metrics
from tests._engine_helpers import seeded_exec_engine

R, C, MONTH = "orders.region", "orders.city", "orders.ordered_at"
CR, CS = "customers.region", "customers.segment"
RC, RM, RCM = (R, C), (R, MONTH), (R, C, MONTH)
TD = [{"field": MONTH, "granularity": "month"}]
JAN, FEB = "2025-01", "2025-02"

A = "orders.amount"
BARE = f"sum({A})"
BY_REGION = f"sum({A}, partition_by={R})"
AT_RC = f"sum({A}, partition_by=[{R}, {C}])"
AT_RM = f"sum({A}, partition_by=[{R}, {MONTH}])"
AT_RCM = f"sum({A}, partition_by=[{R}, {C}, {MONTH}])"
WIN = f"sum({A}, window='2m')"
WIN_AT_RM = f"sum({A}, window='2m', partition_by=[{R}, {MONTH}])"
SHARE = f"{BARE} / {BY_REGION}"

X_BARE = "sum(orders.amount)"
X_BY_REGION = f"sum(orders.amount, partition_by={CR})"
X_AT_RS = f"sum(orders.amount, partition_by=[{CR}, {CS}])"
X_AT_RM = f"sum(orders.amount, partition_by=[{CR}, {MONTH}])"

Template = Callable[[str], str]

CITY_ORDERS = TableSpec(
    name="orders",
    columns=[("id", "INT"), ("customer_id", "INT"), ("region", "TEXT"), ("city", "TEXT"), ("status", "TEXT"),
             ("amount", "DOUBLE"), ("ordered_at", "DATE")],
    rows=[(1, 1, "North", "A", "ok", 10.0, "2025-01-10"), (2, 1, "North", "A", "new", 25.0, "2025-02-10"),
          (3, 2, "North", "B", "ok", 30.0, "2025-01-11"), (4, 2, "North", "B", "new", 40.0, "2025-02-11"),
          (5, 3, "South", "C", "ok", 50.0, "2025-01-12"), (6, 4, "South", "D", "ok", 20.0, "2025-01-13"),
          (7, 4, "South", "D", "new", 40.0, "2025-02-13")],
)


def city_dataset() -> SlayerDataset:
    def f(name: str, datatype: DataType, *, pk: bool = False) -> DatasetField:
        return DatasetField(name=name, datatype=datatype, primary_key=pk)

    return SlayerDataset(
        name="orders", sql_table="orders", data_source=DS,
        fields=[f("id", DataType.INTEGER, pk=True), f("customer_id", DataType.INTEGER), f("region", DataType.STRING),
                f("city", DataType.STRING), f("status", DataType.STRING), f("amount", DataType.FLOAT),
                f("ordered_at", DataType.DATE)],
        metrics=[DatasetMetric(name="share", expression="sum(amount) / sum(amount, partition_by=region)"),
                 DatasetMetric(name="share_at_city",
                               expression="sum(amount, partition_by=[region, city]) / sum(amount, partition_by=region)")],
    )


@pytest.fixture(params=BACKENDS)
def backend(request: pytest.FixtureRequest) -> str:
    return request.param


@contextlib.asynccontextmanager
async def city_engine(backend: str) -> AsyncGenerator[SlayerQueryEngine]:
    async with seeded_exec_engine(dialect=backend, seed=lambda p: seed_backend(backend, p, [CITY_ORDERS]),
                                  datasets=[city_dataset()]) as (eng, _):
        yield eng


def _mt(expr: str, alias: str = "m") -> dict[str, str]:
    return {"metric": expr, "alias": alias}


def _num(v: Any) -> Any:
    return float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else v


def _by(resp: SlayerResponse, keys: tuple[str, ...], name: str = "m") -> dict:
    """``{key or key tuple: value}``; a time key reads as ``YYYY-MM``."""
    def cell(row: dict) -> Any:
        parts = tuple(None if row[k] is None else str(row[k])[:7] if k == MONTH else _num(row[k]) for k in keys)
        return parts[0] if len(parts) == 1 else parts
    out = {cell(r): _num(r[name]) for r in resp.data}
    assert len(out) == len(resp.data), resp.data
    return out


def _assert_cells(got: dict, expected: dict) -> None:
    assert set(got) == set(expected), (got, expected)
    for key, value in expected.items():
        assert got[key] == (None if value is None else pytest.approx(value, abs=1e-3)), (key, got[key], value)


async def _city(backend: str, keys: tuple[str, ...], *metrics: dict, **kw: Any) -> SlayerResponse:
    dims = [k for k in keys if k != MONTH]
    if MONTH in keys:
        kw["time_dimensions"] = TD
    async with city_engine(backend) as eng:
        return await run(eng, population="orders", dimensions=dims, metrics=list(metrics), **kw)


async def _assert_spellings(backend: str, keys: tuple[str, ...], template: Template, *, explicit: str,
                            expected: dict, implicit: str = BARE) -> None:
    """``template`` over the ungrained aggregate reads ``expected``, as over its explicit-grain spelling."""
    written = _by(await _city(backend, keys, _mt(template(explicit))), keys)
    _assert_cells(written, expected)
    _assert_cells(_by(await _city(backend, keys, _mt(template(implicit))), keys), expected)


def _cities(a: Any, b: Any, c: Any, d: Any) -> dict:
    return {("North", "A"): a, ("North", "B"): b, ("South", "C"): c, ("South", "D"): d}


def _region_months(n_jan: Any, n_feb: Any, s_jan: Any, s_feb: Any) -> dict:
    return {("North", JAN): n_jan, ("North", FEB): n_feb, ("South", JAN): s_jan, ("South", FEB): s_feb}


def _city_months(*values: Any) -> dict:
    cells = [("North", "A", JAN), ("North", "A", FEB), ("North", "B", JAN), ("North", "B", FEB),
             ("South", "C", JAN), ("South", "D", JAN), ("South", "D", FEB)]
    return dict(zip(cells, values, strict=True))


# --------------------------------------------------------------------------- #
# A transform over a mixed operand evaluates at the query grain
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(("template", "expected"), [
    (lambda n: f"rank({n} / {BY_REGION}, direction='desc')", _cities(4, 1, 3, 2)),
    (lambda n: f"dense_rank({n} / {BY_REGION}, direction='desc')", _cities(4, 1, 3, 2)),
    (lambda n: f"percent_rank({n} / {BY_REGION})", _cities(0, 1, 1 / 3, 2 / 3)),
    (lambda n: f"ntile({n} / {BY_REGION}, n=2)", _cities(1, 2, 1, 2)),
], ids=["rank", "dense_rank", "percent_rank", "ntile"])
async def test_the_rank_family_ranks_the_query_cells(backend: str, template: Template, expected: dict) -> None:
    await _assert_spellings(backend, RC, template, explicit=AT_RC, expected=expected)


async def test_the_rank_as_a_filter(backend: str) -> None:
    for share in (SHARE, f"{AT_RC} / {BY_REGION}"):
        resp = await _city(backend, RC, _mt(BARE), filter=[f"rank({share}, direction='desc') = 1"])
        assert _by(resp, RC) == {("North", "B"): 70.0}


async def test_the_rank_as_an_order_key(backend: str) -> None:
    for share in (SHARE, f"{AT_RC} / {BY_REGION}"):
        resp = await _city(backend, RC, _mt(BARE),
                           order=[{"field": f"rank({share}, direction='desc')", "direction": "asc"}])
        assert [r[C] for r in resp.data] == ["B", "D", "C", "A"]


@pytest.mark.parametrize(("template", "implicit", "explicit", "expected"), [
    (lambda n: f"rank({n} - {BY_REGION}, direction='desc')", BARE, AT_RC, _cities(4, 1, 3, 2)),
    (lambda n: f"rank({n}, direction='desc')", "orders.share", "orders.share_at_city", _cities(4, 1, 3, 2)),
    # counts 2, 2, 1, 2 over region totals 105 and 110
    (lambda n: f"rank({n} / {BY_REGION}, direction='desc')", "count(orders.*)",
     f"count(orders.*, partition_by=[{R}, {C}])", _cities(1, 1, 4, 3)),
    (lambda n: f"rank({n} / {BY_REGION} + sum({A}, partition_by=[]), direction='desc')", BARE, AT_RC,
     _cities(4, 1, 3, 2)),
], ids=["minus", "dataset-metric", "count", "three-way"])
async def test_other_spellings_of_the_mixed_operand(backend: str, template: Template, implicit: str, explicit: str,
                                                    expected: dict) -> None:
    await _assert_spellings(backend, RC, template, implicit=implicit, explicit=explicit, expected=expected)


@pytest.mark.parametrize(("template", "expected"), [
    (lambda n: f"rank({n} / {BY_REGION}, direction='desc')", _city_months(7, 5, 4, 2, 1, 6, 3)),
    (lambda n: f"rank({n} / {BY_REGION}, partition_by={R}, direction='desc')", _city_months(4, 3, 2, 1, 1, 3, 2)),
], ids=["all-cells", "within-region"])
async def test_a_month_bucket_joins_the_query_grain(backend: str, template: Template, expected: dict) -> None:
    await _assert_spellings(backend, RCM, template, explicit=AT_RCM, expected=expected)


@pytest.mark.parametrize(("template", "expected"), [
    (lambda n: f"cumsum({n} / {BY_REGION})", _region_months(40 / 105, 1, 70 / 110, 1)),
    (lambda n: f"lag({n} / {BY_REGION})", _region_months(None, 40 / 105, None, 70 / 110)),
    (lambda n: f"lead({n} / {BY_REGION})", _region_months(65 / 105, None, 40 / 110, None)),
    (lambda n: f"consecutive_periods({n} / {BY_REGION} > 0.5)", _region_months(0, 1, 1, 0)),
], ids=["cumsum", "lag", "lead", "consecutive_periods"])
async def test_time_ordered_transforms_over_a_mixed_operand(backend: str, template: Template, expected: dict) -> None:
    await _assert_spellings(backend, RM, template, explicit=AT_RM, expected=expected)


@pytest.mark.parametrize(("template", "expected"), [
    # running totals 40, 105, 70, 110 over region totals 105 and 110
    (lambda n: f"rank(cumsum({n}) / {BY_REGION}, direction='desc')", _region_months(4, 1, 3, 1)),
    # share ranks 3, 2, 1, 4 accumulated per region
    (lambda n: f"cumsum(rank({n} / {BY_REGION}, direction='desc'))", _region_months(3, 5, 1, 5)),
], ids=["rank-of-cumsum", "cumsum-of-rank"])
async def test_nested_transforms_over_a_mixed_operand(backend: str, template: Template, expected: dict) -> None:
    await _assert_spellings(backend, RM, template, explicit=AT_RM, expected=expected)


@pytest.mark.parametrize(("template", "explicit", "expected"), [
    (lambda n: f"time_shift({n} / {BY_REGION}, -1)", AT_RM, _region_months(None, 40 / 105, None, 70 / 110)),
    (lambda n: f"change({n} / {BY_REGION})", AT_RM, _region_months(None, 25 / 105, None, -30 / 110)),
    (lambda n: f"change_pct({n} / {BY_REGION})", AT_RM, _region_months(None, 0.625, None, -3 / 7)),
    (lambda n: f"first({n} / {BY_REGION})", AT_RM, _region_months(40 / 105, 40 / 105, 70 / 110, 70 / 110)),
    (lambda n: f"last({n} / {BY_REGION})", AT_RM, _region_months(65 / 105, 65 / 105, 40 / 110, 40 / 110)),
    # cell totals 40, 65, 70, 40 over the grand total
    (lambda n: f"rank({n} / sum({A}, partition_by=[]), direction='desc')", AT_RM, _region_months(3, 2, 1, 3)),
], ids=["time_shift", "change", "change_pct", "first", "last", "scalar-denominator"])
async def test_shapes_that_already_work_keep_their_values(backend: str, template: Template, explicit: str,
                                                          expected: dict) -> None:
    await _assert_spellings(backend, RM, template, explicit=explicit, expected=expected)


async def test_a_top_level_windowed_numerator_keeps_its_values(backend: str) -> None:
    # 2-month totals 40, 105, 70, 110 over region totals 105 and 110
    await _assert_spellings(backend, RM, lambda n: f"rank({n} / {BY_REGION}, direction='desc')", implicit=WIN,
                            explicit=WIN_AT_RM, expected=_region_months(4, 1, 3, 1))


async def test_a_shifted_operand_with_a_dimension_leaf_keeps_its_values(backend: str) -> None:
    expr = f"time_shift({AT_RM} * iif({R} = 'North', 1, 2), -1)"
    _assert_cells(_by(await _city(backend, RM, _mt(expr)), RM), _region_months(None, 40, None, 140))


async def test_a_row_level_leaf_beside_a_coarser_aggregate(backend: str) -> None:
    expr = f"rank({BY_REGION} * iif({C} = 'A', 1, 2), direction='desc')"
    _assert_cells(_by(await _city(backend, RC, _mt(expr)), RC), _cities(4, 3, 1, 1))


# --------------------------------------------------------------------------- #
# Nested layers: second-order aggregations, finer operands, windowed inners
# --------------------------------------------------------------------------- #
async def test_a_second_order_aggregation_under_a_transform(backend: str) -> None:
    # the largest city total is 70 in North and 60 in South
    await _assert_spellings(backend, RC, lambda n: f"rank(max({n}, partition_by={R}), direction='desc')",
                            explicit=AT_RC, expected=_cities(1, 1, 2, 2))


async def test_a_cross_dataset_second_order_aggregation_under_a_transform(backend: str) -> None:
    expected = {("EAST", "corporate"): 1.0, ("EAST", "retail"): 1.0, ("WEST", "retail"): 1.0, ("NORTH", "retail"): None}
    async with seed_engine(backend) as eng:
        for inner in (X_AT_RS, X_BARE):
            resp = await run(eng, population="customers", dimensions=[CR, CS],
                             metrics=[_mt(f"rank(max({inner}, partition_by={CR}), direction='desc')")])
            assert _by(resp, (CR, CS)) == expected, inner


async def test_a_finer_operand_with_a_windowed_inner_as_an_aggregation_source(backend: str) -> None:
    # 2-month region totals over city-month totals rank A-Feb 1, A-Jan 2, D-Jan 3, D-Feb 4, B-Feb 5, C-Jan 6, B-Jan 7
    await _assert_spellings(backend, RM, lambda n: f"max(rank({n} / {AT_RCM}, direction='desc'))", implicit=WIN,
                            explicit=WIN_AT_RM, expected=_region_months(7, 5, 6, 4))


async def test_a_finer_operand_with_a_windowed_inner_under_last(backend: str) -> None:
    # each city's last ratio: A 105/25, B 105/40, C 70/50, D 110/40
    await _assert_spellings(backend, RM, lambda n: f"max(last({n} / {AT_RCM}))", implicit=WIN, explicit=WIN_AT_RM,
                            expected=_region_months(4.2, 4.2, 2.75, 2.75))


async def test_a_finer_operand_with_a_cross_dataset_inner_as_an_aggregation_source(backend: str) -> None:
    # region totals 350 and 300 over (EAST, retail) 300, (EAST, corporate) 50, (WEST, retail) 300
    async with seed_engine(backend) as eng:
        for inner in (X_BY_REGION, X_BARE):
            resp = await run(eng, population="customers", dimensions=[CR],
                             metrics=[_mt(f"max(rank({inner} / {X_AT_RS}, direction='desc'))")])
            assert _by(resp, (CR,)) == {"EAST": 2.0, "WEST": 3.0, "NORTH": None}, inner


async def test_a_finer_operand_with_a_cross_dataset_inner_under_last(backend: str) -> None:
    finer = f"sum(orders.amount, partition_by=[{CR}, {CS}, {MONTH}])"
    expected = {("EAST", JAN): 3.0, ("EAST", FEB): 1.0, ("WEST", FEB): 1.0, ("NORTH", None): None}
    async with seed_engine(backend) as eng:
        for inner in (X_AT_RM, X_BARE):
            resp = await run(eng, population="customers", dimensions=[CR], time_dimensions=TD,
                             metrics=[_mt(f"max(last({inner} / {finer}))")])
            _assert_cells(_by(resp, (CR, MONTH)), expected)


async def test_a_windowed_inner_nested_in_transforms_inside_an_aggregation(backend: str) -> None:
    # running 2-month totals 40, 145, 70, 180 over region totals 105 and 110
    await _assert_spellings(backend, RM, lambda n: f"max(rank(cumsum({n}) / {BY_REGION}, direction='desc'))",
                            implicit=WIN, explicit=WIN_AT_RM, expected=_region_months(4, 2, 3, 1))


async def test_a_windowed_inner_under_last_inside_an_aggregation(backend: str) -> None:
    await _assert_spellings(backend, RM, lambda n: f"max(last({n}))", implicit=WIN, explicit=WIN_AT_RM,
                            expected=_region_months(105, 105, 110, 110))


@pytest.fixture(params=BACKENDS)
async def monthly_engine(request: pytest.FixtureRequest) -> AsyncGenerator[SlayerQueryEngine]:
    async for engine in make_monthly_engine(request):
        yield engine


@pytest.mark.parametrize("month_first", [True, False], ids=["month-year", "year-month"])
async def test_two_time_buckets_are_both_in_the_defaulted_grain(monthly_engine, month_first: bool) -> None:
    year = TimeDimension(field="monthly.ordered_at", granularity=TimeGranularity.YEAR)
    month = monthly_month_td("monthly")
    resp = await monthly_engine.execute(monthly_q(
        metrics=[QueryMetric(metric="sum(rank(sum(monthly.amount), direction='desc'))", alias="m")],
        time_dimensions=[*month, year] if month_first else [year, *month]))
    # monthly totals 15, 35, 30
    assert {str(r["monthly.ordered_at.month"])[:7]: r["m"] for r in resp.data} == {
        "2024-01": 3, "2024-02": 1, "2024-03": 2}


# --------------------------------------------------------------------------- #
# Cross-dataset shares
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("population", [None, "customers"])
async def test_a_cross_dataset_share_ranks_at_the_query_grain(backend: str, population: Optional[str]) -> None:
    # shares: (EAST, corporate) 50/350, (EAST, retail) 300/350, (WEST, retail) 300/300
    expected: dict[tuple, Optional[float]] = {
        ("EAST", "corporate"): 3.0, ("EAST", "retail"): 2.0, ("WEST", "retail"): 1.0}
    kw: dict[str, Any] = {"dimensions": [CR, CS]}
    if population is not None:
        kw["population"] = population
        expected[("NORTH", "retail")] = None
    async with seed_engine(backend) as eng:
        for inner in (X_AT_RS, X_BARE):
            resp = await run(eng, metrics=[_mt(f"rank({inner} / {X_BY_REGION}, direction='desc')")], **kw)
            assert _by(resp, (CR, CS)) == expected, inner


async def test_a_difference_across_two_datasets_ranks_at_the_query_grain(backend: str) -> None:
    # orders minus the region's returns: (EAST, corporate) 50 - 25, (EAST, retail) 300 - 25, WEST has no returns
    expected = {("EAST", "corporate"): 2.0, ("EAST", "retail"): 1.0, ("WEST", "retail"): None}
    async with seed_engine(backend) as eng:
        for inner in (X_AT_RS, X_BARE):
            resp = await run(eng, dimensions=[CR, CS], metrics=[_mt(
                f"rank({inner} - sum(returns.amount, partition_by={CR}), direction='desc')")])
            assert _by(resp, (CR, CS)) == expected, inner


async def test_a_running_cross_dataset_share(backend: str) -> None:
    # EAST 150 then 200 of 350, WEST 300 of 300
    expected = {("EAST", JAN): 150 / 350, ("EAST", FEB): 1.0, ("WEST", FEB): 1.0}
    async with seed_engine(backend) as eng:
        for inner in (X_AT_RM, X_BARE):
            resp = await run(eng, dimensions=[CR], time_dimensions=TD,
                             metrics=[_mt(f"cumsum({inner} / {X_BY_REGION})")])
            _assert_cells(_by(resp, (CR, MONTH)), expected)


# --------------------------------------------------------------------------- #
# A defaulted grain stays unwritten for the mode axis
# --------------------------------------------------------------------------- #
BY_STATUS: dict[str, Any] = {"population": "orders", "dimensions": ["orders.status"]}
ONE_EACH = {"completed": 1.0, "open": 1.0}
MODE_TABLE = [
    pytest.param(BY_STATUS, "max(rank(sum(returns.amount), direction='desc'))", ONE_EACH, "returns.amount_sum",
                 id="returns"),
    pytest.param(BY_STATUS, "max(rank(count(customers.*), direction='desc'))", ONE_EACH, "customers._count",
                 id="customers"),
    pytest.param(BY_STATUS, "weighted_avg(orders.amount, weight=rank(sum(returns.amount), direction='desc'))",
                 {"completed": 75.0, "open": 250.0}, "returns.amount_sum", id="parameter"),
    pytest.param({"dimensions": [CR]}, "max(rank(count(tickets.*), direction='desc'))",
                 {"EAST": 1.0, "NORTH": 1.0, "WEST": 1.0}, "tickets._count", id="unrelated"),
]
PICK = "max(last(sum(returns.amount)))"
ASSOCIATING = [pytest.param(None, id="default"), pytest.param("associate", id="associate")]


def _mode(mode: Optional[str]) -> dict[str, str]:
    return {} if mode is None else {"to_many_handling": mode}


def _no_unwritten_grain_is_named(text: str) -> None:
    assert "_partition_by_" not in text, text
    assert "declares partition_by" not in text, text


def _assert_names_no_unwritten_grain(resp: SlayerResponse) -> None:
    for kind in ("associated", "broadcast"):
        for name in warned_metrics(resp, kind):
            _no_unwritten_grain_is_named(name)


@pytest.mark.parametrize("mode", ASSOCIATING)
@pytest.mark.parametrize(("query", "metric", "expected", "inner"), MODE_TABLE)
async def test_a_defaulted_grain_associates_with_its_warning(backend: str, query: dict, metric: str, expected: dict,
                                                             inner: str, mode: Optional[str]) -> None:
    async with seed_engine(backend) as eng:
        resp = await run(eng, metrics=[_mt(metric)], **query, **_mode(mode))
    assert _by(resp, tuple(query["dimensions"])) == expected
    assert warned_metrics(resp, "associated") == {inner}
    assert not kinds(resp, "broadcast")


@pytest.mark.parametrize(("query", "metric", "expected", "inner"), MODE_TABLE)
async def test_a_defaulted_grain_broadcasts_under_the_deprecated_mode(backend: str, query: dict, metric: str,
                                                                      expected: dict, inner: str) -> None:
    async with seed_engine(backend) as eng:
        resp = await run(eng, metrics=[_mt(metric)], to_many_handling="broadcast", **query)
    assert _by(resp, tuple(query["dimensions"])) == expected
    assert warned_metrics(resp, "broadcast") == {inner}
    assert not kinds(resp, "associated")


@pytest.mark.parametrize(("query", "metric", "_values", "inner"), MODE_TABLE)
async def test_a_defaulted_grain_is_refused_in_error_mode(backend: str, query: dict, metric: str, _values: dict,
                                                          inner: str) -> None:
    metrics = [_mt(metric)]
    async with seed_engine(backend) as eng:
        with pytest.raises(SlayerError) as ei:
            await run(eng, metrics=metrics, to_many_handling="error", **query)
    assert not isinstance(ei.value, PartitionKeyError), str(ei.value)
    assert f"error mode: metric '{inner}'" in str(ei.value)
    _no_unwritten_grain_is_named(str(ei.value))


async def test_a_pick_over_a_defaulted_grain_keeps_the_mode_axis(backend: str) -> None:
    query: dict[str, Any] = {**BY_STATUS, "time_dimensions": TD, "metrics": [_mt(PICK)]}
    keys = ("orders.status", MONTH)
    async with seed_engine(backend) as eng:
        associated = await run(eng, **query)
        broadcast = await run(eng, to_many_handling="broadcast", **query)
        with pytest.raises(ReaggregationError) as ei:
            await run(eng, to_many_handling="error", **query)
    # C1's return (25) is the only one an order row reaches; all returns are 35
    assert _by(associated, keys) == {("completed", JAN): 25.0, ("open", FEB): 25.0}
    assert warned_metrics(associated, "associated") == {"m", "returns.amount_sum"}
    assert _by(broadcast, keys) == {("completed", JAN): 35.0, ("open", FEB): 35.0}
    assert warned_metrics(broadcast, "broadcast") == {"m", "returns.amount_sum"}
    assert "at metric 'm'" in str(ei.value)
    _no_unwritten_grain_is_named(str(ei.value))


FINER_SIBLING = ("max(rank(sum(returns.amount) / "
                 f"sum(returns.amount, partition_by=[orders.status, {CR}]), direction='desc'))")


@pytest.mark.parametrize("mode", ASSOCIATING)
async def test_a_finer_operand_does_not_make_a_siblings_grain_written(backend: str, mode: Optional[str]) -> None:
    async with seed_engine(backend) as eng:
        resp = await run(eng, metrics=[_mt(FINER_SIBLING)], **BY_STATUS, **_mode(mode))
    assert _by(resp, ("orders.status",)) == ONE_EACH
    assert warned_metrics(resp, "associated") == {"returns.amount_sum"}


@pytest.mark.parametrize("mode", ["broadcast", "error"])
async def test_a_written_unattributable_grain_is_still_refused(backend: str, mode: str) -> None:
    metrics = [_mt(FINER_SIBLING)]
    async with seed_engine(backend) as eng:
        with pytest.raises(PartitionKeyError) as ei:
            await run(eng, metrics=metrics, to_many_handling=mode, **BY_STATUS)
    assert "customers_region" in str(ei.value)


# A same-dataset inner under a transform inside an aggregation, by a dimension across a fanning hop.
FAN: dict[str, Any] = {"population": "customers", "dimensions": ["orders.status"],
                       "metrics": [_mt("max(rank(count(customers.*), direction='desc'))")]}


@pytest.mark.parametrize("mode", ASSOCIATING)
async def test_a_same_dataset_inner_associates_with_its_warning(backend: str, mode: Optional[str]) -> None:
    async with seed_engine(backend) as eng:
        resp = await run(eng, **FAN, **_mode(mode))
    # customers per status: completed C1, C2; open C1, C3; none C4
    assert _by(resp, ("orders.status",)) == {"completed": 1.0, "open": 1.0, None: 3.0}
    assert kinds(resp, "associated")
    _assert_names_no_unwritten_grain(resp)


async def test_a_same_dataset_inner_broadcasts_under_the_deprecated_mode(backend: str) -> None:
    async with seed_engine(backend) as eng:
        resp = await run(eng, to_many_handling="broadcast", **FAN)
    assert _by(resp, ("orders.status",)) == {"completed": 1.0, "open": 1.0, None: 1.0}
    assert kinds(resp, "broadcast")
    _assert_names_no_unwritten_grain(resp)


async def test_a_same_dataset_inner_is_refused_in_error_mode(backend: str) -> None:
    async with seed_engine(backend) as eng:
        with pytest.raises(SlayerError) as ei:
            await run(eng, to_many_handling="error", **FAN)
    assert not isinstance(ei.value, PartitionKeyError), str(ei.value)
    assert "error mode" in str(ei.value)
    _no_unwritten_grain_is_named(str(ei.value))


# --------------------------------------------------------------------------- #
# Spelling and sibling invariance
# --------------------------------------------------------------------------- #
DEFAULTED_RANK = f"max(rank({BARE}, direction='desc'))"
WRITTEN_RANK = f"max(rank({AT_RC}, direction='desc'))"


async def test_a_written_and_a_defaulted_spelling_of_one_grain_agree(backend: str) -> None:
    resp = await _city(backend, RC, _mt(DEFAULTED_RANK, "d"), _mt(WRITTEN_RANK, "w"))
    assert {"d", "w"} <= set(resp.columns)
    assert _by(resp, RC, "d") == _by(resp, RC, "w") == _cities(4, 1, 3, 2)


async def test_a_sibling_measure_changes_no_value(backend: str) -> None:
    mixed = _mt(f"rank({SHARE}, direction='desc')")
    alone = await _city(backend, RC, mixed)
    both = await _city(backend, RC, mixed, _mt(BARE, "s"), _mt(f"rank({BARE}, direction='desc')", "r"))
    assert _by(alone, RC) == _by(both, RC) == _cities(4, 1, 3, 2)
    assert _by(both, RC, "s") == _cities(35, 70, 50, 60)
    assert _by(both, RC, "r") == _cities(4, 1, 3, 2)


async def test_a_mixed_operand_measure_beside_a_computed_dimension(backend: str) -> None:
    async with city_engine(backend) as eng:
        resp = await run(eng, population="orders",
                         dimensions=[R, {"field": f"sum({A}, partition_by={C})", "alias": "total"}],
                         metrics=[_mt(f"rank({SHARE}, direction='desc')")])
    assert _by(resp, (R, "total")) == {("North", 35.0): 4.0, ("North", 70.0): 1.0, ("South", 50.0): 3.0,
                                       ("South", 60.0): 2.0}


async def test_an_ungrained_aggregate_in_a_dimension_is_still_refused(backend: str) -> None:
    metrics = [_mt(BARE)]
    async with city_engine(backend) as eng:
        with pytest.raises(ComputedDimensionError):
            await run(eng, population="orders",
                      dimensions=[R, {"field": f"rank({BARE}, direction='desc')", "alias": "r"}],
                      metrics=metrics)


# --------------------------------------------------------------------------- #
# Keys: the planner reads the checker's grain; the defaulted marker
# --------------------------------------------------------------------------- #
REGION = FieldKey(path=(), leaf="region")
CITY = FieldKey(path=(), leaf="city")
BUCKET = FieldKey(path=(), leaf="month")
AMOUNT = FieldKey(path=(), leaf="amount")
SUM = AggregateKey(source=AMOUNT, agg="sum")
SUM_BY_REGION = AggregateKey(source=AMOUNT, agg="sum", partition_keys=Grain.of([REGION]))
SUM_SCALAR = AggregateKey(source=AMOUNT, agg="sum", partition_keys=Grain.EMPTY)
SUM_FINER = AggregateKey(source=AMOUNT, agg="sum", partition_keys=Grain.of([REGION, CITY, BUCKET]))
SUM_WINDOWED = AggregateKey(source=AMOUNT, agg="sum", kwargs=(("window", "2m"),))
MIXED = ArithmeticKey(op="/", operands=(SUM, SUM_BY_REGION))


def _rank(operand: ValueKey) -> TransformKey:
    return TransformKey(op="rank", input=operand)


@pytest.mark.parametrize(("operand", "dims", "bucket", "expected"), [
    (MIXED, [REGION, CITY], None, [REGION, CITY]),
    (MIXED, [REGION], BUCKET, [REGION, BUCKET]),
    (ArithmeticKey(op="+", operands=(MIXED, SUM_SCALAR)), [REGION, CITY], None, [REGION, CITY]),
    (ArithmeticKey(op="*", operands=(SUM_BY_REGION, CITY)), [REGION, CITY], None, [REGION, CITY]),
    (AggregateKey(source=SUM, agg="max", partition_keys=Grain.of([REGION])), [REGION, CITY], None, [REGION]),
    (ArithmeticKey(op="/", operands=(SUM_WINDOWED, SUM_FINER)), [REGION], BUCKET, [REGION, CITY, BUCKET]),
    (ArithmeticKey(op="/", operands=(TransformKey(op="cumsum", input=SUM, time_key=BUCKET), SUM_BY_REGION)),
     [REGION], BUCKET, [REGION, BUCKET]),
    (ArithmeticKey(op="/", operands=(SUM, SUM_SCALAR)), [REGION], BUCKET, [REGION, BUCKET]),
    (REGION, [REGION, CITY], None, [REGION, CITY]),
], ids=["mixed", "mixed-by-month", "three-way", "row-leaf", "second-order", "windowed-finer", "nested-transform",
        "scalar-denominator", "no-aggregate"])
def test_the_planner_reads_the_checkers_operand_grain(operand: ValueKey, dims: list, bucket: Optional[FieldKey],
                                                      expected: list) -> None:
    tds: list[ValueKey] = [] if bucket is None else [bucket]
    producer, _ = effective_root_grain(
        agg=_rank(operand), projected_dim_keys=dims, projected_td_keys=tds, active_bucket=bucket)
    assert producer == Grain.of(expected)
    assert producer == transform_operand_grain(
        operand, query_grain=Grain.of([*dims, *tds]), active_bucket=bucket)


def _defaulted(grain: Grain = Grain.of([REGION, CITY]), source: ValueKey = AMOUNT) -> AggregateKey:
    return AggregateKey.model_validate(
        {"source": source, "agg": "sum", "partition_keys": grain, "grain_defaulted": True})


def test_a_defaulted_grain_needs_a_grain() -> None:
    with pytest.raises(ValidationError):
        AggregateKey.model_validate({"source": AMOUNT, "agg": "sum", "grain_defaulted": True})


def test_a_defaulted_and_a_written_grain_are_one_value() -> None:
    defaulted = _defaulted()
    written = AggregateKey(source=AMOUNT, agg="sum", partition_keys=Grain.of([REGION, CITY]))
    assert defaulted.grain_defaulted
    assert not written.grain_defaulted
    assert defaulted == written
    assert hash(defaulted) == hash(written)
    assert "grain_defaulted" in NON_IDENTITY_KEY_FIELDS


def test_a_defaulted_grain_is_spelled_as_unwritten() -> None:
    assert key_display(_defaulted()) == key_display(SUM)


def test_rewrites_keep_the_marker() -> None:
    defaulted = _defaulted()
    mapped = defaulted.map_children(lambda c: BUCKET if c == CITY else c)
    assert mapped.partition_keys == Grain.of([REGION, BUCKET])
    assert mapped.grain_defaulted

    joined = _defaulted(source=FieldKey(path=("customers",), leaf="spend"))
    rerooted = reroot_value_key(joined, target_path=("customers",))
    assert rerooted.source == FieldKey(path=(), leaf="spend")
    assert rerooted.grain_defaulted

    substituted = substitute_value_keys(_rank(defaulted), {AMOUNT: FieldKey(path=(), leaf="net")})
    assert [k.grain_defaulted for k in walk_value_keys(substituted) if isinstance(k, AggregateKey)] == [True]


def _elaborated(*metrics: str, dimensions: tuple[str, ...] = RC) -> ElaboratedStage:
    query = SlayerQuery.model_validate({
        "population": "orders", "dimensions": list(dimensions),
        "metrics": [_mt(expr, f"m{i}") for i, expr in enumerate(metrics)]})
    return elaborate_query(query=query, bundle=ResolvedSourceBundle(
        dialect="duckdb", population=city_dataset(), referenced_datasets=[]))


def _metric_aggregates(elab: ElaboratedStage) -> list[AggregateKey]:
    n = elab.prebound.n_dims + elab.prebound.n_time_dimensions
    return [k for dm in elab.prebound.declared_metrics[n:] for k in walk_value_keys(dm.bound.value_key)
            if isinstance(k, AggregateKey)]


def _unpartitioned_sums(elab: ElaboratedStage) -> list[AggregateKey]:
    by_region = Grain.of([REGION])
    return [k for k in _metric_aggregates(elab) if k.agg == "sum" and k.partition_keys != by_region]


def test_binding_defaults_a_nested_aggregate_to_the_query_grain() -> None:
    elab = _elaborated(f"max(rank({SHARE}, direction='desc'))")
    (inner,) = _unpartitioned_sums(elab)
    assert inner.partition_keys == Grain.of([REGION, CITY])
    assert inner.grain_defaulted


def test_binding_defaults_under_a_transform_whose_operand_grain_differs() -> None:
    elab = _elaborated(f"rank(max({BARE}, partition_by={R}), direction='desc')")
    (inner,) = _unpartitioned_sums(elab)
    assert inner.partition_keys == Grain.of([REGION, CITY])
    assert inner.grain_defaulted


def test_binding_leaves_a_top_level_query_grain_operand_unpinned() -> None:
    elab = _elaborated(f"rank({SHARE}, direction='desc')")
    (inner,) = _unpartitioned_sums(elab)
    assert inner.partition_keys is None
    assert not inner.grain_defaulted


def test_a_grain_the_query_also_writes_is_pinned_without_the_marker() -> None:
    for metrics in ((DEFAULTED_RANK, WRITTEN_RANK), (WRITTEN_RANK, DEFAULTED_RANK)):
        inners = _unpartitioned_sums(_elaborated(*metrics))
        assert [k.partition_keys for k in inners] == [Grain.of([REGION, CITY])] * 2
        assert not any(k.grain_defaulted for k in inners)


def test_stripping_a_grain_clears_the_marker() -> None:
    # a row-grain root with an attached input, partitioned by exactly the query grain, is what the pass strips
    elab = _elaborated(f"sum({A} * {BY_REGION}, partition_by=[{R}, {C}])")
    written = elab.prebound.declared_metrics[-1].bound.value_key
    assert isinstance(written, AggregateKey)
    assert written.partition_keys is not None
    marked = _substitute_prebound(
        elab.prebound, {written: _defaulted(grain=written.partition_keys, source=written.source)})
    root = marked.declared_metrics[-1].bound.value_key
    assert isinstance(root, AggregateKey)
    assert root.grain_defaulted
    stripped, _ = _strip_redundant_partitions(elab.model_copy(update={"prebound": marked}))
    root = stripped.declared_metrics[-1].bound.value_key
    assert isinstance(root, AggregateKey)
    assert root.partition_keys is None
    assert not root.grain_defaulted


@pytest.mark.parametrize("metrics", [
    (DEFAULTED_RANK,), (DEFAULTED_RANK, AT_RC), (f"rank(max({BARE}, partition_by={R}), direction='desc')",),
    (f"max(rank({SHARE}, direction='desc'))", BARE),
], ids=["nested", "beside-the-written-grain", "second-order", "beside-the-ungrained"])
def test_no_ungrained_aggregate_carries_the_marker_after_stripping(metrics: tuple[str, ...]) -> None:
    stripped, _ = _strip_redundant_partitions(_elaborated(*metrics))
    n = stripped.n_dims + stripped.n_time_dimensions
    for dm in stripped.declared_metrics[n:]:
        for key in walk_value_keys(dm.bound.value_key):
            if isinstance(key, AggregateKey) and key.partition_keys is None:
                assert not key.grain_defaulted, key
