"""Rank-family ordering direction and NULL inputs (spec: queries/transforms › Rank-family
ordering direction; Rank-family NULL inputs rank NULL; queries/measure-naming › Rank
direction spelled as its bare value)."""

from __future__ import annotations

import pytest

from slayer.core import errors as core_errors
from slayer.core.formula import parse_formula
from slayer.core.keys import TransformKey
from slayer.core.models import ModelMeasure
from slayer.core.scope import ModelScope
from slayer.engine.binding import bind_expr
from slayer.engine.syntax import parse_expr
from slayer.ir.source_bundle import ResolvedSourceBundle

from tests._dev1847_fixtures import (
    _SALES_ROWS_WIDE,
    corders_model,
    customers_model,
    dev1847_models,
    gen,
    make_exec_engine,
    regions_model,
    rows_by,
    sales_model,
    sales_q,
)
from tests._rank_direction_fixtures import (
    CITY_DENSE_DESC,
    CITY_NAME_RANK_ASC,
    CITY_RANK_ASC,
    DIALECTS,
    MIN_CITY_RANK_ASC,
    NTILE2,
    PERCENT_RANK,
    RANK_ASC,
    RANK_DESC,
    REGION_TOTALS,
    cell_totals,
    rank_within,
    rank_windows,
)

BOTH_SPELLINGS = ("direction='asc'", "direction='desc'", "lowest first", "highest first")


@pytest.fixture(params=["sqlite", "duckdb"])
async def engine(request):
    async for e in make_exec_engine(request=request):
        yield e


def _sales_with_measure(formula: str):
    model = sales_model()
    model.measures = [ModelMeasure(name="saved_rank", formula=formula)]
    return [model, regions_model(), customers_model(), corders_model()]


@pytest.fixture(params=["sqlite", "duckdb"])
async def saved_asc_engine(request):
    async for e in make_exec_engine(
            request=request, models=_sales_with_measure("rank(sum(amount), direction='asc')")):
        yield e


@pytest.fixture(params=["sqlite", "duckdb"])
async def saved_bare_engine(request):
    async for e in make_exec_engine(request=request, models=_sales_with_measure("rank(sum(amount))")):
        yield e


def _m(formula: str, name: str = "r") -> ModelMeasure:
    return ModelMeasure(formula=formula, name=name)


def _by_region(resp, name: str = "r") -> dict:
    return {k[0]: v[f"sales.{name}"] for k, v in rows_by(resp, "sales.region").items()}


def _by_region_city(resp, name: str = "r") -> dict:
    return {k: v[f"sales.{name}"] for k, v in rows_by(resp, "sales.region", "sales.city").items()}


def _approx(d: dict) -> dict:
    return {k: (None if v is None else pytest.approx(v)) for k, v in d.items()}


def _bind(formula: str):
    models = dev1847_models()
    bundle = ResolvedSourceBundle(dialect="postgres", source_model=models[0],
                                  referenced_models=models[1:])
    return bind_expr(parse_expr(formula), scope=ModelScope(source_model=models[0]),
                     bundle=bundle).value_key


def _assert_missing_direction(msg: str, op: str = "rank") -> None:
    assert f"'{op}'" in msg or f"{op}(" in msg, msg
    for part in BOTH_SPELLINGS:
        assert part in msg, (part, msg)


class TestOracleSelfCheck:
    def test_region_ranks(self):
        flat = {(None, r): v for r, v in REGION_TOTALS.items()}
        assert {k[1]: v for k, v in rank_within(flat, descending=False).items()} == RANK_ASC
        assert {k[1]: v for k, v in rank_within(flat, descending=True).items()} == RANK_DESC
        assert REGION_TOTALS == {k[0]: v for k, v in cell_totals(lambda r: (r[1],)).items()}

    def test_city_ranks(self):
        totals = cell_totals(lambda r: (r[1], r[2]))
        assert rank_within(totals, descending=False) == CITY_RANK_ASC
        assert rank_within(totals, descending=True, dense=True) == CITY_DENSE_DESC
        names = {(r[1], r[2]): r[2] for r in _SALES_ROWS_WIDE}
        assert rank_within(names, descending=False) == CITY_NAME_RANK_ASC


# --------------------------------------------------------------------------- #
# Direction: executed values.
# --------------------------------------------------------------------------- #
class TestDirectionValues:
    async def test_ascending_rank_lowest_first(self, engine):
        resp = await engine.execute(sales_q(
            dimensions=["region"], measures=[_m("rank(sum(amount), direction='asc')")]))
        assert _by_region(resp) == RANK_ASC

    async def test_descending_rank_highest_first(self, engine):
        resp = await engine.execute(sales_q(
            dimensions=["region"], measures=[_m("rank(sum(amount), direction='desc')")]))
        assert _by_region(resp) == RANK_DESC

    async def test_dense_rank_takes_direction(self, engine):
        resp = await engine.execute(sales_q(
            dimensions=["region"], measures=[_m("dense_rank(sum(amount), direction='asc')")]))
        assert _by_region(resp) == RANK_ASC

    @pytest.mark.parametrize("spelling", ["ASC", "Ascending", " ascending "])
    async def test_synonyms_execute(self, engine, spelling):
        resp = await engine.execute(sales_q(
            dimensions=["region"], measures=[_m(f"rank(sum(amount), direction='{spelling}')")]))
        assert _by_region(resp) == RANK_ASC

    async def test_non_numeric_inner(self, engine):
        resp = await engine.execute(sales_q(
            dimensions=["region"], measures=[_m("rank(min(city), direction='asc')")]))
        assert _by_region(resp) == MIN_CITY_RANK_ASC

    async def test_combines_with_partition_by(self, engine):
        resp = await engine.execute(sales_q(
            dimensions=["region", "city"],
            measures=[_m("rank(sum(amount), partition_by=region, direction='asc')")]))
        assert _by_region_city(resp) == CITY_RANK_ASC

    async def test_both_directions_stay_distinct(self, engine):
        resp = await engine.execute(sales_q(
            dimensions=["region"],
            measures=["rank(sum(amount), direction='asc')", "rank(sum(amount), direction='desc')"]))
        assert _by_region(resp, "rank_amount_sum_asc") == RANK_ASC
        assert _by_region(resp, "rank_amount_sum_desc") == RANK_DESC

    async def test_ntile_and_percent_rank_ascending(self, engine):
        resp = await engine.execute(sales_q(
            dimensions=["region"],
            measures=[_m("ntile(sum(amount), n=2)", "nt"), _m("percent_rank(sum(amount))", "pr")]))
        assert _by_region(resp, "nt") == NTILE2
        assert _by_region(resp, "pr") == _approx(PERCENT_RANK)


class TestDirectionInEveryPosition:
    async def test_filter_keeps_the_cheapest(self, engine):
        resp = await engine.execute(sales_q(
            dimensions=["region"], measures=[_m("sum(amount)", "a")],
            filters=["rank(sum(amount), direction='asc') <= 1"]))
        assert set(_by_region(resp, "a")) == {"Gap"}

    async def test_order_by_ascending_rank(self, engine):
        resp = await engine.execute(sales_q(
            dimensions=["region"], measures=[_m("sum(amount)", "a")],
            order=[{"column": "rank(sum(amount), direction='asc')", "direction": "asc"}]))
        assert [r["sales.region"] for r in resp.data] == ["Gap", "North", "South", "East", "Void"]

    async def test_computed_dimension(self, engine):
        dim = {"expression": "rank(sum(amount, partition_by=region), direction='asc')", "name": "rk"}
        resp = await engine.execute(sales_q(
            dimensions=["region", dim], measures=[_m("sum(amount)", "a")]))
        got = {k[0]: k[1] for k in rows_by(resp, "sales.region", "sales.rk")}
        assert got == RANK_ASC

    @pytest.mark.parametrize(("direction", "want"), [("asc", 1340 / 32), ("desc", 810 / 33)])
    async def test_aggregation_parameter(self, engine, direction, want):
        resp = await engine.execute(sales_q(measures=[_m(
            f"weighted_avg(amount, weight=rank(sum(amount, partition_by=region), "
            f"direction='{direction}'))", "w")]))
        [row] = resp.data
        assert float(row["sales.w"]) == pytest.approx(want)

    async def test_saved_model_measure(self, saved_asc_engine):
        resp = await saved_asc_engine.execute(sales_q(dimensions=["region"], measures=["saved_rank"]))
        assert _by_region(resp, "saved_rank") == RANK_ASC


# --------------------------------------------------------------------------- #
# Binding.
# --------------------------------------------------------------------------- #
class TestBinding:
    @pytest.mark.parametrize(("spelling", "canonical"), [
        ("desc", "desc"), ("DESC", "desc"), (" Descending ", "desc"),
        ("asc", "asc"), ("Ascending", "asc"), (" ASCENDING", "asc"),
    ])
    def test_synonyms_normalise(self, spelling, canonical):
        got = _bind(f"rank(amount:sum, direction='{spelling}')")
        want = _bind(f"rank(amount:sum, direction='{canonical}')")
        assert isinstance(got, TransformKey)
        assert got == want
        assert ("direction", canonical) in got.kwargs

    def test_directions_are_distinct_values(self):
        assert _bind("rank(amount:sum, direction='asc')") != _bind("rank(amount:sum, direction='desc')")
        assert (_bind("dense_rank(amount:sum, direction='asc')")
                != _bind("dense_rank(amount:sum, direction='desc')"))

    def test_ntile_and_percent_rank_carry_no_direction(self):
        for formula in ("ntile(amount:sum, n=4)", "percent_rank(amount:sum)"):
            key = _bind(formula)
            assert isinstance(key, TransformKey)
            assert all(k != "direction" for k, _v in key.kwargs)

    def test_error_is_a_query_type_error(self):
        assert issubclass(core_errors.TransformArgumentError, core_errors.QueryTypeError)
        assert issubclass(core_errors.TransformArgumentError, ValueError)


# --------------------------------------------------------------------------- #
# Direction errors.
# --------------------------------------------------------------------------- #
_MISSING = [
    pytest.param({"dimensions": ["region"], "measures": [_m("rank(sum(amount))")]},
                 "rank", id="measure"),
    pytest.param({"dimensions": ["region", "city"],
                  "measures": [_m("dense_rank(sum(amount), partition_by=region)")]},
                 "dense_rank", id="measure-dense-partitioned"),
    pytest.param({"dimensions": ["region"], "measures": [_m("sum(amount)", "a")],
                  "filters": ["rank(sum(amount)) <= 2"]}, "rank", id="filter"),
    pytest.param({"dimensions": ["region"], "measures": [_m("sum(amount)", "a")],
                  "order": [{"column": "rank(sum(amount))", "direction": "asc"}]},
                 "rank", id="order"),
    pytest.param({"dimensions": ["region", {"expression": "rank(sum(amount, partition_by=region))",
                                            "name": "rk"}],
                  "measures": [_m("sum(amount)", "a")]}, "rank", id="computed-dimension"),
    pytest.param({"dimensions": ["region"],
                  "measures": [_m("weighted_avg(amount, weight=rank(sum(amount, "
                                  "partition_by=region)))", "w")]},
                 "rank", id="aggregation-parameter"),
]


class TestDirectionErrors:
    @pytest.mark.parametrize(("fields", "op"), _MISSING)
    async def test_missing_direction_fails_before_sql(self, engine, fields, op):
        query = sales_q(**fields)
        for dry_run in (True, False):
            with pytest.raises(core_errors.TransformArgumentError) as ei:
                await engine.execute(query, dry_run=dry_run)
            _assert_missing_direction(str(ei.value), op=op)

    async def test_saved_measure_without_direction(self, saved_bare_engine):
        query = sales_q(dimensions=["region"], measures=["saved_rank"])
        with pytest.raises(core_errors.TransformArgumentError) as ei:
            await saved_bare_engine.execute(query)
        _assert_missing_direction(str(ei.value))

    @pytest.mark.parametrize("formula", [
        "rank(sum(amount), direction='up')",
        "rank(sum(amount), direction=region)",
        "rank(sum(amount), direction=1)",
        "dense_rank(sum(amount), direction='')",
    ])
    async def test_unrecognised_or_non_literal(self, engine, formula):
        query = sales_q(dimensions=["region"], measures=[_m(formula)])
        with pytest.raises(core_errors.TransformArgumentError) as ei:
            await engine.execute(query)
        msg = str(ei.value)
        for word in ("asc", "desc", "ascending", "descending"):
            assert word in msg, msg

    @pytest.mark.parametrize(("formula", "op"), [
        ("ntile(sum(amount), n=2, direction='desc')", "ntile"),
        ("percent_rank(sum(amount), direction='asc')", "percent_rank"),
    ])
    async def test_ntile_and_percent_rank_reject_direction(self, engine, formula, op):
        query = sales_q(dimensions=["region"], measures=[_m(formula)])
        with pytest.raises(core_errors.TransformArgumentError) as ei:
            await engine.execute(query)
        msg = str(ei.value)
        assert op in msg, msg
        assert "ascending" in msg, msg
        assert "direction" in msg, msg

    @pytest.mark.parametrize("formula", [
        "rank(amount:sum, foo=1, direction='desc')",
        "percent_rank(amount:sum, foo=1)",
        "consecutive_periods(amount:sum > 0, foo=1)",
        "lag(amount:sum, periods=region)",
        "time_shift(amount:sum, granularity='month')",
        "ntile(amount:sum)",
        "ntile(amount:sum, n=0)",
        "ntile(amount:sum, n=region)",
    ])
    def test_other_transform_argument_errors_are_typed(self, formula):
        with pytest.raises(core_errors.TransformArgumentError):
            _bind(formula)

    @pytest.mark.parametrize(("op", "advertised"), [
        ("rank", True), ("dense_rank", True), ("percent_rank", False),
    ])
    def test_unknown_keyword_lists_direction_where_accepted(self, op, advertised):
        formula = f"{op}(sum(amount), foo=1)"
        with pytest.raises(core_errors.TransformArgumentError) as binder:
            _bind(formula)
        with pytest.raises(ValueError) as importer:
            parse_formula(formula)
        for msg in (str(binder.value), str(importer.value)):
            assert "foo" in msg, msg
            assert ("direction" in msg) is advertised, msg


class TestImporterParity:
    @pytest.mark.parametrize("formula", [
        "rank(sum(amount))",
        "dense_rank(sum(amount), partition_by=region)",
        "rank(sum(amount), direction='sideways')",
        "ntile(sum(amount), n=4, direction='asc')",
        "percent_rank(sum(amount), direction='desc')",
    ])
    def test_same_error_as_the_binder(self, formula):
        with pytest.raises(core_errors.TransformArgumentError) as importer:
            parse_formula(formula)
        with pytest.raises(core_errors.TransformArgumentError) as binder:
            _bind(formula)
        assert str(importer.value) == str(binder.value)

    @pytest.mark.parametrize(("formula", "want"), [
        ("rank(sum(amount), direction='Ascending')", "asc"),
        ("dense_rank(sum(amount), partition_by=region, direction='DESC')", "desc"),
    ])
    def test_accepts_and_normalises(self, formula, want):
        field = parse_formula(formula)
        assert field.kwargs["direction"] == want  # type: ignore[union-attr]

    def test_ntile_keeps_no_direction(self):
        field = parse_formula("ntile(sum(amount), n=4)")
        assert "direction" not in field.kwargs  # type: ignore[union-attr]


# --------------------------------------------------------------------------- #
# NULL inputs.
# --------------------------------------------------------------------------- #
class TestNullInputs:
    async def test_mixed_null_and_non_null(self, engine):
        resp = await engine.execute(sales_q(
            dimensions=["region"],
            measures=[_m("rank(sum(amount), direction='desc')", "rk"),
                      _m("percent_rank(sum(amount))", "pr"),
                      _m("ntile(sum(amount), n=2)", "nt")]))
        assert _by_region(resp, "rk") == RANK_DESC
        assert _by_region(resp, "pr") == _approx(PERCENT_RANK)
        assert _by_region(resp, "nt") == NTILE2

    async def test_all_null_partition(self, engine):
        resp = await engine.execute(sales_q(
            dimensions=["region", "city"],
            measures=[_m("dense_rank(sum(amount), partition_by=region, direction='desc')")]))
        assert _by_region_city(resp) == CITY_DENSE_DESC

    async def test_null_row_inside_partition_takes_no_position(self, engine):
        resp = await engine.execute(sales_q(
            dimensions=["region", "city"],
            measures=[_m("rank(city, partition_by=region, direction='asc')")]))
        assert _by_region_city(resp) == CITY_NAME_RANK_ASC

    async def test_filter_drops_null_ranked_rows(self, engine):
        resp = await engine.execute(sales_q(
            dimensions=["region"], measures=[_m("sum(amount)", "a")],
            filters=["rank(sum(amount), direction='asc') <= 5"]))
        assert set(_by_region(resp, "a")) == {"East", "Gap", "North", "South"}

    @pytest.mark.parametrize(("formula", "op"), [
        ("ntile(sum(amount), n=2)", "ntile"),
        ("percent_rank(sum(amount))", "percent_rank"),
        ("dense_rank(sum(amount), direction='asc')", "dense_rank"),
    ])
    async def test_null_inner_is_null_for_every_function(self, engine, formula, op):
        resp = await engine.execute(sales_q(dimensions=["region"], measures=[_m(formula)]))
        got = _by_region(resp)
        assert got["Void"] is None, (op, got)
        assert all(v is not None for k, v in got.items() if k != "Void"), (op, got)


# --------------------------------------------------------------------------- #
# Emission shape across dialects.
# --------------------------------------------------------------------------- #
_EMISSION = [
    pytest.param("rank(sum(amount), direction='asc')", "RANK", False, id="rank-asc"),
    pytest.param("rank(sum(amount), direction='desc')", "RANK", True, id="rank-desc"),
    pytest.param("dense_rank(sum(amount), direction='asc')", "DENSERANK", False, id="dense-asc"),
    pytest.param("dense_rank(sum(amount), direction='desc')", "DENSERANK", True, id="dense-desc"),
    pytest.param("ntile(sum(amount), n=4)", "NTILE", False, id="ntile"),
    pytest.param("percent_rank(sum(amount))", "PERCENTRANK", False, id="percent-rank"),
]


class TestEmission:
    @pytest.mark.parametrize("dialect", DIALECTS)
    @pytest.mark.parametrize(("formula", "fn", "descending"), _EMISSION)
    async def test_window_orders_by_direction_and_isolates_nulls(self, dialect, formula, fn, descending):
        sql = await gen(sales_q(dimensions=["region"], measures=[_m(formula)]), dialect=dialect)
        [window] = rank_windows(sql, dialect=dialect)
        assert window.fn == fn
        assert window.descending is descending
        assert window.null_flag, sql
        assert window.null_guarded, sql
        assert window.partition_sql == []

    @pytest.mark.parametrize("dialect", DIALECTS)
    async def test_partition_keys_precede_the_null_flag(self, dialect):
        sql = await gen(sales_q(
            dimensions=["region", "city"],
            measures=[_m("rank(sum(amount), partition_by=region, direction='asc')")]), dialect=dialect)
        [window] = rank_windows(sql, dialect=dialect)
        assert len(window.partition_sql) == 1, sql
        assert "region" in window.partition_sql[0], sql
        assert window.null_flag
        assert window.null_guarded
        assert not window.descending

    @pytest.mark.parametrize("formula", [p.values[0] for p in _EMISSION])
    async def test_tsql_matches_postgres(self, formula):
        query = sales_q(dimensions=["region"], measures=[_m(formula)])
        [pg] = rank_windows(await gen(query, dialect="postgres"), dialect="postgres")
        [ts] = rank_windows(await gen(query, dialect="tsql"), dialect="tsql")
        assert (ts.fn, ts.descending, ts.null_flag, ts.null_guarded) == (
            pg.fn, pg.descending, True, True)


# --------------------------------------------------------------------------- #
# Result-key naming.
# --------------------------------------------------------------------------- #
class TestNaming:
    @pytest.mark.parametrize(("formula", "key"), [
        ("rank(sum(amount), direction='desc')", "sales.rank_amount_sum_desc"),
        ("rank(sum(amount), direction='Ascending')", "sales.rank_amount_sum_asc"),
        ("rank(sum(amount), partition_by=region, direction='asc')",
         "sales.rank_amount_sum_partition_by_region_asc"),
        ("dense_rank(sum(amount), direction=' DESC ')", "sales.dense_rank_amount_sum_desc"),
        ("ntile(sum(amount), n=4)", "sales.ntile_amount_sum_n_4"),
    ])
    async def test_unnamed_key(self, engine, formula, key):
        resp = await engine.execute(sales_q(dimensions=["region", "city"], measures=[formula]))
        assert key in resp.columns, resp.columns
        assert not any("direction" in c for c in resp.columns), resp.columns
