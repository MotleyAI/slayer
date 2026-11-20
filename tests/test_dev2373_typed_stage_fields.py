"""Every stage column carries a type, read the same by a later stage.

Spec: openspec …/specs/queries/multi-stage — "Every stage column carries a type";
…/specs/queries/time-dimensions — "Untyped stage column fails closed".
"""

from __future__ import annotations

from typing import Any, Optional

import pytest
import sqlglot
from pydantic import ValidationError
from sqlglot import exp
from sqlglot.expressions.core import Expression

from slayer.core.enums import DataType
from slayer.core.errors import ScalarOperandTypeError, TimeDimensionFieldError
from slayer.core.keys import ArithmeticKey, FieldKey, LiteralKey, Phase, value_type
from slayer.core.models import Aggregation, DatasetMetric, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.core.scope import StageField, StageSchema
from slayer.ir.planned import ValueSlot
from slayer.ir.source_bundle import dataset_from_stage_schema
from slayer.sql.render.row_expr import data_type_target
from tests._dev2373_fixtures import (
    DIALECTS,
    DS,
    cast_target,
    customers,
    d,
    emitted_types,
    m,
    num,
    orders,
    plan_type,
    postgres_sql,
    q,
    read_types,
    shop,
    shop_datasets,
    stage_columns,
    value_of,
)
from tests._engine_helpers import _engine_generate

INT, TEXT, DOUBLE, DEC = DataType.INTEGER, DataType.STRING, DataType.FLOAT, DataType.DECIMAL
BOOL, TS = DataType.BOOLEAN, DataType.DATETIME
MONTH = [{"field": "orders.ordered_at", "granularity": "month"}]
BY_REGION = {"dimensions": ["orders.region"]}


def _both(first: dict[str, Any], expected: dict[str, Optional[DataType]], **kw: Any) -> None:
    """Stage ``s`` emits, and a later stage reads, exactly ``expected``."""
    names = list(expected)
    assert emitted_types(first, names=names, **kw) == expected
    assert read_types(first, names=names, **kw) == expected


def _pg(dt: DataType) -> str:
    return data_type_target(dt).sql(dialect="postgres").upper()


# --------------------------------------------------------------------------- #
# Scenarios of "Every stage column carries a type".
# --------------------------------------------------------------------------- #
class TestScenarios:
    def test_computed_measures_keep_their_type_downstream(self) -> None:
        first = {
            "time_dimensions": MONTH,
            "dimensions": [d("lower(orders.status)", "lo"), d("orders.status IN ('paid')", "isp")],
            "metrics": [
                m("sum(orders.amount) / count(orders.*)", "ratio"), m("sum(orders.amount)", "sa"),
                m("cumsum(sum(orders.amount))", "cs"), m("rank(sum(orders.amount), direction='desc')", "rk"),
            ],
        }
        sa = emitted_types(first, names=["sa"])["sa"]
        assert sa is not None
        _both(first, {"ratio": DOUBLE, "lo": TEXT, "cs": sa, "rk": INT, "isp": BOOL})

    def test_cross_dataset_aggregate_keeps_its_type_downstream(self) -> None:
        first = {**BY_REGION, "metrics": [m("max(customers.created_at)", "mc"), m("sum(customers.spend)", "ss")]}
        _both(first, {"mc": TS, "ss": DEC})

    def test_re_aggregated_value_keeps_its_type_downstream(self) -> None:
        metrics = [m("sum(orders.quantity, partition_by=orders.region)", "pr"),
                   m("sum(orders.quantity, partition_by=orders.region) * 2", "pr2")]
        first = {"dimensions": ["orders.region", "orders.status"], "metrics": metrics}
        planned = {name: plan_type(first, name=name) for name in ("pr", "pr2")}
        assert planned == {"pr": INT, "pr2": INT}
        _both(first, planned)

    def test_branch_typing_over_stage_columns(self) -> None:
        s = q(name="s", population="orders", dimensions=[d("lower(orders.status)", "u"), d("orders.quantity + 0", "n")])
        t = q(name="t", population="s", dimensions=[d("iif(s.n > 1, s.u, 'x')", "b"), d("coalesce(s.n, 0)", "c")])
        root = q(population="t", dimensions=["t.b", "t.c"])
        columns = stage_columns([s, t, root], stage="t")
        assert (columns["b"].datatype, columns["c"].datatype) == (TEXT, INT)

    @pytest.mark.parametrize("dialect", DIALECTS)
    async def test_mixed_type_conditional_over_stage_columns_is_refused(self, dialect: str) -> None:
        s = q(name="s", population="orders", dimensions=[d("lower(orders.status)", "s"), d("orders.order_date", "d")])
        root = q(population="s", dimensions=[d("coalesce(s.s, s.d)", "c")])
        async with shop(dialect) as engine:
            with pytest.raises(ScalarOperandTypeError) as info:
                await engine.execute([s, root])
        message = str(info.value)
        assert "String" in message, message
        assert "Date" in message, message
        assert "coalesce" in message.lower(), message

    @pytest.mark.parametrize("dialect", DIALECTS)
    async def test_null_only_stage_column_is_int(self, dialect: str) -> None:
        first = {**BY_REGION, "dimensions": ["orders.region", d("null", "nul")], "metrics": [m("count(orders.*)", "n")]}
        assert emitted_types(first, names=["nul"], dialect=dialect) == {"nul": INT}
        s = q(name="s", population="orders", **first)
        root = q(population="s", dimensions=[d("coalesce(s.nul, 0)", "z")],
                 metrics=[m("sum(s.nul)", "a"), m("count(s.nul)", "c")])
        async with shop(dialect) as engine:
            resp = await engine.execute([s, root])
        ((row),) = resp.data
        assert (value_of(row, "a"), value_of(row, "c"), num(value_of(row, "z"))) == (None, 0, 0.0)

    def test_explicit_type_wins(self) -> None:
        first = {**BY_REGION, "metrics": [m("sum(orders.amount) / count(orders.*)", "r", datatype="Decimal")]}
        _both(first, {"r": DEC})

    @pytest.mark.parametrize("dialect", DIALECTS)
    async def test_saved_query_backed_dataset_reads_its_columns_real_types(self, dialect: str) -> None:
        saved = SlayerDataset.model_validate({"name": "qb", "data_source": DS, "source_queries": [{
            "population": "orders", "dimensions": ["orders.region", d("lower(orders.region)", "lo")],
            "metrics": [m("sum(orders.amount) / count(orders.*)", "r"), m("sum(orders.quantity) + 1", "k")],
        }]})
        async with shop(dialect, datasets=[*shop_datasets(), saved]) as engine:
            for column, observed in (("r", "Float"), ("k", "Integer"), ("lo", "String")):
                query = q(population="qb", time_dimensions=[{"field": f"qb.{column}", "granularity": "month"}])
                with pytest.raises(TimeDimensionFieldError) as info:
                    await engine.execute(query)
                assert observed in str(info.value), (column, str(info.value))


# --------------------------------------------------------------------------- #
# Per newly typed category: the stage column's type, executed values, Postgres cast.
# --------------------------------------------------------------------------- #
#: Monthly ``sum(orders.quantity)``: Jan 5, Feb 7, Mar 5.
_MONTHS = ["2024-01", "2024-02", "2024-03"]


class _Case:
    def __init__(self, first: dict[str, Any], keys: list[str], expected: DataType, values: Any, *,
                 cast: Optional[str], cast_alias: str = "x", datasets: Optional[list[SlayerDataset]] = None) -> None:
        self.first, self.keys, self.expected, self.values = first, keys, expected, values
        self.cast, self.cast_alias, self.datasets = cast, cast_alias, datasets


def _metric(expr: str, *, by: str = "region", **kw: Any) -> dict[str, Any]:
    if by == "month":
        return {"time_dimensions": MONTH, "metrics": [m(expr, **kw)]}
    return {**BY_REGION, "metrics": [m(expr, **kw)]}


def _dim(expr: str) -> dict[str, Any]:
    return {"dimensions": [d(expr)], "metrics": [m("count(orders.*)", "n")]}


_OVERRIDE = shop_datasets(orders_dataset=orders(aggregations=[Aggregation(name="sum", expression="SUM({value}) / 2.0")]))

CASES: dict[str, _Case] = {
    "arithmetic-aggregate": _Case(_metric("sum(orders.amount) / count(orders.*)"), ["region"], DOUBLE,
                                  {"N": 80 / 3, "S": 35.0}, cast=_pg(DOUBLE)),
    "arithmetic-row": _Case(_dim("orders.quantity + 1"), [], INT, {3, 4, 2, 7, 6}, cast=_pg(INT)),
    "scalar": _Case(_dim("lower(orders.status)"), [], TEXT, {"paid", "open"}, cast=None),
    "in": _Case(_dim("orders.status IN ('paid')"), [], BOOL, {True, False}, cast=_pg(BOOL)),
    "literal-text": _Case(_dim("'k'"), [], TEXT, {"k"}, cast=None),
    "literal-int": _Case(_dim("7"), [], INT, {7}, cast=_pg(INT)),
    "null-only": _Case(_dim("null"), [], INT, {None}, cast=_pg(INT)),
    "null-only-arithmetic": _Case(_dim("null + null"), [], INT, {None}, cast=_pg(INT)),
    "arithmetic-null-operand": _Case(_dim("orders.quantity + null"), [], INT, {None}, cast=_pg(INT)),
    "regroup-placeholder": _Case(
        {"dimensions": ["orders.region", "orders.status"], "metrics": [m("sum(orders.quantity, partition_by=orders.region)")]},
        ["region", "status"], INT, {("N", "paid"): 9, ("S", "open"): 8}, cast=None),
    "cross-dataset-sum": _Case(_metric("sum(customers.spend)"), ["region"], DEC, {"N": 350.75, "S": 300.75},
                               cast=_pg(DEC), cast_alias="spend_sum_host"),
    "cross-dataset-max": _Case(_metric("max(customers.created_at)"), ["region"], TS,
                               {"N": "2023-07-01", "S": "2023-06-01"}, cast=None, cast_alias="created_at_max_host"),
    "custom-avg": _Case(_metric("my_avg(orders.quantity)"), ["region"], DOUBLE, {"N": 3.0, "S": 4.0}, cast=_pg(DOUBLE)),
    "custom-count": _Case(_metric("trues(orders.flag)"), ["region"], INT, {"N": 2, "S": 1}, cast=None),
    "builtin-overridden": _Case(_metric("sum(orders.quantity)"), ["region"], DOUBLE, {"N": 4.5, "S": 4.0},
                                cast=_pg(DOUBLE), datasets=_OVERRIDE),
    "abs-of-overridden-built-in": _Case(_metric("abs(sum(orders.quantity))"), ["region"], DOUBLE, {"N": 4.5, "S": 4.0},
                                        cast=_pg(DOUBLE), datasets=_OVERRIDE),
    # Transforms (design decision 15), monthly.
    "lag": _Case(_metric("lag(sum(orders.quantity), 1)", by="month"), ["ordered_at"], INT, [None, 5, 7], cast=None),
    "lead": _Case(_metric("lead(sum(orders.quantity), 1)", by="month"), ["ordered_at"], INT, [7, 5, None], cast=None),
    "time_shift": _Case(_metric("time_shift(sum(orders.quantity), -1)", by="month"), ["ordered_at"], INT, [None, 5, 7],
                        cast=None),
    "first": _Case(_metric("first(sum(orders.quantity))", by="month"), ["ordered_at"], INT, [5, 5, 5], cast=None),
    "last": _Case(_metric("last(sum(orders.quantity))", by="month"), ["ordered_at"], INT, [5, 5, 5], cast=None),
    "lag-text": _Case(_metric("lag(max(orders.status), 1)", by="month"), ["ordered_at"], TEXT, [None, "paid", "paid"],
                      cast=None),
    "cumsum": _Case(_metric("cumsum(sum(orders.quantity))", by="month"), ["ordered_at"], INT, [5, 12, 17], cast=None),
    "change": _Case(_metric("change(sum(orders.quantity))", by="month"), ["ordered_at"], INT, [None, 2, -2], cast=None),
    "change_pct": _Case(_metric("change_pct(sum(orders.quantity))", by="month"), ["ordered_at"], DOUBLE,
                        [None, 0.4, -2 / 7], cast=_pg(DOUBLE)),
    "percent_rank": _Case(_metric("percent_rank(sum(orders.quantity))", by="month"), ["ordered_at"], DOUBLE,
                          [0.0, 1.0, 0.0], cast=_pg(DOUBLE)),
    "rank": _Case(_metric("rank(sum(orders.quantity), direction='desc')", by="month"), ["ordered_at"], INT, [2, 1, 2],
                  cast=None),
    "dense_rank": _Case(_metric("dense_rank(sum(orders.quantity), direction='desc')", by="month"), ["ordered_at"], INT,
                        [2, 1, 2], cast=None),
    "ntile": _Case(_metric("ntile(sum(orders.quantity), n=2)", by="month"), ["ordered_at"], INT, [1, 2, 1], cast=None),
    "consecutive_periods": _Case(_metric("consecutive_periods(sum(orders.quantity) > 5)", by="month"), ["ordered_at"],
                                 INT, [0, 1, 0], cast=None),
}


def _normalize(v: Any) -> Any:
    if isinstance(v, bool) or v is None or isinstance(v, str):
        return v
    try:
        return round(float(v), 9)
    except (TypeError, ValueError):
        return str(v)[:10]


def _expected(values: Any) -> Any:
    if isinstance(values, dict):
        return {k: _normalize(v) for k, v in values.items()}
    if isinstance(values, set):
        return {_normalize(v) for v in values}
    return [_normalize(v) for v in values]


def _observed(rows: list[dict[str, Any]], case: _Case, *, dialect: str) -> Any:
    x = [(r, value_of(r, "x")) for r in rows]
    if case.expected is BOOL:
        return {None if v is None else bool(v) for _, v in x}
    if case.expected is TS:
        return {value_of(r, case.keys[0]): str(v)[:10] for r, v in x}
    if not case.keys:
        return {_normalize(v) for _, v in x}
    if case.keys == ["ordered_at"]:
        ordered = sorted(x, key=lambda rv: str(value_of(rv[0], "ordered_at")))
        assert [str(value_of(r, "ordered_at"))[:7] for r, _ in ordered] == _MONTHS, dialect
        return [_normalize(v) for _, v in ordered]
    if len(case.keys) == 1:
        return {value_of(r, case.keys[0]): _normalize(v) for r, v in x}
    return {tuple(value_of(r, k) for k in case.keys): _normalize(v) for r, v in x
            if tuple(value_of(r, k) for k in case.keys) in case.values}


class TestPerCategory:
    @pytest.mark.parametrize("case_id", list(CASES))
    def test_stage_column_type(self, case_id: str) -> None:
        case = CASES[case_id]
        _both(case.first, {"x": case.expected}, datasets=case.datasets)

    @pytest.mark.parametrize("dialect", DIALECTS)
    @pytest.mark.parametrize("case_id", [
        pytest.param(c, marks=pytest.mark.xfail(strict=True, reason="DEV-2384: an integer-literal dimension groups as GROUP BY n"))
        if c == "literal-int" else c
        for c in CASES
    ])
    async def test_executed_values_through_a_later_stage(self, case_id: str, dialect: str) -> None:
        case = CASES[case_id]
        s = q(name="s", population="orders", **case.first)
        root = q(population="s", dimensions=[*(f"s.{k}" for k in case.keys), "s.x"])
        async with shop(dialect, datasets=case.datasets) as engine:
            resp = await engine.execute([s, root])
        assert _observed(resp.data, case, dialect=dialect) == _expected(case.values), resp.sql

    @pytest.mark.parametrize("case_id", list(CASES))
    async def test_postgres_cast_follows_the_cast_policy(self, case_id: str) -> None:
        case = CASES[case_id]
        s = q(name="s", population="orders", **case.first)
        root = q(population="s", dimensions=[*(f"s.{k}" for k in case.keys), "s.x"])
        sql = await postgres_sql([s, root], datasets=case.datasets)
        casts = {cast_target(e) for e in _computed_projections(sql, alias=case.cast_alias)}
        if case.cast is None:
            assert casts <= {None}, sql  # a bare shifted column (time_shift) computes nothing here
        else:
            assert casts == {case.cast}, sql


def _computed_projections(sql: str, *, alias: str) -> list[Expression]:
    """Every non-column expression projected as ``alias`` anywhere in ``sql``."""
    tree = sqlglot.parse_one(sql, dialect="postgres")
    return [a.this for a in tree.find_all(exp.Alias) if a.alias == alias and not isinstance(a.this, exp.Column)]


# --------------------------------------------------------------------------- #
# A cast computed dimension groups as displayed; an inferred timestamp is not cast (design decision 7).
# --------------------------------------------------------------------------- #
def _by_id(expr: str) -> SlayerQuery:
    return q(population="orders", dimensions=["orders.id", d(expr, "v")], metrics=[m("count(orders.*)", "n")])


async def _projected(query: SlayerQuery, *, dialect: str, alias: str = "v") -> tuple[exp.Select, Expression]:
    sql = await _engine_generate(query=query, dataset=orders(), extra_datasets=[customers()], dialect=dialect)
    tree = sqlglot.parse_one(sql, dialect=dialect)
    (shown,) = [a.this for a in tree.find_all(exp.Alias) if a.alias == alias]
    return shown.find_ancestor(exp.Select), shown


class TestCastComputedDimension:
    @pytest.mark.parametrize("dialect", ["mysql", "clickhouse", "trino", "presto", "postgres", "duckdb", "snowflake"])
    async def test_an_inferred_timestamp_is_not_cast(self, dialect: str) -> None:
        _select, shown = await _projected(_by_id("date_add(orders.ordered_at, 1, 'hour')"), dialect=dialect)
        assert not isinstance(shown, exp.Cast), shown.sql(dialect=dialect)

    async def test_a_declared_timestamp_is_cast(self) -> None:
        query = q(population="orders", dimensions=["orders.region"],
                  metrics=[m("max(orders.ordered_at)", "x", datatype="DateTime")])
        _select, shown = await _projected(query, dialect="postgres", alias="x")
        assert cast_target(shown) == "TIMESTAMP", shown.sql(dialect="postgres")

    async def test_a_ranked_join_back_reads_the_same_cast_grain(self) -> None:
        query = q(population="orders", dimensions=[d("orders.quantity + 1", "v")],
                  metrics=[m("last(orders.amount)", "l"), m("sum(orders.amount)", "s")])
        sql = await _engine_generate(query=query, dataset=orders(), extra_datasets=[customers()], dialect="postgres")
        grains = [n for n in sqlglot.parse_one(sql, dialect="postgres").find_all(exp.Add)]
        assert grains, sql
        assert all(isinstance(g.parent, exp.Cast) for g in grains), sql

    @pytest.mark.parametrize("dialect", ["mysql", "postgres", "clickhouse"])
    async def test_grouped_by_the_displayed_cast(self, dialect: str) -> None:
        select, shown = await _projected(_by_id("orders.quantity + 1"), dialect=dialect)
        assert isinstance(shown, exp.Cast), shown.sql(dialect=dialect)
        group = select.args["group"].expressions
        assert any(g == shown for g in group), select.sql(dialect=dialect)


class TestArithmeticNullOperand:
    def test_only_a_null_literal_is_absorbed(self) -> None:
        quantity, unresolved, null = FieldKey(leaf="quantity"), FieldKey(leaf="unresolved"), LiteralKey(value=None)

        def typed(*operands: Any) -> Optional[DataType]:
            key = ArithmeticKey(op="+", operands=operands)
            return value_type(key, field_type=lambda k: INT if k == quantity else None)

        assert typed(quantity, null) is INT
        assert typed(null, null) is None
        assert typed(quantity, unresolved) is DOUBLE


# --------------------------------------------------------------------------- #
# Registry merge (design decision 13): an explicit type wins over a hidden-first inferred one.
# --------------------------------------------------------------------------- #
class TestExplicitTypeAfterHiddenIntern:
    def test_query_type(self) -> None:
        first = {"time_dimensions": MONTH, "metrics": [
            m("cumsum(sum(orders.quantity))", "cs"), m("sum(orders.quantity)", "sq", datatype="Decimal")]}
        assert emitted_types(first, names=["cs", "sq"]) == {"cs": INT, "sq": DEC}

    def test_query_type_after_a_filter(self) -> None:
        first = {**BY_REGION, "filter": "sum(orders.quantity) > 1",
                 "metrics": [m("sum(orders.quantity)", "sq", datatype="Decimal")]}
        assert emitted_types(first, names=["sq"]) == {"sq": DEC}

    def test_saved_metric_datatype(self) -> None:
        saved = orders().model_copy(update={"metrics": [
            DatasetMetric(name="total_q", expression="sum(quantity)", datatype=DataType.DECIMAL)]})
        first = {"time_dimensions": MONTH, "metrics": [m("cumsum(sum(orders.quantity))", "cs"), {"metric": "orders.total_q"}]}
        assert emitted_types(first, names=["cs", "total_q"], datasets=shop_datasets(orders_dataset=saved)) == {
            "cs": INT, "total_q": DEC}


# --------------------------------------------------------------------------- #
# Structural guarantees (design decision 6).
# --------------------------------------------------------------------------- #
class TestStructural:
    def test_stage_field_requires_a_type(self) -> None:
        with pytest.raises(ValidationError):
            StageField(name="a", sql_alias="a")  # pyright: ignore[reportCallIssue]

    def test_value_slot_requires_a_type(self) -> None:
        key = FieldKey(leaf="a")
        with pytest.raises(ValidationError):
            ValueSlot(id="s0", key=key, declared_name="a", phase=Phase.ROW)  # pyright: ignore[reportCallIssue, reportArgumentType]

    def test_stage_dataset_keeps_every_declared_field_type(self) -> None:
        types = [INT, TEXT, DOUBLE, DEC, BOOL, DataType.DATE, TS, DataType.OPAQUE]
        fields = [StageField(name=f"c{i}", sql_alias=f"c{i}", datatype=t) for i, t in enumerate(types)]
        dataset = dataset_from_stage_schema(name="s", schema=StageSchema(relation_name="s", fields=fields), data_source=DS)
        assert [f.datatype for f in dataset.fields] == types

    def test_stage_dataset_field_types_are_the_stage_field_types(self) -> None:
        first = {
            "time_dimensions": MONTH,
            "dimensions": [d("lower(orders.status)", "lo"), d("null", "nul"), "orders.region"],
            "metrics": [m("sum(orders.amount) / count(orders.*)", "r"), m("my_avg(orders.quantity)", "a"),
                        m("sum(customers.spend)", "ss"), m("lag(sum(orders.quantity), 1)", "lg")],
        }
        s = q(name="s", population="orders", **first)
        columns = stage_columns([s, q(population="s", dimensions=["s.region"])], stage="s")
        schema_fields = list(columns.values())
        assert all(c.datatype is not None for c in schema_fields), {c.name: c.datatype for c in schema_fields}
        dataset = dataset_from_stage_schema(name="s", schema=StageSchema(relation_name="s", fields=schema_fields),
                                            data_source=DS)
        assert {f.name: f.datatype for f in dataset.fields} == {c.name: c.datatype for c in schema_fields}


# --------------------------------------------------------------------------- #
# queries/time-dimensions › "Untyped stage column fails closed" (now a NULL-only, INT column).
# --------------------------------------------------------------------------- #
class TestNullOnlyStageColumnTimeDimension:
    def test_fails_with_the_temporal_error_naming_integer(self) -> None:
        s1 = q(name="s1", population="orders", dimensions=["orders.region", d("null", "nul")],
               metrics=[m("count(orders.*)", "n")])
        outer = q(population="s1", time_dimensions=[{"field": "s1.nul", "granularity": "month"}],
                  metrics=[m("sum(s1.n)", "t")])
        with pytest.raises(TimeDimensionFieldError) as info:
            stage_columns([s1, outer], stage="s1")
        message = str(info.value)
        assert "temporal" in message, message
        assert "Integer" in message, message
