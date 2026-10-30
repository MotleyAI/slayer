"""Value-returning conditionals share one type, checked at plan time (spec: queries/mixed-type-coercion)."""

from __future__ import annotations

from typing import Any

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import ScalarOperandTypeError
from slayer.core.keys import value_type
from slayer.core.models import Column, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.core.scope import ModelScope, StageColumn, StageSchema
from slayer.core.syntax import parse_expr
from slayer.engine.bind_inputs import bind_query_inputs
from slayer.engine.binding import bind_expr
from slayer.engine.key_metadata import scope_column_type
from slayer.engine.plan import plan_stages
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.ir.source_bundle import ResolvedSourceBundle
from tests._dev2177_fixtures import (
    MONTH,
    assert_branch_mismatch,
    check_concat_branches_rejected,
    dimension_query,
    m,
    q,
)
from tests._mcp_idiom_fixtures import idiom_models, make_idiom_engine

TS = "TIMESTAMP '2025-01-01 00:00:00'"
MIXED = "coalesce(status, amount)"

#: Conditional → (template over branches ``a`` / ``b``, accepted spellings of its name in the error).
FUNCTIONS: dict[str, tuple[str, tuple[str, ...]]] = {
    "iif": ("iif(amount > 15, {a}, {b})", ("iif", "case")),
    "case": ("CASE WHEN amount > 15 THEN {a} ELSE {b} END", ("case", "iif")),
    "simple-case": ("CASE status WHEN 'paid' THEN {a} ELSE {b} END", ("case", "iif")),
    "coalesce": ("coalesce({a}, {b})", ("coalesce",)),
    "ifnull": ("ifnull({a}, {b})", ("ifnull", "coalesce")),
    "greatest": ("greatest({a}, {b})", ("greatest",)),
    "least": ("least({a}, {b})", ("least",)),
}
#: Branch pairs every conditional accepts → the joined type.
COMPATIBLE: dict[str, tuple[str, str, DataType]] = {
    "null-absorbed": ("amount", "NULL", DataType.INT),
    "int-double": ("amount", "0.5", DataType.DOUBLE),
    "date-timestamp": ("order_date", TS, DataType.TIMESTAMP),
    "boolean-number": ("status = 'paid'", "amount", DataType.INT),
}
#: ``greatest`` / ``least`` read a TEXT operand beside a number as a number; the others reject the mix.
TEXT_AS_NUMBER = {"greatest", "least"}


def models_with_unknown(*, data_source: str = "test") -> list[SlayerModel]:
    orders, *rest = idiom_models(data_source=data_source)
    orders = orders.model_copy(update={"columns": [
        *orders.columns, Column(name="opaque", sql="amount * 2", type=DataType.UNKNOWN),
    ]})
    return [orders, *rest]


def _bundle(dialect: str) -> ResolvedSourceBundle:
    orders, *rest = models_with_unknown()
    return ResolvedSourceBundle(dialect=dialect, source_model=orders, referenced_models=rest)


def inferred_type(item: dict[str, Any], *, dialect: str) -> DataType | None:
    """The declared type of the one dimension or measure ``x`` in ``item``."""
    query = SlayerQuery.model_validate({"source_model": "orders", **item})
    prebound = bind_query_inputs(query=query, bundle=_bundle(dialect))
    (declared,) = [dm for dm in prebound.declared_measures if dm.public_name == "x"]
    return declared.type


def stage_type(item: dict[str, Any], *, dialect: str) -> DataType | None:
    """The type a later stage's schema reports for ``x``."""
    first = SlayerQuery.model_validate({"name": "s1", "source_model": "orders", **item})
    second = SlayerQuery.model_validate({"source_model": "s1", "dimensions": ["x"]})
    planned = plan_stages(queries=[first, second], bundle=_bundle(dialect))
    (column,) = [
        c for p in planned if p.stage_schema is not None and p.stage_schema.relation_name == "s1"
        for c in p.stage_schema.columns if c.name == "x"
    ]
    return column.type


def as_dimension(expression: str) -> dict[str, Any]:
    return {"dimensions": [{"expression": expression, "name": "x"}]}


@pytest.fixture(params=["sqlite", "duckdb"])
def dialect(request) -> str:
    return request.param


@pytest.fixture
async def engine(dialect: str):
    async for e in make_idiom_engine(dialect=dialect, models=models_with_unknown()):
        yield e


class TestFunctionMatrix:
    @pytest.mark.parametrize("pair", list(COMPATIBLE))
    @pytest.mark.parametrize("function", list(FUNCTIONS))
    async def test_compatible_branches_execute_typed(self, engine: SlayerQueryEngine, dialect: str,
                                                     function: str, pair: str) -> None:
        a, b, joined = COMPATIBLE[pair]
        expression = FUNCTIONS[function][0].format(a=a, b=b)
        resp = await engine.execute(dimension_query(expression))
        assert resp.data, resp.sql
        assert inferred_type(as_dimension(expression), dialect=dialect) is joined

    @pytest.mark.parametrize("function", list(FUNCTIONS))
    async def test_text_beside_a_number(self, engine: SlayerQueryEngine, dialect: str, function: str) -> None:
        template, names = FUNCTIONS[function]
        expression = template.format(a="status", b="amount")
        if function in TEXT_AS_NUMBER:
            await engine.execute(dimension_query(expression))
            assert inferred_type(as_dimension(expression), dialect=dialect) is DataType.DOUBLE
        else:
            await assert_branch_mismatch(engine, dimension_query(expression), "TEXT", "INT", call=names)

    @pytest.mark.parametrize("function", list(FUNCTIONS))
    async def test_non_numeric_text_literal_rejected(self, engine: SlayerQueryEngine, function: str) -> None:
        template, names = FUNCTIONS[function]
        query = dimension_query(template.format(a="'abc'", b="amount"))
        if function in TEXT_AS_NUMBER:
            with pytest.raises(ScalarOperandTypeError):
                await engine.execute(query)
        else:
            await assert_branch_mismatch(engine, query, "TEXT", "INT", call=names)

    @pytest.mark.parametrize("function", list(FUNCTIONS))
    async def test_date_beside_a_number_rejected(self, engine: SlayerQueryEngine, function: str) -> None:
        template, names = FUNCTIONS[function]
        await assert_branch_mismatch(
            engine, dimension_query(template.format(a="order_date", b="amount")), "DATE", "INT", call=names,
        )


class TestScenarios:
    async def test_concatenation_branch_is_text(self, engine: SlayerQueryEngine) -> None:
        await check_concat_branches_rejected(engine)

    async def test_joined_column_branch(self, engine: SlayerQueryEngine) -> None:
        await assert_branch_mismatch(
            engine, dimension_query("CASE WHEN amount > 1 THEN customers.name ELSE amount END"), "TEXT", "INT",
        )

    async def test_arithmetic_branch(self, engine: SlayerQueryEngine) -> None:
        await assert_branch_mismatch(
            engine, dimension_query("CASE WHEN amount > 1 THEN status ELSE amount + 1 END"), "TEXT", "INT",
        )

    async def test_conditional_inside_an_aggregation(self, engine: SlayerQueryEngine) -> None:
        await assert_branch_mismatch(
            engine, q(measures=[m("max(CASE WHEN amount > 1 THEN 'a' || 'b' ELSE amount END)")]), "TEXT", "INT",
        )

    @pytest.mark.parametrize(("expression", "types", "call"), [
        (MIXED, ("TEXT", "INT"), ("coalesce",)),
        ("ifnull(status, amount)", ("TEXT", "INT"), ("ifnull", "coalesce")),
        ("greatest(order_date, amount)", ("DATE", "INT"), ("greatest",)),
    ])
    async def test_null_handling_and_scalar_min_max(self, engine: SlayerQueryEngine, expression: str,
                                                    types: tuple[str, ...], call: tuple[str, ...]) -> None:
        await assert_branch_mismatch(engine, dimension_query(expression), *types, call=call)

    @pytest.mark.parametrize(("expression", "joined"), [
        ("CASE WHEN amount > 1 THEN amount ELSE 0.5 END", DataType.DOUBLE),
        (f"coalesce(order_date, {TS})", DataType.TIMESTAMP),
        ("iif(amount > 15, status = 'paid', 2)", DataType.INT),
        ("CASE WHEN amount > 1 THEN status END", DataType.TEXT),
        ("greatest(status, amount)", DataType.DOUBLE),
    ])
    async def test_compatible_mixes_execute_typed(self, engine: SlayerQueryEngine, dialect: str,
                                                  expression: str, joined: DataType) -> None:
        resp = await engine.execute(dimension_query(expression))
        assert resp.data, resp.sql
        assert inferred_type(as_dimension(expression), dialect=dialect) is joined
        assert stage_type(as_dimension(expression), dialect=dialect) is joined

    async def test_unknown_column_is_not_a_mismatch(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(dimension_query("CASE WHEN amount > 1 THEN opaque ELSE amount END"))
        assert sorted(int(r["orders.x"]) for r in resp.data) == [10, 20, 40, 50, 60, 90, 100, 120, 140, 150, 200]

    async def test_inside_a_stage(self, engine: SlayerQueryEngine) -> None:
        await assert_branch_mismatch(engine, [
            {"name": "s1", "source_model": "orders",
             "dimensions": [{"expression": "status", "name": "s"}, {"expression": "amount", "name": "n"}]},
            {"source_model": "s1", "dimensions": [{"expression": "coalesce(s, n)", "name": "x"}]},
        ], "TEXT", "INT", call=("coalesce",))


class TestEveryPosition:
    @pytest.mark.parametrize(("query", "types"), [
        pytest.param(dimension_query(MIXED), ("TEXT", "INT"), id="dimension"),
        pytest.param(q(time_dimensions=[{"dimension": "greatest(order_date, amount)", "granularity": "month"}],
                       measures=["count(*)"]), ("DATE", "INT"), id="time-dimension"),
        pytest.param(q(measures=[m(f"max({MIXED})")]), ("TEXT", "INT"), id="aggregate-input"),
        pytest.param(q(measures=[m("coalesce(sum(amount), max(status))")]), ("TEXT", "INT"), id="over-aggregates"),
        pytest.param(q(time_dimensions=MONTH, measures=[m(f"time_shift(max({MIXED}), -1)")]), ("TEXT", "INT"),
                     id="transform"),
        pytest.param(q(measures=[m(f"count(*) FILTER (WHERE {MIXED} IS NOT NULL)")]), ("TEXT", "INT"),
                     id="filter-clause"),
        pytest.param(q(measures=["count(*)"], filters=[f"{MIXED} IS NOT NULL"]), ("TEXT", "INT"), id="row-filter"),
        pytest.param(q(dimensions=["status"], measures=["count(*)"],
                       filters=["coalesce(max(status), sum(amount)) IS NOT NULL"]), ("TEXT", "INT"),
                     id="post-aggregation-filter"),
        pytest.param(q(dimensions=["id"], distinct_dimension_values=False, order=[{"column": MIXED}]),
                     ("TEXT", "INT"), id="order"),
    ])
    async def test_rejected_at_plan_time(self, engine: SlayerQueryEngine, query: SlayerQuery,
                                         types: tuple[str, ...]) -> None:
        await assert_branch_mismatch(engine, query, *types)


class TestInferredTypes:
    @pytest.mark.parametrize("dialect_name", ["duckdb", "sqlite", "postgres"])
    def test_coerced_measure_type(self, dialect_name: str) -> None:
        item = {"measures": [{"formula": "max(greatest(status, amount))", "name": "x"}]}
        assert inferred_type(item, dialect=dialect_name) is DataType.DOUBLE
        assert stage_type(item, dialect=dialect_name) is DataType.DOUBLE


class TestValueType:
    @staticmethod
    def _value_type(expression: str) -> DataType | None:
        bundle = _bundle("postgres")
        scope = ModelScope(source_model=bundle.source_model)
        key = bind_expr(parse_expr(expression), scope=scope, bundle=bundle).value_key
        return value_type(key, column_type=scope_column_type(scope=scope, bundle=bundle))

    @pytest.mark.parametrize(("expression", "joined"), [
        ("coalesce(amount, 0.5)", DataType.DOUBLE),
        ("ifnull(amount, NULL)", DataType.INT),
        (f"greatest(order_date, {TS})", DataType.TIMESTAMP),
        ("least(status = 'paid', amount)", DataType.INT),
        ("iif(amount > 1, amount, 0.5)", DataType.DOUBLE),
        ("coalesce(opaque, amount)", DataType.INT),
        ("coalesce(status, order_date)", None),
    ])
    def test_joined_branch_type(self, expression: str, joined: DataType | None) -> None:
        assert self._value_type(expression) is joined

    @pytest.mark.parametrize(("expression", "joined"), [
        ("coalesce(u, n)", DataType.INT), ("iif(n > 1, u, 'x')", DataType.TEXT), ("greatest(u, n)", DataType.INT),
    ])
    def test_untyped_stage_column_is_absorbed(self, expression: str, joined: DataType) -> None:
        schema = StageSchema(relation_name="s1", columns=[
            StageColumn(name="u", sql_alias="u"), StageColumn(name="n", sql_alias="n", type=DataType.INT),
        ])
        bundle = ResolvedSourceBundle(dialect="postgres")
        key = bind_expr(parse_expr(expression), scope=schema, bundle=bundle).value_key
        assert value_type(key, column_type=scope_column_type(scope=schema, bundle=bundle)) is joined
