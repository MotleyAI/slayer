"""Formula aggregations rendered through SqlTemplate (aggregations/formula-templates)."""

from __future__ import annotations

import re
from decimal import Decimal

import duckdb
import pytest
import sqlglot
from sqlglot import exp

from slayer.core.enums import DataType
from slayer.core.models import Aggregation, AggregationParam, Column, ModelMeasure, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.sql.generator import AggRenderSpec, SQLGenerator
from slayer.sql.sql_template import SqlTemplateError
from slayer.storage.sqlite_conn import transaction
from tests._dev1832_fixtures import (
    WAVG_AMOUNT_WEIGHT_QAMT,
    _seed_sqlite as seed_1832_sqlite,
    dev1832_models,
    sales_q,
)
from tests._dev1900_fixtures import (
    BAD_POP,
    _seed_duckdb as seed_1900_duckdb,
    _seed_sqlite as seed_1900_sqlite,
    bad_pop_vals,
    dev1900_models,
    orders_q as orders_1900_q,
)
from tests._dev1910_fixtures import (
    SPEND_WAVG_AMOUNT,
    SPEND_WAVG_AMOUNT_SOUTH,
    SPEND_WAVG_HOME,
    SPEND_WAVG_SOUTH,
)
from tests._engine_helpers import _engine_generate, seeded_exec_engine

TIER1 = ["postgres", "sqlite", "duckdb", "mysql", "clickhouse", "tsql", "snowflake", "bigquery"]

_ROWS = [(1, 10.0, 2.0, 1.0), (2, 20.0, 5.0, 3.0), (3, 30.0, 0.0, 2.0)]
WAVG_NETD_BY_QTY = 113.0 / 6.0
WAVG_PRICE_BY_Q2 = 190.0 / 9.0
SUMSQ_NETD = 1189.0
SCALED_PRICE_100 = 0.6


def _squash(sql: str) -> str:
    return re.sub(r"\s+", "", sql)


def _orders(aggs: tuple[Aggregation, ...] = ()) -> SlayerModel:
    return SlayerModel(
        name="orders", sql_table="orders", data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="price", sql="price", type=DataType.DOUBLE),
            Column(name="discount", sql="discount", type=DataType.DOUBLE),
            Column(name="netd", sql="price - discount", type=DataType.DOUBLE),
            Column(name="quantity", sql="quantity", type=DataType.DOUBLE),
            Column(name="q2", sql="quantity + 1"),
        ],
        aggregations=list(aggs),
    )


def _q(formula: str) -> SlayerQuery:
    return SlayerQuery(source_model="orders", measures=[ModelMeasure(formula=formula, name="m")])


async def _sql(formula: str, *, dialect: str = "postgres", aggs: tuple[Aggregation, ...] = ()) -> str:
    return await _engine_generate(query=_q(formula), model=_orders(aggs), dialect=dialect)


_SUMSQ = Aggregation(name="sumsq", formula="SUM({value} * {value})")
_SCALED = Aggregation(
    name="scaled", formula="SUM({value}) / {scale}",
    params=[AggregationParam(name="scale", sql="1")],
)


def _seed(db_path: str) -> None:
    if db_path.endswith(".duckdb"):
        con = duckdb.connect(db_path)
        try:
            con.execute("CREATE TABLE orders (id DOUBLE, price DOUBLE, discount DOUBLE, quantity DOUBLE)")
            con.executemany("INSERT INTO orders VALUES (?, ?, ?, ?)", _ROWS)
        finally:
            con.close()
        return
    with transaction(db_path) as conn:
        conn.execute("CREATE TABLE orders (id REAL, price REAL, discount REAL, quantity REAL)")
        conn.executemany("INSERT INTO orders VALUES (?, ?, ?, ?)", _ROWS)


_CAST = {
    "postgres": "DOUBLEPRECISION", "sqlite": "REAL", "duckdb": "DOUBLE", "mysql": "DOUBLE",
    "clickhouse": "Nullable(Float64)", "tsql": "FLOAT", "snowflake": "DOUBLE", "bigquery": "FLOAT64",
}


class TestFormulaGoldens:
    @pytest.mark.parametrize("dialect", TIER1)
    async def test_weighted_avg_compound_value(self, dialect: str) -> None:
        nullif = "nullIf" if dialect == "clickhouse" else "NULLIF"
        net = f"CAST(orders.price-orders.discountAS{_CAST[dialect]})"
        sql = _squash(await _sql("netd:weighted_avg(weight=quantity)", dialect=dialect))
        assert f"SUM({net}*orders.quantity)/{nullif}(SUM(orders.quantity),0)" in sql, sql

    @pytest.mark.parametrize("dialect", TIER1)
    async def test_custom_repeated_placeholder(self, dialect: str) -> None:
        net = f"CAST(orders.price-orders.discountAS{_CAST[dialect]})"
        sql = _squash(await _sql("netd:sumsq", dialect=dialect, aggs=(_SUMSQ,)))
        assert f"SUM({net}*{net})" in sql, sql


class TestCompoundValueKeepsGrouping:
    @pytest.mark.parametrize("dialect", TIER1)
    async def test_weighted_avg_multiplies_the_whole_value(self, dialect: str) -> None:
        sql = await _sql("netd:weighted_avg(weight=quantity)", dialect=dialect)
        tree = sqlglot.parse_one(sql, dialect=dialect)
        mul = next(m for m in tree.find_all(exp.Mul) if m.find_ancestor(exp.Sum))
        left_cols = {c.name for c in mul.this.find_all(exp.Column)}
        assert left_cols == {"price", "discount"}, sql
        assert {c.name for c in mul.expression.find_all(exp.Column)} == {"quantity"}, sql

    @pytest.mark.parametrize("dialect", [d for d in TIER1 if d != "clickhouse"])
    async def test_compound_weight_parenthesised_only_under_operator(self, dialect: str) -> None:
        sql = _squash(await _sql("price:weighted_avg(weight=q2)", dialect=dialect))
        assert "SUM(orders.price*(orders.quantity+1))/NULLIF(SUM(orders.quantity+1),0)" in sql

    async def test_compound_weight_clickhouse(self) -> None:
        sql = _squash(await _sql("price:weighted_avg(weight=q2)", dialect="clickhouse"))
        assert "SUM(orders.price*(orders.quantity+1))/nullIf(SUM(orders.quantity+1),0)" in sql

    @pytest.mark.parametrize("dialect", TIER1)
    async def test_plain_weighted_avg_byte_identical(self, dialect: str) -> None:
        sql = _squash(await _sql("price:weighted_avg(weight=quantity)", dialect=dialect))
        nullif = "nullIf" if dialect == "clickhouse" else "NULLIF"
        assert f"SUM(orders.price*orders.quantity)/{nullif}(SUM(orders.quantity),0)" in sql

    @pytest.mark.parametrize("dialect", TIER1)
    async def test_custom_repeated_placeholder_renders_each_occurrence(self, dialect: str) -> None:
        sql = await _sql("netd:sumsq", dialect=dialect, aggs=(_SUMSQ,))
        tree = sqlglot.parse_one(sql, dialect=dialect)
        mul = next(m for m in tree.find_all(exp.Mul) if m.find_ancestor(exp.Sum))
        assert mul.this is not mul.expression
        for side in (mul.this, mul.expression):
            assert {c.name for c in side.find_all(exp.Column)} == {"price", "discount"}, sql

    @pytest.mark.parametrize("dialect", [d for d in TIER1 if d != "clickhouse"])
    async def test_custom_compound_param_under_division(self, dialect: str) -> None:
        sql = _squash(await _sql("price:scaled(scale=q2)", dialect=dialect, aggs=(_SCALED,)))
        assert "SUM(orders.price)/(orders.quantity+1)" in sql


class TestFormulaExecution:
    @pytest.mark.parametrize("dialect", ["sqlite", "duckdb"])
    @pytest.mark.parametrize(("formula", "expected"), [
        ("netd:weighted_avg(weight=quantity)", WAVG_NETD_BY_QTY),
        ("price:weighted_avg(weight=q2)", WAVG_PRICE_BY_Q2),
        ("netd:sumsq", SUMSQ_NETD),
        ("price:scaled(scale=100)", SCALED_PRICE_100),
    ])
    async def test_executes_to_oracle(self, dialect: str, formula: str, expected: float) -> None:
        async with seeded_exec_engine(
            dialect=dialect, seed=_seed, models=[_orders((_SUMSQ, _SCALED))],
        ) as (engine, _db):
            resp = await engine.execute(_q(formula))
        assert resp.data[0]["orders.m"] == pytest.approx(expected)


class TestPlaceholderRecognition:
    async def test_string_literal_placeholder_is_inert(self) -> None:
        lbl = Aggregation(name="lbl", formula="MAX(CASE WHEN {value} > 0 THEN '{value}' END)")
        sql = await _sql("price:lbl", aggs=(lbl,))
        assert "WHEN orders.price > 0 THEN '{value}'" in " ".join(sql.split())
        assert "'orders.price'" not in sql

    async def test_whitespace_inside_braces(self) -> None:
        spaced = Aggregation(name="s1", formula="SUM({ value })")
        tight = Aggregation(name="s1", formula="SUM({value})")
        assert await _sql("price:s1", aggs=(spaced,)) == await _sql("price:s1", aggs=(tight,))


class TestInvalidTemplates:
    @pytest.mark.parametrize("formula", ["SUM({value}", "SUM({t}.amount)"])
    async def test_invalid_formula_fails_naming_the_aggregation(self, formula: str) -> None:
        bad = Aggregation(name="broken_agg", formula=formula)
        with pytest.raises(SqlTemplateError, match="broken_agg"):
            await _engine_generate(query=_q("price:broken_agg"), model=_orders((bad,)), validate=False)

    @pytest.mark.parametrize("dialect", TIER1)
    async def test_unbound_placeholder_fails_naming_aggregation_and_placeholder(
        self, dialect: str,
    ) -> None:
        missing = Aggregation(name="needs_scale", formula="SUM({value}) * {scale}")
        with pytest.raises(SqlTemplateError, match=r"needs_scale[\s\S]*scale|scale[\s\S]*needs_scale"):
            await _sql("price:needs_scale", dialect=dialect, aggs=(missing,))


def _pct_spec(p: str | None = None, *, default: str | None = None) -> AggRenderSpec:
    agg_def = (
        Aggregation(name="percentile", params=[AggregationParam(name="p", sql=default)])
        if default is not None else None
    )
    return AggRenderSpec(
        name="amount", sql="amount", model_name="orders", alias="orders.amount_percentile",
        aggregation="percentile", agg_kwargs={} if p is None else {"p": p},
        aggregation_def=agg_def,
    )


def _emitted_p(sql: str) -> exp.Expression:
    tree = sqlglot.parse_one(sql, dialect="postgres")
    return tree.find(exp.PercentileCont).this


class TestPercentileP:
    @pytest.mark.parametrize(("p", "value"), [
        ("0", Decimal(0)), ("1", Decimal(1)), ("0.50", Decimal("0.5")),
        ("5e-2", Decimal("0.05")), ("-0", Decimal(0)), ("(0.5)", Decimal("0.5")),
    ])
    def test_accepted_query_time(self, p: str, value: Decimal) -> None:
        sql = SQLGenerator(dialect="postgres")._build_percentile(_pct_spec(p)).sql(dialect="postgres")
        emitted = _emitted_p(sql)
        assert Decimal(emitted.sql(dialect="postgres").replace("(", "").replace(")", "")) == value

    @pytest.mark.parametrize(("p", "spelled"), [
        ("0", "0"), ("1", "1"), ("0.50", "0.50"), ("5e-2", "5e-2"), ("-0", "0"), ("(0.5)", "0.5"),
    ])
    def test_literal_spelling_emitted(self, p: str, spelled: str) -> None:
        sql = SQLGenerator(dialect="postgres")._build_percentile(_pct_spec(p)).sql(dialect="postgres")
        assert sql == f"PERCENTILE_CONT({spelled}) WITHIN GROUP (ORDER BY orders.amount)"

    @pytest.mark.parametrize("p", ["(0.5)", "-0", "0.50"])
    def test_accepted_model_default(self, p: str) -> None:
        SQLGenerator(dialect="postgres")._build_percentile(_pct_spec(default=p))

    async def test_model_default_spelling_reaches_the_query_sql(self) -> None:
        pct = Aggregation(name="percentile", params=[AggregationParam(name="p", sql="0.50")])
        sql = await _sql("price:percentile", aggs=(pct,))
        assert "PERCENTILE_CONT(0.50)" in sql

    @pytest.mark.parametrize("p", ["nan", "NaN", "1e999", "1.5", "-0.1", "'0.5'", "quantity",
                                   "0.1 + 0.2", "inf"])
    def test_rejected_query_time(self, p: str) -> None:
        with pytest.raises(ValueError, match=r"numeric literal|\[0, 1\]|Unsafe value"):
            SQLGenerator(dialect="postgres")._build_percentile(_pct_spec(p))

    @pytest.mark.parametrize("p", ["nan", "1e999", "1.5", "'0.5'", "quantity", "0.1 + 0.2",
                                   "pg_sleep(10)"])
    def test_rejected_model_default(self, p: str) -> None:
        with pytest.raises(ValueError, match=r"numeric literal|\[0, 1\]"):
            SQLGenerator(dialect="postgres")._build_percentile(_pct_spec(default=p))

    async def test_non_literal_p_rejected_at_query_level(self) -> None:
        with pytest.raises(ValueError, match=r"must be a numeric literal in \[0, 1\]"):
            await _sql("price:percentile(p=quantity)")


class TestValueResolutionIsAst:
    def test_text_helpers_removed(self) -> None:
        assert not hasattr(SQLGenerator, "_resolve_value_sql")
        assert not hasattr(SQLGenerator, "_paren_fragment")

    def test_resolve_value_ast_returns_expression(self) -> None:
        spec = AggRenderSpec(
            name="netd", sql="price - discount", model_name="orders", alias="orders.netd_sum",
            aggregation="sum", agg_kwargs={}, column_type=DataType.DOUBLE,
        )
        out = SQLGenerator(dialect="postgres")._resolve_value_ast(spec)
        assert isinstance(out, exp.Expression)

    @pytest.mark.parametrize("measure", [SPEND_WAVG_HOME, SPEND_WAVG_AMOUNT])
    async def test_picked_value_not_reparsed(
        self, monkeypatch: pytest.MonkeyPatch, measure: ModelMeasure,
    ) -> None:
        seen: list[str] = []
        original = SQLGenerator._parse

        def _spy(self, sql, *, dialect=None):  # noqa: ANN001
            seen.append(sql)
            return original(self, sql, dialect=dialect)

        monkeypatch.setattr(SQLGenerator, "_parse", _spy)
        async with seeded_exec_engine(
            dialect="sqlite", seed=seed_1900_sqlite, models=dev1900_models(),
        ) as (engine, _db):
            resp = await engine.execute(_picked_q(measure), dry_run=True)
        assert re.search(r"MAX\(\w+\.spend\)", _squash(resp.sql)), resp.sql
        round_trips = [s for s in seen if re.fullmatch(r'\s*\w+\."?spend"?\s*', s)]
        assert round_trips == [], round_trips


def _picked_q(measure: ModelMeasure) -> SlayerQuery:
    return orders_1900_q(dimensions=[BAD_POP], measures=[measure], to_many_handling="associate")


class TestPickedValueExecution:
    @pytest.mark.parametrize("dialect", ["sqlite", "duckdb"])
    @pytest.mark.parametrize(("measure", "south"), [
        (SPEND_WAVG_HOME, SPEND_WAVG_SOUTH),
        (SPEND_WAVG_AMOUNT, SPEND_WAVG_AMOUNT_SOUTH),
    ])
    async def test_cross_model_picked_oracle(
        self, dialect: str, measure: ModelMeasure, south: float,
    ) -> None:
        seed = seed_1900_duckdb if dialect == "duckdb" else seed_1900_sqlite
        async with seeded_exec_engine(
            dialect=dialect, seed=seed, models=dev1900_models(),
        ) as (engine, _db):
            resp = await engine.execute(_picked_q(measure))
        assert float(bad_pop_vals(resp, "orders.wa")[230.0]) == pytest.approx(south)

    async def test_filtered_weight_oracle(self) -> None:
        async with seeded_exec_engine(
            dialect="sqlite", seed=seed_1832_sqlite, models=dev1832_models(),
        ) as (engine, _db):
            resp = await engine.execute(sales_q(measures=[ModelMeasure(
                formula="amount:weighted_avg(weight=q_amount)", name="m")]))
        assert resp.data[0]["sales.m"] == pytest.approx(WAVG_AMOUNT_WEIGHT_QAMT)
