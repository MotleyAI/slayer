"""Formula aggregations rendered through SqlTemplate (aggregations/formula-templates)."""

from __future__ import annotations

import re
from decimal import Decimal

import duckdb
import pytest
import sqlglot
from sqlglot import exp
from pydantic import ValidationError
from sqlglot.expressions.core import Expression

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.errors import AggregationArgumentError, UnresolvableDimensionJoinError
from slayer.core.models import (
    Aggregation, AggregationParam, Column, ModelJoin, ModelMeasure, SlayerModel,
)
from slayer.core.query import ColumnRef, SlayerQuery, TimeDimension
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


async def _joined_sql(agg: Aggregation, formula: str) -> str:
    customers = SlayerModel(
        name="customers", sql_table="customers", data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="weight", sql="weight", type=DataType.DOUBLE),
        ],
    )
    host = _orders((agg,)).model_copy(
        update={"joins": [ModelJoin(target_model="customers", join_pairs=[["id", "id"]])]},
    )
    return await _engine_generate(query=_q(formula), model=host, extra_models=[customers])


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

    async def test_whitespace_placeholder_kwarg_registers_its_join(self) -> None:
        query = "price:scaled(scale='customers.weight')"
        spaced = await _joined_sql(Aggregation(name="scaled", formula="SUM({value}) / MAX({ scale })"), query)
        assert spaced == await _joined_sql(
            Aggregation(name="scaled", formula="SUM({value}) / MAX({scale})"), query,
        )
        assert "JOIN customers" in spaced

    async def test_unused_param_default_registers_no_join(self) -> None:
        unused = Aggregation(
            name="plain", formula="SUM({value})",
            params=[AggregationParam(name="w", sql="customers.weight")],
        )
        assert "customers" not in await _joined_sql(unused, "price:plain")

    @pytest.mark.parametrize("declared", [Aggregation(name="corr"), _SUMSQ])
    async def test_formula_less_builtin_string_kwarg_registers_its_join(
        self, declared: Aggregation,
    ) -> None:
        sql = await _joined_sql(declared, "price:corr(other='customers.weight')")
        assert "CORR(orders.price, customers.weight)" in sql
        assert "JOIN customers" in sql


class TestInvalidTemplates:
    @pytest.mark.parametrize("formula", ["SUM({value}", "SUM({t}.amount)"])
    async def test_invalid_formula_fails_naming_the_aggregation(self, formula: str) -> None:
        bad = Aggregation(name="broken_agg", formula=formula)
        query = _q("price:broken_agg")
        model = _orders((bad,))
        with pytest.raises(SqlTemplateError, match="broken_agg"):
            await _engine_generate(query=query, model=model, validate=False)

    @pytest.mark.parametrize("dialect", TIER1)
    async def test_unbound_placeholder_fails_naming_aggregation_and_placeholder(
        self, dialect: str,
    ) -> None:
        missing = Aggregation(name="needs_scale", formula="SUM({value}) * {scale}")
        with pytest.raises(SqlTemplateError, match=r"needs_scale[\s\S]*scale|scale[\s\S]*needs_scale"):
            await _sql("price:needs_scale", dialect=dialect, aggs=(missing,))

    @pytest.mark.parametrize("sql", ["", "  "])
    def test_empty_param_default_is_rejected(self, sql: str) -> None:
        with pytest.raises(ValidationError, match="'scale' has an empty sql default"):
            AggregationParam(name="scale", sql=sql)

    @pytest.mark.parametrize("name", ["custom_agg", "weighted_avg", "percentile"])
    def test_param_named_value_is_rejected(self, name: str) -> None:
        params = [AggregationParam(name="value", sql="1")]
        with pytest.raises(ValidationError, match=rf"'{name}'.*may not be named 'value'.*aggregated column"):
            Aggregation(name=name, formula="SUM({value})" if name == "custom_agg" else None, params=params)

    @pytest.mark.parametrize(("formula", "aggs"), [
        ("price:sumsq(value=0)", (_SUMSQ,)),
        ("price:sumsq(value=missing.path)", (_SUMSQ,)),
        ("price:weighted_avg(weight=quantity, value=0)", ()),
        ("price:sum(value=0)", (Aggregation(name="sum", formula="SUM({value}) * 2"),)),
    ])
    async def test_value_kwarg_on_value_template_explains(
        self, formula: str, aggs: tuple[Aggregation, ...],
    ) -> None:
        with pytest.raises(SqlTemplateError, match=r"may not be named 'value'.*aggregated column"):
            await _sql(formula, aggs=aggs)

    async def test_dialect_only_formula_accepts_its_kwarg(self) -> None:
        duck = Aggregation(name="dk", formula="SUM({value}) * {scale} // 2")  # `//`: DuckDB-only
        sql = await _sql("price:dk(scale=2)", dialect="duckdb", aggs=(duck,))
        assert "SUM(orders.price) * 2 // 2" in sql

    @pytest.mark.parametrize(("formula", "aggs", "message"), [
        ("price:SUM(value=0)", (), r"'sum' takes no args or kwargs other than window; got 'value'"),
        ("price:percentile(p=0.5, value=0)", (), r"'percentile' does not accept argument 'value'; accepted: p, window"),
        ("price:corr(other=quantity, foo=1)", (), r"'corr' does not accept argument 'foo'; accepted: other, window"),
        ("price:rows(value=1)", (Aggregation(name="rows", formula="COUNT(*)"),),
         r"'rows' takes no args or kwargs other than window; got 'value'"),
        ("price:scaled(scale=2, bogus=1)", (_SCALED,), r"'scaled' does not accept argument 'bogus'; accepted: scale, window"),
    ])
    async def test_unknown_kwarg_is_rejected(
        self, formula: str, aggs: tuple[Aggregation, ...], message: str,
    ) -> None:
        with pytest.raises(ValueError, match=message) as ei:
            await _sql(formula, aggs=aggs)
        assert "aggregated column" not in str(ei.value)

    @pytest.mark.parametrize("name", ["custom_agg", "weighted_avg"])
    @pytest.mark.parametrize("formula", ["", "  "])
    def test_empty_formula_is_rejected(self, name: str, formula: str) -> None:
        with pytest.raises(ValidationError, match=f"'{name}' has an empty formula"):
            Aggregation(name=name, formula=formula)


def _pct_spec(p: str | None = None, *, default: str | None = None) -> AggRenderSpec:
    agg_def = (
        Aggregation(name="percentile", params=[AggregationParam(name="p", sql=default)])
        if default is not None else None
    )
    return AggRenderSpec.model_validate({
        "name": "amount", "sql": "amount", "model_name": "orders", "alias": "orders.amount_percentile",
        "aggregation": "percentile", "agg_kwargs": {} if p is None else {"p": p},
        "aggregation_def": agg_def,
    })


def _emitted_p(sql: str) -> Expression:
    node = sqlglot.parse_one(sql, dialect="postgres").find(exp.PercentileCont)
    assert node is not None
    return node.this


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
        gen = SQLGenerator(dialect="postgres")
        spec = _pct_spec(p)
        with pytest.raises(ValueError, match=r"numeric literal|\[0, 1\]|Unsafe value"):
            gen._build_percentile(spec)

    @pytest.mark.parametrize("p", ["nan", "1e999", "1.5", "'0.5'", "quantity", "0.1 + 0.2",
                                   "pg_sleep(10)"])
    def test_rejected_model_default(self, p: str) -> None:
        gen = SQLGenerator(dialect="postgres")
        spec = _pct_spec(default=p)
        with pytest.raises(ValueError, match=r"numeric literal|\[0, 1\]"):
            gen._build_percentile(spec)

    def test_range_error_shows_the_signed_value(self) -> None:
        gen = SQLGenerator(dialect="postgres")
        spec = _pct_spec("-0.5")
        with pytest.raises(ValueError, match=r"got -0\.5\.$"):
            gen._build_percentile(spec)

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
        assert isinstance(out, Expression)

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
        assert resp.sql is not None
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


_XOR = Aggregation(name="masked", formula="SUM({value}) # {mask}")
_ARR = Aggregation(name="arr", formula="SUM({value}) * ARRAY[{scale}][1]")
_SUM_SCALE = Aggregation(
    name="sum", formula="SUM({value}) * {scale}", params=[AggregationParam(name="scale", sql="2")],
)
_WAVG_K = Aggregation(
    name="weighted_avg", formula="SUM({value}) * {k}", params=[AggregationParam(name="k", sql="7")],
)
_K_UNUSED = Aggregation(name="kx", formula="SUM({value}) * {k}", params=[
    AggregationParam(name="unused", sql="1"), AggregationParam(name="k", sql="2"),
])


async def _windowed_sql(formula: str, agg: Aggregation) -> str:
    model = _orders((agg,)).model_copy(update={"columns": [
        *_orders().columns, Column(name="ordered_at", sql="ordered_at", type=DataType.TIMESTAMP),
    ]})
    query = SlayerQuery(
        source_model="orders", measures=[ModelMeasure(formula=formula, name="m")],
        time_dimensions=[TimeDimension(dimension=ColumnRef(name="ordered_at"), granularity=TimeGranularity.MONTH)],
    )
    return " ".join((await _engine_generate(query=query, model=model)).split())


class TestKwargNamesFollowTheRenderDialect:
    async def test_postgres_array_placeholder_is_accepted(self) -> None:
        sql = await _sql("price:arr(scale=3)", aggs=(_ARR,))
        assert "SUM(orders.price)*(ARRAY[3])[1]" in _squash(sql)

    async def test_placeholder_only_the_target_dialect_sees_is_accepted(self) -> None:
        sql = await _sql("price:masked(mask=3)", aggs=(_XOR,))
        assert "SUM(orders.price) # 3" in sql

    async def test_placeholder_hidden_in_the_target_dialect_is_rejected(self) -> None:
        with pytest.raises(AggregationArgumentError, match="'masked' takes no args or kwargs other than window"):
            await _sql("price:masked(mask=3)", dialect="mysql", aggs=(_XOR,))

    async def test_untokenizable_formula_fails_before_arguments_bind(self) -> None:
        bad = Aggregation(name="bad", formula="SUM({value}) + 'x")
        with pytest.raises(SqlTemplateError, match="Aggregation 'bad': cannot tokenize"):
            await _sql("price:bad(foo=missing.path)", aggs=(bad,))


class TestOverrideFormulaRenders:
    async def test_builtin_override_formula_renders_with_its_default(self) -> None:
        assert "SUM(orders.price) * 2" in await _sql("price:sum", aggs=(_SUM_SCALE,))

    async def test_builtin_override_formula_takes_its_placeholder_kwarg(self) -> None:
        assert "SUM(orders.price) * 3" in await _sql("price:sum(scale=3)", aggs=(_SUM_SCALE,))

    async def test_weighted_avg_override_needs_no_builtin_weight(self) -> None:
        assert "SUM(orders.price) * 7" in await _sql("price:weighted_avg", aggs=(_WAVG_K,))

    async def test_builtin_param_the_override_never_reads_is_rejected(self) -> None:
        with pytest.raises(AggregationArgumentError, match="'weighted_avg' does not accept argument 'weight'; accepted: k, window"):
            await _sql("price:weighted_avg(weight=quantity)", aggs=(_WAVG_K,))

    @pytest.mark.parametrize(("formula", "agg", "rendered"), [
        ("price:sum(window='1y')", _SUM_SCALE, "SUM(_src._w_value) * 2"),
        ("price:weighted_avg(window='1y')", _WAVG_K, "SUM(_src._w_value) * 7"),
        ("price:weighted_avg(k=3, window='1y')", _WAVG_K, "SUM(_src._w_value) * 3"),
    ])
    async def test_windowed_override_formula_renders(self, formula: str, agg: Aggregation, rendered: str) -> None:
        assert rendered in await _windowed_sql(formula, agg)

    async def test_formula_less_override_keeps_the_builder(self) -> None:
        pct = Aggregation(name="percentile", params=[AggregationParam(name="p", sql="0.25")])
        assert "PERCENTILE_CONT(0.25)" in await _sql("price:percentile", aggs=(pct,))


class TestArgumentNamesCheckedFirst:
    async def test_positional_onto_unreferenced_param_is_rejected(self) -> None:
        with pytest.raises(AggregationArgumentError, match="'kx' does not accept argument 'unused'; accepted: k, window"):
            await _sql("kx(price, 5)", aggs=(_K_UNUSED,))

    async def test_positional_name_rejected_before_its_value_binds(self) -> None:
        with pytest.raises(AggregationArgumentError, match="'kx' does not accept argument 'unused'"):
            await _sql("kx(price, missing.path)", aggs=(_K_UNUSED,))

    async def test_keyword_name_rejected_before_its_value_binds(self) -> None:
        with pytest.raises(AggregationArgumentError, match="'scaled' does not accept argument 'bogus'"):
            await _sql("price:scaled(bogus=missing.path)", aggs=(_SCALED,))

    async def test_accepted_name_still_resolves_its_value(self) -> None:
        with pytest.raises(UnresolvableDimensionJoinError, match="missing.path"):
            await _sql("price:scaled(scale=missing.path)", aggs=(_SCALED,))


_COUNT_K = Aggregation(
    name="count", formula="COUNT({value}) * {k}", params=[AggregationParam(name="k", sql="7")],
)


async def _star_sql(formula: str, *, filters: list[str] | None = None) -> str:
    query = SlayerQuery(
        source_model="orders", measures=[ModelMeasure(formula=formula, name="m")], filters=filters,
    )
    return " ".join((await _engine_generate(query=query, model=_orders((_COUNT_K,)))).split())


async def _associated_star_sql(formula: str) -> str:
    customers = SlayerModel(
        name="customers", sql_table="customers", data_source="test", aggregations=[_COUNT_K],
        columns=[Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True)],
    )
    host = _orders().model_copy(update={"joins": [ModelJoin(target_model="customers", join_pairs=[["id", "id"]])]})
    query = SlayerQuery(
        source_model="orders", measures=[ModelMeasure(formula=formula, name="m")],
        dimensions=[ColumnRef(name="quantity")], to_many_handling="associate",
    )
    return " ".join((await _engine_generate(query=query, model=host, extra_models=[customers])).split())


class TestCountStarOverride:
    @pytest.mark.parametrize(("formula", "rendered"), [
        ("*:count", "COUNT(*) * 7"), ("*:count(k=3)", "COUNT(*) * 3"), ("count(*)", "COUNT(*) * 7"),
    ])
    async def test_star_renders_the_override(self, formula: str, rendered: str) -> None:
        assert rendered in await _star_sql(formula)

    async def test_having_renders_the_override(self) -> None:
        assert "HAVING(COUNT(*)*7)>1" in _squash(await _star_sql("*:count", filters=["count(*) > 1"]))

    async def test_windowed_star_renders_the_override(self) -> None:
        assert "COUNT(_src._w_value) * 7" in await _windowed_sql("*:count(window='1y')", _COUNT_K)

    async def test_associated_cross_model_star_renders_the_override(self) -> None:
        assert "COUNT(*) * 7" in await _associated_star_sql("customers.*:count")
