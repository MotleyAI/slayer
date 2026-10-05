"""Placeholders inside ordinary string literals of aggregation formulas (aggregations/formula-templates)."""

from __future__ import annotations

import os
import re
import tempfile

import duckdb
import pytest
import sqlglot
from sqlglot import exp
from sqlglot.expressions.core import Expression

from slayer.core.enums import DataType
from slayer.core.models import (
    Aggregation, AggregationParam, Column, DatasourceConfig, ModelMeasure, SlayerModel,
)
from slayer.core.query import SlayerQuery
from slayer.sql.client import SlayerSQLClient
from slayer.sql.sql_template import (
    SqlTemplateError, aggregation_reads, check_aggregation_definition, placeholder_names, sql_template,
)
from slayer.storage.sqlite_conn import transaction
from slayer.storage.yaml_storage import YAMLStorage
from tests._engine_helpers import _engine_generate, seeded_exec_engine

TIER1 = ["postgres", "sqlite", "duckdb", "mysql", "clickhouse", "tsql", "snowflake", "bigquery"]

_AMOUNTS = [10.0, 20.0, 30.0]
TWICE_SUM = 2 * sum(_AMOUNTS)

_SUM_N = "SUM({value}) * '{n}'"
_SUM_N_CAST = "SUM({value}) * CAST('{n}' AS DOUBLE)"
_LABEL = "MAX(CASE WHEN {value} > 0 THEN '{n}' END)"


def _agg(formula: str, *, name: str = "agg", **defaults: str) -> Aggregation:
    return Aggregation(
        name=name, formula=formula,
        params=[AggregationParam(name=k, sql=v) for k, v in defaults.items()],
    )


def _orders(*aggs: Aggregation) -> SlayerModel:
    return SlayerModel(
        name="orders", sql_table="orders", data_source="test",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="amount", type=DataType.DOUBLE),
        ],
        aggregations=list(aggs),
    )


def _q(formula: str) -> SlayerQuery:
    return SlayerQuery(source_model="orders", measures=[ModelMeasure(formula=formula, name="m")])


async def _dry(formula: str, *, agg: Aggregation, dialect: str = "postgres", validate: bool = False) -> str:
    return await _engine_generate(query=_q(formula), model=_orders(agg), dialect=dialect, validate=validate)


def _then_literal(sql: str, *, dialect: str) -> Expression:
    case = sqlglot.parse_one(sql, read=dialect).find(exp.Case)
    assert case is not None, sql
    return case.args["ifs"][0].args["true"]


def _seed(db_path: str) -> None:
    rows = [(i, a) for i, a in enumerate(_AMOUNTS, start=1)]
    if db_path.endswith(".duckdb"):
        con = duckdb.connect(db_path)
        try:
            con.execute("CREATE TABLE orders (id INTEGER, amount DOUBLE)")
            con.executemany("INSERT INTO orders VALUES (?, ?)", rows)
        finally:
            con.close()
        return
    with transaction(db_path) as conn:
        conn.execute("CREATE TABLE orders (id INTEGER, amount REAL)")
        conn.executemany("INSERT INTO orders VALUES (?, ?)", rows)


async def _save(model: SlayerModel, *, dialect: str = "sqlite") -> None:
    with tempfile.TemporaryDirectory() as tmp:
        storage = YAMLStorage(base_dir=tmp)
        await storage.save_datasource(DatasourceConfig(
            name="test", type=dialect, database=os.path.join(tmp, "x.db")))
        await storage.save_model(model)
        await storage.save_model(model)


def _names_placeholder(*, err: BaseException, name: str, formula: str) -> None:
    msg = str(err)
    assert formula in msg, msg
    assert re.search(rf"[{{'\"]{name}[}}'\"]", msg.replace(formula, "")), msg


class TestQuotedDefaultExecutes:
    @pytest.mark.parametrize(("dialect", "formula"), [("sqlite", _SUM_N), ("duckdb", _SUM_N_CAST)])
    async def test_numeric_default_substitutes(self, dialect: str, formula: str) -> None:
        async with seeded_exec_engine(
            dialect=dialect, seed=_seed, models=[_orders(_agg(formula, name="sum_n", n="2"))],
        ) as (engine, _db):
            resp = await engine.execute(_q("sum_n(amount)"))
        assert float(resp.data[0]["orders.m"]) == pytest.approx(TWICE_SUM)

    @pytest.mark.parametrize(("dialect", "formula"), [("sqlite", _SUM_N), ("duckdb", _SUM_N_CAST)])
    async def test_numeric_kwarg_substitutes(self, dialect: str, formula: str) -> None:
        async with seeded_exec_engine(
            dialect=dialect, seed=_seed, models=[_orders(_agg(formula, name="sum_n", n="2"))],
        ) as (engine, _db):
            resp = await engine.execute(_q("sum_n(amount, n=3)"))
        assert float(resp.data[0]["orders.m"]) == pytest.approx(3 * sum(_AMOUNTS))


class TestQuotedPlaceholderIsRead:
    @pytest.mark.parametrize("dialect", ["sqlite", "duckdb", "postgres"])
    async def test_model_resaves(self, dialect: str) -> None:
        await _save(_orders(_agg(_SUM_N, name="sum_n", n="2")), dialect=dialect)

    @pytest.mark.parametrize("dialect", TIER1)
    def test_placeholder_names_include_quoted(self, dialect: str) -> None:
        assert placeholder_names(_SUM_N, dialect) == frozenset({"value", "n"})

    @pytest.mark.parametrize("dialect", TIER1)
    def test_aggregation_reads_include_quoted(self, dialect: str) -> None:
        agg = _agg(_SUM_N, name="sum_n", n="2")
        assert "n" in aggregation_reads(agg="sum_n", definition=agg, dialect=dialect)


class TestDoubledBraces:
    FORMULA = "MAX(CASE WHEN {value} > 0 THEN '{{n}}' END)"

    @pytest.mark.parametrize("dialect", ["postgres", "duckdb", "sqlite"])
    async def test_doubled_braces_emit_one_brace(self, dialect: str) -> None:
        sql = await _dry("lit(amount)", agg=_agg(self.FORMULA, name="lit"), dialect=dialect)
        lit = _then_literal(sql, dialect=dialect)
        assert isinstance(lit, exp.Literal)
        assert lit.is_string
        assert lit.this == "{n}"

    def test_doubled_braces_are_not_a_read(self) -> None:
        assert placeholder_names(self.FORMULA, "postgres") == frozenset({"value"})


class TestInertBraces:
    @pytest.mark.parametrize("formula", [
        'SUM({value}) + MAX("{n}")',
        "SUM({value}) /* {n} */",
    ])
    def test_quoted_identifier_or_comment_is_inert(self, formula: str) -> None:
        template = sql_template(text=formula, dialect="postgres")
        assert template.placeholder_names == frozenset({"value"})
        rendered = template.render({"value": exp.column("amount")}).sql(dialect="postgres", comments=True)
        assert "{n}" in rendered
        agg = _agg(formula, n="2")
        with pytest.raises(ValueError, match="'n' is never referenced"):
            check_aggregation_definition(where="w", agg=agg, dialect="postgres")


_NON_ORDINARY = [
    pytest.param("postgres", "MAX(CASE WHEN {value} > 0 THEN N'{n}' END)", id="national-postgres"),
    pytest.param("tsql", "MAX(CASE WHEN {value} > 0 THEN N'{n}' END)", id="national-tsql"),
    pytest.param("postgres", "MAX(CASE WHEN {value} > 0 THEN E'{n}' END)", id="escape"),
    pytest.param("postgres", "MAX(CASE WHEN {value} > 0 THEN $$x{n}$$ END)", id="dollar"),
    pytest.param("postgres", "MAX(CASE WHEN {value} > 0 THEN $tag$x{n}$tag$ END)", id="dollar-tagged"),
    pytest.param("bigquery", "MAX(CASE WHEN {value} > 0 THEN r'{n}' END)", id="raw"),
    pytest.param("bigquery", "MAX(CASE WHEN {value} > 0 THEN R'{n}' END)", id="raw-upper"),
    pytest.param("bigquery", 'MAX(CASE WHEN {value} > 0 THEN r"{n}" END)', id="raw-double-quoted"),
    pytest.param("bigquery", "MAX(CASE WHEN {value} > 0 THEN b'{n}' END)", id="byte"),
    pytest.param("bigquery", 'MAX(CASE WHEN {value} > 0 THEN b"{n}" END)', id="byte-double-quoted"),
]


class TestNonOrdinaryLiteralRejected:
    @pytest.mark.parametrize(("dialect", "formula"), _NON_ORDINARY)
    def test_rejected_at_save(self, dialect: str, formula: str) -> None:
        agg = _agg(formula, n="2")
        with pytest.raises(SqlTemplateError) as ei:
            check_aggregation_definition(where="w", agg=agg, dialect=dialect)
        _names_placeholder(err=ei.value, name="n", formula=formula)

    @pytest.mark.parametrize(("dialect", "formula"), _NON_ORDINARY)
    async def test_rejected_at_render(self, dialect: str, formula: str) -> None:
        agg = _agg(formula, name="lit", n="2")
        with pytest.raises(SqlTemplateError) as ei:
            await _dry("lit(amount)", agg=agg, dialect=dialect)
        _names_placeholder(err=ei.value, name="n", formula=formula)


_TRICKY = "a'b\\c"
#: Emitted literal per dialect for the value ``a'b\c``.
_TRICKY_SQL = {
    "duckdb": "'a''b\\c'", "postgres": "'a''b\\c'",
    "mysql": "'a''b\\\\c'", "clickhouse": "'a''b\\\\c'",
    "bigquery": "'a\\'b\\\\c'",
}


class TestQuotedBindingValue:
    @pytest.mark.parametrize("dialect", list(_TRICKY_SQL))
    async def test_string_kwarg_stays_inside_the_literal(self, dialect: str) -> None:
        # The kwarg is the dialect's own spelling of a string literal valued a'b\c.
        kwarg = exp.Literal.string(_TRICKY).sql(dialect=dialect)
        sql = await _dry(f"lbl(amount, n={kwarg!r})", agg=_agg(_LABEL, name="lbl", n="'x'"), dialect=dialect)
        lit = _then_literal(sql, dialect=dialect)
        assert isinstance(lit, exp.Literal)
        assert lit.is_string
        assert lit.this == _TRICKY
        assert f"THEN {_TRICKY_SQL[dialect]} END" in " ".join(sql.split())

    @pytest.mark.parametrize(("default", "value"), [
        ("2", "2"), ("-2", "-2"), ("'x'", "x"), ("'it''s'", "it's"),
    ])
    async def test_literal_default_value_spliced(self, default: str, value: str) -> None:
        sql = await _dry("lbl(amount)", agg=_agg(_LABEL, name="lbl", n=default), validate=True)
        lit = _then_literal(sql, dialect="postgres")
        assert isinstance(lit, exp.Literal)
        assert lit.is_string
        assert lit.this == value

    async def test_quoted_and_unquoted_uses_bind_alike(self) -> None:
        formula = "MAX(CASE WHEN {value} > {n} THEN 'gt {n}' END)"
        sql = " ".join((await _dry("gt(amount)", agg=_agg(formula, name="gt", n="2"), validate=True)).split())
        assert "WHEN orders.amount > 2 THEN 'gt 2' END" in sql


class TestNonLiteralBindingRejected:
    @pytest.mark.parametrize(("formula", "name", "defaults"), [
        pytest.param("MAX(CASE WHEN {value} > 0 THEN '{value}' END)", "value", {}, id="value"),
        pytest.param("MAX(CASE WHEN {value} > 0 THEN '{w}' END)", "w", {"w": "amount"}, id="column-default"),
        pytest.param("MAX(CASE WHEN {value} > 0 THEN '{w}' END)", "w", {"w": "amount + 1"}, id="expression-default"),
    ])
    async def test_rejected_at_save(self, formula: str, name: str, defaults: dict[str, str]) -> None:
        model = _orders(_agg(formula, **defaults))
        with pytest.raises(SqlTemplateError) as ei:
            await _save(model)
        _names_placeholder(err=ei.value, name=name, formula=formula)

    async def test_column_kwarg_rejected_at_binding(self, monkeypatch: pytest.MonkeyPatch) -> None:
        executed: list[str] = []

        async def _spy(self, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003
            executed.append("x")
            raise AssertionError("SQL executed")

        monkeypatch.setattr(SlayerSQLClient, "execute", _spy)
        async with seeded_exec_engine(
            dialect="sqlite", seed=_seed, models=[_orders(_agg(_SUM_N, name="sum_n", n="2"))],
        ) as (engine, _db):
            query = _q("sum_n(amount, n=id)")
            with pytest.raises(SqlTemplateError) as ei:
                await engine.execute(query)
        assert re.search(r"[{'\"]n[}'\"]", str(ei.value)), str(ei.value)
        assert executed == []
