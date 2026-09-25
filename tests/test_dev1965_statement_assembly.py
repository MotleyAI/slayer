"""Statement assembly (spec ``sql/statement-assembly``, ``aggregations/trailing-window``)."""

from __future__ import annotations

import inspect
import re
import tempfile

import pytest
from sqlglot import exp
from sqlglot.expressions.core import Expression

import slayer.sql.dialects.tsql as tsql_module
import slayer.sql.naming as naming
from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, ModelJoin, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql.dialects import SqlDialect, get_dialect
from slayer.sql.dialects.tsql import TsqlDialect
from slayer.sql.generator import SQLGenerator
from slayer.sql.stage_wrapper import build_flat_rename_wrapper
from slayer.storage.yaml_storage import YAMLStorage
from tests import _dev1915_fixtures as F15
from tests._dev1965_fixtures import (
    CUMSUM_BY_MONTH,
    MODE_A_OR,
    ORDERS_ROWS,
    TIER1,
    assert_single_top_level_with,
    cte_names,
    cumsum_chain,
    exec_engine,
    from_name,
    gen,
    month_key,
    orders_model,
    outer_derived,
    parse,
    q,
    where_having_query,
    where_of,
)
from tests._engine_helpers import _engine_generate, seeded_exec_engine
from tests.test_dev1756_identifier_length import _assert_within_limit

_DESC_CREATED = [{"column": "created_at", "direction": "desc"}]
_FETCH = r"FETCH\s+(?:FIRST|NEXT)"


class _CompositionFailure(Exception):
    pass


def _conjuncts(node: Expression) -> list[Expression]:
    if isinstance(node, exp.And):
        return _conjuncts(node.this) + _conjuncts(node.expression)
    return [node]


def _assert_grouped_conjuncts(node: Expression, *, expected: int) -> None:
    """``node`` is an AND of ``expected`` conjuncts; every OR conjunct is parenthesised."""
    parts = _conjuncts(node)
    assert len(parts) == expected, node.sql()
    assert not [p for p in parts if isinstance(p, exp.Or)], node.sql()


def _column_names(node: Expression) -> set[str]:
    return {c.name for c in node.find_all(exp.Column)}


def _carried(top: exp.Select, cte: str) -> set[str]:
    body = next(c.this for c in top.args["with_"].expressions if c.alias_or_name == cte)
    return set(body.named_selects)


# --------------------------------------------------------------------------- #
# One top-level WITH on every dialect.


# --------------------------------------------------------------------------- #


class TestOneTopLevelWith:
    @pytest.mark.parametrize("dialect", TIER1)
    async def test_paginated_chain_hoists_its_with(self, dialect: str) -> None:
        sql = await gen(cumsum_chain(order=_DESC_CREATED, limit=2, offset=1), dialect=dialect)
        top = assert_single_top_level_with(sql, dialect)
        names = cte_names(top)
        assert names[:2] == ["base", "step1"], sql
        assert from_name(outer_derived(top)) == names[-1], sql
        assert len(top.expressions) == 2, sql
        assert not [n for n in top.named_selects if "amount_sum" in n], sql
        assert top.args.get("order") is not None, sql
        if dialect == "tsql":
            assert "LIMIT" not in sql.upper(), sql
            assert re.search(rf"OFFSET 1 ROWS\s+{_FETCH} 2 ROWS ONLY\s*\Z", sql), sql
        else:
            assert top.args.get("limit") is not None, sql
            assert top.args.get("offset") is not None, sql

    @pytest.mark.parametrize("dialect", ["sqlite", "duckdb"])
    async def test_paginated_chain_rows_unchanged(self, dialect: str) -> None:
        async with exec_engine(dialect) as engine:
            resp = await engine.execute(cumsum_chain(order=_DESC_CREATED, limit=2, offset=1))
        assert [
            (month_key(r["orders.created_at"]), float(r["orders.running"])) for r in resp.data
        ] == [("2024-03", 210.0), ("2024-02", 100.0)]
        assert set(resp.data[0]) == {"orders.created_at", "orders.running"}

    async def test_tsql_offset_without_order_gets_a_noop_ordering(self) -> None:
        sql = await gen(cumsum_chain(limit=2, offset=1), dialect="tsql")
        order = assert_single_top_level_with(sql, "tsql").args.get("order")
        assert order is not None, sql
        assert order.find(exp.Null) is not None, sql
        assert order.find(exp.Column) is None, sql
        assert re.search(rf"OFFSET 1 ROWS\s+{_FETCH} 2 ROWS ONLY\s*\Z", sql), sql


    async def test_tsql_composition_failure_propagates(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """No fallback to another statement shape: a pagination failure surfaces."""
        def _boom(self, select, *, limit, offset):  # noqa: ANN001, ARG001
            raise _CompositionFailure

        monkeypatch.setattr(TsqlDialect, "apply_pagination", _boom)
        query = cumsum_chain(limit=2)
        with pytest.raises(_CompositionFailure):
            await gen(query, dialect="tsql")


class TestOuterOrderBy:
    @pytest.mark.parametrize("dialect", TIER1)
    async def test_non_projected_order_field_resolves_against_the_chain(self, dialect: str) -> None:
        sql = await gen(cumsum_chain(
            order=[{"column": "amount:max", "direction": "desc"}], limit=3,
        ), dialect=dialect)
        top = assert_single_top_level_with(sql, dialect)
        carried = set(outer_derived(top).named_selects)
        for ordered in top.args["order"].expressions:
            col = ordered.this
            assert isinstance(col, exp.Column), sql
            assert not col.table, sql
            assert col.name in carried, sql
        assert not [n for n in top.named_selects if "amount_max" in n], sql

    @pytest.mark.parametrize("dialect", ["sqlite", "duckdb"])
    async def test_non_projected_order_field_stays_out_of_rows(self, dialect: str) -> None:
        async with exec_engine(dialect) as engine:
            resp = await engine.execute(cumsum_chain(
                order=[{"column": "amount:max", "direction": "desc"}], limit=3,
            ))
        assert [month_key(r["orders.created_at"]) for r in resp.data] == [
            "2024-04", "2024-03", "2024-02",
        ]
        assert set(resp.data[0]) == {"orders.created_at", "orders.running"}

    @pytest.mark.parametrize("dialect", TIER1)
    async def test_order_by_carries_no_chain_qualifier(self, dialect: str) -> None:
        sql = await gen(cumsum_chain(order=_DESC_CREATED, limit=2), dialect=dialect)
        top = parse(sql, dialect)
        for col in top.args["order"].find_all(exp.Column):
            assert not col.table, sql


# --------------------------------------------------------------------------- #
# Post-phase filters apply at the chain's final select.


# --------------------------------------------------------------------------- #


class TestPostFilter:
    @pytest.mark.parametrize("dialect", TIER1)
    async def test_post_filter_is_the_where_of_the_final_select(self, dialect: str) -> None:
        sql = await gen(cumsum_chain(filters=["cumsum(amount:sum) > 50"]), dialect=dialect)
        assert "_filtered" not in sql, sql
        top = assert_single_top_level_with(sql, dialect)
        final = outer_derived(top)
        last = cte_names(top)[-1]
        assert from_name(final) == last, sql
        where = where_of(final)
        assert where is not None, sql
        assert "50" in where.sql(dialect=dialect), sql
        assert _column_names(where) <= _carried(top, last), sql
        base = next(c.this for c in top.args["with_"].expressions if c.alias_or_name == "base")
        assert base.args.get("where") is None, sql
        assert base.args.get("having") is None, sql

    @pytest.mark.parametrize("dialect", ["sqlite", "duckdb"])
    async def test_post_filter_keeps_the_transform_inputs(self, dialect: str) -> None:
        async with exec_engine(dialect) as engine:
            resp = await engine.execute(cumsum_chain(filters=["cumsum(amount:sum) > 50"]))
        got = {month_key(r["orders.created_at"]): float(r["orders.running"]) for r in resp.data}
        assert got == {m: v for m, v in CUMSUM_BY_MONTH.items() if v > 50}

    _COMPOSITE = {
        "dimensions": [{"expression": "lower(status)", "name": "ls"}],
        "filters": ["cumsum(amount:sum) > 50 or lower(status) == 'c'"],
    }

    async def test_composite_dimension_filter_reads_a_carried_column(self) -> None:
        sql = await gen(cumsum_chain(**self._COMPOSITE), dialect="duckdb")
        top = assert_single_top_level_with(sql, "duckdb")
        where = where_of(outer_derived(top))
        assert where is not None, sql
        assert _column_names(where) <= _carried(top, cte_names(top)[-1]), sql
        assert not list(where.find_all(exp.Lower)), sql

    @pytest.mark.parametrize("dialect", ["sqlite", "duckdb"])
    async def test_composite_dimension_filter_rows(self, dialect: str) -> None:
        async with exec_engine(dialect) as engine:
            resp = await engine.execute(cumsum_chain(**self._COMPOSITE))
        assert sorted(
            (r["orders.ls"], month_key(r["orders.created_at"]), float(r["orders.running"]))
            for r in resp.data
        ) == [("a", "2024-03", 90.0), ("b", "2024-02", 60.0), ("b", "2024-04", 130.0),
              ("c", "2024-03", 60.0)]

    _OR_MIX = ["cumsum(amount:sum) > 250 or cumsum(amount:sum) < 50", "cumsum(amount:sum) < 260"]

    async def test_post_conjuncts_keep_their_grouping(self) -> None:
        sql = await gen(cumsum_chain(filters=self._OR_MIX), dialect="postgres")
        where = where_of(outer_derived(parse(sql, "postgres")))
        assert where is not None, sql
        _assert_grouped_conjuncts(where, expected=2)

    @pytest.mark.parametrize("dialect", ["sqlite", "duckdb"])
    async def test_post_conjunct_rows(self, dialect: str) -> None:
        async with exec_engine(dialect) as engine:
            resp = await engine.execute(cumsum_chain(filters=self._OR_MIX))
        assert [month_key(r["orders.created_at"]) for r in resp.data] == ["2024-01"]


# --------------------------------------------------------------------------- #
# WHERE / HAVING conjuncts are combined as AST.


# --------------------------------------------------------------------------- #


class TestWhereHaving:
    async def test_where_and_having_are_grouped_conjunctions(self) -> None:
        sql = await gen(where_having_query(), dialect="postgres",
                        model=orders_model(filters=[MODE_A_OR]))
        select = next(
            s for s in parse(sql, "postgres").find_all(exp.Select) if s.args.get("where")
        )
        _assert_grouped_conjuncts(select.args["where"].this, expected=3)
        _assert_grouped_conjuncts(select.args["having"].this, expected=2)

    @pytest.mark.parametrize("dialect", ["sqlite", "duckdb"])
    async def test_mode_a_or_filter_keeps_its_grouping(self, dialect: str) -> None:
        async with exec_engine(dialect, model=orders_model(filters=[MODE_A_OR])) as engine:
            resp = await engine.execute(where_having_query())
        assert {r["orders.region"]: float(r["orders.amount_sum"]) for r in resp.data} == {
            "US": 160.0,
        }


# --------------------------------------------------------------------------- #
# Query-backed model wraps compose over the unrewritten statement.


# --------------------------------------------------------------------------- #
LONG = "c" * 310
_CUSTOMER_ROWS = [(i, "gold" if i % 2 else "silver") for i in range(1, 8)]


def _qb_models(data_source: str) -> list[SlayerModel]:
    orders = orders_model()
    orders.data_source = data_source
    orders.joins = [ModelJoin(target_model="customers", join_pairs=[["id", "id"]])]
    customers = SlayerModel(
        name="customers", sql_table="customers", data_source=data_source,
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name=LONG, sql="tier", type=DataType.TEXT),
        ],
    )
    return [orders, customers]


def _qb_stage() -> SlayerQuery:
    return q(dimensions=[f"customers.{LONG}"], measures=["amount:sum"])


class TestQueryBackedWrap:
    def test_wrapper_takes_an_ast_inner_statement(self) -> None:
        params = inspect.signature(build_flat_rename_wrapper).parameters
        assert set(params) == {"source_relation", "inner", "expected_columns", "dialect"}

    @pytest.mark.parametrize(("ds_type", "dialect", "limit"),
                             [("bigquery", "bigquery", 300), ("mssql", "tsql", 128)])
    async def test_backing_columns_are_the_fitted_column_names(
        self, ds_type: str, dialect: str, limit: int,
    ) -> None:
        with tempfile.TemporaryDirectory() as d:
            storage = YAMLStorage(base_dir=d)
            await storage.save_datasource(DatasourceConfig(name="ds", type=ds_type, database="db"))
            for model in _qb_models("ds"):
                await storage.save_model(model)
            engine = SlayerQueryEngine(storage=storage)
            expanded = await engine._expand_query_backed_model(
                model=SlayerModel(name="qb", source_queries=[_qb_stage()], data_source="ds"),
                outer_vars=None, runtime_kwarg=None, dry_run_placeholders=False, _resolving=None,
            )
        assert expanded.sql is not None
        fit = get_dialect(dialect).fit_alias
        assert {c.name: c.sql for c in expanded.columns} == {
            f"customers__{LONG}": fit(f"customers__{LONG}"), "amount_sum": "amount_sum",
        }
        assert set(parse(expanded.sql, dialect).named_selects) == {c.sql for c in expanded.columns}
        _assert_within_limit(expanded.sql, limit, dialect=dialect)

    async def test_duckdb_rows_are_keyed_by_column_names(self) -> None:
        def seed(db_path: str) -> None:
            duckdb = pytest.importorskip("duckdb")
            con = duckdb.connect(db_path)
            con.execute("CREATE TABLE orders (id INTEGER, status VARCHAR, region VARCHAR, "
                        "amount DOUBLE, qty DOUBLE, created_at TIMESTAMP)")
            con.executemany("INSERT INTO orders VALUES (?,?,?,?,?,?)", ORDERS_ROWS)
            con.execute("CREATE TABLE customers (id INTEGER, tier VARCHAR)")
            con.executemany("INSERT INTO customers VALUES (?,?)", _CUSTOMER_ROWS)
            con.close()

        pytest.importorskip("duckdb")
        qb = SlayerModel(name="qb", source_queries=[_qb_stage()], data_source="test")
        async with seeded_exec_engine(
            dialect="duckdb", seed=seed, models=[*_qb_models("test"), qb],
        ) as (engine, _db):
            resp = await engine.execute(SlayerQuery.model_validate({
                "source_model": "qb", "dimensions": [f"customers__{LONG}"],
                "measures": ["amount_sum:sum"],
            }))
        assert {
            r[f"qb.customers__{LONG}"]: float(r["qb.amount_sum_sum"]) for r in resp.data
        } == {"gold": 160.0, "silver": 120.0}


# --------------------------------------------------------------------------- #
# Windowed sample statistics keep sample semantics on MySQL.
# (DuckDB / SQLite values: test_dev1915_windowed_exec.py::test_statistics_family.)


# --------------------------------------------------------------------------- #


def _stat_seams(formula: str) -> dict:
    month = [{"dimension": "created_at", "granularity": "month"}]
    measure = [{"formula": formula, "name": "w"}]
    return {
        "producer_hoist": SlayerQuery.model_validate({
            "source_model": "orders", "time_dimensions": F15.month_td(), "measures": measure,
        }),
        "non_root_stage": [
            {"name": "s1", "source_model": "orders", "time_dimensions": month, "measures": measure},
            {"source_model": "s1", "measures": [{"formula": "w:max", "name": "mx"}]},
        ],
        "root_stage": [
            {"name": "s1", "source_model": "orders", "dimensions": ["id"]},
            {"source_model": "orders", "time_dimensions": month, "measures": measure},
        ],
    }


@pytest.mark.parametrize("seam", ["producer_hoist", "non_root_stage", "root_stage"])
@pytest.mark.parametrize("formula", [
    "amount:covar_samp(other=qty, window='90d')",
    "amount:corr(other=qty, window='90d')",
])
async def test_mysql_windowed_sample_statistic_uses_var_samp(formula: str, seam: str) -> None:
    models = F15.dev1915_models()
    sql = await _engine_generate(
        query=_stat_seams(formula)[seam], model=models[0], extra_models=models[1:],
        dialect="mysql", validate=False,
    )
    assert "VAR_SAMP(" in sql, sql
    assert "VARIANCE(" not in sql.upper(), sql


# --------------------------------------------------------------------------- #
# Removed text-assembly surface.


# --------------------------------------------------------------------------- #


class TestRemovedSurface:
    def test_no_outer_wrap_dialect_hook(self) -> None:
        assert not hasattr(SqlDialect, "emit_outer_wrap")
        assert not hasattr(TsqlDialect, "emit_outer_wrap")
        assert not hasattr(tsql_module, "_offset_ordering_fallback")

    def test_generate_from_planned_has_no_as_ast(self) -> None:
        assert "as_ast" not in inspect.signature(SQLGenerator.generate_from_planned).parameters

    def test_no_filtered_alias(self) -> None:
        assert not hasattr(naming, "FILTERED_ALIAS")

    def test_no_text_cte_splitting_or_parse_seam(self) -> None:
        for name in ("_parse_cte_body", "_split_statement_ctes", "_emit_planned_outer_wrap"):
            assert not hasattr(SQLGenerator, name), name
