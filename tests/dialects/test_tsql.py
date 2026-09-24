"""Tests for TsqlDialect (SQL Server)."""

from __future__ import annotations

import re
import tempfile
from unittest.mock import patch

import sqlglot
from sqlglot import exp

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import Column, DatasourceConfig, ModelMeasure, SlayerModel
from slayer.core.query import ColumnRef, OrderItem, SlayerQuery, TimeDimension
from slayer.engine.query_engine import SlayerQueryEngine, _sql_client_cache_key
from slayer.sql.dialects.tsql import TsqlDialect
from slayer.storage.yaml_storage import YAMLStorage

from tests._engine_helpers import _engine_generate


def test_tsql_sqlglot_name() -> None:
    assert TsqlDialect().sqlglot_name == "tsql"


def test_tsql_explain_prefix_and_postfix() -> None:
    d = TsqlDialect()
    assert d.explain_prefix == "SET SHOWPLAN_ALL ON;"
    assert d.explain_postfix == "; SET SHOWPLAN_ALL OFF"


def test_tsql_log_native_flags() -> None:
    """SQL Server has LOG10 but no LOG2 (sqlglot has no LOG2 emit for tsql)."""
    d = TsqlDialect()
    assert d.should_use_native_log(10) is True
    assert d.should_use_native_log(2) is False


def test_tsql_ds_type_aliases() -> None:
    assert TsqlDialect().ds_type_aliases == frozenset({"mssql", "sqlserver", "tsql"})


# build_date_trunc — DATETRUNC(unit, col), iso_week for week


def test_tsql_build_date_trunc_month() -> None:
    d = TsqlDialect()
    col = sqlglot.parse_one("created_at", dialect="tsql")
    out = d.build_date_trunc(col, TimeGranularity.MONTH)
    sql = out.sql(dialect="tsql").lower()
    assert "datetrunc" in sql
    assert "month" in sql


def test_tsql_build_date_trunc_week_uses_iso_week() -> None:
    """Week must use ISO_WEEK (Monday-start) to be @@DATEFIRST-independent."""
    d = TsqlDialect()
    col = sqlglot.parse_one("created_at", dialect="tsql")
    out = d.build_date_trunc(col, TimeGranularity.WEEK)
    sql = out.sql(dialect="tsql").lower()
    assert "datetrunc" in sql
    assert "iso_week" in sql


def test_tsql_build_date_trunc_week_sunday_shift() -> None:
    """WEEK_SUNDAY composes DATEADD day offsets around the iso_week DATETRUNC."""
    d = TsqlDialect()
    col = sqlglot.parse_one("ordered_at", dialect="tsql")
    out = d.build_date_trunc(col, TimeGranularity.WEEK_SUNDAY)
    sql = out.sql(dialect="tsql").lower()
    assert "datetrunc" in sql
    assert "iso_week" in sql          # inner Monday-week truncation
    assert "dateadd(day, 1," in sql   # inner +1 day
    assert "dateadd(day, -1," in sql  # outer -1 day


def test_tsql_build_date_trunc_casts_non_column_to_timestamp() -> None:
    """``DATETRUNC`` needs a temporal type, so non-column operands are CAST."""
    d = TsqlDialect()
    lit = sqlglot.parse_one("'2025-01-01'", dialect="tsql")
    out = d.build_date_trunc(lit, TimeGranularity.MONTH)
    assert "CAST" in out.sql(dialect="tsql").upper()


# build_time_offset_expr — DATEADD, no INTERVAL


def test_tsql_build_time_offset_expr_day() -> None:
    d = TsqlDialect()
    col = sqlglot.parse_one("created_at", dialect="tsql")
    out = d.build_time_offset_expr(col, offset=3, granularity="day")
    sql = out.sql(dialect="tsql").upper()
    assert "DATEADD" in sql
    assert "DAY" in sql
    assert "3" in sql
    assert "INTERVAL" not in sql


def test_tsql_build_time_offset_expr_negative() -> None:
    """DATEADD takes a signed amount as its second arg — negative values propagate directly into the call."""
    d = TsqlDialect()
    col = sqlglot.parse_one("created_at", dialect="tsql")
    out = d.build_time_offset_expr(col, offset=-2, granularity="month")
    sql = out.sql(dialect="tsql").upper()
    assert "DATEADD" in sql
    assert "-2" in sql or "(-2)" in sql or "-(2)" in sql


def test_tsql_build_time_offset_expr_quarter_normalizes_to_3_month() -> None:
    d = TsqlDialect()
    col = sqlglot.parse_one("created_at", dialect="tsql")
    out = d.build_time_offset_expr(col, offset=1, granularity="quarter")
    sql = out.sql(dialect="tsql").upper()
    assert "DATEADD" in sql
    assert "MONTH" in sql
    assert "3" in sql


# add_intervals_expr — chains DATEADD calls (no INTERVAL)


def test_tsql_add_intervals_expr_uses_dateadd_chain() -> None:
    d = TsqlDialect()
    col = sqlglot.parse_one("created_at", dialect="tsql")
    intervals = [
        exp.Interval(
            this=exp.Literal.number(1),
            unit=exp.Var(this="DAY"),
        ),
    ]
    out = d.add_intervals_expr(col, intervals, sign=-1)
    sql = out.sql(dialect="tsql").upper()
    assert "DATEADD" in sql
    assert "INTERVAL" not in sql  # no INTERVAL keyword in T-SQL


# Median / percentile — not supported on T-SQL


def test_tsql_build_median_raises_not_implemented() -> None:
    d = TsqlDialect()
    inner = sqlglot.parse_one("amount", dialect="tsql")
    with pytest.raises(NotImplementedError, match="median.*T-SQL"):
        d.build_median(inner)


def test_tsql_build_percentile_raises_not_implemented() -> None:
    d = TsqlDialect()
    p = exp.Literal.number("0.5")
    col_expr = exp.column("amount")
    with pytest.raises(NotImplementedError, match="percentile.*T-SQL"):
        d.build_percentile(p=p, col_expr=col_expr)


# Stat aggs — T-SQL canonical names via exp.Anonymous


@pytest.mark.parametrize(
    "agg_name,tsql_fn",
    [
        ("stddev_samp", "STDEV"),
        ("stddev_pop", "STDEVP"),
        ("var_samp", "VAR"),
        ("var_pop", "VARP"),
    ],
)
def test_tsql_build_stat_agg_1arg_uses_tsql_names(
    agg_name: str, tsql_fn: str
) -> None:
    """sqlglot's tsql transpiler emits incorrect names (e.g. VAR_SAMP, VARIANCE_POP)."""
    d = TsqlDialect()
    out = d.build_stat_agg_1arg(agg_name=agg_name, col_expr=exp.column("amount"))
    sql = out.sql(dialect="tsql").upper()
    assert tsql_fn in sql
    # Sanity: NOT the Postgres-canonical name
    assert agg_name.upper() not in sql


# Covar — variance-decomposition formula with T-SQL names


def test_tsql_build_covar_2arg_corr_uses_decomposition() -> None:
    d = TsqlDialect()
    out = d.build_covar_2arg(agg_name="corr", col_expr=exp.column("amount"), other_expr=exp.column("quantity"))
    sql = out.sql(dialect="tsql").upper()
    # T-SQL covariance formula uses VAR / STDEV (sample form for corr/covar_samp)
    assert "VAR" in sql
    assert "STDEV" in sql
    assert "NULLIF" in sql  # zero-denominator guard for corr


def test_tsql_build_covar_2arg_covar_pop_uses_varp() -> None:
    d = TsqlDialect()
    out = d.build_covar_2arg(agg_name="covar_pop", col_expr=exp.column("amount"), other_expr=exp.column("quantity"))
    sql = out.sql(dialect="tsql").upper()
    assert "VARP" in sql


# build_explain_sql — wraps in SHOWPLAN session toggle pair


def test_tsql_build_explain_sql_wraps_in_showplan_pair() -> None:
    d = TsqlDialect()
    assert d.build_explain_sql("SELECT 1") == (
        "SET SHOWPLAN_ALL ON; SELECT 1; SET SHOWPLAN_ALL OFF"
    )


# emit_outer_wrap hoists inner CTEs to top


_INNER_WITH_CTES = (
    "WITH base AS (SELECT id, status FROM orders),\n"
    "     step2 AS (SELECT id, status FROM base)\n"
    "SELECT id AS [orders.id], status AS [orders.status] FROM step2"
)


def _normalise(sql: str) -> str:
    """Collapse whitespace for shape assertions."""
    return " ".join(sql.split())


def test_tsql_emit_outer_wrap_hoists_inner_ctes() -> None:
    """T-SQL rejects ``WITH`` inside a derived-table subquery."""
    out = TsqlDialect().emit_outer_wrap(
        inner_sql=_INNER_WITH_CTES,
        public=["orders.id", "orders.status"],
        projected=["orders.id", "orders.status"],
        order=None,
        limit=None,
        offset_arg=None,
    )
    normalised = _normalise(out)
    assert normalised.startswith("WITH "), (
        f"Expected hoisted statement to start with WITH; got: {out}"
    )
    # No nested WITH inside parens.
    assert "(WITH " not in normalised, (
        f"Hoisted output still has nested WITH inside parens: {out}"
    )
    assert "( WITH " not in normalised, (
        f"Hoisted output still has nested WITH inside parens: {out}"
    )
    assert "base AS" in normalised
    assert "step2 AS" in normalised
    # The original main SELECT body must survive verbatim inside the
    # derived-table wrap — the hoist must not drop or substitute it
    # (Codex MEDIUM #3 pin: the test wouldn't catch a broken impl that
    # hoists CTEs but loses the main FROM clause).
    assert "FROM step2" in normalised, (
        f"Inner main SELECT body lost after CTE hoist: {out}"
    )
    # And the outer projection still names the public aliases.
    assert "[orders.id]" in out
    assert "[orders.status]" in out


def test_tsql_emit_outer_wrap_no_ctes_passthrough_shape() -> None:
    """Without CTEs the hoist is a no-op and the shape matches the base impl."""
    inner = "SELECT id AS [orders.id], status AS [orders.status] FROM orders"
    out = TsqlDialect().emit_outer_wrap(
        inner_sql=inner,
        public=["orders.id", "orders.status"],
        projected=["orders.id", "orders.status"],
        order=None,
        limit=None,
        offset_arg=None,
    )
    normalised = _normalise(out)
    assert not normalised.startswith("WITH "), (
        f"No CTEs in inner — should not emit top-level WITH: {out}"
    )
    assert ") AS _outer" in normalised
    assert "AS _outer" in normalised


def test_tsql_emit_outer_wrap_uses_brackets_for_aliases() -> None:
    """Outer projection identifiers use ``[...]`` brackets."""
    out = TsqlDialect().emit_outer_wrap(
        inner_sql="SELECT 1 AS [orders.x]",
        public=["orders.x"],
        projected=["orders.x"],
        order=None,
        limit=None,
        offset_arg=None,
    )
    # Bracketed alias is present pre-mangle. Bug 2 mangling fires later in
    # rewrite_emitted_sql; emit_outer_wrap stays naive about it.
    assert "[orders.x]" in out
    assert '"orders.x"' not in out
    assert "`orders.x`" not in out


def test_tsql_emit_outer_wrap_with_limit() -> None:
    """Outer wrap with ``LIMIT N`` re-emits as T-SQL ``TOP``/``FETCH NEXT`` via sqlglot."""
    limit = sqlglot.parse_one("SELECT 1 LIMIT 5", dialect="tsql").args.get("limit")
    out = TsqlDialect().emit_outer_wrap(
        inner_sql=_INNER_WITH_CTES,
        public=["orders.id"],
        projected=["orders.id"],
        order=None,
        limit=limit,
        offset_arg=None,
    )
    # Either FETCH NEXT or TOP — both are valid T-SQL spellings; LIMIT
    # itself is not valid T-SQL syntax and must not appear in the output.
    normalised_upper = _normalise(out).upper()
    assert "LIMIT" not in normalised_upper, (
        f"Bare LIMIT survived in T-SQL outer wrap: {out}"
    )
    assert "5" in out


def test_tsql_emit_outer_wrap_no_ctes_with_limit_transposes_pagination() -> None:
    """The no-CTE branch also transposes ``LIMIT`` into ``TOP`` / ``FETCH NEXT``."""
    inner = "SELECT id AS [orders.id] FROM orders"  # no WITH
    limit = sqlglot.parse_one("SELECT 1 LIMIT 5", dialect="tsql").args.get("limit")
    out = TsqlDialect().emit_outer_wrap(
        inner_sql=inner,
        public=["orders.id"],
        projected=["orders.id"],
        order=None,
        limit=limit,
        offset_arg=None,
    )
    upper = _normalise(out).upper()
    assert "LIMIT" not in upper, (
        f"No-CTE T-SQL outer wrap still emits literal LIMIT: {out}"
    )
    assert "5" in out


def test_tsql_emit_outer_wrap_with_offset() -> None:
    """Outer wrap with ``OFFSET N`` re-emits via sqlglot's T-SQL dialect."""
    offset_arg = sqlglot.parse_one(
        "SELECT 1 ORDER BY 1 OFFSET 10 ROWS", dialect="tsql"
    ).args.get("offset")
    out = TsqlDialect().emit_outer_wrap(
        inner_sql=_INNER_WITH_CTES,
        public=["orders.id"],
        projected=["orders.id"],
        order=None,
        limit=None,
        offset_arg=offset_arg,
    )
    assert "10" in out
    assert "OFFSET" in out.upper()


def test_tsql_emit_outer_wrap_with_order_and_offset() -> None:
    """ORDER BY and OFFSET both ride on the outer statement."""
    sql = "SELECT 1 ORDER BY 1 OFFSET 10 ROWS"
    parsed = sqlglot.parse_one(sql, dialect="tsql")
    order = parsed.args.get("order")
    offset_arg = parsed.args.get("offset")
    out = TsqlDialect().emit_outer_wrap(
        inner_sql=_INNER_WITH_CTES,
        public=["orders.id"],
        projected=["orders.id"],
        order=order,
        limit=None,
        offset_arg=offset_arg,
    )
    upper = out.upper()
    assert "ORDER BY" in upper
    assert "OFFSET" in upper
    assert _normalise(out).startswith("WITH ")


def test_tsql_emit_outer_wrap_strips_inner_qualifiers_in_order_by() -> None:
    """The detached ORDER BY may carry inner-CTE qualifiers like ``_base."col"`` from ``_assemble_combined_sql``."""
    order = sqlglot.parse_one(
        'SELECT 1 ORDER BY _base."orders.id" ASC', dialect="tsql"
    ).args.get("order")
    out = TsqlDialect().emit_outer_wrap(
        inner_sql=_INNER_WITH_CTES,
        public=["orders.id"],
        projected=["orders.id"],
        order=order,
        limit=None,
        offset_arg=None,
    )
    # The inner CTE alias must not leak into the outer ORDER BY.
    assert "_base." not in out, (
        f"Inner-CTE qualifier _base. leaked into outer ORDER BY: {out}"
    )


def test_tsql_emit_outer_wrap_hidden_alias_in_order_by() -> None:
    """ORDER BY may reference a hidden inner alias not in ``public``."""
    inner = (
        "WITH base AS (SELECT id, status, created_at FROM orders)\n"
        "SELECT id AS [orders.id], created_at AS [orders.created_at] "
        "FROM base"
    )
    order = sqlglot.parse_one(
        'SELECT 1 ORDER BY _base."orders.created_at" DESC', dialect="tsql"
    ).args.get("order")
    # ``public`` excludes the sort key.
    out = TsqlDialect().emit_outer_wrap(
        inner_sql=inner,
        public=["orders.id"],
        projected=["orders.id"],
        order=order,
        limit=None,
        offset_arg=None,
    )
    # The hidden alias must still appear in the outer ORDER BY (bare,
    # no qualifier) so the derived-table scope can resolve it.
    assert "_base." not in out
    assert "orders.created_at" in out
    # And the outer projection still trims to the public list.
    upper = out.upper()
    select_clause = upper.split("FROM (")[0]
    assert "ORDERS.CREATED_AT" not in select_clause, (
        f"Hidden alias leaked into outer projection (not in public): {out}"
    )


def test_tsql_emit_outer_wrap_preserves_multiple_ctes_in_order() -> None:
    """Multiple inner CTEs are hoisted in declared order (sqlglot's ``With`` node preserves declaration order)."""
    inner = (
        "WITH alpha AS (SELECT 1 AS a),\n"
        "     beta AS (SELECT 2 AS b),\n"
        "     gamma AS (SELECT 3 AS c)\n"
        "SELECT * FROM gamma"
    )
    out = TsqlDialect().emit_outer_wrap(
        inner_sql=inner,
        public=["c"],
        projected=["c"],
        order=None,
        limit=None,
        offset_arg=None,
    )
    normalised = _normalise(out)
    a_idx = normalised.find("alpha")
    b_idx = normalised.find("beta")
    g_idx = normalised.find("gamma")
    assert 0 < a_idx < b_idx < g_idx, (
        f"CTE declaration order lost: alpha@{a_idx} beta@{b_idx} gamma@{g_idx} in {out}"
    )


# Bracketed dotted alias mangling on rewrite_emitted_sql


def test_tsql_rewrite_emitted_sql_mangles_single_dot_alias() -> None:
    """``[a.b]`` becomes ``[a___b]``."""
    sql = "SELECT 1 AS [orders.id] FROM t ORDER BY [orders.id] ASC"
    out = TsqlDialect().rewrite_emitted_sql(sql)
    assert "[orders___id]" in out
    assert "[orders.id]" not in out


def test_tsql_rewrite_emitted_sql_multi_hop_alias() -> None:
    """Multi-hop dotted aliases like ``[a.b.c]`` become ``[a___b___c]``."""
    sql = "SELECT 1 AS [orders.products.category]"
    out = TsqlDialect().rewrite_emitted_sql(sql)
    assert "[orders___products___category]" in out


def test_tsql_rewrite_emitted_sql_leaves_non_dotted_brackets_untouched() -> None:
    """Single-segment bracketed identifiers are unchanged."""
    sql = "SELECT [my_col], [order], [user] FROM [my_table]"
    assert TsqlDialect().rewrite_emitted_sql(sql) == sql


def test_tsql_rewrite_emitted_sql_leaves_brackets_with_spaces_untouched() -> None:
    """T-SQL allows arbitrary chars inside brackets (e.g. ``[my table]``, ``[my.col with spaces]``)."""
    sql = "SELECT [my col] FROM [tbl with space]"
    assert TsqlDialect().rewrite_emitted_sql(sql) == sql


def test_tsql_rewrite_emitted_sql_regex_is_ascii_only() -> None:
    """The dotted-alias regex is ASCII-only."""
    sql = "SELECT 1 AS [café.metric]"
    assert TsqlDialect().rewrite_emitted_sql(sql) == sql, (
        "Non-ASCII word characters must not match. The regex must use "
        "re.ASCII so \\w is ASCII-only."
    )


def test_tsql_rewrite_emitted_sql_idempotent_on_already_mangled() -> None:
    """An already-mangled alias (no dot inside brackets) is left alone."""
    sql = "SELECT 1 AS [orders___id]"
    assert TsqlDialect().rewrite_emitted_sql(sql) == sql


def test_tsql_rewrite_emitted_sql_false_positive_on_user_bracketed_dotted_path() -> None:
    """Characterisation: a user-authored ``[my_schema.my_table]`` does get mangled."""
    sql = "SELECT col FROM [my_schema.my_table]"
    out = TsqlDialect().rewrite_emitted_sql(sql)
    # Documented constraint: word-only bracketed dotted paths get mangled.
    assert out == "SELECT col FROM [my_schema___my_table]", (
        f"Documented constraint changed: {out}"
    )


# decode_result_keys reverses the mangling


def test_tsql_decode_result_keys_reverses_mangle() -> None:
    """Mangled keys are decoded back to SLayer's dotted alias shape on response."""
    d = TsqlDialect()
    rows = [
        {"orders___id": 1, "orders___products___category": "shoes"},
        {"orders___id": 2, "orders___products___category": "boots"},
    ]
    out = d.decode_result_keys(rows)
    assert out == [
        {"orders.id": 1, "orders.products.category": "shoes"},
        {"orders.id": 2, "orders.products.category": "boots"},
    ]


def test_tsql_decode_result_keys_empty_rows() -> None:
    assert TsqlDialect().decode_result_keys([]) == []


# Round-trip bijection sanity via the dialect's regex


@pytest.mark.parametrize(
    "original",
    [
        "orders.id",
        "orders._count",
        "orders.products.category",
        "orders.my___metric",
        "orders.customers.regions.population_sum",
    ],
)
def test_tsql_round_trip_preserves_legitimate_underscores(original: str) -> None:
    """The round-trip is a bijection on SLayer's dotted alias space."""
    d = TsqlDialect()
    sql = f"SELECT 1 AS [{original}]"
    mangled = d.rewrite_emitted_sql(sql)
    m = re.search(r"AS \[([^\]]+)\]", mangled)
    assert m is not None, f"could not find alias in mangled SQL: {mangled}"
    decoded = d.decode_result_keys([{m.group(1): 1}])
    assert decoded == [{original: 1}]


# Engine-level integration: SlayerResponse round-trip on T-SQL alias decoding
#
# Mirrors the BigQuery pattern (``tests/dialects/test_bigquery.py::
# TestEngineDecodeIntegration``). Stubs the SQL client so we exercise
# ``engine.execute()``'s post-fetch decode hook end-to-end without a live
# SQL Server instance. Pins Codex MEDIUM #7 — Bug 2 is an execution-path
# issue, so the dialect-level decode round-trip is not enough.


class _FakeTsqlClient:
    """Stub SQL client that returns T-SQL-mangled row keys (``a___b`` form)."""

    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    async def execute(self, *, sql: str) -> list[dict]:  # noqa: ARG002 — stub signature  # NOSONAR(S7503) — must remain async to match SlayerSQLClient.execute (awaited by engine.execute)
        return [dict(row) for row in self._rows]


async def _build_tsql_engine(rows: list[dict]) -> tuple[SlayerQueryEngine, tempfile.TemporaryDirectory, DatasourceConfig]:
    """Build a SlayerQueryEngine pointed at a fake T-SQL datasource whose SQL client is pre-stubbed with ``rows``."""
    tmp = tempfile.TemporaryDirectory()
    storage = YAMLStorage(base_dir=tmp.name)
    ds = DatasourceConfig(
        name="mssql",
        type="mssql",
        database=":memory:",
    )
    await storage.save_datasource(ds)
    model = SlayerModel(
        name="orders",
        sql_table="orders_t",
        data_source="mssql",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="status", sql="status", type=DataType.TEXT),
        ],
    )
    await storage.save_model(model)
    engine = SlayerQueryEngine(storage=storage)
    engine._sql_clients[_sql_client_cache_key(ds)] = _FakeTsqlClient(rows)
    return engine, tmp, ds


class TestEngineTsqlDecodeIntegration:
    """End-to-end: stub client returns mangled keys; engine decodes them before packaging into ``SlayerResponse``."""

    async def test_non_empty_rows_decoded_in_response(self) -> None:
        # ``orders.status`` encodes to ``orders___status``.
        # ``*:count`` measure alias ``orders._count`` encodes to
        # ``orders____count`` (3 underscores from the dot + 1 leading).
        rows = [{"orders____count": 42, "orders___status": "paid"}]
        engine, tmp, _ = await _build_tsql_engine(rows)
        try:
            query = SlayerQuery(
                source_model="orders",
                measures=[{"formula": "*:count"}],
                dimensions=[ColumnRef(name="status")],
            )
            resp = await engine.execute(query)
            assert resp.data == [{"orders._count": 42, "orders.status": "paid"}]
        finally:
            tmp.cleanup()

    async def test_empty_rows_response_falls_back_to_expected_columns(self) -> None:
        engine, tmp, _ = await _build_tsql_engine(rows=[])
        try:
            query = SlayerQuery(
                source_model="orders",
                measures=[{"formula": "*:count"}],
                dimensions=[ColumnRef(name="status")],
            )
            resp = await engine.execute(query)
            assert resp.data == []
            assert "orders._count" in resp.columns
            assert "orders.status" in resp.columns
        finally:
            tmp.cleanup()


# T-SQL also mangles+decodes,
# so the metadata reconciliation and data-path-only decode scoping apply here
# too, not just BigQuery.


async def _build_labeled_tsql_engine(
    rows: list[dict],
) -> tuple[SlayerQueryEngine, tempfile.TemporaryDirectory, DatasourceConfig]:
    tmp = tempfile.TemporaryDirectory()
    storage = YAMLStorage(base_dir=tmp.name)
    ds = DatasourceConfig(name="mssql", type="mssql", database=":memory:")
    await storage.save_datasource(ds)
    model = SlayerModel(
        name="orders",
        sql_table="orders_t",
        data_source="mssql",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="status", sql="status", type=DataType.TEXT, label="Order Status"),
        ],
    )
    await storage.save_model(model)
    engine = SlayerQueryEngine(storage=storage)
    engine._sql_clients[_sql_client_cache_key(ds)] = _FakeTsqlClient(rows)
    return engine, tmp, ds


async def test_tsql_attributes_survive_alias_mangling() -> None:
    """Mangled expected_columns are decoded so dimension labels survive."""
    engine, tmp, _ = await _build_labeled_tsql_engine([{"orders___status": "paid"}])
    try:
        query = SlayerQuery(source_model="orders", dimensions=[ColumnRef(name="status")])
        resp = await engine.execute(query)
        assert "orders.status" in resp.attributes.dimensions, (
            f"T-SQL attributes lost the dimension after mangling: "
            f"{resp.attributes.dimensions!r}"
        )
        assert resp.attributes.dimensions["orders.status"].label == "Order Status"
    finally:
        tmp.cleanup()


async def test_tsql_explain_does_not_decode_data_rows() -> None:
    """Row decode does not run on the explain path."""
    explain_rows = [{"plan": "..."}]
    engine, tmp, _ = await _build_labeled_tsql_engine(explain_rows)
    try:
        query = SlayerQuery(source_model="orders", dimensions=[ColumnRef(name="status")])
        with patch.object(
            TsqlDialect, "decode_result_keys", autospec=True,
            side_effect=lambda self, rows, **kw: rows,
        ) as spy:
            await engine.execute(query, explain=True)
        decoded_args = [call.args[-1] for call in spy.call_args_list]
        assert explain_rows not in decoded_args, (
            "explain must not decode the fetched EXPLAIN plan rows."
        )
    finally:
        tmp.cleanup()


# T-SQL inner CTEs get dialect-aware bracket
# quoting AND Bug 2 mangling fires on those identifiers.
#
# Pre-fix, the inner CTE assembly emitted hardcoded ANSI double quotes.
# On T-SQL the result PARSED (T-SQL accepts ``"..."`` as identifiers when
# QUOTED_IDENTIFIER is ON, the default) but the dotted alias bypassed
# Bug 2's bracket-anchored mangling regex, so the literal-dot form left
# the ORDER BY resolver unable to match the SELECT alias.


async def _tsql_generate(query: SlayerQuery, model: SlayerModel) -> str:
    """Render ``query`` for T-SQL and return the full emitted SQL."""
    return await _engine_generate(query=query, model=model, dialect="tsql")


def _orders_model_tsql() -> SlayerModel:
    return SlayerModel(
        name="orders", sql_table="orders", data_source="test",
        default_time_dimension="created_at",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
            Column(name="amount", sql="amount", type=DataType.DOUBLE),
            Column(name="total", sql="amount", type=DataType.DOUBLE),
        ],
    )


async def test_tsql_time_shift_inner_cte_uses_mangled_brackets() -> None:
    """``change_pct(total:sum)`` builds shifted/self-join/step CTEs."""
    q = SlayerQuery(
        source_model="orders",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
            date_range=["2024-03-01", "2024-03-31"],
        )],
        measures=[
            ModelMeasure(formula="total:sum"),
            ModelMeasure(formula="change_pct(total:sum)", name="pct"),
        ],
        order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
    )
    sql = await _tsql_generate(q, _orders_model_tsql())
    # No ANSI-quoted identifiers — they'd bypass Bug 2 mangling and the
    # literal-dot form would fail T-SQL's ORDER BY alias resolver.
    assert '"orders.' not in sql, (
        f"T-SQL emission must not contain ANSI-quoted dotted identifiers "
        f"(would bypass Bug 2 mangling):\n{sql}"
    )
    # All dotted aliases must be mangled.
    assert "[orders.created_at]" not in sql, (
        f"T-SQL emission must not contain literal-dot bracketed aliases "
        f"(Bug 2 mangling didn't fire):\n{sql}"
    )
    # Self-join CTE references the mangled form on both sides. (The typed
    # pipeline names the shifted CTE ``shifted__time_shift_inner``; the
    # legacy stack spelled it ``shifted__ts_pct``. Same CTE, same assertion.)
    lookup = "DATEADD(MONTH, -1, base.[orders___created_at])"
    assert f"{lookup} = shifted__time_shift_inner.[orders___created_at]" in sql, (
        f"Self-join ON clause must use mangled bracketed identifiers:\n{sql}"
    )
    assert f"{lookup} IS NULL" in sql, sql
    # Outer ORDER BY references the mangled alias.
    assert "[orders___created_at]" in sql
    # Computed expression's column references in step2 use mangled brackets.
    assert "[orders____time_shift_inner]" in sql, (
        f"Inner computed expression's column refs must be mangled-bracket "
        f"form:\n{sql}"
    )


async def test_tsql_order_by_does_not_wrap_alias_in_case_when_nulls_emulation() -> None:
    """ORDER BY references the SELECT alias at top level, not inside a CASE."""
    q = SlayerQuery(
        source_model="orders",
        dimensions=[ColumnRef(name="id"), ColumnRef(name="created_at")],
        order=[OrderItem(column=ColumnRef(name="id"), direction="asc")],
    )
    sql = await _tsql_generate(q, _orders_model_tsql())
    assert "CASE WHEN" not in sql.upper() or "ORDER BY\n  CASE WHEN" not in sql, (
        f"T-SQL ORDER BY must not wrap the alias in a NULLS-emulation "
        f"CASE WHEN sub-expression — alias resolution fails inside it:\n{sql}"
    )
    # The ORDER BY must reference the mangled alias as the whole expression.
    assert "ORDER BY\n  [orders___id]" in sql or "ORDER BY [orders___id]" in sql, (
        f"T-SQL ORDER BY must reference the SELECT alias as a top-level "
        f"expression:\n{sql}"
    )
