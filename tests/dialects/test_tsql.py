"""DEV-1542: tests for TsqlDialect (SQL Server / Microsoft T-SQL).

T-SQL has the most divergent shape of any Tier-1 dialect:
* ``DATETRUNC(unit, col)`` instead of ``DATE_TRUNC('unit', col)``
* Week uses ``iso_week`` for Monday-based truncation
* ``DATEADD(unit, val, col)`` instead of ``col + INTERVAL N UNIT``
* PERCENTILE_CONT is window-only — ``build_median`` / ``build_percentile``
  raise ``NotImplementedError``
* Statistical aggregate names: STDEV / STDEVP / VAR / VARP (not the
  Postgres canonical names)
* Variance-decomposition formula for CORR / COVAR_SAMP / COVAR_POP
* EXPLAIN is a session-toggle pair: ``SET SHOWPLAN_ALL ON; ... ; SET SHOWPLAN_ALL OFF``
* No native LOG2 (log2_native = False)
"""

from __future__ import annotations

import asyncio
import re
import tempfile

import sqlglot
from sqlglot import exp

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import Column, DatasourceConfig, ModelMeasure, SlayerModel
from slayer.core.query import ColumnRef, OrderItem, SlayerQuery, TimeDimension
from slayer.engine.enrichment import enrich_query
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql.generator import SQLGenerator
from slayer.sql.dialects.tsql import TsqlDialect
from slayer.storage.yaml_storage import YAMLStorage


def _parse_tsql(sql: str) -> exp.Expression:
    return sqlglot.parse_one(sql, dialect="tsql")


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


# ---------------------------------------------------------------------------
# build_date_trunc — DATETRUNC(unit, col), iso_week for week
# ---------------------------------------------------------------------------


def test_tsql_build_date_trunc_month() -> None:
    d = TsqlDialect()
    col = sqlglot.parse_one("created_at", dialect="tsql")
    out = d.build_date_trunc(col, TimeGranularity.MONTH, parse=_parse_tsql)
    sql = out.sql(dialect="tsql").lower()
    assert "datetrunc" in sql
    assert "month" in sql


def test_tsql_build_date_trunc_week_uses_iso_week() -> None:
    """Week must use ISO_WEEK (Monday-start) to be @@DATEFIRST-independent."""
    d = TsqlDialect()
    col = sqlglot.parse_one("created_at", dialect="tsql")
    out = d.build_date_trunc(col, TimeGranularity.WEEK, parse=_parse_tsql)
    sql = out.sql(dialect="tsql").lower()
    assert "datetrunc" in sql
    assert "iso_week" in sql


def test_tsql_build_date_trunc_casts_non_column_to_timestamp() -> None:
    """``DATETRUNC`` requires a temporal type — non-column operands are
    wrapped in ``CAST(... AS TIMESTAMP)``."""
    d = TsqlDialect()
    lit = sqlglot.parse_one("'2025-01-01'", dialect="tsql")
    out = d.build_date_trunc(lit, TimeGranularity.MONTH, parse=_parse_tsql)
    assert "CAST" in out.sql(dialect="tsql").upper()


# ---------------------------------------------------------------------------
# build_time_offset_expr — DATEADD, no INTERVAL
# ---------------------------------------------------------------------------


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
    """DATEADD takes a signed amount as its second arg — negative values
    propagate directly into the call."""
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


# ---------------------------------------------------------------------------
# add_intervals_expr — chains DATEADD calls (no INTERVAL)
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Median / percentile — not supported on T-SQL
# ---------------------------------------------------------------------------


def test_tsql_build_median_raises_not_implemented() -> None:
    d = TsqlDialect()
    inner = sqlglot.parse_one("amount", dialect="tsql")
    with pytest.raises(NotImplementedError, match="median.*T-SQL"):
        d.build_median(inner, parse=_parse_tsql)


def test_tsql_build_percentile_raises_not_implemented() -> None:
    d = TsqlDialect()
    with pytest.raises(NotImplementedError, match="percentile.*T-SQL"):
        d.build_percentile("0.5", "amount", parse=_parse_tsql)


# ---------------------------------------------------------------------------
# Stat aggs — T-SQL canonical names via exp.Anonymous
# ---------------------------------------------------------------------------


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
    """sqlglot's tsql transpiler emits incorrect names (e.g. VAR_SAMP,
    VARIANCE_POP). The override emits the canonical T-SQL names via
    ``exp.Anonymous``."""
    d = TsqlDialect()
    out = d.build_stat_agg_1arg(agg_name, "amount", parse=_parse_tsql)
    sql = out.sql(dialect="tsql").upper()
    assert tsql_fn in sql
    # Sanity: NOT the Postgres-canonical name
    assert agg_name.upper() not in sql


# ---------------------------------------------------------------------------
# Covar — variance-decomposition formula with T-SQL names
# ---------------------------------------------------------------------------


def test_tsql_build_covar_2arg_corr_uses_decomposition() -> None:
    d = TsqlDialect()
    out = d.build_covar_2arg("corr", "amount", "quantity", parse=_parse_tsql)
    sql = out.sql(dialect="tsql").upper()
    # T-SQL covariance formula uses VAR / STDEV (sample form for corr/covar_samp)
    assert "VAR" in sql
    assert "STDEV" in sql
    assert "NULLIF" in sql  # zero-denominator guard for corr


def test_tsql_build_covar_2arg_covar_pop_uses_varp() -> None:
    d = TsqlDialect()
    out = d.build_covar_2arg("covar_pop", "amount", "quantity", parse=_parse_tsql)
    sql = out.sql(dialect="tsql").upper()
    assert "VARP" in sql


# ---------------------------------------------------------------------------
# build_explain_sql — wraps in SHOWPLAN session toggle pair
# ---------------------------------------------------------------------------


def test_tsql_build_explain_sql_wraps_in_showplan_pair() -> None:
    d = TsqlDialect()
    assert d.build_explain_sql("SELECT 1") == (
        "SET SHOWPLAN_ALL ON; SELECT 1; SET SHOWPLAN_ALL OFF"
    )


# ---------------------------------------------------------------------------
# DEV-1571 Bug 1 — emit_outer_wrap hoists inner CTEs to top
# ---------------------------------------------------------------------------


_INNER_WITH_CTES = (
    "WITH base AS (SELECT id, status FROM orders),\n"
    "     step2 AS (SELECT id, status FROM base)\n"
    "SELECT id AS [orders.id], status AS [orders.status] FROM step2"
)


def _normalise(sql: str) -> str:
    """Collapse whitespace for shape assertions."""
    return " ".join(sql.split())


def test_tsql_emit_outer_wrap_hoists_inner_ctes() -> None:
    """T-SQL rejects ``WITH`` inside a derived-table subquery. The override
    lifts the inner CTE list to the outermost statement so::

        SELECT ... FROM (
          WITH base AS (...), step2 AS (...)
          SELECT ... FROM step2
        ) AS _outer

    becomes::

        WITH base AS (...), step2 AS (...)
        SELECT ... FROM (SELECT ... FROM step2) AS _outer

    Bug 1 in DEV-1571.
    """
    out = TsqlDialect().emit_outer_wrap(
        inner_sql=_INNER_WITH_CTES,
        public=["orders.id", "orders.status"],
        order=None,
        limit=None,
        offset_arg=None,
    )
    normalised = _normalise(out)
    assert normalised.startswith("WITH "), (
        f"Expected hoisted statement to start with WITH; got: {out}"
    )
    # No nested WITH inside parens.
    assert "(WITH " not in normalised and "( WITH " not in normalised, (
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
    """When the inner SELECT has no CTEs, the hoist is a no-op and the
    emitted shape matches the base impl: derived-table wrap with public
    aliases on the outer projection.
    """
    inner = "SELECT id AS [orders.id], status AS [orders.status] FROM orders"
    out = TsqlDialect().emit_outer_wrap(
        inner_sql=inner,
        public=["orders.id", "orders.status"],
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
    """Bug 3 for T-SQL: outer projection identifiers use ``[...]``, not
    ``"..."`` or `` ` ``. Generated via sqlglot's T-SQL dialect quoting.
    """
    out = TsqlDialect().emit_outer_wrap(
        inner_sql="SELECT 1 AS [orders.x]",
        public=["orders.x"],
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
    """Outer wrap with ``LIMIT N`` re-emits as T-SQL ``TOP``/``FETCH NEXT``
    via sqlglot. The exact spelling is sqlglot's responsibility — assert
    that no naked ``LIMIT`` token survives.
    """
    limit = sqlglot.parse_one("SELECT 1 LIMIT 5", dialect="tsql").args.get("limit")
    out = TsqlDialect().emit_outer_wrap(
        inner_sql=_INNER_WITH_CTES,
        public=["orders.id"],
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
    """Regression pin: the no-CTE branch must also transpose ``LIMIT``
    into T-SQL's ``TOP`` / ``FETCH NEXT N ROWS ONLY`` syntax, not emit
    literal ``LIMIT N`` (which T-SQL rejects).

    Before the fix, ``TsqlDialect.emit_outer_wrap`` fell back to the base
    impl's string concat whenever the inner SELECT had no top-level CTE,
    re-introducing the bug for any T-SQL query that hit the outer-wrap
    path (DEV-1444) without happening to have a CTE chain. Codex caught
    this in the DEV-1571 PR review.
    """
    inner = "SELECT id AS [orders.id] FROM orders"  # no WITH
    limit = sqlglot.parse_one("SELECT 1 LIMIT 5", dialect="tsql").args.get("limit")
    out = TsqlDialect().emit_outer_wrap(
        inner_sql=inner,
        public=["orders.id"],
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
    """Outer wrap with ``OFFSET N`` re-emits via sqlglot's T-SQL dialect.
    Asserts the offset value survives without raising.
    """
    offset_arg = sqlglot.parse_one(
        "SELECT 1 ORDER BY 1 OFFSET 10 ROWS", dialect="tsql"
    ).args.get("offset")
    out = TsqlDialect().emit_outer_wrap(
        inner_sql=_INNER_WITH_CTES,
        public=["orders.id"],
        order=None,
        limit=None,
        offset_arg=offset_arg,
    )
    assert "10" in out
    assert "OFFSET" in out.upper()


def test_tsql_emit_outer_wrap_with_order_and_offset() -> None:
    """ORDER BY combined with OFFSET — both ride on the outer statement
    (after hoisted CTE list and outer SELECT).
    """
    sql = "SELECT 1 ORDER BY 1 OFFSET 10 ROWS"
    parsed = sqlglot.parse_one(sql, dialect="tsql")
    order = parsed.args.get("order")
    offset_arg = parsed.args.get("offset")
    out = TsqlDialect().emit_outer_wrap(
        inner_sql=_INNER_WITH_CTES,
        public=["orders.id"],
        order=order,
        limit=None,
        offset_arg=offset_arg,
    )
    upper = out.upper()
    assert "ORDER BY" in upper
    assert "OFFSET" in upper
    assert _normalise(out).startswith("WITH ")


def test_tsql_emit_outer_wrap_strips_inner_qualifiers_in_order_by() -> None:
    """The detached ORDER BY may carry inner-CTE qualifiers like
    ``_base."col"`` from ``_assemble_combined_sql``. Those don't resolve
    at the outer wrapper level (only ``_outer`` is in scope). The override
    must strip table qualifiers — matching the base impl's existing
    behaviour from DEV-1444.

    Pin Codex MEDIUM #4.
    """
    order = sqlglot.parse_one(
        'SELECT 1 ORDER BY _base."orders.id" ASC', dialect="tsql"
    ).args.get("order")
    out = TsqlDialect().emit_outer_wrap(
        inner_sql=_INNER_WITH_CTES,
        public=["orders.id"],
        order=order,
        limit=None,
        offset_arg=None,
    )
    # The inner CTE alias must not leak into the outer ORDER BY.
    assert "_base." not in out, (
        f"Inner-CTE qualifier _base. leaked into outer ORDER BY: {out}"
    )


def test_tsql_emit_outer_wrap_hidden_alias_in_order_by() -> None:
    """ORDER BY may reference an inner-projected HIDDEN alias not in
    ``public`` (e.g. a sort key the user didn't ask for in the projection).
    The outer wrapper must still resolve the bare alias against the
    derived-table scope.

    Mirrors the existing _build_outer_wrap behaviour where the derived-
    table subquery exposes every alias (including hidden sort keys), so
    ORDER BY a hidden alias works as long as the qualifier is stripped.

    Pin Codex (Step 5) MEDIUM #2.
    """
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
    """Multiple inner CTEs are hoisted in declared order (sqlglot's
    ``With`` node preserves declaration order).
    """
    inner = (
        "WITH alpha AS (SELECT 1 AS a),\n"
        "     beta AS (SELECT 2 AS b),\n"
        "     gamma AS (SELECT 3 AS c)\n"
        "SELECT * FROM gamma"
    )
    out = TsqlDialect().emit_outer_wrap(
        inner_sql=inner,
        public=["c"],
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


# ---------------------------------------------------------------------------
# DEV-1571 Bug 2 — bracketed dotted alias mangling on rewrite_emitted_sql
# ---------------------------------------------------------------------------


def test_tsql_rewrite_emitted_sql_mangles_single_dot_alias() -> None:
    """``[a.b]`` becomes ``[a___b]`` so T-SQL's ORDER BY resolver sees a
    single dotless identifier and can match the SELECT alias.
    """
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
    """Single-segment bracketed identifiers (``[order]``, ``[my_col]``)
    are unchanged — the regex requires at least one dot.
    """
    sql = "SELECT [my_col], [order], [user] FROM [my_table]"
    assert TsqlDialect().rewrite_emitted_sql(sql) == sql


def test_tsql_rewrite_emitted_sql_leaves_brackets_with_spaces_untouched() -> None:
    """T-SQL allows arbitrary chars inside brackets (e.g. ``[my table]``,
    ``[my.col with spaces]``). The regex uses ``\\w`` so any
    non-word character breaks the match — these survive unchanged.
    """
    sql = "SELECT [my col] FROM [tbl with space]"
    assert TsqlDialect().rewrite_emitted_sql(sql) == sql


def test_tsql_rewrite_emitted_sql_regex_is_ascii_only() -> None:
    """The dotted-alias regex must be compiled with ``re.ASCII`` so
    Unicode word characters (e.g. ``café``) do not widen the match.

    Without ``re.ASCII``, ``\\w`` matches non-ASCII letters and an
    identifier like ``[café.metric]`` would be silently mangled, surprising
    users who legitimately put accented identifiers in their schema.

    Pin Codex (Step 5) LOW #5 — characterisation rather than constraint.
    """
    sql = "SELECT 1 AS [café.metric]"
    assert TsqlDialect().rewrite_emitted_sql(sql) == sql, (
        "Non-ASCII word characters must not match. The regex must use "
        "re.ASCII so \\w is ASCII-only."
    )


def test_tsql_rewrite_emitted_sql_idempotent_on_already_mangled() -> None:
    """An already-mangled alias (no dot inside brackets) is left alone.

    The regex requires at least one ``.`` inside the bracketed identifier,
    so ``___``-form aliases never match it. Pins ``rewrite_emitted_sql``
    being safe to invoke on its own output.
    """
    sql = "SELECT 1 AS [orders___id]"
    assert TsqlDialect().rewrite_emitted_sql(sql) == sql


def test_tsql_rewrite_emitted_sql_false_positive_on_user_bracketed_dotted_path() -> None:
    """Characterisation: a user-authored ``[my_schema.my_table]`` inside
    Column.sql DOES false-positive mangle, mirroring BigQuery's pre-DEV-1571
    constraint.

    T-SQL users writing such paths in ``Column.sql`` must bracket each
    segment individually: ``[my_schema].[my_table]``. Pin so any future
    context-aware narrowing is a deliberate change.
    """
    sql = "SELECT col FROM [my_schema.my_table]"
    out = TsqlDialect().rewrite_emitted_sql(sql)
    # Documented constraint: word-only bracketed dotted paths get mangled.
    assert out == "SELECT col FROM [my_schema___my_table]", (
        f"Documented constraint changed: {out}"
    )


# ---------------------------------------------------------------------------
# DEV-1571 Bug 2 — decode_result_keys reverses the mangling
# ---------------------------------------------------------------------------


def test_tsql_decode_result_keys_reverses_mangle() -> None:
    """Mangled keys are decoded back to SLayer's dotted alias shape on
    response."""
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


# ---------------------------------------------------------------------------
# DEV-1571 Bug 2 — round-trip bijection sanity via the dialect's regex
# ---------------------------------------------------------------------------


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
    """The dialect-level round-trip preserves SLayer's realistic alias
    space — every projection alias has at least one dot from the model
    prefix, so encode is non-trivial AND decode reverses it exactly.
    """
    d = TsqlDialect()
    sql = f"SELECT 1 AS [{original}]"
    mangled = d.rewrite_emitted_sql(sql)
    m = re.search(r"AS \[([^\]]+)\]", mangled)
    assert m is not None, f"could not find alias in mangled SQL: {mangled}"
    decoded = d.decode_result_keys([{m.group(1): 1}])
    assert decoded == [{original: 1}]


# ---------------------------------------------------------------------------
# Engine-level integration: SlayerResponse round-trip on T-SQL alias decoding
#
# Mirrors the BigQuery pattern (``tests/dialects/test_bigquery.py::
# TestEngineDecodeIntegration``). Stubs the SQL client so we exercise
# ``engine.execute()``'s post-fetch decode hook end-to-end without a live
# SQL Server instance. Pins Codex MEDIUM #7 — Bug 2 is an execution-path
# issue, so the dialect-level decode round-trip is not enough.
# ---------------------------------------------------------------------------


class _FakeTsqlClient:
    """Stub SQL client that returns T-SQL-mangled row keys (``a___b`` form)."""

    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    async def execute(self, *, sql: str) -> list[dict]:  # noqa: ARG002 — stub signature  # NOSONAR(S7503) — must remain async to match SlayerSQLClient.execute (awaited by engine.execute)
        return [dict(row) for row in self._rows]


async def _build_tsql_engine(rows: list[dict]) -> tuple[SlayerQueryEngine, tempfile.TemporaryDirectory, DatasourceConfig]:
    """Build a SlayerQueryEngine pointed at a fake T-SQL datasource whose
    SQL client is pre-stubbed with ``rows``."""
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
    engine._sql_clients[(ds.get_connection_string(), "")] = _FakeTsqlClient(rows)
    return engine, tmp, ds


class TestEngineTsqlDecodeIntegration:
    """End-to-end: stub client returns mangled keys; engine decodes them
    before packaging into ``SlayerResponse``. Pins Codex MEDIUM #7.
    """

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


# ---------------------------------------------------------------------------
# DEV-1571 Bug 3 follow-up — T-SQL inner CTEs get dialect-aware bracket
# quoting AND Bug 2 mangling fires on those identifiers.
#
# Pre-fix, the inner CTE assembly emitted hardcoded ANSI double quotes.
# On T-SQL the result PARSED (T-SQL accepts ``"..."`` as identifiers when
# QUOTED_IDENTIFIER is ON, the default) but the dotted alias bypassed
# Bug 2's bracket-anchored mangling regex, so the literal-dot form left
# the ORDER BY resolver unable to match the SELECT alias.
# ---------------------------------------------------------------------------


async def _noop_resolver_tsql(**kw):  # noqa: ARG001  # NOSONAR(S7503) — resolver stub must remain async
    return None


def _tsql_generate(query: SlayerQuery, model: SlayerModel) -> str:
    async def _run() -> str:
        enriched = await enrich_query(
            query=query, model=model,
            resolve_dimension_via_joins=_noop_resolver_tsql,
            resolve_cross_model_measure=_noop_resolver_tsql,
            resolve_join_target=_noop_resolver_tsql,
            dialect="tsql",
        )
        return SQLGenerator(dialect="tsql").generate(enriched=enriched)

    return asyncio.run(_run())


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


def test_tsql_time_shift_inner_cte_uses_mangled_brackets() -> None:
    """``change_pct(total:sum)`` builds shifted/self-join/step CTEs.
    After DEV-1571 Bug 3 follow-up, every identifier in those CTEs uses
    T-SQL brackets AND Bug 2 mangling converts the dotted aliases to
    underscore form so ORDER BY can match them.

    Regression pin for the CI failure on
    ``tests/integration/test_integration_sqlserver.py::TestSQLServerQueries::test_change_pct_with_date_range``.
    """
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
    sql = _tsql_generate(q, _orders_model_tsql())
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
    # Self-join CTE references the mangled form on both sides.
    assert (
        "base.[orders___created_at] = shifted__ts_pct.[orders___created_at]"
        in sql
    ), f"Self-join ON clause must use mangled bracketed identifiers:\n{sql}"
    # Outer ORDER BY references the mangled alias.
    assert "[orders___created_at]" in sql
    # Computed expression's column references in step2 use mangled brackets.
    assert "[orders____ts_pct]" in sql, (
        f"Inner CASE expression's column refs must be mangled-bracket form:\n{sql}"
    )
