"""DEV-1542: TsqlDialect (SQL Server / Microsoft T-SQL).

T-SQL is the most divergent Tier-1 dialect:

* ``DATETRUNC(unit, col)`` (SQL Server 2022+) instead of ``DATE_TRUNC``
* Week uses ``iso_week`` to be ``@@DATEFIRST``-independent (Monday-based)
* ``DATEADD(unit, val, col)`` instead of ``col + INTERVAL N UNIT``
* ``add_intervals_expr`` chains ``DATEADD`` calls (no INTERVAL)
* ``build_median`` / ``build_percentile`` raise — PERCENTILE_CONT in T-SQL
  is a window function only
* Statistical aggregate names: STDEV / STDEVP / VAR / VARP via
  ``exp.Anonymous`` (sqlglot's tsql transpiler emits wrong names)
* Variance-decomposition formula for CORR / COVAR_* with the T-SQL names
* EXPLAIN is a session-toggle pair: ``SET SHOWPLAN_ALL ON; ... ; OFF``
* No native LOG2
* DEV-1571 Bug 1: T-SQL rejects ``WITH`` inside a derived-table subquery.
  ``emit_outer_wrap`` overrides the base to hoist inner top-level CTEs
  to the outer statement.
* DEV-1571 Bug 2: T-SQL's ``ORDER BY`` resolver does not treat
  ``[a.b]`` as a SELECT alias — it tries to resolve it as a column-name
  lookup against the FROM scope. ``rewrite_emitted_sql`` mangles dotted
  bracketed aliases to ``[a___b]``; ``decode_result_keys`` reverses on
  result rows. Same bijection as ``BigqueryDialect``, different regex
  anchor.
"""

from __future__ import annotations

import re
from typing import Any, Callable, Optional

import sqlglot
from sqlglot import exp

from slayer.core.enums import TimeGranularity
from slayer.sql.dialects._alias_mangle import decode_alias, encode_alias
from slayer.sql.dialects.base import SqlDialect, _build_covar_decomposition


# sqlglot's tsql transpiler emits incorrect names (VAR_SAMP, VARIANCE_POP)
# that do not exist in T-SQL — these are the correct T-SQL canonical names.
_TSQL_STAT_NAMES: dict[str, str] = {
    "stddev_samp": "STDEV",
    "stddev_pop": "STDEVP",
    "var_samp": "VAR",
    "var_pop": "VARP",
}


# DEV-1571 Bug 2: bracket-quoted dotted alias. Same shape as BigQuery's
# backtick-anchored regex (``\w+(?:\.\w+)+``) with ``re.ASCII`` keeping
# ``\w`` ASCII-only so accented identifiers like ``[café.metric]`` do
# not mangle.
#
# Caveat (documented constraint, identical to BigQuery's): a fully
# bracketed dotted path of word-only segments (e.g. ``[my_schema.my_table]``)
# WOULD false-positive mangle. T-SQL users writing such paths in
# ``Column.sql`` must bracket each segment individually
# (``[my_schema].[my_table]``). T-SQL identifiers with spaces, hyphens,
# or other non-``\w`` characters (``[my table]``) are safe — the
# non-word character breaks the match.
_TSQL_DOTTED_ALIAS_RE = re.compile(r"\[(\w+(?:\.\w+)+)\]", re.ASCII)


class TsqlDialect(SqlDialect):
    sqlglot_name: str = "tsql"
    ds_type_aliases: frozenset[str] = frozenset({"mssql", "sqlserver", "tsql"})
    explain_prefix: Optional[str] = "SET SHOWPLAN_ALL ON;"
    explain_postfix: str = "; SET SHOWPLAN_ALL OFF"
    log10_native: bool = True
    log2_native: bool = False

    def build_date_trunc(
        self,
        col_expr: exp.Expression,
        granularity: TimeGranularity,
        *,
        parse: Callable[[str], exp.Expression],
    ) -> exp.Expression:
        """T-SQL: ``DATETRUNC(unit, col)``. Week uses ``iso_week``
        (Monday-start) to be ``@@DATEFIRST``-independent. ``DATETRUNC``
        requires a temporal type — wrap non-column/cast operands.

        ``DATETRUNC`` requires **SQL Server 2022+**. SLayer's T-SQL
        support is documented as 2022+ only (see ``CLAUDE.md`` under
        Tier-1 / SQL Server, and ``examples/sqlserver/``). Pre-2022
        SQL Server does not have a single-call truncation function;
        an equivalent ``DATEADD(unit, DATEDIFF(unit, 0, col), 0)``
        fallback exists but isn't a current target — track separately
        if anyone needs it.
        """
        gran_str = granularity.value
        if not isinstance(col_expr, (exp.Column, exp.Cast)):
            col_expr = exp.Cast(this=col_expr, to=exp.DataType.build("TIMESTAMP"))
        tsql_gran = "iso_week" if gran_str == "week" else gran_str
        return exp.Anonymous(
            this="DATETRUNC",
            expressions=[exp.Var(this=tsql_gran), col_expr],
        )

    def build_time_offset_expr(
        self,
        col_expr: exp.Expression,
        offset: int,
        granularity: str,
    ) -> exp.Expression:
        """T-SQL: ``DATEADD(unit, val, col)``. INTERVAL is not valid T-SQL syntax.
        Quarter normalises to ``val * 3`` of MONTH."""
        unit_map = {
            "year": "YEAR", "month": "MONTH", "day": "DAY",
            "quarter": "MONTH", "week": "WEEK",
            "hour": "HOUR", "minute": "MINUTE", "second": "SECOND",
        }
        unit = unit_map.get(granularity, granularity.upper())
        val = offset * 3 if granularity == "quarter" else offset
        return exp.Anonymous(
            this="DATEADD",
            expressions=[exp.Var(this=unit), exp.Literal.number(val), col_expr],
        )

    def add_intervals_expr(
        self,
        expr: exp.Expression,
        intervals: list[exp.Expression],
        sign: int = 1,
    ) -> exp.Expression:
        """T-SQL: chain ``DATEADD(unit, ±amount, col)`` calls.

        Each interval in the list is an ``exp.Interval`` from
        ``duration_interval_exprs``; extract unit name and amount, negate
        when sign < 0.
        """
        result = expr
        for iv in intervals:
            if not isinstance(iv, exp.Interval):
                raise TypeError(
                    f"Expected exp.Interval in T-SQL DATEADD branch, got {type(iv)}"
                )
            unit_str = iv.unit.name.upper()
            amount = exp.Neg(this=iv.this) if sign < 0 else iv.this
            result = exp.Anonymous(
                this="DATEADD",
                expressions=[exp.Var(this=unit_str), amount, result],
            )
        return result

    def build_median(
        self,
        inner: exp.Expression,
        *,
        parse: Callable[[str], exp.Expression],
    ) -> exp.Expression:
        raise NotImplementedError(
            "Aggregation 'median' is not supported on T-SQL (SQL Server): "
            "PERCENTILE_CONT in T-SQL is a window function (requires OVER clause) "
            "and cannot be used as a GROUP BY aggregate. "
            "Use a window subquery or compute the value client-side."
        )

    def build_percentile(
        self,
        p_str: str,
        col_sql: str,
        *,
        parse: Callable[[str], exp.Expression],
    ) -> exp.Expression:
        raise NotImplementedError(
            "Aggregation 'percentile' is not supported on T-SQL (SQL Server): "
            "PERCENTILE_CONT requires a window function OVER clause in T-SQL "
            "and is not valid as a GROUP BY aggregate. "
            "Compute the value client-side or restructure as a window query."
        )

    def build_stat_agg_1arg(
        self,
        agg_name: str,
        col_expr: str,
        *,
        parse: Callable[[str], exp.Expression],
    ) -> exp.Expression:
        """T-SQL: map ``stddev_samp``→``STDEV``, ``stddev_pop``→``STDEVP``,
        ``var_samp``→``VAR``, ``var_pop``→``VARP`` via ``exp.Anonymous``."""
        if agg_name in _TSQL_STAT_NAMES:
            return exp.Anonymous(
                this=_TSQL_STAT_NAMES[agg_name],
                expressions=[parse(col_expr)],
            )
        return super().build_stat_agg_1arg(agg_name, col_expr, parse=parse)

    def build_covar_2arg(
        self,
        agg_name: str,
        col_sql: str,
        other_sql: str,
        *,
        parse: Callable[[str], exp.Expression],
    ) -> exp.Expression:
        """T-SQL has no native CORR / COVAR_* — use the
        variance-decomposition formula with T-SQL names (VAR / VARP / STDEV)."""
        return _build_covar_decomposition(
            col_sql=col_sql,
            other_sql=other_sql,
            agg=agg_name,
            var_fn_samp="VAR",
            var_fn_pop="VARP",
            stddev_fn="STDEV",
            parse=parse,
        )

    # ------------------------------------------------------------------
    # DEV-1571 Bug 1: emit_outer_wrap hoists inner top-level CTEs
    # ------------------------------------------------------------------

    def emit_outer_wrap(
        self,
        *,
        inner_sql: str,
        public: list[str],
        order: Optional[exp.Expression],
        limit: Optional[exp.Expression],
        offset_arg: Optional[exp.Expression],
        parse: Optional[Callable[[str], exp.Expression]] = None,
    ) -> str:
        """T-SQL: hoist inner top-level CTEs to the outer statement.

        SQL Server allows ``WITH`` only as a statement prefix, not inside
        a derived-table subquery. Without this override, SLayer's
        DEV-1444 outer-wrap emits ``SELECT ... FROM (WITH ctes SELECT ...
        FROM step2) AS _outer ORDER BY ...``, which T-SQL rejects with
        ``Incorrect syntax near the keyword 'WITH'``.

        Strategy: parse ``inner_sql`` via the generator's ``_parse``
        (when supplied) so SLayer-specific AST rewrites survive the
        round-trip — LOG10/LOG2 alias preservation (DEV-1337) and
        SQLite JSONExtract function-form (DEV-1331). Then detach the
        top-level ``With`` node from the inner ``Select`` and emit
        ``WITH <ctes> SELECT <public> FROM (<inner_main_select>) AS
        _outer ORDER/LIMIT/OFFSET``.

        When the generator doesn't pass ``parse`` (direct unit-test
        invocation), falls back to ``sqlglot.parse_one(dialect="tsql")``.

        When the inner SELECT has no top-level CTEs, the hoist is a
        no-op — the override falls through to the base impl shape.
        """
        parse_fn = parse if parse is not None else (
            lambda s: sqlglot.parse_one(s, dialect=self.sqlglot_name)
        )
        try:
            parsed = parse_fn(inner_sql)
        except Exception:
            # Parse failure (malformed SQL, sqlglot bug, ...) — defer to
            # the base impl. T-SQL will still reject the nested WITH at
            # the DB layer, but we don't make it worse.
            return super().emit_outer_wrap(
                inner_sql=inner_sql,
                public=public,
                order=order,
                limit=limit,
                offset_arg=offset_arg,
            )
        # sqlglot exposes the WITH clause under the ``with_`` args key
        # (Python-keyword avoidance). Other clauses (``order`` / ``limit``
        # / ``offset``) use their natural names.
        with_node = (
            parsed.args.get("with_") if isinstance(parsed, exp.Select) else None
        )
        if not isinstance(parsed, exp.Select) or with_node is None:
            # No top-level CTEs — base behaviour is fine on T-SQL.
            return super().emit_outer_wrap(
                inner_sql=inner_sql,
                public=public,
                order=order,
                limit=limit,
                offset_arg=offset_arg,
            )
        # Detach the With from the inner Select so the inner main SELECT
        # can be wrapped in the derived table without re-introducing
        # nested WITH.
        parsed.set("with_", None)
        # Strip inner-CTE qualifiers from detached ORDER BY columns so
        # they resolve at the outer-wrapper scope (only ``_outer`` is
        # visible). DEV-1444 carry-over.
        if order is not None:
            for col in order.find_all(exp.Column):
                if col.args.get("table") is not None:
                    col.set("table", None)
        # Build the outer wrap entirely via sqlglot AST so dialect-aware
        # rendering transposes the detached ``Limit`` / ``Offset`` nodes
        # into T-SQL's ``TOP`` / ``FETCH NEXT N ROWS ONLY`` syntax. A
        # naïve ``limit.sql(dialect="tsql")`` only emits ``LIMIT N``
        # because the transposition fires on the wrapping Select, not on
        # a free-standing Limit node.
        derived = exp.Subquery(
            this=parsed,
            alias=exp.TableAlias(this=exp.to_identifier("_outer")),
        )
        outer = exp.Select()
        for a in public:
            outer = outer.select(exp.Identifier(this=a, quoted=True))
        outer = outer.from_(derived)
        outer.set("with_", with_node)
        if order is not None:
            outer.set("order", order)
        if limit is not None:
            outer.set("limit", limit)
        if offset_arg is not None:
            outer.set("offset", offset_arg)
        return outer.sql(dialect=self.sqlglot_name, pretty=True)

    # ------------------------------------------------------------------
    # DEV-1571 Bug 2: bracketed dotted-alias mangling
    # ------------------------------------------------------------------

    def rewrite_emitted_sql(self, sql: str) -> str:
        """Replace ``.`` with ``___`` inside bracket-quoted identifiers.

        T-SQL's ``ORDER BY`` resolver does not treat ``[a.b]`` as a
        SELECT alias — it tries to resolve it as a column-name lookup
        against the FROM scope and fails with ``Invalid column name``.
        Mangling on emit gives the parser a single dotless identifier
        and the alias resolves cleanly. ``decode_result_keys`` reverses
        the mangling on result rows so consumers see SLayer's universal
        dotted alias shape.

        Uses the same bijection as ``BigqueryDialect`` (shared encode in
        ``slayer.sql.dialects._alias_mangle``); only the regex anchor
        differs.
        """
        return _TSQL_DOTTED_ALIAS_RE.sub(
            lambda m: f"[{encode_alias(m.group(1))}]", sql
        )

    def decode_result_keys(
        self,
        rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Reverse the T-SQL alias mangling on result-row keys so
        consumers see SLayer's universal dotted alias shape regardless
        of whether the query ran against T-SQL or another dialect.
        """
        return [{decode_alias(k): v for k, v in row.items()} for row in rows]
