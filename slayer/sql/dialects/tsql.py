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
from typing import Any, Literal
from collections.abc import Callable, Sequence

import sqlglot
from sqlglot import exp

from slayer.core.enums import TimeGranularity
from slayer.sql.naming import OUTER_WRAP_ALIAS, decode_alias, encode_alias
from slayer.sql.dialects._identifier_fit import fit_identifier
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


def _offset_ordering_fallback(
    order: "exp.Expression | None", offset_arg: "exp.Expression | None",
) -> "exp.Expression | None":
    """The ORDER BY an OFFSET-bearing outer wrap must carry: the caller's, or a
    synthesized ``ORDER BY (SELECT NULL)`` no-op when there is none (SQL Server
    rejects OFFSET without ORDER BY). Returns ``order`` unchanged otherwise, so
    a user's ordering is never replaced (DEV-1783)."""
    if order is not None or offset_arg is None:
        return order
    return exp.Order(expressions=[
        exp.Ordered(this=exp.Subquery(this=exp.Select().select(exp.Null()))),
    ])


class TsqlDialect(SqlDialect):
    sqlglot_name: str = "tsql"
    ds_type_aliases: frozenset[str] = frozenset({"mssql", "sqlserver", "tsql"})
    explain_prefix: str | None = "SET SHOWPLAN_ALL ON;"
    explain_postfix: str = "; SET SHOWPLAN_ALL OFF"
    log10_native: bool = True
    log2_native: bool = False
    max_identifier_bytes: int | None = 128  # sysname is nvarchar(128)
    # Anonymous: sqlglot re-emits a parsed APPROX_COUNT_DISTINCT as its
    # Presto-family APPROX_DISTINCT canonical, which is not a T-SQL function.
    approx_count_distinct_anonymous_name: str | None = "APPROX_COUNT_DISTINCT"

    def build_null_safe_eq(
        self, left: exp.Expression, right: exp.Expression,
    ) -> exp.Expression:
        """DEV-1708: T-SQL has no ``IS NOT DISTINCT FROM`` / ``<=>`` — emit the
        portable expanded ``a = b OR (a IS NULL AND b IS NULL)``."""
        return self._expanded_null_safe_eq(left, right)

    def build_ordered(
        self,
        order_col: exp.Expression,
        *,
        descending: bool,
        nulls: Literal["default", "first", "last"] = "default",
    ) -> exp.Ordered:
        """DEV-1571 Bug 2 / DEV-1716 — pin ``nulls_first`` to T-SQL's native
        default for the direction (FIRST on ASC, LAST on DESC).

        Left unset, sqlglot emits ``CASE WHEN <alias> IS NULL THEN 1 ELSE 0
        END, <alias>`` to emulate the nulls-last ordering every other dialect
        gets; the bracketed alias INSIDE the CASE WHEN mis-resolves against the
        FROM scope (``Invalid column name``). So T-SQL trades null-ordering
        parity for a statement that runs — the one place SLayer's null ordering
        is dialect-specific, and only because the portable form is unavailable.

        An EXPLICIT ``first`` / ``last`` policy is honoured as asked — the pin
        exists to avoid the emulation, not to override a stated intent.
        """
        if nulls == "default":
            return exp.Ordered(
                this=order_col, desc=descending, nulls_first=not descending,
            )
        return super().build_ordered(
            order_col, descending=descending, nulls=nulls,
        )

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
        if granularity == TimeGranularity.WEEK_SUNDAY:
            # DEV-1572: delegate to the base generic shift, which composes
            # T-SQL's DATEADD day-offset around the iso_week (Monday) DATETRUNC.
            return super().build_date_trunc(
                col_expr=col_expr, granularity=granularity, parse=parse,
            )
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
            # DEV-1572: a one-period shift of a Sunday-week is one week — same
            # normalization the base ``_granularity_to_unit`` applies (without
            # it, ``DATEADD(WEEK_SUNDAY, ...)`` is invalid T-SQL).
            "week_sunday": "WEEK",
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

    def apply_pagination(
        self,
        select: exp.Select,
        *,
        limit: "int | None",
        offset: "int | None",
    ) -> exp.Select:
        """T-SQL pagination, with the ``OFFSET`` ordering requirement made
        explicit.

        SQL Server rejects ``OFFSET`` without an ``ORDER BY``. When the query is
        genuinely unordered we supply ``ORDER BY (SELECT NULL)`` — the
        conventional no-op ordering, which adds no semantics because there were
        none to preserve, and only makes the statement legal.

        sqlglot happens to inject the same thing today, but that is its
        behaviour and not our contract: doing it here means the rule survives a
        sqlglot upgrade, and it puts the ordering in the AST where a caller (and
        our tests) can see it rather than only in the generated string. A user's
        own ORDER BY is never replaced.

        ``TOP`` versus ``FETCH`` needs no special handling — sqlglot picks
        ``TOP`` for a bare limit and ``OFFSET … FETCH`` once an offset is
        present, which is the correct T-SQL in both cases.
        """
        if offset is not None and select.args.get("order") is None:
            select = select.order_by(
                exp.Subquery(this=exp.Select().select(exp.Null())),
            )
        return super().apply_pagination(select, limit=limit, offset=offset)

    def emit_outer_wrap(
        self,
        *,
        inner_sql: str,
        public: list[str],
        order: exp.Expression | None,
        limit: exp.Expression | None,
        offset_arg: exp.Expression | None,
        parse: Callable[[str], exp.Expression] | None = None,
    ) -> str:
        """T-SQL: hoist inner top-level CTEs to the outer statement AND
        transpose detached pagination to ``TOP`` / ``FETCH NEXT N ROWS
        ONLY`` syntax.

        SQL Server allows ``WITH`` only as a statement prefix, not inside
        a derived-table subquery. Without this override, SLayer's
        DEV-1444 outer-wrap emits ``SELECT ... FROM (WITH ctes SELECT ...
        FROM step2) AS _outer ORDER BY ...``, which T-SQL rejects with
        ``Incorrect syntax near the keyword 'WITH'``.

        Strategy (single AST path, no fallback to the base impl):

        1. Parse ``inner_sql`` via the generator's ``_parse`` (when
           supplied) so SLayer-specific AST rewrites survive the
           round-trip — LOG10/LOG2 alias preservation (DEV-1337) and
           SQLite JSONExtract function-form (DEV-1331).
        2. Detach the top-level ``With`` node (if any) from the inner
           ``Select`` so the inner main SELECT can be wrapped in the
           derived table without re-introducing nested WITH.
        3. Build the outer wrap entirely via sqlglot AST so dialect-
           aware rendering transposes the detached ``Limit`` / ``Offset``
           nodes into T-SQL's ``TOP`` / ``FETCH NEXT N ROWS ONLY``
           syntax. A naïve ``limit.sql(dialect="tsql")`` only emits
           ``LIMIT N`` because the transposition fires on the wrapping
           Select, not on a free-standing Limit node. The CTE-less
           branch must take the AST path too, otherwise any T-SQL query
           that hits the outer-wrap path without CTEs would still emit
           literal ``LIMIT N``.

        When the generator doesn't pass ``parse`` (direct unit-test
        invocation), falls back to ``sqlglot.parse_one(dialect="tsql")``.
        When the parse itself fails (malformed SQL / sqlglot bug), defers
        to the base impl — T-SQL will still reject malformed SQL at the
        DB layer, but we don't make it worse.
        """
        # SQL Server rejects OFFSET without ORDER BY. Resolve the effective
        # ordering BEFORE branching, so BOTH the AST path AND the base-impl
        # fallback (a non-Select inner, base.py also emits a bare OFFSET) get it.
        order = _offset_ordering_fallback(order, offset_arg)
        parse_fn = parse if parse is not None else (
            lambda s: sqlglot.parse_one(s, dialect=self.sqlglot_name)
        )
        try:
            parsed = parse_fn(inner_sql)
        except Exception:
            return super().emit_outer_wrap(
                inner_sql=inner_sql,
                public=public,
                order=order,
                limit=limit,
                offset_arg=offset_arg,
            )
        if not isinstance(parsed, exp.Select):
            return super().emit_outer_wrap(
                inner_sql=inner_sql,
                public=public,
                order=order,
                limit=limit,
                offset_arg=offset_arg,
            )
        # Detach the With (if present) so the inner main SELECT can be
        # wrapped in a derived table. ``with_`` is the sqlglot args key
        # (Python-keyword avoidance); other clauses use their natural
        # names (``order`` / ``limit`` / ``offset``).
        with_node = parsed.args.get("with_")
        if with_node is not None:
            parsed.set("with_", None)
        # Strip inner-CTE qualifiers from detached ORDER BY columns so
        # they resolve at the outer-wrapper scope (only ``_outer`` is
        # visible). DEV-1444 carry-over.
        if order is not None:
            for col in order.find_all(exp.Column):
                if col.args.get("table") is not None:
                    col.set("table", None)
        derived = exp.Subquery(
            this=parsed,
            alias=exp.TableAlias(this=exp.to_identifier(OUTER_WRAP_ALIAS)),
        )
        outer = exp.Select()
        for a in public:
            outer = outer.select(exp.Identifier(this=a, quoted=True))
        outer = outer.from_(derived)
        if with_node is not None:
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

    def fit_alias(self, name: str) -> str:
        """Size the budget against the post-mangle form (``.`` -> ``___`` adds 2
        bytes per dot); return value stays dotted for the regex below."""
        return fit_identifier(
            name=name, limit=self.max_identifier_bytes, expand=encode_alias,
        )

    def emit_alias(self, alias: str) -> str:
        """The final identifier: length-fitted, then dot-mangled."""
        return encode_alias(self.fit_alias(alias))

    def rewrite_emitted_sql(
        self, sql: str, *, aliases: Sequence[str] = (),
    ) -> str:
        """Replace ``.`` with ``___`` inside bracket-quoted identifiers.

        T-SQL's ``ORDER BY`` resolver treats ``[a.b]`` as a column lookup, not a
        SELECT alias, and fails; a dotless identifier resolves cleanly. Same
        bijection as ``BigqueryDialect``, only the regex anchor differs.

        Uses the same bijection as ``BigqueryDialect`` (shared encode in
        ``slayer.sql.naming``); only the regex anchor differs. The base LENGTH
        pass runs first: no-op on under-limit aliases, and over-limit ones
        arrive still-dotted for this pass — no double-encoding.
        """
        sql = super().rewrite_emitted_sql(sql=sql, aliases=aliases)
        return _TSQL_DOTTED_ALIAS_RE.sub(
            lambda m: f"[{encode_alias(m.group(1))}]", sql
        )

    def decode_result_keys(
        self,
        rows: list[dict[str, Any]],
        *,
        aliases: Sequence[str] = (),
    ) -> list[dict[str, Any]]:
        """Reverse the T-SQL alias mangling on result-row keys so consumers see
        SLayer's universal dotted shape whatever dialect ran the query.

        Fitted keys aren't recoverable alone, so the ``emitted -> canonical``
        map is consulted first, falling back to the ``___`` -> ``.`` bijection.
        """
        mapping = self.decode_alias_map(aliases)
        return [
            self._rekey_row(row=row, mapping=mapping, fallback=decode_alias)
            for row in rows
        ]
