"""DEV-1542: SqlDialect strategy base class.

Every dialect-specific SQL-generation quirk lives on a subclass of
``SqlDialect``. The base class itself is a fully concrete Postgres-shaped
default — concrete dialects (``SqliteDialect``, ``TsqlDialect``, ...)
override only the methods whose behaviour differs.

The class is a Pydantic ``BaseModel`` with ``frozen=True`` so registry
singletons can't drift. Method overrides happen via regular subclassing —
fields use class-level defaults (``sqlglot_name: str = "postgres"``).
"""

from __future__ import annotations

from typing import Any, Callable, Optional

from pydantic import BaseModel, ConfigDict
from sqlglot import exp

from slayer.core.enums import TimeGranularity


# ---------------------------------------------------------------------------
# Granularity & duration mapping (used by default impls of date_trunc /
# time-offset / interval helpers)
# ---------------------------------------------------------------------------

_GRANULARITY_TO_DATE_TRUNC = {
    TimeGranularity.SECOND: "second",
    TimeGranularity.MINUTE: "minute",
    TimeGranularity.HOUR: "hour",
    TimeGranularity.DAY: "day",
    TimeGranularity.WEEK: "week",
    TimeGranularity.MONTH: "month",
    TimeGranularity.QUARTER: "quarter",
    TimeGranularity.YEAR: "year",
}

_WINDOW_UNIT_SQL = {
    "y": "year",
    "m": "month",
    "w": "week",
    "d": "day",
    "h": "hour",
    "min": "minute",
    "s": "second",
}


def _granularity_to_unit(granularity: str) -> str:
    """Map a granularity string to a SQL INTERVAL unit name.

    Quarter has no INTERVAL unit on most dialects — callers normalise to
    ``MONTH`` with the value multiplied by 3 before invoking the default.
    Week stays ``WEEK`` (Postgres / MySQL / ClickHouse / BigQuery all
    accept it). SQLite + T-SQL override the whole method.
    """
    return {
        "year": "YEAR",
        "month": "MONTH",
        "day": "DAY",
        "quarter": "MONTH",  # caller multiplies by 3
        "week": "WEEK",
        "hour": "HOUR",
        "minute": "MINUTE",
        "second": "SECOND",
    }.get(granularity, granularity.upper())


# ---------------------------------------------------------------------------
# Shared variance-decomposition formula (used by MySQL + T-SQL overrides
# of build_covar_2arg).
# ---------------------------------------------------------------------------


def _build_covar_decomposition(
    *,
    col_sql: str,
    other_sql: str,
    agg: str,
    var_fn_samp: str,
    var_fn_pop: str,
    stddev_fn: str,
    parse: Callable[[str], exp.Expression],
) -> exp.Expression:
    """Variance-decomposition formula for corr / covar_samp / covar_pop.

    ``cov(x, y) = (Var(x+y) - Var(x) - Var(y)) / 2``
    ``corr(x, y) = cov_samp(x, y) / (Stddev(x) * Stddev(y))``

    Used by MySQL and T-SQL where the native CORR / COVAR_SAMP / COVAR_POP
    functions are absent. Both columns are NULL-guarded against each other
    so rows where either leg is NULL are excluded from all variance calls.

    Uses ``exp.Anonymous`` for aggregate calls to bypass sqlglot's MySQL
    rewrite that aliases VAR_SAMP → VARIANCE = VAR_POP (silently wrong).
    """
    var_fn = var_fn_samp if agg in ("covar_samp", "corr") else var_fn_pop

    x_guarded = parse(
        f"CASE WHEN ({other_sql}) IS NOT NULL THEN ({col_sql}) END"
    )
    y_guarded = parse(
        f"CASE WHEN ({col_sql}) IS NOT NULL THEN ({other_sql}) END"
    )
    xy_sum = exp.Add(this=x_guarded, expression=y_guarded)

    var_xy = exp.Anonymous(this=var_fn, expressions=[xy_sum])
    var_x = exp.Anonymous(this=var_fn, expressions=[x_guarded])
    var_y = exp.Anonymous(this=var_fn, expressions=[y_guarded])

    covar = exp.Div(
        this=exp.Paren(this=exp.Sub(
            this=exp.Sub(this=var_xy, expression=var_x),
            expression=var_y,
        )),
        expression=exp.Literal.number(2),
    )

    if agg != "corr":
        return covar

    std_x = exp.Anonymous(this=stddev_fn, expressions=[x_guarded])
    std_y = exp.Anonymous(this=stddev_fn, expressions=[y_guarded])
    raw_denom = exp.Paren(this=exp.Mul(this=std_x, expression=std_y))
    denom = exp.Anonymous(
        this="NULLIF", expressions=[raw_denom, exp.Literal.number(0)]
    )
    return exp.Div(this=covar, expression=denom)


# ---------------------------------------------------------------------------
# SqlDialect — base class with Postgres-shaped defaults
# ---------------------------------------------------------------------------


class SqlDialect(BaseModel):
    """Strategy class encapsulating one database's SQL-generation quirks.

    The base class IS the Postgres-shaped default. Concrete dialects
    (``SqliteDialect``, ``TsqlDialect``, ...) subclass and override only
    what differs.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    sqlglot_name: str = "postgres"
    ds_type_aliases: frozenset[str] = frozenset()
    explain_prefix: Optional[str] = "EXPLAIN"
    explain_postfix: str = ""
    log10_native: bool = True
    log2_native: bool = True

    # ------------------------------------------------------------------
    # Date-trunc / time arithmetic
    # ------------------------------------------------------------------

    def build_date_trunc(
        self,
        col_expr: exp.Expression,
        granularity: TimeGranularity,
        *,
        parse: Callable[[str], exp.Expression],
    ) -> exp.Expression:
        """Default: ``DATE_TRUNC('unit', col)`` via sqlglot's ``exp.DateTrunc``.

        Non-bare-column / non-cast operands are wrapped in
        ``CAST(... AS TIMESTAMP)`` so Postgres can pick the right
        ``date_trunc`` overload — preserving today's
        ``generator.py:_build_date_trunc`` behaviour.
        """
        gran_str = _GRANULARITY_TO_DATE_TRUNC.get(granularity, granularity.value)
        if not isinstance(col_expr, (exp.Column, exp.Cast)):
            col_expr = exp.Cast(this=col_expr, to=exp.DataType.build("TIMESTAMP"))
        return exp.DateTrunc(this=col_expr, unit=exp.Literal.string(gran_str))

    def build_time_offset_expr(
        self,
        col_expr: exp.Expression,
        offset: int,
        granularity: str,
    ) -> exp.Expression:
        """Default: ``col ± INTERVAL N UNIT`` via ``exp.Add`` / ``exp.Sub``.

        Granularity normalization (preserved across every dialect):
        ``quarter`` becomes ``val * 3`` of ``MONTH``. SQLite additionally
        normalises ``week`` to ``val * 7`` of ``days`` — that branch lives
        on ``SqliteDialect`` since other dialects accept ``WEEK`` natively.
        """
        unit = _granularity_to_unit(granularity)
        val = offset * 3 if granularity == "quarter" else offset
        if val >= 0:
            return exp.Add(
                this=col_expr,
                expression=exp.Interval(
                    this=exp.Literal.number(val),
                    unit=exp.Var(this=unit),
                ),
            )
        return exp.Sub(
            this=col_expr,
            expression=exp.Interval(
                this=exp.Literal.number(-val),
                unit=exp.Var(this=unit),
            ),
        )

    def duration_interval_exprs(
        self,
        parts: list[tuple[int, str]],
        sign: int = 1,
    ) -> list[exp.Expression]:
        """Default: one ``exp.Interval`` per (amount, unit) pair.

        The Add-vs-Sub direction is decided by ``add_intervals_expr`` from
        its own ``sign`` arg, so the Interval values themselves stay
        positive at this layer. sqlglot transpiles each single-unit
        interval per dialect (MySQL/ClickHouse/BigQuery all accept
        ``INTERVAL N UNIT``).
        """
        return [
            exp.Interval(
                this=exp.Literal.number(amount),
                unit=exp.Var(this=_WINDOW_UNIT_SQL[unit].upper()),
            )
            for amount, unit in parts
        ]

    def add_intervals_expr(
        self,
        expr: exp.Expression,
        intervals: list[exp.Expression],
        sign: int = 1,
    ) -> exp.Expression:
        """Default: fold ``exp.Add`` (sign>=0) or ``exp.Sub`` (sign<0) over
        the interval list."""
        op_cls = exp.Add if sign >= 0 else exp.Sub
        result = expr
        for iv in intervals:
            result = op_cls(this=result, expression=iv)
        return result

    # ------------------------------------------------------------------
    # Median / percentile / stat aggregates
    # ------------------------------------------------------------------

    def build_median(
        self,
        inner: exp.Expression,
        *,
        parse: Callable[[str], exp.Expression],
    ) -> exp.Expression:
        """Default: ``PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY inner)``."""
        inner_sql = inner.sql(dialect=self.sqlglot_name)
        return parse(f"PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {inner_sql})")

    def build_percentile(
        self,
        p_str: str,
        col_sql: str,
        *,
        parse: Callable[[str], exp.Expression],
    ) -> exp.Expression:
        """Default: ``PERCENTILE_CONT(p) WITHIN GROUP (ORDER BY col_sql)``.

        ``p_str`` is the original pre-validated string the user provided —
        not a float — so ``0.50`` / ``1`` / scientific notation are
        preserved verbatim (Codex finding #3).
        """
        return parse(
            f"PERCENTILE_CONT({p_str}) WITHIN GROUP (ORDER BY {col_sql})"
        )

    def build_stat_agg_1arg(
        self,
        agg_name: str,
        col_expr: str,
        *,
        parse: Callable[[str], exp.Expression],
    ) -> exp.Expression:
        """Default: emit canonical Postgres-style name and let sqlglot
        transpile per dialect (e.g. var_samp → VARIANCE on SQLite/DuckDB)."""
        return parse(f"{agg_name.upper()}({col_expr})")

    def build_covar_2arg(
        self,
        agg_name: str,
        col_sql: str,
        other_sql: str,
        *,
        parse: Callable[[str], exp.Expression],
    ) -> exp.Expression:
        """Default: native ``CORR(x, y)`` / ``COVAR_SAMP(x, y)`` /
        ``COVAR_POP(x, y)``."""
        return parse(f"{agg_name.upper()}({col_sql}, {other_sql})")

    # ------------------------------------------------------------------
    # Log-alias rewrite
    # ------------------------------------------------------------------

    def should_use_native_log(self, base: int) -> bool:
        """Whether ``log{N}(x)`` should be emitted as the dialect's native
        single-arg function (vs the canonical 2-arg ``LOG(N, x)``).

        Defaults: log10 native = True (every Tier-1+2 dialect except
        Oracle), log2 native = True (Postgres-shaped baseline). Concrete
        dialects override via the ``log10_native`` / ``log2_native``
        fields.
        """
        if base == 10:
            return self.log10_native
        if base == 2:
            return self.log2_native
        return False

    # ------------------------------------------------------------------
    # AST rewrite hook + per-connection UDF registration
    # ------------------------------------------------------------------

    def rewrite_parsed_ast(self, tree: exp.Expression) -> exp.Expression:
        """Default: identity. SQLite overrides to rewrite JSONExtract to
        the function-call form (DEV-1331)."""
        return tree

    def rewrite_emitted_sql(self, sql: str) -> str:
        """Default: identity. Post-pass string-level rewrite of the final
        generator output.

        Symmetric companion to ``rewrite_parsed_ast`` (the input-side
        hook): write-side, applied at the end of
        ``SQLGenerator.generate()`` AFTER ``_apply_outer_projection_trim``.

        Contract: preserve query semantics. Suitable for alias renames,
        identifier mangling/escape, dialect-quoting fixes. Do NOT change
        query shape — use the typed ``build_*`` methods on this class for
        that.

        Overrides today: ``BigqueryDialect`` mangles dotted aliases that
        would otherwise be rejected by BigQuery's output column-name
        grammar.
        """
        return sql

    def decode_result_keys(
        self,
        rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Default: identity. Reverse-pass on result-row keys to undo any
        write-side mangling applied by ``rewrite_emitted_sql``.

        Called at the end of ``SlayerQueryEngine.execute()`` so consumers
        always see SLayer's universal alias shape (``orders._count``,
        ``orders.products.category``) regardless of which dialect a
        query ran on.

        Overrides today: ``BigqueryDialect`` decodes the ``___`` mangling
        back to dots.
        """
        return rows

    def register_udfs(self, dbapi_connection) -> None:
        """Default: no-op. SQLite overrides to register Python aggregate
        / scalar UDFs on every fresh connection."""
        return None

    # ------------------------------------------------------------------
    # EXPLAIN
    # ------------------------------------------------------------------

    def build_explain_sql(self, sql: str) -> str:
        """Wrap ``sql`` in the dialect's EXPLAIN prefix/postfix pair.

        Raises ``ValueError`` when ``explain_prefix`` is ``None``
        (BigQuery — EXPLAIN unsupported). Preserves today's
        ``query_engine.py:_build_explain_sql`` semantics.
        """
        if self.explain_prefix is None:
            raise ValueError(
                f"EXPLAIN is not supported for dialect '{self.sqlglot_name}'. "
                "Use dry_run=True to inspect the generated SQL instead."
            )
        return f"{self.explain_prefix} {sql}{self.explain_postfix}"
