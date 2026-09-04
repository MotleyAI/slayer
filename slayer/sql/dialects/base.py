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

import hashlib
import re
from functools import lru_cache
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Optional
from collections.abc import Callable, Sequence

from pydantic import BaseModel, ConfigDict
from sqlglot import exp
from sqlglot.dialects.dialect import Dialect as _SqlglotDialect

from slayer.core.enums import TimeGranularity
from slayer.core.errors import IdentifierCollisionError
from slayer.sql._identifier_fit import fit_identifier, substitute_quoted
from slayer.sql.naming_bijection import decode_alias, encode_alias

if TYPE_CHECKING:
    import sqlalchemy as sa

    from slayer.core.models import DatasourceConfig


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
        # DEV-1572: a one-period shift of a Sunday-week is just one week.
        "week_sunday": "WEEK",
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


@lru_cache(maxsize=None)
def _sqlglot_backslash_escapes(sqlglot_name: str) -> bool:
    """Whether ``sqlglot``'s tokenizer for ``sqlglot_name`` treats a backslash
    as a string-literal escape character (DEV-1727).

    This is the single source of truth for the Mode-A ``{var}`` escaping regime:
    deriving it from the same tokenizer that later PARSES the substituted SQL
    means our escaping can never drift from the parser. ``STRING_ESCAPES`` is a
    semi-internal sqlglot attribute; guard it so a future sqlglot change that
    renames/reshapes it fails loudly here rather than silently mis-escaping.
    """
    tokenizer = _SqlglotDialect.get_or_raise(sqlglot_name).tokenizer_class
    escapes = getattr(tokenizer, "STRING_ESCAPES", None)
    if not isinstance(escapes, (list, tuple, set, frozenset)):
        raise RuntimeError(
            f"Cannot derive the backslash-escaping regime for sqlglot dialect "
            f"{sqlglot_name!r}: its tokenizer's STRING_ESCAPES is "
            f"{type(escapes).__name__}, expected a collection of strings. A "
            f"sqlglot upgrade may have changed this internal API (DEV-1727)."
        )
    return "\\" in escapes


def _digest(secret: str | None) -> str:
    """Non-reversible id for secret material in cache keys. 16 hex chars keeps
    it log-readable; collisions are negligible at this scale."""
    if not secret:
        return ""
    return hashlib.sha256(secret.encode("utf-8")).hexdigest()[:16]


class SqlDialect(BaseModel):
    """Strategy class encapsulating one database's SQL-generation quirks.

    The base class IS the Postgres-shaped default. Concrete dialects
    (``SqliteDialect``, ``TsqlDialect``, ...) subclass and override only
    what differs.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    sqlglot_name: str = "postgres"
    ds_type_aliases: frozenset[str] = frozenset()
    explain_prefix: str | None = "EXPLAIN"
    explain_postfix: str = ""
    log10_native: bool = True
    log2_native: bool = True

    # Whether NUMERIC/DECIMAL columns are stored and aggregated exactly.
    # False (SQLite's numeric affinity) keeps the inferred cast instead of
    # native-type preservation.
    exact_decimal_native: bool = True

    # Conservative universal identifier budget in BYTES; ``None`` = unbounded
    # (fitting hooks become no-ops). Default is the tightest Tier-1 value
    # (Postgres), so a new dialect over-shortens rather than silently truncating.
    max_identifier_bytes: int | None = 63

    # DEV-1595 approximate-distinct emission. Template dialects set the first
    # ({col} substituted with the column SQL); Oracle/T-SQL set the second so
    # sqlglot does not re-emit a parsed APPROX_COUNT_DISTINCT as APPROX_DISTINCT.
    approx_count_distinct_template: str = "COUNT(DISTINCT {col})"
    approx_count_distinct_anonymous_name: str | None = None

    @property
    def backslash_escapes_strings(self) -> bool:
        """Whether this dialect's string literals treat a backslash as an escape
        character (MySQL/ClickHouse/Snowflake/Redshift/BigQuery/Databricks/Spark)
        rather than an ordinary char (SQLite/Postgres/DuckDB/T-SQL/Trino/Presto/
        Oracle).

        Drives DEV-1727 dialect-aware Mode-A ``{var}`` escaping: pass this to
        ``substitute_variables(..., backslash_escapes=...)`` so a value like
        ``a\\'b`` stays inside its quoted literal on every backend. Derived from
        sqlglot's tokenizer (see :func:`_sqlglot_backslash_escapes`) so it can't
        disagree with the parser; a pinning test freezes the expected value.
        """
        return _sqlglot_backslash_escapes(self.sqlglot_name)

    # ------------------------------------------------------------------
    # Null-safe equality (DEV-1708 / Codex F2)
    # ------------------------------------------------------------------

    def build_null_safe_eq(
        self, left: exp.Expression, right: exp.Expression,
    ) -> exp.Expression:
        """A null-safe equality (``left`` and ``right`` compare equal, and two
        NULLs compare equal) for the cross-model grain join-back's ``ON`` clause.

        Base (Postgres-family) uses sqlglot's ``NullSafeEQ`` → ``IS NOT DISTINCT
        FROM``, which sqlglot also transpiles correctly for DuckDB / Snowflake /
        BigQuery / Trino / Databricks / ClickHouse — and for MySQL, where it
        emits ``<=>``. MySQL therefore needs no override here (an earlier
        version of this docstring claimed one existed). ``SqliteDialect``
        overrides to bare ``IS``; dialects with no native form (T-SQL / Oracle /
        Redshift) to the expanded ``a = b OR (a IS NULL AND b IS NULL)``.
        """
        return exp.NullSafeEQ(this=left, expression=right)

    # ------------------------------------------------------------------
    # ORDER BY term construction (DEV-1747 D5 / P-H)
    # ------------------------------------------------------------------

    def build_ordered(
        self,
        order_col: exp.Expression,
        *,
        descending: bool,
        nulls: Literal["default", "first", "last"] = "default",
    ) -> exp.Ordered:
        """Build one ``ORDER BY`` term with its null-ordering policy applied.

        The single place any render site turns a resolved column plus a
        direction into an ``exp.Ordered`` (P-H). It previously lived on the
        generator as ``_ordered``, which meant the combined and transform-chain
        paths — which built their own ``exp.Ordered`` — silently skipped it.

        ``nulls="default"`` leaves ``nulls_first`` unset, which sqlglot renders
        as **nulls last on every dialect** — an explicit ``NULLS LAST`` where
        the native default differs and the syntax exists, a ``CASE WHEN <col>
        IS NULL …`` emulation where it does not (MySQL / SQLite). That
        uniformity is the point: a semantic layer whose NULLs sort first on
        SQLite and last on Postgres answers the same question two ways.

        T-SQL is the one exception and overrides this, because its emulation
        does not merely look different — the bracketed alias inside the CASE
        re-resolves against the FROM scope and the statement fails.

        ``"first"`` / ``"last"`` are an explicit intent and are honoured as
        asked, emulation included — that is the only way to express them on a
        dialect with no NULLS syntax.
        """
        kwargs: dict = {"this": order_col, "desc": descending}
        if nulls == "first":
            kwargs["nulls_first"] = True
        elif nulls == "last":
            kwargs["nulls_first"] = False
        return exp.Ordered(**kwargs)

    def native_nulls_first(self, *, descending: bool) -> bool:
        """Where NULLs sort in this dialect's OWN ordering for ``descending``.

        Setting ``nulls_first`` to this value is what makes sqlglot emit a bare
        ``ORDER BY``: no NULLS clause, no ``CASE WHEN … IS NULL`` emulation.
        That is wanted for orderings that are internal machinery rather than a
        user-visible sort — a window frame's ``OVER (ORDER BY …)``, where an
        emulation term would change which rows the frame covers.

        Read from the same dialect class that GENERATES the clause, for the
        same reason :func:`_sqlglot_backslash_escapes` reads the tokenizer: a
        hand-kept table would silently disagree with the emitter, and the
        symptom is a wrong sort rather than an error.
        """
        ordering = getattr(
            _SqlglotDialect.get_or_raise(self.sqlglot_name),
            "NULL_ORDERING", None,
        )
        if ordering == "nulls_are_last":
            return False
        if ordering == "nulls_are_small":
            return not descending
        if ordering == "nulls_are_large":
            return descending
        raise RuntimeError(
            f"Cannot derive the native null ordering for sqlglot dialect "
            f"{self.sqlglot_name!r}: NULL_ORDERING is {ordering!r}. A sqlglot "
            f"upgrade may have changed this API.",
        )

    @staticmethod
    def _expanded_null_safe_eq(
        left: exp.Expression, right: exp.Expression,
    ) -> exp.Expression:
        """``left = right OR (left IS NULL AND right IS NULL)`` — the portable
        expansion for dialects without a native null-safe equality operator."""
        eq = exp.EQ(this=left.copy(), expression=right.copy())
        both_null = exp.And(
            this=exp.Is(this=left.copy(), expression=exp.Null()),
            expression=exp.Is(this=right.copy(), expression=exp.Null()),
        )
        return exp.paren(exp.Or(this=eq, expression=exp.paren(both_null)))

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
        if granularity == TimeGranularity.WEEK_SUNDAY:
            # DEV-1572: Sunday-anchored week = Monday-week of (col + 1 day),
            # shifted back 1 day. This is Metabase's own reference formula and
            # reuses each dialect's existing (Monday-based) WEEK truncation, so
            # WEEK_SUNDAY's correctness tracks WEEK's per dialect. BigQuery —
            # whose native WEEK is Sunday — overrides this to emit
            # ``DATE_TRUNC(col, WEEK(SUNDAY))`` directly.
            shifted = self.build_time_offset_expr(
                col_expr=col_expr, offset=1, granularity="day",
            )
            monday = self.build_date_trunc(
                col_expr=shifted, granularity=TimeGranularity.WEEK, parse=parse,
            )
            return self.build_time_offset_expr(
                col_expr=monday, offset=-1, granularity="day",
            )
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

    def build_approx_count_distinct(
        self,
        col_sql: str,
        *,
        parse: Callable[[str], exp.Expression],
    ) -> exp.Expression:
        """Approximate-distinct aggregate, driven by the two config fields.

        Base default is the exact ``COUNT(DISTINCT col)`` fallback (Postgres /
        SQLite / MySQL) — more accurate than an approximation, per the "no
        approximate SQL" rule.
        """
        if self.approx_count_distinct_anonymous_name is not None:
            return exp.Anonymous(
                this=self.approx_count_distinct_anonymous_name,
                expressions=[parse(col_sql)],
            )
        return parse(self.approx_count_distinct_template.replace("{col}", col_sql))

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

    def rewrite_target_ast(self, tree: exp.Expression) -> exp.Expression:
        """Default: identity. Target-keyed AST rewrite (DEV-1576).

        Applied in ``SQLGenerator._parse`` using the generator's **target**
        dialect (``self._dialect``), independent of the parse dialect. This is
        the place for output-shaping a dialect needs that the input-side
        ``rewrite_parsed_ast`` cannot do: formula/measure expressions are
        canonically parsed as Postgres regardless of target, so a
        ``rewrite_parsed_ast`` override would fire for every backend.

        ``PostgresDialect`` overrides this to wrap the first argument of a
        2-arg ``ROUND`` in a numeric ``CAST`` (Postgres has no
        ``round(double precision, integer)`` — only ``round(numeric, int)``).
        SQLite / DuckDB round ``DOUBLE`` natively, so they keep the identity.
        """
        return tree

    def apply_pagination(
        self,
        select: exp.Select,
        *,
        limit: Optional[int],
        offset: Optional[int],
    ) -> exp.Select:
        """Apply LIMIT/OFFSET to a completed ``SELECT`` (P-H).

        The single place pagination is expressed. Every render path routes
        here, so a dialect that spells pagination differently is handled once
        rather than per path — the cross-model combined statement used to append
        raw ``LIMIT``/``OFFSET`` text and emitted literal ``LIMIT`` on SQL
        Server, while the same query carrying a transform layer went through the
        outer wrap and came out correct.

        Setting the bounds on the ``Select`` is what makes transposition work:
        sqlglot rewrites them per dialect only when generating the wrapping
        SELECT, never from a free-standing ``Limit`` node.
        """
        out = select
        if limit is not None:
            out = out.limit(limit)
        if offset is not None:
            out = out.offset(offset)
        return out

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
        """Emit the DEV-1444 outer-projection wrap around ``inner_sql``.

        Contract: ``inner_sql`` is the inner SELECT with **trailing
        pagination already detached** (the planned outer-wrap path,
        ``SQLGenerator._emit_planned_outer_wrap``, owns it — pagination
        arrives as detached AST from the plan). ``order`` / ``limit`` /
        ``offset_arg`` are the detached sqlglot AST nodes the caller pulled
        off the inner; the hook re-emits them on the outer statement.

        ``parse`` is the generator's ``_parse`` callback when the
        generator is the caller (``SQLGenerator._emit_planned_outer_wrap``).
        T-SQL needs it to preserve SLayer-specific AST rewrites (LOG10/
        LOG2 alias preservation, SQLite JSONExtract function-form) when
        the override re-parses ``inner_sql`` to detach the WITH clause.
        The base impl ignores it because it embeds ``inner_sql`` verbatim
        (no re-parse, no rewrite drift).

        Base impl (Postgres-shaped, used by every dialect except T-SQL)::

            SELECT "alias1", "alias2"
            FROM   (<inner_sql>) AS _outer
            ORDER BY ... LIMIT N OFFSET M

        Identifier quoting on the public-alias list is driven by sqlglot
        via ``self.sqlglot_name`` — backticks on MySQL/BigQuery, brackets
        on T-SQL (the override only changes the CTE-hoist shape, not the
        quoting), ANSI double quotes on Postgres/SQLite/DuckDB/...
        (DEV-1571 Bug 3).

        T-SQL's ``WITH``-must-be-statement-prefix rule means
        ``TsqlDialect`` overrides this method to lift the inner top-level
        CTEs to the outer statement (DEV-1571 Bug 1).

        ORDER BY may carry inner-CTE qualifiers like ``_base."col"`` from
        ``_assemble_combined_sql``; those don't resolve at the outer-
        wrapper scope (only ``_outer`` is in scope). The base impl strips
        every Column's ``table`` qualifier so the outer scope can resolve
        each column by its bare alias name (DEV-1444 behaviour preserved).
        """
        del parse  # base impl embeds inner_sql verbatim; no re-parse needed.
        col_sep = ",\n    "
        outer_select = col_sep.join(
            exp.Identifier(this=a, quoted=True).sql(dialect=self.sqlglot_name)
            for a in public
        )
        base = (
            f"SELECT\n    {outer_select}\n"
            f"FROM (\n{inner_sql.rstrip()}\n) AS _outer"
        )
        if order is None and limit is None and offset_arg is None:
            return base
        out = base
        if order is not None:
            order = order.transform(
                lambda node: (
                    self._outer_order_column(
                        col=node, public=public, inner_sql=inner_sql,
                    )
                    if isinstance(node, exp.Column) and len(node.parts) > 1
                    else node
                )
            )
            out += "\n" + order.sql(dialect=self.sqlglot_name, pretty=True)
        if limit is not None:
            out += "\n" + limit.sql(dialect=self.sqlglot_name, pretty=True)
        if offset_arg is not None:
            out += "\n" + offset_arg.sql(dialect=self.sqlglot_name, pretty=True)
        return out

    def _outer_order_column(
        self, *, col: exp.Column, public: Sequence[str], inner_sql: str,
    ) -> exp.Column:
        """Re-resolve a qualified ORDER BY column against the ``_outer`` scope.

        BigQuery parses a quoted dotted alias (`` `orders.created_at` ``) into
        one part per segment, so clearing the ``table`` arg would both drop the
        model prefix and leave an empty qualifier; instead keep the longest
        part-suffix that names a column of the outer scope. ``public`` is the
        authoritative half of that scope; the ``inner_sql`` scan is the fallback
        for an ORDER BY over a hidden hoist, which is projected but not public.
        """
        candidates = [
            ".".join(p.name for p in col.parts[i:]) for i in range(len(col.parts))
        ]
        for candidate in candidates:
            if candidate in public:
                return exp.Column(this=exp.Identifier(this=candidate, quoted=True))
        for candidate in candidates:
            if self.quote_identifier(candidate) in inner_sql:
                return exp.Column(this=exp.Identifier(this=candidate, quoted=True))
        return exp.Column(this=col.parts[-1].copy())

    # DEV-1756 identifier-length fitting. Aliases stay canonical inside SLayer,
    # fitted only on emission and restored on the result keys.

    def quote_identifier(self, name: str) -> str:
        """``name`` wrapped in this dialect's identifier quotes."""
        return exp.Identifier(this=name, quoted=True).sql(dialect=self.sqlglot_name)

    def fit_alias(self, name: str) -> str:
        """Length-only fitting; identity when ``name`` already fits.

        Drives the write pass (not ``emit_alias``), so an under-limit alias
        produces byte-identical SQL even on dialects that mangle dots.
        """
        return fit_identifier(name=name, limit=self.max_identifier_bytes)

    def emit_alias(self, alias: str) -> str:
        """The final identifier a canonical alias reaches the SQL as.

        Equals ``fit_alias`` here; BigQuery/T-SQL compose dot-mangling on top.
        Used to build the read-side map, so must match the emitted token exactly.
        """
        return self.fit_alias(alias)

    def alias_rewrite_map(self, aliases: Sequence[str]) -> dict[str, str]:
        """``{canonical: fitted}`` for the write pass, only where they differ.

        The collision check covers every alias including identities: a short
        alias equal to another's fitted form is just as much a duplicate.
        """
        if self.max_identifier_bytes is None:
            return {}
        allocation: dict[str, str] = {}
        owner: dict[str, str] = {}
        for alias in aliases:
            if alias in allocation:
                continue
            fitted = self.fit_alias(alias)
            prior = owner.get(fitted)
            if prior is not None and prior != alias:
                raise IdentifierCollisionError(
                    first=prior, second=alias, emitted=fitted,
                    dialect=self.sqlglot_name, limit=self.max_identifier_bytes,
                    namespace="projection alias",
                )
            owner[fitted] = alias
            allocation[alias] = fitted
        return {k: v for k, v in allocation.items() if k != v}

    def decode_alias_map(self, aliases: Sequence[str]) -> dict[str, str]:
        """``{emitted: canonical}`` — read-side inverse, rebuilt by re-running
        the pure fitting rather than threading a map through generation.

        Raises on two canonical aliases fitting to one emitted form, for
        symmetry with ``alias_rewrite_map`` — the read side can be handed a
        different alias set than the write side, so its guard is independent.
        Ownership is recorded for identity aliases too (only the output map
        drops them), so a fitted alias colliding with an unchanged one is
        caught just as ``alias_rewrite_map`` catches it.
        """
        out: dict[str, str] = {}
        owner: dict[str, str] = {}
        for alias in aliases:
            emitted = self.emit_alias(alias)
            prior = owner.get(emitted)
            if prior is not None and prior != alias:
                raise IdentifierCollisionError(
                    first=prior, second=alias, emitted=emitted,
                    dialect=self.sqlglot_name, limit=self.max_identifier_bytes,
                    namespace="result key",
                )
            owner[emitted] = alias
            if emitted != alias:
                out[emitted] = alias
        return out

    def _rekey_row(
        self,
        row: dict[str, Any],
        mapping: dict[str, str],
        *,
        fallback: Callable[[str], str] | None = None,
    ) -> dict[str, Any]:
        """Apply ``mapping`` to a row's keys, erroring if two keys collapse onto
        one (silent column loss).

        ``fallback`` decodes keys absent from ``mapping`` (BigQuery/T-SQL
        ``___`` -> ``.``). Applied here, not upstream, so the collapse check
        sees every key.
        """
        out: dict[str, Any] = {}
        for key, value in row.items():
            if key in mapping:
                decoded = mapping[key]
            elif fallback is not None:
                decoded = fallback(key)
            else:
                decoded = key
            if decoded in out:
                raise IdentifierCollisionError(
                    first=key, second=decoded, emitted=decoded,
                    dialect=self.sqlglot_name, limit=self.max_identifier_bytes,
                    namespace="result key",
                )
            out[decoded] = value
        return out

    def rewrite_emitted_sql(
        self, sql: str, *, aliases: Sequence[str] = (),
    ) -> str:
        """Post-pass string rewrite of the final SQL; write-side companion to
        ``rewrite_parsed_ast``, applied at the end of ``generate()``.

        Base impl fits over-limit aliases (DEV-1756), replacing each canonical
        token everywhere it occurs. Driven by the query's own alias set, not a
        length regex, which bounds what it can touch (see :func:`substitute_quoted`).
        Empty ``aliases`` is a no-op. Must preserve query semantics, not shape.
        BigQuery/T-SQL compose dot-mangling after this pass.
        """
        mapping = self.alias_rewrite_map(aliases)
        if not mapping:
            return sql
        return substitute_quoted(sql=sql, mapping=mapping, quote=self.quote_identifier)

    def decode_result_keys(
        self,
        rows: list[dict[str, Any]],
        *,
        aliases: Sequence[str] = (),
    ) -> list[dict[str, Any]]:
        """Reverse the write-side rewrite on result-row keys, so consumers always
        see SLayer's canonical alias shape regardless of dialect or shortening.

        BigQuery/T-SQL additionally decode the ``___`` mangling back to dots.
        """
        mapping = self.decode_alias_map(aliases)
        if not mapping:
            return rows
        return [self._rekey_row(row=row, mapping=mapping) for row in rows]

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

    # ------------------------------------------------------------------
    # Engine / connection / runtime hooks
    #
    # These let a dialect carry its own runtime quirks (connection-string
    # form, engine-creation bridge, per-connection session setup, per-
    # statement timeout, cursor-type-code mapping) without spilling
    # dialect-specific conditionals into ``slayer/sql/engine_factory.py``
    # or ``slayer/sql/client.py``. Defaults are all no-op — concrete
    # dialects override what's relevant.
    # ------------------------------------------------------------------

    def build_connection_url(
        self,
        datasource: "DatasourceConfig",
    ) -> str | None:
        """Hook: dialect-specific connection-string builder.

        Returning ``None`` (the default) means: defer to
        ``DatasourceConfig.get_connection_string()``'s standard branches
        (sqlite / duckdb / tsql / generic URL form). SnowflakeDialect
        overrides this to emit either the
        ``snowflake://?connection_name=<name>`` sentinel or the inline
        ``snowflake-sqlalchemy`` URL.
        """
        return None

    def build_engine(
        self,
        datasource: "DatasourceConfig",
        *,
        connection_string: str,
    ) -> "sa.Engine | None":
        """Hook: build a dialect-specific SQLAlchemy engine.

        Returning ``None`` (the default) means: ``engine_factory`` falls
        back to ``sa.create_engine(connection_string, pool_pre_ping=True)``.
        SnowflakeDialect overrides this when the sentinel URL is in play,
        wiring the ``creator=`` kwarg to delegate to
        ``snowflake.connector.connect(connection_name=...)``.
        """
        return None

    # ------------------------------------------------------------------
    # Credential identity (engine-cache safety)
    # ------------------------------------------------------------------

    def credential_fingerprint(self, datasource: "DatasourceConfig") -> str:
        """Opaque identity of the credentials this datasource authenticates with.

        Part of the engine cache key: a dialect whose secret is *not* in the
        connection string MUST override this, or callers with different
        credentials share one engine. Return a digest, never raw secret —
        keys reach logs.
        """
        return _digest(datasource.credentials_json)

    def apply_session_overrides(
        self,
        dbapi_connection: Any,
        datasource: "DatasourceConfig",
    ) -> None:
        """Hook: per-connection session setup (e.g. ``USE WAREHOUSE``).

        Called by ``engine_factory``'s ``connect`` event listener on every
        new pooled connection. SnowflakeDialect overrides this to issue
        ``USE WAREHOUSE / USE ROLE / USE DATABASE / USE SCHEMA`` from the
        DatasourceConfig's typed fields.
        """
        return None

    def statement_timeout_sql(self, timeout_seconds: int) -> str | None:
        """Hook: SQL to set a per-statement timeout, or ``None`` if the
        dialect doesn't expose one or the existing client.py path handles
        it via a hardcoded branch (mysql / clickhouse / postgres).

        SnowflakeDialect returns
        ``ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = N``.
        """
        return None

    def map_cursor_type_code(self, type_code: int) -> str | None:
        """Hook: dialect-specific cursor-type-code → SLayer category
        (one of ``"number"``, ``"string"``, ``"time"``, ``"boolean"``).

        Returning ``None`` (the default) means: ``client._map_type_code``
        falls back to the Postgres OID map. SnowflakeDialect overrides
        this to return the snowflake-connector ``FieldType`` integer
        codes' mapping.
        """
        return None


class DottedAliasManglingMixin:
    """DEV-1571: shared ``.``-to-``___`` alias mangling for BigQuery / T-SQL.

    ``fit_alias`` / ``emit_alias`` / ``decode_result_keys`` are identical on both;
    only ``rewrite_emitted_sql``'s identifier-quote anchor differs, supplied via
    the three class attributes. Mixed in before ``SqlDialect`` so the base LENGTH
    pass runs first (``super().rewrite_emitted_sql``) and the dot-mangle composes
    on its still-dotted output.
    """

    dotted_alias_re: ClassVar[re.Pattern[str]]
    alias_quote_open: ClassVar[str]
    alias_quote_close: ClassVar[str]

    def fit_alias(self, name: str) -> str:
        """Size the budget against the post-mangle form (``.`` -> ``___`` adds 2
        bytes per dot); the return value stays dotted for the regex."""
        return fit_identifier(
            name=name, limit=self.max_identifier_bytes, expand=encode_alias,
        )

    def emit_alias(self, alias: str) -> str:
        """The final identifier: length-fitted, then dot-mangled."""
        return encode_alias(self.fit_alias(alias))

    def rewrite_emitted_sql(
        self, sql: str, *, aliases: Sequence[str] = (),
    ) -> str:
        """Base LENGTH pass, then ``.`` -> ``___`` inside quoted identifiers."""
        sql = super().rewrite_emitted_sql(sql=sql, aliases=aliases)
        return self.dotted_alias_re.sub(
            lambda m: (
                f"{self.alias_quote_open}{encode_alias(m.group(1))}"
                f"{self.alias_quote_close}"
            ),
            sql,
        )

    def decode_result_keys(
        self,
        rows: list[dict[str, Any]],
        *,
        aliases: Sequence[str] = (),
    ) -> list[dict[str, Any]]:
        """Reverse the mangling on result-row keys: consult the emitted->canonical
        map, falling back to the ``___`` -> ``.`` bijection for fitted keys."""
        mapping = self.decode_alias_map(aliases)
        return [
            self._rekey_row(row=row, mapping=mapping, fallback=decode_alias)
            for row in rows
        ]
